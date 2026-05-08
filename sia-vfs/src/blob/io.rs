use crate::blob::{Blob, BlobMut};
use crate::chunk::{Chunk, ChunkId, ChunkSink, ChunkSource};
use crate::chunk_map::{ChunkMap, ChunkMapEntry};
use futures_io::{AsyncRead, AsyncSeek, AsyncWrite};
use futures_util::{AsyncWriteExt, ready};
use std::cmp::min;
use std::io::{ErrorKind, SeekFrom};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

type RetrieveFut = Pin<Box<dyn Future<Output = Result<Chunk, std::io::Error>> + Send>>;

enum ReadState {
    Ready(Option<Chunk>),
    Retrieving {
        fut: Mutex<RetrieveFut>,
        chunk_id: ChunkId,
    },
}

impl ReadState {
    fn take(&mut self) -> Self {
        std::mem::take(self)
    }
}

impl Default for ReadState {
    fn default() -> Self {
        Self::Ready(None)
    }
}

type SubmitFut = Pin<Box<dyn Future<Output = Result<(), std::io::Error>> + Send>>;

#[derive(Default)]
enum WriteState {
    #[default]
    Idle,
    Buffering(WriteBuffer),
    Submitting {
        fut: Mutex<SubmitFut>,
        chunk_id: ChunkId,
        pos: u64,
        len: usize,
    },
    Retrieving {
        fut: Mutex<RetrieveFut>,
        chunk_id: ChunkId,
    },
}

struct WriteBuffer {
    start_pos: u64,
    content: Vec<u8>,
    dirty: bool,
}

impl WriteBuffer {
    fn new(start_pos: u64, chunk_size: usize, initial_data: &[u8]) -> Self {
        let len = min(chunk_size, initial_data.len());
        let data = &initial_data[..len];
        let mut content = Vec::with_capacity(chunk_size);
        content.extend_from_slice(data);
        Self {
            start_pos,
            content,
            dirty: false,
        }
    }

    fn empty(start_pos: u64, chunk_size: usize) -> Self {
        let content = Vec::with_capacity(chunk_size);
        Self {
            start_pos,
            content,
            dirty: false,
        }
    }
}

impl WriteState {
    fn take(&mut self) -> Self {
        std::mem::take(self)
    }

    fn on_chunk_submission(
        &mut self,
        fut: Mutex<SubmitFut>,
        chunk_id: ChunkId,
        pos: u64,
        len: usize,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<(ChunkId, u64, usize)>> {
        let mut guard = fut.lock().unwrap();
        match guard.as_mut().poll(cx) {
            Poll::Pending => {
                drop(guard);
                *self = WriteState::Submitting {
                    fut,
                    chunk_id,
                    pos,
                    len,
                };
                Poll::Pending
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Ready(Ok(_)) => {
                *self = WriteState::Idle;
                Poll::Ready(Ok((chunk_id, pos, len)))
            }
        }
    }

    fn on_chunk_retrieval(
        &mut self,
        mutex: Mutex<RetrieveFut>,
        chunk_id: ChunkId,
        chunk_offset: usize,
        chunk_size: usize,
        pos: u64,
        len: u64,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        let mut fut = mutex.lock().unwrap();

        match fut.as_mut().poll(cx) {
            Poll::Pending => {
                drop(fut);
                *self = WriteState::Retrieving {
                    fut: mutex,
                    chunk_id,
                };
                Poll::Pending
            }
            Poll::Ready(Err(err)) => Poll::Ready(Err(std::io::Error::other(err))),
            Poll::Ready(Ok(chunk)) => {
                let chunk_size = chunk_size.saturating_sub(chunk_offset);
                assert!(chunk_size > 0);
                let data = &chunk[chunk_offset..(chunk_offset + len as usize)];
                *self = WriteState::Buffering(WriteBuffer::new(pos, chunk_size, data));
                Poll::Ready(Ok(()))
            }
        }
    }

    fn is_dirty(&self) -> bool {
        match self {
            Self::Buffering(buf) => buf.dirty,
            Self::Submitting { .. } => true,
            _ => false,
        }
    }
}

pub struct BlobIo<M> {
    mode: M,
    pos: u64,
}

trait ReadMode: Send + Sync + Unpin {
    fn len(&self) -> u64;
    fn is_empty(&self) -> bool;
    fn poll_read(
        &mut self,
        pos: u64,
        buf: &mut [u8],
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<usize>>;
}

trait WriteMode: Send + Sync + Unpin {
    fn poll_write(
        &mut self,
        pos: u64,
        buf: &[u8],
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<usize>>;
    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<std::io::Result<()>>;
    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<std::io::Result<()>>;
}

struct ReadOnly<S> {
    blob: Blob,
    state: ReadState,
    backend: Arc<S>,
}

impl<S: ChunkSource + 'static> ReadOnly<S> {
    fn on_chunk_retrieval(
        &mut self,
        mutex: Mutex<RetrieveFut>,
        chunk_id: ChunkId,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        let mut fut = mutex.lock().unwrap();

        match fut.as_mut().poll(cx) {
            Poll::Pending => {
                drop(fut);
                self.state = ReadState::Retrieving {
                    fut: mutex,
                    chunk_id,
                };
                Poll::Pending
            }
            Poll::Ready(Err(err)) => Poll::Ready(Err(std::io::Error::other(err))),
            Poll::Ready(Ok(chunk)) => {
                self.state = ReadState::Ready(Some(chunk));
                Poll::Ready(Ok(()))
            }
        }
    }
}

impl<S: ChunkSource + 'static> ReadMode for ReadOnly<S> {
    fn len(&self) -> u64 {
        self.blob.len()
    }

    fn is_empty(&self) -> bool {
        self.blob.is_empty()
    }

    fn poll_read(
        &mut self,
        pos: u64,
        buf: &mut [u8],
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<usize>> {
        let (chunk_id, chunk_offset, len) =
            match read_from_chunk_map(&self.blob.chunk_map, pos, buf) {
                ChunkMapResult::Zero(n) => return Poll::Ready(Ok(n)),
                ChunkMapResult::Eof => return Poll::Ready(Ok(0)),
                ChunkMapResult::Chunk(c) => c,
            };

        loop {
            match self.state.take() {
                ReadState::Ready(Some(chunk)) if &chunk_id == chunk.id() => {
                    // needed chunk ready
                    let n = fill_buf(&chunk, chunk_offset, len as usize, buf);
                    if n == 0 {
                        // continue to next chunk
                        continue;
                    }
                    return Poll::Ready(Ok(n));
                }
                ReadState::Ready(_) => {
                    let (fut, chunk_id) = retrieve_chunk(self.backend.clone(), chunk_id.clone());
                    self.state = ReadState::Retrieving { fut, chunk_id };
                }
                ReadState::Retrieving {
                    fut,
                    chunk_id: ret_chunk_id,
                } => {
                    if chunk_id != ret_chunk_id {
                        // this is not the chunk we're looking for
                        continue;
                    }
                    ready!(self.on_chunk_retrieval(fut, ret_chunk_id, cx))?;
                }
            }
        }
    }
}

pub type BlobReader<S> = BlobIo<ReadOnly<S>>;

impl<S: ChunkSource + 'static> BlobReader<S> {
    pub(crate) fn new_reader(blob: Blob, source: S) -> Self {
        Self {
            mode: ReadOnly {
                blob,
                state: ReadState::default(),
                backend: Arc::new(source),
            },
            pos: 0,
        }
    }
}

impl<M: ReadMode + 'static> BlobIo<M> {
    pub fn len(&self) -> u64 {
        self.mode.len()
    }

    pub fn is_empty(&self) -> bool {
        self.mode.is_empty()
    }
}

impl<M: ReadMode + 'static> AsyncRead for BlobIo<M> {
    #[inline]
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let pos = self.pos;
        if pos > self.len() {
            return Poll::Ready(Ok(0));
        }

        let n = ready!(self.mode.poll_read(pos, buf, cx)?);
        self.pos += n as u64;
        Poll::Ready(Ok(n))
    }
}

impl<M: ReadMode + 'static> AsyncSeek for BlobIo<M> {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<std::io::Result<u64>> {
        let len = self.len();

        let target = match pos {
            SeekFrom::Start(n) => n,
            SeekFrom::Current(n) => self
                .pos
                .checked_add_signed(n)
                .ok_or_else(|| std::io::Error::new(ErrorKind::InvalidInput, "seek underflow"))?,
            SeekFrom::End(n) => len
                .checked_add_signed(n)
                .ok_or_else(|| std::io::Error::new(ErrorKind::InvalidInput, "seek underflow"))?,
        };

        if target > len {
            return Poll::Ready(Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                "seeking beyond EOF unsupported",
            )));
        }

        self.pos = target;
        Poll::Ready(Ok(target))
    }
}

impl<M: WriteMode + 'static> AsyncWrite for BlobIo<M> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let pos = self.pos;
        let n = ready!(self.mode.poll_write(pos, buf, cx)?);
        self.pos += n as u64;
        Poll::Ready(Ok(n))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.mode.poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.mode.poll_close(cx)
    }
}

struct ReadWrite<B> {
    blob: BlobMut,
    state: WriteState,
    max_chunk_size: usize,
    backend: Arc<B>,
    closed: bool,
}

impl<B: ChunkSource + ChunkSink + 'static> ReadWrite<B> {
    fn check_closed(&self) -> Poll<std::io::Result<()>> {
        if self.closed {
            Poll::Ready(Err(std::io::Error::other("already closed")))
        } else {
            Poll::Ready(Ok(()))
        }
    }
}

impl<B: ChunkSource + ChunkSink + 'static> ReadMode for ReadWrite<B> {
    fn len(&self) -> u64 {
        self.blob.len()
    }

    fn is_empty(&self) -> bool {
        self.blob.chunk_map.is_empty() && !self.state.is_dirty()
    }

    fn poll_read(
        &mut self,
        pos: u64,
        buf: &mut [u8],
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<usize>> {
        ready!(self.check_closed()?);
        loop {
            match self.state.take() {
                WriteState::Idle => {
                    let (chunk_id, _, _) = match read_from_chunk_map(&self.blob.chunk_map, pos, buf)
                    {
                        ChunkMapResult::Zero(n) => return Poll::Ready(Ok(n)),
                        ChunkMapResult::Eof => return Poll::Ready(Ok(0)),
                        ChunkMapResult::Chunk(c) => c,
                    };

                    let (fut, chunk_id) = retrieve_chunk(self.backend.clone(), chunk_id);

                    self.state = WriteState::Retrieving { fut, chunk_id }
                }
                WriteState::Buffering(write_buf) => {
                    let end_pos = write_buf.start_pos + write_buf.content.len() as u64;
                    let n = if pos >= write_buf.start_pos && pos < end_pos {
                        let offset = (pos - write_buf.start_pos) as usize;
                        let content = &write_buf.content[offset..];
                        let n = min(buf.len(), content.len());
                        buf[..n].copy_from_slice(&content[..n]);
                        n
                    } else {
                        0
                    };
                    self.state = WriteState::Buffering(write_buf);
                    if n > 0 {
                        // data found in buffer
                        return Poll::Ready(Ok(n));
                    }
                    // not the buffer we need
                    ready!(self.poll_flush(cx))?;
                }
                WriteState::Submitting {
                    fut,
                    chunk_id,
                    pos,
                    len,
                } => {
                    // any pending submission must be completed first
                    ready!(
                        self.state
                            .on_chunk_submission(fut, chunk_id, pos, len, cx)?
                    );
                }
                WriteState::Retrieving {
                    fut,
                    chunk_id: retr_chunk_id,
                } => {
                    let (chunk_id, chunk_offset, len) =
                        match read_from_chunk_map(&self.blob.chunk_map, pos, buf) {
                            ChunkMapResult::Zero(n) => return Poll::Ready(Ok(n)),
                            ChunkMapResult::Eof => return Poll::Ready(Ok(0)),
                            ChunkMapResult::Chunk(c) => c,
                        };

                    if retr_chunk_id != chunk_id {
                        // not the chunk we need, can be discarded
                        continue;
                    }

                    ready!(self.state.on_chunk_retrieval(
                        fut,
                        chunk_id,
                        chunk_offset,
                        self.max_chunk_size,
                        pos,
                        len,
                        cx
                    )?);
                }
            }
        }
    }
}

impl<B: ChunkSource + ChunkSink + 'static> WriteMode for ReadWrite<B> {
    fn poll_write(
        &mut self,
        pos: u64,
        buf: &[u8],
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<usize>> {
        ready!(self.check_closed()?);
        loop {
            match self.state.take() {
                WriteState::Idle => {
                    self.state = match self.blob.chunk_map.get(pos) {
                        Some(ChunkMapEntry::Chunk { chunk_id, .. }) => {
                            let fut = {
                                let chunk_id = chunk_id.clone();
                                let backend = self.backend.clone();
                                Box::pin(async move {
                                    backend.get_chunk(&chunk_id).await?.ok_or_else(|| {
                                        std::io::Error::new(ErrorKind::NotFound, "chunk not found")
                                    })
                                })
                            };
                            WriteState::Retrieving {
                                fut: Mutex::new(fut),
                                chunk_id: chunk_id.clone(),
                            }
                        }
                        Some(ChunkMapEntry::Hole { len }) => {
                            let len = min(len as usize, self.max_chunk_size);
                            WriteState::Buffering(WriteBuffer::empty(pos, len))
                        }
                        None => WriteState::Buffering(WriteBuffer::empty(pos, self.max_chunk_size)),
                    }
                }
                WriteState::Buffering(mut write_buf) => {
                    if pos < write_buf.start_pos
                        || (write_buf.start_pos + write_buf.content.capacity() as u64) < pos
                    {
                        // not the buffer we need
                        self.state = WriteState::Buffering(write_buf);
                        ready!(Self::poll_flush(self, cx))?;
                        continue;
                    }

                    let offset = pos.saturating_sub(write_buf.start_pos) as usize;
                    let n = write_at(&mut write_buf.content, offset, buf);
                    self.state = WriteState::Buffering(write_buf);
                    return Poll::Ready(Ok(n));
                }
                WriteState::Submitting {
                    fut,
                    chunk_id,
                    pos,
                    len,
                } => {
                    // any pending submission must be completed
                    ready!(self.state.on_chunk_submission(fut, chunk_id, pos, len, cx))?;
                }
                WriteState::Retrieving { fut, chunk_id, .. } => {
                    match self.blob.chunk_map.get(pos) {
                        Some(ChunkMapEntry::Chunk {
                            chunk_id: retr_chunk_id,
                            chunk_offset,
                            len,
                        }) if retr_chunk_id == &chunk_id => {
                            let chunk_size = self.max_chunk_size;
                            ready!(self.state.on_chunk_retrieval(
                                fut,
                                chunk_id,
                                chunk_offset,
                                chunk_size,
                                pos,
                                len,
                                cx,
                            ))?;
                        }
                        _ => {
                            // not the chunk we need, can be discarded
                        }
                    }
                }
            }
        }
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        ready!(self.check_closed()?);
        loop {
            match self.state.take() {
                WriteState::Idle | WriteState::Retrieving { .. } => {
                    return Poll::Ready(Ok(()));
                }
                WriteState::Buffering(write_buf) => {
                    if !write_buf.dirty {
                        // buffer is clean, discard
                        continue;
                    }
                    let len = write_buf.content.len();
                    let chunk = Chunk::from(write_buf.content);
                    let chunk_id = chunk.id().clone();
                    let backend = self.backend.clone();
                    let fut = Box::pin(async move { backend.insert_chunk(chunk).await });
                    self.state = WriteState::Submitting {
                        fut: Mutex::new(fut),
                        chunk_id,
                        pos: write_buf.start_pos,
                        len,
                    }
                }
                WriteState::Submitting {
                    fut,
                    chunk_id,
                    pos,
                    len,
                } => {
                    let (chunk_id, pos, len) =
                        ready!(self.state.on_chunk_submission(fut, chunk_id, pos, len, cx))?;

                    let new_len = pos + len as u64;
                    if new_len > self.blob.len() {
                        self.blob.set_len(new_len);
                    }
                    self.blob.chunk_map.insert(pos, len as u64, chunk_id, 0);

                    return Poll::Ready(Ok(()));
                }
            }
        }
    }

    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        if !self.closed {
            ready!(self.poll_flush(cx)?);
            self.closed = true;
        }
        Poll::Ready(Ok(()))
    }
}

pub type BlobWriter<B> = BlobIo<ReadWrite<B>>;

impl<B: ChunkSource + ChunkSink + 'static> BlobWriter<B> {
    pub(crate) fn new_writer(blob: BlobMut, backend: B, max_chunk_size: usize) -> Self {
        let backend = Arc::new(backend);
        Self {
            mode: ReadWrite {
                blob,
                state: WriteState::default(),
                max_chunk_size,
                backend,
                closed: false,
            },
            pos: 0,
        }
    }

    pub async fn set_len(&mut self, new_len: u64) -> Result<(), std::io::Error> {
        self.flush().await?;
        self.mode.blob.set_len(new_len);
        Ok(())
    }

    pub async fn finalize(mut self) -> Result<Blob, std::io::Error> {
        self.close().await?;
        Ok(self.mode.blob.finalize())
    }
}

fn write_at(vec: &mut Vec<u8>, offset: usize, data: &[u8]) -> usize {
    let cap = vec.capacity();
    assert!(offset <= cap);
    let writable = data.len().min(cap - offset);
    let end = offset + writable;
    let prev_len = vec.len();
    if end > prev_len {
        // SAFETY: All writes are within allocated capacity.
        // Potentially uninitialized bytes are never read.
        unsafe {
            if offset > prev_len {
                // zero the "gap"
                std::ptr::write_bytes(vec.as_mut_ptr().add(prev_len), 0, offset - prev_len);
            }
            std::ptr::copy_nonoverlapping(data.as_ptr(), vec.as_mut_ptr().add(offset), writable);
            vec.set_len(end);
        }
    } else {
        vec[offset..end].copy_from_slice(&data[..writable]);
    }
    writable
}

fn read_from_chunk_map(chunk_map: &ChunkMap, pos: u64, buf: &mut [u8]) -> ChunkMapResult {
    match chunk_map.get(pos) {
        Some(ChunkMapEntry::Chunk {
            chunk_id,
            chunk_offset,
            len,
        }) => ChunkMapResult::Chunk((chunk_id.clone(), chunk_offset, len)),
        Some(ChunkMapEntry::Hole { len }) => {
            let n = min(len as usize, buf.len());
            buf[..n].fill(0);
            ChunkMapResult::Zero(n)
        }
        None => ChunkMapResult::Eof,
    }
}

enum ChunkMapResult {
    Chunk((ChunkId, usize, u64)),
    Zero(usize),
    Eof,
}

fn retrieve_chunk<S: ChunkSource + 'static>(
    source: Arc<S>,
    chunk_id: ChunkId,
) -> (Mutex<RetrieveFut>, ChunkId) {
    let fut = {
        let chunk_id = chunk_id.clone();
        Box::pin(async move {
            source
                .get_chunk(&chunk_id)
                .await?
                .ok_or_else(|| std::io::Error::new(ErrorKind::NotFound, "Chunk not found"))
        })
    };

    (Mutex::new(fut), chunk_id)
}

fn fill_buf(chunk: &Chunk, chunk_offset: usize, len: usize, buf: &mut [u8]) -> usize {
    let n = min(len, buf.len());
    let content = &chunk[chunk_offset..];
    if content.is_empty() {
        return 0;
    }
    let n = min(n, content.len());
    buf[..n].copy_from_slice(&content[..n]);
    n
}
