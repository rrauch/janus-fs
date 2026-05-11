use crate::blob::{Blob, BlobMut};
use crate::chunk::{Chunk, ChunkId, ChunkSink, ChunkSource};
use crate::chunk_map::{ChunkMap, ChunkMapEntry};
use futures_io::{AsyncRead, AsyncSeek, AsyncWrite};
use futures_util::{AsyncWriteExt, ready};
use std::cmp::min;
use std::future::Future;
use std::io::{ErrorKind, SeekFrom};
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

type BoxFut<T> = Pin<Box<dyn Future<Output = Result<T, std::io::Error>> + Send>>;

struct ChunkFetch {
    cached: Option<Chunk>,
    inflight: Option<(ChunkId, BoxFut<Chunk>)>,
}

impl ChunkFetch {
    fn new() -> Self {
        Self {
            cached: None,
            inflight: None,
        }
    }

    /// Poll to get a chunk by id. Returns `Poll::Ready(Ok(()))` when `self.cached` contains the
    /// requested chunk.
    fn poll_ensure<S: ChunkSource + 'static>(
        &mut self,
        chunk_id: &ChunkId,
        source: &Arc<S>,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        // Already cached?
        if let Some(ref c) = self.cached {
            if c.id() == chunk_id {
                return Poll::Ready(Ok(()));
            }
        }

        let is_fetching_correct = self
            .inflight
            .as_ref()
            .map(|(id, _)| id == chunk_id)
            .unwrap_or(false);

        if !is_fetching_correct {
            // Start a new fetch
            let id = chunk_id.clone();
            let source = source.clone();
            let fut = Box::pin(async move {
                source
                    .get_chunk(&id)
                    .await?
                    .ok_or_else(|| std::io::Error::new(ErrorKind::NotFound, "chunk not found"))
            });
            self.inflight = Some((chunk_id.clone(), fut));
        }

        let (_, fut) = self.inflight.as_mut().unwrap();
        let chunk = ready!(fut.as_mut().poll(cx))?;
        self.inflight = None;
        self.cached = Some(chunk);
        Poll::Ready(Ok(()))
    }

    fn get_cached(&self) -> &Chunk {
        self.cached.as_ref().expect("chunk not cached")
    }

    fn invalidate(&mut self) {
        self.cached = None;
        self.inflight = None;
    }
}

/// In-flight chunk submission.
struct SubmitInFlight {
    chunk_id: ChunkId,
    pos: u64,
    len: usize,
    fut: BoxFut<()>,
}

/// Write buffer that accumulates bytes for a single chunk.
struct WriteBuffer {
    start_pos: u64,
    content: Vec<u8>,
    dirty: bool,
}

impl WriteBuffer {
    fn new(start_pos: u64, capacity: usize) -> Self {
        Self {
            start_pos,
            content: Vec::with_capacity(capacity),
            dirty: false,
        }
    }

    fn from_data(start_pos: u64, capacity: usize, data: &[u8]) -> Self {
        let mut buf = Self::new(start_pos, capacity);
        buf.content.extend_from_slice(data);
        buf
    }

    fn end_pos(&self) -> u64 {
        self.start_pos + self.content.len() as u64
    }

    fn capacity_end(&self) -> u64 {
        self.start_pos + self.content.capacity() as u64
    }

    fn contains(&self, pos: u64) -> bool {
        pos >= self.start_pos && pos < self.capacity_end()
    }

    fn read_at(&self, pos: u64, buf: &mut [u8]) -> usize {
        if pos < self.start_pos || pos >= self.end_pos() {
            return 0;
        }
        let offset = (pos - self.start_pos) as usize;
        let available = &self.content[offset..];
        let n = min(buf.len(), available.len());
        buf[..n].copy_from_slice(&available[..n]);
        n
    }

    fn write_at(&mut self, pos: u64, data: &[u8]) -> usize {
        debug_assert!(self.contains(pos));
        let offset = (pos - self.start_pos) as usize;
        let n = write_into_vec(&mut self.content, offset, data);
        if n > 0 {
            self.dirty = true;
        }
        n
    }
}

/// Write pipeline: manages an optional buffer and an optional in-flight submission.
struct WritePipeline {
    buffer: Option<WriteBuffer>,
    submit: Option<SubmitInFlight>,
}

impl WritePipeline {
    fn new() -> Self {
        Self {
            buffer: None,
            submit: None,
        }
    }

    fn is_dirty(&self) -> bool {
        self.submit.is_some() || self.buffer.as_ref().map(|b| b.dirty).unwrap_or(false)
    }

    /// Ensure any in-flight submission completes. Returns the completed chunk metadata if one
    /// just finished.
    fn poll_submit_complete(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<Option<(ChunkId, u64, usize)>>> {
        if let Some(s) = self.submit.as_mut() {
            ready!(s.fut.as_mut().poll(cx))?;
            let s = self.submit.take().unwrap();
            Poll::Ready(Ok(Some((s.chunk_id, s.pos, s.len))))
        } else {
            Poll::Ready(Ok(None))
        }
    }

    /// Flush the current buffer by submitting it. Must be called in a loop until it returns
    /// Ready(Ok(())). Returns chunk metadata on successful submission.
    fn poll_flush<B: ChunkSink + 'static>(
        &mut self,
        backend: &Arc<B>,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<Option<(ChunkId, u64, usize)>>> {
        // First, complete any in-flight submission
        if self.submit.is_some() {
            return self.poll_submit_complete(cx);
        }

        // Then submit the dirty buffer
        match self.buffer.take() {
            Some(buf) if buf.dirty => {
                let len = buf.content.len();
                let chunk = Chunk::from(buf.content);
                let chunk_id = chunk.id().clone();
                let b = backend.clone();
                let fut = Box::pin(async move { b.insert_chunk(chunk).await });
                self.submit = Some(SubmitInFlight {
                    chunk_id,
                    pos: buf.start_pos,
                    len,
                    fut,
                });
                self.poll_submit_complete(cx)
            }
            Some(buf) => {
                // Clean buffer, discard
                drop(buf);
                Poll::Ready(Ok(None))
            }
            None => Poll::Ready(Ok(None)),
        }
    }
}

enum ChunkMapResult {
    Chunk {
        chunk_id: ChunkId,
        chunk_offset: usize,
        len: u64,
    },
    Zero(usize),
    Eof,
}

fn resolve_read(chunk_map: &ChunkMap, pos: u64, buf_len: usize) -> ChunkMapResult {
    match chunk_map.get(pos) {
        Some(ChunkMapEntry::Chunk {
            chunk_id,
            chunk_offset,
            len,
        }) => ChunkMapResult::Chunk {
            chunk_id: chunk_id.clone(),
            chunk_offset,
            len,
        },
        Some(ChunkMapEntry::Hole { len }) => {
            let n = min(len as usize, buf_len);
            ChunkMapResult::Zero(n)
        }
        None => ChunkMapResult::Eof,
    }
}

fn copy_from_chunk(chunk: &Chunk, chunk_offset: usize, max_len: u64, buf: &mut [u8]) -> usize {
    let content = &chunk[chunk_offset..];
    if content.is_empty() {
        return 0;
    }
    let n = min(min(max_len as usize, buf.len()), content.len());
    buf[..n].copy_from_slice(&content[..n]);
    n
}

pub struct BlobIo<M> {
    mode: M,
    pos: u64,
}

struct ReadOnly<S> {
    blob: Blob,
    fetch: ChunkFetch,
    backend: Arc<S>,
}

pub type BlobReader<S> = BlobIo<ReadOnly<S>>;

impl<S: ChunkSource + 'static> BlobReader<S> {
    pub(crate) fn new_reader(blob: Blob, source: S) -> Self {
        Self {
            mode: ReadOnly {
                blob,
                fetch: ChunkFetch::new(),
                backend: Arc::new(source),
            },
            pos: 0,
        }
    }

    pub fn len(&self) -> u64 {
        self.mode.blob.len()
    }

    pub fn is_empty(&self) -> bool {
        self.mode.blob.is_empty()
    }
}

impl<S: ChunkSource + 'static> AsyncRead for BlobReader<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let pos = self.pos;
        if pos >= self.mode.blob.len() {
            return Poll::Ready(Ok(0));
        }

        let result = resolve_read(&self.mode.blob.chunk_map, pos, buf.len());
        match result {
            ChunkMapResult::Zero(n) => {
                buf[..n].fill(0);
                self.pos += n as u64;
                Poll::Ready(Ok(n))
            }
            ChunkMapResult::Eof => Poll::Ready(Ok(0)),
            ChunkMapResult::Chunk {
                chunk_id,
                chunk_offset,
                len,
            } => {
                let backend = self.mode.backend.clone();
                ready!(self.mode.fetch.poll_ensure(&chunk_id, &backend, cx))?;
                let chunk = self.mode.fetch.get_cached();
                let n = copy_from_chunk(chunk, chunk_offset, len, buf);
                self.pos += n as u64;
                Poll::Ready(Ok(n))
            }
        }
    }
}

impl<S: ChunkSource + 'static> AsyncSeek for BlobReader<S> {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<std::io::Result<u64>> {
        let len = self.mode.blob.len();
        let target = compute_seek_target(self.pos, len, pos)?;
        self.pos = target;
        Poll::Ready(Ok(target))
    }
}

struct ReadWrite<B> {
    blob: BlobMut,
    fetch: ChunkFetch,
    pipeline: WritePipeline,
    max_chunk_size: usize,
    backend: Arc<B>,
    closed: bool,
}

impl<B: ChunkSource + ChunkSink + 'static> ReadWrite<B> {
    fn check_closed(&self) -> std::io::Result<()> {
        if self.closed {
            Err(std::io::Error::other("already closed"))
        } else {
            Ok(())
        }
    }

    /// Flush the write pipeline, updating the blob's chunk map on completion.
    fn poll_flush_pipeline(&mut self, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        loop {
            match ready!(self.pipeline.poll_flush(&self.backend, cx))? {
                Some((chunk_id, pos, len)) => {
                    let new_len = pos + len as u64;
                    if new_len > self.blob.len() {
                        self.blob.set_len(new_len);
                    }
                    self.blob.chunk_map.insert(pos, len as u64, chunk_id, 0);
                    // Loop to check if there's still a dirty buffer to flush
                }
                None => return Poll::Ready(Ok(())),
            }
        }
    }

    /// Ensure the submission is complete before proceeding.
    fn poll_ensure_submit_complete(&mut self, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        if let Some((chunk_id, pos, len)) = ready!(self.pipeline.poll_submit_complete(cx))? {
            let new_len = pos + len as u64;
            if new_len > self.blob.len() {
                self.blob.set_len(new_len);
            }
            self.blob.chunk_map.insert(pos, len as u64, chunk_id, 0);
        }
        Poll::Ready(Ok(()))
    }

    fn poll_read_impl(
        &mut self,
        pos: u64,
        buf: &mut [u8],
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<usize>> {
        self.check_closed()?;

        // First, complete any in-flight submission
        ready!(self.poll_ensure_submit_complete(cx))?;

        // Try reading from the write buffer first
        if let Some(ref write_buf) = self.pipeline.buffer {
            let n = write_buf.read_at(pos, buf);
            if n > 0 {
                return Poll::Ready(Ok(n));
            }
        }

        // Resolve from chunk map
        let result = resolve_read(&self.blob.chunk_map, pos, buf.len());
        match result {
            ChunkMapResult::Zero(n) => {
                buf[..n].fill(0);
                Poll::Ready(Ok(n))
            }
            ChunkMapResult::Eof => Poll::Ready(Ok(0)),
            ChunkMapResult::Chunk {
                chunk_id,
                chunk_offset,
                len,
            } => {
                ready!(self.fetch.poll_ensure(&chunk_id, &self.backend, cx))?;
                let chunk = self.fetch.get_cached();
                let n = copy_from_chunk(chunk, chunk_offset, len, buf);
                Poll::Ready(Ok(n))
            }
        }
    }

    fn poll_write_impl(
        &mut self,
        pos: u64,
        buf: &[u8],
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<usize>> {
        self.check_closed()?;

        // Ensure submission is complete
        ready!(self.poll_ensure_submit_complete(cx))?;

        // If we have a buffer that doesn't cover this position, flush it
        if let Some(write_buf) = self.pipeline.buffer.as_ref() {
            if !write_buf.contains(pos) {
                // Need to flush first
                ready!(self.poll_flush_pipeline(cx))?;
                // Invalidate fetch cache since chunk map changed
                self.fetch.invalidate();
            }
        }

        // If no buffer, create one
        if self.pipeline.buffer.is_none() {
            match self.blob.chunk_map.get(pos) {
                Some(ChunkMapEntry::Chunk {
                    chunk_id,
                    chunk_offset,
                    ..
                }) => {
                    // Need to fetch existing chunk data for partial overwrite
                    ready!(self.fetch.poll_ensure(chunk_id, &self.backend, cx))?;
                    let chunk = self.fetch.get_cached();
                    let start_pos = pos.saturating_sub(chunk_offset as u64);
                    self.pipeline.buffer = Some(WriteBuffer::from_data(
                        start_pos,
                        chunk.len(),
                        chunk.deref(),
                    ));
                }
                Some(ChunkMapEntry::Hole { len }) => {
                    let cap = min(len as usize, self.max_chunk_size);
                    self.pipeline.buffer = Some(WriteBuffer::new(pos, cap));
                }
                None => {
                    self.pipeline.buffer = Some(WriteBuffer::new(pos, self.max_chunk_size));
                }
            };
        }

        // Write into the buffer
        let write_buf = self.pipeline.buffer.as_mut().unwrap();
        let n = write_buf.write_at(pos, buf);
        Poll::Ready(Ok(n))
    }
}

pub type BlobWriter<B> = BlobIo<ReadWrite<B>>;

impl<B: ChunkSource + ChunkSink + 'static> BlobWriter<B> {
    pub(crate) fn new_writer(blob: BlobMut, backend: B, max_chunk_size: usize) -> Self {
        Self {
            mode: ReadWrite {
                blob,
                fetch: ChunkFetch::new(),
                pipeline: WritePipeline::new(),
                max_chunk_size,
                backend: Arc::new(backend),
                closed: false,
            },
            pos: 0,
        }
    }

    pub fn len(&self) -> u64 {
        self.mode.blob.len()
    }

    pub fn is_empty(&self) -> bool {
        self.mode.blob.chunk_map.is_empty() && !self.mode.pipeline.is_dirty()
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

impl<B: ChunkSource + ChunkSink + 'static> AsyncRead for BlobWriter<B> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let pos = self.pos;
        if pos >= self.mode.blob.len() && !self.mode.pipeline.is_dirty() {
            return Poll::Ready(Ok(0));
        }
        let n = ready!(self.mode.poll_read_impl(pos, buf, cx))?;
        self.pos += n as u64;
        Poll::Ready(Ok(n))
    }
}

impl<B: ChunkSource + ChunkSink + 'static> AsyncSeek for BlobWriter<B> {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<std::io::Result<u64>> {
        let len = self.mode.blob.len();
        let target = compute_seek_target(self.pos, len, pos)?;
        self.pos = target;
        Poll::Ready(Ok(target))
    }
}

impl<B: ChunkSource + ChunkSink + 'static> AsyncWrite for BlobWriter<B> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let pos = self.pos;
        let n = ready!(self.mode.poll_write_impl(pos, buf, cx))?;
        self.pos += n as u64;
        Poll::Ready(Ok(n))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.mode.check_closed()?;
        self.mode.poll_flush_pipeline(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        if !self.mode.closed {
            ready!(self.mode.poll_flush_pipeline(cx))?;
            self.mode.closed = true;
        }
        Poll::Ready(Ok(()))
    }
}

fn compute_seek_target(current: u64, len: u64, pos: SeekFrom) -> std::io::Result<u64> {
    let target = match pos {
        SeekFrom::Start(n) => n,
        SeekFrom::Current(n) => current
            .checked_add_signed(n)
            .ok_or_else(|| std::io::Error::new(ErrorKind::InvalidInput, "seek underflow"))?,
        SeekFrom::End(n) => len
            .checked_add_signed(n)
            .ok_or_else(|| std::io::Error::new(ErrorKind::InvalidInput, "seek underflow"))?,
    };

    if target > len {
        return Err(std::io::Error::new(
            ErrorKind::InvalidInput,
            "seeking beyond EOF unsupported",
        ));
    }

    Ok(target)
}

fn write_into_vec(vec: &mut Vec<u8>, offset: usize, data: &[u8]) -> usize {
    let cap = vec.capacity();
    assert!(offset <= cap);
    let writable = data.len().min(cap - offset);
    let end = offset + writable;
    let prev_len = vec.len();
    if end > prev_len {
        // SAFETY: All writes are within allocated capacity.
        // Bytes between prev_len and offset are zeroed.
        unsafe {
            if offset > prev_len {
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
