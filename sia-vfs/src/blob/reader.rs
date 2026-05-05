use crate::blob::Blob;
use crate::chunk::{Chunk, ChunkId, ChunkSource};
use crate::chunk_map::ChunkMapEntry;
use futures_io::{AsyncRead, AsyncSeek};
use futures_util::ready;
use std::cmp::min;
use std::io::{ErrorKind, SeekFrom};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

enum State {
    Ready { chunk: Option<Chunk> },
    Retrieving { fut: Mutex<RetrieveFut> },
}

impl State {
    fn take(&mut self) -> Self {
        std::mem::take(self)
    }
}

impl Default for State {
    fn default() -> Self {
        Self::Ready { chunk: None }
    }
}

type RetrieveFut = Pin<Box<dyn Future<Output = Result<Chunk, std::io::Error>> + Send>>;

pub struct BlobReader<S> {
    blob: Blob,
    state: State,
    pos: u64,
    source: Arc<S>,
}

impl<S: ChunkSource + 'static> BlobReader<S> {
    pub(crate) fn new(blob: Blob, source: S) -> Self {
        Self {
            blob,
            state: State::default(),
            pos: 0,
            source: Arc::new(source),
        }
    }

    fn retrieve_chunk(&mut self, chunk_id: ChunkId) {
        let source = self.source.clone();

        let fut = Box::pin(async move {
            source
                .get_chunk(&chunk_id)
                .await?
                .ok_or_else(|| std::io::Error::new(ErrorKind::NotFound, "Chunk not found"))
        });

        self.state = State::Retrieving {
            fut: Mutex::new(fut),
        }
    }

    fn on_chunk(
        &mut self,
        mutex: Mutex<RetrieveFut>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        let mut fut = mutex.lock().unwrap();

        match fut.as_mut().poll(cx) {
            Poll::Pending => {
                drop(fut);
                self.state = State::Retrieving { fut: mutex };
                Poll::Pending
            }
            Poll::Ready(Err(err)) => Poll::Ready(Err(std::io::Error::other(err))),
            Poll::Ready(Ok(chunk)) => {
                self.state = State::Ready { chunk: Some(chunk) };
                Poll::Ready(Ok(()))
            }
        }
    }
}

impl<S: ChunkSource + 'static> AsyncRead for BlobReader<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let pos = self.pos;
        let (chunk_id, chunk_offset, len) = match self.blob.chunk_map.get(pos) {
            Some(ChunkMapEntry::Chunk {
                chunk_id,
                chunk_offset,
                len,
            }) => (chunk_id.clone(), chunk_offset, len),
            Some(ChunkMapEntry::Hole { len }) => {
                let n = min(len as usize, buf.len());
                buf[..n].fill(0);
                self.pos += n as u64;
                return Poll::Ready(Ok(n));
            }
            None => {
                // eof
                return Poll::Ready(Ok(0));
            }
        };

        let n = min(len as usize, buf.len());
        loop {
            match self.state.take() {
                State::Ready { chunk: Some(chunk) } if &chunk_id == chunk.id() => {
                    // needed chunk ready
                    let content = &chunk[chunk_offset..];
                    if content.is_empty() {
                        // continue to next chunk
                        continue;
                    }
                    let n = min(n, content.len());
                    buf[..n].copy_from_slice(&content[..n]);
                    self.state = State::Ready { chunk: Some(chunk) };
                    self.pos += n as u64;
                    return Poll::Ready(Ok(n));
                }
                State::Ready { .. } => {
                    self.retrieve_chunk(chunk_id.clone());
                }
                State::Retrieving { fut } => {
                    ready!(self.on_chunk(fut, cx))?;
                }
            }
        }
    }
}

impl<S: ChunkSource + 'static> AsyncSeek for BlobReader<S> {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<std::io::Result<u64>> {
        let pos =
            match pos {
                SeekFrom::Start(n) => n,
                SeekFrom::Current(n) => self.pos.checked_add_signed(n).ok_or_else(|| {
                    std::io::Error::new(ErrorKind::InvalidInput, "seek underflow")
                })?,
                SeekFrom::End(n) => self.blob.len().checked_add_signed(n).ok_or_else(|| {
                    std::io::Error::new(ErrorKind::InvalidInput, "seek underflow")
                })?,
            };

        self.pos = pos;

        let chunk_id = match self.blob.chunk_map.get(pos) {
            Some(ChunkMapEntry::Chunk { chunk_id, .. }) => chunk_id.clone(),
            _ => {
                self.state = State::default();
                return Poll::Ready(Ok(pos));
            }
        };

        loop {
            match self.state.take() {
                State::Ready { chunk: Some(chunk) } => {
                    if chunk.id() != &chunk_id {
                        // this is not the chunk we're looking for
                        continue;
                    }
                    self.state = State::Ready { chunk: Some(chunk) };
                    self.pos = pos;
                    return Poll::Ready(Ok(pos));
                }
                State::Ready { chunk: None } => {
                    // start retrieving chunk
                    self.retrieve_chunk(chunk_id.clone());
                }
                State::Retrieving { fut } => {
                    ready!(self.on_chunk(fut, cx))?;
                }
            }
        }
    }
}
