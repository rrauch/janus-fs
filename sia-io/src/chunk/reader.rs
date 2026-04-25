use crate::cache::Cache;
use crate::chunk::{Chunk, ChunkDownloader, ChunkError, ChunkId};
use crate::object::{Object, ObjectId, Version};
use crate::scheduler::Scheduler;
use bytes::BytesMut;
use futures_io::{AsyncRead, AsyncSeek};
use futures_util::{AsyncReadExt, ready};
use std::io::SeekFrom;
use std::ops::Range;
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

type RetrieveFut = Pin<Box<dyn Future<Output = Result<Chunk, crate::Error>> + Send>>;

pub struct ChunkedReader {
    chunk_size: usize,
    cache: Cache,
    downloader: Arc<Scheduler<ChunkDownloader>>,
    object: Object,
    access_key: (ObjectId, Version),
    pos: u64,
    len: u64,
    state: State,
}

impl ChunkedReader {
    pub(crate) async fn new(
        cache: Cache,
        object: Object,
        chunk_size: usize,
        downloader: Arc<Scheduler<ChunkDownloader>>,
    ) -> Result<Self, crate::Error> {
        assert!(chunk_size > 0);
        let access_key = downloader
            .prepare(object.id())
            .await
            .map_err(|e| crate::Error::CachedError(e.to_string()))?;
        if object.version() != access_key.1 {
            Err(ChunkError::ObjectModified)?
        }

        Ok(Self {
            cache,
            chunk_size,
            downloader,
            access_key,
            pos: 0,
            len: object.size(),
            object,
            state: State::default(),
        })
    }

    fn calc_range(&self, pos: u64) -> Result<(Range<u64>, usize), std::io::Error> {
        let len = self.len;
        if pos >= len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "offset beyond object size",
            ));
        }
        let chunk_size = self.chunk_size as u64;

        let chunk_start = (pos / chunk_size) * chunk_size;
        let chunk_end = std::cmp::min(chunk_start + chunk_size, len);
        let relative_offset = (pos - chunk_start) as usize;

        Ok((chunk_start..chunk_end, relative_offset))
    }

    fn retrieve_chunk(&mut self, pos: u64) -> Result<(), std::io::Error> {
        let (range, _) = self.calc_range(pos)?;

        let chunk_id = ChunkId::from_object(&self.object, range);

        let downloader = self.downloader.clone();
        let id = chunk_id.clone();
        let access_key = self.access_key.clone();
        let source = async move {
            let mut reader = downloader
                .access(&access_key, id.range.start)
                .await
                .map_err(|e| crate::Error::CachedError(e.to_string()))?;
            let len = (id.range().end - id.range().start) as usize;
            let mut buf = BytesMut::zeroed(len);
            reader.as_mut().read_exact(&mut buf).await?;
            let content = buf.freeze();
            Ok(Chunk::new(id, content)?)
        };

        let cache = self.cache.clone();
        let fut = Box::pin(async move { cache.get_chunk(&chunk_id, source).await });

        self.state = State::Retrieving {
            fut: Mutex::new(fut),
        };
        self.pos = pos;
        Ok(())
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

impl AsyncRead for ChunkedReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let pos = self.pos;
        if pos >= self.object.size() {
            // eof
            return Poll::Ready(Ok(0));
        }

        let (range, relative_offset) = self.calc_range(pos)?;

        loop {
            match self.state.take() {
                State::Ready { chunk: Some(chunk) } => {
                    if chunk.id().range() != &range {
                        // this is not the chunk we're looking for
                        continue;
                    }
                    let content = &chunk.content.as_ref()[relative_offset..];
                    if content.is_empty() {
                        // continue to next chunk
                        continue;
                    }
                    let n = std::cmp::min(content.len(), buf.len());
                    buf[..n].copy_from_slice(&content[..n]);
                    self.state = State::Ready { chunk: Some(chunk) };
                    self.pos += n as u64;
                    return Poll::Ready(Ok(n));
                }
                State::Ready { chunk: None } => {
                    self.retrieve_chunk(pos)?;
                }
                State::Retrieving { fut } => {
                    ready!(self.on_chunk(fut, cx))?;
                }
            }
        }
    }
}

impl AsyncSeek for ChunkedReader {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<std::io::Result<u64>> {
        let pos = match pos {
            SeekFrom::Start(n) => n,
            SeekFrom::Current(n) => self.pos.checked_add_signed(n).ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "seek underflow")
            })?,
            SeekFrom::End(n) => self.len.checked_add_signed(n).ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "seek underflow")
            })?,
        };

        // seeking to eof
        if pos == self.len {
            self.state = State::Ready { chunk: None };
            self.pos = pos;
            return Poll::Ready(Ok(pos));
        }

        let (range, _) = self.calc_range(pos)?;

        loop {
            match self.state.take() {
                State::Ready { chunk: Some(chunk) } => {
                    if chunk.id().range() != &range {
                        // this is not the chunk we're looking for
                        continue;
                    }
                    self.state = State::Ready { chunk: Some(chunk) };
                    self.pos = pos;
                    return Poll::Ready(Ok(pos));
                }
                State::Ready { chunk: None } => {
                    // start retrieving chunk
                    self.retrieve_chunk(pos)?;
                }
                State::Retrieving { fut } => {
                    ready!(self.on_chunk(fut, cx))?;
                }
            }
        }
    }
}
