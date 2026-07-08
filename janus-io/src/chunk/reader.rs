use crate::cache::Cache;
use crate::chunk::{Chunk, ChunkId};
use crate::object::{BackendDO, Download};
use bytes::BytesMut;
use futures_io::{AsyncRead, AsyncSeek};
use futures_util::{AsyncReadExt, ready};
use std::io::SeekFrom;
use std::ops::Range;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

enum State {
    Idle,
    Ready {
        chunk: Chunk,
    },
    Retrieving {
        fut: Mutex<RetrieveFut>,
        leftover: Arc<Mutex<Option<Download>>>,
    },
}

impl State {
    fn take(&mut self) -> Self {
        std::mem::take(self)
    }
}

impl Default for State {
    fn default() -> Self {
        Self::Idle
    }
}

type RetrieveFut = Pin<Box<dyn Future<Output = Result<Chunk, crate::Error>> + Send>>;

pub struct ChunkedReader {
    chunk_size: usize,
    cache: Cache,
    object: BackendDO,
    pos: u64,
    len: u64,
    state: State,
    download: Option<Download>,
}

impl ChunkedReader {
    pub(crate) async fn new(
        cache: Cache,
        object: BackendDO,
        chunk_size: usize,
    ) -> Result<Self, crate::Error> {
        assert!(chunk_size > 0);

        Ok(Self {
            cache,
            chunk_size,
            pos: 0,
            len: object.object().size(),
            object,
            state: State::default(),
            download: None,
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

        let chunk_id = ChunkId::from_object(self.object.object(), range);

        let download = match self.download.take() {
            Some(download) if download.offset() == pos && download.can_reuse() => Some(download),
            _ => None,
        };

        let id = chunk_id.clone();
        let object = self.object.clone();

        // shared slot for the leftover download, written only on cache miss
        let leftover: Arc<Mutex<Option<Download>>> = Arc::new(Mutex::new(None));
        let leftover_src = leftover.clone();

        let source = async move {
            let mut download = if let Some(download) = download {
                download
            } else {
                object.open(pos).await?
            };

            let len = (id.range().end - id.range().start) as usize;
            let mut buf = BytesMut::zeroed(len);

            download.read_exact(&mut buf).await?;
            let content = buf.freeze();
            let chunk = Chunk::new(id, content)?;

            // stash the download for sequential reuse
            *leftover_src.lock().unwrap() = Some(download);

            Ok(chunk)
        };

        let cache = self.cache.clone();
        let fut = Box::pin(async move { cache.get_chunk(&chunk_id, source).await });

        self.state = State::Retrieving {
            fut: Mutex::new(fut),
            leftover,
        };
        self.pos = pos;
        Ok(())
    }

    fn on_chunk(
        &mut self,
        mutex: Mutex<RetrieveFut>,
        leftover: Arc<Mutex<Option<Download>>>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        let mut fut = mutex.lock().unwrap();

        match fut.as_mut().poll(cx) {
            Poll::Pending => {
                drop(fut);
                self.state = State::Retrieving {
                    fut: mutex,
                    leftover,
                };
                Poll::Pending
            }
            Poll::Ready(Err(err)) => Poll::Ready(Err(std::io::Error::other(err))),
            Poll::Ready(Ok(chunk)) => {
                drop(fut);
                // on a cache miss, source stashed the download here; on a
                // hit it's empty and there's nothing to reuse
                self.download = leftover.lock().unwrap().take();
                self.state = State::Ready { chunk };
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
        if pos >= self.object.object().size() {
            // eof
            return Poll::Ready(Ok(0));
        }

        let (range, relative_offset) = self.calc_range(pos)?;

        loop {
            match self.state.take() {
                State::Ready { chunk } => {
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
                    self.state = State::Ready { chunk };
                    self.pos += n as u64;
                    return Poll::Ready(Ok(n));
                }
                State::Idle => {
                    self.retrieve_chunk(pos)?;
                }
                State::Retrieving { fut, leftover } => {
                    ready!(self.on_chunk(fut, leftover, cx))?;
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
            self.state = State::Idle;
            self.pos = pos;
            return Poll::Ready(Ok(pos));
        }

        let (range, _) = self.calc_range(pos)?;

        loop {
            match self.state.take() {
                State::Ready { chunk } => {
                    if chunk.id().range() != &range {
                        // this is not the chunk we're looking for
                        continue;
                    }
                    self.state = State::Ready { chunk };
                    self.pos = pos;
                    return Poll::Ready(Ok(pos));
                }
                State::Idle => {
                    // start retrieving chunk
                    self.retrieve_chunk(pos)?;
                }
                State::Retrieving { fut, leftover } => {
                    ready!(self.on_chunk(fut, leftover, cx))?;
                }
            }
        }
    }
}
