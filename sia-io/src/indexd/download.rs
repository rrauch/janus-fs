use crate::indexd::client::{Client, ClientError};
use crate::indexd::object::{Object, ObjectId};
use crate::scheduler::resource_manager::Resource;
use async_trait::async_trait;
use futures_util::AsyncRead;
use sia_storage::{DownloadError, DownloadOptions};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead as TokioAsyncRead, DuplexStream};
use tokio::task::JoinHandle;

#[derive(Debug, Clone)]
pub struct DownloadableObject {
    inner: Object,
    client: Client,
}

impl DownloadableObject {
    #[inline]
    pub fn object(&self) -> &Object {
        &self.inner
    }

    pub async fn open(&self, offset: impl Into<Option<u64>>) -> Result<Download, ClientError> {
        let offset = offset.into();
        let mut options = DownloadOptions::default();
        if let Some(offset) = offset {
            options.offset = offset;
        }
        let offset = offset.unwrap_or(0);
        if let Some(max_inflight) = self.client.download_max_inflight() {
            options.max_inflight = max_inflight;
        }

        let (mut writer, reader) = tokio::io::duplex(self.client.download_buffer_size());

        let client = self.client.clone();
        let object = self.inner.as_inner().clone();
        let len = object.size();
        let jh =
            tokio::spawn(async move { client.sdk().download(&mut writer, &object, options).await });

        Ok(Download {
            reader,
            jh: Some(jh),
            task_error: None,
            error_count: 0,
            offset,
            len,
        })
    }
}

#[derive(Debug)]
pub struct Download {
    reader: DuplexStream,
    jh: Option<JoinHandle<Result<(), DownloadError>>>,
    task_error: Option<std::io::Error>,
    error_count: usize,
    offset: u64,
    len: u64,
}

impl Download {
    pub fn len(&self) -> u64 {
        self.len
    }
}

#[async_trait]
impl Resource for Download {
    fn offset(&self) -> u64 {
        self.offset
    }

    fn can_reuse(&self) -> bool {
        self.offset < self.len && self.error_count == 0
    }

    async fn finalize(self) -> anyhow::Result<()> {
        Ok(())
    }
}

impl Drop for Download {
    fn drop(&mut self) {
        if let Some(jh) = self.jh.take() {
            jh.abort();
        }
    }
}

impl Download {
    fn poll_task(&mut self, cx: &mut Context<'_>, eof: bool) -> Poll<std::io::Result<usize>> {
        let Some(jh) = &mut self.jh else {
            return if eof {
                Poll::Ready(Ok(0))
            } else {
                Poll::Pending
            };
        };

        match Pin::new(jh).poll(cx) {
            Poll::Ready(result) => {
                self.jh = None;
                let err = match result {
                    Ok(Ok(())) => {
                        return if eof {
                            Poll::Ready(Ok(0))
                        } else {
                            Poll::Pending
                        };
                    }
                    Ok(Err(e)) => std::io::Error::new(std::io::ErrorKind::Other, e),
                    Err(e) => std::io::Error::new(std::io::ErrorKind::Other, e),
                };
                self.error_count += 1;
                if eof {
                    Poll::Ready(Err(err))
                } else {
                    self.task_error = Some(err);
                    Poll::Pending
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncRead for Download {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();

        if let Some(e) = this.task_error.take() {
            return Poll::Ready(Err(e));
        }

        let mut read_buf = tokio::io::ReadBuf::new(buf);
        match Pin::new(&mut this.reader).poll_read(cx, &mut read_buf) {
            Poll::Ready(Ok(())) if !read_buf.filled().is_empty() => {
                let n = read_buf.filled().len();
                this.offset += n as u64;
                Poll::Ready(Ok(n))
            }
            Poll::Ready(Ok(())) => this.poll_task(cx, true),
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => {
                let _ = this.poll_task(cx, false);
                Poll::Pending
            }
        }
    }
}

impl Client {
    pub async fn download(&self, object_id: &ObjectId) -> Result<DownloadableObject, ClientError> {
        let object = self.sdk().object(object_id.as_ref()).await?;
        Ok(DownloadableObject {
            inner: object.into(),
            client: self.clone(),
        })
    }
}
