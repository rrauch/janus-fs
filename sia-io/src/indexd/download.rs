use crate::indexd::client::{Client, ClientError};
use crate::indexd::object::{Object, ObjectId};
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

    pub async fn open(
        &self,
        offset: impl Into<Option<u64>>,
    ) -> Result<impl AsyncRead + Send + Unpin, ClientError> {
        let offset = offset.into();
        let mut options = DownloadOptions::default();
        if let Some(offset) = offset {
            options.offset = offset;
        }
        if let Some(max_inflight) = self.client.download_max_inflight() {
            options.max_inflight = max_inflight;
        }

        let (mut writer, reader) = tokio::io::duplex(self.client.download_buffer_size());

        let client = self.client.clone();
        let object = self.inner.as_inner().clone();
        let jh =
            tokio::spawn(async move { client.sdk().download(&mut writer, &object, options).await });

        Ok(Downloader {
            reader,
            jh: Some(jh),
            task_error: None,
        })
    }
}

struct Downloader {
    reader: DuplexStream,
    jh: Option<JoinHandle<Result<(), DownloadError>>>,
    task_error: Option<std::io::Error>,
}

impl Drop for Downloader {
    fn drop(&mut self) {
        if let Some(jh) = self.jh.take() {
            jh.abort();
        }
    }
}

impl Downloader {
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

impl AsyncRead for Downloader {
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
                Poll::Ready(Ok(read_buf.filled().len()))
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
