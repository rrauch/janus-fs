use crate::renterd::client::{ApiRequest, ApiRequestBuilder, Client, ClientError};
use crate::renterd::encode_object_path;
use crate::renterd::object::{File, FileId};
use crate::scheduler::resource_manager::Resource;
use async_trait::async_trait;
use futures_io::AsyncRead;
use futures_util::{TryStreamExt, ready};
use std::fmt::{Debug, Formatter};
use std::pin::Pin;
use std::task::{Context, Poll};

#[derive(Debug, Clone)]
pub struct DownloadableFile {
    inner: File,
    client: Client,
}

impl DownloadableFile {
    #[inline]
    pub fn file(&self) -> &File {
        &self.inner
    }

    pub async fn open(&self, offset: impl Into<Option<u64>>) -> Result<Download, ClientError> {
        let offset = offset.into();
        let inner = Box::pin(
            self.client
                .send_api_request(download_req(
                    self.inner.id().key().as_relative_path(),
                    self.inner.id().bucket(),
                    offset.map(|o| (o, Some(self.inner.size()))),
                ))
                .await?
                .bytes_stream()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
                .into_async_read(),
        );
        let offset = offset.unwrap_or(0);
        let len = self.inner.size();
        Ok(Download {
            inner,
            offset,
            len,
            error_count: 0,
        })
    }
}

pub struct Download {
    inner: Pin<Box<dyn AsyncRead + Send + Unpin + 'static>>,
    offset: u64,
    len: u64,
    error_count: usize,
}

impl Debug for Download {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}@{}", self.offset, self.len)
    }
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

impl AsyncRead for Download {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        Poll::Ready(match ready!(self.inner.as_mut().poll_read(cx, buf)) {
            Ok(n) => {
                self.offset += n as u64;
                Ok(n)
            }
            Err(err) => Err(err),
        })
    }
}

fn download_req<'a>(
    key: &str,
    bucket: &'a str,
    offset_length: Option<(u64, Option<u64>)>,
) -> ApiRequest<'a> {
    let path = encode_object_path(key, "./worker/object");
    let params = vec![("bucket", bucket)];

    let mut builder = ApiRequestBuilder::get(path).params(Some(params));

    if let Some((offset, length)) = offset_length {
        if offset > 0 {
            let value = match length {
                Some(length) if length > 0 => format!("bytes={}-{}", offset, length - 1),
                _ => format!("bytes={}-", offset),
            };
            builder = builder.headers(Some(vec![("range", value)]));
        }
    }

    builder.build()
}

impl Client {
    pub async fn download(&self, file_id: &FileId) -> Result<DownloadableFile, ClientError> {
        self.check_object_id(file_id)?;
        let file = self.object(file_id).await?;
        Ok(DownloadableFile {
            inner: file,
            client: self.clone(),
        })
    }
}
