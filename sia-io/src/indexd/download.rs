use crate::indexd::client::{Client, ClientError};
use crate::indexd::object::{Object, ObjectId};
use crate::scheduler::resource_manager::Resource;
use async_trait::async_trait;
use futures_util::AsyncRead;
use sia_storage::DownloadOptions;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

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
        if let Some(max_buffered_chunks) = self.client.download_max_buffered_chunks() {
            options.max_buffered_chunks = Some(max_buffered_chunks);
        }

        let client = self.client.clone();
        let object = self.inner.as_inner().clone();
        let len = object.size();

        let download = client
            .sdk()
            .download(&object, options)
            .map_err(sia_storage::Error::Download)?;

        Ok(Download {
            download: download.compat(),
            offset,
            len,
            error_count: 0,
        })
    }
}

pub struct Download {
    download: Compat<sia_storage::Download>,
    offset: u64,
    len: u64,
    error_count: usize,
}

impl std::fmt::Debug for Download {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Download")
            .field("download", &"<Compat<sia_storage::Download>>")
            .field("offset", &self.offset)
            .field("len", &self.len)
            .field("error_count", &self.error_count)
            .finish()
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
    #[inline]
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        match Pin::new(&mut this.download).poll_read(cx, buf) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(n)) => {
                this.offset += n as u64;
                Poll::Ready(Ok(n))
            }
            Poll::Ready(Err(e)) => {
                this.error_count += 1;
                Poll::Ready(Err(e))
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
