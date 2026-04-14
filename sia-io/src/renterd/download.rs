use crate::renterd::client::{ApiRequest, ApiRequestBuilder, Client, ClientError};
use crate::renterd::encode_object_path;
use crate::renterd::object::{FileId, File};
use futures_io::AsyncRead;
use futures_util::TryStreamExt;

#[derive(Debug, Clone)]
pub struct DownloadableFile {
    inner: File,
    client: Client,
}

impl DownloadableFile {
    pub fn file(&self) -> &File {
        &self.inner
    }

    pub async fn open(
        &self,
        offset: impl Into<Option<u64>>,
    ) -> Result<impl AsyncRead + Send + Unpin, ClientError> {
        let offset = offset.into();
        Ok(self
            .client
            .send_api_request(download_req(
                self.inner.id().key().as_relative_path(),
                self.inner.id().bucket(),
                offset.map(|o| (o, Some(self.inner.size()))),
            ))
            .await?
            .bytes_stream()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
            .into_async_read())
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
