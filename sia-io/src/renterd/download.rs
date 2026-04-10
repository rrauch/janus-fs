use crate::renterd::client::{ApiRequest, ApiRequestBuilder, Client, ClientError};
use crate::renterd::encode_object_path;
use crate::renterd::object::{Object, ObjectId};
use futures_io::AsyncRead;
use futures_util::TryStreamExt;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DownloadError {
    #[error(transparent)]
    ClientError(#[from] ClientError),
    #[error("object {0} not a file")]
    NotAFile(ObjectId),
}

#[derive(Debug, Clone)]
pub struct DownloadableObject {
    inner: Object,
    client: Client,
}

impl DownloadableObject {
    pub fn object(&self) -> &Object {
        &self.inner
    }

    pub async fn open(
        &self,
        offset: impl Into<Option<u64>>,
    ) -> Result<impl AsyncRead + Send + Unpin, DownloadError> {
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
    pub async fn download(
        &self,
        object_id: &ObjectId,
    ) -> Result<DownloadableObject, DownloadError> {
        self.check_object_id(object_id)?;
        let object = self.object(object_id).await?;
        if object.is_folder() {
            Err(DownloadError::NotAFile(object_id.clone()))?
        }
        Ok(DownloadableObject {
            inner: object,
            client: self.clone(),
        })
    }
}
