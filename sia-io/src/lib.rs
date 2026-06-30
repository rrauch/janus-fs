use crate::cache::Cache;
use crate::confidential::{Confidential, NewSecretExt};
use crate::object::{ObjectEvent, ObjectId};
use crate::tagged::{TaggedValue, TryFromInner, WithFromStr, WithSerde};
use bon::bon;
use futures_util::TryStreamExt;
use mime::Mime;
use serde::{Deserialize, Deserializer};
use sia_storage::ObjectsCursor;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Debug;
use std::num::{NonZeroU64, NonZeroUsize};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::task::JoinHandle;

use crate::chunk::ChunkDownloader;
use crate::scheduler::Scheduler;
use crate::upload::UploadError;
#[cfg(feature = "indexd")]
pub use sia_storage::SealedObject;

pub mod cache;
pub mod chunk;
pub mod confidential;
#[cfg(feature = "indexd")]
pub mod indexd;
#[cfg(feature = "mock")]
pub mod mock;
pub mod object;
#[cfg(feature = "renterd")]
pub mod renterd;
pub mod scheduler;
pub(crate) mod tagged;
pub mod upload;

pub struct MimeTypeKind;
pub type MimeType = TaggedValue<MimeTypeKind, String>;
impl WithFromStr for MimeType {}
impl WithSerde for MimeType {}
impl TryFromInner<String> for MimeType {
    type Err = MimeTypeError;

    fn try_from_inner(inner: String) -> Result<Self, Self::Err>
    where
        Self: Sized,
    {
        // check if mime type is well-formed
        let _ = Mime::from_str(inner.as_str())?;
        Ok(Self::new_from_inner(inner))
    }
}

#[derive(Error, Debug)]
#[error(transparent)]
#[repr(transparent)]
pub struct MimeTypeError(#[from] mime::FromStrError);

pub struct ETagKind;
pub type ETag = TaggedValue<ETagKind, String>;
impl WithFromStr for ETag {}
impl WithSerde for ETag {}

#[derive(Debug, Error)]
pub enum ETagError {
    #[error("empty ETag value")]
    Empty,
    #[error("missing opening quote")]
    MissingOpeningQuote,
    #[error("missing closing quote")]
    MissingClosingQuote,
    #[error("invalid weak prefix")]
    InvalidWeakPrefix,
    #[error("invalid character at position {0}")]
    InvalidCharacter(usize),
}

fn validate_etag(etag: &str) -> Result<(), ETagError> {
    if etag.is_empty() {
        return Err(ETagError::Empty);
    }

    let rest = if let Some(stripped) = etag.strip_prefix("W/") {
        stripped
    } else if etag.starts_with('W') && !etag.starts_with('"') {
        return Err(ETagError::InvalidWeakPrefix);
    } else {
        etag
    };

    if !rest.starts_with('"') {
        return Err(ETagError::MissingOpeningQuote);
    }

    if rest.len() < 2 || !rest.ends_with('"') {
        return Err(ETagError::MissingClosingQuote);
    }

    let opaque = &rest[1..rest.len() - 1];
    for (i, c) in opaque.chars().enumerate() {
        let b = c as u32;
        if !(b == 0x21 || (0x23..=0x7E).contains(&b) || (0x80..=0xFF).contains(&b)) {
            return Err(ETagError::InvalidCharacter(i));
        }
    }

    Ok(())
}

impl TryFromInner<String> for ETag {
    type Err = ETagError;

    fn try_from_inner(inner: String) -> Result<Self, Self::Err>
    where
        Self: Sized,
    {
        validate_etag(inner.as_str())?;
        Ok(Self::new_from_inner(inner))
    }
}

pub type Password<Tag> = Confidential<TaggedValue<Tag, String>>;

impl<Tag> From<String> for Password<Tag> {
    fn from(value: String) -> Self {
        TaggedValue::<Tag, String>::new_from_inner(value).confidential()
    }
}

fn deserialize_null_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    T: Default + Deserialize<'de>,
    D: Deserializer<'de>,
{
    let opt = Option::deserialize(deserializer)?;
    Ok(opt.unwrap_or_default())
}

#[derive(Debug, Clone)]
pub(crate) enum Backend {
    #[cfg(feature = "indexd")]
    Indexd(indexd::client::Client),
    #[cfg(feature = "renterd")]
    Renterd(renterd::client::Client),
    #[cfg(feature = "mock")]
    Mock(mock::MockClient),
}

#[cfg(feature = "indexd")]
impl From<indexd::client::Client> for Backend {
    fn from(value: indexd::client::Client) -> Self {
        Self::Indexd(value)
    }
}

#[cfg(feature = "renterd")]
impl From<renterd::client::Client> for Backend {
    fn from(value: renterd::client::Client) -> Self {
        Self::Renterd(value)
    }
}

pub enum Metadata<'a> {
    #[cfg(feature = "indexd")]
    Indexd(Cow<'a, [u8]>),
    #[cfg(feature = "renterd")]
    Renterd(Cow<'a, HashMap<String, String>>),
    #[cfg(feature = "mock")]
    Mock(Cow<'a, HashMap<String, String>>),
}

pub trait MetadataSource: Send {
    fn to_bytes(&self) -> Cow<'_, [u8]>;
    fn to_map(&self) -> Cow<'_, HashMap<String, String>>;
}

impl<T: MetadataSource> MetadataSource for Box<T> {
    fn to_bytes(&self) -> Cow<'_, [u8]> {
        self.as_ref().to_bytes()
    }

    fn to_map(&self) -> Cow<'_, HashMap<String, String>> {
        self.as_ref().to_map()
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[cfg(feature = "indexd")]
    #[error(transparent)]
    IndexdError(#[from] indexd::client::ClientError),
    #[cfg(feature = "renterd")]
    #[error(transparent)]
    RenterdError(#[from] renterd::client::ClientError),
    #[cfg(feature = "mock")]
    #[error(transparent)]
    MockError(#[from] mock::MockError),
    #[error("backend and input type mismatch")]
    BackendMismatch,
    #[error("cached error: {0}")]
    CachedError(String),
    #[error(transparent)]
    ChunkError(#[from] chunk::ChunkError),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    ConfigError(#[from] ConfigError),
    #[error(transparent)]
    UploadError(#[from] UploadError),
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("invalid chunk size")]
    InvalidChunkSize,
    #[error("invalid download max skip ahead")]
    InvalidDownloadMaxSkipAhead,
    #[error("invalid max concurrent downloads")]
    InvalidMaxConcurrentDownloads,
}

#[derive(Debug, Clone)]
pub struct Client {
    backend: Backend,
    cache: Cache,
    known_object_ids: Arc<papaya::HashMap<ObjectId, ()>>,
    object_event_loop_handle: Option<Arc<JoinHandle<()>>>,
    chunk_size: usize,
    chunk_downloader: Arc<Scheduler<ChunkDownloader>>,
}

impl Drop for Client {
    fn drop(&mut self) {
        if let Some(handle) = self.object_event_loop_handle.take() {
            if let Ok(handle) = Arc::try_unwrap(handle) {
                handle.abort();
            }
        }
    }
}

#[cfg(feature = "mock")]
impl Client {
    pub async fn mock() -> Self {
        Self::builder()
            .backend(Backend::Mock(mock::MockClient::default()))
            .cache(Cache::default())
            .build()
            .await
            .unwrap()
    }
}

#[bon]
impl Client {
    #[builder]
    pub async fn new(
        #[builder(into)] backend: Backend,
        #[builder(default)] cache: Cache,
        #[builder(default = 1024 * 256)] chunk_size: usize,
        #[builder(default = 1024 * 1024)] download_max_skip_ahead: usize,
        #[builder(default = 64)] max_concurrent_downloads: usize,
    ) -> Result<Self, Error> {
        if chunk_size == 0 {
            Err(ConfigError::InvalidChunkSize)?;
        }
        let download_max_skip_ahead = NonZeroU64::new(download_max_skip_ahead as u64)
            .ok_or_else(|| ConfigError::InvalidDownloadMaxSkipAhead)?;
        let max_concurrent_downloads = NonZeroUsize::new(max_concurrent_downloads)
            .ok_or_else(|| ConfigError::InvalidMaxConcurrentDownloads)?;

        let (mut stream, cursor) = backend.list_objects().await?;
        let object_ids = Arc::new(papaya::HashMap::new());
        while let Some(object) = stream.try_next().await? {
            let id = object.id().clone();
            cache.insert_object(object, &backend).await?;
            object_ids.pin().insert(id, ());
        }
        drop(stream);

        let object_event_loop_handle = {
            let object_ids = object_ids.clone();
            let backend = backend.clone();
            let cache = cache.clone();
            tokio::spawn(async move {
                object_event_loop(
                    cursor,
                    object_ids,
                    backend,
                    cache,
                    Duration::from_secs(10),
                    Duration::from_secs(60),
                )
                .await
            })
        };

        let chunk_downloader = Arc::new(
            ChunkDownloader::builder()
                .backend(backend.clone())
                .cache(cache.clone())
                .chunk_size(chunk_size)
                .max_skip_ahead(download_max_skip_ahead)
                .max_concurrent_downloads(max_concurrent_downloads)
                .build(),
        );

        Ok(Self {
            chunk_size,
            backend,
            cache,
            known_object_ids: object_ids,
            object_event_loop_handle: Some(Arc::new(object_event_loop_handle)),
            chunk_downloader,
        })
    }
}

async fn object_event_loop(
    mut cursor: Option<ObjectsCursor>,
    object_ids: Arc<papaya::HashMap<ObjectId, ()>>,
    backend: Backend,
    cache: Cache,
    eof_retry_duration: Duration,
    error_retry_duration: Duration,
) {
    'main: loop {
        let mut event_stream = match backend.object_events(clone_cursor(cursor.as_ref())).await {
            Ok(Some(event_stream)) => event_stream,
            Err(_err) => {
                // error getting events, retry later
                tokio::time::sleep(error_retry_duration).await;
                continue;
            }
            Ok(None) => {
                // backend does NOT support events
                return;
            }
        };

        loop {
            match event_stream.try_next().await {
                Ok(Some(event)) => {
                    let latest_cursor = event.cursor();
                    match event {
                        ObjectEvent::New(object, _) | ObjectEvent::Updated(object, _) => {
                            let id = object.id().clone();
                            let _ = cache.insert_object(object, &backend).await;
                            object_ids.pin().insert(id, ());
                        }
                        ObjectEvent::Deleted(id, _) => {
                            object_ids.pin().remove(&id);
                            let _ = cache.invalidate_object(&id).await;
                        }
                    }
                    cursor = latest_cursor;
                }
                Ok(None) => {
                    // no more events
                    tokio::time::sleep(eof_retry_duration).await;
                    continue 'main;
                }
                Err(_err) => {
                    // error retrieving event, retry later
                    tokio::time::sleep(error_retry_duration).await;
                    continue 'main;
                }
            }
        }
    }
}

fn clone_cursor(cursor: Option<&ObjectsCursor>) -> Option<ObjectsCursor> {
    cursor.map(|cursor| ObjectsCursor {
        after: cursor.after,
        id: cursor.id,
    })
}

#[cfg(test)]
mod tests {
    use crate::upload::UploadableObject;
    use crate::{Client, MetadataSource, indexd, renterd};
    use futures_util::io::Cursor;
    use futures_util::{AsyncReadExt, AsyncSeekExt, TryStreamExt};
    use std::borrow::Cow;
    use std::collections::HashMap;
    use std::io::SeekFrom;

    static ONE_MB: &[u8] = include_bytes!("../testdata/1mb.bin");

    struct MockMetadata;

    impl MetadataSource for MockMetadata {
        fn to_bytes(&self) -> Cow<'_, [u8]> {
            unimplemented!()
        }

        fn to_map(&self) -> Cow<'_, HashMap<String, String>> {
            unimplemented!()
        }
    }

    #[ignore]
    #[tokio::test]
    async fn indexd_test1() -> Result<(), anyhow::Error> {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_test_writer()
            .try_init();
        dotenv::dotenv().ok();
        let indexd = indexd::tests::connect().await?;
        integration_test1(Client::builder().backend(indexd).build().await?).await?;
        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn renterd_test1() -> Result<(), anyhow::Error> {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_test_writer()
            .try_init();
        dotenv::dotenv().ok();
        let renterd = renterd::tests::new_client().await?;
        integration_test1(Client::builder().backend(renterd).build().await?).await?;
        Ok(())
    }

    #[tokio::test]
    async fn mock_test1() -> Result<(), anyhow::Error> {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_test_writer()
            .try_init();
        dotenv::dotenv().ok();
        integration_test1(Client::mock().await).await?;
        Ok(())
    }

    async fn integration_test1(client: Client) -> Result<(), anyhow::Error> {
        assert!(client.num_objects() < 10);

        while let Some(object) = client.list_objects().try_next().await? {
            client.delete_object(object.id()).await?;
        }

        assert_eq!(client.num_objects(), 0);

        let file1 = client
            .upload(UploadableObject::new(
                "/dir1/subdir1/file1",
                Cursor::new(ONE_MB),
                None::<MockMetadata>,
            ))
            .await?;

        assert_eq!(client.num_objects(), 1);

        let objects = client
            .list_objects()
            .map_err(anyhow::Error::from)
            .try_collect::<Vec<_>>()
            .await?;

        assert_eq!(objects.len(), 1);
        assert_eq!(objects.first().unwrap().id(), file1.id());
        assert_eq!(objects.first().unwrap().size(), file1.size());

        let dl1 = client.download(file1.id()).await?.unwrap();
        assert_eq!(dl1.object().size(), ONE_MB.len() as u64);
        let mut buf = Vec::with_capacity(ONE_MB.len());
        let mut reader = dl1.open().await?;
        let read = reader.read_to_end(&mut buf).await?;
        assert_eq!(read, ONE_MB.len());
        assert_eq!(&buf, ONE_MB);

        buf.clear();
        reader.seek(SeekFrom::End(-1024)).await?;
        let read = reader.read_to_end(&mut buf).await?;
        assert_eq!(read, 1024);
        assert_eq!(&buf[..1024], &ONE_MB[ONE_MB.len() - 1024..]);

        client.delete_object(file1.id()).await?;
        assert_eq!(client.num_objects(), 0);
        Ok(())
    }
}
