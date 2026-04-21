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
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::task::JoinHandle;

pub mod confidential;
#[cfg(feature = "indexd")]
pub mod indexd;
pub mod object;
#[cfg(feature = "renterd")]
pub mod renterd;
pub(crate) mod tagged;

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
}

#[derive(Debug, Error)]
pub enum Error {
    #[cfg(feature = "indexd")]
    #[error(transparent)]
    IndexdError(#[from] indexd::client::ClientError),
    #[cfg(feature = "renterd")]
    #[error(transparent)]
    RenterdError(#[from] renterd::client::ClientError),
    #[error("backend and input type mismatch")]
    BackendMismatch,
}

pub struct Client {
    backend: Backend,
    known_object_ids: Arc<papaya::HashMap<ObjectId, ()>>,
    object_event_loop_handle: Option<JoinHandle<()>>,
}

impl Drop for Client {
    fn drop(&mut self) {
        if let Some(handle) = self.object_event_loop_handle.take() {
            handle.abort();
        }
    }
}

#[bon]
impl Client {
    #[builder]
    pub async fn new(#[builder(into)] backend: Backend) -> Result<Self, Error> {
        let (mut stream, cursor) = backend.list_objects().await?;
        let object_ids = Arc::new(papaya::HashMap::new());
        while let Some(object) = stream.try_next().await? {
            object_ids.pin().insert(object.id().clone(), ());
        }
        drop(stream);

        let object_event_loop_handle = {
            let object_ids = object_ids.clone();
            let backend = backend.clone();
            tokio::spawn(async move {
                object_event_loop(
                    cursor,
                    object_ids,
                    backend,
                    Duration::from_secs(10),
                    Duration::from_secs(60),
                )
                .await
            })
        };

        Ok(Self {
            backend,
            known_object_ids: object_ids,
            object_event_loop_handle: Some(object_event_loop_handle),
        })
    }
}

async fn object_event_loop(
    mut cursor: Option<ObjectsCursor>,
    object_ids: Arc<papaya::HashMap<ObjectId, ()>>,
    backend: Backend,
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
                    match &event {
                        ObjectEvent::New(object, _) | ObjectEvent::Updated(object, _) => {
                            object_ids.pin().insert(object.id().clone(), ());
                        }
                        ObjectEvent::Deleted(id, _) => {
                            object_ids.pin().remove(id);
                        }
                    }
                    cursor = event.cursor();
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
