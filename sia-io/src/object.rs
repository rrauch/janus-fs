use crate::renterd::object::AnyObject;
use crate::{Backend, Client, ETag, Metadata, MimeType, indexd, renterd};
use chrono::{DateTime, Utc};
use futures_io::AsyncRead;
use futures_util::{StreamExt, TryStream, TryStreamExt, stream};
use ouroboros::self_referencing;
use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use sia_storage::ObjectsCursor;
use std::borrow::Cow;
use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;
use std::{fmt, iter};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ObjectId {
    #[cfg(feature = "indexd")]
    Indexd(indexd::object::ObjectId),
    #[cfg(feature = "renterd")]
    Renterd(renterd::object::FileId),
}

impl Serialize for ObjectId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let id = match self {
            Self::Indexd(id) => id.to_string(),
            Self::Renterd(id) => id.to_string(),
        };

        serializer.serialize_str(id.as_str())
    }
}

impl<'de> Deserialize<'de> for ObjectId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ObjectIdVisitor;

        impl<'de> Visitor<'de> for ObjectIdVisitor {
            type Value = ObjectId;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string")
            }

            fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(ObjectId::from_str(s).map_err(E::custom)?)
            }
        }

        deserializer.deserialize_str(ObjectIdVisitor)
    }
}

#[derive(Debug, Error)]
pub enum ObjectIdError {
    #[error("object id cannot be an empty string")]
    IsEmpty,
    #[cfg(feature = "indexd")]
    #[error(transparent)]
    IndexdError(#[from] <indexd::object::ObjectId as FromStr>::Err),
    #[cfg(feature = "renterd")]
    #[error(transparent)]
    RenterdError(#[from] <renterd::object::FileId as FromStr>::Err),
    #[error("'{0}' is not a supported object id")]
    UnsupportedId(String),
}

impl FromStr for ObjectId {
    type Err = ObjectIdError;

    #[allow(unreachable_code)]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            Err(ObjectIdError::IsEmpty)?
        }

        #[cfg(feature = "indexd")]
        {
            if s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit()) {
                // most likely a indexd id
                return Ok(indexd::object::ObjectId::from_str(s)?.into());
            }
        }

        #[cfg(feature = "renterd")]
        {
            return Ok(renterd::object::FileId::from_str(s)?.into());
        }

        Err(ObjectIdError::UnsupportedId(s.to_string()))
    }
}

impl Display for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Indexd(id) => Display::fmt(id, f),
            Self::Renterd(id) => Display::fmt(id, f),
        }
    }
}

#[cfg(feature = "indexd")]
impl From<indexd::object::ObjectId> for ObjectId {
    fn from(value: indexd::object::ObjectId) -> Self {
        Self::Indexd(value)
    }
}

#[cfg(feature = "renterd")]
impl From<renterd::object::FileId> for ObjectId {
    fn from(value: renterd::object::FileId) -> Self {
        Self::Renterd(value)
    }
}

#[derive(Debug, Clone)]
pub enum Object {
    #[cfg(feature = "indexd")]
    Indexd {
        id: ObjectId,
        inner: Arc<indexd::object::Object>,
    },
    #[cfg(feature = "renterd")]
    Renterd {
        id: ObjectId,
        inner: Arc<renterd::object::File>,
    },
}

impl Object {
    #[inline]
    pub fn id(&self) -> &ObjectId {
        match &self {
            #[cfg(feature = "indexd")]
            Self::Indexd { id, .. } => id,
            #[cfg(feature = "renterd")]
            Self::Renterd { id, .. } => id,
        }
    }

    #[inline]
    pub fn created(&self) -> Option<&DateTime<Utc>> {
        match &self {
            #[cfg(feature = "indexd")]
            Self::Indexd { inner, .. } => Some(inner.created_at()),
            #[cfg(feature = "renterd")]
            Self::Renterd { .. } => None,
        }
    }

    #[inline]
    pub fn updated(&self) -> &DateTime<Utc> {
        match &self {
            #[cfg(feature = "indexd")]
            Self::Indexd { inner, .. } => inner.updated_at(),
            #[cfg(feature = "renterd")]
            Self::Renterd { inner, .. } => inner.mod_time(),
        }
    }

    #[inline]
    pub fn size(&self) -> u64 {
        match &self {
            #[cfg(feature = "indexd")]
            Self::Indexd { inner, .. } => inner.size(),
            #[cfg(feature = "renterd")]
            Self::Renterd { inner, .. } => inner.size(),
        }
    }

    #[inline]
    pub fn mime_type(&self) -> Option<&MimeType> {
        match &self {
            #[cfg(feature = "indexd")]
            Self::Indexd { .. } => None,
            #[cfg(feature = "renterd")]
            Self::Renterd { inner, .. } => Some(inner.mime_type()),
        }
    }

    #[inline]
    pub fn etag(&self) -> Option<&ETag> {
        match &self {
            #[cfg(feature = "indexd")]
            Self::Indexd { .. } => None,
            #[cfg(feature = "renterd")]
            Self::Renterd { inner, .. } => inner.etag(),
        }
    }

    #[inline]
    pub fn metadata(&self) -> Metadata<'_> {
        match &self {
            #[cfg(feature = "indexd")]
            Self::Indexd { inner, .. } => Metadata::Indexd(inner.metadata().into()),
            #[cfg(feature = "renterd")]
            Self::Renterd { inner, .. } => Metadata::Renterd(Cow::Borrowed(inner.metadata())),
        }
    }
}

#[cfg(feature = "indexd")]
impl From<indexd::object::Object> for Object {
    fn from(value: indexd::object::Object) -> Self {
        let id = value.id().clone().into();
        Self::Indexd {
            id,
            inner: Arc::new(value),
        }
    }
}

#[cfg(feature = "renterd")]
impl From<renterd::object::File> for Object {
    fn from(value: renterd::object::File) -> Self {
        let id = value.id().clone().into();
        Self::Renterd {
            id,
            inner: Arc::new(value),
        }
    }
}

impl Backend {
    #[inline]
    async fn object(&self, id: &ObjectId) -> Result<Object, crate::Error> {
        match (&self, id) {
            #[cfg(feature = "indexd")]
            (Self::Indexd(indexd), ObjectId::Indexd(id)) => {
                Ok(indexd.object(id).await.map(Object::from)?)
            }
            #[cfg(feature = "renterd")]
            (Self::Renterd(renterd), ObjectId::Renterd(id)) => {
                Ok(renterd.object(id).await.map(Object::from)?)
            }
            _ => Err(crate::Error::BackendMismatch),
        }
    }

    #[inline]
    async fn delete_object(&self, id: &ObjectId) -> Result<(), crate::Error> {
        match (&self, id) {
            #[cfg(feature = "indexd")]
            (Self::Indexd(indexd), ObjectId::Indexd(id)) => {
                Ok(indexd.delete_objects(iter::once(id)).await?)
            }
            #[cfg(feature = "renterd")]
            (Self::Renterd(renterd), ObjectId::Renterd(id)) => {
                Ok(renterd.delete_object(id).await?)
            }
            _ => Err(crate::Error::BackendMismatch),
        }
    }

    #[inline]
    async fn download(&self, id: &ObjectId) -> Result<DownloadableObject, crate::Error> {
        match (&self, id) {
            #[cfg(feature = "indexd")]
            (Self::Indexd(indexd), ObjectId::Indexd(id)) => Ok(indexd.download(id).await?.into()),
            #[cfg(feature = "renterd")]
            (Self::Renterd(renterd), ObjectId::Renterd(id)) => {
                Ok(renterd.download(id).await?.into())
            }
            _ => Err(crate::Error::BackendMismatch),
        }
    }

    async fn upload(
        &self,
        name_hint: impl AsRef<str>,
        content: impl AsyncRead + Send + Unpin + 'static,
        metadata: Option<Metadata<'static>>,
    ) -> Result<Object, crate::Error> {
        match (&self, metadata) {
            #[cfg(feature = "indexd")]
            (Self::Indexd(indexd), Some(Metadata::Indexd(metadata))) => Ok(indexd
                .upload(content, Some(metadata.into_owned()))
                .await?
                .into()),
            (Self::Indexd(indexd), None) => Ok(indexd.upload(content, None).await?.into()),
            #[cfg(feature = "renterd")]
            (Self::Renterd(renterd), metadata) => {
                let metadata = match metadata {
                    Some(Metadata::Renterd(m)) => Some(m.into_owned()),
                    None => None,
                    _ => return Err(crate::Error::BackendMismatch),
                };

                let id = renterd
                    .object_id(None, name_hint)
                    .map_err(renterd::client::ClientError::ObjectKeyError)?;

                renterd
                    .upload(
                        &id,
                        None,
                        metadata
                            .as_ref()
                            .map(|m| m.iter().map(|(k, v)| (k.as_str(), v.as_str()))),
                        content,
                    )
                    .await?;

                let object = renterd.object(&id).await?;
                Ok(object.into())
            }
            _ => Err(crate::Error::BackendMismatch),
        }
    }

    pub(super) async fn list_objects(
        &self,
    ) -> Result<
        (
            impl TryStream<Ok = Object, Error = crate::Error> + Send + Unpin,
            Option<ObjectsCursor>,
        ),
        crate::Error,
    > {
        let (stream, cursor): (
            Box<
                dyn TryStream<
                        Ok = Object,
                        Error = crate::Error,
                        Item = Result<Object, crate::Error>,
                    > + Send
                    + Unpin,
            >,
            _,
        ) = match &self {
            Self::Indexd(indexd) => {
                let (objects, cursor) = indexd.list_objects().await?;
                (
                    Box::new(stream::iter(objects.into_iter().map(|o| Ok(o.into())))),
                    cursor,
                )
            }
            Self::Renterd(renterd) => (
                Box::new(
                    renterd
                        .list_objects("")?
                        .map_err(crate::Error::from)
                        .try_filter_map(|any| async move {
                            Ok(match any {
                                AnyObject::File(file) => Some(file.into()),
                                AnyObject::Folder(_) => None,
                            })
                        })
                        .boxed(),
                ),
                None,
            ),
        };
        Ok((stream, cursor))
    }

    pub(super) async fn object_events(
        &self,
        cursor: Option<ObjectsCursor>,
    ) -> Result<
        Option<impl TryStream<Ok = ObjectEvent, Error = crate::Error> + Send + Unpin>,
        crate::Error,
    > {
        let stream: Option<
            Box<
                dyn TryStream<
                        Ok = ObjectEvent,
                        Error = crate::Error,
                        Item = Result<ObjectEvent, crate::Error>,
                    > + Send
                    + Unpin,
            >,
        > = match &self {
            Self::Indexd(indexd) => Some(Box::new(
                indexd
                    .object_events(cursor)
                    .map_err(crate::Error::from)
                    .try_filter_map(|e| async move { Ok(Some(e.into())) })
                    .boxed(),
            )),
            Self::Renterd(_) => None,
        };
        Ok(stream)
    }
}

#[derive(Debug)]
pub enum ObjectEvent {
    New(Object, DateTime<Utc>),
    Updated(Object, DateTime<Utc>),
    Deleted(ObjectId, DateTime<Utc>),
}

impl From<indexd::object::ObjectEvent> for ObjectEvent {
    fn from(value: indexd::object::ObjectEvent) -> Self {
        match value {
            indexd::object::ObjectEvent::New(object, ts) => Self::New(object.into(), ts),
            indexd::object::ObjectEvent::Updated(object, ts) => Self::Updated(object.into(), ts),
            indexd::object::ObjectEvent::Deleted(id, ts) => Self::Deleted(id.into(), ts),
        }
    }
}

impl ObjectEvent {
    #[inline]
    pub fn object_id(&self) -> &ObjectId {
        match self {
            Self::New(o, _) => o.id(),
            Self::Updated(o, _) => o.id(),
            Self::Deleted(id, _) => id,
        }
    }

    #[inline]
    pub fn timestamp(&self) -> &DateTime<Utc> {
        match self {
            Self::New(_, ts) => ts,
            Self::Updated(_, ts) => ts,
            Self::Deleted(_, ts) => ts,
        }
    }

    pub(crate) fn cursor(&self) -> Option<ObjectsCursor> {
        let id = self.object_id();
        match id {
            ObjectId::Indexd(indexd_id) => Some(ObjectsCursor {
                id: indexd_id.clone().into_inner(),
                after: self.timestamp().clone(),
            }),
            ObjectId::Renterd(_) => None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum DownloadableObject {
    #[cfg(feature = "indexd")]
    Indexd {
        object: Object,
        inner: indexd::download::DownloadableObject,
    },
    #[cfg(feature = "renterd")]
    Renterd {
        object: Object,
        inner: renterd::download::DownloadableFile,
    },
}

impl DownloadableObject {
    #[inline]
    pub fn object(&self) -> &Object {
        match &self {
            #[cfg(feature = "indexd")]
            Self::Indexd { object, .. } => object,
            #[cfg(feature = "renterd")]
            Self::Renterd { object, .. } => object,
        }
    }

    #[inline]
    pub async fn open(
        &self,
        offset: impl Into<Option<u64>>,
    ) -> Result<impl AsyncRead + Send + Unpin, crate::Error> {
        let offset = offset.into();
        let reader: Box<dyn AsyncRead + Send + Unpin> = match &self {
            #[cfg(feature = "indexd")]
            Self::Indexd { inner, .. } => Box::new(inner.open(offset).await?),
            #[cfg(feature = "renterd")]
            Self::Renterd { inner, .. } => Box::new(inner.open(offset).await?),
        };
        Ok(reader)
    }
}

#[cfg(feature = "indexd")]
impl From<indexd::download::DownloadableObject> for DownloadableObject {
    fn from(value: indexd::download::DownloadableObject) -> Self {
        let object = Object::from(value.object().clone());
        Self::Indexd {
            object,
            inner: value,
        }
    }
}

#[cfg(feature = "renterd")]
impl From<renterd::download::DownloadableFile> for DownloadableObject {
    fn from(value: renterd::download::DownloadableFile) -> Self {
        let object = Object::from(value.file().clone());
        Self::Renterd {
            object,
            inner: value,
        }
    }
}

#[self_referencing]
struct IterHolder<K: 'static> {
    set: Arc<papaya::HashMap<K, ()>>,
    #[borrows(set)]
    #[covariant]
    guard: papaya::OwnedGuard<'this>,
    #[borrows(set, guard)]
    #[covariant]
    iter: papaya::Iter<'this, K, (), papaya::OwnedGuard<'this>>,
}

impl Client {
    pub fn list_objects(&self) -> impl TryStream<Ok = Object, Error = crate::Error> + Send + Unpin {
        let set = self.known_object_ids.clone();

        let holder = IterHolderBuilder {
            set,
            guard_builder: |set| set.owned_guard(),
            iter_builder: |set, guard| set.iter(guard),
        }
        .build();

        stream::try_unfold(holder, move |mut holder| async move {
            if let Some(id) = holder.with_iter_mut(|iter| iter.next().map(|(id, _)| id)) {
                let object = self.backend.object(id).await?;
                Ok(Some((object, holder)))
            } else {
                Ok(None)
            }
        })
        .boxed()
    }

    pub fn num_objects(&self) -> usize {
        self.known_object_ids.pin().len()
    }

    pub async fn object(&self, id: &ObjectId) -> Result<Option<Object>, crate::Error> {
        if !self.known_object_ids.pin().contains_key(id) {
            return Ok(None);
        }

        Ok(Some(self.backend.object(id).await?))
    }

    pub async fn delete_object(&self, id: &ObjectId) -> Result<(), crate::Error> {
        self.backend.delete_object(id).await?;
        self.known_object_ids.pin().remove(id);
        Ok(())
    }

    #[inline]
    pub async fn download(
        &self,
        id: &ObjectId,
    ) -> Result<Option<DownloadableObject>, crate::Error> {
        if !self.known_object_ids.pin().contains_key(id) {
            return Ok(None);
        }
        Ok(Some(self.backend.download(id).await?))
    }

    #[inline]
    pub async fn upload(
        &self,
        name_hint: impl AsRef<str>,
        content: impl AsyncRead + Send + Unpin + 'static,
        metadata: Option<Metadata<'static>>,
    ) -> Result<Object, crate::Error> {
        let object = self.backend.upload(name_hint, content, metadata).await?;
        self.known_object_ids.pin().insert(object.id().clone(), ());
        Ok(object)
    }
}
