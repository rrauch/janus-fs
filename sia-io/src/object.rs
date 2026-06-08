use crate::cache::Cache;
use crate::chunk::{ChunkDownloader, ChunkedReader};
#[cfg(feature = "indexd")]
use crate::indexd;
#[cfg(feature = "mock")]
use crate::mock;
#[cfg(feature = "renterd")]
use crate::renterd;
use crate::scheduler::Scheduler;
use crate::scheduler::resource_manager::Resource;
use crate::{Backend, Client, ETag, Metadata, MetadataSource, MimeType};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures_io::{AsyncRead, AsyncSeek};
use futures_util::{StreamExt, TryStream, TryStreamExt, stream};
use ouroboros::self_referencing;
use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use sia_storage::ObjectsCursor;
use std::borrow::Cow;
use std::fmt::{Display, Formatter};
use std::hash::Hasher;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::{fmt, iter};
use thiserror::Error;
use twox_hash::XxHash3_64;

const VERSION_HASH_PREFIX: &[u8] = b"_SIA_OBJECT_VERSION_BEGIN_\n";
const VERSION_HASH_SUFFIX: &[u8] = b"\n_SIA_OBJECT_VERSION_END_";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ObjectId {
    #[cfg(feature = "indexd")]
    Indexd(indexd::object::ObjectId),
    #[cfg(feature = "renterd")]
    Renterd(renterd::object::FileId),
    #[cfg(feature = "mock")]
    Mock(mock::MockObjectId),
}

impl Serialize for ObjectId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let id: Cow<str> = match self {
            #[cfg(feature = "indexd")]
            Self::Indexd(id) => Cow::Owned(id.to_string()),
            #[cfg(feature = "renterd")]
            Self::Renterd(id) => Cow::Owned(id.to_string()),
            #[cfg(feature = "mock")]
            Self::Mock(id) => Cow::Borrowed(id.as_ref()),
        };

        serializer.serialize_str(id.as_ref())
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
    #[cfg(feature = "mock")]
    #[error(transparent)]
    MockError(#[from] <mock::MockObjectId as FromStr>::Err),
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

        #[cfg(feature = "mock")]
        {
            if s.starts_with("mock:") {
                return Ok(mock::MockObjectId::from_str(s)?.into());
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
            #[cfg(feature = "indexd")]
            Self::Indexd(id) => Display::fmt(id, f),
            #[cfg(feature = "renterd")]
            Self::Renterd(id) => Display::fmt(id, f),
            #[cfg(feature = "mock")]
            Self::Mock(id) => Display::fmt(id, f),
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

#[cfg(feature = "mock")]
impl From<mock::MockObjectId> for ObjectId {
    fn from(value: mock::MockObjectId) -> Self {
        Self::Mock(value)
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Serialize, Deserialize)]
#[repr(transparent)]
pub struct Version(u64);

impl Display for Version {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone)]
pub enum Object {
    #[cfg(feature = "indexd")]
    Indexd {
        id: ObjectId,
        version: Version,
        inner: Arc<indexd::object::Object>,
    },
    #[cfg(feature = "renterd")]
    Renterd {
        id: ObjectId,
        version: Version,
        inner: Arc<renterd::object::File>,
    },
    #[cfg(feature = "mock")]
    Mock {
        id: ObjectId,
        version: Version,
        inner: Arc<mock::MockObject>,
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
            #[cfg(feature = "mock")]
            Self::Mock { id, .. } => id,
        }
    }

    #[inline]
    pub fn version(&self) -> Version {
        match self {
            #[cfg(feature = "indexd")]
            Self::Indexd { version, .. } => *version,
            #[cfg(feature = "renterd")]
            Self::Renterd { version, .. } => *version,
            #[cfg(feature = "mock")]
            Self::Mock { version, .. } => *version,
        }
    }

    #[inline]
    pub fn created(&self) -> Option<&DateTime<Utc>> {
        match &self {
            #[cfg(feature = "indexd")]
            Self::Indexd { inner, .. } => Some(inner.created_at()),
            #[cfg(feature = "renterd")]
            Self::Renterd { .. } => None,
            #[cfg(feature = "mock")]
            Self::Mock { inner, .. } => Some(&inner.created_at),
        }
    }

    #[inline]
    pub fn updated(&self) -> &DateTime<Utc> {
        match &self {
            #[cfg(feature = "indexd")]
            Self::Indexd { inner, .. } => inner.updated_at(),
            #[cfg(feature = "renterd")]
            Self::Renterd { inner, .. } => inner.mod_time(),
            #[cfg(feature = "mock")]
            Self::Mock { inner, .. } => &inner.updated_at,
        }
    }

    #[inline]
    pub fn size(&self) -> u64 {
        match &self {
            #[cfg(feature = "indexd")]
            Self::Indexd { inner, .. } => inner.size(),
            #[cfg(feature = "renterd")]
            Self::Renterd { inner, .. } => inner.size(),
            #[cfg(feature = "mock")]
            Self::Mock { inner, .. } => inner.size(),
        }
    }

    #[inline]
    pub fn mime_type(&self) -> Option<&MimeType> {
        match &self {
            #[cfg(feature = "indexd")]
            Self::Indexd { .. } => None,
            #[cfg(feature = "renterd")]
            Self::Renterd { inner, .. } => Some(inner.mime_type()),
            #[cfg(feature = "mock")]
            Self::Mock { inner, .. } => inner.mime_type.as_ref(),
        }
    }

    #[inline]
    pub fn etag(&self) -> Option<&ETag> {
        match &self {
            #[cfg(feature = "indexd")]
            Self::Indexd { .. } => None,
            #[cfg(feature = "renterd")]
            Self::Renterd { inner, .. } => inner.etag(),
            #[cfg(feature = "mock")]
            Self::Mock { inner, .. } => inner.etag.as_ref(),
        }
    }

    #[inline]
    pub fn metadata(&self) -> Metadata<'_> {
        match &self {
            #[cfg(feature = "indexd")]
            Self::Indexd { inner, .. } => Metadata::Indexd(Cow::Borrowed(inner.metadata())),
            #[cfg(feature = "renterd")]
            Self::Renterd { inner, .. } => Metadata::Renterd(Cow::Borrowed(inner.metadata())),
            #[cfg(feature = "mock")]
            Self::Mock { inner, .. } => Metadata::Mock(Cow::Borrowed(&inner.metadata)),
        }
    }
}

#[cfg(feature = "indexd")]
impl From<indexd::object::Object> for Object {
    fn from(value: indexd::object::Object) -> Self {
        let mut hasher = XxHash3_64::new();
        hasher.write(VERSION_HASH_PREFIX);
        hasher.write("INDEXD\n".as_bytes());
        value.hash(&mut hasher);
        hasher.write(VERSION_HASH_SUFFIX);
        let version = Version(hasher.finish());

        let id = value.id().clone().into();
        Self::Indexd {
            id,
            version,
            inner: Arc::new(value),
        }
    }
}

#[cfg(feature = "renterd")]
impl From<renterd::object::File> for Object {
    fn from(value: renterd::object::File) -> Self {
        let mut hasher = XxHash3_64::new();
        hasher.write(VERSION_HASH_PREFIX);
        hasher.write("RENTERD\n".as_bytes());
        value.hash(&mut hasher);
        hasher.write(VERSION_HASH_SUFFIX);
        let version = Version(hasher.finish());

        let id = value.id().clone().into();
        Self::Renterd {
            id,
            version,
            inner: Arc::new(value),
        }
    }
}

#[cfg(feature = "mock")]
impl From<mock::MockObject> for Object {
    fn from(value: mock::MockObject) -> Self {
        let mut hasher = XxHash3_64::new();
        hasher.write(VERSION_HASH_PREFIX);
        hasher.write("MOCK\n".as_bytes());
        value.hash(&mut hasher);
        hasher.write(VERSION_HASH_SUFFIX);
        let version = Version(hasher.finish());

        let id = value.id.clone().into();
        Self::Mock {
            id,
            version,
            inner: Arc::new(value),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum BackendDO {
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
    #[cfg(feature = "mock")]
    Mock {
        object: Object,
        content: bytes::Bytes,
    },
}

impl BackendDO {
    #[inline]
    pub fn object(&self) -> &Object {
        match &self {
            #[cfg(feature = "indexd")]
            Self::Indexd { object, .. } => object,
            #[cfg(feature = "renterd")]
            Self::Renterd { object, .. } => object,
            #[cfg(feature = "mock")]
            Self::Mock { object, .. } => object,
        }
    }

    #[inline]
    pub async fn open(&self, offset: impl Into<Option<u64>>) -> Result<Download, crate::Error> {
        let offset = offset.into();
        Ok(match &self {
            #[cfg(feature = "indexd")]
            Self::Indexd { inner, .. } => Download::Indexd(inner.open(offset).await?),
            #[cfg(feature = "renterd")]
            Self::Renterd { inner, .. } => Download::Renterd(inner.open(offset).await?),
            #[cfg(feature = "mock")]
            Self::Mock { content, .. } => {
                Download::Mock(futures_util::io::Cursor::new(content.clone()))
            }
        })
    }
}

#[derive(Debug)]
pub enum Download {
    #[cfg(feature = "indexd")]
    Indexd(indexd::download::Download),
    #[cfg(feature = "renterd")]
    Renterd(renterd::download::Download),
    #[cfg(feature = "mock")]
    Mock(futures_util::io::Cursor<bytes::Bytes>),
}

impl Download {
    pub fn len(&self) -> u64 {
        match self {
            #[cfg(feature = "indexd")]
            Self::Indexd(indexd) => indexd.len(),
            #[cfg(feature = "renterd")]
            Self::Renterd(renterd) => renterd.len(),
            #[cfg(feature = "mock")]
            Self::Mock(cursor) => cursor.get_ref().len() as u64,
        }
    }
}

impl AsyncRead for Download {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.as_mut();
        match this.get_mut() {
            #[cfg(feature = "indexd")]
            Self::Indexd(indexd) => Pin::new(indexd).poll_read(cx, buf),
            #[cfg(feature = "renterd")]
            Self::Renterd(renterd) => Pin::new(renterd).poll_read(cx, buf),
            #[cfg(feature = "mock")]
            Self::Mock(cursor) => Pin::new(cursor).poll_read(cx, buf),
        }
    }
}

#[async_trait]
impl Resource for Download {
    fn offset(&self) -> u64 {
        match self {
            #[cfg(feature = "indexd")]
            Self::Indexd(indexd) => indexd.offset(),
            #[cfg(feature = "renterd")]
            Self::Renterd(renterd) => renterd.offset(),
            #[cfg(feature = "mock")]
            Self::Mock(cursor) => cursor.position(),
        }
    }

    fn can_reuse(&self) -> bool {
        match self {
            #[cfg(feature = "indexd")]
            Self::Indexd(indexd) => indexd.can_reuse(),
            #[cfg(feature = "renterd")]
            Self::Renterd(renterd) => renterd.can_reuse(),
            #[cfg(feature = "mock")]
            Self::Mock(cursor) => {
                // check if anything left to read
                cursor.position() < cursor.get_ref().len() as u64
            }
        }
    }

    async fn finalize(self) -> anyhow::Result<()> {
        match self {
            #[cfg(feature = "indexd")]
            Self::Indexd(indexd) => indexd.finalize().await,
            #[cfg(feature = "renterd")]
            Self::Renterd(renterd) => renterd.finalize().await,
            #[cfg(feature = "mock")]
            Self::Mock(_) => Ok(()),
        }
    }
}

#[cfg(feature = "indexd")]
impl From<indexd::download::DownloadableObject> for BackendDO {
    fn from(value: indexd::download::DownloadableObject) -> Self {
        let object = Object::from(value.object().clone());
        Self::Indexd {
            object,
            inner: value,
        }
    }
}

#[cfg(feature = "renterd")]
impl From<renterd::download::DownloadableFile> for BackendDO {
    fn from(value: renterd::download::DownloadableFile) -> Self {
        let object = Object::from(value.file().clone());
        Self::Renterd {
            object,
            inner: value,
        }
    }
}

impl Backend {
    #[inline]
    pub(crate) async fn object(&self, id: &ObjectId) -> Result<Object, crate::Error> {
        match (&self, id) {
            #[cfg(feature = "indexd")]
            (Self::Indexd(indexd), ObjectId::Indexd(id)) => {
                Ok(indexd.object(id).await.map(Object::from)?)
            }
            #[cfg(feature = "renterd")]
            (Self::Renterd(renterd), ObjectId::Renterd(id)) => {
                Ok(renterd.object(id).await.map(Object::from)?)
            }
            #[cfg(feature = "mock")]
            (Self::Mock(mock), ObjectId::Mock(id)) => Ok(mock.object(id).map(Object::from)?),
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
            #[cfg(feature = "mock")]
            (Self::Mock(mock), ObjectId::Mock(id)) => Ok(mock.delete_object(id)?),
            _ => Err(crate::Error::BackendMismatch),
        }
    }

    #[inline]
    pub(crate) async fn download(&self, id: &ObjectId) -> Result<BackendDO, crate::Error> {
        match (&self, id) {
            #[cfg(feature = "indexd")]
            (Self::Indexd(indexd), ObjectId::Indexd(id)) => {
                Ok(BackendDO::from(indexd.download(id).await?))
            }
            #[cfg(feature = "renterd")]
            (Self::Renterd(renterd), ObjectId::Renterd(id)) => {
                Ok(BackendDO::from(renterd.download(id).await?))
            }
            #[cfg(feature = "mock")]
            (Self::Mock(mock), ObjectId::Mock(id)) => {
                let object = mock.object(id)?;
                let content = object.content.clone();
                Ok(BackendDO::Mock {
                    object: object.into(),
                    content,
                })
            }
            _ => Err(crate::Error::BackendMismatch),
        }
    }

    async fn upload(
        &self,
        name_hint: impl AsRef<str>,
        mut content: impl AsyncRead + Send + Unpin + 'static,
        metadata: Option<Metadata<'_>>,
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
                    Some(Metadata::Renterd(m)) => Some(m.to_owned()),
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
            #[cfg(feature = "mock")]
            (Self::Mock(mock), metadata) => {
                let metadata = match metadata {
                    Some(Metadata::Mock(m)) => Some(m.to_owned()),
                    None => None,
                    _ => return Err(crate::Error::BackendMismatch),
                }
                .unwrap_or_default();
                let id = mock::MockObjectId::try_from(format!("mock:{}", name_hint.as_ref()))?;
                let mut buf = vec![];
                futures_util::AsyncReadExt::read_to_end(&mut content, &mut buf).await?;
                let now = Utc::now();
                let object = mock::MockObject {
                    id,
                    created_at: now,
                    updated_at: now,
                    mime_type: None,
                    etag: None,
                    metadata: metadata.into_owned(),
                    content: buf.into(),
                };
                mock.insert_object(object.clone())?;
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
            #[cfg(feature = "indexd")]
            Self::Indexd(indexd) => {
                let (objects, cursor) = indexd.list_objects().await?;
                (
                    Box::new(stream::iter(objects.into_iter().map(|o| Ok(o.into())))),
                    cursor,
                )
            }
            #[cfg(feature = "renterd")]
            Self::Renterd(renterd) => (
                Box::new(
                    renterd
                        .list_objects("")?
                        .map_err(crate::Error::from)
                        .try_filter_map(|any| async move {
                            Ok(match any {
                                renterd::object::AnyObject::File(file) => Some(file.into()),
                                renterd::object::AnyObject::Folder(_) => None,
                            })
                        })
                        .boxed(),
                ),
                None,
            ),
            #[cfg(feature = "mock")]
            Self::Mock(mock) => (
                Box::new(
                    mock.list_objects()
                        .map_err(crate::Error::from)
                        .try_filter_map(|o| async move { Ok(Some(o.into())) })
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
            #[cfg(feature = "indexd")]
            Self::Indexd(indexd) => Some(Box::new(
                indexd
                    .object_events(cursor)
                    .map_err(crate::Error::from)
                    .try_filter_map(|e| async move { Ok(Some(e.into())) })
                    .boxed(),
            )),
            #[cfg(feature = "renterd")]
            Self::Renterd(_) => None,
            #[cfg(feature = "mock")]
            Self::Mock(_) => None,
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
            #[cfg(feature = "indexd")]
            ObjectId::Indexd(indexd_id) => Some(ObjectsCursor {
                id: indexd_id.clone().into_inner(),
                after: self.timestamp().clone(),
            }),
            #[cfg(feature = "renterd")]
            ObjectId::Renterd(_) => None,
            #[cfg(feature = "mock")]
            ObjectId::Mock(_) => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DownloadableObject {
    chunk_size: usize,
    object: Object,
    backend: Backend,
    cache: Cache,
    downloader: Arc<Scheduler<ChunkDownloader>>,
}

impl DownloadableObject {
    #[inline]
    pub fn object(&self) -> &Object {
        &self.object
    }

    #[inline]
    pub async fn open(&self) -> Result<impl AsyncRead + AsyncSeek + Send + Unpin, crate::Error> {
        ChunkedReader::new(
            self.cache.clone(),
            self.object.clone(),
            self.chunk_size,
            self.downloader.clone(),
        )
        .await
    }
}

pub struct UploadableObject<M, C> {
    name_hint: Cow<'static, str>,
    content: C,
    metadata: Option<M>,
}

impl<M: MetadataSource, C: AsyncRead + Send + Unpin + 'static> UploadableObject<M, C> {
    pub fn new(name_hint: impl Into<Cow<'static, str>>, content: C, metadata: Option<M>) -> Self {
        Self {
            name_hint: name_hint.into(),
            content,
            metadata,
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
    pub fn list_objects(
        &self,
    ) -> impl TryStream<Ok = Object, Error = crate::Error> + Send + Unpin + '_ {
        let set = self.known_object_ids.clone();

        let holder = IterHolderBuilder {
            set,
            guard_builder: |set| set.owned_guard(),
            iter_builder: |set, guard| set.iter(guard),
        }
        .build();

        stream::try_unfold(holder, move |mut holder| async move {
            if let Some(id) = holder.with_iter_mut(|iter| iter.next().map(|(id, _)| id)) {
                let object = self.cache.get_object(id, &self.backend).await?;
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

        Ok(Some(self.cache.get_object(id, &self.backend).await?))
    }

    pub async fn delete_object(&self, id: &ObjectId) -> Result<(), crate::Error> {
        self.backend.delete_object(id).await?;
        self.known_object_ids.pin().remove(id);
        self.cache.invalidate_object(id).await?;
        Ok(())
    }

    #[inline]
    pub async fn download(
        &self,
        id: &ObjectId,
    ) -> Result<Option<DownloadableObject>, crate::Error> {
        Ok(self.object(id).await?.map(|object| DownloadableObject {
            chunk_size: self.chunk_size,
            object,
            backend: self.backend.clone(),
            cache: self.cache.clone(),
            downloader: self.chunk_downloader.clone(),
        }))
    }

    #[inline]
    pub async fn upload<M: MetadataSource, C: AsyncRead + Send + Unpin + 'static>(
        &self,
        uploadable_object: UploadableObject<M, C>,
    ) -> Result<Object, crate::Error> {
        let metadata = uploadable_object
            .metadata
            .as_ref()
            .map(|m| match &self.backend {
                #[cfg(feature = "indexd")]
                Backend::Indexd(_) => Metadata::Indexd(m.to_bytes()),
                #[cfg(feature = "renterd")]
                Backend::Renterd(_) => Metadata::Renterd(m.to_map()),
                #[cfg(feature = "mock")]
                Backend::Mock(_) => Metadata::Mock(m.to_map()),
            });

        let object = self
            .backend
            .upload(
                uploadable_object.name_hint,
                uploadable_object.content,
                metadata,
            )
            .await?;
        self.cache
            .insert_object(object.clone(), &self.backend)
            .await?;
        self.known_object_ids.pin().insert(object.id().clone(), ());
        Ok(object)
    }
}
