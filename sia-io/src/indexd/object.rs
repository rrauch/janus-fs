use crate::indexd::client::{Client, ClientError};
use crate::tagged::{TaggedValue, TryFromInner, WithFromStr, WithSerde};
use crate::upload::UploadError;
use chrono::{DateTime, Utc};
use futures_io::AsyncRead;
use futures_util::{StreamExt, TryStream, TryStreamExt};
use indexmap::IndexMap;
use sia_storage::{Hash256, PackedUpload, UploadOptions};
use sia_storage::{Object as SiaObject, ObjectsCursor};
use std::collections::VecDeque;
use std::convert::Infallible;
use std::hash::{Hash, Hasher};
use thiserror::Error;
use tokio_util::compat::FuturesAsyncReadCompatExt;

pub struct ObjectIdKind;
pub type ObjectId = TaggedValue<ObjectIdKind, Hash256>;

impl WithSerde for ObjectId {}
impl WithFromStr for ObjectId {}

impl TryFromInner<Hash256> for ObjectId {
    type Err = Infallible;

    fn try_from_inner(inner: Hash256) -> Result<Self, Self::Err>
    where
        Self: Sized,
    {
        Ok(Self::new_from_inner(inner))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Object {
    id: ObjectId,
    inner: SiaObject,
}

impl Object {
    #[inline]
    pub fn id(&self) -> &ObjectId {
        &self.id
    }

    #[inline]
    pub fn created_at(&self) -> &DateTime<Utc> {
        self.inner.created_at()
    }

    #[inline]
    pub fn updated_at(&self) -> &DateTime<Utc> {
        self.inner.updated_at()
    }

    #[inline]
    pub fn size(&self) -> u64 {
        self.inner.size()
    }

    #[inline]
    pub fn metadata(&self) -> &[u8] {
        self.inner.metadata.as_slice()
    }

    #[inline]
    pub(crate) fn as_inner(&self) -> &SiaObject {
        &self.inner
    }

    pub(crate) fn hash(&self, hasher: &mut impl Hasher) {
        hasher.write(b"ID:");
        self.id.hash(hasher);
        hasher.write(b"\nCREATED_AT:");
        self.inner.created_at().hash(hasher);
        hasher.write(b"\nUPDATED_AT:");
        self.inner.updated_at().hash(hasher);
        hasher.write(b"\nSIZE:");
        hasher.write_u64(self.size());

        hasher.write(b"\nMETADATA:");
        let metadata = self.metadata();
        hasher.write(metadata);
        hasher.write(b"\nMETADATA_LEN:");
        hasher.write_usize(metadata.len());
    }
}

impl From<SiaObject> for Object {
    fn from(value: SiaObject) -> Self {
        Self {
            id: ObjectId::new_from_inner(value.id()),
            inner: value,
        }
    }
}

#[derive(Debug)]
pub enum ObjectEvent {
    New(Object, DateTime<Utc>),
    Updated(Object, DateTime<Utc>),
    Deleted(ObjectId, DateTime<Utc>),
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

    pub(crate) fn cursor(&self) -> ObjectsCursor {
        ObjectsCursor {
            id: self.object_id().clone().into_inner(),
            after: self.timestamp().clone(),
        }
    }
}

impl TryFrom<ObjectEvent> for Object {
    type Error = ObjectEvent;

    fn try_from(value: ObjectEvent) -> Result<Self, Self::Error> {
        match value {
            ObjectEvent::New(o, _) | ObjectEvent::Updated(o, _) => Ok(o),
            ObjectEvent::Deleted(id, ts) => Err(ObjectEvent::Deleted(id, ts)),
        }
    }
}

#[derive(Debug, Error)]
pub enum ObjectEventError {
    #[error("object expected but not present in object event")]
    MissingObject,
}

impl TryFrom<sia_storage::ObjectEvent> for ObjectEvent {
    type Error = ObjectEventError;

    fn try_from(value: sia_storage::ObjectEvent) -> Result<Self, Self::Error> {
        let ts = value.updated_at;
        if value.deleted {
            Ok(ObjectEvent::Deleted(ObjectId::new_from_inner(value.id), ts))
        } else {
            if let Some(object) = value.object {
                if object.created_at() == object.updated_at() {
                    Ok(ObjectEvent::New(object.into(), ts))
                } else {
                    Ok(ObjectEvent::Updated(object.into(), ts))
                }
            } else {
                Err(ObjectEventError::MissingObject)
            }
        }
    }
}

impl Client {
    pub async fn object(&self, object_id: &ObjectId) -> Result<Object, ClientError> {
        Ok(self.sdk().object(object_id.as_ref()).await?.into())
    }

    pub(crate) fn new_packed_upload(&self) -> Result<PackedUpload, UploadError> {
        let mut options = UploadOptions::default();
        if let Some(max_buffered_slabs) = self.upload_max_buffered_slabs() {
            options.max_buffered_slabs = Some(max_buffered_slabs);
        }
        Ok(self.sdk().upload_packed(options)?)
    }

    pub async fn upload<U: AsyncRead + Send + Unpin + 'static>(
        &self,
        content: U,
        metadata: Option<Vec<u8>>,
    ) -> Result<Object, ClientError> {
        let mut options = UploadOptions::default();
        if let Some(max_buffered_slabs) = self.upload_max_buffered_slabs() {
            options.max_buffered_slabs = Some(max_buffered_slabs);
        }

        let mut object = SiaObject::default();
        if let Some(metadata) = metadata {
            object.metadata = metadata;
        }

        let reader = content.compat();
        let object: Object = self
            .sdk()
            .upload(object, reader, options)
            .await
            .map_err(sia_storage::Error::Upload)?
            .into();

        self.sdk().pin_object(object.as_inner()).await?;
        // retrieve object again to ensure it's identical to remote version
        let object = self.sdk().object(object.id().as_ref()).await?.into();
        Ok(object)
    }

    pub async fn update_object_metadata(
        &self,
        object_id: &ObjectId,
        metadata: Vec<u8>,
    ) -> Result<Object, ClientError> {
        let mut object = self.sdk().object(object_id.as_ref()).await?;
        object.metadata = metadata;
        self.sdk().update_object_metadata(&object).await?;
        Ok(object.into())
    }

    pub async fn delete_objects(
        &self,
        object_ids: impl Iterator<Item = &ObjectId>,
    ) -> Result<(), ClientError> {
        for id in object_ids {
            self.sdk().delete_object(id.as_ref()).await?
        }
        self.sdk().prune_slabs().await?;
        Ok(())
    }

    pub fn object_events(
        &self,
        cursor: Option<ObjectsCursor>,
    ) -> impl TryStream<Ok = ObjectEvent, Error = ClientError> + Send + Unpin {
        let this = self.clone();
        let initial_state = (VecDeque::new(), false, cursor);

        futures_util::stream::try_unfold(initial_state, move |state| {
            let this = this.clone();
            async move {
                let mut objects: VecDeque<ObjectEvent> = state.0;
                let mut eof_reached = state.1;
                let mut cursor: Option<ObjectsCursor> = state.2;

                loop {
                    if let Some(object) = objects.pop_front() {
                        return Ok(Some((object, (objects, eof_reached, cursor))));
                    }

                    if eof_reached {
                        return Ok(None);
                    }

                    let resp = this
                        .sdk()
                        .object_events(cursor.take(), Some(100))
                        .await?
                        .into_iter()
                        .map(|o| o.try_into())
                        .collect::<Result<Vec<ObjectEvent>, _>>()
                        .map_err(|e| ClientError::Other(e.to_string()))?;

                    if let Some(last) = resp.last() {
                        cursor = Some(last.cursor());
                        resp.into_iter().for_each(|o| objects.push_back(o));
                    } else {
                        eof_reached = true;
                    }
                }
            }
        })
        .boxed()
    }

    pub async fn list_objects(&self) -> Result<(Vec<Object>, Option<ObjectsCursor>), ClientError> {
        let mut active_objects = IndexMap::new();
        let mut stream = self.object_events(None);
        let mut latest_delete = None;
        while let Some(event) = stream.try_next().await? {
            match &event {
                ObjectEvent::Updated(o, _) | ObjectEvent::New(o, _) => {
                    active_objects.insert(o.id().clone(), event);
                }
                ObjectEvent::Deleted(id, _) => {
                    active_objects.shift_remove(id);
                    latest_delete = Some(event);
                }
            }
        }
        let mut cursor = active_objects.last().map(|(_, e)| e.cursor());
        let cursor = if let Some(latest_delete) = latest_delete {
            Some(match cursor.take() {
                None => latest_delete.cursor(),
                Some(cursor) if &cursor.after < latest_delete.timestamp() => latest_delete.cursor(),
                Some(cursor) => cursor,
            })
        } else {
            cursor
        };

        Ok((
            active_objects
                .into_values()
                .map(|e| Object::try_from(e).expect("object event conversion to succeed"))
                .collect(),
            cursor,
        ))
    }
}
