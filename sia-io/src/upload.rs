use crate::cache::Cache;
use crate::object::{Object, ObjectId};
use crate::{Backend, Client, Metadata, MetadataSource};
use futures_io::AsyncRead;
use std::borrow::Cow;
use std::sync::Arc;
use thiserror::Error;
use tokio_util::compat::FuturesAsyncReadCompatExt;

#[derive(Debug, Error)]
pub enum UploadError {
    #[error("upload queue is full")]
    UploadQueueFull,
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error("number of uploaded objects does not match expected number")]
    ObjectMismatch,
    #[error(transparent)]
    SiaError(#[from] sia_storage::UploadError),
    #[error("upload error: {0}")]
    Other(String),
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

enum UploaderInner<'a> {
    #[cfg(feature = "indexd")]
    Packed(
        sia_storage::PackedUpload,
        Vec<Option<Box<dyn MetadataSource + 'a>>>,
    ),
    Simple(Option<SimpleUpload<'a>>),
}

struct SimpleUpload<'a> {
    name_hint: Cow<'static, str>,
    content: Box<dyn AsyncRead + Send + Unpin + 'static>,
    metadata_source: Option<Box<dyn MetadataSource + 'a>>,
}

pub struct MultiUploader<'a> {
    inner: UploaderInner<'a>,
    backend: Backend,
    cache: Cache,
    known_object_ids: Arc<papaya::HashMap<ObjectId, ()>>,
    idx: usize,
}

impl<'a> MultiUploader<'a> {
    #[inline]
    pub fn space_remaining(&self) -> u64 {
        match &self.inner {
            #[cfg(feature = "indexd")]
            UploaderInner::Packed(packed, _) => packed.remaining(),
            UploaderInner::Simple(simple) => {
                if simple.is_some() {
                    0
                } else {
                    u64::MAX
                }
            }
        }
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        match &self.inner {
            #[cfg(feature = "indexd")]
            UploaderInner::Packed(_, _) => false,
            UploaderInner::Simple(simple) => simple.is_some(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.idx == 0
    }

    pub fn len(&self) -> usize {
        self.idx
    }

    pub async fn enqueue<M: MetadataSource + 'a, C: AsyncRead + Send + Unpin + 'static>(
        &mut self,
        mut uploadable_object: UploadableObject<M, C>,
    ) -> Result<(), UploadError> {
        if self.is_full() {
            return Err(UploadError::UploadQueueFull);
        }

        match &mut self.inner {
            #[cfg(feature = "indexd")]
            UploaderInner::Packed(packed, meta) => {
                packed
                    .add((&mut uploadable_object.content).compat())
                    .await?;
                meta.push(
                    uploadable_object
                        .metadata
                        .map(|m| Box::new(m) as Box<dyn MetadataSource>),
                );
            }
            UploaderInner::Simple(Some(_)) => {
                return Err(UploadError::UploadQueueFull);
            }
            UploaderInner::Simple(None) => {
                let simple = SimpleUpload {
                    name_hint: uploadable_object.name_hint,
                    content: Box::new(uploadable_object.content),
                    metadata_source: uploadable_object
                        .metadata
                        .map(|m| Box::new(m) as Box<dyn MetadataSource>),
                };
                self.inner = UploaderInner::Simple(Some(simple));
            }
        }

        self.idx += 1;

        Ok(())
    }

    pub async fn process(mut self) -> Result<Vec<Object>, UploadError> {
        let objects = match std::mem::replace(&mut self.inner, UploaderInner::Simple(None)) {
            #[cfg(feature = "indexd")]
            UploaderInner::Packed(packed, meta) => {
                assert_eq!(self.idx, meta.len());
                if self.idx == 0 {
                    return Ok(vec![]);
                }
                self.process_packed(packed, meta).await
            }
            UploaderInner::Simple(Some(simple)) => {
                self.process_simple(simple).await.map(|o| vec![o])
            }
            UploaderInner::Simple(None) => Ok(vec![]),
        }?;

        for object in &objects {
            self.cache
                .insert_object(object.clone(), &self.backend)
                .await
                .map_err(|e| UploadError::Other(e.to_string()))?;
            self.known_object_ids.pin().insert(object.id().clone(), ());
        }

        Ok(objects)
    }

    async fn process_simple(&mut self, simple: SimpleUpload<'a>) -> Result<Object, UploadError> {
        let metadata = simple.metadata_source.as_ref().map(|s| match self.backend {
            #[cfg(feature = "indexd")]
            Backend::Indexd(_) => Metadata::Indexd(s.to_bytes()),
            #[cfg(feature = "renterd")]
            Backend::Renterd(_) => Metadata::Renterd(s.to_map()),
            #[cfg(feature = "mock")]
            Backend::Mock(_) => Metadata::Mock(s.to_map()),
        });

        Ok(self
            .backend
            .upload(simple.name_hint, simple.content, metadata)
            .await
            .map_err(|e| UploadError::Other(e.to_string()))?)
    }

    #[cfg(feature = "indexd")]
    async fn process_packed(
        &mut self,
        packed: sia_storage::PackedUpload,
        mut meta: Vec<Option<Box<dyn MetadataSource + 'a>>>,
    ) -> Result<Vec<Object>, UploadError> {
        let indexd = match &self.backend {
            Backend::Indexd(indexd) => indexd,
            _ => {
                return Err(UploadError::Other(
                    crate::Error::BackendMismatch.to_string(),
                ));
            }
        };

        let objects = packed
            .finalize()
            .await
            .map_err(|e| UploadError::Other(e.to_string()))?;
        if objects.len() != meta.len() {
            return Err(UploadError::ObjectMismatch);
        }

        let mut result_objects = Vec::with_capacity(objects.len());

        for (i, mut o) in objects.into_iter().enumerate() {
            if let Some(m) = meta.get_mut(i) {
                if let Some(s) = m.take() {
                    o.metadata = s.to_bytes().into_owned();
                }
                // pin object
                indexd
                    .sdk()
                    .pin_object(&o)
                    .await
                    .map_err(|e| UploadError::Other(e.to_string()))?;
                // retrieve object again to ensure it's identical to remote version
                result_objects.push(
                    crate::indexd::object::Object::from(
                        indexd
                            .sdk()
                            .object(&o.id())
                            .await
                            .map_err(|e| UploadError::Other(e.to_string()))?,
                    )
                    .into(),
                );
            }
        }

        Ok(result_objects)
    }
}

impl Client {
    #[inline]
    pub fn prepare_multi_upload<'a>(&self) -> Result<MultiUploader<'a>, crate::Error> {
        let inner = match &self.backend {
            #[cfg(feature = "indexd")]
            Backend::Indexd(indexd) => UploaderInner::Packed(indexd.new_packed_upload()?, vec![]),
            _ => UploaderInner::Simple(None),
        };

        Ok(MultiUploader {
            inner,
            backend: self.backend.clone(),
            cache: self.cache.clone(),
            known_object_ids: self.known_object_ids.clone(),
            idx: 0,
        })
    }

    #[inline]
    pub async fn upload<M: MetadataSource, C: AsyncRead + Send + Unpin + 'static>(
        &self,
        uploadable_object: UploadableObject<M, C>,
    ) -> Result<Object, crate::Error> {
        let mut uploader = self.prepare_multi_upload()?;
        uploader.enqueue(uploadable_object).await?;
        let mut objects = uploader.process().await?;
        assert_eq!(objects.len(), 1);
        Ok(objects.swap_remove(0))
    }
}

impl Backend {
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
                    .map_err(crate::renterd::client::ClientError::ObjectKeyError)?;

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
                let id =
                    crate::mock::MockObjectId::try_from(format!("mock:{}", name_hint.as_ref()))?;
                let mut buf = vec![];
                futures_util::AsyncReadExt::read_to_end(&mut content, &mut buf).await?;
                let now = chrono::Utc::now();
                let object = crate::mock::MockObject {
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
}
