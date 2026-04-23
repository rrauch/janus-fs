use crate::Backend;
use crate::cache::{Cache, HasWeight, InnerCache};
use crate::object::{Object, ObjectId};
use async_trait::async_trait;
use sia_storage::SealedObject;

pub(super) type MetadataCache = InnerCache<ObjectId, Object, Box<dyn L2MetadataCache>>;

impl HasWeight for ObjectId {
    fn weigh(&self) -> usize {
        size_of_val(self)
    }
}

impl HasWeight for Object {
    fn weigh(&self) -> usize {
        size_of_val(self)
    }
}

#[async_trait]
pub trait L2MetadataCache: Send + Sync {
    #[cfg(feature = "indexd")]
    async fn get_indexd_object(
        &self,
        id: &crate::indexd::object::ObjectId,
    ) -> Result<Option<SealedObject>, std::io::Error>;

    #[cfg(feature = "indexd")]
    async fn insert_indexd_object(
        &self,
        id: &crate::indexd::object::ObjectId,
        object: SealedObject,
    ) -> Result<(), std::io::Error>;

    #[cfg(feature = "indexd")]
    async fn invalidate_indexd_object(
        &self,
        id: &crate::indexd::object::ObjectId,
    ) -> Result<(), std::io::Error>;

    #[cfg(feature = "renterd")]
    async fn get_renterd_object(
        &self,
        id: &crate::renterd::object::FileId,
    ) -> Result<Option<crate::renterd::object::File>, std::io::Error>;

    #[cfg(feature = "renterd")]
    async fn insert_renterd_object(
        &self,
        object: crate::renterd::object::File,
    ) -> Result<(), std::io::Error>;

    #[cfg(feature = "renterd")]
    async fn invalidate_renterd_object(
        &self,
        id: &crate::renterd::object::FileId,
    ) -> Result<(), std::io::Error>;
}

#[async_trait]
impl L2MetadataCache for Box<dyn L2MetadataCache> {
    #[cfg(feature = "indexd")]
    #[inline]
    async fn get_indexd_object(
        &self,
        id: &crate::indexd::object::ObjectId,
    ) -> Result<Option<SealedObject>, std::io::Error> {
        self.as_ref().get_indexd_object(id).await
    }

    #[cfg(feature = "indexd")]
    #[inline]
    async fn insert_indexd_object(
        &self,
        id: &crate::indexd::object::ObjectId,
        object: SealedObject,
    ) -> Result<(), std::io::Error> {
        self.as_ref().insert_indexd_object(id, object).await
    }

    #[cfg(feature = "indexd")]
    #[inline]
    async fn invalidate_indexd_object(
        &self,
        id: &crate::indexd::object::ObjectId,
    ) -> Result<(), std::io::Error> {
        self.as_ref().invalidate_indexd_object(id).await
    }

    #[cfg(feature = "renterd")]
    #[inline]
    async fn get_renterd_object(
        &self,
        id: &crate::renterd::object::FileId,
    ) -> Result<Option<crate::renterd::object::File>, std::io::Error> {
        self.as_ref().get_renterd_object(id).await
    }

    #[cfg(feature = "renterd")]
    #[inline]
    async fn insert_renterd_object(
        &self,
        object: crate::renterd::object::File,
    ) -> Result<(), std::io::Error> {
        self.as_ref().insert_renterd_object(object).await
    }

    #[cfg(feature = "renterd")]
    #[inline]
    async fn invalidate_renterd_object(
        &self,
        id: &crate::renterd::object::FileId,
    ) -> Result<(), std::io::Error> {
        self.as_ref().invalidate_renterd_object(id).await
    }
}

impl Cache {
    pub(crate) async fn get_object(
        &self,
        id: &ObjectId,
        backend: &Backend,
    ) -> Result<Object, crate::Error> {
        let l2 = self.0.metadata.l2.as_ref();
        self.0
            .metadata
            .l1
            .try_get_with_by_ref(id, async { retrieve_object(id, backend, l2).await })
            .await
            .map_err(|e| crate::Error::CachedError(e.to_string()))
    }

    pub(crate) async fn insert_object(
        &self,
        object: Object,
        backend: &Backend,
    ) -> Result<(), crate::Error> {
        if let Some(l2) = self.0.metadata.l2.as_ref() {
            match &object {
                #[cfg(feature = "indexd")]
                Object::Indexd { inner, .. } => match backend {
                    Backend::Indexd(indexd) => {
                        let sealed_object = inner.as_inner().seal(indexd.sdk().app_key());
                        l2.insert_indexd_object(inner.id(), sealed_object).await?;
                    }
                    _ => Err(crate::Error::BackendMismatch)?,
                },
                #[cfg(feature = "renterd")]
                Object::Renterd { inner, .. } => {
                    l2.insert_renterd_object(inner.as_ref().clone()).await?;
                }
            }
        }
        self.0.metadata.l1.insert(object.id().clone(), object).await;
        Ok(())
    }

    pub(crate) async fn invalidate_object(&self, id: &ObjectId) -> Result<(), crate::Error> {
        if let Some(l2) = self.0.metadata.l2.as_ref() {
            match id {
                #[cfg(feature = "indexd")]
                ObjectId::Indexd(id) => {
                    l2.invalidate_indexd_object(id).await?;
                }
                #[cfg(feature = "renterd")]
                ObjectId::Renterd(id) => {
                    l2.invalidate_renterd_object(id).await?;
                }
            }
        }

        self.0.metadata.l1.invalidate(id).await;
        Ok(())
    }
}

async fn retrieve_object<L2: L2MetadataCache>(
    id: &ObjectId,
    backend: &Backend,
    l2: Option<&L2>,
) -> Result<Object, crate::Error> {
    let object: Option<Object> = if let Some(l2) = l2 {
        match id {
            #[cfg(feature = "indexd")]
            ObjectId::Indexd(id) => l2
                .get_indexd_object(id)
                .await?
                .map(|sealed_object| match backend {
                    Backend::Indexd(indexd) => Ok::<_, crate::Error>(
                        crate::indexd::object::Object::from(
                            sealed_object
                                .open(indexd.sdk().app_key())
                                .map_err(crate::indexd::client::ClientError::from)?,
                        )
                        .into(),
                    ),
                    _ => Err(crate::Error::BackendMismatch)?,
                })
                .transpose()?,
            #[cfg(feature = "renterd")]
            ObjectId::Renterd(id) => l2.get_renterd_object(id).await?.map(|o| o.into()),
        }
    } else {
        None
    };

    if let Some(object) = object {
        // found in L2
        return Ok(object);
    }

    // retrieve from backend
    let object = backend.object(id).await?;

    if let Some(l2) = l2 {
        // insert into L2
        match &object {
            #[cfg(feature = "indexd")]
            Object::Indexd { inner, .. } => match backend {
                Backend::Indexd(indexd) => {
                    let sealed_object = inner.as_inner().seal(indexd.sdk().app_key());
                    l2.insert_indexd_object(inner.id(), sealed_object).await?;
                }
                _ => Err(crate::Error::BackendMismatch)?,
            },
            #[cfg(feature = "renterd")]
            Object::Renterd { inner, .. } => {
                l2.insert_renterd_object(inner.as_ref().clone()).await?;
            }
        }
    }

    Ok(object)
}
