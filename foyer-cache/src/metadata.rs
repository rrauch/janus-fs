use crate::DEFAULT_MEM_BUF_SIZE;
use crate::disk_cache::{DiskCache, Error};
use async_trait::async_trait;
use bon::bon;
use equivalent::Equivalent;
#[cfg(feature = "indexd")]
use janus_io::SealedObject;
use janus_io::cache::metadata::L2MetadataCache;
#[cfg(feature = "indexd")]
use janus_io::indexd::object::ObjectId;
#[cfg(feature = "renterd")]
use janus_io::renterd::FileKind;
#[cfg(feature = "renterd")]
use janus_io::renterd::object::ObjectShadow;
#[cfg(feature = "renterd")]
use janus_io::renterd::object::{File, FileId};
use serde::{Deserialize, Serialize};
use std::path::Path;

const COMPAT_FILE_CONTENT_TYPE: &str = "janus-io/cache/metadata";
const COMPAT_FILE_COMP_VERSION: usize = 1;

// foyer seems to have problems with types with lifetimes
// so we are using owned types only here
#[derive(PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
enum CacheKey {
    #[cfg(feature = "indexd")]
    Indexd(ObjectId),
    #[cfg(feature = "renterd")]
    Renterd(FileId),
}

#[cfg(feature = "indexd")]
impl From<ObjectId> for CacheKey {
    fn from(value: ObjectId) -> Self {
        Self::Indexd(value)
    }
}

#[cfg(feature = "renterd")]
impl From<FileId> for CacheKey {
    fn from(value: FileId) -> Self {
        Self::Renterd(value)
    }
}

#[derive(Hash)]
enum BorrowedCacheKey<'a> {
    #[cfg(feature = "indexd")]
    Indexd(&'a ObjectId),
    #[cfg(feature = "renterd")]
    Renterd(&'a FileId),
}

impl Equivalent<CacheKey> for BorrowedCacheKey<'_> {
    fn equivalent(&self, key: &CacheKey) -> bool {
        match key {
            #[cfg(feature = "indexd")]
            CacheKey::Indexd(other) => {
                if let Self::Indexd(this) = self {
                    *this == other
                } else {
                    false
                }
            }
            #[cfg(feature = "renterd")]
            CacheKey::Renterd(other) => {
                if let Self::Renterd(this) = self {
                    *this == other
                } else {
                    false
                }
            }
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
enum CacheValue {
    #[cfg(feature = "indexd")]
    Indexd(SealedObject),
    #[cfg(feature = "renterd")]
    Renterd(ObjectShadow<FileKind>),
}

#[cfg(feature = "indexd")]
impl From<SealedObject> for CacheValue {
    fn from(value: SealedObject) -> Self {
        Self::Indexd(value)
    }
}

#[cfg(feature = "renterd")]
impl From<File> for CacheValue {
    fn from(value: File) -> Self {
        Self::Renterd(value.into())
    }
}

#[cfg(feature = "indexd")]
impl TryFrom<CacheValue> for SealedObject {
    type Error = ();

    fn try_from(value: CacheValue) -> Result<Self, Self::Error> {
        match value {
            CacheValue::Indexd(o) => Ok(o),
            _ => Err(()),
        }
    }
}

#[cfg(feature = "renterd")]
impl TryFrom<CacheValue> for File {
    type Error = ();

    fn try_from(value: CacheValue) -> Result<Self, Self::Error> {
        match value {
            CacheValue::Renterd(f) => Ok(f.into()),
            _ => Err(()),
        }
    }
}

pub struct FoyerMetadataCache(DiskCache<CacheKey, CacheValue>);

#[bon]
impl FoyerMetadataCache {
    #[builder]
    pub async fn new(
        max_disk_space: u64,
        disk_path: impl AsRef<Path>,
        #[builder(default = DEFAULT_MEM_BUF_SIZE)] mem_buf: usize,
    ) -> Result<Self, Error> {
        let mut cache = DiskCache::new(
            "janus_metadata_cache",
            disk_path,
            max_disk_space,
            mem_buf,
            COMPAT_FILE_CONTENT_TYPE.to_string(),
            COMPAT_FILE_COMP_VERSION,
        )
        .await?;
        cache.init().await?;
        Ok(Self(cache))
    }
}

#[async_trait]
impl L2MetadataCache for FoyerMetadataCache {
    #[cfg(feature = "indexd")]
    async fn get_indexd_object(
        &self,
        id: &ObjectId,
    ) -> Result<Option<SealedObject>, std::io::Error> {
        self.0.get(BorrowedCacheKey::Indexd(id)).await
    }

    #[cfg(feature = "indexd")]
    async fn insert_indexd_object(
        &self,
        id: &ObjectId,
        object: SealedObject,
    ) -> Result<(), std::io::Error> {
        self.0.insert(id.clone(), object).await
    }

    #[cfg(feature = "indexd")]
    async fn invalidate_indexd_object(&self, id: &ObjectId) -> Result<(), std::io::Error> {
        self.0.invalidate(BorrowedCacheKey::Indexd(id)).await
    }

    #[cfg(feature = "renterd")]
    async fn get_renterd_object(&self, id: &FileId) -> Result<Option<File>, std::io::Error> {
        self.0.get(BorrowedCacheKey::Renterd(id)).await
    }

    #[cfg(feature = "renterd")]
    async fn insert_renterd_object(&self, object: File) -> Result<(), std::io::Error> {
        self.0.insert(object.id().clone(), object).await
    }

    #[cfg(feature = "renterd")]
    async fn invalidate_renterd_object(&self, id: &FileId) -> Result<(), std::io::Error> {
        self.0.invalidate(BorrowedCacheKey::Renterd(id)).await
    }
}
