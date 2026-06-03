use crate::blob::BlobError;
use crate::object::metadata::Metadata;
use crate::object::{ObjectCreateResult, ObjectId};
use crate::vfs::directory::DirectoryKind;
use crate::vfs::entity::{EntityError, EntityHandler};
use crate::vfs::file::FileKind;
use crate::vfs::{Backend, Timestamp, Vfs, VfsError, VfsResult, entity};
use crate::{blob, chunk, object};
use futures_util::{StreamExt, TryStream, TryStreamExt};
use sia_io::object::Object as SiaObject;
use std::collections::HashSet;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    VfsError(#[from] VfsError),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    SiaError(#[from] sia_io::Error),
    #[error(transparent)]
    EntityError(#[from] EntityError),
    #[error("chunk_id invalid")]
    InvalidChunkId,
    #[error(transparent)]
    BlobError(#[from] BlobError),
}

pub struct SyncTask<Mode> {
    vfs: Vfs<Mode>,
}

impl<Mode> SyncTask<Mode> {
    pub async fn run(&mut self) -> Result<(), Error> {
        let mut erroneous_object_ids = self.vfs.known_object_ids().await?;

        // The order in which objects are processed is important due to their internal dependency hierarchy:
        // Entities may depend on Blobs & Blobs depend on Chunks
        // Hence we have to process in order of: 1. Chunks 2. Blobs 3. Entities
        let mut chunks = vec![];
        let mut blobs = vec![];
        let mut entities = vec![];

        let mut stream = self.vfs.backend_objects().await?;
        while let Some(sia_object) = stream.try_next().await.map_err(VfsError::IoError)? {
            let metadata: Metadata = sia_object
                .metadata()
                .try_into()
                .expect("metadata conversion to never fail");

            match metadata.get(object::METADATA_VFS_OBJECT_TYPE) {
                Some(chunk::METADATA_OBJECT_TYPE) => chunks.push(sia_object),
                Some(blob::METADATA_OBJECT_TYPE) => blobs.push(sia_object),
                Some(entity::METADATA_OBJECT_TYPE) => entities.push(sia_object),
                _ => {} // ignore
            }
        }

        for sia_object in chunks.into_iter().chain(blobs).chain(entities) {
            match self.sync_object(sia_object).await {
                Ok(id) => {
                    erroneous_object_ids.remove(&id);
                }
                Err(_) => {
                    //todo: log this error
                }
            }
        }

        // mark all objects we didn't see or that failed in this run as erroneous
        let mut tx = self.vfs.tx_rw().await?;
        tx.mark_object_error(erroneous_object_ids.into_iter())
            .await
            .map_err(VfsError::DbError)?;
        tx.commit().await.map_err(VfsError::DbError)?;
        Ok(())
    }

    async fn sync_object(&self, sia_object: SiaObject) -> Result<ObjectId, Error> {
        let remote_location = sia_object.id().to_string();
        let mut tx = self.vfs.tx_rw().await?;
        let id = match tx
            .create_or_mark_object(&remote_location, Timestamp::now())
            .await
            .map_err(VfsError::DbError)?
        {
            ObjectCreateResult::Existing(existing) => existing.id(),
            ObjectCreateResult::New(id) => {
                let metadata: Metadata = sia_object
                    .metadata()
                    .try_into()
                    .expect("metadata conversion to never fail");

                match metadata.get(object::METADATA_VFS_OBJECT_TYPE) {
                    Some(entity::METADATA_OBJECT_TYPE) => {
                        match (
                            metadata.get(entity::METADATA_ENTITY_ID),
                            metadata.get(entity::METADATA_ENTITY_REVISION),
                            metadata.get(entity::METADATA_ENTITY_TYPE),
                        ) {
                            (
                                Some(entity_id),
                                Some(rev),
                                Some(<FileKind as EntityHandler>::METADATA_TYPE),
                            ) => {
                                Self::entity_sync::<FileKind, _>(
                                    &mut tx,
                                    self.vfs.backend(),
                                    entity_id,
                                    rev,
                                    &sia_object,
                                    id,
                                )
                                .await?;
                            }
                            (
                                Some(entity_id),
                                Some(rev),
                                Some(<DirectoryKind as EntityHandler>::METADATA_TYPE),
                            ) => {
                                Self::entity_sync::<DirectoryKind, _>(
                                    &mut tx,
                                    self.vfs.backend(),
                                    entity_id,
                                    rev,
                                    &sia_object,
                                    id,
                                )
                                .await?;
                            }
                            _ => {}
                        }
                    }
                    Some(blob::METADATA_OBJECT_TYPE) => {
                        if let Some(blob_id) = metadata.get(blob::METADATA_BLOB_ID) {
                            Self::blob_sync(&mut tx, self.vfs.backend(), blob_id, &sia_object, id)
                                .await?;
                        }
                    }
                    Some(chunk::METADATA_OBJECT_TYPE) => {
                        if let Some(chunk_id) = metadata.get(chunk::METADATA_CHUNK_ID) {
                            Self::chunk_sync(&mut tx, chunk_id, id).await?;
                        }
                    }
                    None => {}
                    Some(other) => {}
                }

                id
            }
        };
        tx.commit().await.map_err(VfsError::DbError)?;
        Ok(id)
    }
}

impl<Mode> Vfs<Mode> {
    async fn known_object_ids(&self) -> VfsResult<HashSet<ObjectId>> {
        let mut tx = self.tx().await?;
        Ok(tx
            .list_objects()
            .await?
            .into_iter()
            .map(|o| o.id())
            .collect())
    }

    async fn backend_objects(
        &self,
    ) -> VfsResult<impl TryStream<Ok = SiaObject, Error = std::io::Error> + Unpin + '_> {
        let vfs_id = Arc::new(self.id().to_string());

        Ok(self
            .backend()
            .list_objects()
            .await
            .try_filter_map(move |o| {
                let vfs_id = vfs_id.clone();
                async move {
                    let metadata: Metadata = if let Some(metadata) = o.metadata().try_into().ok() {
                        metadata
                    } else {
                        return Ok(None);
                    };

                    if metadata.get("SIA-VFS") != Some("1") {
                        // not a known / supported object
                        return Ok(None);
                    }

                    if metadata.get("VFS-ID") != Some(vfs_id.as_str()) {
                        // not the same vfs
                        return Ok(None);
                    }

                    Ok(Some(o))
                }
            })
            .boxed())
    }
}
