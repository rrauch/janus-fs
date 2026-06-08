use crate::object::metadata::Metadata;
use crate::object::{ObjectCreateResult, ObjectId};
use crate::sync::{Error, METADATA_VFS_VERSION};
use crate::vfs::directory::DirectoryKind;
use crate::vfs::entity::EntityHandler;
use crate::vfs::file::FileKind;
use crate::vfs::{Timestamp, Vfs, VfsResult, entity};
use crate::{blob, chunk, object};
use futures_util::{StreamExt, TryStream, TryStreamExt, stream};
use sia_io::object::Object as SiaObject;
use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::Arc;

pub struct PullTask<Mode> {
    vfs: Vfs<Mode>,
    max_concurrency: usize,
}

impl<Mode> PullTask<Mode> {
    pub(crate) fn new(vfs: Vfs<Mode>, max_concurrency: NonZeroUsize) -> Self {
        Self {
            vfs,
            max_concurrency: max_concurrency.get(),
        }
    }

    pub async fn run(&mut self) -> Result<(), Error> {
        let mut erroneous_object_ids = self.vfs.known_object_ids().await?;

        // The order in which objects are processed is important due to their internal dependency hierarchy:
        // Entities may depend on Blobs & Blobs depend on Chunks
        // Hence we have to process in order of: 1. Chunks 2. Blobs 3. Entities
        let mut chunks = vec![];
        let mut blobs = vec![];
        let mut entities = vec![];

        let mut stream = self.vfs.backend_objects().await?;
        while let Some(sia_object) = stream.try_next().await? {
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

        for group in [chunks, blobs, entities] {
            let processed = self.process_objects(group).await;
            erroneous_object_ids.retain(|oid| !processed.contains(oid));
        }

        // mark all objects we didn't see or that failed in this run as erroneous
        let mut tx = self.vfs.tx_rw().await?;
        tx.mark_object_error(erroneous_object_ids.into_iter())
            .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn process_objects(&self, sia_objects: Vec<SiaObject>) -> Vec<ObjectId> {
        stream::iter(sia_objects)
            .map(|obj| self.sync_object(obj))
            .buffer_unordered(self.max_concurrency)
            .filter_map(|res| async move {
                match res {
                    Ok(id) => Some(id),
                    Err(e) => {
                        // log::warn!("sync failed: {e:?}");
                        None
                    }
                }
            })
            .collect()
            .await
    }

    async fn sync_object(&self, sia_object: SiaObject) -> Result<ObjectId, Error> {
        let remote_location = sia_object.id().to_string();
        let mut tx = self.vfs.tx_rw().await?;
        let id = match tx
            .create_or_mark_object(&remote_location, Timestamp::now())
            .await?
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
                                    self.vfs.sia_client(),
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
                                    self.vfs.sia_client(),
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
                            Self::blob_sync(
                                &mut tx,
                                self.vfs.sia_client(),
                                blob_id,
                                &sia_object,
                                id,
                            )
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
        tx.commit().await?;
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
            .sia_client()
            .list_objects()
            .try_filter_map(move |o| {
                let vfs_id = vfs_id.clone();
                async move {
                    let metadata: Metadata = if let Some(metadata) = o.metadata().try_into().ok() {
                        metadata
                    } else {
                        return Ok(None);
                    };

                    if metadata.get(METADATA_VFS_VERSION) != Some("1") {
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
            .map_err(std::io::Error::other)
            .boxed())
    }
}

#[cfg(test)]
mod tests {
    use crate::blob::io::BlobWriter;
    use crate::blob::io::tests::MockBackend;
    use crate::blob::{Blob, BlobMut};
    use crate::chunk::Chunk;
    use crate::sync::PullTask;
    use crate::vfs::directory::DirectoryDraft;
    use crate::vfs::entity::{DraftEntity, EntityHandler};
    use crate::vfs::file::FileDraft;
    use crate::vfs::tests::new_vfs_with_opts;
    use crate::vfs::{OwnedName, ReadWrite, StorageMode, Vfs, VfsId};
    use futures_util::AsyncWriteExt;
    use std::num::NonZeroUsize;
    use std::ops::Deref;

    #[tokio::test]
    async fn pull_empty() -> anyhow::Result<()> {
        pull_test(
            |_| async move { Ok(()) },
            |vfs| async move {
                assert!(vfs.tx().await?.list_objects().await?.is_empty());
                Ok(())
            },
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn pull_single_chunk() -> anyhow::Result<()> {
        let chunk = Chunk::from(b"foo bar".to_vec());
        pull_test(
            |vfs| {
                let chunk = chunk.clone();
                async move {
                    upload_chunk_object(&vfs, &chunk).await?;
                    Ok(())
                }
            },
            |vfs| {
                let chunk = chunk.clone();
                async move {
                    let mut tx = vfs.tx().await?;
                    let objects = tx.list_objects().await?;
                    assert_eq!(objects.len(), 1);
                    let object = objects.get(0).unwrap();
                    assert_eq!(*object.id().deref(), 1);
                    assert_eq!(
                        object.remote_location(),
                        format!("mock:/chunks/{}.chunk", chunk.id())
                    );

                    let stored_chunk = tx.chunk_by_id(chunk.id()).await?.unwrap();
                    assert_eq!(stored_chunk, chunk);
                    Ok(())
                }
            },
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn pull_empty_blob() -> anyhow::Result<()> {
        let blob = BlobMut::empty().finalize();

        pull_test(
            |vfs| {
                let blob = blob.clone();
                async move {
                    upload_blob_object(&vfs, &blob).await?;
                    Ok(())
                }
            },
            |vfs| {
                let blob = blob.clone();
                async move {
                    let mut tx = vfs.tx().await?;
                    let objects = tx.list_objects().await?;
                    assert_eq!(objects.len(), 1);
                    let object = objects.get(0).unwrap();
                    assert_eq!(*object.id().deref(), 1);
                    assert_eq!(
                        object.remote_location(),
                        format!("mock:/blobs/{}.blob", blob.id())
                    );

                    let stored_blob = tx.blob_by_id(blob.id()).await?.unwrap();
                    assert_eq!(stored_blob.id(), blob.id());
                    assert_eq!(stored_blob.len(), blob.len());
                    assert_eq!(stored_blob.chunk_map(), blob.chunk_map());
                    assert_eq!(stored_blob.mode(), &StorageMode::Synced(object.id()));
                    Ok(())
                }
            },
        )
        .await?;

        Ok(())
    }

    async fn write_blob(
        input: impl AsRef<[u8]>,
        max_chunk_size: usize,
    ) -> anyhow::Result<(Blob, Vec<Chunk>)> {
        let backend = MockBackend::default();
        let mut writer = BlobWriter::new_writer(BlobMut::empty(), backend.clone(), max_chunk_size);
        writer.write_all(input.as_ref()).await?;
        let blob = writer.finalize().await?;
        let chunks = {
            backend
                .get()
                .values()
                .map(|c| c.clone())
                .collect::<Vec<_>>()
        };
        Ok((blob, chunks))
    }

    #[tokio::test]
    async fn pull_blob_ok() -> anyhow::Result<()> {
        let (blob, chunks) = write_blob(b"this is a test", 1024).await?;

        pull_test(
            |vfs| {
                let blob = blob.clone();
                let chunks = chunks.clone();
                async move {
                    for chunk in chunks {
                        upload_chunk_object(&vfs, &chunk).await?;
                    }
                    upload_blob_object(&vfs, &blob).await?;
                    Ok(())
                }
            },
            |vfs| {
                let num_chunks = chunks.len();
                async move {
                    let mut tx = vfs.tx().await?;
                    let objects = tx.list_objects().await?;
                    assert_eq!(objects.len(), num_chunks + 1);
                    Ok(())
                }
            },
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn pull_blob_err() -> anyhow::Result<()> {
        let (blob, _) = write_blob(b"this is a test", 1024).await?;

        pull_test(
            |vfs| {
                let blob = blob.clone();
                async move {
                    // only upload blob without corresponding chunks
                    upload_blob_object(&vfs, &blob).await?;
                    Ok(())
                }
            },
            |vfs| {
                let blob = blob.clone();
                async move {
                    let mut tx = vfs.tx().await?;
                    assert!(tx.list_objects().await?.is_empty());
                    assert!(tx.blob_by_id(blob.id()).await?.is_none());
                    Ok(())
                }
            },
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn pull_file() -> anyhow::Result<()> {
        let (blob, chunks) = write_blob(b"this is file content", 1024).await?;
        let file = FileDraft::new_file_draft(OwnedName::try_from("file.txt")?, blob.clone());
        pull_test(
            |vfs| {
                let blob = blob.clone();
                let chunks = chunks.clone();
                let file = file.clone();
                async move {
                    for chunk in chunks {
                        upload_chunk_object(&vfs, &chunk).await?;
                    }
                    upload_blob_object(&vfs, &blob).await?;
                    upload_entity_object(&vfs, &file).await?;
                    Ok(())
                }
            },
            |vfs| {
                let num_chunks = chunks.len();
                async move {
                    let mut tx = vfs.tx().await?;
                    let objects = tx.list_objects().await?;
                    assert_eq!(objects.len(), num_chunks + 2);
                    Ok(())
                }
            },
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn pull_dir() -> anyhow::Result<()> {
        let dir = DirectoryDraft::new_directory_draft(OwnedName::try_from("dir")?);
        pull_test(
            |vfs| {
                let dir = dir.clone();
                async move {
                    upload_entity_object(&vfs, &dir).await?;
                    Ok(())
                }
            },
            |vfs| async move {
                let mut tx = vfs.tx().await?;
                let objects = tx.list_objects().await?;
                assert_eq!(objects.len(), 1);
                Ok(())
            },
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn pull_idempotent() -> anyhow::Result<()> {
        let (blob, chunks) = write_blob(b"idempotent test", 1024).await?;
        let file = FileDraft::new_file_draft(OwnedName::try_from("file.txt")?, blob.clone());
        let expected_count = chunks.len() + 2;

        pull_test_n(
            2,
            |vfs| {
                let blob = blob.clone();
                let chunks = chunks.clone();
                let file = file.clone();
                async move {
                    for chunk in chunks {
                        upload_chunk_object(&vfs, &chunk).await?;
                    }
                    upload_blob_object(&vfs, &blob).await?;
                    upload_entity_object(&vfs, &file).await?;
                    Ok(())
                }
            },
            |vfs| async move {
                let mut tx = vfs.tx().await?;
                let objects = tx.list_objects().await?;
                assert_eq!(objects.len(), expected_count);
                Ok(())
            },
        )
        .await?;

        Ok(())
    }

    async fn pull_test<F1, Fut1, F2, Fut2>(setup: F1, assert: F2) -> anyhow::Result<()>
    where
        F1: FnOnce(Vfs<ReadWrite>) -> Fut1,
        Fut1: Future<Output = anyhow::Result<()>>,
        F2: FnOnce(Vfs<ReadWrite>) -> Fut2,
        Fut2: Future<Output = anyhow::Result<()>>,
    {
        pull_test_n(1, setup, assert).await
    }

    async fn pull_test_n<F1, Fut1, F2, Fut2>(
        runs: usize,
        setup: F1,
        assert: F2,
    ) -> anyhow::Result<()>
    where
        F1: FnOnce(Vfs<ReadWrite>) -> Fut1,
        Fut1: Future<Output = anyhow::Result<()>>,
        F2: FnOnce(Vfs<ReadWrite>) -> Fut2,
        Fut2: Future<Output = anyhow::Result<()>>,
    {
        let vfs_id = VfsId::generate();
        let (vfs, _temp_dir) = new_vfs_with_opts(Some(vfs_id), None).await?;
        let _temp_dir = _temp_dir.path().to_str().unwrap().to_string();

        setup(vfs.clone()).await?;

        let mut task = PullTask::new(vfs.clone(), NonZeroUsize::new(1).unwrap());
        for _ in 0..runs {
            task.run().await?;
        }

        assert(vfs).await?;
        Ok(())
    }

    async fn upload_entity_object<T: EntityHandler>(
        vfs: &Vfs<ReadWrite>,
        entity: &DraftEntity<T>,
    ) -> anyhow::Result<()> {
        vfs.sia_client()
            .upload(entity.to_uploadable_object(vfs.id()))
            .await?;
        Ok(())
    }

    async fn upload_blob_object(vfs: &Vfs<ReadWrite>, blob: &Blob) -> anyhow::Result<()> {
        vfs.sia_client()
            .upload(blob.to_uploadable_object(vfs.id()))
            .await?;
        Ok(())
    }

    async fn upload_chunk_object(vfs: &Vfs<ReadWrite>, chunk: &Chunk) -> anyhow::Result<()> {
        vfs.sia_client()
            .upload(chunk.to_uploadable_object(vfs.id()))
            .await?;
        Ok(())
    }
}
