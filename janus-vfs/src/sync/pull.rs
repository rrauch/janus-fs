use crate::object::metadata::Metadata;
use crate::object::{ObjectCreateResult, ObjectId};
use crate::sync::{Error, METADATA_VFS_ID, METADATA_VFS_VERSION};
use crate::vfs::directory::DirectoryKind;
use crate::vfs::entity::EntityHandler;
use crate::vfs::file::FileKind;
use crate::vfs::{Timestamp, Vfs, VfsResult, commit, config, entity};
use crate::{blob, chunk, object};
use futures_util::{StreamExt, TryStream, TryStreamExt, stream};
use janus_io::object::Object as RemoteObject;
use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::Arc;

pub struct PullTask {
    vfs: Vfs,
    max_concurrency: usize,
}

impl PullTask {
    pub(super) fn new(vfs: Vfs, max_concurrency: NonZeroUsize) -> Self {
        Self {
            vfs,
            max_concurrency: max_concurrency.get(),
        }
    }

    pub async fn run(&mut self) -> Result<(), Error> {
        tracing::info!("running pull sync task");
        let mut erroneous_object_ids = self.vfs.known_object_ids().await?;

        // The order in which objects are processed is important due to their internal dependency hierarchy:
        // Configs depend on Commits, Commits depend on Entities, Entities may depend on Blobs & Blobs depend on Chunks.
        // Hence we have to process in order of: 1. Chunks 2. Blobs 3. Entities 4. Commits 5. Configs
        let mut chunks = vec![];
        let mut blobs = vec![];
        let mut entities = vec![];
        let mut commits = vec![];
        let mut configs = vec![];

        let mut stream = self.vfs.backend_objects().await?;
        while let Some(remote_object) = stream.try_next().await? {
            let metadata: Metadata = remote_object
                .metadata()
                .try_into()
                .expect("metadata conversion to never fail");

            match metadata.get(object::METADATA_VFS_OBJECT_TYPE) {
                Some(chunk::METADATA_OBJECT_TYPE) => chunks.push(remote_object),
                Some(blob::METADATA_OBJECT_TYPE) => blobs.push(remote_object),
                Some(entity::METADATA_OBJECT_TYPE) => entities.push(remote_object),
                Some(commit::METADATA_OBJECT_TYPE) => commits.push(remote_object),
                Some(config::METADATA_OBJECT_TYPE) => configs.push(remote_object),
                _ => {} // ignore
            }
        }

        // Some entities depend on others being created first (e.g. children before
        // their parent directory), so reorder them to respect those dependencies.
        Self::sort_entities(&mut entities, self.vfs.remote_storage()).await?;

        for group in [chunks, blobs, entities, commits, configs] {
            let processed = self.process_objects(group).await;
            erroneous_object_ids.retain(|oid| !processed.contains(oid));
        }

        // mark all objects we didn't see or that failed in this run as erroneous
        if !erroneous_object_ids.is_empty() {
            let mut tx = self.vfs.tx_rw().await?;
            tx.mark_object_error(erroneous_object_ids.into_iter())
                .await?;
            tx.commit().await?;
        }
        tracing::info!("pull sync complete");
        Ok(())
    }

    async fn process_objects(&self, remote_objects: Vec<RemoteObject>) -> Vec<ObjectId> {
        stream::iter(remote_objects)
            .map(|obj| self.sync_object(obj))
            .buffer_unordered(self.max_concurrency)
            .filter_map(|res| async move {
                match res {
                    Ok(id) => Some(id),
                    Err(e) => {
                        tracing::warn!(error = %e, "sync failed");
                        None
                    }
                }
            })
            .collect()
            .await
    }

    async fn sync_object(&self, remote_object: RemoteObject) -> Result<ObjectId, Error> {
        let remote_location = remote_object.id().to_string();
        let mut tx = self.vfs.tx_rw().await?;
        let id = match tx
            .create_or_mark_object(&remote_location, Timestamp::now())
            .await?
        {
            ObjectCreateResult::Existing(existing) => existing.id(),
            ObjectCreateResult::New(id) => {
                let metadata: Metadata = remote_object
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
                                    self.vfs.remote_storage(),
                                    entity_id,
                                    rev,
                                    &remote_object,
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
                                    self.vfs.remote_storage(),
                                    entity_id,
                                    rev,
                                    &remote_object,
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
                                self.vfs.remote_storage(),
                                blob_id,
                                &remote_object,
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
                    Some(commit::METADATA_OBJECT_TYPE) => {
                        if let Some(commit_id) = metadata.get(commit::METADATA_COMMIT_ID) {
                            Self::commit_sync(
                                &mut tx,
                                self.vfs.remote_storage(),
                                commit_id,
                                &remote_object,
                                id,
                            )
                            .await?;
                        }
                    }
                    Some(config::METADATA_OBJECT_TYPE) => {
                        Self::config_sync(
                            &mut tx,
                            self.vfs.head(),
                            self.vfs.id(),
                            self.vfs.remote_storage(),
                            &remote_object,
                            id,
                        )
                        .await?;
                    }
                    None => {}
                    Some(_other) => {}
                }

                id
            }
        };
        tx.commit().await?;
        Ok(id)
    }
}

impl Vfs {
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
    ) -> VfsResult<impl TryStream<Ok = RemoteObject, Error = std::io::Error> + Unpin + '_> {
        let vfs_id = Arc::new(self.id().to_string());

        Ok(self
            .remote_storage()
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

                    if metadata.get(METADATA_VFS_ID) != Some(vfs_id.as_str()) {
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
    use crate::object::Object;
    use crate::sync::PullTask;
    use crate::vfs::commit::{Commit, CommitId, CommitMut};
    use crate::vfs::config::{Config, ConfigMut, OwnedEntry};
    use crate::vfs::directory::DirectoryDraft;
    use crate::vfs::entity::{DraftEntity, EntityHandler, EntityKey};
    use crate::vfs::file::FileDraft;
    use crate::vfs::tests::new_vfs_with_opts;
    use crate::vfs::{BranchName, OwnedName, StorageMode, Timestamp, Vfs, VfsId};
    use chrono::Utc;
    use futures_util::{AsyncWriteExt, TryStreamExt};
    use std::num::NonZeroUsize;
    use std::ops::Deref;
    use std::time::Duration;

    #[tokio::test]
    async fn pull_empty() -> anyhow::Result<()> {
        pull_test(
            |_| async move { Ok(()) },
            |vfs| async move {
                assert_eq!(vfs.tx().await?.list_objects().await?.len(), 3);
                Ok(())
            },
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn pull_commit() -> anyhow::Result<()> {
        let dir = DirectoryDraft::new_directory_draft(OwnedName::try_from("dir")?, vec![]);
        let entity_key = EntityKey::new(dir.entity_id().clone(), dir.revision().clone());

        let commit = CommitMut {
            entity_key,
            preceding_commit_id: CommitId::zeroed(),
            commit_count: 0,
            created: Timestamp::now(),
        }
        .freeze();

        pull_test(
            |vfs| {
                let commit = commit.clone();
                async move {
                    upload_entity_object(&vfs, &dir).await?;
                    upload_commit_object(&vfs, &commit).await?;
                    Ok(())
                }
            },
            |vfs| {
                let commit = commit.clone();
                async move {
                    let mut tx = vfs.tx().await?;
                    let objects = tx.list_objects().await?;
                    assert_eq!(objects.len(), 5);
                    let object = by_id(&objects, 5);

                    assert_eq!(
                        object.remote_location(),
                        format!("mock:/commits/{}.commit", commit.id())
                    );

                    let commit = commit.into_synced(object.id());
                    let stored_commit = tx.commit_by_id(commit.id()).await?.unwrap();
                    assert_eq!(stored_commit, commit);
                    Ok(())
                }
            },
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn pull_config() -> anyhow::Result<()> {
        let blob = BlobMut::empty().finalize();
        let file = FileDraft::new_file_draft(OwnedName::try_from("file")?, blob.clone());
        let file_key = EntityKey::new(file.entity_id().clone(), file.revision().clone());

        let dir = DirectoryDraft::new_directory_draft(OwnedName::try_from("dir")?, vec![file_key]);
        let dir_key = EntityKey::new(dir.entity_id().clone(), dir.revision().clone());

        let root = DirectoryDraft::new_directory_draft(OwnedName::try_from("ROOT")?, vec![dir_key]);
        let entity_key = EntityKey::new(root.entity_id().clone(), root.revision().clone());

        let commit = CommitMut {
            entity_key,
            preceding_commit_id: CommitId::zeroed(),
            commit_count: 0,
            created: Timestamp::now(),
        }
        .freeze();

        let mut config = ConfigMut::new(VfsId::generate());
        config.heads.insert(
            BranchName::default().into(),
            OwnedEntry {
                description: None,
                commit_id: commit.id().clone(),
            },
        );
        config.last_modified = (Utc::now() + Duration::from_secs(2)).into();
        config.description = Some("foo".to_string());
        let config = config.freeze();

        pull_test(
            |vfs| {
                let config = config.clone();
                let commit = commit.clone();
                async move {
                    upload_blob_object(&vfs, &blob).await?;
                    upload_entity_object(&vfs, &file).await?;
                    upload_entity_object(&vfs, &dir).await?;
                    upload_entity_object(&vfs, &root).await?;
                    upload_commit_object(&vfs, &commit).await?;
                    upload_config_object(&vfs, &config).await?;
                    Ok(())
                }
            },
            |vfs| {
                let config = config.clone();
                let commit = commit.clone();
                async move {
                    let mut tx = vfs.tx().await?;
                    let objects = tx.list_objects().await?;
                    assert_eq!(objects.len(), 9);
                    let object = by_id(&objects, 9);

                    assert_eq!(
                        object.remote_location(),
                        format!(
                            "mock:/configs/{}.config",
                            config.last_modified().to_millis()
                        )
                    );

                    assert_eq!(vfs.current_commit().await?.id(), commit.id());

                    let root = vfs.root().await?;
                    assert!(root.is_synced());
                    let entries = vfs.list(&root).await?.try_collect::<Vec<_>>().await?;
                    assert_eq!(entries.len(), 1);
                    let dir = entries.get(0).unwrap().as_directory().unwrap();
                    assert_eq!(dir.name().as_ref(), "dir");
                    assert!(dir.is_synced());
                    let entries = vfs.list(&dir).await?.try_collect::<Vec<_>>().await?;
                    assert_eq!(entries.len(), 1);
                    let file = entries.get(0).unwrap().as_file().unwrap();
                    assert_eq!(file.name().as_ref(), "file");
                    assert_eq!(file.len(), 0);
                    assert!(file.is_synced());

                    Ok(())
                }
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
                    assert_eq!(objects.len(), 4);
                    let object = by_id(&objects, 4);
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
                    assert_eq!(objects.len(), 4);
                    let object = by_id(&objects, 4);
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
                    assert_eq!(objects.len(), num_chunks + 4);
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
                    assert_eq!(tx.list_objects().await?.len(), 3);
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
                    assert_eq!(objects.len(), num_chunks + 5);
                    Ok(())
                }
            },
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn pull_dir() -> anyhow::Result<()> {
        let dir = DirectoryDraft::new_directory_draft(OwnedName::try_from("dir")?, vec![]);
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
                assert_eq!(objects.len(), 4);
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
        let expected_count = chunks.len() + 5;

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

    fn by_id(objects: &Vec<Object>, id: u64) -> &Object {
        objects.iter().find(|o| o.id().deref() == &id).unwrap()
    }

    async fn pull_test<F1, Fut1, F2, Fut2>(setup: F1, assert: F2) -> anyhow::Result<()>
    where
        F1: FnOnce(Vfs) -> Fut1,
        Fut1: Future<Output = anyhow::Result<()>>,
        F2: FnOnce(Vfs) -> Fut2,
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
        F1: FnOnce(Vfs) -> Fut1,
        Fut1: Future<Output = anyhow::Result<()>>,
        F2: FnOnce(Vfs) -> Fut2,
        Fut2: Future<Output = anyhow::Result<()>>,
    {
        let (vfs, _temp_dir) = new_vfs_with_opts(None).await?;
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
        vfs: &Vfs,
        entity: &DraftEntity<T>,
    ) -> anyhow::Result<()> {
        vfs.remote_storage()
            .upload(entity.to_uploadable_object(vfs.id()))
            .await?;
        Ok(())
    }

    async fn upload_blob_object(vfs: &Vfs, blob: &Blob) -> anyhow::Result<()> {
        vfs.remote_storage()
            .upload(blob.to_uploadable_object(vfs.id()))
            .await?;
        Ok(())
    }

    async fn upload_chunk_object(vfs: &Vfs, chunk: &Chunk) -> anyhow::Result<()> {
        vfs.remote_storage()
            .upload(chunk.to_uploadable_object(vfs.id()))
            .await?;
        Ok(())
    }

    async fn upload_commit_object(vfs: &Vfs, commit: &Commit) -> anyhow::Result<()> {
        vfs.remote_storage()
            .upload(commit.to_uploadable_object(vfs.id()))
            .await?;
        Ok(())
    }

    async fn upload_config_object(vfs: &Vfs, config: &Config) -> anyhow::Result<()> {
        vfs.remote_storage()
            .upload(config.to_uploadable_object(vfs.id()))
            .await?;
        Ok(())
    }
}
