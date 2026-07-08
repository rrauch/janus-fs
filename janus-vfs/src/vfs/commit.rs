use crate::db::{DataError, Read as DbRead, Transaction, TxScope, Write as DbWrite};
use crate::gen_flatbuffers::vfs::commit::{Commit as FlatCommit, CommitBuilder};
use crate::object::metadata::{Metadata, MetadataMut};
use crate::object::{ObjectCreateResult, ObjectId};
use crate::sync::push::{JobItem, PushTask};
use crate::sync::{Error, PullTask};
use crate::vfs::entity::{EntityId, EntityKey, Revision};
use crate::vfs::{BranchName, Head, ROOT_INODE_ID, StorageMode, Timestamp, VfsId};
use crate::{ContentId, object};
use flatbuffers::{FlatBufferBuilder, InvalidFlatbuffer};
use futures_util::AsyncReadExt;
use futures_util::io::Cursor;
use janus_io::RemoteStorage;
use janus_io::object::{Object as RemoteObject, ObjectId as RemoteObjectId};
use janus_io::upload::UploadableObject;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;

pub(crate) const METADATA_OBJECT_TYPE: &'static str = "COMMIT";
pub(crate) const METADATA_COMMIT_ID: &'static str = "COMMIT-ID";

pub struct CommitKind;
pub type CommitId = ContentId<CommitKind>;

impl FromStr for CommitId {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from_str(s).ok_or_else(|| ())
    }
}

#[derive(Debug, Error)]
pub enum CommitError {
    #[error(transparent)]
    FlatbufferError(#[from] InvalidFlatbuffer),
    #[error("invalid timestamp")]
    TimestampError,
    #[error("id mismatch: [{expected}] != [{actual}]")]
    IdMismatch {
        expected: CommitId,
        actual: CommitId,
    },
    #[error("invalid commit id")]
    InvalidCommitId,
}

pub struct CommitMut {
    pub entity_key: EntityKey,
    pub preceding_commit_id: CommitId,
    pub commit_count: u64,
    pub created: Timestamp,
}

impl CommitMut {
    pub fn freeze(self) -> Commit {
        let serialized = to_flatbuffer(
            &self.entity_key,
            &self.preceding_commit_id,
            self.commit_count,
            &self.created,
        );
        self.finalize(StorageMode::Local(Arc::from(serialized)))
    }

    fn finalize(self, mode: StorageMode) -> Commit {
        let id = hash(
            &self.entity_key,
            &self.preceding_commit_id,
            self.commit_count,
            &self.created,
        );
        Commit {
            id,
            entity_key: self.entity_key,
            preceding_commit_id: self.preceding_commit_id,
            commit_count: self.commit_count,
            created: self.created,
            mode,
        }
    }
}

fn hash(
    entity_key: &EntityKey,
    preceding_commit_id: &CommitId,
    commit_count: u64,
    created: &Timestamp,
) -> CommitId {
    let mut hasher = blake3::Hasher::new_derive_key("[janus-vfs]/[v1]/[commit]");
    hasher.update(b"begin:");
    hasher.update(b"\nentity_id:");
    hasher.update(entity_key.id().as_slice());
    hasher.update(b"\nentity_rev:");
    hasher.update(entity_key.revision().as_ref());
    hasher.update(b"\npreceding_commit_id:");
    hasher.update(preceding_commit_id.as_ref());
    hasher.update(b"\ncommit_count:\n");
    hasher.update(&commit_count.to_be_bytes());
    hasher.update(b"\ncreated:\n");
    hasher.update(&created.to_millis().to_be_bytes());
    hasher.update(b"\nend");
    CommitId::new_internal(hasher.finalize())
}

fn to_flatbuffer(
    entity_key: &EntityKey,
    preceding_commit_id: &CommitId,
    commit_count: u64,
    created: &Timestamp,
) -> Vec<u8> {
    let mut b = FlatBufferBuilder::new();
    let mut cb = CommitBuilder::new(&mut b);
    cb.add_entity_id(entity_key.id().as_flatbuffer());
    cb.add_entity_rev(entity_key.revision().as_flatbuffer());
    cb.add_created(created.to_millis());
    cb.add_count(commit_count);
    cb.add_prev_id(preceding_commit_id.as_flatbuffer());
    let commit = cb.finish();
    b.finish(commit, None);
    b.finished_data().to_vec()
}

impl From<Commit> for CommitMut {
    fn from(value: Commit) -> Self {
        Self {
            entity_key: value.entity_key,
            preceding_commit_id: value.preceding_commit_id,
            commit_count: value.commit_count,
            created: value.created,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Commit {
    id: CommitId,
    entity_key: EntityKey,
    preceding_commit_id: CommitId,
    commit_count: u64,
    created: Timestamp,
    mode: StorageMode,
}

impl Commit {
    pub fn id(&self) -> &CommitId {
        &self.id
    }

    pub fn entity(&self) -> &EntityKey {
        &self.entity_key
    }

    pub fn preceding_commit_id(&self) -> &CommitId {
        &self.preceding_commit_id
    }

    pub fn commit_count(&self) -> u64 {
        self.commit_count
    }

    pub fn created(&self) -> &Timestamp {
        &self.created
    }

    pub fn is_synced(&self) -> bool {
        match &self.mode {
            StorageMode::Synced(_) => true,
            StorageMode::Local(_) => false,
        }
    }

    pub(crate) async fn load_from_backend(
        object_id: ObjectId,
        remote_oid: &RemoteObjectId,
        remote_storage: &RemoteStorage,
    ) -> Result<Self, std::io::Error> {
        let dl = remote_storage
            .download(remote_oid)
            .await
            .map_err(std::io::Error::other)?;

        let metadata: object::metadata::Metadata = dl
            .object()
            .metadata()
            .try_into()
            .map_err(std::io::Error::other)?;

        if metadata.get(object::METADATA_VFS_OBJECT_TYPE) != Some(METADATA_OBJECT_TYPE) {
            Err(std::io::Error::other("METADATA_OBJECT_TYPE mismatch"))?
        }

        let mut buffer = Vec::with_capacity(dl.object().size() as usize);
        let mut reader = dl.open().await.map_err(std::io::Error::other)?;
        reader.read_to_end(&mut buffer).await?;
        let this = Self::try_from_flatbuffer(buffer.as_slice(), StorageMode::Synced(object_id))
            .map_err(std::io::Error::other)?;

        let commit_id = this.id.to_string();
        if metadata.get(METADATA_COMMIT_ID) != Some(commit_id.as_str()) {
            Err(std::io::Error::other("METADATA_COMMIT_ID mismatch"))?
        }
        Ok(this)
    }

    pub(crate) fn to_flatbuffer(&self) -> Vec<u8> {
        to_flatbuffer(
            &self.entity_key,
            &self.preceding_commit_id,
            self.commit_count,
            &self.created,
        )
    }

    pub(crate) fn try_from_flatbuffer(
        buffer: &[u8],
        mode: StorageMode,
    ) -> Result<Self, CommitError> {
        let flat_commit = flatbuffers::root::<FlatCommit>(buffer)?;
        let commit_mut = CommitMut {
            entity_key: EntityKey::new(
                EntityId::from_byte_ref(&flat_commit.entity_id().0).clone(),
                Revision::from_byte_ref(&flat_commit.entity_rev().0).clone(),
            ),
            preceding_commit_id: CommitId::from_byte_ref(&flat_commit.prev_id().0).clone(),
            commit_count: flat_commit.count(),
            created: Timestamp::from_millis(flat_commit.created())
                .ok_or_else(|| CommitError::TimestampError)?,
        };

        Ok(commit_mut.finalize(mode))
    }

    pub(crate) fn to_uploadable_object(
        &self,
        vfs_id: &VfsId,
    ) -> UploadableObject<Metadata<'_>, Cursor<Vec<u8>>> {
        let mut metadata = MetadataMut::with_vfs_template(vfs_id, METADATA_OBJECT_TYPE);
        metadata.insert(METADATA_COMMIT_ID.to_string(), self.id.to_string());

        UploadableObject::new(
            format!("/commits/{}.commit", self.id()),
            Cursor::new(self.to_flatbuffer()),
            Some(metadata.freeze()),
        )
    }

    pub(crate) fn into_synced(self, object_id: ObjectId) -> Self {
        let commit_mut = CommitMut::from(self);
        commit_mut.finalize(StorageMode::Synced(object_id))
    }
}

impl PullTask {
    pub(crate) async fn commit_sync<TX: TxScope>(
        tx: &mut Transaction<TX>,
        remote_storage: &RemoteStorage,
        commit_id: &str,
        remote_object: &RemoteObject,
        object_id: ObjectId,
    ) -> Result<(), Error>
    where
        Transaction<TX>: crate::db::Read + crate::db::Write,
    {
        let commit_id = CommitId::try_from_str(commit_id)
            .ok_or_else(|| Error::CommitError(CommitError::InvalidCommitId))?;
        let commit =
            Commit::load_from_backend(object_id, remote_object.id(), remote_storage).await?;

        if commit.id() != &commit_id {
            return Err(Error::CommitError(CommitError::IdMismatch {
                expected: commit_id,
                actual: commit.id,
            }));
        }

        tx.register_commit(&commit).await.map_err(Error::DbError)?;
        Ok(())
    }
}

impl PushTask {
    pub(crate) async fn prepare_commit<TX: TxScope>(
        &mut self,
        commit_id: CommitId,
        tx: &mut Transaction<TX>,
    ) -> Result<Option<Commit>, Error>
    where
        Transaction<TX>: DbWrite,
    {
        tx.mark_sync_job_commit_pending(&commit_id).await?;

        // double-check the commit is still local
        if !tx.is_commit_local(&commit_id).await? {
            // has been synced since last check
            tx.remove_sync_job_commit(&commit_id).await?;
            return Ok(None);
        }

        Ok(Some(tx.commit_by_id(&commit_id).await?.ok_or_else(
            || crate::db::Error::from(DataError::CommitNotFound(commit_id)),
        )?))
    }

    pub(crate) async fn process_commit<TX: TxScope>(
        &mut self,
        commit: Commit,
        object: RemoteObject,
        tx: &mut Transaction<TX>,
    ) -> Result<(), Error>
    where
        Transaction<TX>: DbWrite,
    {
        let remote_location = object.id().to_string();
        let object_id = match tx
            .create_or_mark_object(remote_location.as_str(), Timestamp::now())
            .await?
        {
            ObjectCreateResult::New(oid) => oid,
            ObjectCreateResult::Existing(o) => o.id().clone(),
        };

        let commit = commit.into_synced(object_id);

        tx.register_commit(&commit).await?;
        tx.remove_sync_job_commit(commit.id()).await?;
        Ok(())
    }

    pub(crate) async fn queue_commits<TX: TxScope>(
        &mut self,
        tx: &mut Transaction<TX>,
    ) -> Result<usize, Error>
    where
        Transaction<TX>: DbWrite,
    {
        let mut num_items = 0;
        for (commit_id, len) in tx.pushable_commit_ids().await? {
            let estimated_size = len + 32;
            let item = JobItem::Commit(commit_id);
            if tx.enqueue_sync_job_item(&item, estimated_size).await? {
                num_items += 1;
            }
        }

        Ok(num_items)
    }
}

impl<C: TxScope> Transaction<C>
where
    Self: DbRead,
{
    pub(crate) async fn current_commit_id(
        &mut self,
        head: impl Into<Head>,
    ) -> Result<Option<CommitId>, crate::db::Error> {
        let head = head.into();
        let name = head.name();
        let head_type = match &head {
            Head::Branch(_) => "B",
            Head::Tag(_) => "T",
        };

        Ok(sqlx::query!(
            "SELECT commit_id FROM head where name = ? and type = ?",
            name,
            head_type,
        )
        .map(|r| CommitId::try_from_bytes(r.commit_id))
        .fetch_optional(self.conn())
        .await?
        .flatten())
    }

    pub(crate) async fn commit_by_id(
        &mut self,
        commit_id: &CommitId,
    ) -> Result<Option<Commit>, crate::db::Error> {
        let id = commit_id.as_slice();

        let r = match sqlx::query!("SELECT mode, object_id, data FROM commits WHERE id = ?", id)
            .fetch_optional(self.conn())
            .await?
        {
            Some(r) => r,
            None => return Ok(None),
        };

        let mode = match r.mode.as_str() {
            "L" => StorageMode::Local(Arc::from(r.data.unwrap_or_default())),
            "S" => StorageMode::Synced(
                r.object_id
                    .map(ObjectId::from)
                    .ok_or(DataError::MissingObject)?,
            ),
            other => {
                return Err(DataError::ConversionError(
                    format!("invalid mode: {}", other).into(),
                ))?;
            }
        };

        let commit = match mode {
            StorageMode::Local(fb) => {
                Commit::try_from_flatbuffer(fb.clone().as_ref(), StorageMode::Local(fb))
                    .map_err(|e| DataError::ConversionError(e.to_string().into()))?
            }
            StorageMode::Synced(object_id) => {
                // load object from backend
                let object = self
                    .object_by_id(object_id)
                    .await?
                    .ok_or_else(|| DataError::ObjectNotFound(object_id))?;
                let remote_oid = object.try_to_remote_oid().ok_or_else(|| {
                    DataError::InvalidRemoteLocation(object.remote_location().to_string())
                })?;
                Commit::load_from_backend(object_id, &remote_oid, self.remote_storage()).await?
            }
        };

        if commit.id() != commit_id {
            return Err(DataError::CommitIdMismatch {
                expected: commit_id.clone(),
                actual: commit.id().clone(),
            })?;
        }

        Ok(Some(commit))
    }

    async fn is_commit_local(&mut self, commit_id: &CommitId) -> Result<bool, crate::db::Error> {
        let id = commit_id.as_slice();
        let mode = sqlx::query!("SELECT mode FROM commits WHERE id = ?", id)
            .map(|r| r.mode)
            .fetch_one(self.conn())
            .await?;
        Ok(match mode.as_str() {
            "L" => true,
            _ => false,
        })
    }

    async fn pushable_commit_ids(&mut self) -> Result<Vec<(CommitId, u64)>, crate::db::Error> {
        Ok(
            sqlx::query!("SELECT id, LENGTH(data) AS \"data_len: u64\" FROM commits WHERE mode = 'L' AND ref_count > 0")
                .fetch_all(self.conn())
                .await?
                .into_iter()
                .filter_map(|r| CommitId::try_from_bytes(r.id).map(|c| (c, r.data_len.unwrap_or_default())))
                .collect(),
        )
    }

    pub(crate) async fn commit_item(&mut self, id: i64) -> Result<CommitId, crate::db::Error> {
        Ok(CommitId::try_from_bytes(
            sqlx::query!("SELECT commit_id FROM sync_job_queue WHERE id = ?", id)
                .fetch_one(self.conn())
                .await?
                .commit_id
                .ok_or_else(|| DataError::ConversionError("commit_id is missing".into()))?,
        )
        .ok_or_else(|| DataError::ConversionError("invalid commit id".into()))?)
    }
}

impl<C: TxScope> Transaction<C>
where
    Self: DbWrite,
{
    pub(crate) async fn register_commit(
        &mut self,
        commit: &Commit,
    ) -> Result<(), crate::db::Error> {
        let id = commit.id().as_slice();

        let (mode, data, object_id) = match &commit.mode {
            StorageMode::Local(bytes) => ("L", Some(bytes.as_ref()), None),
            StorageMode::Synced(oid) => ("S", None, Some(*oid.deref() as i64)),
        };

        let entity_id = commit.entity_key.id().as_slice();
        let entity_rev = commit.entity_key.revision().as_slice();

        sqlx::query!(
        "INSERT INTO commits (id, entity_id, entity_rev, mode, object_id, data) VALUES (?, ?, ?, ?, ?, ?)
         ON CONFLICT(id) DO UPDATE SET
             entity_id = excluded.entity_id,
             entity_rev = excluded.entity_rev,
             mode = excluded.mode,
             object_id = excluded.object_id,
             data = excluded.data",
        id,
        entity_id,
        entity_rev,
        mode,
        object_id,
        data,
    )
            .execute(self.conn())
            .await?;

        Ok(())
    }

    pub(crate) async fn update_commit(
        &mut self,
        branch_name: BranchName,
    ) -> Result<Option<Commit>, crate::db::Error> {
        let root_id = *ROOT_INODE_ID.deref() as i64;

        let entity_key = sqlx::query!(
            "SELECT entity_id, entity_rev FROM vfs WHERE inode_id = ? AND parent IS NULL",
            root_id,
        )
        .map(|r| -> Result<EntityKey, crate::db::Error> {
            Ok(EntityKey::new(
                EntityId::try_from_bytes(r.entity_id)
                    .ok_or_else(|| DataError::ConversionError("invalid entity id".into()))?,
                Revision::try_from_bytes(r.entity_rev)
                    .ok_or_else(|| DataError::ConversionError("invalid entity revision".into()))?,
            ))
        })
        .fetch_one(self.conn())
        .await?
        .map_err(std::io::Error::other)?;

        let prev_commit_id = self
            .current_commit_id(branch_name.clone())
            .await?
            .ok_or_else(|| DataError::HeadEntryNotFound(branch_name.clone().into()))?;
        let prev_commit = self
            .commit_by_id(&prev_commit_id)
            .await?
            .ok_or_else(|| DataError::CommitNotFound(prev_commit_id))?;
        if &prev_commit.entity_key == &entity_key {
            // no material change, keep old commit;
            return Ok(None);
        }
        let mut commit = CommitMut::from(prev_commit);

        commit.commit_count += 1;
        commit.entity_key = entity_key;
        commit.created = Timestamp::now();
        commit.preceding_commit_id = prev_commit_id;

        let commit = commit.freeze();
        self.register_commit(&commit).await?;

        let name = branch_name.deref();
        let commit_id = commit.id().as_slice();
        let affected_rows = sqlx::query!(
            "UPDATE head SET commit_id = ? WHERE name = ? AND type = 'B'",
            commit_id,
            name,
        )
        .execute(self.conn())
        .await?
        .rows_affected();

        if affected_rows != 1 {
            Err(DataError::UnexpectedAffectedRows {
                expected: 1,
                actual: affected_rows,
            })?
        }
        Ok(Some(commit))
    }

    async fn remove_sync_job_commit(
        &mut self,
        commit_id: &CommitId,
    ) -> Result<(), crate::db::Error> {
        let commit_id = commit_id.as_slice();
        let affected_rows = sqlx::query!(
            "DELETE FROM sync_job_queue WHERE type = 'T' AND commit_id = ?",
            commit_id
        )
        .execute(self.conn())
        .await?
        .rows_affected();

        if affected_rows != 1 {
            Err(DataError::UnexpectedAffectedRows {
                expected: 1,
                actual: affected_rows,
            })?
        }
        Ok(())
    }

    async fn mark_sync_job_commit_pending(
        &mut self,
        commit_id: &CommitId,
    ) -> Result<(), crate::db::Error> {
        let commit_id = commit_id.as_slice();
        let affected_rows = sqlx::query!(
            "UPDATE sync_job_queue SET pending = 1 WHERE type = 'T' AND commit_id = ? AND pending = 0",
            commit_id
        )
            .execute(self.conn())
            .await?
            .rows_affected();

        if affected_rows != 1 {
            Err(DataError::UnexpectedAffectedRows {
                expected: 1,
                actual: affected_rows,
            })?
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::vfs::commit::{Commit, CommitId, CommitMut};
    use crate::vfs::entity::{EntityId, EntityKey, Revision};
    use crate::vfs::{StorageMode, Timestamp};
    use std::sync::Arc;

    #[test]
    fn flatbuffer_roundtrip() -> anyhow::Result<()> {
        let commit1 = CommitMut {
            entity_key: EntityKey::new(EntityId::generate(), Revision::zeroed()),
            preceding_commit_id: CommitId::zeroed(),
            commit_count: 0,
            created: Timestamp::now(),
        }
        .freeze();

        let fb1: Arc<[u8]> = Arc::from(commit1.to_flatbuffer());
        let de1 = Commit::try_from_flatbuffer(fb1.clone().as_ref(), StorageMode::Local(fb1))?;
        assert_eq!(de1, commit1);

        let commit2 = CommitMut {
            entity_key: commit1.entity_key.clone(),
            preceding_commit_id: commit1.id.clone(),
            commit_count: commit1.commit_count + 1,
            created: Timestamp::now(),
        }
        .freeze();

        let fb2: Arc<[u8]> = Arc::from(commit2.to_flatbuffer());
        let de2 = Commit::try_from_flatbuffer(fb2.clone().as_ref(), StorageMode::Local(fb2))?;
        assert_eq!(de2, commit2);

        Ok(())
    }
}
