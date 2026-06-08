use crate::blob::BlobId;
use crate::chunk::ChunkId;
use crate::db::{DataError, Error as DbError, Read, Transaction, TxScope, Write};
use crate::object::ObjectCreateResult;
use crate::sync::Error;
use crate::vfs::directory::DirectoryKind;
use crate::vfs::entity::{Entity, EntityHandler, EntityId, EntityKey, Revision};
use crate::vfs::file::FileKind;
use crate::vfs::{ROOT_INODE_ID, ReadWrite, StorageMode, Timestamp, Vfs};
use std::ops::Deref;

pub struct PushTask {
    vfs: Vfs<ReadWrite>,
}

impl PushTask {
    pub(crate) fn new(vfs: Vfs<ReadWrite>) -> Self {
        Self { vfs }
    }

    pub async fn run(&mut self) -> Result<(), Error> {
        let mut tx = self.vfs.tx_rw().await?;
        tx.clear_sync_job().await?;
        if !tx.has_push_items().await? {
            // nothing to do
            tx.commit().await?;
            return Ok(());
        }

        let has_items;
        if self.create_new_queue(&mut tx).await? == 0 {
            // nothing to do
            has_items = false;
            tx.clear_sync_job().await?;
        } else {
            has_items = true;
        }
        tx.commit().await?;
        if !has_items {
            // nothing left to do
            return Ok(());
        }

        self.process_queue().await?;

        // clean up
        let mut tx = self.vfs.tx_rw().await?;
        tx.clear_sync_job().await?;
        tx.commit().await?;

        Ok(())
    }

    async fn process_queue(&mut self) -> Result<(), Error> {
        loop {
            let mut tx = self.vfs.tx().await?;
            if let Err(err) = match tx.next_sync_item().await {
                Ok(Some(JobItem::Chunk(chunk_id))) => self.process_chunk(chunk_id, tx).await,
                Ok(Some(JobItem::Blob(blob_id))) => self.process_blob(blob_id, tx).await,
                Ok(Some(JobItem::Entity(key, entity_type))) => match entity_type {
                    EntityType::File => self.process_entity::<FileKind, _>(key, tx).await,
                    EntityType::Dir => self.process_entity::<DirectoryKind, _>(key, tx).await,
                },
                Ok(None) => {
                    return Ok(());
                }
                Err(err) => Err(err.into()),
            } {
                //todo: log error & back off
                eprintln!("{:?}", err);
            }
        }
    }

    async fn process_chunk<TX: TxScope>(
        &mut self,
        chunk_id: ChunkId,
        mut tx: Transaction<TX>,
    ) -> Result<(), Error>
    where
        Transaction<TX>: Read,
    {
        // double-check the chunk is still local
        if !tx.is_chunk_local(&chunk_id).await? {
            // has been synced since last check
            return Ok(());
        }

        let chunk = tx
            .chunk_by_id(&chunk_id)
            .await?
            .ok_or_else(|| DbError::from(DataError::ChunkNotFound(chunk_id)))?;
        drop(tx);

        let object = self
            .vfs
            .sia_client()
            .upload(chunk.to_uploadable_object(self.vfs.id()))
            .await?;

        let mut tx = self.vfs.tx_rw().await?;
        let remote_location = object.id().to_string();
        let object_id = match tx
            .create_or_mark_object(remote_location.as_str(), Timestamp::now())
            .await?
        {
            ObjectCreateResult::New(oid) => oid,
            ObjectCreateResult::Existing(o) => o.id().clone(),
        };

        tx.register_remote_chunk(&chunk_id, object_id).await?;
        tx.remove_sync_job_chunk(&chunk_id).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn process_blob<TX: TxScope>(
        &mut self,
        blob_id: BlobId,
        mut tx: Transaction<TX>,
    ) -> Result<(), Error>
    where
        Transaction<TX>: Read,
    {
        let blob = tx
            .blob_by_id(&blob_id)
            .await?
            .ok_or_else(|| DbError::from(DataError::BlobNotFound(blob_id)))?;

        if let StorageMode::Synced(_) = &blob.mode() {
            // no need to sync
            return Ok(());
        }
        drop(tx);

        let object = self
            .vfs
            .sia_client()
            .upload(blob.to_uploadable_object(self.vfs.id()))
            .await?;

        let mut tx = self.vfs.tx_rw().await?;
        let remote_location = object.id().to_string();
        let object_id = match tx
            .create_or_mark_object(remote_location.as_str(), Timestamp::now())
            .await?
        {
            ObjectCreateResult::New(oid) => oid,
            ObjectCreateResult::Existing(o) => o.id().clone(),
        };

        let blob = blob.into_synced(object_id);
        tx.register_blob(&blob).await?;
        tx.remove_sync_job_blob(blob.id()).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn process_entity<E: EntityHandler, TX: TxScope>(
        &mut self,
        entity_key: EntityKey,
        mut tx: Transaction<TX>,
    ) -> Result<(), Error>
    where
        Transaction<TX>: Read,
    {
        let entity = match tx
            .entity_by_key::<E>(&entity_key)
            .await?
            .ok_or_else(|| DbError::from(DataError::EntityNotFound(entity_key)))?
        {
            Entity::Synced(_) => {
                // already synced
                return Ok(());
            }
            Entity::Local(entity) => entity,
        };
        drop(tx);

        let object = self
            .vfs
            .sia_client()
            .upload(entity.to_uploadable_object(self.vfs.id()))
            .await?;

        let mut tx = self.vfs.tx_rw().await?;
        let remote_location = object.id().to_string();
        let object_id = match tx
            .create_or_mark_object(remote_location.as_str(), Timestamp::now())
            .await?
        {
            ObjectCreateResult::New(oid) => oid,
            ObjectCreateResult::Existing(o) => o.id().clone(),
        };

        let entity = entity.into_synced(object_id);
        tx.register_entity(entity).await?;
        tx.remove_sync_job_entity(&entity_key).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn create_new_queue<TX: TxScope>(
        &mut self,
        tx: &mut Transaction<TX>,
    ) -> Result<usize, Error>
    where
        Transaction<TX>: Write,
    {
        let mut num_items = 0;
        tx.create_sync_job().await?;

        num_items += self.queue_chunks(tx).await?;
        num_items += self.queue_blobs(tx).await?;
        num_items += self.queue_entities(tx).await?;

        Ok(num_items)
    }

    async fn queue_chunks<TX: TxScope>(&mut self, tx: &mut Transaction<TX>) -> Result<usize, Error>
    where
        Transaction<TX>: Write,
    {
        let mut num_items = 0;
        for (chunk_id, len) in tx.pushable_chunk_ids().await? {
            let estimated_size = len + 32;
            let item = JobItem::Chunk(chunk_id);
            if tx.enqueue_sync_job_item(&item, estimated_size).await? {
                num_items += 1;
            }
        }

        Ok(num_items)
    }

    async fn queue_blobs<TX: TxScope>(&mut self, tx: &mut Transaction<TX>) -> Result<usize, Error>
    where
        Transaction<TX>: Write,
    {
        let mut num_items = 0;
        for blob_id in tx.pushable_blob_ids().await? {
            let blob = match tx.blob_by_id(&blob_id).await? {
                Some(blob) => blob,
                None => continue,
            };
            let fb = blob.to_flatbuffer();
            let estimated_size = fb.len() as u64 + 32;
            let item = JobItem::Blob(blob_id);
            if tx.enqueue_sync_job_item(&item, estimated_size).await? {
                num_items += 1;
            }
        }

        Ok(num_items)
    }

    async fn queue_entities<TX: TxScope>(
        &mut self,
        tx: &mut Transaction<TX>,
    ) -> Result<usize, Error>
    where
        Transaction<TX>: Write,
    {
        let mut num_items = 0;
        for (key, entity_type) in tx.pushable_entity_keys().await? {
            let fb = if entity_type == EntityType::File {
                let file = match tx.entity_by_key::<FileKind>(&key).await? {
                    Some(Entity::Local(file)) => file,
                    _ => continue,
                };
                file.to_flatbuffer()
            } else if entity_type == EntityType::Dir {
                let dir = match tx.entity_by_key::<DirectoryKind>(&key).await? {
                    Some(Entity::Local(dir)) => dir,
                    _ => continue,
                };
                dir.to_flatbuffer()
            } else {
                continue;
            };

            let estimated_size = fb.len() as u64 + 32;
            let item = JobItem::Entity(key, entity_type);
            if tx.enqueue_sync_job_item(&item, estimated_size).await? {
                num_items += 1;
            }
        }

        Ok(num_items)
    }
}

enum JobItem {
    Chunk(ChunkId),
    Blob(BlobId),
    Entity(EntityKey, EntityType),
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum EntityType {
    File,
    Dir,
}

impl<C: TxScope> Transaction<C>
where
    Self: Read,
{
    async fn next_sync_item(&mut self) -> Result<Option<JobItem>, DbError> {
        Ok(
            match sqlx::query!(
                "SELECT id, type FROM sync_job_queue ORDER BY estimated_size DESC LIMIT 1"
            )
            .fetch_optional(self.conn())
            .await?
            {
                None => None,
                Some(r) => {
                    let id = r.id;
                    Some(match r.r#type.as_str() {
                        "C" => JobItem::Chunk(self.chunk_item(id).await?),
                        "B" => JobItem::Blob(self.blob_item(id).await?),
                        "E" => {
                            let (entity_key, entity_type) = self.entity_item(id).await?;
                            JobItem::Entity(entity_key, entity_type)
                        }
                        _ => return Err(DataError::ConversionError("invalid job item".into()))?,
                    })
                }
            },
        )
    }

    async fn is_chunk_local(&mut self, chunk_id: &ChunkId) -> Result<bool, DbError> {
        let id = chunk_id.as_slice();
        let mode = sqlx::query!("SELECT mode FROM chunk WHERE id = ?", id)
            .map(|r| r.mode)
            .fetch_one(self.conn())
            .await?;
        Ok(match mode.as_str() {
            "L" => true,
            _ => false,
        })
    }

    async fn chunk_item(&mut self, id: i64) -> Result<ChunkId, DbError> {
        Ok(ChunkId::try_from_bytes(
            sqlx::query!("SELECT chunk_id FROM sync_job_queue WHERE id = ?", id)
                .fetch_one(self.conn())
                .await?
                .chunk_id
                .ok_or_else(|| DataError::ConversionError("chunk_id is missing".into()))?,
        )
        .ok_or_else(|| DataError::ConversionError("invalid chunk id".into()))?)
    }

    async fn blob_item(&mut self, id: i64) -> Result<BlobId, DbError> {
        Ok(BlobId::try_from_bytes(
            sqlx::query!("SELECT blob_id FROM sync_job_queue WHERE id = ?", id)
                .fetch_one(self.conn())
                .await?
                .blob_id
                .ok_or_else(|| DataError::ConversionError("blob_id is missing".into()))?,
        )
        .ok_or_else(|| DataError::ConversionError("invalid blob id".into()))?)
    }

    async fn entity_item(&mut self, id: i64) -> Result<(EntityKey, EntityType), DbError> {
        let r = sqlx::query!(
            "SELECT q.entity_id, q.entity_rev, e.entity_type \
                 FROM sync_job_queue q \
                 JOIN entity e ON e.id = q.entity_id AND e.revision = q.entity_rev \
                 WHERE q.id = ?",
            id
        )
        .fetch_one(self.conn())
        .await?;

        let entity_id = r
            .entity_id
            .and_then(EntityId::try_from_bytes)
            .ok_or_else(|| DataError::ConversionError("entity_id missing or invalid".into()))?;
        let revision = r
            .entity_rev
            .and_then(Revision::try_from_bytes)
            .ok_or_else(|| DataError::ConversionError("entity_rev missing or invalid".into()))?;

        let entity_type = match r.entity_type.as_str() {
            <FileKind as EntityHandler>::DB_TYPE => EntityType::File,
            <DirectoryKind as EntityHandler>::DB_TYPE => EntityType::Dir,
            _ => Err(DataError::ConversionError("invalid entity type".into()))?,
        };

        Ok((EntityKey::new(entity_id, revision), entity_type))
    }

    async fn has_push_items(&mut self) -> Result<bool, DbError> {
        Ok(sqlx::query!(
            "SELECT EXISTS (
    SELECT 1 FROM entity WHERE mode = 'L' AND ref_count > 0
    UNION ALL
    SELECT 1 FROM blob   WHERE mode = 'L' AND ref_count > 0
    UNION ALL
    SELECT 1 FROM chunk  WHERE mode = 'L' AND ref_count > 0
    LIMIT 1
          ) AS \"has_items: bool\""
        )
        .map(|r| r.has_items)
        .fetch_one(self.conn())
        .await?)
    }

    async fn pushable_chunk_ids(&mut self) -> Result<Vec<(ChunkId, u64)>, DbError> {
        Ok(
            sqlx::query!("SELECT id, LENGTH(data) AS \"data_len: u64\" FROM chunk WHERE mode = 'L' AND ref_count > 0")
                .fetch_all(self.conn())
                .await?
                .into_iter()
                .filter_map(|r| ChunkId::try_from_bytes(r.id).map(|c| (c, r.data_len.unwrap_or_default())))
                .collect(),
        )
    }

    async fn pushable_blob_ids(&mut self) -> Result<Vec<BlobId>, DbError> {
        Ok(
            sqlx::query!("SELECT id FROM blob WHERE mode = 'L' AND ref_count > 0")
                .fetch_all(self.conn())
                .await?
                .into_iter()
                .filter_map(|r| BlobId::try_from_bytes(r.id))
                .collect(),
        )
    }

    async fn pushable_entity_keys(&mut self) -> Result<Vec<(EntityKey, EntityType)>, DbError> {
        Ok(sqlx::query!(
            "SELECT id, revision, entity_type FROM entity WHERE mode = 'L' AND ref_count > 0"
        )
        .fetch_all(self.conn())
        .await?
        .into_iter()
        .filter_map(|r| {
            EntityId::try_from_bytes(r.id)
                .map(|id| {
                    Revision::try_from_bytes(r.revision)
                        .map(|rev| (EntityKey::new(id, rev), r.entity_type))
                })
                .flatten()
                .map(|(key, s)| match s.as_str() {
                    <FileKind as EntityHandler>::DB_TYPE => Some((key, EntityType::File)),
                    <DirectoryKind as EntityHandler>::DB_TYPE => Some((key, EntityType::Dir)),
                    _ => None,
                })
                .flatten()
        })
        .collect())
    }
}

impl<C: TxScope> Transaction<C>
where
    Self: Write,
{
    async fn clear_sync_job(&mut self) -> Result<(), DbError> {
        let _ = sqlx::query!("DELETE FROM sync_job")
            .execute(self.conn())
            .await?;
        Ok(())
    }

    async fn create_sync_job(&mut self) -> Result<(), DbError> {
        let root_id = *ROOT_INODE_ID.deref() as i64;

        let r = sqlx::query!(
            "SELECT entity_id, entity_rev FROM vfs WHERE inode_id = ?",
            root_id
        )
        .fetch_one(self.conn())
        .await?;
        let (root_entity_id, root_entity_rev) = (r.entity_id.as_slice(), r.entity_rev.as_slice());
        let created = Timestamp::now().to_millis();

        sqlx::query!(
            "INSERT INTO sync_job (created, root_entity_id, root_entity_rev) VALUES (?, ?, ?)",
            created,
            root_entity_id,
            root_entity_rev,
        )
        .execute(self.conn())
        .await?;

        Ok(())
    }

    async fn enqueue_sync_job_item(
        &mut self,
        item: &JobItem,
        estimated_size: u64,
    ) -> Result<bool, DbError> {
        let (item_type, blob_id, chunk_id, entity_id, entity_rev) = match item {
            JobItem::Chunk(chunk_id) => ("C", None, Some(chunk_id.as_slice()), None, None),
            JobItem::Blob(blob_id) => ("B", Some(blob_id.as_slice()), None, None, None),
            JobItem::Entity(key, _) => (
                "E",
                None,
                None,
                Some(key.id().as_slice()),
                Some(key.revision().as_slice()),
            ),
        };
        let estimated_size = estimated_size as i64;

        let inserted = sqlx::query!(
            "INSERT OR IGNORE INTO sync_job_queue (type, blob_id, chunk_id, entity_id, entity_rev, estimated_size) VALUES (?, ?, ?, ?, ?, ?)",
            item_type,
            blob_id,
            chunk_id,
            entity_id,
            entity_rev,
            estimated_size
        ).execute(self.conn()).await?.rows_affected() > 0;

        Ok(inserted)
    }

    async fn remove_sync_job_entity(&mut self, entity_key: &EntityKey) -> Result<(), DbError> {
        let entity_id = entity_key.id().as_slice();
        let entity_rev = entity_key.revision().as_slice();
        let affected_rows = sqlx::query!(
            "DELETE FROM sync_job_queue WHERE type = 'E' AND entity_id = ? AND entity_rev = ?",
            entity_id,
            entity_rev
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

    async fn remove_sync_job_blob(&mut self, blob_id: &BlobId) -> Result<(), DbError> {
        let blob_id = blob_id.as_slice();
        let affected_rows = sqlx::query!(
            "DELETE FROM sync_job_queue WHERE type = 'B' AND blob_id = ?",
            blob_id
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

    async fn remove_sync_job_chunk(&mut self, chunk_id: &ChunkId) -> Result<(), DbError> {
        let chunk_id = chunk_id.as_slice();
        let affected_rows = sqlx::query!(
            "DELETE FROM sync_job_queue WHERE type = 'C' AND chunk_id = ?",
            chunk_id
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
    use crate::sync::push::PushTask;
    use crate::vfs::tests::new_vfs;
    use futures_util::AsyncWriteExt;

    #[tokio::test]
    async fn basic_push() -> anyhow::Result<()> {
        let (vfs, _temp_dir) = new_vfs().await?;
        let _temp_dir = _temp_dir.path().to_str().unwrap().to_string();

        let dir = vfs
            .create_dir(&vfs.root().await?, "dir1".try_into()?)
            .await?;
        assert!(!dir.is_synced());

        let file = vfs.create_file(&dir, "file1".try_into()?).await?;
        assert!(!file.is_synced());
        let mut fh = vfs.open_rw(&file).await?;
        fh.write_all(b"This is a test").await?;
        fh.commit().await?;

        let mut task = PushTask::new(vfs.clone());
        task.run().await?;

        let file = vfs.inode_by_id(file.inode_id()).await?.unwrap();
        assert!(file.is_synced());

        let dir = vfs.inode_by_id(dir.inode_id()).await?.unwrap();
        assert!(dir.is_synced());

        Ok(())
    }
}
