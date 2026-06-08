use crate::blob::BlobId;
use crate::chunk::ChunkId;
use crate::db::{DataError, Error as DbError, Read, Transaction, TxScope, Write};
use crate::sync::Error;
use crate::vfs::directory::DirectoryKind;
use crate::vfs::entity::EntityKey;
use crate::vfs::file::FileKind;
use crate::vfs::{ROOT_INODE_ID, ReadWrite, Timestamp, Vfs};
use std::ops::Deref;

pub struct PushTask {
    vfs: Vfs<ReadWrite>,
}

impl PushTask {
    pub(crate) fn new(vfs: Vfs<ReadWrite>) -> Self {
        Self { vfs }
    }
    pub(crate) fn vfs(&self) -> &Vfs<ReadWrite> {
        &self.vfs
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
}

pub(crate) enum JobItem {
    Chunk(ChunkId),
    Blob(BlobId),
    Entity(EntityKey, EntityType),
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum EntityType {
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

    pub(crate) async fn enqueue_sync_job_item(
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
