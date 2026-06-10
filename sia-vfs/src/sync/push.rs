use crate::blob::{Blob, BlobId};
use crate::chunk::{Chunk, ChunkId};
use crate::db::{DataError, Error as DbError, Read, Transaction, TxScope, Write};
use crate::sync::Error;
use crate::vfs::directory::DirectoryKind;
use crate::vfs::entity::{EntityKey, LocalEntity};
use crate::vfs::file::FileKind;
use crate::vfs::{ROOT_INODE_ID, ReadWrite, Timestamp, Vfs, VfsId};
use elsa::FrozenVec;
use sia_io::upload::{MultiUploader, UploadError};
use std::num::NonZeroUsize;
use std::ops::Deref;
use std::time::Duration;

const BACKOFF_INITIAL_DELAY: Duration = Duration::from_secs(2);
const BACKOFF_MAX_DELAY: Duration = Duration::from_secs(60);
const BACKOFF_MULTIPLIER: f64 = 1.5;

pub struct PushTask {
    vfs: Vfs<ReadWrite>,
    max_attempts: usize,
}

enum Pending {
    Chunk(Chunk),
    Blob(Blob),
    File(LocalEntity<FileKind>),
    Dir(LocalEntity<DirectoryKind>),
}

impl Pending {
    async fn enqueue<'a>(
        &'a self,
        vfs_id: &VfsId,
        uploader: &mut MultiUploader<'a>,
    ) -> Result<(), UploadError> {
        match self {
            Self::Chunk(c) => uploader.enqueue(c.to_uploadable_object(vfs_id)).await,
            Self::Blob(b) => uploader.enqueue(b.to_uploadable_object(vfs_id)).await,
            Self::File(f) => uploader.enqueue(f.to_uploadable_object(vfs_id)).await,
            Self::Dir(d) => uploader.enqueue(d.to_uploadable_object(vfs_id)).await,
        }
    }
}

impl PushTask {
    pub(super) fn new(vfs: Vfs<ReadWrite>, max_attempts: NonZeroUsize) -> Self {
        Self {
            vfs,
            max_attempts: max_attempts.get(),
        }
    }
    pub(crate) fn vfs(&self) -> &Vfs<ReadWrite> {
        &self.vfs
    }

    pub async fn run(&mut self) -> Result<(), Error> {
        let res = self._run().await;
        // clean up
        let mut tx = self.vfs.tx_rw().await?;
        tx.clear_sync_job().await?;
        tx.commit().await?;
        res
    }

    async fn _run(&mut self) -> Result<(), Error> {
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
        Ok(())
    }

    async fn process_queue(&mut self) -> Result<(), Error> {
        let mut attempt = 0;

        loop {
            match self._process_queue().await {
                Ok(()) => return Ok(()),
                Err(err) => {
                    attempt += 1;
                    eprintln!("process_queue error (attempt {}): {:?}", attempt, err); //todo: logging

                    if attempt >= self.max_attempts {
                        return Err(Error::TooManyErrors);
                    }

                    let delay_ms = (BACKOFF_INITIAL_DELAY.as_millis() as f64
                        * BACKOFF_MULTIPLIER.powi(attempt as i32 - 1))
                    .min(BACKOFF_MAX_DELAY.as_millis() as f64);
                    tokio::time::sleep(Duration::from_millis(delay_ms as u64)).await;
                }
            }
        }
    }

    async fn next_queue_item<TX: TxScope>(
        &mut self,
        tx: &mut Transaction<TX>,
        max_estimated_size: Option<u64>,
    ) -> Result<Option<Pending>, Error>
    where
        Transaction<TX>: Write,
    {
        Ok(match tx.next_sync_item(max_estimated_size).await? {
            Some(JobItem::Chunk(id)) => self.prepare_chunk(id, tx).await?.map(Pending::Chunk),
            Some(JobItem::Blob(id)) => self.prepare_blob(id, tx).await?.map(Pending::Blob),
            Some(JobItem::Entity(key, EntityType::File)) => self
                .prepare_entity::<FileKind, _>(key, tx)
                .await?
                .map(Pending::File),
            Some(JobItem::Entity(key, EntityType::Dir)) => self
                .prepare_entity::<DirectoryKind, _>(key, tx)
                .await?
                .map(Pending::Dir),
            None => None,
        })
    }

    async fn _process_queue(&mut self) -> Result<(), Error> {
        let mut tx = self.vfs.tx_rw().await?;
        tx.reset_pending_job_items().await?;
        tx.commit().await?;

        let mut pending = FrozenVec::new();
        let mut uploader = self.vfs.sia_client().prepare_multi_upload();
        let vfs_id = self.vfs.id().clone();

        loop {
            let mut tx = self.vfs.tx_rw().await?;
            while !uploader.is_full() {
                let item = self
                    .next_queue_item(&mut tx, Some(uploader.space_remaining()))
                    .await?;

                if let Some(item) = item {
                    let r = pending.push_get(Box::new(item));
                    r.enqueue(&vfs_id, &mut uploader).await?;
                } else {
                    if uploader.is_empty() {
                        // Uploader is empty but no item fits the size budget. Take the next item
                        // regardless of size to avoid stalling on oversized items.
                        if let Some(item) = self.next_queue_item(&mut tx, None).await? {
                            let r = pending.push_get(Box::new(item));
                            r.enqueue(&vfs_id, &mut uploader).await?;
                        }
                    }
                    break;
                }
            }
            tx.commit().await?;

            if uploader.is_empty() {
                return Ok(());
            }

            let objects = uploader.process().await?;
            let items = std::mem::take(&mut pending).into_vec();
            assert_eq!(objects.len(), items.len());
            let mut tx = self.vfs.tx_rw().await?;
            for (item, object) in items.into_iter().zip(objects) {
                match item.as_ref() {
                    Pending::Chunk(chunk) => {
                        self.process_chunk(chunk, object, &mut tx).await?;
                    }
                    Pending::Blob(blob) => {
                        self.process_blob(blob.clone(), object, &mut tx).await?;
                    }
                    Pending::File(file) => {
                        self.process_entity(file.clone(), object, &mut tx).await?;
                    }
                    Pending::Dir(dir) => {
                        self.process_entity(dir.clone(), object, &mut tx).await?;
                    }
                }
            }
            tx.commit().await?;
            uploader = self.vfs.sia_client().prepare_multi_upload();
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
    async fn next_sync_item(
        &mut self,
        max_estimated_size: Option<u64>,
    ) -> Result<Option<JobItem>, DbError> {
        let max_estimated_size = max_estimated_size
            .unwrap_or(u64::MAX)
            .try_into()
            .unwrap_or(i64::MAX);

        Ok(
            match sqlx::query!(
                "SELECT id, type FROM sync_job_queue WHERE pending = 0 AND estimated_size <= ? ORDER BY estimated_size DESC LIMIT 1",
                max_estimated_size,
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

    async fn reset_pending_job_items(&mut self) -> Result<(), DbError> {
        let _ = sqlx::query!("UPDATE sync_job_queue SET pending = 0 WHERE pending != 0")
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
    use std::num::NonZeroUsize;

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

        let mut task = PushTask::new(vfs.clone(), NonZeroUsize::new(1).unwrap());
        task.run().await?;

        let file = vfs.inode_by_id(file.inode_id()).await?.unwrap();
        assert!(file.is_synced());

        let dir = vfs.inode_by_id(dir.inode_id()).await?.unwrap();
        assert!(dir.is_synced());

        Ok(())
    }
}
