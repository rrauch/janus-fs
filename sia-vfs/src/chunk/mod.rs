pub(crate) mod chunk_map;
mod compression;

use crate::db::{DataError, Read as DbRead, Transaction, TxScope, Write as DbWrite};
use crate::object::metadata::{Metadata, MetadataMut};
use crate::object::{ObjectCreateResult, ObjectId};
use crate::sync::push::{JobItem, PushTask};
use crate::sync::{Error as SyncError, Error, PullTask};
use crate::vfs::{Read, StorageMode, Timestamp, Vfs, VfsError, VfsId, VfsResult, Write};
use crate::{ContentId, object};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::AsyncReadExt;
use futures_util::io::Cursor;
use sia_io::Client as Sia;
use sia_io::object::{Object as SiaObject, ObjectId as SiaObjectId};
use sia_io::upload::UploadableObject;
use std::io::ErrorKind;
use std::ops::Deref;
use std::sync::Arc;

pub(crate) const METADATA_OBJECT_TYPE: &'static str = "CHUNK";
pub(crate) const METADATA_CHUNK_ID: &'static str = "CHUNK-ID";

pub type ChunkId = ContentId<Chunk>;

#[derive(Debug, Clone, PartialEq)]
pub struct Chunk {
    id: ChunkId,
    content: Bytes,
}

impl Chunk {
    pub(crate) async fn load_from_backend(
        sia_oid: &SiaObjectId,
        sia_client: &Sia,
    ) -> Result<Self, std::io::Error> {
        let dl = sia_client
            .download(sia_oid)
            .await
            .map_err(std::io::Error::other)?
            .ok_or_else(|| std::io::Error::new(ErrorKind::NotFound, "object not found"))?;

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
        let this = Self::from(buffer);

        let chunk_id = this.id().to_string();
        if metadata.get(METADATA_CHUNK_ID) != Some(chunk_id.as_str()) {
            Err(std::io::Error::other("METADATA_CHUNK_ID mismatch"))?
        }

        Ok(this)
    }

    pub fn id(&self) -> &ChunkId {
        &self.id
    }

    pub fn is_zeroed(&self) -> bool {
        let content = self.content.as_ref();
        let len = content.len();
        let ptr = content.as_ptr();
        let num_words = len / 8;
        unsafe {
            for i in 0..num_words {
                if *(ptr.add(i * 8) as *const u64) != 0 {
                    return false;
                }
            }
        }
        content[num_words * 8..].iter().all(|&b| b == 0)
    }

    pub(crate) fn to_uploadable_object(
        &self,
        vfs_id: &VfsId,
    ) -> UploadableObject<Metadata<'_>, Cursor<Bytes>> {
        let mut metadata = MetadataMut::with_vfs_template(vfs_id, METADATA_OBJECT_TYPE);
        metadata.insert(METADATA_CHUNK_ID.to_string(), self.id.to_string());

        UploadableObject::new(
            format!("/chunks/{}.chunk", self.id()),
            Cursor::new(self.content.clone()),
            Some(metadata.freeze()),
        )
    }
}

impl Deref for Chunk {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.content.as_ref()
    }
}

impl From<Bytes> for Chunk {
    fn from(value: Bytes) -> Self {
        let id = hash(value.as_ref());
        Chunk { id, content: value }
    }
}

impl From<Vec<u8>> for Chunk {
    fn from(value: Vec<u8>) -> Self {
        let bytes: Bytes = value.into();
        bytes.into()
    }
}

impl From<Arc<[u8]>> for Chunk {
    fn from(value: Arc<[u8]>) -> Self {
        let bytes = Bytes::from_owner(value);
        bytes.into()
    }
}

fn hash(content: &[u8]) -> ChunkId {
    let mut hasher = blake3::Hasher::new_derive_key("[sia-vfs]/[v0]/[chunk_id]");
    hasher.update(b"begin\nlength:");
    hasher.update(&content.len().to_be_bytes());
    hasher.update(b"\ncontent:");
    hasher.update(content);
    hasher.update(b"\nend");
    ChunkId::new_internal(hasher.finalize())
}

#[async_trait]
pub trait ChunkSource: Send + Sync + Unpin {
    async fn get_chunk(&self, chunk_id: &ChunkId) -> Result<Option<Chunk>, std::io::Error>;
}

#[async_trait]
pub trait ChunkSink: Send + Sync + Unpin {
    async fn insert_chunk(&self, chunk: Chunk) -> Result<(), std::io::Error>;
}

#[async_trait]
impl<Mode: Read + Unpin> ChunkSource for Vfs<Mode> {
    async fn get_chunk(&self, chunk_id: &ChunkId) -> Result<Option<Chunk>, std::io::Error> {
        self.cache()
            .chunk_cache()
            .try_get_with_by_ref(chunk_id, async { self._get_chunk(chunk_id).await })
            .await
            .map_err(std::io::Error::other)
    }
}

#[async_trait]
impl<Mode: Read + Write + Unpin> ChunkSink for Vfs<Mode> {
    async fn insert_chunk(&self, chunk: Chunk) -> Result<(), std::io::Error> {
        let mut tx = self.tx_rw().await.map_err(std::io::Error::other)?;
        tx.register_chunk(&chunk, StorageMode::Local(Arc::from(vec![])))
            .await
            .map_err(std::io::Error::other)?;
        tx.commit().await.map_err(std::io::Error::other)?;
        self.cache()
            .chunk_cache()
            .insert(*chunk.id(), Some(chunk))
            .await;
        Ok(())
    }
}

impl<Mode: Read> Vfs<Mode> {
    async fn _get_chunk(&self, chunk_id: &ChunkId) -> VfsResult<Option<Chunk>> {
        let mut tx = self.tx().await.map_err(std::io::Error::other)?;
        Ok(tx.chunk_by_id(chunk_id).await?)
    }
}

impl<Mode> PullTask<Mode> {
    pub(crate) async fn chunk_sync<TX: TxScope>(
        tx: &mut Transaction<TX>,
        chunk_id: &str,
        object_id: ObjectId,
    ) -> Result<(), SyncError>
    where
        Transaction<TX>: crate::db::Read + crate::db::Write,
    {
        let chunk_id = ChunkId::try_from_str(chunk_id).ok_or_else(|| SyncError::InvalidChunkId)?;
        tx.register_remote_chunk(&chunk_id, object_id)
            .await
            .map_err(VfsError::DbError)?;
        Ok(())
    }
}

impl PushTask {
    pub(crate) async fn prepare_chunk<TX: TxScope>(
        &mut self,
        chunk_id: ChunkId,
        tx: &mut Transaction<TX>,
    ) -> Result<Option<Chunk>, Error>
    where
        Transaction<TX>: crate::db::Write,
    {
        tx.mark_sync_job_chunk_pending(&chunk_id).await?;

        // double-check the chunk is still local
        if !tx.is_chunk_local(&chunk_id).await? {
            // has been synced since last check
            tx.remove_sync_job_chunk(&chunk_id).await?;
            return Ok(None);
        }

        Ok(Some(tx.chunk_by_id(&chunk_id).await?.ok_or_else(|| {
            crate::db::Error::from(DataError::ChunkNotFound(chunk_id))
        })?))
    }

    pub(crate) async fn process_chunk<TX: TxScope>(
        &mut self,
        chunk: &Chunk,
        object: SiaObject,
        tx: &mut Transaction<TX>,
    ) -> Result<(), Error>
    where
        Transaction<TX>: crate::db::Write,
    {
        let remote_location = object.id().to_string();
        let object_id = match tx
            .create_or_mark_object(remote_location.as_str(), Timestamp::now())
            .await?
        {
            ObjectCreateResult::New(oid) => oid,
            ObjectCreateResult::Existing(o) => o.id().clone(),
        };

        tx.register_remote_chunk(chunk.id(), object_id).await?;
        tx.remove_sync_job_chunk(chunk.id()).await?;
        Ok(())
    }

    pub(crate) async fn queue_chunks<TX: TxScope>(
        &mut self,
        tx: &mut Transaction<TX>,
    ) -> Result<usize, Error>
    where
        Transaction<TX>: crate::db::Write,
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
}

impl<C: TxScope> Transaction<C>
where
    Self: DbRead,
{
    pub(crate) async fn chunk_by_id(
        &mut self,
        chunk_id: &ChunkId,
    ) -> Result<Option<Chunk>, crate::db::Error> {
        let id = chunk_id.as_slice();
        let r = match sqlx::query!("SELECT mode, object_id, data FROM chunk WHERE id = ?", id)
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

        let chunk = match mode {
            StorageMode::Local(bytes) => Chunk::from(bytes),
            StorageMode::Synced(object_id) => {
                // load object from backend
                let object = self
                    .object_by_id(object_id)
                    .await?
                    .ok_or_else(|| DataError::ObjectNotFound(object_id))?;
                let sia_oid = object.try_to_sia_oid().ok_or_else(|| {
                    DataError::InvalidRemoteLocation(object.remote_location().to_string())
                })?;
                Chunk::load_from_backend(&sia_oid, self.sia_client()).await?
            }
        };

        if chunk.id() != chunk_id {
            return Err(DataError::ChunkIdMismatch {
                expected: chunk_id.clone(),
                actual: chunk.id().clone(),
            })?;
        }
        Ok(Some(chunk))
    }

    async fn is_chunk_local(&mut self, chunk_id: &ChunkId) -> Result<bool, crate::db::Error> {
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

    async fn pushable_chunk_ids(&mut self) -> Result<Vec<(ChunkId, u64)>, crate::db::Error> {
        Ok(
            sqlx::query!("SELECT id, LENGTH(data) AS \"data_len: u64\" FROM chunk WHERE mode = 'L' AND ref_count > 0")
                .fetch_all(self.conn())
                .await?
                .into_iter()
                .filter_map(|r| ChunkId::try_from_bytes(r.id).map(|c| (c, r.data_len.unwrap_or_default())))
                .collect(),
        )
    }

    pub(crate) async fn chunk_item(&mut self, id: i64) -> Result<ChunkId, crate::db::Error> {
        Ok(ChunkId::try_from_bytes(
            sqlx::query!("SELECT chunk_id FROM sync_job_queue WHERE id = ?", id)
                .fetch_one(self.conn())
                .await?
                .chunk_id
                .ok_or_else(|| DataError::ConversionError("chunk_id is missing".into()))?,
        )
        .ok_or_else(|| DataError::ConversionError("invalid chunk id".into()))?)
    }
}

impl<C: TxScope> Transaction<C>
where
    Self: DbWrite,
{
    pub(crate) async fn register_remote_chunk(
        &mut self,
        chunk_id: &ChunkId,
        object_id: ObjectId,
    ) -> Result<bool, crate::db::Error> {
        if !self._need_create_chunk(chunk_id, Some(object_id)).await? {
            return Ok(false);
        }

        let id = chunk_id.as_slice();
        let object_id = *object_id.deref() as i64;

        sqlx::query!(
            "INSERT INTO chunk (id, mode, object_id) VALUES (?, 'S', ?)",
            id,
            object_id,
        )
        .execute(self.conn())
        .await?;

        Ok(true)
    }

    async fn _need_create_chunk(
        &mut self,
        chunk_id: &ChunkId,
        object_id: Option<ObjectId>,
    ) -> Result<bool, crate::db::Error> {
        let id = chunk_id.as_slice();

        if let Some(existing_mode) = sqlx::query!("SELECT mode FROM chunk WHERE id = ?", id,)
            .map(|r| r.mode)
            .fetch_optional(self.conn())
            .await?
        {
            match (existing_mode.as_str(), object_id) {
                ("L", Some(oid)) => {
                    // Existing local, new synced: perform L->S transition
                    let new_object_id = *oid.deref() as i64;
                    sqlx::query!(
                        "UPDATE chunk SET mode = 'S', object_id = ?, data = NULL \
                            WHERE id = ?",
                        new_object_id,
                        id,
                    )
                    .execute(self.conn())
                    .await?;
                }
                _ => {
                    // Chunk exists and no transition
                }
            }
            return Ok(false);
        }
        Ok(true)
    }

    pub(crate) async fn register_chunk(
        &mut self,
        chunk: &Chunk,
        mode: StorageMode,
    ) -> Result<bool, crate::db::Error> {
        let object_id = match &mode {
            StorageMode::Synced(oid) => Some(*oid),
            _ => None,
        };

        if !self._need_create_chunk(chunk.id(), object_id).await? {
            return Ok(false);
        }

        let id = chunk.id().as_slice();

        let (mode, object_id, data) = match &mode {
            StorageMode::Local(_) => ("L", None, Some(chunk.deref())),
            StorageMode::Synced(oid) => ("S", Some(*oid.deref() as i64), None),
        };

        sqlx::query!(
            "INSERT INTO chunk (id, mode, object_id, data) VALUES (?, ?, ?, ?)",
            id,
            mode,
            object_id,
            data
        )
        .execute(self.conn())
        .await?;

        Ok(true)
    }

    async fn remove_sync_job_chunk(&mut self, chunk_id: &ChunkId) -> Result<(), crate::db::Error> {
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

    async fn mark_sync_job_chunk_pending(
        &mut self,
        chunk_id: &ChunkId,
    ) -> Result<(), crate::db::Error> {
        let chunk_id = chunk_id.as_slice();
        let affected_rows = sqlx::query!(
            "UPDATE sync_job_queue SET pending = 1 WHERE type = 'C' AND chunk_id = ? AND pending = 0",
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
