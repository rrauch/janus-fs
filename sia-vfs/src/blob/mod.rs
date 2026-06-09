pub mod io;

use crate::chunk::ChunkId;
use crate::chunk::chunk_map::ChunkMap;
use crate::db::{DataError, Read as DbRead, Transaction, TxScope, Write as DbWrite, Write};
use crate::gen_flatbuffers::vfs::blob::{Blob as FlatBlob, BlobBuilder, Chunk as FlatChunk};
use crate::object::metadata::{Metadata, MetadataMut};
use crate::object::{ObjectCreateResult, ObjectId};
use crate::sync::push::{JobItem, PushTask};
use crate::sync::{Error as SyncError, Error, PullTask};
use crate::vfs::{Read, StorageMode, Timestamp, Vfs, VfsError, VfsId, VfsResult};
use crate::{ContentId, object};
use flatbuffers::{FlatBufferBuilder, InvalidFlatbuffer};
use futures_util::AsyncReadExt;
use futures_util::io::Cursor;
use sia_io::Client as Sia;
use sia_io::object::Object as SiaObject;
use sia_io::object::ObjectId as SiaObjectId;
use sia_io::upload::UploadableObject;
use std::io::ErrorKind;
use std::ops::Deref;
use std::sync::Arc;
use thiserror::Error;

pub(crate) const METADATA_OBJECT_TYPE: &'static str = "BLOB";
pub(crate) const METADATA_BLOB_ID: &'static str = "BLOB-ID";

pub type BlobId = ContentId<Blob>;

#[derive(Debug, Error)]
pub enum BlobError {
    #[error(transparent)]
    FlatbufferError(#[from] InvalidFlatbuffer),
    #[error("id mismatch: [{expected}] != [{actual}]")]
    IdMismatch { expected: BlobId, actual: BlobId },
    #[error("invalid blob id")]
    InvalidBlobId,
}

#[derive(Debug, Clone)]
pub struct Blob {
    id: BlobId,
    mode: StorageMode,
    chunk_map: ChunkMap,
}

impl Blob {
    pub(crate) async fn load_from_backend(
        object_id: ObjectId,
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
        let this = Self::try_from_flatbuffer(buffer.as_slice(), StorageMode::Synced(object_id))
            .map_err(std::io::Error::other)?;

        let blob_id = this.id().to_string();
        if metadata.get(METADATA_BLOB_ID) != Some(blob_id.as_str()) {
            Err(std::io::Error::other("METADATA_BLOB_ID mismatch"))?
        }
        Ok(this)
    }

    pub(crate) fn try_from_flatbuffer(buffer: &[u8], mode: StorageMode) -> Result<Self, BlobError> {
        let flat_blob = flatbuffers::root::<FlatBlob>(buffer)?;
        let mut chunk_map = ChunkMap::with_len(flat_blob.len());
        flat_blob.chunks().unwrap_or_default().iter().for_each(|c| {
            let chunk_id = ChunkId::from_byte_ref(&c.chunk_id().0);
            chunk_map.insert(
                c.offset(),
                c.len(),
                chunk_id.clone(),
                c.chunk_offset() as usize,
            );
        });
        Ok(BlobMut::from_chunk_map(chunk_map, mode).finalize())
    }

    pub(crate) fn to_flatbuffer(&self) -> Vec<u8> {
        let mut b = FlatBufferBuilder::new();

        let chunks: Vec<_> = self
            .chunk_map
            .iter()
            .map(|r| {
                let content_id = r.chunk_id.as_flatbuffer();
                FlatChunk::new(r.offset, r.len, content_id, r.chunk_offset as u64)
            })
            .collect();

        let chunks = b.create_vector(&chunks);

        let mut bb = BlobBuilder::new(&mut b);
        bb.add_len(self.len());
        bb.add_chunks(chunks);
        let blob = bb.finish();
        b.finish(blob, None);
        b.finished_data().to_vec()
    }

    pub(crate) fn to_uploadable_object(
        &self,
        vfs_id: &VfsId,
    ) -> UploadableObject<Metadata<'_>, Cursor<Vec<u8>>> {
        let mut metadata = MetadataMut::with_vfs_template(vfs_id, METADATA_OBJECT_TYPE);
        metadata.insert(METADATA_BLOB_ID.to_string(), self.id.to_string());

        UploadableObject::new(
            format!("/blobs/{}.blob", self.id()),
            Cursor::new(self.to_flatbuffer()),
            Some(metadata.freeze()),
        )
    }

    pub fn id(&self) -> &BlobId {
        &self.id
    }

    pub fn len(&self) -> u64 {
        self.chunk_map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.chunk_map.is_empty()
    }

    pub fn into_mut(self) -> BlobMut {
        self.into()
    }

    pub(crate) fn mode(&self) -> &StorageMode {
        &self.mode
    }

    pub(crate) fn chunk_map(&self) -> &ChunkMap {
        &self.chunk_map
    }
    pub(crate) fn into_synced(self, object_id: ObjectId) -> Blob {
        let mut blob_mut = BlobMut::from(self);
        blob_mut.mode = StorageMode::Synced(object_id);
        blob_mut.finalize()
    }
}

impl From<BlobMut> for Blob {
    fn from(value: BlobMut) -> Self {
        let chunk_map = value.chunk_map;
        let id = hash(&chunk_map);
        Self {
            id,
            chunk_map,
            mode: value.mode,
        }
    }
}

impl From<Blob> for BlobMut {
    fn from(value: Blob) -> Self {
        Self {
            chunk_map: value.chunk_map,
            mode: value.mode,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BlobMut {
    chunk_map: ChunkMap,
    mode: StorageMode,
}

impl BlobMut {
    pub fn empty() -> Self {
        Self {
            chunk_map: ChunkMap::new(),
            mode: StorageMode::Local(Arc::from(vec![])),
        }
    }

    pub fn from_chunk_map(chunk_map: ChunkMap, mode: StorageMode) -> Self {
        Self { chunk_map, mode }
    }

    pub fn len(&self) -> u64 {
        self.chunk_map.len()
    }

    pub fn set_len(&mut self, new_len: u64) {
        self.chunk_map.set_len(new_len)
    }

    pub fn finalize(self) -> Blob {
        self.into()
    }
}

fn hash(chunk_map: &ChunkMap) -> BlobId {
    let mut hasher = blake3::Hasher::new_derive_key("[sia-vfs]/[v0]/[blob_id]");
    chunk_map.hash(&mut hasher);
    BlobId::new_internal(hasher.finalize())
}

impl<Mode: Read> Vfs<Mode> {
    pub(crate) async fn blob_by_id(&self, blob_id: &BlobId) -> VfsResult<Option<Blob>> {
        Ok(self.tx().await?.blob_by_id(blob_id).await?)
    }
}

impl<Mode> PullTask<Mode> {
    pub(crate) async fn blob_sync<TX: TxScope>(
        tx: &mut Transaction<TX>,
        sia_client: &Sia,
        blob_id: &str,
        sia_object: &SiaObject,
        object_id: ObjectId,
    ) -> Result<(), SyncError>
    where
        Transaction<TX>: crate::db::Read + crate::db::Write,
    {
        let blob_id = BlobId::try_from_str(blob_id).ok_or_else(|| BlobError::InvalidBlobId)?;

        let blob = Blob::load_from_backend(object_id, sia_object.id(), sia_client).await?;

        tx.register_blob(&blob).await.map_err(VfsError::DbError)?;

        if blob.id() != &blob_id {
            return Err(BlobError::IdMismatch {
                expected: blob_id,
                actual: blob.id().clone(),
            })?;
        }

        Ok(())
    }
}

impl PushTask {
    pub(crate) async fn process_blob<TX: TxScope>(
        &mut self,
        blob_id: BlobId,
        mut tx: Transaction<TX>,
    ) -> Result<(), Error>
    where
        Transaction<TX>: crate::db::Read,
    {
        let blob = tx
            .blob_by_id(&blob_id)
            .await?
            .ok_or_else(|| crate::db::Error::from(DataError::BlobNotFound(blob_id)))?;

        if let StorageMode::Synced(_) = &blob.mode() {
            // no need to sync
            return Ok(());
        }
        drop(tx);

        let object = self
            .vfs()
            .sia_client()
            .upload(blob.to_uploadable_object(self.vfs().id()))
            .await?;

        let mut tx = self.vfs().tx_rw().await?;
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

    pub(crate) async fn queue_blobs<TX: TxScope>(
        &mut self,
        tx: &mut Transaction<TX>,
    ) -> Result<usize, Error>
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
}

impl<C: TxScope> Transaction<C>
where
    Self: DbRead,
{
    pub(crate) async fn blob_by_id(
        &mut self,
        blob_id: &BlobId,
    ) -> Result<Option<Blob>, crate::db::Error> {
        let id = blob_id.as_ref();

        let r = match sqlx::query!("SELECT size, mode, object_id FROM blob where id = ?", id)
            .fetch_optional(self.conn())
            .await?
        {
            None => return Ok(None),
            Some(r) => r,
        };

        let len = r.size as u64;

        let mode = match r.mode.as_str() {
            "L" => StorageMode::Local(Arc::from(vec![])),
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

        let rows = sqlx::query!(
            "SELECT offset, len, chunk_id, chunk_offset FROM chunk_map WHERE blob_id = ? ORDER BY offset ASC",
            id
        ).fetch_all(self.conn()).await?;

        let mut chunk_map = ChunkMap::with_len(len);
        for r in rows {
            chunk_map.insert(
                r.offset as u64,
                r.len as u64,
                ChunkId::try_from_bytes(r.chunk_id)
                    .ok_or_else(|| DataError::ConversionError("invalid chunk id".into()))?,
                r.chunk_offset as usize,
            )
        }

        let blob = BlobMut::from_chunk_map(chunk_map, mode).finalize();
        if blob.id() != blob_id {
            Err(DataError::BlobIdMismatch {
                expected: *blob_id,
                actual: *blob.id(),
            })?
        }
        Ok(Some(blob))
    }

    async fn pushable_blob_ids(&mut self) -> Result<Vec<BlobId>, crate::db::Error> {
        Ok(
            sqlx::query!("SELECT id FROM blob WHERE mode = 'L' AND ref_count > 0")
                .fetch_all(self.conn())
                .await?
                .into_iter()
                .filter_map(|r| BlobId::try_from_bytes(r.id))
                .collect(),
        )
    }

    pub(crate) async fn blob_item(&mut self, id: i64) -> Result<BlobId, crate::db::Error> {
        Ok(BlobId::try_from_bytes(
            sqlx::query!("SELECT blob_id FROM sync_job_queue WHERE id = ?", id)
                .fetch_one(self.conn())
                .await?
                .blob_id
                .ok_or_else(|| DataError::ConversionError("blob_id is missing".into()))?,
        )
        .ok_or_else(|| DataError::ConversionError("invalid blob id".into()))?)
    }
}

impl<C: TxScope> Transaction<C>
where
    Self: DbWrite,
{
    pub(crate) async fn register_blob(&mut self, blob: &Blob) -> Result<(), crate::db::Error> {
        let id = blob.id().as_slice();

        if let StorageMode::Synced(object_id) = blob.mode() {
            // upgrade blob to synced if necessary
            let object_id = *object_id.deref() as i64;
            sqlx::query!(
                "UPDATE blob SET mode = 'S', object_id = ? WHERE id = ?",
                object_id,
                id
            )
            .execute(self.conn())
            .await?;
        }

        if sqlx::query!(
            "SELECT EXISTS(SELECT 1 FROM blob WHERE id = ?) as \"blob_exists: bool\"",
            id,
        )
        .fetch_one(self.conn())
        .await?
        .blob_exists
        {
            // blob already exists
            return Ok(());
        }

        let size = blob.len() as i64;
        let (mode, object_id) = match blob.mode() {
            StorageMode::Local(_) => ("L", None),
            StorageMode::Synced(oid) => ("S", Some(*oid.deref() as i64)),
        };

        sqlx::query!(
            "INSERT INTO blob (id, size, mode, object_id) VALUES (?, ?, ?, ?)",
            id,
            size,
            mode,
            object_id,
        )
        .execute(self.conn())
        .await?;

        for chunk in blob.chunk_map().iter() {
            let offset = chunk.offset as i64;
            let len = chunk.len as i64;
            let chunk_id = chunk.chunk_id.as_slice();
            let chunk_offset = chunk.chunk_offset as i64;
            sqlx::query!(
                "INSERT INTO chunk_map (blob_id, offset, len, chunk_id, chunk_offset) VALUES (?, ?, ?, ?, ?)",
                id,
                offset,
                len,
                chunk_id,
                chunk_offset
            ).execute(self.conn()).await?;
        }

        Ok(())
    }

    async fn remove_sync_job_blob(&mut self, blob_id: &BlobId) -> Result<(), crate::db::Error> {
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
}

#[cfg(test)]
mod tests {
    use crate::blob::{Blob, BlobMut};
    use crate::chunk::Chunk;
    use crate::vfs::StorageMode;
    use std::sync::Arc;

    #[test]
    fn flatbuffer_roundtrip() -> anyhow::Result<()> {
        let data = Chunk::from(Vec::from(b"hello world"));

        let mut blob_mut = BlobMut::empty();
        blob_mut.set_len(1024);
        blob_mut
            .chunk_map
            .insert(0, data.len() as u64, *data.id(), 0);
        let blob = blob_mut.finalize();

        let buf = blob.to_flatbuffer();
        let deserialized =
            Blob::try_from_flatbuffer(buf.as_slice(), StorageMode::Local(Arc::from(vec![])))?;

        assert_eq!(blob.id, deserialized.id);
        assert_eq!(blob.len(), deserialized.len());

        Ok(())
    }
}
