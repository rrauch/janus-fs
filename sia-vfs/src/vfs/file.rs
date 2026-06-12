use crate::blob::io::{BlobReader, BlobWriter};
use crate::blob::{Blob, BlobId, BlobMut};
use crate::chunk::{Chunk, ChunkId, ChunkSink, ChunkSource};
use crate::db::{DataError, Db, Error as DbError, Read as DbRead, Write as DbWrite};
use crate::db::{Transaction, TxScope};
use crate::gen_flatbuffers::vfs::entity::{
    Entity as FlatEntity, EntityBody as FlatEntityBody, File as FlatFile, FileArgs,
};
use crate::vfs::directory::Directory;
use crate::vfs::entity::{
    DraftEntity, EntityError, EntityHandler, EntityMut, EntityRef, RawEntityInner,
};
use crate::vfs::{
    Inode, InodeId, InodeMut, Name, OwnedName, Timestamp, TypedInode, Vfs, VfsError, VfsResult,
};
use async_trait::async_trait;
use blake3::Hash;
use flatbuffers::{FlatBufferBuilder, UnionWIPOffset, WIPOffset};
use futures_channel::mpsc;
use futures_io::{AsyncRead, AsyncSeek, AsyncWrite};
use futures_util::{AsyncWriteExt, StreamExt};
use std::borrow::Cow;
use std::collections::HashMap;
use std::io::{Error, SeekFrom};
use std::ops::Deref;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};
use twox_hash::XxHash3_64;
use uuid::Uuid;
use yoke::Yokeable;

pub struct FileKind;

#[derive(Yokeable, Debug, Clone)]
pub struct FileBody<'a> {
    len: u64,
    blob_id: Cow<'a, BlobId>,
}

impl FileBody<'_> {
    pub fn into_owned(self) -> FileBody<'static> {
        FileBody {
            len: self.len,
            blob_id: Cow::Owned(self.blob_id.into_owned()),
        }
    }
}

impl From<Blob> for FileBody<'static> {
    fn from(value: Blob) -> Self {
        Self {
            blob_id: Cow::Owned(value.id().clone()),
            len: value.len(),
        }
    }
}

impl EntityHandler for FileKind {
    type Body = FileBody<'static>;
    const DB_TYPE: &'static str = "F";
    const METADATA_TYPE: &'static str = "FILE";

    fn to_owned(body: &<Self::Body as Yokeable>::Output) -> Self::Body {
        body.clone().into_owned()
    }

    fn extract(entity: FlatEntity) -> Result<<Self::Body as Yokeable>::Output, EntityError> {
        let dir = entity.body_as_file().ok_or(EntityError::ExpectedFile)?;
        Ok(FileBody {
            len: dir.len(),
            blob_id: Cow::Borrowed(BlobId::from_byte_ref(&dir.blob_id().0)),
        })
    }

    fn serialize_body(
        b: &mut FlatBufferBuilder,
        entity: &EntityMut<Self>,
    ) -> (FlatEntityBody, WIPOffset<UnionWIPOffset>) {
        let blob_id = entity.body().blob_id.as_flatbuffer();
        let file = FlatFile::create(
            b,
            &FileArgs {
                len: entity.body().len,
                blob_id: Some(blob_id),
            },
        );
        (FlatEntityBody::File, file.as_union_value())
    }

    fn normalize(value: &mut Self::Body) {
        // nothing do to
    }

    fn hash(entity: &RawEntityInner<Self>) -> Hash {
        let mut hasher = blake3::Hasher::new_derive_key("[sia-vfs]/[v0]/[file_entity]");
        hasher.update(b"begin:\n");
        entity.hash_metadata(&mut hasher);
        hasher.update(b"\nbegin_blob:\nid:\n");
        hasher.update(entity.body().blob_id.as_slice());
        hasher.update(b"\nlength:\n");
        hasher.update(&entity.body().len.to_be_bytes());
        hasher.update(b"\nend_blob\nend");
        hasher.finalize()
    }

    fn references(entity: &RawEntityInner<Self>) -> Vec<EntityRef<'_>> {
        vec![EntityRef::from(entity.body().blob_id.as_ref())]
    }
}

pub type File = TypedInode<FileKind>;
pub type FileMut = InodeMut<FileKind>;
pub(crate) type FileDraft = DraftEntity<FileKind>;

impl FileDraft {
    pub fn new_file_draft(name: OwnedName, blob: Blob) -> Self {
        EntityMut::new(name, blob.into()).freeze()
    }
}

impl File {
    pub fn len(&self) -> u64 {
        self.body().len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn blob_id(&self) -> &BlobId {
        &self.body().blob_id
    }
}

impl FileMut {
    pub(crate) fn set_content(&mut self, new: FileBody<'static>) {
        self.set_body(new);
    }
}

impl Vfs
where
    Self: ChunkSource + 'static,
{
    pub async fn open(&self, file: &File) -> VfsResult<FileHandle<ReadOnly>> {
        let blob = self
            .blob_by_id(file.blob_id())
            .await?
            .ok_or_else(|| DbError::DataError(DataError::BlobNotFound(*file.blob_id())))?;

        Ok(FileHandle::new(
            XxHash3_64::oneshot(Uuid::now_v7().as_bytes()),
            ReadOnly {
                reader: BlobReader::new_reader(blob, self.clone()),
                file: file.clone(),
            },
        ))
    }
}

#[derive(Debug, Error)]
pub enum LockError {
    #[error("lock acquisition timed out")]
    AcquisitionTimeout,
}

struct FileWriteLock {
    guard: Option<OwnedMutexGuard<InodeId>>,
    map: LockMap,
}

impl FileWriteLock {
    pub fn inode_id(&self) -> InodeId {
        *self.guard.as_ref().unwrap().deref()
    }
}

impl Drop for FileWriteLock {
    fn drop(&mut self) {
        // drop the guard here to make sure we don't hold a strong reference
        self.guard.take();

        let mut guard = self.map.lock().expect("lock to not be poisoned");
        guard.retain(|_, v| v.strong_count() >= 1);
    }
}

type LockMap = Arc<Mutex<HashMap<InodeId, Weak<AsyncMutex<InodeId>>>>>;

#[derive(Debug)]
#[repr(transparent)]
pub(super) struct FileWriteLocks(LockMap);

impl FileWriteLocks {
    pub(super) fn new() -> Self {
        Self(Arc::new(Mutex::new(HashMap::new())))
    }
    async fn acquire(&self, inode_id: InodeId) -> Result<FileWriteLock, LockError> {
        let async_lock = {
            let mut outer_lock = self.0.lock().expect("lock to not be poisoned");

            if let Some(async_lock) = outer_lock.get(&inode_id).and_then(|w| w.upgrade()) {
                async_lock
            } else {
                let async_lock = Arc::new(AsyncMutex::new(inode_id));
                outer_lock.insert(inode_id, Arc::downgrade(&async_lock));
                async_lock
            }
        };
        let owned_guard = tokio::time::timeout(Duration::from_secs(5), async_lock.lock_owned())
            .await
            .map_err(|_| LockError::AcquisitionTimeout)?;
        Ok(FileWriteLock {
            guard: Some(owned_guard),
            map: self.0.clone(),
        })
    }
}

impl Vfs
where
    Self: ChunkSource + ChunkSink + 'static,
{
    pub async fn open_rw(&self, file: &File) -> VfsResult<FileHandle<ReadWrite>> {
        let inner = match self {
            Self::ReadWrite(rw) => rw,
            Self::ReadOnly(_) => return Err(VfsError::ReadOnlyFileSystem),
        };

        let blob = self
            .blob_by_id(file.blob_id())
            .await?
            .ok_or_else(|| DbError::DataError(DataError::BlobNotFound(*file.blob_id())))?;
        let lock = inner.file_write_locks.acquire(file.inode_id).await?;
        let mut tx = self.tx_rw().await?;
        let current_file = match tx
            .inode_by_id(file.inode_id)
            .await?
            .ok_or_else(|| DbError::DataError(DataError::InodeNotFound(file.inode_id)))?
        {
            Inode::File(file) => file,
            _ => Err(VfsError::Other(format!(
                "inode [{}] is not a file",
                file.inode_id
            )))?,
        };

        // make sure this is the expected file
        if file != &current_file {
            Err(VfsError::Other(format!(
                "file with inode [{}] has been modified",
                file.inode_id
            )))?;
        }

        let fh_id = tx.create_fh(file.inode_id).await?;
        tx.commit().await?;
        let file_id = file.inode_id;
        let file = current_file.into_mut();
        let reaper_tx = inner.dead_fh_reaper.tx();
        Ok(FileHandle::new(
            fh_id,
            ReadWrite {
                writer: BlobWriter::new_writer(
                    blob.into_mut(),
                    TempChunkTracker {
                        vfs: self.clone(),
                        fh_id,
                    },
                    self.max_chunk_size(),
                ),
                lock,
                file,
                vfs: self.clone(),
                reaper_notifier: ReaperNotifier {
                    fh_id,
                    reaper_tx: Some(reaper_tx),
                },
            },
        ))
    }

    pub async fn create_file(&self, parent: &Directory, name: &Name) -> VfsResult<File> {
        let mut tx = self.tx_rw().await?;
        let inode_id = tx.create_file(name, parent.inode_id()).await?;
        let file = match tx.inode_by_id(inode_id).await? {
            Some(Inode::File(file)) => file,
            _ => {
                return Err(VfsError::Other(format!("inode {} is not a file", inode_id)));
            }
        };
        tx.commit().await?;
        Ok(file)
    }
}

pub trait FileMode {}

pub struct ReadOnly {
    reader: BlobReader<Vfs>,
    file: File,
}

impl FileMode for ReadOnly where Vfs: ChunkSource + 'static {}

pub struct FileHandle<M: FileMode> {
    id: u64,
    inner: M,
}

impl<M: FileMode> FileHandle<M> {
    fn new(id: u64, inner: M) -> Self {
        Self { id, inner }
    }
}

impl FileHandle<ReadOnly> {
    pub fn file(&self) -> &File {
        &self.inner.file
    }

    pub fn len(&self) -> u64 {
        self.inner.file.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.file.is_empty()
    }
}

impl AsyncRead for FileHandle<ReadOnly> {
    #[inline]
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let reader = &mut self.as_mut().inner.reader;
        Pin::new(reader).poll_read(cx, buf)
    }
}

impl AsyncSeek for FileHandle<ReadOnly> {
    #[inline]
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<std::io::Result<u64>> {
        let reader = &mut self.as_mut().inner.reader;
        Pin::new(reader).poll_seek(cx, pos)
    }
}

pub struct ReadWrite {
    writer: BlobWriter<TempChunkTracker>,
    lock: FileWriteLock,
    file: FileMut,
    vfs: Vfs,
    reaper_notifier: ReaperNotifier,
}

struct TempChunkTracker {
    vfs: Vfs,
    fh_id: u64,
}

#[async_trait]
impl ChunkSource for TempChunkTracker {
    async fn get_chunk(&self, chunk_id: &ChunkId) -> Result<Option<Chunk>, Error> {
        self.vfs.get_chunk(chunk_id).await
    }
}

#[async_trait]
impl ChunkSink for TempChunkTracker {
    async fn insert_chunk(&self, chunk: Chunk) -> Result<(), Error> {
        let chunk_id = chunk.id().clone();
        self.vfs.insert_chunk(chunk).await?;
        let mut tx = self.vfs.tx_rw().await.map_err(Error::other)?;
        tx.insert_temp_fh_chunk(self.fh_id, &chunk_id)
            .await
            .map_err(Error::other)?;
        tx.commit().await.map_err(Error::other)?;
        Ok(())
    }
}

struct ReaperNotifier {
    fh_id: u64,
    reaper_tx: Option<mpsc::Sender<u64>>,
}

impl ReaperNotifier {
    fn disarm(&mut self) {
        self.reaper_tx.take();
    }
}

impl Drop for ReaperNotifier {
    fn drop(&mut self) {
        // notify reaper this fh is dead
        if let Some(mut reaper_tx) = self.reaper_tx.take() {
            let _ = reaper_tx.try_send(self.fh_id);
        }
    }
}

impl FileMode for ReadWrite {}

impl FileHandle<ReadWrite> {
    pub fn file_id(&self) -> InodeId {
        self.inner.lock.inode_id()
    }

    pub fn len(&self) -> u64 {
        self.inner.writer.len()
    }

    pub async fn set_len(&mut self, new_len: u64) -> VfsResult<()> {
        Ok(self.inner.writer.set_len(new_len).await?)
    }

    pub fn is_empty(&self) -> bool {
        self.inner.writer.is_empty()
    }

    pub async fn fsync(&mut self) -> VfsResult<()> {
        self.flush().await?;
        let blob = self.inner.writer.fsync().await?;
        let mut tx = self.inner.vfs.tx_rw().await?;
        let file = tx.fsync(self.inner.file.clone(), blob).await?;
        tx.commit().await?;
        self.inner.file = file.into_mut();
        Ok(())
    }

    pub async fn commit(mut self) -> VfsResult<File> {
        let blob = self.inner.writer.finalize().await?;
        let mut tx = self.inner.vfs.tx_rw().await?;
        let file = tx.fsync(self.inner.file, blob).await?;
        tx.delete_fh(self.id).await?;
        tx.commit().await?;
        self.inner.reaper_notifier.disarm();
        Ok(file)
    }
}

impl AsyncRead for FileHandle<ReadWrite> {
    #[inline]
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.as_mut().inner.writer).poll_read(cx, buf)
    }
}

impl AsyncWrite for FileHandle<ReadWrite> {
    #[inline]
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.as_mut().inner.writer).poll_write(cx, buf)
    }

    #[inline]
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.as_mut().inner.writer).poll_flush(cx)
    }

    #[inline]
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.as_mut().inner.writer).poll_close(cx)
    }
}

impl AsyncSeek for FileHandle<ReadWrite> {
    #[inline]
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<std::io::Result<u64>> {
        Pin::new(&mut self.as_mut().inner.writer).poll_seek(cx, pos)
    }
}

#[derive(Debug)]
pub(crate) struct Reaper {
    tx: mpsc::Sender<u64>,
    jh: tokio::task::JoinHandle<()>,
}

impl Reaper {
    pub fn new(db: Db) -> Self {
        let (tx, rx) = mpsc::channel(32);

        let jh = tokio::spawn(async move { Self::run(db, rx).await });

        Self { tx, jh }
    }

    pub fn tx(&self) -> mpsc::Sender<u64> {
        self.tx.clone()
    }

    async fn run(db: Db, mut rx: mpsc::Receiver<u64>) {
        'main: loop {
            let fh_id = match rx.next().await {
                None => break 'main,
                Some(fh_id) => fh_id,
            };

            if let Ok(mut tx) = db.write().await {
                let _ = tx.delete_fh(fh_id).await;
                let _ = tx.commit().await;
            }
        }
    }
}

impl Drop for Reaper {
    fn drop(&mut self) {
        self.jh.abort();
    }
}

impl<C: TxScope> Transaction<C>
where
    Self: DbWrite,
{
    async fn create_file(
        &mut self,
        name: &Name,
        parent_inode_id: InodeId,
    ) -> Result<InodeId, DbError> {
        let blob = BlobMut::empty().finalize();
        self.register_blob(&blob).await?;
        let entity = FileDraft::new_file_draft(name.to_owned(), blob);
        let entity_id = self.register_entity(entity).await?;
        Ok(self
            .create_inode::<FileKind>(&name, parent_inode_id, entity_id)
            .await?)
    }

    async fn fsync(&mut self, mut file: FileMut, blob: Blob) -> Result<File, VfsError> {
        let inode_id = file.inode_id;
        file.set_content(blob.clone().into());
        file.set_last_modified(Timestamp::now());
        self.register_blob(&blob).await?;
        let name = file.name().to_owned();
        let file = match self.update(inode_id, &name, file.freeze()).await? {
            Inode::File(file) => file,
            _ => {
                return Err(VfsError::Other(format!("inode {} is not a file", inode_id)));
            }
        };
        Ok(file)
    }

    async fn create_fh(&mut self, inode_id: InodeId) -> Result<u64, DbError> {
        let inode_id = inode_id.0 as i64;
        Ok(sqlx::query!(
            "INSERT INTO temp_file_handle (inode_id) VALUES (?)",
            inode_id
        )
        .execute(self.conn())
        .await?
        .last_insert_rowid() as u64)
    }

    async fn delete_fh(&mut self, fh_id: u64) -> Result<(), DbError> {
        let id = fh_id as i64;
        sqlx::query!("DELETE FROM temp_file_handle WHERE id = ?", id)
            .execute(self.conn())
            .await?;
        Ok(())
    }

    async fn insert_temp_fh_chunk(
        &mut self,
        fh_id: u64,
        chunk_id: &ChunkId,
    ) -> Result<(), DbError> {
        let fh_id = fh_id as i64;
        let chunk_id = chunk_id.as_slice();

        sqlx::query!(
            "INSERT OR IGNORE INTO temp_file_chunks (file_handle, chunk_id) VALUES (?, ?)",
            fh_id,
            chunk_id
        )
        .execute(self.conn())
        .await?;

        Ok(())
    }
}
