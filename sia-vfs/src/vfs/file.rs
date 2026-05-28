use crate::blob::io::{BlobReader, BlobWriter};
use crate::blob::{Blob, BlobId, BlobMut};
use crate::chunk::{ChunkSink, ChunkSource};
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
    Inode, InodeId, InodeMut, Name, OwnedName, Read, Timestamp, TypedInode, Vfs, VfsError,
    VfsResult, Write,
};
use blake3::Hash;
use flatbuffers::{FlatBufferBuilder, UnionWIPOffset, WIPOffset};
use futures_channel::mpsc;
use futures_io::{AsyncRead, AsyncSeek, AsyncWrite};
use futures_util::StreamExt;
use std::borrow::Cow;
use std::io::SeekFrom;
use std::pin::Pin;
use std::task::{Context, Poll};
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

    fn db_type() -> &'static str {
        "F"
    }

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

impl<Mode: Read> Vfs<Mode>
where
    Self: ChunkSource + 'static,
{
    pub async fn open(&self, file: &File) -> VfsResult<FileHandle<ReadOnly<Mode>>> {
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

impl<Mode: Read + Write> Vfs<Mode>
where
    Self: ChunkSource + ChunkSink + 'static,
{
    pub async fn open_rw(&self, file: &File) -> VfsResult<FileHandle<ReadWrite<Mode>>> {
        let blob = self
            .blob_by_id(file.blob_id())
            .await?
            .ok_or_else(|| DbError::DataError(DataError::BlobNotFound(*file.blob_id())))?;
        //todo: locking
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
        let reaper_tx = self.0.dead_fh_reaper.tx();
        Ok(FileHandle::new(
            fh_id,
            ReadWrite {
                writer: BlobWriter::new_writer(
                    blob.into_mut(),
                    self.clone(),
                    self.max_chunk_size(),
                ),
                file_id,
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

pub struct ReadOnly<Mode> {
    reader: BlobReader<Vfs<Mode>>,
    file: File,
}

impl<Mode> FileMode for ReadOnly<Mode> where Vfs<Mode>: ChunkSource + 'static {}

pub struct FileHandle<M: FileMode> {
    id: u64,
    inner: M,
}

impl<M: FileMode> FileHandle<M> {
    fn new(id: u64, inner: M) -> Self {
        Self { id, inner }
    }
}

impl<Mode> FileHandle<ReadOnly<Mode>>
where
    Vfs<Mode>: ChunkSource + 'static,
{
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

impl<Mode> AsyncRead for FileHandle<ReadOnly<Mode>>
where
    Vfs<Mode>: ChunkSource + 'static,
{
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

impl<Mode> AsyncSeek for FileHandle<ReadOnly<Mode>>
where
    Vfs<Mode>: ChunkSource + 'static,
{
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

pub struct ReadWrite<Mode> {
    writer: BlobWriter<Vfs<Mode>>,
    file_id: InodeId,
    file: FileMut,
    vfs: Vfs<Mode>,
    reaper_notifier: ReaperNotifier,
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

impl<Mode> FileMode for ReadWrite<Mode> where Vfs<Mode>: ChunkSource + ChunkSink + 'static {}

impl<Mode: Read + Write> FileHandle<ReadWrite<Mode>>
where
    Vfs<Mode>: ChunkSource + ChunkSink + 'static,
{
    pub fn file_id(&self) -> InodeId {
        self.inner.file_id
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

    pub async fn commit(mut self) -> VfsResult<File> {
        let blob = self.inner.writer.finalize().await?;
        let mut file = self.inner.file;
        file.set_content(blob.clone().into());
        file.set_last_modified(Timestamp::now());
        let inode_id = file.inode_id;
        let mut tx = self.inner.vfs.tx_rw().await?;
        tx.create_blob_if_not_exist(&blob).await?;
        let name = file.name().to_owned();
        let file = match tx.update(inode_id, &name, file.freeze()).await? {
            Inode::File(file) => file,
            _ => {
                return Err(VfsError::Other(format!("inode {} is not a file", inode_id)));
            }
        };
        tx.delete_fh(self.id).await?;
        tx.commit().await?;
        self.inner.reaper_notifier.disarm();
        Ok(file)
    }
}

impl<Mode> AsyncRead for FileHandle<ReadWrite<Mode>>
where
    Vfs<Mode>: ChunkSource + ChunkSink + 'static,
{
    #[inline]
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.as_mut().inner.writer).poll_read(cx, buf)
    }
}

impl<Mode> AsyncWrite for FileHandle<ReadWrite<Mode>>
where
    Vfs<Mode>: ChunkSource + ChunkSink + 'static,
{
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

impl<Mode> AsyncSeek for FileHandle<ReadWrite<Mode>>
where
    Vfs<Mode>: ChunkSource + ChunkSink + 'static,
{
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
        self.create_blob_if_not_exist(&blob).await?;
        let entity = FileDraft::new_file_draft(name.to_owned(), blob);
        let entity_id = self.create_entity_if_not_exist(entity).await?;
        Ok(self
            .create_inode::<FileKind>(&name, parent_inode_id, entity_id)
            .await?)
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
}
