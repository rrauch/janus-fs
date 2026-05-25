use crate::blob::io::{BlobReader, BlobWriter};
use crate::blob::{Blob, BlobId};
use crate::chunk::{Chunk, ChunkId, ChunkSink, ChunkSource};
use crate::gen_flatbuffers::vfs::entity::{
    Entity as FlatEntity, EntityBody as FlatEntityBody, File as FlatFile, FileArgs,
};
use crate::vfs::directory::Directory;
use crate::vfs::entity::{EntityError, EntityHandler, EntityMut, EntityRef, RawEntityInner};
use crate::vfs::{InodeId, InodeMut, Name, Read, Timestamp, TypedInode, Vfs, VfsResult, Write};
use async_trait::async_trait;
use blake3::Hash;
use flatbuffers::{FlatBufferBuilder, UnionWIPOffset, WIPOffset};
use futures_io::{AsyncRead, AsyncSeek, AsyncWrite};
use std::borrow::Cow;
use std::io::SeekFrom;
use std::pin::Pin;
use std::task::{Context, Poll};
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

impl<Mode: Read> Vfs<Mode> {
    pub async fn open(&self, file: &File) -> VfsResult<FileHandle<ReadOnly>> {
        todo!()
    }
}

impl<Mode: Read + Write> Vfs<Mode> {
    pub async fn open_rw(&self, file: &File) -> VfsResult<FileHandle<ReadWrite>> {
        todo!()
    }

    pub async fn create_file(
        &self,
        parent: &Directory,
        name: &Name,
    ) -> VfsResult<FileHandle<ReadWrite>> {
        todo!()
    }
}

pub trait FileMode {}

pub struct ReadOnly {
    reader: BlobReader<()>,
    file: File,
}

impl FileMode for ReadOnly {}

#[async_trait]
impl ChunkSource for () {
    async fn get_chunk(&self, chunk_id: &ChunkId) -> Result<Option<Chunk>, std::io::Error> {
        todo!()
    }
}

#[async_trait]
impl ChunkSink for () {
    async fn insert_chunk(&self, chunk: Chunk) -> Result<(), std::io::Error> {
        todo!()
    }
}

pub struct FileHandle<M: FileMode> {
    id: Uuid,
    inner: M,
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
    writer: BlobWriter<()>,
    file_id: Option<InodeId>,
    file: FileMut,
}

impl FileMode for ReadWrite {}

impl FileHandle<ReadWrite> {
    pub fn file_id(&self) -> Option<InodeId> {
        self.inner.file_id
    }

    pub fn is_new(&self) -> bool {
        self.inner.file_id.is_none()
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

    pub async fn commit(self) -> VfsResult<File> {
        let blob = self.inner.writer.finalize().await?;
        let mut file = self.inner.file;
        file.set_content(blob.clone().into());
        file.set_last_modified(Timestamp::now());
        let file = file.freeze();
        todo!()
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
