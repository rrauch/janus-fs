use crate::blob::io::{BlobReader, BlobWriter};
use crate::blob::{Blob, BlobId};
use crate::chunk::{Chunk, ChunkId, ChunkSink, ChunkSource};
use crate::vfs::entity::{Entity, EntityRow, RawEntityInner};
use crate::vfs::{
    Container, InodeId, InodeMut, Name, Normalizer, Read, RevisionHasher, TypedInode, Vfs,
    VfsResult, Write,
};
use async_trait::async_trait;
use blake3::Hash;
use chrono::Utc;
use futures_io::{AsyncRead, AsyncSeek, AsyncWrite};
use std::io::SeekFrom;
use std::pin::Pin;
use std::task::{Context, Poll};
use uuid::Uuid;

pub struct FileKind;

impl<Mode> RevisionHasher<BlobInfo, Mode> for FileKind {
    fn hash(inner: &RawEntityInner<Self, BlobInfo, Mode>) -> Hash {
        let mut hasher = blake3::Hasher::new_derive_key("[sia-vfs]/[v0]/[file_revision]");
        hasher.update(b"begin:\n");
        inner.hash_metadata(&mut hasher);
        hasher.update(b"\nbegin_blob:\nid:\n");
        hasher.update(inner.inner.blob_id.as_slice());
        hasher.update(b"\nlength:\n");
        hasher.update(&inner.inner.len.to_be_bytes());
        hasher.update(b"\nend_blob\nend");
        hasher.finalize()
    }
}

impl Normalizer<BlobInfo> for FileKind {
    fn normalize(value: &mut BlobInfo) {}
}

#[derive(Debug, Clone)]
pub struct BlobInfo {
    blob_id: BlobId,
    len: u64,
}

impl BlobInfo {
    fn serialize(&self) -> Vec<u8> {
        todo!()
    }

    fn deserialize(input: &[u8]) -> Result<Self, String> {
        todo!()
    }
}

impl From<Blob> for BlobInfo {
    fn from(value: Blob) -> Self {
        Self {
            blob_id: value.id().clone(),
            len: value.len(),
        }
    }
}

pub type File = TypedInode<FileKind, BlobInfo, InodeId>;
pub type FileMut = InodeMut<FileKind, BlobInfo, InodeId>;

impl File {
    pub fn len(&self) -> u64 {
        self.inner().len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn blob_id(&self) -> &BlobId {
        &self.inner().blob_id
    }
}

impl TryFrom<EntityRow> for Entity<FileKind, BlobInfo> {
    type Error = String;

    fn try_from(value: EntityRow) -> Result<Self, Self::Error> {
        if value.entity_type != "F" {
            return Err(format!(
                "invalid entity_type; expected 'F' but got '{}'",
                value.entity_type
            ));
        }

        let blob_id = value
            .blob_id
            .as_ref()
            .ok_or_else(|| "blob_id is missing".to_string())?;

        let blob_info = BlobInfo::deserialize(
            value
                .data
                .as_ref()
                .ok_or_else(|| "data is missing".to_string())?
                .as_slice(),
        )?;

        if &blob_info.blob_id != blob_id {
            return Err(format!(
                "blob_id mismatch: {} != {}",
                blob_id, blob_info.blob_id
            ));
        }

        (value, blob_info).try_into()
    }
}

impl FileMut {
    pub(crate) fn set_content(&mut self, new: BlobInfo) {
        self.set_inner(new);
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

    pub async fn create_file<T, P>(
        &self,
        parent: &Container<T, P>,
        name: Name,
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
        file.set_last_modified(Utc::now());
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
