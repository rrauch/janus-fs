use crate::blob::Blob;
use crate::blob::io::{BlobReader, BlobWriter};
use crate::chunk::{Chunk, ChunkId, ChunkSink, ChunkSource};
use crate::vfs::{
    Container, Inode, InodeId, InodeInner, InodeKey, InodeMut, Name, Normalizer, Read,
    RevisionHasher, Vfs, VfsResult, Write,
};
use async_trait::async_trait;
use chrono::Utc;
use futures_io::{AsyncRead, AsyncSeek, AsyncWrite};
use std::io::SeekFrom;
use std::pin::Pin;
use std::task::{Context, Poll};
use uuid::Uuid;

pub struct FileKind;

impl RevisionHasher<Blob> for FileKind {
    fn hash(inner: &InodeInner<Self, Blob>) -> blake3::Hash {
        let mut hasher = blake3::Hasher::new_derive_key("[sia-vfs]/[v0]/[file_revision]");
        hasher.update(b"begin:\n");
        inner.hash_metadata(&mut hasher);
        hasher.update(b"\nbegin_blob:\nid:\n");
        hasher.update(inner.inner.id().as_slice());
        hasher.update(b"\nlength:\n");
        hasher.update(&inner.inner.len().to_be_bytes());
        hasher.update(b"\nend_blob\nend");
        hasher.finalize()
    }
}

impl Normalizer<Blob> for FileKind {
    fn normalize(inner: &mut InodeInner<Self, Blob>) {}
}

pub type File = Inode<FileKind, Blob>;
pub type FileMut = InodeMut<FileKind, Blob>;

impl File {
    pub fn len(&self) -> u64 {
        self.0.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn content(&self) -> &Blob {
        &self.0.inner
    }
}

impl FileMut {
    pub(crate) fn set_content(&mut self, new: Blob) {
        self.0.inner = new;
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

    pub async fn create_file<T: RevisionHasher<Vec<InodeKey>>>(
        &self,
        parent: Container<T>,
        name: Name,
    ) -> VfsResult<FileHandle<ReadWrite>> {
        todo!()
    }
}

pub trait FileMode {}

#[repr(transparent)]
pub struct ReadOnly(BlobReader<()>);

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

impl FileMode for ReadOnly {}

pub struct FileHandle<M: FileMode> {
    id: Uuid,
    file: File,
    inner: M,
}

impl<M: FileMode> FileHandle<M> {
    pub fn file_id(&self) -> InodeId {
        self.file.id()
    }
}

impl FileHandle<ReadOnly> {
    pub fn len(&self) -> u64 {
        self.inner.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.0.is_empty()
    }
}

impl AsyncRead for FileHandle<ReadOnly> {
    #[inline]
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let reader = &mut self.as_mut().inner.0;
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
        let reader = &mut self.as_mut().inner.0;
        Pin::new(reader).poll_seek(cx, pos)
    }
}

#[repr(transparent)]
pub struct ReadWrite(BlobWriter<()>);

impl FileMode for ReadWrite {}

impl FileHandle<ReadWrite> {
    pub fn len(&self) -> u64 {
        self.inner.0.len()
    }

    pub async fn set_len(&mut self, new_len: u64) -> VfsResult<()> {
        Ok(self.inner.0.set_len(new_len).await?)
    }

    pub fn is_empty(&self) -> bool {
        self.inner.0.is_empty()
    }

    pub async fn commit(self) -> VfsResult<File> {
        let blob = self.inner.0.finalize().await?;
        let mut file = self.file.into_mut();
        file.set_content(blob);
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
        Pin::new(&mut self.as_mut().inner.0).poll_read(cx, buf)
    }
}

impl AsyncWrite for FileHandle<ReadWrite> {
    #[inline]
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.as_mut().inner.0).poll_write(cx, buf)
    }

    #[inline]
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.as_mut().inner.0).poll_flush(cx)
    }

    #[inline]
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.as_mut().inner.0).poll_close(cx)
    }
}

impl AsyncSeek for FileHandle<ReadWrite> {
    #[inline]
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<std::io::Result<u64>> {
        Pin::new(&mut self.as_mut().inner.0).poll_seek(cx, pos)
    }
}
