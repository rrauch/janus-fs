use crate::blob::Blob;
use crate::blob::io::{BlobReader, BlobWriter};
use crate::chunk::{Chunk, ChunkId, ChunkSink, ChunkSource};
use crate::vfs::{Container, Inode, InodeId, InodeMut, Name, Vfs, VfsResult};
use async_trait::async_trait;
use futures_io::{AsyncRead, AsyncSeek, AsyncWrite};
use std::io::SeekFrom;
use std::pin::Pin;
use std::task::{Context, Poll};
use uuid::Uuid;

pub struct FileKind;
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

impl Vfs {
    pub async fn open<M: Mode>(&self, file: &File) -> VfsResult<FileHandle<M>> {
        todo!()
    }

    pub async fn create_file<T>(
        &self,
        parent: Container<T>,
        name: Name,
    ) -> VfsResult<FileHandle<ReadWrite>> {
        todo!()
    }
}

trait Mode {}

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

impl Mode for ReadOnly {}

pub struct FileHandle<M: Mode> {
    id: Uuid,
    file_id: InodeId,
    inner: M,
}

impl<M: Mode> FileHandle<M> {
    pub fn file_id(&self) -> &InodeId {
        &self.file_id
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

impl Mode for ReadWrite {}

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
