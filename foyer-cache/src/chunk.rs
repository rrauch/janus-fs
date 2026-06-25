use crate::DEFAULT_MEM_BUF_SIZE;
use crate::disk_cache::{DiskCache, Error};
use async_trait::async_trait;
use bon::bon;
use equivalent::Equivalent;
use sia_io::cache::chunk::L2ChunkCache;
use sia_io::chunk::{Chunk, ChunkId};
use std::path::Path;

const COMPAT_FILE_CONTENT_TYPE: &str = "sia-io/cache/chunk";
const COMPAT_FILE_COMP_VERSION: usize = 0;

#[repr(transparent)]
pub struct FoyerChunkCache(DiskCache<ChunkId, Chunk>);

#[bon]
impl FoyerChunkCache {
    #[builder]
    pub async fn new(
        max_disk_space: u64,
        disk_path: impl AsRef<Path>,
        #[builder(default = DEFAULT_MEM_BUF_SIZE)] mem_buf: usize,
    ) -> Result<Self, Error> {
        let mut cache = DiskCache::new(
            "sia_chunk_cache",
            disk_path,
            max_disk_space,
            mem_buf,
            COMPAT_FILE_CONTENT_TYPE.to_string(),
            COMPAT_FILE_COMP_VERSION,
        )
        .await?;
        cache.init().await?;
        Ok(Self(cache))
    }
}

#[derive(Hash)]
struct BorrowedChunkId<'a>(&'a ChunkId);

impl<'a> Equivalent<ChunkId> for BorrowedChunkId<'a> {
    fn equivalent(&self, key: &ChunkId) -> bool {
        self.0 == key
    }
}

#[async_trait]
impl L2ChunkCache for FoyerChunkCache {
    async fn get_chunk(&self, id: &ChunkId) -> Result<Option<Chunk>, std::io::Error> {
        self.0.get(BorrowedChunkId(id)).await
    }

    async fn insert_chunk(&self, chunk: Chunk) -> Result<(), std::io::Error> {
        self.0.insert(chunk.id().clone(), chunk).await
    }
}
