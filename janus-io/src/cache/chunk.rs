use crate::cache::{Cache, HasWeight, InnerCache};
use crate::chunk::{Chunk, ChunkId};
use async_trait::async_trait;
use std::ops::Range;

pub(super) type ChunkCache = InnerCache<ChunkId, Chunk, Box<dyn L2ChunkCache>>;

impl HasWeight for ChunkId {
    fn weigh(&self) -> usize {
        self.object_id().weigh() + size_of::<Range<u64>>()
    }
}

impl HasWeight for Chunk {
    fn weigh(&self) -> usize {
        self.id().weigh() + self.len()
    }
}

impl Cache {
    pub(crate) async fn get_chunk<Fut>(
        &self,
        id: &ChunkId,
        source: Fut,
    ) -> Result<Chunk, crate::Error>
    where
        Fut: Future<Output = Result<Chunk, crate::Error>>,
    {
        let l2 = self.0.chunk.l2.as_ref();
        self.0
            .chunk
            .l1
            .try_get_with_by_ref(id, async { retrieve_chunk(id, source, l2).await })
            .await
            .map_err(|e| crate::Error::CachedError(e.to_string()))
    }
}

async fn retrieve_chunk<Fut, L2: L2ChunkCache>(
    id: &ChunkId,
    source: Fut,
    l2: Option<&L2>,
) -> Result<Chunk, crate::Error>
where
    Fut: Future<Output = Result<Chunk, crate::Error>>,
{
    if let Some(l2) = l2 {
        if let Some(chunk) = l2.get_chunk(id).await? {
            // already in L2
            return Ok(chunk);
        }
    }

    let chunk = source.await?;

    if let Some(l2) = l2 {
        l2.insert_chunk(chunk.clone()).await?;
    }
    Ok(chunk)
}

#[async_trait]
pub trait L2ChunkCache: Send + Sync {
    async fn get_chunk(&self, id: &ChunkId) -> Result<Option<Chunk>, std::io::Error>;
    async fn insert_chunk(&self, chunk: Chunk) -> Result<(), std::io::Error>;
}

#[async_trait]
impl L2ChunkCache for Box<dyn L2ChunkCache> {
    #[inline]
    async fn get_chunk(&self, id: &ChunkId) -> Result<Option<Chunk>, std::io::Error> {
        self.as_ref().get_chunk(id).await
    }

    #[inline]
    async fn insert_chunk(&self, chunk: Chunk) -> Result<(), std::io::Error> {
        self.as_ref().insert_chunk(chunk).await
    }
}
