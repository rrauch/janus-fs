pub(crate) mod chunk_map;

use crate::ContentId;
use async_trait::async_trait;
use bytes::Bytes;
use std::ops::Deref;

pub type ChunkId = ContentId<Chunk>;

#[derive(Debug, Clone)]
pub struct Chunk {
    id: ChunkId,
    content: Bytes,
}

impl Chunk {
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
