pub mod reader;

use crate::ContentId;
use crate::chunk_map::ChunkMap;

pub type BlobId = ContentId<Blob>;

#[derive(Debug, Clone)]
pub struct Blob {
    id: BlobId,
    chunk_map: ChunkMap,
}

impl Blob {
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
}

impl From<BlobMut> for Blob {
    fn from(value: BlobMut) -> Self {
        let chunk_map = value.chunk_map;
        let id = hash(&chunk_map);
        Self { id, chunk_map }
    }
}

impl From<Blob> for BlobMut {
    fn from(value: Blob) -> Self {
        Self {
            chunk_map: value.chunk_map,
        }
    }
}

#[derive(Debug)]
pub struct BlobMut {
    chunk_map: ChunkMap,
}

impl BlobMut {
    pub fn empty() -> Self {
        Self {
            chunk_map: ChunkMap::new(),
        }
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
