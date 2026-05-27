pub mod io;

use crate::ContentId;
use crate::chunk::chunk_map::ChunkMap;
use crate::vfs::StorageMode;

pub type BlobId = ContentId<Blob>;

#[derive(Debug, Clone)]
pub struct Blob {
    id: BlobId,
    mode: StorageMode,
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

    pub(crate) fn mode(&self) -> &StorageMode {
        &self.mode
    }

    pub(crate) fn chunk_map(&self) -> &ChunkMap {
        &self.chunk_map
    }
}

impl From<BlobMut> for Blob {
    fn from(value: BlobMut) -> Self {
        let chunk_map = value.chunk_map;
        let id = hash(&chunk_map);
        Self {
            id,
            chunk_map,
            mode: value.mode,
        }
    }
}

impl From<Blob> for BlobMut {
    fn from(value: Blob) -> Self {
        Self {
            chunk_map: value.chunk_map,
            mode: value.mode,
        }
    }
}

#[derive(Debug)]
pub struct BlobMut {
    chunk_map: ChunkMap,
    mode: StorageMode,
}

impl BlobMut {
    pub fn empty() -> Self {
        Self {
            chunk_map: ChunkMap::new(),
            mode: StorageMode::Local,
        }
    }

    pub fn from_chunk_map(chunk_map: ChunkMap, mode: StorageMode) -> Self {
        Self { chunk_map, mode }
    }

    pub fn len(&self) -> u64 {
        self.chunk_map.len()
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
