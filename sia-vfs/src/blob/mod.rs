pub mod io;

use crate::ContentId;
use crate::chunk::ChunkId;
use crate::chunk::chunk_map::ChunkMap;
use crate::gen_flatbuffers::vfs::blob::{Blob as FlatBlob, BlobBuilder, Chunk as FlatChunk};
use crate::vfs::StorageMode;
use flatbuffers::{FlatBufferBuilder, InvalidFlatbuffer};
use std::sync::Arc;
use thiserror::Error;

pub(crate) const METADATA_OBJECT_TYPE: &'static str = "BLOB";
pub(crate) const METADATA_BLOB_ID: &'static str = "BLOB-ID";

pub type BlobId = ContentId<Blob>;

#[derive(Debug, Error)]
pub enum BlobError {
    #[error(transparent)]
    FlatbufferError(#[from] InvalidFlatbuffer),
}

#[derive(Debug, Clone)]
pub struct Blob {
    id: BlobId,
    mode: StorageMode,
    chunk_map: ChunkMap,
}

impl Blob {
    pub(crate) fn try_from_flatbuffer(buffer: &[u8], mode: StorageMode) -> Result<Self, BlobError> {
        let flat_blob = flatbuffers::root::<FlatBlob>(buffer)?;
        let mut chunk_map = ChunkMap::with_len(flat_blob.len());
        flat_blob.chunks().unwrap_or_default().iter().for_each(|c| {
            let chunk_id = ChunkId::from_byte_ref(&c.chunk_id().0);
            chunk_map.insert(
                c.offset(),
                c.len(),
                chunk_id.clone(),
                c.chunk_offset() as usize,
            );
        });
        Ok(BlobMut::from_chunk_map(chunk_map, mode).finalize())
    }

    pub(crate) fn to_flatbuffer(&self) -> Vec<u8> {
        let mut b = FlatBufferBuilder::new();

        let chunks: Vec<_> = self
            .chunk_map
            .iter()
            .map(|r| {
                let content_id = r.chunk_id.as_flatbuffer();
                FlatChunk::new(r.offset, r.len, content_id, r.chunk_offset as u64)
            })
            .collect();

        let chunks = b.create_vector(&chunks);

        let mut bb = BlobBuilder::new(&mut b);
        bb.add_len(self.len());
        bb.add_chunks(chunks);
        let blob = bb.finish();
        b.finish(blob, None);
        b.finished_data().to_vec()
    }

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

#[derive(Debug, Clone)]
pub struct BlobMut {
    chunk_map: ChunkMap,
    mode: StorageMode,
}

impl BlobMut {
    pub fn empty() -> Self {
        Self {
            chunk_map: ChunkMap::new(),
            mode: StorageMode::Local(Arc::from(vec![])),
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

#[cfg(test)]
mod tests {
    use crate::blob::{Blob, BlobMut};
    use crate::chunk::Chunk;
    use crate::vfs::StorageMode;
    use std::sync::Arc;

    #[test]
    fn flatbuffer_roundtrip() -> anyhow::Result<()> {
        let data = Chunk::from(Vec::from(b"hello world"));

        let mut blob_mut = BlobMut::empty();
        blob_mut.set_len(1024);
        blob_mut
            .chunk_map
            .insert(0, data.len() as u64, *data.id(), 0);
        let blob = blob_mut.finalize();

        let buf = blob.to_flatbuffer();
        let deserialized =
            Blob::try_from_flatbuffer(buf.as_slice(), StorageMode::Local(Arc::from(vec![])))?;

        assert_eq!(blob.id, deserialized.id);
        assert_eq!(blob.len(), deserialized.len());

        Ok(())
    }
}
