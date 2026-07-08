use crate::chunk::{Chunk, ChunkId};
use bytes::Bytes;
use std::fmt::{Display, Formatter};
use thiserror::Error;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Compressor {
    #[cfg(feature = "lz4")]
    Lz4,
    #[cfg(feature = "zstd")]
    Zstd,
}

impl Display for Compressor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let name = match &self {
            #[cfg(feature = "lz4")]
            Self::Lz4 => "lz4",
            #[cfg(feature = "zstd")]
            Self::Zstd => "zstd",
        };
        write!(f, "{}", name)
    }
}

#[derive(Debug, Clone)]
pub struct CompressedChunk {
    id: ChunkId,
    uncompressed_len: usize,
    compressor: Compressor,
    compressed_data: Bytes,
}

impl CompressedChunk {
    pub fn id(&self) -> &ChunkId {
        &self.id
    }

    pub fn len(&self) -> usize {
        self.compressed_data.len()
    }

    pub fn uncompressed_len(&self) -> usize {
        self.uncompressed_len
    }

    pub fn decompress(&self) -> Result<Chunk, CompressionError> {
        match self.compressor {
            #[cfg(feature = "lz4")]
            Compressor::Lz4 => lz4::decompress(
                &self.id,
                self.compressed_data.as_ref(),
                self.uncompressed_len,
            ),
            #[cfg(feature = "zstd")]
            Compressor::Zstd => zstd::decompress(
                &self.id,
                self.compressed_data.as_ref(),
                self.uncompressed_len,
            ),
        }
    }

    pub fn from_uncompressed(
        chunk: &Chunk,
        compressor: Compressor,
    ) -> Result<Self, CompressionError> {
        match compressor {
            #[cfg(feature = "lz4")]
            Compressor::Lz4 => Ok(lz4::compress(chunk)),
            #[cfg(feature = "zstd")]
            Compressor::Zstd => Ok(zstd::compress(chunk)),
        }
    }
}

#[allow(dead_code)]
pub trait ChunkCompressionExt {
    fn compress(&self, compressor: Compressor) -> Result<CompressedChunk, CompressionError>;
}

impl ChunkCompressionExt for Chunk {
    fn compress(&self, compressor: Compressor) -> Result<CompressedChunk, CompressionError> {
        CompressedChunk::from_uncompressed(self, compressor)
    }
}

#[derive(Error, Debug)]
pub enum CompressionError {
    #[error("compressor '{0}' not supported")]
    UnsupportedCompressor(Compressor),
    #[cfg(feature = "lz4")]
    #[error(transparent)]
    Lz4DecompressionError(#[from] lz4_flex::block::DecompressError),
    #[cfg(feature = "zstd")]
    #[error(transparent)]
    ZstdDecompressionError(#[from] std::io::Error),
    #[error("chunk id mismatch: expected '{expected}' != '{actual}'")]
    ChunkIdMismatch { expected: ChunkId, actual: ChunkId },
}

impl TryFrom<&CompressedChunk> for Chunk {
    type Error = CompressionError;

    #[inline]
    fn try_from(value: &CompressedChunk) -> Result<Self, Self::Error> {
        value.decompress()
    }
}

fn from_decompressed(expected: &ChunkId, decompressed: Vec<u8>) -> Result<Chunk, CompressionError> {
    let chunk = Chunk::from(decompressed);
    if chunk.id() != expected {
        return Err(CompressionError::ChunkIdMismatch {
            expected: expected.clone(),
            actual: chunk.id,
        });
    }
    Ok(chunk)
}

#[cfg(feature = "lz4")]
mod lz4 {
    use crate::chunk::compression::{CompressedChunk, Compressor, from_decompressed};
    use crate::chunk::{Chunk, ChunkId};
    use bytes::Bytes;

    pub(super) fn compress(chunk: &Chunk) -> CompressedChunk {
        let compressed = lz4_flex::block::compress(chunk);
        CompressedChunk {
            id: chunk.id.clone(),
            uncompressed_len: chunk.len(),
            compressor: Compressor::Lz4,
            compressed_data: Bytes::from(compressed),
        }
    }

    pub(super) fn decompress(
        chunk_id: &ChunkId,
        compressed_data: &[u8],
        uncompressed_len: usize,
    ) -> Result<Chunk, super::CompressionError> {
        from_decompressed(
            chunk_id,
            lz4_flex::block::decompress(compressed_data, uncompressed_len)?,
        )
    }
}

#[cfg(feature = "zstd")]
mod zstd {
    use crate::chunk::compression::{CompressedChunk, Compressor, from_decompressed};
    use crate::chunk::{Chunk, ChunkId};
    use bytes::Bytes;

    const LEVEL: i32 = 3;

    pub(super) fn compress(chunk: &Chunk) -> CompressedChunk {
        let compressed =
            zstd::bulk::compress(chunk, LEVEL).expect("zstd compression to never fail");
        CompressedChunk {
            id: chunk.id.clone(),
            uncompressed_len: chunk.len(),
            compressor: Compressor::Zstd,
            compressed_data: Bytes::from(compressed),
        }
    }

    pub(super) fn decompress(
        chunk_id: &ChunkId,
        compressed_data: &[u8],
        uncompressed_len: usize,
    ) -> Result<Chunk, super::CompressionError> {
        from_decompressed(
            chunk_id,
            zstd::bulk::decompress(compressed_data, uncompressed_len)?,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{ChunkCompressionExt, Compressor};
    use crate::chunk::Chunk;
    use std::ops::Deref;

    fn roundtrip(compressor: Compressor) -> anyhow::Result<()> {
        roundtrip_with_data(b"this is a test\n".as_slice(), compressor)?;
        roundtrip_with_data(vec![0u8; 1024].as_slice(), compressor)?;
        Ok(())
    }

    fn roundtrip_with_data(data: &[u8], compressor: Compressor) -> anyhow::Result<()> {
        let chunk = Chunk::from(data.to_vec());
        let compressed = chunk.compress(compressor)?;
        let uncompressed = compressed.decompress()?;
        assert_eq!(chunk.id, uncompressed.id);
        assert_eq!(chunk.len(), uncompressed.len());
        assert_eq!(chunk.deref(), uncompressed.deref());
        Ok(())
    }

    #[test]
    fn lz4_roundtrip() -> anyhow::Result<()> {
        roundtrip(Compressor::Lz4)
    }

    #[test]
    fn zstd_roundtrip() -> anyhow::Result<()> {
        roundtrip(Compressor::Zstd)
    }
}
