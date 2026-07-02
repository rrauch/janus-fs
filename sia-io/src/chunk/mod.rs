mod downloader;
mod reader;

use crate::object::{Object, ObjectId, Version};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::ops::Range;
use thiserror::Error;

pub(crate) use downloader::ChunkDownloader;
pub(crate) use reader::ChunkedReader;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChunkId {
    object_id: ObjectId,
    object_version: Version,
    range: Range<u64>,
}

impl ChunkId {
    fn from_object(object: &Object, range: Range<u64>) -> Self {
        Self {
            object_id: object.id().clone(),
            object_version: object.version(),
            range,
        }
    }

    #[inline]
    pub fn object_id(&self) -> &ObjectId {
        &self.object_id
    }

    #[inline]
    pub fn object_version(&self) -> Version {
        self.object_version
    }

    #[inline]
    pub fn range(&self) -> &Range<u64> {
        &self.range
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Chunk {
    id: ChunkId,
    content: Bytes,
}

#[derive(Debug, Error)]
pub enum ChunkError {
    #[error("invalid chunk content length: expected {expected} != actual {actual}")]
    InvalidLength { expected: usize, actual: usize },
    #[error("the object was modified")]
    ObjectModified,
    #[error("object id mismatch")]
    ObjectIdMismatch,
}

impl Chunk {
    pub(crate) fn new(id: ChunkId, content: Bytes) -> Result<Self, ChunkError> {
        let len = (id.range.end - id.range.start) as usize;
        if len != content.len() {
            Err(ChunkError::InvalidLength {
                expected: len,
                actual: content.len(),
            })
        } else {
            Ok(Self { id, content })
        }
    }

    #[inline]
    pub fn id(&self) -> &ChunkId {
        &self.id
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.content.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.content.is_empty()
    }
}
