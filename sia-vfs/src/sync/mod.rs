mod pull;

pub use pull::PullTask;

use crate::blob::BlobError;
use crate::vfs::VfsError;
use crate::vfs::entity::EntityError;
use thiserror::Error;

const METADATA_VFS_VERSION: &'static str = "SIA-VFS";
const METADATA_VFS_ID: &'static str = "VFS-ID";

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    VfsError(#[from] VfsError),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    SiaError(#[from] sia_io::Error),
    #[error(transparent)]
    EntityError(#[from] EntityError),
    #[error("chunk_id invalid")]
    InvalidChunkId,
    #[error(transparent)]
    BlobError(#[from] BlobError),
}
