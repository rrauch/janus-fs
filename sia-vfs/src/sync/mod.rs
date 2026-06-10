mod pull;
pub mod push;

pub use pull::PullTask;

use crate::blob::BlobError;
use crate::db::Error as DbError;
use crate::vfs::VfsError;
use crate::vfs::entity::EntityError;
use sia_io::upload::UploadError;
use thiserror::Error;

pub(crate) const METADATA_VFS_VERSION: &'static str = "SIA-VFS";
pub(crate) const METADATA_VFS_ID: &'static str = "VFS-ID";

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    VfsError(#[from] VfsError),
    #[error(transparent)]
    UploadError(#[from] UploadError),
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
    #[error(transparent)]
    DbError(#[from] DbError),
    #[error("too many errors")]
    TooManyErrors,
}
