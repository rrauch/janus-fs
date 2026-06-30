pub(crate) mod pull;
pub(crate) mod push;

use crate::blob::BlobError;
use crate::db::Error as DbError;
use crate::sync::push::PushTask;
use crate::vfs::commit::CommitError;
use crate::vfs::config::ConfigError;
use crate::vfs::entity::EntityError;
use crate::vfs::{Vfs, VfsError, WeakVfs};
pub use pull::PullTask;
use sia_io::upload::UploadError;
use std::num::NonZeroUsize;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{Semaphore, oneshot};
use tokio::task::JoinHandle;

pub(crate) const METADATA_VFS_VERSION: &'static str = "SIA-VFS";
pub(crate) const METADATA_VFS_ID: &'static str = "VFS-ID";

static SYNC_SEMAPHORE: Semaphore = Semaphore::const_new(1);

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
    #[error("vfs head not a branch")]
    NotBranchError,
    #[error(transparent)]
    CommitError(#[from] CommitError),
    #[error(transparent)]
    ConfigError(#[from] ConfigError),
    #[error(transparent)]
    BlobError(#[from] BlobError),
    #[error(transparent)]
    DbError(#[from] DbError),
    #[error("too many errors")]
    TooManyErrors,
    #[error("max depth exceeded")]
    MaxDepthExceeded,
}

#[derive(Debug)]
pub(crate) struct Syncer {
    jh: JoinHandle<()>,
}

impl Drop for Syncer {
    fn drop(&mut self) {
        self.jh.abort();
    }
}

impl Syncer {
    pub(crate) fn new(
        sync_frequency: Duration,
        initial_sync_delay: Duration,
    ) -> (Self, oneshot::Sender<WeakVfs>) {
        let (tx, rx) = oneshot::channel();
        let jh = tokio::task::spawn(async move {
            let weak: WeakVfs = match rx.await {
                Ok(vfs) => vfs,
                Err(_) => {
                    // sender gone
                    return;
                }
            };
            tokio::time::sleep(initial_sync_delay).await;
            loop {
                if let Ok(vfs) = Vfs::try_from(weak.clone()) {
                    if let Err(err) = vfs.sync().await {
                        //todo: logging
                        eprintln!("{}", err);
                    }
                } else {
                    // vfs shut down
                    return;
                }

                tokio::time::sleep(sync_frequency).await;
            }
        });
        (Self { jh }, tx)
    }
}

pub(crate) async fn push(vfs: Vfs, max_attempts: NonZeroUsize) -> Result<(), Error> {
    let branch_name = vfs
        .head()
        .maybe_branch_name()
        .ok_or_else(|| Error::NotBranchError)?;
    let _permit = SYNC_SEMAPHORE.acquire().await.expect("semaphore closed");
    let mut task = PushTask::new(vfs, branch_name, max_attempts);
    task.run().await
}

pub(crate) async fn pull(vfs: Vfs, max_concurrency: NonZeroUsize) -> Result<(), Error> {
    let _permit = SYNC_SEMAPHORE.acquire().await.expect("semaphore closed");
    let mut task = PullTask::new(vfs, max_concurrency);
    task.run().await
}

impl Vfs {
    pub async fn sync(&self) -> Result<(), Error> {
        if !self.is_read_only() {
            push(self.clone(), self.max_sync_attempts()).await?;
        }
        pull(self.clone(), self.max_sync_concurrency()).await?;
        Ok(())
    }
}
