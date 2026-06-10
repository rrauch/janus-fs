pub(crate) mod pull;
pub(crate) mod push;

use crate::blob::BlobError;
use crate::db::Error as DbError;
use crate::sync::push::PushTask;
use crate::vfs::entity::EntityError;
use crate::vfs::{ReadOnly, ReadWrite, Vfs, VfsError};
use async_trait::async_trait;
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
    #[error(transparent)]
    BlobError(#[from] BlobError),
    #[error(transparent)]
    DbError(#[from] DbError),
    #[error("too many errors")]
    TooManyErrors,
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
    pub(crate) fn new<Mode: Send + Sync + 'static>(
        sync_frequency: Duration,
        initial_sync_delay: Duration,
    ) -> (Self, oneshot::Sender<Vfs<Mode>>)
    where
        Vfs<Mode>: Syncee,
    {
        let (tx, rx) = oneshot::channel();
        let jh = tokio::task::spawn(async move {
            let vfs: Vfs<Mode> = match rx.await {
                Ok(vfs) => vfs,
                Err(_) => {
                    // sender gone
                    return;
                }
            };
            tokio::time::sleep(initial_sync_delay).await;
            loop {
                if let Err(err) = vfs.sync().await {
                    //todo: logging
                    eprintln!("{}", err);
                }
                tokio::time::sleep(sync_frequency).await;
            }
        });
        (Self { jh }, tx)
    }
}

#[async_trait]
pub(crate) trait Syncee {
    async fn sync(&self) -> Result<(), Error>;
}

#[async_trait]
impl Syncee for Vfs<ReadWrite> {
    async fn sync(&self) -> Result<(), Error> {
        push(self.clone(), self.max_sync_attempts()).await?;
        pull(self.clone(), self.max_sync_concurrency()).await?;
        Ok(())
    }
}

#[async_trait]
impl Syncee for Vfs<ReadOnly> {
    async fn sync(&self) -> Result<(), Error> {
        pull(self.clone(), self.max_sync_concurrency()).await?;
        Ok(())
    }
}

pub(crate) async fn push(vfs: Vfs<ReadWrite>, max_attempts: NonZeroUsize) -> Result<(), Error> {
    let _permit = SYNC_SEMAPHORE.acquire().await.expect("semaphore closed");
    let mut task = PushTask::new(vfs, max_attempts);
    task.run().await
}

pub(crate) async fn pull<Mode>(vfs: Vfs<Mode>, max_concurrency: NonZeroUsize) -> Result<(), Error> {
    let _permit = SYNC_SEMAPHORE.acquire().await.expect("semaphore closed");
    let mut task = PullTask::new(vfs, max_concurrency);
    task.run().await
}

impl<Mode> Vfs<Mode>
where
    Self: Syncee,
{
    pub async fn sync(&self) -> Result<(), Error> {
        <Self as Syncee>::sync(self).await
    }
}
