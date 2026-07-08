use crate::io_scheduler::Scheduler;
use crate::io_scheduler::resource_manager::Action::Sleep;
use crate::io_scheduler::resource_manager::{
    Action, Context, QueueCtrl, Resource, ResourceManager,
};
use anyhow::{anyhow, bail};
use futures_util::future::BoxFuture;
use janus_vfs::vfs::file::{FileHandle, ReadWrite};
use janus_vfs::vfs::{InodeId, OwnedName, Vfs};
use std::time::Duration;

pub(crate) struct Upload {
    vfs: Vfs,
}

impl Upload {
    pub(crate) fn new(vfs: Vfs, max_idle: Duration) -> Scheduler<Self> {
        Scheduler::new(
            Upload { vfs },
            false,
            max_idle + Duration::from_millis(10),
            max_idle,
            0,
        )
    }
}

impl ResourceManager for Upload {
    type Resource = FileHandle<ReadWrite>;
    type PreparationKey = (InodeId, OwnedName);
    type AccessKey = InodeId;
    type ResourceData = ();
    type ResourceFuture = BoxFuture<'static, anyhow::Result<Self::Resource>>; // nonexistent, actually

    async fn prepare(
        &self,
        preparation_key: &Self::PreparationKey,
    ) -> anyhow::Result<(Self::AccessKey, Self::ResourceData, Vec<Self::Resource>)> {
        let (parent_id, name) = preparation_key;
        let parent = self
            .vfs
            .inode_by_id((*parent_id).into())
            .await?
            .ok_or(anyhow!("parent not found"))?;

        let parent = parent
            .as_directory()
            .ok_or(anyhow!("inode cannot have children"))?;

        let file = self.vfs.create_file(parent, name).await?;
        let fh = self.vfs.open_rw(&file).await?;

        tracing::debug!(
            file_id = %file.inode_id(),
            file_name = file.name().as_ref(),
            "upload prepared"
        );

        Ok((file.inode_id(), (), vec![fh]))
    }

    fn process(
        &self,
        queue: &mut QueueCtrl<Self>,
        _: &mut Self::ResourceData,
        _: &Context,
    ) -> anyhow::Result<Action> {
        let active_count = queue
            .entries()
            .iter()
            .filter(|e| e.as_idle().is_some() || e.as_active().is_some())
            .count();

        if active_count != 1 {
            bail!(
                "expected active_count to be 1 but found {}, aborting",
                active_count
            );
        };

        Ok(Sleep(Duration::from_secs(5)))
    }
}

impl Resource for FileHandle<ReadWrite> {
    fn offset(&self) -> u64 {
        self.offset()
    }

    fn can_reuse(&self) -> bool {
        !self.is_closed() && self.error_count() == 0
    }

    async fn finalize(self) -> anyhow::Result<()> {
        let _ = self.commit().await?;
        Ok(())
    }
}
