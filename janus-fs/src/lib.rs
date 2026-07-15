mod nfs;

use crate::nfs::JanusNfsFs;
use anyhow::{Result, anyhow};
use janus_io::RemoteStorage;
use janus_vfs::vfs::{Head, Vfs, VfsId};
use nfsserve::tcp::{NFSTcp, NFSTcpListener};
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;

pub(crate) const CHUNK_SIZE: usize = 1024 * 64;

pub struct JanusNfs {
    listener: NFSTcpListener<JanusNfsFs>,
}

impl JanusNfs {
    pub async fn new(
        remote_storage: RemoteStorage,
        volume_id: &str,
        head: Option<Head>,
        read_only: bool,
        sync_frequency: Option<Duration>,
        db_path: &Path,
        listen_address: &str,
        uid: u32,
        gid: u32,
        file_mode: u32,
        dir_mode: u32,
    ) -> Result<Self> {
        let vfs_id = VfsId::from_str(volume_id).map_err(|_| anyhow!("invalid volume id"))?;
        let db_file = db_path.join(format!("{}.sqlite", vfs_id));

        let export_name = format!("{}", &vfs_id);

        let vfs = Vfs::builder()
            .remote_storage(remote_storage)
            .vfs_id(vfs_id)
            .maybe_head(head)
            .maybe_sync_frequency(sync_frequency)
            .read_only(read_only)
            .max_chunk_size(CHUNK_SIZE)
            .db_file(db_file)
            .build()
            .await?;

        let mut listener = NFSTcpListener::bind(
            listen_address,
            JanusNfsFs::new(vfs, uid, gid, file_mode, dir_mode).await?,
        )
        .await?;
        listener.with_export_name(export_name);

        Ok(Self { listener })
    }

    pub async fn run(self) -> Result<()> {
        self.listener.handle_forever().await?;
        Ok(())
    }
}
