mod io_scheduler;
mod nfs;

use crate::nfs::SiaNfsFs;
use anyhow::{Result, anyhow};
use nfsserve::tcp::{NFSTcp, NFSTcpListener};
use sia_io::Client as Sia;
use sia_vfs::vfs::{Head, Vfs, VfsId};
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;

pub(crate) const CHUNK_SIZE: usize = 1024 * 64;

pub struct SiaNfs {
    listener: NFSTcpListener<SiaNfsFs>,
}

impl SiaNfs {
    pub async fn new(
        sia: Sia,
        vfs_id: &str,
        head: Option<Head>,
        read_only: bool,
        db_path: &Path,
        listen_address: &str,
        uid: u32,
        gid: u32,
        file_mode: u32,
        dir_mode: u32,
        write_autocommit_after: Duration,
    ) -> Result<Self> {
        let vfs_id = VfsId::from_str(vfs_id).map_err(|_| anyhow!("invalid vfs id"))?;
        let db_file = db_path.join(format!("{}.sqlite", vfs_id));

        let export_name = format!("{}", &vfs_id);

        let vfs = Vfs::builder()
            .sia_client(sia)
            .vfs_id(vfs_id)
            .maybe_head(head)
            .read_only(read_only)
            .max_chunk_size(CHUNK_SIZE)
            .db_file(db_file)
            .build()
            .await?;

        let mut listener = NFSTcpListener::bind(
            listen_address,
            SiaNfsFs::new(vfs, write_autocommit_after, uid, gid, file_mode, dir_mode).await?,
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
