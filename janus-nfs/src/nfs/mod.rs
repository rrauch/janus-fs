mod upload;

use crate::io_scheduler::Scheduler;
use crate::nfs::upload::Upload;
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures_util::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, TryStreamExt};
use janus_vfs::vfs::directory::Directory;
use janus_vfs::vfs::file::File;
use janus_vfs::vfs::{Inode, InodeId, Name, Vfs};
use nfsserve::nfs;
use nfsserve::nfs::nfsstat3::{
    NFS3ERR_IO, NFS3ERR_ISDIR, NFS3ERR_NOENT, NFS3ERR_NOTDIR, NFS3ERR_NOTSUPP, NFS3ERR_SERVERFAULT,
};
use nfsserve::nfs::{
    fattr3, fileid3, filename3, fsinfo3, ftype3, nfspath3, nfsstat3, nfstime3, sattr3, specdata3,
};
use nfsserve::vfs::{DirEntry, NFSFileSystem, ReadDirResult, VFSCapabilities};
use std::cmp::min;
use std::io::SeekFrom;
use std::time::Duration;
use tracing::instrument;

pub(crate) struct JanusNfsFs {
    vfs: Vfs,
    uploader: Scheduler<Upload>,
    uid: u32,
    gid: u32,
    file_mode: u32,
    dir_mode: u32,
    root_id: InodeId,
    fs_id: u64,
}

impl JanusNfsFs {
    pub(super) async fn new(
        vfs: Vfs,
        upload_max_idle: Duration,
        uid: u32,
        gid: u32,
        file_mode: u32,
        dir_mode: u32,
    ) -> Result<Self> {
        let root_id = vfs.root().await?.inode_id();
        let (_, fs_id) = vfs.id().as_u64_pair();

        let uploader = Upload::new(vfs.clone(), upload_max_idle);
        Ok(Self {
            uploader,
            vfs,
            uid,
            gid,
            file_mode,
            dir_mode,
            root_id,
            fs_id,
        })
    }
}

#[async_trait]
impl NFSFileSystem for JanusNfsFs {
    fn capabilities(&self) -> VFSCapabilities {
        if self.vfs.is_read_only() {
            VFSCapabilities::ReadOnly
        } else {
            VFSCapabilities::ReadWrite
        }
    }

    fn root_dir(&self) -> fileid3 {
        *self.root_id
    }

    #[instrument(skip(self))]
    async fn lookup(&self, dirid: fileid3, filename: &filename3) -> Result<fileid3, nfsstat3> {
        Ok(*self.inode_by_dir_name(dirid, filename).await?.id())
    }

    async fn getattr(&self, id: fileid3) -> Result<fattr3, nfsstat3> {
        Ok(self.to_fattr3(&self.inode_by_id(id).await?))
    }

    async fn setattr(&self, id: fileid3, _setattr: sattr3) -> Result<fattr3, nfsstat3> {
        tracing::debug!("setattr called");
        Ok(self.to_fattr3(&self.inode_by_id(id).await?))
    }

    #[instrument(skip(self))]
    async fn read(
        &self,
        id: fileid3,
        offset: u64,
        count: u32,
    ) -> Result<(Vec<u8>, bool), nfsstat3> {
        let file: File = self
            .inode_by_id(id)
            .await?
            .try_into()
            .map_err(|_| NFS3ERR_ISDIR)?;

        // make sure we don't read beyond eof
        let count = {
            if offset >= file.len() {
                0
            } else {
                let available = file.len() - offset;
                min(count, available as u32) as usize
            }
        };

        if count == 0 {
            tracing::debug!(offset, "read attempt beyond eof detected");
            return Ok((vec![], true));
        }

        let mut file_reader = self.vfs.open(&file).await.map_err(|e| {
            tracing::error!(error = %e, "failed to call read_file for file {}", id);
            NFS3ERR_SERVERFAULT
        })?;
        let _ = file_reader
            .seek(SeekFrom::Start(offset))
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "failed to seek to offset {} for file {}", offset, id);
                NFS3ERR_SERVERFAULT
            })?;

        let mut buf = Vec::with_capacity(count);
        let mut file_reader = file_reader.take(count as u64);
        let bytes_read = file_reader.read_to_end(&mut buf).await.map_err(|e| {
            tracing::error!(error = %e, "read error");
            NFS3ERR_IO
        })?;
        if bytes_read != count {
            tracing::error!(
                expected = count,
                actual = bytes_read,
                "incorrect number of bytes read"
            );
            return Err(NFS3ERR_IO);
        }
        let file_reader = file_reader.into_inner();
        let pos = offset + bytes_read as u64;
        Ok((buf, pos >= file_reader.len()))
    }

    #[instrument(skip(self, data), fields(count = data.len()))]
    async fn write(&self, id: fileid3, offset: u64, data: &[u8]) -> Result<fattr3, nfsstat3> {
        let file: File = self
            .inode_by_id(id)
            .await?
            .try_into()
            .map_err(|_| NFS3ERR_ISDIR)?;

        let mut upload = self
            .uploader
            .access(&file.inode_id(), offset)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "failed to acquire upload handle for {}", file.inode_id());
                NFS3ERR_NOENT
            })?;
        let upload = upload.as_mut();

        upload.write_all(data).await.map_err(|e| {
            tracing::error!(error = %e, "write error");
            NFS3ERR_IO
        })?;

        let inode = Inode::File(upload.fsync().await.map_err(|e| {
            tracing::error!(error = %e, "fsync error");
            NFS3ERR_IO
        })?);
        tracing::debug!(file = ?file, offset = offset, data = data.len(), "write complete");

        Ok(self.to_fattr3(&inode))
    }

    async fn create(
        &self,
        dirid: fileid3,
        filename: &filename3,
        _attr: sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        let id = self.create_exclusive(dirid, filename).await?;
        let inode = self.inode_by_id(id).await?;
        if inode.as_file().is_none() {
            return Err(NFS3ERR_ISDIR);
        }
        Ok((id, self.to_fattr3(&inode)))
    }

    #[instrument(skip(self))]
    async fn create_exclusive(
        &self,
        dirid: fileid3,
        filename: &filename3,
    ) -> Result<fileid3, nfsstat3> {
        let name = try_as_name(filename)?;
        let parent: Directory = self
            .inode_by_id(dirid)
            .await?
            .try_into()
            .map_err(|_| NFS3ERR_NOTDIR)?;

        let file_id = self
            .uploader
            .prepare(&(parent.inode_id(), name.to_owned()))
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "failed to prepare upload");
                NFS3ERR_IO
            })?;

        Ok(*file_id)
    }

    async fn mkdir(
        &self,
        dirid: fileid3,
        dirname: &filename3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        let name = try_as_name(dirname)?;
        let parent: Directory = self
            .inode_by_id(dirid)
            .await?
            .try_into()
            .map_err(|_| NFS3ERR_NOTDIR)?;

        let inode: Inode = self
            .vfs
            .create_dir(&parent, name)
            .await
            .map_err(|_| NFS3ERR_SERVERFAULT)?
            .into();

        Ok((*inode.id(), self.to_fattr3(&inode)))
    }

    async fn remove(&self, dirid: fileid3, filename: &filename3) -> Result<(), nfsstat3> {
        let inode = self.inode_by_dir_name(dirid, filename).await?;

        self.vfs.delete(inode.id()).await.map_err(|e| {
            tracing::error!(err = %e, "rm failed");
            NFS3ERR_SERVERFAULT
        })?;

        Ok(())
    }

    async fn rename(
        &self,
        from_dirid: fileid3,
        from_filename: &filename3,
        to_dirid: fileid3,
        to_filename: &filename3,
    ) -> Result<(), nfsstat3> {
        let source = self.inode_by_dir_name(from_dirid, from_filename).await?;

        let dest_parent: Directory = self
            .inode_by_id(to_dirid)
            .await?
            .try_into()
            .map_err(|_| NFS3ERR_NOTSUPP)?;

        let to_filename = try_as_name(to_filename)?;

        self.vfs.mv(source.id(), &dest_parent).await.map_err(|e| {
            tracing::error!(err = %e, "mv failed");
            NFS3ERR_SERVERFAULT
        })?;

        if to_filename != source.name() {
            // rename
            let to_filename = to_filename.to_owned();
            match self.inode_by_id(*source.id()).await? {
                Inode::Directory(dir) => {
                    let mut dir = dir.into_mut();
                    dir.set_name(to_filename);
                    self.vfs.update(dir).await
                }
                Inode::File(file) => {
                    let mut file = file.into_mut();
                    file.set_name(to_filename);
                    self.vfs.update(file).await
                }
            }
            .map_err(|e| {
                tracing::error!(err = %e, "mv failed");
                NFS3ERR_SERVERFAULT
            })?;
        }

        Ok(())
    }

    async fn readdir(
        &self,
        dirid: fileid3,
        start_after: fileid3,
        max_entries: usize,
    ) -> Result<ReadDirResult, nfsstat3> {
        let dir: Directory = self
            .inode_by_id(dirid)
            .await?
            .try_into()
            .map_err(|_| NFS3ERR_NOTDIR)?;

        let stream = self.vfs.list(&dir).await.map_err(|err| {
            tracing::error!(error = %err, "read_dir failed");
            NFS3ERR_SERVERFAULT
        })?;

        let inodes = stream.try_collect::<Vec<_>>().await.map_err(|err| {
            tracing::error!(error = %err, "read_dir failed");
            NFS3ERR_SERVERFAULT
        })?;

        let mut ret = ReadDirResult {
            entries: Vec::new(),
            end: false,
        };

        let mut start_index = 0;
        if start_after > 0 {
            if let Some(pos) = inodes.iter().position(|inode| *inode.id() == start_after) {
                start_index = pos + 1;
            } else {
                return Err(nfsstat3::NFS3ERR_BAD_COOKIE);
            }
        }
        let remaining_length = inodes.len() - start_index;

        for inode in inodes[start_index..].iter() {
            ret.entries.push(DirEntry {
                fileid: *inode.id(),
                name: inode.name().as_bytes().into(),
                attr: self.to_fattr3(inode),
            });
            if ret.entries.len() >= max_entries {
                break;
            }
        }
        if ret.entries.len() == remaining_length {
            ret.end = true;
        }

        Ok(ret)
    }

    async fn symlink(
        &self,
        _dirid: fileid3,
        _linkname: &filename3,
        _symlink: &nfspath3,
        _attr: &sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        Err(NFS3ERR_NOTSUPP)
    }

    async fn readlink(&self, _id: fileid3) -> Result<nfspath3, nfsstat3> {
        Err(NFS3ERR_NOTSUPP)
    }

    async fn fsinfo(&self, root_fileid: fileid3) -> std::result::Result<fsinfo3, nfsstat3> {
        if root_fileid != *self.root_id {
            return Err(nfsstat3::NFS3ERR_BADHANDLE);
        }
        let inode = self.inode_by_id(root_fileid).await?;

        Ok(fsinfo3 {
            obj_attributes: nfs::post_op_attr::attributes(self.to_fattr3(&inode)),
            rtmax: 1024 * 1024,
            rtpref: 1024 * 124,
            rtmult: 1024 * 1024,
            wtmax: 1024 * 1024,
            wtpref: 1024 * 1024,
            wtmult: 1024 * 1024,
            dtpref: 1024 * 1024,
            maxfilesize: 128 * 1024 * 1024 * 1024,
            time_delta: nfstime3 {
                seconds: 0,
                nseconds: 1000000,
            },
            properties: nfs::FSF_HOMOGENEOUS,
        })
    }
}

impl JanusNfsFs {
    async fn inode_by_id(&self, id: fileid3) -> Result<Inode, nfsstat3> {
        match self
            .vfs
            .inode_by_id(id.into())
            .await
            .map_err(|_| NFS3ERR_SERVERFAULT)?
        {
            Some(inode) => Ok(inode),
            None => Err(NFS3ERR_NOENT),
        }
    }

    async fn inode_by_dir_name(
        &self,
        dirid: fileid3,
        filename: &filename3,
    ) -> Result<Inode, nfsstat3> {
        let name = try_as_name(filename)?;
        let parent = match self.vfs.inode_by_id(dirid.into()).await.map_err(|e| {
            tracing::error!(err = %e, "lookup failed");
            NFS3ERR_SERVERFAULT
        })? {
            None => Err(NFS3ERR_NOENT),
            Some(Inode::File(_)) => Err(NFS3ERR_NOTDIR),
            Some(Inode::Directory(dir)) => Ok(dir),
        }?;

        let path = parent.path().join(name);

        match self.vfs.inode_by_path(&path).await.map_err(|e| {
            tracing::error!(err = %e, "lookup failed");
            NFS3ERR_SERVERFAULT
        })? {
            Some(inode) => Ok(inode),
            None => Err(NFS3ERR_NOENT),
        }
    }

    fn to_fattr3(&self, inode: &Inode) -> fattr3 {
        let size = inode.len().unwrap_or(0);
        let last_modified = to_nfsstime(inode.last_modified());

        fattr3 {
            ftype: match inode {
                Inode::Directory(_) => ftype3::NF3DIR,
                Inode::File(_) => ftype3::NF3REG,
            },
            mode: match inode {
                Inode::Directory(dir) if dir.is_root() => 0o555,
                Inode::Directory(_) => self.dir_mode,
                Inode::File(_) => self.file_mode,
            },
            nlink: 1,
            uid: match inode {
                Inode::Directory(dir) if dir.is_root() => 0,
                _ => self.uid,
            },
            gid: match inode {
                Inode::Directory(dir) if dir.is_root() => 0,
                _ => self.gid,
            },
            size,
            used: size,
            rdev: specdata3::default(),
            fsid: self.fs_id,
            fileid: *inode.id(),
            atime: last_modified,
            mtime: last_modified,
            ctime: last_modified,
        }
    }
}

fn to_nfsstime(date_time: &DateTime<Utc>) -> nfstime3 {
    nfstime3 {
        seconds: date_time.timestamp() as u32,
        nseconds: date_time.timestamp_subsec_nanos(),
    }
}

fn try_as_name(name: &filename3) -> Result<&Name, nfsstat3> {
    let str = std::str::from_utf8(name).map_err(|_| NFS3ERR_SERVERFAULT)?;
    Ok(str.try_into().map_err(|_| NFS3ERR_SERVERFAULT)?)
}
