use crate::object::metadata::Metadata;
use crate::sync::{METADATA_VFS_ID, METADATA_VFS_VERSION};
use crate::vfs::{Vfs, VfsError, VfsId};
use futures_util::{StreamExt, TryStreamExt};
use sia_io::Client as Sia;
use std::str::FromStr;

impl Vfs {
    pub async fn delete_fs(vfs_id: &VfsId, sia_client: &Sia) -> Result<usize, VfsError> {
        let mut deletable_objects = sia_client
            .list_objects()
            .try_filter_map(move |o| async move {
                let metadata: Metadata = if let Some(metadata) = o.metadata().try_into().ok() {
                    metadata
                } else {
                    return Ok(None);
                };

                if metadata.get(METADATA_VFS_VERSION) != Some("1") {
                    // not a known / supported object
                    return Ok(None);
                }

                match metadata
                    .get(METADATA_VFS_ID)
                    .map(|id| VfsId::from_str(id).ok())
                    .flatten()
                {
                    Some(object_vfs) if &object_vfs == vfs_id => Ok(Some(o)),
                    _ => Ok(None),
                }
            })
            .boxed();

        let mut deleted_objects = 0;
        while let Some(Ok(object)) = deletable_objects.next().await {
            sia_client
                .delete_object(object.id())
                .await
                .map_err(std::io::Error::other)?;
            deleted_objects += 1;
        }
        Ok(deleted_objects)
    }
}

#[cfg(test)]
mod tests {
    use crate::vfs::{Vfs, VfsId};
    use sia_io::Client as Sia;
    use std::time::Duration;

    #[tokio::test]
    async fn single_delete() -> anyhow::Result<()> {
        let sia_client = Sia::mock().await;
        let vfs_id = Vfs::create_new(None, &sia_client).await?;

        let deleted = Vfs::delete_fs(&vfs_id, &sia_client).await?;
        assert_eq!(deleted, 3);
        Ok(())
    }

    #[tokio::test]
    async fn delete_all() -> anyhow::Result<()> {
        let sia_client = Sia::mock().await;
        let id1 = Vfs::create_new(None, &sia_client).await?;
        tokio::time::sleep(Duration::from_millis(500)).await;
        let id2 = Vfs::create_new(None, &sia_client).await?;
        tokio::time::sleep(Duration::from_millis(500)).await;
        let id3 = Vfs::create_new(None, &sia_client).await?;
        tokio::time::sleep(Duration::from_millis(500)).await;

        assert_eq!(Vfs::scan(&sia_client).await?.len(), 3);

        let mut deleted = Vfs::delete_fs(&id1, &sia_client).await?;
        deleted += Vfs::delete_fs(&id2, &sia_client).await?;
        deleted += Vfs::delete_fs(&id3, &sia_client).await?;
        assert_eq!(deleted, 9);

        assert!(Vfs::scan(&sia_client).await?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn nonexistent() -> anyhow::Result<()> {
        let sia_client = Sia::mock().await;
        let vfs_id = VfsId::generate();

        assert_eq!(Vfs::delete_fs(&vfs_id, &sia_client).await?, 0);
        Ok(())
    }
}
