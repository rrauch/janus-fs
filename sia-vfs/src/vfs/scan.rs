use crate::object;
use crate::object::metadata::Metadata;
use crate::sync::{METADATA_VFS_ID, METADATA_VFS_VERSION};
use crate::vfs::config::Config;
use crate::vfs::{Vfs, VfsError, VfsId, config};
use futures_util::{StreamExt, TryStreamExt};
use sia_io::Client as Sia;
use std::collections::HashMap;
use std::str::FromStr;

impl Vfs {
    pub async fn scan(sia_client: &Sia) -> Result<Vec<Config>, VfsError> {
        let mut config_objects = sia_client
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

                if metadata.get(object::METADATA_VFS_OBJECT_TYPE)
                    != Some(config::METADATA_OBJECT_TYPE)
                {
                    // not a config object
                    return Ok(None);
                }

                let vfs_id = match metadata
                    .get(METADATA_VFS_ID)
                    .map(|id| VfsId::from_str(id).ok())
                    .flatten()
                {
                    Some(vfs_id) => vfs_id,
                    None => return Ok(None),
                };

                Ok(Some((o, vfs_id)))
            })
            .boxed();

        let mut configs: HashMap<VfsId, Config> = HashMap::new();
        let mut id = 0u64;
        while let Some(Ok((o, vfs_id))) = config_objects.next().await {
            let config = Config::load_from_backend(id.into(), o.id(), sia_client).await?;
            if config.vfs_id() != &vfs_id {
                continue;
            }

            match configs.get(&vfs_id) {
                Some(existing) if existing.last_modified() >= config.last_modified() => {}
                _ => {
                    configs.insert(vfs_id, config);
                }
            }

            id += 1;
        }

        Ok(configs.into_values().collect())
    }
}

#[cfg(test)]
mod tests {
    use crate::vfs::Vfs;
    use sia_io::Client as Sia;
    use std::time::Duration;

    #[tokio::test]
    async fn single_vfs() -> anyhow::Result<()> {
        let sia_client = Sia::mock().await;
        let vfs_id = Vfs::create_new(None, &sia_client).await?;

        let configs = Vfs::scan(&sia_client).await?;
        assert_eq!(configs.len(), 1);

        let config = configs.get(0).unwrap();
        assert_eq!(config.vfs_id(), &vfs_id);

        Ok(())
    }

    #[tokio::test]
    async fn multi_vfs() -> anyhow::Result<()> {
        let sia_client = Sia::mock().await;
        let vfs_id1 = Vfs::create_new(None, &sia_client).await?;
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let vfs_id2 = Vfs::create_new(None, &sia_client).await?;
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let vfs_id3 = Vfs::create_new(None, &sia_client).await?;

        let configs = Vfs::scan(&sia_client).await?;
        assert_eq!(configs.len(), 3);

        assert!(configs.iter().find(|c| c.vfs_id() == &vfs_id1).is_some());
        assert!(configs.iter().find(|c| c.vfs_id() == &vfs_id2).is_some());
        assert!(configs.iter().find(|c| c.vfs_id() == &vfs_id3).is_some());

        Ok(())
    }

    #[tokio::test]
    async fn empty() -> anyhow::Result<()> {
        let sia_client = Sia::mock().await;
        let configs = Vfs::scan(&sia_client).await?;
        assert!(configs.is_empty());

        Ok(())
    }
}
