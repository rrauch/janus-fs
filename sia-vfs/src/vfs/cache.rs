use crate::vfs::path::VfsPath;
use crate::vfs::{Inode, InodeId};
use bon::Builder;
use moka::future::Cache as MokaCache;
use std::sync::Arc;
use std::time::Duration;

type PathCache = MokaCache<VfsPath, Option<InodeId>>;
type InodeCache = MokaCache<InodeId, Option<Inode>>;

#[derive(Builder, Clone, Debug)]
pub struct CacheSettings {
    #[builder(default = 1000)]
    path_cache_capacity: u64,
    #[builder(default = Duration::from_secs(3600))]
    path_cache_ttl: Duration,
    #[builder(default = 1000)]
    inode_cache_capacity: u64,
    #[builder(default = Duration::from_secs(3600))]
    inode_cache_ttl: Duration,
}

impl Default for CacheSettings {
    fn default() -> Self {
        Self::builder().build()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Cache(Arc<Inner>);

impl Cache {
    pub fn new(settings: &CacheSettings) -> Self {
        let path_cache = MokaCache::builder()
            .name("path_cache")
            .support_invalidation_closures()
            .max_capacity(settings.path_cache_capacity)
            .time_to_live(settings.path_cache_ttl)
            .build();
        let inode_cache = MokaCache::builder()
            .name("inode_cache")
            .support_invalidation_closures()
            .max_capacity(settings.inode_cache_capacity)
            .time_to_live(settings.inode_cache_ttl)
            .build();

        Self(Arc::new(Inner {
            path_cache,
            inode_cache,
        }))
    }

    #[inline]
    pub fn path_cache(&self) -> &PathCache {
        &self.0.path_cache
    }

    #[inline]
    pub fn inode_cache(&self) -> &InodeCache {
        &self.0.inode_cache
    }
}

#[derive(Debug)]
struct Inner {
    path_cache: PathCache,
    inode_cache: InodeCache,
}
