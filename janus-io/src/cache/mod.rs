pub mod chunk;
pub mod metadata;

use crate::cache::chunk::{ChunkCache, L2ChunkCache};
use crate::cache::metadata::{L2MetadataCache, MetadataCache};
use bon::bon;
use bytesize::ByteSize;
use moka::future::Cache as MokaCache;
use moka::future::CacheBuilder as MokaCacheBuilder;
use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct Cache(Arc<Inner>);

#[derive(Debug)]
struct Inner {
    metadata: MetadataCache,
    chunk: ChunkCache,
}

impl Default for Cache {
    fn default() -> Self {
        Self::builder().build()
    }
}

#[bon]
impl Cache {
    #[builder]
    pub fn new(
        #[builder(default = ByteSize::mib(8))] metadata_max_mem: ByteSize,
        #[builder(default = Duration::from_secs(3600 * 24))] metadata_max_ttl: Duration,
        #[builder(with = |l2: impl L2MetadataCache + 'static| Box::new(l2))]
        metadata_l2_cache: Option<Box<dyn L2MetadataCache>>,
        #[builder(default = ByteSize::mib(16))] chunk_max_mem: ByteSize,
        #[builder(default = Duration::from_secs(3600 * 24))] chunk_max_ttl: Duration,
        #[builder(with = |l2: impl L2ChunkCache + 'static| Box::new(l2))] chunk_l2_cache: Option<
            Box<dyn L2ChunkCache>,
        >,
    ) -> Self {
        let metadata_cache = MetadataCache::new(
            "meta_l1_cache",
            metadata_max_mem,
            metadata_max_ttl,
            metadata_l2_cache,
        );

        let chunk_cache = ChunkCache::new(
            "chunk_l1_cache",
            chunk_max_mem,
            chunk_max_ttl,
            chunk_l2_cache,
        );

        Self(Arc::new(Inner {
            metadata: metadata_cache,
            chunk: chunk_cache,
        }))
    }
}

trait HasWeight {
    fn weigh(&self) -> usize;
}

struct InnerCache<
    K: HasWeight + Eq + Hash + Send + Sync + 'static,
    V: HasWeight + Send + Sync + Clone + 'static,
    L2: Send + Sync + 'static,
> {
    l1: MokaCache<K, V>,
    l2: Option<L2>,
}

impl<
    K: HasWeight + Eq + Hash + Send + Sync + 'static,
    V: HasWeight + Send + Sync + Clone + 'static,
    L2: Send + Sync,
> InnerCache<K, V, L2>
{
    fn new(name: &str, max_mem: ByteSize, max_ttl: Duration, l2: Option<L2>) -> Self {
        let l1 = MokaCacheBuilder::new(max_mem.0)
            .name(name)
            .weigher(|k: &K, v: &V| (k.weigh() + v.weigh()).min(u32::MAX as usize) as u32)
            .time_to_live(max_ttl)
            .build();
        Self { l1, l2 }
    }
}

impl<
    K: HasWeight + Eq + Hash + Send + Sync + 'static,
    V: HasWeight + Send + Sync + Clone + 'static,
    L2: Send + Sync,
> Debug for InnerCache<K, V, L2>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("InnerCache")
    }
}
