use crate::FoyerError;
use crate::disk_cache::Error::InvalidConfig;
use blocking::unblock;
use equivalent::Equivalent;
use foyer::{
    AdmitAll, BlockEngineConfig, Compression, DeviceBuilder, FifoPicker, FsDeviceBuilder,
    HybridCache, HybridCacheBuilder, HybridCachePolicy, Load, PsyncIoEngineConfig, RecoverMode,
    StorageFilter, StorageKey, StorageValue,
};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::cmp::{max, min};
use std::fs::File;
use std::hash::Hash;
use std::path::{Path, PathBuf};
use thiserror::Error;

const COMPAT_FILE_NAME: &str = "_janus_cache.ctx";
const MAX_COMPAT_FILE_SIZE: u64 = 1024 * 10;

pub struct DiskCache<K: StorageKey, V: StorageValue> {
    storage_dir: PathBuf,
    hybrid_cache: HybridCache<K, V>,
    content_type: String,
    comp_version: usize,
}

impl<K: StorageKey, V: StorageValue + Clone> DiskCache<K, V> {
    pub async fn new(
        name: &'static str,
        storage_dir: impl AsRef<Path>,
        max_disk_space: u64,
        max_mem_buf: usize,
        content_type: String,
        comp_version: usize,
    ) -> Result<Self, Error> {
        let storage_dir = storage_dir.as_ref().to_path_buf();

        let hybrid_cache =
            build_hybrid_cache(name, max_disk_space, &storage_dir, max_mem_buf).await?;

        Ok(Self {
            storage_dir,
            hybrid_cache,
            content_type,
            comp_version,
        })
    }

    pub async fn init(&mut self) -> Result<(), Error> {
        if !self.check_compat_file().await {
            // not compatible, clear cached entries
            self.hybrid_cache.clear().await?;
            self.create_compat_file().await?;
        }
        // wait for disk cache to become ready
        self.hybrid_cache.storage().wait().await;
        Ok(())
    }

    pub async fn get<T: TryFrom<V>>(
        &self,
        key: impl Equivalent<K> + Hash,
    ) -> Result<Option<T>, std::io::Error> {
        let value = match self.hybrid_cache.storage().load(&key).await {
            Ok(Load::Entry { value, .. }) => value,
            Ok(Load::Piece { piece, .. }) => piece.value().clone(),
            Ok(_) => return Ok(None),
            Err(err) => {
                // invalid cache entry detected; remove
                self.hybrid_cache.storage().delete(&key);
                return Err(std::io::Error::other(err));
            }
        };
        
        match T::try_from(value) {
            Ok(value) => Ok(Some(value)),
            Err(_) => {
                // invalid cache entry detected; remove
                self.hybrid_cache.storage().delete(&key);
                Err(std::io::Error::other("invalid cache entry"))
            }
        }
    }

    pub async fn insert(
        &self,
        key: impl Into<K>,
        value: impl Into<V>,
    ) -> Result<(), std::io::Error> {
        // todo: find out if inserting into memory can be avoided here
        let piece = self
            .hybrid_cache
            .memory()
            .insert(key.into(), value.into())
            .piece();
        self.hybrid_cache.storage().enqueue(piece, true);
        Ok(())
    }

    pub async fn invalidate(&self, key: impl Equivalent<K> + Hash) -> Result<(), std::io::Error> {
        self.hybrid_cache.storage().delete(&key);
        Ok(())
    }

    async fn check_compat_file(&self) -> bool {
        let path = self.storage_dir.join(COMPAT_FILE_NAME);
        if let Some(content) = read_ctx_file(path).await {
            if &content.content_type != &self.content_type {
                return false;
            }
            if content.compatibility_version != self.comp_version {
                return false;
            }
            return true;
        }
        false
    }

    async fn create_compat_file(&self) -> Result<(), Error> {
        let path = self.storage_dir.join(COMPAT_FILE_NAME);
        let content = ContextFileContent {
            content_type: Cow::Owned(self.content_type.clone()),
            compatibility_version: self.comp_version,
        };
        Ok(write_compat_file(path, content).await?)
    }
}

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Foyer(#[from] FoyerError),
    #[error(transparent)]
    InvalidConfig(#[from] InvalidConfigError),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
}

#[derive(Error, Debug)]
pub enum InvalidConfigError {
    #[error(transparent)]
    DiskSpace(#[from] DiskSpaceConfigError),
}

#[derive(Error, Debug)]
pub enum DiskSpaceConfigError {
    #[error("given max disk space '{0}' exceeds allowed maximum '{1}'")]
    TooLarge(u64, u64),
    #[error("given max disk space '{0}' does not reach minimum of '{1}'")]
    TooSmall(usize, usize),
}

#[derive(Serialize, Deserialize)]
struct ContextFileContent<'a> {
    content_type: Cow<'a, str>,
    compatibility_version: usize,
}

async fn read_ctx_file(path: PathBuf) -> Option<ContextFileContent<'static>> {
    unblock(move || {
        let fs_metadata = std::fs::metadata(&path)?;
        if fs_metadata.len() > MAX_COMPAT_FILE_SIZE {
            return Err(std::io::Error::other("max ctx file size exceeded"))?;
        }
        let mut file = File::open(&path)?;
        Ok::<_, std::io::Error>(
            serde_json::from_reader(&mut file).map_err(|e| std::io::Error::other(e))?,
        )
    })
    .await
    .ok()
}

async fn write_compat_file(
    path: PathBuf,
    content: ContextFileContent<'static>,
) -> std::io::Result<()> {
    unblock(move || {
        let mut file = File::options().write(true).create(true).open(&path)?;
        serde_json::ser::to_writer(&mut file, &content).map_err(|e| std::io::Error::other(e))
    })
    .await
}

async fn build_hybrid_cache<K: StorageKey, V: StorageValue>(
    name: &'static str,
    max_disk_space: u64,
    disk_path: impl AsRef<Path>,
    max_mem_buf: usize,
) -> Result<HybridCache<K, V>, Error> {
    const ALIGN: usize = 4096;
    const MAX_DISK_CAPACITY: u64 = usize::MAX as u64 - ALIGN as u64;
    const MIN_MEM_BUF_SIZE: usize = 256 * 1024;
    const MIN_BLOCK_SIZE: usize = MIN_MEM_BUF_SIZE * 4;
    const MAX_BLOCK_SIZE: usize = 16 * 1024 * 1024;

    fn align(value: usize) -> usize {
        (value + ALIGN - 1) & !(ALIGN - 1)
    }

    if max_disk_space > MAX_DISK_CAPACITY {
        Err(InvalidConfig(
            DiskSpaceConfigError::TooLarge(max_disk_space, MAX_DISK_CAPACITY).into(),
        ))?
    }

    let disk_capacity = align(max_disk_space as usize);

    if disk_capacity < { MIN_BLOCK_SIZE * 5 } {
        Err(InvalidConfig(
            DiskSpaceConfigError::TooSmall(disk_capacity, MIN_BLOCK_SIZE * 5).into(),
        ))?
    };

    // makes sure block size is aligned and within valid bounds
    let block_size = max(
        MIN_BLOCK_SIZE,
        min(MAX_BLOCK_SIZE, align(disk_capacity / 5)),
    );

    let mem_buf_size = min(max(MIN_MEM_BUF_SIZE, align(max_mem_buf)), block_size);

    let storage_dir = disk_path.as_ref().to_path_buf();
    let device = unblock(move || {
        FsDeviceBuilder::new(&storage_dir)
            .with_capacity(disk_capacity)
            .build()
    })
    .await?;

    Ok(HybridCacheBuilder::new()
        .with_name(name)
        .with_policy(HybridCachePolicy::WriteOnInsertion)
        .memory(1)
        .with_shards(1)
        .storage()
        .with_io_engine_config(PsyncIoEngineConfig::new())
        .with_engine_config(
            BlockEngineConfig::new(device)
                .with_flushers(1)
                .with_reclaimers(1)
                .with_buffer_pool_size(mem_buf_size)
                .with_block_size(block_size)
                .with_eviction_pickers(vec![Box::<FifoPicker>::default()])
                .with_admission_filter(StorageFilter::new().with_condition(AdmitAll))
                .with_reinsertion_filter(StorageFilter::new().with_condition(AdmitAll))
                .with_tombstone_log(true),
        )
        .with_recover_mode(RecoverMode::Quiet)
        .with_compression(Compression::None)
        .build()
        .await?)
}
