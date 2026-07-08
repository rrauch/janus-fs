use crate::blob::BlobId;
use crate::chunk::ChunkId;
use crate::object::ObjectId;
use crate::vfs::cache::Cache;
use crate::vfs::commit::CommitId;
use crate::vfs::directory::{DirectoryBody, DirectoryMut};
use crate::vfs::entity::{EntityId, EntityKey, Revision};
use crate::vfs::{BranchName, Head, Inode, InodeId};
use janus_io::RemoteStorage;
use sqlx::migrate::MigrateError;
use sqlx::pool::PoolConnection;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{ConnectOptions, Connection, Error as SqlxError};
use sqlx::{Pool, Transaction as SqlxTransaction};
use sqlx::{Sqlite, SqliteConnection};
use std::borrow::Cow;
use std::fmt::{Display, Formatter};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tracing::log::LevelFilter;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    SqlxError(#[from] SqlxError),
    #[error(transparent)]
    MigrateError(#[from] MigrateError),
    #[error(transparent)]
    DbStateError(#[from] DbStateError),
    #[error(transparent)]
    DataError(#[from] DataError),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
}

#[derive(Error, Debug)]
pub enum DbStateError {
    #[error("invalid page size: '{0}'")]
    InvalidPageSize(String),
    #[error("connection pool already closed")]
    PoolIsClosed,
}

#[derive(Error, Debug)]
pub enum DataError {
    #[error("conversion failed: {0}")]
    ConversionError(Cow<'static, str>),
    #[error("entity not found: [{0:?}]")]
    EntityNotFound(EntityKey),
    #[error("invalid entity id")]
    InvalidEntityId,
    #[error("invalid revision")]
    InvalidRevision,
    #[error("entity id and/or revision mismatch")]
    EntityMismatch,
    #[error("inode [{0}] not found")]
    InodeNotFound(InodeId),
    #[error("blob [{0}] not found")]
    BlobNotFound(BlobId),
    #[error("wrong number of rows affected: {actual} != {expected}")]
    UnexpectedAffectedRows { expected: u64, actual: u64 },
    #[error("dirty file detected: inode [{0}] should be directory or root")]
    DirtyFile(InodeId),
    #[error("object missing")]
    MissingObject,
    #[error("blob id mismatch: [{expected}] != [{actual}]")]
    BlobIdMismatch { expected: BlobId, actual: BlobId },
    #[error("chunk id mismatch: [{expected}] != [{actual}]")]
    ChunkIdMismatch { expected: ChunkId, actual: ChunkId },
    #[error("commit id mismatch: [{expected}] != [{actual}]")]
    CommitIdMismatch {
        expected: CommitId,
        actual: CommitId,
    },
    #[error("chunk [{0}] not found")]
    ChunkNotFound(ChunkId),
    #[error("commit [{0}] not found")]
    CommitNotFound(CommitId),
    #[error("object [{0}] not found")]
    ObjectNotFound(ObjectId),
    #[error("invalid remote_location: [{0}]")]
    InvalidRemoteLocation(String),
    #[error("storage mode unsupported")]
    UnsupportedStorageMode,
    #[error("head entry [{0}] not found")]
    HeadEntryNotFound(Head),
}

pub struct ReadOnly(PoolConnection<Sqlite>, Arc<RemoteStorage>);

impl AsMut<SqliteConnection> for ReadOnly {
    fn as_mut(&mut self) -> &mut SqliteConnection {
        &mut self.0
    }
}

pub struct ReadWrite(
    SqlxTransaction<'static, Sqlite>,
    Cache,
    Arc<RemoteStorage>,
    Option<BranchName>,
);

impl AsMut<SqliteConnection> for ReadWrite {
    #[inline]
    fn as_mut(&mut self) -> &mut SqliteConnection {
        &mut self.0
    }
}

pub(crate) trait Read: AsMut<SqliteConnection> {
    #[inline]
    fn conn(&mut self) -> &mut SqliteConnection {
        self.as_mut()
    }

    fn remote_storage(&self) -> &RemoteStorage;
}
impl Read for Transaction<ReadOnly> {
    fn remote_storage(&self) -> &RemoteStorage {
        &self.0.1
    }
}

pub(crate) trait Write: Read {}
impl Read for Transaction<ReadWrite> {
    fn remote_storage(&self) -> &RemoteStorage {
        &self.0.2
    }
}
impl Write for Transaction<ReadWrite> {}

pub(crate) trait TxScope: AsMut<SqliteConnection> + Send + Sync + Unpin + 'static {}
impl<T: AsMut<SqliteConnection> + Send + Sync + Unpin + 'static> TxScope for T {}

#[repr(transparent)]
pub(crate) struct Transaction<Scope: TxScope>(Scope);

impl<C: TxScope> Transaction<C>
where
    Self: Read,
{
    pub async fn is_empty(&mut self) -> Result<bool, Error> {
        Ok(
            sqlx::query!("SELECT COUNT(*) AS \"num_rows:i64\" FROM config")
                .fetch_one(self.conn())
                .await?
                .num_rows
                == 0,
        )
    }
}

impl Transaction<ReadWrite> {
    #[inline]
    pub async fn commit(mut self) -> Result<(), Error> {
        self.process_dirty_inodes().await?;

        if let Some(branch_name) = self.0.3.clone() {
            if !self.is_empty().await? && self.update_commit(branch_name).await?.is_some() {
                self.maybe_update_config().await?;
            }
        }

        let changelog = self.get_changelog().await?;
        self.clear_changelog().await?;
        self.0.0.commit().await?;

        // invalidate caches after successful commit
        if !changelog.affected_inode_ids.is_empty() {
            self.0.1.path_cache().invalidate_all();
            self.0
                .1
                .inode_cache()
                .invalidate_entries_if(move |k, _| changelog.affected_inode_ids.contains(k))
                .expect("invalidation closure active");
        }
        if !changelog.affected_chunk_ids.is_empty() {
            self.0
                .1
                .chunk_cache()
                .invalidate_entries_if(move |k, _| changelog.affected_chunk_ids.contains(k))
                .expect("invalidation closure active");
        }

        Ok(())
    }

    async fn process_dirty_inodes(&mut self) -> Result<(), Error> {
        // process dirty inodes, depth-first
        loop {
            let inode_ids = sqlx::query!(
                "SELECT v.inode_id FROM vfs v
                  WHERE v.is_dirty = 1
                    AND NOT EXISTS (
                    SELECT 1
                    FROM vfs c
                    WHERE c.parent = v.inode_id
                      AND c.is_dirty = 1
                    );
                "
            )
            .fetch_all(self.conn())
            .await?
            .into_iter()
            .map(|r| InodeId::new(r.inode_id as u64))
            .collect::<Vec<_>>();

            if inode_ids.is_empty() {
                // nothing left to do
                break;
            }

            for inode_id in inode_ids {
                self.process_dirty_inode(inode_id).await?;
            }
        }

        Ok(())
    }

    async fn process_dirty_inode(&mut self, inode_id: InodeId) -> Result<(), Error> {
        let (name, entity_key) = match self
            .inode_by_id(inode_id)
            .await?
            .ok_or_else(|| DataError::InodeNotFound(inode_id))?
        {
            Inode::Directory(dir) => {
                let name = dir.name().to_owned();
                let entity_key = self.update_directory(dir.into_mut()).await?;
                (name, entity_key)
            }
            Inode::File(_) => {
                return Err(DataError::DirtyFile(inode_id))?;
            }
        };
        self.update_inode(inode_id, &name, &entity_key).await?;
        Ok(())
    }

    async fn update_directory(&mut self, mut dir: DirectoryMut) -> Result<EntityKey, Error> {
        let parent_id = *dir.inode_id().deref() as i64;
        let entries = sqlx::query!(
            "SELECT entity_id, entity_rev FROM vfs WHERE parent = ?",
            parent_id
        )
        .fetch_all(self.conn())
        .await?
        .into_iter()
        .map(|r| -> Result<EntityKey, Error> {
            Ok(EntityKey::new(
                EntityId::try_from_bytes(r.entity_id)
                    .ok_or_else(|| DataError::ConversionError("invalid entity id".into()))?,
                Revision::try_from_bytes(r.entity_rev)
                    .ok_or_else(|| DataError::ConversionError("invalid entity revision".into()))?,
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
        dir.set_body(DirectoryBody::new(entries));
        self.register_entity(dir.freeze()).await
    }

    #[inline]
    #[allow(dead_code)]
    pub async fn rollback(self) -> Result<(), Error> {
        Ok(self.0.0.rollback().await?)
    }

    pub(crate) async fn housekeeping(&mut self) -> Result<(), Error> {
        sqlx::query!("DELETE FROM temp_file_handle")
            .execute(self.conn())
            .await?;

        Ok(())
    }

    async fn clear_changelog(&mut self) -> Result<(), Error> {
        sqlx::query!("DELETE FROM change_log")
            .execute(self.conn())
            .await?;
        Ok(())
    }

    async fn get_changelog(&mut self) -> Result<Changelog, Error> {
        let mut affected_inode_ids = vec![];
        let mut affected_chunk_ids = vec![];

        sqlx::query!("SELECT DISTINCT type, key1 FROM change_log")
            .fetch_all(self.conn())
            .await?
            .into_iter()
            .try_for_each(|r| {
                match r.r#type.as_str() {
                    "C" => {
                        affected_chunk_ids.push(ChunkId::try_from_bytes(r.key1).ok_or_else(
                            || DataError::ConversionError("invalid chunk_id".into()),
                        )?);
                        Ok(())
                    }
                    "I" => {
                        // sqlite should return a string encoded integer here
                        affected_inode_ids.push(InodeId::new(
                            str::from_utf8(r.key1.as_slice())
                                .map_err(|e| DataError::ConversionError(e.to_string().into()))?
                                .parse()
                                .map_err(|e| {
                                    DataError::ConversionError(
                                        format!("invalid inode_id: {}", e).into(),
                                    )
                                })?,
                        ));
                        Ok(())
                    }
                    other => Err(Error::DataError(DataError::ConversionError(
                        format!("invalid changelog type: {}", other).into(),
                    ))),
                }
            })?;

        Ok(Changelog {
            affected_inode_ids,
            affected_chunk_ids,
        })
    }
}

impl<Scope: TxScope> AsMut<SqliteConnection> for Transaction<Scope> {
    #[inline]
    fn as_mut(&mut self) -> &mut SqliteConnection {
        self.0.as_mut()
    }
}

struct Changelog {
    affected_inode_ids: Vec<InodeId>,
    affected_chunk_ids: Vec<ChunkId>,
}

#[derive(Debug, Clone)]
#[repr(transparent)]
pub(crate) struct Db(Arc<DbInner>);

#[derive(Debug)]
struct SqlitePool {
    writer: Pool<Sqlite>,
    reader: Pool<Sqlite>,
}

impl SqlitePool {
    async fn read(
        &self,
        remote_storage: Arc<RemoteStorage>,
    ) -> Result<Transaction<ReadOnly>, SqlxError> {
        Ok(Transaction(ReadOnly(
            self.reader.acquire().await?,
            remote_storage,
        )))
    }

    async fn write(
        &self,
        cache: Cache,
        remote_storage: Arc<RemoteStorage>,
        branch_name: Option<BranchName>,
    ) -> Result<Transaction<ReadWrite>, SqlxError> {
        Ok(Transaction(ReadWrite(
            self.writer.begin().await?,
            cache,
            remote_storage,
            branch_name,
        )))
    }
}

#[derive(Debug)]
struct DbInner {
    pool: Option<SqlitePool>,
    cache: Cache,
    remote_storage: Arc<RemoteStorage>,
    head: Head,
}

impl Drop for DbInner {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.take() {
            tokio::spawn(async move {
                pool.reader.close().await;
                pool.writer.close().await;
            });
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum PageSize {
    Ps512 = 512,
    Ps1024 = 1024,
    Ps2048 = 2048,
    Ps4096 = 4096,
    Ps8192 = 8192,
    Ps16384 = 16384,
    Ps32768 = 32768,
    Ps65536 = 65536,
}

impl Default for PageSize {
    fn default() -> Self {
        PageSize::Ps32768
    }
}

impl Display for PageSize {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value())
    }
}

impl PageSize {
    pub fn value(&self) -> u32 {
        self.clone() as u32
    }
}

impl TryFrom<u32> for PageSize {
    type Error = u32;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            512 => Ok(PageSize::Ps512),
            1024 => Ok(PageSize::Ps1024),
            2048 => Ok(PageSize::Ps2048),
            4096 => Ok(PageSize::Ps4096),
            8192 => Ok(PageSize::Ps8192),
            16384 => Ok(PageSize::Ps16384),
            32768 => Ok(PageSize::Ps32768),
            65536 => Ok(PageSize::Ps65536),
            _ => Err(value),
        }
    }
}

impl Db {
    pub(super) async fn new(
        db_file: PathBuf,
        max_connections: u8,
        page_size: PageSize,
        cache: Cache,
        remote_storage: Arc<RemoteStorage>,
        head: Head,
    ) -> Result<Self, Error> {
        let pool = db_init(db_file.as_path(), max_connections, page_size).await?;

        let mut tx = pool
            .write(
                cache.clone(),
                remote_storage.clone(),
                head.maybe_branch_name(),
            )
            .await?;
        tx.housekeeping().await?;
        tx.commit().await?;

        Ok(Self(Arc::new(DbInner {
            pool: Some(pool),
            cache,
            remote_storage,
            head,
        })))
    }

    pub async fn read(&self) -> Result<Transaction<ReadOnly>, Error> {
        Ok(self
            .0
            .pool
            .as_ref()
            .ok_or_else(|| DbStateError::PoolIsClosed)?
            .read(self.0.remote_storage.clone())
            .await?)
    }

    pub async fn write(&self) -> Result<Transaction<ReadWrite>, Error> {
        let mut tx = self
            .0
            .pool
            .as_ref()
            .ok_or_else(|| DbStateError::PoolIsClosed)?
            .write(
                self.0.cache.clone(),
                self.0.remote_storage.clone(),
                self.0.head.maybe_branch_name(),
            )
            .await?;
        tx.clear_changelog().await?;
        Ok(tx)
    }
}

async fn db_init(
    db_file: &Path,
    max_connections: u8,
    page_size: PageSize,
) -> Result<SqlitePool, Error> {
    prepare_db(db_file, page_size).await?;

    let writer = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with({
            SqliteConnectOptions::new()
                .create_if_missing(false)
                .filename(db_file)
                .log_statements(LevelFilter::Trace)
                .journal_mode(SqliteJournalMode::Wal)
                .foreign_keys(true)
                .pragma("recursive_triggers", "ON")
                .busy_timeout(Duration::from_millis(100))
                .shared_cache(true)
        })
        .await?;

    let reader = SqlitePoolOptions::new()
        .max_connections(max_connections as u32)
        .connect_with({
            SqliteConnectOptions::new()
                .create_if_missing(false)
                .filename(db_file)
                .log_statements(LevelFilter::Trace)
                .journal_mode(SqliteJournalMode::Wal)
                .foreign_keys(true)
                .pragma("recursive_triggers", "ON")
                .busy_timeout(Duration::from_millis(100))
                .shared_cache(true)
                .pragma("query_only", "ON")
        })
        .await?;

    Ok(SqlitePool { writer, reader })
}

async fn connect_with_retry<const MAX_ATTEMPTS: usize>(
    opts: &SqliteConnectOptions,
) -> Result<SqliteConnection, Error> {
    fn is_locked(e: &sqlx::Error) -> bool {
        matches!(
            e,
            sqlx::Error::Database(db) if db.code().as_deref() == Some("5") // database is locked
        )
    }

    let mut attempts = 0;
    loop {
        match SqliteConnection::connect_with(opts).await {
            Ok(conn) => return Ok(conn),
            Err(e) => {
                if is_locked(&e) && attempts < MAX_ATTEMPTS {
                    attempts += 1;
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
                return Err(e.into());
            }
        }
    }
}

async fn prepare_db(db_file: &Path, page_size: PageSize) -> Result<(), Error> {
    let opts = SqliteConnectOptions::new()
        .create_if_missing(true)
        .filename(db_file)
        .log_statements(LevelFilter::Trace)
        .journal_mode(SqliteJournalMode::Delete)
        .foreign_keys(true)
        .pragma("recursive_triggers", "ON")
        .busy_timeout(Duration::from_millis(1000))
        .shared_cache(false);

    let mut conn = connect_with_retry::<10>(&opts).await?;

    async { sqlx::migrate!("./migrations").run(&mut conn).await }.await?;

    async fn get_page_size(conn: &mut SqliteConnection) -> Result<PageSize, Error> {
        Ok(sqlx::query!("PRAGMA page_size")
            .fetch_one(conn)
            .await?
            .page_size
            .map(|c| PageSize::try_from(c as u32).ok())
            .flatten()
            .ok_or(DbStateError::InvalidPageSize(
                "unable to get page_size from database".to_string(),
            ))?)
    }

    let current_page_size = get_page_size(&mut conn).await?;
    conn.close().await?;

    if current_page_size != page_size {
        tracing::info!(
            required = %page_size,
            actual = %current_page_size,
            "database page size needs adjusting",
        );

        {
            let opts = opts.clone().page_size(page_size.value());
            let mut conn = SqliteConnection::connect_with(&opts).await?;
            sqlx::query!("VACUUM").execute(&mut conn).await?;
            conn.close().await?;
        }

        let mut conn = SqliteConnection::connect_with(&opts).await?;
        let current_page_size = get_page_size(&mut conn).await?;
        if current_page_size != page_size {
            tracing::error!(
                required = %page_size,
                actual = %current_page_size,
                "database page size adjustment failed",
            );
        }
        conn.close().await?;
    }
    Ok(())
}
