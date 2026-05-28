pub mod cache;
pub mod directory;
pub mod entity;
pub mod file;
pub mod path;

use crate::blob::{Blob, BlobId, BlobMut};
use crate::chunk::chunk_map::ChunkMap;
use crate::chunk::{Chunk, ChunkId, ChunkSink, ChunkSource};
use crate::db::{
    DataError, Db, Error as DbError, PageSize, Read as DbRead, ReadOnly as DbReadOnly,
    ReadWrite as DbReadWrite, Transaction, TxScope, Write as DbWrite,
};
use crate::vfs::cache::{Cache, CacheSettings};
use crate::vfs::directory::Directory;
use crate::vfs::entity::{
    DraftEntity, Entity, EntityHandler, EntityId, EntityKey, EntityMut, Revision,
};
use crate::vfs::file::{File, Reaper};
use crate::vfs::path::VfsPath;
use async_trait::async_trait;
use bytemuck::TransparentWrapper;
use chrono::{DateTime, Utc};
use derive_where::derive_where;
use std::borrow::{Borrow, Cow};
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::io::Error;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;
use twox_hash::XxHash3_64;

pub(crate) const ROOT_INODE_ID: InodeId = InodeId(1);

#[derive(Debug, Error)]
pub enum VfsError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    NameError(#[from] NameError),
    #[error("root cannot be deleted")]
    DeleteRootError,
    #[error("root cannot be moved")]
    MoveRootError,
    #[error("root cannot be copied")]
    CopyRootError,
    #[error(transparent)]
    DbError(#[from] DbError),
    #[error("root is missing")]
    MissingRoot,
    #[error("other error: {0}")]
    Other(String),
    #[error(transparent)]
    CachedError(#[from] Arc<VfsError>),
}

#[derive(Error, Debug)]
pub enum NameError {
    #[error("Name cannot be empty")]
    Empty,
    #[error("Name cannot be '.' or '..'")]
    Reserved,
    #[error("Name contains invalid character")]
    InvalidCharacter,
    #[error("Name exceeds maximum length of 255 bytes")]
    TooLong,
    #[error("Name cannot have leading or trailing whitespace")]
    LeadingOrTrailingWhitespace,
}

pub type VfsResult<T> = Result<T, VfsError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct InodeId(u64);

impl InodeId {
    pub(crate) fn new(value: u64) -> Self {
        // SQLite's INTEGER is a signed 64-bit value, so clear the top bit
        // to keep the value within i64's non-negative range (0..=i64::MAX).
        Self(value & 0x7FFF_FFFF_FFFF_FFFF)
    }
    pub(crate) fn from_entity_id(entity_id: &EntityId) -> Self {
        Self::new(XxHash3_64::oneshot(entity_id.as_slice()))
    }
}

impl Deref for InodeId {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Display for InodeId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum StorageMode {
    Synced(String),
    Local,
}

#[derive(Debug, PartialEq, Eq, Copy, Clone, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Timestamp(DateTime<Utc>);

impl Timestamp {
    pub fn now() -> Self {
        Self(Utc::now())
    }
    pub fn from_millis(millis: i64) -> Option<Self> {
        DateTime::<Utc>::from_timestamp_millis(millis).map(Self)
    }

    pub fn to_millis(&self) -> i64 {
        self.0.timestamp_millis()
    }
}

impl From<DateTime<Utc>> for Timestamp {
    fn from(value: DateTime<Utc>) -> Self {
        Self(DateTime::<Utc>::from_timestamp_millis(value.timestamp_millis()).unwrap())
    }
}

impl From<Timestamp> for DateTime<Utc> {
    fn from(value: Timestamp) -> Self {
        value.0
    }
}

impl Display for Timestamp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl Deref for Timestamp {
    type Target = DateTime<Utc>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
#[repr(transparent)]
pub struct OwnedName(Cow<'static, str>);

impl Borrow<Name> for OwnedName {
    fn borrow(&self) -> &Name {
        Name::wrap_ref(self.0.as_ref())
    }
}

impl TryFrom<String> for OwnedName {
    type Error = NameError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        check_valid_filename(value.as_str())?;
        Ok(Self(value.into()))
    }
}

impl TryFrom<&'static str> for OwnedName {
    type Error = NameError;

    fn try_from(value: &'static str) -> Result<Self, Self::Error> {
        check_valid_filename(value)?;
        Ok(Self(value.into()))
    }
}

impl TryFrom<Cow<'static, str>> for OwnedName {
    type Error = NameError;

    fn try_from(value: Cow<'static, str>) -> Result<Self, Self::Error> {
        check_valid_filename(value.as_ref())?;
        Ok(Self(value))
    }
}

impl FromStr for OwnedName {
    type Err = NameError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        check_valid_filename(s)?;
        Ok(Self(Cow::Owned(s.to_string())))
    }
}

impl Deref for OwnedName {
    type Target = Name;

    fn deref(&self) -> &Self::Target {
        self.borrow()
    }
}

#[derive(Debug, PartialEq, Eq, TransparentWrapper)]
#[repr(transparent)]
pub struct Name(str);

impl ToOwned for Name {
    type Owned = OwnedName;

    fn to_owned(&self) -> Self::Owned {
        OwnedName(self.0.to_string().into())
    }
}

impl<'a> TryFrom<&'a str> for &'a Name {
    type Error = NameError;

    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        check_valid_filename(value)?;
        Ok(Name::wrap_ref(value))
    }
}

impl Deref for Name {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<str> for Name {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Inode {
    File(File),
    Directory(Directory),
}

impl Ord for Inode {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id().cmp(&other.id())
    }
}

impl PartialOrd for Inode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Inode {
    #[inline]
    pub fn id(&self) -> InodeId {
        match self {
            Self::Directory(i) => i.inode_id,
            Self::File(i) => i.inode_id,
        }
    }

    #[inline]
    pub fn parent_id(&self) -> Option<InodeId> {
        match self {
            Self::Directory(i) => i.parent_id(),
            Self::File(i) => i.parent_id(),
        }
    }

    #[inline]
    pub fn name(&self) -> &Name {
        match self {
            Self::Directory(i) => i.name(),
            Self::File(i) => i.name(),
        }
    }

    #[inline]
    pub fn path(&self) -> &VfsPath {
        match self {
            Self::Directory(i) => i.path(),
            Self::File(i) => i.path(),
        }
    }

    #[inline]
    pub fn len(&self) -> Option<u64> {
        match self {
            Self::Directory(_) => None,
            Self::File(i) => Some(i.len()),
        }
    }

    #[inline]
    pub fn blob_id(&self) -> Option<&BlobId> {
        match self {
            Self::Directory(_) => None,
            Self::File(i) => Some(i.blob_id()),
        }
    }

    #[inline]
    pub fn created(&self) -> &Timestamp {
        match self {
            Self::Directory(i) => i.created(),
            Self::File(i) => i.created(),
        }
    }

    #[inline]
    pub fn last_modified(&self) -> &Timestamp {
        match self {
            Self::Directory(i) => i.last_modified(),
            Self::File(i) => i.last_modified(),
        }
    }

    #[inline]
    pub fn is_directory(&self) -> bool {
        match self {
            Self::Directory(_) => true,
            Self::File(i) => false,
        }
    }

    #[inline]
    pub fn as_directory(&self) -> Option<&Directory> {
        match self {
            Self::Directory(dir) => Some(dir),
            Self::File(i) => None,
        }
    }

    #[inline]
    pub fn is_file(&self) -> bool {
        match self {
            Self::Directory(_) => false,
            Self::File(_) => true,
        }
    }

    #[inline]
    pub fn as_file(&self) -> Option<&File> {
        match self {
            Self::Directory(_) => None,
            Self::File(file) => Some(file),
        }
    }

    #[inline]
    pub fn is_synced(&self) -> bool {
        match self {
            Self::Directory(i) => i.is_synced(),
            Self::File(i) => i.is_synced(),
        }
    }
}

impl From<File> for Inode {
    fn from(value: File) -> Self {
        Self::File(value)
    }
}

impl From<Directory> for Inode {
    fn from(value: Directory) -> Self {
        Self::Directory(value)
    }
}

#[derive_where(Debug, Clone, PartialEq, Eq)]
pub struct TypedInode<T: EntityHandler> {
    parent: Option<InodeId>,
    inode_id: InodeId,
    path: VfsPath,
    entity: Entity<T>,
}

impl<T: EntityHandler> TypedInode<T> {
    pub fn inode_id(&self) -> InodeId {
        self.inode_id
    }

    pub fn path(&self) -> &VfsPath {
        &self.path
    }

    pub fn parent_id(&self) -> Option<InodeId> {
        self.parent
    }

    pub fn into_mut(self) -> InodeMut<T> {
        InodeMut {
            parent: self.parent,
            inode_id: self.inode_id,
            entity: self.entity.into_mut(),
        }
    }
}

impl<T: EntityHandler> Deref for TypedInode<T> {
    type Target = Entity<T>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.entity
    }
}

#[derive_where(Debug)]
pub struct InodeMut<T: EntityHandler> {
    parent: Option<InodeId>,
    inode_id: InodeId,
    entity: EntityMut<T>,
}

impl<T: EntityHandler> InodeMut<T> {
    pub(super) fn new(parent: Option<InodeId>, inode_id: InodeId, entity: EntityMut<T>) -> Self {
        Self {
            parent,
            inode_id,
            entity,
        }
    }

    pub fn inode_id(&self) -> InodeId {
        self.inode_id
    }
    pub fn parent_id(&self) -> Option<InodeId> {
        self.parent
    }

    pub(crate) fn freeze(self) -> DraftEntity<T> {
        self.entity.freeze()
    }
}

impl<T: EntityHandler> Deref for InodeMut<T> {
    type Target = EntityMut<T>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.entity
    }
}

impl<T: EntityHandler> DerefMut for InodeMut<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.entity
    }
}

#[derive_where(Debug, Clone)]
#[repr(transparent)]
pub struct Vfs<Mode>(Arc<Inner>, PhantomData<Mode>);

#[bon::bon]
impl<Mode> Vfs<Mode> {
    #[builder(derive(Debug))]
    pub async fn new(
        db_file: PathBuf,
        #[builder(default)] db_page_size: PageSize,
        #[builder(default = 25)] max_db_connections: u8,
        #[builder(default)] cache_settings: CacheSettings,
        #[builder(default = 64 * 1024)] max_chunk_size: usize,
    ) -> Result<Self, VfsError> {
        let cache = Cache::new(&cache_settings);
        let db = Db::new(db_file, max_db_connections, db_page_size, cache.clone()).await?;

        //todo: check that PageSize & max_chunk_size align

        let reaper = Reaper::new(db.clone());

        Ok(Self(
            Arc::new(Inner {
                db,
                cache,
                max_chunk_size,
                dead_fh_reaper: reaper,
            }),
            PhantomData,
        ))
    }
}

#[derive(Debug)]
struct Inner {
    db: Db,
    cache: Cache,
    max_chunk_size: usize,
    dead_fh_reaper: Reaper,
}

pub trait Read: Send + Sync + 'static {}
pub trait Write: Send + Sync + 'static {}

pub struct ReadOnly;

impl Read for ReadOnly {}

pub struct ReadWrite;

impl Read for ReadWrite {}
impl Write for ReadWrite {}

impl<Mode: Read> Vfs<Mode> {
    #[inline]
    pub async fn root(&self) -> VfsResult<Directory> {
        match self.inode_by_id(ROOT_INODE_ID).await? {
            Some(Inode::Directory(root)) => Ok(root),
            _ => Err(VfsError::MissingRoot),
        }
    }

    pub async fn inode_by_id(&self, inode_id: InodeId) -> VfsResult<Option<Inode>> {
        Ok(self
            .cache()
            .inode_cache()
            .try_get_with(inode_id, async { self._inode_by_id(inode_id).await })
            .await?)
    }

    async fn _inode_by_id(&self, inode_id: InodeId) -> VfsResult<Option<Inode>> {
        Ok(self.tx().await?.inode_by_id(inode_id).await?)
    }

    pub(crate) async fn tx(&self) -> VfsResult<Transaction<DbReadOnly>> {
        Ok(self.0.db.read().await?)
    }

    pub(crate) fn cache(&self) -> &Cache {
        &self.0.cache
    }

    pub(crate) fn max_chunk_size(&self) -> usize {
        self.0.max_chunk_size
    }

    pub(crate) async fn blob_by_id(&self, blob_id: &BlobId) -> VfsResult<Option<Blob>> {
        Ok(self.tx().await?.blob_by_id(blob_id).await?)
    }

    async fn _get_chunk(&self, chunk_id: &ChunkId) -> VfsResult<Option<Chunk>> {
        let mut tx = self.tx().await.map_err(std::io::Error::other)?;
        Ok(tx.chunk_by_id(chunk_id).await?)
    }
}

#[async_trait]
impl<Mode: Read + Unpin> ChunkSource for Vfs<Mode> {
    async fn get_chunk(&self, chunk_id: &ChunkId) -> Result<Option<Chunk>, Error> {
        self.cache()
            .chunk_cache()
            .try_get_with_by_ref(chunk_id, async { self._get_chunk(chunk_id).await })
            .await
            .map_err(Error::other)
    }
}

#[async_trait]
impl<Mode: Read + Write + Unpin> ChunkSink for Vfs<Mode> {
    async fn insert_chunk(&self, chunk: Chunk) -> Result<(), Error> {
        let mut tx = self.tx_rw().await.map_err(std::io::Error::other)?;
        tx.insert_chunk_if_not_exist(&chunk)
            .await
            .map_err(std::io::Error::other)?;
        tx.commit().await.map_err(std::io::Error::other)?;
        self.cache()
            .chunk_cache()
            .insert(*chunk.id(), Some(chunk))
            .await;
        Ok(())
    }
}

impl<Mode: Read + Write> Vfs<Mode> {
    pub async fn update<T: EntityHandler>(&self, modified_inode: InodeMut<T>) -> VfsResult<Inode> {
        let inode_id = modified_inode.inode_id;
        let name = modified_inode.name().to_owned();
        let draft_entity = modified_inode.freeze();
        let mut tx = self.tx_rw().await?;
        let inode = tx.update(inode_id, &name, draft_entity).await?;
        tx.commit().await?;
        Ok(inode)
    }

    pub async fn delete(&self, inode_id: InodeId) -> VfsResult<()> {
        if inode_id == ROOT_INODE_ID {
            return Err(VfsError::DeleteRootError);
        }
        let mut tx = self.tx_rw().await?;
        tx.delete_inode(inode_id).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn mv(&self, inode_id: InodeId, parent: &Directory) -> VfsResult<()> {
        if inode_id == ROOT_INODE_ID {
            return Err(VfsError::MoveRootError);
        }
        let mut tx = self.tx_rw().await?;
        tx.move_inode(inode_id, parent.inode_id()).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn copy(&self, inode_id: InodeId, new_parent: &Directory) -> VfsResult<()> {
        if inode_id == ROOT_INODE_ID {
            return Err(VfsError::CopyRootError);
        }
        todo!()
    }

    pub(crate) async fn tx_rw(&self) -> VfsResult<Transaction<DbReadWrite>> {
        Ok(self.0.db.write().await?)
    }
}

fn check_valid_filename(name: &str) -> Result<(), NameError> {
    if name.is_empty() {
        return Err(NameError::Empty);
    }

    if name.trim() != name {
        return Err(NameError::LeadingOrTrailingWhitespace);
    }

    if name == "." || name == ".." {
        return Err(NameError::Reserved);
    }

    if name.len() > 255 {
        return Err(NameError::TooLong);
    }

    if name.chars().any(|c| {
        matches!(c, '/' | '\0' | '\\' | '*' | '"' | '<' | '>' | '|' | ':') || c.is_control()
    }) {
        return Err(NameError::InvalidCharacter);
    }

    Ok(())
}

impl<C: TxScope> Transaction<C>
where
    Self: DbRead,
{
    pub(crate) async fn inode_by_id(
        &mut self,
        inode_id: InodeId,
    ) -> Result<Option<Inode>, DbError> {
        let id = inode_id.0 as i64;
        let r = match sqlx::query!(
            "SELECT inode_type, path, entity_id, entity_rev, parent FROM vfs WHERE inode_id = ?",
            id
        )
        .fetch_optional(self.conn())
        .await?
        {
            None => return Ok(None),
            Some(r) => r,
        };

        let entity_key = EntityKey::new(
            EntityId::try_from_bytes(r.entity_id)
                .ok_or_else(|| DataError::ConversionError("Invalid entity id".into()))?,
            Revision::try_from_bytes(r.entity_rev)
                .ok_or_else(|| DataError::ConversionError("Invalid entity revision".into()))?,
        );

        let parent = r.parent.map(|id| InodeId(id as u64));
        let path = r
            .path
            .map(|p| VfsPath::from_str(p.as_str()))
            .transpose()
            .map_err(|e| DataError::ConversionError(format!("vfs path error: {}", e).into()))?
            .ok_or_else(|| DataError::ConversionError("inode path is missing".into()))?;

        Ok(Some(match r.inode_type.as_str() {
            "D" => Inode::Directory(Directory {
                parent,
                inode_id,
                path,
                entity: self
                    .entity_by_key(&entity_key)
                    .await?
                    .ok_or_else(|| DataError::EntityNotFound(entity_key))?,
            }),
            "F" => Inode::File(File {
                parent,
                inode_id,
                path,
                entity: self
                    .entity_by_key(&entity_key)
                    .await?
                    .ok_or_else(|| DataError::EntityNotFound(entity_key))?,
            }),
            other => {
                return Err(DataError::ConversionError(
                    format!("unsupported inode_typ: {}", other).into(),
                ))?;
            }
        }))
    }

    pub(crate) async fn blob_by_id(&mut self, blob_id: &BlobId) -> Result<Option<Blob>, DbError> {
        let id = blob_id.as_ref();

        let r = match sqlx::query!(
            "SELECT size, mode, remote_location FROM blob where id = ?",
            id
        )
        .fetch_optional(self.conn())
        .await?
        {
            None => return Ok(None),
            Some(r) => r,
        };

        let len = r.size as u64;

        let mode = match r.mode.as_str() {
            "L" => StorageMode::Local,
            "S" => StorageMode::Synced(r.remote_location.ok_or(DataError::MissingRemoteLocation)?),
            other => {
                return Err(DataError::ConversionError(
                    format!("invalid mode: {}", other).into(),
                ))?;
            }
        };

        let rows = sqlx::query!(
            "SELECT offset, len, chunk_id, chunk_offset FROM chunk_map WHERE blob_id = ? ORDER BY offset ASC",
            id
        ).fetch_all(self.conn()).await?;

        let mut chunk_map = ChunkMap::with_len(len);
        for r in rows {
            chunk_map.insert(
                r.offset as u64,
                r.len as u64,
                ChunkId::try_from_bytes(r.chunk_id)
                    .ok_or_else(|| DataError::ConversionError("invalid chunk id".into()))?,
                r.chunk_offset as usize,
            )
        }

        let blob = BlobMut::from_chunk_map(chunk_map, mode).finalize();
        if blob.id() != blob_id {
            Err(DataError::BlobIdMismatch {
                expected: *blob_id,
                actual: *blob.id(),
            })?
        }
        Ok(Some(blob))
    }

    async fn chunk_by_id(&mut self, chunk_id: &ChunkId) -> Result<Option<Chunk>, DbError> {
        let id = chunk_id.as_slice();
        let r = match sqlx::query!(
            "SELECT mode, remote_location, data FROM chunk WHERE id = ?",
            id
        )
        .fetch_optional(self.conn())
        .await?
        {
            Some(r) => r,
            None => return Ok(None),
        };

        let mode = match r.mode.as_str() {
            "L" => StorageMode::Local,
            "S" => StorageMode::Synced(r.remote_location.ok_or(DataError::MissingRemoteLocation)?),
            other => {
                return Err(DataError::ConversionError(
                    format!("invalid mode: {}", other).into(),
                ))?;
            }
        };

        if let StorageMode::Local = mode {
            let chunk = match r.data {
                Some(bytes) => Chunk::from(bytes),
                None => return Err(DataError::ConversionError("chunk data is missing".into()))?,
            };
            if chunk.id() != chunk_id {
                return Err(DataError::ChunkIdMismatch {
                    expected: chunk_id.clone(),
                    actual: chunk.id().clone(),
                })?;
            }
            Ok(Some(chunk))
        } else {
            Err(DataError::UnsupportedStorageMode)?
        }
    }
}

impl<C: TxScope> Transaction<C>
where
    Self: DbWrite,
{
    async fn delete_inode(&mut self, inode_id: InodeId) -> Result<u64, DbError> {
        let inode_id = inode_id.0 as i64;

        Ok(sqlx::query!("DELETE FROM vfs WHERE inode_id = ?", inode_id)
            .execute(self.conn())
            .await?
            .rows_affected())
    }

    pub(super) async fn create_inode<T: EntityHandler>(
        &mut self,
        name: &Name,
        parent: InodeId,
        entity_key: EntityKey,
    ) -> Result<InodeId, DbError> {
        let inode_type = T::db_type();
        let inode_id = InodeId::from_entity_id(entity_key.id());
        let name = name.as_ref();
        let parent = parent.0 as i64;
        let entity_id = entity_key.id().as_slice();
        let entity_rev = entity_key.revision().as_slice();

        let id = inode_id.0 as i64;
        sqlx::query!(
            "INSERT INTO vfs (inode_id, inode_type, entity_id, entity_rev, name, parent) VALUES (?, ?, ?, ?, ?, ?)",
            id,
            inode_type,
            entity_id,
            entity_rev,
            name,
            parent
        )
            .execute(self.conn())
            .await?;

        Ok(inode_id)
    }

    pub(crate) async fn update_inode(
        &mut self,
        inode_id: InodeId,
        name: &Name,
        entity_key: &EntityKey,
    ) -> Result<(), DbError> {
        let inode_id = inode_id.0 as i64;
        let name = name.as_ref();
        let entity_id = entity_key.id().as_slice();
        let entity_rev = entity_key.revision().as_slice();

        let rows_affected = sqlx::query!(
            "UPDATE vfs SET name = ?, entity_id = ?, entity_rev = ?, is_dirty = 0 WHERE inode_id = ?",
            name,
            entity_id,
            entity_rev,
            inode_id
        )
            .execute(self.conn())
            .await?
            .rows_affected();

        if rows_affected != 1 {
            return Err(DataError::UnexpectedAffectedRows {
                expected: 1,
                actual: rows_affected,
            })?;
        }

        Ok(())
    }

    async fn move_inode(&mut self, inode_id: InodeId, parent_id: InodeId) -> Result<(), DbError> {
        let inode_id = inode_id.0 as i64;
        let parent_id = parent_id.0 as i64;

        let rows_affected = sqlx::query!(
            "UPDATE vfs SET parent = ? WHERE inode_id = ?",
            parent_id,
            inode_id
        )
        .execute(self.conn())
        .await?
        .rows_affected();

        if rows_affected != 1 {
            return Err(DataError::UnexpectedAffectedRows {
                expected: 1,
                actual: rows_affected,
            })?;
        }

        Ok(())
    }

    pub(crate) async fn create_blob_if_not_exist(&mut self, blob: &Blob) -> Result<(), DbError> {
        let id = blob.id().as_slice();

        if let StorageMode::Synced(remote_location) = blob.mode() {
            // upgrade blob to synced if necessary
            sqlx::query!(
                "UPDATE blob SET mode = 'S', remote_location = ? WHERE id = ?",
                remote_location,
                id
            )
            .execute(self.conn())
            .await?;
        }

        if sqlx::query!(
            "SELECT EXISTS(SELECT 1 FROM blob WHERE id = ?) as \"blob_exists: bool\"",
            id,
        )
        .fetch_one(self.conn())
        .await?
        .blob_exists
        {
            // blob already exists
            return Ok(());
        }

        let size = blob.len() as i64;
        let (mode, remote_location) = match blob.mode() {
            StorageMode::Local => ("L", None),
            StorageMode::Synced(loc) => ("S", Some(loc.as_str())),
        };

        sqlx::query!(
            "INSERT INTO blob (id, size, mode, remote_location) VALUES (?, ?, ?, ?)",
            id,
            size,
            mode,
            remote_location,
        )
        .execute(self.conn())
        .await?;

        for chunk in blob.chunk_map().iter() {
            let offset = chunk.offset as i64;
            let len = chunk.len as i64;
            let chunk_id = chunk.chunk_id.as_slice();
            let chunk_offset = chunk.chunk_offset as i64;
            sqlx::query!(
                "INSERT INTO chunk_map (blob_id, offset, len, chunk_id, chunk_offset) VALUES (?, ?, ?, ?, ?)",
                id,
                offset,
                len,
                chunk_id,
                chunk_offset
            ).execute(self.conn()).await?;
        }

        Ok(())
    }

    async fn insert_chunk_if_not_exist(&mut self, chunk: &Chunk) -> Result<bool, DbError> {
        let id = chunk.id().as_slice();

        if sqlx::query!(
            "SELECT EXISTS(SELECT 1 FROM chunk WHERE id = ?) as \"chunk_exists: bool\"",
            id,
        )
        .fetch_one(self.conn())
        .await?
        .chunk_exists
        {
            // chunk already exists
            return Ok(false);
        }

        let data = chunk.deref();

        sqlx::query!(
            "INSERT INTO chunk (id, mode, data) VALUES (?, 'L', ?)",
            id,
            data
        )
        .execute(self.conn())
        .await?;

        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use crate::vfs::directory::Directory;
    use crate::vfs::path::VfsPath;
    use crate::vfs::{Inode, InodeId, Name, OwnedName, Read, ReadWrite, Vfs, VfsError};
    use anyhow::bail;
    use futures_util::{AsyncReadExt, AsyncWriteExt, StreamExt, TryStreamExt};
    use std::ops::Deref;
    use std::str::FromStr;
    use std::time::Duration;
    use tempfile::{TempDir, tempdir};

    async fn new_vfs() -> anyhow::Result<(Vfs<ReadWrite>, TempDir)> {
        let temp_dir = tempdir()?;
        let path = temp_dir.path().join("vfs.sqlite");

        Ok((Vfs::builder().db_file(path).build().await?, temp_dir))
    }

    #[tokio::test]
    async fn bootstrap() -> anyhow::Result<()> {
        let (_vfs, _temp_dir) = new_vfs().await?;
        Ok(())
    }

    #[tokio::test]
    async fn root() -> anyhow::Result<()> {
        let (vfs, _temp_dir) = new_vfs().await?;
        let root = vfs.root().await?;
        assert_eq!(root.inode_id(), InodeId(1));
        let root_path = VfsPath::from_str("/")?;
        assert_eq!(root.path(), &root_path);
        let by_path = vfs.inode_id_by_path(&root_path).await?.unwrap();
        assert_eq!(root.inode_id, by_path);
        let entries = vfs.list(&root).await?.try_collect::<Vec<_>>().await?;
        assert!(entries.is_empty());
        Ok(())
    }

    async fn create_dirs(vfs: &Vfs<ReadWrite>, parent: &Directory) -> anyhow::Result<()> {
        let dir_name: &Name = "dir_1".try_into()?;
        let dir = vfs.create_dir(&parent, dir_name).await?;
        assert_eq!(dir.name(), dir_name);
        let parent_inodes = vec![Inode::Directory(dir.clone())];
        let entries = vfs
            .list(&vfs.root().await?)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(&entries, &parent_inodes);

        let subdir1_name: &Name = "subdir_1".try_into()?;
        let subdir1 = vfs.create_dir(&dir, subdir1_name).await?;
        assert_eq!(subdir1.name(), subdir1_name);

        let subdir2_name: &Name = "subdir_2".try_into()?;
        let subdir2 = vfs.create_dir(&dir, subdir2_name).await?;
        assert_eq!(subdir2.name(), subdir2_name);

        let mut dir1_inodes = vec![
            Inode::Directory(subdir1.clone()),
            Inode::Directory(subdir2.clone()),
        ];
        dir1_inodes.sort();

        assert_eq!(&entries, &parent_inodes);
        let mut entries = vfs
            .list(
                vfs.inode_by_path(&dir.path())
                    .await?
                    .unwrap()
                    .as_directory()
                    .unwrap(),
            )
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        entries.sort();
        assert_eq!(&entries, &dir1_inodes);

        Ok(())
    }

    #[tokio::test]
    async fn create_directories() -> anyhow::Result<()> {
        let (vfs, _temp_dir) = new_vfs().await?;
        let _temp_dir = _temp_dir.path().to_str().unwrap().to_string();
        create_dirs(&vfs, &vfs.root().await?).await?;
        Ok(())
    }

    #[tokio::test]
    async fn delete() -> anyhow::Result<()> {
        let (vfs, _temp_dir) = new_vfs().await?;
        let _temp_dir = _temp_dir.path().to_str().unwrap().to_string();
        create_dirs(&vfs, &vfs.root().await?).await?;
        let path = VfsPath::from_str("/dir_1")?;
        let dir1 = vfs.inode_by_path(&path).await?.unwrap();
        vfs.delete(dir1.id()).await?;
        assert_eq!(vfs.inode_by_path(&path).await?, None);
        assert!(
            vfs.list(&vfs.root().await?)
                .await?
                .try_collect::<Vec<_>>()
                .await?
                .is_empty()
        );
        Ok(())
    }

    #[tokio::test]
    async fn root_undeletable() -> anyhow::Result<()> {
        let (vfs, _temp_dir) = new_vfs().await?;
        let _temp_dir = _temp_dir.path().to_str().unwrap().to_string();
        let root = vfs.root().await?;

        match vfs.delete(root.inode_id).await {
            Err(VfsError::DeleteRootError) => {}
            _ => bail!("root should not be deletable"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn create_file() -> anyhow::Result<()> {
        let (vfs, _temp_dir) = new_vfs().await?;
        let _temp_dir = _temp_dir.path().to_str().unwrap().to_string();
        let root = vfs.root().await?;

        let file_name = "just_a_file.txt".try_into()?;
        let file_content = b"This is a test.".as_slice();

        let file = vfs.create_file(&root, file_name).await?;
        let mut fh = vfs.open_rw(&file).await?;
        fh.write_all(file_content).await?;
        fh.flush().await?;

        assert_eq!(count_fh(&vfs).await?, 1);
        assert_eq!(count_temp_chunks(&vfs).await?, 1);

        let file = fh.commit().await?;

        assert_eq!(count_fh(&vfs).await?, 0);
        assert_eq!(count_temp_chunks(&vfs).await?, 0);

        let ref_file = vfs
            .inode_by_path(&VfsPath::from_str("/just_a_file.txt")?)
            .await?
            .unwrap();
        assert_eq!(ref_file.as_file().unwrap(), &file);
        assert_eq!(file.name(), file_name);
        assert_eq!(file.len(), file_content.len() as u64);

        let mut fh = vfs.open(&file).await?;
        let mut content = Vec::with_capacity(file.len() as usize);
        fh.read_to_end(&mut content).await?;
        assert_eq!(file_content, content.as_slice());

        Ok(())
    }

    #[tokio::test]
    async fn drop_fh() -> anyhow::Result<()> {
        let (vfs, _temp_dir) = new_vfs().await?;
        let _temp_dir = _temp_dir.path().to_str().unwrap().to_string();
        let root = vfs.root().await?;
        let file_name = "another_file.txt".try_into()?;
        let file_content = b"This is another test.".as_slice();
        let file = vfs.create_file(&root, file_name).await?;
        let mut fh = vfs.open_rw(&file).await?;
        fh.write_all(file_content).await?;
        fh.flush().await?;

        assert_eq!(count_fh(&vfs).await?, 1);
        assert_eq!(count_temp_chunks(&vfs).await?, 1);
        drop(fh);

        // dead fh should be auto cleaned
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(count_fh(&vfs).await?, 0);
        assert_eq!(count_temp_chunks(&vfs).await?, 0);

        Ok(())
    }

    async fn count_fh(vfs: &Vfs<ReadWrite>) -> anyhow::Result<u64> {
        let mut tx = vfs.0.db.read().await?;
        Ok(
            sqlx::query!("SELECT COUNT(*) AS fh_count FROM temp_file_handle")
                .fetch_one(tx.as_mut())
                .await
                .map(|r| r.fh_count as u64)?,
        )
    }

    async fn count_temp_chunks(vfs: &Vfs<ReadWrite>) -> anyhow::Result<u64> {
        let mut tx = vfs.0.db.read().await?;
        Ok(
            sqlx::query!("SELECT COUNT(*) AS chunk_count FROM temp_file_chunks")
                .fetch_one(tx.as_mut())
                .await
                .map(|r| r.chunk_count as u64)?,
        )
    }

    #[tokio::test]
    async fn rename_mv() -> anyhow::Result<()> {
        let (vfs, _temp_dir) = new_vfs().await?;
        let _temp_dir = _temp_dir.path().to_str().unwrap().to_string();

        let dir1 = vfs
            .create_dir(&vfs.root().await?, "foo".try_into()?)
            .await?;

        let dir2 = vfs.create_dir(&dir1, "bar".try_into()?).await?;

        let mut file = vfs.create_file(&dir2, "file".try_into()?).await?.into_mut();
        let file_inode = file.inode_id;
        let new_name = OwnedName::from_str("file2")?;
        file.set_name(new_name.clone());
        let file = vfs.update(file).await?;
        assert_eq!(file.name(), new_name.deref());

        assert_eq!(vfs.list(&vfs.root().await?).await?.count().await, 1);

        assert_eq!(count_path_entries(&vfs, "/foo").await?, 1);

        vfs.mv(dir2.inode_id, &vfs.root().await?).await?;

        assert_eq!(vfs.list(&vfs.root().await?).await?.count().await, 2);

        assert_eq!(count_path_entries(&vfs, "/foo").await?, 0);
        assert_eq!(count_path_entries(&vfs, "/bar").await?, 1);

        let inode = vfs
            .inode_by_path(&VfsPath::from_str("/bar/file2")?)
            .await?
            .unwrap();
        assert_eq!(inode.as_file().unwrap().inode_id, file_inode);
        Ok(())
    }

    async fn count_path_entries<Mode: Read>(vfs: &Vfs<Mode>, path: &str) -> anyhow::Result<usize> {
        Ok(vfs
            .list(
                vfs.inode_by_path(&VfsPath::from_str(path)?)
                    .await?
                    .unwrap()
                    .as_directory()
                    .unwrap(),
            )
            .await?
            .count()
            .await)
    }
}
