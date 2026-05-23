pub mod directory;
pub mod entity;
pub mod file;
pub mod path;

use crate::blob::BlobId;
use crate::db::{
    DataError, Db, Error as DbError, Read as DbRead, ReadOnly as DbReadOnly,
    ReadWrite as DbReadWrite, Transaction, TxScope, Write as DbWrite,
};
use crate::vfs::directory::Directory;
use crate::vfs::entity::{
    DraftEntity, Entity, EntityHasher, EntityId, EntityMut, EntityRef, EntityRow, Freezable,
    Normalizer,
};
use crate::vfs::file::File;
use chrono::{DateTime, Utc};
use derive_where::derive_where;
use futures_util::{StreamExt, TryStream};
use std::collections::VecDeque;
use std::fmt::{Display, Formatter};
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;

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
        Self(value)
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

#[derive(Debug, PartialEq, Eq, Clone)]
#[repr(transparent)]
pub struct Name(String);

impl FromStr for Name {
    type Err = NameError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        check_valid_filename(s)?;
        Ok(Name(s.to_string()))
    }
}

impl Deref for Name {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<str> for Name {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone)]
pub enum Inode {
    File(File),
    Directory(Directory),
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
    pub fn created(&self) -> &DateTime<Utc> {
        match self {
            Self::Directory(i) => i.created(),
            Self::File(i) => i.created(),
        }
    }

    #[inline]
    pub fn last_modified(&self) -> &DateTime<Utc> {
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
    pub fn is_file(&self) -> bool {
        match self {
            Self::Directory(_) => false,
            Self::File(i) => true,
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

#[derive_where(Debug, Clone; I)]
pub struct TypedInode<T, I> {
    parent: Option<InodeId>,
    inode_id: InodeId,
    entity: Entity<T, I>,
}

impl<T, I> TypedInode<T, I> {
    pub fn inode_id(&self) -> InodeId {
        self.inode_id
    }

    pub fn parent_id(&self) -> Option<InodeId> {
        self.parent
    }

    pub fn into_mut(self) -> InodeMut<T, I>
    where
        I: Clone,
    {
        InodeMut {
            parent: self.parent,
            inode_id: self.inode_id,
            entity: self.entity.into_mut(),
        }
    }
}

impl<T, I> Deref for TypedInode<T, I> {
    type Target = Entity<T, I>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.entity
    }
}

#[derive_where(Debug; I)]
pub struct InodeMut<T, I> {
    parent: Option<InodeId>,
    inode_id: InodeId,
    entity: EntityMut<T, I>,
}

impl<T, I> InodeMut<T, I> {
    pub(super) fn new(parent: Option<InodeId>, inode_id: InodeId, entity: EntityMut<T, I>) -> Self {
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
}

impl<T, I> InodeMut<T, I>
where
    EntityMut<T, I>: Freezable<T, I>,
{
    pub(crate) fn freeze(self) -> DraftEntity<T, I> {
        self.entity.freeze()
    }
}

impl<T, I> Deref for InodeMut<T, I> {
    type Target = EntityMut<T, I>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.entity
    }
}

impl<T, I> DerefMut for InodeMut<T, I> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.entity
    }
}

#[derive_where(Debug, Clone)]
#[repr(transparent)]
pub struct Vfs<Mode>(Arc<Inner>, PhantomData<Mode>);

#[derive(Debug)]
struct Inner {
    db: Db,
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
        Ok(self.tx().await?.inode_by_id(inode_id).await?)
    }

    pub async fn list(
        &self,
        dir: &Directory,
    ) -> VfsResult<impl TryStream<Ok = Inode, Error = VfsError> + Send + Unpin> {
        let parent_inode_id = dir.inode_id;
        let this = self.clone();
        let inode_ids = self
            .tx()
            .await?
            .inode_ids_by_parent(parent_inode_id)
            .await?;

        Ok(futures_util::stream::try_unfold(
            VecDeque::from(inode_ids),
            move |mut remaining_inode_ids| {
                let this = this.clone();
                async move {
                    let inode_id = match remaining_inode_ids.pop_front() {
                        None => return Ok(None),
                        Some(key) => key,
                    };

                    match this.inode_by_id(inode_id).await? {
                        None => Err(DbError::DataError(DataError::InodeNotFound(inode_id)))?,
                        Some(inode) => Ok(Some((inode, remaining_inode_ids))),
                    }
                }
            },
        )
        .boxed())
    }

    pub(crate) async fn tx(&self) -> VfsResult<Transaction<DbReadOnly>> {
        Ok(self.0.db.read().await?)
    }
}

impl<Mode: Read + Write> Vfs<Mode> {
    pub async fn update<T, I>(&self, modified_inode: InodeMut<T, I>) -> VfsResult<Inode>
    where
        EntityMut<T, I>: Freezable<T, I>,
        (EntityRow, Vec<EntityRef>): From<DraftEntity<T, I>>,
    {
        let inode_id = modified_inode.inode_id;
        let name = modified_inode.name().clone();
        let draft_entity = modified_inode.freeze();
        let mut tx = self.tx_rw().await?;
        let entity_id = tx.create_entity_if_not_exist(draft_entity).await?;
        tx.update_inode(inode_id, &name, &entity_id).await?;
        let inode = tx
            .inode_by_id(inode_id)
            .await?
            .ok_or_else(|| DbError::DataError(DataError::InodeNotFound(inode_id)))?;
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
            "SELECT inode_type, entity_id, parent FROM vfs WHERE inode_id = ?",
            id
        )
        .fetch_optional(self.conn())
        .await?
        {
            None => return Ok(None),
            Some(r) => r,
        };

        let entity_id = EntityId::try_from_bytes(r.entity_id)
            .ok_or_else(|| DataError::ConversionError("Invalid entity id".into()))?;

        let parent = r.parent.map(|id| InodeId(id as u64));

        Ok(Some(match r.inode_type.as_str() {
            "D" => Inode::Directory(Directory {
                parent,
                inode_id,
                entity: self
                    .entity_by_id(&entity_id)
                    .await?
                    .ok_or_else(|| DataError::EntityNotFound(entity_id))?,
            }),
            "F" => Inode::File(File {
                parent,
                inode_id,
                entity: self
                    .entity_by_id(&entity_id)
                    .await?
                    .ok_or_else(|| DataError::EntityNotFound(entity_id))?,
            }),
            other => {
                return Err(DataError::ConversionError(
                    format!("unsupported inode_typ: {}", other).into(),
                ))?;
            }
        }))
    }

    async fn inode_ids_by_parent(
        &mut self,
        parent_inode_id: InodeId,
    ) -> Result<Vec<InodeId>, DbError> {
        let parent_id = parent_inode_id.0 as i64;
        Ok(
            sqlx::query!("SELECT inode_id FROM vfs where parent = ?", parent_id)
                .fetch_all(self.conn())
                .await?
                .into_iter()
                .map(|r| InodeId(r.inode_id as u64))
                .collect::<Vec<_>>(),
        )
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

    pub(super) async fn create_inode<T: AsDbType>(
        &mut self,
        name: &Name,
        parent: InodeId,
        entity_id: EntityId,
    ) -> Result<InodeId, DbError> {
        let inode_type = T::db_type();
        let name = name.as_str();
        let parent = parent.0 as i64;
        let entity_id = entity_id.as_bytes().as_slice();

        let id = sqlx::query!(
            "INSERT INTO vfs (inode_type, entity_id, name, parent) VALUES (?, ?, ?, ?)",
            inode_type,
            entity_id,
            name,
            parent
        )
        .execute(self.conn())
        .await?
        .last_insert_rowid();

        Ok(InodeId(id as u64))
    }

    pub(crate) async fn update_inode(
        &mut self,
        inode_id: InodeId,
        name: &Name,
        entity_id: &EntityId,
    ) -> Result<(), DbError> {
        let inode_id = inode_id.0 as i64;
        let name = name.as_str();
        let entity_id = entity_id.as_bytes().as_slice();

        let rows_affected = sqlx::query!(
            "UPDATE vfs SET name = ?, entity_id = ?, is_dirty = 0 WHERE inode_id = ?",
            name,
            entity_id,
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
}

trait AsDbType {
    fn db_type() -> &'static str;
}

#[cfg(test)]
mod tests {
    use crate::db::{Db, PageSize};
    use tempfile::{TempDir, tempdir};

    async fn new_db() -> anyhow::Result<(Db, TempDir)> {
        let temp_dir = tempdir()?;
        let path = temp_dir.path().join("vfs.sqlite");
        let db = Db::new(path, 10, PageSize::default()).await?;
        Ok((db, temp_dir))
    }

    #[tokio::test]
    async fn bootstrap() -> anyhow::Result<()> {
        let (db, _temp_dir) = new_db().await?;
        println!("");
        Ok(())
    }
}
