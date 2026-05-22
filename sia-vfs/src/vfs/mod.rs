pub mod directory;
pub mod entity;
pub mod file;
pub mod path;

use crate::ContentId;
use crate::blob::BlobId;
use crate::db::{
    DataError, Db, Error as DbError, Read as DbRead, ReadOnly as DbReadOnly,
    ReadWrite as DbReadWrite, Transaction, TxScope, Write as DbWrite,
};
use crate::vfs::directory::Directory;
use crate::vfs::entity::{
    DraftEntity, DraftMode, Entity, EntityId, EntityKey, EntityMut, EntityRow, Freezable,
    Normalizer, RawEntityInner, RevisionHasher,
};
use crate::vfs::file::File;
use blake3::Hash;
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
    Root(Root),
    File(File),
    Directory(Directory),
}

impl Inode {
    #[inline]
    pub fn id(&self) -> InodeId {
        match self {
            Self::Root(i) => i.inode_id,
            Self::Directory(i) => i.inode_id,
            Self::File(i) => i.inode_id,
        }
    }

    #[inline]
    pub fn parent_id(&self) -> Option<InodeId> {
        match self {
            Self::Root(_) => None,
            Self::Directory(i) => Some(i.parent_id()),
            Self::File(i) => Some(i.parent_id()),
        }
    }

    #[inline]
    pub fn name(&self) -> &Name {
        match self {
            Self::Root(i) => i.name(),
            Self::Directory(i) => i.name(),
            Self::File(i) => i.name(),
        }
    }

    #[inline]
    pub fn len(&self) -> Option<u64> {
        match self {
            Self::Root(_) | Self::Directory(_) => None,
            Self::File(i) => Some(i.len()),
        }
    }

    #[inline]
    pub fn blob_id(&self) -> Option<&BlobId> {
        match self {
            Self::Root(_) | Self::Directory(_) => None,
            Self::File(i) => Some(i.blob_id()),
        }
    }

    #[inline]
    pub fn created(&self) -> &DateTime<Utc> {
        match self {
            Self::Root(i) => i.created(),
            Self::Directory(i) => i.created(),
            Self::File(i) => i.created(),
        }
    }

    #[inline]
    pub fn last_modified(&self) -> &DateTime<Utc> {
        match self {
            Self::Root(i) => i.last_modified(),
            Self::Directory(i) => i.last_modified(),
            Self::File(i) => i.last_modified(),
        }
    }

    #[inline]
    pub fn is_container(&self) -> bool {
        match self {
            Self::Root(_) | Self::Directory(_) => true,
            Self::File(i) => false,
        }
    }

    #[inline]
    pub fn is_synced(&self) -> bool {
        match self {
            Self::Root(i) => i.is_synced(),
            Self::Directory(i) => i.is_synced(),
            Self::File(i) => i.is_synced(),
        }
    }
}

impl From<Root> for Inode {
    fn from(value: Root) -> Self {
        Self::Root(value)
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

#[derive_where(Debug, Clone; I, P)]
pub struct TypedInode<T, I, P> {
    parent: P,
    inode_id: InodeId,
    entity: Entity<T, I>,
}

impl<T, I> TypedInode<T, I, InodeId> {
    pub fn parent_id(&self) -> InodeId {
        self.parent
    }
}

impl<T, I, P> TypedInode<T, I, P> {
    pub fn inode_id(&self) -> InodeId {
        self.inode_id
    }

    pub fn into_mut(self) -> InodeMut<T, I, P>
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

impl<T, I, P> Deref for TypedInode<T, I, P> {
    type Target = Entity<T, I>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.entity
    }
}

#[derive_where(Debug; I, P)]
pub struct InodeMut<T, I, P> {
    parent: P,
    inode_id: InodeId,
    entity: EntityMut<T, I>,
}

impl<T, I> InodeMut<T, I, InodeId> {
    pub fn parent_id(&self) -> InodeId {
        self.parent
    }
}

impl<T, I, P> InodeMut<T, I, P> {
    pub(super) fn new(parent: P, inode_id: InodeId, entity: EntityMut<T, I>) -> Self {
        Self {
            parent,
            inode_id,
            entity,
        }
    }

    pub fn inode_id(&self) -> InodeId {
        self.inode_id
    }
}

impl<T, I, P> InodeMut<T, I, P>
where
    EntityMut<T, I>: Freezable<T, I>,
{
    pub(crate) fn freeze(self) -> DraftEntity<T, I> {
        self.entity.freeze()
    }
}

impl<T, I, P> Deref for InodeMut<T, I, P> {
    type Target = EntityMut<T, I>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.entity
    }
}

impl<T, I, P> DerefMut for InodeMut<T, I, P> {
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
    pub async fn root(&self) -> VfsResult<Root> {
        match self.inode_by_id(ROOT_INODE_ID).await? {
            Some(Inode::Root(root)) => Ok(root),
            _ => Err(VfsError::MissingRoot),
        }
    }

    pub async fn inode_by_id(&self, inode_id: InodeId) -> VfsResult<Option<Inode>> {
        Ok(self.tx().await?.inode_by_id(inode_id).await?)
    }

    pub async fn list<T, P>(
        &self,
        inode: &Container<T, P>,
    ) -> VfsResult<impl TryStream<Ok = Inode, Error = VfsError> + Send + Unpin> {
        let parent_inode_id = inode.inode_id;
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
    pub async fn update<T, I, P>(&self, modified_inode: InodeMut<T, I, P>) -> VfsResult<Inode>
    where
        EntityMut<T, I>: Freezable<T, I>,
        EntityRow: From<DraftEntity<T, I>>,
    {
        let inode_id = modified_inode.inode_id;
        let name = modified_inode.name().clone();
        let draft_entity = modified_inode.freeze();
        let mut tx = self.tx_rw().await?;
        let (entity_id, entity_revision) = tx.create_entity_if_not_exist(draft_entity).await?;
        tx.update_inode(inode_id, &name, entity_id, &entity_revision)
            .await?;
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

    pub async fn mv<T, P>(&self, inode_id: InodeId, parent: &Container<T, P>) -> VfsResult<()> {
        if inode_id == ROOT_INODE_ID {
            return Err(VfsError::MoveRootError);
        }
        let mut tx = self.tx_rw().await?;
        tx.move_inode(inode_id, parent.inode_id()).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn copy<T, P>(
        &self,
        inode_id: InodeId,
        new_parent: &Container<T, P>,
    ) -> VfsResult<()> {
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

pub struct RevisionKind;
pub type Revision = ContentId<RevisionKind>;

pub struct RootKind;

impl AsDbType for RootKind {
    fn db_type() -> &'static str {
        "R"
    }
}

impl<Mode> RevisionHasher<Vec<EntityKey>, Mode> for RootKind {
    fn hash(inner: &RawEntityInner<Self, Vec<EntityKey>, Mode>) -> Hash {
        let mut hasher = blake3::Hasher::new_derive_key("[sia-vfs]/[v0]/[root_revision]");
        hasher.update(b"begin:\n");
        inner.hash_metadata(&mut hasher);
        hash_entries(&inner.inner, &mut hasher);
        hasher.update(b"\nend");
        hasher.finalize()
    }
}

impl Normalizer<Vec<EntityKey>> for RootKind {
    fn normalize(value: &mut Vec<EntityKey>) {
        value.sort();
    }
}

fn hash_entries(entries: &Vec<EntityKey>, hasher: &mut blake3::Hasher) {
    hasher.update(b"begin_entries:\nno_entries:");
    hasher.update(&entries.len().to_be_bytes());
    hasher.update(b"\nentries:");
    for entry in entries {
        hasher.update(b"entity_id:");
        hasher.update(entry.entity_id.as_bytes());
        hasher.update(b"\nrevision:");
        hasher.update(entry.revision.as_ref());
        hasher.update(b"\n");
    }
    hasher.update(b"\nend_entries");
}

pub type Container<T, P> = TypedInode<T, Vec<EntityKey>, P>;
pub type ContainerMut<T, P> = InodeMut<T, Vec<EntityKey>, P>;

impl<T, P> Container<T, P> {
    pub(crate) fn entries(&self) -> &Vec<EntityKey> {
        self.entity.inner()
    }

    fn serialize_entries(&self) -> Vec<u8> {
        EntityKey::serialize(self.entries())
    }

    fn deserialize_entries(input: &[u8]) -> Result<Vec<EntityKey>, String> {
        todo!()
    }

    pub(super) fn try_from_row(value: EntityRow) -> Result<Entity<T, Vec<EntityKey>>, String> {
        let entries = Self::deserialize_entries(
            value
                .data
                .as_ref()
                .ok_or_else(|| "data is missing".to_string())?
                .as_slice(),
        )?;

        (value, entries).try_into()
    }
}

impl<T, P> ContainerMut<T, P> {
    fn entries_mut(&mut self) -> &mut Vec<EntityKey> {
        self.inner_mut()
    }
}

pub type Root = Container<RootKind, ()>;

impl TryFrom<EntityRow> for Entity<RootKind, Vec<EntityKey>> {
    type Error = String;

    fn try_from(value: EntityRow) -> Result<Self, Self::Error> {
        if value.entity_type != "R" {
            return Err(format!(
                "invalid entity_type; expected 'R' but got '{}'",
                value.entity_type
            ));
        }

        Container::<_, ()>::try_from_row(value)
    }
}

pub(crate) type RootDraft = DraftEntity<RootKind, Vec<EntityKey>>;

impl RootDraft {
    pub(crate) fn new_root_draft() -> Self {
        let now = Utc::now();
        let name = Name::from_str("ROOT").unwrap();
        Self::new(
            EntityId::generate(),
            Revision::zeroed(),
            name,
            now.clone(),
            now,
            vec![],
            DraftMode,
        )
        .into_mut()
        .freeze()
    }
}

impl From<RootDraft> for EntityRow {
    fn from(value: RootDraft) -> Self {
        let data = EntityKey::serialize(value.inner());
        Self::from((value, None::<BlobId>, Some(data)))
    }
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
            "SELECT inode_type, entity_id, entity_revision, parent FROM vfs WHERE inode_id = ?",
            id
        )
        .fetch_optional(self.conn())
        .await?
        {
            None => return Ok(None),
            Some(r) => r,
        };

        let entity_id = EntityId::try_from(r.entity_id.as_slice())
            .map_err(|e| DataError::ConversionError(e))?;

        let revision = Revision::try_from_bytes(r.entity_revision)
            .ok_or_else(|| DataError::ConversionError("invalid revision".to_string()))?;

        let parent = r.parent.map(|id| InodeId(id as u64));

        Ok(Some(match r.inode_type.as_str() {
            "R" => Inode::Root(Root {
                parent: (),
                inode_id,
                entity: self
                    .entity_by_id_revision(&entity_id, &revision)
                    .await?
                    .ok_or_else(|| DataError::EntityNotFound {
                        entity_id,
                        revision,
                    })?,
            }),
            "D" => Inode::Directory(Directory {
                parent: parent
                    .ok_or_else(|| DataError::ConversionError("parent is missing".to_string()))?,
                inode_id,
                entity: self
                    .entity_by_id_revision(&entity_id, &revision)
                    .await?
                    .ok_or_else(|| DataError::EntityNotFound {
                        entity_id,
                        revision,
                    })?,
            }),
            "F" => Inode::File(File {
                parent: parent
                    .ok_or_else(|| DataError::ConversionError("parent is missing".to_string()))?,
                inode_id,
                entity: self
                    .entity_by_id_revision(&entity_id, &revision)
                    .await?
                    .ok_or_else(|| DataError::EntityNotFound {
                        entity_id,
                        revision,
                    })?,
            }),
            other => {
                return Err(DataError::ConversionError(format!(
                    "unsupported inode_typ: {}",
                    other
                )))?;
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
        entity_revision: &Revision,
    ) -> Result<InodeId, DbError> {
        let inode_type = T::db_type();
        let name = name.as_str();
        let parent = parent.0 as i64;
        let entity_id = entity_id.as_bytes().as_slice();
        let entity_revision = entity_revision.as_slice();

        let id = sqlx::query!(
            "INSERT INTO vfs (inode_type, entity_id, entity_revision, name, parent) VALUES (?, ?, ?, ?, ?)",
            inode_type,
            entity_id,
            entity_revision,
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
        entity_id: EntityId,
        entity_revision: &Revision,
    ) -> Result<(), DbError> {
        let inode_id = inode_id.0 as i64;
        let name = name.as_str();
        let entity_id = entity_id.as_bytes().as_slice();
        let entity_revision = entity_revision.as_slice();

        let rows_affected = sqlx::query!(
            "UPDATE vfs SET name = ?, entity_id = ?, entity_revision = ?, is_dirty = 0 WHERE inode_id = ?",
            name,
            entity_id,
            entity_revision,
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
    use tempfile::{tempdir, TempDir};

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
