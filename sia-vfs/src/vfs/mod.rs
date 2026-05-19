pub mod directory;
mod entity;
pub mod file;
pub mod path;

use crate::ContentId;
use crate::vfs::directory::Directory;
use crate::vfs::entity::{
    EditMode, Entity, EntityKey, EntityMut, Freezable, Normalizer, RawEntity, RawEntityInner,
    RevisionHasher,
};
use crate::vfs::file::File;
use blake3::Hash;
use derive_where::derive_where;
use futures_util::TryStream;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use thiserror::Error;

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

impl Deref for InodeId {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
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

#[derive_where(Debug, Clone; I)]
pub struct TypedInode<T, I> {
    inode_id: InodeId,
    entity: Entity<T, I>,
}

impl<T, I> TypedInode<T, I> {
    pub fn inode_id(&self) -> InodeId {
        self.inode_id
    }

    pub fn into_mut(self) -> InodeMut<T, I>
    where
        I: Clone,
    {
        InodeMut {
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
    inode_id: InodeId,
    entity: EntityMut<T, I>,
}

impl<T, I> InodeMut<T, I> {
    pub fn inode_id(&self) -> InodeId {
        self.inode_id
    }
}

impl<T, I> InodeMut<T, I>
where
    EntityMut<T, I>: Freezable<T, I>,
{
    pub(crate) fn freeze(self) -> RawEntity<T, I, EditMode> {
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
    root: Mutex<Root>,
}

pub trait Read {}
pub trait Write {}

pub struct ReadOnly;

impl Read for ReadOnly {}

pub struct ReadWrite;

impl Read for ReadWrite {}
impl Write for ReadWrite {}

impl<Mode: Read> Vfs<Mode> {
    #[inline]
    pub fn root(&self) -> Root {
        self.0
            .root
            .lock()
            .expect("mutex not to be poisoned")
            .clone()
    }

    pub async fn inode_by_id(&self, inode_id: InodeId) -> VfsResult<Option<Inode>> {
        let root = self.root();
        if inode_id == root.inode_id() {
            return Ok(Some(root.into()));
        }
        todo!()
    }

    pub async fn list<T>(
        &self,
        inode: &Container<T>,
    ) -> VfsResult<impl TryStream<Ok = Inode, Error = VfsError> + Send + Unpin> {
        if true {
            todo!()
        }
        Ok(futures_util::stream::empty())
    }
}

impl<Mode: Read + Write> Vfs<Mode> {
    pub async fn update<T, I>(&self, modified_inode: InodeMut<T, I>) -> VfsResult<TypedInode<T, I>>
    where
        EntityMut<T, I>: Freezable<T, I>,
    {
        let inode_id = modified_inode.inode_id;
        let inode = modified_inode.freeze();
        todo!()
    }

    pub async fn delete(&self, inode_id: InodeId) -> VfsResult<()> {
        if inode_id == self.root().inode_id() {
            return Err(VfsError::DeleteRootError);
        }
        todo!()
    }

    pub async fn mv<T>(&self, inode_id: InodeId, parent: &Container<T>) -> VfsResult<()> {
        if inode_id == self.root().inode_id() {
            return Err(VfsError::MoveRootError);
        }
        todo!()
    }

    pub async fn copy<T>(&self, inode_id: InodeId, new_parent: &Container<T>) -> VfsResult<()> {
        if inode_id == self.root().inode_id() {
            return Err(VfsError::CopyRootError);
        }
        todo!()
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

pub type Container<T> = TypedInode<T, Vec<EntityKey>>;
pub type ContainerMut<T> = InodeMut<T, Vec<EntityKey>>;

impl<T> Container<T> {
    pub(crate) fn entries(&self) -> &Vec<EntityKey> {
        self.entity.inner()
    }
}

impl<T> ContainerMut<T> {
    fn entries_mut(&mut self) -> &mut Vec<EntityKey> {
        self.inner_mut()
    }
}

pub type Root = Container<RootKind>;
