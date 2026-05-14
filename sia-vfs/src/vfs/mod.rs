pub mod directory;
pub mod file;
pub mod path;

use crate::vfs::directory::Directory;
use crate::vfs::file::File;
use chrono::{DateTime, Utc};
use derive_where::derive_where;
use futures_util::TryStream;
use std::marker::PhantomData;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use thiserror::Error;
use uuid::Uuid;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct InodeId(Uuid);

impl Deref for InodeId {
    type Target = Uuid;

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

#[derive_where(Debug, Clone; I)]
struct InodeInner<T, I> {
    id: InodeId,
    name: Name,
    created: DateTime<Utc>,
    last_modified: DateTime<Utc>,
    //extended_attributes: HashMap<String, Bytes>,
    inner: I,
    _phantom: PhantomData<T>,
}

#[derive_where(Debug, Clone; I)]
pub struct Inode<T, I>(Arc<InodeInner<T, I>>);

impl<T, I> Inode<T, I> {
    pub fn id(&self) -> InodeId {
        self.0.id
    }

    pub fn name(&self) -> &Name {
        &self.0.name
    }

    pub fn created(&self) -> &DateTime<Utc> {
        &self.0.created
    }

    pub fn last_modified(&self) -> &DateTime<Utc> {
        &self.0.last_modified
    }
}

impl<T, I: Clone> Inode<T, I> {
    pub fn into_mut(self) -> InodeMut<T, I> {
        InodeMut(Arc::unwrap_or_clone(self.0))
    }
}

#[derive_where(Debug; I)]
pub struct InodeMut<T, I>(InodeInner<T, I>);

impl<T, I> InodeMut<T, I> {
    pub fn id(&self) -> InodeId {
        self.0.id
    }

    pub fn name(&self) -> &Name {
        &self.0.name
    }

    pub fn set_name(&mut self, new: Name) {
        self.0.name = new;
    }

    pub fn created(&self) -> &DateTime<Utc> {
        &self.0.created
    }

    pub fn last_modified(&self) -> &DateTime<Utc> {
        &self.0.last_modified
    }

    pub fn set_last_modified(&mut self, new: DateTime<Utc>) {
        self.0.last_modified = new;
    }

    pub(crate) fn freeze(self) -> Inode<T, I> {
        Inode(Arc::new(self.0))
    }
}

#[derive(Debug, Clone)]
pub enum Entry {
    Root(Root),
    File(File),
    Directory(Directory),
}

impl From<File> for Entry {
    fn from(value: File) -> Self {
        Self::File(value)
    }
}

impl From<Directory> for Entry {
    fn from(value: Directory) -> Self {
        Self::Directory(value)
    }
}

impl From<Root> for Entry {
    fn from(value: Root) -> Self {
        Self::Root(value)
    }
}

#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct Vfs(Arc<Inner>);

#[derive(Debug)]
struct Inner {
    root: Mutex<Root>,
}

impl Vfs {
    #[inline]
    pub fn root_id(&self) -> InodeId {
        let root = self.0.root.lock().expect("lock to not be poisoned");
        root.id()
    }

    pub async fn list<T>(
        &self,
        inode: &Container<T>,
    ) -> VfsResult<impl TryStream<Ok = Entry, Error = VfsError> + Send + Unpin> {
        if true {
            todo!()
        }
        Ok(futures_util::stream::empty())
    }

    pub async fn get_by_id(&self, inode_id: InodeId) -> VfsResult<Option<Entry>> {
        let root_id = self.root_id();
        if inode_id == root_id {
            let root = self.0.root.lock().expect("lock to not be poisoned");
            return Ok(Some(Entry::Root(root.clone())));
        }
        todo!()
    }

    pub async fn update<T, I>(&self, modified_inode: InodeMut<T, I>) -> VfsResult<Inode<T, I>> {
        let inode = modified_inode.freeze();
        todo!()
    }

    pub async fn delete(&self, inode_id: InodeId) -> VfsResult<()> {
        if inode_id == self.root_id() {
            return Err(VfsError::DeleteRootError);
        }
        todo!()
    }

    pub async fn mv(&self, inode_id: InodeId, parent_id: InodeId) -> VfsResult<()> {
        if inode_id == self.root_id() {
            return Err(VfsError::MoveRootError);
        }
        todo!()
    }

    pub async fn copy(&self, inode_id: InodeId, new_parent_id: InodeId) -> VfsResult<()> {
        if inode_id == self.root_id() {
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

type Container<T> = Inode<T, Vec<InodeId>>;
type ContainerMut<T> = InodeMut<T, Vec<InodeId>>;

impl<T> Container<T> {
    pub(crate) fn entries(&self) -> &Vec<InodeId> {
        &self.0.inner
    }
}

impl<T> ContainerMut<T> {
    pub(crate) fn entries_mut(&mut self) -> &mut Vec<InodeId> {
        &mut self.0.inner
    }
}

pub struct RootKind;
pub type Root = Container<RootKind>;
pub type RootMut = ContainerMut<RootKind>;
