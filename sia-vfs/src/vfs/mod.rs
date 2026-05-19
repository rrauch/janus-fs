pub mod directory;
pub mod file;
pub mod path;

use crate::ContentId;
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
    #[error("root revision mismatch")]
    RootRevisionMismatch,
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

pub trait RevisionHasher<I>: Sized {
    fn hash(inner: &InodeInner<Self, I>) -> blake3::Hash;
}

pub trait Normalizer<I>: RevisionHasher<I> {
    fn normalize(inner: &mut InodeInner<Self, I>);
}

#[derive_where(Debug, Clone; I)]
struct InodeInner<T: RevisionHasher<I>, I> {
    id: InodeId,
    revision: Revision,
    name: Name,
    created: DateTime<Utc>,
    last_modified: DateTime<Utc>,
    //extended_attributes: HashMap<String, Bytes>,
    inner: I,
    _phantom: PhantomData<T>,
}

impl<T: RevisionHasher<I>, I> InodeInner<T, I> {
    fn hash_metadata(&self, hasher: &mut blake3::Hasher) {
        hasher.update(b"begin_metadata:\nid:");
        hasher.update(self.id.as_bytes());
        hasher.update(b"\nname:");
        hasher.update(self.name.as_bytes());
        hasher.update(b"\ncreated:");
        hasher.update(&self.created.timestamp().to_be_bytes());
        hasher.update(b"\nlast_modified:");
        hasher.update(&self.last_modified.timestamp().to_be_bytes());
        hasher.update(b"\nend_metadata");
    }
}

#[derive_where(Debug, Clone; I)]
pub struct Inode<T: RevisionHasher<I>, I>(Arc<InodeInner<T, I>>);

impl<T: RevisionHasher<I>, I> Inode<T, I> {
    pub fn id(&self) -> InodeId {
        self.0.id
    }

    pub fn revision(&self) -> &Revision {
        &self.0.revision
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

impl<T: RevisionHasher<I> + Normalizer<I>, I: Clone> Inode<T, I> {
    pub fn into_mut(self) -> InodeMut<T, I> {
        InodeMut(Arc::unwrap_or_clone(self.0))
    }
}

#[derive_where(Debug; I)]
pub struct InodeMut<T: RevisionHasher<I> + Normalizer<I>, I>(InodeInner<T, I>);

impl<T: RevisionHasher<I> + Normalizer<I>, I> InodeMut<T, I> {
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

    pub(crate) fn freeze(mut self) -> Inode<T, I> {
        self.update_revision();
        Inode(Arc::new(self.0))
    }

    fn update_revision(&mut self) {
        T::normalize(&mut self.0);
        self.0.revision = ContentId::new_internal(T::hash(&self.0));
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
        self.0.root.lock().expect("lock to not be poisoned").clone()
    }

    pub async fn get_by_key(&self, inode_key: &InodeKey) -> VfsResult<Option<Entry>> {
        let root = self.root();
        if inode_key.id == root.id() {
            if root.revision() != &inode_key.revision {
                return Err(VfsError::RootRevisionMismatch);
            }
            return Ok(Some(Entry::Root(root)));
        }
        todo!()
    }

    pub async fn list<T: RevisionHasher<Vec<InodeKey>>>(
        &self,
        inode: &Container<T>,
    ) -> VfsResult<impl TryStream<Ok = Entry, Error = VfsError> + Send + Unpin> {
        if true {
            todo!()
        }
        Ok(futures_util::stream::empty())
    }
}

impl<Mode: Read + Write> Vfs<Mode> {
    pub async fn update<T: RevisionHasher<I> + Normalizer<I>, I>(
        &self,
        modified_inode: InodeMut<T, I>,
    ) -> VfsResult<Inode<T, I>> {
        let inode = modified_inode.freeze();
        todo!()
    }

    pub async fn delete(&self, inode_id: InodeId) -> VfsResult<()> {
        if inode_id == self.root().id() {
            return Err(VfsError::DeleteRootError);
        }
        todo!()
    }

    pub async fn mv<T: RevisionHasher<Vec<InodeKey>>>(
        &self,
        inode_id: InodeId,
        parent: Container<T>,
    ) -> VfsResult<()> {
        if inode_id == self.root().id() {
            return Err(VfsError::MoveRootError);
        }
        todo!()
    }

    pub async fn copy<T: RevisionHasher<Vec<InodeKey>>>(
        &self,
        inode_id: InodeId,
        new_parent: Container<T>,
    ) -> VfsResult<()> {
        if inode_id == self.root().id() {
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct InodeKey {
    id: InodeId,
    revision: Revision,
}

pub struct RevisionKind;
pub type Revision = ContentId<RevisionKind>;

type Container<T> = Inode<T, Vec<InodeKey>>;
type ContainerMut<T> = InodeMut<T, Vec<InodeKey>>;

impl<T: RevisionHasher<Vec<InodeKey>>> Container<T> {
    fn entries(&self) -> &Vec<InodeKey> {
        &self.0.inner
    }
}

impl<T: RevisionHasher<Vec<InodeKey>> + Normalizer<Vec<InodeKey>>> ContainerMut<T> {
    fn entries_mut(&mut self) -> &mut Vec<InodeKey> {
        &mut self.0.inner
    }
}

pub struct RootKind;

impl RevisionHasher<Vec<InodeKey>> for RootKind {
    fn hash(inner: &InodeInner<Self, Vec<InodeKey>>) -> blake3::Hash {
        let mut hasher = blake3::Hasher::new_derive_key("[sia-vfs]/[v0]/[root_revision]");
        hasher.update(b"begin:\n");
        hash_entries(&inner.inner, &mut hasher);
        hasher.update(b"\nend");
        hasher.finalize()
    }
}

impl Normalizer<Vec<InodeKey>> for RootKind {
    fn normalize(inner: &mut InodeInner<Self, Vec<InodeKey>>) {
        inner.inner.sort();
    }
}

fn hash_entries(entries: &Vec<InodeKey>, hasher: &mut blake3::Hasher) {
    hasher.update(b"begin_entries:\nno_entries:");
    hasher.update(&entries.len().to_be_bytes());
    hasher.update(b"\nentries:");
    for entry in entries {
        hasher.update(b"inode_id:");
        hasher.update(entry.id.as_bytes());
        hasher.update(b"\nrevision:");
        hasher.update(entry.revision.as_ref());
        hasher.update(b"\n");
    }
    hasher.update(b"\nend_entries");
}

pub type Root = Container<RootKind>;
pub type RootMut = ContainerMut<RootKind>;
