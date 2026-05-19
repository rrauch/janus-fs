use crate::vfs::{
    Container, ContainerMut, InodeInner, InodeKey, Name, Normalizer, Read, RevisionHasher, Vfs,
    VfsResult, Write, hash_entries,
};

pub struct DirectoryKind;

impl RevisionHasher<Vec<InodeKey>> for DirectoryKind {
    fn hash(inner: &InodeInner<Self, Vec<InodeKey>>) -> blake3::Hash {
        let mut hasher = blake3::Hasher::new_derive_key("[sia-vfs]/[v0]/[directory_revision]");
        hasher.update(b"begin:\n");
        hash_entries(&inner.inner, &mut hasher);
        hasher.update(b"\nend");
        hasher.finalize()
    }
}

impl Normalizer<Vec<InodeKey>> for DirectoryKind {
    fn normalize(inner: &mut InodeInner<Self, Vec<InodeKey>>) {
        inner.inner.sort();
    }
}

pub type Directory = Container<DirectoryKind>;
pub type DirectoryMut = ContainerMut<DirectoryKind>;

impl<Mode: Read + Write> Vfs<Mode> {
    pub async fn create_dir<T: RevisionHasher<Vec<InodeKey>>>(
        &self,
        parent: Container<T>,
        name: Name,
    ) -> VfsResult<Directory> {
        todo!()
    }
}
