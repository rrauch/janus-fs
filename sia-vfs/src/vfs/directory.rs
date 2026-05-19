use crate::vfs::entity::{EntityKey, Normalizer, RawEntityInner, RevisionHasher};
use crate::vfs::{Container, ContainerMut, Name, Read, Vfs, VfsResult, Write, hash_entries};
use blake3::{Hash, Hasher};

pub struct DirectoryKind;

impl<Mode> RevisionHasher<Vec<EntityKey>, Mode> for DirectoryKind {
    fn hash(inner: &RawEntityInner<Self, Vec<EntityKey>, Mode>) -> Hash {
        let mut hasher = Hasher::new_derive_key("[sia-vfs]/[v0]/[directory_revision]");
        hasher.update(b"begin:\n");
        hash_entries(&inner.inner, &mut hasher);
        hasher.update(b"\nend");
        hasher.finalize()
    }
}

impl Normalizer<Vec<EntityKey>> for DirectoryKind {
    fn normalize(value: &mut Vec<EntityKey>) {
        value.sort();
    }
}

pub type Directory = Container<DirectoryKind>;
pub type DirectoryMut = ContainerMut<DirectoryKind>;

impl<Mode: Read + Write> Vfs<Mode> {
    pub async fn create_dir<M, T: RevisionHasher<Vec<EntityKey>, M>>(
        &self,
        parent: &Container<T>,
        name: Name,
    ) -> VfsResult<Directory> {
        todo!()
    }
}
