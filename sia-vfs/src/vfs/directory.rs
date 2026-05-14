use crate::vfs::{Container, ContainerMut, InodeId, Name, Vfs, VfsResult};

pub struct DirectoryKind;
pub type Directory = Container<DirectoryKind>;
pub type DirectoryMut = ContainerMut<DirectoryKind>;

impl Vfs {
    pub async fn create_dir(&self, parent_id: InodeId, name: Name) -> VfsResult<Directory> {
        todo!()
    }
}
