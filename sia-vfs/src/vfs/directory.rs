use crate::vfs::{Container, ContainerMut, Name, Vfs, VfsResult};

pub struct DirectoryKind;
pub type Directory = Container<DirectoryKind>;
pub type DirectoryMut = ContainerMut<DirectoryKind>;

impl Vfs {
    pub async fn create_dir<T>(&self, parent: Container<T>, name: Name) -> VfsResult<Directory> {
        todo!()
    }
}
