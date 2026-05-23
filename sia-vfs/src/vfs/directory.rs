use crate::blob::BlobId;
use crate::db::{Error as DbError, Transaction, TxScope, Write as DbWrite};
use crate::vfs::entity::{
    DraftEntity, DraftMode, Entity, EntityHasher, EntityId, EntityRow, Freezable, Normalizer,
    RawEntityInner,
};
use crate::vfs::{
    AsDbType, Container, Inode, InodeId, Name, Read, Vfs, VfsError, VfsResult, Write, hash_entries,
};
use blake3::{Hash, Hasher};
use chrono::Utc;

pub struct DirectoryKind;

impl AsDbType for DirectoryKind {
    fn db_type() -> &'static str {
        "D"
    }
}

impl<Mode> EntityHasher<Vec<EntityId>, Mode> for DirectoryKind {
    fn hash(inner: &RawEntityInner<Self, Vec<EntityId>, Mode>) -> Hash {
        let mut hasher = Hasher::new_derive_key("[sia-vfs]/[v0]/[directory_revision]");
        hasher.update(b"begin:\n");
        inner.hash_metadata(&mut hasher);
        hash_entries(&inner.inner, &mut hasher);
        hasher.update(b"\nend");
        hasher.finalize()
    }
}

impl Normalizer<Vec<EntityId>> for DirectoryKind {
    fn normalize(value: &mut Vec<EntityId>) {
        value.sort();
    }
}

pub type Directory = Container<DirectoryKind, InodeId>;

type DirectoryDraft = DraftEntity<DirectoryKind, Vec<EntityId>>;

impl DirectoryDraft {
    fn new_directory_draft(name: Name) -> Self {
        let now = Utc::now();
        Self::new(
            EntityId::zeroed(),
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

impl From<DirectoryDraft> for EntityRow {
    fn from(value: DirectoryDraft) -> Self {
        //Self::from((value, None::<BlobId>, Some(data)))
        todo!()
    }
}

impl<Mode: Read + Write> Vfs<Mode> {
    pub async fn create_dir<M, T: EntityHasher<Vec<EntityId>, M>, P>(
        &self,
        parent: &Container<T, P>,
        name: Name,
    ) -> VfsResult<Directory> {
        let mut tx = self.tx_rw().await?;
        let inode_id = tx.create_dir(name, parent.inode_id()).await?;
        let dir = match tx.inode_by_id(inode_id).await? {
            Some(Inode::Directory(dir)) => dir,
            _ => {
                return Err(VfsError::Other(format!(
                    "inode {} is not a directory",
                    inode_id
                )));
            }
        };
        tx.commit().await?;
        Ok(dir)
    }
}

impl TryFrom<EntityRow> for Entity<DirectoryKind, Vec<EntityId>> {
    type Error = String;

    fn try_from(value: EntityRow) -> Result<Self, Self::Error> {
        if value.entity_type != "D" {
            return Err(format!(
                "invalid entity_type; expected 'D' but got '{}'",
                value.entity_type
            ));
        }

        Container::<_, ()>::try_from_row(value)
    }
}

impl<C: TxScope> Transaction<C>
where
    Self: DbWrite,
{
    async fn create_dir(
        &mut self,
        name: Name,
        parent_inode_id: InodeId,
    ) -> Result<InodeId, DbError> {
        let entity = DirectoryDraft::new_directory_draft(name.clone());
        let entity_id = self.create_entity_if_not_exist(entity).await?;
        Ok(self
            .create_inode::<DirectoryKind>(&name, parent_inode_id, entity_id)
            .await?)
    }
}
