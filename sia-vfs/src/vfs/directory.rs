use crate::db::{DataError, Error as DbError, Transaction, TxScope, Write as DbWrite};
use crate::gen_flatbuffers::vfs::entity::{
    Directory as FlatDir, DirectoryArgs, DirectoryEntry as FlatDirEntry, Entity as FlatEntity,
    EntityBody as FlatEntityBody,
};
use crate::vfs::entity::{
    DraftEntity, EntityError, EntityHandler, EntityKey, EntityMut, EntityRef, RawEntityInner,
};
use crate::vfs::{Inode, InodeId, InodeMut, Name, OwnedName, TypedInode, Vfs, VfsError, VfsResult};
use blake3::{Hash, Hasher};
use flatbuffers::{FlatBufferBuilder, UnionWIPOffset, WIPOffset};
use futures_util::{StreamExt, TryStream};
use std::borrow::Cow;
use std::collections::VecDeque;
use yoke::Yokeable;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct DirectoryKind;

#[derive(Yokeable, Clone)]
pub struct DirectoryBody<'a> {
    entries: Cow<'a, [EntityKey]>,
}

impl DirectoryBody<'static> {
    pub(crate) fn new(entries: Vec<EntityKey>) -> Self {
        Self {
            entries: Cow::Owned(entries),
        }
    }

    fn sort(&mut self) {
        self.entries.to_mut().sort();
    }
}

impl DirectoryBody<'_> {
    pub fn into_owned(self) -> DirectoryBody<'static> {
        DirectoryBody {
            entries: Cow::Owned(self.entries.into_owned()),
        }
    }

    pub(crate) fn entries(&self) -> &[EntityKey] {
        &self.entries
    }
}

impl EntityHandler for DirectoryKind {
    type Body = DirectoryBody<'static>;
    const DB_TYPE: &'static str = "D";
    const METADATA_TYPE: &'static str = "DIRECTORY";

    fn to_owned(body: &<Self::Body as Yokeable>::Output) -> Self::Body {
        body.clone().into_owned()
    }

    fn extract(entity: FlatEntity) -> Result<<Self::Body as Yokeable>::Output, EntityError> {
        let dir = entity
            .body_as_directory()
            .ok_or(EntityError::ExpectedDirectory)?;
        let entries = match dir.entries() {
            Some(v) => bytemuck::try_cast_slice(v.bytes())
                .map_err(|e| EntityError::BytemuckError(e.to_string()))?,
            None => &[],
        };
        Ok(DirectoryBody {
            entries: Cow::Borrowed(entries),
        })
    }

    fn serialize_body(
        b: &mut FlatBufferBuilder,
        entity: &EntityMut<Self>,
    ) -> (FlatEntityBody, WIPOffset<UnionWIPOffset>) {
        let entries = entity
            .body()
            .entries
            .iter()
            .map(|key| FlatDirEntry::new(key.id().as_flatbuffer(), key.revision().as_flatbuffer()))
            .collect::<Vec<_>>();
        let entries_vec = b.create_vector(&entries);

        let dir = FlatDir::create(
            b,
            &DirectoryArgs {
                entries: Some(entries_vec),
            },
        );

        (FlatEntityBody::Directory, dir.as_union_value())
    }

    fn normalize(value: &mut Self::Body) {
        value.sort();
    }

    fn hash(entity: &RawEntityInner<Self>) -> Hash {
        let mut hasher = Hasher::new_derive_key("[sia-vfs]/[v0]/[directory_entity]");
        hasher.update(b"begin:\n");
        entity.hash_metadata(&mut hasher);
        hash_entries(&entity.body().entries, &mut hasher);
        hasher.update(b"\nend");
        hasher.finalize()
    }

    fn references(entity: &RawEntityInner<Self>) -> Vec<EntityRef<'_>> {
        entity
            .body()
            .entries
            .iter()
            .map(|e| e.into())
            .collect::<Vec<_>>()
    }
}

fn hash_entries(entries: &[EntityKey], hasher: &mut Hasher) {
    hasher.update(b"begin_entries:\nno_entries:");
    hasher.update(&entries.len().to_be_bytes());
    hasher.update(b"\nentries:");
    for entry in entries {
        hasher.update(b"entity_id:");
        hasher.update(entry.id().as_slice());
        hasher.update(b"\nentity_revision:");
        hasher.update(entry.revision().as_slice());
        hasher.update(b"\n");
    }
    hasher.update(b"\nend_entries");
}

pub type Directory = TypedInode<DirectoryKind>;

impl TryFrom<Inode> for Directory {
    type Error = Inode;

    fn try_from(value: Inode) -> Result<Self, Self::Error> {
        match value {
            Inode::Directory(dir) => Ok(dir),
            Inode::File(file) => Err(Inode::File(file)),
        }
    }
}

impl Directory {
    pub fn is_root(&self) -> bool {
        self.parent.is_none()
    }

    pub(crate) fn entries(&self) -> &[EntityKey] {
        &self.body().entries
    }
}

pub(crate) type DirectoryMut = InodeMut<DirectoryKind>;
pub(crate) type DirectoryDraft = DraftEntity<DirectoryKind>;

impl DirectoryDraft {
    pub fn new_directory_draft(name: OwnedName, entries: Vec<EntityKey>) -> Self {
        EntityMut::new(name, DirectoryBody::new(entries)).freeze()
    }
}

impl Vfs {
    pub async fn list(
        &self,
        dir: &Directory,
    ) -> VfsResult<impl TryStream<Ok = Inode, Error = VfsError> + Send + Unpin> {
        let this = self.clone();

        Ok(futures_util::stream::try_unfold(
            VecDeque::from_iter(
                dir.entries()
                    .iter()
                    .map(|e| InodeId::from_entity_id(e.id())),
            ),
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
}

impl Vfs {
    pub async fn create_dir(&self, parent: &Directory, name: &Name) -> VfsResult<Directory> {
        if self.is_read_only() {
            return Err(VfsError::ReadOnlyFileSystem);
        }

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

impl<C: TxScope> Transaction<C>
where
    Self: DbWrite,
{
    async fn create_dir(
        &mut self,
        name: &Name,
        parent_inode_id: InodeId,
    ) -> Result<InodeId, DbError> {
        let entity = DirectoryDraft::new_directory_draft(name.to_owned(), vec![]);
        let entity_id = self.register_entity(entity).await?;
        Ok(self
            .create_inode::<DirectoryKind>(&name, parent_inode_id, entity_id)
            .await?)
    }
}
