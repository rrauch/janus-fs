use crate::blob::BlobId;
use crate::db::{
    DataError, Error as DbError, Read as DbRead, Transaction, TxScope, Write as DbWrite,
};
use crate::gen_flatbuffers::vfs::entity::{
    DirectoryEntry as FlatDirEntry, Entity as FlatEntity, EntityBody as FlatEntityBody,
    EntityBuilder,
};
use crate::object::ObjectId;
use crate::vfs::{Inode, InodeId, Name, NameError, OwnedName, StorageMode, Timestamp, VfsResult};
use crate::{ContentId, TypedUuid};
use derive_where::derive_where;
use flatbuffers::{FlatBufferBuilder, InvalidFlatbuffer, UnionWIPOffset, WIPOffset};
use std::borrow::Cow;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;
use thiserror::Error;
use yoke::{Yoke, Yokeable};

#[derive_where(Debug, Clone, PartialEq, Eq)]
pub enum Entity<T: EntityHandler> {
    Synced(SyncedEntity<T>),
    Local(LocalEntity<T>),
}

impl<T: EntityHandler> Entity<T> {
    #[inline]
    pub fn entity_id(&self) -> &EntityId {
        match self {
            Self::Synced(e) => e.entity_id(),
            Self::Local(e) => e.entity_id(),
        }
    }

    #[inline]
    pub fn name(&self) -> &Name {
        match self {
            Self::Synced(e) => e.name(),
            Self::Local(e) => e.name(),
        }
    }

    #[inline]
    pub fn created(&self) -> &Timestamp {
        match self {
            Self::Synced(e) => e.created(),
            Self::Local(e) => e.created(),
        }
    }

    #[inline]
    pub fn last_modified(&self) -> &Timestamp {
        match self {
            Self::Synced(e) => e.last_modified(),
            Self::Local(e) => e.last_modified(),
        }
    }

    #[inline]
    pub fn is_synced(&self) -> bool {
        match self {
            Self::Synced(_) => true,
            Self::Local(_) => false,
        }
    }

    #[inline]
    pub fn object_id(&self) -> Option<ObjectId> {
        match self {
            Self::Synced(e) => Some(e.object_id()),
            Self::Local(_) => None,
        }
    }

    #[inline]
    pub(crate) fn body(&self) -> &<T::Body as Yokeable<'_>>::Output {
        match self {
            Self::Synced(e) => e.body(),
            Self::Local(e) => e.body(),
        }
    }

    #[inline]
    pub(crate) fn into_mut(self) -> EntityMut<T> {
        match self {
            Self::Synced(e) => e.into_mut(),
            Self::Local(e) => e.into_mut(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncedMode {
    object_id: ObjectId,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalMode;
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DraftMode;

pub type SyncedEntity<T> = RawEntity<T, SyncedMode>;
pub type LocalEntity<T> = RawEntity<T, LocalMode>;
pub type DraftEntity<T> = RawEntity<T, DraftMode>;

#[derive(Debug, Error)]
pub enum EntityError {
    #[error(transparent)]
    InvalidFlatbuffer(#[from] InvalidFlatbuffer),
    #[error("revision mismatch: [{expected}] != [{actual}]")]
    RevisionMismatch {
        expected: Revision,
        actual: Revision,
    },
    #[error(transparent)]
    NameError(#[from] NameError),
    #[error("invalid timestamp")]
    TimestampError,
    #[error("expected directory entity")]
    ExpectedDirectory,
    #[error("expected file entity")]
    ExpectedFile,
    #[error("bytemuck error: {0}")]
    BytemuckError(String),
    #[error("incorrect entity type: [{expected}] != [{actual}]")]
    IncorrectType { expected: String, actual: String },
}

pub trait EntityHandler: Sized {
    type Body: for<'a> Yokeable<'a> + Clone;

    fn db_type() -> &'static str;

    fn to_owned(body: &<Self::Body as Yokeable>::Output) -> Self::Body;

    fn extract(entity: FlatEntity) -> Result<<Self::Body as Yokeable>::Output, EntityError>;
    fn serialize_body(
        b: &mut FlatBufferBuilder,
        entity: &EntityMut<Self>,
    ) -> (FlatEntityBody, WIPOffset<UnionWIPOffset>);

    fn normalize(value: &mut Self::Body);
    fn hash(entity: &RawEntityInner<Self>) -> blake3::Hash;

    fn references(entity: &RawEntityInner<Self>) -> Vec<EntityRef<'_>>;
}

#[derive_where(Debug, PartialEq, Eq)]
pub(super) struct RawEntityInner<T: EntityHandler> {
    revision: Revision,
    created: Timestamp,
    last_modified: Timestamp,
    metadata: Yoke<Metadata<'static>, Arc<[u8]>>,
    #[derive_where(skip)]
    body: Yoke<T::Body, Arc<[u8]>>,
    _phantom: PhantomData<T>,
}

impl<T: EntityHandler> RawEntityInner<T> {
    pub fn id(&self) -> &EntityId {
        self.metadata.get().id
    }

    pub fn revision(&self) -> &Revision {
        &self.revision
    }

    pub fn name(&self) -> &Name {
        self.metadata.get().name
    }

    pub fn created(&self) -> &Timestamp {
        &self.created
    }

    pub fn last_modified(&self) -> &Timestamp {
        &self.last_modified
    }

    pub fn body(&self) -> &<T::Body as Yokeable<'_>>::Output {
        self.body.get()
    }
}

#[derive(Yokeable, Debug, PartialEq, Eq)]
struct Metadata<'a> {
    id: &'a EntityId,
    name: &'a Name,
}

impl<T: EntityHandler> RawEntityInner<T> {
    fn try_from_flatbuffer(buffer: Arc<[u8]>) -> Result<Self, EntityError> {
        let mut created = None;
        let mut last_modified = None;

        let metadata = Yoke::try_attach_to_cart::<EntityError, _>(buffer.clone(), |data| {
            let flat_entity = flatbuffers::root::<FlatEntity>(data)?;
            let id = EntityId::from_byte_ref(&flat_entity.id().0);
            let name: &Name = flat_entity.name().try_into()?;
            created = Some(
                Timestamp::from_millis(flat_entity.created()).ok_or(EntityError::TimestampError)?,
            );
            last_modified = Some(
                Timestamp::from_millis(flat_entity.last_modified())
                    .ok_or(EntityError::TimestampError)?,
            );

            Ok(Metadata { id, name })
        })?;

        let body = Yoke::try_attach_to_cart::<EntityError, _>(buffer, |data| {
            T::extract(flatbuffers::root::<FlatEntity>(data)?)
        })?;

        let mut this = Self {
            revision: Revision::zeroed(), // placeholder
            created: created.unwrap(),
            last_modified: last_modified.unwrap(),
            metadata,
            body,
            _phantom: PhantomData,
        };

        let revision = Revision::new_internal(T::hash(&this));
        this.revision = revision;

        Ok(this)
    }
}

#[derive_where(Debug, Clone, PartialEq, Eq; Mode)]
pub struct RawEntity<T: EntityHandler, Mode>(Arc<(RawEntityInner<T>, Mode)>);

impl<T: EntityHandler, Mode> RawEntity<T, Mode> {
    pub(super) fn new(inner: RawEntityInner<T>, mode: Mode) -> Self {
        Self(Arc::new((inner, mode)))
    }
}

impl<T: EntityHandler> RawEntityInner<T> {
    pub(crate) fn hash_metadata(&self, hasher: &mut blake3::Hasher) {
        hasher.update(b"begin_metadata:\nid:");
        hasher.update(self.id().as_slice());
        hasher.update(b"\nname:");
        hasher.update(self.name().as_bytes());
        hasher.update(b"\ncreated:");
        hasher.update(&self.created().to_millis().to_be_bytes());
        hasher.update(b"\nlast_modified:");
        hasher.update(&self.last_modified().to_millis().to_be_bytes());
        hasher.update(b"\nend_metadata");
    }
}

impl<T: EntityHandler, Mode> RawEntity<T, Mode> {
    pub fn entity_id(&self) -> &EntityId {
        self.0.0.id()
    }

    pub fn revision(&self) -> &Revision {
        self.0.0.revision()
    }

    pub fn name(&self) -> &Name {
        self.0.0.name()
    }

    pub fn created(&self) -> &Timestamp {
        self.0.0.created()
    }

    pub fn last_modified(&self) -> &Timestamp {
        self.0.0.last_modified()
    }

    pub fn body(&self) -> &<T::Body as Yokeable<'_>>::Output {
        self.0.0.body()
    }
    pub(crate) fn to_flatbuffer(&self) -> Arc<[u8]> {
        self.0.0.metadata.backing_cart().clone()
    }

    pub(crate) fn references(&self) -> Vec<EntityRef<'_>> {
        T::references(&self.0.0)
    }
}

impl<T: EntityHandler> RawEntity<T, SyncedMode> {
    pub fn object_id(&self) -> ObjectId {
        self.0.1.object_id
    }
}

impl<T: EntityHandler, Mode: Clone> RawEntity<T, Mode> {
    pub fn into_mut(self) -> EntityMut<T> {
        EntityMut {
            id: self.entity_id().clone(),
            name: self.name().to_owned(),
            created: self.created().clone(),
            last_modified: self.last_modified().clone(),
            body: T::to_owned(self.body()),
            _phantom: PhantomData,
        }
    }
}

#[derive_where(Debug, Clone)]
pub struct EntityMut<T: EntityHandler> {
    id: EntityId,
    name: OwnedName,
    created: Timestamp,
    last_modified: Timestamp,
    //extended_attributes: HashMap<String, Bytes>,
    #[derive_where(skip)]
    body: <T as EntityHandler>::Body,
    _phantom: PhantomData<T>,
}

impl<T: EntityHandler> EntityMut<T> {
    pub(super) fn new(name: OwnedName, body: <T as EntityHandler>::Body) -> Self {
        let now = Timestamp::now();
        Self {
            id: EntityId::generate(),
            name,
            created: now,
            last_modified: now,
            body,
            _phantom: PhantomData,
        }
    }

    pub fn id(&self) -> &EntityId {
        &self.id
    }

    pub fn name(&self) -> &Name {
        &self.name
    }

    pub fn set_name(&mut self, new: OwnedName) {
        self.name = new;
    }

    pub fn created(&self) -> &Timestamp {
        &self.created
    }

    pub fn last_modified(&self) -> &Timestamp {
        &self.last_modified
    }

    pub fn set_last_modified(&mut self, new: Timestamp) {
        self.last_modified = new;
    }

    pub(crate) fn body(&self) -> &<T as EntityHandler>::Body {
        &self.body
    }

    pub(crate) fn body_mut(&mut self) -> &mut <T as EntityHandler>::Body {
        &mut self.body
    }

    pub(crate) fn set_body(&mut self, new: <T as EntityHandler>::Body) {
        self.body = new;
    }

    pub(crate) fn freeze(mut self) -> DraftEntity<T> {
        T::normalize(&mut self.body);
        let inner = RawEntityInner::try_from_flatbuffer(Arc::from(self.to_flatbuffer()))
            .expect("deserialization to never fail");
        RawEntity::new(inner, DraftMode)
    }

    fn to_flatbuffer(&self) -> Vec<u8> {
        let mut b = FlatBufferBuilder::new();

        // Strings (offsets) must be created BEFORE the parent table starts.
        let name = b.create_string(self.name().as_ref());

        let (body_type, body) = T::serialize_body(&mut b, &self);

        let id = self.id.as_flatbuffer();

        let mut eb = EntityBuilder::new(&mut b);
        eb.add_id(id);
        eb.add_name(name);
        eb.add_created(self.created.to_millis());
        eb.add_last_modified(self.last_modified.to_millis());
        eb.add_body_type(body_type);
        eb.add_body(body);
        let entity = eb.finish();
        b.finish(entity, None);
        b.finished_data().to_vec()
    }
}

#[repr(C)]
#[derive(
    Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, bytemuck::Pod, bytemuck::Zeroable,
)]
pub struct EntityKey {
    id: EntityId,
    revision: Revision,
}

// ensure EntityKey can be safely cast from Flatbuffer
const _: () = {
    assert!(size_of::<EntityKey>() == size_of::<FlatDirEntry>());
    assert!(align_of::<EntityKey>() == align_of::<FlatDirEntry>());
    assert!(std::mem::offset_of!(EntityKey, id) == 0);
    assert!(std::mem::offset_of!(EntityKey, revision) == 16);
    assert!(align_of::<EntityKey>() == 1);
};

impl EntityKey {
    pub(crate) fn new(id: EntityId, revision: Revision) -> Self {
        Self { id, revision }
    }

    pub fn id(&self) -> &EntityId {
        &self.id
    }

    pub fn revision(&self) -> &Revision {
        &self.revision
    }
}

pub struct EntityKind;
pub type EntityId = TypedUuid<EntityKind>;

pub struct RevisionKind;
pub type Revision = ContentId<RevisionKind>;

impl<C: TxScope> Transaction<C>
where
    Self: DbRead,
{
    pub(super) async fn entity_by_key<T: EntityHandler>(
        &mut self,
        key: &EntityKey,
    ) -> Result<Option<Entity<T>>, DbError>
    where
        Entity<T>: TryFrom<EntityRow>,
    {
        let id_ref = key.id.as_slice();
        let rev_ref = key.revision.as_slice();

        Ok(sqlx::query!(
            "SELECT name, mode, entity_type, object_id, data FROM entity WHERE id = ? and revision = ?",
            id_ref,
            rev_ref,
        )
            .fetch_optional(self.conn())
            .await?
            .map(|r| -> Result<EntityRow, DbError> {
                Ok(EntityRow {
                    id: key.id.clone(),
                    revision: key.revision.clone(),
                    name: OwnedName::try_from(r.name).map_err(|e| DataError::ConversionError(e.to_string().into()))?,
                    mode: match r.mode.as_str() {
                        "L" => StorageMode::Local,
                        "S" => StorageMode::Synced(r.object_id.map(ObjectId::from).ok_or(DataError::MissingObject)?),
                        other => return Err(DataError::ConversionError(format!("invalid mode: {}", other).into()))?,
                    },
                    entity_type: r.entity_type.into(),
                    data: Arc::from(r.data.unwrap_or_default()),
                })
            })
            .transpose()?
            .map(|e| {
                e.try_into()
                    .map_err(|_| DataError::ConversionError("invalid row".into()))
            })
            .transpose()?)
    }
}

pub(crate) struct EntityRow {
    pub id: EntityId,
    pub revision: Revision,
    pub name: OwnedName,
    pub mode: StorageMode,
    pub entity_type: Cow<'static, str>,
    pub data: Arc<[u8]>,
}

pub(crate) enum EntityRef<'a> {
    Blob(Cow<'a, BlobId>),
    Entity(Cow<'a, EntityKey>),
}

impl From<BlobId> for EntityRef<'static> {
    fn from(value: BlobId) -> Self {
        Self::Blob(Cow::Owned(value))
    }
}

impl<'a> From<&'a BlobId> for EntityRef<'a> {
    fn from(value: &'a BlobId) -> Self {
        Self::Blob(Cow::Borrowed(value))
    }
}

impl From<EntityKey> for EntityRef<'static> {
    fn from(value: EntityKey) -> Self {
        Self::Entity(Cow::Owned(value))
    }
}

impl<'a> From<&'a EntityKey> for EntityRef<'a> {
    fn from(value: &'a EntityKey) -> Self {
        Self::Entity(Cow::Borrowed(value))
    }
}

impl<T: EntityHandler> From<&DraftEntity<T>> for EntityRow {
    fn from(entity: &DraftEntity<T>) -> Self {
        let id = entity.entity_id().clone();
        let revision = entity.revision().clone();
        let name = entity.name().to_owned();
        let data = entity.to_flatbuffer();

        Self {
            id,
            revision,
            name,
            mode: StorageMode::Local,
            entity_type: T::db_type().into(),
            data,
        }
    }
}

impl<T: EntityHandler> TryFrom<EntityRow> for Entity<T> {
    type Error = EntityError;

    fn try_from(value: EntityRow) -> Result<Self, Self::Error> {
        let raw_entity = RawEntityInner::try_from_flatbuffer(value.data)?;
        if raw_entity.revision() != &value.revision {
            return Err(EntityError::RevisionMismatch {
                expected: value.revision,
                actual: raw_entity.revision,
            });
        };

        Ok(match value.mode {
            StorageMode::Synced(object_id) => {
                Entity::Synced(SyncedEntity::new(raw_entity, SyncedMode { object_id }))
            }
            StorageMode::Local => Entity::Local(LocalEntity::new(raw_entity, LocalMode)),
        })
    }
}

impl<C: TxScope> Transaction<C>
where
    Self: DbWrite,
{
    pub(crate) async fn create_entity_if_not_exist<T: EntityHandler>(
        &mut self,
        draft_entity: DraftEntity<T>,
    ) -> Result<EntityKey, DbError> {
        let id = draft_entity.entity_id();
        let rev = draft_entity.revision();
        let id_slice = id.as_slice();
        let rev_slice = rev.as_slice();

        if sqlx::query!(
            "SELECT EXISTS(SELECT 1 FROM entity WHERE id = ? AND revision = ?) as \"entity_exists: bool\"",
            id_slice,
            rev_slice,
        )
            .fetch_one(self.conn())
            .await?
            .entity_exists
        {
            // entity already exists
            return Ok(EntityKey {
                id: id.clone(),
                revision: rev.clone(),
            });
        }

        // entity does not exist yet, creating from scratch
        let refs = draft_entity.references();
        let row = EntityRow::from(&draft_entity);
        let id = row.id.as_slice();
        let rev = row.revision.as_slice();
        let name = row.name.as_ref();
        let entity_type = row.entity_type.as_ref();
        let (mode, object_id) = match &row.mode {
            StorageMode::Local => ("L", None),
            StorageMode::Synced(oid) => ("S", Some(*oid.deref() as i64)),
        };
        let data = row.data.as_ref();

        sqlx::query!(
            "INSERT INTO entity (id, revision, name, entity_type, mode, object_id, data)
                    VALUES (?, ?, ?, ?, ?, ?, ?)",
            id,
            rev,
            name,
            entity_type,
            mode,
            object_id,
            data,
        )
        .execute(self.conn())
        .await?;

        for entity_ref in refs {
            let id = row.id.as_slice();
            let (ref_type, target_entity_id, target_entity_rev, target_blob) = match &entity_ref {
                EntityRef::Blob(blob_id) => ("B", None, None, Some(blob_id.as_slice())),
                EntityRef::Entity(entity_key) => (
                    "E",
                    Some(entity_key.id.as_slice()),
                    Some(entity_key.revision.as_slice()),
                    None,
                ),
            };
            sqlx::query!(
                "INSERT INTO entity_references (entity_id, entity_rev, ref_type, target_entity_id, target_entity_rev, target_blob)
                        VALUES (?, ?, ?, ?, ?, ?)",
                id,
                rev,
                ref_type,
                target_entity_id,
                target_entity_rev,
                target_blob,
            ).execute(self.conn()).await?;
        }

        Ok(EntityKey {
            id: row.id,
            revision: row.revision,
        })
    }

    pub(crate) async fn update<T: EntityHandler>(
        &mut self,
        inode_id: InodeId,
        name: &Name,
        draft_entity: DraftEntity<T>,
    ) -> VfsResult<Inode> {
        let entity_id = self.create_entity_if_not_exist(draft_entity).await?;
        self.update_inode(inode_id, &name, &entity_id).await?;
        Ok(self
            .inode_by_id(inode_id)
            .await?
            .ok_or_else(|| DbError::DataError(DataError::InodeNotFound(inode_id)))?)
    }
}
