use crate::blob::BlobId;
use crate::db::{
    DataError, Error as DbError, Read as DbRead, Transaction, TxScope, Write as DbWrite,
};
use crate::gen_flatbuffers::vfs::entity::{
    DirectoryEntry as FlatDirEntry, Entity as FlatEntity, EntityBody as FlatEntityBody,
    EntityBuilder,
};
use crate::object::metadata::MetadataMut;
use crate::object::{ObjectCreateResult, ObjectId};
use crate::sync::push::{EntityType, JobItem, PushTask};
use crate::sync::{Error as SyncError, Error, PullTask};
use crate::vfs::directory::DirectoryKind;
use crate::vfs::file::FileKind;
use crate::vfs::{
    Inode, InodeId, Name, NameError, OwnedName, StorageMode, Timestamp, VfsError, VfsId, VfsResult,
};
use crate::{ContentId, TypedUuid, object};
use derive_where::derive_where;
use flatbuffers::{FlatBufferBuilder, InvalidFlatbuffer, UnionWIPOffset, WIPOffset};
use futures_util::AsyncReadExt;
use futures_util::io::Cursor;
use janus_io::Client as Sia;
use janus_io::object::Object as SiaObject;
use janus_io::object::ObjectId as SiaObjectId;
use janus_io::upload::UploadableObject;
use std::borrow::Cow;
use std::cell::Cell;
use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;
use thiserror::Error;
use yoke::{Yoke, Yokeable};

pub(crate) const METADATA_OBJECT_TYPE: &'static str = "ENTITY";
pub(crate) const METADATA_ENTITY_ID: &'static str = "ENTITY-ID";
pub(crate) const METADATA_ENTITY_REVISION: &'static str = "ENTITY-REVISION";
pub(crate) const METADATA_ENTITY_TYPE: &'static str = "ENTITY-TYPE";

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
    pub fn revision(&self) -> &Revision {
        match self {
            Self::Synced(e) => e.revision(),
            Self::Local(e) => e.revision(),
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

impl<T: EntityHandler> SyncedEntity<T> {
    pub(crate) async fn load_from_backend(
        object_id: ObjectId,
        sia_oid: &SiaObjectId,
        sia_client: &Sia,
    ) -> Result<Self, std::io::Error> {
        let dl = sia_client
            .download(sia_oid)
            .await
            .map_err(std::io::Error::other)?;

        let metadata: object::metadata::Metadata = dl
            .object()
            .metadata()
            .try_into()
            .map_err(std::io::Error::other)?;

        if metadata.get(object::METADATA_VFS_OBJECT_TYPE) != Some(METADATA_OBJECT_TYPE) {
            Err(std::io::Error::other("METADATA_OBJECT_TYPE mismatch"))?
        }

        if metadata.get(METADATA_ENTITY_TYPE) != Some(T::METADATA_TYPE) {
            Err(std::io::Error::other("METADATA_ENTITY_TYPE mismatch"))?
        }

        let mut buffer = Vec::with_capacity(dl.object().size() as usize);
        let mut reader = dl.open().await.map_err(std::io::Error::other)?;
        reader.read_to_end(&mut buffer).await?;
        let this = Self::try_from_flatbuffer(Arc::from(buffer), object_id)
            .map_err(std::io::Error::other)?;

        let entity_id = this.entity_id().to_string();
        if metadata.get(METADATA_ENTITY_ID) != Some(entity_id.as_str()) {
            Err(std::io::Error::other("METADATA_ENTITY_ID mismatch"))?
        }

        let revision = this.revision().to_string();
        if metadata.get(METADATA_ENTITY_REVISION) != Some(revision.as_str()) {
            Err(std::io::Error::other("METADATA_ENTITY_REVISION mismatch"))?
        }

        Ok(this)
    }

    pub(crate) fn try_from_flatbuffer(
        buffer: Arc<[u8]>,
        object_id: ObjectId,
    ) -> Result<Self, EntityError> {
        Ok(Self::new(
            RawEntityInner::try_from_flatbuffer(buffer)?,
            SyncedMode { object_id },
        ))
    }
}

pub type LocalEntity<T> = RawEntity<T, LocalMode>;

impl<T: EntityHandler> LocalEntity<T> {
    pub(crate) fn into_synced(self, object_id: ObjectId) -> SyncedEntity<T> {
        let (inner, _) = Arc::unwrap_or_clone(self.0);
        RawEntity(Arc::new((inner, SyncedMode { object_id })))
    }
}

pub type DraftEntity<T> = RawEntity<T, DraftMode>;

#[derive(Debug, Error)]
pub enum EntityError {
    #[error(transparent)]
    InvalidFlatbuffer(#[from] InvalidFlatbuffer),
    #[error("id mismatch: [{expected}] != [{actual}]")]
    IdMismatch {
        expected: EntityId,
        actual: EntityId,
    },
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
    #[error("expected local mode")]
    ExpectedLocalMode,
    #[error("bytemuck error: {0}")]
    BytemuckError(String),
    #[error("incorrect entity type: [{expected}] != [{actual}]")]
    IncorrectType { expected: String, actual: String },
    #[error("invalid entity id")]
    InvalidId,
    #[error("invalid entity revision")]
    InvalidRevision,
}

pub trait EntityHandler: Sized {
    type Body: for<'a> Yokeable<'a, Output: Clone> + Clone;
    const DB_TYPE: &'static str;
    const METADATA_TYPE: &'static str;

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

#[derive_where(Clone, Debug, PartialEq, Eq)]
pub struct RawEntityInner<T: EntityHandler> {
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

#[derive(Yokeable, Clone, Debug, PartialEq, Eq)]
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

    pub(crate) fn to_uploadable_object(
        &self,
        vfs_id: &VfsId,
    ) -> UploadableObject<object::metadata::Metadata<'_>, Cursor<Arc<[u8]>>> {
        let mut metadata = MetadataMut::with_vfs_template(vfs_id, METADATA_OBJECT_TYPE);
        metadata.insert(METADATA_ENTITY_ID.to_string(), self.entity_id().to_string());
        metadata.insert(
            METADATA_ENTITY_REVISION.to_string(),
            self.revision().to_string(),
        );
        metadata.insert(
            METADATA_ENTITY_TYPE.to_string(),
            T::METADATA_TYPE.to_string(),
        );

        UploadableObject::new(
            format!("/entities/{}/{}", self.entity_id(), self.revision()),
            Cursor::new(self.to_flatbuffer()),
            Some(metadata.freeze()),
        )
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

impl EntityId {
    pub(crate) fn generate() -> Self {
        Self::_generate()
    }
}

pub struct RevisionKind;
pub type Revision = ContentId<RevisionKind>;

impl<C: TxScope> Transaction<C>
where
    Self: DbRead,
{
    pub(crate) async fn entity_by_key<T: EntityHandler>(
        &mut self,
        key: &EntityKey,
    ) -> Result<Option<Entity<T>>, DbError> {
        let id_ref = key.id.as_slice();
        let rev_ref = key.revision.as_slice();

        if let Some(entity_row) = sqlx::query!(
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
                        "L" => StorageMode::Local(Arc::from(r.data.unwrap_or_default())),
                        "S" => StorageMode::Synced(r.object_id.map(ObjectId::from).ok_or(DataError::MissingObject)?),
                        other => return Err(DataError::ConversionError(format!("invalid mode: {}", other).into()))?,
                    },
                    entity_type: r.entity_type.into(),
                })
            })
            .transpose()? {
            let object_id = match &entity_row.mode {
                StorageMode::Local(_) => None,
                StorageMode::Synced(object_id) => Some(*object_id),
            };

            Ok(match object_id {
                Some(object_id) => {
                    // synced entity
                    let object = self.object_by_id(object_id).await?.ok_or_else(|| DataError::ObjectNotFound(object_id))?;
                    let sia_oid = object.try_to_sia_oid().ok_or_else(|| DataError::ConversionError("invalid remote_location".into()))?;
                    Ok::<_, DbError>(Some(Entity::Synced(SyncedEntity::<T>::load_from_backend(object_id, &sia_oid, self.sia_client()).await?)))
                }
                None => {
                    // local entity
                    Ok(Some(Entity::Local(LocalEntity::<T>::try_from(entity_row).map_err(|_| DataError::ConversionError("invalid row".into()))?)))
                }
            }.map(|o| o.map(|e| if e.entity_id() != &key.id || e.revision() != &key.revision {
                Err(DataError::EntityMismatch)
            } else {
                Ok(e)
            }).transpose())??)
        } else {
            Ok(None)
        }
    }

    pub(crate) async fn entity_type(
        &mut self,
        key: &EntityKey,
    ) -> Result<Option<EntityType>, DbError> {
        let id = key.id().as_slice();
        let rev = key.revision().as_slice();
        Ok(sqlx::query!(
            "SELECT entity_type FROM entity WHERE id = ? AND revision = ?",
            id,
            rev
        )
        .fetch_optional(self.conn())
        .await?
        .map(|r| match r.entity_type.as_str() {
            <FileKind as EntityHandler>::DB_TYPE => Some(EntityType::File),
            <DirectoryKind as EntityHandler>::DB_TYPE => Some(EntityType::Dir),
            _ => None,
        })
        .flatten())
    }
}

pub(crate) struct EntityRow {
    pub id: EntityId,
    pub revision: Revision,
    pub name: OwnedName,
    pub mode: StorageMode,
    pub entity_type: Cow<'static, str>,
}

pub enum EntityRef<'a> {
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
            mode: StorageMode::Local(data),
            entity_type: T::DB_TYPE.into(),
        }
    }
}

impl<T: EntityHandler> From<&SyncedEntity<T>> for EntityRow {
    fn from(entity: &SyncedEntity<T>) -> Self {
        let id = entity.entity_id().clone();
        let revision = entity.revision().clone();
        let name = entity.name().to_owned();

        Self {
            id,
            revision,
            name,
            mode: StorageMode::Synced(entity.object_id()),
            entity_type: T::DB_TYPE.into(),
        }
    }
}

impl<T: EntityHandler> TryFrom<EntityRow> for LocalEntity<T> {
    type Error = EntityError;

    fn try_from(value: EntityRow) -> Result<Self, Self::Error> {
        let data = match value.mode {
            StorageMode::Local(data) => data,
            StorageMode::Synced(_) => {
                return Err(EntityError::ExpectedLocalMode);
            }
        };

        let raw_entity = RawEntityInner::try_from_flatbuffer(data)?;
        if raw_entity.revision() != &value.revision {
            return Err(EntityError::RevisionMismatch {
                expected: value.revision,
                actual: raw_entity.revision,
            });
        };

        Ok(LocalEntity::new(raw_entity, LocalMode))
    }
}

impl PullTask {
    pub(crate) async fn sort_entities(
        objects: &mut Vec<SiaObject>,
        sia_client: &Sia,
    ) -> Result<(), SyncError> {
        const MAX_DEPTH: usize = 1024;

        struct DirInfo {
            children: Vec<EntityKey>,
            depth: Cell<Option<usize>>,
        }

        let mut dirs: HashMap<EntityKey, DirInfo> = HashMap::new();
        let mut key_of: HashMap<SiaObjectId, EntityKey> = HashMap::new();

        for sia_object in objects.iter() {
            let metadata: object::metadata::Metadata = sia_object
                .metadata()
                .try_into()
                .expect("metadata conversion to never fail");

            if metadata.get(METADATA_ENTITY_TYPE)
                != Some(<DirectoryKind as EntityHandler>::METADATA_TYPE)
            {
                continue;
            }

            let dir = SyncedEntity::<DirectoryKind>::load_from_backend(
                0u64.into(),
                sia_object.id(),
                sia_client,
            )
            .await?;

            let key = EntityKey::new(dir.entity_id().clone(), dir.revision().clone());
            key_of.insert(sia_object.id().clone(), key);
            dirs.insert(
                key,
                DirInfo {
                    children: dir.body().entries().to_vec(),
                    depth: Cell::new(None),
                },
            );
        }

        // Depth = max(child depth) + 1; non-dir children contribute 0.
        fn depth(
            key: &EntityKey,
            dirs: &HashMap<EntityKey, DirInfo>,
            budget: usize,
        ) -> Result<usize, SyncError> {
            let info = &dirs[key];
            if let Some(d) = info.depth.get() {
                return Ok(d);
            }
            let Some(budget) = budget.checked_sub(1) else {
                return Err(SyncError::MaxDepthExceeded);
            };

            let mut d = 0;
            for c in info.children.iter().filter(|c| dirs.contains_key(*c)) {
                d = d.max(depth(c, dirs, budget)? + 1);
            }
            info.depth.set(Some(d));
            Ok(d)
        }

        // Precompute depths so we can surface errors (sort_by_key can't fail).
        for key in dirs.keys() {
            depth(key, &dirs, MAX_DEPTH)?;
        }

        // Sort: non-dirs (rank 0) first, then dirs by ascending depth.
        objects.sort_by_key(|o| {
            key_of
                .get(o.id())
                .map_or(0, |k| dirs[k].depth.get().expect("precomputed") + 1)
        });

        Ok(())
    }

    pub(crate) async fn entity_sync<T: EntityHandler, TX: TxScope>(
        tx: &mut Transaction<TX>,
        sia_client: &Sia,
        entity_id: &str,
        rev: &str,
        sia_object: &SiaObject,
        object_id: ObjectId,
    ) -> Result<(), SyncError>
    where
        Transaction<TX>: DbRead + DbWrite,
    {
        let entity_id = EntityId::try_from_str(entity_id).ok_or_else(|| EntityError::InvalidId)?;
        let rev = Revision::try_from_str(rev).ok_or_else(|| EntityError::InvalidRevision)?;

        let entity =
            SyncedEntity::<T>::load_from_backend(object_id, sia_object.id(), sia_client).await?;

        let entity_key = tx
            .register_entity(entity)
            .await
            .map_err(VfsError::DbError)?;

        if entity_key.id() != &entity_id {
            return Err(EntityError::IdMismatch {
                expected: entity_id,
                actual: entity_key.id().clone(),
            })?;
        }

        if entity_key.revision() != &rev {
            return Err(EntityError::RevisionMismatch {
                expected: rev,
                actual: entity_key.revision().clone(),
            })?;
        }

        Ok(())
    }
}

impl PushTask {
    pub(crate) async fn prepare_entity<E: EntityHandler, TX: TxScope>(
        &mut self,
        entity_key: EntityKey,
        tx: &mut Transaction<TX>,
    ) -> Result<Option<LocalEntity<E>>, Error>
    where
        Transaction<TX>: DbWrite,
    {
        tx.mark_sync_job_entity_pending(&entity_key).await?;

        let entity = match tx
            .entity_by_key::<E>(&entity_key)
            .await?
            .ok_or_else(|| DbError::from(DataError::EntityNotFound(entity_key)))?
        {
            Entity::Synced(_) => {
                // already synced
                tx.remove_sync_job_entity(&entity_key).await?;
                return Ok(None);
            }
            Entity::Local(entity) => entity,
        };

        Ok(Some(entity))
    }

    pub(crate) async fn process_entity<E: EntityHandler, TX: TxScope>(
        &mut self,
        entity: LocalEntity<E>,
        object: SiaObject,
        tx: &mut Transaction<TX>,
    ) -> Result<(), Error>
    where
        Transaction<TX>: DbWrite,
    {
        let remote_location = object.id().to_string();
        let object_id = match tx
            .create_or_mark_object(remote_location.as_str(), Timestamp::now())
            .await?
        {
            ObjectCreateResult::New(oid) => oid,
            ObjectCreateResult::Existing(o) => o.id().clone(),
        };
        let entity_key = EntityKey::new(entity.entity_id().clone(), entity.revision().clone());
        let entity = entity.into_synced(object_id);
        tx.register_entity(entity).await?;
        tx.remove_sync_job_entity(&entity_key).await?;
        Ok(())
    }

    pub(crate) async fn queue_entities<TX: TxScope>(
        &mut self,
        tx: &mut Transaction<TX>,
    ) -> Result<usize, Error>
    where
        Transaction<TX>: DbWrite,
    {
        let mut num_items = 0;
        for (key, entity_type) in tx.pushable_entity_keys().await? {
            let fb = if entity_type == EntityType::File {
                let file = match tx.entity_by_key::<FileKind>(&key).await? {
                    Some(Entity::Local(file)) => file,
                    _ => continue,
                };
                file.to_flatbuffer()
            } else if entity_type == EntityType::Dir {
                let dir = match tx.entity_by_key::<DirectoryKind>(&key).await? {
                    Some(Entity::Local(dir)) => dir,
                    _ => continue,
                };
                dir.to_flatbuffer()
            } else {
                continue;
            };

            let estimated_size = fb.len() as u64 + 32;
            let item = JobItem::Entity(key, entity_type);
            if tx.enqueue_sync_job_item(&item, estimated_size).await? {
                num_items += 1;
            }
        }

        Ok(num_items)
    }
}

impl<C: TxScope> Transaction<C>
where
    Self: DbRead,
{
    async fn pushable_entity_keys(&mut self) -> Result<Vec<(EntityKey, EntityType)>, DbError> {
        Ok(sqlx::query!(
            "SELECT id, revision, entity_type FROM entity WHERE mode = 'L' AND ref_count > 0"
        )
        .fetch_all(self.conn())
        .await?
        .into_iter()
        .filter_map(|r| {
            EntityId::try_from_bytes(r.id)
                .map(|id| {
                    Revision::try_from_bytes(r.revision)
                        .map(|rev| (EntityKey::new(id, rev), r.entity_type))
                })
                .flatten()
                .map(|(key, s)| match s.as_str() {
                    <FileKind as EntityHandler>::DB_TYPE => Some((key, EntityType::File)),
                    <DirectoryKind as EntityHandler>::DB_TYPE => Some((key, EntityType::Dir)),
                    _ => None,
                })
                .flatten()
        })
        .collect())
    }

    pub(crate) async fn entity_item(
        &mut self,
        id: i64,
    ) -> Result<(EntityKey, EntityType), DbError> {
        let r = sqlx::query!(
            "SELECT q.entity_id, q.entity_rev, e.entity_type \
                 FROM sync_job_queue q \
                 JOIN entity e ON e.id = q.entity_id AND e.revision = q.entity_rev \
                 WHERE q.id = ?",
            id
        )
        .fetch_one(self.conn())
        .await?;

        let entity_id = r
            .entity_id
            .and_then(EntityId::try_from_bytes)
            .ok_or_else(|| DataError::ConversionError("entity_id missing or invalid".into()))?;
        let revision = r
            .entity_rev
            .and_then(Revision::try_from_bytes)
            .ok_or_else(|| DataError::ConversionError("entity_rev missing or invalid".into()))?;

        let entity_type = match r.entity_type.as_str() {
            <FileKind as EntityHandler>::DB_TYPE => EntityType::File,
            <DirectoryKind as EntityHandler>::DB_TYPE => EntityType::Dir,
            _ => Err(DataError::ConversionError("invalid entity type".into()))?,
        };

        Ok((EntityKey::new(entity_id, revision), entity_type))
    }
}

impl<C: TxScope> Transaction<C>
where
    Self: DbWrite,
{
    pub(crate) async fn register_entity<T: EntityHandler, Mode>(
        &mut self,
        raw_entity: RawEntity<T, Mode>,
    ) -> Result<EntityKey, DbError>
    where
        EntityRow: for<'a> From<&'a RawEntity<T, Mode>>,
    {
        let id = raw_entity.entity_id();
        let rev = raw_entity.revision();
        let id_slice = id.as_slice();
        let rev_slice = rev.as_slice();

        if let Some(mode) = sqlx::query!(
            "SELECT mode FROM entity WHERE id = ? AND revision = ?",
            id_slice,
            rev_slice,
        )
        .map(|r| r.mode)
        .fetch_optional(self.conn())
        .await?
        {
            let row = EntityRow::from(&raw_entity);
            match (mode.as_str(), &row.mode) {
                ("L", StorageMode::Synced(oid)) => {
                    // Existing local, new synced: perform L->S transition
                    let new_object_id = *oid.deref() as i64;
                    sqlx::query!(
                        "UPDATE entity SET mode = 'S', object_id = ?, data = NULL \
                            WHERE id = ? AND revision = ?",
                        new_object_id,
                        id_slice,
                        rev_slice,
                    )
                    .execute(self.conn())
                    .await?;
                }
                _ => {
                    // Entity exists and no transition
                }
            }
            return Ok(EntityKey {
                id: id.clone(),
                revision: rev.clone(),
            });
        }

        // entity does not exist yet, creating from scratch
        let refs = raw_entity.references();
        let row = EntityRow::from(&raw_entity);
        let id = row.id.as_slice();
        let rev = row.revision.as_slice();
        let name = row.name.as_ref();
        let entity_type = row.entity_type.as_ref();
        let (mode, object_id, data) = match &row.mode {
            StorageMode::Local(data) => ("L", None, Some(data.as_ref())),
            StorageMode::Synced(oid) => ("S", Some(*oid.deref() as i64), None),
        };

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
        let entity_id = self.register_entity(draft_entity).await?;
        self.update_inode(inode_id, &name, &entity_id).await?;
        Ok(self
            .inode_by_id(inode_id)
            .await?
            .ok_or_else(|| DbError::DataError(DataError::InodeNotFound(inode_id)))?)
    }

    async fn remove_sync_job_entity(&mut self, entity_key: &EntityKey) -> Result<(), DbError> {
        let entity_id = entity_key.id().as_slice();
        let entity_rev = entity_key.revision().as_slice();
        let affected_rows = sqlx::query!(
            "DELETE FROM sync_job_queue WHERE type = 'E' AND entity_id = ? AND entity_rev = ?",
            entity_id,
            entity_rev
        )
        .execute(self.conn())
        .await?
        .rows_affected();

        if affected_rows != 1 {
            Err(DataError::UnexpectedAffectedRows {
                expected: 1,
                actual: affected_rows,
            })?
        }
        Ok(())
    }

    async fn mark_sync_job_entity_pending(
        &mut self,
        entity_key: &EntityKey,
    ) -> Result<(), DbError> {
        let entity_id = entity_key.id().as_slice();
        let entity_rev = entity_key.revision().as_slice();

        let affected_rows = sqlx::query!(
            "UPDATE sync_job_queue SET pending = 1 WHERE type = 'E' AND entity_id = ? AND entity_rev = ? AND pending = 0",
            entity_id,
            entity_rev
        )
            .execute(self.conn())
            .await?
            .rows_affected();

        if affected_rows != 1 {
            Err(DataError::UnexpectedAffectedRows {
                expected: 1,
                actual: affected_rows,
            })?
        }
        Ok(())
    }
}
