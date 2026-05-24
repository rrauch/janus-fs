use crate::blob::BlobId;
use crate::db::{
    DataError, Error as DbError, Read as DbRead, Transaction, TxScope, Write as DbWrite,
};
use crate::vfs::{AsDbType, Name};
use crate::{ContentId, TypedUuid};
use chrono::{DateTime, Utc};
use derive_where::derive_where;
use std::borrow::Cow;
use std::marker::PhantomData;
use std::str::FromStr;
use std::sync::Arc;

#[derive_where(Debug, Clone; I)]
pub enum Entity<T, I> {
    Synced(SyncedEntity<T, I>),
    Local(LocalEntity<T, I>),
}

impl<T, I> Entity<T, I> {
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
    pub fn created(&self) -> &DateTime<Utc> {
        match self {
            Self::Synced(e) => e.created(),
            Self::Local(e) => e.created(),
        }
    }

    #[inline]
    pub fn last_modified(&self) -> &DateTime<Utc> {
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
    pub fn remote_location(&self) -> Option<&String> {
        match self {
            Self::Synced(e) => Some(e.remote_location()),
            Self::Local(_) => None,
        }
    }

    #[inline]
    pub(crate) fn inner(&self) -> &I {
        match self {
            Self::Synced(e) => e.inner(),
            Self::Local(e) => e.inner(),
        }
    }

    #[inline]
    pub(crate) fn into_mut(self) -> EntityMut<T, I>
    where
        I: Clone,
    {
        match self {
            Self::Synced(e) => e.into_mut(),
            Self::Local(e) => e.into_mut(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SyncedMode {
    remote_location: String,
}
#[derive(Debug, Clone)]
pub struct LocalMode;
#[derive(Debug, Clone)]
pub struct DraftMode;

pub type SyncedEntity<T, I> = RawEntity<T, I, SyncedMode>;
pub type LocalEntity<T, I> = RawEntity<T, I, LocalMode>;
pub type DraftEntity<T, I> = RawEntity<T, I, DraftMode>;

#[derive_where(Debug, Clone; I, Mode)]
pub struct RawEntity<T, I, Mode>(Arc<(Revision, RawEntityInner<T, I, Mode>)>);

impl<T, I, Mode> RawEntity<T, I, Mode> {
    pub(super) fn new(
        entity_id: EntityId,
        revision: Revision,
        name: Name,
        created: DateTime<Utc>,
        last_modified: DateTime<Utc>,
        inner: I,
        mode: Mode,
    ) -> Self {
        let inner = RawEntityInner {
            id: entity_id,
            name,
            created,
            last_modified,
            inner,
            mode,
            _phantom: PhantomData,
        };

        Self(Arc::new((revision, inner)))
    }

    pub(super) fn into_inner(self) -> RawEntityInner<T, I, Mode>
    where
        I: Clone,
        Mode: Clone,
    {
        let (_, inner) = Arc::unwrap_or_clone(self.0);
        inner
    }
}

#[derive_where(Debug, Clone; I, Mode)]
pub(crate) struct RawEntityInner<T, I, Mode> {
    id: EntityId,
    name: Name,
    created: DateTime<Utc>,
    last_modified: DateTime<Utc>,
    //extended_attributes: HashMap<String, Bytes>,
    pub(super) inner: I,
    mode: Mode,
    _phantom: PhantomData<T>,
}

impl<T, I, Mode> RawEntityInner<T, I, Mode> {
    pub(crate) fn hash_metadata(&self, hasher: &mut blake3::Hasher) {
        hasher.update(b"begin_metadata:\nid:");
        hasher.update(self.id.as_slice());
        hasher.update(b"\nname:");
        hasher.update(self.name.as_bytes());
        hasher.update(b"\ncreated:");
        hasher.update(&self.created.timestamp().to_be_bytes());
        hasher.update(b"\nlast_modified:");
        hasher.update(&self.last_modified.timestamp().to_be_bytes());
        hasher.update(b"\nend_metadata");
    }
}

impl<T, I, Mode> RawEntityInner<T, I, Mode> {
    fn into_draft(self) -> RawEntityInner<T, I, DraftMode> {
        RawEntityInner {
            id: self.id,
            name: self.name,
            created: self.created,
            last_modified: self.last_modified,
            inner: self.inner,
            mode: DraftMode,
            _phantom: PhantomData,
        }
    }
}

impl<T, I, Mode> RawEntity<T, I, Mode> {
    pub fn entity_id(&self) -> &EntityId {
        &self.0.1.id
    }

    pub fn revision(&self) -> &Revision {
        &self.0.0
    }

    pub fn name(&self) -> &Name {
        &self.0.1.name
    }

    pub fn created(&self) -> &DateTime<Utc> {
        &self.0.1.created
    }

    pub fn last_modified(&self) -> &DateTime<Utc> {
        &self.0.1.last_modified
    }

    pub fn inner(&self) -> &I {
        &self.0.1.inner
    }
}

impl<T, I> RawEntity<T, I, SyncedMode> {
    pub fn remote_location(&self) -> &String {
        &self.0.1.mode.remote_location
    }
}

impl<T, I: Clone, Mode: Clone> RawEntity<T, I, Mode> {
    pub fn into_mut(self) -> EntityMut<T, I> {
        EntityMut(self.into_inner().into_draft())
    }
}

#[derive_where(Debug; I)]
pub struct EntityMut<T, I>(RawEntityInner<T, I, DraftMode>);

impl<T, I> EntityMut<T, I> {
    pub fn id(&self) -> &EntityId {
        &self.0.id
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

    pub(crate) fn inner(&self) -> &I {
        &self.0.inner
    }

    pub(crate) fn inner_mut(&mut self) -> &mut I {
        &mut self.0.inner
    }

    pub(crate) fn set_inner(&mut self, new: I) {
        self.0.inner = new;
    }
}

pub trait Freezable<T, I> {
    fn freeze(self) -> DraftEntity<T, I>;
}

impl<T: EntityHasher<I, DraftMode> + Normalizer<I>, I> Freezable<T, I> for EntityMut<T, I> {
    fn freeze(mut self) -> DraftEntity<T, I> {
        T::normalize(&mut self.0.inner);
        let revision = ContentId::new_internal(T::hash(&self.0));
        RawEntity(Arc::new((revision, self.0)))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EntityKey {
    id: EntityId,
    revision: Revision,
}

impl EntityKey {
    pub(super) fn new(id: EntityId, revision: Revision) -> Self {
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

pub(crate) trait EntityHasher<I, Mode>: Sized {
    fn hash(inner: &RawEntityInner<Self, I, Mode>) -> blake3::Hash;
}

pub trait Normalizer<I> {
    fn normalize(value: &mut I);
}

impl<C: TxScope> Transaction<C>
where
    Self: DbRead,
{
    pub(super) async fn entity_by_key<T, I>(
        &mut self,
        key: &EntityKey,
    ) -> Result<Option<Entity<T, I>>, DbError>
    where
        Entity<T, I>: TryFrom<EntityRow>,
    {
        let id_ref = key.id.as_slice();
        let rev_ref = key.revision.as_slice();

        Ok(sqlx::query!(
            "SELECT name, created as \"created: i64\", last_modified as \"last_modified: i64\", mode, entity_type, remote_location, data FROM entity WHERE id = ? and revision = ?",
            id_ref,
            rev_ref,
        )
            .fetch_optional(self.conn())
            .await?
            .map(|r| -> Result<EntityRow, DbError> {
                Ok(EntityRow {
                    id: key.id.clone(),
                    revision: key.revision.clone(),
                    name: Name::from_str(r.name.as_str()).map_err(|e| DataError::ConversionError(e.to_string().into()))?,
                    created: DateTime::<Utc>::from_timestamp(r.created, 0).ok_or_else(|| DataError::ConversionError("invalid created timestamp".into()))?,
                    last_modified: DateTime::<Utc>::from_timestamp(r.last_modified, 0).ok_or_else(|| DataError::ConversionError("invalid created timestamp".into()))?,
                    mode: match r.mode.as_str() {
                        "L" => Mode::Local,
                        "S" => Mode::Synced,
                        other => return Err(DataError::ConversionError(format!("invalid mode: {}", other).into()))?,
                    },
                    entity_type: r.entity_type.into(),
                    remote_location: r.remote_location,
                    data: r.data,
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
    pub name: Name,
    pub created: DateTime<Utc>,
    pub last_modified: DateTime<Utc>,
    pub mode: Mode,
    pub entity_type: Cow<'static, str>,
    pub remote_location: Option<String>,
    pub data: Option<Vec<u8>>,
}

pub(crate) enum EntityRef {
    Blob(BlobId),
    Entity(EntityKey),
}

pub(super) enum Mode {
    Synced,
    Local,
}

impl<T: AsDbType, I: Clone> From<(DraftEntity<T, I>, Option<Vec<u8>>)> for EntityRow {
    fn from((entity, data): (DraftEntity<T, I>, Option<Vec<u8>>)) -> Self {
        let id = entity.entity_id().clone();
        let revision = entity.revision().clone();
        let value = entity.into_inner();

        Self {
            id,
            revision,
            name: value.name,
            created: value.created,
            last_modified: value.last_modified,
            mode: Mode::Local,
            entity_type: T::db_type().into(),
            remote_location: None,
            data,
        }
    }
}

impl<T, I> TryFrom<(EntityRow, I)> for Entity<T, I> {
    type Error = String;

    fn try_from((value, inner): (EntityRow, I)) -> Result<Self, Self::Error> {
        Ok(match value.mode {
            Mode::Synced => {
                let remote_location = value
                    .remote_location
                    .ok_or_else(|| "remote_location is missing".to_string())?;
                Entity::Synced(SyncedEntity::new(
                    value.id,
                    value.revision,
                    value.name,
                    value.created,
                    value.last_modified,
                    inner,
                    SyncedMode { remote_location },
                ))
            }
            Mode::Local => Entity::Local(LocalEntity::new(
                value.id,
                value.revision,
                value.name,
                value.created,
                value.last_modified,
                inner,
                LocalMode,
            )),
        })
    }
}

impl<C: TxScope> Transaction<C>
where
    Self: DbWrite,
{
    pub(crate) async fn create_entity_if_not_exist<T, I>(
        &mut self,
        draft_entity: DraftEntity<T, I>,
    ) -> Result<EntityKey, DbError>
    where
        (EntityRow, Vec<EntityRef>): From<DraftEntity<T, I>>,
    {
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

        let (row, refs): (EntityRow, Vec<EntityRef>) = draft_entity.into();
        let id = row.id.as_slice();
        let rev = row.revision.as_slice();
        let name = row.name.as_str();
        let created = row.created.timestamp();
        let last_modified = row.last_modified.timestamp();
        let entity_type = row.entity_type.as_ref();
        let mode = match row.mode {
            Mode::Local => "L",
            Mode::Synced => "S",
        };
        let remote_location = row.remote_location.as_ref().map(|l| l.as_str());
        let data = row.data.as_ref().map(|d| d.as_slice());

        sqlx::query!(
            "INSERT INTO entity (id, revision, name, created, last_modified, entity_type, mode, remote_location, data)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            id,
            rev,
            name,
            created,
            last_modified,
            entity_type,
            mode,
            remote_location,
            data,
        ).execute(self.conn()).await?;

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
}
