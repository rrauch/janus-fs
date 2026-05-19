use crate::ContentId;
use crate::vfs::{Name, Revision};
use chrono::{DateTime, Utc};
use derive_where::derive_where;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;
use uuid::Uuid;

#[derive_where(Debug, Clone; I)]
pub enum Entity<T, I> {
    Synced(SyncedEntity<T, I>),
    Local(LocalEntity<T, I>),
}

impl<T, I> Entity<T, I> {
    #[inline]
    pub fn entity_id(&self) -> EntityId {
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

    pub fn to_key(&self) -> EntityKey {
        EntityKey {
            entity_id: self.entity_id(),
            revision: self.revision().clone(),
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EntityKey {
    pub(super) entity_id: EntityId,
    pub(super) revision: Revision,
}

pub struct SyncedMode;
pub struct LocalMode;
pub struct DraftMode;

pub type SyncedEntity<T, I> = RawEntity<T, I, SyncedMode>;
pub type LocalEntity<T, I> = RawEntity<T, I, LocalMode>;
pub type DraftEntity<T, I> = RawEntity<T, I, DraftMode>;

#[derive_where(Debug, Clone; I)]
pub struct RawEntity<T, I, Mode>(Arc<RawEntityInner<T, I, Mode>>);

#[derive_where(Debug, Clone; I)]
pub(crate) struct RawEntityInner<T, I, Mode> {
    entity_id: EntityId,
    revision: Revision,
    name: Name,
    created: DateTime<Utc>,
    last_modified: DateTime<Utc>,
    //extended_attributes: HashMap<String, Bytes>,
    pub(super) inner: I,
    _phantom: PhantomData<(T, Mode)>,
}

impl<T, I, Mode> RawEntityInner<T, I, Mode> {
    pub(crate) fn hash_metadata(&self, hasher: &mut blake3::Hasher) {
        hasher.update(b"begin_metadata:\nentity_id:");
        hasher.update(self.entity_id.as_bytes());
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
            entity_id: self.entity_id,
            revision: self.revision,
            name: self.name,
            created: self.created,
            last_modified: self.last_modified,
            inner: self.inner,
            _phantom: PhantomData,
        }
    }
}

impl<T, I, Mode> RawEntity<T, I, Mode> {
    fn entity_id(&self) -> EntityId {
        self.0.entity_id
    }

    fn revision(&self) -> &Revision {
        &self.0.revision
    }

    fn name(&self) -> &Name {
        &self.0.name
    }

    fn created(&self) -> &DateTime<Utc> {
        &self.0.created
    }

    fn last_modified(&self) -> &DateTime<Utc> {
        &self.0.last_modified
    }

    fn inner(&self) -> &I {
        &self.0.inner
    }
}

impl<T, I: Clone, Mode> RawEntity<T, I, Mode> {
    pub fn into_mut(self) -> EntityMut<T, I> {
        EntityMut(Arc::unwrap_or_clone(self.0).into_draft())
    }
}

#[derive_where(Debug; I)]
pub struct EntityMut<T, I>(RawEntityInner<T, I, DraftMode>);

impl<T, I> EntityMut<T, I> {
    pub fn entity_id(&self) -> EntityId {
        self.0.entity_id
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

impl<T: RevisionHasher<I, DraftMode> + Normalizer<I>, I> Freezable<T, I> for EntityMut<T, I> {
    fn freeze(mut self) -> DraftEntity<T, I> {
        T::normalize(&mut self.0.inner);
        self.0.revision = ContentId::new_internal(T::hash(&self.0));
        RawEntity(Arc::new(self.0))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct EntityId(Uuid);

impl Deref for EntityId {
    type Target = Uuid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub trait RevisionHasher<I, Mode>: Sized {
    fn hash(inner: &RawEntityInner<Self, I, Mode>) -> blake3::Hash;
}

pub trait Normalizer<I> {
    fn normalize(value: &mut I);
}
