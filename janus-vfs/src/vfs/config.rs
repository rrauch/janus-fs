use crate::db::{DataError, Read as DbRead, Transaction, TxScope, Write as DbWrite};
use crate::gen_flatbuffers::vfs::config::{
    Config as FlatConfig, ConfigBuilder as FlatConfigBuilder, Head as FlatHead,
    HeadBuilder as FlatHeadBuilder, HeadKind as FlatHeadKind,
};
use crate::object;
use crate::object::metadata::{Metadata, MetadataMut};
use crate::object::{ObjectCreateResult, ObjectId};
use crate::sync::push::{EntityType, JobItem, PushTask};
use crate::sync::{Error, PullTask};
use crate::vfs::commit::CommitId;
use crate::vfs::directory::DirectoryKind;
use crate::vfs::entity::{Entity, EntityId, EntityKey, Revision};
use crate::vfs::file::FileKind;
use crate::vfs::{
    BranchName, Head, Name, ROOT_INODE_ID, StorageMode, TagName, Timestamp, Vfs, VfsError, VfsId,
};
use flatbuffers::{FlatBufferBuilder, ForwardsUOffset, InvalidFlatbuffer};
use futures_util::AsyncReadExt;
use futures_util::io::Cursor;
use janus_io::RemoteStorage;
use janus_io::object::{Object as RemoteObject, ObjectId as RemoteObjectId};
use janus_io::upload::UploadableObject;
use std::collections::{HashMap, VecDeque};
use std::num::NonZeroU32;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;
use yoke::{Yoke, Yokeable};

pub(crate) const METADATA_OBJECT_TYPE: &'static str = "CONFIG";

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error(transparent)]
    InvalidFlatbuffer(#[from] InvalidFlatbuffer),
    #[error("invalid timestamp")]
    InvalidTimestamp,
    #[error("invalid chunk size")]
    InvalidChunkSize,
}

#[derive(Debug, Clone)]
pub struct Config {
    inner: Yoke<Inner<'static>, Arc<[u8]>>,
    mode: StorageMode,
}

impl Config {
    pub fn vfs_id(&self) -> &VfsId {
        self.inner.get().vfs_id
    }

    pub fn chunk_size(&self) -> NonZeroU32 {
        self.inner.get().chunk_size
    }

    pub fn description(&self) -> Option<&str> {
        self.inner.get().description
    }

    pub fn last_modified(&self) -> &Timestamp {
        &self.inner.get().last_modified
    }

    pub fn heads(&self) -> &Heads<'_> {
        &self.inner.get().heads
    }

    pub fn is_synced(&self) -> bool {
        match &self.mode {
            StorageMode::Synced(_) => true,
            StorageMode::Local(_) => false,
        }
    }
}

impl Config {
    pub(crate) fn into_synced(mut self, object_id: ObjectId) -> Self {
        self.mode = StorageMode::Synced(object_id);
        self
    }

    pub(crate) fn as_flatbuffer(&self) -> &[u8] {
        self.inner.backing_cart().as_ref()
    }

    pub(crate) async fn load_from_backend(
        object_id: ObjectId,
        remote_oid: &RemoteObjectId,
        remote_storage: &RemoteStorage,
    ) -> Result<Self, std::io::Error> {
        let dl = remote_storage
            .download(remote_oid)
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

        let mut buffer = Vec::with_capacity(dl.object().size() as usize);
        let mut reader = dl.open().await.map_err(std::io::Error::other)?;
        reader.read_to_end(&mut buffer).await?;
        Ok(
            Self::try_from_flatbuffer(Arc::from(buffer), Some(object_id))
                .map_err(std::io::Error::other)?,
        )
    }

    pub(crate) fn try_from_flatbuffer(
        fb: Arc<[u8]>,
        synced_object_id: Option<ObjectId>,
    ) -> Result<Self, ConfigError> {
        let mode = if let Some(oid) = synced_object_id {
            StorageMode::Synced(oid)
        } else {
            StorageMode::Local(fb.clone())
        };

        let inner = Yoke::try_attach_to_cart::<ConfigError, _>(fb, |data| {
            let flat_config = flatbuffers::root::<FlatConfig>(data)?;
            let vfs_id = VfsId::from_byte_ref(&flat_config.vfs_id().0);
            let chunk_size = NonZeroU32::new(flat_config.chunk_size())
                .ok_or_else(|| ConfigError::InvalidChunkSize)?;
            let description = flat_config.description();
            let last_modified = Timestamp::from_millis(flat_config.last_modified())
                .ok_or_else(|| ConfigError::InvalidTimestamp)?;

            let heads = Heads {
                inner: flat_config.heads().unwrap_or_default(),
            };

            Ok(Inner {
                vfs_id,
                chunk_size,
                description,
                last_modified,
                heads,
            })
        })?;
        Ok(Self { inner, mode })
    }

    pub(crate) fn to_uploadable_object(
        &self,
        vfs_id: &VfsId,
    ) -> UploadableObject<Metadata<'_>, Cursor<Vec<u8>>> {
        let metadata = MetadataMut::with_vfs_template(vfs_id, METADATA_OBJECT_TYPE);
        UploadableObject::new(
            format!("/configs/{}.config", self.last_modified().to_millis()),
            Cursor::new(self.inner.backing_cart().as_ref().to_vec()),
            Some(metadata.freeze()),
        )
    }
}

#[derive(Yokeable, Clone, Debug)]
struct Inner<'a> {
    vfs_id: &'a VfsId,
    chunk_size: NonZeroU32,
    description: Option<&'a str>,
    last_modified: Timestamp,
    heads: Heads<'a>,
}

#[derive(Debug, Clone)]
pub struct OwnedEntry {
    pub description: Option<String>,
    pub commit_id: CommitId,
}

#[derive(Debug, Clone, Copy)]
pub struct Entry<'a> {
    description: Option<&'a str>,
    commit_id: &'a CommitId,
}

impl<'a> Entry<'a> {
    fn from_flat(head: FlatHead<'a>) -> Self {
        Self {
            description: head.description(),
            commit_id: CommitId::from_byte_ref(&head.commit_id().0),
        }
    }

    pub fn description(&self) -> Option<&'a str> {
        self.description
    }

    pub fn commit_id(&self) -> &'a CommitId {
        self.commit_id
    }

    pub fn to_owned(&self) -> OwnedEntry {
        OwnedEntry {
            description: self.description.map(|d| d.to_string()),
            commit_id: self.commit_id.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Heads<'a> {
    inner: flatbuffers::Vector<'a, ForwardsUOffset<FlatHead<'a>>>,
}

impl<'a> PartialEq for Heads<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.inner.len() == other.inner.len() && self.inner.iter().eq(other.inner.iter())
    }
}

impl<'a> Heads<'a> {
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn contains_key(&self, head: &Head) -> bool {
        self.get(head).is_some()
    }

    pub fn get(&self, head: &Head) -> Option<Entry<'a>> {
        self.inner
            .lookup_by_key(head.name(), |head, name| head.key_compare_with_value(name))
            .map(Entry::from_flat)
    }

    pub fn iter(&self) -> impl Iterator<Item = (Head, Entry<'a>)> + 'a {
        self.inner.iter().filter_map(|head| {
            let name = head.name();
            match head.kind() {
                FlatHeadKind::Branch => BranchName::from_str(name).ok().map(Head::Branch),
                FlatHeadKind::Tag => TagName::from_str(name).ok().map(Head::Tag),
                _ => None,
            }
            .map(|h| (h, Entry::from_flat(head)))
        })
    }
}

impl From<Config> for ConfigMut {
    fn from(value: Config) -> Self {
        Self {
            vfs_id: value.vfs_id().clone(),
            chunk_size: value.chunk_size(),
            description: value.description().map(|s| s.to_string()),
            last_modified: value.last_modified().clone(),
            heads: value
                .heads()
                .iter()
                .map(|(h, e)| (h, e.to_owned()))
                .collect(),
        }
    }
}

pub(crate) struct ConfigMut {
    vfs_id: VfsId,
    chunk_size: NonZeroU32,
    pub description: Option<String>,
    pub last_modified: Timestamp,
    pub heads: HashMap<Head, OwnedEntry>,
}

impl ConfigMut {
    pub fn new(vfs_id: VfsId, chunk_size: NonZeroU32) -> Self {
        Self {
            vfs_id,
            chunk_size,
            description: None,
            last_modified: Timestamp::now(),
            heads: HashMap::default(),
        }
    }

    pub fn freeze(self) -> Config {
        Config::try_from_flatbuffer(Arc::from(self.to_flatbuffer()), None)
            .expect("deserialization to never fail")
    }

    fn to_flatbuffer(&self) -> Vec<u8> {
        let mut fbb = FlatBufferBuilder::new();

        // entries **have** to be sorted
        let mut pairs: Vec<(&Head, &OwnedEntry)> = self.heads.iter().collect();
        pairs.sort_by(|(a, _), (b, _)| a.name().cmp(b.name()));

        let heads: Vec<_> = pairs
            .into_iter()
            .map(|(h, e)| {
                let kind = if h.is_tag() {
                    FlatHeadKind::Tag
                } else {
                    FlatHeadKind::Branch
                };
                let name = fbb.create_string(h.name());
                let description = e
                    .description
                    .as_ref()
                    .map(|d| fbb.create_string(d.as_str()));

                let mut head_builder = FlatHeadBuilder::new(&mut fbb);
                head_builder.add_name(name);
                head_builder.add_kind(kind);
                if let Some(description) = description {
                    head_builder.add_description(description);
                }
                head_builder.add_commit_id(e.commit_id.as_flatbuffer());
                head_builder.finish()
            })
            .collect();

        let description = self
            .description
            .as_ref()
            .map(|d| fbb.create_string(d.as_str()));

        let heads_vec = fbb.create_vector(&heads);

        let mut config_builder = FlatConfigBuilder::new(&mut fbb);
        config_builder.add_vfs_id(self.vfs_id.as_flatbuffer());
        config_builder.add_chunk_size(self.chunk_size.get());
        config_builder.add_last_modified(self.last_modified.to_millis());
        if let Some(description) = description {
            config_builder.add_description(description);
        }
        config_builder.add_heads(heads_vec);
        let config = config_builder.finish();
        fbb.finish(config, None);
        fbb.finished_data().to_vec()
    }
}

impl PullTask {
    pub(crate) async fn config_sync<TX: TxScope>(
        tx: &mut Transaction<TX>,
        head: &Head,
        vfs_id: &VfsId,
        remote_storage: &RemoteStorage,
        remote_object: &RemoteObject,
        object_id: ObjectId,
    ) -> Result<(), Error>
    where
        Transaction<TX>: crate::db::Read + crate::db::Write,
    {
        let config =
            Config::load_from_backend(object_id, remote_object.id(), remote_storage).await?;
        tx.maybe_set_config(&config, head, vfs_id).await?;
        Ok(())
    }
}

impl PushTask {
    pub(crate) async fn queue_config<TX: TxScope>(
        &mut self,
        tx: &mut Transaction<TX>,
    ) -> Result<usize, Error>
    where
        Transaction<TX>: DbWrite,
    {
        if let Some(data) = tx.pushable_config_data().await? {
            let estimated_size = data.len() as u64 + 32;
            let item = JobItem::Config(data);
            tx.enqueue_sync_job_item(&item, estimated_size).await?;
            Ok(1)
        } else {
            Ok(0)
        }
    }

    pub(crate) async fn prepare_config<TX: TxScope>(
        &mut self,
        config_data: Vec<u8>,
        tx: &mut Transaction<TX>,
    ) -> Result<Option<Config>, Error>
    where
        Transaction<TX>: DbWrite,
    {
        let config = Config::try_from_flatbuffer(Arc::from(config_data), None)?;
        tx.mark_sync_job_config_pending(&config).await?;

        Ok(Some(config))
    }

    pub(crate) async fn process_config<TX: TxScope>(
        &mut self,
        config: &Config,
        object: RemoteObject,
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

        let config = config.clone().into_synced(object_id);
        tx.mark_config_as_synced(&config).await?;
        tx.remove_sync_job_config(&config).await?;
        Ok(())
    }
}

impl<C: TxScope> Transaction<C>
where
    Self: DbRead,
{
    pub(crate) async fn current_config(&mut self) -> Result<Config, crate::db::Error> {
        let r = sqlx::query!("SELECT mode, object_id, data FROM config LIMIT 1")
            .fetch_one(self.conn())
            .await?;

        let mode = match r.mode.as_str() {
            "L" => StorageMode::Local(Arc::from(r.data.unwrap_or_default())),
            "S" => StorageMode::Synced(
                r.object_id
                    .map(ObjectId::from)
                    .ok_or(DataError::MissingObject)?,
            ),
            other => {
                return Err(DataError::ConversionError(
                    format!("invalid mode: {}", other).into(),
                ))?;
            }
        };

        let config = match mode {
            StorageMode::Local(fb) => Config::try_from_flatbuffer(fb.clone(), None)
                .map_err(|e| DataError::ConversionError(e.to_string().into()))?,
            StorageMode::Synced(object_id) => {
                // load object from backend
                let object = self
                    .object_by_id(object_id)
                    .await?
                    .ok_or_else(|| DataError::ObjectNotFound(object_id))?;
                let remote_oid = object.try_to_remote_oid().ok_or_else(|| {
                    DataError::InvalidRemoteLocation(object.remote_location().to_string())
                })?;
                Config::load_from_backend(object_id, &remote_oid, self.remote_storage()).await?
            }
        };

        Ok(config)
    }

    async fn pushable_config_data(&mut self) -> Result<Option<Vec<u8>>, crate::db::Error> {
        Ok(
            sqlx::query!("SELECT data FROM config WHERE mode = 'L' LIMIT 1")
                .map(|r| r.data)
                .fetch_optional(self.conn())
                .await?
                .flatten(),
        )
    }

    pub(crate) async fn config_item(&mut self, id: i64) -> Result<Vec<u8>, crate::db::Error> {
        Ok(
            sqlx::query!("SELECT config FROM sync_job_queue WHERE id = ?", id)
                .fetch_one(self.conn())
                .await?
                .config
                .ok_or_else(|| DataError::ConversionError("config is missing".into()))?,
        )
    }

    async fn current_heads(&mut self) -> Result<Vec<Head>, crate::db::Error> {
        Ok(sqlx::query!("SELECT name, type FROM head")
            .fetch_all(self.conn())
            .await?
            .into_iter()
            .filter_map(|r| {
                let name = r.name.as_str();
                match r.r#type.as_str() {
                    "B" => BranchName::from_str(name).ok().map(Head::from),
                    "T" => TagName::from_str(name).ok().map(Head::from),
                    _ => None,
                }
            })
            .collect())
    }

    pub(crate) async fn chunk_size(&mut self) -> Result<NonZeroU32, crate::db::Error> {
        Ok(sqlx::query!("SELECT chunk_size FROM config")
            .map(|r| NonZeroU32::new(r.chunk_size as u32))
            .fetch_one(self.conn())
            .await?
            .ok_or_else(|| DataError::ConversionError("chunk_size invalid".into()))?)
    }
}

impl<C: TxScope> Transaction<C>
where
    Self: DbWrite,
{
    pub(crate) async fn maybe_update_config(&mut self) -> Result<Option<Config>, crate::db::Error> {
        let prev = self.current_config().await?;
        let mut config_mut = ConfigMut::from(prev.clone());

        let heads = sqlx::query!("SELECT name, type, commit_id FROM head")
            .fetch_all(self.conn())
            .await?
            .into_iter()
            .filter_map(|r| {
                match r.r#type.as_str() {
                    "B" => BranchName::from_str(r.name.as_str()).ok().map(Head::from),
                    "T" => TagName::from_str(r.name.as_str()).ok().map(Head::from),
                    _ => None,
                }
                .map(|head| {
                    CommitId::try_from_bytes(r.commit_id).map(|c| {
                        let entry = OwnedEntry {
                            description: None,
                            commit_id: c,
                        };
                        (head, entry)
                    })
                })
                .flatten()
            })
            .collect::<HashMap<_, _>>();

        config_mut.heads.retain(|k, _| heads.contains_key(k));

        for (head, entry) in heads {
            if let Some(existing) = config_mut.heads.get_mut(&head) {
                existing.commit_id = entry.commit_id;
            } else {
                config_mut.heads.insert(head, entry);
            }
        }

        config_mut.last_modified = Timestamp::now();
        let current = config_mut.freeze();

        if current.heads() == prev.heads() {
            // no change
            return Ok(None);
        }

        let last_modified = current.last_modified().to_millis();
        let (mode, object_id, data) = match &current.mode {
            StorageMode::Synced(oid) => ("S", Some(*oid.deref() as i64), None),
            StorageMode::Local(data) => ("L", None, Some(data.as_ref())),
        };
        sqlx::query!(
            "UPDATE config SET last_modified = ?, mode = ?, object_id = ?, data = ?",
            last_modified,
            mode,
            object_id,
            data,
        )
        .execute(self.conn())
        .await?;

        Ok(Some(current))
    }

    async fn mark_config_as_synced(&mut self, config: &Config) -> Result<(), crate::db::Error> {
        let oid = match &config.mode {
            StorageMode::Synced(oid) => oid,
            _ => return Err(DataError::UnsupportedStorageMode)?,
        };

        let oid = *oid.deref() as i64;
        let data = config.as_flatbuffer();
        sqlx::query!(
            "UPDATE config SET mode = 'S', object_id = ?, data = NULL WHERE mode = 'L' AND data = ?",
            oid,
            data,
        ).execute(self.conn()).await?;
        Ok(())
    }

    async fn mark_sync_job_config_pending(
        &mut self,
        config: &Config,
    ) -> Result<(), crate::db::Error> {
        let bytes = config.as_flatbuffer();
        let affected_rows = sqlx::query!(
            "UPDATE sync_job_queue SET pending = 1 WHERE type = 'F' AND config = ? AND pending = 0",
            bytes
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

    async fn remove_sync_job_config(&mut self, config: &Config) -> Result<(), crate::db::Error> {
        let bytes = config.as_flatbuffer();
        let affected_rows = sqlx::query!(
            "DELETE FROM sync_job_queue WHERE type = 'F' AND config = ?",
            bytes
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

    async fn maybe_set_config(
        &mut self,
        config: &Config,
        head: &Head,
        vfs_id: &VfsId,
    ) -> Result<(), crate::db::Error> {
        if !self.is_empty().await? {
            let current = self.current_config().await?;
            if config.last_modified() <= current.last_modified() || !config.is_synced() {
                // nothing to do
                return Ok(());
            }
        }

        self.set_config(config, head, vfs_id).await
    }

    async fn set_config(
        &mut self,
        config: &Config,
        head: &Head,
        vfs_id: &VfsId,
    ) -> Result<(), crate::db::Error> {
        if !config.heads().contains_key(&head) {
            return Err(DataError::HeadEntryNotFound(head.clone()))?;
        }

        let prev_commit_id = self.current_commit_id(head.clone()).await?;

        for (head, entry) in config.heads().iter() {
            let name = head.name();
            let head_type = match &head {
                Head::Branch(_) => "B",
                Head::Tag(_) => "T",
            };
            let commit_id = entry.commit_id.as_slice();

            sqlx::query!(
                "INSERT INTO head (name, type, commit_id) VALUES (?, ?, ?)
                 ON CONFLICT(name) DO UPDATE SET
                     type = excluded.type,
                     commit_id = excluded.commit_id
                ",
                name,
                head_type,
                commit_id
            )
            .execute(self.conn())
            .await?;
        }

        let last_modified = config.last_modified().to_millis();
        let (mode, object_id, data) = match &config.mode {
            StorageMode::Synced(oid) => ("S", Some(*oid.deref() as i64), None),
            StorageMode::Local(data) => ("L", None, Some(data.as_ref())),
        };

        let head_name = head.name();
        let vfs_id = vfs_id.as_slice();
        let chunk_size = config.chunk_size().get();

        if self.is_empty().await? {
            // initial config
            sqlx::query!(
                "INSERT INTO config (vfs_id, head, last_modified, chunk_size, mode, object_id, data) VALUES (?, ?, ?, ?, ?, ?, ?)",
                vfs_id,
                head_name,
                last_modified,
                chunk_size,
                mode,
                object_id,
                data,
            ).execute(self.conn()).await?;
        } else {
            // update
            let rows_affected = sqlx::query!(
                "UPDATE config SET head = ?, last_modified = ?, mode = ?, object_id = ?, data = ? WHERE vfs_id = ?",
                head_name,
                last_modified,
                mode,
                object_id,
                data,
                vfs_id
            ).execute(self.conn()).await?.rows_affected();
            if rows_affected != 1 {
                return Err(DataError::UnexpectedAffectedRows {
                    expected: 1,
                    actual: rows_affected,
                })?;
            }
        }

        let mut obsolete_heads = self.current_heads().await?;
        obsolete_heads.retain(|h| !config.heads().contains_key(h));
        for head in obsolete_heads {
            let name = head.name();
            let head_type = match &head {
                Head::Branch(_) => "B",
                Head::Tag(_) => "T",
            };
            sqlx::query!(
                "DELETE FROM head WHERE name = ? AND type = ?",
                name,
                head_type,
            )
            .execute(self.conn())
            .await?;
        }

        let current_commit_id = self
            .current_commit_id(head.clone())
            .await?
            .ok_or_else(|| DataError::HeadEntryNotFound(head.clone()))?;

        if Some(current_commit_id) != prev_commit_id {
            // commit changed, full vfs update needed
            let commit = self
                .commit_by_id(&current_commit_id)
                .await?
                .ok_or_else(|| DataError::CommitNotFound(current_commit_id))?;
            let root_inode_id = *ROOT_INODE_ID as i64;
            // If the current root already matches the new commit's root, we're done.
            let current_root = sqlx::query!(
                "SELECT entity_id, entity_rev FROM vfs WHERE inode_id = ?",
                root_inode_id,
            )
            .fetch_optional(self.conn())
            .await?
            .map(|r| -> Result<EntityKey, crate::db::Error> {
                Ok(EntityKey::new(
                    EntityId::try_from_bytes(r.entity_id)
                        .ok_or_else(|| DataError::InvalidEntityId)?,
                    Revision::try_from_bytes(r.entity_rev)
                        .ok_or_else(|| DataError::InvalidRevision)?,
                ))
            })
            .transpose()?;

            let unchanged = current_root.as_ref().is_some_and(|r| {
                r.id() == commit.entity().id() && r.revision() == commit.entity().revision()
            });

            if !unchanged {
                // Tear down the entire VFS and rebuild from scratch.
                sqlx::query!("DELETE FROM vfs").execute(self.conn()).await?;

                let root = self
                    .entity_by_key::<DirectoryKind>(commit.entity())
                    .await?
                    .ok_or_else(|| DataError::EntityNotFound(commit.entity().clone()))?;

                let entity_id = root.entity_id().as_slice();
                let entity_rev = root.revision().as_slice();
                let name = root.name().as_ref();
                sqlx::query!(
                    "INSERT INTO vfs (inode_id, inode_type, entity_id, entity_rev, name, path) VALUES (?, 'D', ?, ?, ?, '/')",
                    root_inode_id,
                    entity_id,
                    entity_rev,
                    name,
                ).execute(self.conn()).await?;

                // Rebuild the tree breadth-first from the root's children.
                let mut queue = VecDeque::new();
                queue.push_back((ROOT_INODE_ID, FileOrDir::Dir(root)));

                while let Some((parent, entity)) = queue.pop_front() {
                    for key in entity.children() {
                        let child = match self
                            .entity_type(key)
                            .await?
                            .ok_or_else(|| DataError::EntityNotFound(key.clone()))?
                        {
                            EntityType::File => FileOrDir::File(
                                self.entity_by_key(&key)
                                    .await?
                                    .ok_or_else(|| DataError::EntityNotFound(key.clone()))?,
                            ),
                            EntityType::Dir => FileOrDir::Dir(
                                self.entity_by_key(&key)
                                    .await?
                                    .ok_or_else(|| DataError::EntityNotFound(key.clone()))?,
                            ),
                        };
                        let key =
                            EntityKey::new(child.entity_id().clone(), child.revision().clone());
                        let child_inode = match &child {
                            FileOrDir::File(_) => {
                                self.create_inode::<FileKind>(child.name(), parent, key)
                                    .await?
                            }
                            FileOrDir::Dir(_) => {
                                self.create_inode::<DirectoryKind>(child.name(), parent, key)
                                    .await?
                            }
                        };

                        queue.push_back((child_inode, child));
                    }
                }
            }
        }

        Ok(())
    }
}

enum FileOrDir {
    File(Entity<FileKind>),
    Dir(Entity<DirectoryKind>),
}

impl FileOrDir {
    #[inline]
    fn name(&self) -> &Name {
        match self {
            Self::File(file) => file.name(),
            Self::Dir(dir) => dir.name(),
        }
    }

    #[inline]
    fn entity_id(&self) -> &EntityId {
        match self {
            Self::File(file) => file.entity_id(),
            Self::Dir(dir) => dir.entity_id(),
        }
    }

    #[inline]
    fn revision(&self) -> &Revision {
        match self {
            Self::File(file) => file.revision(),
            Self::Dir(dir) => dir.revision(),
        }
    }

    #[inline]
    fn children(&self) -> &[EntityKey] {
        match self {
            Self::File(_) => &[],
            Self::Dir(dir) => dir.body().entries(),
        }
    }
}

impl Vfs {
    pub async fn create_branch(
        vfs_id: &VfsId,
        remote_storage: &RemoteStorage,
        branch_name: BranchName,
        description: Option<String>,
        commit_id: CommitId,
    ) -> Result<Config, VfsError> {
        Self::create_head(
            vfs_id,
            remote_storage,
            branch_name.into(),
            description,
            commit_id,
        )
        .await
    }

    pub async fn create_tag(
        vfs_id: &VfsId,
        remote_storage: &RemoteStorage,
        tag_name: TagName,
        description: Option<String>,
        commit_id: CommitId,
    ) -> Result<Config, VfsError> {
        Self::create_head(
            vfs_id,
            remote_storage,
            tag_name.into(),
            description,
            commit_id,
        )
        .await
    }

    async fn create_head(
        vfs_id: &VfsId,
        remote_storage: &RemoteStorage,
        head: Head,
        description: Option<String>,
        commit_id: CommitId,
    ) -> Result<Config, VfsError> {
        let mut config: ConfigMut = Vfs::scan(remote_storage)
            .await?
            .into_iter()
            .find(|c| c.vfs_id() == vfs_id)
            .ok_or_else(|| VfsError::Other("vfs not found".to_string()))?
            .into();
        if config.heads.contains_key(&head) {
            return Err(VfsError::Other(
                "head already exists, cannot create".to_string(),
            ));
        }

        if config
            .heads
            .values()
            .map(|e| &e.commit_id)
            .find(|c| &&commit_id == c)
            .is_none()
        {
            return Err(VfsError::Other("commit not found".to_string()));
        }

        config.heads.insert(
            head,
            OwnedEntry {
                description,
                commit_id,
            },
        );
        config.last_modified = Timestamp::now();

        let config = config.freeze();

        remote_storage
            .upload(config.to_uploadable_object(vfs_id))
            .await
            .map_err(std::io::Error::other)?;

        Ok(config)
    }

    pub async fn delete_branch(
        vfs_id: &VfsId,
        remote_storage: &RemoteStorage,
        branch_name: BranchName,
    ) -> Result<Config, VfsError> {
        let head = Head::from(branch_name);
        Self::delete_head(vfs_id, remote_storage, &head).await
    }

    pub async fn delete_tag(
        vfs_id: &VfsId,
        remote_storage: &RemoteStorage,
        tag_name: TagName,
    ) -> Result<Config, VfsError> {
        let head = Head::from(tag_name);
        Self::delete_head(vfs_id, remote_storage, &head).await
    }

    async fn delete_head(
        vfs_id: &VfsId,
        remote_storage: &RemoteStorage,
        head: &Head,
    ) -> Result<Config, VfsError> {
        let mut config: ConfigMut = Vfs::scan(remote_storage)
            .await?
            .into_iter()
            .find(|c| c.vfs_id() == vfs_id)
            .ok_or_else(|| VfsError::Other("vfs not found".to_string()))?
            .into();
        if config.heads.remove(&head).is_none() {
            return Err(VfsError::Other("head not found in vfs config".to_string()));
        };
        config.last_modified = Timestamp::now();
        let config = config.freeze();

        remote_storage
            .upload(config.to_uploadable_object(vfs_id))
            .await
            .map_err(std::io::Error::other)?;

        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use crate::vfs::commit::CommitId;
    use crate::vfs::config::{ConfigMut, OwnedEntry};
    use crate::vfs::{BranchName, DEFAULT_CHUNK_SIZE, Head, TagName, Timestamp, Vfs, VfsId};
    use janus_io::RemoteStorage;
    use std::str::FromStr;

    #[test]
    fn flatbuffer_roundtrip() -> anyhow::Result<()> {
        let vfs_id = VfsId::generate();
        let last_modified = Timestamp::now();
        let head1: Head = BranchName::from_str("main")?.into();
        let description1 = "descr1".to_string();
        let commit_id1 = CommitId::zeroed();
        let head2: Head = TagName::from_str("foo")?.into();
        let description2 = "descr2".to_string();
        let commit_id2 = CommitId::zeroed();

        let mut config_mut = ConfigMut::new(vfs_id.clone(), DEFAULT_CHUNK_SIZE);
        config_mut.last_modified = last_modified;
        config_mut.description = Some("test config".to_string());
        config_mut.heads.insert(
            head1.clone(),
            OwnedEntry {
                description: Some(description1.clone()),
                commit_id: commit_id1.clone(),
            },
        );
        config_mut.heads.insert(
            head2.clone(),
            OwnedEntry {
                description: Some(description2.clone()),
                commit_id: commit_id2.clone(),
            },
        );

        let config = config_mut.freeze();
        assert_eq!(config.vfs_id(), &vfs_id);
        assert_eq!(config.description(), Some("test config"));
        assert_eq!(config.last_modified(), &last_modified);
        assert_eq!(config.heads().len(), 2);
        assert!(!config.is_synced());
        let entry1 = config.heads().get(&head1).unwrap();
        assert_eq!(entry1.description(), Some(description1.as_str()));
        assert_eq!(entry1.commit_id(), &commit_id1);
        let entry2 = config.heads().get(&head2).unwrap();
        assert_eq!(entry2.description(), Some(description2.as_str()));
        assert_eq!(entry2.commit_id(), &commit_id2);
        assert_eq!(
            config
                .heads()
                .iter()
                .map(|(_, e)| e.commit_id())
                .collect::<Vec<_>>(),
            vec![&commit_id2, &commit_id1] //sort order check
        );

        Ok(())
    }

    #[tokio::test]
    async fn create_delete_branch() -> anyhow::Result<()> {
        create_delete_head(BranchName::from_str("branch1")?.into()).await
    }

    #[tokio::test]
    async fn create_delete_tag() -> anyhow::Result<()> {
        create_delete_head(TagName::from_str("tag1")?.into()).await
    }

    async fn create_delete_head(head: Head) -> anyhow::Result<()> {
        let remote_storage = RemoteStorage::mock().await;
        let vfs_id = Vfs::create_new(None, None, &remote_storage).await?;
        let config = Vfs::scan(&remote_storage)
            .await?
            .into_iter()
            .find(|c| c.vfs_id() == &vfs_id)
            .unwrap();

        let main = BranchName::default().into();
        let commit = config
            .heads()
            .get(&main)
            .map(|e| e.commit_id.clone())
            .unwrap();

        let config =
            Vfs::create_head(&vfs_id, &remote_storage, head.clone(), None, commit.clone()).await?;
        assert_eq!(config.heads().get(&head).unwrap().commit_id(), &commit);

        let config = Vfs::delete_head(&vfs_id, &remote_storage, &head).await?;
        assert!(!config.heads().contains_key(&head));

        Ok(())
    }
}
