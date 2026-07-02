use crate::scheduler::queue::ctrl::{Active, Idle, QueueCtrl, Waiting};
use anyhow::Result;
use async_trait::async_trait;
use std::fmt::Debug;
use std::future::Future;
use std::hash::Hash;
use std::ops::Deref;
use std::time::{Duration, SystemTime};

pub(crate) trait ResourceManager: Debug + Send + Sync + Sized {
    type Resource: Resource;
    type PreparationKey: Hash + Eq + Clone + Send + 'static + Sync + Debug;
    type AccessKey: Hash + Eq + Clone + Send + 'static + Sync + Debug;
    type ResourceData: Send + Sync + 'static;
    type ResourceFuture: Future<Output = Result<Self::Resource>> + Send;

    fn prepare(
        &self,
        preparation_key: &Self::PreparationKey,
    ) -> impl Future<Output = Result<(Self::AccessKey, Self::ResourceData, Vec<Self::Resource>)>> + Send;

    fn process(
        &self,
        queue: &mut QueueCtrl<Self>,
        data: &mut Self::ResourceData,
        ctx: &Context,
    ) -> Result<Action>;
}

#[async_trait]
pub(crate) trait Resource: Debug + Send {
    fn offset(&self) -> u64;
    fn can_reuse(&self) -> bool;
    async fn finalize(self) -> Result<()>;
}

#[async_trait]
impl<T: Resource> Resource for Box<T> {
    #[inline]
    fn offset(&self) -> u64 {
        self.deref().offset()
    }

    #[inline]
    fn can_reuse(&self) -> bool {
        self.deref().can_reuse()
    }

    #[inline]
    async fn finalize(self) -> Result<()> {
        let this = *self;
        this.finalize().await
    }
}

pub(crate) struct Context {
    pub last_activity: SystemTime,
    pub previous_call: Option<SystemTime>,
    pub iteration: usize,
}
pub(crate) enum Action {
    Again,
    Sleep(Duration),
}

#[derive(Clone)]
pub enum Entry {
    Waiting(Waiting),
    Idle(Idle),
    Active(Active),
}

impl Entry {
    pub fn offset(&self) -> u64 {
        match self {
            Entry::Waiting(w) => w.offset,
            Entry::Idle(i) => i.offset,
            Entry::Active(a) => a.offset,
        }
    }

    pub fn as_waiting(&self) -> Option<&Waiting> {
        if let Entry::Waiting(e) = &self {
            Some(e)
        } else {
            None
        }
    }

    pub fn as_idle(&self) -> Option<&Idle> {
        if let Entry::Idle(e) = &self {
            Some(e)
        } else {
            None
        }
    }

    pub fn as_active(&self) -> Option<&Active> {
        if let Entry::Active(e) = &self {
            Some(e)
        } else {
            None
        }
    }
}

impl From<Waiting> for Entry {
    fn from(value: Waiting) -> Self {
        Entry::Waiting(value)
    }
}

impl From<Idle> for Entry {
    fn from(value: Idle) -> Self {
        Entry::Idle(value)
    }
}

impl From<Active> for Entry {
    fn from(value: Active) -> Self {
        Entry::Active(value)
    }
}
