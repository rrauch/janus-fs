use crate::Backend;
use crate::cache::Cache;
use crate::chunk::{Chunk, ChunkId};
use crate::object::{BackendDO, Download, ObjectId, Version};
use crate::scheduler::Scheduler;
use crate::scheduler::queue::ctrl::QueueCtrl;
use crate::scheduler::resource_manager::Action::{Again, Sleep};
use crate::scheduler::resource_manager::{Action, Context, Entry, Resource, ResourceManager};
use anyhow::bail;
use bon::bon;
use bytes::BytesMut;
use futures_util::AsyncReadExt;
use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use itertools::Itertools;
use std::cmp::min;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::Semaphore;
use tokio::time::timeout;

#[derive(Debug)]
pub struct ChunkDownloader {
    backend: Backend,
    cache: Cache,
    max_skip_ahead: u64,
    chunk_size: usize,
    download_limiter: Arc<Semaphore>,
    max_active: usize,
    idle_min_wait: Duration,
    idle_timeout_min: Duration,
    wait_min_advance: Duration,
    wait_min_new: Duration,
}

#[bon]
impl ChunkDownloader {
    #[builder]
    pub fn new(
        backend: Backend,
        cache: Cache,
        chunk_size: usize,
        max_skip_ahead: NonZeroU64,
        max_concurrent_downloads: NonZeroUsize,
        #[builder(default = Duration::from_millis(2000))] max_queue_idle: Duration,
        #[builder(default = Duration::from_millis(50))] idle_min_wait: Duration,
        #[builder(default = Duration::from_millis(150))] idle_timeout_min: Duration,
        #[builder(default = Duration::from_millis(1500))] idle_timeout_max: Duration,
        #[builder(default = Duration::from_millis(10))] wait_min_advance: Duration,
        #[builder(default = Duration::from_millis(200))] wait_min_new: Duration,
    ) -> Scheduler<Self> {
        let max_active = max_concurrent_downloads.get();
        let download_limiter = Arc::new(Semaphore::new(max_active));

        let max_skip_ahead = {
            // round up to nearest multiple of chunk size
            let chunk_size = chunk_size as u64;
            let max_skip_ahead = max_skip_ahead.get();
            if max_skip_ahead % chunk_size == 0 {
                max_skip_ahead
            } else {
                ((max_skip_ahead / chunk_size) + 1) * chunk_size
            }
        };

        let downloader = Self {
            backend,
            cache,
            max_skip_ahead,
            chunk_size,
            download_limiter,
            max_active,
            idle_min_wait,
            idle_timeout_min,
            wait_min_advance,
            wait_min_new,
        };

        Scheduler::new(downloader, true, max_queue_idle, idle_timeout_max, 2)
    }
}

impl ChunkDownloader {
    async fn advance(
        mut reader: Download,
        dst_offset: u64,
        cache: Cache,
        dl: &BackendDO,
        max_skip_ahead: u64,
        chunk_size: usize,
    ) -> anyhow::Result<Download> {
        let len = reader.len();
        if dst_offset >= len {
            bail!("advance beyond reader length");
        }

        let mut offset = reader.offset();
        if offset >= dst_offset {
            bail!("dst_offset needs to be ahead of current reader.offset");
        }

        let n = dst_offset.saturating_sub(offset);
        if n > max_skip_ahead {
            bail!("advance beyond MAX_SKIP_AHEAD");
        }

        tracing::debug!(
            begin_offset = offset,
            end_offset = dst_offset,
            num_bytes = n,
            "advancing reader"
        );

        let chunk_size = chunk_size as u64;

        let next_chunk_in = if offset % chunk_size == 0 {
            0
        } else {
            chunk_size - (offset % chunk_size)
        };

        if next_chunk_in > 0 {
            // only partial chunk, skipping
            tracing::trace!(bytes_to_skip = next_chunk_in, "skipping bytes");
            let mut take = reader.take(next_chunk_in);
            futures_util::io::copy(&mut take, &mut futures_util::io::sink()).await?;
            reader = take.into_inner();
        }

        while offset < dst_offset {
            let size = min(dst_offset - offset, chunk_size) as usize;
            let expected_offset = offset + size as u64;

            if size == (chunk_size as usize) || offset + size as u64 >= len {
                // making sure this is a full chunk
                // the final chunk of the file may be smaller

                let chunk_id = ChunkId::from_object(dl.object(), offset..(offset + size as u64));
                let mut buf = BytesMut::zeroed(size);
                reader.read_exact(&mut buf).await?;
                let content = buf.freeze();
                let chunk = Chunk::new(chunk_id, content)?;
                cache.insert_chunk(chunk).await?;
            }
            offset = reader.offset();
            if offset < expected_offset {
                let bytes_to_skip = expected_offset - offset;
                tracing::trace!(bytes_to_skip, "skipping bytes");
                let mut take = reader.take(bytes_to_skip);
                futures_util::io::copy(&mut take, &mut futures_util::io::sink()).await?;
                reader = take.into_inner();
            }
        }

        Ok(reader)
    }

    async fn download(
        backend: &Backend,
        download_limiter: Arc<Semaphore>,
        known_dl: &BackendDO,
        offset: u64,
    ) -> anyhow::Result<Download> {
        tracing::debug!(offset, "starting new download");

        tracing::trace!("waiting for download permit");
        let _download_permit =
            timeout(Duration::from_secs(60), download_limiter.acquire_owned()).await??;
        tracing::trace!("download permit acquired");

        let dl = backend.download(known_dl.object().id()).await?;

        if dl.object().version() != known_dl.object().version() {
            bail!("object version has changed, cannot continue");
        }

        tracing::trace!(
            object_id = dl.object().id().to_string(),
            offset = offset,
            "opening new stream"
        );

        let download = dl.open(offset).await?;

        tracing::debug!(
            object_id = dl.object().id().to_string(),
            offset = download.offset(),
            "new download created"
        );

        Ok(download)
    }
}

impl ResourceManager for ChunkDownloader {
    type Resource = Download;
    type PreparationKey = ObjectId;
    type AccessKey = (ObjectId, Version);
    type ResourceData = BackendDO;
    type ResourceFuture = BoxFuture<'static, anyhow::Result<Self::Resource>>;

    async fn prepare(
        &self,
        object_id: &Self::PreparationKey,
    ) -> anyhow::Result<(Self::AccessKey, Self::ResourceData, Vec<Self::Resource>)> {
        let dl = self.backend.download(object_id).await?;
        let access_key = (dl.object().id().clone(), dl.object().version());

        Ok((access_key, dl, vec![]))
    }

    fn process(
        &self,
        queue: &mut QueueCtrl<Self>,
        data: &mut Self::ResourceData,
        _ctx: &Context,
    ) -> anyhow::Result<Action> {
        let entries = queue.entries();
        // calculate available slots
        let mut unused_slots = self.max_active.saturating_sub(
            entries
                .iter()
                .filter(|e| e.as_active().is_some() || e.as_idle().is_some())
                .count(),
        );

        // divide entries into clusters
        let clusters = cluster_entries(entries, self.max_skip_ahead);

        let wait_new_threshold = SystemTime::now() - self.wait_min_new;
        let wait_advance_threshold = SystemTime::now() - self.wait_min_advance;
        let idle_wait_threshold = SystemTime::now() - self.idle_min_wait;

        let mut extra_slots_needed = 0usize;
        let mut sleep = Duration::from_millis(1000);

        for cluster in clusters {
            let first = cluster
                .get(0)
                .expect("cluster should have at least one entry");

            let second = cluster.get(1);

            match first {
                Entry::Waiting(waiting) => {
                    if waiting.offset == 0 || waiting.since <= wait_new_threshold {
                        if unused_slots > 0 {
                            let data = data.clone();
                            let download_limiter = self.download_limiter.clone();
                            let backend = self.backend.clone();
                            let offset = waiting.offset;

                            queue.prepare(
                                waiting,
                                async move {
                                    Self::download(&backend, download_limiter, &data, offset).await
                                }
                                .boxed(),
                            )?;

                            unused_slots = unused_slots.saturating_sub(1);
                        } else {
                            extra_slots_needed += 1;
                        }
                    } else {
                        sleep = min(calc_wait_duration(waiting.since, wait_new_threshold), sleep);
                    }
                }
                Entry::Idle(idle) => {
                    let waiting = second.map(|e| e.as_waiting()).flatten();
                    if waiting.is_some() {
                        if idle.since <= idle_wait_threshold {
                            let waiting = waiting.unwrap();
                            if waiting.since <= wait_advance_threshold {
                                if let Some(reader) = queue.take_idle(idle) {
                                    let offset = waiting.offset;
                                    let data = data.clone();
                                    let cache = self.cache.clone();
                                    let max_skip_ahead = self.max_skip_ahead;
                                    let chunk_size = self.chunk_size;
                                    queue.prepare(
                                        waiting,
                                        async move {
                                            Self::advance(
                                                reader,
                                                offset,
                                                cache,
                                                &data,
                                                max_skip_ahead,
                                                chunk_size,
                                            )
                                            .await
                                        }
                                        .boxed(),
                                    )?;
                                }
                            } else {
                                // still have to wait a little longer
                                sleep = min(
                                    calc_wait_duration(waiting.since, wait_advance_threshold),
                                    sleep,
                                );
                            }
                        } else {
                            sleep = min(calc_wait_duration(idle.since, idle_wait_threshold), sleep);
                        }
                    }
                }
                Entry::Active(_) => {
                    // do nothing here
                }
            }
        }

        if extra_slots_needed > 0 {
            // a previous task could not be started because max active had been reached
            // under pressure we can free idle tasks early
            // idle_timeout_min has to be reached to be freeable
            let idle_timeout_threshold = SystemTime::now() - self.idle_timeout_min;

            tracing::trace!(extra_slots_needed, "attempting to free extra slots");

            // get fresh entries
            let clusters = cluster_entries(queue.entries(), self.max_skip_ahead);

            let mut next_expiration = None;
            let mut again = false;

            for idle in clusters
                .into_iter()
                .filter_map(|v| {
                    // only consider clusters with single idle entries
                    if v.len() == 1 {
                        match v.into_iter().next() {
                            Some(Entry::Idle(idle)) => {
                                // make sure they have exceeded the soft idle timeout
                                if idle.since <= idle_timeout_threshold {
                                    Some(idle)
                                } else {
                                    // not yet, update next_expiration if applicable
                                    let expires_at = idle.since + self.idle_timeout_min;
                                    next_expiration = Some(match next_expiration {
                                        None => expires_at,
                                        Some(current) => min(current, expires_at),
                                    });
                                    None
                                }
                            }
                            _ => None,
                        }
                    } else {
                        None
                    }
                })
                .sorted_unstable_by(|a, b| a.since.cmp(&b.since))
                .take(extra_slots_needed)
            {
                tracing::debug!(offset = idle.offset, "freeing idle task early");
                queue.finalize(&idle)?;
                again = true;
            }

            if again {
                // we were able to free extra resources, run again
                return Ok(Again);
            }

            if let Some(next_expiration) = next_expiration {
                sleep = min(
                    calc_wait_duration(next_expiration, SystemTime::now()),
                    sleep,
                );
            }
        }

        Ok(Sleep(sleep))
    }
}

fn cluster_entries(entries: Vec<Entry>, max_distance: u64) -> Vec<Vec<Entry>> {
    if entries.is_empty() {
        return vec![];
    }

    let mut clusters: Vec<Vec<Entry>> = Vec::new();
    let mut current_cluster: Vec<Entry> = Vec::new();

    for entry in entries {
        if current_cluster.is_empty()
            || entry.offset() - current_cluster.last().unwrap().offset() <= max_distance
                && entry.as_waiting().is_some()
        {
            current_cluster.push(entry);
        } else {
            clusters.push(current_cluster);
            current_cluster = vec![entry];
        }
    }

    if !current_cluster.is_empty() {
        clusters.push(current_cluster);
    }

    clusters
}

fn calc_wait_duration(target: SystemTime, earlier: SystemTime) -> Duration {
    target.duration_since(earlier).unwrap_or_default()
}
