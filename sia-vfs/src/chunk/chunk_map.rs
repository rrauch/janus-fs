use crate::chunk::ChunkId;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
struct ChunkEntry {
    end: u64,
    chunk_id: ChunkId,
    chunk_offset: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChunkMapEntry<'a> {
    Chunk {
        chunk_id: &'a ChunkId,
        chunk_offset: usize,
        len: u64,
    },
    Hole {
        len: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkRange<'a> {
    pub offset: u64,
    pub len: u64,
    pub chunk_id: &'a ChunkId,
    pub chunk_offset: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct ChunkMap {
    len: u64,
    /// Keyed by range start offset. Invariant: non-overlapping, sorted, coalesced.
    entries: BTreeMap<u64, ChunkEntry>,
}

impl ChunkMap {
    pub fn new() -> Self {
        Self {
            len: 0,
            entries: BTreeMap::new(),
        }
    }

    pub fn with_len(len: u64) -> Self {
        Self {
            len,
            entries: BTreeMap::new(),
        }
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = ChunkRange<'_>> {
        self.entries.iter().map(|(&start, entry)| ChunkRange {
            offset: start,
            len: entry.end - start,
            chunk_id: &entry.chunk_id,
            chunk_offset: entry.chunk_offset,
        })
    }

    pub fn insert(&mut self, offset: u64, len: u64, chunk_id: ChunkId, chunk_offset: usize) {
        if len == 0 {
            return;
        }
        // Clamp to file length
        let end = offset.saturating_add(len).min(self.len);
        if offset >= self.len || end <= offset {
            return;
        }

        self.remove_range(offset, end);

        self.entries.insert(
            offset,
            ChunkEntry {
                end,
                chunk_id,
                chunk_offset,
            },
        );

        self.try_merge_at(offset);
        if let Some((&prev_start, _)) = self.entries.range(..offset).next_back() {
            self.try_merge_at(prev_start);
        }
    }

    pub fn remove(&mut self, offset: u64, len: u64) {
        if len == 0 {
            return;
        }
        if offset >= self.len {
            return;
        }
        let end = offset.saturating_add(len).min(self.len);
        self.remove_range(offset, end);
    }

    pub fn set_len(&mut self, new_len: u64) {
        if new_len < self.len {
            self.remove_range(new_len, self.len);
        }
        self.len = new_len;
    }

    pub fn get(&self, offset: u64) -> Option<ChunkMapEntry<'_>> {
        if offset >= self.len {
            return None;
        }

        if let Some((&start, entry)) = self.entries.range(..=offset).next_back() {
            if offset < entry.end {
                let remaining = entry.end - offset;
                let chunk_off = entry.chunk_offset + (offset - start) as usize;
                return Some(ChunkMapEntry::Chunk {
                    chunk_id: &entry.chunk_id,
                    chunk_offset: chunk_off,
                    len: remaining,
                });
            }
        }

        let hole_end = self
            .entries
            .range((
                std::ops::Bound::Excluded(offset),
                std::ops::Bound::Unbounded,
            ))
            .next()
            .map(|(&next_start, _)| next_start.min(self.len))
            .unwrap_or(self.len);

        Some(ChunkMapEntry::Hole {
            len: hole_end - offset,
        })
    }

    fn remove_range(&mut self, start: u64, end: u64) {
        if start >= end {
            return;
        }

        let iter_start = match self.entries.range(..=start).next_back() {
            Some((&k, _)) => std::ops::Bound::Included(k),
            None => std::ops::Bound::Unbounded,
        };

        let mut to_remove = Vec::new();
        let mut to_insert: Vec<(u64, ChunkEntry)> = Vec::new();

        for (&e_start, entry) in self.entries.range((iter_start, std::ops::Bound::Unbounded)) {
            if e_start >= end {
                break;
            }
            if entry.end <= start {
                continue;
            }

            to_remove.push(e_start);

            if e_start < start {
                to_insert.push((
                    e_start,
                    ChunkEntry {
                        end: start,
                        chunk_id: entry.chunk_id.clone(),
                        chunk_offset: entry.chunk_offset,
                    },
                ));
            }

            if entry.end > end {
                to_insert.push((
                    end,
                    ChunkEntry {
                        end: entry.end,
                        chunk_id: entry.chunk_id.clone(),
                        chunk_offset: entry.chunk_offset + (end - e_start) as usize,
                    },
                ));
            }
        }

        for k in to_remove {
            self.entries.remove(&k);
        }
        for (k, v) in to_insert {
            self.entries.insert(k, v);
        }
    }

    fn try_merge_at(&mut self, start: u64) {
        let entry = match self.entries.get(&start) {
            Some(v) => v.clone(),
            None => return,
        };

        if let Some((&next_start, next_entry)) = self
            .entries
            .range((std::ops::Bound::Excluded(start), std::ops::Bound::Unbounded))
            .next()
        {
            if entry.end == next_start
                && entry.chunk_id == next_entry.chunk_id
                && entry.chunk_offset + (entry.end - start) as usize == next_entry.chunk_offset
            {
                let merged_end = next_entry.end;
                self.entries.remove(&next_start);
                self.entries.get_mut(&start).unwrap().end = merged_end;
            }
        }
    }

    pub(crate) fn hash(&self, hasher: &mut blake3::Hasher) {
        hasher.update(b"begin\nlength:");
        hasher.update(&self.len().to_be_bytes());
        hasher.update(b"\nno_chunks:");
        hasher.update(&(self.iter().count() as u32).to_be_bytes());
        hasher.update(b"\nnchunks:");
        for chunk_range in self.iter() {
            hasher.update(b"begin_chunk\noffset:");
            hasher.update(&chunk_range.offset.to_be_bytes());
            hasher.update(b"\nlength:");
            hasher.update(&chunk_range.len.to_be_bytes());
            hasher.update(b"\nchunk_id:");
            hasher.update(chunk_range.chunk_id.as_ref());
            hasher.update(b"\nchunk_offset:");
            hasher.update(&chunk_range.chunk_offset.to_be_bytes());
            hasher.update(b"\nend_chunk");
        }
        hasher.update(b"\nend");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk::Chunk;

    fn cid(n: u8) -> ChunkId {
        let chunk = Chunk::from(vec![n; 32]);
        chunk.id().clone()
    }

    #[test]
    fn empty_map() {
        let map = ChunkMap::with_len(100);
        assert_eq!(map.get(0), Some(ChunkMapEntry::Hole { len: 100 }));
        assert_eq!(map.get(50), Some(ChunkMapEntry::Hole { len: 50 }));
        assert_eq!(map.get(100), None);
    }

    #[test]
    fn basic_insert_and_get() {
        let id = cid(1);
        let mut map = ChunkMap::with_len(100);
        map.insert(10, 20, id.clone(), 0);

        assert_eq!(map.get(0), Some(ChunkMapEntry::Hole { len: 10 }));
        assert_eq!(
            map.get(10),
            Some(ChunkMapEntry::Chunk {
                chunk_id: &id,
                chunk_offset: 0,
                len: 20
            })
        );
        assert_eq!(
            map.get(15),
            Some(ChunkMapEntry::Chunk {
                chunk_id: &id,
                chunk_offset: 5,
                len: 15
            })
        );
        assert_eq!(map.get(30), Some(ChunkMapEntry::Hole { len: 70 }));
    }

    #[test]
    fn insert_clamps_to_len() {
        let id = cid(1);
        let mut map = ChunkMap::with_len(50);
        map.insert(40, 20, id.clone(), 0);

        assert_eq!(
            map.get(40),
            Some(ChunkMapEntry::Chunk {
                chunk_id: &id,
                chunk_offset: 0,
                len: 10
            })
        );
        assert_eq!(map.get(50), None);
    }

    #[test]
    fn insert_beyond_len_is_noop() {
        let mut map = ChunkMap::with_len(50);
        map.insert(50, 10, cid(1), 0);
        assert_eq!(map.get(49), Some(ChunkMapEntry::Hole { len: 1 }));
    }

    #[test]
    fn overlapping_insert_splits() {
        let id1 = cid(1);
        let id2 = cid(2);
        let mut map = ChunkMap::with_len(100);
        map.insert(0, 30, id1.clone(), 0);
        map.insert(10, 10, id2.clone(), 0);

        assert_eq!(
            map.get(0),
            Some(ChunkMapEntry::Chunk {
                chunk_id: &id1,
                chunk_offset: 0,
                len: 10
            })
        );
        assert_eq!(
            map.get(10),
            Some(ChunkMapEntry::Chunk {
                chunk_id: &id2,
                chunk_offset: 0,
                len: 10
            })
        );
        assert_eq!(
            map.get(20),
            Some(ChunkMapEntry::Chunk {
                chunk_id: &id1,
                chunk_offset: 20,
                len: 10
            })
        );
    }

    #[test]
    fn remove_splits_entry() {
        let id = cid(1);
        let mut map = ChunkMap::with_len(100);
        map.insert(0, 30, id.clone(), 0);
        map.remove(10, 10);

        assert_eq!(
            map.get(0),
            Some(ChunkMapEntry::Chunk {
                chunk_id: &id,
                chunk_offset: 0,
                len: 10
            })
        );
        assert_eq!(map.get(10), Some(ChunkMapEntry::Hole { len: 10 }));
        assert_eq!(
            map.get(20),
            Some(ChunkMapEntry::Chunk {
                chunk_id: &id,
                chunk_offset: 20,
                len: 10
            })
        );
    }

    #[test]
    fn set_len_truncates() {
        let id = cid(1);
        let mut map = ChunkMap::with_len(100);
        map.insert(0, 50, id.clone(), 0);
        map.set_len(25);

        assert_eq!(map.len(), 25);
        assert_eq!(
            map.get(0),
            Some(ChunkMapEntry::Chunk {
                chunk_id: &id,
                chunk_offset: 0,
                len: 25
            })
        );
        assert_eq!(map.get(25), None);
    }

    #[test]
    fn set_len_extends() {
        let mut map = ChunkMap::with_len(10);
        map.set_len(100);
        assert_eq!(map.len(), 100);
        assert_eq!(map.get(50), Some(ChunkMapEntry::Hole { len: 50 }));
    }

    #[test]
    fn adjacent_entries_merge() {
        let id = cid(1);
        let mut map = ChunkMap::with_len(100);
        map.insert(0, 10, id.clone(), 0);
        map.insert(10, 10, id.clone(), 10);

        assert_eq!(
            map.get(0),
            Some(ChunkMapEntry::Chunk {
                chunk_id: &id,
                chunk_offset: 0,
                len: 20
            })
        );
        assert_eq!(map.entries.len(), 1);
    }

    #[test]
    fn adjacent_entries_no_merge_different_chunk() {
        let mut map = ChunkMap::with_len(100);
        map.insert(0, 10, cid(1), 0);
        map.insert(10, 10, cid(2), 0);

        assert_eq!(map.entries.len(), 2);
    }

    #[test]
    fn iter_entries() {
        let id1 = cid(1);
        let id2 = cid(2);
        let mut map = ChunkMap::with_len(100);
        map.insert(0, 10, id1.clone(), 0);
        map.insert(20, 10, id2.clone(), 5);

        let entries: Vec<_> = map.iter().collect();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].offset, 0);
        assert_eq!(entries[0].len, 10);
        assert_eq!(entries[1].offset, 20);
        assert_eq!(entries[1].chunk_offset, 5);
    }
}
