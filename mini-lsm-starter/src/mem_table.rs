// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use crate::iterators::StorageIterator;
use crate::key::{KeyBytes, KeySlice, TS_DEFAULT};
use crate::lsm_storage::BlockCache;
use crate::table::{SsTable, SsTableBuilder};
use crate::wal::Wal;
use anyhow::{Context, Result};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;

/// A basic mem-table based on crossbeam-skiplist (sorted map)
///
/// An initial implementation of memtable is part of week 1, day 1. It will be incrementally implemented in other
/// chapters of week 1 and week 2.
pub struct MemTable {
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    wal: Option<Wal>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
}

/// Create a bound of `Bytes` from a bound of `&[u8]`.
pub(crate) fn map_bound(bound: Bound<KeySlice>) -> Bound<KeyBytes> {
    match bound {
        Bound::Included(x) => Bound::Included(KeyBytes::from_bytes_with_ts(
            Bytes::copy_from_slice(x.key_ref()),
            x.ts(),
        )),
        Bound::Excluded(x) => Bound::Excluded(KeyBytes::from_bytes_with_ts(
            Bytes::copy_from_slice(x.key_ref()),
            x.ts(),
        )),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl MemTable {
    /// Create a new mem-table.
    pub fn create(id: usize) -> Self {
        Self {
            map: Arc::new(SkipMap::new()),
            approximate_size: Arc::new(AtomicUsize::new(0)),
            wal: None,
            id,
        }
    }

    /// Create a new mem-table with WAL
    pub fn create_with_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let wal = Wal::create(&path)?;
        Ok(Self {
            map: Arc::new(SkipMap::new()),
            wal: Some(wal),
            id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Create a memtable from WAL
    pub fn recover_from_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let map = SkipMap::new();
        let wal = Wal::recover(&path, &map)
            .with_context(|| format!("wal recovery for memtable: {}", id))?;
        let mut size = 0;
        for pair in map.iter() {
            size += pair.key().key_len() + pair.value().len(); // TODO: this might need a change
        }
        Ok(Self {
            map: Arc::new(map),
            wal: Some(wal),
            id,
            approximate_size: Arc::new(AtomicUsize::new(size)),
        })
    }

    pub fn for_testing_put_slice(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put(KeySlice::from_slice(key, TS_DEFAULT), value)
    }

    pub fn for_testing_get_slice(&self, key: &[u8]) -> Option<Bytes> {
        self.get(KeySlice::from_slice(key, TS_DEFAULT))
    }

    pub fn for_testing_scan_slice(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> MemTableIterator {
        // This function is only used in week 1 tests, so during the week 3 key-ts refactor, you do
        // not need to consider the bound exclude/include logic. Simply provide `DEFAULT_TS` as the
        // timestamp for the key-ts pair.
        let lower = match lower {
            Bound::Included(key) => Bound::Included(KeySlice::from_slice(key, TS_DEFAULT)),
            Bound::Excluded(key) => Bound::Excluded(KeySlice::from_slice(key, TS_DEFAULT)),
            Bound::Unbounded => Bound::Unbounded,
        };
        let upper = match upper {
            Bound::Included(key) => Bound::Included(KeySlice::from_slice(key, TS_DEFAULT)),
            Bound::Excluded(key) => Bound::Excluded(KeySlice::from_slice(key, TS_DEFAULT)),
            Bound::Unbounded => Bound::Unbounded,
        };
        self.scan(lower, upper)
    }

    /// Get a value by key.
    pub fn get(&self, key: KeySlice) -> Option<Bytes> {
        let key_bytes = Bytes::from_static(unsafe { std::mem::transmute(key.key_ref()) });
        let key = &KeyBytes::from_bytes_with_ts(key_bytes, key.ts());
        if let Some(x) = self.map.get(key) {
            let value = x.value();
            return Some(x.value().clone());
        }
        None
    }

    /// Put a key-value pair into the mem-table.
    ///
    /// In week 1, day 1, simply put the key-value pair into the skipmap.
    /// In week 2, day 6, also flush the data to WAL.
    /// In week 3, day 5, modify the function to use the batch API.
    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        self.put_batch(&[(key, value)])
    }

    /// Implement this in week 3, day 5; if you want to implement this earlier, use `&[u8]` as the key type.
    pub fn put_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        if let Some(wal) = &self.wal {
            wal.put_batch(data)?;
        }
        data.iter().for_each(|item| {
            let keylen = item.0.key_len();
            let valuelen = item.1.len();
            let k =
                KeyBytes::from_bytes_with_ts(Bytes::copy_from_slice(item.0.key_ref()), item.0.ts());
            let v = Bytes::copy_from_slice(item.1);
            let size = keylen + valuelen;
            self.approximate_size
                .fetch_add(size, std::sync::atomic::Ordering::Relaxed);
            self.map.insert(k, v);
        });
        Ok(())
    }

    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()?;
        }
        Ok(())
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, lower: Bound<KeySlice>, upper: Bound<KeySlice>) -> MemTableIterator {
        let mut iter = MemTableIteratorBuilder {
            map: self.map.clone(),
            iter_builder: |mp| mp.range((map_bound(lower), map_bound(upper))),
            item: (KeyBytes::new(), Bytes::new()),
            state_valid: true,
        }
        .build();
        iter.next().unwrap();
        iter
    }

    /// Flush the mem-table to SSTable. Implement in week 1 day 6.
    pub fn flush(
        &self,
        mut builder: SsTableBuilder,
        block_cache: Arc<BlockCache>,
        path: &PathBuf,
    ) -> Result<SsTable> {
        // can we do it in background?
        for entry in self.map.iter() {
            let key = entry.key();
            let value = entry.value();
            builder.add(
                KeySlice::from_slice(key.key_ref(), key.ts()),
                value.as_ref(),
            );
        }
        let sst = builder.build(self.id(), Some(block_cache), path)?;
        Ok(sst)
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Only use this function when closing the database
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

type SkipMapRangeIter<'a> = crossbeam_skiplist::map::Range<
    'a,
    KeyBytes,
    (Bound<KeyBytes>, Bound<KeyBytes>),
    KeyBytes,
    Bytes,
>;

/// An iterator over a range of `SkipMap`. This is a self-referential structure and please refer to week 1, day 2
/// chapter for more information.
///
/// This is part of week 1, day 2.
#[self_referencing]
pub struct MemTableIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `MemTableIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (KeyBytes, Bytes),
    state_valid: bool,
}

impl StorageIterator for MemTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        self.with_item(|item| item.1.as_ref())
    }

    fn key(&self) -> KeySlice<'_> {
        self.with_item(|item| KeySlice::from_slice(item.0.key_ref(), item.0.ts()))
    }

    fn is_valid(&self) -> bool {
        *self.with_state_valid(|state_valid| state_valid)
    }

    fn next(&mut self) -> Result<()> {
        self.with_mut(|fields| match fields.iter.next() {
            Some(entry) => {
                *fields.item = (entry.key().clone(), entry.value().clone());
                *fields.state_valid = true;
            }
            None => *fields.state_valid = false,
        });
        Ok(())
    }
}
