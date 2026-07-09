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

use crate::iterators::StorageIterator;
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::key::KeySlice;
use std::collections::HashMap;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use anyhow::{Result, bail};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::Manifest;
use crate::mem_table::{MemTable, map_bound};
use crate::mvcc::LsmMvccInner;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    /// incase of tiered compaction strategy:
    /// this tuple denotes (tier_id, vec[sst_ids])
    /// and tier_id is generated based on the first SST id for that tier
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        let handle = {
            let mut guard = self.flush_thread.lock();
            guard.take()
        };
        if let Some(handle) = handle {
            handle.join().expect("flush thread panicked.");
        }
        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)?;
        Ok(())
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn mvcc(&self) -> &LsmMvccInner {
        self.mvcc.as_ref().unwrap()
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        std::fs::create_dir_all(path)?;
        let state = LsmStorageState::create(&options);

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1024)),
            next_sst_id: AtomicUsize::new(1),
            compaction_controller,
            manifest: None,
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let state = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        // scan the current memtable
        let val = state.memtable.get(key);
        if let Some(val) = val {
            // TOMBSTONE check
            if val.is_empty() {
                return Ok(None);
            }
            return Ok(Some(val));
        }
        // Scan frozen memtables
        for memtable in state.imm_memtables.iter() {
            let val = memtable.get(key);

            if let Some(val) = val {
                // TOMBSTONE check
                if val.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(val));
            }
        }
        let key_slice = KeySlice::from_slice(key);
        for sst in &state.l0_sstables {
            let ss_table = state.sstables.get(sst);
            let Some(st) = ss_table else {
                bail!("SsTable does not exist in LsmState, should be impossible!");
            };
            let key_hash = farmhash::fingerprint32(key);
            if let Some(bloom) = &st.bloom
                && bloom.may_contain(key_hash)
            {
                let sst_iter = SsTableIterator::create_and_seek_to_key(st.clone(), key_slice)?;
                if sst_iter.key() == key_slice {
                    let val = sst_iter.value();
                    if val.is_empty() {
                        return Ok(None);
                    }
                    return Ok(Some(Bytes::copy_from_slice(val)));
                }
            }
        }
        // compacted levels
        self.get_from_levels(&state, key, key_slice)
    }

    fn get_from_levels(
        &self,
        state: &LsmStorageState,
        key: &[u8],
        key_slice: KeySlice<'_>,
    ) -> Result<Option<Bytes>> {
        for (_, level_sst_ids) in &state.levels {
            let mut left = 0;
            let mut right = level_sst_ids.len();
            while left < right {
                let mid = left + (right - left) / 2;
                let Some(table) = state.sstables.get(&level_sst_ids[mid]) else {
                    bail!("SsTable does not exist in LsmState, should be impossible!");
                };
                if table.last_key().raw_ref() < key {
                    left = mid + 1;
                } else {
                    right = mid;
                }
            }

            let Some(sst_id) = level_sst_ids.get(left) else {
                continue;
            };
            let Some(st) = state.sstables.get(sst_id) else {
                bail!("SsTable does not exist in LsmState, should be impossible!");
            };
            if key < st.first_key().raw_ref() {
                continue;
            }
            let key_hash = farmhash::fingerprint32(key);
            if let Some(bloom) = &st.bloom
                && !bloom.may_contain(key_hash)
            {
                continue;
            }
            let sst_iter = SsTableIterator::create_and_seek_to_key(st.clone(), key_slice)?;
            if sst_iter.is_valid() && sst_iter.key() == key_slice {
                let val = sst_iter.value();
                if val.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(Bytes::copy_from_slice(val)));
            }
        }
        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let state = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        state.memtable.put(key, value)?;
        self.check_and_freeze_if_necessary(state)?;
        Ok(())
    }

    fn check_and_freeze_if_necessary(&self, state: Arc<LsmStorageState>) -> Result<()> {
        let options = self.options.clone();
        if state.memtable.approximate_size() > options.target_sst_size {
            // force freeze of mutable memtable and create a new one
            // Multiple threads could be calling put at the same, and they could try to freeze the memtable at the same time
            // so the freezing process (freeze mutable, create a new one, update the state (self.state)) should be synchronous.
            // DANGER: a consecutive will get high latency because it will get blocked while freezing is in progress
            let Some(statelock) = self.state_lock.try_lock() else {
                return Ok(());
            };
            let latest_state = self.state.read().clone();
            if state.memtable.approximate_size() > options.target_sst_size {
                self.force_freeze_memtable(&statelock)?
            }
        }
        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        let state = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        state.memtable.put(key, &[])?; // TOMBSTONE
        self.check_and_freeze_if_necessary(state)?;
        Ok(())
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        unimplemented!()
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        // if this was WAL it could have taken certain miliseconds to get created
        // But as we are not yet locking the state with write, its not an issue for us.
        let new_memtable = Arc::new(MemTable::create(self.next_sst_id()));
        {
            let mut state_guard = self.state.write();
            let mut new_state = (**state_guard).clone();
            let old_memtable = new_state.memtable.clone();
            new_state.imm_memtables.insert(0, old_memtable);
            new_state.memtable = new_memtable;
            *state_guard = Arc::new(new_state);
        }
        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        // Select the last memtable from imm_memtable.
        // create sst file corresponding to a memtable
        // Remove the memtable from imm_memtable
        let _state_lock = self.state_lock.lock();
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        let Some(last_memtable) = snapshot.imm_memtables.last() else {
            return Ok(());
        };
        let path = self.path_of_sst(last_memtable.id());
        let block_cache = self.block_cache.clone();
        let sst = last_memtable.flush(
            SsTableBuilder::new(self.options.block_size),
            block_cache,
            &path,
        )?;
        {
            let mut state_guard = self.state.write(); // we cannot afford to block here for very long time
            // replace the old state with new is just shifting pointers and not that expensive here
            let mut new_state = (**state_guard).clone();

            dbg!("coming here to modify the state");
            match &self.options.compaction_options {
                CompactionOptions::Tiered(options) => {
                    new_state.imm_memtables.pop();
                    new_state
                        .levels
                        .insert(0, (sst.sst_id(), vec![sst.sst_id()]));
                    dbg!("coming here in tiered comnpaction flush");
                    new_state.sstables.insert(sst.sst_id(), Arc::new(sst));
                    dbg!(
                        "State after flush",
                        new_state.sstables.keys().collect::<Vec<_>>()
                    );
                    *state_guard = Arc::new(new_state);
                }
                _ => {
                    new_state.imm_memtables.pop(); // O(1) operation, cheap
                    new_state.l0_sstables.insert(0, sst.sst_id()); // O(len(l0_sstables)), not that expensive
                    new_state.sstables.insert(sst.sst_id(), Arc::new(sst));
                    *state_guard = Arc::new(new_state); // O(1) operation
                }
            };
        } // write guard droppped.
        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    fn create_sst_iters(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        snapshot: &LsmStorageState,
    ) -> Result<Vec<Box<SsTableIterator>>> {
        let mut sst_iters = vec![];
        for sst_id in &snapshot.l0_sstables {
            // let table = Arc::new(SsTable::open(
            //     *sst_id,
            //     Some(Arc::clone(&self.block_cache)),
            //     FileObject::open(&self.path.join(format!("{}.sst", sst_id)))
            //         .context(format!("Error opening the sst file: {}.sst", sst_id))?,
            // )?);
            let Some(table) = snapshot.sstables.get(sst_id) else {
                continue;
            };
            let table = table.clone();
            let first_key = table.first_key().raw_ref();
            let last_key = table.last_key().raw_ref();
            let skip_for_lower = match lower {
                Bound::Included(target) => last_key < target,
                Bound::Excluded(target) => last_key <= target,
                Bound::Unbounded => false,
            };
            if skip_for_lower {
                continue;
            }

            let skip_for_upper = match upper {
                Bound::Included(target) => first_key > target,
                Bound::Excluded(target) => first_key >= target,
                Bound::Unbounded => false,
            };

            if skip_for_upper {
                continue;
            }

            let mut sst_iter = match lower {
                Bound::Included(key) | Bound::Excluded(key) => {
                    SsTableIterator::create_and_seek_to_key(table, KeySlice::from_slice(key))?
                }
                Bound::Unbounded => SsTableIterator::create_and_seek_to_first(table)?,
            };

            if let Bound::Excluded(key) = lower
                && sst_iter.is_valid()
                && sst_iter.key().raw_ref() == key
            {
                sst_iter.next()?;
            }

            sst_iters.push(Box::new(sst_iter));
        }
        Ok(sst_iters)
    }

    fn create_concat_iters(
        &self,
        snapshot: &LsmStorageState,
        leve_sst_ids: &[usize],
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<SstConcatIterator> {
        let level_sstables = self.get_sstables(snapshot, leve_sst_ids);

        // First SST whose range can contain the lower bound.
        let lower_idx = match lower {
            Bound::Unbounded => 0,
            _ => {
                let mut left = 0;
                let mut right = level_sstables.len();
                while left < right {
                    let mid = left + (right - left) / 2;
                    let last_key = level_sstables[mid].last_key().raw_ref();
                    let lower_is_to_the_right = match lower {
                        Bound::Included(target) => last_key < target,
                        Bound::Excluded(target) => last_key <= target,
                        _ => unreachable!(),
                    };
                    if lower_is_to_the_right {
                        left = mid + 1;
                    } else {
                        right = mid;
                    }
                }
                left
            }
        };

        // First SST whose range starts after the upper bound. This is the exclusive end index.
        let upper_idx = match upper {
            Bound::Unbounded => level_sstables.len(),
            _ => {
                let mut left = 0;
                let mut right = level_sstables.len();
                while left < right {
                    let mid = left + (right - left) / 2;
                    let first_key = level_sstables[mid].first_key().raw_ref();
                    let mid_is_after_upper = match upper {
                        Bound::Included(target) => first_key > target,
                        Bound::Excluded(target) => first_key >= target,
                        _ => unreachable!(),
                    };
                    if mid_is_after_upper {
                        right = mid;
                    } else {
                        left = mid + 1;
                    }
                }
                left
            }
        };

        if lower_idx >= upper_idx {
            return SstConcatIterator::create_and_seek_to_first(vec![]);
        }

        let include_vec = level_sstables[lower_idx..upper_idx].to_vec();
        let mut iter = match lower {
            Bound::Included(key) | Bound::Excluded(key) => {
                SstConcatIterator::create_and_seek_to_key(include_vec, KeySlice::from_slice(key))?
            }
            Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(include_vec)?,
        };

        if let Bound::Excluded(key) = lower
            && iter.is_valid()
            && iter.key().raw_ref() == key
        {
            iter.next()?;
        }
        Ok(iter)
    }

    fn get_sstables(&self, snapshot: &LsmStorageState, sst_ids: &[usize]) -> Vec<Arc<SsTable>> {
        sst_ids
            .iter()
            .map(|sst_id| {
                // unwrap should be safe here?
                snapshot.sstables.get(sst_id).unwrap().clone()
            })
            .collect()
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        // in memory iterators
        let mut memtables = vec![];
        memtables.push(Box::new(snapshot.memtable.scan(lower, upper)));
        snapshot.imm_memtables.iter().for_each(|imm| {
            memtables.push(Box::new(imm.scan(lower, upper)));
        });

        // l0 sst iterators
        let sst_iters = self.create_sst_iters(lower, upper, &snapshot)?;
        let mem_merge_iters = MergeIterator::create(memtables);
        let sst_merge_iters = MergeIterator::create(sst_iters);
        let mem_iters_and_l0_sst_iters =
            TwoMergeIterator::create(mem_merge_iters, sst_merge_iters)?;
        // l1 concat iterators
        let mut level_iters = vec![];
        for (level, level_sst_ids) in &snapshot.levels {
            level_iters.push(Box::new(self.create_concat_iters(
                &snapshot,
                level_sst_ids,
                lower,
                upper,
            )?));
        }
        let level_merge_iter = MergeIterator::create(level_iters);
        let inner = TwoMergeIterator::create(mem_iters_and_l0_sst_iters, level_merge_iter)?;
        let iter = LsmIterator::new(inner, map_bound(upper).clone())?;
        Ok(FusedIterator::new(iter))
    }
}
