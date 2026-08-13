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

use std::{
    collections::HashSet,
    ops::Bound,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering::Acquire, Ordering::SeqCst},
    },
};

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use parking_lot::Mutex;

use crate::{
    iterators::{StorageIterator, two_merge_iterator::TwoMergeIterator},
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::{LsmStorageInner, WriteBatchRecord, map_user_bound},
};

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Write set and read set
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl Transaction {
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if self.local_storage.contains_key(key) {
            let v = self.local_storage.get(key).unwrap();
            if v.value().is_empty() {
                return Ok(None);
            }
            return Ok(Some(v.value().clone()));
        }
        self.inner.get_with_ts(key, self.read_ts)
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        let fused_iter = self.inner.scan_with_ts(lower, upper, self.read_ts)?;
        let mut txn_iter = TxnLocalIteratorBuilder {
            map: self.local_storage.clone(),
            iter_builder: |mp| mp.range((map_user_bound(lower), map_user_bound(upper))),
            item: (Bytes::new(), Bytes::new()),
            state_valid: true,
        }
        .build();
        txn_iter.next().unwrap();
        TxnIterator::create(
            self.clone(),
            TwoMergeIterator::create(txn_iter, fused_iter)
                .context("creating two merge iterator over TxnLocalIterator and FusedIterator")?,
        )
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        assert!(!self.committed.load(Acquire));
        let user_key = Bytes::copy_from_slice(key);
        let user_value = Bytes::copy_from_slice(value);
        self.local_storage.insert(user_key, user_value);
    }

    pub fn delete(&self, key: &[u8]) {
        self.put(key, &[]);
    }

    pub fn commit(&self) -> Result<()> {
        if self.committed.load(Acquire) {
            bail!("transaction already committed");
        }
        let records = self
            .local_storage
            .iter()
            .map(|item| {
                if item.value().is_empty() {
                    WriteBatchRecord::Del(item.key().clone())
                } else {
                    WriteBatchRecord::Put(item.key().clone(), item.value().clone())
                }
            })
            .collect::<Vec<_>>();
        self.inner
            .write_batch(&records)
            .context("transaction::commit write batch")?;
        self.committed.store(true, SeqCst);
        Ok(())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        // we register a new reader on new_txn of LsmMvccInner::new_txn and remove it here
        // as TxnIterator owns the transaction, when the transaction goes out of scope
        // drop will be called and thus reader can be removed.
        self.inner.mvcc().ts.lock().1.remove_reader(self.read_ts);
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing]
pub struct TxnLocalIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `TxnLocalIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
    state_valid: bool,
}

impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        self.with_item(|item| item.1.as_ref())
    }

    fn key(&self) -> &[u8] {
        self.with_item(|item| item.0.as_ref())
    }

    fn is_valid(&self) -> bool {
        *self.with_state_valid(|state_valid| state_valid)
    }

    fn next(&mut self) -> Result<()> {
        self.with_mut(|fields| match fields.iter.next() {
            Some(entry) => {
                *fields.item = (entry.key().clone(), entry.value().clone());
                *fields.state_valid = true
            }
            None => *fields.state_valid = false,
        });
        Ok(())
    }
}

pub struct TxnIterator {
    _txn: Arc<Transaction>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub fn create(
        txn: Arc<Transaction>,
        iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
    ) -> Result<Self> {
        let mut obj = TxnIterator { _txn: txn, iter };
        obj.advance_to_visible_key()?;
        Ok(obj)
    }

    fn advance_to_visible_key(&mut self) -> Result<()> {
        if !self.iter.is_valid() || !self.iter.value().is_empty() {
            return Ok(());
        }
        let user_key = self.iter.key().to_vec();
        while self.iter.is_valid() && self.iter.key() == user_key {
            self.iter
                .next()
                .context("TxnIterator failed to advance to visible key")?;
        }
        Ok(())
    }
}

impl StorageIterator for TxnIterator {
    type KeyType<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.iter
            .next()
            .context("TxnIterator next, failed to advance")?;
        self.advance_to_visible_key()
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
