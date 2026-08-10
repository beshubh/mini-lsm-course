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

use bytes::Bytes;
use std::ops::Bound;

use anyhow::Result;

use crate::{
    iterators::{
        StorageIterator, concat_iterator::SstConcatIterator, merge_iterator::MergeIterator,
        two_merge_iterator::TwoMergeIterator,
    },
    mem_table::MemTableIterator,
    table::SsTableIterator,
};

fn is_past_upper_bound(upper_bound: &Bound<Bytes>, value: &[u8]) -> bool {
    match upper_bound.as_ref() {
        Bound::Included(b) => value > b.as_ref(),
        Bound::Excluded(b) => value >= b.as_ref(),
        Bound::Unbounded => false,
    }
}

/// Represents the internal type for an LSM iterator. This type will be changed across the course for multiple times.
type LsmIteratorInner = TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    MergeIterator<SstConcatIterator>,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    end_bound: Bound<Bytes>,
    read_ts: u64,
    stopped: bool,
}

impl LsmIterator {
    pub(crate) fn new(
        iter: LsmIteratorInner,
        end_bound: Bound<Bytes>,
        read_ts: u64,
    ) -> Result<Self> {
        let mut obj = Self {
            inner: iter,
            end_bound: end_bound.clone(),
            read_ts,
            stopped: false,
        };
        obj.advance_to_visible_key()?;
        if obj.is_valid() && is_past_upper_bound(&end_bound, obj.key()) {
            obj.stopped = true;
        }
        Ok(obj)
    }

    fn advance_to_visible_key(&mut self) -> Result<()> {
        loop {
            // Versions newer than the snapshot are invisible, but an older version of the same
            // user key may still be visible.
            while self.inner.is_valid() && self.inner.key().ts() > self.read_ts {
                self.inner.next()?;
            }

            if !self.inner.is_valid() || !self.inner.value().is_empty() {
                return Ok(());
            }

            // A visible tombstone shadows every older version of this user key.
            let user_key = self.inner.key().key_ref().to_vec();
            while self.inner.is_valid() && user_key.as_slice() == self.inner.key().key_ref() {
                self.inner.next()?;
            }
        }
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.inner.is_valid() && !self.stopped
    }

    fn key(&self) -> &[u8] {
        self.inner.key().into_inner()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.stopped {
            return Ok(());
        }
        let prev_key = self.inner.key().key_ref().to_vec();
        self.inner.next()?;

        // The visible version has been returned, so discard the remaining older versions.
        while self.inner.is_valid() && self.inner.key().key_ref() == prev_key.as_slice() {
            self.inner.next()?;
        }

        self.advance_to_visible_key()?;
        if self.is_valid() && is_past_upper_bound(&self.end_bound, self.key()) {
            self.stopped = true
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a>
        = I::KeyType<'a>
    where
        Self: 'a;

    fn is_valid(&self) -> bool {
        if self.has_errored {
            return false;
        }
        self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            return Err(anyhow::anyhow!("iterator has errored"));
        }
        if !self.is_valid() {
            return Ok(());
        }
        let res = self.iter.next();
        match res {
            Ok(_) => Ok(()),

            Err(e) => {
                self.has_errored = true;
                Err(e)
            }
        }
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
