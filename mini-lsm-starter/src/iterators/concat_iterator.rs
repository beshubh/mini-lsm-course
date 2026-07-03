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

use std::sync::Arc;

use anyhow::{Context, Result, bail};

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        if sstables.is_empty() {
            return Ok(Self {
                current: None,
                next_sst_idx: 0,
                sstables,
            });
        }
        let current = SsTableIterator::create_and_seek_to_first(sstables.first().unwrap().clone())
            .context("concat iter: unable to create first sst iterator from sstable")?;
        Ok(Self {
            current: Some(current),
            next_sst_idx: 1,
            sstables,
        })
    }

    fn seek_to_idx(&mut self, idx: usize, key: KeySlice) -> Result<()> {
        let sst = &self.sstables[idx];
        self.current = Some(SsTableIterator::create_and_seek_to_key(sst.clone(), key)?);
        self.next_sst_idx = idx + 1;
        Ok(())
    }

    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let mut left = 0_usize;
        let mut right = self.sstables.len();
        while left < right {
            let mid = (left + right) / 2;
            let mid_key = self.sstables[mid].first_key();
            if mid_key.as_key_slice() <= key {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        let idx = if left == 0 { 0 } else { left - 1 };
        self.seek_to_idx(idx, key)
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        if sstables.is_empty() {
            return Ok(Self {
                current: None,
                next_sst_idx: 0,
                sstables,
            });
        }
        let mut itr = Self::create_and_seek_to_first(sstables)?;
        itr.seek_to_key(key)
            .context("concat iter: error seeking to key")?;
        Ok(itr)
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice<'_> {
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        match &self.current {
            None => false,
            Some(inner) => inner.is_valid(),
        }
    }

    fn next(&mut self) -> Result<()> {
        if !self.is_valid() {
            bail!("concat iter: iterator not valid before next");
        }
        self.current.as_mut().unwrap().next()?;
        if !self.current.as_ref().unwrap().is_valid() {
            if self.next_sst_idx < self.sstables.len() {
                self.current = Some(SsTableIterator::create_and_seek_to_first(
                    self.sstables[self.next_sst_idx].clone(),
                )?);
                self.next_sst_idx += 1;
            } else {
                self.current = None;
            }
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
