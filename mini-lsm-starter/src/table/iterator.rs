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

use std::sync::Arc;

use anyhow::Result;
use anyhow::bail;

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        // find the first block meta from the block metas
        // Duplicate code, idk what to do for this as yet!
        let Some(first_block_meta) = table.block_meta.first() else {
            bail!("Block meta's does not contain anything");
        };
        let block = table.read_block(0)?;
        let mut block_iter = BlockIterator::new(block);
        block_iter.seek_to_first();
        let mut itr = SsTableIterator {
            table: Arc::clone(&table),
            blk_iter: block_iter,
            blk_idx: 0,
        };
        itr.seek_to_first()?;
        Ok(itr)
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        let block = self.table.read_block(0)?;
        self.blk_iter = BlockIterator::new(block);
        self.blk_iter.seek_to_first();
        self.blk_idx = 0;
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let mut itr = Self::create_and_seek_to_first(table)?;
        itr.seek_to_key(key)?;
        Ok(itr)
    }

    pub fn first_key_at_idx(&mut self, idx: usize) -> KeySlice<'_> {
        let block_meta = &self.table.block_meta[idx];
        KeySlice::from(block_meta.first_key.as_key_slice())
    }

    pub fn key_in_block(&mut self, idx: usize, key: KeySlice) -> bool {
        let block_meta = &self.table.block_meta[idx];
        let first_key = KeySlice::from(block_meta.first_key.as_key_slice());
        let last_key = KeySlice::from(block_meta.last_key.as_key_slice());
        first_key <= key && last_key >= key
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let mut left = 0_usize;
        let mut right = self.table.block_meta.len();
        // binary search: find the first block whose first_key > key
        while left < right {
            let mid = (left + right) / 2;
            if self.first_key_at_idx(mid) <= key {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        // left is the first block with first_key > key; target block is left - 1
        let idx = if left == 0 { 0 } else { left - 1 };
        self.seek_to_idx(idx)?;
        self.blk_iter.seek_to_key(key);
        if !self.blk_iter.is_valid() && self.blk_idx + 1 < self.table.num_of_blocks() {
            self.seek_to_idx(self.blk_idx + 1)?;
        }
        Ok(())
    }

    pub fn seek_to_idx(&mut self, idx: usize) -> Result<()> {
        let block_meta = &self.table.block_meta[idx];
        self.blk_idx = idx;
        let block = self.table.read_block(idx)?;
        self.blk_iter = BlockIterator::new(block);
        self.blk_iter.seek_to_first();
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice<'_> {
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();
        if !self.blk_iter.is_valid() {
            // we should move to the next block or error out if the block are over?
            if self.blk_idx + 1 < self.table.num_of_blocks() {
                self.seek_to_idx(self.blk_idx + 1)?;
            }
        }
        Ok(())
    }
}
