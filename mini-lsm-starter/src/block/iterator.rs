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

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Block encoding layout (on-disk):
///
/// ```text
/// ┌──────────────────────────────────────────────────────────────┐
/// │                        Key-Value Entries                     │
/// │                                                              │
/// │  Entry 0: ┌──────┬─────────┬────────┬──────────┐            │
/// │           │keylen│   key   │valuelen│   value  │            │
/// │           │u16 BE│keylen B │ u16 BE │valuelen B│            │
/// │           └──────┴─────────┴────────┴──────────┘            │
/// │  Entry 1: ┌──────┬─────────┬────────┬──────────┐            │
/// │           │keylen│   key   │valuelen│   value  │            │
/// │           │u16 BE│keylen B │ u16 BE │valuelen B│            │
/// │           └──────┴─────────┴────────┴──────────┘            │
/// │  ...                                                         │
/// │                                                              │
/// ├──────────────────────────────────────────────────────────────┤
/// │                      Offset Array                            │
/// │  [offset_0 (u16), offset_1 (u16), ..., offset_n-1 (u16)]    │
/// │  (each u16 points to the start of the corresponding entry)   │
/// ├──────────────────────────────────────────────────────────────┤
/// │  num_offsets (u16 BE) — number of key-value entries          │
/// └──────────────────────────────────────────────────────────────┘
/// ```
///
/// Key-value entry encoding within the data section:
///
/// ```text
/// ┌──────────┬──────────────────────┬──────────┬──────────────────┐
/// │ keylen   │ key content          │ valuelen │ value content    │
/// │ (2 bytes)│ (keylen bytes)       │ (2 bytes)│ (valuelen bytes) │
/// └──────────┴──────────────────────┴──────────┴──────────────────┘
///   ↑                                                            ↑
///   offset for this entry points here                             end of entry
/// ```

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    pub fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    fn clear(&mut self) {
        self.key.clear();
        self.value_range = (0, 0);
        self.idx = self.block.offsets.len();
    }

    fn key_at_idx(&self, idx: usize) -> KeySlice<'_> {
        let mut pos = self.block.offsets[idx] as usize;
        let keylen = u16::from_be_bytes([self.block.data[pos], self.block.data[pos + 1]]) as usize;
        pos += 2;
        KeySlice::from_slice(&self.block.data[pos..pos + keylen])
    }

    fn seek_to_idx(&mut self, idx: usize) {
        if idx >= self.block.offsets.len() {
            self.clear();
            return;
        }

        let mut pos = self.block.offsets[idx] as usize;
        let keylen = u16::from_be_bytes([self.block.data[pos], self.block.data[pos + 1]]) as usize;
        pos += 2;
        let key = KeyVec::from_vec(self.block.data[pos..pos + keylen].to_vec());
        pos += keylen;
        let valuelen =
            u16::from_be_bytes([self.block.data[pos], self.block.data[pos + 1]]) as usize;
        pos += 2;
        let value_start = pos;
        let value_end = pos + valuelen;

        self.key = key;
        self.idx = idx;
        self.value_range = (value_start, value_end);
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice<'_> {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key().is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        if self.block.offsets.is_empty() {
            self.clear();
            return;
        }

        self.seek_to_idx(0);
        self.first_key = self.key.clone();
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.seek_to_idx(self.idx + 1);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, target: KeySlice) {
        let mut left = 0;
        let mut right = self.block.offsets.len();

        while left < right {
            let mid = (left + right) / 2;
            if self.key_at_idx(mid) < target {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        self.seek_to_idx(left);
    }
}
