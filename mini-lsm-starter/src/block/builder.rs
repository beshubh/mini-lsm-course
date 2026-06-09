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

use bytes::BufMut;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
    last_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: vec![],
            data: vec![],
            block_size,
            first_key: KeyVec::new(),
            last_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    /// You may find the `bytes::BufMut` trait useful for manipulating binary data.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let Ok(keyoffset) = self.data.len().try_into() else {
            eprintln!("data size too large, cannot put more keys to the block");
            return false;
        };

        let mut overlap_len = 0;
        if self.offsets.is_empty() {
            self.first_key.set_from_slice(key);
        } else {
            overlap_len = self
                .first_key()
                .into_inner()
                .iter()
                .zip(key.into_inner().iter())
                .take_while(|(a, b)| a == b)
                .count();
        }

        let rest_key = &key.raw_ref()[overlap_len..];
        // 2  overlap_len
        // 2  rest_key_len
        // N  rest_key
        // 2  value_len
        // M  value
        if !self.is_empty()
            && 2 + 2 + rest_key.len() + 2 + value.len() + self.data.len() >= self.block_size
        {
            return false;
        }

        let rest_key_len: u16 = rest_key
            .len()
            .try_into()
            .map_err(|_| "key too large to encode as u16")
            .unwrap();

        self.data.put_u16(overlap_len as u16);
        self.data.put_u16(rest_key_len);
        self.data.put_slice(rest_key);

        let valuelen: u16 = value
            .len()
            .try_into()
            .map_err(|_| "value too large to encode as u16")
            .unwrap();

        self.data.put_u16(valuelen);
        self.data.put_slice(value);
        self.offsets.push(keyoffset);
        self.last_key.set_from_slice(key);
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }

    pub fn first_key(&self) -> KeyVec {
        self.first_key.clone()
    }

    pub fn last_key(&self) -> KeyVec {
        self.last_key.clone()
    }

    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }
}
