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
use std::{io::Write, path::Path};

use anyhow::Result;
use bytes::BufMut;
use bytes::Bytes;

use super::{BlockMeta, SsTable};

use crate::table::bloom::Bloom;
use crate::{
    block::BlockBuilder,
    key::{KeyBytes, KeySlice},
    lsm_storage::BlockCache,
    table::FileObject,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,

    key_hashes: Vec<u32>,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: vec![],
            last_key: vec![],
            data: vec![],
            meta: Vec::new(),
            block_size,
            key_hashes: vec![],
        }
    }

    pub fn is_empty(&self) -> bool {
        self.builder.is_empty()
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.meta.is_empty() && self.builder.is_empty() {
            self.first_key = key.to_key_vec().into_inner();
        }
        if !self.builder.add(key, value) {
            self.finish_block();
            assert!(
                self.builder.add(key, value),
                "key-value pair exceeds block size"
            );
        }
        self.last_key = key.to_key_vec().into_inner();
        self.key_hashes.push(self.hash(key));
    }

    fn hash(&self, key: KeySlice) -> u32 {
        farmhash::fingerprint32(key.raw_ref())
    }

    fn finish_block(&mut self) {
        if self.builder.is_empty() {
            return;
        }

        let pos = self.data.len();
        // Slit to a new block and clear older first and last keys
        let current_builder =
            std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));

        let first_key = current_builder.first_key();
        let last_key = current_builder.last_key();
        // Push current block in data, and block meta to `meta`
        let curr_block = current_builder.build();

        let block_meta = BlockMeta {
            first_key: first_key.into_key_bytes(),
            last_key: last_key.into_key_bytes(),
            offset: pos,
        };
        self.data.extend_from_slice(&curr_block.encode());
        self.meta.push(block_meta);
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.finish_block();

        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path.as_ref())?;
        let meta_offset = self.data.len();
        let bits_per_key = Bloom::bloom_bits_per_key(self.key_hashes.len(), 0.01);
        let bloom_filter = Bloom::build_from_key_hashes(&self.key_hashes, bits_per_key);

        // encode block meta
        BlockMeta::encode_block_meta(&self.meta, &mut self.data);
        self.data.put_u32(meta_offset.try_into().unwrap());

        // encode bloom filter
        let bloom_offset = self.data.len();
        bloom_filter.encode(&mut self.data);
        self.data.put_u32(bloom_offset.try_into().unwrap());
        // flush
        file.write_all(&self.data)?;
        file.sync_all()?;
        Ok(SsTable {
            file: FileObject::open(path.as_ref())?,
            block_meta_offset: meta_offset,
            block_meta: self.meta.clone(),
            first_key: KeyBytes::from_bytes(Bytes::from(self.first_key)),
            last_key: KeyBytes::from_bytes(Bytes::from(self.last_key)),
            block_cache,
            id,
            bloom: Some(bloom_filter),
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
