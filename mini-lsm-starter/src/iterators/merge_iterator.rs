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

use std::cmp::{self};
use std::collections::BinaryHeap;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut heap = BinaryHeap::new();
        for (idx, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                heap.push(HeapWrapper(idx, iter));
            }
        }
        let first = match heap.len() {
            0 => None,
            _ => heap.pop(),
        };
        Self {
            iters: heap,
            current: first,
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice<'_> {
        match self.current.as_ref() {
            None => KeySlice::from_slice(&[]),
            Some(item) => item.1.key(),
        }
    }

    fn value(&self) -> &[u8] {
        match self.current.as_ref() {
            None => &[],
            Some(item) => item.1.value(),
        }
    }

    fn is_valid(&self) -> bool {
        self.current.as_ref().is_some_and(|x| x.1.is_valid())
    }

    fn next(&mut self) -> Result<()> {
        let mut current = self.current.take().unwrap(); // Log (K)
        let current_key = current.1.key().to_key_vec();
        current.1.next()?; // NOTE: this error needs to be handlede manually

        if current.1.is_valid() {
            self.iters.push(current);
        }
        while let Some(mut top) = self.iters.pop() {
            // Log (K)
            // Skip all the older keys, because latest one is the lates value.
            if top.1.key() != current_key.as_key_slice() {
                self.current = Some(top);
                return Ok(());
            }
            top.1.next()?;
            if top.1.is_valid() {
                self.iters.push(top);
            }
        }
        self.current = None;
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        let mut n = self.iters.len();
        if self.current.is_some() {
            n += 1;
        }

        n
    }
}
