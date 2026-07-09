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

use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    // Add fields as need
    current_a: bool,
    both_invalid: bool,
}

impl<
    A: 'static + StorageIterator,
    B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
> TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        let mut current_a = false;
        if a.is_valid() && b.is_valid() {
            if a.key() <= b.key() {
                current_a = true;
            }
        } else if a.is_valid() {
            current_a = true;
        }
        Ok(Self {
            a,
            b,
            current_a,
            both_invalid: false,
        })
    }
}

impl<
    A: 'static + StorageIterator,
    B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
> StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        if self.current_a {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        if self.current_a {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn is_valid(&self) -> bool {
        if self.both_invalid {
            return false;
        }
        if self.current_a {
            self.a.is_valid()
        } else {
            self.b.is_valid()
        }
    }

    fn next(&mut self) -> Result<()> {
        if self.current_a {
            self.a.next()?;
            // skip all the elments from b that are less than a
            while self.a.is_valid() && self.b.is_valid() && self.a.key() >= self.b.key() {
                self.b.next()?;
            }
        } else {
            self.b.next()?;
            while self.b.is_valid() && self.a.is_valid() && self.b.key() > self.a.key() {
                self.a.next()?;
            }
        }

        if !self.a.is_valid() && !self.b.is_valid() {
            self.both_invalid = true;
            return Ok(());
        }
        if self.a.is_valid() && self.b.is_valid() {
            self.current_a = self.a.key() <= self.b.key();
        } else {
            self.current_a = self.a.is_valid();
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.a.num_active_iterators() + self.b.num_active_iterators()
    }
}
