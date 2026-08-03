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

use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use std::{fs::File, io::Write};

use anyhow::Result;
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::compact::CompactionTask;

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
}

impl Manifest {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let f = std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)?;
        Ok(Self {
            file: Arc::new(Mutex::new(f)),
        })
    }

    pub fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let f = std::fs::OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)?;
        let manifest = Manifest {
            file: Arc::new(Mutex::new(f)),
        };
        let mut records = vec![];
        let mut buf: Vec<u8> = vec![];
        manifest.file.lock().read_to_end(&mut buf)?;
        let stream = serde_json::Deserializer::from_slice(&buf).into_iter::<ManifestRecord>();
        for res in stream {
            match res {
                Ok(record) => {
                    records.push(record);
                }
                Err(e) => return Err(e.into()),
            }
        }

        Ok((manifest, records))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, record: ManifestRecord) -> Result<()> {
        let encoded_record = serde_json::to_vec(&record)?;
        {
            let file = Arc::clone(&self.file);
            let mut guard = file.lock();
            guard.write_all(&encoded_record)?;
            guard.sync_all()?;
        }
        Ok(())
    }
}
