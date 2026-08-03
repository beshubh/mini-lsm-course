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

use std::io::{self, Read, Seek};
use std::path::Path;
use std::sync::Arc;
use std::{fs::File, io::Write};

use anyhow::{Context, Result, bail};
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
        let mut f = std::fs::OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)?;
        f.seek(std::io::SeekFrom::Start(0))?;
        let mut records = vec![];
        loop {
            let mut length_bytes = [0u8; 4];
            match f.read_exact(&mut length_bytes) {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                    // clean EOF if no header bytes were present.
                    // Note: read_exact alone cannot distinguish:
                    // - zero bytes read
                    // - partially written 4-byte header
                    break;
                }
                Err(err) => return Err(err.into()),
            }
            let length = u32::from_be_bytes(length_bytes) as usize;
            let mut encoded_record = vec![0u8; length];
            f.read_exact(&mut encoded_record)
                .context("reading manifest file for the record")?;
            let mut checksum_bytes = [0u8; 4];
            f.read_exact(&mut checksum_bytes)
                .context("reading manfiest for checksum")?;
            let expected = u32::from_be_bytes(checksum_bytes);
            let actual = crc32fast::hash(&encoded_record);
            if actual != expected {
                bail!("manifest checksum mismatch: expected {expected}, got {actual}")
            }
            let record = serde_json::from_slice::<ManifestRecord>(&encoded_record)?;
            records.push(record);
        }
        let manifest = Manifest {
            file: Arc::new(Mutex::new(f)),
        };
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
            let length = encoded_record.len() as u32;
            let checksum = crc32fast::hash(&encoded_record);
            guard.write_all(&length.to_be_bytes())?;
            guard.write_all(&encoded_record)?;
            guard.write_all(&checksum.to_be_bytes())?;
            guard.sync_all()?;
        }
        Ok(())
    }
}
