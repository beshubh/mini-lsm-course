// REMOVE THIS LINE after fully implementing this functionality
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

use anyhow::{Context, Result, bail, ensure};
use bytes::BufMut;
use bytes::Bytes;
use crc32fast::Hasher;
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

use std::fs::File;
use std::fs::OpenOptions;
use std::io::BufWriter;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use crate::key::KeySlice;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

const HEADER_SIZE: usize = 8;

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Wal {
            file: Arc::new(Mutex::new(BufWriter::new(
                OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(&path)
                    .context("Wal file create error")?,
            ))),
        })
    }

    pub fn checksum(payload: &[u8]) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(payload);
        hasher.finalize()
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&path)
            .context("wal recover: walfile does not exists")?;
        loop {
            let mut length_buf = [0u8; 4];
            let bytes_read = file
                .read(&mut length_buf)
                .context("reading payload length while recovering WAL")?;
            if bytes_read == 0 {
                break;
            }
            if bytes_read < length_buf.len() {
                file.read_exact(&mut length_buf[bytes_read..])
                    .context("truncated payload length in WAL")?;
            }

            // `put` writes all integers in big-endian order.
            let length = u32::from_be_bytes(length_buf) as usize;

            let mut crc_buf = [0u8; 4];
            file.read_exact(&mut crc_buf)
                .context("reading checksum while recovering WAL")?;
            let expected_crc = u32::from_be_bytes(crc_buf);

            // `with_capacity` alone creates an empty vector, so there would be
            // no bytes for `read_exact` to fill.
            let mut payload = vec![0u8; length];
            file.read_exact(&mut payload)
                .context("reading payload while recovering WAL")?;

            let actual_crc = Self::checksum(&payload);
            if actual_crc != expected_crc {
                bail!("WAL record checksum failed");
            }

            ensure!(payload.len() >= 8, "WAL record payload is too short");

            let key_len = u32::from_be_bytes(payload[..4].try_into()?) as usize;
            let key_start = 4usize;
            let key_end = key_start
                .checked_add(key_len)
                .context("key length overflows WAL record size")?;
            let value_length_end = key_end
                .checked_add(4)
                .context("key length overflows WAL record size")?;
            ensure!(
                value_length_end <= payload.len(),
                "invalid key length in WAL record"
            );

            let value_len =
                u32::from_be_bytes(payload[key_end..value_length_end].try_into()?) as usize;
            let value_start = value_length_end;
            let value_end = value_start
                .checked_add(value_len)
                .context("value length overflows WAL record size")?;
            ensure!(
                value_end == payload.len(),
                "invalid value length in WAL record"
            );

            // Copy the slices because `payload` is dropped at the end of this
            // iteration, while entries in the skiplist must own their bytes.
            let key = Bytes::copy_from_slice(&payload[key_start..key_end]);
            let value = Bytes::copy_from_slice(&payload[value_start..value_end]);
            skiplist.insert(key, value);
        }
        Ok(Wal {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        // [[....]=length, [....]=crc, [... length bytes for payload]]
        let mut payload = vec![];
        payload.put_u32(key.len() as u32);
        payload.extend_from_slice(key);
        payload.put_u32(value.len() as u32);
        payload.extend_from_slice(value);
        let length = payload.len() as u32;
        let crc = Self::checksum(&payload);
        let mut record = Vec::with_capacity((8u32 + length) as usize);
        record.extend_from_slice(&length.to_be_bytes());
        record.extend_from_slice(&crc.to_be_bytes());
        record.extend_from_slice(&payload);

        self.file
            .lock()
            .write_all(&record)
            .context("unable to `put` to walfile")?;
        Ok(())
    }

    /// Implement this in week 3, day 5; if you want to implement this earlier, use `&[u8]` as the key type.
    pub fn put_batch(&self, _data: &[(KeySlice, &[u8])]) -> Result<()> {
        unimplemented!()
    }

    pub fn sync(&self) -> Result<()> {
        let mut guard = self.file.lock();
        guard.flush()?;
        guard.get_mut().sync_all()?;
        Ok(())
    }
}
