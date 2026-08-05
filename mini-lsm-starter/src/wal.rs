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

use anyhow::{Context, Result, bail};
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

use crate::key::KeyBytes;
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

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&path)
            .context("wal recover: walfile does not exists")?;
        loop {
            let mut keylen_buf = [0u8; 4];
            let bytes_read = file
                .read(&mut keylen_buf)
                .context("reading payload length while recovering WAL")?;
            if bytes_read == 0 {
                break;
            }
            if bytes_read < keylen_buf.len() {
                file.read_exact(&mut keylen_buf[bytes_read..])
                    .context("truncated payload length in WAL")?;
            }

            let keylen = u32::from_be_bytes(keylen_buf) as usize;
            let mut key = vec![0u8; keylen];
            file.read_exact(&mut key)
                .context("walfile truncated reading key")?;
            let mut ts_buf = [0u8; 8];
            file.read_exact(&mut ts_buf)
                .context("walfile truncated, reading key ts")?;
            let key_ts = u64::from_be_bytes(ts_buf);

            let mut valuelen_buf = [0u8; 4];
            file.read_exact(&mut valuelen_buf)
                .context("walfile truncated reading value length")?;
            let valuelen = u32::from_be_bytes(valuelen_buf) as usize;
            let mut value = vec![0u8; valuelen];
            file.read_exact(&mut value)
                .context("walfile truncated reading value")?;

            let mut crc_buf = [0u8; 4];
            file.read_exact(&mut crc_buf)
                .context("reading checksum while recovering WAL")?;
            let expected_crc = u32::from_be_bytes(crc_buf);

            // `with_capacity` alone creates an empty vector, so there would be
            // no bytes for `read_exact` to fill.
            let length = 8 + key.len() + value.len();
            let mut payload = Vec::with_capacity(length);
            payload.extend_from_slice(&keylen_buf);
            payload.extend_from_slice(&key);
            payload.extend_from_slice(&ts_buf);
            payload.extend_from_slice(&valuelen_buf);
            payload.extend_from_slice(&value);

            let actual_crc = crc32fast::hash(&payload);
            if actual_crc != expected_crc {
                bail!("WAL record checksum failed");
            }

            let key = KeyBytes::from_bytes_with_ts(Bytes::from(key), key_ts);
            let value = Bytes::copy_from_slice(&value);
            skiplist.insert(key, value);
        }
        Ok(Wal {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        // [[...bytes for payload] | [....]=crc]
        let mut payload = vec![];
        payload.put_u32(key.key_len() as u32);
        payload.extend_from_slice(key.key_ref());
        payload.put_u64(key.ts());
        payload.put_u32(value.len() as u32);
        payload.extend_from_slice(value);
        let crc = crc32fast::hash(&payload);
        payload.put_u32(crc);
        self.file
            .lock()
            .write_all(&payload)
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
