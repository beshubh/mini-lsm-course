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
            let mut batch_len_buf = [0u8; 4];
            let bytes_read = file
                .read(&mut batch_len_buf)
                .context("reading payload length while recovering WAL")?;
            if bytes_read == 0 {
                break;
            }
            if bytes_read < batch_len_buf.len() {
                file.read_exact(&mut batch_len_buf[bytes_read..])
                    .context("truncated body length in WAL")?;
            }

            let batch_len = u32::from_be_bytes(batch_len_buf) as usize;
            let mut body = vec![0u8; batch_len];
            file.read_exact(&mut body)
                .context("walfile truncated reading body")?;

            let mut cursor = 0usize;
            let mut key_value_pairs = vec![];
            while cursor < body.len() {
                let keylen = u16::from_be_bytes(body[cursor..(cursor + 2)].try_into().unwrap());
                cursor += 2 as usize;

                let key: Vec<_> = body[cursor..(cursor + keylen as usize)]
                    .iter()
                    .cloned()
                    .collect();
                cursor += keylen as usize;
                let key_ts = u64::from_be_bytes(body[cursor..(cursor + 8)].try_into().unwrap());
                cursor += 8;

                let valuelen =
                    u16::from_be_bytes(body[cursor..(cursor + 2)].try_into().unwrap()) as usize;
                cursor += 2;
                let value: Vec<_> = body[cursor..(cursor + valuelen)].iter().cloned().collect();
                cursor += valuelen;
                key_value_pairs.push(((key, key_ts), value));
            }
            let mut crc_buf = [0u8; 4];
            file.read_exact(&mut crc_buf)
                .context("reading checksum while recovering WAL")?;
            let expected_crc = u32::from_be_bytes(crc_buf);

            let actual_crc = crc32fast::hash(&body);
            if actual_crc != expected_crc {
                bail!("wal record checksum failed");
            }
            key_value_pairs
                .into_iter()
                .for_each(|((key, key_ts), value)| {
                    let key = KeyBytes::from_bytes_with_ts(Bytes::from(key), key_ts);
                    let value = Bytes::from(value);
                    skiplist.insert(key, value);
                });
        }
        Ok(Wal {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        // [[...bytes for payload] | [....]=crc]
        self.put_batch(&[(key, value)])
    }

    /// Implement this in week 3, day 5; if you want to implement this earlier, use `&[u8]` as the key type.
    pub fn put_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        let mut buf = vec![];
        for item in data {
            buf.put_u16(item.0.key_len() as u16);
            buf.extend_from_slice(item.0.key_ref());
            buf.put_u64(item.0.ts());
            buf.put_u16(item.1.len() as u16);
            buf.extend_from_slice(item.1.as_ref());
        }
        let body_length = buf.len();
        let checksum = Self::checksum(&buf);
        let mut payload = vec![];
        payload.put_u32(body_length as u32);
        payload.extend_from_slice(&buf);
        payload.put_u32(checksum);
        self.file
            .lock()
            .write_all(&payload)
            .context("wal `put_batch` failed to write to walfie")
    }

    pub fn sync(&self) -> Result<()> {
        let mut guard = self.file.lock();
        guard.flush()?;
        guard.get_mut().sync_all()?;
        Ok(())
    }
}
