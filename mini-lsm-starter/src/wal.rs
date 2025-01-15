#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use nom::AsBytes;
use parking_lot::Mutex;

use crate::key::{KeyBytes, KeySlice};
use crate::table::validate_checksum;
use crate::{U16_SIZE, U32_SIZE, U64_SIZE};

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::create(path)?;
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    /// Recover WAL from a given path
    /// Each record in the WAL has the following layout:
    /// | key_raw_len | key | timestamp | value_len | value | checksum |
    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        let file = OpenOptions::new().read(true).append(true).open(path)?;

        let mut reader = BufReader::new(&file);
        let mut buf_checksum = [0_u8; U32_SIZE];

        while let (Ok(mut key_data), Ok(mut value_data), Ok(_)) = (
            read_data_u16_with_length(&mut reader),
            read_data_u16_with_length(&mut reader),
            reader.read_exact(&mut buf_checksum),
        ) {
            let data_and_checksum = [key_data.as_bytes(), &value_data, &buf_checksum].concat();
            if validate_checksum(&data_and_checksum).is_err() {
                continue;
            }

            // Remove the length data.
            key_data.drain(..2);
            value_data.drain(..2);

            let (key, ts) = key_data
                .split_last_chunk::<U64_SIZE>()
                .with_context(|| "key should contain timestamp")?;
            let ts = u64::from_le_bytes(*ts);

            let key = KeyBytes::from_bytes_with_ts(Bytes::copy_from_slice(key), ts);
            let value = Bytes::from(value_data);
            skiplist.insert(key, value);
        }

        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        self.put_batch(&[(key, value)])
    }

    /// Implement this in week 3, day 5.
    /// Each record in the WAL has the following layout:
    /// | key_raw_len | key | timestamp | value_len | value | checksum |
    pub fn put_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        let mut buf = self.file.lock();
        for (key, value) in data {
            let mut keyvalue_data: Vec<u8> =
                Vec::with_capacity(key.raw_len() + value.len() + U16_SIZE * 2 + U32_SIZE);

            let key_raw_len = key.raw_len() as u16;
            keyvalue_data.extend_from_slice(&key_raw_len.to_le_bytes());
            keyvalue_data.extend_from_slice(key.key_ref());
            keyvalue_data.extend_from_slice(&key.ts().to_le_bytes());

            let value_len = value.len() as u16;
            keyvalue_data.extend_from_slice(&value_len.to_le_bytes());
            keyvalue_data.extend_from_slice(value);

            let checksum = crc32fast::hash(&keyvalue_data);
            keyvalue_data.extend_from_slice(&checksum.to_le_bytes());

            buf.write_all(&keyvalue_data)?;
        }
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut buf = self.file.lock();
        buf.flush()?;
        buf.get_mut().sync_all()?;
        Ok(())
    }
}

fn read_data_u16_with_length<R: Read>(reader: &mut R) -> Result<Vec<u8>> {
    let mut data: Vec<u8> = vec![0; U16_SIZE];

    reader.read_exact(data.as_mut_slice())?;
    let size = u16::from_le_bytes([data[0], data[1]]);
    data.resize(size as usize + U16_SIZE, 0);
    reader.read_exact(&mut data[2..])?;

    Ok(data)
}
