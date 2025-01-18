#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::{Buf, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

use crate::key::{KeyBytes, KeySlice};
use crate::table::validate_checksum;
use crate::{U16_SIZE, U32_SIZE};

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
    /// All records in the WAL file is stored as batch with the following layout:
    /// | HEADER     |                 BODY                                    | FOOTER  |
    /// | batch size | key_len | key | timestamp | value_len | value | ... | checksum|
    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        let file = OpenOptions::new().read(true).append(true).open(path)?;

        let mut reader = BufReader::new(&file);
        loop {
            if Self::recover_one_batch(&mut reader, skiplist).is_err() {
                break;
            }
        }

        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    /// One batch has the following layout:
    /// | HEADER     |                 BODY                                    | FOOTER  |
    /// | batch size | key_len | key | timestamp | value_len | value | ... | checksum|
    fn recover_one_batch<R: Read>(
        reader: &mut R,
        skiplist: &SkipMap<KeyBytes, Bytes>,
    ) -> Result<()> {
        let mut batch_size = [0; U32_SIZE];
        reader.read_exact(&mut batch_size)?;
        let batch_size = u32::from_le_bytes(batch_size);

        let mut body_and_checksum = vec![0; batch_size as usize + U32_SIZE];
        reader.read_exact(&mut body_and_checksum)?;
        let mut body = validate_checksum(&body_and_checksum)?;

        while body.has_remaining() {
            let key_len = body.get_u16_le();
            let key = body.copy_to_bytes(key_len as usize);
            let ts = body.get_u64_le();

            let value_len = body.get_u16_le();
            let value = body.copy_to_bytes(value_len as usize);

            let key = KeyBytes::from_bytes_with_ts(key, ts);
            skiplist.insert(key, value);
        }

        Ok(())
    }

    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        self.put_batch(&[(key, value)])
    }

    /// Implement this in week 3, day 5.
    /// The whole batch has this layout:
    /// | HEADER     |                 BODY                                    | FOOTER  |
    /// | batch size | key_len | key | timestamp | value_len | value | ... | checksum|
    pub fn put_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        let mut body = Vec::new();

        for (key, value) in data {
            let key_len = key.key_len() as u16;
            body.extend_from_slice(&key_len.to_le_bytes());
            body.extend_from_slice(key.key_ref());
            body.extend_from_slice(&key.ts().to_le_bytes());

            let value_len = value.len() as u16;
            body.extend_from_slice(&value_len.to_le_bytes());
            body.extend_from_slice(value);
        }

        let mut buf = self.file.lock();

        let batch_size = body.len() as u32;
        buf.write_all(&batch_size.to_le_bytes())?;

        buf.write_all(&body)?;

        let checksum = crc32fast::hash(&body);
        buf.write_all(&checksum.to_le_bytes())?;

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
