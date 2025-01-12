#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use nom::AsBytes;
use parking_lot::Mutex;

use crate::table::validate_checksum;

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
    /// | key_len | key | value_len | value | checksum |
    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        let file = OpenOptions::new().read(true).append(true).open(path)?;

        let mut reader = BufReader::new(&file);
        let mut buf_checksum = [0_u8; 4];

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

            let key = Bytes::from(key_data);
            let value = Bytes::from(value_data);
            skiplist.insert(key, value);
        }

        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_batch(&[(key, value)])
    }

    /// Implement this in week 3, day 5.
    /// Each record in the WAL has the following layout:
    /// | key_len | key | value_len | value | checksum |
    pub fn put_batch(&self, data: &[(&[u8], &[u8])]) -> Result<()> {
        let mut buf = self.file.lock();
        for (key, value) in data {
            let mut keyvalue_data: Vec<u8> =
                Vec::with_capacity(key.len() + value.len() + 2 + 2 + 4);
            add_data_u16(&mut keyvalue_data, key)?;
            add_data_u16(&mut keyvalue_data, value)?;

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

fn add_data_u16<W: Write>(writer: &mut W, data: &[u8]) -> Result<()> {
    let data_len = data.len() as u16;
    writer.write_all(&data_len.to_le_bytes())?;
    writer.write_all(data)?;
    Ok(())
}

fn read_data_u16_with_length<R: Read>(reader: &mut R) -> Result<Vec<u8>> {
    let mut data: Vec<u8> = vec![0, 0];

    reader.read_exact(data.as_mut_slice())?;
    let size = u16::from_le_bytes([data[0], data[1]]);
    data.resize(size as usize + 2, 0);
    reader.read_exact(&mut data[2..])?;

    Ok(data)
}
