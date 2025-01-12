#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

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

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        let file = OpenOptions::new().read(true).append(true).open(path)?;

        let mut reader = BufReader::new(&file);
        while let (Ok(key), Ok(value)) = (read_data_u16(&mut reader), read_data_u16(&mut reader)) {
            let key = Bytes::from(key);
            let value = Bytes::from(value);
            skiplist.insert(key, value);
        }

        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut buf = self.file.lock();
        add_data_u16(buf.by_ref(), key)?;
        add_data_u16(buf.by_ref(), value)?;
        Ok(())
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, _data: &[(&[u8], &[u8])]) -> Result<()> {
        unimplemented!()
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

fn read_data_u16<R: Read>(reader: &mut R) -> Result<Vec<u8>> {
    let mut data_len = [0_u8; 2];
    reader.read_exact(&mut data_len)?;
    let data_len = u16::from_le_bytes(data_len);

    let mut data = vec![0_u8; data_len as usize];
    reader.read_exact(&mut data)?;

    Ok(data)
}
