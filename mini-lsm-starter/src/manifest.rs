use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;
use std::{fs::File, io::BufReader};

use anyhow::Result;
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::compact::CompactionTask;
use crate::table::validate_checksum;

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
}

impl ManifestRecord {
    /// Record should have format
    /// | len | json | checksum |
    fn to_writer<W: Write>(&self, writer: &mut W) -> Result<()> {
        let record = serde_json::to_vec(self)?;
        let record_len = (record.len() as u16).to_le_bytes();

        let mut data = record_len.to_vec();
        data.extend_from_slice(&record);

        let checksum = crc32fast::hash(&data);
        data.extend_from_slice(&checksum.to_le_bytes());

        writer.write_all(&data)?;
        Ok(())
    }

    /// Record should have format
    /// | len | json | checksum |
    fn from_reader<R: Read>(reader: &mut R) -> Result<Self> {
        let mut data = vec![0; 2];
        reader.read_exact(&mut data[..2])?;

        let record_len = u16::from_le_bytes([data[0], data[1]]);
        data.resize(record_len as usize + 2 + 4, 0);
        reader.read_exact(&mut data[2..])?;

        let data = validate_checksum(&data)?;
        let record: ManifestRecord = serde_json::from_slice(&data[2..])?;

        Ok(record)
    }
}

impl Manifest {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::create(path)?;
        Ok(Self {
            file: Arc::new(Mutex::new(file)),
        })
    }

    pub fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let file = OpenOptions::new().read(true).append(true).open(path)?;

        let mut buf = BufReader::new(&file);
        let mut records: Vec<ManifestRecord> = Vec::new();
        while let Ok(record) = ManifestRecord::from_reader(&mut buf) {
            records.push(record)
        }

        Ok((
            Self {
                file: Arc::new(Mutex::new(file)),
            },
            records,
        ))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, record: ManifestRecord) -> Result<()> {
        let mut bytes = Vec::new();
        record.to_writer(&mut bytes)?;
        self.file.lock().write_all(&bytes)?;
        Ok(())
    }
}
