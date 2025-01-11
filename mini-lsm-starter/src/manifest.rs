use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::{fs::File, io::BufReader};

use anyhow::Result;
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

use crate::compact::CompactionTask;

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
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

        let buf = BufReader::new(&file);
        let mut records: Vec<ManifestRecord> = Vec::new();
        let stream = Deserializer::from_reader(buf).into_iter::<ManifestRecord>();
        for record in stream.flatten() {
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
        let bytes = serde_json::to_vec(&record)?;
        self.file.lock().write_all(&bytes)?;
        Ok(())
    }
}
