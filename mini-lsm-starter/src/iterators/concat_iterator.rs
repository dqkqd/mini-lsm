use std::{ops::Bound, sync::Arc};

use anyhow::Result;

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        if let Some(table) = sstables.first() {
            if let Ok(iter) = SsTableIterator::create_and_seek_to_first(table.clone()) {
                if iter.is_valid() {
                    return Ok(Self {
                        current: Some(iter),
                        next_sst_idx: 1,
                        sstables,
                    });
                }
            }
        }

        // Cannot find iterator.
        Ok(Self {
            current: None,
            next_sst_idx: sstables.len(),
            sstables,
        })
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        for (sst_idx, table) in sstables.iter().enumerate() {
            if let Ok(iter) = SsTableIterator::create_and_seek_to_key(table.clone(), key) {
                if iter.is_valid() {
                    return Ok(Self {
                        current: Some(iter),
                        next_sst_idx: sst_idx + 1,
                        sstables,
                    });
                }
            }
        }

        // Cannot find iterator.
        Ok(Self {
            current: None,
            next_sst_idx: sstables.len(),
            sstables,
        })
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current
            .as_ref()
            .map(|current| current.key())
            .unwrap_or_default()
    }

    fn value(&self) -> &[u8] {
        self.current
            .as_ref()
            .map(|current| current.value())
            .unwrap_or_default()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some() && self.next_sst_idx <= self.sstables.len()
    }

    fn next(&mut self) -> Result<()> {
        if let Some(iter) = self.current.as_mut() {
            iter.next()?;

            // Seek to the next iterator if the current one is invalid.
            if !iter.is_valid() {
                if let Some(table) = self.sstables.get(self.next_sst_idx) {
                    *iter = SsTableIterator::create_and_seek_to_first(table.clone())?
                };
                self.next_sst_idx += 1;
            }
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        if self.current.is_some() {
            1
        } else {
            0
        }
    }
}
