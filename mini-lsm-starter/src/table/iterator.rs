use std::{ops::Bound, sync::Arc};

use anyhow::{bail, Result};

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let blk_idx = 0;
        let block = table.read_block_cached(blk_idx)?;
        let blk_iter = BlockIterator::create_and_seek_to_first(block);

        Ok(SsTableIterator {
            table,
            blk_iter,
            blk_idx,
        })
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        // Only load block from disk if block id is different
        if self.blk_idx != 0 {
            self.seek_to_block_idx(0)?;
        } else {
            self.blk_iter.seek_to_first();
        }
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let blk_idx = table.find_block_idx(key);
        let block = table.read_block_cached(blk_idx)?;
        let blk_iter = BlockIterator::create_and_seek_to_key(block, key);

        Ok(SsTableIterator {
            table,
            blk_iter,
            blk_idx,
        })
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let blk_idx = self.table.find_block_idx(key);

        // Do not load
        if blk_idx >= self.table.num_of_blocks() {
            return Ok(());
        }

        let block = self.table.read_block_cached(blk_idx)?;
        self.blk_iter = BlockIterator::create_and_seek_to_key(block, key);
        self.blk_idx = blk_idx;

        Ok(())
    }

    pub(crate) fn create_with_bound(
        table: Arc<SsTable>,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<Self> {
        let first_key = table.first_key.clone();
        let first_key = first_key.as_key_slice();

        let last_key = table.last_key.clone();
        let last_key = last_key.as_key_slice();

        // Adjust lower bound with the first key.
        let lower = lower.map(KeySlice::from_slice);
        let lower = match lower {
            Bound::Unbounded => Bound::Included(first_key),
            Bound::Included(key) => Bound::Included(key.max(first_key)),
            Bound::Excluded(key) if key < first_key => Bound::Included(first_key),
            b => b,
        };

        // Adjust upper bound with the last key.
        let upper = upper.map(KeySlice::from_slice);
        let upper = match upper {
            Bound::Unbounded => Bound::Included(last_key),
            Bound::Included(key) => Bound::Included(key.min(last_key)),
            Bound::Excluded(key) if key > first_key => Bound::Included(last_key),
            b => b,
        };

        let lower_key = match lower {
            Bound::Included(key) | Bound::Excluded(key) => key,
            Bound::Unbounded => unreachable!("unbounded has been filtered out already"),
        };
        let upper_key = match upper {
            Bound::Included(key) | Bound::Excluded(key) => key,
            Bound::Unbounded => unreachable!("unbounded has been filtered out already"),
        };
        if lower_key > upper_key {
            bail!("lower bound greater than than upper bound");
        }
        if lower_key == upper_key
            && !matches!((lower, upper), (Bound::Included(_), Bound::Included(_)))
        {
            bail!("lower bound equals upper bound but one of the bound is not `Bound::Included`");
        }

        let mut iter = Self::create_and_seek_to_key(table, lower_key)?;

        // Exclude lower key.
        if matches!(lower, Bound::Excluded(_)) && iter.is_valid() && iter.key() == lower_key {
            iter.next()?;
        }
        // This iterator might be invalid after calling `next`.
        if !iter.is_valid() {
            bail!("invalid iterator");
        }

        // invalidate this iterator if it exceeds upper boundary.
        if matches!(upper, Bound::Excluded(_)) && iter.is_valid() && iter.key() >= upper_key {
            bail!("invalid range");
        }
        if matches!(upper, Bound::Included(_)) && iter.is_valid() && iter.key() > upper_key {
            bail!("invalid range");
        }

        Ok(iter)
    }

    /// Seek to the first key at block index.
    fn seek_to_block_idx(&mut self, blk_idx: usize) -> Result<()> {
        let block = self.table.read_block_cached(blk_idx)?;
        self.blk_iter = BlockIterator::create_and_seek_to_first(block);
        self.blk_idx = blk_idx;
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();

        if !self.is_valid() {
            // It is ok to fail, don't check error here.
            let _ = self.seek_to_block_idx(self.blk_idx + 1);
        }

        Ok(())
    }
}
