use std::sync::Arc;

use anyhow::Result;

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
        let block = table.read_block(blk_idx)?;
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
        let block = table.read_block(blk_idx)?;
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

        let block = self.table.read_block(blk_idx)?;
        self.blk_iter = BlockIterator::create_and_seek_to_key(block, key);
        self.blk_idx = blk_idx;

        Ok(())
    }

    /// Seek to the first key at block index.
    fn seek_to_block_idx(&mut self, blk_idx: usize) -> Result<()> {
        let block = self.table.read_block(blk_idx)?;
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
