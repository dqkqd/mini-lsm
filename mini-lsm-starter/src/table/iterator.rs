use std::{
    ops::{Bound, Range},
    sync::Arc,
};

use anyhow::{bail, Context, Result};

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
        lower: Bound<KeySlice>,
        upper: Bound<KeySlice>,
    ) -> Result<Self> {
        // Range from table.
        let first_key = table.first_key.clone();
        let first_key = first_key.as_key_slice();
        let last_key = table.last_key.clone();
        let last_key = last_key.as_key_slice();
        let table_range = Range {
            start: Bound::Included(first_key),
            end: Bound::Included(last_key),
        };

        // Provided range.
        let range = Range {
            start: lower,
            end: upper,
        };
        let range = range_overlap(range, table_range)
            .with_context(|| "provided range does not overlap with table")?;

        let lower_key = match range.start {
            Bound::Included(key) | Bound::Excluded(key) => key,
            Bound::Unbounded => unreachable!("table range should not be unbounded"),
        };
        let mut iter = Self::create_and_seek_to_key(table, lower_key)?;

        // Key not in range, this could be because the range has excluded lower bound.
        if iter.is_valid() && !range_contains(&range, iter.key()) {
            iter.next()?;
        }

        // Check again if the iterator is invalid,
        // Or the key still is not in scan range after calling `next`.
        if !iter.is_valid() || !range_contains(&range, iter.key()) {
            bail!("cannot find correct iter for provided range");
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

fn range_contains(range: &Range<Bound<KeySlice>>, key: KeySlice) -> bool {
    match range.start {
        Bound::Included(k) if key < k => return false,
        Bound::Excluded(k) if key <= k => return false,
        _ => {}
    }
    match range.end {
        Bound::Included(k) if key > k => return false,
        Bound::Excluded(k) if key >= k => return false,
        _ => {}
    }
    true
}

fn range_overlap<'a>(
    lhs: Range<Bound<KeySlice<'a>>>,
    rhs: Range<Bound<KeySlice<'a>>>,
) -> Option<Range<Bound<KeySlice<'a>>>> {
    let lhs = lhs.clone();

    let start = match (lhs.start, rhs.start) {
        (Bound::Unbounded, rhs) => rhs,
        (lhs, Bound::Unbounded) => lhs,
        (Bound::Included(lhs), Bound::Included(rhs)) => Bound::Included(lhs.max(rhs)),
        (Bound::Excluded(lhs), Bound::Excluded(rhs)) => Bound::Excluded(lhs.max(rhs)),
        (Bound::Included(lhs), Bound::Excluded(rhs)) => {
            if lhs > rhs {
                Bound::Included(lhs)
            } else {
                Bound::Excluded(rhs)
            }
        }
        (Bound::Excluded(lhs), Bound::Included(rhs)) => {
            if lhs > rhs {
                Bound::Excluded(lhs)
            } else {
                Bound::Included(rhs)
            }
        }
    };

    let end = match (lhs.end, rhs.end) {
        (Bound::Unbounded, rhs) => rhs,
        (lhs, Bound::Unbounded) => lhs,
        (Bound::Included(lhs), Bound::Included(rhs)) => Bound::Included(lhs.min(rhs)),
        (Bound::Excluded(lhs), Bound::Excluded(rhs)) => Bound::Excluded(lhs.min(rhs)),
        (Bound::Included(lhs), Bound::Excluded(rhs)) => {
            if lhs < rhs {
                Bound::Included(lhs)
            } else {
                Bound::Excluded(rhs)
            }
        }
        (Bound::Excluded(lhs), Bound::Included(rhs)) => {
            if lhs < rhs {
                Bound::Excluded(lhs)
            } else {
                Bound::Included(rhs)
            }
        }
    };

    match (start, end) {
        (Bound::Unbounded, _) => Some(Range { start, end }),
        (_, Bound::Unbounded) => Some(Range { start, end }),
        (Bound::Included(l), Bound::Included(r)) if l <= r => Some(Range { start, end }),
        (Bound::Included(l), Bound::Excluded(r)) if l < r => Some(Range { start, end }),
        (Bound::Excluded(l), Bound::Included(r)) if l < r => Some(Range { start, end }),
        (Bound::Excluded(l), Bound::Excluded(r)) if l < r => Some(Range { start, end }),
        _ => None,
    }
}
