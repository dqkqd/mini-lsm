use std::{cmp::Ordering, sync::Arc};

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        let (from, to) = self.value_range;
        &self.block.data[from..to]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key().is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to_index(0);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.seek_to_index(self.idx + 1);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let position = self.block.offsets.binary_search_by(|offset| {
            match self.block.key_at_offset(*offset) {
                Some(key_pos) => key_pos.key.cmp(&key),
                // If there are no key found at this position,
                // then we have stepped outside block's data boundary,
                // in such cases we can assume that it is always greater.
                None => Ordering::Greater,
            }
        });

        let idx = match position {
            Ok(idx) => idx,
            Err(idx) => idx,
        };
        self.seek_to_index(idx);
    }

    fn seek_to_index(&mut self, index: usize) {
        let (key_pos, value_pos) = self.block.key_value_at_index(index).unwrap_or_default();

        self.idx = index;
        self.key = key_pos.key.to_key_vec();
        if self.idx == 0 {
            self.first_key = key_pos.key.to_key_vec();
        }
        self.value_range = value_pos.range;
    }
}
