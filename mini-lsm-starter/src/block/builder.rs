use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    // TODO:: can we make this u16?
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        if block_size >= u16::MAX as usize {
            panic!("do not support large block size");
        }
        BlockBuilder {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let key_size = key.len();
        let value_size = value.len();

        // Entry size includes:
        // 2 bytes: key size
        // key_size bytes: the key itself
        // 2 bytes: value size
        // value_size: the value itself
        let entry_size = 2 + key_size + 2 + value_size;
        if entry_size >= u16::MAX as usize {
            return false;
        }

        let key_size = key_size as u16;
        let value_size = value_size as u16;

        // This is the first key
        if self.is_empty() {
            self.first_key = key.to_key_vec();
        } else {
            // Check if adding this entry will exceed `block_size`.
            //
            // Total block size includes:
            // - self.data and new data
            // - self.offsets and new offset
            // - number of elements
            let total_block_size =
                (self.data.len() + entry_size) + (self.offsets.len() + 1) * 2 + 2;
            if total_block_size >= self.block_size {
                return false;
            }
        }

        self.offsets.push(self.data.len() as u16);
        self.data.extend_from_slice(&key_size.to_le_bytes());
        self.data.extend_from_slice(key.raw_ref());
        self.data.extend_from_slice(&value_size.to_le_bytes());
        self.data.extend_from_slice(value);

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
