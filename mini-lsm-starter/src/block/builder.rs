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
    /// The first key in the block, it can be None if the block is empty.
    first_key: Option<KeyVec>,
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
            first_key: None,
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        if self.first_key.is_none() {
            self.add_first_key_value(key, value)
        } else {
            self.add_key_value_prefixed_encoding(key, value)
        }
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.first_key.is_none()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
            first_key: self.first_key,
        }
    }

    /// Add the first key into block. This operation is always return true.
    /// The layout should be:
    /// | key len (u16) | key | value len (u16) | value |
    fn add_first_key_value(&mut self, key: KeySlice, value: &[u8]) -> bool {
        self.offsets.push(self.data.len() as u16);
        self.first_key = Some(key.to_key_vec());
        self.add_data_u16(key.raw_ref());
        self.add_data_u16(value);

        true
    }

    /// Add key into block with first key prefix encoding.
    /// This operation will be failed if the new size exceed total block size.
    ///
    /// The layout should be:
    /// | key_overlap_len (u16) | rest_key_len (u16) | key (rest_key_len) | value len (u16) | value |
    fn add_key_value_prefixed_encoding(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let first_key = self.first_key.as_ref().unwrap();

        let key_overlap_len = shared_prefix_length(first_key.raw_ref(), key.raw_ref());
        let rest_key = &key.raw_ref()[key_overlap_len..];

        let total_len = 2 + 2 + rest_key.len() + 2 + value.len();
        if self.data.len() + total_len > self.block_size {
            return false;
        }
        let key_overlap_len = key_overlap_len as u16;

        self.offsets.push(self.data.len() as u16);
        self.data.extend_from_slice(&key_overlap_len.to_le_bytes());
        self.add_data_u16(rest_key);
        self.add_data_u16(value);

        true
    }

    /// Add a variable length `data` into data.
    /// This is a helper function for adding key and value with their length.
    fn add_data_u16(&mut self, data: &[u8]) {
        let data_len = data.len() as u16;
        self.data.extend_from_slice(&data_len.to_le_bytes());
        self.data.extend_from_slice(data);
    }
}

/// Length of the comment prefix.
/// This is basically the first position i where lhs[i] != rhs[i]
fn shared_prefix_length(lhs: &[u8], rhs: &[u8]) -> usize {
    lhs.iter()
        .zip(rhs)
        .position(|(l, r)| l != r)
        .unwrap_or_else(|| rhs.len().min(lhs.len()))
}
