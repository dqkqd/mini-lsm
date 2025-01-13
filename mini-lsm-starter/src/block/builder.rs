use crate::{
    key::{KeySlice, KeyVec},
    U16_SIZE, U64_SIZE,
};

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

    /// Add the first key into block.
    /// We don't check key, value size this time, so the operation always succeeds.
    /// The layout should be:
    /// | key len (u16) | key | timestamp | value len (u16) | value |
    fn add_first_key_value(&mut self, key: KeySlice, value: &[u8]) -> bool {
        self.first_key = Some(key.to_key_vec());

        self.offsets.push(self.data.len() as u16);

        let key_len = key.raw_len() as u16;
        self.data.extend_from_slice(&key_len.to_le_bytes());
        self.data.extend_from_slice(key.key_ref());
        self.data.extend_from_slice(&key.ts().to_le_bytes());

        let value_len = value.len() as u16;
        self.data.extend_from_slice(&value_len.to_le_bytes());
        self.data.extend_from_slice(value);

        true
    }

    /// Add key into block with first key prefix encoding.
    /// This operation will be failed if the new size exceed total block size.
    ///
    /// The layout should be:
    /// | key_overlap_len (u16) | rest_key_len and timestamp (u16) | rest_key | timestamp | value len (u16) | value |
    fn add_key_value_prefixed_encoding(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let first_key = self.first_key.as_ref().unwrap();

        let key_overlap_len = shared_prefix_length(first_key.key_ref(), key.key_ref());
        let rest_key = &key.key_ref()[key_overlap_len..];

        let total_len = U16_SIZE + U16_SIZE + rest_key.len() + U64_SIZE + U16_SIZE + value.len();
        if self.data.len() + total_len > self.block_size {
            return false;
        }

        self.offsets.push(self.data.len() as u16);

        let key_overlap_len = key_overlap_len as u16;
        self.data.extend_from_slice(&key_overlap_len.to_le_bytes());
        let rest_key_len = (rest_key.len() + U64_SIZE) as u16;
        self.data.extend_from_slice(&rest_key_len.to_le_bytes());
        self.data.extend_from_slice(rest_key);
        self.data.extend_from_slice(&key.ts().to_le_bytes());

        let value_len = value.len() as u16;
        self.data.extend_from_slice(&value_len.to_le_bytes());
        self.data.extend_from_slice(value);

        true
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
