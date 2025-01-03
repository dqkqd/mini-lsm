mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::Bytes;
pub use iterator::BlockIterator;

use crate::key::KeySlice;

#[derive(Default)]
pub(crate) struct BlockKeyPosition<'a> {
    pub key: KeySlice<'a>,
    pub range: (usize, usize),
}

#[derive(Default)]
pub(crate) struct BlockValuePosition {
    pub range: (usize, usize),
}

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
#[derive(Default)]
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let num_elements = self.offsets.len() as u16;

        let mut bytes = self.data.clone();
        bytes.extend(self.offsets.iter().flat_map(|u| u.to_le_bytes()));
        bytes.extend(num_elements.to_le_bytes());

        Bytes::from(bytes)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        Self::decode_checked(data).unwrap_or_default()
    }

    fn decode_checked(data: &[u8]) -> Option<Self> {
        let (data, num_elements) = data.split_last_chunk::<2>()?;

        let num_elements = u16::from_le_bytes(*num_elements);
        let offset_start = data.len().checked_sub(num_elements as usize * 2)?;

        let (data, offsets) = data.split_at_checked(offset_start)?;

        let offsets: Vec<u16> = offsets
            .chunks(2)
            .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
            .collect();

        Some(Self {
            data: Vec::from(data),
            offsets,
        })
    }

    /// Return a pair (key, value) position at specific index.
    pub(crate) fn key_value_at_index(
        &self,
        index: usize,
    ) -> Option<(BlockKeyPosition, BlockValuePosition)> {
        let key = self.key_at_index(index)?;
        let value = self.value_at_offset(key.range.1 as u16)?;
        Some((key, value))
    }

    // Return a key position at specific index.
    pub(crate) fn key_at_index(&self, index: usize) -> Option<BlockKeyPosition> {
        let offset = self.offsets.get(index)?;
        self.key_at_offset(*offset)
    }

    // Return a key position at specific offset.
    pub(crate) fn key_at_offset(&self, offset: u16) -> Option<BlockKeyPosition> {
        let offset = offset as usize;

        let data = self.data[offset..].as_ref();
        let (key_len, key) = data.split_first_chunk::<2>()?;
        let key_len = u16::from_le_bytes(*key_len) as usize;
        let (key, _) = key.split_at_checked(key_len)?;

        Some(BlockKeyPosition {
            key: KeySlice::from_slice(key),
            range: (offset + 2, offset + 2 + key_len),
        })
    }

    // Return a value position at specific offset.
    fn value_at_offset(&self, offset: u16) -> Option<BlockValuePosition> {
        let offset = offset as usize;

        let data = self.data[offset..].as_ref();
        let (value_len, value) = data.split_first_chunk::<2>()?;
        let value_len = u16::from_le_bytes(*value_len) as usize;
        let _ = value.split_at_checked(value_len)?;

        Some(BlockValuePosition {
            range: (offset + 2, offset + 2 + value_len),
        })
    }
}
