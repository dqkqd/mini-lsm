mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::Bytes;
pub use iterator::BlockIterator;

use crate::key::KeyVec;

#[derive(Default)]
pub(crate) struct BlockKey {
    pub key: KeyVec,
    pub range: (usize, usize),
}

#[derive(Default)]
pub(crate) struct BlockData<'a> {
    data: &'a [u8],
    range: (usize, usize),
}

impl From<BlockData<'_>> for BlockKey {
    fn from(block_data: BlockData<'_>) -> Self {
        Self {
            key: KeyVec::from_vec(block_data.data.to_vec()),
            range: block_data.range,
        }
    }
}

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
#[derive(Default)]
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
    pub(crate) first_key: Option<KeyVec>,
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
        let num_elements = u16_at(data, (data.len() - 2) as u16)?;

        let data = &data[..data.len() - 2];
        let offset_start = data.len().checked_sub(num_elements as usize * 2)?;
        let (data, offsets) = data.split_at_checked(offset_start)?;

        let offsets: Vec<u16> = offsets
            .chunks(2)
            .filter_map(|chunk| u16_at(chunk, 0))
            .collect();

        let first_key = data_at(data, 0)
            .map(BlockKey::from)
            .map(|block_key| block_key.key);

        let block = Self {
            data: Vec::from(data),
            offsets,
            first_key,
        };

        Some(block)
    }

    /// Return a pair (key, value) position at specific index.
    pub(crate) fn key_value_at_index(&self, index: usize) -> Option<(BlockKey, BlockData)> {
        let offset = self.offsets.get(index)?;
        let key = self.key_at_offset(*offset)?;
        let value = self.data_at_offset(key.range.1 as u16)?;
        Some((key, value))
    }

    // Return a key position at specific offset.
    pub(crate) fn key_at_offset(&self, offset: u16) -> Option<BlockKey> {
        if offset == 0 {
            // The first key is saved same as normal data.
            self.data_at_offset(offset).map(BlockKey::from)
        } else {
            // Other keys is saved using prefixed encoding
            // | key_overlap_len (u16) | rest_key_len (u16) | key (rest_key_len)
            let key_overlap_len = self.sizehint_at_offset(offset)?;
            let rest_key = self.data_at_offset(offset + 2)?;

            let first_key = self.first_key.as_ref()?;

            let key = [
                &first_key.raw_ref()[..key_overlap_len as usize],
                rest_key.data,
            ]
            .concat();

            Some(BlockKey {
                key: KeyVec::from_vec(key),
                range: rest_key.range,
            })
        }
    }

    /// Get variable length data at offset.
    /// | data len (u16) | data |
    fn data_at_offset(&self, offset: u16) -> Option<BlockData> {
        data_at(&self.data, offset)
    }

    // Get 2 bytes length (u16) at offset.
    fn sizehint_at_offset(&self, offset: u16) -> Option<u16> {
        u16_at(&self.data, offset)
    }
}

fn u16_at(data: &[u8], at: u16) -> Option<u16> {
    let offset = at as usize;
    let data = &data[offset..];
    let (data_len, _) = data.split_first_chunk::<2>()?;
    Some(u16::from_le_bytes(*data_len))
}

fn data_at(data: &[u8], at: u16) -> Option<BlockData> {
    let data_len = u16_at(data, at)?;
    let from = (at + 2) as usize;
    let to = (at + 2 + data_len) as usize;
    Some(BlockData {
        data: &data[from..to],
        range: (from, to),
    })
}
