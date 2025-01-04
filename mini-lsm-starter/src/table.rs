#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
pub use builder::SsTableBuilder;
use bytes::{Buf, Bytes};
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        // the number of blocks
        let number_blocks = block_meta.len();
        if number_blocks >= u32::MAX as usize {
            panic!("too many block meta");
        }
        buf.extend_from_slice(&(block_meta.len() as u32).to_le_bytes());

        for block in block_meta {
            if block.offset >= u32::MAX as usize {
                panic!("block meta offset must less than u32");
            }
            buf.extend_from_slice(&(block.offset as u32).to_le_bytes());
            buf.extend_from_slice(&(block.first_key.len() as u16).to_le_bytes());
            buf.extend_from_slice(block.first_key.raw_ref());
            buf.extend_from_slice(&(block.last_key.len() as u16).to_le_bytes());
            buf.extend_from_slice(block.last_key.raw_ref());
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let number_blocks = buf.get_u32_le() as usize;

        (0..number_blocks)
            .map(|_| {
                let offset = buf.get_u32_le() as usize;

                let first_key_len = buf.get_u16_le();
                let first_key = buf.copy_to_bytes(first_key_len as usize);

                let last_key_len = buf.get_u16_le();
                let last_key = buf.copy_to_bytes(last_key_len as usize);

                BlockMeta {
                    offset,
                    first_key: KeyBytes::from_bytes(first_key),
                    last_key: KeyBytes::from_bytes(last_key),
                }
            })
            .collect()
    }

    pub(crate) fn first_key(block_meta: &[BlockMeta]) -> KeyBytes {
        block_meta
            .first()
            .map(|meta| meta.first_key.clone())
            .unwrap_or_default()
    }

    pub(crate) fn last_key(block_meta: &[BlockMeta]) -> KeyBytes {
        block_meta
            .last()
            .map(|meta| meta.last_key.clone())
            .unwrap_or_default()
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    ///
    /// The layout was saved like this:
    /// | data block | data block | .... | meta block | bloom filter | meta block offset | bloom filter offset |
    ///
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        // offsets
        let offset = file.read(file.size() - 8, 8)?;
        let block_meta_offset: [u8; 4] = offset[..4].try_into().unwrap();
        let block_meta_offset = u32::from_le_bytes(block_meta_offset) as u64;
        let bloom_filter_offset: [u8; 4] = offset[4..].try_into().unwrap();
        let bloom_filter_offset = u32::from_le_bytes(bloom_filter_offset) as u64;

        // Calculate size based on offsets.
        let block_meta_size = bloom_filter_offset - block_meta_offset;
        let bloom_filter_size = file.size() - 8 - bloom_filter_offset;

        // Read the whole block meta and bloom (instead of reading separately), to avoid disk seek.
        let mut meta = file.read(block_meta_offset, block_meta_size + bloom_filter_size)?;
        let bloom = meta.split_off(block_meta_size as usize);
        let block_meta = meta;

        let bloom = Bloom::decode(&bloom)?;

        let block_meta = BlockMeta::decode_block_meta(Bytes::from(block_meta));
        let first_key = BlockMeta::first_key(&block_meta);
        let last_key = BlockMeta::last_key(&block_meta);

        let sst = Self {
            file,
            block_meta,
            block_meta_offset: block_meta_offset as usize,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: Some(bloom),
            max_ts: u64::MAX,
        };

        Ok(sst)
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let block_meta_offset = self
            .block_meta
            .get(block_idx)
            .map(|b| b.offset)
            .with_context(|| "block idx out of range")?;

        let next_block_meta_offset = self
            .block_meta
            .get(block_idx + 1)
            .map(|b| b.offset)
            .unwrap_or(self.block_meta_offset);

        let block_data = self.file.read(
            block_meta_offset as u64,
            (next_block_meta_offset - block_meta_offset) as u64,
        )?;

        let block = Block::decode(&block_data);

        Ok(Arc::new(block))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        match self.block_cache.as_ref() {
            Some(block_cache) => {
                let block = block_cache
                    .try_get_with((self.sst_id(), block_idx), || self.read_block(block_idx));

                match block {
                    Ok(block) => Ok(block),
                    Err(e) => bail!(format!("cannot get block: {}", e)),
                }
            }
            None => self.read_block(block_idx),
        }
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        // Get the first index where `key >= table.block_meta.first_key`.
        let idx = self
            .block_meta
            .binary_search_by(|meta| meta.first_key.as_key_slice().cmp(&key));

        // Adjust the index again to make sure it does not fall into wrong block.
        // For example, suppose we have 2 blocks like this:
        // Searching for key 5 and key 2 both pointing into block-1.
        //          block-0    block-1
        //          [1, 4]     [6, 8]
        // case 1:          5
        // case 2:    2
        //
        // In the first case, it is correct that the index pointing to block-1.
        // In the second case, it should point into block-0 instead.
        match idx {
            // The key equal first, no need to check here.
            Ok(idx) => idx,

            // The key might not exist.
            Err(mut idx) => {
                if let Some(prev_idx) = idx.checked_sub(1) {
                    let block_meta = &self.block_meta[prev_idx];
                    if block_meta.last_key.as_key_slice() >= key {
                        idx = prev_idx;
                    }
                }
                idx
            }
        }
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}
