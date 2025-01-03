use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Result};

use super::{BlockMeta, SsTable};
use crate::{block::BlockBuilder, key::KeySlice, lsm_storage::BlockCache, table::FileObject};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        let builder = BlockBuilder::new(block_size);
        Self {
            builder,
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if !self.builder.add(key, value) {
            self.split();
            if !self.builder.add(key, value) {
                panic!("cannot add new key after splitting builder");
            }
        }
    }

    /// Split current block and allocate a new one to accept keys.
    fn split(&mut self) {
        // Take the current `self.builder` out.
        let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));

        let block = builder.build();

        let offset = self.data.len();
        let first_key = block
            .key_at_index(0)
            .unwrap_or_default()
            .key
            .to_key_vec()
            .into_key_bytes();
        let last_key = block
            .offsets
            .last()
            .and_then(|offset| block.key_at_offset(*offset))
            .unwrap_or_default()
            .key
            .to_key_vec()
            .into_key_bytes();

        self.meta.push(BlockMeta {
            offset,
            first_key,
            last_key,
        });

        self.data.extend_from_slice(block.encode().as_ref());
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        // split current block to get the latest data.
        self.split();

        let first_key = BlockMeta::first_key(&self.meta);
        let last_key = BlockMeta::last_key(&self.meta);

        let block_meta_offset = self.data.len();
        if block_meta_offset > u32::MAX as usize {
            bail!("block meta offset too large");
        }

        // data block
        let mut data = self.data;
        // block meta
        BlockMeta::encode_block_meta(&self.meta, &mut data);
        // block meta offset
        data.extend_from_slice(&(block_meta_offset as u32).to_le_bytes());

        // save to file
        let file = FileObject::create(path.as_ref(), data)?;

        let sst = SsTable {
            file,
            block_meta: self.meta,
            block_meta_offset,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: None,
            max_ts: u64::MAX,
        };

        Ok(sst)
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
