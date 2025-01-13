pub mod block;
pub mod compact;
pub mod debug;
pub mod iterators;
pub mod key;
pub mod lsm_iterator;
pub mod lsm_storage;
pub mod manifest;
pub mod mem_table;
pub mod mvcc;
pub mod table;
pub mod wal;

const U16_SIZE: usize = std::mem::size_of::<u16>();
const U32_SIZE: usize = std::mem::size_of::<u32>();
const U64_SIZE: usize = std::mem::size_of::<u64>();

#[cfg(test)]
mod tests;
