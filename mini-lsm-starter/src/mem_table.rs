#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;

use crate::iterators::StorageIterator;
use crate::key::{KeyBytes, KeySlice, TS_DEFAULT};
use crate::table::SsTableBuilder;
use crate::wal::Wal;

/// A basic mem-table based on crossbeam-skiplist.
///
/// An initial implementation of memtable is part of week 1, day 1. It will be incrementally implemented in other
/// chapters of week 1 and week 2.
pub struct MemTable {
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    wal: Option<Wal>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
}

/// Create a bound of `Bytes` from a bound of `&[u8]`.
pub(crate) fn map_bound(bound: Bound<KeySlice>) -> Bound<KeyBytes> {
    match bound {
        Bound::Included(x) => Bound::Included(x.to_key_vec().into_key_bytes()),
        Bound::Excluded(x) => Bound::Excluded(x.to_key_vec().into_key_bytes()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl MemTable {
    /// Create a new mem-table.
    pub fn create(id: usize) -> Self {
        MemTable {
            map: Arc::new(SkipMap::new()),
            wal: None,
            id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Create a new mem-table with WAL
    pub fn create_with_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let wal = Wal::create(path)?;
        Ok(Self {
            map: Arc::new(SkipMap::new()),
            wal: Some(wal),
            id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Create a memtable from WAL
    pub fn recover_from_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let map = SkipMap::new();
        let wal = Wal::recover(path, &map)?;
        let approximated_size: usize = map
            .iter()
            .map(|entry| entry.key().raw_len() + entry.value().len())
            .sum();

        Ok(Self {
            map: Arc::new(map),
            wal: Some(wal),
            id,
            approximate_size: Arc::new(AtomicUsize::new(approximated_size)),
        })
    }

    pub fn for_testing_put_slice(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let key = KeySlice::for_testing_from_slice_no_ts(key);
        self.put(key, value)
    }

    pub fn for_testing_get_slice(&self, key: &[u8]) -> Option<Bytes> {
        self.get(key)
    }

    pub fn for_testing_scan_slice(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> MemTableIterator {
        let lower = match lower {
            Bound::Included(x) => Bound::Included(KeySlice::for_testing_from_slice_no_ts(x)),
            Bound::Excluded(x) => Bound::Excluded(KeySlice::for_testing_from_slice_no_ts(x)),
            Bound::Unbounded => Bound::Unbounded,
        };
        let upper = match upper {
            Bound::Included(x) => Bound::Included(KeySlice::for_testing_from_slice_no_ts(x)),
            Bound::Excluded(x) => Bound::Excluded(KeySlice::for_testing_from_slice_no_ts(x)),
            Bound::Unbounded => Bound::Unbounded,
        };
        self.scan(lower, upper)
    }

    /// Get a value by key.
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        let key = Bytes::from_static(unsafe { std::mem::transmute::<&[u8], &[u8]>(key) });
        let key = KeyBytes::from_bytes_with_ts(key, TS_DEFAULT);
        self.map.get(&key).map(|v| v.value().clone())
    }

    /// Put a key-value pair into the mem-table.
    ///
    /// In week 1, day 1, simply put the key-value pair into the skipmap.
    /// In week 2, day 6, also flush the data to WAL.
    /// In week 3, day 5, modify the function to use the batch API.
    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        if let Some(wal) = &self.wal {
            wal.put(key, value)?;
        }

        let key = key.to_key_vec().into_key_bytes();
        let value = Bytes::copy_from_slice(value);
        let size = key.raw_len() + value.len();

        self.map.insert(key, value);
        self.approximate_size
            .fetch_add(size, std::sync::atomic::Ordering::Relaxed);

        Ok(())
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, _data: &[(KeySlice, &[u8])]) -> Result<()> {
        todo!()
        // if let Some(wal) = &self.wal {
        //     let data: Vec<(&[u8], &[u8])> = data
        //         .iter()
        //         .map(|&(key, value)| (key.raw_ref(), value))
        //         .collect();
        //     wal.put_batch(&data)?;
        // }
        //
        // let mut size = 0;
        // for (key, value) in data {
        //     let key = Bytes::copy_from_slice(key.raw_ref());
        //     let value = Bytes::copy_from_slice(value);
        //     size += key.len() + value.len();
        //     self.map.insert(key, value);
        // }
        //
        // self.approximate_size
        //     .fetch_add(size, std::sync::atomic::Ordering::Relaxed);
        //
        // Ok(())
    }

    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()?;
        }
        Ok(())
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, lower: Bound<KeySlice>, upper: Bound<KeySlice>) -> MemTableIterator {
        let lower = map_bound(lower);
        let upper = map_bound(upper);

        let mut iter = MemTableIteratorBuilder {
            map: self.map.clone(),
            iter_builder: |map| map.range((lower, upper)),
            item: (KeyBytes::new(), Bytes::new()),
            valid: true,
        }
        .build();

        // Call `next`` to get the first value,
        // it is ok to `unwrap` since `next` always return `Ok(())`
        iter.next().unwrap();

        iter
    }

    /// Flush the mem-table to SSTable. Implement in week 1 day 6.
    pub fn flush(&self, builder: &mut SsTableBuilder) -> Result<()> {
        for entry in self.map.iter() {
            let key = entry.key().as_key_slice();
            let value = entry.value();
            builder.add(key, value);
        }
        Ok(())
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Only use this function when closing the database
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

type SkipMapRangeIter<'a> = crossbeam_skiplist::map::Range<
    'a,
    KeyBytes,
    (Bound<KeyBytes>, Bound<KeyBytes>),
    KeyBytes,
    Bytes,
>;

/// An iterator over a range of `SkipMap`. This is a self-referential structure and please refer to week 1, day 2
/// chapter for more information.
///
/// This is part of week 1, day 2.
#[self_referencing]
pub struct MemTableIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `MemTableIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (KeyBytes, Bytes),
    valid: bool,
}

impl StorageIterator for MemTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        self.borrow_item().1.as_ref()
    }

    fn key(&self) -> KeySlice {
        self.borrow_item().0.as_key_slice()
    }

    fn is_valid(&self) -> bool {
        *self.borrow_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.with_mut(|fields| {
            if let Some(item) = fields.iter.next() {
                *fields.item = (item.key().clone(), item.value().clone());
            } else {
                *fields.valid = false;
            }
        });
        Ok(())
    }
}
