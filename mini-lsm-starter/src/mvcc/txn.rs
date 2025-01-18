use std::{
    collections::HashSet,
    ops::Bound,
    sync::{atomic::AtomicBool, Arc},
};

use anyhow::{bail, Result};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use parking_lot::Mutex;

use crate::{
    iterators::{two_merge_iterator::TwoMergeIterator, StorageIterator},
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::{LsmStorageInner, WriteBatchRecord},
};

use super::CommittedTxnData;

// TODO: move this out
fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
        Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Write set and read set
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl Transaction {
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if self.committed() {
            panic!("getting from comitted transaction");
        }

        self.record_read(key);

        if let Some(entry) = self.local_storage.get(key) {
            if entry.value().is_empty() {
                Ok(None)
            } else {
                Ok(Some(entry.value().clone()))
            }
        } else {
            self.inner.get_with_ts(key, self.read_ts)
        }
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        if self.committed() {
            panic!("scanning from comitted transaction");
        }

        let mut local_iter = TxnLocalIteratorBuilder {
            map: self.local_storage.clone(),
            iter_builder: |map| map.range((map_bound(lower), map_bound(upper))),
            item: (Bytes::new(), Bytes::new()),
            valid: true,
        }
        .build();
        // Call `next`` to get the first value,
        // it is ok to `unwrap` since `next` always return `Ok(())`
        local_iter.next().unwrap();

        let lsm_iterator = self.inner.scan_with_ts(lower, upper, self.read_ts)?;

        let two_merge_iter = TwoMergeIterator::create(local_iter, lsm_iterator)?;

        TxnIterator::create(self.clone(), two_merge_iter)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        if self.committed() {
            panic!("putting from comitted transaction");
        }

        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));

        self.record_write(key);
    }

    pub fn delete(&self, key: &[u8]) {
        if self.committed() {
            panic!("deleting from comitted transaction");
        }

        self.put(key, &[]);
    }

    pub fn commit(&self) -> Result<()> {
        if self.committed() {
            panic!("comitting from comitted transaction");
        }

        if !self.local_storage.is_empty() {
            let batch: Vec<WriteBatchRecord<Bytes>> = self
                .local_storage
                .iter()
                .map(|entry| {
                    if entry.value().is_empty() {
                        WriteBatchRecord::Del(entry.key().clone())
                    } else {
                        WriteBatchRecord::Put(entry.key().clone(), entry.value().clone())
                    }
                })
                .collect();

            let mvcc = self.inner.mvcc();
            let expected_commit_ts = mvcc.latest_commit_ts() + 1;

            let (read_set, write_set) = match &self.key_hashes {
                Some(key_hashes) => {
                    let key_hashes = key_hashes.lock();
                    (key_hashes.0.clone(), key_hashes.1.clone())
                }
                None => (HashSet::new(), HashSet::new()),
            };
            let watermark = mvcc.watermark();

            {
                let _commit_lock = mvcc.commit_lock.lock();

                let mut committed_txns = mvcc.committed_txns.lock();
                // gc old committed ts transaction
                committed_txns.retain(|&k, _| k >= watermark);

                // no need to verify if this transaction has no write
                if !write_set.is_empty() {
                    let valid_transaction = committed_txns
                        .range(self.read_ts + 1..expected_commit_ts)
                        .all(|(_, committed_txn_data)| {
                            committed_txn_data.key_hashes.is_disjoint(&read_set)
                        });
                    if !valid_transaction {
                        bail!("invalid transaction");
                    }
                }

                let commit_ts = self.inner.write_batch_inner(&batch)?;

                let committed_txn_data = CommittedTxnData {
                    key_hashes: write_set,
                    read_ts: self.read_ts,
                    commit_ts,
                };
                committed_txns.insert(commit_ts, committed_txn_data);
            }
        }

        self.committed
            .store(true, std::sync::atomic::Ordering::SeqCst);

        Ok(())
    }

    fn committed(&self) -> bool {
        self.committed.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn record_read<T: AsRef<[u8]>>(&self, key: T) {
        if let Some(key_hashes) = &self.key_hashes {
            let mut key_hashes = key_hashes.lock();
            key_hashes.0.insert(farmhash::hash32(key.as_ref()));
        }
    }

    fn record_write<T: AsRef<[u8]>>(&self, key: T) {
        if let Some(key_hashes) = &self.key_hashes {
            let mut key_hashes = key_hashes.lock();
            key_hashes.1.insert(farmhash::hash32(key.as_ref()));
        }
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if let Some(mvcc) = self.inner.mvcc.as_ref() {
            let mut ts = mvcc.ts.lock();
            let read_ts = self.read_ts;
            ts.1.remove_reader(read_ts);
        }
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing]
pub struct TxnLocalIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `TxnLocalIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
    valid: bool,
}

impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        self.borrow_item().1.as_ref()
    }

    fn key(&self) -> &[u8] {
        self.borrow_item().0.as_ref()
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

pub struct TxnIterator {
    txn: Arc<Transaction>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub fn create(
        txn: Arc<Transaction>,
        iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
    ) -> Result<Self> {
        Ok(Self { txn, iter })
    }

    fn skip_deleted_keys(&mut self) -> Result<()> {
        while self.is_valid() && self.value().is_empty() {
            self.txn.record_read(self.iter.key());
            self.iter.next()?;
        }
        Ok(())
    }
}

impl StorageIterator for TxnIterator {
    type KeyType<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.txn.record_read(self.iter.key());
        self.iter.next()?;
        self.skip_deleted_keys()?;
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
