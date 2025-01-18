#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::collections::HashMap;
use std::fs::{self, File};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::{KeySlice, TS_MIN, TS_RANGE_BEGIN, TS_RANGE_END};
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::{MemTable, MemTableIterator};
use crate::mvcc::txn::{Transaction, TxnIterator};
use crate::mvcc::LsmMvccInner;
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

type MemtablesOrdered<'a> = std::iter::Chain<
    std::iter::Once<&'a std::sync::Arc<MemTable>>,
    std::slice::Iter<'a, std::sync::Arc<MemTable>>,
>;

const TOMBSTONE: [u8; 0] = [];

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    /// SST objects.
    pub levels: Vec<(usize, Vec<usize>)>,
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

enum RecoveryState {
    Old(Manifest, Vec<ManifestRecord>),
    New(Manifest),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }

    fn ordered_memtables(&self) -> MemtablesOrdered<'_> {
        // Search from memtable firsts, and then others `imm_memtables`.
        std::iter::once(&self.memtable).chain(&self.imm_memtables)
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        if !self.inner.options.enable_wal {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
            while !self.inner.state.read().imm_memtables.is_empty() {
                self.inner.force_flush_next_imm_memtable()?;
            }
        } else {
            self.sync()?;
        }
        self.flush_notifier.send(())?;
        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<Arc<Transaction>> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        // Do not check for error, in case the folder is already created.
        let _ = fs::create_dir_all(path);
        let state = LsmStorageState::create(&options);

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let mut storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1024)),
            next_sst_id: AtomicUsize::new(1),
            compaction_controller,
            manifest: None,
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        let (state, manifest) = storage.recover()?;

        let mut max_ts = TS_MIN;
        for memtable in state.ordered_memtables() {
            let mut iter = memtable.scan(Bound::Unbounded, Bound::Unbounded);
            while iter.is_valid() {
                let key = iter.key();
                max_ts = max_ts.max(key.ts());
                iter.next()?;
            }
        }
        if max_ts == TS_MIN {
            for table in state.sstables.values() {
                max_ts = max_ts.max(table.max_ts())
            }
        }
        let mvcc = LsmMvccInner::new(max_ts);

        storage.state = Arc::new(RwLock::new(Arc::new(state)));
        storage.manifest = Some(manifest);
        storage.mvcc = Some(mvcc);

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        let state = self.state.read();
        state.memtable.sync_wal()?;
        for memtable in &state.imm_memtables {
            memtable.sync_wal()?;
        }
        Ok(())
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    pub fn get(self: &Arc<Self>, key: &[u8]) -> Result<Option<Bytes>> {
        let mvcc = self.mvcc.as_ref().with_context(|| "mvcc is not enabled")?;
        let txn = mvcc.new_txn(self.clone(), self.options.serializable);
        txn.get(key)
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get_with_ts(&self, key: &[u8], ts: u64) -> Result<Option<Bytes>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        let keyhash = farmhash::fingerprint32(key);
        let lower = Bound::Included(KeySlice::from_slice(key, ts));
        let upper = Bound::Included(KeySlice::from_slice(key, TS_RANGE_END));

        let memtable_iters: Vec<Box<MemTableIterator>> = snapshot
            .ordered_memtables()
            .map(|iter| iter.scan(lower, upper))
            .map(Box::new)
            .collect();

        let l0_iters: Vec<Box<SsTableIterator>> = snapshot
            .l0_sstables
            .iter()
            .filter_map(|sst_id| snapshot.sstables.get(sst_id).cloned())
            // Use bloom filter.
            .filter(|table| {
                table
                    .bloom
                    .as_ref()
                    .is_some_and(|bloom| bloom.may_contain(keyhash))
            })
            .filter_map(|table| SsTableIterator::create_with_bound(table, lower, upper).ok())
            .map(Box::new)
            .collect();

        let memtable_and_l0_iters = TwoMergeIterator::create(
            MergeIterator::create(memtable_iters),
            MergeIterator::create(l0_iters),
        )?;

        let leveled_iters: Vec<Box<SstConcatIterator>> = snapshot
            .levels
            .iter()
            .filter_map(|(_, sst_ids)| {
                let sstables: Vec<Arc<SsTable>> = sst_ids
                    .iter()
                    .filter_map(|sst_id| snapshot.sstables.get(sst_id).cloned())
                    // Use bloom filter.
                    .filter(|table| {
                        table
                            .bloom
                            .as_ref()
                            .is_some_and(|bloom| bloom.may_contain(keyhash))
                    })
                    .collect();
                SstConcatIterator::create_with_bound(sstables, lower, upper).ok()
            })
            .map(Box::new)
            .collect();
        let leveled_iters = MergeIterator::create(leveled_iters);

        let iters = TwoMergeIterator::create(memtable_and_l0_iters, leveled_iters)?;
        let lsm_iterator = LsmIterator::new(iters, lower, upper, ts)?;

        let mut value = None;
        if lsm_iterator.is_valid() && lsm_iterator.key() == key {
            value = Some(Bytes::copy_from_slice(lsm_iterator.value()))
        }
        // Filter out tombstone value.
        value = value.filter(|value| !value.is_empty());

        Ok(value)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        let mvcc = self
            .mvcc
            .as_ref()
            .with_context(|| "mvcc is not initialized")?;

        let _write_lock = mvcc.write_lock.lock();
        let ts = mvcc.latest_commit_ts() + 1;

        let memtable_reaches_capacity = {
            let state = self.state.read();
            let data: Vec<(KeySlice, &[u8])> = batch
                .iter()
                .map(|record| match record {
                    WriteBatchRecord::Put(key, value) => {
                        (KeySlice::from_slice(key.as_ref(), ts), value.as_ref())
                    }
                    WriteBatchRecord::Del(key) => {
                        (KeySlice::from_slice(key.as_ref(), ts), TOMBSTONE.as_ref())
                    }
                })
                .collect();
            for (key, value) in data {
                state.memtable.put(key, value)?;
            }
            // state.memtable.put_batch(&data)?;
            state.memtable.approximate_size() >= self.options.target_sst_size
        };

        if memtable_reaches_capacity {
            let state_lock = self.state_lock.lock();
            if self.state.read().memtable.approximate_size() >= self.options.target_sst_size {
                self.force_freeze_memtable(&state_lock)?;
            }
        }

        mvcc.update_commit_ts(ts);

        Ok(())
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Put(key, value)])
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Del(key)])
    }

    pub(crate) fn path_of_manifest(&self) -> PathBuf {
        self.path.join("MANIFEST")
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let id = self.next_sst_id();
        let memtable = self.new_memtable(id)?;

        {
            let mut state = self.state.write();

            let mut new_state = state.as_ref().clone();
            let memtable = std::mem::replace(&mut new_state.memtable, Arc::new(memtable));
            new_state.imm_memtables.insert(0, memtable);

            *state = Arc::new(new_state);
        }

        if let Some(manifest) = &self.manifest {
            manifest.add_record(_state_lock_observer, ManifestRecord::NewMemtable(id))?;
        }

        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let memtable = {
            let guard = self.state.read();
            guard.imm_memtables.last().cloned()
        };

        if let Some(memtable) = memtable {
            let mut builder = SsTableBuilder::new(self.options.block_size);
            memtable.flush(&mut builder)?;
            let sst_id = memtable.id();
            let path = self.path_of_sst(sst_id);
            let sstable = builder.build(sst_id, Some(self.block_cache.clone()), path)?;

            {
                let _state_lock = self.state_lock.lock();
                let mut state = self.state.write();
                let mut new_state = state.as_ref().clone();

                new_state.imm_memtables.pop();

                if self.compaction_controller.flush_to_l0() {
                    new_state.l0_sstables.insert(0, sst_id);
                } else {
                    new_state.levels.insert(0, (sst_id, vec![sst_id]));
                }
                new_state.sstables.insert(sst_id, Arc::new(sstable));

                *state = Arc::new(new_state);
            }

            // record
            if let Some(manifest) = self.manifest.as_ref() {
                self.sync_dir()?;
                manifest.add_record(
                    &self.state_lock.lock(),
                    ManifestRecord::Flush(memtable.id()),
                )?;
            }
        }

        Ok(())
    }

    pub fn new_txn(self: &Arc<Self>) -> Result<Arc<Transaction>> {
        let mvcc = self.mvcc.as_ref().with_context(|| "mvcc is not enabled")?;
        let txn = mvcc.new_txn(self.clone(), self.options.serializable);
        Ok(txn)
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        let mvcc = self.mvcc.as_ref().with_context(|| "mvcc is not enabled")?;
        let txn = mvcc.new_txn(self.clone(), self.options.serializable);
        txn.scan(lower, upper)
    }

    /// Create an iterator over a range of keys.
    pub fn scan_with_ts(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        ts: u64,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        let lower = match lower {
            Bound::Included(key) => Bound::Included(KeySlice::from_slice(key, ts)),
            Bound::Excluded(key) => Bound::Excluded(KeySlice::from_slice(key, TS_RANGE_END)),
            Bound::Unbounded => Bound::Unbounded,
        };
        let upper = match upper {
            Bound::Included(key) => Bound::Included(KeySlice::from_slice(key, TS_RANGE_END)),
            Bound::Excluded(key) => Bound::Excluded(KeySlice::from_slice(key, TS_RANGE_BEGIN)),
            Bound::Unbounded => Bound::Unbounded,
        };

        let memtable_iters: Vec<Box<MemTableIterator>> = snapshot
            .ordered_memtables()
            .map(|iter| iter.scan(lower, upper))
            .map(Box::new)
            .collect();

        let l0_iters: Vec<Box<SsTableIterator>> = snapshot
            .l0_sstables
            .iter()
            .filter_map(|sst_id| snapshot.sstables.get(sst_id).cloned())
            .filter_map(|table| SsTableIterator::create_with_bound(table, lower, upper).ok())
            .map(Box::new)
            .collect();

        let memtable_and_l0_iters = TwoMergeIterator::create(
            MergeIterator::create(memtable_iters),
            MergeIterator::create(l0_iters),
        )?;

        let leveled_iters: Vec<Box<SstConcatIterator>> = snapshot
            .levels
            .iter()
            .filter_map(|(_, sst_ids)| {
                let sstables: Vec<Arc<SsTable>> = sst_ids
                    .iter()
                    .filter_map(|sst_id| snapshot.sstables.get(sst_id).cloned())
                    .collect();
                SstConcatIterator::create_with_bound(sstables, lower, upper).ok()
            })
            .map(Box::new)
            .collect();
        let leveled_iters = MergeIterator::create(leveled_iters);

        let iters = TwoMergeIterator::create(memtable_and_l0_iters, leveled_iters)?;
        let lsm_iterator = LsmIterator::new(iters, lower, upper, ts)?;
        let fused_iterator = FusedIterator::new(lsm_iterator);

        Ok(fused_iterator)
    }

    fn new_memtable(&self, id: usize) -> Result<MemTable> {
        let memtable = if self.options.enable_wal {
            let path = self.path_of_wal(id);
            MemTable::create_with_wal(id, path)?
        } else {
            MemTable::create(id)
        };

        Ok(memtable)
    }

    fn recovery_state(&self) -> Result<RecoveryState> {
        let manifest_path = self.path_of_manifest();
        let recovery_state = match Manifest::recover(&manifest_path) {
            Ok((manifest, records)) if records.is_empty() => RecoveryState::New(manifest),
            Ok((manifest, records)) => RecoveryState::Old(manifest, records),
            Err(_) => Manifest::create(&manifest_path).map(RecoveryState::New)?,
        };
        Ok(recovery_state)
    }

    fn recover(&mut self) -> Result<(LsmStorageState, Manifest)> {
        let recovery_state = self.recovery_state()?;

        match recovery_state {
            RecoveryState::New(manifest) => {
                let mut state = LsmStorageState::create(&self.options);

                // Need to init new memtable and add record to manifest
                let memtable = self.new_memtable(state.memtable.id())?;
                manifest.add_record_when_init(ManifestRecord::NewMemtable(memtable.id()))?;

                state.memtable = Arc::new(memtable);

                Ok((state, manifest))
            }

            RecoveryState::Old(manifest, records) => {
                let mut state = self.recover_from_manifest(records);

                // Update sstables by loading from disk.
                for sst_id in state
                    .levels
                    .iter()
                    .flat_map(|(_, sst_ids)| sst_ids)
                    .chain(&state.l0_sstables)
                    .cloned()
                {
                    let path = self.path_of_sst(sst_id);
                    let file_object = FileObject::open(&path)?;
                    let table = SsTable::open(sst_id, None, file_object)?;
                    state.sstables.insert(sst_id, Arc::new(table));
                }

                // Update lower level ordered by `first_key`.
                for (_, sst_ids) in state.levels.iter_mut() {
                    sst_ids.sort_unstable_by_key(|sst_id| {
                        let table = state
                            .sstables
                            .get(sst_id)
                            .unwrap_or_else(|| panic!("cannot read sstable for {sst_id}"));
                        table.first_key()
                    });
                }

                // Update `next_sst_id`.
                let max_memtable_id = state
                    .ordered_memtables()
                    .map(|mem| mem.id())
                    .max()
                    .unwrap_or_default();
                let max_sst_id = state
                    .levels
                    .iter()
                    .flat_map(|(_, sst_ids)| sst_ids)
                    .chain(&state.l0_sstables)
                    .max()
                    .cloned()
                    .unwrap_or_default();
                self.next_sst_id.store(
                    max_sst_id.max(max_memtable_id) + 1,
                    std::sync::atomic::Ordering::SeqCst,
                );

                // Recover immutable memtables from wal.
                if self.options.enable_wal {
                    // In the recovery process, memtables are not created with WAL.
                    for memtable in state.imm_memtables.iter_mut() {
                        let id = memtable.id();
                        let path = self.path_of_wal(id);
                        let new_memtable = match MemTable::recover_from_wal(id, &path) {
                            Ok(memtable) => memtable,
                            Err(_) => MemTable::create_with_wal(id, &path)?,
                        };
                        *memtable = Arc::new(new_memtable);
                    }
                }

                // Recover memtable.
                // When loading memtables from manifest records, we only stored immutable memtables.
                // To recover memtable, we take one memtable from immutable memtables.
                // We create a new one in case immutable memtables is emptied (all memtables are flushed).
                if state.imm_memtables.is_empty() {
                    let memtable = self.new_memtable(self.next_sst_id())?;
                    manifest.add_record_when_init(ManifestRecord::NewMemtable(memtable.id()))?;
                    state.memtable = Arc::new(memtable);
                } else {
                    state.memtable = state.imm_memtables.remove(0);
                };

                Ok((state, manifest))
            }
        }
    }

    /// Recover data from manifest records.
    /// At this point:
    /// - state's lower levels are not recovered as sorted runs.
    /// - all memtables are restored in immutable memtables.
    fn recover_from_manifest(&self, records: Vec<ManifestRecord>) -> LsmStorageState {
        let mut state = LsmStorageState::create(&self.options);

        // Simulate actions.
        for record in records {
            match record {
                ManifestRecord::Compaction(task, sst_ids) => {
                    (state, _) = self
                        .compaction_controller
                        .apply_compaction_result(&state, &task, &sst_ids, true);
                }
                ManifestRecord::Flush(id) => {
                    let memtable_id = state.imm_memtables.pop().map(|memtable| memtable.id());
                    assert_eq!(memtable_id, Some(id));

                    if self.compaction_controller.flush_to_l0() {
                        state.l0_sstables.insert(0, id);
                    } else {
                        state.levels.insert(0, (id, vec![id]));
                    }
                }
                ManifestRecord::NewMemtable(id) => {
                    // Put all to immutable memtables for now.
                    state
                        .imm_memtables
                        .insert(0, Arc::new(MemTable::create(id)));
                }
            }
        }

        state
    }
}
