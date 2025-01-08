#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::collections::BTreeSet;
use std::fs;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn build_ssts<I>(&self, mut iter: I, compact_to_bottom: bool) -> Result<Vec<Arc<SsTable>>>
    where
        I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
    {
        let mut new_sstables = Vec::new();
        let mut build = |builder: SsTableBuilder| -> Result<()> {
            let sst_id = self.next_sst_id();
            let path = self.path_of_sst(sst_id);
            let table = builder.build(sst_id, Some(self.block_cache.clone()), path)?;
            new_sstables.push(Arc::new(table));
            Ok(())
        };

        let mut builder = SsTableBuilder::new(self.options.block_size);
        while iter.is_valid() {
            let (key, value) = (iter.key(), iter.value());

            // do not add deleted key if compacting to bottom
            if compact_to_bottom {
                if !value.is_empty() {
                    builder.add(key, value);
                }
            } else {
                builder.add(key, value);
            }

            if builder.estimated_size() >= self.options.target_sst_size {
                let oversized_builder =
                    std::mem::replace(&mut builder, SsTableBuilder::new(self.options.block_size));
                build(oversized_builder)?;
            }

            if iter.next().is_err() {
                break;
            }
        }

        // Make sure there is no left over (key, value).
        if !builder.is_empty() {
            build(builder)?;
        }

        Ok(new_sstables)
    }

    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        match task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let sstables = {
                    let guard = self.state.read();
                    guard.sstables.clone()
                };

                let l0_iters: Vec<Box<SsTableIterator>> = l0_sstables
                    .iter()
                    .filter_map(|sst_id| sstables.get(sst_id).cloned())
                    .filter_map(|table| SsTableIterator::create_and_seek_to_first(table).ok())
                    .map(Box::new)
                    .collect();
                let l0_iters = MergeIterator::create(l0_iters);

                let l1_sstables: Vec<Arc<SsTable>> = l1_sstables
                    .iter()
                    .filter_map(|sst_id| sstables.get(sst_id).cloned())
                    .collect();
                let l1_iter = SstConcatIterator::create_and_seek_to_first(l1_sstables)?;

                let iter = TwoMergeIterator::create(l0_iters, l1_iter)?;
                self.build_ssts(iter, task.compact_to_bottom_level())
            }
            CompactionTask::Simple(simple_leveled_compaction_task) => {
                match simple_leveled_compaction_task.upper_level {
                    // l0 -> l1 compaction
                    None => {
                        let sstables = {
                            let guard = self.state.read();
                            guard.sstables.clone()
                        };
                        let l0_iters: Vec<Box<SsTableIterator>> = simple_leveled_compaction_task
                            .upper_level_sst_ids
                            .iter()
                            .filter_map(|sst_id| sstables.get(sst_id).cloned())
                            .filter_map(|table| {
                                SsTableIterator::create_and_seek_to_first(table).ok()
                            })
                            .map(Box::new)
                            .collect();
                        let l0_iters = MergeIterator::create(l0_iters);

                        let l1_sstables: Vec<Arc<SsTable>> = simple_leveled_compaction_task
                            .lower_level_sst_ids
                            .iter()
                            .filter_map(|sst_id| sstables.get(sst_id).cloned())
                            .collect();
                        let l1_iters = SstConcatIterator::create_and_seek_to_first(l1_sstables)?;

                        let iter = TwoMergeIterator::create(l0_iters, l1_iters)?;
                        self.build_ssts(iter, task.compact_to_bottom_level())
                    }
                    Some(_) => {
                        let sstables = {
                            let guard = self.state.read();
                            guard.sstables.clone()
                        };
                        let upper_sstables: Vec<Arc<SsTable>> = simple_leveled_compaction_task
                            .upper_level_sst_ids
                            .iter()
                            .filter_map(|sst_id| sstables.get(sst_id).cloned())
                            .collect();
                        let lower_sstables: Vec<Arc<SsTable>> = simple_leveled_compaction_task
                            .lower_level_sst_ids
                            .iter()
                            .filter_map(|sst_id| sstables.get(sst_id).cloned())
                            .collect();

                        let upper_iters =
                            SstConcatIterator::create_and_seek_to_first(upper_sstables)?;
                        let lower_iters =
                            SstConcatIterator::create_and_seek_to_first(lower_sstables)?;

                        let iter = TwoMergeIterator::create(upper_iters, lower_iters)?;

                        self.build_ssts(iter, task.compact_to_bottom_level())
                    }
                }
            }
            CompactionTask::Leveled(_) => unimplemented!(),
            CompactionTask::Tiered(_) => unimplemented!(),
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let (l0_sstables, l1_sstables) = {
            let state = self.state.read();
            // To avoid new l0 flushed during compacting, we clone sst ids that we want to compact.
            // l1_sstables does not need to be cloned though, but `task` requires it to be moved.
            (state.l0_sstables.clone(), state.levels[0].1.clone())
        };

        // Save it for changing state later.
        let compacting_sst_ids: BTreeSet<usize> =
            l0_sstables.iter().chain(&l1_sstables).cloned().collect();

        let task = CompactionTask::ForceFullCompaction {
            l0_sstables,
            l1_sstables,
        };
        let sstables = self.compact(&task)?;

        {
            let _state_lock = self.state_lock.lock();

            let mut state = self.state.write();
            let mut new_state = state.as_ref().clone();

            // Remove staled sstables.
            new_state
                .l0_sstables
                .retain(|sst_id| !compacting_sst_ids.contains(sst_id));
            new_state
                .sstables
                .retain(|sst_id, _| !compacting_sst_ids.contains(sst_id));

            // Add new sstables.
            new_state.levels = vec![(1, Vec::with_capacity(sstables.len()))];
            for table in sstables {
                new_state.levels[0].1.push(table.sst_id());
                new_state.sstables.insert(table.sst_id(), table);
            }

            *state = Arc::new(new_state);
        }

        // Remove staled files
        for sst_id in compacting_sst_ids {
            fs::remove_file(self.path_of_sst(sst_id))?
        }

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        if let Some(task) = {
            let snapshot = self.state.read().clone();
            self.compaction_controller
                .generate_compaction_task(&snapshot)
        } {
            let sstables = self.compact(&task)?;
            let new_sst_ids: Vec<usize> = sstables.iter().map(|table| table.sst_id()).collect();

            let compacting_sst_ids = {
                let _state_lock = self.state_lock.lock();
                let mut state = self.state.write();

                let (mut new_state, compacting_sst_ids) = self
                    .compaction_controller
                    .apply_compaction_result(&state, &task, &new_sst_ids, false);
                // Add new sstables.
                for table in sstables {
                    new_state.sstables.insert(table.sst_id(), table);
                }

                *state = Arc::new(new_state);

                compacting_sst_ids
            };

            // Remove staled files
            for sst_id in compacting_sst_ids {
                fs::remove_file(self.path_of_sst(sst_id))?
            }
        }

        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let total_memtables = self.state.read().imm_memtables.len() + 1;
        if total_memtables > self.options.num_memtable_limit {
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
