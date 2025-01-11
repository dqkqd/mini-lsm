use std::{collections::BTreeSet, sync::Arc};

use serde::{Deserialize, Serialize};

use crate::{lsm_storage::LsmStorageState, table::SsTable};

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

#[derive(Debug)]
struct LevelSizedData {
    target_size_in_mb: f64,
    size_ratio: f64,
    level: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    // Compute current size and target size.
    fn compute_level_sized_data(&self, snapshot: &LsmStorageState) -> Vec<LevelSizedData> {
        let mut target_size_in_mb = self.options.base_level_size_mb as f64;
        let reversed_level_sized_data: Vec<LevelSizedData> = snapshot
            .levels
            .iter()
            .rev()
            .map(|(level, sst_ids)| {
                let current_size_in_bytes: u64 = sst_ids
                    .iter()
                    .filter_map(|sst_id| snapshot.sstables.get(sst_id))
                    .map(|table| table.table_size())
                    .sum();
                let current_size_in_mb = (current_size_in_bytes as f64) / ((1 << 20) as f64);

                let data = LevelSizedData {
                    target_size_in_mb,
                    size_ratio: current_size_in_mb / target_size_in_mb,
                    level: *level,
                };

                if current_size_in_mb <= target_size_in_mb {
                    target_size_in_mb = 0.0;
                }
                target_size_in_mb /= self.options.level_size_multiplier as f64;

                data
            })
            .collect();

        reversed_level_sized_data.into_iter().rev().collect()
    }

    fn find_overlapping_ssts(
        &self,
        snapshot: &LsmStorageState,
        sst_ids: &[usize],
        in_level: usize,
    ) -> Vec<usize> {
        let (first_key, last_key) = {
            let tables: Vec<&Arc<SsTable>> = sst_ids
                .iter()
                .filter_map(|sst_id| snapshot.sstables.get(sst_id))
                .collect();

            let first_key = tables
                .iter()
                .map(|table| table.first_key())
                .min()
                .cloned()
                .expect("cannot find mininum first key");

            let last_key = tables
                .iter()
                .map(|table| table.last_key())
                .max()
                .cloned()
                .expect("cannot find maximum last key");

            (first_key, last_key)
        };

        let output = &snapshot.levels[in_level - 1].1;
        let left = output.partition_point(|sst_id| {
            let table = snapshot.sstables.get(sst_id).unwrap();
            table.last_key() < &first_key
        });
        let right = output.partition_point(|sst_id| {
            let table = snapshot.sstables.get(sst_id).unwrap();
            table.first_key() <= &last_key
        });

        output[left..right].to_vec()
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        let level_sized_data = self.compute_level_sized_data(snapshot);

        let (upper_level, upper_level_sst_ids, lower_level) = if snapshot.l0_sstables.len()
            >= self.options.level0_file_num_compaction_trigger
        {
            // compact at lower level with target size > 0
            let position = level_sized_data.partition_point(|data| data.target_size_in_mb == 0.0);
            let lower_level = snapshot.levels.get(position)?.0;
            // Compact all sst ids in level 0, because they are most likely to overlap.
            let upper_level_sst_ids = snapshot.l0_sstables.clone();
            (None, upper_level_sst_ids, lower_level)
        } else {
            let highest_priority = level_sized_data
                .iter()
                .filter(|data| data.size_ratio >= 1.0)
                .max_by(|x, y| x.size_ratio.total_cmp(&y.size_ratio))?;
            let upper_level = highest_priority.level;
            let lower_level = upper_level + 1;
            // use the oldest sst id to compact with lower level.
            let oldest_upper_level_sst_id = snapshot.levels.get(upper_level - 1)?.1.iter().min()?;
            (
                Some(upper_level),
                vec![*oldest_upper_level_sst_id],
                lower_level,
            )
        };

        let last_level = snapshot.levels.last()?;

        // Do not compact at the bottom level
        if upper_level == Some(last_level.0) {
            return None;
        }

        let lower_level_sst_ids =
            self.find_overlapping_ssts(snapshot, &upper_level_sst_ids, lower_level);

        let is_lower_level_bottom_level = lower_level == last_level.0 && {
            let (_, suffix) = last_level.1.split_at(lower_level_sst_ids.len());
            lower_level_sst_ids == suffix
        };

        Some(LeveledCompactionTask {
            upper_level,
            upper_level_sst_ids,
            lower_level,
            lower_level_sst_ids,
            is_lower_level_bottom_level,
        })
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[usize],
        _in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        let mut new_snapshot = snapshot.clone();
        let deleted_sst_ids: BTreeSet<usize> = task
            .upper_level_sst_ids
            .iter()
            .chain(&task.lower_level_sst_ids)
            .cloned()
            .collect();

        let upper_level_sst_ids = match task.upper_level {
            Some(upper_level) => &mut new_snapshot.levels[upper_level - 1].1,
            None => &mut new_snapshot.l0_sstables,
        };
        upper_level_sst_ids.retain(|sst_id| !deleted_sst_ids.contains(sst_id));

        let lower_level_sst_ids = &mut new_snapshot.levels[task.lower_level - 1].1;
        lower_level_sst_ids.retain(|sst_id| !deleted_sst_ids.contains(sst_id));

        let first_key = {
            let sst_id = output[0];
            let table = new_snapshot
                .sstables
                .get(&sst_id)
                .expect("sstables should contains output's sst_id");
            table.first_key()
        };

        let insert_index = lower_level_sst_ids.partition_point(|sst_id| {
            let table = new_snapshot.sstables.get(sst_id).unwrap();
            table.first_key() <= first_key
        });

        *lower_level_sst_ids = [
            &lower_level_sst_ids[..insert_index],
            output,
            &lower_level_sst_ids[insert_index..],
        ]
        .concat();

        (new_snapshot, deleted_sst_ids.into_iter().collect())
    }
}
