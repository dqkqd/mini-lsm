use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        if snapshot.levels.is_empty() {
            return None;
        }

        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            return Some(SimpleLeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: 1,
                lower_level_sst_ids: snapshot.levels[0].1.clone(),
                is_lower_level_bottom_level: snapshot.levels.len() == 1,
            });
        }

        let max_levels = snapshot.levels.len().min(self.options.max_levels);
        let levels = &snapshot.levels[..max_levels];

        for windows in levels.windows(2) {
            let (upper_level, upper_level_sst_ids) = &windows[0];
            let (lower_level, lower_level_sst_ids) = &windows[1];

            if lower_level_sst_ids.len() * 100
                < self.options.size_ratio_percent * upper_level_sst_ids.len()
            {
                return Some(SimpleLeveledCompactionTask {
                    upper_level: Some(*upper_level),
                    upper_level_sst_ids: upper_level_sst_ids.clone(),
                    lower_level: *lower_level,
                    lower_level_sst_ids: lower_level_sst_ids.clone(),
                    is_lower_level_bottom_level: *upper_level == max_levels,
                });
            }
        }

        None
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &SimpleLeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut new_snapshot = snapshot.clone();
        let deleted_sst_ids: Vec<usize> = task
            .upper_level_sst_ids
            .iter()
            .chain(&task.lower_level_sst_ids)
            .cloned()
            .collect();

        match task.upper_level {
            // level 0 compaction
            None => {
                new_snapshot
                    .l0_sstables
                    .retain(|sst_id| !deleted_sst_ids.contains(sst_id));

                if let Some((_, l1_sstables)) = new_snapshot.levels.get_mut(task.lower_level - 1) {
                    *l1_sstables = output.to_vec();
                }
            }

            Some(upper_level) => {
                if let Some((_, upper_level_sst_ids)) = new_snapshot.levels.get_mut(upper_level - 1)
                {
                    *upper_level_sst_ids = Vec::new();
                };

                if let Some((_, lower_level_sst_ids)) =
                    new_snapshot.levels.get_mut(task.lower_level - 1)
                {
                    *lower_level_sst_ids = output.to_vec();
                };
            }
        }

        (new_snapshot, deleted_sst_ids)
    }
}
