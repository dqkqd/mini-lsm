use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
    pub max_merge_width: Option<usize>,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        if snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        // Trigger by space amplification ratio
        if let Some((last_level, upper_levels)) = snapshot.levels.split_last() {
            let last_level_size = last_level.1.len();
            let upper_levels_size = upper_levels
                .iter()
                .map(|(_, levels)| levels.len())
                .sum::<usize>();

            let ratio = upper_levels_size as f64 * 100.0 / last_level_size as f64;
            if ratio >= self.options.max_size_amplification_percent as f64 {
                eprintln!("compaction triggered by space amplification ratio: {ratio}");
                return Some(TieredCompactionTask {
                    tiers: snapshot.levels.clone(),
                    bottom_tier_included: true,
                });
            }
        }

        // Trigger by size ratio
        let mut previous_tier_size = 0.0;
        let size_ratio = 100.0 + self.options.size_ratio as f64;
        for (i, (_, levels)) in snapshot.levels.iter().enumerate() {
            // Only do compaction if there are more than `min_merge_width` tiers to be merged.
            if i + 1 >= self.options.min_merge_width && previous_tier_size != 0.0 {
                let current_size_ratio = levels.len() as f64 * 100.0 / previous_tier_size;
                if current_size_ratio > size_ratio {
                    eprintln!(
                        "compaction triggered by size ratio: {current_size_ratio} > {size_ratio}"
                    );
                    return Some(TieredCompactionTask {
                        tiers: snapshot.levels.iter().take(i).cloned().collect(),
                        bottom_tier_included: i >= snapshot.levels.len() - 1,
                    });
                }
            }
            previous_tier_size += levels.len() as f64;
        }

        todo!()
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut should_delete_tiers: BTreeSet<usize> =
            task.tiers.iter().map(|(tier, _)| tier).cloned().collect();
        let should_deleted_sst_ids: BTreeSet<usize> = task
            .tiers
            .iter()
            .flat_map(|(_, sst_ids)| sst_ids)
            .cloned()
            .collect();

        let mut new_snapshot = snapshot.clone();
        // tier_id is generated based on the first sst_id.
        let next_tier_id = new_snapshot
            .levels
            .iter()
            .filter_map(|(_, sst_ids)| sst_ids.first())
            .next()
            .cloned()
            .expect("snapshot should contain at least one sst")
            + 1;

        // Delete the first tier.
        if let Some((first_tier, first_sst_ids)) = new_snapshot.levels.first_mut() {
            if should_delete_tiers.contains(first_tier) {
                // Manually filter, because there might be new elements coming in while compacting.
                first_sst_ids.retain(|sst_id| !should_deleted_sst_ids.contains(sst_id));
                // Ignore this tier.
                should_delete_tiers.remove(first_tier);
            }
        }

        // Find the first index to insert new tier.
        let index = new_snapshot
            .levels
            .iter()
            .position(|(tier, _)| should_delete_tiers.contains(tier))
            .unwrap_or(new_snapshot.levels.len());
        new_snapshot
            .levels
            .insert(index, (next_tier_id, output.to_vec()));

        // Remove iters should be deleted or those emptied.
        new_snapshot
            .levels
            .retain(|(tier, sst_ids)| !should_delete_tiers.contains(tier) && !sst_ids.is_empty());

        (new_snapshot, should_deleted_sst_ids.into_iter().collect())
    }
}
