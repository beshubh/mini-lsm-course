// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::table::SsTable;
use serde::{Deserialize, Serialize};
use std::ops::Bound;
use std::sync::Arc;

use crate::common::overlapping_sst_range;
use crate::lsm_storage::LsmStorageState;

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

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_ssts(
        &self,
        snapshot: &LsmStorageState,
        sst_ids: &[usize],
        in_level: usize,
    ) -> Vec<usize> {
        let level_sst_ids = &snapshot.levels[in_level - 1].1;
        let sst_tables = self.get_sstables(snapshot, sst_ids);
        let level_sstables = self.get_sstables(snapshot, level_sst_ids);
        if sst_tables.is_empty() {
            return vec![];
        }
        let min_key = sst_tables.iter().map(|sst| sst.first_key()).min().unwrap();
        let max_key = sst_tables.iter().map(|sst| sst.last_key()).max().unwrap();
        let range = overlapping_sst_range(
            &level_sstables,
            Bound::Included(min_key.raw_ref()),
            Bound::Included(max_key.raw_ref()),
        );

        level_sstables[range]
            .iter()
            .map(|sst| sst.sst_id())
            .collect()
    }

    fn get_sstables(&self, snapshot: &LsmStorageState, sst_ids: &[usize]) -> Vec<Arc<SsTable>> {
        sst_ids
            .iter()
            .map(|sst_id| {
                // unwrap should be safe here?
                snapshot.sstables.get(sst_id).unwrap().clone()
            })
            .collect()
    }
    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        // actual sizes calculation
        let mut actual_level_sizes = vec![0u64; self.options.max_levels];
        for (i, item) in actual_level_sizes.iter_mut().enumerate() {
            let sstables = self.get_sstables(snapshot, &snapshot.levels[i].1);
            *item = sstables.iter().map(|sst| sst.file.size()).sum::<u64>();
        }

        // target size calculation
        let actual_bottom_size = *actual_level_sizes.last().unwrap() as usize;
        let base_level_size_bytes = self.options.base_level_size_mb * 1024 * 1024;
        let mut bottom_target = actual_bottom_size;
        let mut target_sizes = vec![0; self.options.max_levels];

        if actual_bottom_size <= base_level_size_bytes {
            bottom_target = base_level_size_bytes;
        } else {
            let mut current_target = bottom_target;
            for next_target in target_sizes[..actual_level_sizes.len() - 1]
                .iter_mut()
                .rev()
            {
                if current_target <= base_level_size_bytes {
                    break;
                }
                *next_target = current_target / self.options.level_size_multiplier;
                current_target = *next_target;
            }
        }
        *target_sizes.last_mut().unwrap() = bottom_target;
        let mut first_non_zero_target_level_idx = target_sizes.len();
        for (idx, target) in target_sizes.iter().enumerate() {
            if *target > 0 {
                first_non_zero_target_level_idx = idx;
                break;
            }
        }
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: first_non_zero_target_level_idx + 1,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    snapshot,
                    &snapshot.l0_sstables,
                    first_non_zero_target_level_idx + 1,
                ),
                is_lower_level_bottom_level: first_non_zero_target_level_idx
                    == snapshot.levels.len() - 1,
            });
        }
        // priority
        let mut priority = vec![0f64; self.options.max_levels];
        for i in 0..actual_level_sizes.len() {
            let current_size = actual_level_sizes[i];
            let target_size = target_sizes[i];
            if target_size == 0 {
                continue;
            }
            let ratio = current_size as f64 / target_size as f64;
            if ratio > 1.0 {
                priority[i] = ratio;
            }
        }
        let upper_idx = priority
            .iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| a.total_cmp(b));
        if let Some((upper_idx, max_priority)) = upper_idx {
            if *max_priority < 1.0 {
                return None;
            }
            // select the oldest for now
            // TODO: for practice we can choose an SST with large number of tombstones
            let upper_sst_ids = vec![*snapshot.levels[upper_idx].1.iter().min().unwrap()];
            return Some(LeveledCompactionTask {
                upper_level: Some(upper_idx + 1),
                lower_level_sst_ids: self.find_overlapping_ssts(
                    snapshot,
                    &upper_sst_ids,
                    upper_idx + 2,
                ),
                upper_level_sst_ids: upper_sst_ids,
                lower_level: upper_idx + 2,
                is_lower_level_bottom_level: upper_idx + 2 == snapshot.levels.len(),
            });
        }
        None
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        let mut new_state = snapshot.clone();
        let mut compacted_sst_ids = task.upper_level_sst_ids.clone();
        compacted_sst_ids.extend(task.lower_level_sst_ids.iter());

        let mut remove_compacted = |in_level: usize| {
            compacted_sst_ids.iter().for_each(|compacted_sst| {
                if let Some(position) = new_state.levels[in_level - 1]
                    .1
                    .iter()
                    .position(|x| x == compacted_sst)
                {
                    new_state.levels[in_level - 1].1.remove(position);
                }
            });
        };
        if let Some(upper_level) = task.upper_level {
            remove_compacted(upper_level);
        } else {
            compacted_sst_ids.iter().for_each(|compacted_sst| {
                if let Some(position) = new_state
                    .l0_sstables
                    .iter()
                    .position(|x| x == compacted_sst)
                {
                    new_state.l0_sstables.remove(position);
                }
            });
        }
        remove_compacted(task.lower_level);
        new_state.levels[task.lower_level - 1].1.extend(output);
        if !in_recovery {
            new_state.levels[task.lower_level - 1]
                .1
                .sort_by_key(|sst_id| {
                    let sst = new_state.sstables.get(sst_id).unwrap();
                    sst.first_key()
                });
        }
        (new_state, compacted_sst_ids)
    }
}
