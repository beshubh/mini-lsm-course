// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    // lower_level_num_files / upper_level_num_files, when the ratio is too low meaning upper level has
    // too many files we should trigger compaction
    pub size_ratio_percent: usize,
    // when number of SSTs in L0 is >= this number, trigger compaction of L0 to L1
    pub level0_file_num_compaction_trigger: usize,
    // total number of levels excluding L0 in LSM tree.
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
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            // L0 compaction
            return Some(SimpleLeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: 1,
                lower_level_sst_ids: snapshot.levels[0].1.clone(),
                is_lower_level_bottom_level: self.options.max_levels == 1,
            });
        }
        // L1 [ ] upper most
        // L2 [ ] [ ]
        // L3 [ ] [ ] [ ] lower most
        for upper in 0..self.options.max_levels - 1 {
            // TODO: what should we do if the levels currently do not have this lower level or upper
            // level? ideally we should just trust LSMStorageState::create, that it will crease the levels
            let lower = upper + 1;
            let lower_level_num_files = snapshot.levels[lower].1.len();
            let upper_level_num_files = snapshot.levels[upper].1.len();
            if upper_level_num_files == 0 {
                continue;
            }
            if (lower_level_num_files / upper_level_num_files) * 100
                < self.options.size_ratio_percent
            {
                // we should compact
                return Some(SimpleLeveledCompactionTask {
                    upper_level: Some(upper + 1),
                    upper_level_sst_ids: snapshot.levels[upper].1.clone(),
                    lower_level: lower + 1,
                    lower_level_sst_ids: snapshot.levels[lower].1.clone(),
                    is_lower_level_bottom_level: lower == self.options.max_levels - 1,
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
        let mut new_state = snapshot.clone();
        let mut compacted_sst_ids = task.upper_level_sst_ids.clone();
        compacted_sst_ids.extend(task.lower_level_sst_ids.iter());
        if let Some(upper_level) = task.upper_level {
            new_state.levels[upper_level - 1].1.clear();
        } else {
            compacted_sst_ids.iter().for_each(|compacted_sst| {
                if let Some(position) = new_state
                    .l0_sstables
                    .iter()
                    .position(|x| x == compacted_sst)
                {
                    new_state.l0_sstables.remove(position);
                }
                // we don't do this?
                // new_state.sstables.remove(&compacted_sst);
            });
        }
        new_state.levels[task.lower_level - 1].1.clear();
        new_state.levels[task.lower_level - 1].1.extend(output);
        (new_state, compacted_sst_ids)
    }
}
