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

use crate::lsm_storage::LsmStorageState;
use serde::{Deserialize, Serialize};

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

    fn tier_size(tier_ssts: &[usize]) -> usize {
        // tier_ssts
        //     .iter()
        //     .map(|sst| sstables.get(sst).unwrap().table_size())
        //     .sum()
        tier_ssts.len()
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        // dbg!(&snapshot.levels);
        // dbg!(&snapshot.sstables.keys().collect::<Vec<_>>());

        // trigger by space amplification
        // space_amplification estimate = engine_size / last_level_size
        // space amplication = all tiers except last / last tier size
        if snapshot.levels.is_empty() {
            return None;
        }
        if snapshot.levels.len() == 1 {
            return None;
        }
        let upper_tier_size = snapshot
            .levels
            .iter()
            .take(snapshot.levels.len() - 1)
            .map(|(tier_id, tier_ssts)| Self::tier_size(tier_ssts))
            .sum::<usize>();

        let last_tier_size = Self::tier_size(&snapshot.levels.last().unwrap().1);

        if last_tier_size == 0 {
            return None;
        }
        let space_amplification = upper_tier_size as f64 / last_tier_size as f64;
        if space_amplification >= self.options.max_size_amplification_percent as f64 * 0.01 {
            return Some(TieredCompactionTask {
                tiers: snapshot.levels.clone().into_iter().collect::<Vec<_>>(),
                bottom_tier_included: true,
            });
        }

        if snapshot.levels.len() >= self.options.num_tiers {
            return Some(TieredCompactionTask {
                tiers: snapshot
                    .levels
                    .clone()
                    .into_iter()
                    .take(snapshot.levels.len() - 1)
                    .collect(),
                bottom_tier_included: false,
            });
        }

        None
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        // we already have a lock here on the state
        let mut new_state = snapshot.clone();
        let compacted_tiers = task.tiers.clone();
        let mut compacted_ssts = vec![];

        compacted_tiers.iter().for_each(|(_, sst_ids)| {
            compacted_ssts.extend(sst_ids);
        });
        // bottom level condition?
        // remove old tiers
        let mut last_compacted_tier: Option<usize> = None;
        compacted_tiers.iter().for_each(|(compacted_tier, _)| {
            if let Some(position) = new_state
                .levels
                .iter()
                .position(|(x, _)| x == compacted_tier)
            {
                last_compacted_tier = Some(position);
                new_state.levels.remove(position);
            }
        });
        if let Some(last_compacted_tier) = last_compacted_tier
            && last_compacted_tier < new_state.levels.len()
        {
            new_state
                .levels
                .insert(last_compacted_tier, (output[0], output.to_vec()));
        } else {
            new_state.levels.push((output[0], output.to_vec()));
        }
        (new_state, compacted_ssts)
    }
}
