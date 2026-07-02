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

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::StorageIterator;
use crate::iterators::merge_iterator::MergeIterator;
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
    fn get_sst_tables(
        &self,
        snapshot: &LsmStorageState,
        sst_ids: &Vec<usize>,
    ) -> Vec<Arc<SsTable>> {
        sst_ids
            .iter()
            .map(|sst_id| snapshot.sstables.get(sst_id).unwrap().clone())
            .collect()
    }

    fn compact_and_generate(
        &self,
        snapshot: &LsmStorageState,
        merge_iterator: &mut MergeIterator<SsTableIterator>,
    ) -> Result<Vec<SsTable>> {
        let mut sst_builder = SsTableBuilder::new(self.options.block_size);
        let mut new_sstables = vec![];
        while merge_iterator.is_valid() {
            sst_builder.add(merge_iterator.key(), merge_iterator.value());
            merge_iterator
                .next()
                .context("compaction_and_generate: failed to advance while compaction")?;
            // when SST gets too big we split it.
            if sst_builder.estimated_size() >= self.options.target_sst_size {
                let new_sst_id = self.next_sst_id();
                new_sstables.push(
                    sst_builder
                        .build(
                            new_sst_id,
                            Some(self.block_cache.clone()),
                            self.path_of_sst(new_sst_id),
                        )
                        .context("compaction: failed to build new sst_table, builder.build()")?,
                );
                sst_builder = SsTableBuilder::new(self.options.block_size);
            }
        }
        if !sst_builder.is_empty() {
            let new_sst_id = self.next_sst_id();
            new_sstables.push(
                sst_builder
                    .build(
                        new_sst_id,
                        Some(self.block_cache.clone()),
                        self.path_of_sst(new_sst_id),
                    )
                    .context("compaction: failed to build new ss_table, builder.build()")?,
            );
        }

        Ok(new_sstables)
    }

    fn compact(&self, task: &CompactionTask) -> Result<Vec<SsTable>> {
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };
        match task {
            CompactionTask::Leveled(LeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level,
                lower_level_sst_ids,
                is_lower_level_bottom_level,
            })
            | CompactionTask::Simple(SimpleLeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level,
                lower_level_sst_ids,
                is_lower_level_bottom_level,
            }) => {
                let mut upper_level_sstables = self.get_sst_tables(&snapshot, upper_level_sst_ids);
                let lower_level_sstables = self.get_sst_tables(&snapshot, lower_level_sst_ids);
                if upper_level.is_none() {
                    upper_level_sstables = self.get_sst_tables(&snapshot, &snapshot.l0_sstables);
                }
                let mut iters = vec![];
                upper_level_sstables.iter().for_each(|ss_table| {
                    iters.push(Box::new(
                        SsTableIterator::create_and_seek_to_first(ss_table.clone()).unwrap(),
                    ));
                });
                lower_level_sstables.iter().for_each(|ss_table| {
                    iters.push(Box::new(
                        SsTableIterator::create_and_seek_to_first(ss_table.clone()).unwrap(),
                    ));
                });

                let mut merge_iterator = MergeIterator::create(iters);
                self.compact_and_generate(&snapshot, &mut merge_iterator)
            }
            CompactionTask::Tiered(TieredCompactionTask {
                tiers,
                bottom_tier_included,
            }) => {
                let mut sst_table_ids = vec![];
                for (_, sst_ids) in tiers {
                    for sst_id in sst_ids {
                        sst_table_ids.push(*sst_id);
                    }
                }
                let ss_tables = self.get_sst_tables(&snapshot, &sst_table_ids);
                let mut iters = vec![];
                ss_tables.iter().for_each(|sst| {
                    iters.push(Box::new(
                        SsTableIterator::create_and_seek_to_first(sst.clone()).unwrap(),
                    ));
                });
                let mut merge_iter = MergeIterator::create(iters);
                self.compact_and_generate(&snapshot, &mut merge_iter)
            }
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let l0_sst = self.get_sst_tables(&snapshot, l0_sstables);
                let l1_sst = self.get_sst_tables(&snapshot, l1_sstables);
                let mut iters = vec![];
                l0_sst.iter().for_each(|sst| {
                    iters.push(Box::new(
                        SsTableIterator::create_and_seek_to_first(sst.clone()).unwrap(),
                    ));
                });
                l1_sst.iter().for_each(|sst| {
                    iters.push(Box::new(
                        SsTableIterator::create_and_seek_to_first(sst.clone()).unwrap(),
                    ));
                });
                let mut merge_iter = MergeIterator::create(iters);
                self.compact_and_generate(&snapshot, &mut merge_iter)
            }
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };
        let compaction_task = CompactionTask::ForceFullCompaction {
            l0_sstables: snapshot.l0_sstables.clone(),
            l1_sstables: snapshot.levels[0].1.clone(),
        };
        let mut sst_to_compact = self.get_sst_tables(&snapshot, &snapshot.l0_sstables);
        sst_to_compact.extend(self.get_sst_tables(&snapshot, &snapshot.levels[0].1));

        // do the compaction: should we think about doing this in a thread?
        let new_ssts = self.compact(&compaction_task)?;
        let new_sst_ids = new_ssts.iter().map(|sst| sst.sst_id()).collect::<Vec<_>>();

        // update the state
        {
            let state_lock = self.state_lock.lock();
            let mut state_guard = self.state.write();
            let mut new_state = (**state_guard).clone();
            new_state.l0_sstables.clear();
            new_state.levels[0].1 = new_sst_ids;
            *state_guard = Arc::new(new_state);
        }

        // remove all sst_to_compact files

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        unimplemented!()
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
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        if snapshot.imm_memtables.len() + 1 > self.options.num_memtable_limit {
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
