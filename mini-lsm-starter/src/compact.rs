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
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::KeySlice;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::manifest::ManifestRecord;
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
    fn get_sst_tables(&self, snapshot: &LsmStorageState, sst_ids: &[usize]) -> Vec<Arc<SsTable>> {
        sst_ids
            .iter()
            .map(|sst_id| snapshot.sstables.get(sst_id).unwrap().clone())
            .collect()
    }

    fn compact_and_generate<I>(
        &self,
        iter: &mut I,
        compact_to_bottom_level: bool,
    ) -> Result<Vec<SsTable>>
    where
        I: for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
    {
        let mut sst_builder = SsTableBuilder::new(self.options.block_size);
        let mut new_sstables = vec![];
        while iter.is_valid() {
            if iter.value().is_empty() && compact_to_bottom_level {
                iter.next()
                    .context("compaction_and_generate: failed to advance while compaction")?;
                continue;
            }
            sst_builder.add(iter.key(), iter.value());
            iter.next()
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

    fn compact(&self, task: &CompactionTask, snapshot: &LsmStorageState) -> Result<Vec<SsTable>> {
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
                let upper_level_sstables = self.get_sst_tables(snapshot, upper_level_sst_ids);
                let lower_level_sstables = self.get_sst_tables(snapshot, lower_level_sst_ids);
                match upper_level {
                    None => {
                        // L0 compaction, need merge iterator on l0 sstables
                        let mut upper_iters = vec![];
                        upper_level_sstables.iter().for_each(|ss_table| {
                            upper_iters.push(Box::new(
                                SsTableIterator::create_and_seek_to_first(ss_table.clone())
                                    .unwrap(),
                            ));
                        });
                        let upper_iter = MergeIterator::create(upper_iters);
                        let lower_iter =
                            SstConcatIterator::create_and_seek_to_first(lower_level_sstables)
                                .context(
                                    "compaction: error creating concat iterator in compact()",
                                )?;
                        let mut final_iter = TwoMergeIterator::create(upper_iter, lower_iter)?;
                        self.compact_and_generate(&mut final_iter, task.compact_to_bottom_level())
                    }
                    Some(_) => {
                        // for non-L0 level compaction, ConcatOperator works for both
                        let upper_iter =
                            SstConcatIterator::create_and_seek_to_first(upper_level_sstables)
                                .context("compaction: error creating concat iterator for upper")?;
                        let lower_iter =
                            SstConcatIterator::create_and_seek_to_first(lower_level_sstables)
                                .context(
                                    "compaction: error creating concat iterator in compact()",
                                )?;

                        let mut final_iter = TwoMergeIterator::create(upper_iter, lower_iter)?;
                        self.compact_and_generate(&mut final_iter, task.compact_to_bottom_level())
                    }
                }
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
                let ss_tables = self.get_sst_tables(snapshot, &sst_table_ids);
                let mut iters = vec![];
                ss_tables.iter().for_each(|sst| {
                    iters.push(Box::new(
                        SsTableIterator::create_and_seek_to_first(sst.clone()).unwrap(),
                    ));
                });
                let mut merge_iter = MergeIterator::create(iters);
                self.compact_and_generate(&mut merge_iter, task.compact_to_bottom_level())
            }
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let l0_sst = self.get_sst_tables(snapshot, l0_sstables);
                let l1_sst = self.get_sst_tables(snapshot, l1_sstables);
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
                self.compact_and_generate(&mut merge_iter, task.compact_to_bottom_level())
            }
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };
        let l0_sstables = snapshot.l0_sstables.clone();
        let l1_sstables = snapshot.levels[0].1.clone();
        let mut sst_to_compact = self.get_sst_tables(&snapshot, &l0_sstables);
        sst_to_compact.extend(self.get_sst_tables(&snapshot, &l1_sstables));
        let compaction_task = CompactionTask::ForceFullCompaction {
            l0_sstables,
            l1_sstables,
        };
        // do the compaction: should we think about doing this in a thread?
        let new_ssts = self.compact(&compaction_task, &snapshot)?;
        let new_sst_ids = new_ssts.iter().map(|sst| sst.sst_id()).collect::<Vec<_>>();

        // What if while doing compaction there are new entries in l0_sstables
        // A: we have copy of l0, so won't be an issue

        // update the state
        {
            let state_lock = self.state_lock.lock();
            let mut state_guard = self.state.write();
            let mut new_state = (**state_guard).clone();
            // remove compacted ssts from l0
            sst_to_compact.iter().for_each(|compacted_sst| {
                if let Some(idx) = new_state
                    .l0_sstables
                    .iter()
                    .position(|x| *x == compacted_sst.sst_id())
                {
                    new_state.l0_sstables.remove(idx);
                }
            });

            new_state.levels[0].1 = new_sst_ids.clone();
            for sst in &sst_to_compact {
                new_state.sstables.remove(&sst.sst_id());
            }
            for sst in new_ssts {
                new_state.sstables.insert(sst.sst_id(), Arc::new(sst));
            }
            *state_guard = Arc::new(new_state);
            self.sync_dir()?;
            if let Some(manifest) = &self.manifest {
                manifest.add_record(
                    &state_lock,
                    ManifestRecord::Compaction(compaction_task, new_sst_ids),
                )?;
            }
        }

        // remove all sst_to_compact files
        for sst in sst_to_compact {
            std::fs::remove_file(self.path_of_sst(sst.sst_id()))
                .context("compaction: failed to remove compacted sst files")?;
        }
        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let controller = match &self.options.compaction_options {
            CompactionOptions::Simple(opts) => {
                let simple_controller = SimpleLeveledCompactionController::new(opts.clone());
                CompactionController::Simple(simple_controller)
            }
            CompactionOptions::Tiered(options) => {
                let tiered_controller = TieredCompactionController::new(options.clone());
                CompactionController::Tiered(tiered_controller)
            }
            _ => todo!(),
        };
        let snapshot = {
            let snapshot = self.state.read();
            snapshot.clone()
        };
        let compaction_res = controller.generate_compaction_task(&snapshot);
        // drop the read snapshot

        if let Some(task) = compaction_res {
            let new_ssts = self.compact(&task, &snapshot)?;
            drop(snapshot);
            let output = new_ssts.iter().map(|sst| sst.sst_id()).collect::<Vec<_>>();
            let state_lock = self.state_lock.lock();
            let mut state_guard = self.state.write();
            let snapshot = (**state_guard).clone();
            let (mut new_state, deleted_ssts) =
                controller.apply_compaction_result(&snapshot, &task, &output, false);

            // remove deleted ssts from sstables
            deleted_ssts.iter().for_each(|sst_id| {
                new_state.sstables.remove(sst_id);
            });
            let new_sst_ids: Vec<usize> = new_ssts.iter().map(|s| s.sst_id()).collect();
            // add new generated ssts to sstables
            for sst in new_ssts {
                new_state.sstables.insert(sst.sst_id(), Arc::new(sst));
            }

            *state_guard = Arc::new(new_state);
            // remove files of deleted ssts
            for sst in deleted_ssts {
                std::fs::remove_file(self.path_of_sst(sst))
                    .context("compaction: failed to remove compacted sst, trigg")?;
            }

            self.sync_dir()?;
            if let Some(manifest) = &self.manifest {
                manifest.add_record(&state_lock, ManifestRecord::Compaction(task, new_sst_ids))?;
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
