use std::ops::Bound;

use bytes::Bytes;
use tempfile::tempdir;

use crate::{
    compact::CompactionOptions,
    lsm_storage::{LsmStorageOptions, MiniLsm, WriteBatchRecord},
};

use super::harness::{
    check_iter_result_by_key_and_ts, check_lsm_iter_result_by_key,
    construct_merge_iterator_over_storage,
};

#[test]
fn test_same_read_ts_pins_watermark_until_last_transaction_drops() {
    let dir = tempdir().unwrap();
    let options = LsmStorageOptions::default_for_week2_test(CompactionOptions::NoCompaction);
    let storage = MiniLsm::open(&dir, options).unwrap();

    storage.put(b"before", b"1").unwrap();
    let txn1 = storage.new_txn().unwrap();
    let txn2 = storage.new_txn().unwrap();
    assert_eq!(txn1.read_ts, txn2.read_ts);

    let pinned_ts = txn1.read_ts;
    storage.put(b"after", b"2").unwrap();
    let latest_ts = storage.inner.mvcc().latest_commit_ts();
    assert!(latest_ts > pinned_ts);
    assert_eq!(storage.inner.mvcc().watermark(), pinned_ts);

    drop(txn1);
    assert_eq!(storage.inner.mvcc().watermark(), pinned_ts);

    drop(txn2);
    assert_eq!(storage.inner.mvcc().watermark(), latest_ts);
}

#[test]
fn test_successive_watermarks_compact_internal_versions_without_changing_visible_reads() {
    let dir = tempdir().unwrap();
    let options = LsmStorageOptions::default_for_week2_test(CompactionOptions::NoCompaction);
    let storage = MiniLsm::open(&dir, options).unwrap();

    storage
        .write_batch(&[
            WriteBatchRecord::Put(b"a", b"1"),
            WriteBatchRecord::Put(b"b", b"1"),
        ])
        .unwrap();
    let snapshot1a = storage.new_txn().unwrap();
    let snapshot1b = storage.new_txn().unwrap();
    assert_eq!(snapshot1a.read_ts, snapshot1b.read_ts);

    storage
        .write_batch(&[
            WriteBatchRecord::Put(b"a", b"2"),
            WriteBatchRecord::Put(b"d", b"2"),
        ])
        .unwrap();
    let snapshot2 = storage.new_txn().unwrap();

    storage
        .write_batch(&[
            WriteBatchRecord::Put(b"a", b"3"),
            WriteBatchRecord::Del(b"d"),
        ])
        .unwrap();
    let snapshot3 = storage.new_txn().unwrap();

    storage
        .write_batch(&[
            WriteBatchRecord::Put(b"c", b"4"),
            WriteBatchRecord::Del(b"a"),
        ])
        .unwrap();
    storage.force_flush().unwrap();

    drop(snapshot1a);
    assert_eq!(storage.inner.mvcc().watermark(), 1);
    storage.force_full_compaction().unwrap();
    let mut internal = construct_merge_iterator_over_storage(&storage.inner.state.read());
    check_iter_result_by_key_and_ts(
        &mut internal,
        vec![
            ((Bytes::from("a"), 4), Bytes::new()),
            ((Bytes::from("a"), 3), Bytes::from("3")),
            ((Bytes::from("a"), 2), Bytes::from("2")),
            ((Bytes::from("a"), 1), Bytes::from("1")),
            ((Bytes::from("b"), 1), Bytes::from("1")),
            ((Bytes::from("c"), 4), Bytes::from("4")),
            ((Bytes::from("d"), 3), Bytes::new()),
            ((Bytes::from("d"), 2), Bytes::from("2")),
        ],
    );
    check_lsm_iter_result_by_key(
        &mut snapshot1b.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
        vec![
            (Bytes::from("a"), Bytes::from("1")),
            (Bytes::from("b"), Bytes::from("1")),
        ],
    );

    drop(snapshot1b);
    assert_eq!(storage.inner.mvcc().watermark(), 2);
    storage.force_full_compaction().unwrap();
    let mut internal = construct_merge_iterator_over_storage(&storage.inner.state.read());
    check_iter_result_by_key_and_ts(
        &mut internal,
        vec![
            ((Bytes::from("a"), 4), Bytes::new()),
            ((Bytes::from("a"), 3), Bytes::from("3")),
            ((Bytes::from("a"), 2), Bytes::from("2")),
            ((Bytes::from("b"), 1), Bytes::from("1")),
            ((Bytes::from("c"), 4), Bytes::from("4")),
            ((Bytes::from("d"), 3), Bytes::new()),
            ((Bytes::from("d"), 2), Bytes::from("2")),
        ],
    );
    check_lsm_iter_result_by_key(
        &mut snapshot2.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
        vec![
            (Bytes::from("a"), Bytes::from("2")),
            (Bytes::from("b"), Bytes::from("1")),
            (Bytes::from("d"), Bytes::from("2")),
        ],
    );

    drop(snapshot2);
    assert_eq!(storage.inner.mvcc().watermark(), 3);
    storage.force_full_compaction().unwrap();
    let mut internal = construct_merge_iterator_over_storage(&storage.inner.state.read());
    check_iter_result_by_key_and_ts(
        &mut internal,
        vec![
            ((Bytes::from("a"), 4), Bytes::new()),
            ((Bytes::from("a"), 3), Bytes::from("3")),
            ((Bytes::from("b"), 1), Bytes::from("1")),
            ((Bytes::from("c"), 4), Bytes::from("4")),
        ],
    );
    check_lsm_iter_result_by_key(
        &mut snapshot3.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
        vec![
            (Bytes::from("a"), Bytes::from("3")),
            (Bytes::from("b"), Bytes::from("1")),
        ],
    );

    drop(snapshot3);
    assert_eq!(storage.inner.mvcc().watermark(), 4);
    storage.force_full_compaction().unwrap();
    let mut internal = construct_merge_iterator_over_storage(&storage.inner.state.read());
    check_iter_result_by_key_and_ts(
        &mut internal,
        vec![
            ((Bytes::from("b"), 1), Bytes::from("1")),
            ((Bytes::from("c"), 4), Bytes::from("4")),
        ],
    );
    check_lsm_iter_result_by_key(
        &mut storage.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
        vec![
            (Bytes::from("b"), Bytes::from("1")),
            (Bytes::from("c"), Bytes::from("4")),
        ],
    );
}
