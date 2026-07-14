use std::ops::{Bound, Range};
use std::sync::Arc;

use crate::table::SsTable;

/// Returns the contiguous range of sorted, non-overlapping SSTs that may contain
/// keys within the requested bounds.
pub(crate) fn overlapping_sst_range(
    sstables: &[Arc<SsTable>],
    lower: Bound<&[u8]>,
    upper: Bound<&[u8]>,
) -> Range<usize> {
    let start = sstables.partition_point(|sst| match lower {
        Bound::Included(key) => sst.last_key().raw_ref() < key,
        Bound::Excluded(key) => sst.last_key().raw_ref() <= key,
        Bound::Unbounded => false,
    });

    let end = sstables.partition_point(|sst| match upper {
        Bound::Included(key) => sst.first_key().raw_ref() <= key,
        Bound::Excluded(key) => sst.first_key().raw_ref() < key,
        Bound::Unbounded => true,
    });

    start.min(end)..end
}
