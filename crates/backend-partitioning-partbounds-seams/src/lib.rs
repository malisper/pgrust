//! Seam declarations for the `backend-partitioning-core` unit's
//! `partitioning/partbounds.c` boundary — the partition-bound search routines
//! `execPartition.c`'s `get_partition_for_tuple` calls.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.
//!
//! C's `FmgrInfo *partsupfunc` / `Oid *partcollation` arrays and the
//! `PartitionBoundInfo` come from the partitioned table's relcache entry, so
//! the caller passes the owned `PartitionKeyData` / `PartitionBoundInfoData`
//! views; the support functions dispatch by their stored lookup key.

use types_core::primitive::Oid;
use types_datum::Datum;
use types_error::PgResult;
use types_nodes::partition::{
    PartitionBoundInfoData, PartitionKeyData, PartitionRangeDatumKind,
};

seam_core::seam!(
    /// `compute_partition_hash_value(partnatts, partsupfunc, partcollation,
    /// values, isnull)` (partbounds.c): the combined hash of the partition-key
    /// values for HASH routing. The support functions can `ereport(ERROR)`,
    /// carried on `Err`.
    pub fn compute_partition_hash_value(
        key: &PartitionKeyData<'_>,
        values: &[Datum],
        isnull: &[bool],
    ) -> PgResult<u64>
);

seam_core::seam!(
    /// `partition_list_bsearch(partsupfunc, partcollation, boundinfo, value,
    /// &is_equal)` (partbounds.c): binary-search a LIST partition's bounds for
    /// `value`, returning `(bound_offset, is_equal)` (`bound_offset == -1` when
    /// below all bounds). The comparison function can `ereport(ERROR)`.
    pub fn partition_list_bsearch(
        key: &PartitionKeyData<'_>,
        boundinfo: &PartitionBoundInfoData<'_>,
        value: Datum,
    ) -> PgResult<(i32, bool)>
);

seam_core::seam!(
    /// `partition_range_datum_bsearch(partsupfunc, partcollation, boundinfo,
    /// nvalues, values, &is_equal)` (partbounds.c): binary-search a RANGE
    /// partition's bounds for the key tuple, returning `(bound_offset,
    /// is_equal)`. The comparison function can `ereport(ERROR)`.
    pub fn partition_range_datum_bsearch(
        key: &PartitionKeyData<'_>,
        boundinfo: &PartitionBoundInfoData<'_>,
        nvalues: i32,
        values: &[Datum],
    ) -> PgResult<(i32, bool)>
);

seam_core::seam!(
    /// `partition_rbound_datum_cmp(partsupfunc, partcollation, rb_datums,
    /// rb_kind, tuple_datums, n_tuple_datums)` (partbounds.c): compare a range
    /// bound against the key tuple (`<0`, `0`, `>0`). The comparison function
    /// can `ereport(ERROR)`.
    pub fn partition_rbound_datum_cmp(
        partcollation: &[Oid],
        rb_datums: &[Datum],
        rb_kind: &[PartitionRangeDatumKind],
        tuple_datums: &[Datum],
        n_tuple_datums: i32,
    ) -> PgResult<i32>
);

seam_core::seam!(
    /// `FunctionCall2Coll(&key->partsupfunc[0], key->partcollation[0],
    /// last_datum, value)` for a LIST partition's cached-find double-check
    /// (`get_partition_for_tuple` in execPartition.c): compare the last-found
    /// LIST bound datum against the new key datum using the partition's first
    /// support (comparison) function. The comparison function can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn partition_list_datum_cmp(
        key: &PartitionKeyData<'_>,
        last_datum: Datum,
        value: Datum,
    ) -> PgResult<i32>
);
