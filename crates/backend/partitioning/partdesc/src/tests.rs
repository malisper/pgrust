//! Unit tests for the partdesc owner.
//!
//! End-to-end partition routing requires link 4 (MergeAttributes /
//! StorePartitionBound — `CREATE TABLE ... PARTITION OF`), which is not yet
//! landed, so a partition cannot be created to drive a full build here. These
//! tests cover the lifetime-free units the build/lookup paths rest on: the
//! `PartitionDescData` deep clone (the directory's caching/re-projection
//! primitive), the default-OID lookup, and the empty `PartitionDirectory`
//! create/destroy round trip.

use super::*;
use nodes::partition::{PartitionBoundInfoData, PartitionStrategy};

fn empty_partdesc<'mcx>(mcx: Mcx<'mcx>) -> PgResult<PartitionDescData<'mcx>> {
    Ok(PartitionDescData {
        nparts: 0,
        detached_exist: false,
        oids: mcx::vec_with_capacity_in(mcx, 0)?,
        is_leaf: mcx::vec_with_capacity_in(mcx, 0)?,
        boundinfo: None,
        last_found_datum_index: 0,
        last_found_part_index: 0,
        last_found_count: 0,
    })
}

#[test]
fn clone_in_roundtrips_a_list_partdesc() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();

    // A two-partition LIST descriptor (canonical order already applied).
    let mut oids: mcx::PgVec<Oid> = mcx::vec_with_capacity_in(mcx, 2).unwrap();
    oids.push(101);
    oids.push(102);
    let mut is_leaf: mcx::PgVec<bool> = mcx::vec_with_capacity_in(mcx, 2).unwrap();
    is_leaf.push(true);
    is_leaf.push(true);

    let mut indexes: mcx::PgVec<i32> = mcx::vec_with_capacity_in(mcx, 2).unwrap();
    indexes.push(0);
    indexes.push(1);

    let boundinfo = PartitionBoundInfoData {
        strategy: PartitionStrategy::List,
        ndatums: 0,
        datums: mcx::vec_with_capacity_in(mcx, 0).unwrap(),
        kind: None,
        interleaved_parts: None,
        nindexes: 2,
        indexes,
        null_index: -1,
        default_index: 1,
    };

    let src = PartitionDescData {
        nparts: 2,
        detached_exist: false,
        oids,
        is_leaf,
        boundinfo: Some(mcx::alloc_in(mcx, boundinfo).unwrap()),
        last_found_datum_index: -1,
        last_found_part_index: -1,
        last_found_count: 0,
    };

    // Clone into a second context, then read back.
    let ctx2 = MemoryContext::new("test2");
    let mcx2 = ctx2.mcx();
    let cloned = PartitionDescData::clone_in(&src, mcx2).unwrap();

    assert_eq!(cloned.nparts, 2);
    assert_eq!(&cloned.oids[..], &[101, 102]);
    assert_eq!(&cloned.is_leaf[..], &[true, true]);
    let bi = cloned.boundinfo.as_deref().unwrap();
    assert_eq!(bi.default_index, 1);
    assert_eq!(&bi.indexes[..], &[0, 1]);

    // default-OID lookup uses default_index into oids.
    assert_eq!(get_default_oid_from_partdesc(Some(&cloned)), 102);
}

#[test]
fn get_default_oid_handles_none_and_no_default() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();

    assert_eq!(get_default_oid_from_partdesc(None), InvalidOid);

    let pd = empty_partdesc(mcx).unwrap();
    // No boundinfo -> InvalidOid.
    assert_eq!(get_default_oid_from_partdesc(Some(&pd)), InvalidOid);
}

#[test]
fn empty_directory_create_destroy_roundtrip() {
    // No entries pinned -> destroy is a no-op over the (empty) pin set and
    // never reaches the relcache refcount seam.
    let dir = CreatePartitionDirectory(false);
    assert!(DestroyPartitionDirectory(&dir).is_ok());
    assert_eq!(get_default_oid_from_partdesc(None), InvalidOid);
}
