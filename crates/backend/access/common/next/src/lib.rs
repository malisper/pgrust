//! `backend-access-common-next` — the `access/common` bundle covering
//! `attmap.c`, `tupconvert.c`, and `syncscan.c`.
//!
//! * [`attmap`] — build/manage attribute-number maps between `TupleDesc`s.
//! * [`tupconvert`] — set up and run rowtype conversions using those maps.
//! * [`syncscan`] — synchronized-seqscan start-location LRU.
//!
//! Inward seams owned and installed by [`init_seams`]:
//!   * `backend-access-common-next-seams` — the attmap/tupconvert entry points
//!     other crates reach across a dependency cycle.
//!   * `backend-access-common-tupconvert-seams` — `execute_attr_map_slot` (the
//!     slot-pool variant; this unit delegates it to the execTuples slot-payload
//!     owner).
//!   * `backend-access-common-syncscan-seams` — `ss_get_location`,
//!     `ss_report_location`, and the `ipci.c` shmem-size/init slots.

#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

pub mod attmap;
pub mod syncscan;
pub mod tupconvert;

#[cfg(test)]
mod syncscan_tests;

/// Install every inward seam this unit owns.
pub fn init_seams() {
    // attmap.c / tupconvert.c entry points (backend-access-common-next-seams).
    next_seams::build_attrmap_by_name::set(
        attmap::build_attrmap_by_name,
    );
    next_seams::build_attrmap_by_name_if_req::set(
        attmap::build_attrmap_by_name_if_req,
    );
    next_seams::convert_tuples_by_name::set(tupconvert::convert_tuples_by_name);
    next_seams::convert_tuples_by_name_attrmap::set(
        tupconvert::convert_tuples_by_name_attrmap,
    );
    next_seams::execute_attr_map_cols::set(tupconvert::execute_attr_map_cols);
    next_seams::execute_attr_map_tuple::set(
        tupconvert::execute_attr_map_tuple,
    );

    // attmap.c map constructor/destructor (backend-access-common-next-seams).
    // The seam contract carries the map by value (`AttrMap`), while this unit's
    // local helpers work in `PgBox<AttrMap>`; adapt at the boundary.
    next_seams::make_attrmap::set(make_attrmap_seam);
    next_seams::free_attrmap::set(free_attrmap_seam);

    // tupconvert.c slot variant (backend-access-common-tupconvert-seams).
    tupconvert_seams::execute_attr_map_slot::set(
        tupconvert::execute_attr_map_slot,
    );

    // syncscan.c (backend-access-common-syncscan-seams).
    syncscan_seams::ss_get_location::set(syncscan::ss_get_location);
    syncscan_seams::ss_report_location::set(syncscan::ss_report_location);
    syncscan_seams::sync_scan_shmem_size::set(syncscan::sync_scan_shmem_size);
    syncscan_seams::sync_scan_shmem_init::set(syncscan::sync_scan_shmem_init);
}

/// Value-returning adapter for the `make_attrmap` seam: the local helper
/// allocates a `PgBox<AttrMap>`; the seam contract returns the `AttrMap` by
/// value. Dereference the box out (the `attnums` vector still lives in `mcx`).
fn make_attrmap_seam<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    maplen: i32,
) -> types_error::PgResult<types_tuple::attmap::AttrMap<'mcx>> {
    Ok(mcx::PgBox::into_inner(attmap::make_attrmap(mcx, maplen)?))
}

/// Value-taking adapter for the `free_attrmap` seam: the C `pfree`s the map; in
/// the owned model the storage is reclaimed when the value drops.
fn free_attrmap_seam(map: types_tuple::attmap::AttrMap<'_>) {
    drop(map);
}
