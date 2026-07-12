//! cbstore: scan-only columnar table AM for the ClickBench charter
//! (docs/design/clickbench-format.md, docs/design/cbstore-impl.md).
//! Buffer-cache-bypass by design (approved): reads are mmap of the part
//! file, writes are direct pwrites + fsync; the main fork file's existence
//! stays owned by the ordinary smgr create/unlink machinery.

#![allow(non_snake_case)]

pub mod bloom;
pub mod format;
pub mod hll;
pub mod lz4dec;
pub mod part_cache;
pub mod reader;
pub mod scan;
pub mod segfile;
pub mod writer;

use ::datum::Datum;
use ::types_error::{PgError, PgResult};

pub use format::ColType;
pub use scan::{CbDictLane, CbScanDescData, MetaAggScan, ZoneCmp, ZoneQual};
pub use writer::{coltypes_of, finish_bulk_insert, multi_insert, tuple_insert};

pub fn rel_main_path(rel: &::types_rel::Relation<'_>) -> String {
    relpath::GetRelationPath(
        rel.rd_locator.get(),
        rel.rd_backend,
        ::types_core::ForkNumber::MAIN_FORKNUM,
    )
}

// 4B-U or 1B in-line varlena payload bytes; external/compressed refuse (COPY
// is the supported load path and produces in-line 4B-U images).
pub fn varlena_bytes<'a>(d: Datum) -> PgResult<&'a [u8]> {
    unsafe {
        let p = d.as_usize() as *const u8;
        let b0 = *p;
        if b0 & 0x01 != 0 {
            if b0 == 0x01 {
                return Err(Box::new(PgError::error(
                    "cbstore: TOASTed input value; load via COPY".to_string(),
                )));
            }
            let len = ((b0 >> 1) & 0x7f) as usize;
            return Ok(std::slice::from_raw_parts(p.add(1), len - 1));
        }
        if b0 & 0x02 != 0 {
            return Err(Box::new(PgError::error(
                "cbstore: compressed input value; load via COPY".to_string(),
            )));
        }
        let word = (p as *const u32).read_unaligned();
        let len = (word >> 2) as usize;
        Ok(std::slice::from_raw_parts(p.add(4), len - 4))
    }
}

// Footer row count for planner sizing (cbstore-impl.md §7.2); None while
// the table has no committed footer. Served from the session part cache.
pub fn footer_rows(rel: &::types_rel::Relation<'_>) -> PgResult<Option<u64>> {
    Ok(part_cache::cached_part(rel)?.map(|p| p.total_rows()))
}

// Ingest-time per-column NDV from the part footer (v2 parts only); per-entry
// 0 = unknown. ANALYZE prefers these over the sampled Duj1 estimate.
pub fn footer_ndv(rel: &::types_rel::Relation<'_>) -> PgResult<Option<Vec<u64>>> {
    let ncols = writer::coltypes_of(rel)?.len();
    reader::part_footer_ndv(&rel_main_path(rel), ncols)
}

// v5 whole-part per-column sorted-asc flags for planner pathkey derivation;
// None while the table has no committed footer. Pre-v5 parts read all-false.
pub fn footer_sorted(rel: &::types_rel::Relation<'_>) -> PgResult<Option<Vec<bool>>> {
    Ok(part_cache::cached_part(rel)?.map(|p| p.sorted.iter().map(|&s| s == 1).collect()))
}

pub fn unsupported(what: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("cbstore does not support {what}"))
            .with_sqlstate(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}
