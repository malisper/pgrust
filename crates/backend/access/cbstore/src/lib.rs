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
pub use scan::{
    CbDictLane, CbGranuleMetaStep, CbScanDescData, MetaAggScan, MetaZeroQual, ZoneCmp, ZoneQual,
};
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

// Per-column on-disk chunk bytes summed over the part's committed row
// groups (planner column-fraction seqscan disk costing). Within an RG the
// column chunks are laid out contiguously in column order
// (writer flush: chunk_off is the running body offset), so column i's bytes
// are the offset delta to column i+1's chunk; the last column runs to the
// end of the RG body (the next RG's file_off, or this footer's offset for
// the newest RG). Stale interior footers left by earlier COPY batches
// inflate at most the last column of each batch's tail RG — noise at
// costing precision. None while the table has no committed footer.
pub fn footer_col_bytes(rel: &::types_rel::Relation<'_>) -> PgResult<Option<Vec<u64>>> {
    let Some(part) = part_cache::cached_part(rel)? else { return Ok(None) };
    let ncols = part.ncols;
    let mut out = vec![0u64; ncols];
    for (r, rg) in part.rgs.iter().enumerate() {
        let rg_end = part.rgs.get(r + 1).map_or(part.footer_off, |next| next.file_off);
        let body_len = rg_end.saturating_sub(rg.file_off);
        for i in 0..ncols {
            let start = rg.chunks[i].0.min(body_len);
            let end = if i + 1 < ncols { rg.chunks[i + 1].0 } else { body_len };
            out[i] += end.min(body_len).saturating_sub(start);
        }
    }
    Ok(Some(out))
}

pub fn unsupported(what: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("cbstore does not support {what}"))
            .with_sqlstate(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}
