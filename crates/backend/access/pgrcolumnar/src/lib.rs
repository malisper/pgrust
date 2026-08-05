//! pgrcolumnar: scan-only columnar table AM for the analytics charter
//! (docs/design/clickbench-format.md, docs/design/pgrcolumnar-impl.md).
//! Buffer-cache-bypass by design (approved): reads are mmap of the part
//! file, writes are direct pwrites + fsync; the main fork file's existence
//! stays owned by the ordinary smgr create/unlink machinery.

#![allow(non_snake_case)]
// wasm32: positioned file I/O (FileExt) and dev/ino identity (MetadataExt)
// live behind std's unstable wasi_ext; the wasm build runs on the pinned
// nightly (wasm/wasm-build.sh), native stable builds never see this.
#![cfg_attr(target_family = "wasm", feature(wasi_ext))]

pub mod bloom;
pub mod condcache;
pub mod format;
pub mod hll;
pub mod loadsort;
pub mod lz4dec;
pub mod part_cache;
pub mod reader;
pub mod scan;
pub mod segfile;
pub mod sortkey;
pub mod writer;

use ::datum::Datum;
use ::types_error::{PgError, PgResult};

pub use format::ColType;
pub use reader::dict_frame_stats;
pub use scan::{
    CbAggMetaStep, CbDictLane, CbGranuleMetaStep, CbScanDescData, MetaAggScan, MetaZeroQual,
    ZoneCmp, ZoneQual,
};
pub use writer::{
    at_eoxact, begin_parallel_ingest, coltypes_of, finish_bulk_insert, multi_insert, tuple_insert,
    CbWriter, ColOrderProbe, EncodedRg, ParallelIngestPlan, RgChunkEncoder,
};

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

// Footer row count for planner sizing (pgrcolumnar-impl.md §7.2); None while
// the table has no committed footer. Served from the session part cache.
pub fn footer_rows(rel: &::types_rel::Relation<'_>) -> PgResult<Option<u64>> {
    Ok(part_cache::cached_part(rel)?.map(|p| p.total_rows()))
}

// Ingest-time per-column NDV from the part footer (v2 parts only); per-entry
// 0 = unknown. ANALYZE prefers these over the sampled Duj1 estimate.
// Served from the session part cache like footer_rows/footer_sorted: the
// planner asks on EVERY plan of a pgrcolumnar rel (plancat footer-NDV group-key
// estimation), and the standalone part_footer_ndv path re-read + re-parsed
// the entire footer from disk per call (~2MB / ~1.3ms on the 100M bank —
// the dominant term of the ~1.9ms per-query plan constant; fixed-overhead
// audit 2026-07-14). Pre-v2 parts carry an empty ndv vec on Part — mapped
// back to None to preserve part_footer_ndv's exact contract.
pub fn footer_ndv(rel: &::types_rel::Relation<'_>) -> PgResult<Option<Vec<u64>>> {
    Ok(part_cache::cached_part(rel)?
        .and_then(|p| if p.ndv.is_empty() { None } else { Some(p.ndv.clone()) }))
}

// v8 per-column NDV register sketches (the inherited-ANALYZE union source:
// per-child NDV scalars cannot be combined, register sketches union
// losslessly). A direct targeted read (header + footer directory + blobs),
// NOT the session part cache: the consumer runs once per ANALYZE of a
// parent, and caching register images on every Part would tax the
// plan-time cache for it. None: no committed footer (zero committed rows).
// Per-column None entries: sketch absent (pre-v8 part, append-invalidated
// chain, or the writer kill switch).
pub fn footer_ndv_hll(
    rel: &::types_rel::Relation<'_>,
) -> PgResult<Option<Vec<Option<hll::Hll>>>> {
    let ncols = writer::coltypes_of(rel)?.len();
    reader::part_footer_ndv_hll(&rel_main_path(rel), ncols)
}

// v5 whole-part per-column sorted-asc flags for planner pathkey derivation;
// None while the table has no committed footer. Pre-v5 parts read all-false.
pub fn footer_sorted(rel: &::types_rel::Relation<'_>) -> PgResult<Option<Vec<bool>>> {
    Ok(part_cache::cached_part(rel)?.map(|p| p.sorted.iter().map(|&s| s == 1).collect()))
}

// Per-column on-disk chunk bytes summed over the part's committed row
// groups (planner column-fraction seqscan disk costing); None while the
// table has no committed footer. Served from the once-per-cached-Part
// computation (reader.rs Part::col_bytes, the walk moved there verbatim):
// the planner asks on EVERY plan of a pgrcolumnar rel, and the O(rgs x ncols)
// chunk-directory walk measured 85% of the replan loop / 76% of the whole
// serial per-query fixed path at 100M (fixedpath2 round 2) — ~0.2ms of the
// ~0.25ms post-footer_ndv plan constant. Identical values => identical
// plans.
pub fn footer_col_bytes(rel: &::types_rel::Relation<'_>) -> PgResult<Option<Vec<u64>>> {
    Ok(part_cache::cached_part(rel)?.map(|p| p.col_bytes().to_vec()))
}

// Every committed RG carries exact v7 zero/empty counts (q2box lane): the
// plan-time answerability probe for the meta zero-count qual arm
// (scan.rs meta_agg refuses whole-part on any RG without them). Served
// from the session part cache like its siblings above; None while the
// table has no committed footer.
pub fn footer_zerocnt_all(rel: &::types_rel::Relation<'_>) -> PgResult<Option<bool>> {
    Ok(part_cache::cached_part(rel)?.map(|p| p.zerocnt_all_rgs()))
}

// v7 per-column part-global stitch dict sizes (0 = no stitch for that
// column: pre-v7 part, non-dict column, or invalidated by append) — the
// SE-TOPNNI plan-time answerability probe for DictCode text sort keys (a
// text key without a stitch would engage the runtime sort sink and then
// contract-break at accept). Served from the session part cache like its
// siblings above; None while the table has no committed footer; empty on
// pre-v7 parts.
pub fn footer_stitch_gndv(rel: &::types_rel::Relation<'_>) -> PgResult<Option<Vec<u64>>> {
    Ok(part_cache::cached_part(rel)?.map(|p| p.stitch_gndv_all().to_vec()))
}

pub fn unsupported(what: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("cbstore does not support {what}"))
            .with_sqlstate(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}
