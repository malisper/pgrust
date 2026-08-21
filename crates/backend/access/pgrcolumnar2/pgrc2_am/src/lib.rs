//! # pgrc2_am — the pgrcolumnar2 table-AM glue (chunk M3-H)
//!
//! DDL/AM integration for the pgrcolumnar2 storage system
//! (`docs/design/lanev3-m3-chunks.md` §2/§5 M3-H row; format spec
//! `docs/design/pgrc2-format.md`, cited as `spec §N`): the `pgrcolumnar2`
//! AM identity (O-M3-2: sibling name, one DDL-minted pg_am row during
//! coexistence, NO format-routing knob), the O-7 per-table directory
//! lifecycle, COPY routing into the frozen M3-D writer face, the scan face
//! over the frozen M3-F reader, and the O-8/O-M3-1 typed-refusal surfaces.
//!
//! ## The settled design (recorded here, cited by the lane report)
//!
//! - **Registration is the closed-AM name probe** (the old-AM precedent,
//!   `docs/design/pgrcolumnar-impl.md` §7.1): a user-minted pg_am row
//!   `CREATE ACCESS METHOD pgrcolumnar2 TYPE TABLE HANDLER
//!   heap_tableam_handler` is recognized by `pg_am.amname == "pgrcolumnar2"`
//!   at relcache entry build and recorded in `tableam_vocab`'s sibling
//!   registry; handlers are never invoked. No new pg_proc/fmgr rows — the
//!   se-entrycost surface is untouched by construction.
//! - **O-7 directory lifecycle.** Table data lives in real files in a
//!   per-table directory, a SIBLING of the main-fork path
//!   ([`dirpath::table_dir_path`]; names frozen in
//!   `pgrc2_format::dirlayout`). The main fork itself stays a normal empty
//!   smgr file (created/dropped by the ordinary catalog machinery), which is
//!   what kills the #142/#136/#152 main-fork-squatting incident classes. The
//!   DIRECTORY's transactional lifecycle is this crate's pendingDeletes
//!   mirror ([`session`]): created lazily at first ingest (delete-at-abort
//!   registered), scheduled delete-at-commit on DROP and on
//!   relfilelocator swap (TRUNCATE / SET AM rewrite), with subxact
//!   reparenting mirroring `AtSubCommit_smgr`/`AtSubAbort_smgr`. Crash
//!   residue INSIDE a directory is reclaimed by
//!   `pgrc2_write::publish::recover_and_clean` at the FIRST open of the
//!   directory per postmaster lifetime — reader or writer, whichever comes
//!   first, through [`inval::ensure_dir_recovered`] (the #480 gap wiring:
//!   spec §13.3 "recovery runs before readers"; a pure SELECT after a crash
//!   must never see the reader walk's strict `ManifestMissing` refusal on a
//!   healthy table). Later opens cost one map probe, zero syscalls
//!   (manifest-driven, spec §13.3 dead-band discipline). A whole directory
//!   orphaned by a crash of its creating transaction matches PostgreSQL's
//!   own orphan-relfilenode posture (unreferenced, never rescanned; stated,
//!   not hidden).
//! - **COPY + SELECT only (O-M3-1a).** `table_tuple_insert`/`multi_insert`
//!   route here from COPY and the bulk receivers (CTAS/matview refresh —
//!   they end in `table_finish_bulk_insert`, the publish point). Trickle
//!   INSERT/UPDATE/DELETE refuse TYPED at the ModifyTable gate before any
//!   AM call; UPDATE/DELETE/lock/ON CONFLICT arms in tableam refuse typed
//!   as well (belt and braces). TID scans / WHERE CURRENT OF / bitmap /
//!   sample scans refuse typed per the exactness-sweep C12 posture — clean
//!   errors, never an invalid TID.
//! - **Visibility is the old-AM law at manifest grain** ([`probe`]):
//!   a generation is visible iff its `publisher_fxid`'s xid is the current
//!   transaction's, or committed AND not in the scan snapshot
//!   (`XidInMVCCSnapshot`). Walking the `prev_gen` chain past invisible
//!   generations yields exactly the table state the snapshot may see
//!   (manifests are append-only states).
//! - **Publish serialization**: generational manifests require one publisher
//!   at a time per table; pgrust is thread-per-backend in ONE process, so a
//!   process-global per-table mutex ([`inval::publish_lock`]) held across
//!   `TableWriter::publish` is sufficient single-node serialization (O-3
//!   scope). Documented limit: cross-process writers (none exist) would
//!   need a heavyweight lock.
//! - **The ino-reuse hole is closed here** ([`inval`]): every part opened
//!   for a scan records its `(dev, ino, len)` cache key under the relation
//!   oid; the relcache invalidation callback (register-BEFORE-first-insert
//!   law, the part_cache precedent) drops those keys from the shared
//!   `PartRegistry`, so a DROP+recreate that reuses an inode at equal
//!   length can never serve stale bytes to a later scan.
//! - **Session state is one `thread_local!` block** ([`session`]) — the
//!   TLS census counts declaration blocks; this crate adds exactly one
//!   (ledgered in the session-surface census test).
//!
//! ## Not here (owned elsewhere)
//!
//! Codec kernels/elections (M3-C; with none wired into the writer's
//! `CandidateSource` seam yet, COPY through this crate elects
//! VERBATIM/CONST only — a REPORTED composition gap, not a silent one),
//! real meta builders (M3-E), the lx_source columnar implementor and
//! parallel claims at execution grain (M3-G), parallel ingest (M3-I),
//! trickle DML/tombstones/DV (M5).

pub use pgrc2_format as format;

pub mod analyze;
pub mod dirpath;
pub mod dml;
pub mod factcache;
pub mod footer;
pub mod ingest;
pub mod inval;
pub mod probe;
pub mod scan;
pub mod schema;
pub mod session;

#[cfg(test)]
mod tests;

use types_error::PgError;

/// The SQL-surface AM name (O-M3-2 ruling: sibling name, frozen).
pub const AM_NAME: &str = "pgrcolumnar2";

/// Typed refusal, the ONE error identity for every unsupported surface
/// (exactness-sweep C12 posture: clean 0A000, never an invalid TID, never a
/// panic on a reachable path). Message shape mirrors the old AM's frozen
/// `cbstore does not support {what}` with the sibling identity.
pub fn unsupported(what: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("pgrcolumnar2 does not support {what}"))
            .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

/// Map a writer error to a PgError with a class-honest sqlstate: refusals
/// are 0A000, format/manifest damage is data corruption, I/O is io error.
pub fn write_error(e: pgrc2_write::WriteError) -> Box<PgError> {
    use pgrc2_write::WriteError as W;
    let sqlstate = match &e {
        W::Refused { .. } => types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
        W::Format(_) | W::TwoWitnessSkew { .. } | W::RoundTrip { .. } | W::ManifestChain { .. } => {
            types_error::ERRCODE_DATA_CORRUPTED
        }
        W::Io { .. } => types_error::ERRCODE_IO_ERROR,
        W::Contract { .. } => types_error::ERRCODE_INTERNAL_ERROR,
    };
    Box::new(PgError::error(format!("{e}")).with_sqlstate(sqlstate))
}

/// Map a reader error likewise.
pub fn read_error(e: pgrc2_read::ReadError) -> Box<PgError> {
    use pgrc2_read::ReadError as R;
    let sqlstate = match &e {
        R::Unsupported { .. } => types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
        R::Io { .. } => types_error::ERRCODE_IO_ERROR,
        _ => types_error::ERRCODE_DATA_CORRUPTED,
    };
    Box::new(PgError::error(format!("{e}")).with_sqlstate(sqlstate))
}
