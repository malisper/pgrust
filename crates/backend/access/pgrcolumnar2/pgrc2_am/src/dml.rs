//! # dml — the M5e trickle-DML AM-face contract (M5e-SCOUT scaffold; NO
//! execution path)
//!
//! **Posture (the scout law):** nothing here is reachable — no tableam arm
//! calls these faces, no hook is registered, and the module changes no
//! behavior. It exists so the M5e build lane wires INSTEAD of surveys. The
//! survey artifact (producer map, copy sources, open questions) is
//! `docs/design/lanev4-m5e-scout.md`; the write-side producer faces are
//! `pgrc2_write::dml`.
//!
//! ## The dispatch sites this module's build-out claims (the S3 sweep's
//! chokepoints, `crates/backend/access/table/tableam/src/lib.rs`)
//!
//! - `table_tuple_insert` — the Pgrcolumnar2 arm today materializes +
//!   deforms the slot and buffers through `crate::ingest::ingest_row` (the
//!   CTAS/matview receiver leg). M5e re-routes the TRICKLE half into the
//!   DM-1 rowstore buffer; the bulk-receiver half stays on the writer path.
//! - `table_tuple_delete` / `table_tuple_update` — typed refusals today
//!   (`crate::unsupported`). M5e lands the tombstone write (DELETE) and
//!   tombstone + rowstore insert (UPDATE, delete+insert per the ruled
//!   design) behind them.
//! - `table_finish_bulk_insert` → `crate::ingest::finish_bulk` — the ONE
//!   statement-end publish point; the trickle flush joins it, never a
//!   second publish protocol.
//!
//! ## The v3 gate (scout finding F-1 — RESTORED)
//!
//! lanev3's M3-H ModifyTable gate (`nodemodifytable`
//! `check_valid_result_rel` @ `dc3c67c56214`, lines 1233–1248: INSERT/
//! UPDATE/DELETE/MERGE on a pgrcolumnar2 result relation refuse TYPED
//! before any AM call) was NOT carried into the v4 tableam port. Before
//! the restore, a trickle `INSERT INTO <pgrc2 table> VALUES …` reached the
//! Pgrcolumnar2 `tuple_insert` arm, buffered the row in the per-(fxid,cid)
//! `WriterRegistry`, and — because `mt_source_exhausted`'s statement-end
//! flush loop tests `is_pgrcolumnar_am_oid` (the v1 registry) only — was
//! never published: the registry's unconditional eoxact purge abandoned
//! it. An ACKED INSERT SILENTLY LOST ITS ROW. The gate is RESTORED (the
//! F-1 lane, `lx/f1-trickle-gate`): `check_valid_result_rel` calls
//! [`trickle_unsupported`] for a pgrcolumnar2 result relation before any
//! AM work, born-RED-proven at unit grain (nodemodifytable's
//! `check_valid_result_rel_tests`) and at real-backend grain
//! (`scripts/pgrc2-trickle-gate-e2e.sh`, which also pins the
//! ACKED-AND-LOST witness the pre-fix tree exhibits).
//!
//! ## Routing law (charter Amendment 6, clause 2)
//!
//! Single-row DML on HEAP tables stays classic — no columnar/JIT
//! engagement tax on point work (the ol* corpus shapes elect serial/classic,
//! witnessed by election census rows). The pgrc2 write face is what classic
//! ModifyTable CALLS through the tableam dispatch above; there is no
//! separate "columnar DML executor" — v3's `lx_dml` host lineage (OL-6,
//! #539/#706) is the OLTP-register's M5-tier concern, not an M5e
//! dependency.

use types_error::PgError;

/// The trickle-DML operation vocabulary at the AM face (append-only).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrickleOp {
    Insert,
    Delete,
    Update,
    Merge,
}

impl TrickleOp {
    /// The refusal-message noun, frozen to the v3 M3-H gate's wording so
    /// error-parity suites see ONE message shape across the lanes.
    pub fn refusal_what(self) -> &'static str {
        match self {
            TrickleOp::Insert => {
                "INSERT (bulk-load with COPY; trickle DML arrives with the M5 delta store)"
            }
            TrickleOp::Delete => "DELETE",
            TrickleOp::Update => "UPDATE",
            TrickleOp::Merge => "MERGE",
        }
    }
}

/// The typed trickle refusal (0A000 through the one [`crate::unsupported`]
/// identity) — what the restored ModifyTable gate raises until the M5e
/// build flips each operation live, and what each dispatch arm keeps for
/// the shapes it does not yet claim.
pub fn trickle_unsupported(op: TrickleOp) -> Box<PgError> {
    crate::unsupported(op.refusal_what())
}

#[cfg(test)]
mod dml_scaffold_tests {
    use super::*;
    use pgrc2_scan::PartDeletes;
    use pgrc2_write::dml::{DvCompile, DvTriplet};

    #[test]
    fn producer_payload_decodes_through_the_s3b_consumer_face() {
        // The DM-2 contract boundary end-to-end at crate grain: the
        // `pgrc2_write::dml::DvCompile` producer's payload + minted triplet
        // decode through the LANDED S3b consumer exactly as
        // `crate::scan`'s manifest-triplet validation hands them over.
        let mut c = DvCompile::new();
        c.mark(3, 7);
        c.mark(3, 4090);
        for r in 0..600u16 {
            c.mark(11, r);
        }
        let payload = c.encode(42, 5).expect("encode");
        let triplet = DvTriplet::for_payload(5, &payload);
        // The caller-side checks scan.rs performs before decode:
        assert_eq!(payload.len() as u64, triplet.dv_len);
        assert_eq!(pgrc2_format::wire::crc32c(&payload), triplet.dv_crc);
        let deletes =
            PartDeletes::from_dv_payload(&payload, 42, triplet.dv_gen, 16).expect("decode");
        assert_eq!(deletes.deleted_rows, 602);
        let sparse = deletes.granule_mask(3).expect("granule 3");
        assert!(PartDeletes::is_deleted(sparse, 7));
        assert!(PartDeletes::is_deleted(sparse, 4090));
        assert!(!PartDeletes::is_deleted(sparse, 8));
        let dense = deletes.granule_mask(11).expect("granule 11");
        assert!(PartDeletes::is_deleted(dense, 599));
        assert!(!PartDeletes::is_deleted(dense, 600));
        assert!(deletes.granule_mask(0).is_none());
        // Identity mismatches refuse typed (the corruption posture):
        assert!(PartDeletes::from_dv_payload(&payload, 41, triplet.dv_gen, 16).is_err());
        assert!(PartDeletes::from_dv_payload(&payload, 42, 4, 16).is_err());
        assert!(PartDeletes::from_dv_payload(&payload, 42, triplet.dv_gen, 11).is_err());
    }

    #[test]
    fn refusal_wording_matches_the_v3_gate() {
        let e = trickle_unsupported(TrickleOp::Insert);
        assert_eq!(
            e.message(),
            "pgrcolumnar2 does not support INSERT (bulk-load with COPY; \
             trickle DML arrives with the M5 delta store)"
        );
    }
}
