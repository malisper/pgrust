//! COPY routing into the frozen M3-D writer face (`TableWriter::append_row`
//! → `finish` → `publish`), with the per-(fxid,cid) `WriterRegistry`
//! discipline and the O-7 lazy directory creation.
//!
//! Reachability (O-M3-1a): `table_tuple_insert` / `table_multi_insert` on a
//! pgrcolumnar2 relation are reachable from COPY FROM and the bulk
//! receivers (CTAS / matview refresh) ONLY — trickle DML refuses typed at
//! the ModifyTable gate before any AM call. All of them end in
//! `table_finish_bulk_insert` → [`finish_bulk`], the publish point.
//!
//! ## Election posture (the M3-A2 full-registry wiring, M3-J)
//!
//! `SealEnv.sources` carries the full-registry [`CodecCandidates`] source
//! and `SealEnv.resolver` is [`CodecResolver`] (PR #474's contract): parts
//! written through COPY carry real BYTE_FOR/ALP/ALP_RD/DELTA_FOR/
//! DICT_CODES/PACKED_NUMERIC/LZ4 elections and seal-verify resolves the
//! SAME vtables a conformant reader will. Per-column postures are derived
//! from catalog facts in [`codec_candidates`] (numeric unlocks the
//! PACKED_NUMERIC arm; dict election only where the schema-derived
//! collation class is `C`, so code-eq == value-eq — the byte-semantics
//! lattice `DICT_EXEC` additionally requires the seal-time zero-null
//! proof). The candidates are cached per relfilenumber in the session
//! block (a pure function of the relation schema; purged with the writers
//! at transaction end).
//!
//! ## The #253 commit fence
//!
//! `TableWriter::publish` arms `xact_seams::force_sync_commit` when
//! installed (always, in a real backend). [`finish_bulk`] logs the
//! `PublishOutcome` witness at DEBUG1 — the e2e battery greps
//! `commit_fence_armed=true` — and REFUSES a publish whose fence came back
//! unarmed inside a live backend (the D-lane field report witnessed the
//! unarmed path only in unit tests; in a backend an unarmed fence would
//! mean acked COPY data that a kill -9 can lose).

use std::sync::Arc;

use pgrc2_write::dict::TextSemantics;
use pgrc2_write::elect::{CandidateSource, CodecCandidates, ColumnPosture, DictPolicy};
use pgrc2_write::ingest::{NoExternalDetoast, RawDatum};
use pgrc2_write::par::{
    NoDetoastProvider, ParProviders, RealVfsProvider,
};
use pgrc2_write::seal::CodecResolver;
use pgrc2_write::shred_jsonb::JsonbShredSource;
use pgrc2_write::writer::{PartCutPolicy, SealEnv, SubxactEvidence, TableWriter, TxnStamp};
use pgrc2_write::wvfs::RealVfs;
use pgrc2_format::relopt::ShredOptions;
use types_error::{PgError, PgResult};
use types_rel::Relation;
use types_tuple::varatt::varsize_any;

use crate::probe::ClogTxnProbe;
use crate::{session, write_error};

/// The provisional dict NDV cap. The §9 ledger marks the cap RE-MEASURE
/// under global-code dictionaries before a tuned default ships; 100k is the
/// witnessed LowCardinality-cliff anchor, recorded in every bank MANIFEST
/// via the recipe version so a re-fit is a clean manifest diff (the O-M3-3
/// FSST precedent).
const DICT_NDV_CAP: u64 = 100_000;

/// Full-registry election postures from catalog facts (pure function of
/// the relation schema; cached per relfilenumber in the session block).
///
/// - `cold: true` is the STORAGE posture, every column (CMP-B election
///   widening, ruled 2026-08-10 — `docs/design/pgrc2-compression.md` §3
///   CMP-B row): pgrc2 sealed parts ARE the frozen storage tier, so the
///   cold/size arm (DELTA_FOR — the frozen-matrix "cold/size election
///   only" encoding) competes on EVERY int-family stream and wins exactly
///   where its exact bytes clear the standing ≥10% law (elections change
///   layout, never answers; scan cost is priced by the arc's P1 bar).
/// - `numeric`: exactly the numeric-typed columns (unlocks PACKED_NUMERIC).
/// - `dict`: text-family varlena columns whose schema-derived collation
///   class is `C` (attcollation "C"/"POSIX"; the conservative
///   `OtherDeterministic` arm never dict-elects — code-eq == value-eq is
///   the caller-asserted lattice and byte equality only IS value equality
///   under C collation). `exec_ok` rides the same proof; seal additionally
///   gates `DICT_EXEC` on the zero-null witness.
/// - `fused` stays false: the M3 scan consumes flat decode and the S4
///   verdict (FFOR wins fused, loses flat 1.9–2.7×) STANDS until the
///   CMP-B oracle census overturns it with evidence — the census reports
///   FFOR's marginal size attribution for exactly that adjudication.
///   (The wrapper offer no longer rides any posture flag: O-CMP-3(a),
///   landed by CMP-A, offers it on every election.)
fn codec_candidates(rel: &Relation<'_>) -> PgResult<CodecCandidates> {
    use types_core::catalog::{BPCHAROID, BYTEAOID, NUMERICOID, TEXTOID, VARCHAROID};
    let schemas = crate::schema::col_schemas(rel)?;
    let atts = &rel.rd_att.attrs;
    /// The bank/storage posture: cold size arms offered by default.
    const STORAGE_POSTURE: ColumnPosture = ColumnPosture {
        fused: false,
        cold: true,
        numeric: false,
        dict: None,
    };
    let mut cands = CodecCandidates::new(STORAGE_POSTURE);
    for s in &schemas {
        let att = &atts[(s.attno - 1) as usize];
        let mut p = STORAGE_POSTURE;
        if att.atttypid == NUMERICOID {
            p.numeric = true;
        }
        let text_family = matches!(att.atttypid, TEXTOID | VARCHAROID | BPCHAROID | BYTEAOID);
        // [sqe-bpchar] code-eq == value-eq holds for bpchar ONLY under the
        // uniform-padding witness (atttypmod >= 5 pads every stored image
        // to the declared char width). BARE bpchar stores unpadded: 'ab'
        // and 'ab ' are byte-distinct dict codes that compare EQUAL under
        // bpchareq — asserting DICT_EXEC there would be the wrong-answer
        // class the §7 lattice exists to prevent. (Latent before the
        // pad-aware lane: the seam refused every bpchar shape, so the
        // flag was never consumed; pinned now that group-on-codes rides.)
        let bpchar_unpadded = att.atttypid == BPCHAROID && att.atttypmod < 5;
        if text_family
            && matches!(s.class, pgrc2_format::class::StorageClass::VarlenaVerbatim)
            && s.collation_class == pgrc2_format::class::CollationClass::C
        {
            // FSST-UNLOCK: TEXT/VARCHAR under a UTF-8 database CLAIM
            // Utf8Chars — the claim is catalog-founded (PG's input
            // functions verify server encoding on every value), but the
            // election VERIFIES it per part before any char-length fact
            // or FSST arm rides it (`resolve_text_policy`, elect.rs) —
            // verified, never trusted. The encoding gate is REQUIRED, not
            // belt-and-braces: in a non-UTF8 database (e.g. LATIN1) the
            // bytes can be accidentally UTF-8-valid while the true char
            // semantics are the server encoding's — lead-byte counts
            // would be wrong facts. bytea keeps byte semantics by type;
            // bpchar stays BytesOnly (space-padded images — its char
            // facts are not the length() surface, spec §7 lattice).
            // `utf8_claims_born()` gates the claim's BIRTH (default OFF:
            // the retired unlock — the M5d DECLINE — OR the Option-C
            // re-pose probe; the blessed lineage stays the default-cut
            // posture).
            let utf8_text = pgrc2_write::elect::utf8_claims_born()
                && matches!(att.atttypid, TEXTOID | VARCHAROID)
                && mbutils::GetDatabaseEncoding() == wchar::PG_UTF8;
            p.dict = Some(DictPolicy {
                ndv_cap: DICT_NDV_CAP,
                // Bare bpchar keeps storage-grade dedup only (see above).
                exec_ok: !bpchar_unpadded,
                sem: if utf8_text {
                    TextSemantics::Utf8Chars
                } else {
                    TextSemantics::BytesOnly
                },
            });
        }
        if p != STORAGE_POSTURE {
            cands = cands.with_column(s.attno, 0, p);
        }
        // [json-rung1] jsonb DERIVED shred lanes (path_ord ≥ 1): offer
        // the dict arm to the TEXT lanes. A text lane's values are the
        // verbatim jsonb string bytes under the lane lattice's C-class
        // byte law (shredlane::col_schema: TextCollated / CollationClass
        // ::C) — code-eq IS value-eq, so `exec_ok` holds; the seal still
        // gates DICT_EXEC on the zero-null proof. BytesOnly claims no
        // char facts. Byval lanes (NumericFs mantissa words) never see
        // the dict arm (the election is class-gated to VarlenaVerbatim);
        // the image column itself keeps dict: None (jsonb-as-value:
        // code-eq ≠ value-eq, the spec §7 lattice).
        if att.atttypid == types_core::catalog::JSONBOID
            && matches!(s.class, pgrc2_format::class::StorageClass::VarlenaVerbatim)
        {
            let mut lp = STORAGE_POSTURE;
            lp.dict = Some(DictPolicy {
                ndv_cap: DICT_NDV_CAP,
                exec_ok: true,
                sem: TextSemantics::BytesOnly,
            });
            cands = cands.with_lane_default(s.attno, lp);
        }
    }
    Ok(cands)
}

/// Run `f` with the cached (or freshly derived) candidates for `rel`.
fn with_rel_candidates<R>(
    rel: &Relation<'_>,
    relfilenumber: u64,
    f: impl FnOnce(&CodecCandidates) -> R,
) -> PgResult<R> {
    let have = session::with_candidates(|m| m.contains_key(&relfilenumber));
    if !have {
        let built = codec_candidates(rel)?;
        session::with_candidates(|m| m.insert(relfilenumber, built));
    }
    Ok(session::with_candidates(|m| {
        f(m.get(&relfilenumber).expect("just inserted"))
    }))
}

fn current_stamp() -> PgResult<TxnStamp> {
    // The CURRENT (sub)transaction's full xid, not the top's: a publish
    // from inside a savepoint must become invisible when that savepoint
    // rolls back even though the top transaction commits — clog carries the
    // subabort verdict only on the SUBXACT xid (the DDL battery's
    // savepoint leg witnessed the top-fxid form leaking rows).
    let fxid = xact::GetCurrentFullTransactionId()?;
    let cid = xact_seams::get_current_command_id::call(false)?;
    Ok(TxnStamp {
        fxid: fxid.value,
        cid,
    })
}

fn subxact_evidence(rel: &Relation<'_>) -> SubxactEvidence {
    let cur = xact_seams::get_current_sub_transaction_id::call();
    SubxactEvidence {
        cur_subxact_valid: cur != 0,
        rel_created_in_cur_subxact: rel.rd_createSubid.get() == cur && cur != 0,
        new_relfilelocator_in_cur_subxact: rel.rd_newRelfilelocatorSubid.get() == cur && cur != 0,
    }
}

fn open_writer(rel: &Relation<'_>, stamp: TxnStamp) -> PgResult<TableWriter> {
    let locator = rel.rd_locator.get();
    let dir = crate::dirpath::table_dir_path(locator, rel.rd_backend);
    // O-7 lazy creation: mkdir on first ingest; a creation registered for
    // delete-at-abort. Crash residue inside a pre-existing dir is reclaimed
    // through the shared once-per-lifetime guard (which also holds the
    // publish lock — recover_and_clean's no-concurrent-publisher
    // precondition, which the old per-open direct call here violated).
    if crate::dirpath::mkdir_if_absent(&dir)? {
        session::schedule_dir_delete_at_abort(dir.clone());
        // Born clean: created THIS lifetime, nothing to recover.
        crate::inval::mark_dir_recovered(&dir);
    } else {
        let probe = ClogTxnProbe::new();
        crate::inval::ensure_dir_recovered(locator.relNumber as u64, &dir, &probe)?;
    }
    let schemas = crate::schema::col_schemas(rel)?;
    TableWriter::open(
        dir,
        schemas,
        locator.spcOid,
        locator.dbOid,
        locator.relNumber as u64,
        stamp,
        &subxact_evidence(rel),
        PartCutPolicy::default(),
    )
    .map_err(write_error)
}

/// Build one writer-face row image from a deformed slot row. By-reference
/// datums are passed as raw images; varlena images may arrive in any inline
/// toast form (the writer normalizes; external pointers refuse typed —
/// unreachable from COPY, whose input datums are always inline).
///
/// SAFETY of the pointer reads: the slot is materialized/deformed by the
/// caller; byref datums point at live images for the duration of the call
/// (the writer copies what it keeps).
///
/// Public since M4-S3b: the engine parallel-COPY driver (copy_cmd ->
/// pgrc2_ingest_par) builds its capture rows through EXACTLY this
/// conversion — one row image law for the serial and parallel arms.
pub fn raw_row<'a>(
    rel: &Relation<'_>,
    values: &'a [datum::Datum],
    isnull: &'a [bool],
) -> PgResult<Vec<RawDatum<'a>>> {
    let atts = &rel.rd_att.attrs;
    if values.len() < atts.len() || isnull.len() < atts.len() {
        return Err(Box::new(PgError::error(
            "pgrcolumnar2: slot narrower than relation descriptor".to_string(),
        )));
    }
    let mut out = Vec::with_capacity(atts.len());
    for (i, att) in atts.iter().enumerate() {
        if isnull[i] {
            out.push(RawDatum::Null);
            continue;
        }
        if att.attbyval {
            out.push(RawDatum::Word(values[i].as_u64()));
        } else if att.attlen > 0 {
            let p = values[i].as_u64() as *const u8;
            // SAFETY: fixed-length byref datum — attlen readable bytes.
            let img = unsafe { core::slice::from_raw_parts(p, att.attlen as usize) };
            out.push(RawDatum::Bytes(img));
        } else if att.attlen == -1 {
            let p = values[i].as_u64() as *const u8;
            // SAFETY: varlena datum — header-readable; varsize_any gives
            // the full inline image length for 1B/4B forms.
            let b0 = unsafe { *p };
            if b0 == 0x01 {
                // 1B_E external/indirect pointer: COPY never produces one;
                // refuse typed rather than size an unknown tag.
                return Err(crate::unsupported(
                    "ingesting externally-toasted datums (detoast capability arrives with the DML sink)",
                ));
            }
            let len = unsafe { varsize_any(p) };
            // SAFETY: inline varlena image — len readable bytes.
            let img = unsafe { core::slice::from_raw_parts(p, len) };
            out.push(RawDatum::Bytes(img));
        } else {
            return Err(crate::unsupported("cstring-typlen columns"));
        }
    }
    Ok(out)
}

/// The TY-3 shred arm's registration (JSON-routing landing, the OD-4
/// two-arm election's write wiring): register every jsonb column of the
/// relation on the production [`JsonbShredSource`] — catalog knowledge
/// (`atttypid`) the writer cannot derive from `ColSchema`, exactly the
/// `DictPolicy` posture. A pure function of the relation schema, so the
/// serial seals, the statement finish, and the parallel providers can
/// never disagree. Non-jsonb tables register nothing (the source leaves
/// unregistered columns alone — the NoShred-equivalent degenerate).
/// Budgets ride `ShredOptions::default()` until the reloption plumbing
/// lands at this seam (`pgrc2_shred_max_paths`/`pgrc2_shred_paths` —
/// spec §17 vocabulary, noted in the routing doc).
fn shred_source_for(rel: &Relation<'_>) -> JsonbShredSource {
    let mut s = JsonbShredSource::new();
    for a in crate::schema::jsonb_attnos(rel) {
        s = s.with_column(a);
    }
    s
}

/// Ingest one deformed row (the `table_tuple_insert` / `table_multi_insert`
/// arm body). The caller has already deformed the slot
/// (`slot_getallattrs`).
pub fn ingest_row(
    rel: &Relation<'_>,
    values: &[datum::Datum],
    isnull: &[bool],
) -> PgResult<()> {
    session::ensure_session_hooks();
    crate::inval::ensure_inval_registered()?;
    let stamp = current_stamp()?;
    let relfilenumber = rel.rd_locator.get().relNumber as u64;
    let row = raw_row(rel, values, isnull)?;
    let mut vfs = RealVfs;
    let resolver = CodecResolver;
    let mut shred = shred_source_for(rel);
    let shred_opts = ShredOptions::default();

    let mut open_err: Option<Box<PgError>> = None;
    let res = with_rel_candidates(rel, relfilenumber, |cands| {
        let sources: [&dyn CandidateSource; 1] = [cands];
        session::with_writers(|w| {
            let writer = match w.get_or_open(&mut vfs, relfilenumber, stamp, || {
                open_writer(rel, stamp).map_err(|e| {
                    open_err = Some(e);
                    pgrc2_write::WriteError::Contract {
                        detail: "writer open failed (see raised error)",
                    }
                })
            }) {
                Ok(w) => w,
                Err(e) => return Err(e),
            };
            let mut env = SealEnv {
                vfs: &mut vfs,
                sources: &sources,
                resolver: &resolver,
                shred: &mut shred,
                shred_opts: &shred_opts,
            };
            writer.append_row(&row, &mut NoExternalDetoast, &mut env)
        })
    })?;
    if let Some(e) = open_err {
        return Err(e);
    }
    res.map_err(write_error)
}

/// Statement-end publish (the `table_finish_bulk_insert` arm body): seal
/// the remainder, publish ONE manifest generation under the per-table
/// publish lock, witness the outcome. No-op when this statement buffered
/// nothing (heap relations pass through long before reaching here).
pub fn finish_bulk(rel: &Relation<'_>) -> PgResult<()> {
    let cur = xact::GetCurrentFullTransactionIdIfAny();
    if !cur.is_valid() {
        // No xid ⇒ no writer could have been opened by this statement.
        return Ok(());
    }
    let cid = xact_seams::get_current_command_id::call(false)?;
    let stamp = TxnStamp {
        fxid: cur.value,
        cid,
    };
    let relfilenumber = rel.rd_locator.get().relNumber as u64;
    let mut vfs = RealVfs;
    let taken = session::with_writers(|w| w.take_for_publish(&mut vfs, relfilenumber, stamp))
        .map_err(write_error)?;
    let Some(mut writer) = taken else {
        return Ok(());
    };
    let resolver = CodecResolver;
    let mut shred = shred_source_for(rel);
    let shred_opts = ShredOptions::default();
    with_rel_candidates(rel, relfilenumber, |cands| {
        let sources: [&dyn CandidateSource; 1] = [cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &resolver,
            shred: &mut shred,
            shred_opts: &shred_opts,
        };
        writer.finish(&mut env)
    })?
    .map_err(write_error)?;
    if writer.sealed_parts().is_empty() {
        return Ok(());
    }
    // M5a: capture the Stats-sidecar payloads BEFORE publish clears the
    // writer (the `publish_stats_sidecars` caller contract). The ENGINE
    // path previously dropped `SealedPart.stats_payload` on the floor —
    // only the library harness published the companions, so every
    // engine-loaded table violated the ST-1 "stats are a seal byproduct;
    // banks ship stats-built" law and the M5a ANALYZE fold declined
    // (sidecar-absent) on engine-cut banks — the witness that found this.
    let stats_payloads: Vec<Vec<u8>> =
        writer.sealed_parts().iter().map(|p| p.stats_payload.clone()).collect();
    let lock = crate::inval::publish_lock(relfilenumber);
    let guard = pgsync::lock(&lock);
    let probe = ClogTxnProbe::new();
    let outcome = writer.publish(&mut vfs, &probe).map_err(write_error)?;
    drop(guard);
    probe.take_error()?;
    // Post-publish companions (the harness pattern; derived class — a
    // crash between publish and here is a reclaimable/regenerable gap,
    // never a torn manifest).
    {
        let schemas = crate::schema::col_schemas(rel)?;
        let fp = pgrc2_format::ident::schema_fingerprint(&schemas);
        let dir = crate::dirpath::table_dir_path(rel.rd_locator.get(), rel.rd_backend);
        let pairs: Vec<(u32, &[u8])> = outcome
            .part_nos
            .iter()
            .copied()
            .zip(stats_payloads.iter().map(|p| p.as_slice()))
            .collect();
        pgrc2_write::sidecar::publish_stats_sidecars(&mut vfs, &dir, fp, &pairs, stamp.fxid)
            .map_err(write_error)?;
    }
    let _ = elog::elog(
        types_error::DEBUG1,
        format!(
            "pgrc2 publish: rel={} gen={} parts={} commit_fence_armed={}",
            rel.name(),
            outcome.gen,
            outcome.part_nos.len(),
            outcome.commit_fence_armed
        ),
    );
    if !outcome.commit_fence_armed {
        // Inside a backend the force_sync_commit seam is ALWAYS installed
        // (xact::init at boot); an unarmed fence here means acked bytes a
        // kill -9 could lose — refuse loudly instead of acking.
        return Err(Box::new(
            PgError::error(
                "pgrcolumnar2: publish completed without the synchronous-commit fence armed"
                    .to_string(),
            )
            .with_sqlstate(types_error::ERRCODE_INTERNAL_ERROR),
        ));
    }
    Ok(())
}

/// The parallel-COPY writer seam (M4X parquet-parallel lane). Opens (or
/// re-uses) the statement writer through EXACTLY the serial registry
/// discipline — same `open_writer`, same stamp, same lazy-mkdir/recovery —
/// builds the production [`ParProviders`] from the SAME election facts
/// serial seals use (full-registry [`CodecCandidates`] rebuilt from catalog
/// facts — a pure function of the relation schema, identical to the cached
/// copy `ingest_row` consults — [`CodecResolver`], no-detoast, and the
/// catalog-driven jsonb shred registration [`shred_source_for`] — the
/// same source serial seals consult),
/// and runs `f` with both.
///
/// The driver sits at executor altitude (copy_cmd → pgrc2_ingest_par —
/// this crate is below `runtime` and can never host the rtpool binding,
/// the M3-H split-crate law) and MUST deposit through
/// [`TableWriter::deposit_sealed_parts`] (the `parallel_ingest`
/// convenience does). On success the writer is exactly where an
/// equivalent serial statement would have left it; publish stays
/// [`finish_bulk`] via `table_finish_bulk_insert`, UNCHANGED — fence,
/// witness, publish lock and all.
///
/// Reentrancy contract: `f` runs INSIDE the session writer-registry
/// borrow for the whole parallel session; it must not re-enter any
/// pgrc2_am session face (the parallel engine takes everything it needs
/// as passed capabilities, so the production driver does not).
pub fn with_parallel_copy_writer<R>(
    rel: &Relation<'_>,
    f: impl FnOnce(&mut TableWriter, ParProviders) -> PgResult<R>,
) -> PgResult<R> {
    session::ensure_session_hooks();
    crate::inval::ensure_inval_registered()?;
    let stamp = current_stamp()?;
    let relfilenumber = rel.rd_locator.get().relNumber as u64;
    // Rebuilt, not cloned out of the session cache: codec_candidates is a
    // pure function of the relation schema (the cache exists only to skip
    // recomputation), so the workers' Arc and the serial cache can never
    // disagree.
    let cands = codec_candidates(rel)?;
    let providers = ParProviders {
        vfs: Arc::new(RealVfsProvider),
        detoast: Arc::new(NoDetoastProvider),
        shred: Arc::new(shred_source_for(rel)),
        sources: vec![Arc::new(cands)],
        resolver: Arc::new(CodecResolver),
        shred_opts: ShredOptions::default(),
        // v4 delta (TY-1): no structural elections at the AM ingest seam
        // yet — the parallel seal elects exactly as serial with the
        // default (none) policy; the structural planes wire at S3b.
        structural: Default::default(),
    };
    let mut vfs = RealVfs;
    let mut open_err: Option<Box<PgError>> = None;
    let mut body_out: Option<PgResult<R>> = None;
    let reg = session::with_writers(|w| {
        let writer = match w.get_or_open(&mut vfs, relfilenumber, stamp, || {
            open_writer(rel, stamp).map_err(|e| {
                open_err = Some(e);
                pgrc2_write::WriteError::Contract {
                    detail: "writer open failed (see raised error)",
                }
            })
        }) {
            Ok(w) => w,
            Err(e) => return Err(e),
        };
        body_out = Some(f(writer, providers));
        Ok(())
    });
    if let Some(e) = open_err {
        return Err(e);
    }
    reg.map_err(write_error)?;
    body_out.expect("writer body ran")
}
