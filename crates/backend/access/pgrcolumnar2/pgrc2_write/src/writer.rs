//! The serial ingest face + writer lifecycle (chunk M3-D row: "per-(xid,cid)
//! writer lifecycle + eoxact purge, serial ingest face").
//!
//! ## Lifecycle discipline (the proven old-writer laws, carried verbatim)
//!
//! - **One writer per table per backend**, keyed by relfilenumber in a
//!   [`WriterRegistry`]; the txn stamp (fxid, cid) is a STALENESS FIELD, not
//!   part of the key. A stamp mismatch EVICTS the old writer by ABORT —
//!   buffered rows are dropped and its temp files unlinked, never published
//!   (an errored statement's writer must not leak rows into a later
//!   statement of the same transaction).
//! - **Publish is the taking statement's act**: [`WriterRegistry::
//!   take_for_publish`] removes the writer and re-checks the stamp; a stale
//!   writer is aborted, not published (the savepoint-rollback case).
//! - **eoxact purge is unconditional on COMMIT and ABORT alike**: anything
//!   still registered at transaction end is by construction abandoned — the
//!   successful statement already took its writer at publish. (M3-H
//!   registers the xact callback; the registry lives in the session
//!   envelope, never in a crate thread-local — the TLS census is pinned.)
//!
//! ## Freeze parity (the old-writer belt, decision-table-pinned)
//!
//! Frozen writes are sound only when an abort of the writing
//! (sub)transaction also unlinks the table's files: `cur_subxact` valid AND
//! (relation created in it OR its relfilelocator newly minted in it). The
//! belt SILENTLY DOWNGRADES otherwise (the loud C-parity error is COPY
//! FREEZE's, at the command layer). At M3 the decision is recorded as a
//! writer fact ([`FreezeDecision`]) with NO on-disk bit: part visibility is
//! the manifest clog fence, and a table created in the writing transaction
//! is invisible to every other snapshot anyway; where a frozen flag lands
//! on disk (manifest flags) is an A-lane transcription owed with the M5
//! visibility rung — reported, not invented here.
//!
//! ## The part-cut policy
//!
//! Whole-part buffering is bounded by [`PartCutPolicy`]: a part is cut when
//! the row or byte budget trips, tested ONLY at a `cut_granule_rows`
//! boundary. Cut points are a pure function of the accumulated rows —
//! determinism preserved (same input partition ⇒ same parts ⇒ same bytes).
//!
//! The granule gate is shared with the parallel cut cursor and is what
//! makes a parallel-loaded table byte-identical to a serial-loaded one
//! under EITHER budget; see [`PartCutPolicy`] for why an ungated serial cut
//! produced partitions `crate::par` could not reproduce.

use pgrc2_format::abi::ColumnMetaBuilder;
use pgrc2_format::class::{ColSchema, StorageClass};
use pgrc2_format::ident::schema_fingerprint;
use pgrc2_format::relopt::ShredOptions;
use std::collections::BTreeMap;

use crate::elect::CandidateSource;
use crate::ingest::{normalize_varlena, ColBuffer, ExternalDetoast, RawDatum};
use crate::publish::{publish_parts, PublishOutcome, TxnProbe};
use crate::seal::{seal_part, PartSpec, SealReport, SealedPart, VerifyResolver};
use crate::shred::{validate_lanes, ShredLaneSource};
use crate::structural::StructuralPolicy;
use crate::wvfs::WriteVfs;
use crate::{WriteError, WriteResult};

/// The txn identity stamp: epoch-qualified fxid (the manifest's clog-fence
/// currency) + command id. Passed capabilities — this crate never reads
/// transaction state itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TxnStamp {
    pub fxid: u64,
    pub cid: u32,
}

/// Subtransaction evidence for the freeze belt (M3-H passes the relcache
/// facts; tests script them).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SubxactEvidence {
    pub cur_subxact_valid: bool,
    pub rel_created_in_cur_subxact: bool,
    pub new_relfilelocator_in_cur_subxact: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FreezeDecision {
    Frozen,
    Downgraded,
}

/// The old-writer freeze belt, verbatim: frozen iff the current subxact is
/// valid AND the relation (or its relfilelocator) was created in it.
pub fn freeze_decision(e: &SubxactEvidence) -> FreezeDecision {
    if e.cur_subxact_valid
        && (e.rel_created_in_cur_subxact || e.new_relfilelocator_in_cur_subxact)
    {
        FreezeDecision::Frozen
    } else {
        FreezeDecision::Downgraded
    }
}

/// The default cut-decision granule, in rows.
///
/// Chosen to bound byte-budget overshoot (see [`PartCutPolicy`]): parts run
/// ~190k rows at ClickBench widths under the 256 MiB default, so an 8,192
/// granule overshoots by at most ~4%. The capture-chunk grain (65,536, one
/// band) would overshoot by up to ~34%.
pub const DEFAULT_CUT_GRANULE_ROWS: u32 = 8_192;

/// Deterministic part-cut bounds.
///
/// ## The granule gate (M3-I) — a correctness boundary, not a tuning knob
///
/// The budget predicate is evaluated ONLY at a `cut_granule_rows` boundary,
/// by BOTH ingest paths. This is what makes a parallel-loaded table
/// byte-identical to a serial-loaded one: the parallel cut cursor
/// ([`crate::par::ParEngine`]) can close a part only at a capture-chunk
/// boundary, so a serial writer free to cut after an arbitrary row would
/// produce partitions the parallel path cannot reproduce.
///
/// The gate is necessary but NOT sufficient (#597): the byte OPERAND both
/// paths feed [`PartCutPolicy::should_cut`] must also be identical — see
/// that method's doc for the operand law and the divergence it closed.
///
/// Before this gate the two agreed only when the ROW budget governed; under
/// the byte budget they legally diverged and only the O-10 logical multiset
/// identity held. That was the live case, not the corner — production runs
/// the 256 MiB byte budget, which governs at ClickBench row widths (the 10M
/// ingest-attribution run wrote 55 files where a row-governed cut gives
/// ~10), and a bank MANIFEST carries part geometry, so a divergent
/// partition breaks bank identity.
///
/// The gate costs overshoot: a part closes at the first granule boundary at
/// or after a budget trips, so it may exceed `max_bytes` by up to one
/// granule's worth of rows. That is the price of the identity law and is
/// bounded by [`DEFAULT_CUT_GRANULE_ROWS`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartCutPolicy {
    pub max_rows: u64,
    pub max_bytes: u64,
    /// Cut-decision granularity in rows (must be > 0). The parallel path
    /// pins its capture-chunk grain to exactly this value.
    pub cut_granule_rows: u32,
}

impl Default for PartCutPolicy {
    fn default() -> PartCutPolicy {
        PartCutPolicy {
            // 1 Mi rows = exactly 16 bands = exactly 128 default granules.
            max_rows: 1 << 20,
            max_bytes: 256 << 20,
            cut_granule_rows: DEFAULT_CUT_GRANULE_ROWS,
        }
    }
}

impl PartCutPolicy {
    /// True at a cut-decision boundary (never at zero rows).
    pub fn at_granule(&self, rows: u64) -> bool {
        rows > 0 && rows % self.cut_granule_rows as u64 == 0
    }

    /// THE cut predicate — the ONE implementation shared by serial ingest
    /// ([`TableWriter::append_row`]) and the parallel cut cursor
    /// ([`crate::par::ParEngine`]). Both paths must ask this and nothing
    /// else; a second copy of the budget arithmetic is how the partitions
    /// drift apart again.
    ///
    /// The OPERANDS are law too (#597): `bytes` must be the part-accumulated
    /// [`crate::ingest::ColBuffer::approx_bytes`] sum AS SERIAL INGEST
    /// OBSERVES IT (`TableWriter::buffered_bytes`). The parallel cursor once
    /// fed a chunk-LOCAL `approx_bytes` sum, which omits the heap pad8 the
    /// part assembler's splice inserts at each interior chunk seam
    /// (`ColBuffer::splice_chunk` — 0-7 bytes per byref column per seam);
    /// on varlena schemas where the byte budget governs, the skewed operand
    /// crossed `max_bytes` at a later granule boundary than serial and the
    /// two paths cut different partitions — byte-divergent banks from
    /// identical input. The cursor now replays the splice's pad arithmetic
    /// per column so both callers feed identical bytes at every granule
    /// boundary.
    ///
    /// Callers gate the O(ncols) byte scan on [`PartCutPolicy::at_granule`]
    /// first; the granule test is repeated here so the predicate is correct
    /// standalone.
    pub fn should_cut(&self, rows: u64, bytes: u64) -> bool {
        self.at_granule(rows) && (rows >= self.max_rows || bytes >= self.max_bytes)
    }
}

/// Everything a seal needs beyond the writer's own state — bundled so the
/// ingest face stays one call (`append_row` cuts parts internally when the
/// policy trips).
pub struct SealEnv<'a> {
    pub vfs: &'a mut dyn WriteVfs,
    pub sources: &'a [&'a dyn CandidateSource],
    pub resolver: &'a dyn VerifyResolver,
    pub shred: &'a mut dyn ShredLaneSource,
    pub shred_opts: &'a ShredOptions,
}

/// One table's buffered writer for one (fxid, cid) statement.
pub struct TableWriter {
    spec: PartSpec,
    table_dir: String,
    schema: Vec<ColSchema>,
    stamp: TxnStamp,
    freeze: FreezeDecision,
    policy: PartCutPolicy,
    /// Structural-election facts (TY-1 ArrayDual; catalog-declared —
    /// [`TableWriter::set_structural`]). Default: none.
    structural: StructuralPolicy,
    cols: Vec<ColBuffer>,
    sealed: Vec<SealedPart>,
    reports: Vec<SealReport>,
    seq_next: u32,
    scratch: Vec<u8>,
}

impl TableWriter {
    pub fn open(
        table_dir: String,
        schema: Vec<ColSchema>,
        spc: u32,
        db: u32,
        relfilenumber: u64,
        stamp: TxnStamp,
        evidence: &SubxactEvidence,
        policy: PartCutPolicy,
    ) -> WriteResult<TableWriter> {
        if schema.is_empty() {
            return Err(WriteError::Contract {
                detail: "table with zero columns",
            });
        }
        if policy.cut_granule_rows == 0 {
            return Err(WriteError::Contract {
                detail: "cut_granule_rows must be positive",
            });
        }
        for w in schema.windows(2) {
            if w[1].attno <= w[0].attno {
                return Err(WriteError::Contract {
                    detail: "schema attnos must strictly increase",
                });
            }
        }
        let spec = PartSpec {
            spc,
            db,
            relfilenumber,
            schema_fingerprint: schema_fingerprint(&schema),
        };
        let cols = schema.iter().map(|s| ColBuffer::new(*s)).collect();
        Ok(TableWriter {
            spec,
            table_dir,
            schema,
            stamp,
            freeze: freeze_decision(evidence),
            policy,
            structural: StructuralPolicy::default(),
            cols,
            sealed: Vec::new(),
            reports: Vec::new(),
            seq_next: 0,
            scratch: Vec::new(),
        })
    }

    /// Declare the table's structural-election facts (TY-1: array columns
    /// + their catalog element facts). M3-H derives these at DDL/COPY setup;
    /// tests declare directly. Must be set before the first appended row of
    /// a statement (per-part permanence — a mid-statement change would
    /// split election behavior across parts).
    pub fn set_structural(&mut self, structural: StructuralPolicy) {
        self.structural = structural;
    }

    pub fn structural(&self) -> &StructuralPolicy {
        &self.structural
    }

    pub fn stamp(&self) -> TxnStamp {
        self.stamp
    }

    pub fn freeze(&self) -> FreezeDecision {
        self.freeze
    }

    pub fn spec(&self) -> &PartSpec {
        &self.spec
    }

    pub fn buffered_rows(&self) -> u64 {
        self.cols.first().map(|c| c.rows()).unwrap_or(0)
    }

    pub fn sealed_parts(&self) -> &[SealedPart] {
        &self.sealed
    }

    pub fn seal_reports(&self) -> &[SealReport] {
        &self.reports
    }

    fn buffered_bytes(&self) -> u64 {
        self.cols.iter().map(|c| c.approx_bytes()).sum()
    }

    /// The serial ingest face: append one row (COPY-shaped; M3-H routes
    /// here). Varlena inputs are normalized per the detoast law; the part
    /// cut policy seals inline when a budget trips.
    pub fn append_row(
        &mut self,
        row: &[RawDatum<'_>],
        ext: &mut dyn ExternalDetoast,
        env: &mut SealEnv<'_>,
    ) -> WriteResult<()> {
        if row.len() != self.cols.len() {
            return Err(WriteError::Contract {
                detail: "row width mismatch",
            });
        }
        for (i, d) in row.iter().enumerate() {
            let col = &mut self.cols[i];
            match (d, col.schema.class) {
                (RawDatum::Null, _) => col.append_null(),
                (RawDatum::Word(w), StorageClass::ByvalWord { .. })
                | (RawDatum::Word(w), StorageClass::F32)
                | (RawDatum::Word(w), StorageClass::F64)
                | (RawDatum::Word(w), StorageClass::Bool) => col.append_word(*w)?,
                (RawDatum::Bytes(b), StorageClass::Fixed { .. }) => col.append_fixed(b)?,
                (RawDatum::Bytes(b), StorageClass::VarlenaVerbatim) => {
                    let mut scratch = std::mem::take(&mut self.scratch);
                    let res = (|| {
                        let (payload, _) = normalize_varlena(b, ext, &mut scratch)?;
                        self.cols[i].append_varlena_payload(payload)
                    })();
                    self.scratch = scratch;
                    res?;
                }
                _ => {
                    return Err(WriteError::Contract {
                        detail: "datum shape does not match column class",
                    })
                }
            }
        }
        // The granule gate is the byte-identity boundary (see
        // `PartCutPolicy`); it also keeps the O(ncols) byte scan off the
        // per-row path — it now runs once per granule, not once per row.
        // The granule gate is the byte-identity boundary (see
        // `PartCutPolicy`; a byte-budget parallel partition must be
        // byte-identical to serial), so the byte budget is consulted at every
        // granule and never between them. Wide rows inside one granule are
        // bounded by the varlena cap and admitted fallibly (arena ceiling).
        let rows = self.buffered_rows();
        if self.policy.at_granule(rows) && self.policy.should_cut(rows, self.buffered_bytes()) {
            self.cut_part(env)?;
        }
        Ok(())
    }

    /// Seal the buffered rows into a part (no-op on empty buffers).
    fn cut_part(&mut self, env: &mut SealEnv<'_>) -> WriteResult<()> {
        if self.buffered_rows() == 0 {
            return Ok(());
        }
        // Derive shred lanes (dual-store, O-4): the image lane is the
        // parent column itself; typed lanes come from the seam.
        let mut lanes = Vec::new();
        for col in &self.cols {
            if col.schema.class == StorageClass::VarlenaVerbatim {
                let derived = env.shred.shred(col, env.shred_opts)?;
                validate_lanes(col, &derived, env.shred_opts)?;
                lanes.extend(derived);
            }
        }
        // M3-E's real builders, one per stream in `seal_part`'s own stream
        // order (roots then lanes). Was `StandinMetaBuilder` — see
        // `crate::meta_wire` for what that cost and why the seal driver did
        // not have to change to fix it.
        let mut builders: Vec<Box<dyn ColumnMetaBuilder>> =
            crate::meta_wire::builders_for_streams(&self.cols, &lanes);
        let (sealed, report) = seal_part(
            env.vfs,
            &self.table_dir,
            &self.spec,
            &self.cols,
            &lanes,
            &mut builders,
            env.sources,
            env.resolver,
            &self.structural,
            self.stamp.fxid,
            self.seq_next,
        )?;
        self.seq_next += 1;
        self.sealed.push(sealed);
        self.reports.push(report);
        self.cols = self.schema.iter().map(|s| ColBuffer::new(*s)).collect();
        Ok(())
    }

    /// Statement end: seal any remaining buffered rows.
    pub fn finish(&mut self, env: &mut SealEnv<'_>) -> WriteResult<()> {
        self.cut_part(env)
    }

    /// `finish` for a writer already taken out of the registry: a failure
    /// aborts the writer (sealed temps unlinked) before surfacing, since no
    /// at_eoxact sweep covers it any more.
    pub fn finish_or_abort(&mut self, env: &mut SealEnv<'_>) -> WriteResult<()> {
        match self.finish(env) {
            Ok(()) => Ok(()),
            Err(e) => {
                let _ = self.abort(env.vfs);
                Err(e)
            }
        }
    }

    /// The parallel-ingest seam, fact side (chunk M3-I). The rtpool BINDING
    /// lives one crate up (`pgrc2_ingest_par` — a `runtime` dependency here
    /// would close the `pgrc2_am → pgrc2_write` cycle #473 introduced); it
    /// builds a [`crate::par::ParEngine`] from these facts and deposits the
    /// ordered results through [`TableWriter::deposit_sealed_parts`].
    ///
    /// Contract: no serial-buffered rows may be pending (one statement is
    /// either serial or parallel, never spliced).
    pub fn par_facts(&self) -> WriteResult<ParFacts> {
        if self.buffered_rows() != 0 {
            return Err(WriteError::Contract {
                detail: "parallel ingest over serial-buffered rows",
            });
        }
        Ok(ParFacts {
            schema: self.schema.clone(),
            spec: self.spec,
            table_dir: self.table_dir.clone(),
            fxid: self.stamp.fxid,
            policy: self.policy,
            structural: self.structural.clone(),
            base_seq: self.seq_next,
        })
    }

    /// The parallel-ingest seam, deposit side: the ORDERED COMMIT. Accepts
    /// parts sealed through the frozen face with contiguous seqs starting at
    /// this writer's `seq_next` — self-checking via the temp-name law
    /// (`tmp-<fxid>-<seq>`), so a driver that skipped, reordered, or
    /// cross-stamped a part is refused typed. After deposit, `publish`/
    /// `abort`/registry lifecycle proceed UNCHANGED.
    pub fn deposit_sealed_parts(
        &mut self,
        parts: Vec<SealedPart>,
        reports: Vec<SealReport>,
    ) -> WriteResult<()> {
        if parts.len() != reports.len() {
            return Err(WriteError::Contract {
                detail: "deposit parts/reports length mismatch",
            });
        }
        for (i, p) in parts.iter().enumerate() {
            let expect = pgrc2_format::dirlayout::temp_file_name(
                self.stamp.fxid,
                self.seq_next + i as u32,
            );
            if p.tmp_name != expect {
                return Err(WriteError::Contract {
                    detail: "deposit violates the ordered-commit temp-name law",
                });
            }
        }
        self.seq_next += parts.len() as u32;
        self.sealed.extend(parts);
        self.reports.extend(reports);
        Ok(())
    }

    /// Publish every sealed part as one manifest generation (spec §13.3).
    /// Caller holds the table publish lock and commits AFTER this returns.
    pub fn publish(
        &mut self,
        vfs: &mut dyn WriteVfs,
        probe: &dyn TxnProbe,
    ) -> WriteResult<PublishOutcome> {
        let outcome = publish_parts(
            vfs,
            &self.table_dir,
            &self.spec,
            &self.sealed,
            self.stamp.fxid,
            probe,
        )?;
        self.sealed.clear();
        Ok(outcome)
    }

    /// `publish` with the `finish_or_abort` failure contract.
    pub fn publish_or_abort(
        &mut self,
        vfs: &mut dyn WriteVfs,
        probe: &dyn TxnProbe,
    ) -> WriteResult<PublishOutcome> {
        match self.publish(vfs, probe) {
            Ok(o) => Ok(o),
            Err(e) => {
                let _ = self.abort(vfs);
                Err(e)
            }
        }
    }

    /// Abort: unlink this writer's temp files, drop buffered rows. Missing
    /// files are fine (never-written or already-cleaned); other I/O errors
    /// surface.
    pub fn abort(&mut self, vfs: &mut dyn WriteVfs) -> WriteResult<()> {
        for s in std::mem::take(&mut self.sealed) {
            let path = format!("{}/{}", self.table_dir, s.tmp_name);
            match vfs.unlink_path(&path) {
                Ok(()) => {}
                Err(WriteError::Io { errno, .. }) if errno == libc::ENOENT => {}
                Err(e) => return Err(e),
            }
        }
        self.cols = self.schema.iter().map(|s| ColBuffer::new(*s)).collect();
        Ok(())
    }
}

/// The facts a parallel-ingest driver needs to build an engine for one
/// statement (the M3-I split-crate seam currency).
#[derive(Debug, Clone)]
pub struct ParFacts {
    pub schema: Vec<ColSchema>,
    pub spec: PartSpec,
    pub table_dir: String,
    pub fxid: u64,
    pub policy: PartCutPolicy,
    /// Structural-election facts (TY-1): the parallel seal path must elect
    /// exactly as serial would — byte-identical-parts law.
    pub structural: StructuralPolicy,
    pub base_seq: u32,
}

/// The per-backend writer registry (session-envelope state — M3-H owns
/// placement + the xact-callback registration; this object owns the
/// semantics).
#[derive(Default)]
pub struct WriterRegistry {
    writers: BTreeMap<u64, TableWriter>,
}

impl WriterRegistry {
    pub fn new() -> WriterRegistry {
        WriterRegistry::default()
    }

    pub fn len(&self) -> usize {
        self.writers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.writers.is_empty()
    }

    pub fn registered_stamps(&self) -> impl Iterator<Item = (u64, TxnStamp)> + '_ {
        self.writers.iter().map(|(k, w)| (*k, w.stamp()))
    }

    /// Get the table's writer, EVICTING a stale one first (stamp mismatch ⇒
    /// abort + drop, the old-writer law). `open` constructs a fresh writer
    /// when none (or a stale one) is registered.
    pub fn get_or_open(
        &mut self,
        vfs: &mut dyn WriteVfs,
        relfilenumber: u64,
        stamp: TxnStamp,
        open: impl FnOnce() -> WriteResult<TableWriter>,
    ) -> WriteResult<&mut TableWriter> {
        if let Some(w) = self.writers.get_mut(&relfilenumber) {
            if w.stamp() != stamp {
                w.abort(vfs)?;
                self.writers.remove(&relfilenumber);
            }
        }
        if !self.writers.contains_key(&relfilenumber) {
            let w = open()?;
            if w.stamp() != stamp {
                return Err(WriteError::Contract {
                    detail: "opened writer carries a foreign stamp",
                });
            }
            self.writers.insert(relfilenumber, w);
        }
        Ok(self.writers.get_mut(&relfilenumber).expect("just inserted"))
    }

    /// Remove the writer for publishing, re-checking the stamp (the
    /// finish-time staleness recheck): a stale writer is aborted and None
    /// returned — never published.
    pub fn take_for_publish(
        &mut self,
        vfs: &mut dyn WriteVfs,
        relfilenumber: u64,
        stamp: TxnStamp,
    ) -> WriteResult<Option<TableWriter>> {
        match self.writers.remove(&relfilenumber) {
            None => Ok(None),
            Some(mut w) => {
                if w.stamp() != stamp {
                    w.abort(vfs)?;
                    return Ok(None);
                }
                Ok(Some(w))
            }
        }
    }

    /// End-of-transaction purge — IDENTICAL on commit and abort: everything
    /// still here is abandoned by construction (the successful statement
    /// already took its writer at publish). Abort each and clear.
    pub fn at_eoxact(&mut self, vfs: &mut dyn WriteVfs) -> WriteResult<()> {
        let mut writers = std::mem::take(&mut self.writers);
        for (_, w) in writers.iter_mut() {
            w.abort(vfs)?;
        }
        Ok(())
    }
}
