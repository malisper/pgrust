//! The publish/commit round protocol shared by the `qa_crash_harness`
//! binary (real directory, RealVfs, killed with SIGKILL by the ladder
//! driver) and the in-process crash legs (SimVfs). One round =
//! ingest rows → seal → publish (spec §13.3, all five steps) → append a
//! CRC-guarded commit record to the `clog` file + fsync — the commit-record
//! stand-in for the transaction layer, honoring the #253 ordering (nothing
//! is acked before its publish AND commit record are durable).
//!
//! The harness SELF-RECOVERS on start: read the clog, run
//! `recover_and_clean`, resolve the effective manifest, and resume at the
//! next round — so every ladder respawn exercises crash recovery before it
//! writes a byte. The checker proves the old-or-new law: effective rows ==
//! clog-committed rounds × rows/round exactly, every live part decodes to
//! the deterministic oracle, both manifest walks (writer-side
//! `effective_manifest`, reader-side `resolve_effective`) agree, and after
//! cleanup the directory holds EXACTLY the live file set.

use pgrc2_format::dirlayout::{manifest_file_name, part_file_name, CURRENT_FILE_NAME};
use pgrc2_format::class::ColSchema;
use pgrc2_format::wire::crc32c;
use pgrc2_read::manifest_walk::{resolve_effective, TableExpect};
use pgrc2_write::publish::{effective_manifest, recover_and_clean, TxnVerdict};
use pgrc2_write::writer::PartCutPolicy;
use pgrc2_write::wvfs::WriteVfs;
use pgrc2_write::{WriteError, WriteResult};
use std::collections::BTreeSet;

use crate::adapters::{memdir_of, ForcedPlan, Probe};
use crate::corpus::{
    append_rows_planned, finish_planned, open_writer, verify_manifest, Fixture, OracleVal,
    RowOracle,
};
use crate::{int8_col, text_col};

pub const FXID_BASE: u64 = 1_000;
pub const RELFILENUMBER: u64 = 4242;

fn mix(i: u64) -> u64 {
    i.wrapping_mul(0x9E37_79B9_7F4A_7C15).rotate_left(31) ^ i
}

/// The ladder's deterministic row truth: value(row) is a pure function of
/// the GLOBAL row index, so any committed prefix is checkable.
pub struct LadderOracle {
    schema: Vec<ColSchema>,
}

impl LadderOracle {
    pub fn new() -> LadderOracle {
        LadderOracle {
            schema: vec![int8_col(1), text_col(2)],
        }
    }
}

impl Default for LadderOracle {
    fn default() -> LadderOracle {
        LadderOracle::new()
    }
}

impl RowOracle for LadderOracle {
    fn schema(&self) -> &[ColSchema] {
        &self.schema
    }

    fn value(&self, col: usize, row: u64) -> Option<OracleVal> {
        match col {
            0 => {
                if row % 7 == 3 {
                    None
                } else {
                    Some(OracleVal::Word(mix(row)))
                }
            }
            _ => {
                if row % 5 == 2 {
                    None
                } else if row % 13 == 0 {
                    Some(OracleVal::Bytes(Vec::new()))
                } else {
                    let len = 3 + (mix(row) % 60) as usize;
                    Some(OracleVal::Bytes(
                        (0..len).map(|k| (mix(row ^ (k as u64 + 11)) & 0xFF) as u8).collect(),
                    ))
                }
            }
        }
    }
}

fn ladder_fixture(base: &str) -> Fixture {
    let o = LadderOracle::new();
    Fixture {
        name: "ladder",
        dir: format!("{base}/table"),
        spc: 1663,
        db: 5,
        relfilenumber: RELFILENUMBER,
        schema: o.schema.clone(),
        oracle: Vec::new(), // rows come from LadderOracle, never from here
        plans: vec![ForcedPlan::ByteFor {
            delta_width: 8,
            signed: true,
        }],
        policy: PartCutPolicy::default(),
    }
}

// ---------------------------------------------------------------------------
// the clog file (CRC-guarded commit records)
// ---------------------------------------------------------------------------

fn clog_path(base: &str) -> String {
    format!("{base}/clog")
}

/// Parse the clog: 12-byte records `[fxid u64 LE][crc32c(fxid bytes) u32]`.
/// A torn / invalid trailing record is IGNORED (it never committed).
pub fn read_clog(vfs: &mut dyn WriteVfs, base: &str) -> WriteResult<BTreeSet<u64>> {
    let path = clog_path(base);
    if !vfs.exists_path(&path)? {
        return Ok(BTreeSet::new());
    }
    let bytes = vfs.read_full(&path)?;
    let mut out = BTreeSet::new();
    for rec in bytes.chunks(12) {
        if rec.len() < 12 {
            break;
        }
        let fxid = u64::from_le_bytes(rec[..8].try_into().expect("8 bytes"));
        let crc = u32::from_le_bytes(rec[8..12].try_into().expect("4 bytes"));
        if crc == crc32c(&rec[..8]) {
            out.insert(fxid);
        }
        // An invalid record mid-file cannot occur (records are appended
        // fsync-then-ack); tolerate by skipping — the commit it would have
        // been simply never happened.
    }
    Ok(out)
}

/// Append + fsync one commit record — THE commit point of a round.
pub fn append_commit(vfs: &mut dyn WriteVfs, base: &str, fxid: u64) -> WriteResult<()> {
    let path = clog_path(base);
    let (fd, off) = if vfs.exists_path(&path)? {
        let len = vfs.read_full(&path)?.len() as u64;
        (vfs.open_rw(&path)?, len)
    } else {
        (vfs.create_rw(&path)?, 0)
    };
    let mut rec = Vec::with_capacity(12);
    rec.extend_from_slice(&fxid.to_le_bytes());
    rec.extend_from_slice(&crc32c(&fxid.to_le_bytes()).to_le_bytes());
    vfs.pwrite_at(&fd, off, &rec)?;
    vfs.fsync_file(&fd)?;
    vfs.close_file(fd)?;
    Ok(())
}

/// The probe a fresh process derives from the clog: committed iff a valid
/// record exists; everything else is dead (no live writers at start).
pub fn probe_from_clog(clog: &BTreeSet<u64>) -> Probe {
    let mut p = Probe::new(TxnVerdict::Aborted);
    for fxid in clog {
        p.mark(*fxid, TxnVerdict::Committed);
    }
    p
}

pub fn round_fxid(round: u64, salt: u64) -> u64 {
    FXID_BASE + round * 8 + salt
}

// ---------------------------------------------------------------------------
// rounds
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunSummary {
    pub resumed_at_round: u64,
    pub completed_rounds: u64,
    pub effective_gen: u64,
}

/// Recover, resume, and run publish/commit rounds until `target_rounds`
/// rounds are committed. When `commit_last` is false the FINAL round's
/// publish completes durably but its commit record is never written — the
/// #254 durable-but-uncommitted shape — and the harness exits.
pub fn run_rounds(
    vfs: &mut dyn WriteVfs,
    base: &str,
    target_rounds: u64,
    rows_per_round: u64,
    salt: u64,
    commit_last: bool,
) -> WriteResult<RunSummary> {
    let fx = ladder_fixture(base);
    let oracle = LadderOracle::new();
    // --- recover ----------------------------------------------------------
    let clog = read_clog(vfs, base)?;
    let mut probe = probe_from_clog(&clog);
    recover_and_clean(vfs, &fx.dir, &probe)?;
    let eff = effective_manifest(vfs, &fx.dir, &probe)?;
    let total: u64 = eff
        .as_ref()
        .map(|m| m.parts.iter().map(|p| p.rows).sum())
        .unwrap_or(0);
    if total % rows_per_round != 0 {
        return Err(WriteError::Contract {
            detail: "recovered row total is not a whole number of rounds",
        });
    }
    let start = total / rows_per_round;
    let mut gen = eff.map(|m| m.header.gen).unwrap_or(0);
    // --- rounds -----------------------------------------------------------
    for round in start..target_rounds {
        let fxid = round_fxid(round, salt);
        probe.mark(fxid, TxnVerdict::InProgress);
        let mut w = open_writer(&fx, fxid)?;
        let lo = round * rows_per_round;
        append_rows_planned(vfs, &mut w, &oracle, lo, lo + rows_per_round, &fx.plans)?;
        finish_planned(vfs, &mut w, &fx.plans)?;
        let outcome = w.publish(vfs, &probe)?;
        gen = outcome.gen;
        let last = round + 1 == target_rounds;
        if last && !commit_last {
            // #254 shape: durable publish, no commit record — exit here.
            println!("UNCOMMITTED round={round} gen={gen} fxid={fxid}");
            return Ok(RunSummary {
                resumed_at_round: start,
                completed_rounds: round,
                effective_gen: gen,
            });
        }
        append_commit(vfs, base, fxid)?;
        probe.mark(fxid, TxnVerdict::Committed);
        println!(
            "ACK round={round} gen={gen} total={}",
            (round + 1) * rows_per_round
        );
    }
    Ok(RunSummary {
        resumed_at_round: start,
        completed_rounds: target_rounds,
        effective_gen: gen,
    })
}

// ---------------------------------------------------------------------------
// the checker (old-or-new + residue + oracle, both manifest walks)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckReport {
    pub effective_gen: u64,
    pub total_rows: u64,
    pub committed_rounds: u64,
    pub removed_residue: usize,
}

/// Full post-crash adjudication of a harness directory. `Err(String)` is a
/// battery failure with the exact law that broke.
pub fn check_dir(
    vfs: &mut dyn WriteVfs,
    base: &str,
    rows_per_round: u64,
) -> Result<CheckReport, String> {
    let fx = ladder_fixture(base);
    let oracle = LadderOracle::new();
    // Killed before bootstrap: no table dir yet ⇒ the empty state (nothing
    // was acked — an existing clog with records but no dir is a failure).
    let dir_exists = vfs.exists_path(&fx.dir).unwrap_or(false);
    if !dir_exists {
        let clog = read_clog(vfs, base).unwrap_or_default();
        if !clog.is_empty() {
            return Err("clog has commit records but the table dir is missing".to_string());
        }
        return Ok(CheckReport {
            effective_gen: 0,
            total_rows: 0,
            committed_rounds: 0,
            removed_residue: 0,
        });
    }
    let clog = read_clog(vfs, base).map_err(|e| format!("clog read: {e}"))?;
    let probe = probe_from_clog(&clog);

    // Writer-side walk.
    let eff = effective_manifest(vfs, &fx.dir, &probe).map_err(|e| format!("effective: {e}"))?;
    // Reader-side walk over a snapshot of the same directory.
    let names = vfs.list_dir(&fx.dir).map_err(|e| format!("listdir: {e}"))?;
    let mut files = std::collections::BTreeMap::new();
    for n in &names {
        let b = vfs
            .read_full(&format!("{}/{n}", fx.dir))
            .map_err(|e| format!("read {n}: {e}"))?;
        files.insert(n.clone(), b);
    }
    let memdir = memdir_of(&files);
    let eff_gen_w = eff.as_ref().map(|m| m.header.gen).unwrap_or(0);
    let reader_walk = resolve_effective(&memdir, &probe, &TableExpect::default());

    // The two walks must agree — EXCEPT one legal pre-recovery state: a
    // dangling CURRENT naming a never-linked generation ABOVE the effective
    // one (a pre-commit dirent-loss window can persist the CURRENT rename
    // while dropping the manifest link) refuses ManifestMissing on the
    // reader side while the writer's scan fallback answers. That is
    // un-recovered crash residue, legal ONLY here; the #462 repoint law
    // makes recovery repair it, enforced by the STRICT post-recovery
    // agreement check below. Any other divergence is a battery failure.
    let (eff_gen, total) = match reader_walk {
        Err(pgrc2_read::ReadError::ManifestMissing { gen }) if gen > eff_gen_w => {
            let t: u64 = eff
                .as_ref()
                .map(|m| m.parts.iter().map(|p| p.rows).sum())
                .unwrap_or(0);
            (eff_gen_w, t) // un-recovered residue: writer walk is the authority
        }
        Err(e) => return Err(format!("resolve_effective: {e:?}")),
        Ok(reff) => match (&eff, &reff) {
            (None, None) => (0, 0),
            (Some(w), Some(r)) => {
                if w.header.gen != r.manifest.header.gen {
                    return Err(format!(
                        "walk disagreement: writer gen {} vs reader gen {}",
                        w.header.gen, r.manifest.header.gen
                    ));
                }
                let t: u64 = w.parts.iter().map(|p| p.rows).sum();
                (w.header.gen, t)
            }
            (w, r) => {
                return Err(format!(
                    "walk disagreement: writer {:?} vs reader {:?}",
                    w.as_ref().map(|m| m.header.gen),
                    r.as_ref().map(|m| m.manifest.header.gen)
                ))
            }
        },
    };

    // Old-or-new: exactly the clog-committed rounds, nothing torn between.
    let committed_rounds = clog.len() as u64;
    if total != committed_rounds * rows_per_round {
        return Err(format!(
            "old-or-new broken: effective rows {total} != committed rounds \
             {committed_rounds} × {rows_per_round}"
        ));
    }

    // Acked data intact, byte-for-byte vs the deterministic oracle.
    if let Some(m) = &eff {
        let verified =
            verify_manifest(&files, m, &oracle).map_err(|e| format!("decode-vs-oracle: {e:?}"))?;
        if verified != total {
            return Err(format!("verified {verified} rows, manifest claims {total}"));
        }
    }

    // Residue reclaim: after cleanup the directory holds EXACTLY the live
    // set (+ CURRENT, whose durability is best-effort by design).
    let report =
        recover_and_clean(vfs, &fx.dir, &probe).map_err(|e| format!("recover_and_clean: {e}"))?;

    // STRICT post-recovery walk agreement (issue #462 closed): recovery's
    // repoint law guarantees CURRENT never survives naming a removed
    // generation, so from here the reader walk must answer EXACTLY the
    // writer's verdict — a refusal here is a battery failure, full stop.
    let names_pr = vfs
        .list_dir(&fx.dir)
        .map_err(|e| format!("listdir post-recovery: {e}"))?;
    let mut files_pr = std::collections::BTreeMap::new();
    for n in &names_pr {
        let b = vfs
            .read_full(&format!("{}/{n}", fx.dir))
            .map_err(|e| format!("read post-recovery {n}: {e}"))?;
        files_pr.insert(n.clone(), b);
    }
    let memdir_pr = memdir_of(&files_pr);
    match resolve_effective(&memdir_pr, &probe, &TableExpect::default()) {
        Ok(r) => {
            let rgen = r.as_ref().map(|m| m.manifest.header.gen).unwrap_or(0);
            if rgen != eff_gen {
                return Err(format!(
                    "post-recovery walk disagreement: writer gen {eff_gen} vs reader gen {rgen}"
                ));
            }
        }
        Err(e) => {
            return Err(format!(
                "post-recovery reader walk refused a recovered table (#462 law broken): {e:?}"
            ))
        }
    }
    let after: BTreeSet<String> = vfs
        .list_dir(&fx.dir)
        .map_err(|e| format!("listdir after clean: {e}"))?
        .into_iter()
        .collect();
    let mut allowed: BTreeSet<String> = BTreeSet::new();
    let mut required: BTreeSet<String> = BTreeSet::new();
    if let Some(m) = &eff {
        for p in &m.parts {
            required.insert(part_file_name(p.part_no));
        }
        for g in 1..=m.header.gen {
            required.insert(manifest_file_name(g));
        }
    }
    allowed.extend(required.iter().cloned());
    allowed.insert(CURRENT_FILE_NAME.to_string());
    // [fmt-land] The effective generation's bank-grain stats plane is a
    // LIVE derived sidecar: `recover_and_clean` keeps exactly
    // `bankstats-<eff_gen>.pgrc2bs` (dead/old-generation planes are
    // reclaimed above). Allowed but never REQUIRED — the plane is
    // best-effort by charter (a crash between the manifest/CURRENT ack and
    // the plane rename legally publishes without one) — and when present
    // it must POSITIVELY prove its binding to the effective part-set
    // (gen + schema fingerprint + the full per-part identity vector),
    // never ride through the checker as unadjudicated bytes.
    if let Some(m) = &eff {
        let plane = pgrc2_format::bankstats::bankstats_file_name(m.header.gen);
        if after.contains(&plane) {
            let bytes = vfs
                .read_full(&format!("{}/{plane}", fx.dir))
                .map_err(|e| format!("read live plane {plane}: {e}"))?;
            let (h, pidents, _cols) = pgrc2_format::bankstats::decode_meta(&bytes)
                .map_err(|e| format!("live plane {plane} undecodable: {e:?}"))?;
            let bound = h.gen == m.header.gen
                && h.schema_fingerprint == m.header.schema_fingerprint
                && h.part_count as usize == m.parts.len()
                && pidents.iter().zip(m.parts.iter()).all(|(a, b)| {
                    a.part_no == b.part_no
                        && a.granule_count == b.granule_count
                        && a.band_count == b.band_count
                        && a.rows == b.rows
                        && a.footer_off == b.footer_off
                });
            if !bound {
                return Err(format!("live plane {plane} fails the identity witness"));
            }
        }
        allowed.insert(plane);
    }
    for n in &after {
        if !allowed.contains(n) {
            return Err(format!("residue survived cleanup: {n}"));
        }
    }
    for n in &required {
        if !after.contains(n) {
            return Err(format!("live file missing after cleanup: {n}"));
        }
    }
    Ok(CheckReport {
        effective_gen: eff_gen,
        total_rows: total,
        committed_rounds,
        removed_residue: report.removed.len(),
    })
}

/// Real-filesystem directory bootstrap (RealVfs mkdir refuses EEXIST;
/// tolerate it — reruns share the base).
pub fn ensure_dirs(vfs: &mut dyn WriteVfs, base: &str) -> WriteResult<()> {
    for d in [base.to_string(), format!("{base}/table")] {
        match vfs.mkdir_path(&d) {
            Ok(()) => {}
            Err(WriteError::Io { errno, .. }) if errno == libc::EEXIST => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
