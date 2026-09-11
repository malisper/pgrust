//! Manifest publish (spec §13; rulings O-2/O-3) — the crash story.
//!
//! ## The five-step ordering (spec §13.3; the #253 law)
//!
//! 1. every new part file: part_no patched (header + footer echoes),
//!    fsync'd, renamed `tmp-…` → `part-<no>.pgrc2`;
//! 2. `manifest-<gen>.pgrc2m` written + fsync'd (`prev_gen` chained,
//!    `publisher_fxid` = the publishing txn's FullTransactionId);
//! 3. `CURRENT` replaced via `CURRENT.tmp` + fsync + rename + file fsync;
//! 4. the table directory fsync'd (dirent durability — the dirent-loss
//!    model is the test);
//! 5. only then may the commit record be written — [`publish_parts`]'s last
//!    act arms the cbstore #253 commit fence
//!    (`xact_seams::force_sync_commit`, seam-guarded exactly like the old
//!    writer: uninstalled means no backend, i.e. unit tests).
//!
//! ## The clog fence (#254)
//!
//! A generation is EFFECTIVE iff its epoch-qualified `publisher_fxid`
//! committed ([`TxnProbe`] — a passed capability over the clog).
//! [`effective_manifest`] tries the `CURRENT` hint, walks `prev_gen` past
//! non-committed generations, and falls back to a directory scan when the
//! hint chain is unreadable (crash residue). An aborted or
//! crashed-before-commit publish is structurally invisible; xid recycling
//! cannot resurrect it.
//!
//! ## Dead-band GC (O-M3-1(a) scope)
//!
//! [`recover_and_clean`] is the abort/DROP/crash cleanup discipline: dead
//! writer temps, orphan `part-*` files (renamed by a publish whose txn never
//! committed), dead manifest generations, and `CURRENT.tmp` residue. Under
//! O-M3-1(a) nothing else can create garbage; the compaction rung is M5's.
//! The #462 repoint law: before removing ANYTHING, recovery normalizes
//! `CURRENT` to the effective generation (durably — dir fsync barrier), so
//! no crash point, including inside recovery itself, leaves `CURRENT`
//! naming a removed generation. See [`normalize_current`].
//!
//! ## Serialization precondition
//!
//! Publishes to one table are serialized by the caller (M3-H's table-level
//! lock; the M3-I parallel-COPY handoff keeps ordered-commit). Two live
//! publishers on one directory would race `next_part_no` — refused by
//! construction nowhere here; it is the caller's lock to hold.

use pgrc2_format::dirlayout::{
    self, manifest_file_name, parse_manifest_file_name, parse_part_file_name, part_file_name,
    CURRENT_FILE_NAME, CURRENT_TMP_FILE_NAME,
};
use pgrc2_format::manifest::{CommitPointer, Manifest, ManifestHeader, PartRecord, MANIFEST_MAGIC};
use pgrc2_format::part::{FooterFixed, PartHeader};
use pgrc2_format::wire::crc32c;
use pgrc2_format::FORMAT_VERSION;

use crate::seal::{PartSpec, SealedPart};
use crate::wvfs::WriteVfs;
use crate::{WriteError, WriteResult};

/// Clog verdicts for a FullTransactionId (a passed capability; M3-H wires
/// the real clog probe, tests script it).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnVerdict {
    Committed,
    Aborted,
    InProgress,
}

pub trait TxnProbe {
    fn verdict(&self, fxid: u64) -> TxnVerdict;
}

/// Publish witnesses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishOutcome {
    pub gen: u64,
    pub part_nos: Vec<u32>,
    /// True iff the #253 commit fence seam was installed and armed (false
    /// exactly when running outside a backend — unit tests).
    pub commit_fence_armed: bool,
    /// [fmt-land] The bank-grain stats plane write witness (best-effort
    /// derived sidecar; `bankplane.rs`). Never affects publish success.
    pub bankstats_plane: crate::bankplane::PlaneOutcome,
}

fn full_path(dir: &str, name: &str) -> String {
    format!("{dir}/{name}")
}

/// Read + validate one manifest generation file; `None` when the file is
/// missing or fails validation (crash residue is unreadable, never fatal —
/// committed generations were fsync'd and DO decode).
fn read_manifest_opt(vfs: &mut dyn WriteVfs, dir: &str, gen: u64) -> Option<Manifest> {
    let path = full_path(dir, &manifest_file_name(gen));
    match vfs.exists_path(&path) {
        Ok(true) => {}
        _ => return None,
    }
    let bytes = vfs.read_full(&path).ok()?;
    Manifest::decode(&bytes).ok()
}

/// The newest EFFECTIVE (committed-publisher) manifest generation, or None
/// for an empty table. `CURRENT` is a hint + O(1) entry point; the chain
/// walk and the scan fallback are the authority (spec §13.2).
/// `own_fxid`: Some(f) for a publisher — its own in-progress generation is a
/// valid base to chain on (a second publish in one transaction), and a
/// FOREIGN in-progress head refuses the publish (reusing its generation and
/// part numbers would overwrite that transaction's files); None for readers,
/// which skip every non-committed generation.
pub fn effective_manifest(
    vfs: &mut dyn WriteVfs,
    dir: &str,
    probe: &dyn TxnProbe,
    own_fxid: Option<u64>,
) -> WriteResult<Option<Manifest>> {
    // Hint arm: CURRENT → candidate gen → walk prev_gen.
    let current_path = full_path(dir, CURRENT_FILE_NAME);
    if vfs.exists_path(&current_path)? {
        if let Ok(bytes) = vfs.read_full(&current_path) {
            if let Ok(cp) = CommitPointer::decode(&bytes) {
                if let Some(m) = walk_chain(vfs, dir, cp.gen, probe, own_fxid)? {
                    return Ok(Some(m));
                }
            }
        }
    }
    // Scan arm: newest decodable committed generation wins (the chain is
    // linear — serialized publishes — so scan order == chain order).
    let mut gens: Vec<u64> = vfs
        .list_dir(dir)?
        .iter()
        .filter_map(|n| parse_manifest_file_name(n))
        .collect();
    gens.sort_unstable_by(|a, b| b.cmp(a));
    for g in gens {
        if let Some(m) = read_manifest_opt(vfs, dir, g) {
            match probe.verdict(m.header.publisher_fxid) {
                TxnVerdict::Committed => return Ok(Some(m)),
                TxnVerdict::InProgress if own_fxid == Some(m.header.publisher_fxid) => {
                    return Ok(Some(m))
                }
                TxnVerdict::InProgress if own_fxid.is_some() => {
                    return Err(WriteError::Contract {
                        detail: "another transaction's publish of this table is in progress",
                    })
                }
                _ => continue,
            }
        }
    }
    Ok(None)
}

/// Walk prev_gen from `gen` until a committed generation; None when the
/// chain is unreadable (caller falls back to the scan).
fn walk_chain(
    vfs: &mut dyn WriteVfs,
    dir: &str,
    mut gen: u64,
    probe: &dyn TxnProbe,
    own_fxid: Option<u64>,
) -> WriteResult<Option<Manifest>> {
    while gen != 0 {
        let Some(m) = read_manifest_opt(vfs, dir, gen) else { return Ok(None) };
        match probe.verdict(m.header.publisher_fxid) {
            TxnVerdict::Committed => return Ok(Some(m)),
            TxnVerdict::InProgress if own_fxid == Some(m.header.publisher_fxid) => {
                return Ok(Some(m))
            }
            TxnVerdict::InProgress if own_fxid.is_some() => {
                return Err(WriteError::Contract {
                    detail: "another transaction's publish of this table is in progress",
                })
            }
            _ => {}
        }
        gen = m.header.prev_gen;
    }
    Ok(None)
}

/// The sealed footer image's elected grain (SB-10): the footer is the
/// grain truth; the manifest echoes it (never recomputes from defaults).
fn footer_grain_rows(footer_image: &[u8]) -> WriteResult<u32> {
    let f = pgrc2_format::part::FooterFixed::decode(footer_image).map_err(WriteError::Format)?;
    Ok(f.granule_rows)
}

/// Publish sealed parts as one manifest generation, per the §13.3 ordering.
/// Returns after step 4 + the step-5 fence arm; the caller's transaction
/// commit is the effectiveness switch (clog fence).
pub fn publish_parts(
    vfs: &mut dyn WriteVfs,
    dir: &str,
    spec: &PartSpec,
    sealed: &[SealedPart],
    fxid: u64,
    probe: &dyn TxnProbe,
) -> WriteResult<PublishOutcome> {
    if sealed.is_empty() {
        return Err(WriteError::Contract {
            detail: "publish with zero sealed parts",
        });
    }
    let base = effective_manifest(vfs, dir, probe, Some(fxid))?;
    if let Some(b) = &base {
        if b.header.relfilenumber != spec.relfilenumber
            || b.header.schema_fingerprint != spec.schema_fingerprint
            || b.header.spc != spec.spc
            || b.header.db != spec.db
        {
            return Err(WriteError::Contract {
                detail: "manifest base identity mismatch",
            });
        }
    }
    let prev_gen = base.as_ref().map(|b| b.header.gen).unwrap_or(0);
    let gen = prev_gen + 1;
    let mut next_part_no = base.as_ref().map(|b| b.header.next_part_no).unwrap_or(0);

    // ---- step 1: patch part_no, fsync, rename each part -------------------
    let mut part_nos = Vec::with_capacity(sealed.len());
    let mut new_records = Vec::with_capacity(sealed.len());
    for s in sealed {
        let part_no = next_part_no;
        next_part_no += 1;
        let tmp_path = full_path(dir, &s.tmp_name);
        let fd = vfs.open_rw(&tmp_path)?;
        // Header echo (spec §5.1) — rebuilt deterministically.
        let mut hdr_bytes = Vec::with_capacity(pgrc2_format::part::PART_HEADER_LEN);
        PartHeader::new(
            part_no,
            spec.schema_fingerprint,
            spec.spc,
            spec.db,
            spec.relfilenumber,
        )
        .encode_into(&mut hdr_bytes);
        vfs.pwrite_at(&fd, 0, &hdr_bytes)?;
        // Footer echo (spec §5.3) — the sealed image with part_no patched.
        let mut footer = FooterFixed::decode(&s.footer_image)?;
        footer.part_no = part_no;
        let mut footer_bytes = Vec::with_capacity(pgrc2_format::part::FOOTER_FIXED_LEN);
        footer.encode_into(&mut footer_bytes);
        vfs.pwrite_at(&fd, s.footer_off, &footer_bytes)?;
        vfs.fsync_file(&fd)?;
        vfs.close_file(fd)?;
        vfs.rename_path(&tmp_path, &full_path(dir, &part_file_name(part_no)))?;
        part_nos.push(part_no);
        new_records.push(PartRecord {
            rows: s.rows,
            file_len: s.file_len,
            footer_off: s.footer_off,
            dv_gen: 0,
            dv_len: 0,
            part_no,
            flags: 0,
            // SB-10 grain echo (the recorded A-lane manifest ripple, now
            // closed): the manifest records the part's ELECTED grain and
            // the grain-true geometry — sourced from the sealed footer
            // image (the grain truth), never recomputed from defaults, so
            // the manifest is self-consistent for non-default-grain parts.
            granule_count: s.granule_count,
            band_count: s.band_count,
            dv_crc: 0,
            granule_rows: footer_grain_rows(&s.footer_image)?,
        });
    }

    // ---- step 2: write + fsync manifest-<gen> ------------------------------
    let mut parts = base.as_ref().map(|b| b.parts.clone()).unwrap_or_default();
    parts.extend(new_records);
    let manifest = Manifest {
        header: ManifestHeader {
            gen,
            prev_gen,
            publisher_fxid: fxid,
            relfilenumber: spec.relfilenumber,
            schema_fingerprint: spec.schema_fingerprint,
            magic: MANIFEST_MAGIC,
            format_version: FORMAT_VERSION,
            spc: spec.spc,
            db: spec.db,
            part_count: parts.len() as u32,
            next_part_no,
            flags: 0,
            reserved: 0,
        },
        parts,
    };
    let mbytes = manifest.encode();
    // A dead residue manifest at this gen (crashed uncommitted publish) is
    // structurally invisible; create-truncate reclaims the name.
    let mpath = full_path(dir, &manifest_file_name(gen));
    let mfd = vfs.create_rw(&mpath)?;
    vfs.pwrite_at(&mfd, 0, &mbytes)?;
    vfs.fsync_file(&mfd)?;
    vfs.close_file(mfd)?;

    // ---- step 3: CURRENT via tmp + fsync + rename + file fsync -------------
    // manifest_crc = the manifest's OWN trailing crc (crc32c over the body,
    // excluding the trailing crc word) — the semantics pgrc2_read's
    // resolve_effective enforces. M3-H reconciliation: the original write
    // covered the whole file (trailing word included), which no reader ever
    // accepted; spec §13.2 clarification reported to the A-lane.
    let cp = CommitPointer::new(
        gen,
        mbytes.len() as u64,
        crc32c(&mbytes[..mbytes.len() - 4]),
    );
    let cp_bytes = cp.encode();
    let ct_path = full_path(dir, CURRENT_TMP_FILE_NAME);
    let cfd = vfs.create_rw(&ct_path)?;
    vfs.pwrite_at(&cfd, 0, &cp_bytes)?;
    vfs.fsync_file(&cfd)?;
    vfs.close_file(cfd)?;
    let cur_path = full_path(dir, CURRENT_FILE_NAME);
    vfs.rename_path(&ct_path, &cur_path)?;
    let cfd2 = vfs.open_rw(&cur_path)?;
    vfs.fsync_file(&cfd2)?;
    vfs.close_file(cfd2)?;

    // ---- step 4: directory fsync (dirent durability) -----------------------
    vfs.fsync_dir(dir)?;

    // ---- step 5 arm: the #253 commit fence (cbstore precedent verbatim:
    // seam-guarded — uninstalled means no backend, i.e. unit tests). The
    // commit record itself is the caller's transaction machinery.
    // ---- step 4.5 [fmt-land]: the bank-grain stats plane -------------------
    // AFTER the manifest+CURRENT are durable (the plane is derived FROM
    // this part-set; writing it earlier could bind a plane to a manifest
    // that never becomes durable) and BEFORE the commit fence. Best-effort:
    // failure/skip never fails the publish — an absent plane is the
    // reader's per-part fallback, never corruption. If the txn later
    // aborts, the plane names a gen whose manifest is structurally
    // invisible — dead bytes with a failed witness, reclaimed with the
    // dead generation.
    let bankstats_plane =
        crate::bankplane::publish_plane(vfs, dir, base.as_ref(), &manifest, sealed);

    let commit_fence_armed = xact_seams::force_sync_commit::is_installed();
    if commit_fence_armed {
        xact_seams::force_sync_commit::call();
    }

    Ok(PublishOutcome {
        gen,
        part_nos,
        commit_fence_armed,
        bankstats_plane,
    })
}

/// Cleanup findings (the dead-band GC witness).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RecoveryReport {
    /// 0 = no effective generation (empty table).
    pub effective_gen: u64,
    pub removed: Vec<String>,
    /// True when `CURRENT` was rewritten to the effective generation (or
    /// unlinked, for the empty-table posture) because it would otherwise
    /// dangle at a generation the removal pass reclaims — the #462 law.
    pub current_repointed: bool,
}

fn parse_temp_fxid(name: &str) -> Option<u64> {
    let rest = name.strip_prefix("tmp-")?.strip_suffix(".pgrc2t")?;
    let (fxid, _seq) = rest.split_once('-')?;
    fxid.parse().ok()
}

/// Does the commit pointer pin exactly the on-disk manifest it names
/// (length + the manifest's own trailing crc — the facts the reader walk
/// enforces, spec §13.2)? Missing file = no.
fn pointer_matches_manifest(
    vfs: &mut dyn WriteVfs,
    dir: &str,
    cp: &CommitPointer,
) -> WriteResult<bool> {
    let path = full_path(dir, &manifest_file_name(cp.gen));
    if !vfs.exists_path(&path)? {
        return Ok(false);
    }
    let b = vfs.read_full(&path)?;
    Ok(b.len() >= 4
        && b.len() as u64 == cp.manifest_len
        && crc32c(&b[..b.len() - 4]) == cp.manifest_crc)
}

/// The #462 repoint law. The removal pass reclaims dead generations, and
/// `CURRENT` may name one — a crashed publish's residue, or (pre-clean) a
/// dirent-loss window that persisted the `CURRENT` rename while dropping
/// the `manifest-<g>` link (both are namespace ops of the same un-fsynced
/// window; F's fsynced-before-commit guarantee covers only COMMITTED
/// generations). The reader walk is deliberately scan-free (spec §13.2:
/// a missing manifest under an intact `CURRENT` is its corruption
/// tripwire), so a dangling `CURRENT` left durable would refuse a healthy
/// table forever. Therefore: normalize `CURRENT` FIRST, and fsync the
/// directory BEFORE the removal pass unlinks anything — one un-fsynced
/// window's namespace ops persist as arbitrary subsets, so the repoint and
/// the removals must never share a window. A `CURRENT` naming an intact,
/// genuinely in-progress candidate is the legal §13.3 pre-commit state and
/// is left untouched (its prev_gen chain reaches the effective generation).
///
/// Returns true when `CURRENT` was rewritten to the effective generation
/// (publish step-3 recipe) or unlinked (no effective generation — absent
/// `CURRENT` IS the empty-table posture the reader expects).
fn normalize_current(
    vfs: &mut dyn WriteVfs,
    dir: &str,
    eff: Option<&Manifest>,
    eff_gen: u64,
    probe: &dyn TxnProbe,
) -> WriteResult<bool> {
    let cur_path = full_path(dir, CURRENT_FILE_NAME);
    let exists = vfs.exists_path(&cur_path)?;
    let decoded = if exists {
        vfs.read_full(&cur_path)
            .ok()
            .and_then(|b| CommitPointer::decode(&b).ok())
    } else {
        None
    };
    let safe = match &decoded {
        Some(cp) => {
            // Will the named generation SURVIVE the removal pass? This
            // predicate mirrors the removal loop's keep decision exactly.
            let survives = if cp.gen != 0 && cp.gen == eff_gen {
                true // the effective generation itself (kept chain history)
            } else if cp.gen > eff_gen {
                matches!(
                    read_manifest_opt(vfs, dir, cp.gen)
                        .map(|m| probe.verdict(m.header.publisher_fxid)),
                    Some(TxnVerdict::InProgress)
                )
            } else {
                // gen 0, or an older-than-effective generation: the reader
                // would answer stale data — normalize.
                false
            };
            survives && pointer_matches_manifest(vfs, dir, cp)?
        }
        // Absent CURRENT is exactly right for an empty table; with an
        // effective generation it must be restored (walk agreement).
        None => !exists && eff.is_none(),
    };
    if safe {
        return Ok(false);
    }
    match eff {
        Some(_) => {
            // Publish step-3 recipe: tmp + fsync + rename + file fsync.
            let mbytes = vfs.read_full(&full_path(dir, &manifest_file_name(eff_gen)))?;
            let cp = CommitPointer::new(
                eff_gen,
                mbytes.len() as u64,
                crc32c(&mbytes[..mbytes.len() - 4]),
            );
            let ct_path = full_path(dir, CURRENT_TMP_FILE_NAME);
            let cfd = vfs.create_rw(&ct_path)?;
            vfs.pwrite_at(&cfd, 0, &cp.encode())?;
            vfs.fsync_file(&cfd)?;
            vfs.close_file(cfd)?;
            vfs.rename_path(&ct_path, &cur_path)?;
            let cfd2 = vfs.open_rw(&cur_path)?;
            vfs.fsync_file(&cfd2)?;
            vfs.close_file(cfd2)?;
        }
        None => {
            // Unreachable with exists == false (that state was `safe`), but
            // guard anyway: unlink on a missing name must not kill recovery.
            if exists {
                vfs.unlink_path(&cur_path)?;
            } else {
                return Ok(false);
            }
        }
    }
    // The barrier: the repoint/unlink is DURABLE before the removal pass
    // may unlink anything (the mid-recovery #462 window).
    vfs.fsync_dir(dir)?;
    Ok(true)
}

/// The abort/crash cleanup discipline (spec §13.3 tail): remove dead writer
/// temps, orphan parts, dead generations, and `CURRENT.tmp` residue.
/// Precondition: no concurrent publisher on this directory (caller's lock).
pub fn recover_and_clean(
    vfs: &mut dyn WriteVfs,
    dir: &str,
    probe: &dyn TxnProbe,
) -> WriteResult<RecoveryReport> {
    let eff = effective_manifest(vfs, dir, probe, None)?;
    let eff_gen = eff.as_ref().map(|m| m.header.gen).unwrap_or(0);
    let live_parts: std::collections::BTreeSet<u32> = eff
        .as_ref()
        .map(|m| m.parts.iter().map(|p| p.part_no).collect())
        .unwrap_or_default();

    let current_repointed = normalize_current(vfs, dir, eff.as_ref(), eff_gen, probe)?;
    let mut report = RecoveryReport {
        effective_gen: eff_gen,
        current_repointed,
        ..RecoveryReport::default()
    };
    for name in vfs.list_dir(dir)? {
        let remove = if dirlayout::is_temp_file_name(&name) {
            match parse_temp_fxid(&name) {
                // A live writer's scratch stays; everything else is dead.
                Some(fxid) => probe.verdict(fxid) != TxnVerdict::InProgress,
                None => true,
            }
        } else if let Some(pn) = parse_part_file_name(&name) {
            // Renamed by a publish whose txn never committed → orphan.
            !live_parts.contains(&pn)
        } else if let Some(g) = parse_manifest_file_name(&name) {
            if g <= eff_gen {
                false // chain history, immutable, cheap — kept.
            } else {
                // A generation past the effective one: dead unless its
                // publisher is genuinely still in progress.
                match read_manifest_opt(vfs, dir, g) {
                    Some(m) => match probe.verdict(m.header.publisher_fxid) {
                        TxnVerdict::InProgress => false,
                        TxnVerdict::Aborted => true,
                        TxnVerdict::Committed => {
                            // A committed gen NEWER than the effective walk
                            // found — structurally impossible on an intact
                            // chain.
                            return Err(WriteError::ManifestChain {
                                at: "committed generation above effective",
                            });
                        }
                    },
                    None => true, // undecodable residue
                }
            }
        } else if let Some(g) = pgrc2_format::bankstats::parse_bankstats_file_name(&name) {
            // [fmt-land] The bank-grain stats plane is DERIVED: keep only
            // the effective generation's (and a genuinely in-progress
            // publisher's — mirror of the manifest rule); planes of older
            // generations fail the reader's validity witness by
            // construction and are dead bytes.
            if g == eff_gen {
                false
            } else if g > eff_gen {
                !matches!(
                    read_manifest_opt(vfs, dir, g)
                        .map(|m| probe.verdict(m.header.publisher_fxid)),
                    Some(TxnVerdict::InProgress)
                )
            } else {
                true
            }
        } else {
            // `.pgrc2bs.tmp` = an interrupted plane write's residue (the
            // publish path unlinks it on the next attempt; recovery does
            // too). CURRENT.tmp exactly as before.
            name == CURRENT_TMP_FILE_NAME || name.ends_with(".pgrc2bs.tmp")
        };
        if remove {
            vfs.unlink_path(&full_path(dir, &name))?;
            report.removed.push(name);
        }
    }
    Ok(report)
}
