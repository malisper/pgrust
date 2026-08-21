//! [fmt-land] Bank-grain stats plane, generated AS DATA IS COPIED IN
//! (Michael 2026-08-17: "doing landing the bankstats plane sgtm. We should
//! generate it as we copy data in").
//!
//! Layout/validation law: `pgrc2_format::bankstats` (the fmt-layout lane's
//! format module). This module is the WRITE-PATH producer: every
//! `publish_parts` generation folds the freshly sealed parts' captured
//! slices (`SealedPart::plane_slices` — a SEAL byproduct, never a file
//! read-back) together with the prior generation's plane into
//! `bankstats-<gen>.pgrc2bs`, so freshly written banks carry the plane
//! from birth — no offline `--bankstats-build` pass required (the bench
//! builder stays as the BACKFILL tool for pre-existing banks).
//!
//! Discipline (the sidecar.rs doctrine, applied verbatim):
//! - DERIVED artifact: a pure function of the sealed parts, rebuildable at
//!   any time; never the source of truth. The reader's validity witness
//!   (gen + schema fingerprint + full per-part identity vector) binds it
//!   to one exact part-set — a compacted/appended/foreign part-set fails
//!   the witness and the reader falls back to per-part sections.
//! - BEST-EFFORT: any failure here (missing prior plane, IO error,
//!   arity skew) SKIPS the plane and never fails the publish — an absent
//!   plane is a slower read, never a wrong one.
//! - CRASH story: whole image into `<final>.tmp`, fsync, atomic rename,
//!   dir fsync — an interrupted write leaves only the `.tmp` (unlinked on
//!   the next publish attempt); the final name either holds a complete
//!   image or does not exist. A torn/forged final image fails the meta
//!   crc / column crc / identity witness and is refused typed
//!   (`pgrc2_format::bankstats::decode_meta` + `ColPayloadRef::new`).
//! - CARRY-FORWARD: an append publish reuses the prior generation's plane
//!   slices for the base parts (validated against the BASE manifest
//!   first) and appends the new parts' captured slices. A bank whose
//!   plane is missing (pre-plane lineage, or a crash exactly between
//!   manifest rename and plane rename) publishes WITHOUT a plane until
//!   the backfill tool runs once; every later publish carries it forward.
//! - GC: old-generation planes persist until the compaction/GC rung,
//!   exactly like `.pgrc2s` companions (sidecar.rs posture). When the M5
//!   compaction rung rewrites a part-set, its own publish writes the new
//!   generation's plane and the old ones are dead bytes with a failed
//!   witness, never misread.
//!
//! Kill switch: `PGRUST_PGRC2_BANKSTATS_SEAL=0|off` skips generation (the
//! A/B ingest-cost arm; default ON — the plane is a separate sidecar file,
//! part/manifest bytes are IDENTICAL either way, so blessed bank dirshas
//! over format files are unaffected; ledger row in lint-determinism.allow).

use crate::seal::SealedPart;
use crate::wvfs::WriteVfs;
use pgrc2_format::bankstats as fb;
use pgrc2_format::manifest::Manifest;

/// Kill switch (default ON). Read once, `pgsync::OnceLock`-cached.
pub fn seal_plane_enabled() -> bool {
    static F: pgsync::OnceLock<bool> = pgsync::OnceLock::new();
    *F.get_or_init(|| {
        !matches!(
            std::env::var("PGRUST_PGRC2_BANKSTATS_SEAL").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

/// The publish outcome witness for the plane write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlaneOutcome {
    /// Written + renamed; carries the final file name and image bytes.
    Written { file_name: String, bytes: u64 },
    /// Kill switch off.
    Disabled,
    /// Base parts exist but no valid prior plane to carry forward —
    /// the backfill tool owns this bank's first plane.
    NoPriorPlane,
    /// Structural skip (arity/column-set skew, IO failure). The publish
    /// itself succeeded; the plane is simply absent for this generation.
    Skipped { reason: &'static str },
}

fn ident_of(p: &pgrc2_format::manifest::PartRecord) -> fb::PartIdent {
    fb::PartIdent {
        part_no: p.part_no,
        granule_count: p.granule_count,
        band_count: p.band_count,
        rows: p.rows,
        footer_off: p.footer_off,
    }
}

/// Read + validate the BASE generation's plane and extract its per-column
/// slices. Returns `(attnos, per-col per-part (stats_body, digest))` or
/// `None` on any refusal.
#[allow(clippy::type_complexity)]
fn carry_forward(
    vfs: &mut dyn WriteVfs,
    dir: &str,
    base: &Manifest,
) -> Option<(Vec<u32>, Vec<Vec<(Vec<u8>, Option<[u8; fb::BANKSTATS_DIGEST_LEN]>)>>)> {
    let path = format!("{dir}/{}", fb::bankstats_file_name(base.header.gen));
    let bytes = vfs.read_full(&path).ok()?;
    let (header, pidents, cols) = fb::decode_meta(&bytes).ok()?;
    let ok = header.gen == base.header.gen
        && header.schema_fingerprint == base.header.schema_fingerprint
        && header.part_count as usize == base.parts.len()
        && pidents
            .iter()
            .zip(base.parts.iter())
            .all(|(a, b)| *a == ident_of(b));
    if !ok {
        return None;
    }
    let pc = base.parts.len();
    let mut attnos = Vec::with_capacity(cols.len());
    let mut out = Vec::with_capacity(cols.len());
    for e in &cols {
        if e.off as usize + e.len as usize > bytes.len() {
            return None;
        }
        let payload = &bytes[e.off as usize..(e.off + e.len) as usize];
        let view = fb::ColPayloadRef::new(e, header.part_count, payload).ok()?;
        let mut slices = Vec::with_capacity(pc);
        for pi in 0..pc {
            let body = view.stats_body(pi).map(|b| b.to_vec()).unwrap_or_default();
            let digest = view
                .digest(pi)
                .and_then(|d| <[u8; fb::BANKSTATS_DIGEST_LEN]>::try_from(d).ok());
            slices.push((body, digest));
        }
        attnos.push(e.attno);
        out.push(slices);
    }
    Some((attnos, out))
}

/// Assemble + durably publish `bankstats-<gen>.pgrc2bs` for the manifest
/// just published. `manifest.parts` = base parts (if any) followed by the
/// `sealed` parts in order (exactly `publish_parts`' construction).
/// Best-effort by doctrine: never returns Err.
pub fn publish_plane(
    vfs: &mut dyn WriteVfs,
    dir: &str,
    base: Option<&Manifest>,
    manifest: &Manifest,
    sealed: &[SealedPart],
) -> PlaneOutcome {
    if !seal_plane_enabled() {
        return PlaneOutcome::Disabled;
    }
    let base_count = manifest.parts.len() - sealed.len();
    // Canonical column set: the first sealed part's captured attno list
    // (attno-sorted at seal); every sealed part must agree.
    let Some(first) = sealed.first() else {
        return PlaneOutcome::Skipped {
            reason: "no sealed parts",
        };
    };
    let attnos: Vec<u32> = first.plane_slices.iter().map(|s| s.attno).collect();
    if attnos.is_empty() {
        return PlaneOutcome::Skipped {
            reason: "no plane slices captured",
        };
    }
    for s in sealed {
        if s.plane_slices.len() != attnos.len()
            || s.plane_slices
                .iter()
                .zip(attnos.iter())
                .any(|(sl, a)| sl.attno != *a)
        {
            return PlaneOutcome::Skipped {
                reason: "sealed-part column-set skew",
            };
        }
    }
    // Base carry-forward.
    let carried: Option<(Vec<u32>, Vec<Vec<(Vec<u8>, Option<[u8; 24]>)>>)> = if base_count > 0 {
        let b = base.expect("base manifest when base parts exist");
        match carry_forward(vfs, dir, b) {
            Some((ca, cs)) if ca == attnos => Some((ca, cs)),
            Some(_) => {
                return PlaneOutcome::Skipped {
                    reason: "prior plane column-set skew",
                }
            }
            None => return PlaneOutcome::NoPriorPlane,
        }
    } else {
        None
    };
    // Assemble ColInputs: base slices (owned, carried) then sealed slices
    // (borrowed from the seal captures).
    static EMPTY: [u8; 0] = [];
    let mut cols: Vec<fb::ColInput<'_>> = Vec::with_capacity(attnos.len());
    for (ci, attno) in attnos.iter().enumerate() {
        let mut bodies: Vec<&[u8]> = Vec::with_capacity(manifest.parts.len());
        let mut digests: Vec<Option<[u8; fb::BANKSTATS_DIGEST_LEN]>> =
            Vec::with_capacity(manifest.parts.len());
        if let Some((_, carried_cols)) = &carried {
            for (body, digest) in &carried_cols[ci] {
                bodies.push(if body.is_empty() { &EMPTY } else { body });
                digests.push(*digest);
            }
        }
        for s in sealed {
            let sl = &s.plane_slices[ci];
            bodies.push(&sl.stats_body);
            digests.push(sl.digest);
        }
        cols.push(fb::ColInput {
            attno: *attno,
            stats_bodies: bodies,
            digests,
        });
    }
    let idents: Vec<fb::PartIdent> = manifest.parts.iter().map(ident_of).collect();
    let Ok(image) = fb::build_image(
        manifest.header.gen,
        manifest.header.schema_fingerprint,
        &idents,
        &cols,
    ) else {
        return PlaneOutcome::Skipped {
            reason: "build_image refused",
        };
    };
    // Durable write: tmp + fsync + rename + dir fsync (sidecar.rs law).
    let final_name = fb::bankstats_file_name(manifest.header.gen);
    let final_path = format!("{dir}/{final_name}");
    let tmp_path = format!("{final_path}.tmp");
    let write = (|| -> crate::WriteResult<()> {
        match vfs.unlink_path(&tmp_path) {
            Ok(()) => {}
            Err(crate::WriteError::Io { errno, .. }) if errno == libc::ENOENT => {}
            Err(e) => return Err(e),
        }
        let fd = vfs.create_rw(&tmp_path)?;
        vfs.pwrite_at(&fd, 0, &image)?;
        vfs.fsync_file(&fd)?;
        vfs.close_file(fd)?;
        vfs.rename_path(&tmp_path, &final_path)?;
        vfs.fsync_dir(dir)?;
        Ok(())
    })();
    match write {
        Ok(()) => PlaneOutcome::Written {
            file_name: final_name,
            bytes: image.len() as u64,
        },
        Err(_) => PlaneOutcome::Skipped {
            reason: "plane write io",
        },
    }
}
