//! SimVfs composition battery (§5 M3-K: "SimVfs batteries over
//! seal/publish/manifest with the dirent-loss model"): crash at EVERY vfs-op
//! boundary of the publish window × S adversarial persistence outcomes
//! (torn sectors + dirent subsets/reorder) ⇒ the table is exactly OLD;
//! retry then yields exactly NEW; residue is reclaimed by name; a completed
//! + committed publish survives ANY adversarial outcome wholesale (the
//! acked arm). Born-RED: a publisher that skips the part-content fsync IS
//! caught by the same checker (torn-visible under a committed manifest).
//!
//! #462 extension: the RECOVERY window gets the same treatment — crash at
//! every vfs-op boundary of `recover_and_clean` × adversarial outcomes must
//! itself be old-or-new (recovery is rerunnable, and the repoint law means
//! CURRENT never survives naming a removed generation). Born-RED teeth: the
//! pre-#462 recovery shape (remove without repoint) and the wrong ordering
//! (remove, then repoint, no dir-fsync barrier) are both caught.

mod common;

use pgrc2_qa::adapters::{memdir_of, Probe};
use pgrc2_qa::corpus::{
    append_fixture_rows, finish, int8_fixture, open_writer, verify_manifest, Fixture,
};
use pgrc2_qa::harness::round_fxid;
use pgrc2_qa::simvfs::SimVfs;
use pgrc2_qa::XorShift;
use pgrc2_format::dirlayout::{
    manifest_file_name, part_file_name, CURRENT_FILE_NAME, CURRENT_TMP_FILE_NAME,
};
use pgrc2_format::manifest::{CommitPointer, Manifest, ManifestHeader, PartRecord, MANIFEST_MAGIC};
use pgrc2_format::part::{FooterFixed, PartHeader};
use pgrc2_format::wire::crc32c;
use pgrc2_read::manifest_walk::{resolve_effective, TableExpect};
use pgrc2_write::publish::{
    effective_manifest, publish_parts, recover_and_clean, TxnVerdict,
};
use pgrc2_write::seal::{PartSpec, SealedPart};
use pgrc2_write::writer::PartCutPolicy;
use pgrc2_write::wvfs::WriteVfs;
use std::collections::BTreeSet;

const N: u64 = 900; // rows per generation (2 generations in the oracle)

fn sweep_fixture() -> Fixture {
    int8_fixture(
        "simvfs_sweep",
        201,
        2 * N,
        vec![pgrc2_qa::adapters::ForcedPlan::ByteFor {
            delta_width: 8,
            signed: true,
        }],
        PartCutPolicy::default(),
        |i| {
            if i % 6 == 1 {
                None
            } else {
                Some((i as i64).wrapping_mul(0x9E37_79B9) ^ 0x55AA)
            }
        },
    )
}

struct Ctx {
    fx: Fixture,
    fxid1: u64,
    fxid2: u64,
}

/// Base state: gen1 (rows 0..N) published + committed, everything durable;
/// gen2's rows (N..2N) SEALED (tmp files volatile) with the SealedPart
/// descriptors returned for replayable publishes.
fn base_state(ctx: &Ctx) -> (SimVfs, Probe, Vec<SealedPart>, PartSpec) {
    let mut vfs = SimVfs::new();
    vfs.mkdir_path(&ctx.fx.dir).expect("mkdir");
    let mut probe = Probe::new(TxnVerdict::Aborted);
    probe.mark(ctx.fxid1, TxnVerdict::InProgress);
    let mut w = open_writer(&ctx.fx, ctx.fxid1).expect("open w1");
    append_fixture_rows(&mut vfs, &mut w, &ctx.fx, 0, N).expect("rows gen1");
    finish(&mut vfs, &mut w, &ctx.fx).expect("finish gen1");
    w.publish(&mut vfs, &probe).expect("publish gen1");
    probe.mark(ctx.fxid1, TxnVerdict::Committed);

    // Seal gen2 (publish deferred — replayed per trial via publish_parts).
    probe.mark(ctx.fxid2, TxnVerdict::InProgress);
    let mut w2 = open_writer(&ctx.fx, ctx.fxid2).expect("open w2");
    append_fixture_rows(&mut vfs, &mut w2, &ctx.fx, N, 2 * N).expect("rows gen2");
    finish(&mut vfs, &mut w2, &ctx.fx).expect("finish gen2");
    let sealed = w2.sealed_parts().to_vec();
    let spec = *w2.spec();
    (vfs, probe, sealed, spec)
}

/// Adjudicate a post-crash universe where gen2's publisher never committed:
/// effective must be EXACTLY gen1 (both walks), decode == oracle prefix,
/// cleanup leaves exactly the live set, and a fresh publish succeeds.
fn adjudicate_old_then_retry(
    vfs: &mut SimVfs,
    ctx: &Ctx,
    label: &str,
) -> Result<(), String> {
    let probe = Probe::new(TxnVerdict::Aborted)
        .set(ctx.fxid1, TxnVerdict::Committed)
        .set(ctx.fxid2, TxnVerdict::Aborted);
    // Both walks agree on OLD.
    let eff = effective_manifest(vfs, &ctx.fx.dir, &probe)
        .map_err(|e| format!("{label}: effective: {e}"))?
        .ok_or_else(|| format!("{label}: table lost gen1"))?;
    if eff.header.gen != 1 {
        return Err(format!("{label}: expected gen1, got gen{}", eff.header.gen));
    }
    let files = vfs.snapshot_dir(&ctx.fx.dir);
    let rdir = memdir_of(&files);
    match resolve_effective(&rdir, &probe, &TableExpect::default()) {
        Ok(Some(reff)) => {
            if reff.manifest.header.gen != 1 {
                return Err(format!(
                    "{label}: reader walk gen{}",
                    reff.manifest.header.gen
                ));
            }
        }
        Ok(None) => return Err(format!("{label}: reader walk lost gen1")),
        // Legal UN-RECOVERED residue (not the pinned #462 defect, which is
        // closed): a pre-commit dirent-loss window persisted the CURRENT
        // rename but dropped the manifest-2 link. The writer walk above
        // already proved OLD; the #462 repoint law makes recovery repair
        // this, enforced by the STRICT post-recovery check below.
        Err(pgrc2_read::ReadError::ManifestMissing { gen: 2 }) => {}
        Err(e) => return Err(format!("{label}: reader walk: {e:?}")),
    }
    let rows = verify_manifest(&files, &eff, &ctx.fx)
        .map_err(|e| format!("{label}: old decode-vs-oracle: {e:?}"))?;
    if rows != N {
        return Err(format!("{label}: old rows {rows} != {N}"));
    }
    // Residue reclaim.
    recover_and_clean(vfs, &ctx.fx.dir, &probe)
        .map_err(|e| format!("{label}: recover: {e}"))?;
    // STRICT post-recovery agreement (issue #462 closed): the repoint law
    // guarantees the reader walk answers EXACTLY gen1 on the recovered dir.
    let files_pr = vfs.snapshot_dir(&ctx.fx.dir);
    let rdir_pr = memdir_of(&files_pr);
    match resolve_effective(&rdir_pr, &probe, &TableExpect::default()) {
        Ok(Some(r)) if r.manifest.header.gen == 1 => {}
        other => {
            return Err(format!(
                "{label}: post-recovery reader walk must agree on gen1 (#462 law), got {other:?}"
            ))
        }
    }
    let after: BTreeSet<String> = vfs
        .list_dir(&ctx.fx.dir)
        .map_err(|e| format!("{label}: listdir: {e}"))?
        .into_iter()
        .collect();
    let mut allowed: BTreeSet<String> = eff
        .parts
        .iter()
        .map(|p| part_file_name(p.part_no))
        .collect();
    allowed.insert(manifest_file_name(1));
    allowed.insert(CURRENT_FILE_NAME.to_string());
    // [fmt-land] gen1's bank-grain stats plane is a LIVE derived sidecar:
    // `recover_and_clean` keeps exactly the effective generation's plane
    // (the dead gen2 plane is reclaimed with the dead generation). Allowed
    // but never REQUIRED (best-effort by charter); when present it must
    // positively prove its binding to gen1's part-set.
    let plane = pgrc2_format::bankstats::bankstats_file_name(1);
    if after.contains(&plane) {
        let bytes = vfs
            .read_full(&format!("{}/{plane}", ctx.fx.dir))
            .map_err(|e| format!("{label}: read live plane: {e}"))?;
        let (h, pidents, _cols) = pgrc2_format::bankstats::decode_meta(&bytes)
            .map_err(|e| format!("{label}: live plane undecodable: {e:?}"))?;
        let bound = h.gen == 1
            && h.schema_fingerprint == eff.header.schema_fingerprint
            && h.part_count as usize == eff.parts.len()
            && pidents.iter().zip(eff.parts.iter()).all(|(a, b)| {
                a.part_no == b.part_no
                    && a.granule_count == b.granule_count
                    && a.band_count == b.band_count
                    && a.rows == b.rows
                    && a.footer_off == b.footer_off
            });
        if !bound {
            return Err(format!("{label}: live plane fails the identity witness"));
        }
    }
    allowed.insert(plane);
    for f in &after {
        if !allowed.contains(f) {
            return Err(format!("{label}: residue survived cleanup: {f}"));
        }
    }
    // Retry with a fresh fxid: reseal + publish + commit ⇒ exactly NEW.
    let fxid3 = round_fxid(7, 3);
    let probe2 = Probe::new(TxnVerdict::Aborted)
        .set(ctx.fxid1, TxnVerdict::Committed)
        .set(ctx.fxid2, TxnVerdict::Aborted)
        .set(fxid3, TxnVerdict::InProgress);
    let mut w3 = open_writer(&ctx.fx, fxid3).map_err(|e| format!("{label}: open w3: {e}"))?;
    append_fixture_rows(vfs, &mut w3, &ctx.fx, N, 2 * N)
        .map_err(|e| format!("{label}: reseal: {e}"))?;
    finish(vfs, &mut w3, &ctx.fx).map_err(|e| format!("{label}: finish3: {e}"))?;
    w3.publish(vfs, &probe2)
        .map_err(|e| format!("{label}: retry publish: {e}"))?;
    let probe3 = Probe::new(TxnVerdict::Aborted)
        .set(ctx.fxid1, TxnVerdict::Committed)
        .set(fxid3, TxnVerdict::Committed);
    let eff2 = effective_manifest(vfs, &ctx.fx.dir, &probe3)
        .map_err(|e| format!("{label}: effective2: {e}"))?
        .ok_or_else(|| format!("{label}: retry lost table"))?;
    let files2 = vfs.snapshot_dir(&ctx.fx.dir);
    let rows2 = verify_manifest(&files2, &eff2, &ctx.fx)
        .map_err(|e| format!("{label}: new decode-vs-oracle: {e:?}"))?;
    if rows2 != 2 * N {
        return Err(format!("{label}: new rows {rows2} != {}", 2 * N));
    }
    Ok(())
}

/// The main sweep: every op boundary of the publish window × seeds.
#[test]
fn publish_boundary_sweep_torn_sectors_and_dirent_loss() {
    let s = common::scale();
    let ctx = Ctx {
        fx: sweep_fixture(),
        fxid1: round_fxid(1, 0),
        fxid2: round_fxid(2, 0),
    };
    // Baseline op count of the publish window (uncrashed clone).
    let (vfs0, probe0, sealed, spec) = base_state(&ctx);
    let total_ops = {
        let mut v = vfs0.clone();
        let before = v.op_count();
        publish_parts(&mut v, &ctx.fx.dir, &spec, &sealed, ctx.fxid2, &probe0)
            .expect("uncrashed publish");
        v.op_count() - before
    };
    // Tooth: the sweep denominator is real.
    assert!(
        total_ops >= 15,
        "publish window has only {total_ops} vfs ops — sweep denominator broken"
    );

    let mut trials = 0u64;
    let mut err_points = 0u64;
    let mut acked_tail_points = 0u64;
    for n in 1..=total_ops {
        // Crash before op n.
        let mut crashed = vfs0.clone();
        crashed.crash_at_op(n);
        let res = publish_parts(
            &mut crashed,
            &ctx.fx.dir,
            &spec,
            &sealed,
            ctx.fxid2,
            &probe0,
        );
        assert!(crashed.killed(), "crash flag not set at op {n}");
        // A killed universe returning Ok is legal ONLY in the post-ack
        // tail: publish's durable ack is manifest+CURRENT+fsync_dir, and
        // the [fmt-land] step-4.5 bankstats plane maintenance runs AFTER
        // that ack, best-effort by charter (its failure never fails the
        // publish) — so a crash landing in the plane's ops acks first.
        // Such a point must positively PROVE the honest ack per seed
        // below; the Err arm keeps the whole old-arm oracle unchanged.
        let acked = match &res {
            Err(_) => {
                err_points += 1;
                false
            }
            Ok(out) => {
                assert_eq!(out.gen, 2, "op {n}: killed+Ok acked an unexpected generation");
                acked_tail_points += 1;
                true
            }
        };
        for seed in 0..s.simvfs_seeds {
            let mut universe = crashed.clone();
            let mut rng = XorShift::new(0xC0FFEE ^ (n << 8) ^ seed);
            universe.crash_and_revive(&mut rng);
            trials += 1;
            if acked {
                // The ack must be honest under EVERY adversary: with the
                // publisher committed, gen2 is effective and decodes to
                // the full oracle byte-for-byte.
                let probe_c = Probe::new(TxnVerdict::Aborted)
                    .set(ctx.fxid1, TxnVerdict::Committed)
                    .set(ctx.fxid2, TxnVerdict::Committed);
                let mut u2 = universe.clone();
                let eff = effective_manifest(&mut u2, &ctx.fx.dir, &probe_c)
                    .unwrap_or_else(|e| panic!("op{n} seed{seed}: acked effective: {e}"))
                    .unwrap_or_else(|| panic!("op{n} seed{seed}: acked publish lost"));
                assert_eq!(eff.header.gen, 2, "op{n} seed{seed}: acked gen not effective");
                let files = u2.snapshot_dir(&ctx.fx.dir);
                let rows = verify_manifest(&files, &eff, &ctx.fx).unwrap_or_else(|e| {
                    panic!("op{n} seed{seed}: acked decode-vs-oracle: {e:?}")
                });
                assert_eq!(rows, 2 * N, "op{n} seed{seed}: acked rows");
            }
            // In BOTH arms the publisher never COMMITTED (no commit
            // record), so the aborted world stays lawful: exactly OLD
            // after recovery, then retry yields exactly NEW.
            if let Err(msg) = adjudicate_old_then_retry(&mut universe, &ctx, &format!("op{n} seed{seed}"))
            {
                panic!("SimVfs battery failure: {msg}");
            }
        }
    }
    // Tooth: the battery ran the full grid, and BOTH arms are populated —
    // pre-ack crashes erred, and (with the plane enabled) the post-ack
    // tail produced killed+Ok points that were positively adjudicated.
    assert_eq!(trials, total_ops * s.simvfs_seeds);
    assert!(err_points > 0, "no crash point erred — the sweep exercised nothing");
    if pgrc2_write::bankplane::seal_plane_enabled() {
        assert!(
            acked_tail_points > 0,
            "bankstats plane enabled but no post-ack tail point swept"
        );
    }
    println!(
        "simvfs sweep: {total_ops} boundaries x {} seeds = {trials} universes, all old-or-new",
        s.simvfs_seeds
    );
}

/// Seal-window crashes compose the same way (coarse stride — D's matrix
/// owns the exhaustive seal sweep; this leg proves the COMPOSITION).
#[test]
fn seal_window_crash_composes() {
    let s = common::scale();
    let ctx = Ctx {
        fx: sweep_fixture(),
        fxid1: round_fxid(1, 0),
        fxid2: round_fxid(2, 0),
    };
    // Build gen1 only.
    let mut vfs = SimVfs::new();
    vfs.mkdir_path(&ctx.fx.dir).expect("mkdir");
    let mut probe = Probe::new(TxnVerdict::Aborted);
    probe.mark(ctx.fxid1, TxnVerdict::InProgress);
    let mut w = open_writer(&ctx.fx, ctx.fxid1).expect("open w1");
    append_fixture_rows(&mut vfs, &mut w, &ctx.fx, 0, N).expect("rows gen1");
    finish(&mut vfs, &mut w, &ctx.fx).expect("finish gen1");
    w.publish(&mut vfs, &probe).expect("publish gen1");
    let base_ops = vfs.op_count();

    // Measure the seal+publish window of gen2 on a clone.
    let window = {
        let mut v = vfs.clone();
        let p = Probe::new(TxnVerdict::Aborted)
            .set(ctx.fxid1, TxnVerdict::Committed)
            .set(ctx.fxid2, TxnVerdict::InProgress);
        let mut w2 = open_writer(&ctx.fx, ctx.fxid2).expect("open w2");
        append_fixture_rows(&mut v, &mut w2, &ctx.fx, N, 2 * N).expect("rows");
        finish(&mut v, &mut w2, &ctx.fx).expect("finish");
        w2.publish(&mut v, &p).expect("publish");
        v.op_count() - base_ops
    };
    let stride = (window / 9).max(1);
    let mut points = 0u64;
    let mut n = 1;
    while n <= window {
        let mut crashed = vfs.clone();
        crashed.crash_at_op(n);
        let p = Probe::new(TxnVerdict::Aborted)
            .set(ctx.fxid1, TxnVerdict::Committed)
            .set(ctx.fxid2, TxnVerdict::InProgress);
        let mut w2 = open_writer(&ctx.fx, ctx.fxid2).expect("open w2");
        let r = append_fixture_rows(&mut crashed, &mut w2, &ctx.fx, N, 2 * N)
            .and_then(|()| finish(&mut crashed, &mut w2, &ctx.fx))
            .and_then(|()| w2.publish(&mut crashed, &p).map(|out| out.gen));
        assert!(crashed.killed(), "armed crash at op {n} did not fire");
        // Killed+Ok is legal ONLY for the post-ack tail (the [fmt-land]
        // step-4.5 bankstats plane runs after publish's durable ack,
        // best-effort by charter) — and then the ack must be proven
        // honest per seed; killed+Err keeps the whole old-arm oracle.
        let acked = match &r {
            Err(_) => false,
            Ok(gen) => {
                assert_eq!(*gen, 2, "seal op {n}: killed+Ok acked an unexpected generation");
                true
            }
        };
        for seed in 0..s.simvfs_seeds.min(2) {
            let mut universe = crashed.clone();
            let mut rng = XorShift::new(0xBEEF ^ (n << 6) ^ seed);
            universe.crash_and_revive(&mut rng);
            if acked {
                let probe_c = Probe::new(TxnVerdict::Aborted)
                    .set(ctx.fxid1, TxnVerdict::Committed)
                    .set(ctx.fxid2, TxnVerdict::Committed);
                let mut u2 = universe.clone();
                let eff = effective_manifest(&mut u2, &ctx.fx.dir, &probe_c)
                    .unwrap_or_else(|e| panic!("seal op{n} seed{seed}: acked effective: {e}"))
                    .unwrap_or_else(|| panic!("seal op{n} seed{seed}: acked publish lost"));
                assert_eq!(eff.header.gen, 2, "seal op{n} seed{seed}: acked gen not effective");
                let files = u2.snapshot_dir(&ctx.fx.dir);
                let rows = verify_manifest(&files, &eff, &ctx.fx).unwrap_or_else(|e| {
                    panic!("seal op{n} seed{seed}: acked decode-vs-oracle: {e:?}")
                });
                assert_eq!(rows, 2 * N, "seal op{n} seed{seed}: acked rows");
            }
            if let Err(msg) =
                adjudicate_old_then_retry(&mut universe, &ctx, &format!("seal op{n} seed{seed}"))
            {
                panic!("seal-window composition failure: {msg}");
            }
        }
        points += 1;
        n += stride;
    }
    assert!(points >= 8, "seal window stride produced only {points} points");
}

/// The acked arm: a COMPLETED publish with a COMMITTED publisher survives
/// every adversarial persistence outcome wholesale (#253).
#[test]
fn acked_publish_survives_any_adversary() {
    let s = common::scale();
    let ctx = Ctx {
        fx: sweep_fixture(),
        fxid1: round_fxid(1, 0),
        fxid2: round_fxid(2, 0),
    };
    let (vfs0, probe0, sealed, spec) = base_state(&ctx);
    let mut v = vfs0.clone();
    publish_parts(&mut v, &ctx.fx.dir, &spec, &sealed, ctx.fxid2, &probe0).expect("publish");
    let probe = Probe::new(TxnVerdict::Aborted)
        .set(ctx.fxid1, TxnVerdict::Committed)
        .set(ctx.fxid2, TxnVerdict::Committed);
    for seed in 0..(s.simvfs_seeds * 2).max(4) {
        let mut universe = v.clone();
        let mut rng = XorShift::new(0xACED ^ seed);
        universe.crash_and_revive(&mut rng);
        let eff = effective_manifest(&mut universe, &ctx.fx.dir, &probe)
            .expect("effective")
            .expect("acked table present");
        assert_eq!(eff.header.gen, 2, "acked publish lost (seed {seed})");
        let files = universe.snapshot_dir(&ctx.fx.dir);
        let rows = verify_manifest(&files, &eff, &ctx.fx)
            .unwrap_or_else(|e| panic!("acked data damaged (seed {seed}): {e:?}"));
        assert_eq!(rows, 2 * N);
    }
}

/// Born-RED tooth: a publisher that SKIPS the part-content fsync is caught
/// by the same checker — some adversarial outcome tears the visible part.
#[test]
fn born_red_no_fsync_publisher_is_caught() {
    let ctx = Ctx {
        fx: sweep_fixture(),
        fxid1: round_fxid(1, 0),
        fxid2: round_fxid(2, 0),
    };
    let (vfs0, _probe0, sealed, spec) = base_state(&ctx);

    // The sabotaged five steps: NO content fsync on part files.
    fn bad_publish(
        vfs: &mut SimVfs,
        dir: &str,
        spec: &PartSpec,
        sealed: &[SealedPart],
        fxid: u64,
        prev: &Manifest,
    ) {
        let mut next_part_no = prev.header.next_part_no;
        let mut parts = prev.parts.clone();
        for s in sealed {
            let part_no = next_part_no;
            next_part_no += 1;
            let tmp = format!("{dir}/{}", s.tmp_name);
            let fd = vfs.open_rw(&tmp).expect("open tmp");
            let mut hdr = Vec::new();
            PartHeader::new(part_no, spec.schema_fingerprint, spec.spc, spec.db, spec.relfilenumber)
                .encode_into(&mut hdr);
            vfs.pwrite_at(&fd, 0, &hdr).expect("hdr");
            let mut footer = FooterFixed::decode(&s.footer_image).expect("footer");
            footer.part_no = part_no;
            let mut fb = Vec::new();
            footer.encode_into(&mut fb);
            vfs.pwrite_at(&fd, s.footer_off, &fb).expect("footer");
            // SIN: no fsync_file here.
            vfs.close_file(fd).expect("close");
            vfs.rename_path(&tmp, &format!("{dir}/{}", part_file_name(part_no)))
                .expect("rename");
            parts.push(PartRecord {
                rows: s.rows,
                file_len: s.file_len,
                footer_off: s.footer_off,
                dv_gen: 0,
                dv_len: 0,
                part_no,
                flags: 0,
                granule_count: s.granule_count,
                band_count: s.band_count,
                dv_crc: 0,
                granule_rows: 0,
            });
        }
        let gen = prev.header.gen + 1;
        let manifest = Manifest {
            header: ManifestHeader {
                gen,
                prev_gen: prev.header.gen,
                publisher_fxid: fxid,
                relfilenumber: spec.relfilenumber,
                schema_fingerprint: spec.schema_fingerprint,
                magic: MANIFEST_MAGIC,
                format_version: pgrc2_format::FORMAT_VERSION,
                spc: spec.spc,
                db: spec.db,
                part_count: parts.len() as u32,
                next_part_no,
                flags: 0,
                reserved: 0,
            },
            parts,
        };
        let mb = manifest.encode();
        let mpath = format!("{dir}/{}", manifest_file_name(gen));
        let mfd = vfs.create_rw(&mpath).expect("manifest create");
        vfs.pwrite_at(&mfd, 0, &mb).expect("manifest write");
        vfs.fsync_file(&mfd).expect("manifest fsync");
        vfs.close_file(mfd).expect("manifest close");
        let cp = CommitPointer::new(gen, mb.len() as u64, crc32c(&mb));
        let ct = format!("{dir}/{CURRENT_TMP_FILE_NAME}");
        let cfd = vfs.create_rw(&ct).expect("cur tmp");
        vfs.pwrite_at(&cfd, 0, &cp.encode()).expect("cur write");
        vfs.fsync_file(&cfd).expect("cur fsync");
        vfs.close_file(cfd).expect("cur close");
        vfs.rename_path(&ct, &format!("{dir}/{CURRENT_FILE_NAME}")).expect("cur rename");
        let c2 = vfs.open_rw(&format!("{dir}/{CURRENT_FILE_NAME}")).expect("cur open");
        vfs.fsync_file(&c2).expect("cur fsync2");
        vfs.close_file(c2).expect("cur close2");
        vfs.fsync_dir(dir).expect("dir fsync");
    }

    let mut v = vfs0.clone();
    let probe_walk = Probe::new(TxnVerdict::Aborted).set(ctx.fxid1, TxnVerdict::Committed);
    let prev = effective_manifest(&mut v, &ctx.fx.dir, &probe_walk)
        .expect("effective")
        .expect("gen1");
    bad_publish(&mut v, &ctx.fx.dir, &spec, &sealed, ctx.fxid2, &prev);

    // Publisher commits — and SOME adversarial outcome must now show a torn
    // part under a committed, effective manifest.
    let probe = Probe::new(TxnVerdict::Aborted)
        .set(ctx.fxid1, TxnVerdict::Committed)
        .set(ctx.fxid2, TxnVerdict::Committed);
    let mut caught = false;
    for seed in 0..16u64 {
        let mut universe = v.clone();
        let mut rng = XorShift::new(0xDEAD_2026 ^ seed);
        universe.crash_and_revive(&mut rng);
        let eff = match effective_manifest(&mut universe, &ctx.fx.dir, &probe) {
            Ok(Some(m)) => m,
            _ => continue,
        };
        if eff.header.gen != 2 {
            continue;
        }
        let files = universe.snapshot_dir(&ctx.fx.dir);
        if verify_manifest(&files, &eff, &ctx.fx).is_err() {
            caught = true;
            break;
        }
    }
    assert!(
        caught,
        "born-RED failure: the no-fsync publisher was NOT caught by any adversarial outcome — \
         the torn-sector detector is toothless"
    );
}

/// The #462 state every recovery leg below starts from: gen1 committed,
/// gen2 published DURABLY (all four steps + no commit) by an fxid the probe
/// calls Aborted — CURRENT durably names the dead gen2.
fn dead_gen2_state(ctx: &Ctx) -> (SimVfs, Probe) {
    let (vfs0, probe0, sealed, spec) = base_state(ctx);
    let mut v = vfs0.clone();
    publish_parts(&mut v, &ctx.fx.dir, &spec, &sealed, ctx.fxid2, &probe0)
        .expect("durable publish of gen2");
    let probe = Probe::new(TxnVerdict::Aborted)
        .set(ctx.fxid1, TxnVerdict::Committed)
        .set(ctx.fxid2, TxnVerdict::Aborted);
    (v, probe)
}

/// #462 leg 1: crash at EVERY op boundary of the recovery window × seeds.
/// A crash mid-recovery must itself be old-or-new: re-recovery completes,
/// both walks agree on gen1, residue is reclaimed, retry yields NEW —
/// adjudicated by the same strict checker as the publish sweep.
#[test]
fn recovery_window_crash_sweep_never_dangles() {
    let s = common::scale();
    let ctx = Ctx {
        fx: sweep_fixture(),
        fxid1: round_fxid(1, 0),
        fxid2: round_fxid(2, 0),
    };
    let (v, probe) = dead_gen2_state(&ctx);
    // Baseline op count of the recovery window (uncrashed clone).
    let window = {
        let mut c = v.clone();
        let before = c.op_count();
        let rep = recover_and_clean(&mut c, &ctx.fx.dir, &probe).expect("uncrashed recovery");
        assert!(rep.current_repointed, "premise: this state requires a repoint");
        assert!(
            rep.removed.iter().any(|n| n == &manifest_file_name(2)),
            "premise: dead gen2 reclaimed"
        );
        c.op_count() - before
    };
    // Tooth: the sweep denominator is real (repoint + removal ⇒ many ops).
    assert!(
        window >= 10,
        "recovery window has only {window} vfs ops — sweep denominator broken"
    );
    let mut trials = 0u64;
    for n in 1..=window {
        let mut crashed = v.clone();
        crashed.crash_at_op(n);
        let err = recover_and_clean(&mut crashed, &ctx.fx.dir, &probe);
        assert!(err.is_err(), "armed crash at recovery op {n} did not fire");
        assert!(crashed.killed(), "crash flag not set at recovery op {n}");
        for seed in 0..s.simvfs_seeds {
            let mut universe = crashed.clone();
            let mut rng = XorShift::new(0x4620_0462 ^ (n << 8) ^ seed);
            universe.crash_and_revive(&mut rng);
            trials += 1;
            if let Err(msg) =
                adjudicate_old_then_retry(&mut universe, &ctx, &format!("recovery op{n} seed{seed}"))
            {
                panic!("recovery-window sweep failure: {msg}");
            }
        }
    }
    assert_eq!(trials, window * s.simvfs_seeds);
    println!(
        "recovery sweep: {window} boundaries x {} seeds = {trials} universes, all old-or-new",
        s.simvfs_seeds
    );
}

/// #462 leg 2, empty-table shape: the ONLY generation is durable but
/// uncommitted; recovery unlinks CURRENT (empty-table posture). Crash at
/// every boundary of THAT window: re-recovery must leave both walks
/// agreeing on EMPTY, and a fresh publish+commit must yield exactly NEW.
#[test]
fn recovery_window_crash_sweep_empty_table() {
    let s = common::scale();
    let ctx = Ctx {
        fx: sweep_fixture(),
        fxid1: round_fxid(1, 0),
        fxid2: round_fxid(2, 0),
    };
    // Gen1 published durably, publisher ABORTED — nothing effective.
    let mut v = SimVfs::new();
    v.mkdir_path(&ctx.fx.dir).expect("mkdir");
    let probe_pub = Probe::new(TxnVerdict::Aborted).set(ctx.fxid1, TxnVerdict::InProgress);
    let mut w = open_writer(&ctx.fx, ctx.fxid1).expect("open w1");
    append_fixture_rows(&mut v, &mut w, &ctx.fx, 0, N).expect("rows gen1");
    finish(&mut v, &mut w, &ctx.fx).expect("finish gen1");
    w.publish(&mut v, &probe_pub).expect("publish gen1");
    let probe = Probe::new(TxnVerdict::Aborted);
    let window = {
        let mut c = v.clone();
        let before = c.op_count();
        let rep = recover_and_clean(&mut c, &ctx.fx.dir, &probe).expect("uncrashed recovery");
        assert!(rep.current_repointed, "premise: CURRENT unlinked");
        assert_eq!(rep.effective_gen, 0);
        c.op_count() - before
    };
    assert!(window >= 6, "empty-table recovery window only {window} ops");
    for n in 1..=window {
        let mut crashed = v.clone();
        crashed.crash_at_op(n);
        assert!(
            recover_and_clean(&mut crashed, &ctx.fx.dir, &probe).is_err(),
            "armed crash at op {n} did not fire"
        );
        for seed in 0..s.simvfs_seeds {
            let mut universe = crashed.clone();
            let mut rng = XorShift::new(0x4620_EE ^ (n << 8) ^ seed);
            universe.crash_and_revive(&mut rng);
            let label = format!("empty op{n} seed{seed}");
            // Re-recovery completes and the walks agree on EMPTY.
            recover_and_clean(&mut universe, &ctx.fx.dir, &probe)
                .unwrap_or_else(|e| panic!("{label}: re-recovery: {e}"));
            let eff = effective_manifest(&mut universe, &ctx.fx.dir, &probe)
                .unwrap_or_else(|e| panic!("{label}: effective: {e}"));
            assert!(eff.is_none(), "{label}: writer walk resurrected a dead gen");
            let files = universe.snapshot_dir(&ctx.fx.dir);
            let rdir = memdir_of(&files);
            match resolve_effective(&rdir, &probe, &TableExpect::default()) {
                Ok(None) => {}
                other => panic!("{label}: reader walk must see EMPTY, got {other:?}"),
            }
            // Fresh publish + commit ⇒ exactly NEW (name reclaim works).
            let fxid3 = round_fxid(9, 5);
            let probe2 = Probe::new(TxnVerdict::Aborted).set(fxid3, TxnVerdict::InProgress);
            let mut w3 =
                open_writer(&ctx.fx, fxid3).unwrap_or_else(|e| panic!("{label}: open: {e}"));
            append_fixture_rows(&mut universe, &mut w3, &ctx.fx, 0, N)
                .unwrap_or_else(|e| panic!("{label}: rows: {e}"));
            finish(&mut universe, &mut w3, &ctx.fx)
                .unwrap_or_else(|e| panic!("{label}: finish: {e}"));
            w3.publish(&mut universe, &probe2)
                .unwrap_or_else(|e| panic!("{label}: publish: {e}"));
            let probe3 = Probe::new(TxnVerdict::Aborted).set(fxid3, TxnVerdict::Committed);
            let eff2 = effective_manifest(&mut universe, &ctx.fx.dir, &probe3)
                .unwrap_or_else(|e| panic!("{label}: effective2: {e}"))
                .unwrap_or_else(|| panic!("{label}: retry lost table"));
            assert_eq!(eff2.header.gen, 1, "{label}");
            let files2 = universe.snapshot_dir(&ctx.fx.dir);
            let rows = verify_manifest(&files2, &eff2, &ctx.fx)
                .unwrap_or_else(|e| panic!("{label}: decode-vs-oracle: {e:?}"));
            assert_eq!(rows, N, "{label}");
        }
    }
}

/// Born-RED tooth 1: the PRE-#462 recovery shape — remove the dead
/// generation, never touch CURRENT — must be REJECTED by the strict
/// post-recovery agreement check (the exact signature the old code left).
#[test]
fn born_red_remove_without_repoint_is_caught() {
    let ctx = Ctx {
        fx: sweep_fixture(),
        fxid1: round_fxid(1, 0),
        fxid2: round_fxid(2, 0),
    };
    let (mut v, probe) = dead_gen2_state(&ctx);
    // SIN (the old recover_and_clean): unlink manifest-2, leave CURRENT.
    v.unlink_path(&format!("{}/{}", ctx.fx.dir, manifest_file_name(2)))
        .expect("unlink manifest-2");
    let files = v.snapshot_dir(&ctx.fx.dir);
    let rdir = memdir_of(&files);
    match resolve_effective(&rdir, &probe, &TableExpect::default()) {
        Err(pgrc2_read::ReadError::ManifestMissing { gen: 2 }) => {} // caught
        other => panic!(
            "born-RED failure: remove-without-repoint was NOT caught by the strict \
             post-recovery check — got {other:?}"
        ),
    }
}

/// Born-RED tooth 2: the WRONG ordering — remove first, then repoint, no
/// dir-fsync barrier between — must be caught by some adversarial outcome
/// (the unlink persists while the CURRENT rename does not ⇒ dangling).
#[test]
fn born_red_repoint_after_remove_is_caught() {
    let ctx = Ctx {
        fx: sweep_fixture(),
        fxid1: round_fxid(1, 0),
        fxid2: round_fxid(2, 0),
    };
    let (v0, probe) = dead_gen2_state(&ctx);
    let mut v = v0.clone();
    let dir = ctx.fx.dir.clone();
    // SIN 1: unlink the dead generation FIRST.
    v.unlink_path(&format!("{dir}/{}", manifest_file_name(2)))
        .expect("unlink manifest-2");
    // SIN 2: repoint CURRENT afterwards, correctly in isolation (tmp +
    // fsync + rename + file fsync) but with NO dir fsync anywhere — the
    // unlink and the rename share one un-fsynced window.
    let mb = v
        .read_full(&format!("{dir}/{}", manifest_file_name(1)))
        .expect("manifest-1 bytes");
    let cp = CommitPointer::new(1, mb.len() as u64, crc32c(&mb[..mb.len() - 4]));
    let ct = format!("{dir}/{CURRENT_TMP_FILE_NAME}");
    let cfd = v.create_rw(&ct).expect("cur tmp");
    v.pwrite_at(&cfd, 0, &cp.encode()).expect("cur write");
    v.fsync_file(&cfd).expect("cur fsync");
    v.close_file(cfd).expect("cur close");
    v.rename_path(&ct, &format!("{dir}/{CURRENT_FILE_NAME}"))
        .expect("cur rename");
    let c2 = v.open_rw(&format!("{dir}/{CURRENT_FILE_NAME}")).expect("cur open");
    v.fsync_file(&c2).expect("cur fsync2");
    v.close_file(c2).expect("cur close2");

    let mut caught = false;
    for seed in 0..64u64 {
        let mut universe = v.clone();
        let mut rng = XorShift::new(0x0462_BAD0 ^ seed);
        universe.crash_and_revive(&mut rng);
        let files = universe.snapshot_dir(&dir);
        let rdir = memdir_of(&files);
        if matches!(
            resolve_effective(&rdir, &probe, &TableExpect::default()),
            Err(pgrc2_read::ReadError::ManifestMissing { gen: 2 })
        ) {
            caught = true;
            break;
        }
    }
    assert!(
        caught,
        "born-RED failure: the barrier-less remove-then-repoint ordering was NOT \
         caught by any adversarial outcome — the dirent-loss detector is toothless"
    );
}
