//! DST P4 inc-1 — the crash-recovery property loop (the payoff e2e) and the
//! red battery, sim-cfg only.
//!
//! SUBSTRATE CHOICE (documented per the charter): the workload drives the
//! REAL fd-layer durability primitives — `OpenTransientFile`/`pg_fsync`/
//! `durable_rename` (with its parent-dir fsync) over the vfs data plane —
//! implementing the canonical WAL commit protocol (append CRC'd record →
//! fsync WAL = commit point → apply to heap non-durably → periodic
//! checkpoint = heap fsync + control-file durable_rename). Recovery scans
//! the post-crash image exactly the way xlog replay does: control file →
//! checkpoint horizon → CRC-gated sequential record replay onto the heap
//! base. Booting the REAL xlogrecovery under sim needs an initdb'd datadir
//! inside the SimVfs namespace (initdb is external C today; `--single` is
//! on the wasm lineage, not this one) — that integration is inc-2; this
//! harness is the property loop the scoping doc §4.4 requires, driven at
//! the deepest layer the substrate offers today.
//!
//! THE STANDING PROPERTY, at every cut point:
//!   1. recovery completes (control readable+intact, WAL horizon sane,
//!      no replay discontinuity) — a failure here is a FINDING;
//!   2. committed (acked) data survives: every txn whose WAL fsync
//!      returned success is within the recovery horizon;
//!   3. uncommitted data is absent / no torn record applies: the
//!      post-recovery heap equals the deterministic fold of exactly the
//!      recovered-horizon transactions.
//!
//! Run with: RUSTFLAGS='--cfg pgrust_sim' cargo test -p fd crash_sweep

use vfs::sim::{
    classify_path, CrashImage, FaultDecision, FaultRule, OpKind, OpMatch, PathClass,
    SeededFaultPlan, SimVfs,
};

use crate::desc::{CloseTransientFile, OpenTransientFile, OpenTransientFilePerm};
use crate::sync::{durable_rename, fsync_fname, pg_fsync};

const SEED: u64 = 0x5EED_FA17_0001;

const WAL: &str = "/data/pg_wal/000000010000000000000001";
const HEAP: &str = "/data/base/heap";
const CONTROL: &str = "/data/pg_control";
const CONTROL_TMP: &str = "/data/pg_control.tmp";

const NSLOTS: usize = 8;
const HEAP_LEN: usize = NSLOTS * 8;
const CKPT_EVERY: u64 = 8;
const TXNS: u64 = 48;
/// len u32 + txid u64 + slot u8 + val u64 + crc u32 (before padding).
const MIN_REC: usize = 25;

// -------------------------------------------------------------------------
// deterministic generators (all randomness from explicit seeds)
// -------------------------------------------------------------------------

fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Transaction t writes `val` to `slot`, padded so records span multiple
/// 512 B sectors (the torn-write arm needs multi-sector records).
fn gen_txn(t: u64) -> (usize, u64, usize) {
    let mut s = SEED ^ t.wrapping_mul(0xA076_1D64_78BD_642F);
    let slot = (splitmix(&mut s) % NSLOTS as u64) as usize;
    let val = splitmix(&mut s) | 1; // nonzero
    let pad = 600 + (splitmix(&mut s) % 1900) as usize;
    (slot, val, pad)
}

fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &b in data {
        crc ^= b as u32;
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

fn u32le(b: &[u8]) -> u32 {
    u32::from_le_bytes(b[..4].try_into().unwrap())
}
fn u64le(b: &[u8]) -> u64 {
    u64::from_le_bytes(b[..8].try_into().unwrap())
}

fn encode_record(txid: u64, slot: usize, val: u64, pad: usize) -> Vec<u8> {
    let len = MIN_REC + pad;
    let mut rec = Vec::with_capacity(len);
    rec.extend_from_slice(&(len as u32).to_le_bytes());
    rec.extend_from_slice(&txid.to_le_bytes());
    rec.push(slot as u8);
    rec.extend_from_slice(&val.to_le_bytes());
    rec.resize(len - 4, txid as u8); // deterministic pad
    let crc = crc32(&rec);
    rec.extend_from_slice(&crc.to_le_bytes());
    rec
}

fn control_payload(ckpt_end: u64, ckpt_txid: u64) -> Vec<u8> {
    let mut p = Vec::with_capacity(20);
    p.extend_from_slice(&ckpt_end.to_le_bytes());
    p.extend_from_slice(&ckpt_txid.to_le_bytes());
    let crc = crc32(&p);
    p.extend_from_slice(&crc.to_le_bytes());
    p
}

/// Whole-file read through the vfs; None if the path is gone.
fn read_opt(path: &str) -> Option<Vec<u8>> {
    let fd = vfs::open(&super::cpath(path), libc::O_RDONLY, 0);
    if fd < 0 {
        return None;
    }
    let size = vfs::file_size(fd);
    let mut buf = vec![0u8; size.max(0) as usize];
    if size > 0 {
        assert_eq!(vfs::pread(fd, &mut buf, 0), size as isize);
    }
    let _ = vfs::close(fd);
    Some(buf)
}

// -------------------------------------------------------------------------
// the workload ("initdb" + txn stream over the real fd primitives)
// -------------------------------------------------------------------------

/// Durable bootstrap (the "initdb"): the property loop cuts only AFTER this
/// (a crash mid-initdb means re-initdb, not recovery).
fn bootstrap() {
    super::vfs_mkdir_p("/data/pg_wal");
    super::vfs_mkdir_p("/data/base");
    super::vfs_write_file(WAL, b"");
    super::vfs_write_file(HEAP, &[0u8; HEAP_LEN]);
    super::vfs_write_file(CONTROL, &control_payload(0, 0));
    for f in [WAL, HEAP, CONTROL] {
        fsync_fname(f, false).unwrap();
    }
    for d in ["/data/pg_wal", "/data/base", "/data", "/"] {
        fsync_fname(d, true).unwrap();
    }
}

#[derive(Debug, Clone, Copy)]
struct Acked {
    txid: u64,
    #[allow(dead_code)]
    slot: usize,
    #[allow(dead_code)]
    val: u64,
}

struct RunOutcome {
    acked: Vec<Acked>,
    completed: bool,
}

fn checkpoint(heap_fd: i32, ckpt_end: u64, ckpt_txid: u64) -> Result<(), ()> {
    // Heap pages durable BEFORE the control file points past them. (Teeth
    // check, 2026-07-18: removing this fsync makes the sweep report 284
    // property violations — the harness catches the protocol bug.)
    if pg_fsync(heap_fd) != 0 {
        return Err(());
    }
    let payload = control_payload(ckpt_end, ckpt_txid);
    let tmp_fd = match OpenTransientFilePerm(
        CONTROL_TMP,
        libc::O_CREAT | libc::O_TRUNC | libc::O_RDWR,
        0o600,
    ) {
        Ok(fd) if fd >= 0 => fd,
        _ => return Err(()),
    };
    if vfs::pwrite(tmp_fd, &payload, 0) != payload.len() as isize {
        let _ = CloseTransientFile(tmp_fd);
        return Err(());
    }
    if pg_fsync(tmp_fd) != 0 {
        let _ = CloseTransientFile(tmp_fd);
        return Err(());
    }
    if CloseTransientFile(tmp_fd) != 0 {
        return Err(());
    }
    // The product's atomic-replace discipline: fsync old, rename, fsync
    // the parent dir (dirent durability).
    match durable_rename(CONTROL_TMP, CONTROL, ::types_error::LOG) {
        Ok(0) => Ok(()),
        _ => Err(()),
    }
}

/// The commit protocol. `retry_fsync_believer` is the red-battery fsyncgate
/// bug arm: on WAL fsync failure, retry once and TRUST the OK (upstream's
/// PANIC discipline exists precisely to forbid this).
fn run_workload(txns: u64, retry_fsync_believer: bool) -> RunOutcome {
    let mut acked = Vec::new();
    let wal_fd = match OpenTransientFile(WAL, libc::O_RDWR) {
        Ok(fd) if fd >= 0 => fd,
        _ => return RunOutcome { acked, completed: false },
    };
    let heap_fd = match OpenTransientFile(HEAP, libc::O_RDWR) {
        Ok(fd) if fd >= 0 => fd,
        _ => {
            let _ = CloseTransientFile(wal_fd);
            return RunOutcome { acked, completed: false };
        }
    };
    let mut wal_end: i64 = vfs::file_size(wal_fd);
    let mut completed = true;
    for t in 1..=txns {
        let (slot, val, pad) = gen_txn(t);
        let rec = encode_record(t, slot, val, pad);
        if vfs::pwrite(wal_fd, &rec, wal_end) != rec.len() as isize {
            completed = false;
            break;
        }
        // WAL flush = the commit point. Any failure here is a
        // PANIC-equivalent for a correct engine: stop, never ack.
        let mut rc = pg_fsync(wal_fd);
        if rc != 0 && retry_fsync_believer {
            rc = pg_fsync(wal_fd); // the fsyncgate bug
        }
        if rc != 0 {
            completed = false;
            break;
        }
        wal_end += rec.len() as i64;
        acked.push(Acked { txid: t, slot, val });
        // Apply to the heap page cache (durable only at the next ckpt).
        if vfs::pwrite(heap_fd, &val.to_le_bytes(), (slot * 8) as i64) != 8 {
            completed = false;
            break;
        }
        if t % CKPT_EVERY == 0 && checkpoint(heap_fd, wal_end as u64, t).is_err() {
            completed = false;
            break;
        }
    }
    let _ = CloseTransientFile(heap_fd);
    let _ = CloseTransientFile(wal_fd);
    RunOutcome { acked, completed }
}

// -------------------------------------------------------------------------
// recovery (control → checkpoint horizon → CRC-gated replay)
// -------------------------------------------------------------------------

struct Recovered {
    ckpt_txid: u64,
    replayed: Vec<(u64, usize, u64)>, // (txid, slot, val)
    heap: Vec<u8>,
}

impl Recovered {
    fn horizon(&self) -> u64 {
        self.ckpt_txid + self.replayed.len() as u64
    }
}

fn recover() -> Result<Recovered, String> {
    // The control file is only ever replaced via durable_rename: it must
    // ALWAYS be present and intact after a crash.
    let ctl = read_opt(CONTROL).ok_or("pg_control missing after crash")?;
    if ctl.len() != 20 {
        return Err(format!("pg_control wrong length {}", ctl.len()));
    }
    if u32le(&ctl[16..20]) != crc32(&ctl[..16]) {
        return Err("pg_control crc mismatch".into());
    }
    let ckpt_end = u64le(&ctl[0..8]) as usize;
    let ckpt_txid = u64le(&ctl[8..16]);

    let mut heap = read_opt(HEAP).ok_or("heap missing after crash")?;
    if heap.len() < HEAP_LEN {
        return Err(format!("heap shorter than its durable floor: {}", heap.len()));
    }
    heap.truncate(HEAP_LEN);

    let wal = read_opt(WAL).ok_or("wal missing after crash")?;
    if wal.len() < ckpt_end {
        return Err(format!(
            "wal ({}) shorter than the checkpoint horizon ({ckpt_end}) — \
             control points past durable WAL",
            wal.len()
        ));
    }

    let mut replayed = Vec::new();
    let mut pos = ckpt_end;
    let mut expect_txid = ckpt_txid + 1;
    while pos + 4 <= wal.len() {
        let len = u32le(&wal[pos..pos + 4]) as usize;
        if len < MIN_REC || pos + len > wal.len() {
            break; // absent/torn tail = end of WAL
        }
        let rec = &wal[pos..pos + len];
        if u32le(&rec[len - 4..]) != crc32(&rec[..len - 4]) {
            break; // torn record = end of WAL
        }
        let txid = u64le(&rec[4..12]);
        let slot = rec[12] as usize;
        let val = u64le(&rec[13..21]);
        if txid != expect_txid {
            return Err(format!("WAL txid discontinuity: got {txid}, expected {expect_txid}"));
        }
        if slot >= NSLOTS {
            return Err(format!("replayed record has bad slot {slot}"));
        }
        heap[slot * 8..slot * 8 + 8].copy_from_slice(&val.to_le_bytes());
        replayed.push((txid, slot, val));
        expect_txid += 1;
        pos += len;
    }
    Ok(Recovered { ckpt_txid, replayed, heap })
}

/// The standing property (scoping doc §4.4): committed survives,
/// uncommitted absent, internal consistency holds.
fn check_properties(tag: &str, acked: &[Acked], rec: &Recovered, failures: &mut Vec<String>) {
    let m = rec.horizon();
    // 1. committed data survives
    for a in acked {
        if a.txid > m {
            failures.push(format!(
                "{tag}: ACKED txn {} lost (recovery horizon {m}, ckpt {})",
                a.txid, rec.ckpt_txid
            ));
        }
    }
    // 2. the post-recovery heap is the fold of EXACTLY txns 1..=m
    let mut expect = vec![0u8; HEAP_LEN];
    for t in 1..=m {
        let (slot, val, _) = gen_txn(t);
        expect[slot * 8..slot * 8 + 8].copy_from_slice(&val.to_le_bytes());
    }
    if rec.heap != expect {
        failures.push(format!(
            "{tag}: post-recovery heap diverges from fold(1..={m}) — uncommitted or torn data applied"
        ));
    }
    // 3. every replayed record matches the generator (no garbage applied)
    for (i, (txid, slot, val)) in rec.replayed.iter().enumerate() {
        let t = rec.ckpt_txid + 1 + i as u64;
        let (gs, gv, _) = gen_txn(t);
        if *txid != t || *slot != gs || *val != gv {
            failures.push(format!(
                "{tag}: replayed record {i} corrupt: txid={txid} slot={slot} val={val}"
            ));
        }
    }
}

fn per_point_seed(arm: u64, k: u64) -> u64 {
    SEED ^ arm.wrapping_mul(0xE703_7ED1_A0B4_28DB) ^ k.wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

// -------------------------------------------------------------------------
// THE PAYOFF: systematic cut sweep + fsync-error sweep + torn-WAL sweep
// -------------------------------------------------------------------------

#[test]
fn crash_recovery_property_sweep() {
    super::setup();
    let mut failures: Vec<String> = Vec::new();

    // Fault-free baseline: the workload completes and defines the op span.
    SimVfs::reset();
    bootstrap();
    let boot_ops = SimVfs::op_seq();
    let base = run_workload(TXNS, false);
    assert!(base.completed, "fault-free baseline must complete");
    assert_eq!(base.acked.len(), TXNS as usize);
    let workload_ops = SimVfs::op_seq() - boot_ops;
    assert!(workload_ops > 100, "baseline too small to sweep ({workload_ops} ops)");

    // ---- ARM A: cut at EVERY op boundary of the workload (step 1),
    //      surviving-subset image seeded per cut point ----
    let mut arm_a_cuts = 0u64;
    for k in 1..=workload_ops {
        SimVfs::reset();
        bootstrap();
        // inc-3 whole-node kill: the cut freezes ALL vfs mutation, so the
        // workload's post-cut error-path ops (closes, late writes) cannot
        // touch the crash image; revive() is the recovery boot.
        SimVfs::set_kill_on_cut(true);
        SeededFaultPlan::install(per_point_seed(1, k), vec![FaultRule::crash_at_op(k)]);
        let out = run_workload(TXNS, false);
        if SimVfs::cut_count() == 0 {
            failures.push(format!("armA k={k}: planned cut never fired"));
            continue;
        }
        arm_a_cuts += 1;
        SimVfs::revive();
        match recover() {
            Ok(rec) => check_properties(&format!("armA k={k}"), &out.acked, &rec, &mut failures),
            Err(e) => failures.push(format!("armA k={k}: RECOVERY FAILED: {e}")),
        }
    }
    assert_eq!(arm_a_cuts, workload_ops, "every sweep point must cut");

    // ---- ARM B: EIO on the j-th fsync (fsyncgate discipline: the engine
    //      stops = PANIC, then the node dies) ----
    let mut arm_b_points = 0u64;
    for j in 1.. {
        SimVfs::reset();
        bootstrap();
        SimVfs::set_kill_on_cut(true);
        SeededFaultPlan::install(
            per_point_seed(2, j),
            vec![FaultRule::nth_matching(
                OpMatch {
                    kinds: Some(vec![OpKind::Fsync, OpKind::Fdatasync]),
                    class: None,
                    path_contains: None,
                },
                j,
                FaultDecision::Errno(libc::EIO),
            )],
        );
        let out = run_workload(TXNS, false);
        if SimVfs::fault_log().is_empty() {
            break; // j exceeds the workload's fsync count — arm exhausted
        }
        arm_b_points += 1;
        assert!(!out.completed, "an injected fsync EIO must stop the engine (j={j})");
        if SimVfs::cut_count() == 0 {
            SimVfs::cut(); // the PANIC-induced node death
        }
        SimVfs::revive();
        match recover() {
            Ok(rec) => check_properties(&format!("armB j={j}"), &out.acked, &rec, &mut failures),
            Err(e) => failures.push(format!("armB j={j}: RECOVERY FAILED: {e}")),
        }
    }
    assert!(arm_b_points > TXNS / 2, "fsync sweep too small ({arm_b_points})");

    // ---- ARM C: torn write on the m-th WAL-class write (crash mid-record;
    //      the tear must never replay — CRC is the gate) ----
    for m in 1..=TXNS {
        SimVfs::reset();
        bootstrap();
        SimVfs::set_kill_on_cut(true);
        let pp = (137 * m as usize) % 600; // strict prefix: < MIN_REC + min pad
        SeededFaultPlan::install(
            per_point_seed(3, m),
            vec![FaultRule::nth_matching(
                OpMatch {
                    kinds: Some(vec![OpKind::PWriteV]),
                    class: Some(PathClass::Wal),
                    path_contains: None,
                },
                m,
                FaultDecision::TornWrite { persist_prefix: pp },
            )],
        );
        let out = run_workload(TXNS, false);
        if SimVfs::cut_count() == 0 {
            failures.push(format!("armC m={m}: torn-write cut never fired"));
            continue;
        }
        if out.acked.iter().any(|a| a.txid == m) {
            failures.push(format!("armC m={m}: txn with torn WAL record was acked"));
        }
        SimVfs::revive();
        match recover() {
            Ok(rec) => {
                if rec.horizon() >= m {
                    failures.push(format!(
                        "armC m={m}: torn record replayed (horizon {})",
                        rec.horizon()
                    ));
                }
                check_properties(&format!("armC m={m}"), &out.acked, &rec, &mut failures);
            }
            Err(e) => failures.push(format!("armC m={m}: RECOVERY FAILED: {e}")),
        }
    }

    eprintln!(
        "SWEEP: armA {arm_a_cuts} cut points (every op), armB {arm_b_points} fsync-EIO points, \
         armC {TXNS} torn-WAL points, {} violations",
        failures.len()
    );
    assert!(
        failures.is_empty(),
        "CRASH-RECOVERY PROPERTY VIOLATIONS ({}):\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// Same seed, same plan, run twice: byte-identical fault logs and identical
/// recovered state (the seed-replay identity gate at the harness level).
#[test]
fn sweep_replay_same_seed_byte_identical() {
    super::setup();
    fn one() -> (Vec<String>, Vec<u8>, Vec<(u64, usize, u64)>, u64) {
        SimVfs::reset();
        bootstrap();
        SimVfs::set_kill_on_cut(true);
        SeededFaultPlan::install(SEED ^ 0x1234, vec![FaultRule::crash_at_op(57)]);
        let _ = run_workload(TXNS, false);
        if SimVfs::cut_count() == 0 {
            SimVfs::cut();
        }
        SimVfs::revive();
        let rec = recover().expect("recovery after replayed cut");
        (SimVfs::fault_log(), rec.heap, rec.replayed, rec.ckpt_txid)
    }
    let a = one();
    let b = one();
    assert!(!a.0.is_empty(), "the cut must be in the log");
    assert_eq!(a.0, b.0, "fault logs must be byte-identical across replay");
    assert_eq!(a.1, b.1, "recovered heap must be identical");
    assert_eq!(a.2, b.2, "replayed record stream must be identical");
    assert_eq!(a.3, b.3);
}

// -------------------------------------------------------------------------
// RED BATTERY: deliberately weakened arms MUST be caught. If any of these
// pass with the weakening behaving like the disciplined arm, the model has
// no teeth — that is a failed gate.
// -------------------------------------------------------------------------

/// R1: skipping the parent-dir fsync after rename produces a detectable
/// post-crash loss; the real durable_rename (which fsyncs the parent)
/// survives the same cut.
#[test]
fn red_missing_parent_dir_fsync_is_caught() {
    super::setup();
    let new_ctl = control_payload(999, 42);

    // Weakened arm: raw rename, NO parent-dir fsync.
    SimVfs::reset();
    bootstrap();
    super::vfs_write_file(CONTROL_TMP, &new_ctl);
    fsync_fname(CONTROL_TMP, false).unwrap();
    assert_eq!(vfs::rename(&super::cpath(CONTROL_TMP), &super::cpath(CONTROL)), 0);
    SimVfs::cut();
    let ctl = read_opt(CONTROL).expect("the OLD control dirent is durable");
    assert_eq!(
        ctl,
        control_payload(0, 0),
        "the model MUST expose the lost dirent: control reverts to the old image"
    );
    assert_ne!(ctl, new_ctl, "if the new control survived, the model has no teeth");

    // Disciplined arm: durable_rename (fsyncs the parent dir) — survives.
    SimVfs::reset();
    bootstrap();
    super::vfs_write_file(CONTROL_TMP, &new_ctl);
    fsync_fname(CONTROL_TMP, false).unwrap();
    assert_eq!(durable_rename(CONTROL_TMP, CONTROL, ::types_error::LOG).unwrap(), 0);
    SimVfs::cut();
    assert_eq!(
        read_opt(CONTROL).expect("control present"),
        new_ctl,
        "durable_rename's parent-dir fsync makes the replace crash-durable"
    );
}

/// R2: the test-only atomic-multi-sector write mode masks a torn WAL record
/// that the 512 B floor catches — at the RECOVERY level (CRC gate).
#[test]
fn red_atomic_multisector_write_masks_torn_wal_record() {
    super::setup();
    fn arm(atomic: bool) -> (Vec<Acked>, Recovered) {
        SimVfs::reset();
        bootstrap();
        SimVfs::set_atomic_write_mode(atomic);
        SeededFaultPlan::install(
            SEED ^ 0xF00D,
            vec![FaultRule::nth_matching(
                OpMatch {
                    kinds: Some(vec![OpKind::PWriteV]),
                    class: Some(PathClass::Wal),
                    path_contains: None,
                },
                3,
                FaultDecision::TornWrite { persist_prefix: 550 },
            )],
        );
        let out = run_workload(6, false);
        assert!(!out.completed);
        assert_eq!(SimVfs::cut_count(), 1);
        let rec = recover().expect("recovery");
        (out.acked, rec)
    }

    let (acked_floor, rec_floor) = arm(false);
    let (acked_weak, rec_weak) = arm(true);
    assert!(acked_floor.iter().all(|a| a.txid <= 2));
    assert!(acked_weak.iter().all(|a| a.txid <= 2));

    // Floor arm: the tear is real — CRC rejects record 3, replay stops.
    assert_eq!(
        rec_floor.horizon(),
        2,
        "floor arm must catch the torn record (horizon {})",
        rec_floor.horizon()
    );
    // Weakened arm: the whole multi-sector record "survived" the crash, so
    // its CRC verifies and the tear is MASKED — exactly the failure class
    // the floor exists to expose. If this arm ever behaves like the floor
    // arm, the weakening isn't weakening anything and the gate is dead.
    assert_eq!(
        rec_weak.horizon(),
        3,
        "atomic mode must mask the tear (horizon {})",
        rec_weak.horizon()
    );
}

/// R3: the fsyncgate bug — retry a failed WAL fsync, trust the OK, ack the
/// txn. The doomed epoch never reaches disk; the property harness must
/// catch the acked-data loss.
#[test]
fn red_fsyncgate_retry_believer_is_caught() {
    super::setup();
    SimVfs::reset();
    bootstrap();
    // EIO on the 2nd WAL-class fsync (= txn 2's commit flush).
    SeededFaultPlan::install(
        SEED ^ 0x9A7E,
        vec![FaultRule::nth_matching(
            OpMatch {
                kinds: Some(vec![OpKind::Fsync]),
                class: Some(PathClass::Wal),
                path_contains: None,
            },
            2,
            FaultDecision::Errno(libc::EIO),
        )],
    );
    // Pin the ADVERSARIAL disk. Under the inc-2 N2 model the doomed epoch
    // routes through the CrashImage policy, so on a kind (seeded-lucky) disk
    // the believer's record can genuinely survive — exactly why the bug
    // class is insidious on real hardware. The red arm asserts the catch on
    // the disk that loses it.
    SimVfs::set_crash_image(CrashImage::DropAll);
    let out = run_workload(4, true /* the believer retries and trusts the OK */);
    assert!(out.completed, "the believer never notices anything wrong");
    assert!(
        out.acked.iter().any(|a| a.txid == 2),
        "the believer acked the doomed txn"
    );

    SimVfs::cut();
    let rec = recover().expect("recovery still completes — the loss is silent");
    assert!(
        rec.horizon() < 2,
        "txn 2's record must be gone despite the 'successful' retry (horizon {})",
        rec.horizon()
    );

    let mut failures = Vec::new();
    check_properties("red-fsyncgate", &out.acked, &rec, &mut failures);
    assert!(
        failures.iter().any(|f| f.contains("ACKED txn 2 lost")),
        "the property harness must flag the believer's acked-data loss: {failures:?}"
    );
}

/// The engine's path classifier speaks the fd-layer's actual paths.
#[test]
fn classifier_matches_harness_paths() {
    use std::path::Path;
    assert_eq!(classify_path(Path::new(WAL)), PathClass::Wal);
    assert_eq!(classify_path(Path::new(HEAP)), PathClass::Heap);
    assert_eq!(classify_path(Path::new(CONTROL)), PathClass::Config);
    assert_eq!(classify_path(Path::new(CONTROL_TMP)), PathClass::Config);
    let _ = CrashImage::DropAll; // vocabulary reachable from the harness
}
