//! GH issue #2 crash-consistency battery: a committed multi-segment part
//! must survive a POSIX-legal crash. The failure this pins down: the writer
//! fdatasyncs every segment file but (pre-fix) never fsync'd the PARENT
//! DIRECTORY before publishing the commit pointer (`footer_off`), and
//! fdatasync persists file DATA, not the directory entry that names the
//! file — so a crash could keep `path.1`'s bytes and the published footer
//! while dropping `path.1`'s dirent, leaving every later read failing with
//! "footer offset out of bounds" on a table that counted as committed.
//! SimVfs models exactly this (dirent rule 3: namespace ops are volatile
//! until the parent dir is fsync'd).
//!
//! Run: `RUSTFLAGS='--cfg pgrust_sim' cargo test -p pgrcolumnar --test sim_dirsync`
//!
//! Each #[test] runs on its own thread = its own thread-local SimVfs
//! universe; the segment-size override is process-global and identical in
//! every test (OnceLock, first-init-wins).
#![cfg(pgrust_sim)]

use std::ffi::CString;

use datum::Datum;
use pgrcolumnar::reader::part_footer_rows;
use pgrcolumnar::writer::open_writer_at;
use pgrcolumnar::ColType;
use vfs::sim::{FaultRule, NoFaults, SeededFaultPlan, SimVfs};

/// Tiny segments so the >1 GiB spill shape is cheap to mint. BLCKSZ
/// multiple (pad_and_sync's md contract).
const SEG: u64 = 64 * 1024;

const PART: &str = "/base/5/1000";
const PART_DIR: &str = "/base/5";

fn c(s: &str) -> CString {
    CString::new(s).unwrap()
}

fn init_seg() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| pgrcolumnar::segfile::set_seg_bytes_for_tests(SEG));
}

/// Fresh universe + durable scaffolding: the datadir tree and the empty
/// seg-0 file are pre-history (smgr/mdcreate owns seg 0's existence — its
/// durability rides the ordinary relation-create machinery, so the battery
/// ingests it durable-from-birth and puts only THIS crate's mints at risk).
fn scaffold() {
    init_seg();
    SimVfs::reset();
    SimVfs::ingest_dir(&c("/base"), 0o700).unwrap();
    SimVfs::ingest_dir(&c(PART_DIR), 0o700).unwrap();
    SimVfs::ingest_file(&c(PART), b"", 0o600).unwrap();
}

/// Append `n` incompressible rows (xorshift64 — LZ4-resistant, so the part
/// body really grows) and commit via the production finish() path.
fn load(n: usize, seed: u64) -> types_error::PgResult<()> {
    let mut w = open_writer_at(PART, vec![ColType::I64])?;
    let mut x = seed | 1;
    for _ in 0..n {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        w.append_row(&[Datum::from_i64(x as i64)], &[false])?;
    }
    w.finish()
}

fn path_exists(p: &str) -> bool {
    let mut fi = vfs::FileInfo::zeroed();
    vfs::stat(&c(p), &mut fi) == 0
}

/// Committed rows as every read path sees them (header + footer through the
/// SegFile pread plane — the same bounds gates scans ride).
fn committed_rows() -> types_error::PgResult<Option<u64>> {
    part_footer_rows(PART, 1)
}

// Enough rows to spill past the shrunken segment boundary: 64 B header +
// n*8 bytes of incompressible payload (~96 KiB at 12k rows > SEG).
const ROWS: usize = 12_000;

/// THE DISEASE (GH #2), as a durability property: COPY > one segment,
/// commit, crash. The part must read back complete — which requires the
/// minted segments' dirents to be durable before (or with) the publish.
/// Pre-fix this fails with "footer offset out of bounds": the crash keeps
/// seg data + the published footer_off but drops the `.1`/`.2` dirents.
#[test]
fn committed_multiseg_part_survives_crash() {
    scaffold();
    load(ROWS, 0x5eed).unwrap();
    assert!(path_exists("/base/5/1000.1"), "engagement: the load must spill a segment");

    SimVfs::cut(); // power loss, adversarial DropAll floor

    assert!(path_exists("/base/5/1000.1"), "minted segment dirent survives the crash");
    assert_eq!(
        committed_rows().expect("committed part must stay readable"),
        Some(ROWS as u64),
        "all committed rows survive"
    );
}

/// THE ORDERING WITNESS: in the publish sequence the parent-dir fsync must
/// land AFTER the minted segment's data sync and BEFORE the header pwrite
/// that publishes footer_off (file-then-dir-then-publish — the fd.c
/// durable-create recipe). Also the cost law: exactly ONE dir fsync per
/// mint-bearing commit, not one per commit.
#[test]
fn dir_fsync_orders_between_data_sync_and_publish() {
    scaffold();
    SimVfs::set_op_trace(true);
    load(ROWS, 0x0bee).unwrap();
    let trace = SimVfs::op_trace();

    let is_dir_fsync =
        |l: &str| l.contains("kind=Fsync") && l.ends_with(&format!(" path={PART_DIR}"));
    let dir_fsyncs: Vec<usize> = trace
        .iter()
        .enumerate()
        .filter(|(_, l)| is_dir_fsync(l))
        .map(|(i, _)| i)
        .collect();
    assert_eq!(dir_fsyncs.len(), 1, "one dir fsync per mint-bearing commit: {dir_fsyncs:?}");
    let dir_fsync = dir_fsyncs[0];

    // The publish: the LAST header write (off=0 on seg 0; the path is the
    // trace line's final field, so ends_with cannot alias `.1` pwrites at
    // segment-relative offset 0). The first off-0 header write is the
    // header-first init of the empty part.
    let publish = trace
        .iter()
        .rposition(|l| {
            l.contains("kind=PWriteV")
                && l.contains(" off=0 ")
                && l.ends_with(&format!(" path={PART}"))
        })
        .expect("header publish pwrite in trace");

    // The minted segment's own data sync (pad_and_sync). position, not
    // rposition: the post-publish sync_data() legitimately fdatasyncs every
    // segment AGAIN after the header write — the recipe's requirement is
    // that the mint's content sync happens before the dir fsync, which
    // happens before the publish.
    let seg1_sync = trace
        .iter()
        .position(|l| l.contains("kind=Fdatasync") && l.contains("path=/base/5/1000.1"))
        .expect("minted segment fdatasync in trace");

    assert!(
        seg1_sync < dir_fsync && dir_fsync < publish,
        "publish order must be data-sync < dir-fsync < header-publish, got \
         seg1_sync={seg1_sync} dir_fsync={dir_fsync} publish={publish}"
    );
}

/// The no-mint commit pays NOTHING: single-segment commits (and re-commits
/// appending within seg 0) perform zero directory fsyncs.
#[test]
fn single_segment_commit_pays_no_dir_fsync() {
    scaffold();
    SimVfs::set_op_trace(true);
    load(100, 0xabcd).unwrap();
    load(50, 0xd00d).unwrap(); // reopen-append, still within seg 0
    assert!(!path_exists("/base/5/1000.1"), "engagement: single-segment shape");
    let dir_fsyncs = SimVfs::op_trace()
        .iter()
        .filter(|l| l.contains("kind=Fsync") && l.ends_with(&format!(" path={PART_DIR}")))
        .count();
    assert_eq!(dir_fsyncs, 0, "no mint => no dir fsync on the commit path");

    SimVfs::cut();
    assert_eq!(committed_rows().unwrap(), Some(150), "both commits durable");
}

/// Crash-cut sweep over the whole append-and-publish window: cut at every
/// vfs op of a segment-spilling append onto an already-committed part, under
/// the seeded partial-survival crash image. After every cut the table must
/// read as EITHER the old committed state or the new one — never an error,
/// never a third state. (The pre-fix hole is the tail of this sweep: publish
/// durable, dirent gone.)
#[test]
fn publish_window_crash_sweep() {
    const OLD: usize = 1_000;

    // Rehearsal: measure the op window of the append leg.
    scaffold();
    load(OLD, 7).unwrap();
    let base_ops = SimVfs::op_seq();
    load(ROWS, 11).unwrap();
    let window = SimVfs::op_seq() - base_ops;
    assert!(window > 20, "engagement: the append leg must consult real ops, got {window}");

    for k in 1..=window {
        scaffold();
        load(OLD, 7).unwrap();
        // Whole-node kill: from the cut on, every op is refused without
        // mutating anything — the process is gone, exactly like a real
        // power loss. (Without the kill, the writer would keep running
        // against the post-crash image and "heal" the trajectory.)
        SimVfs::set_kill_on_cut(true);
        SeededFaultPlan::install(0x1000 + k, vec![FaultRule::crash_at_op(k)]);
        let acked = load(ROWS, 11).is_ok();
        SimVfs::revive();
        SimVfs::set_fault_plan(Box::new(NoFaults));

        // Old-or-new, never an error, and an ACKED commit must read new
        // (cuts landing on the post-publish fd closes are the acked arm).
        match committed_rows() {
            Ok(Some(n)) if n == (OLD + ROWS) as u64 => {}
            Ok(Some(n)) if n == OLD as u64 && !acked => {}
            other => panic!(
                "k={k}/{window} acked={acked}: post-crash part must read as old ({OLD}, \
                 unacked only) or new ({}) state, got {other:?}",
                OLD + ROWS
            ),
        }
    }
}
