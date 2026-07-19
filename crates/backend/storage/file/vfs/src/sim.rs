//! SimVfs — deterministic in-memory filesystem for DST (P1 skeleton + P4
//! fault model, increment 1).
//!
//! Owner: WS-C (contract §4, §5.1); P4 disk-fault model per
//! docs/design/dst-and-wasm.md §4.1. Compiled only under `--cfg pgrust_sim`
//! (the sim harness selector); never present in product builds. Run the sim
//! battery with `RUSTFLAGS='--cfg pgrust_sim' cargo test -p vfs sim::`.
//!
//! Shape (frozen surface, lib.rs): SimVfs is a ZST with `pub const fn new()`
//! — `ACTIVE` is a const. State homing has two modes (provider seam,
//! dst-p3-scheduler §9): the DEFAULT keeps ALL state in a thread-local (a
//! thread is a universe — every harness battery relies on that isolation);
//! a whole-server sim boot re-homes to ONE process-shared universe via the
//! boot installer [`SimVfs::install_process_universe`], because the server's
//! real OS threads must all see the same simulated disk. [`SimVfs::reset`]
//! tears the (thread-local) universe down (fresh disk, fd counter back to
//! base, NoFaults plan) for back-to-back runs on one thread.
//!
//! DETERMINISM RULES (binding, contract §4.1):
//! - BTree ordering ONLY — no HashMap/HashSet anywhere in this module.
//! - No wall clock: `FileInfo::mtime_*` is always 0 (a logical clock, if
//!   ever needed, comes from the harness).
//! - Monotonic fd assignment from a high base ([`SIM_FD_BASE`]) — small-int
//!   posix fds mixed into sim traffic fault loudly as EBADF, catching raw-fd
//!   domain mixups (the FileGetRawDesc carve-out).
//! - All randomness comes from explicit seeds (splitmix64 streams — the
//!   SimEntropy generator family); SimVfs itself contains ZERO ambient
//!   entropy sources. Same seed + same plan spec ⇒ same failure at the same
//!   op, and every injected fault is logged with an op-sequence number
//!   ([`SimVfs::fault_log`]) so a failure replays from the log line + the
//!   plan spec alone.
//!
//! THE P4 DISK-FAULT MODEL (scoping doc §4.1 — an adversarial SUPERSET of
//! what posix filesystems do, with three binding constraints):
//!
//! 1. **512 B atomicity floor** ([`SECTOR_SIZE`]): a write in flight at a
//!    crash survives as a prefix ending on an ABSOLUTE sector boundary (or
//!    the whole write) — never a partial sector, never atomic multi-sector.
//!    The floor is a documented model parameter; the test-only
//!    [`SimVfs::set_atomic_write_mode`] weakening exists ONLY so the red
//!    battery can prove the floor has teeth.
//! 2. **fsync-failure state machine** (the fsyncgate semantics): writes
//!    since the last SUCCESSFUL fsync form the unsynced journal. A FAILED
//!    fsync moves that journal to may-be-lost **permanently** — and (inc-2,
//!    review N2) "may be lost" is literal: the installed [`CrashImage`]
//!    policy decides which SUBSET of the doomed epoch already reached the
//!    platter before the error (kept ops fold into the durable image right
//!    then, possibly sector-torn; dropped ops are gone for good). A later
//!    successful fsync promotes only writes issued after the failure; the
//!    dropped part never resurrects, and never survives a crash. This is
//!    what distinguishes "we PANIC'd and recovered from WAL" (correct) from
//!    "we retried fsync, it said OK, and we believed it" (the fsyncgate
//!    bug) — and also catches protocols that assume "failed fsync ⇒ old
//!    bytes intact" (in-place multi-sector overwrite shapes). Inc-3 HARD
//!    MODE: the same state machine now covers DIRECTORIES — every namespace
//!    op is journaled as an unsynced dirent op on its parent, and a FAILED
//!    parent-dir fsync dooms that dirent epoch through the [`CrashImage`]
//!    policy (kept dirents fold into the durable entry image right then;
//!    dropped ones — a rename, say — are durably lost and no later
//!    successful fsync resurrects them).
//! 3. **Dirent durability requires the parent-dir fsync**: namespace ops
//!    (create/rename/unlink/mkdir/rmdir) are volatile until the PARENT
//!    directory is fsync'd. At a crash each directory's unsynced dirent
//!    journal routes through the SAME [`CrashImage`] policy as file
//!    journals (fsync is the only BARRIER on the namespace plane too — a
//!    kind disk may persist an un-fsync'd rename; the [`CrashImage::DropAll`]
//!    floor reverts every directory to its last fsync'd entry image), and
//!    the namespace is rebuilt from the root. The classic
//!    lost-dirent-after-rename bug class (missing fsync_parent_path in a
//!    durable_rename shape) is therefore testable — and caught.
//!
//! **Crash ("cut") primitive** ([`SimVfs::cut`]): discard everything not
//! durable per rules 1–3, deterministically. The surviving subset of each
//! file's unsynced journal is chosen by the installed [`CrashImage`] policy
//! (default [`CrashImage::DropAll`], the adversarial floor; seeded arbitrary
//! subsets via [`CrashImage::SeededSubset`] — fsync is the only barrier, so
//! unsynced writes may survive in any combination, sector-torn).
//!
//! **Whole-node kill** (inc-3, [`SimVfs::set_kill_on_cut`]): when armed, a
//! cut also FREEZES the node — every later vfs op is refused (EIO) without
//! consulting the plan and without mutating ANY state, until
//! [`SimVfs::revive`]. This closes the inc-2 review exposure: a writer
//! process that keeps executing past its crash point (error/unwind paths
//! issuing VFD reopens, repair flushes) could otherwise mutate the
//! post-crash image before the harness packs it. Default off: the
//! model-level batteries deliberately recover in the same universe.
//!
//! Namespace model: rooted at "/". Relative paths resolve against the boot
//! cwd, which defaults to the root (harness batteries address data dirs
//! absolutely or mint them at "/"; a whole-server boot sets it to the
//! datadir via [`SimVfs::set_boot_cwd`]). Entry
//! names must be UTF-8 (EINVAL otherwise). No symlinks: `lstat` ≡ `stat`,
//! `read_link` fails EINVAL like readlink(2) on a non-symlink. Where
//! platforms disagree on an errno, SimVfs speaks the Linux dialect (e.g.
//! EISDIR for unlink-of-directory).

use std::cell::{Cell, RefCell, UnsafeCell};
use std::collections::BTreeMap;
use std::ffi::CStr;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock, PoisonError};

use crate::{c_int, mode_t, off_t, set_errno, FileInfo, Vfs, VfsDirIter, VfsResult, PG_O_DIRECT};

/// Sim fds start here. Anything below this base is a raw posix fd and gets
/// EBADF — sim-scope callers must never route raw fds through the trait.
pub const SIM_FD_BASE: c_int = 1_000_000;

/// Fixed pinned fd budget returned by `fd_budget_probe` (frozen surface:
/// "SimVfs: fixed pinned budget, no real fds touched"). Near PG's
/// max_files_per_process default so fd's `set_max_safe_fds` arithmetic
/// exercises realistic values.
pub const SIM_FD_BUDGET: usize = 960;

/// The atomicity floor (scoping doc §4.1): torn writes tear on 512 B sector
/// boundaries only. The ported code, like upstream, relies on ≤512 B writes
/// being sector-atomic (`controldata_utils` pins PG_CONTROL_MAX_SAFE_SIZE =
/// 512 with a compile-time assert) — a byte-granularity tear would
/// manufacture false positives against a design assumption the port itself
/// asserts.
pub const SECTOR_SIZE: usize = 512;

// ===========================================================================
// Fault-model interface (frozen at P1; P4 fills in the machinery)
// ===========================================================================

/// Which trait op is about to run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpKind {
    Open,
    Close,
    PReadV,
    PWriteV,
    Fsync,
    Fdatasync,
    FlushRange,
    Ftruncate,
    TruncatePath,
    Fallocate,
    FileSize,
    FadviseWillneed,
    Stat,
    Fstat,
    Lstat,
    ReadLink,
    Unlink,
    Rename,
    Mkdir,
    Rmdir,
    ReadDir,
    FdBudgetProbe,
}

/// Description of the op the fault plan is consulted about. `pread`/`pwrite`
/// present as single-iovec `PReadV`/`PWriteV` (they share the data plane).
/// P4 addition: fd-addressed ops resolve `path` to the fd's node's CURRENT
/// primary name at op time (review N4: renames — the WAL-recycle shape —
/// retarget class rules from the next op on; unlinked-but-open fds keep
/// their open-time path), so path-class rules apply to the data/durability
/// plane too.
#[derive(Debug, Clone)]
pub struct OpDesc<'a> {
    pub kind: OpKind,
    pub path: Option<&'a Path>,
    pub fd: Option<c_int>,
    pub offset: Option<off_t>,
    pub len: Option<usize>,
}

/// What the fault plan wants done. P4 injection is restricted to fd's
/// `errcode_for_file_access` errno vocabulary (contract §1.1): ENOENT,
/// EEXIST, ENOSPC/EDQUOT, EMFILE/ENFILE, EACCES/EPERM, EIO. SimVfs never
/// emits EINTR (ops are single-shot; retry policy lives in fd).
///
/// P4 semantics:
/// - `Errno(e)`: the op fails with errno `e` before taking effect. On
///   fsync/fdatasync of a file this ALSO fires the fsyncgate state machine
///   (the unsynced journal moves to lost-permanently).
/// - `ShortWrite(n)` / `ShortRead(n)`: the data-plane op transfers at most
///   `n` bytes and reports the short count (posix-legal partial transfer).
/// - `TornWrite { persist_prefix }`: the system CRASHES DURING this write.
///   The write's surviving on-disk prefix is `persist_prefix` floored to the
///   512 B sector boundary (rule 1); every other unsynced write survives per
///   the installed [`CrashImage`] policy; the op returns EIO (the simulated
///   process is gone — the harness treats this op as the cut).
/// - `Crash`: cut at the op boundary, before the op takes effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultDecision {
    Proceed,
    Errno(i32),
    ShortRead(usize),
    ShortWrite(usize),
    TornWrite { persist_prefix: usize },
    Crash,
}

/// Consulted before every op. Mutable so plans can count/schedule.
pub trait FaultPlan: Send {
    // Send: the process-shared universe mode (provider seam §9) homes
    // SimState — plan included — behind a process-global Mutex, and the
    // shared-universe increment migrates a simulated process's universe
    // between threads across scheduler quanta. Plans are seeded POD state
    // machines; Send is structural for every implementor.
    fn before_op(&mut self, op: &OpDesc<'_>) -> FaultDecision;

    /// Drain human-readable notes the plan wants appended to the fault log
    /// after this op's decision (review N5: a losing rule whose nth firing
    /// was consumed by a higher-priority rule logs a SUPPRESSED line so plan
    /// authors see the silent consumption). Default: nothing.
    fn drain_notes(&mut self) -> Vec<String> {
        Vec::new()
    }
}

/// The always-proceed plan (default).
#[derive(Debug, Default, Clone, Copy)]
pub struct NoFaults;

impl FaultPlan for NoFaults {
    fn before_op(&mut self, _op: &OpDesc<'_>) -> FaultDecision {
        FaultDecision::Proceed
    }
}

// ===========================================================================
// P4 fault-plan engine — deterministic, seeded, replayable from the log
// ===========================================================================

/// Path classes for fail-by-class rules (data-dir vocabulary).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathClass {
    /// pg_wal / pg_xlog segments.
    Wal,
    /// pg_control / *.conf (including their durable_rename .tmp companions).
    Config,
    /// pgsql_tmp trees and *.tmp staging files.
    Temp,
    /// base/ and global/ relation storage.
    Heap,
    Other,
}

/// Classify a path. Order matters: Wal > Config > Temp > Heap (pg_control's
/// durable_rename companion `pg_control.tmp` is Config, not Temp;
/// `base/pgsql_tmp/...` is Temp, not Heap).
pub fn classify_path(p: &Path) -> PathClass {
    let s = p.to_string_lossy();
    if s.contains("pg_wal") || s.contains("pg_xlog") {
        PathClass::Wal
    } else if s.contains("pg_control") || s.contains(".conf") {
        PathClass::Config
    } else if s.contains("pgsql_tmp") || s.ends_with(".tmp") {
        PathClass::Temp
    } else if s.contains("/base/") || s.contains("/global/") || s.ends_with("/base") {
        PathClass::Heap
    } else {
        PathClass::Other
    }
}

/// Op matcher for [`FaultRule`]. Empty (default) matches every op.
#[derive(Debug, Clone, Default)]
pub struct OpMatch {
    /// Restrict to these op kinds (None = any).
    pub kinds: Option<Vec<OpKind>>,
    /// Restrict to ops whose resolved path is in this class (ops without a
    /// resolvable path never match a class rule).
    pub class: Option<PathClass>,
    /// Restrict to paths containing this substring.
    pub path_contains: Option<String>,
}

impl OpMatch {
    pub fn any() -> Self {
        OpMatch::default()
    }

    fn matches(&self, op: &OpDesc<'_>) -> bool {
        if let Some(kinds) = &self.kinds {
            if !kinds.contains(&op.kind) {
                return false;
            }
        }
        if let Some(class) = self.class {
            match op.path {
                Some(p) => {
                    if classify_path(p) != class {
                        return false;
                    }
                }
                None => return false,
            }
        }
        if let Some(sub) = &self.path_contains {
            match op.path {
                Some(p) => {
                    if !p.to_string_lossy().contains(sub.as_str()) {
                        return false;
                    }
                }
                None => return false,
            }
        }
        true
    }
}

/// One rule of a [`SeededFaultPlan`]: on the `nth` (1-based) op matching
/// `matcher`, inject `action`. Non-sticky rules fire exactly once; sticky
/// rules fire on the nth and every later match.
#[derive(Debug, Clone)]
pub struct FaultRule {
    pub matcher: OpMatch,
    pub nth: u64,
    pub action: FaultDecision,
    pub sticky: bool,
}

impl FaultRule {
    /// Fail the nth op matching `matcher` (once).
    pub fn nth_matching(matcher: OpMatch, nth: u64, action: FaultDecision) -> Self {
        FaultRule { matcher, nth, action, sticky: false }
    }

    /// Crash at the nth op of the run, whatever it is.
    pub fn crash_at_op(nth: u64) -> Self {
        FaultRule::nth_matching(OpMatch::any(), nth, FaultDecision::Crash)
    }
}

/// The P4 deterministic seeded fault plan. Same `(seed, rules)` ⇒ the same
/// decision at the same op-sequence number, every run: all entropy is the
/// explicit seed (splitmix64 — the SimEntropy generator family), never
/// ambient randomness. Every rule counts its matches independently; when
/// several rules would fire on one op, the FIRST in rule order wins (rule
/// order is priority). Every non-Proceed decision is logged centrally by
/// SimVfs with its op-sequence number.
pub struct SeededFaultPlan {
    seed: u64,
    rules: Vec<(FaultRule, u64 /* matches so far */)>,
    /// Pending suppressed-decision notes (review N5), drained into the
    /// central fault log by the op that produced them.
    notes: Vec<String>,
}

impl SeededFaultPlan {
    pub fn new(seed: u64, rules: Vec<FaultRule>) -> Self {
        SeededFaultPlan {
            seed,
            rules: rules.into_iter().map(|r| (r, 0)).collect(),
            notes: Vec::new(),
        }
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// The common harness shape: install as the active plan AND arm the
    /// seeded crash-image policy from the same seed.
    pub fn install(seed: u64, rules: Vec<FaultRule>) {
        SimVfs::set_crash_image(CrashImage::SeededSubset(seed));
        SimVfs::set_fault_plan(Box::new(SeededFaultPlan::new(seed, rules)));
    }
}

impl FaultPlan for SeededFaultPlan {
    fn before_op(&mut self, op: &OpDesc<'_>) -> FaultDecision {
        let mut decision = FaultDecision::Proceed;
        for (i, (rule, matched)) in self.rules.iter_mut().enumerate() {
            if !rule.matcher.matches(op) {
                continue;
            }
            *matched += 1;
            let fire = *matched == rule.nth || (rule.sticky && *matched > rule.nth);
            if !fire {
                continue;
            }
            if decision == FaultDecision::Proceed {
                decision = rule.action;
            } else {
                // Review N5: the losing rule's nth firing is consumed by a
                // higher-priority rule — say so in the log instead of eating
                // it silently (rule order is priority; counters always run).
                self.notes.push(format!(
                    "SUPPRESSED rule#{i} nth={} action={:?} (lost to an earlier rule)",
                    rule.nth, rule.action
                ));
            }
        }
        decision
    }

    fn drain_notes(&mut self) -> Vec<String> {
        std::mem::take(&mut self.notes)
    }
}

/// What survives of the unsynced journals at a cut — and (review N2) what
/// a DOOMED fsync epoch persisted before its fsync errored. fsync is the
/// only barrier: everything promoted by a successful fsync always survives;
/// the policy governs both the currently-unsynced set at a cut and the
/// failed epoch's partial writeback at fsync-failure time (kept doomed ops
/// fold into the durable image right then; dropped ones never resurrect).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrashImage {
    /// Adversarial floor (default): nothing unsynced survives.
    DropAll,
    /// Kindest legal disk: every unsynced op survives whole.
    KeepAll,
    /// Seeded arbitrary subset of each file's unsynced ops; surviving
    /// writes may additionally tear on the 512 B floor. Deterministic in
    /// (seed, cut number, node, op index).
    SeededSubset(u64),
}

/// splitmix64 (Steele/Lea/Flood) — the same pure generator family SimEntropy
/// uses. All sim randomness flows from explicit seeds through this.
fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Deterministic per-journal-entry coin for [`CrashImage::SeededSubset`].
fn subset_coin(seed: u64, cut_no: u64, node: usize, idx: usize) -> u64 {
    let mut s = seed
        ^ cut_no.wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ (node as u64).wrapping_mul(0xA076_1D64_78BD_642F)
        ^ (idx as u64).wrapping_mul(0xE703_7ED1_A0B4_28DB);
    splitmix64(&mut s)
}

/// Deterministic coin for a DOOMED fsync epoch (review N2) — domain-separated
/// from the cut stream so epoch subsets and cut subsets are independent
/// draws of the same seed.
fn doomed_coin(seed: u64, epoch_no: u64, node: usize, idx: usize) -> u64 {
    subset_coin(seed ^ 0xD003_ED00_5EED_C0DE, epoch_no, node, idx)
}

/// The 512 B atomicity floor: the surviving prefix of a torn write ends on
/// an ABSOLUTE sector boundary (`off + p ≡ 0 mod 512`), or is the whole
/// write (the final partial sector rides out with the preceding full ones —
/// a write whose tail shares a sector with its body either got that sector
/// or didn't). `atomic` is the red-battery weakening: the whole write
/// persists regardless (multi-sector atomicity — the behavior no disk
/// guarantees, kept ONLY to prove the floor has teeth).
fn sector_prefix(off: usize, len: usize, want: usize, atomic: bool) -> usize {
    if atomic || want >= len {
        return len;
    }
    let end = off + want;
    (end / SECTOR_SIZE * SECTOR_SIZE).saturating_sub(off).min(len)
}

// ===========================================================================
// In-memory tree
// ===========================================================================

type NodeId = usize;

/// One op of a file's unsynced journal (everything since the last
/// SUCCESSFUL fsync). Order preserved for deterministic subset replay.
#[derive(Debug, Clone)]
enum JournalOp {
    Write { off: usize, data: Vec<u8> },
    SetLen { len: usize },
}

/// Apply one journal op to an image, keeping at most `keep` bytes of a
/// Write (usize::MAX = whole; SetLen is metadata and never tears).
fn apply_journal_op(img: &mut Vec<u8>, op: &JournalOp, keep: usize) {
    match op {
        JournalOp::Write { off, data } => {
            let n = keep.min(data.len());
            if n == 0 {
                return;
            }
            let end = *off + n;
            if img.len() < end {
                img.resize(end, 0);
            }
            img[*off..end].copy_from_slice(&data[..n]);
        }
        JournalOp::SetLen { len } => img.resize(*len, 0),
    }
}

/// A regular file: two-image store + the unsynced journal. (`Clone` is the
/// RED-battery stale-snapshot arm only — see [`SimVfs::arm_red_adoption`].)
#[derive(Debug, Default, Clone)]
struct SimFile {
    /// What the process sees (the page-cache view).
    volatile: Vec<u8>,
    /// The last successfully-synced on-disk image.
    durable: Vec<u8>,
    /// Ops since the last SUCCESSFUL fsync. A successful fsync folds these
    /// into `durable`; a FAILED fsync discards them (fsyncgate: that epoch
    /// is may-be-lost permanently — `volatile` keeps showing it, but no
    /// later fsync can make it durable and no crash lets it survive).
    unsynced: Vec<JournalOp>,
    /// Permission bits only (type bits synthesized in stat).
    mode: u32,
    nlink: u32,
    /// PG_O_DIRECT was requested on some open of this file (recorded per
    /// contract §4.2; read by direct-IO faulting, write-only today).
    #[allow(dead_code)]
    o_direct_seen: bool,
}

/// One namespace op of a directory's unsynced dirent journal (inc-3 dir
/// hard mode — the dirent analog of [`JournalOp`]). Order preserved for
/// deterministic subset replay; dirents are atomic (no tearing).
#[derive(Debug, Clone)]
enum DirentOp {
    Set { name: String, node: NodeId },
    Remove { name: String },
}

/// Apply one dirent op to a durable entry image.
fn apply_dirent_op(map: &mut BTreeMap<String, NodeId>, op: &DirentOp) {
    match op {
        DirentOp::Set { name, node } => {
            map.insert(name.clone(), *node);
        }
        DirentOp::Remove { name } => {
            map.remove(name);
        }
    }
}

#[derive(Debug, Default, Clone)]
struct SimDir {
    /// Volatile entry image (deterministic BTree order), name → node.
    entries: BTreeMap<String, NodeId>,
    /// Entry image as of this directory's last successful fsync. A crash
    /// folds a policy-chosen subset of `unsynced` into this, then reverts
    /// `entries` to it — dirent durability requires the parent-dir fsync
    /// (rule 3).
    durable_entries: BTreeMap<String, NodeId>,
    /// Namespace ops since this directory's last SUCCESSFUL fsync (inc-3
    /// dir hard mode). A successful fsync folds these into
    /// `durable_entries`; a FAILED fsync dooms the epoch through the
    /// [`CrashImage`] policy (fsyncgate, namespace plane).
    unsynced: Vec<DirentOp>,
    mode: u32,
}

#[derive(Debug, Clone)]
enum Node {
    File(SimFile),
    Dir(SimDir),
    /// Arena slot whose node became unreferenced with no open handles.
    Free,
}

#[derive(Debug, Clone)]
struct NodeSlot {
    node: Node,
    open_count: u32,
}

#[derive(Debug, Clone)]
struct OpenFile {
    node: NodeId,
    /// Open flags as given (recorded for access-mode faulting; sim does not
    /// enforce access modes on the data plane).
    #[allow(dead_code)]
    flags: c_int,
    /// The (normalized) path this fd was opened under. OpDesc.path for
    /// fd-addressed ops resolves the node's CURRENT name at op time (review
    /// N4); this open-time record is the fallback for unlinked-but-open fds.
    path: PathBuf,
}

struct SimState {
    nodes: Vec<NodeSlot>,
    /// Absolute normalized path → node. Includes "/" and every dir. This is
    /// the VOLATILE namespace view; crash rebuilds it from the directories'
    /// durable entry images.
    namespace: BTreeMap<PathBuf, NodeId>,
    open: BTreeMap<c_int, OpenFile>,
    next_fd: c_int,
    /// Every fd `open` has handed out, in order (replay invariant).
    fd_trace: Vec<c_int>,
    plan: Box<dyn FaultPlan>,
    /// Increments on every plan consult; the fault log speaks these.
    op_seq: u64,
    /// Deterministic log of every injected fault + every cut. Byte-stable
    /// across same-seed replays (the replay-identity gate byte-compares it).
    fault_log: Vec<String>,
    crash_image: CrashImage,
    /// RED-BATTERY WEAKENING ONLY: pretend multi-sector writes are atomic.
    atomic_writes: bool,
    /// Cuts so far (crash numbering for logs and harness detection).
    cuts: u64,
    /// Doomed fsync epochs so far (review N2: numbers the doomed-epoch
    /// subset draws, independently of cut numbering; inc-3: shared by the
    /// file and dir planes — node id disambiguates the coin).
    doomed_epochs: u64,
    /// inc-3 WHOLE-NODE KILL: when armed, a cut also freezes the node.
    kill_on_cut: bool,
    /// The node is dead (a cut fired with kill armed). Every vfs op is
    /// refused without mutation until [`SimVfs::revive`].
    killed: bool,
    /// Ops refused while dead (unwind-residue evidence counter).
    frozen_ops: u64,
    /// inc-3 op-trace hook: when on, every consulted op is recorded
    /// (seq/kind/class/path) — the sweep's stratifier reads this.
    trace_on: bool,
    trace: Vec<String>,
}

impl SimState {
    fn fresh() -> Self {
        let root = NodeSlot {
            node: Node::Dir(SimDir {
                entries: BTreeMap::new(),
                durable_entries: BTreeMap::new(),
                unsynced: Vec::new(),
                mode: 0o700,
            }),
            open_count: 0,
        };
        let mut namespace = BTreeMap::new();
        namespace.insert(PathBuf::from("/"), 0);
        SimState {
            nodes: vec![root],
            namespace,
            open: BTreeMap::new(),
            next_fd: SIM_FD_BASE,
            fd_trace: Vec::new(),
            plan: Box::new(NoFaults),
            op_seq: 0,
            fault_log: Vec::new(),
            crash_image: CrashImage::DropAll,
            atomic_writes: false,
            cuts: 0,
            doomed_epochs: 0,
            kill_on_cut: false,
            killed: false,
            frozen_ops: 0,
            trace_on: false,
            trace: Vec::new(),
        }
    }
}

thread_local! {
    static SIM: RefCell<SimState> = RefCell::new(SimState::fresh());
    /// Shared-universe binding (s2 §6 item 1): `Some` binds THIS thread to a
    /// process-shared universe instead of its private thread-local one.
    static BOUND: Cell<Option<&'static UniverseCell>> = const { Cell::new(None) };
}

// ===========================================================================
// Shared universes (s2 §6 item 1) — one filesystem per SIMULATED PROCESS.
//
// C processes share one fd table across all their threads; pgrust's product
// backends are threads of one server process, so C-parity for the sim is:
// every thread of one simulated process sees ONE universe (namespace + fd
// table + fault engine). The universe lives in a process-global registry
// keyed by a caller-chosen u64 (the simulated-process id, minted by the
// harness — today exactly one sim server per harness, id 1); the id reaches
// children by parent-side capture at the spawn doors (`current_universe_id`
// next to `register_child`, `adopt_universe` right after `enter_child`) —
// process identity flows down the spawn tree the way fork inherits the fd
// table.
//
// CONCURRENCY: no locks. Soundness rests on the permit scheduler's
// one-runner-at-a-time invariant (exactly one registered thread executes at
// any moment), ASSERTED on every shared access through the injected probe
// below (vfs is a frozen leaf crate — libc + plain types, Cargo.toml §1.1 —
// so it cannot ask pgsync itself; the enabler injects a plain fn pointer).
// The probe is strict: true only when the global permit scheduler exists AND
// the calling thread currently holds the permit — sharing is only legal
// under the scheduler, and an unregistered thread touching a shared
// universe dies loudly at the access (the F6 loud-assert precedent).
// The RefCell stays: within a quantum it catches reentrancy exactly as the
// thread-local always did.
// ===========================================================================

/// One shared universe. `Sync` is asserted, not derived: the permit
/// scheduler serializes all access (see the module note above), and every
/// touch runs the permit assert first.
struct UniverseCell {
    state: RefCell<SimState>,
}

// SAFETY: access is serialized by the permit scheduler (one runnable thread
// at a time); every access path asserts the injected permit probe before
// touching `state`. See the section comment above.
unsafe impl Sync for UniverseCell {}

/// The process-global registry of shared universes. Guarded by the SAME
/// permit invariant as the cells (share/adopt assert the probe first);
/// entries are leaked (`&'static`) — the simulated machine's disk outlives
/// any thread, like a real disk.
struct UniverseRegistry(UnsafeCell<BTreeMap<u64, &'static UniverseCell>>);

// SAFETY: as for UniverseCell — permit-serialized, probe-asserted.
unsafe impl Sync for UniverseRegistry {}

static UNIVERSES: UniverseRegistry = UniverseRegistry(UnsafeCell::new(BTreeMap::new()));

/// The injected permit probe (0 = none installed). Sharing REQUIRES it:
/// `share_universe_as` refuses to run without a probe, so scheduler-off sim
/// runs (crash-sweep, sim-net baseline, every unit battery) can never reach
/// the shared path and stay byte-identical by construction.
static PERMIT_PROBE: AtomicUsize = AtomicUsize::new(0);

/// RED-battery adoption sabotage (0 = off; see [`SimVfs::arm_red_adoption`]).
static RED_ADOPTION: AtomicUsize = AtomicUsize::new(0);

/// RED-battery adoption-sabotage SCOPE (0 = unscoped: every adoption is
/// sabotaged). Non-zero = a leaked `*mut String` thread-name substring:
/// only adopting threads whose OS thread name contains it are sabotaged.
/// Lets a corpus break exactly ONE class of children (e.g. the wpool
/// standbys, thread names `pg:standby:<pid>`) while the rest of the spawn
/// tree adopts honestly — the parallel-query red needs the WORKERS broken,
/// not the leader session that would otherwise die first and mask them.
static RED_SCOPE: AtomicUsize = AtomicUsize::new(0);

/// Does the red-adoption sabotage apply to THIS (adopting) thread?
fn red_applies_here() -> bool {
    let scope = RED_SCOPE.load(Ordering::Acquire);
    if scope == 0 {
        return true;
    }
    // SAFETY: only ever stored as a leaked Box<String> in arm_red_adoption_scope.
    let scope: &String = unsafe { &*(scope as *const String) };
    std::thread::current().name().is_some_and(|n| n.contains(scope.as_str()))
}

/// Deliberately broken sharing shapes for the red battery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedAdoption {
    /// `adopt_universe` silently does nothing: the child keeps its empty
    /// private universe — the exact pre-lane bug, resurrected.
    Empty,
    /// Adoption takes a deep SNAPSHOT of the shared state instead of
    /// binding: the child sees a plausible but frozen copy.
    Stale,
}

fn assert_permit(what: &str) {
    let probe = PERMIT_PROBE.load(Ordering::Acquire);
    assert!(
        probe != 0,
        "SimVfs shared universe: {what} without an installed permit probe \
         (sharing is only legal under the permit scheduler)"
    );
    // SAFETY: only ever stored from a `fn() -> bool` in set_shared_access_probe.
    let probe: fn() -> bool = unsafe { std::mem::transmute(probe) };
    assert!(
        probe(),
        "SimVfs shared universe: {what} from a thread that does not hold the \
         scheduler permit (unregistered thread, or access outside a quantum)"
    );
}

// ---------------------------------------------------------------------------
// Universe homing (provider seam, dst-p3-scheduler §9). Two modes:
//
// - THREAD-LOCAL (default): a thread is a universe. Every existing harness
//   battery (vfs units, fd crash_sweep, the recovery sweeps) relies on this
//   isolation — nothing changes for them.
// - PROCESS-SHARED: one universe for the whole process, installed exactly
//   once by the sim BOOT INSTALLER ([`SimVfs::install_process_universe`])
//   before any secondary thread performs a vfs op. A whole-server sim boot
//   spawns real OS threads (checkpointer, walwriter, backends) that must all
//   see the SAME simulated disk; thread-local homing would hand each an
//   empty namespace. Mutex serialization here is beneath the DST model (the
//   P3 scheduler serializes execution above it; until then, cross-thread fs
//   op interleaving is only as deterministic as the thread schedule).
//
// This is sim-internal state homing, not provider selection: `ActiveVfs`
// stays a compile-time alias (P1 §1.2). Product builds do not compile this
// module at all.
// ---------------------------------------------------------------------------

static SHARED_MODE: AtomicBool = AtomicBool::new(false);
static SHARED: OnceLock<Mutex<SimState>> = OnceLock::new();

fn with<R>(f: impl FnOnce(&mut SimState) -> R) -> R {
    // Thread-bound universe (shared-universe SimVfs: one filesystem per
    // simulated process) wins: the binding is explicit per-thread and
    // permit-serialized, so it takes precedence over the process-global
    // shared mode (provider seam §9) — a thread is never in both.
    if let Some(u) = BOUND.with(|b| b.get()) {
        assert_permit("state access");
        return f(&mut u.state.borrow_mut());
    }
    if SHARED_MODE.load(Ordering::Acquire) {
        // Poison recovery: a panic inside a closure (an unwinding elog ERROR
        // crossing a vfs op is a bug, but sim must stay diagnosable) leaves
        // the state as-of the last completed mutation; keep serving.
        let mut guard = SHARED
            .get()
            .expect("SHARED_MODE set without SHARED state")
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        return f(&mut guard);
    }
    SIM.with(|cell| f(&mut cell.borrow_mut()))
}

/// The red stale-snapshot clone: everything cloneable is cloned; the fault
/// plan (a `Box<dyn FaultPlan>`) resets to [`NoFaults`] — red runs assert
/// visibility properties, never fault-plan continuity.
fn snapshot_for_red(src: &SimState) -> SimState {
    SimState {
        nodes: src.nodes.clone(),
        namespace: src.namespace.clone(),
        open: src.open.clone(),
        next_fd: src.next_fd,
        fd_trace: src.fd_trace.clone(),
        plan: Box::new(NoFaults),
        op_seq: src.op_seq,
        fault_log: src.fault_log.clone(),
        crash_image: src.crash_image,
        atomic_writes: src.atomic_writes,
        cuts: src.cuts,
        doomed_epochs: src.doomed_epochs,
        kill_on_cut: src.kill_on_cut,
        killed: src.killed,
        frozen_ops: src.frozen_ops,
        trace_on: src.trace_on,
        trace: src.trace.clone(),
    }
}

/// The deterministic simulated filesystem. ZST; state is thread-local (one
/// simulated universe per harness thread).
pub struct SimVfs;

impl SimVfs {
    pub const fn new() -> Self {
        SimVfs
    }

    /// Harness API: tear down the current thread's simulated disk entirely —
    /// empty tree, fd counter back to [`SIM_FD_BASE`], empty fd trace and
    /// fault log, [`NoFaults`] plan, [`CrashImage::DropAll`], floor armed.
    /// NOT a crash (crash keeps durable state). On a shared-universe-bound
    /// thread this resets the SHARED universe (the whole simulated disk).
    pub fn reset() {
        with(|st| *st = SimState::fresh());
    }

    /// BOOT INSTALLER (provider seam, dst-p3-scheduler §9): re-home the sim
    /// universe from thread-local to PROCESS-SHARED. One-shot, irreversible
    /// for the process lifetime; call before any secondary thread performs a
    /// vfs op (the whole-server boot calls it from PostmasterMain before the
    /// first datadir access). Panics if called twice — two installers in one
    /// process is a harness bug.
    pub fn install_process_universe() {
        assert!(
            SHARED.set(Mutex::new(SimState::fresh())).is_ok(),
            "install_process_universe: shared sim universe already installed"
        );
        SHARED_MODE.store(true, Ordering::Release);
    }

    /// BOOT INSTALLER companion: set the sim cwd relative paths resolve
    /// against (the product chdir()s into the datadir and addresses it
    /// relatively). One-shot; must be an absolute path. Panics on a second
    /// call with a DIFFERENT path.
    pub fn set_boot_cwd(dir: &Path) {
        assert!(dir.is_absolute(), "set_boot_cwd: path must be absolute");
        let mut norm = PathBuf::from("/");
        for comp in dir.components() {
            match comp {
                Component::RootDir | Component::CurDir => {}
                Component::Prefix(_) => panic!("set_boot_cwd: prefix component"),
                Component::ParentDir => {
                    norm.pop();
                }
                Component::Normal(name) => norm.push(name),
            }
        }
        if let Err(_already) = BOOT_CWD.set(norm.clone()) {
            assert_eq!(
                BOOT_CWD.get(),
                Some(&norm),
                "set_boot_cwd: called twice with different paths"
            );
        }
    }

    /// ASSET-INGEST API (§9.3): create a directory in the namespace with its
    /// dirent already DURABLE (both entry images). Idempotent for an
    /// existing directory (shared ancestor prefixes of asset roots). The
    /// ingest precedes fault-plan installation and the schedule by
    /// construction, so it consults no plan and emits no events. Parent must
    /// already exist (asset walks are top-down).
    pub fn ingest_dir(path: &CStr, mode: u32) -> Result<(), i32> {
        let p = norm_path(path)?;
        with(|st| {
            if let Some(id) = st.lookup(&p) {
                return match st.nodes[id].node {
                    Node::Dir(_) => Ok(()),
                    _ => Err(libc::ENOTDIR),
                };
            }
            let (parent, name) = split_parent(&p)?;
            let pid = st.dir_id(&parent)?;
            let id = st.nodes.len();
            st.nodes.push(NodeSlot {
                node: Node::Dir(SimDir {
                    entries: BTreeMap::new(),
                    durable_entries: BTreeMap::new(),
                    // Durable from birth (§9.3): the ingested namespace is
                    // pre-history — empty inc-3 dirent journal, exactly as
                    // ingest_file's empty file journal below.
                    unsynced: Vec::new(),
                    mode: mode & 0o7777,
                }),
                open_count: 0,
            });
            if let Node::Dir(d) = &mut st.nodes[pid].node {
                d.entries.insert(name.clone(), id);
                d.durable_entries.insert(name, id);
            }
            st.namespace.insert(p.clone(), id);
            Ok(())
        })
    }

    /// ASSET-INGEST API (§9.3): install a file whose content is DURABLE from
    /// birth (volatile == durable, empty journal) — the host snapshot is the
    /// pre-history no fault plan may cut into. Fails EEXIST on collision
    /// (one manifest entry per path; overlapping roots are a manifest bug).
    pub fn ingest_file(path: &CStr, data: &[u8], mode: u32) -> Result<(), i32> {
        let p = norm_path(path)?;
        with(|st| {
            if st.lookup(&p).is_some() {
                return Err(libc::EEXIST);
            }
            let (parent, name) = split_parent(&p)?;
            let pid = st.dir_id(&parent)?;
            let id = st.nodes.len();
            st.nodes.push(NodeSlot {
                node: Node::File(SimFile {
                    volatile: data.to_vec(),
                    durable: data.to_vec(),
                    unsynced: Vec::new(),
                    mode: mode & 0o7777,
                    nlink: 1,
                    o_direct_seen: false,
                }),
                open_count: 0,
            });
            if let Node::Dir(d) = &mut st.nodes[pid].node {
                d.entries.insert(name.clone(), id);
                d.durable_entries.insert(name, id);
            }
            st.namespace.insert(p.clone(), id);
            Ok(())
        })
    }

    /// Harness API: install a fault plan.
    pub fn set_fault_plan(plan: Box<dyn FaultPlan>) {
        with(|st| st.plan = plan);
    }

    // --- shared universes (s2 §6 item 1; see the section comment above) ---

    /// Install the permit probe (a plain fn pointer — vfs is a leaf crate
    /// and cannot depend on pgsync; the enabler passes
    /// `pgsync::sim::current_thread_holds_permit`). Must be installed
    /// before [`SimVfs::share_universe_as`]; idempotent.
    pub fn set_shared_access_probe(probe: fn() -> bool) {
        PERMIT_PROBE.store(probe as usize, Ordering::Release);
    }

    /// Root call, on the simulated process's founding thread: MOVE this
    /// thread's private universe (with everything already seeded into it)
    /// into the process-global registry under `id`, and bind this thread to
    /// it. Panics if `id` is taken, or if no permit probe is installed
    /// (sharing is only legal under the permit scheduler), or if this
    /// thread does not hold the permit.
    pub fn share_universe_as(id: u64) {
        assert_permit("share_universe_as");
        assert!(
            BOUND.with(|b| b.get()).is_none(),
            "SimVfs: share_universe_as on an already-bound thread"
        );
        let moved = SIM.with(|cell| cell.replace(SimState::fresh()));
        let cell: &'static UniverseCell =
            Box::leak(Box::new(UniverseCell { state: RefCell::new(moved) }));
        // SAFETY: permit-serialized (asserted above).
        let reg = unsafe { &mut *UNIVERSES.0.get() };
        let prev = reg.insert(id, cell);
        assert!(prev.is_none(), "SimVfs: universe id {id} already shared");
        BOUND.with(|b| b.set(Some(cell)));
    }

    /// The parent-side capture for spawn-door inheritance: the universe id
    /// this thread is bound to, if any. `None` on unbound threads — spawn
    /// sites pass it through unchanged, so scheduler-off / sharing-off runs
    /// never adopt and stay byte-identical.
    pub fn current_universe_id() -> Option<u64> {
        let bound = BOUND.with(|b| b.get())?;
        assert_permit("current_universe_id");
        // SAFETY: permit-serialized (asserted above).
        let reg = unsafe { &*UNIVERSES.0.get() };
        reg.iter()
            .find(|(_, c)| std::ptr::eq(**c, bound))
            .map(|(&id, _)| id)
    }

    /// Child-side, right after the spawn-door `enter_child`: bind this
    /// thread to the shared universe `id`. Panics if the id is unknown
    /// (loud — a child adopting a universe its parent never shared is a
    /// wiring bug). Subject to [`SimVfs::arm_red_adoption`] sabotage.
    pub fn adopt_universe(id: u64) {
        assert_permit("adopt_universe");
        match if red_applies_here() { RED_ADOPTION.load(Ordering::Acquire) } else { 0 } {
            1 => return, // RedAdoption::Empty: the pre-lane bug, resurrected
            2 => {
                // RedAdoption::Stale: a frozen deep copy instead of a bind.
                // SAFETY: permit-serialized (asserted above).
                let reg = unsafe { &*UNIVERSES.0.get() };
                let cell = reg.get(&id).unwrap_or_else(|| {
                    panic!("SimVfs: adopt_universe({id}): no such shared universe")
                });
                let snap = snapshot_for_red(&cell.state.borrow());
                SIM.with(|c| *c.borrow_mut() = snap);
                return;
            }
            _ => {}
        }
        // SAFETY: permit-serialized (asserted above).
        let reg = unsafe { &*UNIVERSES.0.get() };
        let cell = *reg
            .get(&id)
            .unwrap_or_else(|| panic!("SimVfs: adopt_universe({id}): no such shared universe"));
        BOUND.with(|b| b.set(Some(cell)));
    }

    /// Is THIS thread bound to a shared universe? (The loadsort prefetch
    /// runtime gate: feeders are only spawned when the pump is bound.)
    pub fn shared_universe_active() -> bool {
        BOUND.with(|b| b.get()).is_some()
    }

    /// RED battery: sabotage every later [`SimVfs::adopt_universe`] with a
    /// deliberately broken sharing shape. Harness-only; `None` disarms.
    pub fn arm_red_adoption(mode: Option<RedAdoption>) {
        let v = match mode {
            None => 0,
            Some(RedAdoption::Empty) => 1,
            Some(RedAdoption::Stale) => 2,
        };
        RED_ADOPTION.store(v, Ordering::Release);
    }

    /// RED battery: scope the adoption sabotage to threads whose OS thread
    /// name contains `scope` (`None` = unscoped, the default). Harness-only;
    /// set once at boot, before any spawn door runs (the leaked string is
    /// never freed — one arming per red process, like the mode itself).
    pub fn arm_red_adoption_scope(scope: Option<&str>) {
        let v = match scope {
            None => 0,
            Some(s) => Box::leak(Box::new(s.to_string())) as *mut String as usize,
        };
        RED_SCOPE.store(v, Ordering::Release);
    }

    /// Harness API: what survives of the unsynced set at a cut.
    pub fn set_crash_image(policy: CrashImage) {
        with(|st| st.crash_image = policy);
    }

    /// RED-BATTERY WEAKENING (test-only, default off): pretend multi-sector
    /// writes persist atomically at a crash. Exists ONLY so the red battery
    /// can prove the 512 B floor catches what this mode masks. Never set it
    /// in a property harness.
    pub fn set_atomic_write_mode(on: bool) {
        with(|st| st.atomic_writes = on);
    }

    /// The crash-simulation primitive: discard everything not durable per
    /// the model rules (sector floor, fsync barrier, dirent-vs-dir-fsync),
    /// producing the post-crash image deterministically. All open fds die.
    pub fn cut() {
        with(|st| crash_locked(st, None));
    }

    /// Simulated power loss (alias of [`SimVfs::cut`], kept for the P1
    /// harness surface).
    pub fn crash(&self) {
        with(|st| crash_locked(st, None));
    }

    /// Deterministic dump of the whole tree: (path, None) for dirs,
    /// (path, Some((volatile, durable))) for files. BTree order.
    pub fn image_dump(&self) -> Vec<(PathBuf, Option<(Vec<u8>, Vec<u8>)>)> {
        with(|st| {
            st.namespace
                .iter()
                .map(|(path, &id)| match &st.nodes[id].node {
                    Node::File(f) => {
                        (path.clone(), Some((f.volatile.clone(), f.durable.clone())))
                    }
                    _ => (path.clone(), None),
                })
                .collect()
        })
    }

    /// Every fd `open` has returned, in order. Replay runs must reproduce
    /// this exactly (monotonic-assignment determinism rule).
    pub fn fd_trace(&self) -> Vec<c_int> {
        with(|st| st.fd_trace.clone())
    }

    /// The deterministic fault/cut log: one line per injected fault (with
    /// its op-sequence number) and per cut. Same seed + same plan spec ⇒
    /// byte-identical log (the replay-identity gate).
    pub fn fault_log() -> Vec<String> {
        with(|st| st.fault_log.clone())
    }

    /// Ops consulted so far (the op-sequence counter the fault log speaks).
    pub fn op_seq() -> u64 {
        with(|st| st.op_seq)
    }

    /// Cuts (crashes) since reset — the harness's "did the plan fire" probe.
    pub fn cut_count() -> u64 {
        with(|st| st.cuts)
    }

    /// inc-3 harness API: arm the WHOLE-NODE KILL — from the next cut on
    /// the node is dead: every vfs op is refused (EIO) without consulting
    /// the plan and without mutating anything, until [`SimVfs::revive`].
    /// Default off ([`SimVfs::reset`] clears it): the model-level batteries
    /// deliberately recover in the same universe.
    pub fn set_kill_on_cut(on: bool) {
        with(|st| st.kill_on_cut = on);
    }

    /// Is the node dead (a cut fired with the kill armed)?
    pub fn killed() -> bool {
        with(|st| st.killed)
    }

    /// Ops refused while the node was dead — the unwind-residue evidence
    /// counter (cumulative until reset).
    pub fn frozen_op_count() -> u64 {
        with(|st| st.frozen_ops)
    }

    /// Bring the node back up on the SAME disk (the recovery boot's view):
    /// ops flow again; the durable image is exactly what the cut left.
    pub fn revive() {
        with(|st| st.killed = false);
    }

    /// inc-3 harness API: record every consulted op (seq/kind/class/path).
    /// Off by default; the sweep's stratifier enables it on the baseline.
    pub fn set_op_trace(on: bool) {
        with(|st| st.trace_on = on);
    }

    /// The recorded op trace (empty unless [`SimVfs::set_op_trace`] is on).
    pub fn op_trace() -> Vec<String> {
        with(|st| st.trace.clone())
    }

    /// The [`crate::VfsFd`] drop arm (finding F1b): exactly [`Vfs::close`] —
    /// same fault gating, same fd-table release — except it tolerates the
    /// thread's sim universe already being torn down. Guard drops legally run
    /// inside thread-exit TLS destructors, and TLS destructor order is
    /// unspecified: once `SimState` is destroyed its open-fd table died with
    /// it, so there is nothing left to release (0, not EBADF).
    ///
    /// NOTE for fault-plan authors: because this IS the one close code
    /// path, guard drops on leak/unwind paths CONSUME plan `Close` steps (and
    /// can trigger a planned `Crash`) exactly like deliberate closes — an
    /// unwind that drops N live holders advances Close-op sequencing by N vs
    /// the pre-guard behavior (where those closes went posix-side/EBADF).
    /// Shared-universe arm: a bound thread's guard drops release into the
    /// PROCESS fd table — but only while the thread still holds the permit
    /// (guards declared after the spawn-door SlotGuard drop inside the
    /// final quantum, per the door discipline). A guard dropping in OS TLS
    /// teardown AFTER deregistration releases nothing: the fd stays open in
    /// the process table, exactly as when a C thread dies holding a process
    /// fd.
    pub fn close_on_drop(fd: c_int) -> c_int {
        if let Ok(Some(u)) = BOUND.try_with(|b| b.get()) {
            let probe = PERMIT_PROBE.load(Ordering::Acquire);
            if probe != 0 {
                // SAFETY: only ever stored from a fn() -> bool.
                let probe: fn() -> bool = unsafe { std::mem::transmute(probe) };
                if probe() {
                    return close_locked(&mut u.state.borrow_mut(), fd);
                }
            }
            return 0;
        }
        SIM.try_with(|cell| close_locked(&mut cell.borrow_mut(), fd)).unwrap_or(0)
    }
}

impl Default for SimVfs {
    fn default() -> Self {
        Self::new()
    }
}

// [`Vfs::close`]'s whole body, shared with [`SimVfs::close_on_drop`] so the
// guard-drop release and the deliberate close are ONE code path.
fn close_locked(st: &mut SimState, fd: c_int) -> c_int {
    if refuse_if_killed(st, OpKind::Close) {
        return fail(libc::EIO);
    }
    let opath = st.open.get(&fd).map(|of| of.path.clone());
    if let Some(e) = gate_simple(
        st,
        &OpDesc {
            kind: OpKind::Close,
            path: opath.as_deref(),
            fd: Some(fd),
            offset: None,
            len: None,
        },
    ) {
        return fail(e);
    }
    let Some(of) = st.open.remove(&fd) else {
        return fail(libc::EBADF);
    };
    st.nodes[of.node].open_count -= 1;
    // FD_DELETE_AT_CLOSE law: unlinked data lives until the LAST close.
    st.maybe_free(of.node);
    // A dir removed while its handle was open frees on last close — unless a
    // durable dirent still references it (crash could resurrect it).
    if st.nodes[of.node].open_count == 0
        && matches!(st.nodes[of.node].node, Node::Dir(_))
        && !st.namespace.values().any(|&id| id == of.node)
        && !st.durably_referenced(of.node)
    {
        st.nodes[of.node].node = Node::Free;
    }
    0
}

/// The cut: compute the post-crash image deterministically.
///
/// Data plane: each file becomes durable-image + a policy-chosen subset of
/// its unsynced journal, sector-torn per the floor. `forced` (the in-flight
/// torn write of a `TornWrite` decision) always survives, last.
/// Namespace plane: every directory reverts to its last-fsync'd entry image
/// and the namespace is rebuilt from the root; unreachable nodes are freed
/// (files reachable under several names get that nlink).
fn crash_locked(st: &mut SimState, forced: Option<(NodeId, JournalOp)>) {
    st.cuts += 1;
    let cut = st.cuts;
    let seq = st.op_seq;
    let policy = st.crash_image;
    let atomic = st.atomic_writes;
    st.fault_log.push(format!(
        "CUT#{cut} seq={seq} policy={policy:?} atomic={atomic}"
    ));
    if st.kill_on_cut && !st.killed {
        // inc-3 whole-node kill: the simulated process is GONE — freeze
        // every later vfs op until revive() so unwind residue cannot
        // repair the post-crash image before the harness packs it.
        st.killed = true;
        st.fault_log.push(format!("KILL#{cut} seq={seq} node frozen (whole-node kill armed)"));
    }
    st.open.clear();

    // ---- data plane ----
    for id in 0..st.nodes.len() {
        st.nodes[id].open_count = 0;
        let forced_here = match &forced {
            Some((nid, op)) if *nid == id => Some(op.clone()),
            _ => None,
        };
        let mut line: Option<String> = None;
        if let Node::File(f) = &mut st.nodes[id].node {
            if f.unsynced.is_empty() && forced_here.is_none() {
                f.volatile = f.durable.clone();
                continue;
            }
            let entries = std::mem::take(&mut f.unsynced);
            let mut img = f.durable.clone();
            let (mut kept, mut torn) = (0usize, 0usize);
            for (i, e) in entries.iter().enumerate() {
                let (keep, cap) = match policy {
                    CrashImage::DropAll => (false, 0),
                    CrashImage::KeepAll => (true, usize::MAX),
                    CrashImage::SeededSubset(seed) => {
                        let coin = subset_coin(seed, cut, id, i);
                        let keep = coin & 1 == 1;
                        let cap = match e {
                            JournalOp::Write { off, data }
                                if keep && (coin >> 1) & 3 == 0 =>
                            {
                                sector_prefix(
                                    *off,
                                    data.len(),
                                    ((coin >> 3) as usize) % (data.len() + 1),
                                    atomic,
                                )
                            }
                            _ => usize::MAX,
                        };
                        (keep, cap)
                    }
                };
                if keep {
                    kept += 1;
                    if cap != usize::MAX {
                        torn += 1;
                    }
                    apply_journal_op(&mut img, e, cap);
                }
            }
            let had_forced = forced_here.is_some();
            if let Some(op) = forced_here {
                // The in-flight torn write: its (already sector-floored)
                // prefix is what reached the platter — it survives whole.
                apply_journal_op(&mut img, &op, usize::MAX);
            }
            line = Some(format!(
                "CUT#{cut} node={id} unsynced={} kept={kept} torn={torn} inflight={}",
                entries.len(),
                had_forced
            ));
            f.durable = img.clone();
            f.volatile = img;
        }
        if let Some(l) = line {
            st.fault_log.push(l);
        }
    }

    // ---- namespace plane (inc-3 hard mode): each directory's unsynced
    // dirent journal routes through the SAME CrashImage policy as file
    // journals — fsync is the only BARRIER on the namespace plane too, so
    // un-fsync'd dirents may survive a crash in any combination (DropAll,
    // the default floor, reproduces the revert-to-durable behavior
    // exactly). Kept ops fold into the durable entry image; the volatile
    // view then collapses to it.
    for id in 0..st.nodes.len() {
        let mut line: Option<String> = None;
        if let Node::Dir(d) = &mut st.nodes[id].node {
            let ops = std::mem::take(&mut d.unsynced);
            if !ops.is_empty() {
                let mut kept = 0usize;
                for (i, op) in ops.iter().enumerate() {
                    let keep = match policy {
                        CrashImage::DropAll => false,
                        CrashImage::KeepAll => true,
                        CrashImage::SeededSubset(seed) => subset_coin(seed, cut, id, i) & 1 == 1,
                    };
                    if keep {
                        kept += 1;
                        apply_dirent_op(&mut d.durable_entries, op);
                    }
                }
                line = Some(format!(
                    "CUT#{cut} dir={id} unsynced={} kept={kept}",
                    ops.len()
                ));
            }
            d.entries = d.durable_entries.clone();
        }
        if let Some(l) = line {
            st.fault_log.push(l);
        }
    }

    // Rebuild the namespace from the root over the reverted entry images.
    let mut refs = vec![0u32; st.nodes.len()];
    refs[0] = 1;
    let mut visited = vec![false; st.nodes.len()];
    visited[0] = true;
    let mut ns: BTreeMap<PathBuf, NodeId> = BTreeMap::new();
    ns.insert(PathBuf::from("/"), 0);
    let mut stack: Vec<(PathBuf, NodeId)> = vec![(PathBuf::from("/"), 0)];
    while let Some((p, id)) = stack.pop() {
        let children: Vec<(String, NodeId)> = match &st.nodes[id].node {
            Node::Dir(d) => d.entries.iter().map(|(k, &v)| (k.clone(), v)).collect(),
            _ => Vec::new(),
        };
        for (name, cid) in children {
            if matches!(st.nodes[cid].node, Node::Free) {
                // A durable dirent may never point at a freed node (the free
                // rules guard on durable references — but a policy-kept
                // journal Set can reference a node freed while volatile-
                // unreachable); drop it defensively from BOTH images.
                if let Node::Dir(d) = &mut st.nodes[id].node {
                    d.entries.remove(&name);
                    d.durable_entries.remove(&name);
                }
                continue;
            }
            refs[cid] += 1;
            let cp = p.join(&name);
            let is_dir = matches!(st.nodes[cid].node, Node::Dir(_));
            ns.insert(cp.clone(), cid);
            if is_dir && !visited[cid] {
                visited[cid] = true;
                stack.push((cp, cid));
            }
        }
    }
    st.namespace = ns;
    for (id, slot) in st.nodes.iter_mut().enumerate() {
        if id == 0 {
            continue;
        }
        if matches!(slot.node, Node::Free) {
            continue;
        }
        if refs[id] == 0 {
            slot.node = Node::Free;
            continue;
        }
        if let Node::File(f) = &mut slot.node {
            f.nlink = refs[id];
        }
    }
}

// ---------------------------------------------------------------------------
// path helpers
// ---------------------------------------------------------------------------

/// The sim boot cwd (provider seam §9): relative paths resolve against it.
/// Defaults to "/" (harness batteries address data dirs absolutely or mint
/// them at the root); set at most once, by the boot installer, to the
/// datadir the product `ChangeToDataDir`'d into — the REAL chdir happens
/// too (conffile reads are vfs-EXCLUDED host stdio and need the host cwd),
/// so this keeps the sim view and the host view pointing at the same tree.
static BOOT_CWD: OnceLock<PathBuf> = OnceLock::new();

/// Lexically normalize. Relative paths resolve against [`BOOT_CWD`]
/// (default "/": no cwd unless a sim boot installed one).
fn norm_path(path: &CStr) -> Result<PathBuf, i32> {
    let bytes = path.to_bytes();
    if bytes.is_empty() {
        return Err(libc::ENOENT);
    }
    let s = match std::str::from_utf8(bytes) {
        Ok(s) => s,
        Err(_) => return Err(libc::EINVAL), // String-keyed namespace: UTF-8 only
    };
    let mut out = if s.starts_with('/') {
        PathBuf::from("/")
    } else {
        BOOT_CWD.get().cloned().unwrap_or_else(|| PathBuf::from("/"))
    };
    for comp in Path::new(s).components() {
        match comp {
            Component::RootDir | Component::CurDir => {}
            Component::Prefix(_) => return Err(libc::EINVAL),
            Component::ParentDir => {
                out.pop();
            }
            Component::Normal(name) => out.push(name),
        }
    }
    Ok(out)
}

/// Split into (parent, leaf name). Errors on "/".
fn split_parent(path: &Path) -> Result<(PathBuf, String), i32> {
    let parent = path.parent().ok_or(libc::EINVAL)?.to_path_buf();
    let name = path
        .file_name()
        .ok_or(libc::EINVAL)?
        .to_str()
        .ok_or(libc::EINVAL)?
        .to_string();
    Ok((parent, name))
}

fn fail(errno: i32) -> c_int {
    set_errno(errno);
    -1
}

fn fail_isize(errno: i32) -> isize {
    set_errno(errno);
    -1
}

// ---------------------------------------------------------------------------
// state helpers
// ---------------------------------------------------------------------------

impl SimState {
    fn lookup(&self, path: &Path) -> Option<NodeId> {
        self.namespace.get(path).copied()
    }

    fn dir_id(&self, path: &Path) -> Result<NodeId, i32> {
        let id = self.lookup(path).ok_or(libc::ENOENT)?;
        match self.nodes[id].node {
            Node::Dir(_) => Ok(id),
            _ => Err(libc::ENOTDIR),
        }
    }

    fn file_of_fd(&self, fd: c_int) -> Result<NodeId, i32> {
        let of = self.open.get(&fd).ok_or(libc::EBADF)?;
        match self.nodes[of.node].node {
            Node::File(_) => Ok(of.node),
            _ => Err(libc::EBADF), // data-plane op on a directory fd
        }
    }

    fn file_mut(&mut self, id: NodeId) -> &mut SimFile {
        match &mut self.nodes[id].node {
            Node::File(f) => f,
            _ => unreachable!("node {id} is not a file"),
        }
    }

    /// True if any directory's DURABLE entry image still references `id`
    /// (a crash could resurrect the node; it must not be freed).
    fn durably_referenced(&self, id: NodeId) -> bool {
        self.nodes.iter().any(|s| {
            matches!(&s.node, Node::Dir(d) if d.durable_entries.values().any(|&v| v == id))
        })
    }

    fn maybe_free(&mut self, id: NodeId) {
        if self.nodes[id].open_count != 0 {
            return;
        }
        let unlinked_file = matches!(&self.nodes[id].node, Node::File(f) if f.nlink == 0);
        if unlinked_file && !self.durably_referenced(id) {
            self.nodes[id].node = Node::Free;
        }
    }

    /// The fd-op path view for the fault plan — resolved AT OP TIME (review
    /// N4): the fd's node's current primary name, first in BTree order, so
    /// path-class rules track renames (the WAL-recycle shape: a segment
    /// renamed into pg_wal while open is Wal-class from that op on). Falls
    /// back to the open-time path when the node is no longer reachable
    /// (unlinked-but-open FD_DELETE_AT_CLOSE temp files keep their class).
    fn fd_path(&self, fd: c_int) -> Option<PathBuf> {
        let of = self.open.get(&fd)?;
        self.namespace
            .iter()
            .find(|(_, &id)| id == of.node)
            .map(|(p, _)| p.clone())
            .or_else(|| Some(of.path.clone()))
    }

    /// Volatile namespace mutation on a directory PLUS its dirent journal
    /// (inc-3 dir hard mode: every entry change is an unsynced dirent op
    /// until the parent's fsync promotes it).
    fn dir_set_entry(&mut self, dir: NodeId, name: &str, node: NodeId) {
        if let Node::Dir(d) = &mut self.nodes[dir].node {
            d.entries.insert(name.to_string(), node);
            d.unsynced.push(DirentOp::Set { name: name.to_string(), node });
        }
    }

    fn dir_remove_entry(&mut self, dir: NodeId, name: &str) {
        if let Node::Dir(d) = &mut self.nodes[dir].node {
            d.entries.remove(name);
            d.unsynced.push(DirentOp::Remove { name: name.to_string() });
        }
    }

    fn consult(&mut self, op: &OpDesc<'_>) -> FaultDecision {
        self.op_seq += 1;
        if self.trace_on {
            let (class, path) = match op.path {
                Some(p) => (classify_path(p), p.display().to_string()),
                None => (PathClass::Other, "-".to_string()),
            };
            // inc-5: offset rides the diagnostic trace so harness stratifiers
            // can identify page REWRITES (the FPW-red selector); `-` when the
            // op has no offset. Purely diagnostic — plans never read it.
            let off = op.offset.map(|o| o.to_string()).unwrap_or_else(|| "-".into());
            self.trace.push(format!(
                "OP seq={} kind={:?} class={class:?} off={off} path={path}",
                self.op_seq, op.kind
            ));
        }
        let d = self.plan.before_op(op);
        if d != FaultDecision::Proceed {
            let path = op
                .path
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "-".to_string());
            self.fault_log.push(format!(
                "FAULT seq={} op={:?} path={} fd={:?} off={:?} len={:?} decision={:?}",
                self.op_seq, op.kind, path, op.fd, op.offset, op.len, d
            ));
        }
        for note in self.plan.drain_notes() {
            self.fault_log.push(format!("NOTE seq={} {note}", self.op_seq));
        }
        d
    }
}

/// Sweep freeable nodes that just lost their last durable reference (runs
/// after a directory fsync promotes a new entry image).
fn gc_unreferenced(st: &mut SimState) {
    let mut free: Vec<NodeId> = Vec::new();
    for id in 1..st.nodes.len() {
        if st.nodes[id].open_count != 0 {
            continue;
        }
        let candidate = match &st.nodes[id].node {
            Node::File(f) => f.nlink == 0,
            Node::Dir(_) => !st.namespace.values().any(|&v| v == id),
            Node::Free => false,
        };
        if candidate && !st.durably_referenced(id) {
            free.push(id);
        }
    }
    for id in free {
        st.nodes[id].node = Node::Free;
    }
}

/// inc-3 whole-node kill: a dead node refuses every op WITHOUT consulting
/// the plan, without mutating anything and without advancing `op_seq` — the
/// simulated process's unwind cannot touch the post-crash image. Returns
/// true when the op must be refused (EIO).
fn refuse_if_killed(st: &mut SimState, kind: OpKind) -> bool {
    if !st.killed {
        return false;
    }
    st.frozen_ops += 1;
    if st.frozen_ops == 1 {
        st.fault_log.push(format!(
            "KILLED seq={} first-refused={kind:?} (node dead; all vfs ops frozen at the cut)",
            st.op_seq
        ));
    }
    true
}

/// Fault gate for non-data-plane ops (data-plane reads/writes additionally
/// understand Short*/Torn*). Returns Some(errno) if the op must fail.
fn gate_simple(st: &mut SimState, op: &OpDesc<'_>) -> Option<i32> {
    match st.consult(op) {
        FaultDecision::Proceed => None,
        FaultDecision::Errno(e) => Some(e),
        FaultDecision::Crash => {
            crash_locked(st, None);
            Some(libc::EIO)
        }
        // Short/torn decisions are only meaningful on the data plane; a
        // plan emitting them elsewhere is a plan bug — proceed loudly.
        FaultDecision::ShortRead(_)
        | FaultDecision::ShortWrite(_)
        | FaultDecision::TornWrite { .. } => {
            debug_assert!(false, "Short/Torn decision on non-data-plane op {:?}", op.kind);
            None
        }
    }
}

/// fsync/fdatasync body: plan gate + the fsyncgate state machine + promote.
fn sync_locked(st: &mut SimState, fd: c_int, kind: OpKind) -> c_int {
    if refuse_if_killed(st, kind) {
        return fail(libc::EIO);
    }
    let opath = st.fd_path(fd);
    let desc = OpDesc { kind, path: opath.as_deref(), fd: Some(fd), offset: None, len: None };
    match st.consult(&desc) {
        FaultDecision::Proceed => promote(st, fd),
        FaultDecision::Errno(e) => {
            // The fsyncgate state machine: a failed fsync means the dirty
            // epoch is may-be-lost PERMANENTLY — and, per review N2, "may be
            // lost" means an ARBITRARY SUBSET of the epoch may already have
            // reached the platter before the error (real writeback persists
            // pages in any order). The epoch's journal is routed through the
            // installed CrashImage policy: kept ops fold into the durable
            // image NOW (they made it to disk, possibly sector-torn);
            // dropped ops are gone for good. Either way the journal empties:
            // volatile keeps showing everything (page-cache view), no later
            // successful fsync resurrects the dropped ops, and no crash
            // brings them back. A protocol that assumes "failed fsync ⇒ old
            // bytes intact" is now caught too; retry-and-believe remains
            // caught under the DropAll/unlucky-seed arms.
            if let Some(of) = st.open.get(&fd).cloned() {
                let policy = st.crash_image;
                let atomic = st.atomic_writes;
                let node_id = of.node;
                let resolved = st.fd_path(fd).unwrap_or_else(|| of.path.clone());
                let mut line: Option<String> = None;
                let mut dir_epoch = false;
                // inc-3 DIR-FSYNC HARD MODE: the fsyncgate state machine
                // covers the namespace plane too — a FAILED dir fsync dooms
                // the pending dirent epoch through the policy (kept dirents
                // fold into the durable entry image NOW; dropped ones — a
                // rename, say — are durably lost; no later successful fsync
                // resurrects them, because promotion applies the journal,
                // not a volatile snapshot).
                if let Node::Dir(d) = &mut st.nodes[node_id].node {
                    if !d.unsynced.is_empty() {
                        st.doomed_epochs += 1;
                        let epoch = st.doomed_epochs;
                        let entries = std::mem::take(&mut d.unsynced);
                        let mut kept = 0usize;
                        for (i, entry) in entries.iter().enumerate() {
                            let keep = match policy {
                                CrashImage::DropAll => false,
                                CrashImage::KeepAll => true,
                                CrashImage::SeededSubset(seed) => {
                                    doomed_coin(seed, epoch, node_id, i) & 1 == 1
                                }
                            };
                            if keep {
                                kept += 1;
                                apply_dirent_op(&mut d.durable_entries, entry);
                            }
                        }
                        let seq = st.op_seq;
                        line = Some(format!(
                            "FSYNCGATE seq={seq} fd={fd} path={} epoch={epoch} \
                             epoch_ops={} kept={kept} torn=0 policy={policy:?} plane=dir",
                            resolved.display(),
                            entries.len()
                        ));
                        dir_epoch = true;
                    }
                }
                if let Node::File(f) = &mut st.nodes[node_id].node {
                    if !f.unsynced.is_empty() {
                        st.doomed_epochs += 1;
                        let epoch = st.doomed_epochs;
                        let entries = std::mem::take(&mut f.unsynced);
                        let (mut kept, mut torn) = (0usize, 0usize);
                        for (i, entry) in entries.iter().enumerate() {
                            let (keep, cap) = match policy {
                                CrashImage::DropAll => (false, 0),
                                CrashImage::KeepAll => (true, usize::MAX),
                                CrashImage::SeededSubset(seed) => {
                                    let coin = doomed_coin(seed, epoch, node_id, i);
                                    let keep = coin & 1 == 1;
                                    let cap = match entry {
                                        JournalOp::Write { off, data }
                                            if keep && (coin >> 1) & 3 == 0 =>
                                        {
                                            sector_prefix(
                                                *off,
                                                data.len(),
                                                ((coin >> 3) as usize) % (data.len() + 1),
                                                atomic,
                                            )
                                        }
                                        _ => usize::MAX,
                                    };
                                    (keep, cap)
                                }
                            };
                            if keep {
                                kept += 1;
                                if cap != usize::MAX {
                                    torn += 1;
                                }
                                apply_journal_op(&mut f.durable, entry, cap);
                            }
                        }
                        let seq = st.op_seq;
                        line = Some(format!(
                            "FSYNCGATE seq={seq} fd={fd} path={} epoch={epoch} \
                             epoch_ops={} kept={kept} torn={torn} policy={policy:?}",
                            resolved.display(),
                            entries.len()
                        ));
                    }
                }
                if let Some(l) = line {
                    st.fault_log.push(l);
                }
                if dir_epoch {
                    // Kept Remove ops may have dropped a node's last durable
                    // reference — it can no longer resurrect at a crash.
                    gc_unreferenced(st);
                }
            }
            fail(e)
        }
        FaultDecision::Crash => {
            crash_locked(st, None);
            fail(libc::EIO)
        }
        FaultDecision::ShortRead(_)
        | FaultDecision::ShortWrite(_)
        | FaultDecision::TornWrite { .. } => {
            debug_assert!(false, "Short/Torn decision on {kind:?}");
            promote(st, fd)
        }
    }
}

// ===========================================================================
// Vfs impl
// ===========================================================================

impl Vfs for SimVfs {
    fn open(&self, path: &CStr, flags: c_int, mode: mode_t) -> c_int {
        let p = match norm_path(path) {
            Ok(p) => p,
            Err(e) => return fail(e),
        };
        with(|st| {
            if refuse_if_killed(st, OpKind::Open) {
                return fail(libc::EIO);
            }
            if let Some(e) = gate_simple(
                st,
                &OpDesc { kind: OpKind::Open, path: Some(&p), fd: None, offset: None, len: None },
            ) {
                return fail(e);
            }

            let o_direct = PG_O_DIRECT != 0 && flags & PG_O_DIRECT != 0;
            let accmode = flags & libc::O_ACCMODE;
            let node = match st.lookup(&p) {
                Some(id) => {
                    if flags & libc::O_CREAT != 0 && flags & libc::O_EXCL != 0 {
                        return fail(libc::EEXIST);
                    }
                    match &mut st.nodes[id].node {
                        Node::Dir(_) => {
                            // Directory opens are read-only (dir-fsync handles).
                            if accmode != libc::O_RDONLY {
                                return fail(libc::EISDIR);
                            }
                            id
                        }
                        Node::File(f) => {
                            if o_direct {
                                f.o_direct_seen = true;
                            }
                            if flags & libc::O_TRUNC != 0 && accmode != libc::O_RDONLY {
                                // Truncation hits the volatile image only; it
                                // becomes durable at the next fsync.
                                f.volatile.clear();
                                f.unsynced.push(JournalOp::SetLen { len: 0 });
                            }
                            id
                        }
                        Node::Free => return fail(libc::ENOENT),
                    }
                }
                None => {
                    if flags & libc::O_CREAT == 0 {
                        return fail(libc::ENOENT);
                    }
                    let (parent, name) = match split_parent(&p) {
                        Ok(v) => v,
                        Err(e) => return fail(e),
                    };
                    let pid = match st.dir_id(&parent) {
                        Ok(v) => v,
                        Err(e) => return fail(e),
                    };
                    let id = st.nodes.len();
                    st.nodes.push(NodeSlot {
                        node: Node::File(SimFile {
                            volatile: Vec::new(),
                            durable: Vec::new(),
                            unsynced: Vec::new(),
                            mode: mode as u32 & 0o7777,
                            nlink: 1,
                            o_direct_seen: o_direct,
                        }),
                        open_count: 0,
                    });
                    st.dir_set_entry(pid, &name, id);
                    st.namespace.insert(p.clone(), id);
                    id
                }
            };

            st.nodes[node].open_count += 1;
            let fd = st.next_fd;
            st.next_fd += 1;
            st.open.insert(fd, OpenFile { node, flags, path: p.clone() });
            st.fd_trace.push(fd);
            fd
        })
    }

    fn close(&self, fd: c_int) -> c_int {
        with(|st| close_locked(st, fd))
    }

    fn preadv(&self, fd: c_int, iov: &[libc::iovec], off: off_t) -> isize {
        with(|st| {
            if refuse_if_killed(st, OpKind::PReadV) {
                return fail_isize(libc::EIO);
            }
            if off < 0 {
                return fail_isize(libc::EINVAL);
            }
            let want: usize = iov.iter().map(|v| v.iov_len).sum();
            let opath = st.fd_path(fd);
            let mut cap = want;
            match st.consult(&OpDesc {
                kind: OpKind::PReadV,
                path: opath.as_deref(),
                fd: Some(fd),
                offset: Some(off),
                len: Some(want),
            }) {
                FaultDecision::Proceed => {}
                FaultDecision::Errno(e) => return fail_isize(e),
                FaultDecision::ShortRead(n) => cap = cap.min(n),
                FaultDecision::Crash => {
                    crash_locked(st, None);
                    return fail_isize(libc::EIO);
                }
                FaultDecision::ShortWrite(_) | FaultDecision::TornWrite { .. } => {
                    debug_assert!(false, "write decision on preadv");
                }
            }
            let node = match st.file_of_fd(fd) {
                Ok(n) => n,
                Err(e) => return fail_isize(e),
            };
            let f = st.file_mut(node);
            let start = (off as usize).min(f.volatile.len());
            let avail = f.volatile.len() - start;
            let mut remaining = cap.min(avail);
            let mut done = 0usize;
            for v in iov {
                if remaining == 0 {
                    break;
                }
                let n = v.iov_len.min(remaining);
                // SAFETY: caller contract — iov bases valid for writes of
                // their lengths; source range is inside `volatile`.
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        f.volatile.as_ptr().add(start + done),
                        v.iov_base as *mut u8,
                        n,
                    );
                }
                done += n;
                remaining -= n;
            }
            done as isize
        })
    }

    fn pwritev(&self, fd: c_int, iov: &[libc::iovec], off: off_t) -> isize {
        with(|st| {
            if refuse_if_killed(st, OpKind::PWriteV) {
                return fail_isize(libc::EIO);
            }
            if off < 0 {
                return fail_isize(libc::EINVAL);
            }
            let want: usize = iov.iter().map(|v| v.iov_len).sum();
            let opath = st.fd_path(fd);
            let mut cap = want;
            let mut torn: Option<usize> = None;
            match st.consult(&OpDesc {
                kind: OpKind::PWriteV,
                path: opath.as_deref(),
                fd: Some(fd),
                offset: Some(off),
                len: Some(want),
            }) {
                FaultDecision::Proceed => {}
                FaultDecision::Errno(e) => return fail_isize(e),
                FaultDecision::ShortWrite(n) => cap = cap.min(n),
                FaultDecision::TornWrite { persist_prefix } => torn = Some(persist_prefix),
                FaultDecision::Crash => {
                    crash_locked(st, None);
                    return fail_isize(libc::EIO);
                }
                FaultDecision::ShortRead(_) => {
                    debug_assert!(false, "read decision on pwritev");
                }
            }
            let node = match st.file_of_fd(fd) {
                Ok(n) => n,
                Err(e) => return fail_isize(e),
            };
            let start = off as usize;

            // Gather the (possibly short-capped) bytes once; the journal
            // keeps them for crash-time subset/tear replay.
            let mut data: Vec<u8> = Vec::with_capacity(cap);
            for v in iov {
                if data.len() == cap {
                    break;
                }
                let n = v.iov_len.min(cap - data.len());
                // SAFETY: caller contract — iov bases valid for reads of
                // their lengths.
                let sl = unsafe { std::slice::from_raw_parts(v.iov_base as *const u8, n) };
                data.extend_from_slice(sl);
            }

            if let Some(pp) = torn {
                // Crash DURING this write (512 B atomicity floor): the
                // surviving prefix ends on an absolute sector boundary or is
                // the whole write; the caller never observes a result — the
                // simulated process is gone. EIO tells the harness the cut
                // fired.
                let keep = sector_prefix(start, data.len(), pp, st.atomic_writes);
                let seq = st.op_seq;
                let atomic = st.atomic_writes;
                st.fault_log.push(format!(
                    "TORN seq={seq} node={node} off={start} want={} kept={keep} atomic={atomic}",
                    data.len()
                ));
                data.truncate(keep);
                crash_locked(st, Some((node, JournalOp::Write { off: start, data })));
                return fail_isize(libc::EIO);
            }

            let f = st.file_mut(node);
            let done = data.len();
            if done > 0 {
                let end = start + done;
                if f.volatile.len() < end {
                    f.volatile.resize(end, 0);
                }
                f.volatile[start..end].copy_from_slice(&data);
                f.unsynced.push(JournalOp::Write { off: start, data });
            }
            done as isize
        })
    }

    fn pread(&self, fd: c_int, buf: &mut [u8], off: off_t) -> isize {
        let iov = [libc::iovec {
            iov_base: buf.as_mut_ptr() as *mut libc::c_void,
            iov_len: buf.len(),
        }];
        self.preadv(fd, &iov, off)
    }

    fn pwrite(&self, fd: c_int, buf: &[u8], off: off_t) -> isize {
        let iov = [libc::iovec {
            iov_base: buf.as_ptr() as *mut libc::c_void,
            iov_len: buf.len(),
        }];
        self.pwritev(fd, &iov, off)
    }

    fn fsync(&self, fd: c_int) -> c_int {
        with(|st| sync_locked(st, fd, OpKind::Fsync))
    }

    fn fdatasync(&self, fd: c_int) -> c_int {
        // Identical promotion semantics to fsync (no metadata split).
        with(|st| sync_locked(st, fd, OpKind::Fdatasync))
    }

    fn flush_range(&self, fd: c_int, off: off_t, len: off_t) -> c_int {
        with(|st| {
            if refuse_if_killed(st, OpKind::FlushRange) {
                return fail(libc::EIO);
            }
            let opath = st.fd_path(fd);
            if let Some(e) = gate_simple(
                st,
                &OpDesc {
                    kind: OpKind::FlushRange,
                    path: opath.as_deref(),
                    fd: Some(fd),
                    offset: Some(off),
                    len: Some(len.max(0) as usize),
                },
            ) {
                return fail(e);
            }
            // Hint; MAY no-op. Deliberately does NOT promote durability.
            if st.open.contains_key(&fd) {
                0
            } else {
                fail(libc::EBADF)
            }
        })
    }

    fn ftruncate(&self, fd: c_int, len: off_t) -> c_int {
        with(|st| {
            if refuse_if_killed(st, OpKind::Ftruncate) {
                return fail(libc::EIO);
            }
            if len < 0 {
                return fail(libc::EINVAL);
            }
            let opath = st.fd_path(fd);
            if let Some(e) = gate_simple(
                st,
                &OpDesc {
                    kind: OpKind::Ftruncate,
                    path: opath.as_deref(),
                    fd: Some(fd),
                    offset: None,
                    len: Some(len as usize),
                },
            ) {
                return fail(e);
            }
            let node = match st.file_of_fd(fd) {
                Ok(n) => n,
                Err(e) => return fail(e),
            };
            let f = st.file_mut(node);
            f.volatile.resize(len as usize, 0);
            f.unsynced.push(JournalOp::SetLen { len: len as usize });
            0
        })
    }

    fn truncate_path(&self, path: &CStr, len: off_t) -> c_int {
        let p = match norm_path(path) {
            Ok(p) => p,
            Err(e) => return fail(e),
        };
        with(|st| {
            if refuse_if_killed(st, OpKind::TruncatePath) {
                return fail(libc::EIO);
            }
            if len < 0 {
                return fail(libc::EINVAL);
            }
            if let Some(e) = gate_simple(
                st,
                &OpDesc {
                    kind: OpKind::TruncatePath,
                    path: Some(&p),
                    fd: None,
                    offset: None,
                    len: Some(len as usize),
                },
            ) {
                return fail(e);
            }
            let Some(id) = st.lookup(&p) else {
                return fail(libc::ENOENT);
            };
            match &mut st.nodes[id].node {
                Node::File(f) => {
                    f.volatile.resize(len as usize, 0);
                    f.unsynced.push(JournalOp::SetLen { len: len as usize });
                    0
                }
                Node::Dir(_) => fail(libc::EISDIR),
                Node::Free => fail(libc::ENOENT),
            }
        })
    }

    fn fallocate(&self, fd: c_int, off: off_t, len: off_t) -> c_int {
        // posix_fallocate convention (frozen surface): 0 on success, POSITIVE
        // errno on failure — no -1, no TLS errno. Sim models the Linux success
        // arm: zero-extend to off+len.
        with(|st| {
            if refuse_if_killed(st, OpKind::Fallocate) {
                return libc::EIO; // positive-errno convention
            }
            if off < 0 || len <= 0 {
                return libc::EINVAL;
            }
            let opath = st.fd_path(fd);
            match st.consult(&OpDesc {
                kind: OpKind::Fallocate,
                path: opath.as_deref(),
                fd: Some(fd),
                offset: Some(off),
                len: Some(len as usize),
            }) {
                FaultDecision::Proceed => {}
                FaultDecision::Errno(e) => return e,
                FaultDecision::Crash => {
                    crash_locked(st, None);
                    return libc::EIO;
                }
                FaultDecision::ShortRead(_)
                | FaultDecision::ShortWrite(_)
                | FaultDecision::TornWrite { .. } => {
                    debug_assert!(false, "Short/Torn decision on fallocate");
                }
            }
            let node = match st.file_of_fd(fd) {
                Ok(n) => n,
                Err(e) => return e, // positive-errno convention
            };
            let f = st.file_mut(node);
            let end = (off + len) as usize;
            if f.volatile.len() < end {
                f.volatile.resize(end, 0); // fallocate-as-zero-extend
                f.unsynced.push(JournalOp::SetLen { len: end });
            }
            0
        })
    }

    fn file_size(&self, fd: c_int) -> off_t {
        with(|st| {
            if refuse_if_killed(st, OpKind::FileSize) {
                return fail(libc::EIO) as off_t;
            }
            let opath = st.fd_path(fd);
            if let Some(e) = gate_simple(
                st,
                &OpDesc {
                    kind: OpKind::FileSize,
                    path: opath.as_deref(),
                    fd: Some(fd),
                    offset: None,
                    len: None,
                },
            ) {
                return fail(e) as off_t;
            }
            match st.file_of_fd(fd) {
                Ok(node) => st.file_mut(node).volatile.len() as off_t,
                Err(e) => fail(e) as off_t,
            }
        })
    }

    fn fadvise_willneed(&self, fd: c_int, off: off_t, len: off_t) -> c_int {
        with(|st| {
            if refuse_if_killed(st, OpKind::FadviseWillneed) {
                return fail(libc::EIO);
            }
            let opath = st.fd_path(fd);
            if let Some(e) = gate_simple(
                st,
                &OpDesc {
                    kind: OpKind::FadviseWillneed,
                    path: opath.as_deref(),
                    fd: Some(fd),
                    offset: Some(off),
                    len: Some(len.max(0) as usize),
                },
            ) {
                return fail(e);
            }
            0 // hint; MAY no-op
        })
    }

    fn stat(&self, path: &CStr, out: &mut FileInfo) -> c_int {
        let p = match norm_path(path) {
            Ok(p) => p,
            Err(e) => return fail(e),
        };
        with(|st| {
            if refuse_if_killed(st, OpKind::Stat) {
                return fail(libc::EIO);
            }
            if let Some(e) = gate_simple(
                st,
                &OpDesc { kind: OpKind::Stat, path: Some(&p), fd: None, offset: None, len: None },
            ) {
                return fail(e);
            }
            let Some(id) = st.lookup(&p) else {
                return fail(libc::ENOENT);
            };
            *out = info_of(&st.nodes[id].node);
            0
        })
    }

    fn fstat(&self, fd: c_int, out: &mut FileInfo) -> c_int {
        with(|st| {
            if refuse_if_killed(st, OpKind::Fstat) {
                return fail(libc::EIO);
            }
            let opath = st.fd_path(fd);
            if let Some(e) = gate_simple(
                st,
                &OpDesc {
                    kind: OpKind::Fstat,
                    path: opath.as_deref(),
                    fd: Some(fd),
                    offset: None,
                    len: None,
                },
            ) {
                return fail(e);
            }
            let Some(of) = st.open.get(&fd).cloned() else {
                return fail(libc::EBADF);
            };
            *out = info_of(&st.nodes[of.node].node);
            0
        })
    }

    fn lstat(&self, path: &CStr, out: &mut FileInfo) -> c_int {
        // No symlinks in sim: lstat ≡ stat (the plan still sees Lstat).
        let p = match norm_path(path) {
            Ok(p) => p,
            Err(e) => return fail(e),
        };
        with(|st| {
            if refuse_if_killed(st, OpKind::Lstat) {
                return fail(libc::EIO);
            }
            if let Some(e) = gate_simple(
                st,
                &OpDesc { kind: OpKind::Lstat, path: Some(&p), fd: None, offset: None, len: None },
            ) {
                return fail(e);
            }
            let Some(id) = st.lookup(&p) else {
                return fail(libc::ENOENT);
            };
            *out = info_of(&st.nodes[id].node);
            0
        })
    }

    fn read_link(&self, path: &CStr, buf: &mut [u8]) -> isize {
        let p = match norm_path(path) {
            Ok(p) => p,
            Err(e) => return fail_isize(e),
        };
        with(|st| {
            if refuse_if_killed(st, OpKind::ReadLink) {
                return fail_isize(libc::EIO);
            }
            if let Some(e) = gate_simple(
                st,
                &OpDesc {
                    kind: OpKind::ReadLink,
                    path: Some(&p),
                    fd: None,
                    offset: None,
                    len: Some(buf.len()),
                },
            ) {
                return fail_isize(e);
            }
            match st.lookup(&p) {
                // readlink(2) on a non-symlink: EINVAL. Sim has no symlinks.
                Some(_) => fail_isize(libc::EINVAL),
                None => fail_isize(libc::ENOENT),
            }
        })
    }

    fn unlink(&self, path: &CStr) -> c_int {
        let p = match norm_path(path) {
            Ok(p) => p,
            Err(e) => return fail(e),
        };
        with(|st| {
            if refuse_if_killed(st, OpKind::Unlink) {
                return fail(libc::EIO);
            }
            if let Some(e) = gate_simple(
                st,
                &OpDesc {
                    kind: OpKind::Unlink,
                    path: Some(&p),
                    fd: None,
                    offset: None,
                    len: None,
                },
            ) {
                return fail(e);
            }
            let Some(id) = st.lookup(&p) else {
                return fail(libc::ENOENT);
            };
            if matches!(st.nodes[id].node, Node::Dir(_)) {
                return fail(libc::EISDIR); // Linux dialect
            }
            let (parent, name) = match split_parent(&p) {
                Ok(v) => v,
                Err(e) => return fail(e),
            };
            st.namespace.remove(&p);
            if let Ok(pid) = st.dir_id(&parent) {
                st.dir_remove_entry(pid, &name);
            }
            let f = st.file_mut(id);
            f.nlink = f.nlink.saturating_sub(1);
            // Data lives until last close (FD_DELETE_AT_CLOSE temp files) —
            // or until the last DURABLE dirent stops referencing it.
            st.maybe_free(id);
            0
        })
    }

    fn rename(&self, from: &CStr, to: &CStr) -> c_int {
        let (fp, tp) = match (norm_path(from), norm_path(to)) {
            (Ok(a), Ok(b)) => (a, b),
            (Err(e), _) | (_, Err(e)) => return fail(e),
        };
        with(|st| {
            if refuse_if_killed(st, OpKind::Rename) {
                return fail(libc::EIO);
            }
            if let Some(e) = gate_simple(
                st,
                &OpDesc {
                    kind: OpKind::Rename,
                    path: Some(&fp),
                    fd: None,
                    offset: None,
                    len: None,
                },
            ) {
                return fail(e);
            }
            if fp == tp {
                return if st.lookup(&fp).is_some() { 0 } else { fail(libc::ENOENT) };
            }
            let Some(src) = st.lookup(&fp) else {
                return fail(libc::ENOENT);
            };
            let src_is_dir = matches!(st.nodes[src].node, Node::Dir(_));
            if src_is_dir && tp.starts_with(&fp) {
                return fail(libc::EINVAL); // moving a dir into itself
            }
            let (fparent, fname) = match split_parent(&fp) {
                Ok(v) => v,
                Err(e) => return fail(e),
            };
            let (tparent, tname) = match split_parent(&tp) {
                Ok(v) => v,
                Err(e) => return fail(e),
            };
            let tpid = match st.dir_id(&tparent) {
                Ok(v) => v,
                Err(e) => return fail(e),
            };

            // Atomic replace of an existing destination.
            if let Some(dst) = st.lookup(&tp) {
                let dst_is_dir = matches!(st.nodes[dst].node, Node::Dir(_));
                match (src_is_dir, dst_is_dir) {
                    (false, false) => {
                        st.namespace.remove(&tp);
                        let f = st.file_mut(dst);
                        f.nlink = f.nlink.saturating_sub(1);
                        st.maybe_free(dst);
                    }
                    (true, true) => {
                        let empty = match &st.nodes[dst].node {
                            Node::Dir(d) => d.entries.is_empty(),
                            _ => unreachable!(),
                        };
                        if !empty {
                            return fail(libc::ENOTEMPTY);
                        }
                        st.namespace.remove(&tp);
                        if st.nodes[dst].open_count == 0 && !st.durably_referenced(dst) {
                            st.nodes[dst].node = Node::Free;
                        }
                    }
                    (false, true) => return fail(libc::EISDIR),
                    (true, false) => return fail(libc::ENOTDIR),
                }
            }

            // Move the entry itself (volatile namespace only: durability of
            // BOTH dirents requires the corresponding dir fsyncs).
            st.namespace.remove(&fp);
            if let Ok(fpid) = st.dir_id(&fparent) {
                st.dir_remove_entry(fpid, &fname);
            }
            st.dir_set_entry(tpid, &tname, src);
            st.namespace.insert(tp.clone(), src);

            // Directory rename: rewrite the whole subtree's namespace keys.
            if src_is_dir {
                let moved: Vec<(PathBuf, NodeId)> = st
                    .namespace
                    .range(fp.clone()..)
                    .take_while(|(k, _)| k.starts_with(&fp))
                    .map(|(k, v)| (k.clone(), *v))
                    .collect();
                for (old_key, id) in moved {
                    let rel = old_key.strip_prefix(&fp).expect("prefix-scanned key");
                    let new_key = tp.join(rel);
                    st.namespace.remove(&old_key);
                    st.namespace.insert(new_key, id);
                }
            }
            0
        })
    }

    fn mkdir(&self, path: &CStr, mode: mode_t) -> c_int {
        let p = match norm_path(path) {
            Ok(p) => p,
            Err(e) => return fail(e),
        };
        with(|st| {
            if refuse_if_killed(st, OpKind::Mkdir) {
                return fail(libc::EIO);
            }
            if let Some(e) = gate_simple(
                st,
                &OpDesc { kind: OpKind::Mkdir, path: Some(&p), fd: None, offset: None, len: None },
            ) {
                return fail(e);
            }
            if st.lookup(&p).is_some() {
                return fail(libc::EEXIST);
            }
            let (parent, name) = match split_parent(&p) {
                Ok(v) => v,
                Err(e) => return fail(e), // mkdir("/")
            };
            let pid = match st.dir_id(&parent) {
                Ok(v) => v,
                Err(e) => return fail(e),
            };
            let id = st.nodes.len();
            st.nodes.push(NodeSlot {
                node: Node::Dir(SimDir {
                    entries: BTreeMap::new(),
                    durable_entries: BTreeMap::new(),
                    unsynced: Vec::new(),
                    mode: mode as u32 & 0o7777,
                }),
                open_count: 0,
            });
            st.dir_set_entry(pid, &name, id);
            st.namespace.insert(p.clone(), id);
            0
        })
    }

    fn rmdir(&self, path: &CStr) -> c_int {
        let p = match norm_path(path) {
            Ok(p) => p,
            Err(e) => return fail(e),
        };
        with(|st| {
            if refuse_if_killed(st, OpKind::Rmdir) {
                return fail(libc::EIO);
            }
            if let Some(e) = gate_simple(
                st,
                &OpDesc { kind: OpKind::Rmdir, path: Some(&p), fd: None, offset: None, len: None },
            ) {
                return fail(e);
            }
            let Some(id) = st.lookup(&p) else {
                return fail(libc::ENOENT);
            };
            match &st.nodes[id].node {
                Node::Dir(d) => {
                    if !d.entries.is_empty() {
                        return fail(libc::ENOTEMPTY);
                    }
                }
                Node::File(_) => return fail(libc::ENOTDIR),
                Node::Free => return fail(libc::ENOENT),
            }
            let (parent, name) = match split_parent(&p) {
                Ok(v) => v,
                Err(e) => return fail(e), // rmdir("/")
            };
            st.namespace.remove(&p);
            if let Ok(pid) = st.dir_id(&parent) {
                st.dir_remove_entry(pid, &name);
            }
            if st.nodes[id].open_count == 0 && !st.durably_referenced(id) {
                st.nodes[id].node = Node::Free;
            }
            0
        })
    }

    fn read_dir(&self, path: &CStr) -> VfsResult<VfsDirIter> {
        let p = norm_path(path).map_err(|e| {
            set_errno(e);
            e
        })?;
        with(|st| {
            if refuse_if_killed(st, OpKind::ReadDir) {
                set_errno(libc::EIO);
                return Err(libc::EIO);
            }
            if let Some(e) = gate_simple(
                st,
                &OpDesc {
                    kind: OpKind::ReadDir,
                    path: Some(&p),
                    fd: None,
                    offset: None,
                    len: None,
                },
            ) {
                set_errno(e);
                return Err(e);
            }
            let id = st.dir_id(&p).map_err(|e| {
                set_errno(e);
                e
            })?;
            let names: Vec<String> = match &st.nodes[id].node {
                Node::Dir(d) => d.entries.keys().cloned().collect(),
                _ => unreachable!(),
            };
            // Deterministic BTree order; "." and ".." never yielded (frozen
            // VfsDirIter semantics — matches fd's AllocateDir exposure).
            Ok(VfsDirIter::from_names(names))
        })
    }

    fn fd_budget_probe(&self, max_to_probe: usize) -> usize {
        with(|st| {
            if refuse_if_killed(st, OpKind::FdBudgetProbe) {
                return 0;
            }
            let _ = gate_simple(
                st,
                &OpDesc {
                    kind: OpKind::FdBudgetProbe,
                    path: None,
                    fd: None,
                    offset: None,
                    len: Some(max_to_probe),
                },
            );
            // Fixed pinned budget, no real fds touched: determinism over
            // realism. EMFILE budget INJECTION flows through FaultPlan on
            // open, not through here.
            SIM_FD_BUDGET.min(max_to_probe)
        })
    }
}

/// fsync/fdatasync promotion: fold the unsynced journal into the durable
/// image (NOT `durable = volatile` — after a failed fsync the doomed epoch
/// is gone from the journal, and this divergence is exactly the fsyncgate
/// semantics). Dir fsync promotes by applying the dirent JOURNAL (inc-3
/// hard mode: NOT a snapshot of the volatile entries — dirents dropped by
/// an earlier doomed epoch must never resurrect).
fn promote(st: &mut SimState, fd: c_int) -> c_int {
    let Some(of) = st.open.get(&fd).cloned() else {
        return fail(libc::EBADF);
    };
    let mut promoted_dir = false;
    match &mut st.nodes[of.node].node {
        Node::File(f) => {
            let ops = std::mem::take(&mut f.unsynced);
            for op in &ops {
                apply_journal_op(&mut f.durable, op, usize::MAX);
            }
        }
        Node::Dir(d) => {
            let ops = std::mem::take(&mut d.unsynced);
            for op in &ops {
                apply_dirent_op(&mut d.durable_entries, op);
            }
            promoted_dir = true;
        }
        Node::Free => return fail(libc::EBADF),
    }
    if promoted_dir {
        // Nodes that just lost their last durable reference are gone for
        // good (nothing can resurrect them at a crash anymore).
        gc_unreferenced(st);
    }
    0
}

// mtime stays zeroed: no wall clock in sim, ever. dev/ino/uid/gid synthetic
// population is the ratified P4-backlog rider (Ruling 2) — untouched here.
fn info_of(node: &Node) -> FileInfo {
    match node {
        Node::File(f) => FileInfo {
            size: f.volatile.len() as i64,
            mode: libc::S_IFREG as u32 | f.mode,
            nlink: f.nlink as u64,
            ..FileInfo::zeroed()
        },
        Node::Dir(d) => FileInfo {
            size: 0,
            mode: libc::S_IFDIR as u32 | d.mode,
            nlink: 2,
            ..FileInfo::zeroed()
        },
        Node::Free => FileInfo::zeroed(),
    }
}

// ===========================================================================
// Tests: sim semantics, differential golden (sim vs posix), same-ops replay,
// the P4 fault model battery. Run with:
//   RUSTFLAGS='--cfg pgrust_sim' cargo test -p vfs sim::
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::get_errno;
    use crate::posix::PosixVfs;
    use std::ffi::CString;

    fn c(s: &str) -> CString {
        CString::new(s).unwrap()
    }

    /// Fresh universe for this test thread.
    fn fresh() -> SimVfs {
        SimVfs::reset();
        SimVfs::new()
    }

    fn open_rw_create(v: &dyn Vfs, path: &str) -> c_int {
        v.open(&c(path), libc::O_CREAT | libc::O_RDWR, 0o600 as mode_t)
    }

    /// fsync a directory (dirent durability — rule 3).
    fn dir_fsync(v: &dyn Vfs, path: &str) {
        let dfd = v.open(&c(path), libc::O_RDONLY, 0 as mode_t);
        assert!(dfd >= SIM_FD_BASE, "dir open {path}");
        assert_eq!(v.fsync(dfd), 0, "dir fsync {path}");
        assert_eq!(v.close(dfd), 0);
    }

    /// Whole-file read via a fresh fd; None if the path is gone.
    fn read_file(v: &dyn Vfs, path: &str) -> Option<Vec<u8>> {
        let fd = v.open(&c(path), libc::O_RDONLY, 0 as mode_t);
        if fd < 0 {
            return None;
        }
        let size = v.file_size(fd);
        let mut buf = vec![0u8; size as usize];
        if size > 0 {
            assert_eq!(v.pread(fd, &mut buf, 0), size as isize);
        }
        assert_eq!(v.close(fd), 0);
        Some(buf)
    }

    #[test]
    fn happy_path_create_write_fsync_reopen_read() {
        let v = fresh();
        assert_eq!(v.mkdir(&c("/base"), 0o700 as mode_t), 0);
        let fd = open_rw_create(&v, "/base/f");
        assert!(fd >= SIM_FD_BASE);
        assert_eq!(v.pwrite(fd, b"hello world", 0), 11);
        assert_eq!(v.fsync(fd), 0);
        assert_eq!(v.close(fd), 0);

        let fd2 = v.open(&c("/base/f"), libc::O_RDONLY, 0 as mode_t);
        assert!(fd2 > fd, "monotonic fd assignment");
        assert_eq!(v.file_size(fd2), 11);
        let mut buf = [0u8; 32];
        assert_eq!(v.pread(fd2, &mut buf, 0), 11);
        assert_eq!(&buf[..11], b"hello world");
        // short read at EOF boundary, then past EOF
        assert_eq!(v.pread(fd2, &mut buf, 6), 5);
        assert_eq!(v.pread(fd2, &mut buf, 100), 0);
        assert_eq!(v.close(fd2), 0);
    }

    #[test]
    fn o_excl_o_trunc_and_errno_semantics() {
        let v = fresh();
        let fd = open_rw_create(&v, "/f");
        assert_eq!(v.pwrite(fd, b"data", 0), 4);
        assert_eq!(v.close(fd), 0);

        // O_EXCL on existing file
        set_errno(0);
        let r = v.open(&c("/f"), libc::O_CREAT | libc::O_EXCL | libc::O_RDWR, 0o600 as mode_t);
        assert_eq!(r, -1);
        assert_eq!(get_errno(), libc::EEXIST);

        // missing file, no O_CREAT
        set_errno(0);
        assert_eq!(v.open(&c("/nope"), libc::O_RDONLY, 0 as mode_t), -1);
        assert_eq!(get_errno(), libc::ENOENT);

        // O_TRUNC empties it
        let fd = v.open(&c("/f"), libc::O_RDWR | libc::O_TRUNC, 0 as mode_t);
        assert!(fd >= SIM_FD_BASE);
        assert_eq!(v.file_size(fd), 0);
        assert_eq!(v.close(fd), 0);

        // data-plane op on a raw-domain (small-int) fd
        set_errno(0);
        assert_eq!(v.pwrite(7, b"x", 0), -1, "raw posix fd must not work on sim");
        assert_eq!(get_errno(), libc::EBADF);
    }

    #[test]
    fn unlink_keeps_data_until_last_close() {
        // FD_DELETE_AT_CLOSE temp-file pattern.
        let v = fresh();
        let fd = open_rw_create(&v, "/tmpfile");
        assert_eq!(v.pwrite(fd, b"temp payload", 0), 12);
        assert_eq!(v.unlink(&c("/tmpfile")), 0);

        // Gone from the namespace...
        let mut fi = FileInfo::zeroed();
        assert_eq!(v.stat(&c("/tmpfile"), &mut fi), -1);
        // ...but the open handle still reads.
        let mut buf = [0u8; 12];
        assert_eq!(v.pread(fd, &mut buf, 0), 12);
        assert_eq!(&buf, b"temp payload");
        assert_eq!(v.close(fd), 0);

        // After the last close the node is freed; the name is reusable.
        let fd2 = open_rw_create(&v, "/tmpfile");
        assert_eq!(v.file_size(fd2), 0);
        assert_eq!(v.close(fd2), 0);
    }

    #[test]
    fn crash_discards_unsynced_keeps_synced() {
        let v = fresh();
        let fd = open_rw_create(&v, "/wal");
        // Rule 3: the dirent itself needs the parent-dir fsync to be durable.
        dir_fsync(&v, "/");
        assert_eq!(v.pwrite(fd, b"SYNCED--", 0), 8);
        assert_eq!(v.fsync(fd), 0);
        assert_eq!(v.pwrite(fd, b"UNSYNCED", 8), 8);
        assert_eq!(v.file_size(fd), 16);

        v.crash();
        assert_eq!(SimVfs::cut_count(), 1);

        // fd is dead after the crash
        set_errno(0);
        assert_eq!(v.fsync(fd), -1);
        assert_eq!(get_errno(), libc::EBADF);

        let fd2 = v.open(&c("/wal"), libc::O_RDONLY, 0 as mode_t);
        assert!(fd2 >= SIM_FD_BASE, "durable dirent must survive the crash");
        assert_eq!(v.file_size(fd2), 8, "unsynced tail discarded");
        let mut buf = [0u8; 8];
        assert_eq!(v.pread(fd2, &mut buf, 0), 8);
        assert_eq!(&buf, b"SYNCED--");
        assert_eq!(v.close(fd2), 0);
    }

    #[test]
    fn readdir_is_deterministic_btree_order() {
        let v = fresh();
        assert_eq!(v.mkdir(&c("/d"), 0o700 as mode_t), 0);
        // scrambled creation order
        for name in ["zeta", "alpha", "mid", "beta"] {
            let fd = open_rw_create(&v, &format!("/d/{name}"));
            assert_eq!(v.close(fd), 0);
        }
        let names1: Vec<String> =
            v.read_dir(&c("/d")).unwrap().map(Result::unwrap).collect();
        let names2: Vec<String> =
            v.read_dir(&c("/d")).unwrap().map(Result::unwrap).collect();
        assert_eq!(names1, names2, "two reads identical");
        assert_eq!(names1, vec!["alpha", "beta", "mid", "zeta"], "BTree order, no dot entries");
    }

    #[test]
    fn rename_atomic_replace_and_dir_subtree() {
        let v = fresh();
        let fd = open_rw_create(&v, "/a.tmp");
        assert_eq!(v.pwrite(fd, b"new", 0), 3);
        assert_eq!(v.fsync(fd), 0);
        assert_eq!(v.close(fd), 0);
        let fd = open_rw_create(&v, "/a");
        assert_eq!(v.pwrite(fd, b"old-contents", 0), 12);
        assert_eq!(v.close(fd), 0);

        // atomic replace (the durable_rename building block)
        assert_eq!(v.rename(&c("/a.tmp"), &c("/a")), 0);
        let fd = v.open(&c("/a"), libc::O_RDONLY, 0 as mode_t);
        let mut buf = [0u8; 3];
        assert_eq!(v.pread(fd, &mut buf, 0), 3);
        assert_eq!(&buf, b"new");
        assert_eq!(v.close(fd), 0);
        let mut fi = FileInfo::zeroed();
        assert_eq!(v.stat(&c("/a.tmp"), &mut fi), -1);

        // dir rename rewrites the subtree
        assert_eq!(v.mkdir(&c("/dir"), 0o700 as mode_t), 0);
        assert_eq!(v.mkdir(&c("/dir/sub"), 0o700 as mode_t), 0);
        let fd = open_rw_create(&v, "/dir/sub/leaf");
        assert_eq!(v.pwrite(fd, b"leafdata", 0), 8);
        assert_eq!(v.close(fd), 0);
        assert_eq!(v.rename(&c("/dir"), &c("/dir2")), 0);
        assert_eq!(v.stat(&c("/dir2/sub/leaf"), &mut fi), 0);
        assert_eq!(fi.size, 8);
        assert_eq!(v.stat(&c("/dir/sub/leaf"), &mut fi), -1);
    }

    #[test]
    fn rmdir_semantics() {
        let v = fresh();
        assert_eq!(v.mkdir(&c("/d"), 0o700 as mode_t), 0);
        let fd = open_rw_create(&v, "/d/f");
        assert_eq!(v.close(fd), 0);

        set_errno(0);
        assert_eq!(v.rmdir(&c("/d")), -1);
        assert_eq!(get_errno(), libc::ENOTEMPTY);

        assert_eq!(v.unlink(&c("/d/f")), 0);
        assert_eq!(v.rmdir(&c("/d")), 0);
        let mut fi = FileInfo::zeroed();
        assert_eq!(v.stat(&c("/d"), &mut fi), -1);

        // rmdir of a file: ENOTDIR
        let fd = open_rw_create(&v, "/plain");
        assert_eq!(v.close(fd), 0);
        set_errno(0);
        assert_eq!(v.rmdir(&c("/plain")), -1);
        assert_eq!(get_errno(), libc::ENOTDIR);
    }

    #[test]
    fn fallocate_zero_extends_and_ftruncate() {
        let v = fresh();
        let fd = open_rw_create(&v, "/seg");
        assert_eq!(v.pwrite(fd, b"abc", 0), 3);
        // positive-errno convention: 0 = success
        assert_eq!(v.fallocate(fd, 0, 100), 0);
        assert_eq!(v.file_size(fd), 100);
        let mut buf = [1u8; 4];
        assert_eq!(v.pread(fd, &mut buf, 3), 4);
        assert_eq!(&buf, &[0, 0, 0, 0], "extension is zero-filled");
        assert_eq!(v.ftruncate(fd, 2), 0);
        assert_eq!(v.file_size(fd), 2);
        // bad fd → positive errno, per convention
        assert_eq!(v.fallocate(5, 0, 10), libc::EBADF);
        assert_eq!(v.close(fd), 0);
    }

    #[test]
    fn fd_budget_probe_pinned() {
        let v = fresh();
        assert_eq!(v.fd_budget_probe(10_000), SIM_FD_BUDGET);
        assert_eq!(v.fd_budget_probe(100), 100);
        // and again — fixed, not stateful
        assert_eq!(v.fd_budget_probe(10_000), SIM_FD_BUDGET);
    }

    #[test]
    fn pg_o_direct_accepted_and_recorded() {
        let v = fresh();
        let fd = v.open(
            &c("/dio"),
            libc::O_CREAT | libc::O_RDWR | PG_O_DIRECT,
            0o600 as mode_t,
        );
        assert!(fd >= SIM_FD_BASE, "PG_O_DIRECT must be accepted");
        assert_eq!(v.pwrite(fd, b"x", 0), 1);
        assert_eq!(v.close(fd), 0);
    }

    #[test]
    fn stat_shapes_and_lstat_and_readlink() {
        let v = fresh();
        assert_eq!(v.mkdir(&c("/dd"), 0o750 as mode_t), 0);
        let fd = open_rw_create(&v, "/dd/f");
        assert_eq!(v.pwrite(fd, b"xy", 0), 2);

        let mut fi = FileInfo::zeroed();
        assert_eq!(v.stat(&c("/dd"), &mut fi), 0);
        assert!(fi.is_dir());
        assert_eq!(v.stat(&c("/dd/f"), &mut fi), 0);
        assert!(fi.is_file());
        assert_eq!(fi.size, 2);
        assert_eq!(v.fstat(fd, &mut fi), 0);
        assert_eq!(fi.size, 2);
        assert_eq!(v.lstat(&c("/dd/f"), &mut fi), 0);
        assert!(fi.is_file(), "no symlinks in sim: lstat == stat");
        assert_eq!(fi.mtime_sec, 0, "no wall clock in sim");

        let mut buf = [0u8; 16];
        set_errno(0);
        assert_eq!(v.read_link(&c("/dd/f"), &mut buf), -1);
        assert_eq!(get_errno(), libc::EINVAL, "readlink on non-symlink");
        assert_eq!(v.close(fd), 0);
    }

    // -----------------------------------------------------------------
    // Differential golden test (contract §4.4b, trait-level arm).
    // -----------------------------------------------------------------

    /// Runs the golden script against any Vfs with a path prefix; returns
    /// every observable byte/result the script produces.
    fn golden_script(v: &dyn Vfs, base: &str) -> Vec<Vec<u8>> {
        let mut out: Vec<Vec<u8>> = Vec::new();
        let dir = format!("{base}/gdir");
        assert_eq!(v.mkdir(&c(&dir), 0o700 as mode_t), 0);

        let tmp = format!("{dir}/data.tmp");
        let fd =
            v.open(&c(&tmp), libc::O_CREAT | libc::O_EXCL | libc::O_RDWR, 0o600 as mode_t);
        assert!(fd >= 0);

        // vectored write: "hello " + "world"
        let (a, b) = (b"hello ".to_vec(), b"world".to_vec());
        let iov = [
            libc::iovec { iov_base: a.as_ptr() as *mut libc::c_void, iov_len: a.len() },
            libc::iovec { iov_base: b.as_ptr() as *mut libc::c_void, iov_len: b.len() },
        ];
        out.push(vec![v.pwritev(fd, &iov, 0) as u8]);
        // overwrite in the middle + extend past EOF (hole)
        out.push(vec![v.pwrite(fd, b"XYZ", 3) as u8]);
        out.push(vec![v.pwrite(fd, b"tail", 20) as u8]);
        assert_eq!(v.fsync(fd), 0);
        assert_eq!(v.close(fd), 0);

        // durable_rename shape (the fsyncs compose above the trait, in fd)
        let fin = format!("{dir}/data.bin");
        assert_eq!(v.rename(&c(&tmp), &c(&fin)), 0);

        let fd = v.open(&c(&fin), libc::O_RDONLY, 0 as mode_t);
        assert!(fd >= 0);
        out.push(v.file_size(fd).to_le_bytes().to_vec());

        // vectored read back, split across two buffers
        let mut r1 = vec![0u8; 7];
        let mut r2 = vec![0u8; 64];
        let iov = [
            libc::iovec { iov_base: r1.as_mut_ptr() as *mut libc::c_void, iov_len: r1.len() },
            libc::iovec { iov_base: r2.as_mut_ptr() as *mut libc::c_void, iov_len: r2.len() },
        ];
        let n = v.preadv(fd, &iov, 0);
        out.push(n.to_le_bytes().to_vec());
        out.push(r1);
        out.push(r2[..(n as usize).saturating_sub(7)].to_vec());

        // plain pread of the hole region
        let mut hole = vec![0xAAu8; 6];
        let n = v.pread(fd, &mut hole, 14);
        out.push(n.to_le_bytes().to_vec());
        out.push(hole);

        // deterministic namespace view
        let mut names: Vec<String> =
            v.read_dir(&c(&dir)).unwrap().map(Result::unwrap).collect();
        names.sort(); // posix order is fs-defined; sim is already sorted
        out.push(names.join(",").into_bytes());

        let mut fi = FileInfo::zeroed();
        assert_eq!(v.stat(&c(&fin), &mut fi), 0);
        out.push(fi.size.to_le_bytes().to_vec());
        assert_eq!(v.close(fd), 0);
        assert_eq!(v.unlink(&c(&fin)), 0);
        assert_eq!(v.rmdir(&c(&dir)), 0);
        out
    }

    #[test]
    fn differential_golden_sim_vs_posix() {
        let sim = fresh();
        let sim_out = golden_script(&sim, "");

        let posix = PosixVfs::new();
        let base = std::env::temp_dir().join(format!("vfs-sim-golden-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();
        let posix_out = golden_script(&posix, base.to_str().unwrap());
        let _ = std::fs::remove_dir_all(&base);

        assert_eq!(sim_out, posix_out, "sim and posix must be byte-identical");
    }

    // -----------------------------------------------------------------
    // Same-ops-replay test (contract §4.4c): record the op stream from a
    // seeded scripted run, replay into a fresh SimVfs, assert byte-identical
    // volatile+durable images and identical fd assignment.
    // -----------------------------------------------------------------

    /// All randomness from the harness seed (determinism rule).
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            // xorshift64*
            let mut x = self.0;
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            self.0 = x;
            x.wrapping_mul(0x2545_F491_4F6C_DD1D)
        }
        fn below(&mut self, n: usize) -> usize {
            (self.next() % n as u64) as usize
        }
    }

    #[derive(Debug, Clone)]
    enum ScriptOp {
        Open { path: String, flags: c_int, expect: c_int },
        PWrite { fd: c_int, off: off_t, data: Vec<u8>, expect: isize },
        Fsync { fd: c_int, expect: c_int },
        Ftruncate { fd: c_int, len: off_t, expect: c_int },
        Close { fd: c_int, expect: c_int },
        Rename { from: String, to: String, expect: c_int },
        Unlink { path: String, expect: c_int },
        Crash,
    }

    /// Record pass fills in `expect`; replay pass asserts the observed
    /// result matches the recording.
    fn apply(v: &SimVfs, op: &mut ScriptOp, replay: bool) {
        match op {
            ScriptOp::Open { path, flags, expect } => {
                let r = v.open(&c(path), *flags, 0o600 as mode_t);
                if replay {
                    assert_eq!(r, *expect, "open({path}) fd/result diverged on replay");
                } else {
                    *expect = r;
                }
            }
            ScriptOp::PWrite { fd, off, data, expect } => {
                let r = v.pwrite(*fd, data, *off);
                if replay {
                    assert_eq!(r, *expect, "pwrite(fd={fd}) diverged on replay");
                } else {
                    *expect = r;
                }
            }
            ScriptOp::Fsync { fd, expect } => {
                let r = v.fsync(*fd);
                if replay {
                    assert_eq!(r, *expect);
                } else {
                    *expect = r;
                }
            }
            ScriptOp::Ftruncate { fd, len, expect } => {
                let r = v.ftruncate(*fd, *len);
                if replay {
                    assert_eq!(r, *expect);
                } else {
                    *expect = r;
                }
            }
            ScriptOp::Close { fd, expect } => {
                let r = v.close(*fd);
                if replay {
                    assert_eq!(r, *expect);
                } else {
                    *expect = r;
                }
            }
            ScriptOp::Rename { from, to, expect } => {
                let r = v.rename(&c(from), &c(to));
                if replay {
                    assert_eq!(r, *expect);
                } else {
                    *expect = r;
                }
            }
            ScriptOp::Unlink { path, expect } => {
                let r = v.unlink(&c(path));
                if replay {
                    assert_eq!(r, *expect);
                } else {
                    *expect = r;
                }
            }
            ScriptOp::Crash => v.crash(),
        }
    }

    #[test]
    fn same_ops_replay_byte_identical() {
        const SEED: u64 = 0x5EED_0D57_0001;
        const N_OPS: usize = 400;

        // ---- record pass: generate the script from the seed and run it ----
        let vfs = fresh();
        let mut rng = Rng(SEED);
        let paths: Vec<String> = (0..6).map(|i| format!("/f{i}")).collect();
        let mut live_fds: Vec<c_int> = Vec::new();
        let mut script: Vec<ScriptOp> = Vec::new();

        for _ in 0..N_OPS {
            let mut op = match rng.below(100) {
                0..=24 => ScriptOp::Open {
                    path: paths[rng.below(paths.len())].clone(),
                    flags: libc::O_CREAT | libc::O_RDWR,
                    expect: 0,
                },
                25..=59 => {
                    if live_fds.is_empty() {
                        continue;
                    }
                    let fd = live_fds[rng.below(live_fds.len())];
                    let len = 1 + rng.below(200);
                    let mut data = vec![0u8; len];
                    for b in &mut data {
                        *b = rng.next() as u8;
                    }
                    ScriptOp::PWrite { fd, off: rng.below(4096) as off_t, data, expect: 0 }
                }
                60..=71 => {
                    if live_fds.is_empty() {
                        continue;
                    }
                    ScriptOp::Fsync { fd: live_fds[rng.below(live_fds.len())], expect: 0 }
                }
                72..=78 => {
                    if live_fds.is_empty() {
                        continue;
                    }
                    ScriptOp::Ftruncate {
                        fd: live_fds[rng.below(live_fds.len())],
                        len: rng.below(2048) as off_t,
                        expect: 0,
                    }
                }
                79..=86 => {
                    if live_fds.is_empty() {
                        continue;
                    }
                    let i = rng.below(live_fds.len());
                    let fd = live_fds.remove(i);
                    ScriptOp::Close { fd, expect: 0 }
                }
                87..=91 => ScriptOp::Rename {
                    from: paths[rng.below(paths.len())].clone(),
                    to: paths[rng.below(paths.len())].clone(),
                    expect: 0,
                },
                92..=96 => ScriptOp::Unlink {
                    path: paths[rng.below(paths.len())].clone(),
                    expect: 0,
                },
                _ => {
                    live_fds.clear(); // crash drops every open fd
                    ScriptOp::Crash
                }
            };
            apply(&vfs, &mut op, false);
            if let ScriptOp::Open { expect, .. } = &op {
                if *expect >= 0 {
                    live_fds.push(*expect);
                }
            }
            script.push(op);
        }

        let recorded_images = vfs.image_dump();
        let recorded_fds = vfs.fd_trace();
        assert!(!recorded_fds.is_empty(), "script must have opened something");

        // ---- replay pass: same recorded stream into a fresh SimVfs ----
        let vfs = fresh();
        for op in &mut script {
            apply(&vfs, op, true);
        }

        assert_eq!(
            recorded_fds,
            vfs.fd_trace(),
            "fd assignment must be identical across replay"
        );
        assert_eq!(
            recorded_images,
            vfs.image_dump(),
            "volatile+durable images must be byte-identical across replay"
        );
    }

    // -----------------------------------------------------------------
    // Fault-plan plumbing: the plan is consulted on every op and its
    // decisions are honored.
    // -----------------------------------------------------------------

    struct CountingPlan {
        ops: Vec<OpKind>,
        fail_nth: Option<(usize, i32)>,
    }
    impl FaultPlan for CountingPlan {
        fn before_op(&mut self, op: &OpDesc<'_>) -> FaultDecision {
            self.ops.push(op.kind);
            if let Some((n, e)) = self.fail_nth {
                if self.ops.len() == n {
                    return FaultDecision::Errno(e);
                }
            }
            FaultDecision::Proceed
        }
    }

    #[test]
    fn fault_plan_is_consulted_and_honored() {
        let v = fresh();
        // 3rd op (the pwrite) fails ENOSPC — the fd ENOSPC convention.
        SimVfs::set_fault_plan(Box::new(CountingPlan {
            ops: Vec::new(),
            fail_nth: Some((3, libc::ENOSPC)),
        }));
        assert_eq!(v.mkdir(&c("/d"), 0o700 as mode_t), 0); // op 1
        let fd = open_rw_create(&v, "/d/f"); // op 2
        assert!(fd >= SIM_FD_BASE);
        set_errno(0);
        assert_eq!(v.pwrite(fd, b"boom", 0), -1); // op 3 → ENOSPC
        assert_eq!(get_errno(), libc::ENOSPC);
        assert_eq!(v.pwrite(fd, b"fine", 0), 4); // op 4 proceeds
        assert_eq!(v.close(fd), 0);
        // the injected fault is in the log, with its op-sequence number
        let log = SimVfs::fault_log();
        assert_eq!(log.len(), 1, "one injected fault, one log line: {log:?}");
        assert!(
            log[0].contains("seq=3") && log[0].contains("Errno"),
            "log line carries the op-sequence number: {}",
            log[0]
        );
    }

    struct ShortWritePlan;
    impl FaultPlan for ShortWritePlan {
        fn before_op(&mut self, op: &OpDesc<'_>) -> FaultDecision {
            if op.kind == OpKind::PWriteV {
                FaultDecision::ShortWrite(3)
            } else {
                FaultDecision::Proceed
            }
        }
    }

    #[test]
    fn short_write_decision_caps_the_write() {
        let v = fresh();
        SimVfs::set_fault_plan(Box::new(ShortWritePlan));
        let fd = open_rw_create(&v, "/s");
        assert_eq!(v.pwrite(fd, b"abcdef", 0), 3, "short write honored");
        assert_eq!(v.file_size(fd), 3);
        assert_eq!(v.close(fd), 0);
    }

    // Finding F1b: the VfsFd guard's drop must release the fd IN THE SIM
    // TABLE (never posix-side), and into_raw must disarm it (no double-close).
    #[test]
    fn vfsfd_guard_drop_releases_sim_side_and_into_raw_disarms() {
        let v = fresh();
        let mut info = crate::FileInfo::zeroed();

        // Drop arm: guard falls out of scope armed → sim fd released.
        let raw = open_rw_create(&v, "/g1");
        assert!(raw >= SIM_FD_BASE);
        // SAFETY: raw is live, sim-minted, exclusively owned by the guard.
        let guard = unsafe { crate::VfsFd::from_raw(raw) };
        assert_eq!(v.fstat(raw, &mut info), 0, "open before drop");
        drop(guard);
        assert_eq!(v.fstat(raw, &mut info), -1, "guard drop must close sim-side");
        assert_eq!(get_errno(), libc::EBADF);

        // Disarm arm: into_raw hands the fd back, no close happens; the
        // deliberate vfs close is then the ONLY close (no EBADF double-close).
        let raw2 = open_rw_create(&v, "/g2");
        // SAFETY: as above.
        let guard2 = unsafe { crate::VfsFd::from_raw(raw2) };
        let handed = guard2.into_raw();
        assert_eq!(handed, raw2);
        assert_eq!(v.fstat(raw2, &mut info), 0, "into_raw must not close");
        assert_eq!(v.close(raw2), 0, "single deliberate close after disarm");

        // Teardown tolerance is exercised end-to-end by fd's
        // thread_exit_with_live_vfs_fd_holders_does_not_abort_process; here
        // pin the direct contract: close_on_drop == close semantics while the
        // universe is alive.
        let raw3 = open_rw_create(&v, "/g3");
        assert_eq!(SimVfs::close_on_drop(raw3), 0);
        assert_eq!(v.fstat(raw3, &mut info), -1);
        assert_eq!(get_errno(), libc::EBADF);
    }

    // ===================================================================
    // P4 fault-model battery
    // ===================================================================

    /// Rule 1: torn writes tear on ABSOLUTE 512 B sector boundaries — any
    /// prefix of whole sectors, never a partial sector.
    #[test]
    fn torn_write_tears_on_absolute_sector_boundaries() {
        let v = fresh();
        let fd = open_rw_create(&v, "/f");
        dir_fsync(&v, "/");
        // durable base: 2048 bytes of 0xBB
        let base = vec![0xBBu8; 2048];
        assert_eq!(v.pwrite(fd, &base, 0), 2048);
        assert_eq!(v.fsync(fd), 0);

        // Crash during a 1300-byte write at off=100 with persist_prefix=700:
        // end = 100+700 = 800 → floored to 512 → surviving prefix = 412
        // bytes ([100, 512) — an absolute sector boundary).
        SeededFaultPlan::install(
            0x1,
            vec![FaultRule::nth_matching(
                OpMatch { kinds: Some(vec![OpKind::PWriteV]), ..OpMatch::default() },
                1,
                FaultDecision::TornWrite { persist_prefix: 700 },
            )],
        );
        // NOTE: install arms SeededSubset, but there are no other unsynced
        // ops, so only the forced in-flight write matters.
        let new = vec![0xEEu8; 1300];
        set_errno(0);
        assert_eq!(v.pwrite(fd, &new, 100), -1, "cut during the write");
        assert_eq!(get_errno(), libc::EIO);
        assert_eq!(SimVfs::cut_count(), 1);

        let img = read_file(&v, "/f").expect("durable dirent");
        assert_eq!(img.len(), 2048);
        assert!(img[..100].iter().all(|&b| b == 0xBB));
        assert!(
            img[100..512].iter().all(|&b| b == 0xEE),
            "surviving prefix reaches exactly the sector boundary"
        );
        assert!(
            img[512..].iter().all(|&b| b == 0xBB),
            "nothing past the sector boundary — never a partial sector"
        );

        // persist_prefix >= len ⇒ the whole write survives (the final
        // partial sector rides out with its body).
        let v = fresh();
        let fd = open_rw_create(&v, "/g");
        dir_fsync(&v, "/");
        assert_eq!(v.pwrite(fd, &vec![0xBBu8; 1024], 0), 1024);
        assert_eq!(v.fsync(fd), 0);
        SeededFaultPlan::install(
            0x1,
            vec![FaultRule::nth_matching(
                OpMatch { kinds: Some(vec![OpKind::PWriteV]), ..OpMatch::default() },
                1,
                FaultDecision::TornWrite { persist_prefix: 9999 },
            )],
        );
        assert_eq!(v.pwrite(fd, &vec![0xEEu8; 700], 100), -1);
        let img = read_file(&v, "/g").expect("durable dirent");
        assert!(img[100..800].iter().all(|&b| b == 0xEE), "full write survived");
    }

    /// Rule 2 (fsyncgate): a failed fsync's dirty epoch is may-be-lost
    /// permanently — a later successful fsync does NOT resurrect it; the
    /// durable image reverts to the last successfully-synced state for
    /// those bytes.
    #[test]
    fn fsyncgate_failed_fsync_never_resurrects() {
        let v = fresh();
        let fd = open_rw_create(&v, "/f");
        dir_fsync(&v, "/");
        assert_eq!(v.pwrite(fd, b"AAAA", 0), 4);
        assert_eq!(v.fsync(fd), 0); // epoch 0 durable

        // Fail the NEXT fsync with EIO.
        SimVfs::set_fault_plan(Box::new(SeededFaultPlan::new(
            0,
            vec![FaultRule::nth_matching(
                OpMatch { kinds: Some(vec![OpKind::Fsync]), ..OpMatch::default() },
                1,
                FaultDecision::Errno(libc::EIO),
            )],
        )));

        assert_eq!(v.pwrite(fd, b"BBBB", 4), 4); // epoch 1 (doomed)
        set_errno(0);
        assert_eq!(v.fsync(fd), -1, "injected fsync failure");
        assert_eq!(get_errno(), libc::EIO);

        assert_eq!(v.pwrite(fd, b"CCCC", 8), 4); // epoch 2
        assert_eq!(v.fsync(fd), 0, "retry succeeds — but must not resurrect epoch 1");

        // The page-cache view still shows everything (the trap!).
        let mut buf = [0u8; 12];
        assert_eq!(v.pread(fd, &mut buf, 0), 12);
        assert_eq!(&buf, b"AAAABBBBCCCC", "volatile view hides the loss");

        SimVfs::cut();

        // Post-crash: epoch 0 and epoch 2 are durable; epoch 1 is GONE even
        // though a later fsync succeeded — the fsyncgate distinction.
        let img = read_file(&v, "/f").expect("durable dirent");
        assert_eq!(img.len(), 12);
        assert_eq!(&img[0..4], b"AAAA");
        assert_eq!(&img[4..8], &[0u8; 4], "doomed epoch silently gone");
        assert_eq!(&img[8..12], b"CCCC");

        // The log carries the fsyncgate event with its op-seq.
        assert!(
            SimVfs::fault_log().iter().any(|l| l.starts_with("FSYNCGATE ")),
            "fsyncgate transition must be logged: {:?}",
            SimVfs::fault_log()
        );
    }

    /// Rule 3: dirent durability requires the parent-dir fsync — creation
    /// and rename both revert if the parent dir was never fsync'd (the
    /// lost-dirent-after-rename class durable_rename exists to prevent).
    #[test]
    fn dirent_durability_requires_parent_dir_fsync() {
        // --- creation ---
        let v = fresh();
        let fd = open_rw_create(&v, "/a");
        assert_eq!(v.pwrite(fd, b"adata", 0), 5);
        assert_eq!(v.fsync(fd), 0); // content durable, dirent NOT
        assert_eq!(v.close(fd), 0);
        dir_fsync(&v, "/"); // NOW the dirent is durable
        let fd = open_rw_create(&v, "/c");
        assert_eq!(v.pwrite(fd, b"cdata", 0), 5);
        assert_eq!(v.fsync(fd), 0); // content durable, dirent created AFTER the dir fsync
        assert_eq!(v.close(fd), 0);

        SimVfs::cut();

        assert_eq!(read_file(&v, "/a").as_deref(), Some(b"adata".as_slice()));
        assert!(read_file(&v, "/c").is_none(), "un-fsync'd dirent lost at crash");

        // --- rename without the parent-dir fsync: the new name is LOST ---
        let v = fresh();
        let fd = open_rw_create(&v, "/old");
        assert_eq!(v.pwrite(fd, b"payload", 0), 7);
        assert_eq!(v.fsync(fd), 0);
        assert_eq!(v.close(fd), 0);
        dir_fsync(&v, "/");
        assert_eq!(v.rename(&c("/old"), &c("/new")), 0);
        // (no dir fsync — the durable_rename discipline violated)
        SimVfs::cut();
        assert!(read_file(&v, "/new").is_none(), "rename dirent lost at crash");
        assert_eq!(
            read_file(&v, "/old").as_deref(),
            Some(b"payload".as_slice()),
            "the file is still at its OLD name"
        );

        // --- same rename + parent fsync: durable ---
        let v = fresh();
        let fd = open_rw_create(&v, "/old");
        assert_eq!(v.pwrite(fd, b"payload", 0), 7);
        assert_eq!(v.fsync(fd), 0);
        assert_eq!(v.close(fd), 0);
        dir_fsync(&v, "/");
        assert_eq!(v.rename(&c("/old"), &c("/new")), 0);
        dir_fsync(&v, "/");
        SimVfs::cut();
        assert_eq!(read_file(&v, "/new").as_deref(), Some(b"payload".as_slice()));
        assert!(read_file(&v, "/old").is_none());
    }

    /// The seeded-subset crash image is deterministic in the seed and
    /// sensitive to it; KeepAll keeps everything.
    #[test]
    fn seeded_subset_cut_is_deterministic_and_seed_sensitive() {
        fn run(seed: u64) -> (Vec<(PathBuf, Option<(Vec<u8>, Vec<u8>)>)>, Vec<String>) {
            let v = fresh();
            let fd = open_rw_create(&v, "/f");
            dir_fsync(&v, "/");
            assert_eq!(v.pwrite(fd, &[0x11u8; 1024], 0), 1024);
            assert_eq!(v.fsync(fd), 0);
            // 8 unsynced writes
            for i in 0..8u8 {
                let val = 0x20 + i;
                assert_eq!(v.pwrite(fd, &vec![val; 96], (i as off_t) * 128), 96);
            }
            SimVfs::set_crash_image(CrashImage::SeededSubset(seed));
            SimVfs::cut();
            (v.image_dump(), SimVfs::fault_log())
        }

        let (img_a1, log_a1) = run(0xA11CE);
        let (img_a2, log_a2) = run(0xA11CE);
        assert_eq!(img_a1, img_a2, "same seed ⇒ byte-identical post-crash image");
        assert_eq!(log_a1, log_a2, "same seed ⇒ byte-identical fault log");

        let (img_b, _) = run(0xB0B);
        assert_ne!(img_a1, img_b, "different seed ⇒ different surviving subset");

        // KeepAll: the kindest legal disk — everything unsynced survives.
        let v = fresh();
        let fd = open_rw_create(&v, "/f");
        dir_fsync(&v, "/");
        assert_eq!(v.pwrite(fd, &[0x11u8; 64], 0), 64);
        assert_eq!(v.fsync(fd), 0);
        assert_eq!(v.pwrite(fd, &[0x22u8; 64], 64), 64);
        SimVfs::set_crash_image(CrashImage::KeepAll);
        SimVfs::cut();
        let img = read_file(&v, "/f").unwrap();
        assert_eq!(img.len(), 128);
        assert!(img[64..].iter().all(|&b| b == 0x22));
    }

    /// Fail-by-path-class + fail-the-Nth-matching-op: the engine rules fire
    /// on fd-addressed data-plane ops through the resolved open path.
    #[test]
    fn path_class_and_nth_matching_rules() {
        // classifier vocabulary
        assert_eq!(classify_path(Path::new("/data/pg_wal/000000010000000000000001")), PathClass::Wal);
        assert_eq!(classify_path(Path::new("/data/pg_control")), PathClass::Config);
        assert_eq!(classify_path(Path::new("/data/pg_control.tmp")), PathClass::Config);
        assert_eq!(classify_path(Path::new("/data/postgresql.conf")), PathClass::Config);
        assert_eq!(classify_path(Path::new("/data/base/pgsql_tmp/pgsql_tmp1.0")), PathClass::Temp);
        assert_eq!(classify_path(Path::new("/data/base/5/16384")), PathClass::Heap);
        assert_eq!(classify_path(Path::new("/data/global/1213")), PathClass::Heap);
        assert_eq!(classify_path(Path::new("/somewhere/else")), PathClass::Other);

        let v = fresh();
        assert_eq!(v.mkdir(&c("/data"), 0o700 as mode_t), 0);
        assert_eq!(v.mkdir(&c("/data/pg_wal"), 0o700 as mode_t), 0);
        assert_eq!(v.mkdir(&c("/data/base"), 0o700 as mode_t), 0);
        let wal = open_rw_create(&v, "/data/pg_wal/wal");
        let heap = open_rw_create(&v, "/data/base/heap");

        // The 2nd WAL-class write fails ENOSPC. Heap writes never match.
        SimVfs::set_fault_plan(Box::new(SeededFaultPlan::new(
            0,
            vec![FaultRule::nth_matching(
                OpMatch {
                    kinds: Some(vec![OpKind::PWriteV]),
                    class: Some(PathClass::Wal),
                    path_contains: None,
                },
                2,
                FaultDecision::Errno(libc::ENOSPC),
            )],
        )));

        assert_eq!(v.pwrite(heap, b"h1", 0), 2);
        assert_eq!(v.pwrite(wal, b"w1", 0), 2, "1st wal write proceeds");
        assert_eq!(v.pwrite(heap, b"h2", 2), 2);
        set_errno(0);
        assert_eq!(v.pwrite(wal, b"w2", 2), -1, "2nd wal write fails");
        assert_eq!(get_errno(), libc::ENOSPC);
        assert_eq!(v.pwrite(wal, b"w3", 2), 2, "rule fired once, disarmed");

        let log = SimVfs::fault_log();
        assert_eq!(log.len(), 1);
        assert!(
            log[0].contains("pg_wal") && log[0].contains("PWriteV"),
            "log line carries the resolved path + op: {}",
            log[0]
        );
        assert_eq!(v.close(wal), 0);
        assert_eq!(v.close(heap), 0);
    }

    /// Same plan + same seed run twice ⇒ byte-identical fault logs and
    /// images (the replay-identity gate, model level).
    #[test]
    fn fault_log_replay_byte_identical() {
        fn faulted_run() -> (Vec<String>, Vec<(PathBuf, Option<(Vec<u8>, Vec<u8>)>)>) {
            let v = fresh();
            SeededFaultPlan::install(
                0xD57,
                vec![
                    FaultRule::nth_matching(
                        OpMatch { kinds: Some(vec![OpKind::Fsync]), ..OpMatch::default() },
                        2,
                        FaultDecision::Errno(libc::EIO),
                    ),
                    FaultRule::crash_at_op(24),
                ],
            );
            let fd = open_rw_create(&v, "/f");
            dir_fsync(&v, "/");
            let mut i: u64 = 0;
            loop {
                // keep issuing ops until the planned crash fires
                let r = v.pwrite(fd, &i.to_le_bytes(), (i * 8) as off_t);
                if r < 0 && SimVfs::cut_count() > 0 {
                    break;
                }
                let _ = v.fsync(fd);
                if SimVfs::cut_count() > 0 {
                    break;
                }
                i += 1;
                assert!(i < 1000, "planned crash never fired");
            }
            (SimVfs::fault_log(), v.image_dump())
        }

        let (log1, img1) = faulted_run();
        let (log2, img2) = faulted_run();
        assert!(!log1.is_empty());
        assert_eq!(log1, log2, "fault logs must be byte-identical across replay");
        assert_eq!(img1, img2, "post-crash images must be byte-identical across replay");
    }

    /// RED BATTERY (model level): the test-only atomic-multi-sector mode
    /// masks exactly the tear the 512 B floor catches — proving the floor
    /// is load-bearing. If the two arms ever agree, the model lost its
    /// teeth and this gate fails.
    #[test]
    fn red_atomic_multisector_mode_masks_the_tear() {
        fn torn_run(atomic: bool) -> Vec<u8> {
            let v = fresh();
            let fd = open_rw_create(&v, "/f");
            dir_fsync(&v, "/");
            assert_eq!(v.pwrite(fd, &[0xBBu8; 2048], 0), 2048);
            assert_eq!(v.fsync(fd), 0);
            SimVfs::set_atomic_write_mode(atomic);
            SeededFaultPlan::install(
                0x2,
                vec![FaultRule::nth_matching(
                    OpMatch { kinds: Some(vec![OpKind::PWriteV]), ..OpMatch::default() },
                    1,
                    FaultDecision::TornWrite { persist_prefix: 700 },
                )],
            );
            assert_eq!(v.pwrite(fd, &[0xEEu8; 1300], 100), -1);
            read_file(&v, "/f").expect("durable dirent")
        }

        let floored = torn_run(false);
        let weakened = torn_run(true);

        // Weakened arm: the FULL 1300-byte write survived — multi-sector
        // atomicity that no disk guarantees. The tear is masked.
        assert!(weakened[100..1400].iter().all(|&b| b == 0xEE), "weakened arm masks the tear");
        // Floor arm: the tear is real — data stops at the sector boundary.
        assert!(floored[100..512].iter().all(|&b| b == 0xEE));
        assert!(floored[512..].iter().all(|&b| b == 0xBB), "floor arm catches the tear");
        assert_ne!(floored, weakened, "if these agree the model has no teeth");
    }

    /// Review N2: a DOOMED fsync epoch routes through the CrashImage policy —
    /// an arbitrary (seeded) subset of the failed epoch persisted before the
    /// error; the rest is gone for good and no later fsync resurrects it.
    #[test]
    fn doomed_epoch_routes_through_crash_image_policy() {
        // Base image: 4 sectors of 0xAA, durable; then a doomed epoch of 4
        // sector-aligned writes 0xB0+i; fail the fsync; inspect durable via
        // a cut under KeepAll of the (empty) post-failure set.
        fn run(policy: CrashImage) -> (Vec<u8>, Vec<u8>) {
            let v = fresh();
            let fd = open_rw_create(&v, "/f");
            dir_fsync(&v, "/");
            assert_eq!(v.pwrite(fd, &[0xAAu8; 2048], 0), 2048);
            assert_eq!(v.fsync(fd), 0);
            SimVfs::set_crash_image(policy);
            SimVfs::set_fault_plan(Box::new(SeededFaultPlan::new(
                0,
                vec![FaultRule::nth_matching(
                    OpMatch { kinds: Some(vec![OpKind::Fsync]), ..OpMatch::default() },
                    1,
                    FaultDecision::Errno(libc::EIO),
                )],
            )));
            for i in 0..4u8 {
                assert_eq!(v.pwrite(fd, &vec![0xB0 + i; 512], (i as off_t) * 512), 512);
            }
            set_errno(0);
            assert_eq!(v.fsync(fd), -1, "injected fsync failure");
            assert_eq!(get_errno(), libc::EIO);
            let volatile = read_file(&v, "/f").unwrap();
            // Everything unsynced is now gone from the journal; a cut under
            // ANY policy exposes exactly the durable image.
            SimVfs::cut();
            let durable = read_file(&v, "/f").unwrap();
            (volatile, durable)
        }

        // DropAll (the adversarial floor, = inc-1 behavior): epoch fully lost.
        let (vol, dur) = run(CrashImage::DropAll);
        assert_eq!(vol.len(), 2048);
        assert!(vol.iter().all(|&b| b >= 0xB0), "volatile hides the loss");
        assert!(dur.iter().all(|&b| b == 0xAA), "DropAll: doomed epoch fully lost");

        // KeepAll (kindest legal disk): the whole epoch made it out before
        // the error — durable shows all of it.
        let (_, dur) = run(CrashImage::KeepAll);
        assert!(dur.iter().all(|&b| b >= 0xB0), "KeepAll: doomed epoch fully persisted");

        // SeededSubset: deterministically scan for a seed whose doomed-epoch
        // draw keeps a PROPER nonempty subset — the N2 semantics proper.
        let mut found = None;
        for seed in 0..64u64 {
            let (_, dur) = run(CrashImage::SeededSubset(seed));
            let kept_blocks: Vec<bool> = (0..4)
                .map(|i| dur[i * 512..(i + 1) * 512].iter().all(|&b| b == 0xB0 + i as u8))
                .collect();
            let dropped_blocks: Vec<bool> = (0..4)
                .map(|i| dur[i * 512..(i + 1) * 512].iter().all(|&b| b == 0xAA))
                .collect();
            // every sector is wholly kept or wholly dropped (aligned
            // sector-sized writes cannot half-survive)
            for i in 0..4 {
                assert!(
                    kept_blocks[i] || dropped_blocks[i],
                    "seed {seed}: sector {i} neither kept nor dropped"
                );
            }
            let kept = kept_blocks.iter().filter(|&&k| k).count();
            if kept > 0 && kept < 4 {
                found = Some(seed);
                break;
            }
        }
        let seed = found.expect("a proper-subset seed exists in the first 64");

        // Same seed twice ⇒ identical durable images (determinism).
        let (_, d1) = run(CrashImage::SeededSubset(seed));
        let (_, d2) = run(CrashImage::SeededSubset(seed));
        assert_eq!(d1, d2, "doomed-epoch subset must be deterministic in the seed");

        // No resurrection: after the failure, a NEW write + successful fsync
        // promotes only the new epoch; dropped doomed ops stay lost.
        let v = fresh();
        let fd = open_rw_create(&v, "/f");
        dir_fsync(&v, "/");
        assert_eq!(v.pwrite(fd, &[0xAAu8; 2048], 0), 2048);
        assert_eq!(v.fsync(fd), 0);
        SimVfs::set_crash_image(CrashImage::SeededSubset(seed));
        SimVfs::set_fault_plan(Box::new(SeededFaultPlan::new(
            0,
            vec![FaultRule::nth_matching(
                OpMatch { kinds: Some(vec![OpKind::Fsync]), ..OpMatch::default() },
                1,
                FaultDecision::Errno(libc::EIO),
            )],
        )));
        for i in 0..4u8 {
            assert_eq!(v.pwrite(fd, &vec![0xB0 + i; 512], (i as off_t) * 512), 512);
        }
        assert_eq!(v.fsync(fd), -1);
        let after_fail = {
            let mut probe = Vec::new();
            for (p, img) in v.image_dump() {
                if p == Path::new("/f") {
                    probe = img.unwrap().1; // the durable image
                }
            }
            probe
        };
        assert_eq!(v.pwrite(fd, &[0xCCu8; 512], 2048), 512);
        assert_eq!(v.fsync(fd), 0, "post-failure epoch promotes fine");
        let mut expected = after_fail.clone();
        expected.resize(2560, 0);
        expected[2048..2560].fill(0xCC);
        for (p, img) in v.image_dump() {
            if p == Path::new("/f") {
                assert_eq!(
                    img.unwrap().1,
                    expected,
                    "later fsync promotes ONLY the new epoch — no resurrection"
                );
            }
        }
        assert!(
            SimVfs::fault_log().iter().any(|l| l.starts_with("FSYNCGATE ")
                && l.contains("policy=SeededSubset")),
            "the doomed-epoch draw is logged with its policy: {:?}",
            SimVfs::fault_log()
        );
    }

    /// Review N4: fd-addressed ops resolve their path AT OP TIME — a file
    /// renamed into pg_wal while open (the WAL-recycle shape) is Wal-class
    /// from the next op on; an unlinked-but-open fd keeps its open path.
    #[test]
    fn renamed_while_open_fd_reclassifies_at_op_time() {
        let v = fresh();
        assert_eq!(v.mkdir(&c("/data"), 0o700 as mode_t), 0);
        assert_eq!(v.mkdir(&c("/data/pg_wal"), 0o700 as mode_t), 0);
        let fd = open_rw_create(&v, "/data/recycle.tmp");

        // Fail the FIRST Wal-class write.
        SimVfs::set_fault_plan(Box::new(SeededFaultPlan::new(
            0,
            vec![FaultRule::nth_matching(
                OpMatch {
                    kinds: Some(vec![OpKind::PWriteV]),
                    class: Some(PathClass::Wal),
                    path_contains: None,
                },
                1,
                FaultDecision::Errno(libc::EIO),
            )],
        )));

        // Pre-rename the fd is Temp-class: the Wal rule must not match.
        assert_eq!(v.pwrite(fd, b"tmp", 0), 3, "Temp-class write proceeds");
        // The recycle: rename INTO pg_wal while the fd stays open.
        assert_eq!(
            v.rename(&c("/data/recycle.tmp"), &c("/data/pg_wal/000000010000000000000042")),
            0
        );
        set_errno(0);
        assert_eq!(v.pwrite(fd, b"wal", 0), -1, "post-rename the SAME fd is Wal-class");
        assert_eq!(get_errno(), libc::EIO);
        let log = SimVfs::fault_log();
        assert!(
            log.iter().any(|l| l.contains("pg_wal/000000010000000000000042")),
            "the log carries the CURRENT (renamed) path: {log:?}"
        );

        // Unlinked-but-open: falls back to the open-time path (class keeps).
        let v = fresh();
        let fd = open_rw_create(&v, "/data.tmp");
        SimVfs::set_fault_plan(Box::new(SeededFaultPlan::new(
            0,
            vec![FaultRule::nth_matching(
                OpMatch {
                    kinds: Some(vec![OpKind::PWriteV]),
                    class: Some(PathClass::Temp),
                    path_contains: None,
                },
                1,
                FaultDecision::Errno(libc::ENOSPC),
            )],
        )));
        assert_eq!(v.unlink(&c("/data.tmp")), 0);
        set_errno(0);
        assert_eq!(v.pwrite(fd, b"x", 0), -1, "unlinked fd keeps its open-time class");
        assert_eq!(get_errno(), libc::ENOSPC);
        assert_eq!(v.close(fd), 0);
    }

    /// inc-3 WHOLE-NODE KILL: without it, a writer that keeps executing past
    /// its crash point can REPAIR the post-crash image (the inc-2 review
    /// exposure — arm 1 demonstrates it, deliberately); with it armed, every
    /// post-cut op is refused and mutates NOTHING until revive(). If arm 1
    /// ever stops repairing, the exposure this primitive closes is gone and
    /// the red lost its teeth.
    #[test]
    fn whole_node_kill_freezes_all_mutation_at_cut() {
        fn setup(kill: bool) -> (SimVfs, c_int) {
            let v = fresh();
            let fd = open_rw_create(&v, "/f");
            dir_fsync(&v, "/");
            assert_eq!(v.pwrite(fd, &[0xAAu8; 512], 0), 512);
            assert_eq!(v.fsync(fd), 0);
            SimVfs::set_kill_on_cut(kill);
            SeededFaultPlan::install(
                0x7,
                vec![FaultRule::nth_matching(
                    OpMatch { kinds: Some(vec![OpKind::PWriteV]), ..OpMatch::default() },
                    1,
                    FaultDecision::Crash,
                )],
            );
            set_errno(0);
            assert_eq!(v.pwrite(fd, &[0xBBu8; 512], 0), -1, "the cut");
            assert_eq!(get_errno(), libc::EIO);
            assert_eq!(SimVfs::cut_count(), 1);
            (v, fd)
        }

        // Arm 1 — kill OFF (the exposure): "unwind residue" reopens the
        // file and repairs the image; the pack would see the repair.
        let (v, _) = setup(false);
        assert!(!SimVfs::killed());
        let fd2 = v.open(&c("/f"), libc::O_RDWR, 0 as mode_t);
        assert!(fd2 >= SIM_FD_BASE);
        assert_eq!(v.pwrite(fd2, &[0xCCu8; 512], 0), 512);
        assert_eq!(v.fsync(fd2), 0);
        let img = read_file(&v, "/f").unwrap();
        assert!(
            img.iter().all(|&b| b == 0xCC),
            "kill OFF: post-cut ops repaired the image — the exposure is real"
        );

        // Arm 2 — kill ON: the SAME residue is refused; nothing mutates.
        let (v, dead_fd) = setup(true);
        assert!(SimVfs::killed());
        set_errno(0);
        assert_eq!(v.open(&c("/f"), libc::O_RDWR, 0 as mode_t), -1, "dead node refuses open");
        assert_eq!(get_errno(), libc::EIO);
        assert_eq!(v.pwrite(dead_fd, &[0xCCu8; 512], 0), -1, "dead node refuses writes");
        assert_eq!(v.fsync(dead_fd), -1, "dead node refuses fsync");
        assert_eq!(v.unlink(&c("/f")), -1, "dead node refuses unlink");
        assert_eq!(v.rename(&c("/f"), &c("/g")), -1, "dead node refuses rename");
        let mut info = FileInfo::zeroed();
        assert_eq!(v.stat(&c("/f"), &mut info), -1, "dead node refuses stat");
        assert!(SimVfs::frozen_op_count() >= 6, "refusals counted");
        let log = SimVfs::fault_log();
        assert!(log.iter().any(|l| l.starts_with("KILL#1 ")), "kill logged: {log:?}");
        assert!(log.iter().any(|l| l.starts_with("KILLED ")), "first refusal logged");
        // Guard drops on the unwind path are tolerated (and refused).
        let _ = SimVfs::close_on_drop(dead_fd);
        // Revive = the recovery boot on the SAME disk: exactly the at-cut
        // image, untouched by the frozen residue.
        SimVfs::revive();
        assert!(!SimVfs::killed());
        let img = read_file(&v, "/f").unwrap();
        assert_eq!(img.len(), 512);
        assert!(
            img.iter().all(|&b| b == 0xAA),
            "kill ON: nothing after the cut mutated the disk"
        );
    }

    /// inc-3 DIR-FSYNC HARD MODE: a FAILED parent-dir fsync dooms the
    /// pending dirent epoch through the CrashImage policy. Under the
    /// DropAll floor the rename is durably lost even though a RETRY fsync
    /// "succeeds" (the dir-plane fsyncgate believer catch), and later
    /// namespace ops promote fine without resurrecting the dropped dirents.
    /// Under KeepAll the epoch persisted before the error.
    #[test]
    fn dir_fsyncgate_failed_parent_fsync_dooms_dirent_epoch() {
        fn rename_with_failed_parent_fsync(policy: CrashImage) -> SimVfs {
            let v = fresh();
            let fd = open_rw_create(&v, "/old");
            assert_eq!(v.pwrite(fd, b"payload", 0), 7);
            assert_eq!(v.fsync(fd), 0);
            assert_eq!(v.close(fd), 0);
            dir_fsync(&v, "/");
            assert_eq!(v.rename(&c("/old"), &c("/new")), 0);
            SimVfs::set_crash_image(policy);
            SimVfs::set_fault_plan(Box::new(SeededFaultPlan::new(
                0,
                vec![FaultRule::nth_matching(
                    OpMatch { kinds: Some(vec![OpKind::Fsync]), ..OpMatch::default() },
                    1,
                    FaultDecision::Errno(libc::EIO),
                )],
            )));
            let dfd = v.open(&c("/"), libc::O_RDONLY, 0 as mode_t);
            assert!(dfd >= SIM_FD_BASE);
            set_errno(0);
            assert_eq!(v.fsync(dfd), -1, "injected parent-dir fsync failure");
            assert_eq!(get_errno(), libc::EIO);
            assert_eq!(v.fsync(dfd), 0, "the believer's retry 'succeeds'");
            assert_eq!(v.close(dfd), 0);
            v
        }

        // Believer arm (DropAll): the rename epoch is durably LOST despite
        // the successful retry; post-failure ops still promote fine.
        let v = rename_with_failed_parent_fsync(CrashImage::DropAll);
        let fd = open_rw_create(&v, "/late");
        assert_eq!(v.pwrite(fd, b"late", 0), 4);
        assert_eq!(v.fsync(fd), 0);
        assert_eq!(v.close(fd), 0);
        dir_fsync(&v, "/");
        SimVfs::cut();
        assert!(
            read_file(&v, "/new").is_none(),
            "hard mode: the rename is durably lost despite the successful retry"
        );
        assert_eq!(
            read_file(&v, "/old").as_deref(),
            Some(b"payload".as_slice()),
            "the old dirent stands"
        );
        assert_eq!(
            read_file(&v, "/late").as_deref(),
            Some(b"late".as_slice()),
            "post-failure epoch promotes fine — no wholesale resurrection"
        );
        assert!(
            SimVfs::fault_log()
                .iter()
                .any(|l| l.starts_with("FSYNCGATE ") && l.contains("plane=dir")),
            "the doomed dirent epoch is logged: {:?}",
            SimVfs::fault_log()
        );

        // Kindest-disk arm (KeepAll): the epoch reached the platter before
        // the error — the rename IS durable.
        let v = rename_with_failed_parent_fsync(CrashImage::KeepAll);
        SimVfs::cut();
        assert_eq!(read_file(&v, "/new").as_deref(), Some(b"payload".as_slice()));
        assert!(read_file(&v, "/old").is_none());

        // SeededSubset: deterministic in the seed; some seed keeps a PROPER
        // nonempty subset of 4 independent dirent creations.
        fn subset_run(seed: u64) -> Vec<bool> {
            let v = fresh();
            dir_fsync(&v, "/");
            for i in 0..4u8 {
                let fd = open_rw_create(&v, &format!("/s{i}"));
                assert_eq!(v.pwrite(fd, b"x", 0), 1);
                assert_eq!(v.fsync(fd), 0);
                assert_eq!(v.close(fd), 0);
            }
            SimVfs::set_crash_image(CrashImage::SeededSubset(seed));
            SimVfs::set_fault_plan(Box::new(SeededFaultPlan::new(
                0,
                vec![FaultRule::nth_matching(
                    OpMatch { kinds: Some(vec![OpKind::Fsync]), ..OpMatch::default() },
                    1,
                    FaultDecision::Errno(libc::EIO),
                )],
            )));
            let dfd = v.open(&c("/"), libc::O_RDONLY, 0 as mode_t);
            assert_eq!(v.fsync(dfd), -1);
            assert_eq!(v.close(dfd), 0);
            // Post-failure set is empty; the cut exposes the durable image.
            SimVfs::set_crash_image(CrashImage::DropAll);
            SimVfs::cut();
            (0..4).map(|i| read_file(&v, &format!("/s{i}")).is_some()).collect()
        }
        let mut proper = None;
        for seed in 0..64u64 {
            let kept = subset_run(seed);
            let n = kept.iter().filter(|&&k| k).count();
            if n > 0 && n < 4 {
                proper = Some(seed);
                break;
            }
        }
        let seed = proper.expect("a proper-subset seed exists in the first 64");
        assert_eq!(subset_run(seed), subset_run(seed), "deterministic in the seed");
    }

    /// inc-3: fsync is the only BARRIER on the namespace plane too — on a
    /// kind disk (KeepAll) un-fsync'd dirents may survive the cut; the
    /// DropAll floor still loses them (rule-3 guarantee unchanged, see
    /// dirent_durability_requires_parent_dir_fsync).
    #[test]
    fn unsynced_dirents_survive_cut_per_policy() {
        let v = fresh();
        let fd = open_rw_create(&v, "/a");
        assert_eq!(v.pwrite(fd, b"x", 0), 1);
        assert_eq!(v.fsync(fd), 0);
        assert_eq!(v.close(fd), 0);
        // No dir fsync at all.
        SimVfs::set_crash_image(CrashImage::KeepAll);
        SimVfs::cut();
        assert_eq!(
            read_file(&v, "/a").as_deref(),
            Some(b"x".as_slice()),
            "kindest legal disk persisted the dirent without the parent fsync"
        );
        assert!(
            SimVfs::fault_log().iter().any(|l| l.contains(" dir=") && l.contains("kept=1")),
            "the dir-plane cut draw is logged: {:?}",
            SimVfs::fault_log()
        );
    }

    /// Review N5: when two rules fire on one op, the loser's consumed nth is
    /// LOGGED (a SUPPRESSED note), not silently eaten.
    #[test]
    fn suppressed_rule_firing_is_logged() {
        let v = fresh();
        SimVfs::set_fault_plan(Box::new(SeededFaultPlan::new(
            0,
            vec![
                FaultRule::nth_matching(OpMatch::any(), 2, FaultDecision::Errno(libc::EIO)),
                FaultRule::nth_matching(OpMatch::any(), 2, FaultDecision::Errno(libc::ENOSPC)),
            ],
        )));
        let fd = open_rw_create(&v, "/f"); // op 1: both count, neither fires
        set_errno(0);
        assert_eq!(v.pwrite(fd, b"x", 0), -1, "op 2: rule#0 wins");
        assert_eq!(get_errno(), libc::EIO, "first rule in order wins");
        // rule#1's nth firing was consumed on op 2 — it must NOT fire later.
        assert_eq!(v.pwrite(fd, b"y", 0), 1, "op 3 proceeds: loser's nth was consumed");
        let log = SimVfs::fault_log();
        assert!(
            log.iter()
                .any(|l| l.contains("SUPPRESSED rule#1")
                    && l.contains(&format!("Errno({})", libc::ENOSPC))),
            "the consumed firing must be visible in the log: {log:?}"
        );
        assert_eq!(v.close(fd), 0);
    }
}
