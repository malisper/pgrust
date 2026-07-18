//! SimVfs — deterministic in-memory filesystem for DST (P1 skeleton).
//!
//! Owner: WS-C (contract §4, §5.1). Compiled only under `--cfg pgrust_sim`
//! (the sim harness selector); never present in product builds. Run the sim
//! battery with `RUSTFLAGS='--cfg pgrust_sim' cargo test -p vfs sim::`.
//!
//! Shape (frozen surface, lib.rs): SimVfs is a ZST with `pub const fn new()`
//! — `ACTIVE` is a const — and ALL state lives in a thread-local. The sim
//! harness runs the whole simulated process single-threaded; a thread is a
//! universe. [`SimVfs::reset`] tears the universe down (fresh disk, fd
//! counter back to base, NoFaults plan) for back-to-back runs on one thread.
//!
//! DETERMINISM RULES (binding, contract §4.1):
//! - BTree ordering ONLY — no HashMap/HashSet anywhere in this module.
//! - No wall clock: `FileInfo::mtime_*` is always 0 in P1 (a logical clock,
//!   if ever needed, comes from the harness in P4).
//! - Monotonic fd assignment from a high base ([`SIM_FD_BASE`]) — small-int
//!   posix fds mixed into sim traffic fault loudly as EBADF, catching raw-fd
//!   domain mixups (the FileGetRawDesc carve-out).
//! - All randomness comes from the harness seed; SimVfs itself contains zero
//!   entropy sources.
//!
//! TWO-IMAGE DURABILITY (P1 trivial impl, contract §4.3): writes land in the
//! `volatile` image and record their dirty range; fsync/fdatasync promote the
//! whole volatile image to `durable`; [`SimVfs::crash`] rolls every file back
//! to its durable image and drops all open fds. Byte-granular torn-write
//! persistence, namespace-op durability (create/rename/unlink vs dir fsync),
//! seeded scheduling, crash-point enumeration and EMFILE budget injection are
//! ALL P4 — but every op already consults the [`FaultPlan`], so P4 adds no
//! new plumbing through fd.
//!
//! Namespace model: rooted at "/". Relative paths resolve against the root
//! (the sim harness addresses data dirs absolutely; there is no cwd). Entry
//! names must be UTF-8 (EINVAL otherwise) per the frozen `BTreeSet<String>` /
//! `VfsDirIter::from_names` representation. No symlinks in P1: `lstat` ≡
//! `stat`, `read_link` fails EINVAL like readlink(2) on a non-symlink. Where
//! platforms disagree on an errno, SimVfs speaks the Linux dialect (e.g.
//! EISDIR for unlink-of-directory).

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::CStr;
use std::ops::Range;
use std::path::{Component, Path, PathBuf};

use crate::{c_int, mode_t, off_t, set_errno, FileInfo, Vfs, VfsDirIter, VfsResult, PG_O_DIRECT};

/// Sim fds start here. Anything below this base is a raw posix fd and gets
/// EBADF — sim-scope callers must never route raw fds through the trait.
pub const SIM_FD_BASE: c_int = 1_000_000;

/// Fixed pinned fd budget returned by `fd_budget_probe` (frozen surface:
/// "SimVfs: fixed pinned budget, no real fds touched"). Near PG's
/// max_files_per_process default so fd's `set_max_safe_fds` arithmetic
/// exercises realistic values.
pub const SIM_FD_BUDGET: usize = 960;

// ===========================================================================
// Fault-model INTERFACE (stubs only — machinery is P4; contract §4.3)
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
#[derive(Debug, Clone)]
pub struct OpDesc<'a> {
    pub kind: OpKind,
    pub path: Option<&'a Path>,
    pub fd: Option<c_int>,
    pub offset: Option<off_t>,
    pub len: Option<usize>,
}

/// What the fault plan wants done. P1 only ever produces `Proceed` (via
/// [`NoFaults`]); the other arms are the frozen P4 interface. P4 injection is
/// restricted to fd's `errcode_for_file_access` errno vocabulary (contract
/// §1.1): ENOENT, EEXIST, ENOSPC/EDQUOT, EMFILE/ENFILE, EACCES/EPERM, EIO.
/// SimVfs never emits EINTR (ops are single-shot; retry policy lives in fd).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultDecision {
    Proceed,
    Errno(i32),
    ShortRead(usize),
    ShortWrite(usize),
    TornWrite { persist_prefix: usize },
    Crash,
}

/// Consulted before every op. Mutable so P4 plans can count/schedule.
pub trait FaultPlan {
    fn before_op(&mut self, op: &OpDesc<'_>) -> FaultDecision;
}

/// The P1 plan: always proceed.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoFaults;

impl FaultPlan for NoFaults {
    fn before_op(&mut self, _op: &OpDesc<'_>) -> FaultDecision {
        FaultDecision::Proceed
    }
}

// ===========================================================================
// In-memory tree
// ===========================================================================

type NodeId = usize;

/// A regular file: two-image store + dirty ranges (contract §4.1 shape).
#[derive(Debug, Default)]
struct SimFile {
    volatile: Vec<u8>,
    durable: Vec<u8>,
    dirty: Vec<Range<usize>>,
    /// Permission bits only (type bits synthesized in stat).
    mode: u32,
    nlink: u32,
    /// PG_O_DIRECT was requested on some open of this file (recorded per
    /// contract §4.2; read by P4 direct-IO faulting, write-only today).
    #[allow(dead_code)]
    o_direct_seen: bool,
}

#[derive(Debug, Default)]
struct SimDir {
    /// Deterministic readdir: BTree order (contract §4.1).
    entries: BTreeSet<String>,
    mode: u32,
}

#[derive(Debug)]
enum Node {
    File(SimFile),
    Dir(SimDir),
    /// Arena slot whose file reached nlink == 0 with no open handles.
    Free,
}

#[derive(Debug)]
struct NodeSlot {
    node: Node,
    open_count: u32,
}

#[derive(Debug, Clone)]
struct OpenFile {
    node: NodeId,
    /// Open flags as given (recorded for P4 access-mode faulting; sim does
    /// not enforce access modes on the data plane in P1).
    #[allow(dead_code)]
    flags: c_int,
}

struct SimState {
    nodes: Vec<NodeSlot>,
    /// Absolute normalized path → node. Includes "/" and every dir.
    namespace: BTreeMap<PathBuf, NodeId>,
    open: BTreeMap<c_int, OpenFile>,
    next_fd: c_int,
    /// Every fd `open` has handed out, in order (replay invariant).
    fd_trace: Vec<c_int>,
    plan: Box<dyn FaultPlan>,
}

impl SimState {
    fn fresh() -> Self {
        let root = NodeSlot {
            node: Node::Dir(SimDir { entries: BTreeSet::new(), mode: 0o700 }),
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
        }
    }
}

thread_local! {
    static SIM: RefCell<SimState> = RefCell::new(SimState::fresh());
}

fn with<R>(f: impl FnOnce(&mut SimState) -> R) -> R {
    SIM.with(|cell| f(&mut cell.borrow_mut()))
}

/// The deterministic simulated filesystem. ZST; state is thread-local (one
/// simulated universe per harness thread).
pub struct SimVfs;

impl SimVfs {
    pub const fn new() -> Self {
        SimVfs
    }

    /// Harness API: tear down the current thread's simulated disk entirely —
    /// empty tree, fd counter back to [`SIM_FD_BASE`], empty fd trace,
    /// [`NoFaults`] plan. NOT a crash (crash keeps durable state).
    pub fn reset() {
        SIM.with(|cell| *cell.borrow_mut() = SimState::fresh());
    }

    /// Harness API: install a fault plan (P4 machinery arrives later; the
    /// plumbing is live today).
    pub fn set_fault_plan(plan: Box<dyn FaultPlan>) {
        with(|st| st.plan = plan);
    }

    /// Simulated power loss: every file rolls back to its durable image and
    /// all open fds are dropped. Namespace ops are treated as immediately
    /// durable in P1 (modeling them against dir-fsync ordering is P4).
    pub fn crash(&self) {
        with(crash_locked);
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
}

impl Default for SimVfs {
    fn default() -> Self {
        Self::new()
    }
}

fn crash_locked(st: &mut SimState) {
    st.open.clear();
    for slot in &mut st.nodes {
        if let Node::File(f) = &mut slot.node {
            f.volatile = f.durable.clone();
            f.dirty.clear();
        }
        slot.open_count = 0;
    }
    // Files that were unlinked-but-open are gone for good now.
    for slot in &mut st.nodes {
        if let Node::File(f) = &slot.node {
            if f.nlink == 0 {
                slot.node = Node::Free;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// path helpers
// ---------------------------------------------------------------------------

/// Lexically normalize. Relative paths resolve against "/" (no cwd in sim).
fn norm_path(path: &CStr) -> Result<PathBuf, i32> {
    let bytes = path.to_bytes();
    if bytes.is_empty() {
        return Err(libc::ENOENT);
    }
    let s = match std::str::from_utf8(bytes) {
        Ok(s) => s,
        Err(_) => return Err(libc::EINVAL), // String-keyed namespace: UTF-8 only
    };
    let mut out = PathBuf::from("/");
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

    fn maybe_free(&mut self, id: NodeId) {
        let slot = &mut self.nodes[id];
        if slot.open_count == 0 {
            if let Node::File(f) = &slot.node {
                if f.nlink == 0 {
                    slot.node = Node::Free;
                }
            }
        }
    }

    fn consult(&mut self, op: &OpDesc<'_>) -> FaultDecision {
        self.plan.before_op(op)
    }
}

/// Fault gate for non-data-plane ops (data-plane reads/writes additionally
/// understand Short*/Torn*). Returns Some(errno) if the op must fail.
fn gate_simple(st: &mut SimState, op: &OpDesc<'_>) -> Option<i32> {
    match st.consult(op) {
        FaultDecision::Proceed => None,
        FaultDecision::Errno(e) => Some(e),
        FaultDecision::Crash => {
            crash_locked(st);
            Some(libc::EIO)
        }
        // Short/torn decisions are only meaningful on the data plane; a P4
        // plan emitting them elsewhere is a plan bug — proceed loudly.
        FaultDecision::ShortRead(_)
        | FaultDecision::ShortWrite(_)
        | FaultDecision::TornWrite { .. } => {
            debug_assert!(false, "Short/Torn decision on non-data-plane op {:?}", op.kind);
            None
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
                                f.dirty.clear();
                                f.dirty.push(0..0);
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
                            dirty: Vec::new(),
                            mode: mode as u32 & 0o7777,
                            nlink: 1,
                            o_direct_seen: o_direct,
                        }),
                        open_count: 0,
                    });
                    match &mut st.nodes[pid].node {
                        Node::Dir(d) => {
                            d.entries.insert(name);
                        }
                        _ => unreachable!(),
                    }
                    st.namespace.insert(p.clone(), id);
                    id
                }
            };

            st.nodes[node].open_count += 1;
            let fd = st.next_fd;
            st.next_fd += 1;
            st.open.insert(fd, OpenFile { node, flags });
            st.fd_trace.push(fd);
            fd
        })
    }

    fn close(&self, fd: c_int) -> c_int {
        with(|st| {
            if let Some(e) = gate_simple(
                st,
                &OpDesc { kind: OpKind::Close, path: None, fd: Some(fd), offset: None, len: None },
            ) {
                return fail(e);
            }
            let Some(of) = st.open.remove(&fd) else {
                return fail(libc::EBADF);
            };
            st.nodes[of.node].open_count -= 1;
            // FD_DELETE_AT_CLOSE law: unlinked data lives until the LAST close.
            st.maybe_free(of.node);
            // A dir removed while its handle was open frees on last close.
            if st.nodes[of.node].open_count == 0 {
                if matches!(st.nodes[of.node].node, Node::Dir(_))
                    && !st.namespace.values().any(|&id| id == of.node)
                {
                    st.nodes[of.node].node = Node::Free;
                }
            }
            0
        })
    }

    fn preadv(&self, fd: c_int, iov: &[libc::iovec], off: off_t) -> isize {
        with(|st| {
            if off < 0 {
                return fail_isize(libc::EINVAL);
            }
            let want: usize = iov.iter().map(|v| v.iov_len).sum();
            let mut cap = want;
            match st.consult(&OpDesc {
                kind: OpKind::PReadV,
                path: None,
                fd: Some(fd),
                offset: Some(off),
                len: Some(want),
            }) {
                FaultDecision::Proceed => {}
                FaultDecision::Errno(e) => return fail_isize(e),
                FaultDecision::ShortRead(n) => cap = cap.min(n),
                FaultDecision::Crash => {
                    crash_locked(st);
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
            if off < 0 {
                return fail_isize(libc::EINVAL);
            }
            let want: usize = iov.iter().map(|v| v.iov_len).sum();
            let mut cap = want;
            match st.consult(&OpDesc {
                kind: OpKind::PWriteV,
                path: None,
                fd: Some(fd),
                offset: Some(off),
                len: Some(want),
            }) {
                FaultDecision::Proceed => {}
                FaultDecision::Errno(e) => return fail_isize(e),
                FaultDecision::ShortWrite(n) => cap = cap.min(n),
                // P1 stub: a torn write behaves as a short write. Byte-granular
                // torn PERSISTENCE (prefix survives crash, tail does not) is P4.
                FaultDecision::TornWrite { persist_prefix } => cap = cap.min(persist_prefix),
                FaultDecision::Crash => {
                    crash_locked(st);
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
            let f = st.file_mut(node);
            let start = off as usize;
            let mut done = 0usize;
            for v in iov {
                if done == cap {
                    break;
                }
                let n = v.iov_len.min(cap - done);
                let end = start + done + n;
                if f.volatile.len() < end {
                    f.volatile.resize(end, 0);
                }
                // SAFETY: caller contract — iov bases valid for reads of
                // their lengths; destination range was just sized.
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        v.iov_base as *const u8,
                        f.volatile.as_mut_ptr().add(start + done),
                        n,
                    );
                }
                done += n;
            }
            if done > 0 {
                f.dirty.push(start..start + done);
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
        with(|st| {
            if let Some(e) = gate_simple(
                st,
                &OpDesc { kind: OpKind::Fsync, path: None, fd: Some(fd), offset: None, len: None },
            ) {
                return fail(e);
            }
            promote(st, fd)
        })
    }

    fn fdatasync(&self, fd: c_int) -> c_int {
        // P1: identical promotion semantics to fsync (no metadata split yet).
        with(|st| {
            if let Some(e) = gate_simple(
                st,
                &OpDesc {
                    kind: OpKind::Fdatasync,
                    path: None,
                    fd: Some(fd),
                    offset: None,
                    len: None,
                },
            ) {
                return fail(e);
            }
            promote(st, fd)
        })
    }

    fn flush_range(&self, fd: c_int, off: off_t, len: off_t) -> c_int {
        with(|st| {
            if let Some(e) = gate_simple(
                st,
                &OpDesc {
                    kind: OpKind::FlushRange,
                    path: None,
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
            if len < 0 {
                return fail(libc::EINVAL);
            }
            if let Some(e) = gate_simple(
                st,
                &OpDesc {
                    kind: OpKind::Ftruncate,
                    path: None,
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
            f.dirty.push(0..len as usize);
            0
        })
    }

    fn truncate_path(&self, path: &CStr, len: off_t) -> c_int {
        let p = match norm_path(path) {
            Ok(p) => p,
            Err(e) => return fail(e),
        };
        with(|st| {
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
                    f.dirty.push(0..len as usize);
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
            if off < 0 || len <= 0 {
                return libc::EINVAL;
            }
            match st.consult(&OpDesc {
                kind: OpKind::Fallocate,
                path: None,
                fd: Some(fd),
                offset: Some(off),
                len: Some(len as usize),
            }) {
                FaultDecision::Proceed => {}
                FaultDecision::Errno(e) => return e,
                FaultDecision::Crash => {
                    crash_locked(st);
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
                let old = f.volatile.len();
                f.volatile.resize(end, 0); // fallocate-as-zero-extend
                f.dirty.push(old..end);
            }
            0
        })
    }

    fn file_size(&self, fd: c_int) -> off_t {
        with(|st| {
            if let Some(e) = gate_simple(
                st,
                &OpDesc {
                    kind: OpKind::FileSize,
                    path: None,
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
            if let Some(e) = gate_simple(
                st,
                &OpDesc {
                    kind: OpKind::FadviseWillneed,
                    path: None,
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
            if let Some(e) = gate_simple(
                st,
                &OpDesc { kind: OpKind::Fstat, path: None, fd: Some(fd), offset: None, len: None },
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
        // No symlinks in P1 sim: lstat ≡ stat (the plan still sees Lstat).
        let p = match norm_path(path) {
            Ok(p) => p,
            Err(e) => return fail(e),
        };
        with(|st| {
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
                if let Node::Dir(d) = &mut st.nodes[pid].node {
                    d.entries.remove(&name);
                }
            }
            let f = st.file_mut(id);
            f.nlink = f.nlink.saturating_sub(1);
            // Data lives until last close (FD_DELETE_AT_CLOSE temp files).
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
                        if st.nodes[dst].open_count == 0 {
                            st.nodes[dst].node = Node::Free;
                        }
                    }
                    (false, true) => return fail(libc::EISDIR),
                    (true, false) => return fail(libc::ENOTDIR),
                }
            }

            // Move the entry itself.
            st.namespace.remove(&fp);
            if let Ok(fpid) = st.dir_id(&fparent) {
                if let Node::Dir(d) = &mut st.nodes[fpid].node {
                    d.entries.remove(&fname);
                }
            }
            if let Node::Dir(d) = &mut st.nodes[tpid].node {
                d.entries.insert(tname);
            }
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
                    entries: BTreeSet::new(),
                    mode: mode as u32 & 0o7777,
                }),
                open_count: 0,
            });
            if let Node::Dir(d) = &mut st.nodes[pid].node {
                d.entries.insert(name);
            }
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
                if let Node::Dir(d) = &mut st.nodes[pid].node {
                    d.entries.remove(&name);
                }
            }
            if st.nodes[id].open_count == 0 {
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
                Node::Dir(d) => d.entries.iter().cloned().collect(),
                _ => unreachable!(),
            };
            // Deterministic BTree order; "." and ".." never yielded (frozen
            // VfsDirIter semantics — matches fd's AllocateDir exposure).
            Ok(VfsDirIter::from_names(names))
        })
    }

    fn fd_budget_probe(&self, max_to_probe: usize) -> usize {
        with(|st| {
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
            // realism. EMFILE budget INJECTION is P4 and flows through
            // FaultPlan on open, not through here.
            SIM_FD_BUDGET.min(max_to_probe)
        })
    }
}

/// fsync/fdatasync promotion: the whole volatile image becomes durable
/// (trivial P1 model; range-granular promotion is P4).
fn promote(st: &mut SimState, fd: c_int) -> c_int {
    let Some(of) = st.open.get(&fd).cloned() else {
        return fail(libc::EBADF);
    };
    match &mut st.nodes[of.node].node {
        Node::File(f) => {
            f.durable = f.volatile.clone();
            f.dirty.clear();
        }
        Node::Dir(_) => {
            // Dir fsync: namespace ops are already durable in P1 (their
            // crash-ordering model vs dir fsync is P4).
        }
        Node::Free => return fail(libc::EBADF),
    }
    0
}

// mtime (and any future FileInfo fields, e.g. the dev+ino contract revision
// WS-B requested) stay zeroed: no wall clock in sim, ever. When dev/ino land,
// sim should populate ino = NodeId (stable, deterministic) + a fixed dev.
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
// Tests: sim semantics, differential golden (sim vs posix), same-ops replay.
// Run with: RUSTFLAGS='--cfg pgrust_sim' cargo test -p vfs sim::
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
        assert_eq!(v.pwrite(fd, b"SYNCED--", 0), 8);
        assert_eq!(v.fsync(fd), 0);
        assert_eq!(v.pwrite(fd, b"UNSYNCED", 8), 8);
        assert_eq!(v.file_size(fd), 16);

        v.crash();

        // fd is dead after the crash
        set_errno(0);
        assert_eq!(v.fsync(fd), -1);
        assert_eq!(get_errno(), libc::EBADF);

        let fd2 = v.open(&c("/wal"), libc::O_RDONLY, 0 as mode_t);
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
    // The fd::File*-level differential run is DEFERRED until WS-A inc-2
    // lands (fd does not front the trait yet at vfs-trait-v1).
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
    // First same-ops-replay test (contract §4.4c): record the op stream
    // from a seeded scripted run, replay into a fresh SimVfs, assert
    // byte-identical volatile+durable images and identical fd assignment.
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
    // Fault-interface stub: the plan is consulted on every op and its
    // decisions are honored (P1 vocabulary: Proceed/Errno/Short*).
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
}
