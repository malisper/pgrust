//! Large-object drain module (LARGEOBJECT): the SQL-drainable half of the
//! backend/libpq/be-fsstubs.c + backend/storage/large_object/inv_api.c
//! surface — the server-side `lo_*` function family and the pg_largeobject /
//! pg_largeobject_metadata catalogs. Every large object here is created and
//! unlinked inside the group that touches it (fresh, monotonically-unique
//! loids from `LargeObjState`), so no state crosses group boundaries and no
//! object is ever alive across two groups.
//!
//! Surface covered (verbatim REL_18_3 semantics):
//!   - creation: lo_create(loid) and lo_from_bytea(loid, bytea) (both take an
//!     EXPLICIT loid so the result is deterministic); lo_creat(mode) is
//!     exercised only with its server-assigned oid immediately consumed by
//!     lo_unlink (SELECT lo_unlink(lo_creat(-1)) -> 1), never compared.
//!   - bytea path: lo_put(loid, offset, bytea), lo_get(loid),
//!     lo_get(loid, offset, length) fragment.
//!   - descriptor path (inside a BEGIN..COMMIT bracket, so the fd survives —
//!     the first lo_open in a fresh transaction is fd 0 on both engines):
//!     lo_open, lowrite/loread, lo_lseek/lo_lseek64 (SEEK_SET/CUR/END),
//!     lo_tell/lo_tell64, lo_truncate/lo_truncate64, lo_close.
//!   - unlink: lo_unlink(loid).
//!   - chunk boundary: writes straddling the 2 kB LOBLKSIZE page edge
//!     (BLCKSZ/4 = 2048), zero-filled holes, and a total-ordered
//!     pg_largeobject introspection (loid, pageno unique -> byte-exact).
//!   - metadata/ownership: pg_largeobject_metadata projection.
//!   - error arms: nonexistent loid (get/put/unlink/open), bad fd
//!     (read/write), negative seek target, invalid whence, negative-length
//!     fragment, read-from-write-only / write-to-read-only descriptor.
//!
//! lo_import / lo_export are server-file (and superuser-gated) — deliberately
//! SKIPPED; the in-DB bytea path (lo_from_bytea / lo_get) covers the same
//! inv_api read/write drain without touching the server filesystem.
//!
//! Determinism laws (same discipline as crate::heap / crate::spill):
//!   - bytea results always cross the wire as encode(..,'hex') text; no raw
//!     bytea, no float anything (B1).
//!   - loid-scoped introspection carries a TOTAL ORDER BY (loid, pageno) so a
//!     placement divergence is a countable/ordered row difference, never a
//!     row-order one.
//!   - explicit loids live in a high band (LOID_BASE) that the server's own
//!     OID counter cannot reach inside a session, so lo_creat's server-oid
//!     objects can never alias an explicit loid.
//!   - error probes are ISOLATED: a loid-based error is a standalone
//!     statement (its own implicit transaction), a descriptor-based error
//!     lives in a self-contained BEGIN..ROLLBACK so the aborted transaction
//!     never poisons a following statement.

use crate::stmt::{Gen, StmtKind};

/// Explicit-loid band: far above any OID the server allocates inside a
/// session, so lo_creat's server-assigned objects never alias one of ours.
const LOID_BASE: u64 = 2_100_000_000;

/// A loid that is never created — the nonexistent-object error target.
const LOID_NONEXIST: u64 = 1_999_999_999;

/// INV_READ | INV_WRITE (libpq-fs.h: 0x40000 | 0x20000).
const INV_RW: u32 = 0x00060000;
/// INV_WRITE only.
const INV_W: u32 = 0x00020000;
/// INV_READ only.
const INV_R: u32 = 0x00040000;

/// Session-persistent loid allocator (swapped in and out of `Gen` by the
/// session loop exactly like `HeapState`). Only a monotonic counter — large
/// objects never persist across groups, so there is nothing else to carry.
#[derive(Clone, Debug, Default)]
pub struct LargeObjState {
    next_loid: u32,
}

impl LargeObjState {
    pub fn new() -> LargeObjState {
        LargeObjState::default()
    }
}

/// Grab a fresh, never-before-used explicit loid.
fn alloc(g: &mut Gen) -> u64 {
    let n = g.largeobj.next_loid;
    g.largeobj.next_loid += 1;
    LOID_BASE + n as u64
}

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_largeobj_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("largeobj");
    let action = g.weights.pick(
        g.rng,
        &[
            "largeobj:bytea",
            "largeobj:creat",
            "largeobj:putget",
            "largeobj:fd",
            "largeobj:chunk",
            "largeobj:trunc",
            "largeobj:lo64",
            "largeobj:meta",
            "largeobj:err",
        ],
    );
    g.fire(action);
    match action {
        "largeobj:bytea" => gen_bytea(g),
        "largeobj:creat" => gen_creat(g),
        "largeobj:putget" => gen_putget(g),
        "largeobj:fd" => gen_fd(g),
        "largeobj:chunk" => gen_chunk(g),
        "largeobj:trunc" => gen_trunc(g),
        "largeobj:lo64" => gen_lo64(g),
        "largeobj:meta" => gen_meta(g),
        _ => gen_err(g),
    }
}

fn raw(s: String) -> StmtKind {
    StmtKind::Raw(s)
}

/// A deterministic incompressible-ish hex payload of `nbytes` bytes,
/// expressed as a decode(repeat(..)) so the module source stays light.
fn payload(byte: &str, nbytes: usize) -> String {
    format!("decode(repeat('{byte}', {nbytes}), 'hex')")
}

// ---------------------------------------------------------------- bytea ----

/// lo_from_bytea create + lo_get roundtrip + metadata projection + unlink.
fn gen_bytea(g: &mut Gen) -> Vec<StmtKind> {
    let l = alloc(g);
    vec![
        raw(format!(
            "SELECT lo_from_bytea({l}, '\\xde01ad02be03ef04'::bytea);"
        )),
        raw(format!("SELECT encode(lo_get({l}), 'hex');")),
        raw(format!(
            "SELECT oid = {l}, pg_get_userbyid(lomowner) = current_user, \
             lomacl IS NULL FROM pg_largeobject_metadata WHERE oid = {l};"
        )),
        raw(format!("SELECT lo_unlink({l});")),
    ]
}

// ---------------------------------------------------------------- creat ----

/// lo_creat(mode): the server-assigned-oid path. The oid differs per engine,
/// so it is immediately consumed by lo_unlink and never compared — the result
/// is 1 on both sides while inv_create/inv_drop still run.
fn gen_creat(g: &mut Gen) -> Vec<StmtKind> {
    let mode = match g.weights.pick(g.rng, &["largeobj:creat:neg", "largeobj:creat:rw"]) {
        "largeobj:creat:rw" => INV_RW as i64,
        _ => -1,
    };
    vec![raw(format!("SELECT lo_unlink(lo_creat({mode}));"))]
}

// -------------------------------------------------------------- put/get ----

/// lo_create + lo_put at an offset (zero-filled prefix) + full and fragment
/// lo_get + unlink.
fn gen_putget(g: &mut Gen) -> Vec<StmtKind> {
    let l = alloc(g);
    let off = 1 + g.rng.below(20);
    vec![
        raw(format!("SELECT lo_create({l});")),
        raw(format!("SELECT lo_put({l}, {off}, '\\xaabbccdd'::bytea);")),
        raw(format!("SELECT encode(lo_get({l}), 'hex');")),
        raw(format!("SELECT encode(lo_get({l}, {off}, 3), 'hex');")),
        raw(format!("SELECT lo_unlink({l});")),
    ]
}

// ------------------------------------------------------------------- fd ----

/// SEEK_SET / SEEK_CUR / SEEK_END whence pick.
fn pick_whence(g: &mut Gen) -> i32 {
    match g.weights.pick(
        g.rng,
        &["largeobj:whence:set", "largeobj:whence:cur", "largeobj:whence:end"],
    ) {
        "largeobj:whence:cur" => 1,
        "largeobj:whence:end" => 2,
        _ => 0,
    }
}

/// Full descriptor lifecycle in a BEGIN..COMMIT bracket: open (fd 0), write,
/// seek (SEEK_SET/CUR/END), read, tell, close; then a post-commit get + unlink.
fn gen_fd(g: &mut Gen) -> Vec<StmtKind> {
    let l = alloc(g);
    let whence = pick_whence(g);
    // A seek target that stays in range for every whence: 2 from SET, +1 from
    // CUR (fd sits at 8 after the write), -3 from END (len 8 -> 5).
    let off: i64 = match whence {
        1 => 1,   // SEEK_CUR (fd at 8 -> 9, still < len? clamp read to 0)
        2 => -3,  // SEEK_END (len 8 -> 5)
        _ => 2,   // SEEK_SET
    };
    vec![
        raw("BEGIN;".to_string()),
        raw(format!("SELECT lo_create({l});")),
        raw(format!("SELECT lo_open({l}, {INV_RW});")),
        raw("SELECT lowrite(0, '\\x0102030405060708'::bytea);".to_string()),
        raw(format!("SELECT lo_lseek(0, {off}, {whence});")),
        raw("SELECT encode(loread(0, 4), 'hex');".to_string()),
        raw("SELECT lo_tell(0);".to_string()),
        raw("SELECT lo_close(0);".to_string()),
        raw("COMMIT;".to_string()),
        raw(format!("SELECT encode(lo_get({l}), 'hex');")),
        raw(format!("SELECT lo_unlink({l});")),
    ]
}

// ---------------------------------------------------------------- chunk ----

/// A write straddling the 2 kB LOBLKSIZE page edge: lo_put at offset 2040 of
/// 40 bytes writes into pages 0 and 1 over a zero-filled hole, then a
/// boundary-straddling fragment get and a total-ordered page introspection.
fn gen_chunk(g: &mut Gen) -> Vec<StmtKind> {
    let l = alloc(g);
    // Offsets around the first page edge (2048): start just before, run past.
    let start = 2040 + g.rng.below(6); // 2040..2045
    vec![
        raw(format!("SELECT lo_create({l});")),
        raw(format!("SELECT lo_put({l}, {start}, {});", payload("7e", 40))),
        raw(format!("SELECT length(lo_get({l}));")),
        raw(format!("SELECT encode(lo_get({l}, 2044, 8), 'hex');")),
        raw(format!(
            "SELECT loid, pageno, length(data) FROM pg_largeobject \
             WHERE loid = {l} ORDER BY loid, pageno;"
        )),
        raw(format!("SELECT lo_unlink({l});")),
    ]
}

// ---------------------------------------------------------------- trunc ----

/// lo_truncate grow (zero-fill past EOF) or shrink, in a descriptor bracket.
fn gen_trunc(g: &mut Gen) -> Vec<StmtKind> {
    let l = alloc(g);
    let grow = g.weights.pick(g.rng, &["largeobj:trunc:grow", "largeobj:trunc:shrink"])
        == "largeobj:trunc:grow";
    g.fire(if grow { "largeobj:trunc:grow" } else { "largeobj:trunc:shrink" });
    // Base payload 100 bytes; grow to a value crossing the page edge, shrink
    // to a small value.
    let newlen: i64 = if grow { 2100 + g.rng.below(60) as i64 } else { 10 + g.rng.below(40) as i64 };
    vec![
        raw("BEGIN;".to_string()),
        raw(format!("SELECT lo_create({l});")),
        raw(format!("SELECT lo_open({l}, {INV_RW});")),
        raw(format!("SELECT lowrite(0, {});", payload("5c", 100))),
        raw(format!("SELECT lo_truncate(0, {newlen});")),
        raw("SELECT lo_lseek(0, 0, 2);".to_string()),
        raw("SELECT lo_tell(0);".to_string()),
        raw("SELECT lo_close(0);".to_string()),
        raw("COMMIT;".to_string()),
        raw(format!("SELECT length(lo_get({l}));")),
        raw(format!("SELECT lo_unlink({l});")),
    ]
}

// ----------------------------------------------------------------- lo64 ----

/// The 64-bit descriptor variants: lo_lseek64 / lo_tell64 / lo_truncate64
/// over a multi-page (3000-byte) object.
fn gen_lo64(g: &mut Gen) -> Vec<StmtKind> {
    let l = alloc(g);
    vec![
        raw("BEGIN;".to_string()),
        raw(format!("SELECT lo_create({l});")),
        raw(format!("SELECT lo_open({l}, {INV_RW});")),
        raw(format!("SELECT lowrite(0, {});", payload("99", 3000))),
        raw("SELECT lo_lseek64(0, 2500, 0);".to_string()),
        raw("SELECT lo_tell64(0);".to_string()),
        raw("SELECT lo_truncate64(0, 2600);".to_string()),
        raw("SELECT lo_lseek64(0, 0, 2);".to_string()),
        raw("SELECT lo_tell64(0);".to_string()),
        raw("SELECT lo_close(0);".to_string()),
        raw("COMMIT;".to_string()),
        raw(format!("SELECT length(lo_get({l}));")),
        raw(format!("SELECT lo_unlink({l});")),
    ]
}

// ----------------------------------------------------------------- meta ----

/// pg_largeobject_metadata / pg_largeobject catalog projections over a small
/// deterministic object.
fn gen_meta(g: &mut Gen) -> Vec<StmtKind> {
    let l = alloc(g);
    vec![
        raw(format!("SELECT lo_from_bytea({l}, {});", payload("4d", 24))),
        raw(format!(
            "SELECT count(*) FROM pg_largeobject_metadata WHERE oid = {l};"
        )),
        raw(format!(
            "SELECT count(*), sum(length(data)) FROM pg_largeobject WHERE loid = {l};"
        )),
        raw(format!(
            "SELECT oid = {l}, lomacl IS NULL FROM pg_largeobject_metadata \
             WHERE oid = {l} ORDER BY oid;"
        )),
        raw(format!("SELECT lo_unlink({l});")),
    ]
}

// ------------------------------------------------------------------ err ----

/// Error arms — each isolated. loid-based errors are standalone statements;
/// descriptor-based errors live in a self-contained BEGIN..ROLLBACK.
fn gen_err(g: &mut Gen) -> Vec<StmtKind> {
    let arm = g.weights.pick(
        g.rng,
        &[
            "largeobj:err:getne",
            "largeobj:err:putne",
            "largeobj:err:unlinkne",
            "largeobj:err:openne",
            "largeobj:err:badfdread",
            "largeobj:err:badfdwrite",
            "largeobj:err:negseek",
            "largeobj:err:badwhence",
            "largeobj:err:negfrag",
            "largeobj:err:readonly",
            "largeobj:err:writeonly",
        ],
    );
    g.fire(arm);
    let ne = LOID_NONEXIST;
    match arm {
        // "large object %u does not exist"
        "largeobj:err:getne" => vec![raw(format!("SELECT lo_get({ne});"))],
        "largeobj:err:putne" => {
            vec![raw(format!("SELECT lo_put({ne}, 0, '\\x00'::bytea);"))]
        }
        "largeobj:err:unlinkne" => vec![raw(format!("SELECT lo_unlink({ne});"))],
        "largeobj:err:openne" => vec![raw(format!("SELECT lo_open({ne}, {INV_R});"))],
        // "invalid large-object descriptor: %d"
        "largeobj:err:badfdread" => vec![raw("SELECT loread(999, 4);".to_string())],
        "largeobj:err:badfdwrite" => {
            vec![raw("SELECT lowrite(999, '\\x00'::bytea);".to_string())]
        }
        // "invalid large object seek target: .."
        "largeobj:err:negseek" => err_bracket(g, "SELECT lo_lseek(0, -5, 0);"),
        // "invalid whence setting: %d"
        "largeobj:err:badwhence" => err_bracket(g, "SELECT lo_lseek(0, 0, 9);"),
        // "requested length cannot be negative"
        "largeobj:err:negfrag" => {
            let l = alloc(g);
            vec![
                raw("BEGIN;".to_string()),
                raw(format!("SELECT lo_create({l});")),
                raw(format!("SELECT lo_get({l}, 0, -1);")),
                raw("ROLLBACK;".to_string()),
            ]
        }
        // "large object descriptor %d was not opened for reading"
        "largeobj:err:readonly" => {
            let l = alloc(g);
            vec![
                raw("BEGIN;".to_string()),
                raw(format!("SELECT lo_create({l});")),
                raw(format!("SELECT lo_open({l}, {INV_W});")),
                raw("SELECT loread(0, 4);".to_string()),
                raw("ROLLBACK;".to_string()),
            ]
        }
        // "large object descriptor %d was not opened for writing"
        _ => {
            let l = alloc(g);
            vec![
                raw("BEGIN;".to_string()),
                raw(format!("SELECT lo_create({l});")),
                raw(format!("SELECT lo_open({l}, {INV_R});")),
                raw("SELECT lowrite(0, '\\x00'::bytea);".to_string()),
                raw("ROLLBACK;".to_string()),
            ]
        }
    }
}

/// A descriptor-based error probe: create + open (fd 0) an object, run the
/// erroring body, then ROLLBACK (valid in an aborted transaction; drops the
/// object with the rollback).
fn err_bracket(g: &mut Gen, body: &str) -> Vec<StmtKind> {
    let l = alloc(g);
    vec![
        raw("BEGIN;".to_string()),
        raw(format!("SELECT lo_create({l});")),
        raw(format!("SELECT lo_open({l}, {INV_RW});")),
        raw(body.to_string()),
        raw("ROLLBACK;".to_string()),
    ]
}
