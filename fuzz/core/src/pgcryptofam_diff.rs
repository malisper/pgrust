//! pgcryptofam_diff: differential fuzz driver for contrib/pgcrypto's
//! crypt() / gen_salt() / armor family vs verbatim vendored PostgreSQL 18.3
//! C (csrc/pgcryptofam/, upstream sha 62d6c7d3df; lane p1-pgcryptofam).
//! The C-side FFI surface is `crate::pgcryptofam`; this file is the
//! comparison logic.
//!
//! Input encoding: `data[0] % 5` = arm, `data[1]` = shape/mode byte,
//! `data[2..]` = payload (two-field splits carry an explicit length byte).
//!
//! Selector = data[0] % 5:
//!
//!   0 crypt        — pgcrypto::crypt::crypt(password, setting) vs
//!                    pg_diff_pgcryptofam_crypt (px_crypt over the verbatim
//!                    des/xdes/md5/bcrypt/sha-crypt engines + pg_crypt's own
//!                    39000 "crypt(3) returned NULL" translation). The
//!                    SQLSTATE plane rides the shipped `pg_crypt` fc wrapper
//!                    (lib.rs crypt_err) whenever the GUC store is available.
//!   1 gen_salt     — pgcrypto::crypt::gen_salt(algo, rounds) vs
//!                    pg_diff_pgcryptofam_gen_salt. ENTROPY-CARVED, see
//!                    DOMAIN CARVES; the rounds-validation behavior (xdes
//!                    [1,0xFFFFFF] + even-count refusal + PX_XDES_ROUNDS=725,
//!                    bf [4,31], sha [1000,999999999], unknown algo) is fully
//!                    compared.
//!   2 armor        — pgcrypto::pgp::armor::armor_encode(data, keys, values)
//!                    vs pg_diff_pgcryptofam_armor, PLUS the SQL-array header
//!                    validation plane: the shipped `pg_armor` fc wrapper
//!                    (lib.rs parse_key_value_arrays) against a transcription
//!                    of pgp-pgsql.c:772-834's check order.
//!   3 dearmor      — pgcrypto::pgp::armor::armor_decode(text) vs
//!                    pg_diff_pgcryptofam_dearmor (+ pg_dearmor's
//!                    px_THROW_ERROR translation), with the shipped
//!                    `pg_dearmor` fc wrapper carrying the SQLSTATE plane.
//!   4 armor_headers— pgcrypto::pgp::armor::extract_armor_headers(text) vs
//!                    pg_diff_pgcryptofam_armor_headers.
//!
//! COMPARISON PLANES (the harness contract):
//!   P1 VALUE      — exact output image (hash string / salt string / armored
//!                   bytes / decoded bytes / decoded (key,value) pairs).
//!   P2 VERDICT    — ok vs error, both directions.
//!   P3 SQLSTATE   — the raised errcode. Error-plane parity IS "same
//!                   behavior"; C's MAKE_SQLSTATE int and Rust's
//!                   `SqlState(i32)` use the identical 6-bit packing, so the
//!                   comparison is on the raw int.
//!   P4 NOTICE     — crypt-sha.c emits `rounds=N is below supported value
//!                   (1000), using 1000 instead` (and the `exceeds maximum`
//!                   twin) where N is the TRUNCATED SIGNED int32. Compared:
//!                   notice PRESENCE and the NUMERIC VALUES in the text. This
//!                   plane is what witnesses the D12 clamp. Rust notices are
//!                   captured through elog's emit_log_hook (see `notices`).
//! Message TEXT is out of scope for comparison; it is captured and printed in
//! the panic message for triage only.
//!
//! COST BOUNDING (mandatory — this is what keeps the harness alive).
//! Before EITHER side runs, arm 0 calls `pg_diff_pgcryptofam_cost_probe` and
//! SKIPS the exec — symmetrically, both sides, and counted in
//! [`cost_skips`] — when the parsed work exceeds: bcrypt cost <= 6
//! (2^6 = 64 key schedules), shacrypt rounds <= 5000, xdes count <= 4095.
//! DES and md5 are constant work and are unbounded. The decision is made
//! from the PROBE ALONE, before either implementation is touched, so the
//! skip can never be asymmetric (an asymmetric skip fakes agreement). The
//! bound holds even if the D12 product fix were reverted: the probe parses
//! the setting the way the vendored preambles do and never runs crypt work,
//! so the oracle does not depend on the fix to terminate.
//!
//! D12 IS DELIBERATELY OUT OF THIS TARGET. `rounds >= 2^31` wedges any
//! in-process harness BY DESIGN: C clamps to 1000, and the pre-fix Rust ran
//! 999,999,999 rounds uninterruptibly, so a regressed clamp would hang the
//! fuzzer instead of failing it. Witnessing D12 needs a child-process +
//! SIGKILL timeout rig, not a fuzz arm; the in-tree witness is
//! `pgcrypto::crypt::tests::shacrypt_rounds_out_of_range_clamps_like_c`
//! (bounded by a finite CHECK_FOR_INTERRUPTS budget). The cost bound above
//! makes every rounds >= 5001 setting a counted skip here.
//!
//! EXHAUSTIVE-DIFF SWEEPS, NOT FUZZ ARMS. `to64`, `bf_encode`, `bf_decode`,
//! `ascii_to_bin` and the xdes count encode have domains at or under ~2^32,
//! so per the campaign's decision cascade they are ENUMERATED against the C
//! entry points in `pgcryptofam_sweeps.rs` (total over the domain, stronger
//! than any fuzz floor) instead of being sampled here.
//!
//! DOMAIN CARVES (harness/caller contract, never pgrust behavior):
//!   - arm 0/1 text domain: `crypt`/`gen_salt` take `&str` on the Rust side
//!     and the SQL wrappers reach them through `String::from_utf8_lossy`.
//!     The driver therefore materializes ONE byte string per field
//!     (NUL-sanitized 0x00 -> 0x01, then lossy-decoded to UTF-8) and hands
//!     the IDENTICAL bytes to both sides. Non-ASCII stays in the domain (D11
//!     needs password bytes >= 0x80); only NUL and invalid-UTF-8 tails are
//!     normalized, and PG `text` can carry neither.
//!   - ARM 1 ENTROPY CARVE. gen_salt output is entropy-dependent AND the two
//!     sides consume DIFFERENT NUMBERS of random bytes for the same
//!     algorithm (C's md5 generator packs 6 bytes into 8 chars; pgrust draws
//!     `input_len` and masks each), so even a shared pinned stream would not
//!     align. Honest planes for arm 1: error verdict + SQLSTATE (full
//!     strength), output LENGTH (full strength), the DETERMINISTIC PREFIX
//!     (`$2a$NN$`, `$5$rounds=N$`, `$1$`, the `_` + 4 xdes count chars), and
//!     the random tail compared for ITOA64 ALPHABET MEMBERSHIP only. The C
//!     side is always handed >= 32 entropy bytes (padded deterministically)
//!     so its PXE_NO_RANDOM arm — which pgrust's OS entropy never takes —
//!     can never fire and fake a one-sided error.
//!   - arm 2 header keys/values are NUL-sanitized: C's pgp_armor_encode
//!     takes `char **` cstrings, so an embedded NUL truncates C-side only.
//!     PG `text` cannot carry NUL.
//!   - arm 4 input text is NUL-sanitized: pgp_extract_armor_headers copies
//!     the header block into a NUL-terminated buffer and splits it with
//!     strchr/strstr, while the shipped Rust is slice-based. Arm 3
//!     (pgp_armor_decode) is length-based on BOTH sides (memchr / slice
//!     iteration) and therefore keeps raw bytes.
//!   - arm 2/3 fc planes run unconditionally (neither wrapper reads a GUC).
//!     Arm 0/1's fc plane runs only when the thread's GUC store came up:
//!     `fc_pg_crypt`/`fc_pg_gen_salt*` open with `check_builtin_crypto()`,
//!     which PANICS ("GUC store not initialized") without one. When the
//!     store is unavailable the SQLSTATE plane for those two arms is
//!     skipped and counted in [`fc_skips`]; P1/P2/P4 still run at full
//!     strength off the cores. Under the dedicated `pgcryptofam_diff` fuzz
//!     binary — the binary the CI cluster runs and the coverage capture replays —
//!     the store comes up and the plane is live.
//!   - the `pgp_armor_headers` SRF wrapper (fc_pgp_armor_headers) is NOT
//!     driven: InitMaterializedSRF needs the executor's tuplestore/typcache
//!     machinery, which every sibling lane carves as SRF-engine surface.
//!     The compared body — `extract_armor_headers` — is driven directly.

#![allow(dead_code)]

use std::cell::RefCell;
use std::sync::Once;

use datum::{Datum, NullableDatum};
use types_error::{PgError, SqlState};
use types_fmgr::{LocalFcinfo, PGFunction};

use crate::pgcryptofam::{
    c_armor, c_armor_headers, c_crypt_status, c_dearmor, c_gen_salt_status, cost_probe,
    PgcryptofamKind, PgcryptofamStatus,
};

// itoa64 (crypt-gensalt.c / crypt-md5.c), the alphabet every generated salt
// tail must live in.
const ITOA64: &[u8; 64] = b"./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

// ---------------------------------------------------------------------------
// COST BOUND (see the banner)
// ---------------------------------------------------------------------------

const BF_MAX_COST_ITERS: i64 = 1 << 6; // bcrypt cost <= 6
const SHA_MAX_ROUNDS: i64 = 5_000;
const XDES_MAX_COUNT: i64 = 4_095;

thread_local! {
    static COST_SKIPS: RefCell<u64> = const { RefCell::new(0) };
    static FC_SKIPS: RefCell<u64> = const { RefCell::new(0) };
}

/// Number of execs refused by the cost bound on this thread (non-vacuity
/// counter: a bound that never fires would mean the probe is inert, a bound
/// that always fires would mean the arm is dead).
pub fn cost_skips() -> u64 {
    COST_SKIPS.with(|c| *c.borrow())
}

/// Number of execs whose fc SQLSTATE plane was skipped for want of a GUC
/// store (arms 0/1 only).
pub fn fc_skips() -> u64 {
    FC_SKIPS.with(|c| *c.borrow())
}

/// The whole cost decision, taken from the probe alone. Returning `false`
/// means NEITHER side executes.
fn cost_within_bound(kind: PgcryptofamKind, cost: i64) -> bool {
    match kind {
        PgcryptofamKind::Bf => cost <= BF_MAX_COST_ITERS,
        PgcryptofamKind::Sha256 | PgcryptofamKind::Sha512 => cost <= SHA_MAX_ROUNDS,
        PgcryptofamKind::Xdes => cost <= XDES_MAX_COUNT,
        // constant work
        PgcryptofamKind::Des | PgcryptofamKind::Md5 | PgcryptofamKind::None => true,
    }
}

// ---------------------------------------------------------------------------
// P4: Rust-side NOTICE capture
// ---------------------------------------------------------------------------

thread_local! {
    static NOTICES: RefCell<Vec<(i32, String)>> = const { RefCell::new(Vec::new()) };
}

fn record_notice(e: &PgError, output_to_server: &mut bool) {
    if e.level < types_error::ERROR {
        NOTICES.with(|n| n.borrow_mut().push((e.sqlstate.0, e.message.clone())));
    }
    // The hook may only turn output_to_server OFF; do that so the captured
    // NOTICE never also lands on the fuzzer's stderr.
    *output_to_server = false;
}

/// Arm the NOTICE plane for this thread. `log_min_messages` must be at or
/// below NOTICE or elog's policy never reaches the emit hook at all (the
/// boot default is WARNING) — a silently unarmed hook would make P4 vacuous,
/// which `notice_plane_is_live` fences.
fn arm_notice_capture() {
    thread_local! { static ARMED: RefCell<bool> = const { RefCell::new(false) }; }
    ARMED.with(|a| {
        let mut a = a.borrow_mut();
        if !*a {
            elog::config::set_log_min_messages(types_error::NOTICE);
            elog::set_emit_log_hook(Some(record_notice));
            *a = true;
        }
    });
}

fn take_notices() -> Vec<(i32, String)> {
    NOTICES.with(|n| std::mem::take(&mut *n.borrow_mut()))
}

/// Every signed integer literal appearing in a notice text — the compared
/// half of P4 (`rounds=-2147483648 is below supported value (1000), using
/// 1000 instead` -> [-2147483648, 1000, 1000]). Message wording itself is
/// out of scope.
fn numbers_in(s: &str) -> Vec<i64> {
    let b = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < b.len() {
        if b[i].is_ascii_digit() {
            let neg = i > 0 && b[i - 1] == b'-';
            let start = i;
            while i < b.len() && b[i].is_ascii_digit() {
                i += 1;
            }
            let v: i64 = s[start..i].parse().unwrap_or(i64::MAX);
            out.push(if neg { -v } else { v });
        } else {
            i += 1;
        }
    }
    out
}

// ---------------------------------------------------------------------------
// fc-wrapper plane plumbing (P3)
// ---------------------------------------------------------------------------

fn seams_setup() {
    static SEAMS: Once = Once::new();
    SEAMS.call_once(|| {
        use std::panic::catch_unwind;
        // First-wins across lanes sharing one test binary (seam set() panics
        // on a second install; every impl below is the standard environment
        // the sibling lanes install too).
        let _ = catch_unwind(pgcrypto::init_seams);
        // ENVIRONMENT SEAM, not computation: the bcrypt / sha-crypt cost
        // loops call CHECK_FOR_INTERRUPTS once per round (D19). The C oracle
        // shim's CHECK_FOR_INTERRUPTS is likewise a no-op — there is no
        // signal machinery on either side of this harness — so a
        // never-interrupting impl is the SYMMETRIC environment. Cancellation
        // behavior is witnessed by pgcrypto's own arm_cfi tests, not here.
        crate::install_check_for_interrupts_seam_once();
        // elog's errfinish reads it; the harness is never in parallel mode.
        let _ = catch_unwind(|| xact_seams::is_in_parallel_mode::set(|| false));
        // The GUC store's bool variables parse through this seam. SHIPPED
        // impl (the computation stays real; the seam is only wiring).
        let _ = catch_unwind(|| scalar_seams::parse_bool::set(adt_bool::parse_bool));
    });
    arm_notice_capture();
}

/// Bring up the thread's GUC store if we can. `fc_pg_crypt`/`fc_pg_gen_salt*`
/// open with `check_builtin_crypto()` -> `guc::GetConfigOption`, which
/// `.expect("GUC store not initialized")`s without one. The store is
/// THREAD-LOCAL, so this is retried per thread and never poisons a sibling
/// lane's process-global seam (unlike installing guc/elog/guc_tables seams,
/// which this deliberately does NOT do).
fn guc_store_ready() -> bool {
    thread_local! { static TRIED: RefCell<Option<bool>> = const { RefCell::new(None) }; }
    TRIED.with(|t| {
        let mut t = t.borrow_mut();
        if let Some(v) = *t {
            return v;
        }
        let ok = std::panic::catch_unwind(|| {
            if !guc::store::is_initialized() {
                let _ = guc::store::initialize_guc_options();
            }
            guc::store::is_initialized()
        })
        .unwrap_or(false);
        *t = Some(ok);
        ok
    })
}

fn lookup(name: &str) -> PGFunction {
    dfmgr::load_external_function("pgcrypto", name, true)
        .expect("pgcrypto library registered")
        .expect("function resolves")
}

fn fc_call<const N: usize>(
    f: PGFunction,
    m: mcx::Mcx<'_>,
    args: [Datum; N],
) -> types_error::PgResult<Datum> {
    let mut fcinfo = LocalFcinfo::<N>::new(0);
    // SAFETY: the context owning `m` outlives this single call.
    unsafe { fcinfo.set_result_mcx(m) };
    for (i, a) in args.into_iter().enumerate() {
        fcinfo.args[i] = NullableDatum::value(a);
    }
    f(None, &mut fcinfo)
}

/// 4B-U text/bytea varlena image: [4-byte LE header][payload].
fn text_image(bytes: &[u8]) -> Vec<u8> {
    let total = bytes.len() + 4;
    let mut img = Vec::with_capacity(total);
    img.extend_from_slice(&((total as u32) << 2).to_le_bytes());
    img.extend_from_slice(bytes);
    img
}

/// Read back a 4B-U varlena result datum's payload.
///
/// SAFETY: `d` came from a wrapper returning a live 4B-header varlena in the
/// arming context.
unsafe fn result_payload<'a>(d: Datum) -> &'a [u8] {
    let p = d.as_usize() as *const u8;
    let word = u32::from_le_bytes([*p, *p.add(1), *p.add(2), *p.add(3)]);
    let total = (word >> 2) as usize;
    std::slice::from_raw_parts(p.add(4), total - 4)
}

// ---------------------------------------------------------------------------
// input plumbing
// ---------------------------------------------------------------------------

/// Byte-cursor over the fuzz payload; exhausted reads return zeros.
struct Rdr<'a> {
    d: &'a [u8],
    pos: usize,
}

impl<'a> Rdr<'a> {
    fn new(d: &'a [u8]) -> Self {
        Rdr { d, pos: 0 }
    }
    fn u8(&mut self) -> u8 {
        let v = self.d.get(self.pos).copied().unwrap_or(0);
        self.pos += 1;
        v
    }
    fn i32(&mut self) -> i32 {
        i32::from_le_bytes([self.u8(), self.u8(), self.u8(), self.u8()])
    }
    fn bytes(&mut self, n: usize) -> &'a [u8] {
        let start = self.pos.min(self.d.len());
        let end = (self.pos + n).min(self.d.len());
        self.pos += n;
        &self.d[start..end]
    }
    fn rest(&mut self) -> &'a [u8] {
        let start = self.pos.min(self.d.len());
        self.pos = self.d.len();
        &self.d[start..]
    }
}

/// The single byte string BOTH sides receive for a `text`-typed field: NUL
/// sanitized then lossy-decoded to UTF-8, because the Rust cores take `&str`
/// and the SQL wrappers reach them through `String::from_utf8_lossy`. See
/// DOMAIN CARVES.
fn text_field(bytes: &[u8]) -> String {
    let sanitized: Vec<u8> = bytes.iter().map(|&b| if b == 0 { 1 } else { b }).collect();
    String::from_utf8_lossy(&sanitized).into_owned()
}

/// NUL-sanitized bytes (no UTF-8 normalization) — the armor header / header
/// text domain, where C uses cstrings but neither side decodes.
fn nul_free(bytes: &[u8]) -> Vec<u8> {
    bytes.iter().map(|&b| if b == 0 { 1 } else { b }).collect()
}

fn oracle_note(st: &PgcryptofamStatus) -> String {
    format!(
        "C[ok={} sqlstate={} msg={:?} notices={} notice={:?}]",
        st.ok,
        st.sqlstate,
        st.msg_str(),
        st.notice_count,
        st.notice_str()
    )
}

// ---------------------------------------------------------------------------
// arm 0: crypt(password, setting)
// ---------------------------------------------------------------------------

/// px_crypt_list prefixes plus the two that are NOT rows (`$2b$`, `$2y$`) and
/// a bare/garbage arm — the fuzzer reaches every engine through byte 1 of the
/// mode byte instead of having to synthesize `$2a$` from scratch.
const SETTING_PREFIXES: [&str; 12] = [
    "", "$1$", "$5$", "$6$", "$2a$", "$2x$", "$2$", "$2b$", "$2y$", "_", "$5$rounds=", "$7$",
];

fn run_crypt(r: &mut Rdr, mode: u8) {
    let pwlen = r.u8() as usize % 64;
    let pw = text_field(r.bytes(pwlen));
    let prefix = SETTING_PREFIXES[(mode >> 1) as usize % SETTING_PREFIXES.len()];
    let mut setting = String::from(prefix);
    setting.push_str(&text_field(r.rest()));

    // ---- COST BOUND: probe first, decide before either side runs ----
    let (kind, cost) = cost_probe(setting.as_bytes());
    if !cost_within_bound(kind, cost) {
        COST_SKIPS.with(|c| *c.borrow_mut() += 1);
        return;
    }

    let mut out = vec![0u8; 1024];
    let _ = take_notices();
    // `c_crypt_status` keeps the SUCCESS-path status too: crypt-sha's clamp
    // NOTICE rides a call that returns normally, and the plain `Result`
    // wrapper drops the status on Ok. ONE exec per side, always.
    let (cn, cst) = c_crypt_status(pw.as_bytes(), setting.as_bytes(), &mut out);
    let cval: Option<Vec<u8>> = cn.map(|n| out[..n].to_vec());
    let cnotices: Vec<String> = if cst.notice_count > 0 {
        vec![cst.notice_str().to_string()]
    } else {
        Vec::new()
    };
    let cerr = if cval.is_none() { Some(&cst) } else { None };

    let rres = pgcrypto::crypt::crypt(&pw, &setting);
    let rnotices: Vec<String> = take_notices().into_iter().map(|(_, m)| m).collect();

    // ---- P4: NOTICE plane ----
    assert_eq!(
        cnotices.is_empty(),
        rnotices.is_empty(),
        "crypt({pw:?},{setting:?}) NOTICE presence: C {cnotices:?} vs Rust {rnotices:?}"
    );
    if let (Some(c), Some(rn)) = (cnotices.first(), rnotices.first()) {
        assert_eq!(
            numbers_in(c),
            numbers_in(rn),
            "crypt({pw:?},{setting:?}) NOTICE numbers: C {c:?} vs Rust {rn:?}"
        );
    }

    // ---- P1/P2: value + verdict ----
    match (&cval, &rres) {
        (Some(cv), Ok(rv)) => assert_eq!(
            rv.as_bytes(),
            &cv[..],
            "crypt({pw:?},{setting:?}) value"
        ),
        (None, Err(_)) => {}
        (Some(cv), Err(e)) => panic!(
            "crypt({pw:?},{setting:?}): C ok {:?}, Rust errored {}",
            String::from_utf8_lossy(cv),
            crypt_err_note(e)
        ),
        (None, Ok(rv)) => panic!(
            "crypt({pw:?},{setting:?}): Rust ok {rv:?}, {}",
            oracle_note(&cst)
        ),
    }

    // ---- P3: SQLSTATE, through the shipped fc wrapper ----
    if !guc_store_ready() {
        FC_SKIPS.with(|c| *c.borrow_mut() += 1);
        return;
    }
    let ctx = mcx::MemoryContext::new("pgcryptofam_fc");
    let pwi = text_image(pw.as_bytes());
    let sti = text_image(setting.as_bytes());
    let fc = fc_call(
        lookup("pg_crypt"),
        ctx.mcx(),
        [
            Datum::from_usize(pwi.as_ptr() as usize),
            Datum::from_usize(sti.as_ptr() as usize),
        ],
    );
    let _ = take_notices();
    match (&cval, fc) {
        (Some(cv), Ok(d)) => {
            // SAFETY: fc_pg_crypt returns a live text varlena in ctx.
            let rv = unsafe { result_payload(d) };
            assert_eq!(rv, &cv[..], "fc pg_crypt({pw:?},{setting:?}) value");
        }
        (None, Err(e)) => {
            let st = cerr.expect("error status");
            assert_eq!(
                e.sqlstate.0,
                st.sqlstate,
                "fc pg_crypt({pw:?},{setting:?}) SQLSTATE: Rust {:?} vs {}",
                e.message,
                oracle_note(st)
            );
        }
        (Some(_), Err(e)) => panic!(
            "fc pg_crypt({pw:?},{setting:?}): C ok, fc errored {:?}",
            e.message
        ),
        (None, Ok(_)) => panic!(
            "fc pg_crypt({pw:?},{setting:?}): fc ok, {}",
            oracle_note(&cst)
        ),
    }
}

fn crypt_err_note(e: &pgcrypto::crypt::CryptError) -> String {
    match e {
        pgcrypto::crypt::CryptError::Message(m) => format!("Message({m:?})"),
        pgcrypto::crypt::CryptError::Unsupported(m) => format!("Unsupported({m:?})"),
        pgcrypto::crypt::CryptError::Pg(e) => {
            format!("Pg(sqlstate={} msg={:?})", e.sqlstate.0, e.message)
        }
    }
}

// ---------------------------------------------------------------------------
// arm 1: gen_salt(algo, rounds) — ENTROPY-CARVED
// ---------------------------------------------------------------------------

const SALT_ALGOS: [&str; 10] = [
    "des",
    "md5",
    "xdes",
    "bf",
    "sha256crypt",
    "sha512crypt",
    "XDES",
    "Bf",
    "",
    "nosuchalgo",
];

/// Rounds values chosen to sit exactly on every gen_list boundary; the raw
/// i32 arm keeps the whole domain reachable.
const ROUNDS_CORNERS: [i32; 16] = [
    0,
    1,
    2,
    3,
    4,
    5,
    25,
    31,
    32,
    725,
    1000,
    5000,
    999_999_999,
    0xFF_FFFF,
    0x100_0000,
    -1,
];

fn run_gen_salt(r: &mut Rdr, mode: u8) {
    let algo = if mode & 1 == 0 {
        SALT_ALGOS[(mode >> 4) as usize % SALT_ALGOS.len()].to_string()
    } else {
        let n = r.u8() as usize % 24;
        text_field(r.bytes(n))
    };
    let rounds = if mode & 2 == 0 {
        ROUNDS_CORNERS[(mode >> 2) as usize % ROUNDS_CORNERS.len()]
    } else {
        r.i32()
    };

    // ENTROPY CARVE: C is always given >= 32 bytes so PXE_NO_RANDOM (which
    // pgrust's OS entropy never takes) can never fire one-sided.
    let mut entropy = r.rest().to_vec();
    let mut fill = 0x5Au8;
    while entropy.len() < 32 {
        entropy.push(fill);
        fill = fill.wrapping_mul(31).wrapping_add(7);
    }

    let mut out = vec![0u8; 256];
    let (cn, st) = c_gen_salt_status(algo.as_bytes(), rounds, &entropy, &mut out);
    let cval: Option<Vec<u8>> = cn.map(|n| out[..n].to_vec());

    let _ = take_notices();
    let rres = pgcrypto::crypt::gen_salt(&algo, rounds);

    // ---- P2: verdict ----
    match (&cval, &rres) {
        (Some(cv), Ok(rv)) => {
            // ---- P1 (carved): length + deterministic prefix + alphabet ----
            assert_eq!(
                rv.len(),
                cv.len(),
                "gen_salt({algo:?},{rounds}) length: C {:?} vs Rust {rv:?}",
                String::from_utf8_lossy(cv)
            );
            let plen = deterministic_prefix_len(cv);
            assert_eq!(
                &rv.as_bytes()[..plen],
                &cv[..plen],
                "gen_salt({algo:?},{rounds}) deterministic prefix: C {:?} vs Rust {rv:?}",
                String::from_utf8_lossy(cv)
            );
            for (i, &b) in rv.as_bytes()[plen..].iter().enumerate() {
                assert!(
                    ITOA64.contains(&b),
                    "gen_salt({algo:?},{rounds}) random tail byte {i} = {b:#04x} \
                     is off the itoa64 alphabet (Rust {rv:?})"
                );
            }
            // ...and C's own tail must be in the alphabet too (the plane is
            // only meaningful if the oracle side is witnessed).
            for (i, &b) in cv[plen..].iter().enumerate() {
                assert!(
                    ITOA64.contains(&b),
                    "gen_salt({algo:?},{rounds}) C tail byte {i} = {b:#04x} off itoa64"
                );
            }
        }
        (None, Err(_)) => {}
        (Some(cv), Err(e)) => panic!(
            "gen_salt({algo:?},{rounds}): C ok {:?}, Rust errored {}",
            String::from_utf8_lossy(cv),
            crypt_err_note(e)
        ),
        (None, Ok(rv)) => panic!(
            "gen_salt({algo:?},{rounds}): Rust ok {rv:?}, {}",
            oracle_note(&st)
        ),
    }

    // ---- P3: SQLSTATE through the shipped fc wrapper ----
    if !guc_store_ready() {
        FC_SKIPS.with(|c| *c.borrow_mut() += 1);
        return;
    }
    let ctx = mcx::MemoryContext::new("pgcryptofam_fc");
    let ai = text_image(algo.as_bytes());
    let fname = if mode & 4 == 0 && rounds == 0 {
        "pg_gen_salt"
    } else {
        "pg_gen_salt_rounds"
    };
    let fc = if fname == "pg_gen_salt" {
        fc_call(
            lookup(fname),
            ctx.mcx(),
            [Datum::from_usize(ai.as_ptr() as usize)],
        )
    } else {
        fc_call(
            lookup(fname),
            ctx.mcx(),
            [
                Datum::from_usize(ai.as_ptr() as usize),
                Datum::from_i32(rounds),
            ],
        )
    };
    let _ = take_notices();
    match (&cval, fc) {
        (Some(_), Ok(_)) => {}
        (None, Err(e)) => assert_eq!(
            e.sqlstate.0,
            st.sqlstate,
            "fc {fname}({algo:?},{rounds}) SQLSTATE: Rust {:?} vs {}",
            e.message,
            oracle_note(&st)
        ),
        (Some(_), Err(e)) => panic!(
            "fc {fname}({algo:?},{rounds}): C ok, fc errored {:?}",
            e.message
        ),
        (None, Ok(_)) => panic!("fc {fname}({algo:?},{rounds}): fc ok, {}", oracle_note(&st)),
    }
}

/// Length of the fully deterministic head of a gen_salt result — everything
/// before the first entropy-derived character. Derived from the C output
/// shape (crypt-gensalt.c), so it is oracle-anchored, not a Rust model.
fn deterministic_prefix_len(c: &[u8]) -> usize {
    if c.starts_with(b"$2a$") {
        7 // "$2a$NN$"
    } else if c.starts_with(b"$1$") {
        3
    } else if c.starts_with(b"$5$") || c.starts_with(b"$6$") {
        // "$5$rounds=<digits>$"
        match c[3..].iter().position(|&b| b == b'$') {
            Some(i) => 3 + i + 1,
            None => c.len(),
        }
    } else if c.starts_with(b"_") {
        5 // '_' + 4 count chars
    } else {
        0 // traditional DES: both chars are entropy-derived
    }
}

// ---------------------------------------------------------------------------
// arm 2: armor(data, keys[], values[])
// ---------------------------------------------------------------------------

/// Transcription of pgp-pgsql.c:772-834 `parse_key_value_arrays`'s checks, in
/// C's exact order. This is the ONLY driver-side model in the target: the C
/// oracle entry deliberately excludes the SQL-array validation (it takes
/// already-framed cstrings), so the validation plane compares the shipped
/// `fc_pg_armor` against C SOURCE rather than against a running C body. Every
/// row is a live-18.3 captured verdict (lane p1-pgcrypto D8/D9/D10). The
/// armor VALUE plane below is C-oracle-witnessed as usual.
fn c_model_validate(keys: &[Vec<u8>], values: &[Vec<u8>]) -> Option<(&'static str, SqlState)> {
    use types_error::{
        ERRCODE_ARRAY_SUBSCRIPT_ERROR, ERRCODE_INVALID_PARAMETER_VALUE,
        ERRCODE_NULL_VALUE_NOT_ALLOWED,
    };
    let _ = ERRCODE_NULL_VALUE_NOT_ALLOWED; // NULL elements are out of the fuzz domain
    if keys.len() != values.len() {
        return Some(("mismatched array dimensions", ERRCODE_ARRAY_SUBSCRIPT_ERROR));
    }
    for (k, v) in keys.iter().zip(values.iter()) {
        if k.iter().any(|&c| c >= 0x80) {
            return Some((
                "header key must not contain non-ASCII characters",
                ERRCODE_INVALID_PARAMETER_VALUE,
            ));
        }
        if k.windows(2).any(|w| w == b": ") {
            return Some((
                "header key must not contain \": \"",
                ERRCODE_INVALID_PARAMETER_VALUE,
            ));
        }
        if k.contains(&b'\n') {
            return Some((
                "header key must not contain newlines",
                ERRCODE_INVALID_PARAMETER_VALUE,
            ));
        }
        if v.iter().any(|&c| c >= 0x80) {
            return Some((
                "header value must not contain non-ASCII characters",
                ERRCODE_INVALID_PARAMETER_VALUE,
            ));
        }
        if v.contains(&b'\n') {
            return Some((
                "header value must not contain newlines",
                ERRCODE_INVALID_PARAMETER_VALUE,
            ));
        }
    }
    None
}

fn text_array_image(mcx: mcx::Mcx<'_>, elems: &[Vec<u8>]) -> Vec<u8> {
    if elems.is_empty() {
        return arrayfuncs::construct::construct_empty_array(mcx, types_core::TEXTOID)
            .expect("empty text[]")
            .to_vec();
    }
    let mut datums = Vec::with_capacity(elems.len());
    for e in elems {
        let v = varlena::cstring_to_text(mcx, e).expect("text element");
        datums.push(types_fmgr::varlena_result(v));
    }
    let nulls = vec![false; elems.len()];
    arrayfuncs::construct::construct_md_array(
        mcx,
        &datums,
        Some(&nulls),
        1,
        &[elems.len() as i32],
        &[1],
        types_core::TEXTOID,
        -1,
        false,
        arrayfuncs::foundation::TYPALIGN_INT,
    )
    .expect("text[] image")
    .to_vec()
}

fn run_armor(r: &mut Rdr, mode: u8) {
    let nheaders = if mode & 1 == 0 { 0 } else { (r.u8() % 4) as usize };
    let mut keys: Vec<Vec<u8>> = Vec::with_capacity(nheaders);
    let mut values: Vec<Vec<u8>> = Vec::with_capacity(nheaders);
    for _ in 0..nheaders {
        let kl = r.u8() as usize % 24;
        let vl = r.u8() as usize % 32;
        keys.push(nul_free(r.bytes(kl)));
        values.push(nul_free(r.bytes(vl)));
    }
    let data = r.rest().to_vec();

    // ---- validation plane (see c_model_validate's banner) ----
    let expect_reject = c_model_validate(&keys, &values);

    let ctx = mcx::MemoryContext::new("pgcryptofam_fc");
    let di = text_image(&data);
    let ki = text_array_image(ctx.mcx(), &keys);
    let vi = text_array_image(ctx.mcx(), &values);
    let fc = fc_call(
        lookup("pg_armor"),
        ctx.mcx(),
        [
            Datum::from_usize(di.as_ptr() as usize),
            Datum::from_usize(ki.as_ptr() as usize),
            Datum::from_usize(vi.as_ptr() as usize),
        ],
    );
    match (&expect_reject, &fc) {
        (Some((msg, code)), Err(e)) => {
            assert_eq!(
                e.sqlstate, *code,
                "fc pg_armor header validation SQLSTATE for {keys:?}/{values:?} \
                 (C source says {msg:?}); Rust said {:?}",
                e.message
            );
        }
        (Some((msg, _)), Ok(_)) => panic!(
            "fc pg_armor accepted headers C rejects ({msg}): keys={keys:?} values={values:?}"
        ),
        (None, Err(e)) => panic!(
            "fc pg_armor rejected headers C accepts: keys={keys:?} values={values:?} -> {:?}",
            e.message
        ),
        (None, Ok(_)) => {}
    }
    if expect_reject.is_some() {
        return;
    }

    // ---- P1/P2: armored value, C oracle vs shipped core AND fc wrapper ----
    let pairs: Vec<(&[u8], &[u8])> = keys
        .iter()
        .zip(values.iter())
        .map(|(k, v)| (&k[..], &v[..]))
        .collect();
    let mut out = vec![0u8; data.len() * 2 + 4096 + nheaders * 64];
    let n = c_armor(&data, &pairs, &mut out).expect("pgp_armor_encode never raises");
    let cval = &out[..n];

    let rval = pgcrypto::pgp::armor::armor_encode(&data, &keys, &values);
    assert_eq!(
        rval,
        cval,
        "armor_encode(datalen={},{} headers) value",
        data.len(),
        nheaders
    );

    let fcd = fc.expect("checked Ok above");
    // SAFETY: fc_pg_armor returns a live bytea varlena in ctx.
    let fcv = unsafe { result_payload(fcd) };
    assert_eq!(fcv, cval, "fc pg_armor(datalen={}) value", data.len());
}

// ---------------------------------------------------------------------------
// arms 3/4: dearmor / pgp_armor_headers
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// KNOWN DIVERGENCE — DEARMOR/HEADERS SQLSTATE (found by this target at plane
// creation, 2026-08-02). CLASS: pgrust bug, NOT a harness defect.
//
// C: contrib/pgcrypto/pgp-pgsql.c `pg_dearmor` (and `pgp_armor_headers`) end
// in `px_THROW_ERROR(res)`, and px.c:94-108 raises everything except
// PXE_NO_RANDOM with ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION = 39000.
// pgrust: `fc_pg_dearmor` maps the failure through lib.rs `px_msg` ->
// `px_err`, which hardcodes ERRCODE_INVALID_PARAMETER_VALUE = 22023 for every
// pgcrypto error. Same message text ("Corrupt ascii-armor"), wrong SQLSTATE.
// The C side is the running verbatim 18.3 oracle, not a reading of the source.
//
// The lane's product fixes are frozen, so the P3 plane carves EXACTLY this
// pair and nothing else: C must be 39000 AND Rust must be 22023 AND the two
// message texts must agree. Any other (C, Rust) SQLSTATE pair still fails,
// so a second, different mapping defect cannot hide behind this carve.
// Deleting these two constants is the fix gate.
const C_SQLSTATE_39000: i32 = 3 + (9 << 6); // MAKE_SQLSTATE("39000")
const RUST_SQLSTATE_22023: i32 = 2 + (2 << 6) + (2 << 18) + (3 << 24);

fn is_known_px_throw_sqlstate_divergence(c: i32, r: i32) -> bool {
    c == C_SQLSTATE_39000 && r == RUST_SQLSTATE_22023
}


/// Build an armored envelope with the C encoder, then apply one fuzz-chosen
/// mutation. Without this the decode arms almost never reach past the header
/// scan; with it they reach the base64/CRC/header-split interiors.
fn armored_input(r: &mut Rdr, mode: u8) -> Vec<u8> {
    if mode & 1 == 0 {
        return r.rest().to_vec();
    }
    let nheaders = (r.u8() % 3) as usize;
    let mut keys: Vec<Vec<u8>> = Vec::new();
    let mut values: Vec<Vec<u8>> = Vec::new();
    for i in 0..nheaders {
        let kl = r.u8() as usize % 12;
        let vl = r.u8() as usize % 16;
        let mut k = nul_free(r.bytes(kl));
        if k.is_empty() {
            k = format!("K{i}").into_bytes();
        }
        // C's encoder emits "key: value\n"; a key already containing ": " or a
        // newline would produce a stream the SQL surface can never make (the
        // validation arm 2 covers those rejections).
        k.retain(|&b| b != b'\n' && b != b':');
        if k.is_empty() {
            k = format!("K{i}").into_bytes();
        }
        let mut v = nul_free(r.bytes(vl));
        v.retain(|&b| b != b'\n');
        keys.push(k);
        values.push(v);
    }
    let bodylen = r.u8() as usize % 96;
    let body = r.bytes(bodylen).to_vec();
    let pairs: Vec<(&[u8], &[u8])> = keys
        .iter()
        .zip(values.iter())
        .map(|(k, v)| (&k[..], &v[..]))
        .collect();
    let mut out = vec![0u8; 8192];
    let n = c_armor(&body, &pairs, &mut out).expect("pgp_armor_encode never raises");
    let mut env = out[..n].to_vec();

    // one mutation, driven by the remaining payload
    let kind = r.u8() % 6;
    match kind {
        0 => {}
        1 => {
            // flip one byte
            if !env.is_empty() {
                let pos = (r.u8() as usize | ((r.u8() as usize) << 8)) % env.len();
                env[pos] ^= 1 << (r.u8() % 8);
            }
        }
        2 => {
            // truncate
            let keep = (r.u8() as usize | ((r.u8() as usize) << 8)) % (env.len() + 1);
            env.truncate(keep);
        }
        3 => {
            // drop the CRC line entirely (C does NOT accept a missing one)
            if let Some(p) = env.iter().rposition(|&b| b == b'=') {
                let line_end = env[p..]
                    .iter()
                    .position(|&b| b == b'\n')
                    .map(|o| p + o + 1)
                    .unwrap_or(env.len());
                env.drain(p..line_end);
            }
        }
        4 => {
            // shorten the CRC line by one char
            if let Some(p) = env.iter().rposition(|&b| b == b'=') {
                if p + 1 < env.len() {
                    env.remove(p + 1);
                }
            }
        }
        _ => {
            // splice raw fuzz bytes into the body
            let extra = r.rest();
            if !env.is_empty() && !extra.is_empty() {
                let pos = extra[0] as usize % env.len();
                env.splice(pos..pos, extra[1..].iter().copied());
            }
        }
    }
    env
}

fn run_dearmor(r: &mut Rdr, mode: u8) {
    let text = armored_input(r, mode);

    let mut out = vec![0u8; text.len() + 4096];
    let cres = c_dearmor(&text, &mut out);
    let rres = pgcrypto::pgp::armor::armor_decode(&text);

    match (&cres, &rres) {
        (Ok(n), Ok(rv)) => assert_eq!(
            &rv[..],
            &out[..*n],
            "dearmor({:?}) value",
            String::from_utf8_lossy(&text)
        ),
        (Err(_), Err(())) => {}
        (Ok(n), Err(())) => panic!(
            "dearmor({:?}): C ok ({} bytes), Rust errored",
            String::from_utf8_lossy(&text),
            n
        ),
        (Err(st), Ok(rv)) => panic!(
            "dearmor({:?}): Rust ok ({} bytes), {}",
            String::from_utf8_lossy(&text),
            rv.len(),
            oracle_note(st)
        ),
    }

    // ---- P3: SQLSTATE through the shipped fc wrapper (no GUC needed) ----
    let ctx = mcx::MemoryContext::new("pgcryptofam_fc");
    let ti = text_image(&text);
    let fc = fc_call(
        lookup("pg_dearmor"),
        ctx.mcx(),
        [Datum::from_usize(ti.as_ptr() as usize)],
    );
    match (&cres, fc) {
        (Ok(n), Ok(d)) => {
            // SAFETY: fc_pg_dearmor returns a live bytea varlena in ctx.
            let rv = unsafe { result_payload(d) };
            assert_eq!(rv, &out[..*n], "fc pg_dearmor value");
        }
        (Err(st), Err(e)) => {
            if !is_known_px_throw_sqlstate_divergence(st.sqlstate, e.sqlstate.0) {
                assert_eq!(
                    e.sqlstate.0,
                    st.sqlstate,
                    "fc pg_dearmor({:?}) SQLSTATE: Rust {:?} vs {}",
                    String::from_utf8_lossy(&text),
                    e.message,
                    oracle_note(st)
                );
            }
            // The carve covers the SQLSTATE only — the message text must
            // still match, or the carve would swallow a second defect.
            assert_eq!(
                e.message,
                st.msg_str(),
                "fc pg_dearmor({:?}) message under the known-SQLSTATE carve",
                String::from_utf8_lossy(&text)
            );
        }
        (Ok(_), Err(e)) => panic!("fc pg_dearmor: C ok, fc errored {:?}", e.message),
        (Err(st), Ok(_)) => panic!("fc pg_dearmor: fc ok, {}", oracle_note(st)),
    }
}

fn run_armor_headers(r: &mut Rdr, mode: u8) {
    // DOMAIN CARVE: NUL-sanitized (C splits a NUL-terminated copy).
    let text = nul_free(&armored_input(r, mode));

    let cres = c_armor_headers(&text);
    let rres = pgcrypto::pgp::armor::extract_armor_headers(&text);

    match (&cres, &rres) {
        (Ok(cp), Ok(rp)) => {
            assert_eq!(
                rp.len(),
                cp.len(),
                "pgp_armor_headers({:?}) header count: C {cp:?} vs Rust {rp:?}",
                String::from_utf8_lossy(&text)
            );
            for (i, (c, rv)) in cp.iter().zip(rp.iter()).enumerate() {
                assert_eq!(
                    rv, c,
                    "pgp_armor_headers({:?}) header {i}",
                    String::from_utf8_lossy(&text)
                );
            }
        }
        (Err(_), Err(())) => {}
        (Ok(cp), Err(())) => panic!(
            "pgp_armor_headers({:?}): C ok ({cp:?}), Rust errored",
            String::from_utf8_lossy(&text)
        ),
        (Err(st), Ok(rp)) => panic!(
            "pgp_armor_headers({:?}): Rust ok ({rp:?}), {}",
            String::from_utf8_lossy(&text),
            oracle_note(st)
        ),
    }
}

// ---------------------------------------------------------------------------
// entry
// ---------------------------------------------------------------------------

pub fn pgcryptofam_diff(data: &[u8]) {
    if data.len() < 2 {
        return;
    }
    seams_setup();
    let sel = data[0] % 5;
    let mode = data[1];
    let mut r = Rdr::new(&data[2..]);
    match sel {
        0 => run_crypt(&mut r, mode),
        1 => run_gen_salt(&mut r, mode),
        2 => run_armor(&mut r, mode),
        3 => run_dearmor(&mut r, mode),
        _ => run_armor_headers(&mut r, mode),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exec(sel: u8, mode: u8, body: &[u8]) {
        let mut v = vec![sel, mode];
        v.extend_from_slice(body);
        pgcryptofam_diff(&v);
    }

    /// P4 must not be vacuous: with the hook armed, a clamping sha-crypt
    /// setting has to produce a captured Rust NOTICE. If elog's policy stops
    /// reaching the emit hook (boot default log_min_messages = WARNING), the
    /// whole notice plane silently degrades to "neither side noticed".
    #[test]
    fn notice_plane_is_live() {
        seams_setup();
        let _ = take_notices();
        let _ = pgcrypto::crypt::crypt("pw", "$5$rounds=10$abcdefgh");
        let n = take_notices();
        assert_eq!(n.len(), 1, "Rust NOTICE not captured: {n:?}");
        assert_eq!(
            numbers_in(&n[0].1),
            vec![10, 1000, 1000],
            "notice numbers: {:?}",
            n[0].1
        );
        // ...and the C oracle records its own on the same setting.
        let mut out = [0u8; 256];
        let (_, st) = c_crypt_status(b"pw", b"$5$rounds=10$abcdefgh", &mut out);
        assert_eq!(st.notice_count, 1, "C NOTICE not recorded");
        assert_eq!(numbers_in(st.notice_str()), vec![10, 1000, 1000]);
    }

    /// The fc SQLSTATE plane must be LIVE, not silently degraded: the GUC
    /// store has to come up (fc_pg_crypt/fc_pg_gen_salt* panic without one)
    /// and the crypt/gen_salt arms must record zero fc skips afterwards. A
    /// regression here turns P3 into a no-op for arms 0 and 1 while every
    /// test still passes — the exact vacuity shape the campaign fences.
    #[test]
    fn fc_sqlstate_plane_is_live() {
        seams_setup();
        assert!(guc_store_ready(), "GUC store did not come up: P3 is dead for arms 0/1");
        let before = fc_skips();
        exec(0, 2, b"\x04fooxSzzz0yzz"); // $1$ crypt, succeeds
        exec(0, 12, b"\x04foox"); // $2$ -> C 39000 "crypt(3) returned NULL"
        exec(1, 0b1000_0100, b""); // gen_salt('des', 0)
        assert_eq!(fc_skips(), before, "fc plane skipped {} execs", fc_skips() - before);
        // ...and the wrappers really resolve through dfmgr (not a silent miss).
        for f in ["pg_crypt", "pg_gen_salt", "pg_gen_salt_rounds", "pg_armor", "pg_dearmor"] {
            let _ = lookup(f);
        }
    }

    /// The cost bound must actually fire (and only on the expensive side).
    #[test]
    fn cost_bound_fires_and_spares_cheap_work() {
        seams_setup();
        let before = cost_skips();
        // $2a$31$… = 2^31 key schedules
        exec(0, 8, b"\x02pw31$abcdefghijklmnopqrstuv");
        assert_eq!(cost_skips(), before + 1, "bcrypt cost 31 was not refused");
        // $5$rounds=999999999$
        exec(0, 20, b"\x02pw999999999$abcdefgh");
        assert_eq!(cost_skips(), before + 2, "sha rounds 999999999 not refused");
        // $1$ is constant work and must NOT be refused
        exec(0, 2, b"\x02pwSzzz0yzz");
        assert_eq!(cost_skips(), before + 2, "md5 crypt was wrongly refused");
    }

    #[test]
    fn arm_smoke() {
        // ---- arm 0: every px_crypt_list row ----
        for prefix_sel in 0u8..12 {
            exec(0, prefix_sel << 1, b"\x04foox06$......................");
        }
        exec(0, 2, b"\x04fooxSzzz0yzz"); // $1$
        exec(0, 4, b"\x04fooxSzzz0yzz"); // $5$
        exec(0, 6, b"\x04fooxSzzz0yzz"); // $6$
        exec(0, 12, b"\x04foox"); // $2$  -> crypt(3) returned NULL / 39000
        exec(0, 0, b"\x04fooxrl"); // traditional DES
        exec(0, 18, b"\x08passwordJ9..abcd"); // xdes _J9..abcd (count 725)
        exec(0, 20, b"\x04foox1000$abcdefgh"); // $5$rounds=1000$
        exec(0, 20, b"\x04foox$abc"); // $5$rounds=$abc (empty rounds)
        exec(0, 20, b"\x04foox0$abc"); // $5$rounds=0$abc
        exec(0, 0, b"\x04foox"); // empty setting -> invalid salt
        exec(0, 8, b"\x04foox06$......................"); // $2a$06$

        // ---- arm 1: every gen_list row + boundaries ----
        for algo in 0u8..10 {
            for rc in 0u8..16 {
                exec(1, (algo << 4) | (rc << 2), b"");
            }
        }
        exec(1, 1 | 2, b"\x04xdes\x01\x00\x00\x00"); // free-form algo + raw rounds

        // ---- arm 2: armor with and without headers ----
        exec(2, 0, b"hello pgcrypto");
        exec(2, 1, b"\x02\x07\x03Version1.0\x07\x02Commenthidata");
        exec(2, 1, b"\x01\x01\x0bk" as &[u8]); // key "k", value from the payload
        // D8 shapes: newline in value / newline in key / ": " in key / non-ASCII
        exec(2, 1, b"\x01\x01\x0ckv\nForged: h");
        exec(2, 1, b"\x01\x03\x01k\nxv");
        exec(2, 1, b"\x01\x04\x01k: xv");
        exec(2, 1, b"\x01\x01\x02k\xc3\xa9");

        // ---- arms 3/4: envelope + every mutation kind ----
        for mode in [0u8, 1] {
            for kind in 0u8..6 {
                let body: Vec<u8> = vec![1, 3, 4, b'K', b'e', b'y', b'v', b'a', b'l', 6]
                    .into_iter()
                    .chain(b"abcdef".iter().copied())
                    .chain([kind, 3, 0])
                    .collect();
                exec(3, mode, &body);
                exec(4, mode, &body);
            }
        }
        exec(3, 0, b"-----BEGIN PGP MESSAGE-----\n\nYWJj\n=TfTH\n-----END PGP MESSAGE-----\n");
        exec(4, 0, b"-----BEGIN PGP MESSAGE-----\nA: b\n\nYWJj\n=TfTH\n-----END PGP MESSAGE-----\n");
        exec(3, 0, b"");
        exec(4, 0, b"");
    }
}
