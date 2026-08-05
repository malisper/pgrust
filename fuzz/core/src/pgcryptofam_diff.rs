//! pgcryptofam_diff: differential fuzz driver for contrib/pgcrypto's
//! crypt() / gen_salt() / armor / digest / hmac family vs verbatim vendored
//! PostgreSQL 18.3 C (csrc/pgcryptofam/, upstream sha 62d6c7d3df; lane
//! p1-pgcryptofam). The C-side FFI surface is `crate::pgcryptofam`; this file
//! is the comparison logic.
//!
//! THE PGRUST SIDE IS ALWAYS THE SHIPPED `fc_*` WRAPPER, reached through
//! `dfmgr::load_external_function("pgcrypto", ...)`. The driver never calls a
//! core directly and the pgcrypto crate's module visibility is UNCHANGED —
//! widening a product's API surface for a harness is a change to the product
//! for the test's sake (coordinator ruling, 2026-08-02). Going through the
//! wrappers also buys a plane a core-level driver would miss: the wrappers'
//! own error translations (`crypt_err`'s 39000 NULL path, `px_err`'s 22023,
//! `Cannot use "%s": %s`) are compared against the C wrapper bodies the
//! oracle entries transcribe verbatim.
//!
//! Input encoding: `data[0] % 6` = arm, `data[1]` = shape/mode byte,
//! `data[2..]` = payload (two-field splits carry an explicit length byte).
//!
//! Selector = data[0] % 6:
//!
//!   0 crypt        — fc `pg_crypt`(password, setting) vs
//!                    pg_diff_pgcryptofam_crypt (px_crypt over the verbatim
//!                    des/xdes/md5/bcrypt/sha-crypt engines + pg_crypt's own
//!                    39000 "crypt(3) returned NULL" translation).
//!                    COST-BOUNDED, see below.
//!   1 gen_salt     — fc `pg_gen_salt` / `pg_gen_salt_rounds`(algo, rounds)
//!                    vs pg_diff_pgcryptofam_gen_salt. ENTROPY-CARVED, see
//!                    DOMAIN CARVES; the rounds-validation behavior (xdes
//!                    [1,0xFFFFFF] + even-count refusal + PX_XDES_ROUNDS=725,
//!                    bf [4,31], sha [1000,999999999], unknown algo,
//!                    pg_strcasecmp matching) is fully compared.
//!   2 armor        — fc `pg_armor`(data, keys[], values[]) vs
//!                    pg_diff_pgcryptofam_armor, PLUS the SQL-array header
//!                    validation plane: the wrapper's `parse_key_value_arrays`
//!                    against a transcription of pgp-pgsql.c:772-834's check
//!                    order (the only driver-side model in the target — see
//!                    `c_model_validate`).
//!   3 dearmor      — fc `pg_dearmor`(text) vs pg_diff_pgcryptofam_dearmor
//!                    (+ pg_dearmor's px_THROW_ERROR translation).
//!   4 digest       — fc `pg_digest`(data, name) vs
//!                    pg_diff_pgcryptofam_digest (find_digest_provider ->
//!                    downcase_truncate_identifier -> px_find_digest ->
//!                    px_md_*).
//!   5 hmac         — fc `pg_hmac`(data, key, name) vs
//!                    pg_diff_pgcryptofam_hmac (verbatim px-hmac.c).
//!
//! COMPARISON PLANES (the harness contract):
//!   P1 VALUE      — exact output image (hash string / salt string / armored
//!                   bytes / decoded bytes / digest / mac).
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
//!                   captured through elog's emit_log_hook (`record_notice`);
//!                   `notice_plane_is_live` fences it against going vacuous.
//! Message TEXT is out of scope for comparison; it is captured and printed in
//! the panic message for triage only.
//!
//! COST BOUNDING (mandatory — this is what keeps the harness alive AND what
//! keeps the fleet floor inside its deadline; crypto targets are slow and a
//! loose bound is how a floor under-runs).
//! Before EITHER side runs, arm 0 calls `pg_diff_pgcryptofam_cost_probe` and
//! SKIPS the exec — symmetrically, both sides, and counted in
//! [`cost_skips`] — unless the parsed work is at or under:
//!   * bcrypt  cost PINNED TO 04   (2^4 = 16 key schedules; every higher
//!                                  cost, up to 2^31, is refused)
//!   * shacrypt rounds PINNED TO 1000 (crypt-sha's own parser clamps every
//!                                  below-range value UP to MIN = 1000, so
//!                                  `cost <= 1000` accepts exactly the
//!                                  clamped-to-minimum band — where the
//!                                  D6/D12 NOTICE evidence lives — and
//!                                  refuses the 5000 default and above)
//!   * xdes    count <= 255        (of a 0xFFFFFF domain)
//! DES and md5 are constant work (25 and 1000 iterations) and are unbounded.
//! The decision is made from the PROBE ALONE, before either implementation is
//! touched, so the skip can never be asymmetric — an asymmetric skip fakes
//! agreement. The bound holds even if the D12 product fix were reverted: the
//! probe parses the setting the way the vendored preambles do and never runs
//! crypt work, so the oracle does not depend on the fix to terminate.
//!
//! D12 IS DELIBERATELY OUT OF THIS TARGET. `rounds >= 2^31` wedges any
//! in-process harness BY DESIGN: C clamps to 1000, and the pre-fix Rust ran
//! 999,999,999 rounds uninterruptibly, so a regressed clamp would hang the
//! fuzzer instead of failing it. Witnessing D12 needs a child-process +
//! SIGKILL timeout rig, not a fuzz arm; the in-tree witness is
//! `pgcrypto::crypt::tests::shacrypt_rounds_out_of_range_clamps_like_c`
//! (bounded by a finite CHECK_FOR_INTERRUPTS budget). Every rounds > 1000
//! setting is a counted skip here.
//!
//! EXHAUSTIVE-DIFF SWEEPS, NOT FUZZ ARMS. `to64`, `bf_encode`, `bf_decode`,
//! `ascii_to_bin` and the xdes count encode have domains at or under ~2^32,
//! so per the campaign's decision cascade they are ENUMERATED in
//! `pgcryptofam_sweeps.rs` (total over the domain, stronger than any fuzz
//! floor) instead of being sampled here. That file documents which of them
//! the shipped-wrapper-only route can and cannot address.
//!
//! DOMAIN CARVES (harness/caller contract, never pgrust behavior):
//!   - arm 0 byte domain: since D21 (817f379310d) `fc_pg_crypt` carries
//!     password, setting, and result as RAW BYTES end to end, exactly like
//!     C's text_to_cstring -> px_crypt -> cstring_to_text. The driver hands
//!     both sides IDENTICAL bytes, NUL-sanitized ONLY (0x00 -> 0x01: PG
//!     `text` can never carry NUL in any server encoding, and the oracle's
//!     frame_to_cstring would truncate C-side alone). Invalid UTF-8 STAYS in
//!     the domain — non-UTF-8 passwords and settings are precisely what
//!     witness a D21 regression (D11 also needs password bytes >= 0x80) —
//!     and crypt outputs are compared byte for byte (see DIVERGENCE 2).
//!   - arm 1 algo / arm 4/5 name text domain: those wrappers still reach
//!     their cores through `String::from_utf8_lossy`, so the driver
//!     materializes ONE byte string per field (NUL-sanitized 0x00 -> 0x01,
//!     then lossy-decoded to UTF-8) and hands the IDENTICAL bytes to both
//!     sides.
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
//!     PG `text` cannot carry NUL. Arm 3 (pgp_armor_decode) is length-based
//!     on BOTH sides (memchr / slice iteration) and keeps raw bytes.
//!   - arms 0/1 need the thread's GUC store: `fc_pg_crypt` /
//!     `fc_pg_gen_salt*` open with `check_builtin_crypto()`, which PANICS
//!     ("GUC store not initialized") without one. `guc_store_ready()` brings
//!     it up once per thread (THREAD-LOCAL — it installs no process-global
//!     seam, so it cannot poison a sibling lane's `Once`); if it cannot, the
//!     two arms return without executing and are counted in [`fc_skips`].
//!     `fc_plane_is_live` fences that against silent degradation.
//!   - CHECK_FOR_INTERRUPTS is installed as a never-interrupting no-op. That
//!     is the SYMMETRIC environment: the C oracle's shim CFI is a no-op too.
//!     Cancellability (D19) is witnessed by pgcrypto's own `arm_cfi` tests.
//!
//! SURFACE NOT COVERED BY THIS TARGET (stated, not hidden):
//!   `pgp_armor_headers` / `extract_armor_headers`. Its only shipped entry
//!   point is `fc_pgp_armor_headers`, a MATERIALIZE-SRF wrapper:
//!   `InitMaterializedSRF(.., flags = 0)` resolves its tupdesc through
//!   `get_call_result_type` -> pg_proc, which needs the executor's
//!   syscache/tuplestore fixtures — the SRF-engine surface every sibling
//!   lane carves. With product visibility unchanged there is no non-SRF way
//!   in, so this target does not cover it and does not pretend to. The C
//!   oracle entry `pg_diff_pgcryptofam_armor_headers` exists and is
//!   smoke-anchored in `pgcryptofam.rs`; routing a pgrust side to it needs
//!   either a pinned pg_proc fixture or a proof, and is owed elsewhere.

#![allow(dead_code)]

use std::cell::RefCell;
use std::sync::Once;

use datum::{Datum, NullableDatum};
use types_error::{PgError, SqlState};
use types_fmgr::{LocalFcinfo, PGFunction};

use crate::pgcryptofam::{
    c_armor, c_crypt_status, c_dearmor, c_digest, c_gen_salt_status, c_hmac, cost_probe,
    PgcryptofamKind, PgcryptofamStatus,
};

// itoa64 (crypt-gensalt.c / crypt-md5.c), the alphabet every generated salt
// tail must live in.
const ITOA64: &[u8; 64] = b"./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

// ---------------------------------------------------------------------------
// COST BOUND (see the banner)
// ---------------------------------------------------------------------------

/// bcrypt pinned to cost 04: `cost_probe` reports `1 << N`, and the vendored
/// preamble only ever yields N >= 4, so this accepts exactly `$2a$04$` /
/// `$2x$04$` and refuses 05..31.
const BF_MAX_COST_ITERS: i64 = 1 << 4;
/// shacrypt pinned to 1000 (see the banner).
const SHA_MAX_ROUNDS: i64 = 1_000;
/// xdes capped at 255 of a 0xFFFFFF domain (the encoder's full domain is
/// swept exhaustively elsewhere; here only the DECODE path needs exercising).
const XDES_MAX_COUNT: i64 = 255;

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

/// Number of execs whose arm was skipped for want of a GUC store (arms 0/1).
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
/// below NOTICE or elog's policy never reaches the emit hook at all (the boot
/// default is WARNING) — a silently unarmed hook would make P4 vacuous, which
/// `notice_plane_is_live` fences.
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
// fc-wrapper plumbing — the ONLY route to the pgrust side
// ---------------------------------------------------------------------------

pub(crate) fn seams_setup() {
    static SEAMS: Once = Once::new();
    SEAMS.call_once(|| {
        use std::panic::catch_unwind;
        // First-wins across lanes sharing one test binary (seam set() panics
        // on a second install; every impl below is the standard environment
        // the sibling lanes install too).
        let _ = catch_unwind(pgcrypto::init_seams);
        // ENVIRONMENT SEAM, not computation: the bcrypt / sha-crypt / DES
        // cost loops call CHECK_FOR_INTERRUPTS once per round (D19). The C
        // oracle shim's CHECK_FOR_INTERRUPTS is likewise a no-op — there is
        // no signal machinery on either side of this harness — so a
        // never-interrupting impl is the SYMMETRIC environment.
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
/// THREAD-LOCAL, so this is retried per thread and installs no process-global
/// seam (unlike guc/elog/guc_tables::init_seams, which this deliberately does
/// NOT call — those poison a sibling lane's `Once` in the shared test binary).
pub(crate) fn guc_store_ready() -> bool {
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

pub(crate) fn lookup(name: &str) -> PGFunction {
    dfmgr::load_external_function("pgcrypto", name, true)
        .expect("pgcrypto library registered")
        .expect("function resolves")
}

pub(crate) fn fc_call<const N: usize>(
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
pub(crate) fn text_image(bytes: &[u8]) -> Vec<u8> {
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
pub(crate) unsafe fn result_payload<'a>(d: Datum) -> &'a [u8] {
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

/// The single byte string BOTH sides receive for a `text`-typed field whose
/// SHIPPED wrapper still reaches its core through `String::from_utf8_lossy`
/// (arm 1's algo, arm 4/5's hash name): NUL sanitized then lossy-decoded to
/// UTF-8. NOT used by arm 0 — since D21 crypt is raw bytes end to end. See
/// DOMAIN CARVES.
fn text_field(bytes: &[u8]) -> String {
    let sanitized: Vec<u8> = bytes.iter().map(|&b| if b == 0 { 1 } else { b }).collect();
    String::from_utf8_lossy(&sanitized).into_owned()
}

/// NUL-sanitized bytes (no UTF-8 normalization) — the armor header domain,
/// where C uses cstrings but neither side decodes.
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
// DIVERGENCE 1 — px_THROW_ERROR SQLSTATE — FOUND BY THIS TARGET AND FIXED.
//
// Found at plane creation (2026-08-02) against the running verbatim 18.3
// oracle, not a reading of the source. C: `pg_dearmor` (pgp-pgsql.c) ends in
// `px_THROW_ERROR(res)`, and px.c:94-108 raises everything except
// PXE_NO_RANDOM with ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION = 39000.
// pgrust's `px_msg` hardcoded ERRCODE_INVALID_PARAMETER_VALUE = 22023 for
// every px-throwing path — same message text, wrong SQLSTATE, on dearmor,
// pgp_armor_headers and the four pgp sym/pub wrappers.
//
// FIXED in crates/contrib/pgcrypto/src/lib.rs (`px_msg` now mirrors
// px_THROW_ERROR: 39000, or XX000 for the PXE_NO_RANDOM message). The P3
// plane below is therefore UNCARVED — SQLSTATE is compared unconditionally,
// and this comment is the record, not an exception.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// DIVERGENCE 2 — CRYPT VALUE WAS `text` MADE FROM A `String` — FOUND BY THIS
// TARGET (first smoke, 2026-08-02) AND FIXED (D21, main 817f379310d).
//
// C's crypt() result BEGINS WITH A VERBATIM COPY OF THE SETTING BYTES:
// crypt-des.c copies setting[0..2] (traditional) / setting[0..9] (xdes) and
// crypt-md5.c re-emits the raw salt run, then appends an all-itoa64 hash.
// pgrust's crypt cone used to launder password, salt, and those copied
// result bytes through `String::from_utf8_lossy`, so a setting whose copied
// prefix truncated a multibyte sequence was U+FFFD-substituted pgrust-side
// while C returned the raw bytes:
//   crypt('foox', <U+FFFD>.)  C -> EF BF + "jiOTA4TpMRw"  (13 bytes)
//                   pre-D21 Rust -> EF BF BD + same 11     (14 bytes)
// D21 rewrote the cone to `&[u8] -> Vec<u8>` end to end (`fc_pg_crypt`
// passes pw.data()/salt.data() raw), so the value plane below is UNCARVED —
// crypt outputs are compared BYTE FOR BYTE, both the raw input domain and
// the raw output comparison live here. The interim `crypt_value_matches`
// lossy-image mask is deleted per its own stated fix gate (task #145): with
// it in place, a regression of D21 would have been invisible. The
// `crypt_value_plane_rejects_the_lossy_image` test is the standing
// must-fail control. This comment is the record, not an exception.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// arm 0: crypt(password, setting)
// ---------------------------------------------------------------------------

/// px_crypt_list prefixes plus the two that are NOT rows (`$2b$`, `$2y$`) and
/// a bare/garbage arm — the fuzzer reaches every engine through the mode byte
/// instead of having to synthesize `$2a$` from scratch.
const SETTING_PREFIXES: [&str; 12] = [
    "", "$1$", "$5$", "$6$", "$2a$", "$2x$", "$2$", "$2b$", "$2y$", "_", "$5$rounds=", "$7$",
];

fn run_crypt(r: &mut Rdr, mode: u8) {
    // RAW BYTES both sides, NUL-sanitized only (D21 — see DOMAIN CARVES).
    let pwlen = r.u8() as usize % 64;
    let pw = nul_free(r.bytes(pwlen));
    let prefix = SETTING_PREFIXES[(mode >> 1) as usize % SETTING_PREFIXES.len()];
    let mut setting = prefix.as_bytes().to_vec();
    setting.extend_from_slice(&nul_free(r.rest()));
    // Triage-only lossy images for panic TEXT; every comparison below is on
    // the raw bytes.
    let pw_d = String::from_utf8_lossy(&pw);
    let setting_d = String::from_utf8_lossy(&setting);

    // ---- COST BOUND: probe first, decide before EITHER side runs ----
    let (kind, cost) = cost_probe(&setting);
    if !cost_within_bound(kind, cost) {
        COST_SKIPS.with(|c| *c.borrow_mut() += 1);
        return;
    }
    if !guc_store_ready() {
        FC_SKIPS.with(|c| *c.borrow_mut() += 1);
        return;
    }

    let mut out = vec![0u8; 1024];
    drop(take_notices());
    // `c_crypt_status` keeps the SUCCESS-path status too: crypt-sha's clamp
    // NOTICE rides a call that returns normally, and the plain `Result`
    // wrapper drops the status on Ok. ONE exec per side, always.
    let (cn, cst) = c_crypt_status(&pw, &setting, &mut out);
    let cval: Option<&[u8]> = cn.map(|n| &out[..n]);
    let cnotices: Vec<String> = if cst.notice_count > 0 {
        vec![cst.notice_str().to_string()]
    } else {
        Vec::new()
    };

    let ctx = mcx::MemoryContext::new("pgcryptofam_fc");
    let pwi = text_image(&pw);
    let sti = text_image(&setting);
    let fc = fc_call(
        lookup("pg_crypt"),
        ctx.mcx(),
        [
            Datum::from_usize(pwi.as_ptr() as usize),
            Datum::from_usize(sti.as_ptr() as usize),
        ],
    );
    let rnotices: Vec<String> = take_notices().into_iter().map(|(_, m)| m).collect();

    // ---- P4: NOTICE plane ----
    assert_eq!(
        cnotices.is_empty(),
        rnotices.is_empty(),
        "crypt({pw_d:?},{setting_d:?}) NOTICE presence: C {cnotices:?} vs Rust {rnotices:?}"
    );
    if let (Some(c), Some(rn)) = (cnotices.first(), rnotices.first()) {
        assert_eq!(
            numbers_in(c),
            numbers_in(rn),
            "crypt({pw_d:?},{setting_d:?}) NOTICE numbers: C {c:?} vs Rust {rn:?}"
        );
    }

    // ---- P1/P2/P3 ----
    match (cval, fc) {
        (Some(cv), Ok(d)) => {
            // SAFETY: fc_pg_crypt returns a live text varlena in ctx.
            let rv = unsafe { result_payload(d) };
            // BYTE-EXACT, like every other value plane in this target. C's
            // result can carry non-UTF-8 setting-prefix bytes and pgrust
            // must reproduce them verbatim (D21; see DIVERGENCE 2).
            assert_eq!(rv, cv, "crypt({pw_d:?},{setting_d:?}) value");
        }
        (None, Err(e)) => assert_eq!(
            e.sqlstate.0,
            cst.sqlstate,
            "crypt({pw_d:?},{setting_d:?}) SQLSTATE: Rust {:?}/{} vs {}",
            e.message,
            e.sqlstate.0,
            oracle_note(&cst)
        ),
        (Some(cv), Err(e)) => panic!(
            "crypt({pw_d:?},{setting_d:?}): C ok {:?}, Rust errored {:?}/{}",
            String::from_utf8_lossy(cv),
            e.message,
            e.sqlstate.0
        ),
        (None, Ok(_)) => panic!(
            "crypt({pw_d:?},{setting_d:?}): Rust ok, {}",
            oracle_note(&cst)
        ),
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
    if !guc_store_ready() {
        FC_SKIPS.with(|c| *c.borrow_mut() += 1);
        return;
    }
    let algo = if mode & 1 == 0 {
        SALT_ALGOS[(mode >> 4) as usize % SALT_ALGOS.len()].to_string()
    } else {
        let n = r.u8() as usize % 24;
        text_field(r.bytes(n))
    };
    // mode bit 2 picks the ONE-argument wrapper, which pins rounds to 0 on the
    // pgrust side; the oracle is then handed rounds = 0 too, so the two sides
    // stay on the same input.
    let one_arg = mode & 4 == 0;
    let rounds = if one_arg {
        0
    } else if mode & 2 == 0 {
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
    let cval: Option<&[u8]> = cn.map(|n| &out[..n]);

    let ctx = mcx::MemoryContext::new("pgcryptofam_fc");
    let ai = text_image(algo.as_bytes());
    let fc = if one_arg {
        fc_call(
            lookup("pg_gen_salt"),
            ctx.mcx(),
            [Datum::from_usize(ai.as_ptr() as usize)],
        )
    } else {
        fc_call(
            lookup("pg_gen_salt_rounds"),
            ctx.mcx(),
            [
                Datum::from_usize(ai.as_ptr() as usize),
                Datum::from_i32(rounds),
            ],
        )
    };
    drop(take_notices());

    match (cval, fc) {
        (Some(cv), Ok(d)) => {
            // SAFETY: fc_pg_gen_salt* returns a live text varlena in ctx.
            let rv = unsafe { result_payload(d) };
            // ---- P1 (carved): length + deterministic prefix + alphabet ----
            assert_eq!(
                rv.len(),
                cv.len(),
                "gen_salt({algo:?},{rounds}) length: C {:?} vs Rust {:?}",
                String::from_utf8_lossy(cv),
                String::from_utf8_lossy(rv)
            );
            let plen = deterministic_prefix_len(cv);
            assert_eq!(
                &rv[..plen],
                &cv[..plen],
                "gen_salt({algo:?},{rounds}) deterministic prefix: C {:?} vs Rust {:?}",
                String::from_utf8_lossy(cv),
                String::from_utf8_lossy(rv)
            );
            for (i, &b) in rv[plen..].iter().enumerate() {
                assert!(
                    ITOA64.contains(&b),
                    "gen_salt({algo:?},{rounds}) random tail byte {i} = {b:#04x} \
                     is off the itoa64 alphabet (Rust {:?})",
                    String::from_utf8_lossy(rv)
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
        (None, Err(e)) => assert_eq!(
            e.sqlstate.0,
            st.sqlstate,
            "gen_salt({algo:?},{rounds}) SQLSTATE: Rust {:?}/{} vs {}",
            e.message,
            e.sqlstate.0,
            oracle_note(&st)
        ),
        (Some(cv), Err(e)) => panic!(
            "gen_salt({algo:?},{rounds}): C ok {:?}, Rust errored {:?}",
            String::from_utf8_lossy(cv),
            e.message
        ),
        (None, Ok(_)) => panic!("gen_salt({algo:?},{rounds}): Rust ok, {}", oracle_note(&st)),
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
///
/// NB: C's two NULL-element checks (ERRCODE_NULL_VALUE_NOT_ALLOWED) are out
/// of the fuzz domain — the driver never builds a text[] with NULLs.
/// pgcrypto's own `armor_header_tests` cover both.
fn c_model_validate(keys: &[Vec<u8>], values: &[Vec<u8>]) -> Option<(&'static str, SqlState)> {
    use types_error::{ERRCODE_ARRAY_SUBSCRIPT_ERROR, ERRCODE_INVALID_PARAMETER_VALUE};
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

    // ---- P1/P2: armored value, C oracle vs the shipped wrapper ----
    let pairs: Vec<(&[u8], &[u8])> = keys
        .iter()
        .zip(values.iter())
        .map(|(k, v)| (&k[..], &v[..]))
        .collect();
    let mut out = vec![0u8; data.len() * 2 + 4096 + nheaders * 64];
    let n = c_armor(&data, &pairs, &mut out).expect("pgp_armor_encode never raises");
    let cval = &out[..n];

    let fcd = fc.expect("checked Ok above");
    // SAFETY: fc_pg_armor returns a live bytea varlena in ctx.
    let fcv = unsafe { result_payload(fcd) };
    assert_eq!(
        fcv,
        cval,
        "fc pg_armor(datalen={}, {nheaders} headers) value",
        data.len()
    );
}

// ---------------------------------------------------------------------------
// arm 3: dearmor(text)
// ---------------------------------------------------------------------------

/// Build an armored envelope with the C encoder, then apply one fuzz-chosen
/// mutation. Without this the decode arm almost never reaches past the header
/// scan; with it, it reaches the base64/CRC/header-split interiors.
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
        // C's encoder emits "key: value\n"; a key already containing ": " or
        // a newline would produce a stream the SQL surface can never make
        // (arm 2 covers those rejections).
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
    match r.u8() % 6 {
        0 => {}
        1 => {
            // flip one bit
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
            assert_eq!(
                rv,
                &out[..*n],
                "dearmor({:?}) value",
                String::from_utf8_lossy(&text)
            );
        }
        (Err(st), Err(e)) => {
            assert_eq!(
                e.sqlstate.0,
                st.sqlstate,
                "dearmor({:?}) SQLSTATE: Rust {:?} vs {}",
                String::from_utf8_lossy(&text),
                e.message,
                oracle_note(st)
            );
            assert_eq!(
                e.message,
                st.msg_str(),
                "dearmor({:?}) message",
                String::from_utf8_lossy(&text)
            );
        }
        (Ok(n), Err(e)) => panic!(
            "dearmor({:?}): C ok ({n} bytes), Rust errored {:?}",
            String::from_utf8_lossy(&text),
            e.message
        ),
        (Err(st), Ok(_)) => panic!(
            "dearmor({:?}): Rust ok, {}",
            String::from_utf8_lossy(&text),
            oracle_note(st)
        ),
    }
}

// ---------------------------------------------------------------------------
// arms 4/5: digest(data, type) and hmac(data, key, type)
// ---------------------------------------------------------------------------

/// Every name `px_find_digest` resolves, plus case variants (C downcases via
/// `downcase_truncate_identifier`, pgrust via `to_ascii_lowercase`) and the
/// misses that drive `Cannot use "%s": No such hash algorithm`.
const HASH_NAMES: [&str; 16] = [
    "md5", "sha1", "sha224", "sha256", "sha384", "sha512", "MD5", "SHA256", "Sha512", "crc32", "",
    "sha", "md", "sha2", "sha1 ", " md5",
];

fn hash_name(r: &mut Rdr, mode: u8) -> String {
    if mode & 1 == 0 {
        HASH_NAMES[(mode >> 2) as usize % HASH_NAMES.len()].to_string()
    } else {
        let n = r.u8() as usize % 80; // spans NAMEDATALEN-1 = 63 truncation
        text_field(r.bytes(n))
    }
}

fn run_digest(r: &mut Rdr, mode: u8) {
    let name = hash_name(r, mode);
    let data = r.rest().to_vec();

    let mut out = vec![0u8; 256];
    let cres = c_digest(name.as_bytes(), &data, &mut out);

    let ctx = mcx::MemoryContext::new("pgcryptofam_fc");
    let di = text_image(&data);
    let ni = text_image(name.as_bytes());
    // fc_pg_digest(arg0 = data, arg1 = type)
    let fc = fc_call(
        lookup("pg_digest"),
        ctx.mcx(),
        [
            Datum::from_usize(di.as_ptr() as usize),
            Datum::from_usize(ni.as_ptr() as usize),
        ],
    );
    match (&cres, fc) {
        (Ok(n), Ok(d)) => {
            // SAFETY: fc_pg_digest returns a live bytea varlena in ctx.
            let rv = unsafe { result_payload(d) };
            assert_eq!(rv, &out[..*n], "digest({name:?}, {} bytes) value", data.len());
        }
        (Err(st), Err(e)) => assert_eq!(
            e.sqlstate.0,
            st.sqlstate,
            "digest({name:?}) SQLSTATE: Rust {:?}/{} vs {}",
            e.message,
            e.sqlstate.0,
            oracle_note(st)
        ),
        (Ok(_), Err(e)) => panic!("digest({name:?}): C ok, Rust errored {:?}", e.message),
        (Err(st), Ok(_)) => panic!("digest({name:?}): Rust ok, {}", oracle_note(st)),
    }
}

fn run_hmac(r: &mut Rdr, mode: u8) {
    let name = hash_name(r, mode);
    // Key lengths straddle both block sizes (64 and 128) so the
    // key-longer-than-B hashing branch and the zero-pad branch both run.
    let keylen = r.u8() as usize % 160;
    let key = r.bytes(keylen).to_vec();
    let data = r.rest().to_vec();

    let mut out = vec![0u8; 256];
    let cres = c_hmac(name.as_bytes(), &key, &data, &mut out);

    let ctx = mcx::MemoryContext::new("pgcryptofam_fc");
    let di = text_image(&data);
    let ki = text_image(&key);
    let ni = text_image(name.as_bytes());
    // fc_pg_hmac(arg0 = data, arg1 = key, arg2 = type)
    let fc = fc_call(
        lookup("pg_hmac"),
        ctx.mcx(),
        [
            Datum::from_usize(di.as_ptr() as usize),
            Datum::from_usize(ki.as_ptr() as usize),
            Datum::from_usize(ni.as_ptr() as usize),
        ],
    );
    match (&cres, fc) {
        (Ok(n), Ok(d)) => {
            // SAFETY: fc_pg_hmac returns a live bytea varlena in ctx.
            let rv = unsafe { result_payload(d) };
            assert_eq!(
                rv,
                &out[..*n],
                "hmac({name:?}, keylen {}, {} bytes) value",
                key.len(),
                data.len()
            );
        }
        (Err(st), Err(e)) => assert_eq!(
            e.sqlstate.0,
            st.sqlstate,
            "hmac({name:?}) SQLSTATE: Rust {:?}/{} vs {}",
            e.message,
            e.sqlstate.0,
            oracle_note(st)
        ),
        (Ok(_), Err(e)) => panic!("hmac({name:?}): C ok, Rust errored {:?}", e.message),
        (Err(st), Ok(_)) => panic!("hmac({name:?}): Rust ok, {}", oracle_note(st)),
    }
}

// ---------------------------------------------------------------------------
// entry
// ---------------------------------------------------------------------------

pub fn pgcryptofam_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    if data.len() < 2 {
        return;
    }
    seams_setup();
    let sel = data[0] % 6;
    let mode = data[1];
    let mut r = Rdr::new(&data[2..]);
    match sel {
        0 => run_crypt(&mut r, mode),
        1 => run_gen_salt(&mut r, mode),
        2 => run_armor(&mut r, mode),
        3 => run_dearmor(&mut r, mode),
        4 => run_digest(&mut r, mode),
        _ => run_hmac(&mut r, mode),
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
        assert!(guc_store_ready(), "GUC store did not come up");
        drop(take_notices());
        let ctx = mcx::MemoryContext::new("pgcryptofam_notice_probe");
        let pw = text_image(b"pw");
        let st = text_image(b"$5$rounds=10$abcdefgh");
        fc_call(
            lookup("pg_crypt"),
            ctx.mcx(),
            [
                Datum::from_usize(pw.as_ptr() as usize),
                Datum::from_usize(st.as_ptr() as usize),
            ],
        )
        .expect("clamped shacrypt succeeds");
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
        let (_, cst) = c_crypt_status(b"pw", b"$5$rounds=10$abcdefgh", &mut out);
        assert_eq!(cst.notice_count, 1, "C NOTICE not recorded");
        assert_eq!(numbers_in(cst.notice_str()), vec![10, 1000, 1000]);
    }

    /// Must-fail control for the task #145 de-masking: the retired
    /// `crypt_value_matches` compared pgrust's output against the LOSSY
    /// IMAGE of C's raw bytes whenever those bytes were not UTF-8 — which
    /// accepted exactly the pre-D21 defect. This pins, on a live C-oracle
    /// vector: (a) the D21 seed really produces non-UTF-8 C output, (b) the
    /// shipped fc plane reproduces it BYTE FOR BYTE, and (c) the pre-D21
    /// lossy image DIFFERS from those raw bytes — so `run_crypt`'s
    /// byte-exact plane fails on a D21 regression where the old mask
    /// (`rust == from_utf8_lossy(c)` on the invalid-UTF-8 arm) passed.
    #[test]
    fn crypt_value_plane_rejects_the_lossy_image() {
        seams_setup();
        assert!(guc_store_ready(), "GUC store did not come up");
        // trad-DES copies setting[0..2] = EF BF (truncated multibyte).
        let (pw, setting): (&[u8], &[u8]) = (b"foox", b"\xef\xbf\xbd.");
        let mut out = vec![0u8; 1024];
        let (cn, _) = c_crypt_status(pw, setting, &mut out);
        let cv = &out[..cn.expect("C crypt succeeds on the D21 seed")];
        assert!(
            std::str::from_utf8(cv).is_err(),
            "seed no longer exercises the non-UTF-8 output plane: {cv:?}"
        );
        let ctx = mcx::MemoryContext::new("pgcryptofam_d21_probe");
        let pwi = text_image(pw);
        let sti = text_image(setting);
        let d = fc_call(
            lookup("pg_crypt"),
            ctx.mcx(),
            [
                Datum::from_usize(pwi.as_ptr() as usize),
                Datum::from_usize(sti.as_ptr() as usize),
            ],
        )
        .expect("fc pg_crypt succeeds on the D21 seed");
        // SAFETY: fc_pg_crypt returns a live varlena in ctx.
        let rv = unsafe { result_payload(d) };
        assert_eq!(rv, cv, "D21 regressed: crypt output is not C's raw bytes");
        let lossy = String::from_utf8_lossy(cv).into_owned().into_bytes();
        assert_ne!(
            lossy.as_slice(),
            cv,
            "control lost its teeth: the lossy image equals the raw bytes"
        );
    }

    /// Every plane's pgrust side must actually resolve through dfmgr, and the
    /// GUC store must come up, or arms 0/1 silently degrade to zero execs.
    #[test]
    fn fc_plane_is_live() {
        seams_setup();
        assert!(guc_store_ready(), "GUC store did not come up: arms 0/1 are dead");
        let before = fc_skips();
        exec(0, 2, b"\x04fooxSzzz0yzz"); // $1$ crypt, succeeds
        exec(0, 12, b"\x04foox"); // $2$ -> C 39000 "crypt(3) returned NULL"
        exec(1, 0b1000_0100, b""); // gen_salt('des', 0)
        assert_eq!(fc_skips(), before, "an arm skipped for want of a GUC store");
        // `lookup` panics on a dfmgr miss, so calling it IS the assertion.
        for f in [
            "pg_crypt",
            "pg_gen_salt",
            "pg_gen_salt_rounds",
            "pg_armor",
            "pg_dearmor",
            "pg_digest",
            "pg_hmac",
        ] {
            lookup(f);
        }
    }

    /// The cost bound must actually fire, at the RETUNED pins, and must not
    /// refuse the constant-work engines.
    #[test]
    fn cost_bound_fires_at_the_retuned_pins() {
        seams_setup();
        let before = cost_skips();
        // bcrypt: 04 accepted, 05 and 31 refused (pinned to 04)
        exec(0, 8, b"\x02pw04$......................");
        assert_eq!(cost_skips(), before, "bcrypt cost 04 was wrongly refused");
        exec(0, 8, b"\x02pw05$......................");
        assert_eq!(cost_skips(), before + 1, "bcrypt cost 05 was not refused");
        exec(0, 8, b"\x02pw31$abcdefghijklmnopqrstuv");
        assert_eq!(cost_skips(), before + 2, "bcrypt cost 31 was not refused");
        // shacrypt: 1000 accepted, 1001 / default 5000 / 999999999 refused
        exec(0, 20, b"\x02pw1000$abcdefgh");
        assert_eq!(cost_skips(), before + 2, "sha rounds 1000 was wrongly refused");
        exec(0, 20, b"\x02pw1001$abcdefgh");
        assert_eq!(cost_skips(), before + 3, "sha rounds 1001 was not refused");
        exec(0, 4, b"\x02pwabcdefgh"); // $5$abcdefgh -> the 5000 default
        assert_eq!(cost_skips(), before + 4, "sha default 5000 was not refused");
        exec(0, 20, b"\x02pw999999999$abcdefgh");
        assert_eq!(cost_skips(), before + 5, "sha rounds 999999999 not refused");
        // xdes: '_' + 4 count chars, little-endian 6-bit groups.
        exec(0, 18, b"\x08passwordz3..abcd"); // 63 | 5<<6 = 383 > 255
        assert_eq!(cost_skips(), before + 6, "xdes count 383 was not refused");
        exec(0, 18, b"\x08passwordz1..abcd"); // 63 | 3<<6 = 255 <= 255
        assert_eq!(cost_skips(), before + 6, "xdes count 255 was wrongly refused");
        // constant-work engines are never refused
        exec(0, 2, b"\x04fooxSzzz0yzz"); // $1$ md5, 1000 iterations
        exec(0, 0, b"\x04fooxrl"); // traditional DES, 25 iterations
        assert_eq!(cost_skips(), before + 6, "a constant-work engine was refused");
    }

    #[test]
    fn arm_smoke() {
        // ---- arm 0: every px_crypt_list row ----
        for prefix_sel in 0u8..12 {
            exec(0, prefix_sel << 1, b"\x04foox04$......................");
        }
        exec(0, 2, b"\x04fooxSzzz0yzz"); // $1$
        exec(0, 12, b"\x04foox"); // $2$  -> crypt(3) returned NULL / 39000
        exec(0, 0, b"\x04fooxrl"); // traditional DES
        exec(0, 18, b"\x08passwordz1..abcd"); // xdes, count 255
        exec(0, 20, b"\x04foox1000$abcdefgh"); // $5$rounds=1000$
        exec(0, 20, b"\x04foox$abc"); // $5$rounds=$abc (empty rounds -> clamp NOTICE)
        exec(0, 20, b"\x04foox0$abc"); // $5$rounds=0$abc (clamp NOTICE)
        exec(0, 0, b"\x04foox"); // empty setting -> invalid salt
        // D21 regression seeds (DIVERGENCE 2): trad-DES copies setting[0..2]
        // = EF BF — a truncated multibyte sequence — VERBATIM into the
        // result, so the byte-exact value plane runs over non-UTF-8 output...
        exec(0, 0, b"\x04foox\xef\xbf\xbd.");
        // ...and the raw input plane: non-UTF-8 setting and password bytes
        // stay in the domain un-laundered (pre-D21 these were lossy-collapsed).
        exec(0, 0, b"\x04foox\xff.");
        exec(0, 0, b"\x04fo\xffxab");

        // ---- arm 1: every gen_list row + boundaries, both wrappers ----
        for algo in 0u8..10 {
            for rc in 0u8..16 {
                exec(1, (algo << 4) | (rc << 2) | 4, b""); // pg_gen_salt_rounds
                exec(1, (algo << 4) | (rc << 2), b""); // pg_gen_salt (rounds 0)
            }
        }
        exec(1, 1 | 2 | 4, b"\x04xdes\x01\x00\x00\x00"); // free-form algo + raw rounds

        // ---- arm 2: armor with and without headers ----
        exec(2, 0, b"hello pgcrypto");
        exec(2, 1, b"\x02\x07\x03Version1.0\x07\x02Commenthidata");
        // D8 shapes: newline in value / newline in key / ": " in key / non-ASCII
        exec(2, 1, b"\x01\x01\x0ckv\nForged: h");
        exec(2, 1, b"\x01\x03\x01k\nxv");
        exec(2, 1, b"\x01\x04\x01k: xv");
        exec(2, 1, b"\x01\x01\x02k\xc3\xa9");

        // ---- arm 3: envelope + every mutation kind ----
        for mode in [0u8, 1] {
            for kind in 0u8..6 {
                let body: Vec<u8> = vec![1, 3, 4, b'K', b'e', b'y', b'v', b'a', b'l', 6]
                    .into_iter()
                    .chain(b"abcdef".iter().copied())
                    .chain([kind, 3, 0])
                    .collect();
                exec(3, mode, &body);
            }
        }
        exec(3, 0, b"-----BEGIN PGP MESSAGE-----\n\nYWJj\n=TfTH\n-----END PGP MESSAGE-----\n");
        exec(3, 0, b"");

        // ---- arms 4/5: digest + hmac over every name and both block sizes ----
        for ni in 0u8..16 {
            exec(4, ni << 2, b"abc");
            exec(5, ni << 2, b"\x10keykeykeykeykeykabc");
        }
        exec(4, 1, b"\x06sha256The quick brown fox");
        exec(5, 1, b"\x03md5\x14aaaaaaaaaaaaaaaaaaaadata"); // key < B
        exec(5, 1, b"\x06sha512\x90"); // keylen 144 > B = 128 -> key is hashed
        exec(4, 0, b""); // md5 of the empty string
        exec(5, 0, b"\x00"); // hmac with an empty key
    }
}
