//! Kani C≡Rust equivalence proofs: PostgreSQL uuid family
//! (uuid_in / uuid_out / uuid_cmp / uuid_eq,ne,lt,le,gt,ge) vs the shipped
//! pgrust `adt_uuid` crate.
//!
//! C side: proofs/uuid/c/pg_uuid.c (vendored verbatim from postgres master
//! src/backend/utils/adt/uuid.c; shims documented there).
//!
//! Input-domain notes:
//! - uuid_in's C signature is a NUL-terminated cstring; the Rust core takes
//!   a byte slice. The shared value domain is "strings without interior
//!   NUL": harnesses assume every symbolic byte != 0 and hand C the same
//!   bytes plus a terminating NUL. Capped at the canonical lengths
//!   (36 hyphenated, 32 bare) plus 38 ({...} braced) — other lengths reject
//!   on both sides by construction of the parser (needs exactly 32 hex
//!   digits + optional separators).
//! - uuid_cmp returns C memcmp()'s raw value whose magnitude is
//!   implementation-defined (sign-only contract, and the btree comparator
//!   contract is sign-only too). Rust ratifies -1/0/1; the harness compares
//!   signum. Ruling documented in c/pg_uuid.c.
//!
//! uuid_hash/uuid_hash_extended + uuid_extract_version/timestamp harnesses
//! need BOTH C files: `--c-lib c/pg_uuid.c ../hash/pg_hashfn.c` (the hash
//! rows compose on the already-vendored hash_bytes C).
//!
//! Run recipe (measured 2026-07-28, Kani 0.67.0):
//!   uuid_out + comparators (fast class, <2s):
//!     timeout 30 cargo kani -Z c-ffi -Z stubbing --c-lib c/pg_uuid.c \
//!         --solver kissat --harness <h>
//!   eq_uuid_in_len32/len36 (~19s each):
//!     timeout 30 cargo kani -Z c-ffi -Z stubbing --no-assertion-reach-checks \
//!         --c-lib c/pg_uuid.c --solver kissat --harness <h>
//!   control_* (both MUST-FAIL controls): DEFAULT solver — suite rule:
//!     controls validate by counterexample and kissat does not terminate
//!     usefully on failing harnesses. Same flags as their green siblings
//!     minus `--solver kissat`.
//!   eq_uuid_in_len38 (~21s; kissat walls >30s here, use the default
//!   incremental solver):
//!     timeout 30 cargo kani -Z c-ffi -Z stubbing --no-assertion-reach-checks \
//!         --c-lib c/pg_uuid.c --harness eq_uuid_in_len38
//!   Reject-path stubs come from the shared proof_support crate
//!   (proofs/support): stub_from_utf8_lossy, stub_format, and the
//!   field-identical stub_pg_error_error (sqlstate left at the shipped
//!   default, so the shipped .with_sqlstate(ERRCODE_INVALID_TEXT_
//!   REPRESENTATION) in invalid_syntax_err stays load-bearing). See their
//!   doc comments for the soundness contracts.
//!   --no-assertion-reach-checks turns off only reachability-coverage
//!   properties (assertions are still verified); accept-path reachability is
//!   witnessed by control_uuid_in_accept_reachable_must_fail. The two
//!   control_* harnesses MUST report VERIFICATION FAILED — that is their
//!   pass condition (non-vacuity of the rig).

use std::os::raw::c_int;

#[repr(C)]
pub struct CPgUuid {
    pub data: [u8; 16],
}

extern "C" {
    fn pg_string_to_uuid(source: *const u8, uuid: *mut CPgUuid) -> c_int;
    fn pg_uuid_out(uuid: *const CPgUuid, buf: *mut u8) -> c_int;
    fn pg_uuid_internal_cmp(a: *const CPgUuid, b: *const CPgUuid) -> c_int;
    fn pg_uuid_lt(a: *const CPgUuid, b: *const CPgUuid) -> c_int;
    fn pg_uuid_le(a: *const CPgUuid, b: *const CPgUuid) -> c_int;
    fn pg_uuid_eq(a: *const CPgUuid, b: *const CPgUuid) -> c_int;
    fn pg_uuid_ge(a: *const CPgUuid, b: *const CPgUuid) -> c_int;
    fn pg_uuid_gt(a: *const CPgUuid, b: *const CPgUuid) -> c_int;
    fn pg_uuid_ne(a: *const CPgUuid, b: *const CPgUuid) -> c_int;
    fn pg_uuid_cmp(a: *const CPgUuid, b: *const CPgUuid) -> c_int;
}

#[cfg(kani)]
mod proofs {
    use super::*;
    use adt_uuid::{uuid_in, uuid_out_into, PgUuid, UUID_OUT_LEN};
    use proof_support::stubs;

    // ---------------- uuid_out ----------------

    /// Every 16-byte uuid formats to identical 36-byte text (and C
    /// NUL-terminates at 36).
    #[kani::proof]
    fn eq_uuid_out() {
        let u: [u8; 16] = kani::any();

        let mut cbuf = [0xAAu8; 37];
        unsafe { pg_uuid_out(&CPgUuid { data: u }, cbuf.as_mut_ptr()) };

        let mut rbuf = [0u8; UUID_OUT_LEN];
        let n = uuid_out_into(&u, &mut rbuf);

        assert!(n == 36);
        assert!(cbuf[36] == 0, "C output not NUL-terminated at 36");
        assert!(cbuf[..36] == rbuf[..], "divergence: uuid_out text differs");
    }

    // ---------------- uuid_in ----------------

    /// Shared driver: `bytes[..len]` is the string value (no interior NUL);
    /// C sees it NUL-terminated. Asserts identical accept/reject verdict
    /// and, on accept, identical parsed bytes.
    fn check_uuid_in<const LEN: usize>(bytes: [u8; LEN]) {
        for i in 0..LEN {
            kani::assume(bytes[i] != 0); // cstring value domain
        }

        let mut cbuf = [0u8; 64];
        cbuf[..LEN].copy_from_slice(&bytes);
        cbuf[LEN] = 0;

        let mut cu = CPgUuid { data: [0xBB; 16] };
        let c_err = unsafe { pg_string_to_uuid(cbuf.as_ptr(), &mut cu) } != 0;

        let r = uuid_in(&bytes, None);

        match r {
            Ok(ru) => {
                assert!(!c_err, "divergence: Rust accepts, C rejects");
                assert!(ru == cu.data, "divergence: parsed uuid bytes differ");
            }
            Err(_) => {
                assert!(c_err, "divergence: Rust rejects, C accepts");
            }
        }
    }

    /// Canonical hyphenated form length (8-4-4-4-12 = 36 chars), all byte
    /// values symbolic: covers the accept path AND every reject path at
    /// this length (bad hex, misplaced hyphens, stray '{', ...).
    /// unwind bound: the Rust reject path builds the error message
    /// (String::from_utf8_lossy + format! over the <=38-byte input) whose
    /// loops are data-dependent — the measured symex-hang trap. 42 > 38+1
    /// covers every loop in scope (parse loop is 16, lossy/format loops are
    /// bounded by input length).
    #[kani::proof]
    #[kani::unwind(42)]
    #[kani::stub(std::string::String::from_utf8_lossy, stubs::stub_from_utf8_lossy)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_uuid_in_len36() {
        check_uuid_in::<36>(kani::any());
    }

    /// Bare 32-hex-digit form, all byte values symbolic.
    #[kani::proof]
    #[kani::unwind(42)]
    #[kani::stub(std::string::String::from_utf8_lossy, stubs::stub_from_utf8_lossy)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_uuid_in_len32() {
        check_uuid_in::<32>(kani::any());
    }

    /// Braced form {8-4-4-4-12} (38 chars), all byte values symbolic:
    /// exercises the braces accept path and its reject variants.
    #[kani::proof]
    #[kani::unwind(42)]
    #[kani::stub(std::string::String::from_utf8_lossy, stubs::stub_from_utf8_lossy)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_uuid_in_len38() {
        check_uuid_in::<38>(kani::any());
    }

    // ---------------- comparators ----------------

    fn sign(x: i32) -> i32 {
        if x < 0 {
            -1
        } else if x > 0 {
            1
        } else {
            0
        }
    }

    /// uuid_cmp: sign-equivalence (memcmp magnitude is implementation-
    /// defined; the btree comparator contract is sign-only).
    #[kani::proof]
    fn eq_uuid_cmp() {
        let a: PgUuid = kani::any();
        let b: PgUuid = kani::any();
        let ca = CPgUuid { data: a };
        let cb = CPgUuid { data: b };

        let c = unsafe { pg_uuid_cmp(&ca, &cb) };
        let r = adt_uuid::uuid_internal_cmp(&a, &b);
        assert!(sign(c) == sign(r), "divergence: uuid_cmp sign differs");

        // Same core, exercised through the exported internal_cmp shim too.
        let c2 = unsafe { pg_uuid_internal_cmp(&ca, &cb) };
        assert!(sign(c2) == sign(r));
    }

    macro_rules! eq_bool_op {
        ($harness:ident, $cfn:ident, $rfn:ident) => {
            #[kani::proof]
            fn $harness() {
                let a: PgUuid = kani::any();
                let b: PgUuid = kani::any();
                let ca = CPgUuid { data: a };
                let cb = CPgUuid { data: b };
                let c = unsafe { $cfn(&ca, &cb) } != 0;
                let r = adt_uuid::$rfn(&a, &b);
                assert!(c == r, "divergence: boolean uuid comparator differs");
            }
        };
    }

    eq_bool_op!(eq_uuid_eq, pg_uuid_eq, uuid_eq);
    eq_bool_op!(eq_uuid_ne, pg_uuid_ne, uuid_ne);
    eq_bool_op!(eq_uuid_lt, pg_uuid_lt, uuid_lt);
    eq_bool_op!(eq_uuid_le, pg_uuid_le, uuid_le);
    eq_bool_op!(eq_uuid_gt, pg_uuid_gt, uuid_gt);
    eq_bool_op!(eq_uuid_ge, pg_uuid_ge, uuid_ge);

    // ---------------- uuid_hash / uuid_hash_extended (oids 2963/3412) ----------------
    // Composition rows: C uuid_hash is hash_any(key->data, UUID_LEN) and the
    // shipped Rust is hashfn::hash_bytes(key) — the C side comes from the
    // already-vendored proofs/hash/pg_hashfn.c (pass BOTH .c files to
    // --c-lib; no new C vendored here). Full symbolic 16-byte image, and
    // (for extended) full symbolic seed.

    extern "C" {
        fn pg_hash_bytes(k: *const u8, keylen: i32) -> u32;
        fn pg_hash_bytes_extended(k: *const u8, keylen: i32, seed: u64) -> u64;
    }

    #[kani::proof]
    fn eq_uuid_hash() {
        let u: [u8; 16] = kani::any();
        let c = unsafe { pg_hash_bytes(u.as_ptr(), 16) };
        let r = adt_uuid::uuid_hash(&u);
        assert!(r == c);
    }

    #[kani::proof]
    fn eq_uuid_hash_extended() {
        let u: [u8; 16] = kani::any();
        let seed: u64 = kani::any();
        let c = unsafe { pg_hash_bytes_extended(u.as_ptr(), 16, seed) };
        let r = adt_uuid::uuid_hash_extended(&u, seed);
        assert!(r == c);
    }

    // ------- uuid_extract_version / uuid_extract_timestamp (6343/6342) -------
    // NULL verdict (non-RFC-9562 variant / versions without a timestamp) +
    // value parity over the full symbolic 16-byte image.

    extern "C" {
        fn pg_uuid_extract_version(data: *const u8, isnull: *mut i32) -> u16;
        fn pg_uuid_extract_timestamp(data: *const u8, isnull: *mut i32) -> i64;
    }

    #[kani::proof]
    fn eq_uuid_extract_version() {
        let u: [u8; 16] = kani::any();
        let mut isnull: i32 = -1;
        let c = unsafe { pg_uuid_extract_version(u.as_ptr(), &mut isnull) };
        match adt_uuid::uuid_extract_version(&u) {
            Some(v) => assert!(isnull == 0 && v == c),
            None => assert!(isnull == 1),
        }
    }

    #[kani::proof]
    fn eq_uuid_extract_timestamp() {
        let u: [u8; 16] = kani::any();
        // All three regimes must be reachable (vacuity insurance).
        kani::cover!((u[8] & 0xc0) == 0x80 && (u[6] >> 4) == 1);
        kani::cover!((u[8] & 0xc0) == 0x80 && (u[6] >> 4) == 7);
        kani::cover!((u[8] & 0xc0) != 0x80);
        let mut isnull: i32 = -1;
        let c = unsafe { pg_uuid_extract_timestamp(u.as_ptr(), &mut isnull) };
        match adt_uuid::uuid_extract_timestamp(&u) {
            Some(ts) => assert!(isnull == 0 && ts == c),
            None => assert!(isnull == 1),
        }
    }

    // ---------------- negative controls ----------------

    /// MUST FAIL: uuid_in accept-path vacuity control. The standing
    /// eq_uuid_in_* recipe runs with --no-assertion-reach-checks, so this
    /// control proves the accept path is actually reachable at the
    /// canonical length: it asserts REJECTION for all len-36 inputs, which
    /// can only fail if some input is accepted by both sides. A PASS here
    /// would mean the eq harnesses' accept-path byte comparison is vacuous.
    #[kani::proof]
    #[kani::unwind(42)]
    #[kani::stub(std::string::String::from_utf8_lossy, stubs::stub_from_utf8_lossy)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn control_uuid_in_accept_reachable_must_fail() {
        let bytes: [u8; 36] = kani::any();
        for i in 0..36 {
            kani::assume(bytes[i] != 0);
        }

        let mut cbuf = [0u8; 64];
        cbuf[..36].copy_from_slice(&bytes);
        cbuf[36] = 0;
        let mut cu = CPgUuid { data: [0xBB; 16] };
        let c_err = unsafe { pg_string_to_uuid(cbuf.as_ptr(), &mut cu) } != 0;
        let r_err = uuid_in(&bytes, None).is_err();

        assert!(
            c_err && r_err,
            "expected failure: some len-36 input is accepted (accept path live)"
        );
    }

    // ---------------- negative control ----------------

    /// MUST FAIL: feeds C and Rust deliberately different uuids and asserts
    /// equal output — proves the rig is non-vacuous (a passing control is a
    /// broken gate).
    #[kani::proof]
    fn control_uuid_out_mismatch_must_fail() {
        let u: [u8; 16] = kani::any();
        let mut v = u;
        v[0] ^= 0x10; // guaranteed different first hex digit

        let mut cbuf = [0u8; 37];
        unsafe { pg_uuid_out(&CPgUuid { data: u }, cbuf.as_mut_ptr()) };

        let mut rbuf = [0u8; UUID_OUT_LEN];
        uuid_out_into(&v, &mut rbuf);

        assert!(
            cbuf[..36] == rbuf[..],
            "expected failure: control mismatch detected (rig is live)"
        );
    }
}

// ===========================================================================
// WAVE 5 (2026-07-28): uuid_recv (2961), uuid_send (2962), and the
// generate_uuidv7 core (the pure assembly behind 6429 uuidv7 / 6430
// uuidv7_interval — their clock plumbing is a later seam lane).
//
// C side: the WAVE 5 section appended to c/pg_uuid.c (verbatim REL_18
// uuid.c bodies; wire shims U1, RNG seam U2, SUBMS host-config choice U3 —
// documented there).
//
//   - uuid_recv: CORE-level (adt_uuid::uuid_recv on a directly-held
//     StringInfo — the datum->StringInfo pointer round-trip WALLED the
//     int-arith wrapper-level recv harnesses, so the fc arg plumbing stays
//     out; ledger wording "core-level (direct StringInfo)").  Full symbolic
//     message bytes + symbolic dlen/cursor; value + cursor + verdict +
//     sqlstate 08P01 parity.
//   - uuid_send: WRAPPER-level over a real result-mcx frame (int-arith
//     send precedent, release-gate tier expected); full 20-byte wire image
//     byte-compared.
//   - generate_uuidv7: RNG-SEAM core — pg_strong_random is stubbed to one
//     shared symbolic 8-byte block fed to both sides (tz-seam pattern), so
//     the theorem quantifies over ALL RNG outputs; sub_ms fenced to the
//     caller contract [0, 1e6) (ns remainder; also keeps the /1e6 divider
//     domain inside the sloped-wall budget); unix_ts_ms fully symbolic.
//     The RNG-failure ereport arm is unreachable under the seam and leaves
//     the proof.  HOST-FLAVOR NOTE (shim U3): on this macOS box both sides
//     compile the 10-bit SUBMS arm (incl. the data[7] ^= data[8] >> 6
//     line); the Linux 12-bit arm needs a linux-host rerun to claim.
//     control_generate_uuidv7_rng_skew (C fed different rand bytes) MUST
//     FAIL — witnesses the seam is load-bearing.
//
// Run recipes in runqueue.txt (mcx-stub harnesses need -Z stubbing).
// ===========================================================================

#[cfg(kani)]
mod wave5 {
    use super::CPgUuid;
    use datum::{Datum, NullableDatum};
    use proof_support::{mcx_stubs, stubs};
    use std::sync::atomic::{AtomicU8, Ordering::Relaxed};
    use types_error::{ERRCODE_PROTOCOL_VIOLATION, ERROR};
    use types_fmgr::LocalFcinfo;

    extern "C" {
        fn pg_uuid_recv(
            data: *const u8,
            len: i32,
            cursor: *mut i32,
            out: *mut CPgUuid,
        ) -> std::os::raw::c_int;
        fn pg_uuid_send(uuid: *const CPgUuid, out: *mut u8) -> i32;
        fn pg_generate_uuidv7(
            unix_ts_ms: u64,
            sub_ms: u32,
            rand8: *const u8,
            out: *mut CPgUuid,
        ) -> std::os::raw::c_int;
    }

    // ---------------- uuid_recv (2961): core, direct StringInfo -----------

    #[kani::proof]
    #[kani::unwind(24)] // copy loops <= CAP+1 (20-byte cap + slack guard)
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    // RVR lesson: grow/deallocate stubs mandatory when append_bytes'
    // try_reserve/grow branch is reachable (real arena + Acct recursion
    // otherwise enters symex).
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_uuid_recv_core() {
        const CAP: usize = 20;
        let data: [u8; CAP] = kani::any();
        let dlen: usize = kani::any();
        kani::assume(dlen <= CAP);
        let cur: usize = kani::any();
        kani::assume(cur <= CAP);

        let mut ccur: i32 = cur as i32;
        let mut cu = CPgUuid { data: [0xBB; 16] };
        let cst = unsafe { pg_uuid_recv(data.as_ptr(), dlen as i32, &mut ccur, &mut cu) };

        let ctx = mcx::MemoryContext::new_bump("kani-uuid-recv");
        let mut si = match stringinfo::StringInfo::with_capacity_in(ctx.mcx(), CAP + 2) {
            Ok(s) => s,
            Err(e) => {
                core::mem::forget(e);
                panic!("stub alloc failed")
            }
        };
        if let Err(e) = si.append_bytes(&data[..dlen]) {
            core::mem::forget(e);
            panic!("append within capacity failed");
        }
        si.cursor = cur;
        match adt_uuid::uuid_recv(&mut si) {
            Ok(u) => {
                assert!(cst == 0);
                assert!(u == cu.data);
                assert!(si.cursor == ccur as usize);
            }
            Err(e) => {
                assert!(cst == 4);
                assert!(e.sqlstate == ERRCODE_PROTOCOL_VIOLATION);
                assert!(e.level == ERROR);
                core::mem::forget(e);
            }
        }
        core::mem::forget(si);
        core::mem::forget(ctx);
    }

    /// Per-length cells for the recv core (width wall: full-symbolic dlen
    /// makes every append_bytes store a symbolic-offset write — CNF >6GiB
    /// both solvers; concrete dlen cells with symbolic bytes + symbolic
    /// cursor prove per the pg_lsn precedent). Cells d0..d20 enumerate the
    /// dlen<=20 domain exhaustively.
    macro_rules! uuid_recv_cell {
        ($($name:ident: $dlen:expr, $uw:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind($uw)] // exact: cell's copy length + 2 (unwind-slack law)
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
            #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $name() {
                const CAP: usize = 20;
                // dead-symbolic-bytes law: zero-fill slots beyond dlen
                let mut data = [0u8; CAP];
                let live: [u8; $dlen] = kani::any();
                let mut _zi = 0;
                while _zi < $dlen {
                    data[_zi] = live[_zi];
                    _zi += 1;
                }
                let dlen: usize = $dlen;
                let cur: usize = kani::any();
                kani::assume(cur <= CAP);
                // Err regime only: the 16-byte read is guarded off, so no
                // symbolic-offset muxes enter the formula. Ok regime covered
                // by the fully concrete ok-cells below.
                kani::assume(cur + 16 > dlen);

                let mut ccur: i32 = cur as i32;
                let mut cu = CPgUuid { data: [0xBB; 16] };
                let cst = unsafe { pg_uuid_recv(data.as_ptr(), dlen as i32, &mut ccur, &mut cu) };

                let ctx = mcx::MemoryContext::new_bump("kani-uuid-recv");
                let mut si = match stringinfo::StringInfo::with_capacity_in(ctx.mcx(), CAP + 2) {
                    Ok(s) => s,
                    Err(e) => {
                        core::mem::forget(e);
                        panic!("stub alloc failed")
                    }
                };
                if let Err(e) = si.append_bytes(&data[..dlen]) {
                    core::mem::forget(e);
                    panic!("append within capacity failed");
                }
                si.cursor = cur;
                match adt_uuid::uuid_recv(&mut si) {
                    Ok(u) => {
                        assert!(cst == 0);
                        assert!(u == cu.data);
                        assert!(si.cursor == ccur as usize);
                        kani::cover!(true, "cell Ok arm reachable");
                    }
                    Err(e) => {
                        assert!(cst == 4);
                        assert!(e.sqlstate == ERRCODE_PROTOCOL_VIOLATION);
                        assert!(e.level == ERROR);
                        core::mem::forget(e);
                        kani::cover!(true, "cell Err arm reachable");
                    }
                }
                core::mem::forget(si);
                core::mem::forget(ctx);
            }
        )*};
    }

    uuid_recv_cell! {
        eq_uuid_recv_core_d20: 20, 23;
        eq_uuid_recv_core_d19: 19, 22;
        eq_uuid_recv_core_d18: 18, 21;
        eq_uuid_recv_core_d17: 17, 20;
        eq_uuid_recv_core_d16: 16, 19;
        eq_uuid_recv_core_d15: 15, 19;
        eq_uuid_recv_core_d14: 14, 19;
        eq_uuid_recv_core_d13: 13, 19;
        eq_uuid_recv_core_d12: 12, 19;
        eq_uuid_recv_core_d11: 11, 19;
        eq_uuid_recv_core_d10: 10, 19;
        eq_uuid_recv_core_d9: 9, 19;
        eq_uuid_recv_core_d8: 8, 19;
        eq_uuid_recv_core_d7: 7, 19;
        eq_uuid_recv_core_d6: 6, 19;
        eq_uuid_recv_core_d5: 5, 19;
        eq_uuid_recv_core_d4: 4, 19;
        eq_uuid_recv_core_d3: 3, 19;
        eq_uuid_recv_core_d2: 2, 19;
        eq_uuid_recv_core_d1: 1, 19;
        eq_uuid_recv_core_d0: 0, 19;
    }

    /// Fully concrete (dlen, cur) Ok-regime cells: 16 symbolic payload
    /// bytes at a concrete offset — no symbolic-offset muxes. Union of
    /// ok-cells + err-cells = full (dlen<=20, cur<=20) domain.
    macro_rules! uuid_recv_ok_cell {
        ($($name:ident: $dlen:expr, $cur:expr, $uw:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind($uw)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
            #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $name() {
                const CAP: usize = 20;
                let mut data = [0u8; CAP];
                let live: [u8; $dlen] = kani::any();
                let mut _zi = 0;
                while _zi < $dlen {
                    data[_zi] = live[_zi];
                    _zi += 1;
                }
                let dlen: usize = $dlen;
                let cur: usize = $cur;

                let mut ccur: i32 = cur as i32;
                let mut cu = CPgUuid { data: [0xBB; 16] };
                let cst = unsafe { pg_uuid_recv(data.as_ptr(), dlen as i32, &mut ccur, &mut cu) };

                let ctx = mcx::MemoryContext::new_bump("kani-uuid-recv");
                let mut si = match stringinfo::StringInfo::with_capacity_in(ctx.mcx(), CAP + 2) {
                    Ok(s) => s,
                    Err(e) => {
                        core::mem::forget(e);
                        panic!("stub alloc failed")
                    }
                };
                if let Err(e) = si.append_bytes(&data[..dlen]) {
                    core::mem::forget(e);
                    panic!("append within capacity failed");
                }
                si.cursor = cur;
                match adt_uuid::uuid_recv(&mut si) {
                    Ok(u) => {
                        assert!(cst == 0);
                        assert!(u == cu.data);
                        assert!(si.cursor == ccur as usize);
                        kani::cover!(true, "cell Ok arm reachable");
                    }
                    Err(e) => {
                        assert!(cst == 4);
                        assert!(e.sqlstate == ERRCODE_PROTOCOL_VIOLATION);
                        assert!(e.level == ERROR);
                        core::mem::forget(e);
                        kani::cover!(true, "cell Err arm reachable");
                    }
                }
                core::mem::forget(si);
                core::mem::forget(ctx);
            }
        )*};
    }

    uuid_recv_ok_cell! {
        eq_uuid_recv_core_ok16_0: 16, 0, 19;
        eq_uuid_recv_core_ok17_0: 17, 0, 20;
        eq_uuid_recv_core_ok17_1: 17, 1, 20;
        eq_uuid_recv_core_ok18_0: 18, 0, 21;
        eq_uuid_recv_core_ok18_1: 18, 1, 21;
        eq_uuid_recv_core_ok18_2: 18, 2, 21;
        eq_uuid_recv_core_ok19_0: 19, 0, 22;
        eq_uuid_recv_core_ok19_1: 19, 1, 22;
        eq_uuid_recv_core_ok19_2: 19, 2, 22;
        eq_uuid_recv_core_ok19_3: 19, 3, 22;
        eq_uuid_recv_core_ok20_0: 20, 0, 23;
        eq_uuid_recv_core_ok20_1: 20, 1, 23;
        eq_uuid_recv_core_ok20_2: 20, 2, 23;
        eq_uuid_recv_core_ok20_3: 20, 3, 23;
        eq_uuid_recv_core_ok20_4: 20, 4, 23;
    }

    /// Both-arm reachability for the recv rig (Ok needs cursor+16 <= dlen).
    #[kani::proof]
    #[kani::unwind(24)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    // RVR lesson: grow/deallocate stubs mandatory when append_bytes'
    // try_reserve/grow branch is reachable (real arena + Acct recursion
    // otherwise enters symex).
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn cover_uuid_recv_both_arms() {
        const CAP: usize = 20;
        let data: [u8; CAP] = kani::any();
        let dlen: usize = kani::any();
        kani::assume(dlen <= CAP);
        let ctx = mcx::MemoryContext::new_bump("kani-uuid-recv-cover");
        let mut si = match stringinfo::StringInfo::with_capacity_in(ctx.mcx(), CAP + 2) {
            Ok(s) => s,
            Err(e) => {
                core::mem::forget(e);
                panic!("stub alloc failed")
            }
        };
        if let Err(e) = si.append_bytes(&data[..dlen]) {
            core::mem::forget(e);
            panic!("append within capacity failed");
        }
        match adt_uuid::uuid_recv(&mut si) {
            Ok(_) => kani::cover!(true, "uuid_recv Ok arm reachable"),
            Err(e) => {
                kani::cover!(true, "uuid_recv Err arm reachable");
                core::mem::forget(e);
            }
        }
        core::mem::forget(si);
        core::mem::forget(ctx);
    }

    // ---------------- uuid_send (2962): wrapper-level ----------------------

    #[kani::proof]
    #[kani::unwind(24)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    // RVR lesson: grow/deallocate stubs mandatory when append_bytes'
    // try_reserve/grow branch is reachable (real arena + Acct recursion
    // otherwise enters symex).
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_uuid_send() {
        let u: [u8; 16] = kani::any();
        let cu = CPgUuid { data: u };
        let mut cbuf = [0u8; 20];
        let clen = unsafe { pg_uuid_send(&cu, cbuf.as_mut_ptr()) };

        let ctx = mcx::MemoryContext::new_bump("kani-uuid-send");
        let mut f = LocalFcinfo::<1>::new(0);
        // SAFETY: ctx outlives the call (forgotten, never freed).
        unsafe { f.set_result_mcx(ctx.mcx()) };
        f.args[0] = NullableDatum::value(Datum::from_usize(u.as_ptr() as usize));
        let d = match adt_uuid::builtins::fc_uuid_send(None, &mut f) {
            Ok(d) => d,
            Err(e) => {
                core::mem::forget(e);
                panic!("uuid_send errored")
            }
        };
        let img = unsafe { core::slice::from_raw_parts(d.as_usize() as *const u8, 20) };
        assert!(clen == 20);
        let mut i = 0;
        while i < 20 {
            assert!(img[i] == cbuf[i]);
            i += 1;
        }
        core::mem::forget(ctx);
    }

    // ------------- generate_uuidv7 core: RNG seam ---------------------------

    static RNG_BYTES: [AtomicU8; 8] = [const { AtomicU8::new(0) }; 8];

    /// RNG-seam stub for pg_strong_random::pg_strong_random — fills the
    /// caller's buffer from the harness-chosen symbolic block and reports
    /// success (seam internals + the failure arm leave the proof).
    fn stub_strong_random(buf: &mut [u8]) -> bool {
        for (i, b) in buf.iter_mut().enumerate() {
            *b = RNG_BYTES[i % 8].load(Relaxed);
        }
        true
    }

    #[kani::proof]
    #[kani::stub(pg_strong_random::pg_strong_random, stub_strong_random)]
    fn eq_generate_uuidv7() {
        let unix_ts_ms: u64 = kani::any();
        let sub_ms: u32 = kani::any();
        // caller contract: sub_ms is a nanoseconds-within-ms remainder
        kani::assume(sub_ms < 1_000_000);
        let rand: [u8; 8] = kani::any();
        let mut i = 0;
        while i < 8 {
            RNG_BYTES[i].store(rand[i], Relaxed);
            i += 1;
        }

        let mut cu = CPgUuid { data: [0; 16] };
        unsafe { pg_generate_uuidv7(unix_ts_ms, sub_ms, rand.as_ptr(), &mut cu) };

        match adt_uuid::generate_uuidv7(unix_ts_ms, sub_ms) {
            Ok(r) => assert!(r == cu.data),
            Err(e) => {
                core::mem::forget(e);
                panic!("unreachable: RNG seam always succeeds")
            }
        }
    }

    /// MUST FAIL (RNG-seam-is-load-bearing witness): C gets different rand
    /// bytes. DEFAULT solver.
    #[kani::proof]
    #[kani::stub(pg_strong_random::pg_strong_random, stub_strong_random)]
    fn control_generate_uuidv7_rng_skew() {
        let unix_ts_ms: u64 = kani::any();
        let sub_ms: u32 = kani::any();
        kani::assume(sub_ms < 1_000_000);
        let rand: [u8; 8] = kani::any();
        let mut skewed = rand;
        skewed[3] ^= 0x40;
        let mut i = 0;
        while i < 8 {
            RNG_BYTES[i].store(rand[i], Relaxed);
            i += 1;
        }
        let mut cu = CPgUuid { data: [0; 16] };
        unsafe { pg_generate_uuidv7(unix_ts_ms, sub_ms, skewed.as_ptr(), &mut cu) };
        match adt_uuid::generate_uuidv7(unix_ts_ms, sub_ms) {
            Ok(r) => assert!(r == cu.data), // expected failure (byte 11 skew)
            Err(e) => {
                core::mem::forget(e);
                panic!("unreachable")
            }
        }
    }
}
