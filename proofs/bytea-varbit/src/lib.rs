//! Kani C≡Rust equivalence: bytea escape-format kernels + byteaout's
//! escape/hex paths + varbit comparison/padding.
//!
//! Rust side (shipped code, path-deps — never copied):
//!  - adt_encode::{esc_enc_len, esc_dec_len, esc_encode_body, esc_decode_body}
//!    (crates/backend/utils/adt/encode/src/lib.rs; the *_body slice cores were
//!    factored out of the PgVec-taking esc_encode/esc_decode for this proof —
//!    behavior identical, the PgVec paths call them)
//!  - varlena::bytea::byteaout_into (std Vec-based cstring core of byteaout)
//!  - adt_varbit::{bit_cmp_payload, pad_last}
//!
//! C side: proofs/bytea-varbit/c/pg_bytea_varbit.c (REL_15_STABLE encode.c /
//! varlena.c / varbit.c — provenance + shims documented there).
//!
//! Claims and fences:
//!  - esc_* and byteaout: byte-for-byte output equality (incl. byteaout's
//!    trailing NUL) and accept/reject verdict parity; error MESSAGE text is
//!    outside the claim (C ereport shimmed to a -1 sentinel).
//!  - bit_cmp: SIGN equality — C returns raw memcmp magnitudes, Rust returns
//!    -1/0/1; every caller (biteq..bitcmp fmgr wrappers, btree support)
//!    consumes only the sign. bitlen/bytelen are NOT fenced to the valid
//!    varbit relation: equivalence holds on the wider domain (both sides
//!    treat them independently), so the proof covers the valid range a
//!    fortiori. bitlens >= 0 per varbit invariant.
//!  - pad_last: fenced to the domain of C's compiled-out
//!    Assert(0 <= pad < 8) via the valid varbit relation
//!    ceil(bitlen/8) == bytelen (usize arithmetic in the Rust body wraps
//!    outside it — pgrust never calls it outside; C's macro asserts).
//!  - Input caps: 8 symbolic payload bytes with symbolic length (escape
//!    output up to 33 bytes compared). Exception: byteaout's ESCAPE path
//!    end-to-end is a measured WALL (std Vec symex, see wall_byteaout_escape)
//!    — its kernel logic is covered by the esc_* proofs; the hex path proves
//!    end-to-end at len<=8.
//!
//! Run with: timeout 30 cargo kani -Z c-ffi -Z stubbing \
//!   --c-lib c/pg_bytea_varbit.c --harness <name>
//! (+ --solver kissat for the sub-second harnesses; esc_encode/dec_len/
//!  decode/byteaout_hex solve faster on the DEFAULT solver, 20-37s.)
//!
//! Negative control: control_esc_enc_len_off_by_one feeds C a one-shorter
//! slice and must FAIL (run with the default solver, not kissat).
//! Verified failing 0.6s, 2026-07-28.

#[cfg(kani)]
mod proofs {
    use core::mem::MaybeUninit;

    extern "C" {
        fn pg_esc_encode(src: *const u8, srclen: usize, dst: *mut u8) -> u64;
        fn pg_esc_decode(src: *const u8, srclen: usize, dst: *mut u8) -> i64;
        fn pg_esc_enc_len(src: *const u8, srclen: usize) -> u64;
        fn pg_esc_dec_len(src: *const u8, srclen: usize) -> i64;
        fn pg_byteaout_esc_len(data: *const u8, len: i32) -> u64;
        fn pg_byteaout_esc(data: *const u8, len: i32, result: *mut u8) -> i32;
        fn pg_byteaout_hex(data: *const u8, len: i32, result: *mut u8) -> i32;
        fn pg_bit_cmp(
            bits1: *const u8,
            bytelen1: i32,
            bitlen1: i32,
            bits2: *const u8,
            bytelen2: i32,
            bitlen2: i32,
        ) -> i32;
        fn pg_varbit_pad(bits: *mut u8, bytelen: i32, bitlen: i32) -> i32;
    }

    const N: usize = 8;

    fn sym_input() -> ([u8; N], usize) {
        let src: [u8; N] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= N);
        (src, len)
    }

    // ---------------- encode.c escape kernels ----------------

    #[kani::proof]
    #[kani::unwind(10)]
    fn eq_esc_enc_len() {
        let (src, len) = sym_input();
        let c = unsafe { pg_esc_enc_len(src.as_ptr(), len) };
        let r = adt_encode::esc_enc_len(&src[..len]);
        assert!(c == r);
    }

    // Full-domain harness: 29s default solver / 44s kissat (over the 10s
    // standing-suite budget; ladder attempted: kissat worse, len case-split
    // ~33s per half, no help — cost is symex of the 32-byte output buffers,
    // scales with the cap: len<=4 + 16-byte buffers probe = 10.3s).
    #[kani::proof]
    #[kani::unwind(35)]
    fn eq_esc_encode() {
        let (src, len) = sym_input();
        esc_encode_body_eq(src, len);
    }

    fn esc_encode_body_eq(src: [u8; N], len: usize) {
        let mut cbuf = [0u8; 4 * N];
        let clen = unsafe { pg_esc_encode(src.as_ptr(), len, cbuf.as_mut_ptr()) };
        let mut rbuf = [MaybeUninit::new(0u8); 4 * N];
        let w = adt_encode::esc_encode_body(&src[..len], &mut rbuf);
        assert!(w as u64 == clen);
        // Both buffers are zero-initialized and written for exactly w == clen
        // bytes, so a concrete-bound full compare is equivalent (and cheaper
        // for the solver than a symbolic-bound loop).
        let mut i = 0usize;
        while i < 4 * N {
            assert!(unsafe { rbuf[i].assume_init() } == cbuf[i]);
            i += 1;
        }
    }

    // Stub for PgError::new on the reject paths: identical value construction
    // minus the #[track_caller] Location::caller() capture (Kani unsupported
    // construct, measured trap). Only the error F/L location field leaves the
    // claim; level, sqlstate and message text stay in.
    fn stub_pg_error_new(
        level: types_error::ErrorLevel,
        message: impl Into<String>,
    ) -> types_error::PgError {
        types_error::PgError {
            level,
            sqlstate: types_error::default_sqlstate_for_level(level),
            message: message.into(),
            message_raw: None,
            detail: None,
            detail_log: None,
            hint: None,
            context: None,
            backtrace: None,
            message_id: None,
            domain: None,
            context_domain: None,
            hide_statement: false,
            hide_context: false,
            location: None,
            saved_errno: None,
            cursor_position: None,
            internal_position: None,
            internal_query: None,
            schema_name: None,
            table_name: None,
            column_name: None,
            datatype_name: None,
            constraint_name: None,
            plpgsql_context_attached: false,
        }
    }

    #[kani::proof]
    #[kani::unwind(10)]
    #[kani::stub(types_error::PgError::new, stub_pg_error_new)]
    fn eq_esc_dec_len() {
        let (src, len) = sym_input();
        let c = unsafe { pg_esc_dec_len(src.as_ptr(), len) };
        match adt_encode::esc_dec_len(&src[..len]) {
            Ok(r) => {
                assert!(c >= 0);
                assert!(r == c as u64);
            }
            Err(_) => assert!(c == -1),
        }
    }

    #[kani::proof]
    #[kani::unwind(10)]
    #[kani::stub(types_error::PgError::new, stub_pg_error_new)]
    fn eq_esc_decode() {
        let (src, len) = sym_input();
        let mut cbuf = [0u8; N];
        let c = unsafe { pg_esc_decode(src.as_ptr(), len, cbuf.as_mut_ptr()) };
        let mut rbuf = [MaybeUninit::new(0u8); N];
        match adt_encode::esc_decode_body(&src[..len], &mut rbuf) {
            Ok(w) => {
                assert!(c >= 0);
                assert!(w as i64 == c);
                let mut i = 0usize;
                while i < w {
                    assert!(unsafe { rbuf[i].assume_init() } == cbuf[i]);
                    i += 1;
                }
            }
            Err(_) => assert!(c == -1),
        }
    }

    // ---------------- byteaout (varlena.c) ----------------

    // WALL — not part of the standing suite (wall_ prefix, not eq_).
    // Measured 2026-07-28: symbolic len<=8 >120s, len<=4 >45s, len<=3 >40s,
    // even CONCRETE len=2 costs 30.3s — the cost is std Vec machinery in
    // symex (per-byte push/extend with symbolic offsets + growth branches),
    // not the escape logic: the slice-based esc_* kernels prove fine at 8
    // bytes above, and byteaout's hex path (single unsafe write loop, no
    // per-byte pushes) proves at len<=8 below. byteaout's escape-path claim
    // therefore stands only at the kernel level, not end-to-end.
    // (Re-enable by restoring #[kani::proof] #[kani::unwind(20)].)
    #[allow(dead_code)]
    fn wall_byteaout_escape() {
        const M: usize = 3;
        let src: [u8; M] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= M);
        let clen = unsafe { pg_byteaout_esc_len(src.as_ptr(), len as i32) };
        let mut cbuf = [0u8; 4 * M + 1];
        unsafe { pg_byteaout_esc(src.as_ptr(), len as i32, cbuf.as_mut_ptr()) };

        // Pre-sized like the shipped caller's retained fn_extra scratch buffer
        // (fc_byteaout's out_scratch): keeps Vec growth machinery out of symex.
        let mut out = Vec::with_capacity(4 * M + 1);
        varlena::bytea::byteaout_into(
            &src[..len],
            guc_tables::consts::BYTEA_OUTPUT_ESCAPE,
            &mut out,
        )
        .unwrap();

        // C's counting loop includes the trailing NUL ("empty string has 1
        // char"); Rust's out includes its push(0). Same total, same bytes.
        assert!(out.len() as u64 == clen);
        let mut i = 0usize;
        while i < out.len() {
            assert!(out[i] == cbuf[i]);
            i += 1;
        }
    }

    #[kani::proof]
    #[kani::unwind(20)]
    fn eq_byteaout_hex() {
        let (src, len) = sym_input();
        let mut cbuf = [0u8; 2 * N + 3];
        unsafe { pg_byteaout_hex(src.as_ptr(), len as i32, cbuf.as_mut_ptr()) };

        let mut out = Vec::new();
        varlena::bytea::byteaout_into(&src[..len], guc_tables::consts::BYTEA_OUTPUT_HEX, &mut out)
            .unwrap();

        assert!(out.len() == 2 * len + 3);
        let mut i = 0usize;
        while i < out.len() {
            assert!(out[i] == cbuf[i]);
            i += 1;
        }
    }


    // ---------------- varbit.c ----------------

    #[kani::proof]
    #[kani::unwind(10)]
    fn eq_bit_cmp() {
        let ab: [u8; N] = kani::any();
        let bb: [u8; N] = kani::any();
        let alen: usize = kani::any();
        let blen: usize = kani::any();
        kani::assume(alen <= N && blen <= N);
        let abits: i32 = kani::any();
        let bbits: i32 = kani::any();
        kani::assume(abits >= 0 && bbits >= 0);

        // Build the Rust payloads: [bitlen i32 ne][bytes].
        let mut pa = [0u8; 4 + N];
        pa[..4].copy_from_slice(&abits.to_ne_bytes());
        let mut i = 0usize;
        while i < alen {
            pa[4 + i] = ab[i];
            i += 1;
        }
        let mut pb = [0u8; 4 + N];
        pb[..4].copy_from_slice(&bbits.to_ne_bytes());
        let mut i = 0usize;
        while i < blen {
            pb[4 + i] = bb[i];
            i += 1;
        }

        let r = adt_varbit::bit_cmp_payload(&pa[..4 + alen], &pb[..4 + blen]);
        let c = unsafe {
            pg_bit_cmp(ab.as_ptr(), alen as i32, abits, bb.as_ptr(), blen as i32, bbits)
        };
        // Sign equality: C returns raw memcmp magnitudes, Rust -1/0/1.
        assert!((c < 0) == (r < 0));
        assert!((c == 0) == (r == 0));
        assert!((c > 0) == (r > 0));
    }

    #[kani::proof]
    #[kani::unwind(10)]
    fn eq_pad_last() {
        let mut bits: [u8; N] = kani::any();
        let bitlen: usize = kani::any();
        let bytelen: usize = kani::any();
        kani::assume(bytelen <= N);
        // Valid varbit relation => pad in [0, 8): the domain of C's Assert.
        kani::assume(bitlen.div_ceil(8) == bytelen);

        let mut cbits = bits;
        unsafe { pg_varbit_pad(cbits.as_mut_ptr(), bytelen as i32, bitlen as i32) };
        adt_varbit::pad_last(&mut bits[..bytelen], bitlen);

        let mut i = 0usize;
        while i < N {
            assert!(bits[i] == cbits[i]);
            i += 1;
        }
    }

    // ---------------- negative control (must FAIL) ----------------

    #[kani::proof]
    #[kani::unwind(10)]
    fn control_esc_enc_len_off_by_one() {
        let (src, len) = sym_input();
        kani::assume(len >= 1);
        let c = unsafe { pg_esc_enc_len(src.as_ptr(), len - 1) };
        let r = adt_encode::esc_enc_len(&src[..len]);
        assert!(c == r);
    }
}
