//! Kani C≡Rust equivalence: the CRC SQL builtins (ledger oids 6364/6365):
//!   crc32(bytea)  -> cryptohashfuncs::fc_crc32_bytea  (traditional CRC-32)
//!   crc32c(bytea) -> cryptohashfuncs::fc_crc32c_bytea (CRC-32C, Castagnoli)
//!
//! TIER: entry-point (fc_) proofs. The SHIPPED FMGR WRAPPERS are invoked
//! through a real `LocalFcinfo<1>` frame whose arg 0 is a real inline bytea
//! varlena image, so the whole shipped path is inside the theorem:
//! Datum -> arg_varlena_packed (header decode; detoast arm provably not
//! taken on inline headers) -> PackedVarlena::data -> CRC kernel ->
//! Datum::from_i64. Result compared as the SQL-visible int8: C's
//! PG_RETURN_INT64(crc) zero-extends the uint32 crc to int64, matched
//! exactly by Rust's `Datum::from_i64(crc as i64)` (u32 -> i64 zero-extend).
//!
//! Varlena images: per-length cells use the 4-byte-unpadded header form
//! (little-endian word (len+4)<<2 via the SHIPPED set_varsize_4b_word,
//! payload at offset 4), matching PG_GETARG_BYTEA_PP semantics for an
//! untoasted datum. One extra cell per function uses the 1-byte short
//! header (shipped set_varsize_short) so BOTH inline header-decode arms of
//! arg_varlena_packed are in-theorem (text-cmp precedent).
//!
//! CODE PATH IN-THEOREM (SIMD note, prove-target skill):
//!   - crc32:  crc32c::traditional_crc32 — pure table loop, fully proven.
//!   - crc32c: on this host (aarch64-apple-darwin) target_feature="crc" is
//!     compile-time ON, so the shipped `crc32c::pg_comp_crc32c` statically
//!     dispatches to the armv8 `__crc32c*` intrinsics, which Kani cannot
//!     codegen. Every crc32c harness therefore carries
//!     #[kani::stub(crc32c::pg_comp_crc32c, crc32c::pg_comp_crc32c_sb8)],
//!     pinning the SHIPPED software slicing-by-8 kernel into the theorem.
//!     PROVEN: fc wrapper shell + fin_crc32c + pg_comp_crc32c_sb8 (sb8
//!     table path) vs verbatim REL_18_STABLE pg_crc32c_sb8.c.
//!     OUT OF SCOPE: the hardware arms (armv8 __crc32c*, sse42), i.e.
//!     excluded(blocked:simd) — the C reference is likewise pinned to its
//!     sb8 arm (no USE_*_CRC32C defined; see c/pg_crc32c.h provenance).
//!   - Byte order: little-endian pinned on both sides (both target
//!     platforms are LE; WORDS_BIGENDIAN undefined in the vendored C, and
//!     the Rust crate compile_errors on BE).
//!
//! C side: proofs/crc/c/pg_crc.c (crc32_bytea/crc32c_bytea bodies +
//! pg_crc32_table, verbatim REL_18_STABLE src/backend/utils/hash/pg_crc.c),
//! c/pg_crc32c_sb8.c (verbatim src/port/pg_crc32c_sb8.c), c/pg_crc.h +
//! c/pg_crc32c.h (verbatim macro headers). Provenance + every shim
//! documented per file. Native replay: both files compile with cc and
//! reproduce the standard check vectors for "123456789"
//! (CRC-32 0xCBF43926, CRC-32C 0xE3069283) — table fidelity witnessed.
//!
//! Bounds: per-length cells len 0..=8 (concrete length literal, fully
//! symbolic payload bytes) per function; plus sb8 kernel path-coverage
//! harnesses (symbolic initial crc accumulator): 16-byte aligned buffer
//! (8-byte main loop, two iterations) and 13 bytes at offset 1 (leading
//! align loop + main loop + tail loop). Cell alignment inside the fc
//! images is whatever CBMC assigns the stack array, so the kernel
//! harnesses pin both alignment regimes explicitly.
//!
//! Negative control (DEFAULT solver; MUST FAIL): control_crc32_skewed_byte
//! feeds C a payload with byte 0 XOR-flipped vs the image the Rust wrapper
//! sees — proves the rig is non-vacuous.
//!
//! Compile gate / run spelling:
//!   cargo kani -Z c-ffi -Z stubbing --c-lib c/pg_crc.c \
//!     --c-lib c/pg_crc32c_sb8.c --harness <name> --exact

#[cfg(kani)]
mod proofs {
    use datum::{Datum, NullableDatum};
    use types_fmgr::LocalFcinfo;
    use types_tuple::varatt;

    extern "C" {
        // c/pg_crc.c — fmgr-unwrapped SQL function bodies (verbatim cores)
        fn pg_crc32_bytea(data: *const u8, len: usize) -> i64;
        fn pg_crc32c_bytea(data: *const u8, len: usize) -> i64;
        // c/pg_crc32c_sb8.c — verbatim kernel (C name unchanged; no clash
        // with the mangled Rust symbol)
        fn pg_comp_crc32c_sb8(crc: u32, data: *const u8, len: usize) -> u32;
    }

    const CAP: usize = 8;
    const HDR4: usize = varatt::VARHDRSZ; // 4
    const HDR1: usize = varatt::VARHDRSZ_SHORT; // 1

    /// Inline bytea, 4-byte unpadded header: LE word (len+4)<<2 built with
    /// the SHIPPED encoder, then `len` (concrete) symbolic payload bytes.
    struct VarImg4 {
        img: [u8; HDR4 + CAP],
        len: usize,
    }

    fn sym_bytea4(len: usize) -> VarImg4 {
        let payload: [u8; CAP] = kani::any();
        let mut img = [0u8; HDR4 + CAP];
        let w = varatt::set_varsize_4b_word((len + HDR4) as u32).to_ne_bytes();
        let mut i = 0;
        while i < HDR4 {
            img[i] = w[i];
            i += 1;
        }
        let mut i = 0;
        while i < CAP {
            img[HDR4 + i] = payload[i];
            i += 1;
        }
        VarImg4 { img, len }
    }

    impl VarImg4 {
        fn datum(&self) -> Datum {
            Datum::from_usize(self.img.as_ptr() as usize)
        }
        /// payload pointer, as C's VARDATA_ANY on a 4B header
        fn data(&self) -> *const u8 {
            self.img[HDR4..].as_ptr()
        }
    }

    /// Inline bytea, 1-byte short header (shipped set_varsize_short).
    struct VarImg1 {
        img: [u8; HDR1 + CAP],
        len: usize,
    }

    fn sym_bytea1(len: usize) -> VarImg1 {
        let payload: [u8; CAP] = kani::any();
        let mut img = [0u8; HDR1 + CAP];
        // SAFETY: img is writable and len + 1 <= VARATT_SHORT_MAX.
        unsafe { varatt::set_varsize_short(img.as_mut_ptr(), len + HDR1) };
        let mut i = 0;
        while i < CAP {
            img[HDR1 + i] = payload[i];
            i += 1;
        }
        VarImg1 { img, len }
    }

    impl VarImg1 {
        fn datum(&self) -> Datum {
            Datum::from_usize(self.img.as_ptr() as usize)
        }
        fn data(&self) -> *const u8 {
            self.img[HDR1..].as_ptr()
        }
    }

    /// Run a shipped fc_* wrapper on a real strict 1-arg frame. The CRC
    /// builtins never error on an inline varlena arg (detoast arm not
    /// taken), so the Err arm is statically dead; forget the boxed error
    /// to keep drop machinery out of symex.
    fn call1<E>(
        fc: fn(
            Option<&mut types_fmgr::FmgrInfo>,
            &mut types_fmgr::FunctionCallInfoBaseData,
        ) -> Result<Datum, E>,
        a: Datum,
    ) -> Datum {
        let mut f = LocalFcinfo::<1>::new(0);
        f.args[0] = NullableDatum::value(a);
        match fc(None, &mut f) {
            Ok(d) => d,
            Err(e) => {
                core::mem::forget(e);
                panic!("crc builtin errored")
            }
        }
    }

    fn check_crc32_4b(len: usize) {
        let v = sym_bytea4(len);
        let c = unsafe { pg_crc32_bytea(v.data(), v.len) };
        let r = call1(cryptohashfuncs::fc_crc32_bytea, v.datum());
        assert!(c == r.as_i64());
    }

    fn check_crc32c_4b(len: usize) {
        let v = sym_bytea4(len);
        let c = unsafe { pg_crc32c_bytea(v.data(), v.len) };
        let r = call1(cryptohashfuncs::fc_crc32c_bytea, v.datum());
        assert!(c == r.as_i64());
    }

    // ---------------- crc32(bytea): per-length cells, 4B header ----------

    macro_rules! crc32_cells {
        ($($h:ident: $len:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind(10)]
            fn $h() { check_crc32_4b($len); }
        )*};
    }

    crc32_cells! {
        eq_crc32_len0: 0;
        eq_crc32_len1: 1;
        eq_crc32_len2: 2;
        eq_crc32_len3: 3;
        eq_crc32_len4: 4;
        eq_crc32_len5: 5;
        eq_crc32_len6: 6;
        eq_crc32_len7: 7;
        eq_crc32_len8: 8;
    }

    /// 1-byte short-header decode arm of arg_varlena_packed in-theorem.
    #[kani::proof]
    #[kani::unwind(10)]
    fn eq_crc32_short_hdr_len5() {
        let v = sym_bytea1(5);
        let c = unsafe { pg_crc32_bytea(v.data(), v.len) };
        let r = call1(cryptohashfuncs::fc_crc32_bytea, v.datum());
        assert!(c == r.as_i64());
    }

    // ---------------- crc32c(bytea): per-length cells, 4B header ---------
    // Every harness stubs the shipped compile-time armv8-intrinsic dispatch
    // to the shipped sb8 software kernel (see module doc: SIMD arm is
    // excluded(blocked:simd); the C reference is pinned to its sb8 arm).

    macro_rules! crc32c_cells {
        ($($h:ident: $len:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind(10)]
            #[kani::stub(crc32c::pg_comp_crc32c, crc32c::pg_comp_crc32c_sb8)]
            fn $h() { check_crc32c_4b($len); }
        )*};
    }

    crc32c_cells! {
        eq_crc32c_len0: 0;
        eq_crc32c_len1: 1;
        eq_crc32c_len2: 2;
        eq_crc32c_len3: 3;
        eq_crc32c_len4: 4;
        eq_crc32c_len5: 5;
        eq_crc32c_len6: 6;
        eq_crc32c_len7: 7;
        eq_crc32c_len8: 8;
    }

    /// 1-byte short-header decode arm of arg_varlena_packed in-theorem.
    #[kani::proof]
    #[kani::unwind(10)]
    #[kani::stub(crc32c::pg_comp_crc32c, crc32c::pg_comp_crc32c_sb8)]
    fn eq_crc32c_short_hdr_len5() {
        let v = sym_bytea1(5);
        let c = unsafe { pg_crc32c_bytea(v.data(), v.len) };
        let r = call1(cryptohashfuncs::fc_crc32c_bytea, v.datum());
        assert!(c == r.as_i64());
    }

    // ------------- sb8 kernel path coverage (alignment-pinned) -----------
    // The fc cells above leave payload alignment to CBMC's object layout;
    // these pin both regimes of the kernel's three loops, with a fully
    // symbolic initial accumulator (any mid-stream state).

    /// 8-byte-aligned 16-byte buffer: align loop 0 iters, 8-byte main loop
    /// 2 iters, tail 0 — the slicing-by-8 fast path.
    #[kani::proof]
    #[kani::unwind(6)]
    fn eq_crc32c_sb8_kernel_len16_aligned() {
        #[repr(align(8))]
        struct Aligned([u8; 16]);
        let a = Aligned(kani::any());
        let crc0: u32 = kani::any();
        let c = unsafe { pg_comp_crc32c_sb8(crc0, a.0.as_ptr(), 16) };
        assert!(crc32c::pg_comp_crc32c_sb8(crc0, &a.0) == c);
    }

    /// 13 bytes at offset 1 of an 8-byte-aligned buffer: align loop 3
    /// iters, main loop 1 iter, tail loop 2 iters — all three loops fire.
    #[kani::proof]
    #[kani::unwind(6)]
    fn eq_crc32c_sb8_kernel_len13_off1() {
        #[repr(align(8))]
        struct Aligned([u8; 16]);
        let a = Aligned(kani::any());
        let crc0: u32 = kani::any();
        let s = &a.0[1..14];
        let c = unsafe { pg_comp_crc32c_sb8(crc0, s.as_ptr(), 13) };
        assert!(crc32c::pg_comp_crc32c_sb8(crc0, s) == c);
    }

    // ---------------- negative control (DEFAULT solver; MUST FAIL) -------

    /// Rig non-vacuity witness: C is fed the image payload with byte 0
    /// XOR-flipped; the equality assert must FAIL with a counterexample.
    /// Uses assert!(a == b) (never assert_eq!) per the skill's control law.
    #[kani::proof]
    #[kani::unwind(10)]
    fn control_crc32_skewed_byte() {
        let v = sym_bytea4(3);
        let mut skew = [0u8; 3];
        let mut i = 0;
        while i < 3 {
            skew[i] = v.img[HDR4 + i];
            i += 1;
        }
        skew[0] ^= 1;
        let c = unsafe { pg_crc32_bytea(skew.as_ptr(), 3) };
        let r = call1(cryptohashfuncs::fc_crc32_bytea, v.datum());
        assert!(c == r.as_i64()); // MUST FAIL
    }
}
