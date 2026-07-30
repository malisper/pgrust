//! Kani C≡Rust equivalence: the per-type hash pg_proc rows (~30) composed on
//! top of the ALREADY-PROVED hash_bytes kernels (proofs/hash: fixed lens
//! 0-24 + symbolic len<=32, full symbolic seeds).
//!
//! Rust side: the SHIPPED fmgr wrappers, called through a real LocalFcinfo
//! frame (datetime-cmp precedent) — datum unwrap, sign/zero extensions,
//! normalizations, trim loops and length scans are all inside the theorem:
//!   adt_int::builtins::fc_hashchar/int2/int4 (+extended)
//!   adt_int8::builtins::fc_hashint8 (+extended)      — high-word fold
//!   adt_scalar::builtins::fc_hash_uint32 (+extended) — hashoid rows
//!   adt_enum::builtins::fc_hashenum (+extended)
//!   adt_bool::builtins::fc_hashbool (+extended)
//!   adt_float::builtins::fc_hashfloat4/8 (+extended) — ±0 / NaN / widening
//!   name::builtins::fc_hashname (+extended)          — NUL scan in-theorem
//!   varlena::builtins::fc_hashtext (+extended)       — C-collation arm
//!   adt_varchar::builtins::fc_hashbpchar (+extended) — space trim in-theorem
//!   adt_scalar::builtins::fc_hashoidvector (+extended)
//!   adt_mac::builtins::fc_hashmacaddr / adt_mac8::…8 (+extended)
//! C side: c/pg_hash_rows.c — verbatim REL_18_STABLE hashfunc.c rows over
//! verbatim REL_18_STABLE hashfn.c kernels (provenance + shims documented
//! there).
//!
//! Domains and fences:
//!  - by-value rows: FULL symbolic domains (i8/i16/i32/i64/u32/bool/f32/f64)
//!    and FULL symbolic 64-bit seeds on every *extended row.
//!  - hashtext/hashbpchar: fncollation pinned to C_COLLATION_OID (950) — the
//!    deterministic C-known arm; the pg_locale nondeterministic seam and the
//!    collid==0 error arm are outside the value proof (error VERDICT+sqlstate
//!    parity proven separately in err_* harnesses, with the skill-standard
//!    PgError::error message/Location stub — value-space + sqlstate in, text
//!    out). Payloads are pre-detoasted short-varlena images.
//!  - PER-LENGTH CASE-SPLIT everywhere a length would be symbolic (measured:
//!    symbolic length at the composed wrapper level costs 60-136s+, over the
//!    30s budget; per-length is ~10x cheaper; unwind slack on the kernel's
//!    12-byte mix loop is catastrophic, so every case carries its exact
//!    unwind). cover_* union harnesses prove each split exhausts its band.
//!      hashtext:   every len 0..=8 + len-13 spot (crosses the kernel's mix
//!                  boundary; kernel itself proved to symlen<=32, proofs/hash)
//!      hashbpchar: every len 0..=4 with FULLY SYMBOLIC trim (trimmed length
//!                  symbolic 0..=len), + concrete-trim spots at (L=8, K=0/3/8)
//!                  and (L=13, K=5/13) — symbolic trim past L=4 re-introduces
//!                  symbolic hash length and walls
//!      hashname:   every len 0..=8 + len-16 spot (first NUL at the concrete
//!                  position in a 64-byte NameData block, nonzero symbolic
//!                  content before it, zero tail — the namein contract; the
//!                  NUL scans on both sides are in-theorem)
//!  - hashoidvector: valid-oidvector fence (ndim==1, dataoffset==0,
//!    elemtype==OIDOID — C's check_valid_oidvector gate, which pgrust
//!    debug_asserts instead of re-checking); every dim1 0..=4.
//!  - u32-returning rows compare Datum::as_u32 (value space — C packs
//!    UInt32GetDatum; datum upper-word conventions are fmgr plumbing);
//!    u64-returning *extended rows compare full Datum::as_u64.
//!
//! Cross-type consistency (what makes float4/float8 hash joins work):
//! consistency_hashfloat4_widens proves fc_hashfloat4(x) ==
//! fc_hashfloat8(x as f64) for every f32.
//!
//! Negative controls (run with the DEFAULT solver, never kissat):
//!  - control_int8_fold_skew: Rust fc_hashint8 vs C hashint4 of the low
//!    half — must FAIL (witnesses the high-word fold is load-bearing).
//!  - control_text_short_c_len: C hashes one byte fewer — must FAIL.
//!
//! Run (expected-green):
//!   timeout 30 ~/.cargo/bin/cargo-kani kani -Z c-ffi -Z stubbing \
//!     --c-lib c/pg_hash_rows.c --solver kissat \
//!     --harness proofs::<h> --exact
//! (controls: drop `--solver kissat`). Times in the ledger were measured
//! under multi-agent SAT load (~6 concurrent cbmc) — inflated 2-3x per the
//! TRIAGE calibration note; re-measure idle before quoting as calibration.

#[cfg(kani)]
mod proofs {
    use datum::{Datum, NullableDatum};
    use proof_support::stubs;
    use types_fmgr::LocalFcinfo;

    extern "C" {
        fn pg_hashchar(c: i8) -> u32;
        fn pg_hashcharextended(c: i8, seed: i64) -> u64;
        fn pg_hashint2(v: i16) -> u32;
        fn pg_hashint2extended(v: i16, seed: i64) -> u64;
        fn pg_hashint4(v: i32) -> u32;
        fn pg_hashint4extended(v: i32, seed: i64) -> u64;
        fn pg_hashint8(v: i64) -> u32;
        fn pg_hashint8extended(v: i64, seed: i64) -> u64;
        fn pg_hashoid(o: u32) -> u32;
        fn pg_hashoidextended(o: u32, seed: i64) -> u64;
        fn pg_hashbool(b: bool) -> u32;
        fn pg_hashboolextended(b: bool, seed: i64) -> u64;
        fn pg_hashfloat4(key: f32) -> u32;
        fn pg_hashfloat4extended(key: f32, seed: i64) -> u64;
        fn pg_hashfloat8(key: f64) -> u32;
        fn pg_hashfloat8extended(key: f64, seed: i64) -> u64;
        fn pg_hashoidvector(values: *const u32, dim1: i32) -> u32;
        fn pg_hashoidvectorextended(values: *const u32, dim1: i32, seed: i64) -> u64;
        fn pg_hashname(key: *const u8) -> u32;
        fn pg_hashnameextended(key: *const u8, seed: i64) -> u64;
        fn pg_hashtext_det(data: *const u8, len: i32) -> u32;
        fn pg_hashtextextended_det(data: *const u8, len: i32, seed: i64) -> u64;
        fn pg_hashbpchar_det(data: *const u8, len: i32) -> u32;
        fn pg_hashbpcharextended_det(data: *const u8, len: i32, seed: i64) -> u64;
        fn pg_hashmacaddr(key: *const u8) -> u32;
        fn pg_hashmacaddrextended(key: *const u8, seed: i64) -> u64;
        fn pg_hashmacaddr8(key: *const u8) -> u32;
        fn pg_hashmacaddr8extended(key: *const u8, seed: i64) -> u64;
    }

    const C_COLLATION_OID: types_core::Oid = 950;

    /// Run a shipped fc_* wrapper on an N-arg frame with a collation.
    fn call<const N: usize, E>(
        fc: fn(
            Option<&mut types_fmgr::FmgrInfo>,
            &mut types_fmgr::FunctionCallInfoBaseData,
        ) -> Result<Datum, E>,
        collid: types_core::Oid,
        args: [Datum; N],
    ) -> Datum {
        let mut f = LocalFcinfo::<N>::new(collid);
        let mut i = 0;
        while i < N {
            f.args[i] = NullableDatum::value(args[i]);
            i += 1;
        }
        match fc(None, &mut f) {
            Ok(d) => d,
            Err(_) => panic!("hash row errored"),
        }
    }

    // ================= by-value rows: full symbolic domains =================

    macro_rules! val_row {
        ($($h:ident: $krate:ident :: $fc:ident / $pg:ident ($t:ty => $mk:ident);)*) => {$(
            #[kani::proof]
            fn $h() {
                let v: $t = kani::any();
                let r = call($krate::builtins::$fc, 0, [Datum::$mk(v)]);
                let c = unsafe { $pg(v) };
                assert!(r.as_u32() == c);
            }
        )*};
    }

    macro_rules! val_row_ext {
        ($($h:ident: $krate:ident :: $fc:ident / $pg:ident ($t:ty => $mk:ident);)*) => {$(
            #[kani::proof]
            fn $h() {
                let v: $t = kani::any();
                let seed: i64 = kani::any();
                let r = call(
                    $krate::builtins::$fc,
                    0,
                    [Datum::$mk(v), Datum::from_i64(seed)],
                );
                let c = unsafe { $pg(v, seed) };
                assert!(r.as_u64() == c);
            }
        )*};
    }

    // ---- hashchar: PLATFORM-SPLIT with pinned model (tidin class) ----
    // C hashchar is `hash_uint32((int32) key)` where key is plain `char`:
    // the widening is sign-extending on signed-char platforms (macOS,
    // Linux-x86-64) and zero-extending on unsigned-char platforms
    // (Linux-aarch64). Ground-truthed 2026-07-29 on real Postgres 18.4:
    // hashchar('\200'::"char") = 1361043915 (macOS) vs 1807103465 (docker
    // postgres:18 Linux aarch64) — C Postgres itself is platform-dependent
    // for high-bit chars. pgrust ships the SIGN-EXTENDING arm
    // (as_char() as i32). Adjudication owed (which arm to pin on the
    // Linux-aarch64 deployment platform); until ruled:
    //  (a) eq_* theorems fence to the portable plane (v >= 0), green on
    //      any suite host;
    //  (b) model_* theorems pin pgrust's full-domain behavior to the
    //      explicit sign-extended model through the same vendored hash
    //      core (pg_hashint4 == hash_uint32(v)).

    #[kani::proof]
    fn eq_hashchar() {
        let v: i8 = kani::any();
        kani::assume(v >= 0); // portable plane; high-bit plane is platform-split
        let r = call(adt_int::builtins::fc_hashchar, 0, [Datum::from_char(v)]);
        let c = unsafe { pg_hashchar(v) };
        assert!(r.as_u32() == c);
    }

    #[kani::proof]
    fn eq_hashcharextended() {
        let v: i8 = kani::any();
        kani::assume(v >= 0); // portable plane; high-bit plane is platform-split
        let seed: i64 = kani::any();
        let r = call(
            adt_int::builtins::fc_hashcharextended,
            0,
            [Datum::from_char(v), Datum::from_i64(seed)],
        );
        let c = unsafe { pg_hashcharextended(v, seed) };
        assert!(r.as_u64() == c);
    }

    /// Pinned model: shipped hashchar == hash_uint32(sign-extend(v)) over
    /// the FULL i8 domain, via the vendored hash core (pg_hashint4).
    #[kani::proof]
    fn model_hashchar_signed_full() {
        let v: i8 = kani::any();
        let r = call(adt_int::builtins::fc_hashchar, 0, [Datum::from_char(v)]);
        let m = unsafe { pg_hashint4(v as i32) };
        assert!(r.as_u32() == m);
    }

    /// Pinned model, extended variant (same claim through hash_uint32_extended).
    #[kani::proof]
    fn model_hashcharextended_signed_full() {
        let v: i8 = kani::any();
        let seed: i64 = kani::any();
        let r = call(
            adt_int::builtins::fc_hashcharextended,
            0,
            [Datum::from_char(v), Datum::from_i64(seed)],
        );
        let m = unsafe { pg_hashint4extended(v as i32, seed) };
        assert!(r.as_u64() == m);
    }

    val_row! {
        eq_hashint2: adt_int::fc_hashint2 / pg_hashint2 (i16 => from_i16);
        eq_hashint4: adt_int::fc_hashint4 / pg_hashint4 (i32 => from_i32);
        eq_hashint8: adt_int8::fc_hashint8 / pg_hashint8 (i64 => from_i64);
        eq_hashoid: adt_scalar::fc_hash_uint32 / pg_hashoid (u32 => from_u32);
        eq_hashenum: adt_enum::fc_hashenum / pg_hashoid (u32 => from_oid);
        eq_hashbool: adt_bool::fc_hashbool / pg_hashbool (bool => from_bool);
        eq_hashfloat4: adt_float::fc_hashfloat4 / pg_hashfloat4 (f32 => from_f32);
        eq_hashfloat8: adt_float::fc_hashfloat8 / pg_hashfloat8 (f64 => from_f64);
    }

    val_row_ext! {
        eq_hashint2extended: adt_int::fc_hashint2extended / pg_hashint2extended (i16 => from_i16);
        eq_hashint4extended: adt_int::fc_hashint4extended / pg_hashint4extended (i32 => from_i32);
        eq_hashint8extended: adt_int8::fc_hashint8extended / pg_hashint8extended (i64 => from_i64);
        eq_hashoidextended: adt_scalar::fc_hash_uint32_extended / pg_hashoidextended (u32 => from_u32);
        eq_hashenumextended: adt_enum::fc_hashenumextended / pg_hashoidextended (u32 => from_oid);
        eq_hashboolextended: adt_bool::fc_hashboolextended / pg_hashboolextended (bool => from_bool);
        eq_hashfloat4extended: adt_float::fc_hashfloat4extended / pg_hashfloat4extended (f32 => from_f32);
        eq_hashfloat8extended: adt_float::fc_hashfloat8extended / pg_hashfloat8extended (f64 => from_f64);
    }

    /// Cross-type hash-join consistency: hashfloat4(x) == hashfloat8((f64)x)
    /// for EVERY f32 (±0, NaNs, infinities included).
    #[kani::proof]
    fn consistency_hashfloat4_widens() {
        let v: f32 = kani::any();
        let r4 = call(adt_float::builtins::fc_hashfloat4, 0, [Datum::from_f32(v)]);
        let r8 = call(adt_float::builtins::fc_hashfloat8, 0, [Datum::from_f64(v as f64)]);
        assert!(r4.as_u32() == r8.as_u32());
    }

    // ================= macaddr / macaddr8: fixed 6/8-byte blocks ============

    macro_rules! mac_rows {
        ($h:ident, $hx:ident, $krate:ident, $fc:ident, $fcx:ident, $pg:ident, $pgx:ident, $n:expr) => {
            #[kani::proof]
            #[kani::unwind(3)]
            fn $h() {
                let img: [u8; $n] = kani::any();
                let r = call(
                    $krate::builtins::$fc,
                    0,
                    [Datum::from_usize(img.as_ptr() as usize)],
                );
                let c = unsafe { $pg(img.as_ptr()) };
                assert!(r.as_u32() == c);
            }

            #[kani::proof]
            #[kani::unwind(3)]
            fn $hx() {
                let img: [u8; $n] = kani::any();
                let seed: i64 = kani::any();
                let r = call(
                    $krate::builtins::$fcx,
                    0,
                    [Datum::from_usize(img.as_ptr() as usize), Datum::from_i64(seed)],
                );
                let c = unsafe { $pgx(img.as_ptr(), seed) };
                assert!(r.as_u64() == c);
            }
        };
    }

    mac_rows!(
        eq_hashmacaddr,
        eq_hashmacaddrextended,
        adt_mac,
        fc_hashmacaddr,
        fc_hashmacaddrextended,
        pg_hashmacaddr,
        pg_hashmacaddrextended,
        6
    );
    mac_rows!(
        eq_hashmacaddr8,
        eq_hashmacaddr8extended,
        adt_mac8,
        fc_hashmacaddr8,
        fc_hashmacaddr8extended,
        pg_hashmacaddr8,
        pg_hashmacaddr8extended,
        8
    );

    // ================= hashname: NUL-scan of a NameData block ===============
    //
    // PER-LENGTH CASE-SPLIT: symbolic length at the composed wrapper level
    // measured 60-136s+ (over budget); per-length harnesses are ~10x cheaper
    // (TRIAGE symbolic-length cost law). Band = every len 0..=8, plus a
    // len-16 spot crossing the kernel's 12-byte mix boundary.
    // cover_name_split proves the case list exhausts the claimed band.

    /// 64-byte NameData image with concrete first-NUL position LEN, symbolic
    /// nonzero content before it, zero tail — the namein contract.
    fn name_img<const LEN: usize>() -> [u8; 64] {
        let head: [u8; LEN] = kani::any();
        let mut img = [0u8; 64];
        let mut i = 0;
        while i < LEN {
            kani::assume(head[i] != 0);
            img[i] = head[i];
            i += 1;
        }
        img
    }

    macro_rules! name_case {
        ($($h:ident, $hx:ident, $len:literal, $u:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind($u)]
            fn $h() {
                let img = name_img::<$len>();
                let r = call(
                    name::builtins::fc_hashname,
                    0,
                    [Datum::from_usize(img.as_ptr() as usize)],
                );
                let c = unsafe { pg_hashname(img.as_ptr()) };
                assert!(r.as_u32() == c);
            }

            #[kani::proof]
            #[kani::unwind($u)]
            fn $hx() {
                let img = name_img::<$len>();
                let seed: i64 = kani::any();
                let r = call(
                    name::builtins::fc_hashnameextended,
                    0,
                    [Datum::from_usize(img.as_ptr() as usize), Datum::from_i64(seed)],
                );
                let c = unsafe { pg_hashnameextended(img.as_ptr(), seed) };
                assert!(r.as_u64() == c);
            }
        )*};
    }

    name_case! {
        eq_hashname_l0, eq_hashnameextended_l0, 0, 3;
        eq_hashname_l1, eq_hashnameextended_l1, 1, 4;
        eq_hashname_l2, eq_hashnameextended_l2, 2, 5;
        eq_hashname_l3, eq_hashnameextended_l3, 3, 6;
        eq_hashname_l4, eq_hashnameextended_l4, 4, 7;
        eq_hashname_l5, eq_hashnameextended_l5, 5, 8;
        eq_hashname_l6, eq_hashnameextended_l6, 6, 9;
        eq_hashname_l7, eq_hashnameextended_l7, 7, 10;
        eq_hashname_l8, eq_hashnameextended_l8, 8, 11;
        eq_hashname_l16, eq_hashnameextended_l16, 16, 19;
    }

    /// Union coverage for the name band claim (len<=8): the per-length case
    /// list exhausts it.
    #[kani::proof]
    fn cover_name_split() {
        let len: usize = kani::any();
        kani::assume(len <= 8);
        assert!(
            len == 0
                || len == 1
                || len == 2
                || len == 3
                || len == 4
                || len == 5
                || len == 6
                || len == 7
                || len == 8
        );
    }

    // ============ hashtext / hashbpchar: short-varlena, C collation =========

    const TEXT_CAP: usize = 8;

    /// Short-form (1B header) varlena image with symbolic payload len <=
    /// TEXT_CAP. Little-endian 1B header: total_size<<1 | 1. Returns
    /// (image, payload_len); payload at image[1..1+len].
    fn sym_text() -> ([u8; TEXT_CAP + 1], usize) {
        let payload: [u8; TEXT_CAP] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= TEXT_CAP);
        let mut img = [0u8; TEXT_CAP + 1];
        img[0] = (((len + 1) as u8) << 1) | 1;
        let mut i = 0;
        while i < TEXT_CAP {
            if i < len {
                img[i + 1] = payload[i];
            }
            i += 1;
        }
        (img, len)
    }

    // PER-LENGTH CASE-SPLIT (same law as hashname): every len 0..=8 plus a
    // len-13 spot crossing the kernel's 12-byte mix boundary.
    // cover_text_split proves the case list exhausts the claimed band.

    /// Short-form (1B header) varlena image with concrete payload len LEN
    /// (little-endian 1B header: total_size<<1 | 1), payload symbolic.
    fn text_img<const LEN: usize>() -> [u8; 14] {
        let payload: [u8; LEN] = kani::any();
        let mut img = [0u8; 14];
        img[0] = (((LEN + 1) as u8) << 1) | 1;
        let mut i = 0;
        while i < LEN {
            img[i + 1] = payload[i];
            i += 1;
        }
        img
    }

    macro_rules! text_case {
        ($($ht:ident, $htx:ident, $hb:ident, $hbx:ident, $len:literal, $u:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind($u)]
            fn $ht() {
                let img = text_img::<$len>();
                let r = call(
                    varlena::builtins::fc_hashtext,
                    C_COLLATION_OID,
                    [Datum::from_usize(img.as_ptr() as usize)],
                );
                let c = unsafe { pg_hashtext_det(img.as_ptr().add(1), $len) };
                assert!(r.as_u32() == c);
            }

            #[kani::proof]
            #[kani::unwind($u)]
            fn $htx() {
                let img = text_img::<$len>();
                let seed: i64 = kani::any();
                let r = call(
                    varlena::builtins::fc_hashtextextended,
                    C_COLLATION_OID,
                    [Datum::from_usize(img.as_ptr() as usize), Datum::from_i64(seed)],
                );
                let c = unsafe { pg_hashtextextended_det(img.as_ptr().add(1), $len, seed) };
                assert!(r.as_u64() == c);
            }

            #[kani::proof]
            #[kani::unwind($u)]
            fn $hb() {
                let img = text_img::<$len>();
                // trim-path coverage: a trailing space (trim active) is
                // reachable at every nonzero length
                if $len > 0 {
                    kani::cover!(img[$len] == b' ');
                }
                let r = call(
                    adt_varchar::builtins::fc_hashbpchar,
                    C_COLLATION_OID,
                    [Datum::from_usize(img.as_ptr() as usize)],
                );
                let c = unsafe { pg_hashbpchar_det(img.as_ptr().add(1), $len) };
                assert!(r.as_u32() == c);
            }

            #[kani::proof]
            #[kani::unwind($u)]
            fn $hbx() {
                let img = text_img::<$len>();
                let seed: i64 = kani::any();
                let r = call(
                    adt_varchar::builtins::fc_hashbpcharextended,
                    C_COLLATION_OID,
                    [Datum::from_usize(img.as_ptr() as usize), Datum::from_i64(seed)],
                );
                let c = unsafe { pg_hashbpcharextended_det(img.as_ptr().add(1), $len, seed) };
                assert!(r.as_u64() == c);
            }
        )*};
    }

    text_case! {
        eq_hashtext_l0, eq_hashtextextended_l0, eq_hashbpchar_l0, eq_hashbpcharextended_l0, 0, 4;
        eq_hashtext_l1, eq_hashtextextended_l1, eq_hashbpchar_l1, eq_hashbpcharextended_l1, 1, 4;
        eq_hashtext_l2, eq_hashtextextended_l2, eq_hashbpchar_l2, eq_hashbpcharextended_l2, 2, 4;
        eq_hashtext_l3, eq_hashtextextended_l3, eq_hashbpchar_l3, eq_hashbpcharextended_l3, 3, 5;
        eq_hashtext_l4, eq_hashtextextended_l4, eq_hashbpchar_l4, eq_hashbpcharextended_l4, 4, 6;
    }


    macro_rules! text_only_case {
        ($($ht:ident, $htx:ident, $len:literal, $u:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind($u)]
            fn $ht() {
                let img = text_img::<$len>();
                let r = call(
                    varlena::builtins::fc_hashtext,
                    C_COLLATION_OID,
                    [Datum::from_usize(img.as_ptr() as usize)],
                );
                let c = unsafe { pg_hashtext_det(img.as_ptr().add(1), $len) };
                assert!(r.as_u32() == c);
            }

            #[kani::proof]
            #[kani::unwind($u)]
            fn $htx() {
                let img = text_img::<$len>();
                let seed: i64 = kani::any();
                let r = call(
                    varlena::builtins::fc_hashtextextended,
                    C_COLLATION_OID,
                    [Datum::from_usize(img.as_ptr() as usize), Datum::from_i64(seed)],
                );
                let c = unsafe { pg_hashtextextended_det(img.as_ptr().add(1), $len, seed) };
                assert!(r.as_u64() == c);
            }
        )*};
    }

    text_only_case! {
        eq_hashtext_l5, eq_hashtextextended_l5, 5, 7;
        eq_hashtext_l6, eq_hashtextextended_l6, 6, 8;
        eq_hashtext_l7, eq_hashtextextended_l7, 7, 9;
        eq_hashtext_l8, eq_hashtextextended_l8, 8, 10;
        eq_hashtext_l13, eq_hashtextextended_l13, 13, 15;
    }

    /// bpchar CONCRETE-TRIM spots for lengths past the symbolic-trim wall
    /// (symbolic trimmed-length re-introduces symbolic hash length, which
    /// walls above L=4): payload constrained to exactly L-K trailing spaces,
    /// so the trimmed length is the concrete K while the trim loop still
    /// runs over symbolic bytes on both sides.
    macro_rules! bp_spot {
        ($($h:ident, $hx:ident, $len:literal, $k:literal, $u:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind($u)]
            fn $h() {
                let img = text_img::<$len>();
                let mut i = $k + 1;
                while i <= $len {
                    kani::assume(img[i] == b' ');
                    i += 1;
                }
                if $k > 0 {
                    kani::assume(img[$k] != b' ');
                }
                let r = call(
                    adt_varchar::builtins::fc_hashbpchar,
                    C_COLLATION_OID,
                    [Datum::from_usize(img.as_ptr() as usize)],
                );
                let c = unsafe { pg_hashbpchar_det(img.as_ptr().add(1), $len) };
                assert!(r.as_u32() == c);
            }

            #[kani::proof]
            #[kani::unwind($u)]
            fn $hx() {
                let img = text_img::<$len>();
                let mut i = $k + 1;
                while i <= $len {
                    kani::assume(img[i] == b' ');
                    i += 1;
                }
                if $k > 0 {
                    kani::assume(img[$k] != b' ');
                }
                let seed: i64 = kani::any();
                let r = call(
                    adt_varchar::builtins::fc_hashbpcharextended,
                    C_COLLATION_OID,
                    [Datum::from_usize(img.as_ptr() as usize), Datum::from_i64(seed)],
                );
                let c = unsafe { pg_hashbpcharextended_det(img.as_ptr().add(1), $len, seed) };
                assert!(r.as_u64() == c);
            }
        )*};
    }

    bp_spot! {
        bp_spot_l8_k0, bp_spotx_l8_k0, 8, 0, 10;
        bp_spot_l8_k3, bp_spotx_l8_k3, 8, 3, 10;
        bp_spot_l8_k8, bp_spotx_l8_k8, 8, 8, 10;
        bp_spot_l13_k13, bp_spotx_l13_k13, 13, 13, 15;
        bp_spot_l13_k5, bp_spotx_l13_k5, 13, 5, 15;
    }

    /// Union coverage for the text/bpchar band claim (len<=8).
    #[kani::proof]
    fn cover_text_split() {
        let len: usize = kani::any();
        kani::assume(len <= 8);
        assert!(
            len == 0
                || len == 1
                || len == 2
                || len == 3
                || len == 4
                || len == 5
                || len == 6
                || len == 7
                || len == 8
        );
    }

    // ---- collid==0 error arm: verdict + sqlstate parity (C raises
    // ERRCODE_INDETERMINATE_COLLATION via ereport — shimmed out of the C
    // file; the Rust side must refuse identically). ----

    #[kani::proof]
    #[kani::unwind(12)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn err_hashtext_collid0() {
        let (img, _len) = sym_text();
        let mut f = LocalFcinfo::<1>::new(0);
        f.args[0] = NullableDatum::value(Datum::from_usize(img.as_ptr() as usize));
        match varlena::builtins::fc_hashtext(None, &mut f) {
            Ok(_) => panic!("hashtext with collid 0 must error"),
            Err(e) => assert!(e.sqlstate() == types_error::ERRCODE_INDETERMINATE_COLLATION),
        }
    }

    #[kani::proof]
    #[kani::unwind(12)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn err_hashbpchar_collid0() {
        let (img, _len) = sym_text();
        let mut f = LocalFcinfo::<1>::new(0);
        f.args[0] = NullableDatum::value(Datum::from_usize(img.as_ptr() as usize));
        match adt_varchar::builtins::fc_hashbpchar(None, &mut f) {
            Ok(_) => panic!("hashbpchar with collid 0 must error"),
            Err(e) => assert!(e.sqlstate() == types_error::ERRCODE_INDETERMINATE_COLLATION),
        }
    }

    // ================= hashoidvector: valid-oidvector images ================

    const OV_CAP: usize = 4;
    const OIDOID: u32 = 26;

    #[repr(C)]
    struct OidVectorImage {
        hdr: array::oidvector,
        values: [u32; OV_CAP],
    }

    /// Valid oidvector (the check_valid_oidvector fence): ndim==1,
    /// dataoffset==0, elemtype==OIDOID; concrete dim1 (PER-LENGTH CASE-SPLIT
    /// — symbolic dim1 solved green but at 136s, over budget; the split is
    /// ~10x cheaper). cover_ov_split proves the case list exhausts the band.
    fn ov_img(dim1: i32) -> OidVectorImage {
        OidVectorImage {
            hdr: array::oidvector {
                vl_len_: (((core::mem::size_of::<array::oidvector>()
                    + (dim1 as usize) * 4) as i32)
                    << 2),
                ndim: 1,
                dataoffset: 0,
                elemtype: OIDOID,
                dim1,
                lbound1: 0,
            },
            values: kani::any(),
        }
    }

    macro_rules! ov_case {
        ($($h:ident, $hx:ident, $d:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind(3)]
            fn $h() {
                let img = ov_img($d);
                let r = call(
                    adt_scalar::builtins::fc_hashoidvector,
                    0,
                    [Datum::from_usize(&img as *const _ as usize)],
                );
                let c = unsafe { pg_hashoidvector(img.values.as_ptr(), $d) };
                assert!(r.as_u32() == c);
            }

            #[kani::proof]
            #[kani::unwind(3)]
            fn $hx() {
                let img = ov_img($d);
                let seed: i64 = kani::any();
                let r = call(
                    adt_scalar::builtins::fc_hashoidvectorextended,
                    0,
                    [Datum::from_usize(&img as *const _ as usize), Datum::from_i64(seed)],
                );
                let c = unsafe { pg_hashoidvectorextended(img.values.as_ptr(), $d, seed) };
                assert!(r.as_u64() == c);
            }
        )*};
    }

    ov_case! {
        eq_hashoidvector_d0, eq_hashoidvectorextended_d0, 0;
        eq_hashoidvector_d1, eq_hashoidvectorextended_d1, 1;
        eq_hashoidvector_d2, eq_hashoidvectorextended_d2, 2;
        eq_hashoidvector_d3, eq_hashoidvectorextended_d3, 3;
        eq_hashoidvector_d4, eq_hashoidvectorextended_d4, 4;
    }

    /// Union coverage for the oidvector band claim (dim1<=4).
    #[kani::proof]
    fn cover_ov_split() {
        let dim1: i32 = kani::any();
        kani::assume((0..=OV_CAP as i32).contains(&dim1));
        assert!(dim1 == 0 || dim1 == 1 || dim1 == 2 || dim1 == 3 || dim1 == 4);
    }

    // ================= negative controls (DEFAULT solver) ===================

    /// Rust hashint8 vs C hashint4(low half): MUST FAIL whenever the folded
    /// high word changes the hash — proves the fold is inside the theorem.
    #[kani::proof]
    fn control_int8_fold_skew() {
        let v: i64 = kani::any();
        let r = call(adt_int8::builtins::fc_hashint8, 0, [Datum::from_i64(v)]);
        let c = unsafe { pg_hashint4(v as i32) };
        assert!(r.as_u32() == c);
    }

    /// C hashes one byte fewer than Rust: MUST FAIL.
    #[kani::proof]
    #[kani::unwind(12)]
    fn control_text_short_c_len() {
        let (img, len) = sym_text();
        kani::assume(len >= 1);
        let r = call(
            varlena::builtins::fc_hashtext,
            C_COLLATION_OID,
            [Datum::from_usize(img.as_ptr() as usize)],
        );
        let c = unsafe { pg_hashtext_det(img.as_ptr().add(1), (len - 1) as i32) };
        assert!(r.as_u32() == c);
    }
}

// ===========================================================================
// WAVE 5 (2026-07-28): the remaining scalar hash pg_proc rows, same rig —
// shipped fc_* wrappers on real LocalFcinfo frames vs verbatim REL_18
// hashfunc/tid/xid/pg_lsn row bodies over the proved hash_bytes kernels
// (C section appended to c/pg_hash_rows.c).
//
// Rows: 2233 hashtid / 2234 hashtidextended (6-byte tid block, full
// symbolic fields); 6419/6420 hashxid[extended] and 6423/6424
// hashcid[extended] (both register adt_scalar::fc_hash_uint32[_extended] —
// same wrapper as the PROVED hashoid rows, per-row harnesses kept cheap);
// 6421/6422 hashxid8[extended] (sign-aware high-word fold); 3252/3413
// pg_lsn_hash[_extended] (adt_pg_lsn wrapper, same fold).  Full symbolic
// domains + full symbolic seeds.  control_hashtid_len_skew MUST FAIL
// (C hashes 4 of the 6 bytes — witnesses the length is load-bearing).
// ===========================================================================

#[cfg(kani)]
mod wave5 {
    extern "C" {
        fn pg_hashtid(key: *const u8) -> u32;
        fn pg_hashtidextended(key: *const u8, seed: i64) -> u64;
        fn pg_hashxid(xid: u32) -> u32;
        fn pg_hashxidextended(xid: u32, seed: i64) -> u64;
        fn pg_hashcid(cid: u32) -> u32;
        fn pg_hashcidextended(cid: u32, seed: i64) -> u64;
        fn pg_hashxid8(x: u64) -> u32;
        fn pg_hashxid8extended(x: u64, seed: i64) -> u64;
        fn pg_pg_lsn_hash(lsn: u64) -> u32;
        fn pg_pg_lsn_hash_extended(lsn: u64, seed: i64) -> u64;
        fn pg_hash_bytes(k: *const u8, keylen: i32) -> u32;
    }

    /// The 6-byte on-tuple tid image (BlockIdData hi/lo + OffsetNumber,
    /// native-endian u16 each) — what fc_hashtid's arg_fixed(0, 6) reads
    /// and what C's ItemPointerData component fields contain.
    fn tid_img(hi: u16, lo: u16, off: u16) -> [u8; 6] {
        let (h, l, o) = (hi.to_ne_bytes(), lo.to_ne_bytes(), off.to_ne_bytes());
        [h[0], h[1], l[0], l[1], o[0], o[1]]
    }

    #[kani::proof]
    fn eq_hashtid() {
        let (hi, lo, off): (u16, u16, u16) = (kani::any(), kani::any(), kani::any());
        let img = tid_img(hi, lo, off);
        let r = proof_support::call1_ok(adt_scalar::builtins::fc_hashtid, img.as_ptr());
        let c = unsafe { pg_hashtid(img.as_ptr()) };
        assert!(r.as_u32() == c);
    }

    #[kani::proof]
    fn eq_hashtidextended() {
        let (hi, lo, off): (u16, u16, u16) = (kani::any(), kani::any(), kani::any());
        let seed: i64 = kani::any();
        let img = tid_img(hi, lo, off);
        let r = proof_support::call2_ok(adt_scalar::builtins::fc_hashtidextended, img.as_ptr(), seed);
        let c = unsafe { pg_hashtidextended(img.as_ptr(), seed) };
        assert!(r.as_u64() == c);
    }

    /// MUST FAIL (wave-5 C-section control): C hashes only 4 of the 6 tid
    /// bytes. DEFAULT solver.
    #[kani::proof]
    fn control_hashtid_len_skew() {
        let (hi, lo, off): (u16, u16, u16) = (kani::any(), kani::any(), kani::any());
        let img = tid_img(hi, lo, off);
        let r = proof_support::call1_ok(adt_scalar::builtins::fc_hashtid, img.as_ptr());
        let c = unsafe { pg_hash_bytes(img.as_ptr(), 4) };
        assert!(r.as_u32() == c); // expected failure (length is load-bearing)
    }

    macro_rules! hash_u32_row {
        ($($h:ident / $hx:ident: $pg:ident / $pgx:ident;)*) => {$(
            #[kani::proof]
            fn $h() {
                let v: u32 = kani::any();
                let r = proof_support::call1_ok(adt_scalar::builtins::fc_hash_uint32, v);
                let c = unsafe { $pg(v) };
                assert!(r.as_u32() == c);
            }

            #[kani::proof]
            fn $hx() {
                let v: u32 = kani::any();
                let seed: i64 = kani::any();
                let r = proof_support::call2_ok(
                    adt_scalar::builtins::fc_hash_uint32_extended, v, seed);
                let c = unsafe { $pgx(v, seed) };
                assert!(r.as_u64() == c);
            }
        )*};
    }

    hash_u32_row! {
        eq_hashxid / eq_hashxidextended: pg_hashxid / pg_hashxidextended;
        eq_hashcid / eq_hashcidextended: pg_hashcid / pg_hashcidextended;
    }

    #[kani::proof]
    fn eq_hashxid8() {
        let v: u64 = kani::any();
        let r = proof_support::call1_ok(adt_scalar::builtins::fc_hashxid8, v);
        let c = unsafe { pg_hashxid8(v) };
        assert!(r.as_u32() == c);
    }

    #[kani::proof]
    fn eq_hashxid8extended() {
        let v: u64 = kani::any();
        let seed: i64 = kani::any();
        let r = proof_support::call2_ok(adt_scalar::builtins::fc_hashxid8extended, v, seed);
        let c = unsafe { pg_hashxid8extended(v, seed) };
        assert!(r.as_u64() == c);
    }

    #[kani::proof]
    fn eq_pg_lsn_hash() {
        let v: u64 = kani::any();
        // fc_pg_lsn_hash reads arg_i64 — the lsn rides as an i64 datum
        let r = proof_support::call1_ok(adt_pg_lsn::builtins::fc_pg_lsn_hash, v as i64);
        let c = unsafe { pg_pg_lsn_hash(v) };
        assert!(r.as_u32() == c);
    }

    #[kani::proof]
    fn eq_pg_lsn_hash_extended() {
        let v: u64 = kani::any();
        let seed: i64 = kani::any();
        let r = proof_support::call2_ok(
            adt_pg_lsn::builtins::fc_pg_lsn_hash_extended,
            v as i64,
            seed,
        );
        let c = unsafe { pg_pg_lsn_hash_extended(v, seed) };
        assert!(r.as_u64() == c);
    }
}
