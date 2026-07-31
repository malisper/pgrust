//! proofs/vector-io — oidvectorin (54) / oidvectorout (55) /
//! int2vectorin (40) / int2vectorout (41) equivalence harnesses.
//!
//! Theorem shapes (banded-cell ladders, tidout/oidin precedent):
//!
//! - **in-direction** (wrapper-level: shipped `fc_oidvectorin` /
//!   `fc_int2vectorin` on a real `LocalFcinfo` frame + result-mcx bump
//!   context under the proof_support mcx-stubs recipe): per-length cells
//!   over fully symbolic NUL-free ASCII cstrings, len 0..=5 (the oidin
//!   family measured wall starts at single-token len 6), plus concrete
//!   spots for the longer regimes (multi-element, hex/octal base-0 arms,
//!   range rejects, strtol saturation). Parity: verdict class
//!   (ok / 22P02 / 22003 + level ERROR) and, on accept, the ENTIRE result
//!   image — varlena header, ndim/dataoffset/elemtype/dim1/lbound1, and
//!   every element — against the C loop core's outputs. escontext == NULL
//!   (hard-error) plane; the soft-error ride is out of proof. Message
//!   text/Location out of proof (field-identical PgError stubs).
//! - **out-direction** (wrapper-level: shipped `fc_oidvectorout` /
//!   `fc_int2vectorout` on layout-locked valid images — the
//!   check_valid_* fence is a documented harness precondition): per-dim
//!   cells n in {0,1,2}; oidvector elements symbolic < 1e4 (the sloped
//!   divider law) + concrete spots incl u32::MAX; int2vector elements
//!   FULL symbolic i16 + spots incl i16::MIN. The compared object is the
//!   full cstring image including the NUL terminator, vs the C bodies
//!   with the sprintf("%u") -> pg_ultoa_n spec anchor ([shim V4] in
//!   c/pg_vector_io.c; same ruling as the recorded oidout/cidout rows).
//!
//! Controls (MUST FAIL, default solver): control_ovin_shifted_input
//! (C parses str+1) and control_ovout_value_skew (C hashes values[0]^1).
//!
//! Run: from proofs/vector-io/,
//!   timeout 450 cargo kani -Z c-ffi -Z stubbing \
//!     --c-lib c/pg_vector_io.c --c-lib ../intout/c/pg_intout.c \
//!     --solver kissat --harness proofs::<name> --exact

#[cfg(kani)]
mod proofs {
    use datum::{Datum, NullableDatum};
    use proof_support::{mcx_stubs, stubs};
    use std::os::raw::c_int;
    use types_error::{
        ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, ERROR,
    };
    use types_fmgr::LocalFcinfo;
    use ::allocator_api2::vec::Vec as A2Vec;

    extern "C" {
        fn pg_oidvectorin(s: *const u8, values: *mut u32, cap: c_int, n_out: *mut c_int) -> c_int;
        fn pg_oidvectorout(values: *const u32, dim1: i32, rp: *mut u8) -> c_int;
        fn pg_int2vectorin(s: *const u8, values: *mut i16, cap: c_int, n_out: *mut c_int)
            -> c_int;
        fn pg_int2vectorout(values: *const i16, dim1: i32, rp: *mut u8) -> c_int;
    }

    const OIDOID: u32 = 26;
    const INT2OID: u32 = 21;

    /// Loop-free model of `allocator_api2::vec::Vec::resize` for the
    /// byte-sized instantiations the ovin rig reaches (fc_oidvectorin's
    /// 24-byte header fill, `img.resize(OIDVECTOR_HDRSZ, 0)`). The shipped
    /// body extends via `extend_with`, a per-element loop that unrolls 24
    /// deep and walled/failed every ovin cell at the token-sized unwind
    /// bounds (2026-07-30 measure-sweep FAILED cluster — the ONLY failing
    /// check suite-wide was that loop's unwinding assertion). `write_bytes`
    /// lowers to memset, which CBMC models natively without unrolling;
    /// byte-for-byte identical to the shipped semantics for grow-from-len-0
    /// with a Copy byte element (the only shape in this family). Plumbing
    /// only, never logic — same contract class as the mcx-stubs recipe.
    pub fn stub_vec_resize<T: Clone, A: ::allocator_api2::alloc::Allocator>(
        v: &mut A2Vec<T, A>,
        new_len: usize,
        value: T,
    ) {
        let old = v.len();
        if new_len > old {
            v.reserve(new_len - old);
            // SAFETY: capacity reserved above; T is a no-drop byte type in
            // every instantiation this family reaches (const-asserted).
            unsafe {
                let p = v.as_mut_ptr().add(old);
                assert!(core::mem::size_of::<T>() == 1 && !core::mem::needs_drop::<T>());
                let b = *(&value as *const T as *const u8);
                core::ptr::write_bytes(p.cast::<u8>(), b, new_len - old);
                v.set_len(new_len);
            }
        } else {
            v.truncate(new_len);
        }
    }

    // =================== in-direction shared rig =======================

    /// Symbolic NUL-free ASCII cstring of exact length LEN inside a CAP=8
    /// buffer; slots past the terminator are literal zero (dead-symbolic-
    /// bytes law).
    fn sym_cstring<const LEN: usize>() -> [u8; 8] {
        let mut buf = [0u8; 8];
        let mut i = 0;
        while i < LEN {
            let b: u8 = kani::any();
            kani::assume(b >= 1 && b <= 127);
            buf[i] = b;
            i += 1;
        }
        buf
    }

    /// Ok-arm image check shared by both in-functions.
    /// `elem_size` = 4 (oid) or 2 (int2).
    fn check_img_header(img: &[u8], n: i32, elemtype: u32, elem_size: usize) {
        let total = 24 + elem_size * n as usize;
        let vl = ::datum::varlena::set_varsize_4b(total);
        assert!(img[0] == vl[0] && img[1] == vl[1] && img[2] == vl[2] && img[3] == vl[3]);
        let f = |o: usize| i32::from_ne_bytes([img[o], img[o + 1], img[o + 2], img[o + 3]]);
        assert!(f(4) == 1); // ndim
        assert!(f(8) == 0); // dataoffset
        assert!(f(12) as u32 == elemtype);
        assert!(f(16) == n); // dim1
        assert!(f(20) == 0); // lbound1
    }

    macro_rules! ovin_cell {
        ($($h:ident: $len:literal, unwind=$uw:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind($uw)]
            #[kani::stub(A2Vec::resize, stub_vec_resize)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
            #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(types_error::PgError::new, stubs::stub_pg_error_new)]
            fn $h() {
                let buf = sym_cstring::<$len>();
                ovin_check(&buf);
            }
        )*};
    }

    fn ovin_check(buf: &[u8; 8]) {
        let mut cvals = [0u32; 4];
        let mut cn: c_int = 0;
        let cst = unsafe { pg_oidvectorin(buf.as_ptr(), cvals.as_mut_ptr(), 4, &mut cn) };
        assert!(cst != 99); // repalloc arm unreachable under len <= 7

        let ctx = mcx::MemoryContext::new_bump("kani-ovin");
        let mut f = LocalFcinfo::<1>::new(0);
        // SAFETY: ctx outlives the call (forgotten, never freed).
        unsafe { f.set_result_mcx(ctx.mcx()) };
        f.args[0] = NullableDatum::value(Datum::from_usize(buf.as_ptr() as usize));
        match adt_scalar::builtins::fc_oidvectorin(None, &mut f) {
            Ok(d) => {
                assert!(cst == 0);
                let n = cn;
                let img = unsafe {
                    core::slice::from_raw_parts(d.as_usize() as *const u8, 24 + 4 * n as usize)
                };
                check_img_header(img, n, OIDOID, 4);
                let mut i = 0usize;
                while i < n as usize {
                    let o = 24 + 4 * i;
                    let v = u32::from_ne_bytes([img[o], img[o + 1], img[o + 2], img[o + 3]]);
                    assert!(v == cvals[i]);
                    i += 1;
                }
            }
            Err(e) => {
                assert!(cst == 1 || cst == 2);
                if cst == 1 {
                    assert!(e.sqlstate == ERRCODE_INVALID_TEXT_REPRESENTATION);
                } else {
                    assert!(e.sqlstate == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
                }
                assert!(e.level == ERROR);
                core::mem::forget(e);
            }
        }
        core::mem::forget(ctx);
    }

    // token-count-sized bounds are sound again now that the header fill
    // (img.resize(24, 0) -> extend_with, the 24-deep loop that FAILED the
    // whole ovin family in the 2026-07-30 measure sweep) rides the
    // loop-free stub_vec_resize model.
    ovin_cell! {
        eq_ovin_len0: 0, unwind=4;
        eq_ovin_len1: 1, unwind=5;
        eq_ovin_len2: 2, unwind=6;
        eq_ovin_len3: 3, unwind=7;
        eq_ovin_len4: 4, unwind=8;
        eq_ovin_len5: 5, unwind=9;
    }

    /// Concrete spots for the regimes past the symbolic cells: multi-token,
    /// base-0 hex/octal arms, u32 boundary, range rejects, trailing junk.
    #[kani::proof]
    #[kani::unwind(24)]
    #[kani::stub(A2Vec::resize, stub_vec_resize)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(types_error::PgError::new, stubs::stub_pg_error_new)]
    fn eq_ovin_spots() {
        // each spot padded into the CAP=8 rig where it fits, else a wider
        // concrete buffer through the same core pair
        ovin_spot(b"1 2 3 4\0");
        ovin_spot(b"0x1f 07\0");
        ovin_spot(b" -1 \0\0\0\0");
        ovin_spot(b"1,2\0\0\0\0\0");
        ovin_spot16(b"4294967295 1\0\0\0\0");
        ovin_spot16(b"4294967296\0\0\0\0\0\0");
        ovin_spot16(b"  010 0X2f  \0\0\0\0");
    }

    fn ovin_spot(buf: &[u8; 8]) {
        ovin_check(buf);
    }

    fn ovin_spot16(buf: &[u8; 16]) {
        let mut cvals = [0u32; 4];
        let mut cn: c_int = 0;
        let cst = unsafe { pg_oidvectorin(buf.as_ptr(), cvals.as_mut_ptr(), 4, &mut cn) };
        assert!(cst != 99);
        let ctx = mcx::MemoryContext::new_bump("kani-ovin-spot");
        let mut f = LocalFcinfo::<1>::new(0);
        // SAFETY: ctx outlives the call (forgotten, never freed).
        unsafe { f.set_result_mcx(ctx.mcx()) };
        f.args[0] = NullableDatum::value(Datum::from_usize(buf.as_ptr() as usize));
        match adt_scalar::builtins::fc_oidvectorin(None, &mut f) {
            Ok(d) => {
                assert!(cst == 0);
                let img = unsafe {
                    core::slice::from_raw_parts(d.as_usize() as *const u8, 24 + 4 * cn as usize)
                };
                check_img_header(img, cn, OIDOID, 4);
                let mut i = 0usize;
                while i < cn as usize {
                    let o = 24 + 4 * i;
                    let v = u32::from_ne_bytes([img[o], img[o + 1], img[o + 2], img[o + 3]]);
                    assert!(v == cvals[i]);
                    i += 1;
                }
            }
            Err(e) => {
                assert!((cst == 1 && e.sqlstate == ERRCODE_INVALID_TEXT_REPRESENTATION)
                    || (cst == 2 && e.sqlstate == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE));
                assert!(e.level == ERROR);
                core::mem::forget(e);
            }
        }
        core::mem::forget(ctx);
    }

    /// Both-arm reachability for the in-rig (vacuity insurance; one shared
    /// cover harness per the kissat property-batch lesson).
    #[kani::proof]
    #[kani::unwind(6)]
    #[kani::stub(A2Vec::resize, stub_vec_resize)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(types_error::PgError::new, stubs::stub_pg_error_new)]
    fn cover_ovin_both_arms() {
        let buf = sym_cstring::<2>();
        let ctx = mcx::MemoryContext::new_bump("kani-ovin-cover");
        let mut f = LocalFcinfo::<1>::new(0);
        // SAFETY: ctx outlives the call.
        unsafe { f.set_result_mcx(ctx.mcx()) };
        f.args[0] = NullableDatum::value(Datum::from_usize(buf.as_ptr() as usize));
        match adt_scalar::builtins::fc_oidvectorin(None, &mut f) {
            Ok(d) => {
                kani::cover!(true, "ovin Ok arm reachable");
                let _ = d;
            }
            Err(e) => {
                kani::cover!(
                    e.sqlstate == ERRCODE_INVALID_TEXT_REPRESENTATION,
                    "ovin 22P02 arm reachable"
                );
                core::mem::forget(e);
            }
        }
        core::mem::forget(ctx);
    }

    /// MUST FAIL (in-rig control): C parses from str+1. DEFAULT solver.
    /// (Rides stub_vec_resize like the eq cells so the expected failure is
    /// the VALUE skew, not the header-fill unwinding assertion.)
    #[kani::proof]
    #[kani::unwind(6)]
    #[kani::stub(A2Vec::resize, stub_vec_resize)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(types_error::PgError::new, stubs::stub_pg_error_new)]
    fn control_ovin_shifted_input() {
        let buf = sym_cstring::<2>();
        let mut cvals = [0u32; 4];
        let mut cn: c_int = 0;
        // deliberate skew: C starts one byte in
        let cst =
            unsafe { pg_oidvectorin(buf.as_ptr().add(1), cvals.as_mut_ptr(), 4, &mut cn) };
        let ctx = mcx::MemoryContext::new_bump("kani-ovin-ctl");
        let mut f = LocalFcinfo::<1>::new(0);
        // SAFETY: ctx outlives the call.
        unsafe { f.set_result_mcx(ctx.mcx()) };
        f.args[0] = NullableDatum::value(Datum::from_usize(buf.as_ptr() as usize));
        match adt_scalar::builtins::fc_oidvectorin(None, &mut f) {
            Ok(d) => {
                assert!(cst == 0);
                let img = unsafe {
                    core::slice::from_raw_parts(d.as_usize() as *const u8, 24 + 4 * cn as usize)
                };
                // expected failure: dim1 / values disagree for e.g. "11\0"
                check_img_header(img, cn, OIDOID, 4);
                let mut i = 0usize;
                while i < cn as usize {
                    let o = 24 + 4 * i;
                    let v = u32::from_ne_bytes([img[o], img[o + 1], img[o + 2], img[o + 3]]);
                    assert!(v == cvals[i]);
                    i += 1;
                }
            }
            Err(e) => {
                assert!(cst == 1 || cst == 2);
                core::mem::forget(e);
            }
        }
        core::mem::forget(ctx);
    }

    // int2vectorin cells: same rig over the signed strtol model.
    macro_rules! i2vin_cell {
        ($($h:ident: $len:literal, unwind=$uw:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind($uw)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
            #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(types_error::PgError::new, stubs::stub_pg_error_new)]
            fn $h() {
                let buf = sym_cstring::<$len>();
                i2vin_check(&buf);
            }
        )*};
    }

    fn i2vin_check(buf: &[u8; 8]) {
        let mut cvals = [0i16; 4];
        let mut cn: c_int = 0;
        let cst = unsafe { pg_int2vectorin(buf.as_ptr(), cvals.as_mut_ptr(), 4, &mut cn) };
        assert!(cst != 99);

        let ctx = mcx::MemoryContext::new_bump("kani-i2vin");
        let mut f = LocalFcinfo::<1>::new(0);
        // SAFETY: ctx outlives the call (forgotten, never freed).
        unsafe { f.set_result_mcx(ctx.mcx()) };
        f.args[0] = NullableDatum::value(Datum::from_usize(buf.as_ptr() as usize));
        match adt_int::builtins::fc_int2vectorin(None, &mut f) {
            Ok(d) => {
                // escontext == NULL plane: the soft NULL return is
                // unreachable; a null datum here is a rig defect.
                assert!(!f.isnull);
                assert!(cst == 0);
                let n = cn;
                let img = unsafe {
                    core::slice::from_raw_parts(d.as_usize() as *const u8, 24 + 2 * n as usize)
                };
                check_img_header(img, n, INT2OID, 2);
                let mut i = 0usize;
                while i < n as usize {
                    let o = 24 + 2 * i;
                    let v = i16::from_ne_bytes([img[o], img[o + 1]]);
                    assert!(v == cvals[i]);
                    i += 1;
                }
            }
            Err(e) => {
                assert!((cst == 1 && e.sqlstate == ERRCODE_INVALID_TEXT_REPRESENTATION)
                    || (cst == 2 && e.sqlstate == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE));
                assert!(e.level == ERROR);
                core::mem::forget(e);
            }
        }
        core::mem::forget(ctx);
    }

    i2vin_cell! {
        eq_i2vin_len0: 0, unwind=4;
        eq_i2vin_len1: 1, unwind=5;
        eq_i2vin_len2: 2, unwind=6;
        eq_i2vin_len3: 3, unwind=7;
        eq_i2vin_len4: 4, unwind=8;
        eq_i2vin_len5: 5, unwind=9;
    }

    /// Concrete spots: i16 boundaries, ranges, strtol saturation, trailing
    /// junk after a token, multi-token.
    #[kani::proof]
    #[kani::unwind(28)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(types_error::PgError::new, stubs::stub_pg_error_new)]
    fn eq_i2vin_spots() {
        i2vin_spot(b"1 2 3 4\0");
        i2vin_spot(b"-1 +2 \0\0");
        i2vin_spot(b"32768\0\0\0");
        i2vin_spot(b"1x\0\0\0\0\0\0");
        i2vin_spot24(b"32767 -32768 0\0\0\0\0\0\0\0\0\0\0");
        i2vin_spot24(b"-32769\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0");
        i2vin_spot24(b"99999999999999999999\0\0\0\0");
    }

    fn i2vin_spot(buf: &[u8; 8]) {
        i2vin_check(buf);
    }

    fn i2vin_spot24(buf: &[u8; 24]) {
        let mut cvals = [0i16; 4];
        let mut cn: c_int = 0;
        let cst = unsafe { pg_int2vectorin(buf.as_ptr(), cvals.as_mut_ptr(), 4, &mut cn) };
        assert!(cst != 99);
        let ctx = mcx::MemoryContext::new_bump("kani-i2vin-spot");
        let mut f = LocalFcinfo::<1>::new(0);
        // SAFETY: ctx outlives the call.
        unsafe { f.set_result_mcx(ctx.mcx()) };
        f.args[0] = NullableDatum::value(Datum::from_usize(buf.as_ptr() as usize));
        match adt_int::builtins::fc_int2vectorin(None, &mut f) {
            Ok(d) => {
                assert!(!f.isnull);
                assert!(cst == 0);
                let img = unsafe {
                    core::slice::from_raw_parts(d.as_usize() as *const u8, 24 + 2 * cn as usize)
                };
                check_img_header(img, cn, INT2OID, 2);
                let mut i = 0usize;
                while i < cn as usize {
                    let o = 24 + 2 * i;
                    assert!(i16::from_ne_bytes([img[o], img[o + 1]]) == cvals[i]);
                    i += 1;
                }
            }
            Err(e) => {
                assert!((cst == 1 && e.sqlstate == ERRCODE_INVALID_TEXT_REPRESENTATION)
                    || (cst == 2 && e.sqlstate == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE));
                assert!(e.level == ERROR);
                core::mem::forget(e);
            }
        }
        core::mem::forget(ctx);
    }

    // =================== out-direction shared rig ======================

    /// Layout-locked valid oidvector image (check_valid_oidvector fence is
    /// a harness precondition, as in the recorded oidvector comparator and
    /// hashoidvector rows).
    #[repr(C)]
    struct OidVec4 {
        hdr: array::oidvector,
        values: [u32; 4],
    }

    fn ovec(dim1: i32, values: [u32; 4]) -> OidVec4 {
        OidVec4 {
            hdr: array::oidvector {
                vl_len_: 0, // never read by the compared code
                ndim: 1,
                dataoffset: 0,
                elemtype: OIDOID,
                dim1,
                lbound1: 0,
            },
            values,
        }
    }

    #[repr(C)]
    struct I2Vec4 {
        hdr: array::int2vector,
        values: [i16; 4],
    }

    fn i2vec(dim1: i32, values: [i16; 4]) -> I2Vec4 {
        I2Vec4 {
            hdr: array::int2vector {
                vl_len_: 0,
                ndim: 1,
                dataoffset: 0,
                elemtype: INT2OID,
                dim1,
                lbound1: 0,
            },
            values,
        }
    }

    fn ovout_check(img: &OidVec4) {
        let dim = img.hdr.dim1;
        let mut cbuf = [0u8; 64];
        let clen = unsafe { pg_oidvectorout(img.values.as_ptr(), dim, cbuf.as_mut_ptr()) };

        let ctx = mcx::MemoryContext::new_bump("kani-ovout");
        let mut f = LocalFcinfo::<1>::new(0);
        // SAFETY: ctx outlives the call (forgotten, never freed).
        unsafe { f.set_result_mcx(ctx.mcx()) };
        f.args[0] = NullableDatum::value(Datum::from_usize(img as *const OidVec4 as usize));
        let d = match adt_scalar::builtins::fc_oidvectorout(None, &mut f) {
            Ok(d) => d,
            Err(e) => {
                core::mem::forget(e);
                panic!("oidvectorout errored on a valid image")
            }
        };
        let out =
            unsafe { core::slice::from_raw_parts(d.as_usize() as *const u8, clen as usize + 1) };
        let mut i = 0usize;
        while i <= clen as usize {
            assert!(out[i] == cbuf[i]); // full image incl NUL terminator
            i += 1;
        }
        core::mem::forget(ctx);
    }

    macro_rules! ovout_cell {
        ($($h:ident: dim=$dim:literal, bound=$bound:expr, unwind=$uw:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind($uw)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
            #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $h() {
                let mut values = [0u32; 4];
                let mut i = 0;
                while i < $dim {
                    let v: u32 = kani::any();
                    kani::assume(v < $bound);
                    values[i] = v;
                    i += 1;
                }
                let img = ovec($dim as i32, values);
                ovout_check(&img);
            }
        )*};
    }

    ovout_cell! {
        eq_ovout_n0: dim=0, bound=1u32, unwind=4;
        eq_ovout_n1: dim=1, bound=10_000u32, unwind=8;
        eq_ovout_n2: dim=2, bound=10_000u32, unwind=8;
    }

    /// Concrete spots past the <1e4 band: 10-digit values incl u32::MAX,
    /// dim 3.
    /// unwind 26: the full-image compare loop runs clen+1 = 24 iterations
    /// for "0 4294967295 1000000000" (the old 14 tripped the unwinding
    /// assertion — 2026-07-30 sweep FAILED cluster, harness defect).
    #[kani::proof]
    #[kani::unwind(26)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_ovout_spots() {
        ovout_check(&ovec(3, [0, u32::MAX, 1_000_000_000, 0]));
        ovout_check(&ovec(3, [4_294_967_294, 10_000, 99_999, 0]));
    }

    /// MUST FAIL (out-rig control): C emits values[0]^1. DEFAULT solver.
    #[kani::proof]
    #[kani::unwind(8)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn control_ovout_value_skew() {
        let v: u32 = 42; // concrete: the expected failure must decode fast
        let img = ovec(1, [v, 0, 0, 0]);
        let skew = [v ^ 1, 0, 0, 0];
        let mut cbuf = [0u8; 64];
        let clen = unsafe { pg_oidvectorout(skew.as_ptr(), 1, cbuf.as_mut_ptr()) };
        let ctx = mcx::MemoryContext::new_bump("kani-ovout-ctl");
        let mut f = LocalFcinfo::<1>::new(0);
        // SAFETY: ctx outlives the call.
        unsafe { f.set_result_mcx(ctx.mcx()) };
        f.args[0] = NullableDatum::value(Datum::from_usize(&img as *const OidVec4 as usize));
        let d = match adt_scalar::builtins::fc_oidvectorout(None, &mut f) {
            Ok(d) => d,
            Err(e) => {
                core::mem::forget(e);
                panic!("oidvectorout errored on a valid image")
            }
        };
        let out =
            unsafe { core::slice::from_raw_parts(d.as_usize() as *const u8, clen as usize + 1) };
        let mut i = 0usize;
        while i <= clen as usize {
            assert!(out[i] == cbuf[i]); // expected failure (value skew)
            i += 1;
        }
        core::mem::forget(ctx);
    }

    fn i2vout_check(img: &I2Vec4) {
        let dim = img.hdr.dim1;
        let mut cbuf = [0u8; 32];
        let clen = unsafe { pg_int2vectorout(img.values.as_ptr(), dim, cbuf.as_mut_ptr()) };

        let ctx = mcx::MemoryContext::new_bump("kani-i2vout");
        let mut f = LocalFcinfo::<1>::new(0);
        // SAFETY: ctx outlives the call (forgotten, never freed).
        unsafe { f.set_result_mcx(ctx.mcx()) };
        f.args[0] = NullableDatum::value(Datum::from_usize(img as *const I2Vec4 as usize));
        let d = match adt_int::builtins::fc_int2vectorout(None, &mut f) {
            Ok(d) => d,
            Err(e) => {
                core::mem::forget(e);
                panic!("int2vectorout errored on a valid image")
            }
        };
        let out =
            unsafe { core::slice::from_raw_parts(d.as_usize() as *const u8, clen as usize + 1) };
        let mut i = 0usize;
        while i <= clen as usize {
            assert!(out[i] == cbuf[i]);
            i += 1;
        }
        core::mem::forget(ctx);
    }

    macro_rules! i2vout_cell {
        ($($h:ident: dim=$dim:literal, unwind=$uw:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind($uw)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
            #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $h() {
                let mut values = [0i16; 4];
                let mut i = 0;
                while i < $dim {
                    values[i] = kani::any(); // FULL symbolic i16
                    i += 1;
                }
                let img = i2vec($dim as i32, values);
                i2vout_check(&img);
            }
        )*};
    }

    i2vout_cell! {
        eq_i2vout_n0: dim=0, unwind=4;
        eq_i2vout_n1: dim=1, unwind=9;
        eq_i2vout_n2: dim=2, unwind=16;
    }

    /// dim-2 banded cell: elements in (-100,100) (the full-i16 dim-2 cell
    /// is a measured memory wall at the 6GB local cap).
    #[kani::proof]
    #[kani::unwind(12)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_i2vout_n2_band100() {
        let (a, b): (i16, i16) = (kani::any(), kani::any());
        kani::assume(a > -100 && a < 100 && b > -100 && b < 100);
        let img = i2vec(2, [a, b, 0, 0]);
        i2vout_check(&img);
    }

    /// Concrete spots: dim 3 with the signed boundaries. ONE context per
    /// harness (multi-context harnesses walled on registry machinery).
    #[kani::proof]
    #[kani::unwind(18)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_i2vout_spot_bounds() {
        i2vout_check(&i2vec(3, [i16::MIN, -1, i16::MAX, 0]));
    }

    /// Second boundary spot cell (same shape, one context).
    #[kani::proof]
    #[kani::unwind(16)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_i2vout_spot_mixed() {
        i2vout_check(&i2vec(3, [0, 10_000, -9_999, 0]));
    }
}
