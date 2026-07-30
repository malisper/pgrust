//! Kani C≡Rust equivalence: TYPCACHE-INSTANTIATION PROBE — generic container
//! operators (arrays, ranges) proven for the CONCRETE element types int4,
//! int8, date, uuid and text, with the typcache seam stubbed IDENTICALLY on
//! both sides.
//!
//! THE CLAIM IS PER-INSTANTIATION, NOT GENERIC: each theorem reads "given
//! element type T (typcache entry stubbed on both sides to T's pg_type
//! attributes and concrete comparators), the container operator behaves
//! identically". Nothing is claimed about other element types or about
//! typcache lookup internals. Instantiations:
//!   int4  typlen=4  byval 'i'  fc_int4eq / fc_btint4cmp
//!   int8  typlen=8  byval 'd'  fc_int8eq / fc_btint8cmp
//!   date  typlen=4  byval 'i'  fc_date_eq / fc_date_cmp
//!   uuid  typlen=16 byref 'c'  fc_uuid_eq / fc_uuid_cmp (byref fixed-len
//!         element: fetch_att pointer datums + 16-byte memcmp comparator).
//!         STATUS 2026-07-28: uuid[] equality harnesses are wall(memory) at
//!         the 6GiB solve cap — propositional reduction of the byref
//!         16-byte element walk exceeds the cap on BOTH solvers, at every
//!         rung of the ladder (nelems<=1/2, no nulls, no ndim0, dead
//!         symbolic bytes zeroed, covers hoisted, reach-checks off). One
//!         unwatchdogged run completed at 306s solver time, so the formula
//!         is solvable just above the cap — harnesses kept at FULL claim
//!         strength below for re-open on a bigger box. The
//!         control_array_seam_skew_uuid must-fail control DOES complete
//!         (17s), so the uuid seam plumbing itself is exercised.
//!   text  typlen=-1 byref 'i'  fc_texteq / fc_bttextcmp — varlena element
//!         walk (short 1B-header images, VARSIZE_ANY stepping) in-theorem.
//!         COLLATION FENCE: C_COLLATION_OID only (text-cmp precedent); the
//!         C locale arm is poisoned, so a green proof also shows it is
//!         unreachable under the fence. Element lengths are PER-DESCRIPTOR
//!         FIXED (len<=4 slices, text-cmp small-length precedent) with
//!         fully symbolic contents; text arrays carry NO NULLS (the null
//!         bitmap walk is element-type-independent code proven at the
//!         int4/int8/date/uuid instantiations).
//! Range instantiations: int4range (+canonical), int8range, daterange —
//! canonical finfo for the latter two is INVALID on BOTH sides (only
//! range_adjacent consults it; adjacent is out of scope there, ledger wall).
//!
//! Rust side (shipped code, path-dep — never copied):
//!   - arrayfuncs::ops::{array_cmp_core, array_eq_loop} — the complete
//!     post-typcache slice of C array_cmp / array_eq (element walk incl.
//!     null bitmap, fmgr element invocation through the shipped
//!     LocalFcinfo/FmgrInfo::invoke machinery, dimensionality tiebreakers).
//!     The concrete comparator FmgrInfo is built with the SHIPPED
//!     nbt_compare::builtins::fc_btint4cmp / adt_int::builtins::fc_int4eq
//!     wrappers, so datum unwrap/pack is inside the theorem.
//!   - adt_rangetypes::ops::{range_eq_internal, ..., range_cmp_internal},
//!     rangetypes::{range_is_empty, range_serialize, make_range,
//!     canonical_adjust_i32} — driven through a hand-built RangeInfo whose
//!     fields mirror the C static typcache entry exactly.
//! C side: c/pg_typcache_inst.c — verbatim REL_18_STABLE arrayfuncs.c /
//! rangetypes.c / nbtcompare.c / int.c bodies (provenance + all shims in its
//! header).
//!
//! Both sides consume the SAME serialized images, built by the harness per
//! the on-disk spec (numeric-probe image-builder precedent):
//!   - arrays: 1-D ArrayType, nelems<=4 symbolic, symbolic elements, lbound,
//!     and per-element null flags (null bitmap layout in-theorem); array_cmp
//!     additionally covers the ndim=0 empty form and fully independent
//!     dims/lbounds (tiebreaker arms in-theorem).
//!   - ranges: (flags, lower, upper) with FULLY SYMBOLIC flags byte (all 256
//!     values incl. RANGE_LB_NULL/UB_NULL/CONTAIN_EMPTY bits) and full-i32
//!     symbolic bounds; bounds present in the image iff
//!     RANGE_HAS_LBOUND/UBOUND(flags), the same rule both deserializers use.
//!
//! Claim boundaries (mirror into the ledger):
//!   - PER-TYPE: int4 instantiation only (see above).
//!   - Array rows are proven at the post-typcache core slice; the shipped
//!     fmgr wrappers' argument fetch (arg_array_bytes detoast/flatten) and,
//!     for array_eq/array_ne only, the caller-level dims fast-path
//!     (ops.rs array_eq_internal — code-identical to the fenced C check)
//!     stay in the tested tier. eq/ne harnesses fence both arrays to the
//!     same (ndim=1, dim, lbound); ALL other structure is symbolic.
//!     The one-line </<=/>/>=/!= result mappings of the Rust fc_ wrappers
//!     are asserted in-harness against C's FULL verbatim operator bodies.
//!   - Range rows are proven at the *_internal slice both sides share; the
//!     Rust fc_ wrapper (arg fetch + cached_range_info memo) stays in the
//!     tested tier. elem_contained_by = contains_elem with swapped args on
//!     both sides (wrappers vendored/shipped are arg swaps).
//!   - DETOASTING OUT OF SCOPE (bytea-cmp precedent): images model the
//!     post-detoast caller contract; expanded arrays never fed (the verbatim
//!     VARATT_IS_EXPANDED_HEADER test takes the flat arm).
//!   - Error paths: verdict parity only (C PROOF_EREPORT_FLAG convention vs
//!     Rust Err arm); reachable in-domain only for range_adjacent's
//!     int4range_canonical INT32_MAX overflow (covered + witnessed).
//!     Message text/sqlstate out of proof (PgError::error/format stubbed).
//!   - Harness scaffolding excluded from the claim (brin-minmax precedent):
//!     proof_support static-buffer allocator model
//!     (Mcx::{allocate,grow,deallocate} stubbed) with an OPAQUE DUMMY
//!     MemoryContext no theorem code reads; env/OnceLock stubs;
//!     types_fmgr::fcinfo::opaque_false stubbed value-identically (shipped fn
//!     is aarch64 inline asm, Kani-unsupported); mem::forget of
//!     context-carrying values; C two-slot palloc0. Ledger wording:
//!     "modulo static-buffer allocator model".
//!
//! Controls (DEFAULT solver, must FAIL):
//!   - control_array_lt_vs_c_le / control_range_lt_vs_c_le — rig
//!     non-vacuity (mismatched operators).
//!   - control_array_seam_skew — Rust told typalign='d' while C is told
//!     typalign='i' for the same images: proves the stubbed seam attributes
//!     are load-bearing (a skewed seam model cannot pass).
//!
//! Run recipe:
//!   cd proofs/typcache-inst
//!   timeout 30 cargo kani -Z c-ffi -Z stubbing --c-lib c/pg_typcache_inst.c \
//!       --solver kissat --harness proofs::<eq_*> --exact
//!   (controls: default solver, expect VERIFICATION FAILED)
//!   ARRAY harnesses additionally need --no-assertion-reach-checks (each
//!   reach check = one full external-kissat solve; eq_array_cmp_date went
//!   timeout(400s) -> 173s with it) and release-gate timeouts (75-830s
//!   measured under load, kissat re-solves per property batch).

#[cfg(kani)]
mod proofs {
    use arrayfuncs::ops::{array_cmp_core, array_eq_loop, ElemMeta};
    use adt_rangetypes as rt;
    use datum::Datum;
    use proof_support::{mcx_stubs, stubs};
    use std::os::raw::c_int;
    use types_error::PgResult;
    use types_fmgr::FmgrInfo;

    extern "C" {
        fn pg_c_get_err() -> c_int;

        fn pg_c_array_eq(a1: *const core::ffi::c_void, a2: *const core::ffi::c_void, coll: u32) -> c_int;
        fn pg_c_array_ne(a1: *const core::ffi::c_void, a2: *const core::ffi::c_void, coll: u32) -> c_int;
        fn pg_c_array_lt(a1: *const core::ffi::c_void, a2: *const core::ffi::c_void, coll: u32) -> c_int;
        fn pg_c_array_gt(a1: *const core::ffi::c_void, a2: *const core::ffi::c_void, coll: u32) -> c_int;
        fn pg_c_array_le(a1: *const core::ffi::c_void, a2: *const core::ffi::c_void, coll: u32) -> c_int;
        fn pg_c_array_ge(a1: *const core::ffi::c_void, a2: *const core::ffi::c_void, coll: u32) -> c_int;
        fn pg_c_btarraycmp(a1: *const core::ffi::c_void, a2: *const core::ffi::c_void, coll: u32) -> i32;

        fn pg_c_range_eq(r1: *const core::ffi::c_void, r2: *const core::ffi::c_void) -> c_int;
        fn pg_c_range_ne(r1: *const core::ffi::c_void, r2: *const core::ffi::c_void) -> c_int;
        fn pg_c_range_contains(r1: *const core::ffi::c_void, r2: *const core::ffi::c_void) -> c_int;
        fn pg_c_range_contained_by(r1: *const core::ffi::c_void, r2: *const core::ffi::c_void) -> c_int;
        fn pg_c_range_before(r1: *const core::ffi::c_void, r2: *const core::ffi::c_void) -> c_int;
        fn pg_c_range_after(r1: *const core::ffi::c_void, r2: *const core::ffi::c_void) -> c_int;
        fn pg_c_range_overlaps(r1: *const core::ffi::c_void, r2: *const core::ffi::c_void) -> c_int;
        fn pg_c_range_adjacent(r1: *const core::ffi::c_void, r2: *const core::ffi::c_void) -> c_int;
        fn pg_c_range_contains_elem(r: *const core::ffi::c_void, val: i32) -> c_int;
        fn pg_c_range_contains_elem64(r: *const core::ffi::c_void, val: i64) -> c_int;
        fn pg_c_elem_contained_by(val: u64, r: *const core::ffi::c_void) -> c_int;
        fn pg_c_range_cmp(r1: *const core::ffi::c_void, r2: *const core::ffi::c_void) -> i32;
        fn pg_c_range_lt(r1: *const core::ffi::c_void, r2: *const core::ffi::c_void) -> c_int;
        fn pg_c_range_le(r1: *const core::ffi::c_void, r2: *const core::ffi::c_void) -> c_int;
        fn pg_c_range_ge(r1: *const core::ffi::c_void, r2: *const core::ffi::c_void) -> c_int;
        fn pg_c_range_gt(r1: *const core::ffi::c_void, r2: *const core::ffi::c_void) -> c_int;
        fn pg_c_range_empty(r1: *const core::ffi::c_void) -> c_int;
    }

    const INT4OID: u32 = 23;
    const INT4RANGEOID: u32 = 3904;
    const F_INT4EQ: u32 = 65;
    const F_BTINT4CMP: u32 = 351;
    const F_INT4RANGE_CANONICAL: u32 = 3914;

    // ---------- harness plumbing (brin-minmax precedent) ----------

    /// Unwrap without dragging Debug/format machinery into the formula; the
    /// Err box is forgotten (error-drop trap).
    fn ok<T>(r: PgResult<T>) -> T {
        match r {
            Ok(v) => v,
            Err(e) => {
                core::mem::forget(e);
                panic!("unexpected PgError");
            }
        }
    }

    /// Stub for types_fmgr::fcinfo::opaque_false: the shipped fn is an
    /// aarch64 inline-asm `mov wzr` returning 0 (a codegen-barrier trick);
    /// Kani has no InlineAsm support. Value-identical.
    fn stub_opaque_false() -> u8 {
        0
    }

    /// Opaque dummy context: the Mcx handle is just &MemoryContext, and with
    /// Mcx::{allocate,grow,deallocate} stubbed to the proof heap, no code in
    /// the theorem reads the pointee. Replaces MemoryContext::new_bump, whose
    /// accounting/pool/TLS construction machinery walls symex.
    fn dummy_mcx() -> mcx::Mcx<'static> {
        const _: () = assert!(core::mem::size_of::<mcx::MemoryContext>() <= 1024);
        const _: () = assert!(core::mem::align_of::<mcx::MemoryContext>() <= 16);
        static SLOT: DummySlot = DummySlot([0u8; 1024]);
        // SAFETY: never read — every Allocator entry point is stubbed and the
        // harness forgets all context-carrying values (no drops).
        let ctx: &'static mcx::MemoryContext =
            unsafe { &*(SLOT.0.as_ptr() as *const mcx::MemoryContext) };
        ctx.mcx()
    }

    #[repr(align(16))]
    struct DummySlot([u8; 1024]);
    // SAFETY: the slot is never actually read or written through.
    unsafe impl Sync for DummySlot {}

    /// Shared stub set for every harness in this family.
    macro_rules! tci_harness {
        ($(#[$m:meta])* fn $h:ident() $body:block) => {
            #[kani::proof]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
            #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            // message-text machinery walls symex; value/verdict stays in-theorem
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            #[kani::stub(std::string::String::from_utf8_lossy, stubs::stub_from_utf8_lossy)]
            // shipped opaque_false is aarch64 inline asm (Kani-unsupported)
            #[kani::stub(types_fmgr::fcinfo::opaque_false, stub_opaque_false)]
            $(#[$m])*
            fn $h() $body
        };
    }

    // ---------- the concrete int4 seam instantiation ----------

    /// The concrete int4 element attributes — MUST stay identical to the C
    /// side's pg_int4_typentry (the stubbed typcache seam).
    fn int4_meta() -> ElemMeta {
        ElemMeta { typlen: 4, typbyval: true, typalign: b'i' }
    }

    fn cmp_finfo() -> FmgrInfo {
        FmgrInfo::new(nbt_compare::builtins::fc_btint4cmp, F_BTINT4CMP, 2, true, false)
    }

    fn eq_finfo() -> FmgrInfo {
        FmgrInfo::new(adt_int::builtins::fc_int4eq, F_INT4EQ, 2, true, false)
    }

    /// Rust mirror of the C int4range typcache entry.
    fn int4range_info() -> rt::RangeInfo {
        rt::RangeInfo {
            pin: None,
            rngtypid: INT4RANGEOID,
            collation: 0,
            elem_typid: INT4OID,
            elem: rt::ElemInfo { typlen: 4, typbyval: true, typalign: b'i', typstorage: b'p' },
            cmp: cmp_finfo(),
            canonical_oid: F_INT4RANGE_CANONICAL,
            elem_hash: None,
            elem_hash_extended: None,
            // constructor2-only props, unused by the ops under proof
            own_typlen: -1,
            own_typbyval: false,
            own_typalign: b'd',
        }
    }

    // ---------------- array images (on-disk spec) ----------------

    /// 1-D int4 ArrayType image: 24B header (16 fixed + dim + lbound)
    /// [+ 1B null bitmap padded to dataoffset 32] + packed non-null elems.
    #[repr(align(8))]
    struct ABuf([u8; 48]);

    fn put_i32(b: &mut ABuf, off: usize, v: i32) {
        b.0[off..off + 4].copy_from_slice(&v.to_ne_bytes());
    }

    /// `ndim0`: the empty-array (ndim=0, 16-byte) form. Otherwise 1-D with
    /// `n` logical elements, per-element null flags, symbolic lbound.
    fn build_arr(ndim0: bool, n: usize, lb: i32, nulls: &[bool; 4], elems: &[i32; 4]) -> (ABuf, usize) {
        let mut b = ABuf([0; 48]);
        if ndim0 {
            b.0[0..4].copy_from_slice(&(16u32 << 2).to_ne_bytes());
            put_i32(&mut b, 4, 0); // ndim
            put_i32(&mut b, 8, 0); // dataoffset
            put_i32(&mut b, 12, INT4OID as i32);
            return (b, 16);
        }
        let hasnull = (0..n).any(|i| nulls[i]);
        let data_off = if hasnull { 32 } else { 24 };
        let mut off = data_off;
        for i in 0..4 {
            if i < n && !nulls[i] {
                put_i32(&mut b, off, elems[i]);
                off += 4;
            }
        }
        let total = off;
        b.0[0..4].copy_from_slice(&((total as u32) << 2).to_ne_bytes());
        put_i32(&mut b, 4, 1); // ndim
        put_i32(&mut b, 8, if hasnull { 32 } else { 0 });
        put_i32(&mut b, 12, INT4OID as i32);
        put_i32(&mut b, 16, n as i32); // dim0
        put_i32(&mut b, 20, lb); // lbound0
        if hasnull {
            let mut bm = 0u8;
            for i in 0..4 {
                if i < n && !nulls[i] {
                    bm |= 1 << i;
                }
            }
            b.0[24] = bm;
        }
        (b, total)
    }

    struct SymArr {
        buf: ABuf,
        total: usize,
        ndim0: bool,
        n: usize,
        lb: i32,
        nulls: [bool; 4],
    }

    fn sym_arr(allow_ndim0: bool) -> SymArr {
        let ndim0: bool = if allow_ndim0 { kani::any() } else { false };
        let n: usize = kani::any();
        kani::assume(n <= 4);
        let lb: i32 = kani::any();
        let nulls: [bool; 4] = kani::any();
        let elems: [i32; 4] = kani::any();
        let (buf, total) = build_arr(ndim0, n, lb, &nulls, &elems);
        SymArr { buf, total, ndim0, n, lb, nulls }
    }

    fn cptr(b: &ABuf) -> *const core::ffi::c_void {
        b.0.as_ptr().cast()
    }

    // ---- array_eq / array_ne: same-dims fence (see module docs) ----

    macro_rules! array_eq_harness {
        ($name:ident, $cfn:ident, $negate:expr) => {
            tci_harness! {
                #[kani::unwind(6)]
                fn $name() {
                    let n: usize = kani::any();
                    kani::assume(n <= 4);
                    let lb: i32 = kani::any();
                    let (nulls1, elems1): ([bool; 4], [i32; 4]) = (kani::any(), kani::any());
                    let (nulls2, elems2): ([bool; 4], [i32; 4]) = (kani::any(), kani::any());
                    let (b1, t1) = build_arr(false, n, lb, &nulls1, &elems1);
                    let (b2, t2) = build_arr(false, n, lb, &nulls2, &elems2);

                    let c = unsafe { $cfn(cptr(&b1), cptr(&b2), 0) };
                    assert!(unsafe { pg_c_get_err() } == 0);

                    let mut eqfn = eq_finfo();
                    let r = ok(array_eq_loop(dummy_mcx(), &b1.0[..t1], &b2.0[..t2], 0, int4_meta(), &mut eqfn));
                    let r = if $negate { !r } else { r };
                    assert!(r as c_int == c);

                    // regime reachability witnesses
                    kani::cover!(n > 0 && nulls1[0] && nulls2[0]); // null==null skip
                    kani::cover!(n > 0 && nulls1[0] && !nulls2[0]); // null mismatch
                    kani::cover!(n > 0 && c == (!$negate) as c_int); // op-true with elems
                    kani::cover!(n > 0 && c == ($negate) as c_int); // op-false with elems
                }
            }
        };
    }

    array_eq_harness!(eq_array_eq_int4, pg_c_array_eq, false);
    array_eq_harness!(eq_array_ne_int4, pg_c_array_ne, true);

    // ---- array_cmp family: fully independent structure both sides ----

    fn rust_array_cmp(a: &SymArr, b: &SymArr, meta: ElemMeta) -> i32 {
        let mut cmpfn = cmp_finfo();
        ok(array_cmp_core(dummy_mcx(), &a.buf.0[..a.total], &b.buf.0[..b.total], 0, meta, &mut cmpfn))
    }

    tci_harness! {
        #[kani::unwind(6)]
        fn eq_array_cmp_int4() {
            let a = sym_arr(true);
            let b = sym_arr(true);
            let c = unsafe { pg_c_btarraycmp(cptr(&a.buf), cptr(&b.buf), 0) };
            assert!(unsafe { pg_c_get_err() } == 0);
            let r = rust_array_cmp(&a, &b, int4_meta());
            assert!(r == c);

            // regime reachability witnesses
            kani::cover!(r == 0);
            kani::cover!(r < 0 && a.n == b.n && !a.ndim0 && !b.ndim0); // element decide
            kani::cover!(a.n > 0 && a.nulls[0] && !b.nulls[0] && !a.ndim0 && !b.ndim0); // null > non-null
            kani::cover!(r != 0 && a.n != b.n); // nitems tiebreak
            kani::cover!(r != 0 && a.n == b.n && a.lb != b.lb && !a.ndim0 && !b.ndim0); // lbound tiebreak
            kani::cover!(a.ndim0 != b.ndim0 && r != 0); // ndims tiebreak
        }
    }

    macro_rules! array_ineq_harness {
        ($name:ident, $cfn:ident, $map:expr) => {
            tci_harness! {
                #[kani::unwind(6)]
                fn $name() {
                    let a = sym_arr(true);
                    let b = sym_arr(true);
                    let c = unsafe { $cfn(cptr(&a.buf), cptr(&b.buf), 0) };
                    assert!(unsafe { pg_c_get_err() } == 0);
                    let r = rust_array_cmp(&a, &b, int4_meta());
                    let map: fn(i32) -> bool = $map;
                    assert!(map(r) as c_int == c);
                }
            }
        };
    }

    array_ineq_harness!(eq_array_lt_int4, pg_c_array_lt, |r| r < 0);
    array_ineq_harness!(eq_array_gt_int4, pg_c_array_gt, |r| r > 0);
    array_ineq_harness!(eq_array_le_int4, pg_c_array_le, |r| r <= 0);
    array_ineq_harness!(eq_array_ge_int4, pg_c_array_ge, |r| r >= 0);

    // ---------------- range images (on-disk spec) ----------------

    #[repr(align(8))]
    struct RBuf([u8; 24]);

    /// int4range image: 8B header (vl_len + rngtypid), bounds present iff
    /// RANGE_HAS_LBOUND/UBOUND(flags), flags byte last. `flags` is FULLY
    /// symbolic u8 at every call site.
    fn build_range(flags: u8, lo: i32, hi: i32) -> (RBuf, usize) {
        let mut b = RBuf([0; 24]);
        b.0[4..8].copy_from_slice(&INT4RANGEOID.to_ne_bytes());
        let mut off = 8usize;
        if rt::range_has_lbound(flags) {
            b.0[off..off + 4].copy_from_slice(&lo.to_ne_bytes());
            off += 4;
        }
        if rt::range_has_ubound(flags) {
            b.0[off..off + 4].copy_from_slice(&hi.to_ne_bytes());
            off += 4;
        }
        b.0[off] = flags;
        off += 1;
        b.0[0..4].copy_from_slice(&((off as u32) << 2).to_ne_bytes());
        (b, off)
    }

    struct SymRange {
        buf: RBuf,
        total: usize,
        flags: u8,
    }

    fn sym_range() -> SymRange {
        let flags: u8 = kani::any();
        let lo: i32 = kani::any();
        let hi: i32 = kani::any();
        let (buf, total) = build_range(flags, lo, hi);
        SymRange { buf, total, flags }
    }

    fn rptr(b: &RBuf) -> *const core::ffi::c_void {
        b.0.as_ptr().cast()
    }

    // ---- simple range pair operators (no allocation, no error in-domain) ----

    macro_rules! range_pair_harness {
        ($($name:ident: $cfn:ident / $rop:ident;)*) => {$(
            tci_harness! {
                #[kani::unwind(4)]
                fn $name() {
                    let a = sym_range();
                    let b = sym_range();
                    let c = unsafe { $cfn(rptr(&a.buf), rptr(&b.buf)) };
                    assert!(unsafe { pg_c_get_err() } == 0);
                    let mut ri = int4range_info();
                    let r = ok(rt::ops::$rop(dummy_mcx(), &mut ri, &a.buf.0[..a.total], &b.buf.0[..b.total]));
                    assert!(r as c_int == c);
                    // flag-lattice reachability witnesses
                    kani::cover!(a.flags & rt::RANGE_EMPTY != 0 && b.flags & rt::RANGE_EMPTY != 0);
                    kani::cover!(a.flags & rt::RANGE_EMPTY != 0 && b.flags & rt::RANGE_EMPTY == 0);
                    kani::cover!(a.flags & (rt::RANGE_LB_INF | rt::RANGE_UB_INF) != 0 && c != 0);
                    kani::cover!(c != 0);
                    kani::cover!(c == 0);
                    core::mem::forget(ri);
                }
            }
        )*};
    }

    range_pair_harness! {
        eq_range_eq_int4range: pg_c_range_eq / range_eq_internal;
        eq_range_ne_int4range: pg_c_range_ne / range_ne_internal;
        eq_range_contains_int4range: pg_c_range_contains / range_contains_internal;
        eq_range_contained_by_int4range: pg_c_range_contained_by / range_contained_by_internal;
        eq_range_before_int4range: pg_c_range_before / range_before_internal;
        eq_range_after_int4range: pg_c_range_after / range_after_internal;
        eq_range_overlaps_int4range: pg_c_range_overlaps / range_overlaps_internal;
    }

    // ---- range_cmp + btree wrappers ----

    fn rust_range_cmp(a: &SymRange, b: &SymRange) -> i32 {
        let mut ri = int4range_info();
        let r = ok(rt::ops::range_cmp_internal(dummy_mcx(), &mut ri, &a.buf.0[..a.total], &b.buf.0[..b.total]));
        core::mem::forget(ri);
        r
    }

    tci_harness! {
        #[kani::unwind(4)]
        fn eq_range_cmp_int4range() {
            let a = sym_range();
            let b = sym_range();
            let c = unsafe { pg_c_range_cmp(rptr(&a.buf), rptr(&b.buf)) };
            assert!(unsafe { pg_c_get_err() } == 0);
            let r = rust_range_cmp(&a, &b);
            assert!(r == c);
            kani::cover!(r == 0 && a.flags & rt::RANGE_EMPTY == 0); // equal non-empty
            kani::cover!(r == -1 && a.flags & rt::RANGE_EMPTY != 0); // empty sorts first
            kani::cover!(r != 0 && a.flags & rt::RANGE_LB_INC != b.flags & rt::RANGE_LB_INC); // inclusivity tiebreak
        }
    }

    macro_rules! range_ineq_harness {
        ($name:ident, $cfn:ident, $map:expr) => {
            tci_harness! {
                #[kani::unwind(4)]
                fn $name() {
                    let a = sym_range();
                    let b = sym_range();
                    let c = unsafe { $cfn(rptr(&a.buf), rptr(&b.buf)) };
                    assert!(unsafe { pg_c_get_err() } == 0);
                    let r = rust_range_cmp(&a, &b);
                    let map: fn(i32) -> bool = $map;
                    assert!(map(r) as c_int == c);
                }
            }
        };
    }

    range_ineq_harness!(eq_range_lt_int4range, pg_c_range_lt, |r| r < 0);
    range_ineq_harness!(eq_range_le_int4range, pg_c_range_le, |r| r <= 0);
    range_ineq_harness!(eq_range_ge_int4range, pg_c_range_ge, |r| r >= 0);
    range_ineq_harness!(eq_range_gt_int4range, pg_c_range_gt, |r| r > 0);

    // ---- range_empty / range_contains_elem ----

    #[kani::proof]
    #[kani::unwind(4)]
    fn eq_range_empty_int4range() {
        let a = sym_range();
        let c = unsafe { pg_c_range_empty(rptr(&a.buf)) };
        assert!(unsafe { pg_c_get_err() } == 0);
        let r = rt::range_is_empty(&a.buf.0[..a.total]);
        assert!(r as c_int == c);
        kani::cover!(r);
        kani::cover!(!r);
    }

    tci_harness! {
        #[kani::unwind(4)]
        fn eq_range_contains_elem_int4range() {
            let a = sym_range();
            let val: i32 = kani::any();
            let c = unsafe { pg_c_range_contains_elem(rptr(&a.buf), val) };
            assert!(unsafe { pg_c_get_err() } == 0);
            let mut ri = int4range_info();
            let r = ok(rt::ops::range_contains_elem_internal(
                dummy_mcx(),
                &mut ri,
                &a.buf.0[..a.total],
                Datum::from_i32(val),
            ));
            assert!(r as c_int == c);
            kani::cover!(r); // contained
            kani::cover!(!r && a.flags & rt::RANGE_EMPTY == 0); // rejected by a bound
            core::mem::forget(ri);
        }
    }

    // ---- range_adjacent: allocation + canonical function + error arm ----

    tci_harness! {
        // 32: Vec<u8,Mcx>::extend_with byte-fill of the serialized probe
        // range (24B image) dominates the bound.
        #[kani::unwind(32)]
        fn eq_range_adjacent_int4range() {
            let a = sym_range();
            let b = sym_range();
            let c = unsafe { pg_c_range_adjacent(rptr(&a.buf), rptr(&b.buf)) };
            let cerr = unsafe { pg_c_get_err() };
            let mut ri = int4range_info();
            let r = rt::ops::range_adjacent_internal(dummy_mcx(), &mut ri, &a.buf.0[..a.total], &b.buf.0[..b.total]);
            match r {
                Ok(v) => {
                    assert!(cerr == 0);
                    assert!(v as c_int == c);
                    kani::cover!(v); // adjacency witnessed (incl. canonical path)
                    kani::cover!(!v);
                }
                Err(e) => {
                    // verdict parity only: canonical INT32_MAX overflow class
                    assert!(cerr != 0);
                    core::mem::forget(e);
                }
            }
            kani::cover!(cerr != 0); // error arm reachable (INT32_MAX bounds)
            core::mem::forget(ri);
        }
    }

    // ---------------- controls (DEFAULT solver, must FAIL) ----------------

    /// Mismatched operators: shipped cmp<0 vs C array_le. MUST FAIL.
    tci_harness! {
        #[kani::unwind(6)]
        fn control_array_lt_vs_c_le() {
            let a = sym_arr(false);
            let b = sym_arr(false);
            let c = unsafe { pg_c_array_le(cptr(&a.buf), cptr(&b.buf), 0) };
            assert!(unsafe { pg_c_get_err() } == 0);
            let r = rust_array_cmp(&a, &b, int4_meta());
            assert!((r < 0) as c_int == c);
        }
    }

    /// SEAM-SKEW control: Rust is told typalign='d' while C's typcache entry
    /// says 'i', same images. A skewed seam model must NOT pass. MUST FAIL.
    /// (Rust gets the full buffer so the skewed walk stays in-bounds and the
    /// failure is a value divergence, not an OOB.)
    tci_harness! {
        #[kani::unwind(6)]
        fn control_array_seam_skew() {
            let elems1: [i32; 4] = kani::any();
            let elems2: [i32; 4] = kani::any();
            let (b1, _t1) = build_arr(false, 2, 1, &[false; 4], &elems1);
            let (b2, _t2) = build_arr(false, 2, 1, &[false; 4], &elems2);
            let c = unsafe { pg_c_btarraycmp(cptr(&b1), cptr(&b2), 0) };
            assert!(unsafe { pg_c_get_err() } == 0);
            let mut cmpfn = cmp_finfo();
            let skewed = ElemMeta { typlen: 4, typbyval: true, typalign: b'd' };
            let r = ok(array_cmp_core(dummy_mcx(), &b1.0[..], &b2.0[..], 0, skewed, &mut cmpfn));
            assert!(r == c);
        }
    }

    /// Mismatched range operators: shipped cmp<0 vs C range_le. MUST FAIL.
    tci_harness! {
        #[kani::unwind(4)]
        fn control_range_lt_vs_c_le() {
            let a = sym_range();
            let b = sym_range();
            let c = unsafe { pg_c_range_le(rptr(&a.buf), rptr(&b.buf)) };
            assert!(unsafe { pg_c_get_err() } == 0);
            let r = rust_range_cmp(&a, &b);
            assert!((r < 0) as c_int == c);
        }
    }

    // ================================================================
    // int8 / date / uuid / text instantiations (same seam pattern; the
    // C-side entries live in pg_typcache_inst.c and MUST stay identical
    // to the metas/finfos below).
    // ================================================================

    const INT8OID: u32 = 20;
    const TEXTOID: u32 = 25;
    const DATEOID: u32 = 1082;
    const UUIDOID: u32 = 2950;
    const INT8RANGEOID: u32 = 3926;
    const DATERANGEOID: u32 = 3912;
    const F_INT8EQ: u32 = 467;
    const F_BTINT8CMP: u32 = 842;
    const F_DATE_EQ: u32 = 1086;
    const F_DATE_CMP: u32 = 1092;
    const F_UUID_EQ: u32 = 2956;
    const F_UUID_CMP: u32 = 2960;
    const F_TEXTEQ: u32 = 67;
    const F_BTTEXTCMP: u32 = 360;
    const C_COLL: u32 = types_core::C_COLLATION_OID;

    fn int8_meta() -> ElemMeta {
        ElemMeta { typlen: 8, typbyval: true, typalign: b'd' }
    }
    fn date_meta() -> ElemMeta {
        ElemMeta { typlen: 4, typbyval: true, typalign: b'i' }
    }
    fn uuid_meta() -> ElemMeta {
        ElemMeta { typlen: 16, typbyval: false, typalign: b'c' }
    }
    fn text_meta() -> ElemMeta {
        ElemMeta { typlen: -1, typbyval: false, typalign: b'i' }
    }

    fn int8_cmp_finfo() -> FmgrInfo {
        FmgrInfo::new(nbt_compare::builtins::fc_btint8cmp, F_BTINT8CMP, 2, true, false)
    }
    fn int8_eq_finfo() -> FmgrInfo {
        FmgrInfo::new(adt_int8::builtins::fc_int8eq, F_INT8EQ, 2, true, false)
    }
    fn date_cmp_finfo() -> FmgrInfo {
        FmgrInfo::new(adt_date::builtins::fc_date_cmp, F_DATE_CMP, 2, true, false)
    }
    fn date_eq_finfo() -> FmgrInfo {
        FmgrInfo::new(adt_date::builtins::fc_date_eq, F_DATE_EQ, 2, true, false)
    }
    fn uuid_cmp_finfo() -> FmgrInfo {
        FmgrInfo::new(adt_uuid::builtins::fc_uuid_cmp, F_UUID_CMP, 2, true, false)
    }
    fn uuid_eq_finfo() -> FmgrInfo {
        FmgrInfo::new(adt_uuid::builtins::fc_uuid_eq, F_UUID_EQ, 2, true, false)
    }
    fn text_cmp_finfo() -> FmgrInfo {
        FmgrInfo::new(varlena::builtins::fc_bttextcmp, F_BTTEXTCMP, 2, true, false)
    }
    fn text_eq_finfo() -> FmgrInfo {
        FmgrInfo::new(varlena::builtins::fc_texteq, F_TEXTEQ, 2, true, false)
    }

    /// Rust mirror of the C int8range typcache entry (canonical INVALID
    /// both sides — adjacent/make_range out of scope, see module docs).
    fn int8range_info() -> rt::RangeInfo {
        rt::RangeInfo {
            pin: None,
            rngtypid: INT8RANGEOID,
            collation: 0,
            elem_typid: INT8OID,
            elem: rt::ElemInfo { typlen: 8, typbyval: true, typalign: b'd', typstorage: b'p' },
            cmp: int8_cmp_finfo(),
            canonical_oid: 0,
            elem_hash: None,
            elem_hash_extended: None,
            own_typlen: -1,
            own_typbyval: false,
            own_typalign: b'd',
        }
    }

    /// Rust mirror of the C daterange typcache entry (canonical INVALID
    /// both sides).
    fn daterange_info() -> rt::RangeInfo {
        rt::RangeInfo {
            pin: None,
            rngtypid: DATERANGEOID,
            collation: 0,
            elem_typid: DATEOID,
            elem: rt::ElemInfo { typlen: 4, typbyval: true, typalign: b'i', typstorage: b'p' },
            cmp: date_cmp_finfo(),
            canonical_oid: 0,
            elem_hash: None,
            elem_hash_extended: None,
            own_typlen: -1,
            own_typbyval: false,
            own_typalign: b'd',
        }
    }

    // ------------- fixed-size-element array images (int8/date/uuid) -------------

    /// Image buffer sized PER TYPE (32B header/bitmap + 4 elements): keeping
    /// the buffer minimal is load-bearing — a shared 104B buffer sent the
    /// date/int8 harnesses past the 6GiB RSS cap (run-1 measurement) where
    /// the 48B int4 buffer solves in 41-100s.
    #[repr(align(8))]
    struct WBuf<const N: usize>([u8; N]);

    fn wput_i32<const N: usize>(b: &mut WBuf<N>, off: usize, v: i32) {
        b.0[off..off + 4].copy_from_slice(&v.to_ne_bytes());
    }

    /// 1-D array of fixed-size elements (esz in {4,8,16}); layout mirrors the
    /// C walk exactly: data at 24 (32 with nulls, MAXALIGNed overhead), each
    /// element esz bytes, att_align_nominal a no-op for these (offsets stay
    /// multiples of the alignment).
    fn build_arr_w<const N: usize>(
        oid: u32,
        esz: usize,
        ndim0: bool,
        n: usize,
        lb: i32,
        nulls: &[bool; 4],
        elems: &[[u8; 16]; 4],
    ) -> (WBuf<N>, usize) {
        let mut b = WBuf([0; N]);
        if ndim0 {
            b.0[0..4].copy_from_slice(&(16u32 << 2).to_ne_bytes());
            wput_i32(&mut b, 4, 0);
            wput_i32(&mut b, 8, 0);
            wput_i32(&mut b, 12, oid as i32);
            return (b, 16);
        }
        let hasnull = (0..n).any(|i| nulls[i]);
        let data_off = if hasnull { 32 } else { 24 };
        let mut off = data_off;
        for i in 0..4 {
            if i < n && !nulls[i] {
                b.0[off..off + esz].copy_from_slice(&elems[i][..esz]);
                off += esz;
            }
        }
        let total = off;
        b.0[0..4].copy_from_slice(&((total as u32) << 2).to_ne_bytes());
        wput_i32(&mut b, 4, 1);
        wput_i32(&mut b, 8, if hasnull { 32 } else { 0 });
        wput_i32(&mut b, 12, oid as i32);
        wput_i32(&mut b, 16, n as i32);
        wput_i32(&mut b, 20, lb);
        if hasnull {
            let mut bm = 0u8;
            for i in 0..4 {
                if i < n && !nulls[i] {
                    bm |= 1 << i;
                }
            }
            b.0[24] = bm;
        }
        (b, total)
    }

    /// Only the first `maxn` elements are symbolic; the rest are literal
    /// zero (dead symbolic bytes measurably inflate CNF flattening).
    fn sym_elems(maxn: usize) -> [[u8; 16]; 4] {
        let mut e = [[0u8; 16]; 4];
        let mut i = 0;
        while i < maxn {
            e[i] = kani::any();
            i += 1;
        }
        e
    }

    struct SymArrW<const N: usize> {
        buf: WBuf<N>,
        total: usize,
        ndim0: bool,
        n: usize,
        lb: i32,
        nulls: [bool; 4],
    }

    fn sym_arr_w<const N: usize>(
        oid: u32,
        esz: usize,
        allow_ndim0: bool,
        maxn: usize,
        withnulls: bool,
    ) -> SymArrW<N> {
        let ndim0: bool = if allow_ndim0 { kani::any() } else { false };
        let n: usize = kani::any();
        kani::assume(n <= maxn);
        let lb: i32 = kani::any();
        let nulls: [bool; 4] = if withnulls { kani::any() } else { [false; 4] };
        let elems: [[u8; 16]; 4] = sym_elems(maxn);
        let (buf, total) = build_arr_w(oid, esz, ndim0, n, lb, &nulls, &elems);
        SymArrW { buf, total, ndim0, n, lb, nulls }
    }

    fn wptr<const N: usize>(b: &WBuf<N>) -> *const core::ffi::c_void {
        b.0.as_ptr().cast()
    }

    // ------------- text array images (per-descriptor fixed small lengths) -------------

    /// Text image: 24B header + 4 short-1B-header elements (len<=4, so elem
    /// size 1..=5, INTALIGN-stepped exactly like the C walk). No null bitmap
    /// (module-doc fence). Max 24 + 4*8 = 56.
    #[repr(align(8))]
    struct TBuf([u8; 64]);

    fn build_text_arr(n: usize, lb: i32, lens: &[usize; 4], data: &[[u8; 4]; 4]) -> (TBuf, usize) {
        let mut b = TBuf([0; 64]);
        let mut off = 24usize;
        for i in 0..4 {
            if i < n {
                // short varlena: 1B header, VARSIZE_SHORT = len+1
                b.0[off] = (((lens[i] + 1) as u8) << 1) | 1;
                b.0[off + 1..off + 1 + lens[i]].copy_from_slice(&data[i][..lens[i]]);
                off += 1 + lens[i];
                off = (off + 3) & !3; // att_align_nominal 'i'
            }
        }
        let total = off;
        b.0[0..4].copy_from_slice(&((total as u32) << 2).to_ne_bytes());
        b.0[4..8].copy_from_slice(&1i32.to_ne_bytes());
        b.0[8..12].copy_from_slice(&0i32.to_ne_bytes());
        b.0[12..16].copy_from_slice(&(TEXTOID as i32).to_ne_bytes());
        b.0[16..20].copy_from_slice(&(n as i32).to_ne_bytes());
        b.0[20..24].copy_from_slice(&lb.to_ne_bytes());
        (b, total)
    }

    fn tptr(b: &TBuf) -> *const core::ffi::c_void {
        b.0.as_ptr().cast()
    }

    // Length descriptors (fixed per harness; contents fully symbolic).
    // Pairwise: idx0 equal lens (content compare), idx1 0-vs-3 (empty vs
    // nonempty: len tiebreak), idx2 4-vs-1 (prefix + len tiebreak),
    // idx3 equal lens.
    const TLENS_A: [usize; 4] = [2, 0, 4, 3];
    const TLENS_B: [usize; 4] = [2, 3, 1, 3];

    // ------------- typed array harness macros -------------

    macro_rules! typed_array_eq_harness {
        ($name:ident, $cfn:ident, $negate:expr, $oid:expr, $esz:expr, $bufn:literal, $maxn:literal, $withnulls:literal, $covers:literal,
         $meta:expr, $eqf:expr, $coll:expr, $unwind:literal) => {
            tci_harness! {
                #[kani::unwind($unwind)]
                fn $name() {
                    let n: usize = kani::any();
                    kani::assume(n <= $maxn);
                    let lb: i32 = kani::any();
                    // symbolic nulls make byref element offsets data-dependent;
                    // $withnulls=false keeps every read offset CONCRETE
                    // (json-escape symbolic-offset lesson; uuid instantiation).
                    let nulls1: [bool; 4] = if $withnulls { kani::any() } else { [false; 4] };
                    let nulls2: [bool; 4] = if $withnulls { kani::any() } else { [false; 4] };
                    let (elems1, elems2) = (sym_elems($maxn), sym_elems($maxn));
                    let (b1, t1) = build_arr_w::<$bufn>($oid, $esz, false, n, lb, &nulls1, &elems1);
                    let (b2, t2) = build_arr_w::<$bufn>($oid, $esz, false, n, lb, &nulls2, &elems2);

                    let c = unsafe { $cfn(wptr(&b1), wptr(&b2), $coll) };
                    assert!(unsafe { pg_c_get_err() } == 0);

                    let mut eqfn = $eqf;
                    let r = ok(array_eq_loop(dummy_mcx(), &b1.0[..t1], &b2.0[..t2], $coll, $meta, &mut eqfn));
                    let r = if $negate { !r } else { r };
                    assert!(r as c_int == c);

                    // covers optionally hoisted to a dedicated cover harness
                    // (records-family trick: each cover = one extra property
                    // through propositional reduction; uuid RSS-walls with
                    // them in the equality harness)
                    if $covers {
                        if $withnulls {
                            kani::cover!(n > 0 && nulls1[0] && nulls2[0]); // null==null skip
                            kani::cover!(n > 0 && nulls1[0] && !nulls2[0]); // null mismatch
                        }
                        kani::cover!(n > 0 && c == (!$negate) as c_int); // op-true with elems
                        kani::cover!(n > 0 && c == ($negate) as c_int); // op-false with elems
                    }
                }
            }
        };
    }

    macro_rules! typed_array_cmp_harness {
        ($name:ident, $cfn:ident, $map:expr, $oid:expr, $esz:expr, $bufn:literal, $maxn:literal, $ndim0:literal, $withnulls:literal, $covers:literal,
         $meta:expr, $cmpf:expr, $coll:expr, $unwind:literal) => {
            tci_harness! {
                #[kani::unwind($unwind)]
                fn $name() {
                    let a = sym_arr_w::<$bufn>($oid, $esz, $ndim0, $maxn, $withnulls);
                    let b = sym_arr_w::<$bufn>($oid, $esz, $ndim0, $maxn, $withnulls);
                    let c = unsafe { $cfn(wptr(&a.buf), wptr(&b.buf), $coll) };
                    assert!(unsafe { pg_c_get_err() } == 0);
                    let mut cmpfn = $cmpf;
                    let r = ok(array_cmp_core(
                        dummy_mcx(),
                        &a.buf.0[..a.total],
                        &b.buf.0[..b.total],
                        $coll,
                        $meta,
                        &mut cmpfn,
                    ));
                    let map: fn(i32) -> i32 = $map;
                    assert!(map(r) == c as i32);

                    if $covers {
                        kani::cover!(r == 0);
                        kani::cover!(r < 0 && a.n == b.n && !a.ndim0 && !b.ndim0); // element decide
                        if $withnulls {
                            kani::cover!(a.n > 0 && a.nulls[0] && !b.nulls[0] && !a.ndim0 && !b.ndim0); // null > non-null
                        }
                        kani::cover!(r != 0 && a.n != b.n); // nitems tiebreak
                        kani::cover!(r != 0 && a.n == b.n && a.lb != b.lb && !a.ndim0 && !b.ndim0); // lbound tiebreak
                    }
                }
            }
        };
    }

    // ---- int8[] ----

    typed_array_eq_harness!(eq_array_eq_int8, pg_c_array_eq, false, INT8OID, 8, 64, 4, true, true, int8_meta(), int8_eq_finfo(), 0, 6);
    typed_array_eq_harness!(eq_array_ne_int8, pg_c_array_ne, true, INT8OID, 8, 64, 4, true, true, int8_meta(), int8_eq_finfo(), 0, 6);
    typed_array_cmp_harness!(eq_array_cmp_int8, pg_c_btarraycmp, |r| r, INT8OID, 8, 64, 4, true, true, true, int8_meta(), int8_cmp_finfo(), 0, 6);
    typed_array_cmp_harness!(eq_array_lt_int8, pg_c_array_lt, |r| (r < 0) as i32, INT8OID, 8, 64, 4, true, true, true, int8_meta(), int8_cmp_finfo(), 0, 6);
    typed_array_cmp_harness!(eq_array_gt_int8, pg_c_array_gt, |r| (r > 0) as i32, INT8OID, 8, 64, 4, true, true, true, int8_meta(), int8_cmp_finfo(), 0, 6);
    typed_array_cmp_harness!(eq_array_le_int8, pg_c_array_le, |r| (r <= 0) as i32, INT8OID, 8, 64, 4, true, true, true, int8_meta(), int8_cmp_finfo(), 0, 6);
    typed_array_cmp_harness!(eq_array_ge_int8, pg_c_array_ge, |r| (r >= 0) as i32, INT8OID, 8, 64, 4, true, true, true, int8_meta(), int8_cmp_finfo(), 0, 6);

    // ---- date[] ----

    typed_array_eq_harness!(eq_array_eq_date, pg_c_array_eq, false, DATEOID, 4, 48, 4, true, true, date_meta(), date_eq_finfo(), 0, 6);
    typed_array_eq_harness!(eq_array_ne_date, pg_c_array_ne, true, DATEOID, 4, 48, 4, true, true, date_meta(), date_eq_finfo(), 0, 6);
    typed_array_cmp_harness!(eq_array_cmp_date, pg_c_btarraycmp, |r| r, DATEOID, 4, 48, 4, true, true, true, date_meta(), date_cmp_finfo(), 0, 6);
    typed_array_cmp_harness!(eq_array_lt_date, pg_c_array_lt, |r| (r < 0) as i32, DATEOID, 4, 48, 4, true, true, true, date_meta(), date_cmp_finfo(), 0, 6);
    typed_array_cmp_harness!(eq_array_gt_date, pg_c_array_gt, |r| (r > 0) as i32, DATEOID, 4, 48, 4, true, true, true, date_meta(), date_cmp_finfo(), 0, 6);
    typed_array_cmp_harness!(eq_array_le_date, pg_c_array_le, |r| (r <= 0) as i32, DATEOID, 4, 48, 4, true, true, true, date_meta(), date_cmp_finfo(), 0, 6);
    typed_array_cmp_harness!(eq_array_ge_date, pg_c_array_ge, |r| (r >= 0) as i32, DATEOID, 4, 48, 4, true, true, true, date_meta(), date_cmp_finfo(), 0, 6);

    // ---- uuid[] (byref; unwind covers the 16-byte memcmp expansion) ----

    typed_array_eq_harness!(eq_array_eq_uuid, pg_c_array_eq, false, UUIDOID, 16, 96, 4, true, true, uuid_meta(), uuid_eq_finfo(), 0, 18);
    typed_array_eq_harness!(eq_array_ne_uuid, pg_c_array_ne, true, UUIDOID, 16, 96, 4, true, true, uuid_meta(), uuid_eq_finfo(), 0, 18);
    typed_array_cmp_harness!(eq_array_cmp_uuid, pg_c_btarraycmp, |r| r, UUIDOID, 16, 96, 4, true, true, true, uuid_meta(), uuid_cmp_finfo(), 0, 18);
    typed_array_cmp_harness!(eq_array_lt_uuid, pg_c_array_lt, |r| (r < 0) as i32, UUIDOID, 16, 96, 4, true, true, true, uuid_meta(), uuid_cmp_finfo(), 0, 18);
    typed_array_cmp_harness!(eq_array_gt_uuid, pg_c_array_gt, |r| (r > 0) as i32, UUIDOID, 16, 96, 4, true, true, true, uuid_meta(), uuid_cmp_finfo(), 0, 18);
    typed_array_cmp_harness!(eq_array_le_uuid, pg_c_array_le, |r| (r <= 0) as i32, UUIDOID, 16, 96, 4, true, true, true, uuid_meta(), uuid_cmp_finfo(), 0, 18);
    typed_array_cmp_harness!(eq_array_ge_uuid, pg_c_array_ge, |r| (r >= 0) as i32, UUIDOID, 16, 96, 4, true, true, true, uuid_meta(), uuid_cmp_finfo(), 0, 18);

    // ---- text[] (fixed per-index lengths, symbolic contents; C collation) ----

    macro_rules! text_array_eq_harness {
        ($name:ident, $cfn:ident, $negate:expr) => {
            tci_harness! {
                #[kani::unwind(6)]
                fn $name() {
                    let n: usize = kani::any();
                    kani::assume(n <= 4);
                    let lb: i32 = kani::any();
                    let data1: [[u8; 4]; 4] = kani::any();
                    let data2: [[u8; 4]; 4] = kani::any();
                    let (b1, t1) = build_text_arr(n, lb, &TLENS_A, &data1);
                    let (b2, t2) = build_text_arr(n, lb, &TLENS_B, &data2);

                    let c = unsafe { $cfn(tptr(&b1), tptr(&b2), C_COLL) };
                    assert!(unsafe { pg_c_get_err() } == 0);

                    let mut eqfn = text_eq_finfo();
                    let r = ok(array_eq_loop(dummy_mcx(), &b1.0[..t1], &b2.0[..t2], C_COLL, text_meta(), &mut eqfn));
                    let r = if $negate { !r } else { r };
                    assert!(r as c_int == c);

                    kani::cover!(n > 0 && c == (!$negate) as c_int); // equal contents (idx0 lens match)
                    kani::cover!(n >= 2 && c == ($negate) as c_int); // len shortcut (idx1: 0 vs 3)
                    kani::cover!(n == 1 && c == ($negate) as c_int); // content mismatch, equal lens
                }
            }
        };
    }

    text_array_eq_harness!(eq_array_eq_text, pg_c_array_eq, false);
    text_array_eq_harness!(eq_array_ne_text, pg_c_array_ne, true);

    macro_rules! text_array_cmp_harness {
        ($name:ident, $cfn:ident, $map:expr) => {
            tci_harness! {
                #[kani::unwind(6)]
                fn $name() {
                    let na: usize = kani::any();
                    let nb: usize = kani::any();
                    kani::assume(na <= 4 && nb <= 4);
                    let lba: i32 = kani::any();
                    let lbb: i32 = kani::any();
                    let data1: [[u8; 4]; 4] = kani::any();
                    let data2: [[u8; 4]; 4] = kani::any();
                    let (b1, t1) = build_text_arr(na, lba, &TLENS_A, &data1);
                    let (b2, t2) = build_text_arr(nb, lbb, &TLENS_B, &data2);

                    let c = unsafe { $cfn(tptr(&b1), tptr(&b2), C_COLL) };
                    assert!(unsafe { pg_c_get_err() } == 0);

                    let mut cmpfn = text_cmp_finfo();
                    let r = ok(array_cmp_core(dummy_mcx(), &b1.0[..t1], &b2.0[..t2], C_COLL, text_meta(), &mut cmpfn));
                    let map: fn(i32) -> i32 = $map;
                    assert!(map(r) == c as i32);

                    kani::cover!(r == 0);
                    kani::cover!(r < 0 && na >= 2 && na == nb); // len-tiebreak element decide (idx1)
                    kani::cover!(r != 0 && na == nb && na >= 1 && lba != lbb); // lbound tiebreak
                    kani::cover!(r != 0 && na != nb); // nitems tiebreak
                }
            }
        };
    }

    text_array_cmp_harness!(eq_array_cmp_text, pg_c_btarraycmp, |r| r);
    text_array_cmp_harness!(eq_array_lt_text, pg_c_array_lt, |r| (r < 0) as i32);
    text_array_cmp_harness!(eq_array_gt_text, pg_c_array_gt, |r| (r > 0) as i32);
    text_array_cmp_harness!(eq_array_le_text, pg_c_array_le, |r| (r <= 0) as i32);
    text_array_cmp_harness!(eq_array_ge_text, pg_c_array_ge, |r| (r >= 0) as i32);

    // ------------- int8range / daterange images -------------

    #[repr(align(8))]
    struct RBufW([u8; 32]);

    /// Range image with esz-byte bounds (esz in {4,8}); bound offsets stay
    /// esz-aligned, so the C att_align_pointer in range_deserialize is a
    /// no-op — identical rule both deserializers use.
    fn build_range_w(oid: u32, esz: usize, flags: u8, lo: &[u8; 8], hi: &[u8; 8]) -> (RBufW, usize) {
        let mut b = RBufW([0; 32]);
        b.0[4..8].copy_from_slice(&oid.to_ne_bytes());
        let mut off = 8usize;
        if rt::range_has_lbound(flags) {
            b.0[off..off + esz].copy_from_slice(&lo[..esz]);
            off += esz;
        }
        if rt::range_has_ubound(flags) {
            b.0[off..off + esz].copy_from_slice(&hi[..esz]);
            off += esz;
        }
        b.0[off] = flags;
        off += 1;
        b.0[0..4].copy_from_slice(&((off as u32) << 2).to_ne_bytes());
        (b, off)
    }

    struct SymRangeW {
        buf: RBufW,
        total: usize,
        flags: u8,
    }

    fn sym_range_w(oid: u32, esz: usize) -> SymRangeW {
        let flags: u8 = kani::any();
        let lo: [u8; 8] = kani::any();
        let hi: [u8; 8] = kani::any();
        let (buf, total) = build_range_w(oid, esz, flags, &lo, &hi);
        SymRangeW { buf, total, flags }
    }

    fn rwptr(b: &RBufW) -> *const core::ffi::c_void {
        b.0.as_ptr().cast()
    }

    // ------------- typed range harnesses -------------

    macro_rules! typed_range_pair_harness {
        ($($name:ident: $cfn:ident / $rop:ident [$oid:expr, $esz:expr, $info:expr];)*) => {$(
            tci_harness! {
                #[kani::unwind(4)]
                fn $name() {
                    let a = sym_range_w($oid, $esz);
                    let b = sym_range_w($oid, $esz);
                    let c = unsafe { $cfn(rwptr(&a.buf), rwptr(&b.buf)) };
                    assert!(unsafe { pg_c_get_err() } == 0);
                    let mut ri = $info;
                    let r = ok(rt::ops::$rop(dummy_mcx(), &mut ri, &a.buf.0[..a.total], &b.buf.0[..b.total]));
                    assert!(r as c_int == c);
                    kani::cover!(a.flags & rt::RANGE_EMPTY != 0 && b.flags & rt::RANGE_EMPTY != 0);
                    kani::cover!(a.flags & rt::RANGE_EMPTY != 0 && b.flags & rt::RANGE_EMPTY == 0);
                    kani::cover!(a.flags & (rt::RANGE_LB_INF | rt::RANGE_UB_INF) != 0 && c != 0);
                    kani::cover!(c != 0);
                    kani::cover!(c == 0);
                    core::mem::forget(ri);
                }
            }
        )*};
    }

    typed_range_pair_harness! {
        eq_range_eq_int8range: pg_c_range_eq / range_eq_internal [INT8RANGEOID, 8, int8range_info()];
        eq_range_ne_int8range: pg_c_range_ne / range_ne_internal [INT8RANGEOID, 8, int8range_info()];
        eq_range_contains_int8range: pg_c_range_contains / range_contains_internal [INT8RANGEOID, 8, int8range_info()];
        eq_range_contained_by_int8range: pg_c_range_contained_by / range_contained_by_internal [INT8RANGEOID, 8, int8range_info()];
        eq_range_before_int8range: pg_c_range_before / range_before_internal [INT8RANGEOID, 8, int8range_info()];
        eq_range_after_int8range: pg_c_range_after / range_after_internal [INT8RANGEOID, 8, int8range_info()];
        eq_range_overlaps_int8range: pg_c_range_overlaps / range_overlaps_internal [INT8RANGEOID, 8, int8range_info()];
        eq_range_eq_daterange: pg_c_range_eq / range_eq_internal [DATERANGEOID, 4, daterange_info()];
        eq_range_ne_daterange: pg_c_range_ne / range_ne_internal [DATERANGEOID, 4, daterange_info()];
        eq_range_contains_daterange: pg_c_range_contains / range_contains_internal [DATERANGEOID, 4, daterange_info()];
        eq_range_contained_by_daterange: pg_c_range_contained_by / range_contained_by_internal [DATERANGEOID, 4, daterange_info()];
        eq_range_before_daterange: pg_c_range_before / range_before_internal [DATERANGEOID, 4, daterange_info()];
        eq_range_after_daterange: pg_c_range_after / range_after_internal [DATERANGEOID, 4, daterange_info()];
        eq_range_overlaps_daterange: pg_c_range_overlaps / range_overlaps_internal [DATERANGEOID, 4, daterange_info()];
    }

    fn rust_range_cmp_w(info: rt::RangeInfo, a: &SymRangeW, b: &SymRangeW) -> i32 {
        let mut ri = info;
        let r = ok(rt::ops::range_cmp_internal(dummy_mcx(), &mut ri, &a.buf.0[..a.total], &b.buf.0[..b.total]));
        core::mem::forget(ri);
        r
    }

    macro_rules! typed_range_cmp_harness {
        ($($name:ident: $cfn:ident / $map:expr => [$oid:expr, $esz:expr, $info:expr];)*) => {$(
            tci_harness! {
                #[kani::unwind(4)]
                fn $name() {
                    let a = sym_range_w($oid, $esz);
                    let b = sym_range_w($oid, $esz);
                    let c = unsafe { $cfn(rwptr(&a.buf), rwptr(&b.buf)) };
                    assert!(unsafe { pg_c_get_err() } == 0);
                    let r = rust_range_cmp_w($info, &a, &b);
                    let map: fn(i32) -> i32 = $map;
                    assert!(map(r) == c as i32);
                    kani::cover!(r == 0 && a.flags & rt::RANGE_EMPTY == 0); // equal non-empty
                    kani::cover!(r == -1 && a.flags & rt::RANGE_EMPTY != 0); // empty sorts first
                }
            }
        )*};
    }

    typed_range_cmp_harness! {
        eq_range_cmp_int8range: pg_c_range_cmp / |r| r => [INT8RANGEOID, 8, int8range_info()];
        eq_range_lt_int8range: pg_c_range_lt / (|r| (r < 0) as i32) => [INT8RANGEOID, 8, int8range_info()];
        eq_range_le_int8range: pg_c_range_le / (|r| (r <= 0) as i32) => [INT8RANGEOID, 8, int8range_info()];
        eq_range_ge_int8range: pg_c_range_ge / (|r| (r >= 0) as i32) => [INT8RANGEOID, 8, int8range_info()];
        eq_range_gt_int8range: pg_c_range_gt / (|r| (r > 0) as i32) => [INT8RANGEOID, 8, int8range_info()];
        eq_range_cmp_daterange: pg_c_range_cmp / |r| r => [DATERANGEOID, 4, daterange_info()];
        eq_range_lt_daterange: pg_c_range_lt / (|r| (r < 0) as i32) => [DATERANGEOID, 4, daterange_info()];
        eq_range_le_daterange: pg_c_range_le / (|r| (r <= 0) as i32) => [DATERANGEOID, 4, daterange_info()];
        eq_range_ge_daterange: pg_c_range_ge / (|r| (r >= 0) as i32) => [DATERANGEOID, 4, daterange_info()];
        eq_range_gt_daterange: pg_c_range_gt / (|r| (r > 0) as i32) => [DATERANGEOID, 4, daterange_info()];
    }

    #[kani::proof]
    #[kani::unwind(4)]
    fn eq_range_empty_int8range() {
        let a = sym_range_w(INT8RANGEOID, 8);
        let c = unsafe { pg_c_range_empty(rwptr(&a.buf)) };
        assert!(unsafe { pg_c_get_err() } == 0);
        let r = rt::range_is_empty(&a.buf.0[..a.total]);
        assert!(r as c_int == c);
        kani::cover!(r);
        kani::cover!(!r);
    }

    #[kani::proof]
    #[kani::unwind(4)]
    fn eq_range_empty_daterange() {
        let a = sym_range_w(DATERANGEOID, 4);
        let c = unsafe { pg_c_range_empty(rwptr(&a.buf)) };
        assert!(unsafe { pg_c_get_err() } == 0);
        let r = rt::range_is_empty(&a.buf.0[..a.total]);
        assert!(r as c_int == c);
        kani::cover!(r);
        kani::cover!(!r);
    }

    tci_harness! {
        #[kani::unwind(4)]
        fn eq_range_contains_elem_int8range() {
            let a = sym_range_w(INT8RANGEOID, 8);
            let val: i64 = kani::any();
            let c = unsafe { pg_c_range_contains_elem64(rwptr(&a.buf), val) };
            assert!(unsafe { pg_c_get_err() } == 0);
            let mut ri = int8range_info();
            let r = ok(rt::ops::range_contains_elem_internal(
                dummy_mcx(),
                &mut ri,
                &a.buf.0[..a.total],
                Datum::from_i64(val),
            ));
            assert!(r as c_int == c);
            kani::cover!(r);
            kani::cover!(!r && a.flags & rt::RANGE_EMPTY == 0);
            core::mem::forget(ri);
        }
    }

    tci_harness! {
        #[kani::unwind(4)]
        fn eq_range_contains_elem_daterange() {
            let a = sym_range_w(DATERANGEOID, 4);
            let val: i32 = kani::any();
            let c = unsafe { pg_c_range_contains_elem(rwptr(&a.buf), val) };
            assert!(unsafe { pg_c_get_err() } == 0);
            let mut ri = daterange_info();
            let r = ok(rt::ops::range_contains_elem_internal(
                dummy_mcx(),
                &mut ri,
                &a.buf.0[..a.total],
                Datum::from_i32(val),
            ));
            assert!(r as c_int == c);
            kani::cover!(r);
            kani::cover!(!r && a.flags & rt::RANGE_EMPTY == 0);
            core::mem::forget(ri);
        }
    }

    // ---- elem_contained_by_range: C fmgr body (arg swap) vs the shared
    // internal — flips the elem_contained_by_range ledger row per-type.
    // (The shipped Rust fc_ wrapper is the same arg swap; its arg fetch
    // stays in the tested tier, family convention.)

    tci_harness! {
        #[kani::unwind(4)]
        fn eq_elem_contained_by_int4range() {
            let a = sym_range();
            let val: i32 = kani::any();
            let c = unsafe { pg_c_elem_contained_by(Datum::from_i32(val).as_u64(), rptr(&a.buf)) };
            assert!(unsafe { pg_c_get_err() } == 0);
            let mut ri = int4range_info();
            let r = ok(rt::ops::range_contains_elem_internal(
                dummy_mcx(),
                &mut ri,
                &a.buf.0[..a.total],
                Datum::from_i32(val),
            ));
            assert!(r as c_int == c);
            kani::cover!(r);
            kani::cover!(!r && a.flags & rt::RANGE_EMPTY == 0);
            core::mem::forget(ri);
        }
    }

    tci_harness! {
        #[kani::unwind(4)]
        fn eq_elem_contained_by_int8range() {
            let a = sym_range_w(INT8RANGEOID, 8);
            let val: i64 = kani::any();
            let c = unsafe { pg_c_elem_contained_by(Datum::from_i64(val).as_u64(), rwptr(&a.buf)) };
            assert!(unsafe { pg_c_get_err() } == 0);
            let mut ri = int8range_info();
            let r = ok(rt::ops::range_contains_elem_internal(
                dummy_mcx(),
                &mut ri,
                &a.buf.0[..a.total],
                Datum::from_i64(val),
            ));
            assert!(r as c_int == c);
            kani::cover!(r);
            kani::cover!(!r && a.flags & rt::RANGE_EMPTY == 0);
            core::mem::forget(ri);
        }
    }

    tci_harness! {
        #[kani::unwind(4)]
        fn eq_elem_contained_by_daterange() {
            let a = sym_range_w(DATERANGEOID, 4);
            let val: i32 = kani::any();
            let c = unsafe { pg_c_elem_contained_by(Datum::from_i32(val).as_u64(), rwptr(&a.buf)) };
            assert!(unsafe { pg_c_get_err() } == 0);
            let mut ri = daterange_info();
            let r = ok(rt::ops::range_contains_elem_internal(
                dummy_mcx(),
                &mut ri,
                &a.buf.0[..a.total],
                Datum::from_i32(val),
            ));
            assert!(r as c_int == c);
            kani::cover!(r);
            kani::cover!(!r && a.flags & rt::RANGE_EMPTY == 0);
            core::mem::forget(ri);
        }
    }

    // ---------------- new-type controls (DEFAULT solver, must FAIL) ----------------

    /// SEAM-SKEW: Rust told typlen=4 for an int8[] image C reads with
    /// typlen=8. A skewed element-size model must NOT pass. MUST FAIL.
    tci_harness! {
        #[kani::unwind(6)]
        fn control_array_seam_skew_int8() {
            let elems1: [[u8; 16]; 4] = kani::any();
            let elems2: [[u8; 16]; 4] = kani::any();
            let (b1, _t1) = build_arr_w::<64>(INT8OID, 8, false, 2, 1, &[false; 4], &elems1);
            let (b2, _t2) = build_arr_w::<64>(INT8OID, 8, false, 2, 1, &[false; 4], &elems2);
            let c = unsafe { pg_c_btarraycmp(wptr(&b1), wptr(&b2), 0) };
            assert!(unsafe { pg_c_get_err() } == 0);
            let mut cmpfn = int8_cmp_finfo();
            let skewed = ElemMeta { typlen: 4, typbyval: true, typalign: b'i' };
            let r = ok(array_cmp_core(dummy_mcx(), &b1.0[..], &b2.0[..], 0, skewed, &mut cmpfn));
            assert!(r == c);
        }
    }

    /// SEAM-SKEW: Rust told typlen=8 for a uuid[] image C reads with
    /// typlen=16 (element boundaries shift). MUST FAIL.
    tci_harness! {
        #[kani::unwind(18)]
        fn control_array_seam_skew_uuid() {
            let elems1: [[u8; 16]; 4] = kani::any();
            let elems2: [[u8; 16]; 4] = kani::any();
            let (b1, _t1) = build_arr_w::<64>(UUIDOID, 16, false, 2, 1, &[false; 4], &elems1);
            let (b2, _t2) = build_arr_w::<64>(UUIDOID, 16, false, 2, 1, &[false; 4], &elems2);
            let c = unsafe { pg_c_btarraycmp(wptr(&b1), wptr(&b2), 0) };
            assert!(unsafe { pg_c_get_err() } == 0);
            let mut cmpfn = uuid_cmp_finfo();
            let skewed = ElemMeta { typlen: 8, typbyval: false, typalign: b'c' };
            let r = ok(array_cmp_core(dummy_mcx(), &b1.0[..], &b2.0[..], 0, skewed, &mut cmpfn));
            assert!(r == c);
        }
    }

    /// SEAM-SKEW: Rust told typalign='d' for a text[] image C walks with
    /// 'i'. Elem lens [2,2]: C reads elem1 at 28, the skewed walk at 32 —
    /// a PLANTED second varlena at 32 keeps the skewed read valid, so the
    /// failure is a clean value divergence. MUST FAIL.
    tci_harness! {
        #[kani::unwind(6)]
        fn control_array_seam_skew_text() {
            let data: [[u8; 4]; 4] = kani::any();
            let (mut b1, t1) = build_text_arr(2, 1, &[2, 2, 0, 0], &data);
            // planted decoy at the DOUBLEALIGN'd offset the skewed walk hits
            b1.0[32] = ((3u8) << 1) | 1; // short varlena, len 2
            b1.0[33] = !data[1][0]; // differs from the real elem1 byte
            b1.0[34] = data[1][1];
            let data2: [[u8; 4]; 4] = kani::any();
            let (b2, _t2) = build_text_arr(2, 1, &[2, 2, 0, 0], &data2);
            let c = unsafe { pg_c_btarraycmp(tptr(&b1), tptr(&b2), C_COLL) };
            assert!(unsafe { pg_c_get_err() } == 0);
            let _ = t1;
            let mut cmpfn = text_cmp_finfo();
            let skewed = ElemMeta { typlen: -1, typbyval: false, typalign: b'd' };
            let r = ok(array_cmp_core(dummy_mcx(), &b1.0[..], &b2.0[..], C_COLL, skewed, &mut cmpfn));
            assert!(r == c);
        }
    }

    /// SEAM-SKEW: Rust RangeInfo told elem typlen=4 for an int8range image
    /// (bound offsets shift). MUST FAIL.
    tci_harness! {
        #[kani::unwind(4)]
        fn control_range_seam_skew_int8() {
            let a = sym_range_w(INT8RANGEOID, 8);
            let b = sym_range_w(INT8RANGEOID, 8);
            let c = unsafe { pg_c_range_cmp(rwptr(&a.buf), rwptr(&b.buf)) };
            assert!(unsafe { pg_c_get_err() } == 0);
            let mut ri = int8range_info();
            ri.elem = rt::ElemInfo { typlen: 4, typbyval: true, typalign: b'i', typstorage: b'p' };
            // int8 comparator still applied to the misread bound datums
            let r = ok(rt::ops::range_cmp_internal(dummy_mcx(), &mut ri, &a.buf.0[..a.total], &b.buf.0[..b.total]));
            core::mem::forget(ri);
            assert!(r == c);
        }
    }
}
