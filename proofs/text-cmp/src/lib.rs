//! Kani C≡Rust equivalence: the text / bpchar / name-cross comparator
//! families (~40 pg_proc rows):
//!   texteq/textne/text_lt/le/gt/ge, bttextcmp, text_larger/smaller;
//!   text_pattern_lt/le/ge/gt + bttext_pattern_cmp;
//!   bpchareq/ne/lt/le/gt/ge, bpcharcmp (via lt..cmp), bpchar_larger/smaller,
//!   bpchar_pattern_lt/le/ge/gt + btbpchar_pattern_cmp;
//!   nameeqtext/namenetext/namelttext/nameletext/namegttext/namegetext,
//!   btnametextcmp; texteqname/textnename/textltname/textlename/textgtname/
//!   textgename, bttextnamecmp.
//!
//! Rust side (shipped code, path-dep — never copied): the SHIPPED FMGR
//! WRAPPERS `varlena::builtins::fc_*`, `adt_varchar::builtins::fc_*`,
//! `name::builtins::fc_*`, invoked through a real `LocalFcinfo<2>` frame
//! (datetime-cmp scope-expansion pattern). The varlena arguments are real
//! 1-byte-header (short) inline varlena images built with the shipped
//! `types_tuple::varatt::set_varsize_short`, so the whole shipped path is
//! inside the theorem: datum -> arg_varlena_packed (header decode) ->
//! PackedVarlena::data -> value core -> Datum::from_bool/from_i32 (and for
//! larger/smaller the returned winner-image-pointer datum).
//!
//! C side: proofs/text-cmp/c/pg_text_cmp.c — verbatim REL_18_STABLE
//! varlena.c + varchar.c bodies (provenance + shims documented there).
//!
//! Fences and claims (mirrored in the ledger):
//!  - COLLATION FENCE: collid = C_COLLATION_OID only. Non-C collations
//!    route to pg_strncoll/locale (C) and pg_locale_seams (Rust) — the
//!    same seam boundary as proofs/name-ascii; the vendored C poisons that
//!    arm. texteq/textne/bpchareq/bpcharne on REL_18 route on
//!    mylocale->deterministic (length+memcmp fast path); under the fence
//!    deterministic == true on both sides, harness-checked against the
//!    poisoned arm. The pattern families are collation-free by contract
//!    (pure memcmp semantics) and carry no fence.
//!  - DETOASTING OUT OF SCOPE: inputs model the post-PG_GETARG_*_PP caller
//!    contract (pre-detoasted payloads; bytea-cmp varlena pattern). The
//!    harness images are 1B-header inline varlenas, so the shipped
//!    arg_varlena_packed detoast arm is provably not taken.
//!  - Bounds: symbolic lengths 0..=8 each side over fully symbolic bytes
//!    (81 length combos per harness, contents exhaustive). Name-cross:
//!    FULL 64-byte symbolic NameData fenced to the C datatype invariant
//!    data[63] == 0 (C strlen(NameStr) requires an in-buffer terminator)
//!    x symbolic text len<=8.
//!  - cmp rows assert exact SQL-visible int32 VALUE equality: C returns the
//!    raw memcmp/strncmp byte difference; CBMC's memcmp model returns the
//!    difference of the first mismatching unsigned chars — the glibc
//!    convention the shipped cores document (varlena/src/lib.rs:122,
//!    ratified: bytea-cmp/network/name families). Boolean rows assert
//!    verdict equality.
//!  - larger/smaller rows assert the returned datum is the winning
//!    DETOASTED input image pointer (C returns the PG_GETARG_*_PP pointer;
//!    the C shim returns a winner index).
//!  - kani::cover! witnesses prove the fast-path regimes are reachable:
//!    texteq length-shortcut/memcmp/equal arms; bpchar trailing-space trim
//!    actually firing (raw lengths differ, verdict still equal).
//!
//! Negative controls (run with the DEFAULT solver; MUST FAIL):
//!  - control_bttextcmp_short_c_len: C sees a one-shorter left length.
//!  - control_bpchareq_untrimmed_c: C computes texteq (no trim) against
//!    Rust fc_bpchareq (trim) — counterexample "a " vs "a".

#[cfg(kani)]
mod proofs {
    use datum::{Datum, NullableDatum};
    use std::os::raw::c_int;
    use types_core::C_COLLATION_OID;
    use types_fmgr::LocalFcinfo;
    use types_tuple::{varatt, NameData};

    extern "C" {
        // text family (varlena.c)
        fn pg_texteq(d1: *const u8, l1: usize, d2: *const u8, l2: usize, collid: u32) -> c_int;
        fn pg_textne(d1: *const u8, l1: usize, d2: *const u8, l2: usize, collid: u32) -> c_int;
        fn pg_text_lt(d1: *const u8, l1: c_int, d2: *const u8, l2: c_int, collid: u32) -> c_int;
        fn pg_text_le(d1: *const u8, l1: c_int, d2: *const u8, l2: c_int, collid: u32) -> c_int;
        fn pg_text_gt(d1: *const u8, l1: c_int, d2: *const u8, l2: c_int, collid: u32) -> c_int;
        fn pg_text_ge(d1: *const u8, l1: c_int, d2: *const u8, l2: c_int, collid: u32) -> c_int;
        fn pg_bttextcmp(d1: *const u8, l1: c_int, d2: *const u8, l2: c_int, collid: u32) -> c_int;
        fn pg_text_larger(d1: *const u8, l1: c_int, d2: *const u8, l2: c_int, collid: u32)
            -> c_int;
        fn pg_text_smaller(d1: *const u8, l1: c_int, d2: *const u8, l2: c_int, collid: u32)
            -> c_int;

        // text pattern ops (collation-free)
        fn pg_text_pattern_lt(d1: *const u8, l1: c_int, d2: *const u8, l2: c_int) -> c_int;
        fn pg_text_pattern_le(d1: *const u8, l1: c_int, d2: *const u8, l2: c_int) -> c_int;
        fn pg_text_pattern_ge(d1: *const u8, l1: c_int, d2: *const u8, l2: c_int) -> c_int;
        fn pg_text_pattern_gt(d1: *const u8, l1: c_int, d2: *const u8, l2: c_int) -> c_int;
        fn pg_bttext_pattern_cmp(d1: *const u8, l1: c_int, d2: *const u8, l2: c_int) -> c_int;

        // name <-> text cross (varlena.c)
        fn pg_nameeqtext(n1: *const u8, d2: *const u8, l2: usize, collid: u32) -> c_int;
        fn pg_namenetext(n1: *const u8, d2: *const u8, l2: usize, collid: u32) -> c_int;
        fn pg_namelttext(n1: *const u8, d2: *const u8, l2: c_int, collid: u32) -> c_int;
        fn pg_nameletext(n1: *const u8, d2: *const u8, l2: c_int, collid: u32) -> c_int;
        fn pg_namegttext(n1: *const u8, d2: *const u8, l2: c_int, collid: u32) -> c_int;
        fn pg_namegetext(n1: *const u8, d2: *const u8, l2: c_int, collid: u32) -> c_int;
        fn pg_btnametextcmp(n1: *const u8, d2: *const u8, l2: c_int, collid: u32) -> c_int;
        fn pg_texteqname(d1: *const u8, l1: usize, n2: *const u8, collid: u32) -> c_int;
        fn pg_textnename(d1: *const u8, l1: usize, n2: *const u8, collid: u32) -> c_int;
        fn pg_textltname(d1: *const u8, l1: c_int, n2: *const u8, collid: u32) -> c_int;
        fn pg_textlename(d1: *const u8, l1: c_int, n2: *const u8, collid: u32) -> c_int;
        fn pg_textgtname(d1: *const u8, l1: c_int, n2: *const u8, collid: u32) -> c_int;
        fn pg_textgename(d1: *const u8, l1: c_int, n2: *const u8, collid: u32) -> c_int;
        fn pg_bttextnamecmp(d1: *const u8, l1: c_int, n2: *const u8, collid: u32) -> c_int;

        // bpchar family (varchar.c)
        fn pg_bpchareq(d1: *const u8, l1: c_int, d2: *const u8, l2: c_int, collid: u32) -> c_int;
        fn pg_bpcharne(d1: *const u8, l1: c_int, d2: *const u8, l2: c_int, collid: u32) -> c_int;
        fn pg_bpcharlt(d1: *const u8, l1: c_int, d2: *const u8, l2: c_int, collid: u32) -> c_int;
        fn pg_bpcharle(d1: *const u8, l1: c_int, d2: *const u8, l2: c_int, collid: u32) -> c_int;
        fn pg_bpchargt(d1: *const u8, l1: c_int, d2: *const u8, l2: c_int, collid: u32) -> c_int;
        fn pg_bpcharge(d1: *const u8, l1: c_int, d2: *const u8, l2: c_int, collid: u32) -> c_int;
        fn pg_bpcharcmp(d1: *const u8, l1: c_int, d2: *const u8, l2: c_int, collid: u32) -> c_int;
        fn pg_bpchar_larger(d1: *const u8, l1: c_int, d2: *const u8, l2: c_int, collid: u32)
            -> c_int;
        fn pg_bpchar_smaller(d1: *const u8, l1: c_int, d2: *const u8, l2: c_int, collid: u32)
            -> c_int;

        // bpchar pattern ops (collation-free)
        fn pg_bpchar_pattern_lt(d1: *const u8, l1: c_int, d2: *const u8, l2: c_int) -> c_int;
        fn pg_bpchar_pattern_le(d1: *const u8, l1: c_int, d2: *const u8, l2: c_int) -> c_int;
        fn pg_bpchar_pattern_ge(d1: *const u8, l1: c_int, d2: *const u8, l2: c_int) -> c_int;
        fn pg_bpchar_pattern_gt(d1: *const u8, l1: c_int, d2: *const u8, l2: c_int) -> c_int;
        fn pg_btbpchar_pattern_cmp(d1: *const u8, l1: c_int, d2: *const u8, l2: c_int) -> c_int;
    }

    const CAP: usize = 8;
    const COLL_C: u32 = C_COLLATION_OID;

    /// One pre-detoasted text/bpchar argument: a real 1-byte-header (short)
    /// inline varlena image over symbolic payload bytes + symbolic length
    /// <= CAP, built with the SHIPPED header encoder.
    struct VarImg {
        img: [u8; CAP + varatt::VARHDRSZ_SHORT],
        len: usize,
    }

    fn sym_varlena() -> VarImg {
        let payload: [u8; CAP] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= CAP);
        let mut img = [0u8; CAP + varatt::VARHDRSZ_SHORT];
        // SAFETY: img is writable and len + 1 <= VARATT_SHORT_MAX.
        unsafe { varatt::set_varsize_short(img.as_mut_ptr(), len + varatt::VARHDRSZ_SHORT) };
        let mut i = 0;
        while i < CAP {
            img[i + varatt::VARHDRSZ_SHORT] = payload[i];
            i += 1;
        }
        VarImg { img, len }
    }

    impl VarImg {
        fn datum(&self) -> Datum {
            Datum::from_usize(self.img.as_ptr() as usize)
        }
        fn data(&self) -> *const u8 {
            // payload pointer, as C's VARDATA_ANY on a 1B header
            self.img[varatt::VARHDRSZ_SHORT..].as_ptr()
        }
    }

    /// Full 64-byte symbolic NameData fenced to the C datatype invariant:
    /// NAME values always carry an in-buffer NUL terminator (data[63] == 0
    /// admits every terminated buffer; C strlen(NameStr) needs it).
    fn sym_name() -> NameData {
        let mut n = NameData { data: kani::any() };
        n.data[63] = 0;
        n
    }

    fn name_datum(n: &NameData) -> Datum {
        Datum::from_usize(n.data.as_ptr() as usize)
    }

    /// Run a shipped fc_* wrapper on a real 2-arg frame. The fenced
    /// comparators never error (collid is a valid constant), so the Err arm
    /// is statically dead.
    fn call<E>(
        fc: fn(
            Option<&mut types_fmgr::FmgrInfo>,
            &mut types_fmgr::FunctionCallInfoBaseData,
        ) -> Result<Datum, E>,
        a: Datum,
        b: Datum,
        collid: u32,
    ) -> Datum {
        let mut f = LocalFcinfo::<2>::new(collid);
        f.args[0] = NullableDatum::value(a);
        f.args[1] = NullableDatum::value(b);
        match fc(None, &mut f) {
            Ok(d) => d,
            Err(_) => panic!("comparator errored"),
        }
    }

    // ================= text family =================

    #[kani::proof]
    #[kani::unwind(10)]
    fn eq_texteq() {
        let (a, b) = (sym_varlena(), sym_varlena());
        // Fast-path coverage witnesses: length shortcut, memcmp arm, equality.
        kani::cover!(a.len != b.len);
        kani::cover!(a.len == b.len && a.img != b.img);
        kani::cover!(a.img == b.img);
        let c = unsafe { pg_texteq(a.data(), a.len, b.data(), b.len, COLL_C) };
        let r = call(varlena::builtins::fc_texteq, a.datum(), b.datum(), COLL_C);
        assert!((c != 0) == r.as_bool());
    }

    #[kani::proof]
    #[kani::unwind(10)]
    fn eq_textne() {
        let (a, b) = (sym_varlena(), sym_varlena());
        let c = unsafe { pg_textne(a.data(), a.len, b.data(), b.len, COLL_C) };
        let r = call(varlena::builtins::fc_textne, a.datum(), b.datum(), COLL_C);
        assert!((c != 0) == r.as_bool());
    }

    macro_rules! text_bool_op {
        ($($h:ident: $fc:path, $cfn:ident;)*) => {$(
            #[kani::proof]
            #[kani::unwind(10)]
            fn $h() {
                let (a, b) = (sym_varlena(), sym_varlena());
                let c = unsafe {
                    $cfn(a.data(), a.len as c_int, b.data(), b.len as c_int, COLL_C)
                };
                let r = call($fc, a.datum(), b.datum(), COLL_C);
                assert!((c != 0) == r.as_bool());
            }
        )*};
    }

    text_bool_op! {
        eq_text_lt: varlena::builtins::fc_text_lt, pg_text_lt;
        eq_text_le: varlena::builtins::fc_text_le, pg_text_le;
        eq_text_gt: varlena::builtins::fc_text_gt, pg_text_gt;
        eq_text_ge: varlena::builtins::fc_text_ge, pg_text_ge;
    }

    #[kani::proof]
    #[kani::unwind(10)]
    fn eq_bttextcmp() {
        let (a, b) = (sym_varlena(), sym_varlena());
        let c = unsafe { pg_bttextcmp(a.data(), a.len as c_int, b.data(), b.len as c_int, COLL_C) };
        let r = call(varlena::builtins::fc_bttextcmp, a.datum(), b.datum(), COLL_C);
        // exact SQL-visible int32 value (raw memcmp byte difference)
        assert!(c == r.as_i32());
    }

    macro_rules! minmax_op {
        ($($h:ident: $fc:path, $cfn:ident;)*) => {$(
            #[kani::proof]
            #[kani::unwind(10)]
            fn $h() {
                let (a, b) = (sym_varlena(), sym_varlena());
                let widx = unsafe {
                    $cfn(a.data(), a.len as c_int, b.data(), b.len as c_int, COLL_C)
                };
                let r = call($fc, a.datum(), b.datum(), COLL_C);
                // C returns the winning PG_GETARG_*_PP pointer (detoasted
                // image); the shipped wrapper must return the same image.
                let want = if widx == 0 { a.img.as_ptr() } else { b.img.as_ptr() } as usize;
                assert!(r.as_usize() == want);
            }
        )*};
    }

    minmax_op! {
        eq_text_larger: varlena::builtins::fc_text_larger, pg_text_larger;
        eq_text_smaller: varlena::builtins::fc_text_smaller, pg_text_smaller;
    }

    // ---- text pattern ops (collation-free memcmp semantics) ----

    macro_rules! pattern_bool_op {
        ($($h:ident: $fc:path, $cfn:ident;)*) => {$(
            #[kani::proof]
            #[kani::unwind(10)]
            fn $h() {
                let (a, b) = (sym_varlena(), sym_varlena());
                let c = unsafe {
                    $cfn(a.data(), a.len as c_int, b.data(), b.len as c_int)
                };
                let r = call($fc, a.datum(), b.datum(), COLL_C);
                assert!((c != 0) == r.as_bool());
            }
        )*};
    }

    pattern_bool_op! {
        eq_text_pattern_lt: varlena::builtins::fc_text_pattern_lt, pg_text_pattern_lt;
        eq_text_pattern_le: varlena::builtins::fc_text_pattern_le, pg_text_pattern_le;
        eq_text_pattern_ge: varlena::builtins::fc_text_pattern_ge, pg_text_pattern_ge;
        eq_text_pattern_gt: varlena::builtins::fc_text_pattern_gt, pg_text_pattern_gt;
        eq_bpchar_pattern_lt: adt_varchar::builtins::fc_bpchar_pattern_lt, pg_bpchar_pattern_lt;
        eq_bpchar_pattern_le: adt_varchar::builtins::fc_bpchar_pattern_le, pg_bpchar_pattern_le;
        eq_bpchar_pattern_ge: adt_varchar::builtins::fc_bpchar_pattern_ge, pg_bpchar_pattern_ge;
        eq_bpchar_pattern_gt: adt_varchar::builtins::fc_bpchar_pattern_gt, pg_bpchar_pattern_gt;
    }

    macro_rules! pattern_cmp_op {
        ($($h:ident: $fc:path, $cfn:ident;)*) => {$(
            #[kani::proof]
            #[kani::unwind(10)]
            fn $h() {
                let (a, b) = (sym_varlena(), sym_varlena());
                let c = unsafe {
                    $cfn(a.data(), a.len as c_int, b.data(), b.len as c_int)
                };
                let r = call($fc, a.datum(), b.datum(), COLL_C);
                assert!(c == r.as_i32());
            }
        )*};
    }

    pattern_cmp_op! {
        eq_bttext_pattern_cmp: varlena::builtins::fc_bttext_pattern_cmp, pg_bttext_pattern_cmp;
        eq_btbpchar_pattern_cmp:
            adt_varchar::builtins::fc_btbpchar_pattern_cmp, pg_btbpchar_pattern_cmp;
    }

    // ================= bpchar family =================

    #[kani::proof]
    #[kani::unwind(10)]
    fn eq_bpchareq() {
        let (a, b) = (sym_varlena(), sym_varlena());
        let c = unsafe { pg_bpchareq(a.data(), a.len as c_int, b.data(), b.len as c_int, COLL_C) };
        // Trim-path coverage witness: raw lengths differ yet the verdict is
        // equal — only reachable when the trailing-blank trim actually fires.
        kani::cover!(a.len != b.len && c != 0);
        let r = call(adt_varchar::builtins::fc_bpchareq, a.datum(), b.datum(), COLL_C);
        assert!((c != 0) == r.as_bool());
    }

    macro_rules! bpchar_bool_op {
        ($($h:ident: $fc:path, $cfn:ident;)*) => {$(
            #[kani::proof]
            #[kani::unwind(10)]
            fn $h() {
                let (a, b) = (sym_varlena(), sym_varlena());
                let c = unsafe {
                    $cfn(a.data(), a.len as c_int, b.data(), b.len as c_int, COLL_C)
                };
                let r = call($fc, a.datum(), b.datum(), COLL_C);
                assert!((c != 0) == r.as_bool());
            }
        )*};
    }

    bpchar_bool_op! {
        eq_bpcharne: adt_varchar::builtins::fc_bpcharne, pg_bpcharne;
        eq_bpcharlt: adt_varchar::builtins::fc_bpcharlt, pg_bpcharlt;
        eq_bpcharle: adt_varchar::builtins::fc_bpcharle, pg_bpcharle;
        eq_bpchargt: adt_varchar::builtins::fc_bpchargt, pg_bpchargt;
        eq_bpcharge: adt_varchar::builtins::fc_bpcharge, pg_bpcharge;
    }

    #[kani::proof]
    #[kani::unwind(10)]
    fn eq_bpcharcmp() {
        let (a, b) = (sym_varlena(), sym_varlena());
        let c = unsafe { pg_bpcharcmp(a.data(), a.len as c_int, b.data(), b.len as c_int, COLL_C) };
        let r = call(adt_varchar::builtins::fc_bpcharcmp, a.datum(), b.datum(), COLL_C);
        assert!(c == r.as_i32());
    }

    minmax_op! {
        eq_bpchar_larger: adt_varchar::builtins::fc_bpchar_larger, pg_bpchar_larger;
        eq_bpchar_smaller: adt_varchar::builtins::fc_bpchar_smaller, pg_bpchar_smaller;
    }

    // ================= name <-> text cross =================
    // Full 64-byte symbolic name (terminated) x symbolic text len<=8;
    // strlen/name_str scan the whole buffer -> unwind 66.

    macro_rules! nametext_bool_op {
        ($($h:ident: $fc:path, $cfn:ident;)*) => {$(
            #[kani::proof]
            #[kani::unwind(66)]
            fn $h() {
                let n = sym_name();
                let t = sym_varlena();
                let c = unsafe { $cfn(n.data.as_ptr(), t.data(), t.len, COLL_C) };
                let r = call($fc, name_datum(&n), t.datum(), COLL_C);
                assert!((c != 0) == r.as_bool());
            }
        )*};
    }

    macro_rules! nametext_bool_op_int {
        ($($h:ident: $fc:path, $cfn:ident;)*) => {$(
            #[kani::proof]
            #[kani::unwind(66)]
            fn $h() {
                let n = sym_name();
                let t = sym_varlena();
                let c = unsafe { $cfn(n.data.as_ptr(), t.data(), t.len as c_int, COLL_C) };
                let r = call($fc, name_datum(&n), t.datum(), COLL_C);
                assert!((c != 0) == r.as_bool());
            }
        )*};
    }

    nametext_bool_op! {
        eq_nameeqtext: name::builtins::fc_nameeqtext, pg_nameeqtext;
        eq_namenetext: name::builtins::fc_namenetext, pg_namenetext;
    }

    nametext_bool_op_int! {
        eq_namelttext: name::builtins::fc_namelttext, pg_namelttext;
        eq_nameletext: name::builtins::fc_nameletext, pg_nameletext;
        eq_namegttext: name::builtins::fc_namegttext, pg_namegttext;
        eq_namegetext: name::builtins::fc_namegetext, pg_namegetext;
    }

    #[kani::proof]
    #[kani::unwind(66)]
    fn eq_btnametextcmp() {
        let n = sym_name();
        let t = sym_varlena();
        let c = unsafe { pg_btnametextcmp(n.data.as_ptr(), t.data(), t.len as c_int, COLL_C) };
        let r = call(name::builtins::fc_btnametextcmp, name_datum(&n), t.datum(), COLL_C);
        assert!(c == r.as_i32());
    }

    macro_rules! textname_bool_op {
        ($($h:ident: $fc:path, $cfn:ident;)*) => {$(
            #[kani::proof]
            #[kani::unwind(66)]
            fn $h() {
                let t = sym_varlena();
                let n = sym_name();
                let c = unsafe { $cfn(t.data(), t.len, n.data.as_ptr(), COLL_C) };
                let r = call($fc, t.datum(), name_datum(&n), COLL_C);
                assert!((c != 0) == r.as_bool());
            }
        )*};
    }

    macro_rules! textname_bool_op_int {
        ($($h:ident: $fc:path, $cfn:ident;)*) => {$(
            #[kani::proof]
            #[kani::unwind(66)]
            fn $h() {
                let t = sym_varlena();
                let n = sym_name();
                let c = unsafe { $cfn(t.data(), t.len as c_int, n.data.as_ptr(), COLL_C) };
                let r = call($fc, t.datum(), name_datum(&n), COLL_C);
                assert!((c != 0) == r.as_bool());
            }
        )*};
    }

    textname_bool_op! {
        eq_texteqname: name::builtins::fc_texteqname, pg_texteqname;
        eq_textnename: name::builtins::fc_textnename, pg_textnename;
    }

    textname_bool_op_int! {
        eq_textltname: name::builtins::fc_textltname, pg_textltname;
        eq_textlename: name::builtins::fc_textlename, pg_textlename;
        eq_textgtname: name::builtins::fc_textgtname, pg_textgtname;
        eq_textgename: name::builtins::fc_textgename, pg_textgename;
    }

    #[kani::proof]
    #[kani::unwind(66)]
    fn eq_bttextnamecmp() {
        let t = sym_varlena();
        let n = sym_name();
        let c = unsafe { pg_bttextnamecmp(t.data(), t.len as c_int, n.data.as_ptr(), COLL_C) };
        let r = call(name::builtins::fc_bttextnamecmp, t.datum(), name_datum(&n), COLL_C);
        assert!(c == r.as_i32());
    }

    /// Width-1 probe (NOT part of the standing gate): pins strlen(name)==63
    /// (all bytes nonzero, terminator at 63) to test whether the ordered
    /// name-cross harness cost is unwind-DEPTH-bound — the skill's mandated
    /// check before case-splitting a slow symbolic-length harness.
    #[kani::proof]
    #[kani::unwind(66)]
    fn probe_textltname_len63() {
        let mut n = NameData { data: kani::any() };
        let mut i = 0;
        while i < 63 {
            kani::assume(n.data[i] != 0);
            i += 1;
        }
        n.data[63] = 0;
        let t = sym_varlena();
        let c = unsafe { pg_textltname(t.data(), t.len as c_int, n.data.as_ptr(), COLL_C) };
        let r = call(name::builtins::fc_textltname, t.datum(), name_datum(&n), COLL_C);
        assert!((c != 0) == r.as_bool());
    }

    // ============ negative controls (MUST FAIL: rig non-vacuity) ============
    // Run with the DEFAULT solver (kissat re-enumerates SAT passes on failing
    // harnesses and never terminates).

    /// C sees a one-byte-shorter left length than the shipped wrapper.
    /// Expected: VERIFICATION FAILED with a decodable counterexample.
    #[kani::proof]
    #[kani::unwind(10)]
    fn control_bttextcmp_short_c_len() {
        let (a, b) = (sym_varlena(), sym_varlena());
        kani::assume(a.len >= 1);
        let c = unsafe {
            pg_bttextcmp(a.data(), (a.len - 1) as c_int, b.data(), b.len as c_int, COLL_C)
        };
        let r = call(varlena::builtins::fc_bttextcmp, a.datum(), b.datum(), COLL_C);
        assert!(c == r.as_i32());
    }

    /// C computes texteq (no trailing-blank trim) against the shipped
    /// fc_bpchareq (trim). Expected: VERIFICATION FAILED ("a " vs "a").
    #[kani::proof]
    #[kani::unwind(10)]
    fn control_bpchareq_untrimmed_c() {
        let (a, b) = (sym_varlena(), sym_varlena());
        let c = unsafe { pg_texteq(a.data(), a.len, b.data(), b.len, COLL_C) };
        let r = call(adt_varchar::builtins::fc_bpchareq, a.datum(), b.datum(), COLL_C);
        assert!((c != 0) == r.as_bool());
    }
}
