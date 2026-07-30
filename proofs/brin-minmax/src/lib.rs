//! Kani C≡Rust equivalence: BRIN minmax opclass support functions — the
//! first index-support family.
//!
//! Rust side (shipped, path-dep — never copied):
//!   crates/backend/access/brin/brin_minmax/src/lib.rs —
//!   brin_minmax_opcinfo / brin_minmax_add_value / brin_minmax_consistent /
//!   brin_minmax_union, including minmax_get_strategy_procinfo's per-subtype
//!   strategy cache and fmgr::function_call2_coll_in dispatch.
//! C side: c/pg_brin_minmax.c (verbatim REL_18_STABLE brin_minmax.c bodies +
//! verbatim int.c/date.c/uuid.c comparators; see its header for every shim).
//!
//! THEOREM SHAPE (support-function VALUE equivalence; AM machinery/page
//! behavior out of scope): given the SAME resolved comparator family at the
//! catalog seam, add_value/consistent/union produce identical results —
//! returned verdict, updated bv_values (by value for byval, by pointee for
//! byref), and bv_allnulls — for all symbolic inputs.
//!
//! THE CATALOG SEAM: both sides resolve strategy -> operator -> proc ->
//! comparator through their real code paths (minmax_get_strategy_procinfo
//! verbatim on C, the shipped Rust fn incl. its cache), but the CATALOG
//! CONTENT is modeled identically on both sides as the real REL_18 pg_amop/
//! pg_operator rows for the int4/date/uuid btree comparator families:
//!   - Rust: kani::stub of lsyscache::amop::get_opfamily_member,
//!     lsyscache::operator::get_opcode, fmgr_core::fmgr_info (the stub
//!     returns an FmgrInfo whose fn_addr is the SHIPPED fc_* comparator
//!     wrapper — the comparator body and datum unwrap/pack stay in-theorem).
//!   - C: SearchSysCache4/SysCacheGetAttrNotNull/get_opcode/fmgr_info_cxt
//!     shims over the same oid tables, dispatching to the VERBATIM C
//!     comparator bodies.
//! Syscache internals are out of proof. Negative control `control_seam_skew`
//! skews the C table only (strategy-1 proc dispatches <=) and must FAIL —
//! witnessing the seam model is load-bearing on both sides.
//!
//! Types proven (one theorem instantiation per attached type):
//!   int4 (byval, attlen 4), date (byval, attlen 4), uuid (byref, attlen 16
//!   — exercises the datumCopy by-ref path; copies land in an allocator
//!   model on both sides — C: static bump arena, Rust: one leaked heap
//!   object per copy — out of proof; VALUES compared by pointee, with all
//!   16-byte pointee reads performed in C via pg_read16, see read16_c).
//!
//! Fences / bounds:
//!   - consistent: strategy fixed per harness (1..=5, five harnesses per
//!     type). Out-of-range strategies are an elog(ERROR)/panic on the
//!     respective sides — error paths, not a value-parity surface.
//!   - subtype == atttypid (the only rows the model catalog carries; the
//!     shipped cross-type rows for date/timestamp minmax are a different
//!     family's theorem).
//!   - collation is passed through symbolically to the comparator (unused
//!     by int4/date/uuid comparators on either side).
//!   - isnull=false for add_value (C asserts, Rust debug_asserts — the AM
//!     caller contract).
//!
//! Harness scaffolding (not part of the claim): the allocator is the
//! proof_support static-buffer bump model (Mcx::allocate/grow/deallocate all
//! stubbed), and the Mcx handle points at an OPAQUE DUMMY MemoryContext —
//! sound because with every Allocator entry point stubbed, nothing reachable
//! from the theorem reads the context object (a size/align-checked zeroed
//! static; any accidental read would be of harness-owned zeros, and the
//! negative control plus 23 green harnesses referee the rig). PgError
//! message plumbing is stubbed field-identically (value/verdict in-theorem,
//! message text out). Structures are mem::forgotten so drop machinery stays
//! out of symex. Ledger wording: "modulo static-buffer allocator model".
//!
//! Run (measured 2026-07-28 recipe, encoded in run-all.sh): kissat for the
//! scalar (int4/date) and all consistent harnesses — the DEFAULT solver
//! rss-killed int4 add_value at 6.7-8.7 GiB even with --slice-formula;
//! DEFAULT solver for the uuid add_value/union harnesses (both solvers green
//! there; default is faster, kissat has more memory headroom) and for the
//! MUST-FAIL control (kissat never terminates on failing harnesses):
//!   timeout 600 cargo kani -Z c-ffi -Z stubbing --c-lib c/pg_brin_minmax.c \
//!       --harness <h> --exact [--solver kissat]

#![recursion_limit = "512"]

#[cfg(kani)]
mod proofs {
    use datum::Datum;
    use proof_support::{mcx_stubs, stubs};
    use std::rc::Rc;
    use types_brin::{BrinDesc, BrinValues};
    use types_core::{Oid, RegProcedure};
    use types_error::PgResult;
    use types_fmgr::{FmgrInfo, PGFunction};
    use types_scan::scankey::ScanKeyData;
    use types_tuple::{FormData_pg_attribute, TupleDescData};

    extern "C" {
        fn pg_run_minmax_add_value(
            atttypid: u32, attlen: i16, attbyval: i32, opfamily: u32,
            allnulls: i32, min: usize, max: usize, newval: usize, colloid: u32,
            out_min: *mut usize, out_max: *mut usize, out_allnulls: *mut i32,
        ) -> i32;
        fn pg_run_minmax_consistent(
            atttypid: u32, attlen: i16, attbyval: i32, opfamily: u32,
            min: usize, max: usize, strategy: u16, subtype: u32, value: usize,
            colloid: u32,
        ) -> i32;
        fn pg_run_minmax_union(
            atttypid: u32, attlen: i16, attbyval: i32, opfamily: u32,
            a_min: usize, a_max: usize, b_min: usize, b_max: usize,
            colloid: u32, out_min: *mut usize, out_max: *mut usize,
        ) -> i32; // int returns: Kani Unit-vs-void FFI trap
        fn pg_run_minmax_opcinfo(
            typoid: u32, nstored: *mut u16, regular_nulls: *mut i32,
            typid0: *mut u32, typid1: *mut u32,
        ) -> i32;
        /// 16-byte pointee read done in C: C-returned datums have no Kani
        /// provenance, and a Rust-side int->ptr deref of them explodes SAT
        /// (see c/pg_brin_minmax.c pg_read16 comment).
        fn pg_read16(d: usize, out: *mut u8) -> i32;
        /// negative control: C-side strategy-1 procs dispatch <= when 1
        static mut pg_seam_skew: i32;
    }

    // type oids + opfamily tokens (opfamily is opaque to the model — it keys
    // on lefttype; tokens are the real brin minmax opfamily oids for hygiene)
    const INT4OID: Oid = 23;
    const DATEOID: Oid = 1082;
    const UUIDOID: Oid = 2950;
    const FAM_INT4: Oid = 4054;
    const FAM_DATE: Oid = 4059;
    const FAM_UUID: Oid = 4081;

    // ---------- the catalog-seam model (mirrors c/pg_brin_minmax.c) ----------

    /// Stub for lsyscache::amop::get_opfamily_member: the real REL_18 pg_amop
    /// btree-comparator rows for int4/date/uuid, strategy 1..=5.
    fn stub_get_opfamily_member(
        _opfamily: Oid,
        lefttype: Oid,
        righttype: Oid,
        strategy: i16,
    ) -> PgResult<Oid> {
        if lefttype != righttype || !(1..=5).contains(&strategy) {
            return Ok(0);
        }
        let ops: [Oid; 5] = match lefttype {
            INT4OID => [97, 523, 96, 525, 521],
            DATEOID => [1095, 1096, 1093, 1098, 1097],
            UUIDOID => [2974, 2976, 2972, 2977, 2975],
            _ => return Ok(0),
        };
        Ok(ops[strategy as usize - 1])
    }

    /// Stub for lsyscache::operator::get_opcode: pg_operator oprcode rows.
    fn stub_get_opcode(opno: Oid) -> PgResult<RegProcedure> {
        Ok(match opno {
            97 => 66,
            523 => 149,
            96 => 65,
            525 => 150,
            521 => 147,
            1095 => 1087,
            1096 => 1088,
            1093 => 1086,
            1098 => 1090,
            1097 => 1089,
            2974 => 2954,
            2976 => 2955,
            2972 => 2956,
            2977 => 2957,
            2975 => 2958,
            _ => 0,
        })
    }

    /// Stub for fmgr_core::fmgr_info: resolves the proc oid to the SHIPPED
    /// fc_* comparator wrapper (comparator body stays in-theorem); the
    /// builtin-table walk is out of proof.
    fn stub_fmgr_info(function_id: Oid) -> PgResult<FmgrInfo> {
        let f: PGFunction = match function_id {
            66 => adt_int::builtins::fc_int4lt,
            149 => adt_int::builtins::fc_int4le,
            65 => adt_int::builtins::fc_int4eq,
            150 => adt_int::builtins::fc_int4ge,
            147 => adt_int::builtins::fc_int4gt,
            1087 => adt_date::builtins::fc_date_lt,
            1088 => adt_date::builtins::fc_date_le,
            1086 => adt_date::builtins::fc_date_eq,
            1090 => adt_date::builtins::fc_date_ge,
            1089 => adt_date::builtins::fc_date_gt,
            2954 => adt_uuid::builtins::fc_uuid_lt,
            2955 => adt_uuid::builtins::fc_uuid_le,
            2956 => adt_uuid::builtins::fc_uuid_eq,
            2957 => adt_uuid::builtins::fc_uuid_ge,
            2958 => adt_uuid::builtins::fc_uuid_gt,
            _ => panic!("unwired proc oid"),
        };
        Ok(FmgrInfo::new(f, function_id, 2, true, false))
    }

    // ---------- harness plumbing ----------

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

    fn empty_tupdesc<'m>(mcx: mcx::Mcx<'m>) -> TupleDescData<'m> {
        TupleDescData {
            natts: 0,
            tdtypeid: 0,
            tdtypmod: -1,
            tdrefcount: -1,
            constr: None,
            compact_attrs: mcx::vec_new_in(mcx),
            attrs: mcx::vec_new_in(mcx),
        }
    }

    /// One-attribute BrinDesc mirroring what brininitialize builds; bd_info
    /// comes from the SHIPPED brin_minmax_opcinfo (in-theorem wiring).
    fn mk_bdesc<'m>(
        mcx: mcx::Mcx<'m>,
        atttypid: Oid,
        attlen: i16,
        attbyval: bool,
        opfamily: Oid,
    ) -> BrinDesc<'m> {
        let mut att = FormData_pg_attribute::default();
        att.atttypid = atttypid;
        att.attlen = attlen;
        att.attbyval = attbyval;
        att.attnum = 1;
        let mut attrs = ok(mcx::vec_with_capacity_in(mcx, 1));
        attrs.push(att);
        BrinDesc {
            bd_tupdesc: Rc::new(TupleDescData {
                natts: 1,
                tdtypeid: 0,
                tdtypmod: -1,
                tdrefcount: -1,
                constr: None,
                compact_attrs: mcx::vec_new_in(mcx),
                attrs,
            }),
            bd_disktdesc: empty_tupdesc(mcx),
            bd_totalstored: 2,
            bd_opfamily: vec![opfamily],
            bd_opcintype: vec![atttypid],
            bd_indcollation: vec![0],
            bd_pages_per_range: 128,
            bd_info: vec![brin_minmax::brin_minmax_opcinfo(atttypid)],
        }
    }

    fn brin_values(allnulls: bool, min: Datum, max: Datum) -> BrinValues {
        BrinValues {
            bv_attno: 1,
            bv_hasnulls: false,
            bv_allnulls: allnulls,
            bv_values: [min, max, Datum::null()],
            bv_mem_value: None,
        }
    }

    fn scankey(strategy: u16, subtype: Oid, value: Datum, colloid: Oid) -> ScanKeyData {
        ScanKeyData {
            sk_flags: 0,
            sk_attno: 1,
            sk_strategy: strategy,
            sk_subtype: subtype,
            sk_collation: colloid,
            // dummy carrier: brin_minmax never invokes sk_func (it resolves
            // comparators through the strategy cache)
            sk_func: FmgrInfo::new(adt_int::builtins::fc_int4eq, 0, 2, true, false),
            sk_argument: value,
        }
    }

    /// 16-byte pointee read for uuid datum comparisons, performed INSIDE C
    /// (pg_read16). Measured (allnulls arm, default solver): a Rust-side
    /// `usize as *const [u8;16]` deref of a datum that round-tripped through
    /// the datum-copy machinery or the FFI out-params has no Kani provenance
    /// and explodes propositional reduction (11.3 GiB rss-kill); the same 16
    /// bytes read by C code in the shared goto-program cost 2.3s. The read is
    /// harness plumbing either way — never part of the claim.
    fn read16_c(d: usize) -> [u8; 16] {
        let mut out = [0u8; 16];
        unsafe { pg_read16(d, out.as_mut_ptr()) };
        out
    }

    // $u = exact unwind bound: 8 for scalar (byval) harnesses (strategy-cache
    // reset loop = 5 iters dominates), 17 for uuid (16-byte memcmp + 1).
    // Slack here is not a nicety: unwind(20)'s dead loop copies pushed the
    // two-comparator consistent_eq harnesses past the 6 GiB memory protocol.
    macro_rules! seam_stubs {
        ($u:literal, $(#[$m:meta])* fn $h:ident() $body:block) => {
            #[kani::proof]
            #[kani::unwind($u)]
            #[kani::stub(lsyscache::amop::get_opfamily_member, stub_get_opfamily_member)]
            #[kani::stub(lsyscache::operator::get_opcode, stub_get_opcode)]
            #[kani::stub(fmgr_core::fmgr_info, stub_fmgr_info)]
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

    /// Stub for adt_scalar::datum_ops::datum_copy on BYVAL harnesses: the
    /// shipped fn's byval arm verbatim (`if typbyval { return Ok(value) }`),
    /// with the non-byval branch replaced by a LOUD proof failure — if any
    /// byval harness ever routed a by-ref copy here, the proof fails rather
    /// than passing vacuously. Motivation: the dead non-byval branch drags
    /// varlena/expanded-datum sizing into the CNF (3.4x program expression)
    /// and walls the 6GB memory protocol; the by-ref copy path is proven by
    /// the uuid harnesses with the real datum_copy.
    fn stub_datum_copy_byval(
        _mcx: mcx::Mcx<'_>,
        value: Datum,
        typbyval: bool,
        _typlen: i16,
    ) -> PgResult<Datum> {
        assert!(typbyval, "byval datum_copy stub reached a by-ref copy");
        Ok(value)
    }

    /// Stub for adt_scalar::datum_ops::datum_copy on FIXED-LENGTH BY-REF
    /// harnesses (uuid: typlen 16): the shipped fn's `typlen > 0` arm
    /// verbatim — datum_get_size(typlen>0) == typlen, maxaligned alloc, real
    /// byte copy — with the varlena (-1) / cstring (-2) sizing arms replaced
    /// by a LOUD proof failure (they wall the memory protocol and are dead
    /// for attlen 16).
    /// The copy target is a DISTINCT LEAKED HEAP OBJECT per call rather than
    /// a slice of the shared proof-heap static: pointee reads of a datum
    /// carved out of the 2 KiB bump static explode propositional reduction
    /// (measured 6-11 GiB, rss-killed) while reads of small distinct Kani
    /// heap objects are cheap. Allocation strategy is out of proof either
    /// way ("modulo static-buffer/leaked-object allocator model").
    fn stub_datum_copy_fixedlen(
        _mcx: mcx::Mcx<'_>,
        value: Datum,
        typbyval: bool,
        typlen: i16,
    ) -> PgResult<Datum> {
        if typbyval {
            return Ok(value);
        }
        assert!(
            typlen == 16,
            "fixed-len datum_copy stub: this family only copies uuid (typlen 16)"
        );
        let dst: &'static mut [u8; 16] = Box::leak(Box::new([0u8; 16]));
        // SAFETY: src live for 16 bytes per by-ref datum contract; dst fresh.
        unsafe {
            core::ptr::copy_nonoverlapping(value.as_usize() as *const u8, dst.as_mut_ptr(), 16)
        };
        Ok(Datum::from_usize(dst.as_mut_ptr() as usize))
    }

    /// seam_stubs + the byval datum_copy stub (int4/date add_value + union).
    /// Flat copy of seam_stubs' attribute list (nesting the macros trips the
    /// attribute-expansion recursion limit).
    macro_rules! seam_stubs_byval {
        ($(#[$m:meta])* fn $h:ident() $body:block) => {
            #[kani::proof]
            // byval harnesses touch no 16-byte memcpy/memcmp: the deepest
            // loop is the strategy-cache reset (5 iters) + mcx registry
            // sweep. unwind(20)'s dead loop copies inflated the formula past
            // the 6 GiB memory protocol (tree RSS 6.6-9.2 GB, rss-killed);
            // unwind(8) is exact-fit and verifies with unwinding checks on.
            #[kani::unwind(8)]
            #[kani::stub(lsyscache::amop::get_opfamily_member, stub_get_opfamily_member)]
            #[kani::stub(lsyscache::operator::get_opcode, stub_get_opcode)]
            #[kani::stub(fmgr_core::fmgr_info, stub_fmgr_info)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
            #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            #[kani::stub(std::string::String::from_utf8_lossy, stubs::stub_from_utf8_lossy)]
            #[kani::stub(types_fmgr::fcinfo::opaque_false, stub_opaque_false)]
            #[kani::stub(adt_scalar::datum_ops::datum_copy, stub_datum_copy_byval)]
            $(#[$m])*
            fn $h() $body
        };
    }

    /// seam_stubs + the fixed-len datum_copy stub (uuid add_value + union).
    /// Flat copy of seam_stubs' attribute list (nesting the macros trips the
    /// attribute-expansion recursion limit).
    macro_rules! seam_stubs_fixedlen {
        ($(#[$m:meta])* fn $h:ident() $body:block) => {
            #[kani::proof]
            // exact fit: 16-byte memcpy/memcmp (16+1) > strategy-cache reset
            // (5) — unwind slack inflates the formula (memory protocol).
            #[kani::unwind(17)]
            #[kani::stub(lsyscache::amop::get_opfamily_member, stub_get_opfamily_member)]
            #[kani::stub(lsyscache::operator::get_opcode, stub_get_opcode)]
            #[kani::stub(fmgr_core::fmgr_info, stub_fmgr_info)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
            #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            #[kani::stub(std::string::String::from_utf8_lossy, stubs::stub_from_utf8_lossy)]
            #[kani::stub(types_fmgr::fcinfo::opaque_false, stub_opaque_false)]
            #[kani::stub(adt_scalar::datum_ops::datum_copy, stub_datum_copy_fixedlen)]
            $(#[$m])*
            fn $h() $body
        };
    }

    // ---------- add_value: byval types (int4, date) ----------

    macro_rules! addvalue_byval {
        ($($h:ident: $typ:expr, $fam:expr;)*) => {$(
            seam_stubs_byval! {
                fn $h() {
                    let newval: i32 = kani::any();
                    let min: i32 = kani::any();
                    let max: i32 = kani::any();
                    let allnulls: bool = kani::any();
                    let colloid: u32 = kani::any();

                    let m = dummy_mcx();
                    let bdesc = mk_bdesc(m, $typ, 4, true, $fam);
                    let mut col = brin_values(
                        allnulls,
                        Datum::from_i32(min),
                        Datum::from_i32(max),
                    );
                    let r = ok(brin_minmax::brin_minmax_add_value(
                        m, &bdesc, &mut col, Datum::from_i32(newval), false, colloid,
                    ));

                    let (mut c_min, mut c_max, mut c_all): (usize, usize, i32) = (0, 0, 0);
                    let c = unsafe {
                        pg_run_minmax_add_value(
                            $typ, 4, 1, $fam, allnulls as i32,
                            Datum::from_i32(min).as_usize(),
                            Datum::from_i32(max).as_usize(),
                            Datum::from_i32(newval).as_usize(), colloid,
                            &mut c_min, &mut c_max, &mut c_all,
                        )
                    };

                    assert!(r as i32 == c);
                    assert!(col.bv_allnulls as i32 == c_all);
                    assert!(col.bv_values[0].as_i32() == c_min as u32 as i32);
                    assert!(col.bv_values[1].as_i32() == c_max as u32 as i32);
                    core::mem::forget(col);
                    core::mem::forget(bdesc);
                }
            }
        )*};
    }

    addvalue_byval! {
        eq_int4_add_value: INT4OID, FAM_INT4;
        eq_date_add_value: DATEOID, FAM_DATE;
    }

    // ---------- add_value: uuid (byref, attlen 16, datumCopy path) ----------

    // allnulls is CASE-SPLIT to the two concrete bool values (this pair of
    // harnesses exhausts the bool domain — no coverage harness needed): with
    // allnulls symbolic, both program arms enter one formula and cbmc's
    // propositional reduction blows the 6 GiB memory protocol (measured
    // 7.2-8.4 GB, rss-killed). eq_uuid_add_value = allnulls-false arm
    // (comparator + conditional datumCopy updates); eq_uuid_add_value_nulls =
    // allnulls-true arm (unconditional datumCopy of newval into both slots).
    macro_rules! uuid_add_value_split {
        ($($h:ident: $allnulls:expr;)*) => {$(
        seam_stubs_fixedlen! {
        fn $h() {
            let new_img: [u8; 16] = kani::any();
            let min_img: [u8; 16] = kani::any();
            let max_img: [u8; 16] = kani::any();
            let allnulls: bool = $allnulls;

            let m = dummy_mcx();
            let bdesc = mk_bdesc(m, UUIDOID, 16, false, FAM_UUID);
            let mut col = brin_values(
                allnulls,
                Datum::from_usize(min_img.as_ptr() as usize),
                Datum::from_usize(max_img.as_ptr() as usize),
            );
            let r = ok(brin_minmax::brin_minmax_add_value(
                m, &bdesc, &mut col,
                Datum::from_usize(new_img.as_ptr() as usize), false, 0,
            ));

            let (mut c_min, mut c_max, mut c_all): (usize, usize, i32) = (0, 0, 0);
            let c = unsafe {
                pg_run_minmax_add_value(
                    UUIDOID, 16, 0, FAM_UUID, allnulls as i32,
                    min_img.as_ptr() as usize, max_img.as_ptr() as usize,
                    new_img.as_ptr() as usize, 0,
                    &mut c_min, &mut c_max, &mut c_all,
                )
            };

            assert!(r as i32 == c);
            assert!(col.bv_allnulls as i32 == c_all);
            // by-ref summaries compare by pointee (C-returned datums read via
            // pg_read16 — see its comment; Rust-held datums read in Rust)
            assert!(read16_c(col.bv_values[0].as_usize()) == read16_c(c_min));
            assert!(read16_c(col.bv_values[1].as_usize()) == read16_c(c_max));
            core::mem::forget(col);
            core::mem::forget(bdesc);
        }
        }
        )*};
    }

    uuid_add_value_split! {
        eq_uuid_add_value: false;
        eq_uuid_add_value_nulls: true;
    }

    // ---------- consistent: one harness per strategy (concrete selector;
    // the invalid-strategy arm is a panic/elog on the respective sides) ----

    macro_rules! consistent_byval {
        ($($h:ident: $typ:expr, $fam:expr, $strategy:expr;)*) => {$(
            seam_stubs! {
                8,
                fn $h() {
                    let min: i32 = kani::any();
                    let max: i32 = kani::any();
                    let value: i32 = kani::any();
                    let colloid: u32 = kani::any();

                    let m = dummy_mcx();
                    let bdesc = mk_bdesc(m, $typ, 4, true, $fam);
                    let col = brin_values(false, Datum::from_i32(min), Datum::from_i32(max));
                    let key = scankey($strategy, $typ, Datum::from_i32(value), colloid);
                    let r = ok(brin_minmax::brin_minmax_consistent(m, &bdesc, &col, &key));

                    let c = unsafe {
                        pg_run_minmax_consistent(
                            $typ, 4, 1, $fam,
                            Datum::from_i32(min).as_usize(),
                            Datum::from_i32(max).as_usize(),
                            $strategy, $typ,
                            Datum::from_i32(value).as_usize(), colloid,
                        )
                    };
                    assert!(r as i32 == c);
                    core::mem::forget(key);
                    core::mem::forget(col);
                    core::mem::forget(bdesc);
                }
            }
        )*};
    }

    consistent_byval! {
        eq_int4_consistent_lt: INT4OID, FAM_INT4, 1;
        eq_int4_consistent_le: INT4OID, FAM_INT4, 2;
        eq_int4_consistent_eq: INT4OID, FAM_INT4, 3;
        eq_int4_consistent_ge: INT4OID, FAM_INT4, 4;
        eq_int4_consistent_gt: INT4OID, FAM_INT4, 5;
        eq_date_consistent_lt: DATEOID, FAM_DATE, 1;
        eq_date_consistent_le: DATEOID, FAM_DATE, 2;
        eq_date_consistent_eq: DATEOID, FAM_DATE, 3;
        eq_date_consistent_ge: DATEOID, FAM_DATE, 4;
        eq_date_consistent_gt: DATEOID, FAM_DATE, 5;
    }

    macro_rules! consistent_uuid {
        ($($h:ident: $strategy:expr;)*) => {$(
            seam_stubs! {
                17,
                fn $h() {
                    let min_img: [u8; 16] = kani::any();
                    let max_img: [u8; 16] = kani::any();
                    let val_img: [u8; 16] = kani::any();

                    let m = dummy_mcx();
                    let bdesc = mk_bdesc(m, UUIDOID, 16, false, FAM_UUID);
                    let col = brin_values(
                        false,
                        Datum::from_usize(min_img.as_ptr() as usize),
                        Datum::from_usize(max_img.as_ptr() as usize),
                    );
                    let key = scankey(
                        $strategy, UUIDOID,
                        Datum::from_usize(val_img.as_ptr() as usize), 0,
                    );
                    let r = ok(brin_minmax::brin_minmax_consistent(m, &bdesc, &col, &key));

                    let c = unsafe {
                        pg_run_minmax_consistent(
                            UUIDOID, 16, 0, FAM_UUID,
                            min_img.as_ptr() as usize, max_img.as_ptr() as usize,
                            $strategy, UUIDOID, val_img.as_ptr() as usize, 0,
                        )
                    };
                    assert!(r as i32 == c);
                    core::mem::forget(key);
                    core::mem::forget(col);
                    core::mem::forget(bdesc);
                }
            }
        )*};
    }

    consistent_uuid! {
        eq_uuid_consistent_lt: 1;
        eq_uuid_consistent_le: 2;
        eq_uuid_consistent_eq: 3;
        eq_uuid_consistent_ge: 4;
        eq_uuid_consistent_gt: 5;
    }

    // ---------- union ----------

    macro_rules! union_byval {
        ($($h:ident: $typ:expr, $fam:expr;)*) => {$(
            seam_stubs_byval! {
                fn $h() {
                    let a_min: i32 = kani::any();
                    let a_max: i32 = kani::any();
                    let b_min: i32 = kani::any();
                    let b_max: i32 = kani::any();
                    let colloid: u32 = kani::any();

                    let m = dummy_mcx();
                    let bdesc = mk_bdesc(m, $typ, 4, true, $fam);
                    let mut col_a = brin_values(false, Datum::from_i32(a_min), Datum::from_i32(a_max));
                    let col_b = brin_values(false, Datum::from_i32(b_min), Datum::from_i32(b_max));
                    ok(brin_minmax::brin_minmax_union(m, &bdesc, colloid, &mut col_a, &col_b));

                    let (mut c_min, mut c_max): (usize, usize) = (0, 0);
                    unsafe {
                        pg_run_minmax_union(
                            $typ, 4, 1, $fam,
                            Datum::from_i32(a_min).as_usize(),
                            Datum::from_i32(a_max).as_usize(),
                            Datum::from_i32(b_min).as_usize(),
                            Datum::from_i32(b_max).as_usize(), colloid,
                            &mut c_min, &mut c_max,
                        )
                    };
                    assert!(col_a.bv_values[0].as_i32() == c_min as u32 as i32);
                    assert!(col_a.bv_values[1].as_i32() == c_max as u32 as i32);
                    core::mem::forget(col_a);
                    core::mem::forget(col_b);
                    core::mem::forget(bdesc);
                }
            }
        )*};
    }

    union_byval! {
        eq_int4_union: INT4OID, FAM_INT4;
        eq_date_union: DATEOID, FAM_DATE;
    }

    seam_stubs_fixedlen! {
        fn eq_uuid_union() {
            let a_min: [u8; 16] = kani::any();
            let a_max: [u8; 16] = kani::any();
            let b_min: [u8; 16] = kani::any();
            let b_max: [u8; 16] = kani::any();

            let m = dummy_mcx();
            let bdesc = mk_bdesc(m, UUIDOID, 16, false, FAM_UUID);
            let mut col_a = brin_values(
                false,
                Datum::from_usize(a_min.as_ptr() as usize),
                Datum::from_usize(a_max.as_ptr() as usize),
            );
            let col_b = brin_values(
                false,
                Datum::from_usize(b_min.as_ptr() as usize),
                Datum::from_usize(b_max.as_ptr() as usize),
            );
            ok(brin_minmax::brin_minmax_union(m, &bdesc, 0, &mut col_a, &col_b));

            let (mut c_min, mut c_max): (usize, usize) = (0, 0);
            unsafe {
                pg_run_minmax_union(
                    UUIDOID, 16, 0, FAM_UUID,
                    a_min.as_ptr() as usize, a_max.as_ptr() as usize,
                    b_min.as_ptr() as usize, b_max.as_ptr() as usize, 0,
                    &mut c_min, &mut c_max,
                )
            };
            assert!(read16_c(col_a.bv_values[0].as_usize()) == read16_c(c_min));
            assert!(read16_c(col_a.bv_values[1].as_usize()) == read16_c(c_max));
            core::mem::forget(col_a);
            core::mem::forget(col_b);
            core::mem::forget(bdesc);
        }
    }

    // ---------- opcinfo: pure metadata, full symbolic typoid ----------

    #[kani::proof]
    fn eq_opcinfo() {
        let typoid: u32 = kani::any();
        let r = brin_minmax::brin_minmax_opcinfo(typoid);

        let (mut ns, mut rn, mut t0, mut t1) = (0u16, 0i32, 0u32, 0u32);
        unsafe { pg_run_minmax_opcinfo(typoid, &mut ns, &mut rn, &mut t0, &mut t1) };

        assert!(r.oi_nstored == ns);
        assert!(r.oi_regular_nulls as i32 == rn);
        assert!(r.oi_typids[0] == t0);
        assert!(r.oi_typids[1] == t1);
        assert!(matches!(r.kind, types_brin::BrinOpcKind::MinMax));
        core::mem::forget(r);
    }

    // ---------- negative control (MUST FAIL; run with the DEFAULT solver:
    // kissat never terminates on failing harnesses) ----------

    /// C-side catalog model skewed (strategy-1 proc dispatches <=), Rust side
    /// canonical — the counterexample newval == min == max (C: le(0,0) reports
    /// "updated", Rust: lt(0,0) does not) proves the shared comparator-seam
    /// model is load-bearing and the rig non-vacuous. Inputs are CONCRETE so
    /// the refutation is solver-trivial (a fully symbolic failing harness
    /// OOMs CaDiCaL and never terminates under kissat).
    // seam_stubs_byval (not bare seam_stubs): the control is int4 add_value
    // shaped, and without the byval datum_copy stub the shipped fn's dead
    // varlena-sizing branch inflates the CNF past the memory protocol
    // (measured 22 GiB rss-kill). Same stub set as the harness it refutes.
    seam_stubs_byval! {
        fn control_seam_skew() {
            unsafe { pg_seam_skew = 1 };
            let newval: i32 = 0;
            let min: i32 = 0;
            let max: i32 = 0;

            let m = dummy_mcx();
            let bdesc = mk_bdesc(m, INT4OID, 4, true, FAM_INT4);
            let mut col = brin_values(false, Datum::from_i32(min), Datum::from_i32(max));
            let r = ok(brin_minmax::brin_minmax_add_value(
                m, &bdesc, &mut col, Datum::from_i32(newval), false, 0,
            ));

            let (mut c_min, mut c_max, mut c_all): (usize, usize, i32) = (0, 0, 0);
            let c = unsafe {
                pg_run_minmax_add_value(
                    INT4OID, 4, 1, FAM_INT4, 0,
                    Datum::from_i32(min).as_usize(),
                    Datum::from_i32(max).as_usize(),
                    Datum::from_i32(newval).as_usize(), 0,
                    &mut c_min, &mut c_max, &mut c_all,
                )
            };
            assert!(r as i32 == c);
            core::mem::forget(col);
            core::mem::forget(bdesc);
        }
    }
}

// ===========================================================================
// brin_minmax_multi DISTANCE lane (tail-triage group 2, 2026-07-28/29)
// ===========================================================================
//
// Rust side (shipped, path-dep): brin_minmax_multi/src/builtins.rs
// fc_dist_* / fc_summary_{in,recv}, reached through the PUBLIC
// MINMAX_MULTI_BUILTINS registration table via a COMPILE-TIME oid lookup —
// no shipped-code visibility edits, and the oid -> function wiring of the
// dispatch table is itself pinned by every harness. Frames are real
// LocalFcinfo<N> (proof_support::call*), so datum unwrap -> core -> f64
// Datum pack is inside each theorem.
//
// C side: c/pg_brin_multi_dist.c — verbatim REL_18_STABLE
// brin_minmax_multi.c distance/summary bodies (see its header for every
// shim). SEPARATE c-lib from pg_brin_minmax.c: run distance harnesses with
// `--c-lib c/pg_brin_multi_dist.c` ONLY (mbconv law: whole-family linking
// fakes solver walls); the seam harnesses above keep pg_brin_minmax.c.
//
// Ledger rows: 4621-4625 (int2/int4/int8/float4/float8), 4627 tid,
// 4628 uuid, 4629 date, 4630 time, 4632 timetz, 4633 pg_lsn, 4634 macaddr,
// 4635 macaddr8, 4636 inet, 4637 timestamp, 4638 summary_in,
// 4640 summary_recv. (4626 numeric / 4631 interval / 4639 summary_out stay
// excluded per ledger; 4641 summary_send skipped — byteasend delegation,
// bytea-family rig.)
//
// DIVIDER SCREEN (band-immune refinement checked FIRST, per charter): every
// /256 in this family divides by a LITERAL POWER OF TWO — exponent-only,
// no significand divider circuit (geo-cmp law: pow2 divides are FULL-DOMAIN
// provable). The uuid/macaddr/macaddr8/inet chains therefore go straight to
// fully-symbolic harnesses (loop-free in C for macaddr; fixed 16-iter loops
// elsewhere, exact-fit unwinds). If a chain still walls, the runqueue names
// the fallback (per-byte-prefix cells + native differential), NOT a
// magnitude ladder — assumes cannot shrink a loop-free circuit.
//
// FENCES (= the C caller contract; C Asserts are compiled out, the shipped
// Rust keeps them as LIVE debug_assert!s under Kani):
//   - float4/float8: assume(one-side NaN or a1 <= a2)  [Assert(a1 <= a2)]
//   - uuid/macaddr/macaddr8: assume lexicographic a <= b [Assert(delta>=0)]
//   - inet: assume masked-prefix a <= b (fence predicate computed in the
//     harness, mirroring the body's masking; fence-only, never the oracle)
//   - timetz: assume |zone| <= 86400 per side (superset of any real
//     displacement, tz-seam wording). The zone-wrap plane OUTSIDE the fence
//     is a REAL structural difference (C subtracts zones in int32 before
//     widening; Rust widens first) — probe_dist_timetz_zone_wrap documents
//     it as an EXPECTED-FAIL witness; unreachable from validated timetz
//     input (timetz_in bounds the zone), adjudicate before any recording.
//
// NAN screen: no vendored section reaches the NAN macro/get_float8_nan —
// canonical-NAN shim NOT needed (documented in the C header).
//
// RUN (from proofs/brin-minmax/): see runqueue.txt. Family flags:
//   timeout 75 cargo kani -Z c-ffi -Z stubbing \
//     --c-lib c/pg_brin_multi_dist.c --no-overflow-checks \
//     --harness <h> --exact [--solver kissat]
// --no-overflow-checks is REQUIRED family-wide: (a) float rule (NaN
// production checks are property noise), (b) the time/timetz/pg_lsn
// subtractions wrap by design (C compiles with -fwrapv; shipped Rust release
// wraps) — overflow assertions there are not parity.

#[cfg(kani)]
mod dist_proofs {
    use datum::Datum;
    use proof_support::{call, call2, stubs};
    use types_fmgr::PGFunction;

    extern "C" {
        fn pg_dist_float4(a1: f32, a2: f32) -> f64;
        fn pg_dist_float8(a1: f64, a2: f64) -> f64;
        fn pg_dist_int2(a1: i16, a2: i16) -> f64;
        fn pg_dist_int4(a1: i32, a2: i32) -> f64;
        fn pg_dist_int8(a1: i64, a2: i64) -> f64;
        fn pg_dist_tid(hi1: u16, lo1: u16, pos1: u16, hi2: u16, lo2: u16, pos2: u16) -> f64;
        fn pg_dist_uuid(d1: *const u8, d2: *const u8) -> f64;
        fn pg_dist_date(a1: i32, a2: i32) -> f64;
        fn pg_dist_time(a1: i64, a2: i64) -> f64;
        fn pg_dist_timetz(ta_time: i64, ta_zone: i32, tb_time: i64, tb_zone: i32) -> f64;
        fn pg_dist_timestamp(a1: i64, a2: i64) -> f64;
        fn pg_dist_pg_lsn(a1: u64, a2: u64) -> f64;
        fn pg_dist_macaddr(a: *const u8, b: *const u8) -> f64;
        fn pg_dist_macaddr8(a: *const u8, b: *const u8) -> f64;
        fn pg_dist_inet(
            fam_a: u8,
            bits_a: u8,
            addr_a: *const u8,
            fam_b: u8,
            bits_b: u8,
            addr_b: *const u8,
        ) -> f64;
        fn pg_max_heap_tuples_per_page() -> std::os::raw::c_int;
        fn pg_summary_in() -> std::os::raw::c_int;
        fn pg_summary_recv() -> std::os::raw::c_int;
        fn pg_dist_errcode_get() -> i32;
        // negative control only — NOT postgres code
        fn pg_dist_int4_wrong(a1: i32, a2: i32) -> f64;
    }

    /// Compile-time oid -> shipped fc_* lookup through the PUBLIC
    /// registration table: the dispatch wiring is part of every theorem and
    /// no shipped code needs a visibility edit.
    const fn builtin(oid: u32) -> PGFunction {
        let t = brin_minmax_multi::MINMAX_MULTI_BUILTINS;
        let mut i = 0;
        while i < t.len() {
            if t[i].foid == oid {
                return t[i].func;
            }
            i += 1;
        }
        panic!("oid not registered in MINMAX_MULTI_BUILTINS")
    }

    const FC_DIST_INT2: PGFunction = builtin(4621);
    const FC_DIST_INT4: PGFunction = builtin(4622);
    const FC_DIST_INT8: PGFunction = builtin(4623);
    const FC_DIST_FLOAT4: PGFunction = builtin(4624);
    const FC_DIST_FLOAT8: PGFunction = builtin(4625);
    const FC_DIST_TID: PGFunction = builtin(4627);
    const FC_DIST_UUID: PGFunction = builtin(4628);
    const FC_DIST_DATE: PGFunction = builtin(4629);
    const FC_DIST_TIME: PGFunction = builtin(4630);
    const FC_DIST_TIMETZ: PGFunction = builtin(4632);
    const FC_DIST_PG_LSN: PGFunction = builtin(4633);
    const FC_DIST_MACADDR: PGFunction = builtin(4634);
    const FC_DIST_MACADDR8: PGFunction = builtin(4635);
    const FC_DIST_INET: PGFunction = builtin(4636);
    const FC_DIST_TIMESTAMP: PGFunction = builtin(4637);
    const FC_SUMMARY_IN: PGFunction = builtin(4638);
    const FC_SUMMARY_RECV: PGFunction = builtin(4640);

    /// Distance wrappers are infallible on every harnessed plane (the one
    /// fallible seam, inet detoast, is pruned by literal 4B headers). Err is
    /// forgotten (Box drop-glue trap) and fails loudly.
    fn ok(r: Result<Datum, Box<types_error::PgError>>) -> Datum {
        match r {
            Ok(d) => d,
            Err(e) => {
                core::mem::forget(e);
                panic!("distance wrapper errored")
            }
        }
    }

    fn any_f64() -> f64 {
        f64::from_bits(kani::any())
    }
    fn any_f32() -> f32 {
        f32::from_bits(kani::any())
    }

    /// Lexicographic byte-order fence (the C caller contract the shipped
    /// debug_assert!(delta >= 0) encodes). Fence-only, never the oracle.
    fn lex_le(a: &[u8], b: &[u8]) -> bool {
        let mut i = 0;
        while i < a.len() {
            if a[i] != b[i] {
                return a[i] < b[i];
            }
            i += 1;
        }
        true
    }

    // ---------- scalar subtract-then-(or after)-widen distances ----------

    /// int2/int4/int8/date/timestamp: C widens EACH side to double then
    /// subtracts; time/pg_lsn: C subtracts in the integer domain (wrapping)
    /// then converts once — the shipped Rust mirrors each order-of-ops
    /// exactly and the theorems pin it bit-exactly over the full domain.
    macro_rules! scalar_dist {
        ($($h:ident: $fc:ident / $pg:ident, $ty:ty, $ctor:ident;)*) => {$(
            #[kani::proof]
            fn $h() {
                let a: $ty = kani::any();
                let b: $ty = kani::any();
                let r = ok(call2($fc, Datum::$ctor(a), Datum::$ctor(b)));
                let c = unsafe { $pg(a, b) };
                assert!(r.as_f64().to_bits() == c.to_bits());
            }
        )*};
    }

    scalar_dist! {
        eq_dist_int2: FC_DIST_INT2 / pg_dist_int2, i16, from_i16;
        eq_dist_int4: FC_DIST_INT4 / pg_dist_int4, i32, from_i32;
        eq_dist_int8: FC_DIST_INT8 / pg_dist_int8, i64, from_i64;
        eq_dist_date: FC_DIST_DATE / pg_dist_date, i32, from_i32;
        eq_dist_timestamp: FC_DIST_TIMESTAMP / pg_dist_timestamp, i64, from_i64;
        eq_dist_pg_lsn: FC_DIST_PG_LSN / pg_dist_pg_lsn, u64, from_u64;
    }

    /// Validated TimeADT domain (time_in/time_recv bound to [0, 24h]).
    const USECS_PER_DAY: i64 = 86_400_000_000;

    /// dist_time, CONTRACT domain: both times valid TimeADT. The i64
    /// subtract cannot wrap here. The out-of-contract wrap plane (C -fwrapv
    /// == shipped release wrap) is Kani-unprovable through shipped code —
    /// Kani compiles debug overflow checks in, so full-i64 FAILs there are
    /// check artifacts, not value divergences (datetime wave-7 lesson) —
    /// and is covered by the native differential
    /// tests/dist_wrap_native.rs instead.
    #[kani::proof]
    fn eq_dist_time() {
        let a: i64 = kani::any();
        let b: i64 = kani::any();
        kani::assume(a >= 0 && a <= USECS_PER_DAY);
        kani::assume(b >= 0 && b <= USECS_PER_DAY);
        let r = ok(call2(FC_DIST_TIME, Datum::from_i64(a), Datum::from_i64(b)));
        let c = unsafe { pg_dist_time(a, b) };
        assert!(r.as_f64().to_bits() == c.to_bits());
    }

    // ---------- float distances (NaN lattice + contract fence) ----------

    /// Fence = the C caller contract (Assert(a1 <= a2), compiled out in C,
    /// live debug_assert! in shipped Rust); the NaN arms sit BEFORE the
    /// assert on both sides and are fully in-theorem (every payload).
    #[kani::proof]
    fn eq_dist_float4() {
        let a = any_f32();
        let b = any_f32();
        kani::assume(a.is_nan() || b.is_nan() || a <= b);
        let r = ok(call2(FC_DIST_FLOAT4, Datum::from_u32(a.to_bits()), Datum::from_u32(b.to_bits())));
        let c = unsafe { pg_dist_float4(a, b) };
        assert!(r.as_f64().to_bits() == c.to_bits());
        kani::cover!(a.is_nan() && b.is_nan());
        kani::cover!(a.is_nan() != b.is_nan());
        kani::cover!(!a.is_nan() && !b.is_nan());
    }

    #[kani::proof]
    fn eq_dist_float8() {
        let a = any_f64();
        let b = any_f64();
        kani::assume(a.is_nan() || b.is_nan() || a <= b);
        let r = ok(call2(FC_DIST_FLOAT8, Datum::from_f64(a), Datum::from_f64(b)));
        let c = unsafe { pg_dist_float8(a, b) };
        assert!(r.as_f64().to_bits() == c.to_bits());
        kani::cover!(a.is_nan() && b.is_nan());
        kani::cover!(a.is_nan() != b.is_nan());
        kani::cover!(!a.is_nan() && !b.is_nan());
    }

    // ---------- tid (uint32-wrap comment claim + constant wiring) ----------

    #[kani::proof]
    fn eq_dist_tid() {
        // the two constants must agree or the whole theorem is against the
        // wrong page geometry (wiring assert, both are compile-time)
        assert!(
            unsafe { pg_max_heap_tuples_per_page() }
                == types_storage::bufpage::MaxHeapTuplesPerPage as i32
        );

        let (hi1, lo1, pos1): (u16, u16, u16) = (kani::any(), kani::any(), kani::any());
        let (hi2, lo2, pos2): (u16, u16, u16) = (kani::any(), kani::any(), kani::any());

        // on-disk ItemPointerData image: bi_hi, bi_lo, ip_posid (ne bytes)
        let mut t1 = [0u8; 6];
        t1[0..2].copy_from_slice(&hi1.to_ne_bytes());
        t1[2..4].copy_from_slice(&lo1.to_ne_bytes());
        t1[4..6].copy_from_slice(&pos1.to_ne_bytes());
        let mut t2 = [0u8; 6];
        t2[0..2].copy_from_slice(&hi2.to_ne_bytes());
        t2[2..4].copy_from_slice(&lo2.to_ne_bytes());
        t2[4..6].copy_from_slice(&pos2.to_ne_bytes());

        // no fence: neither side asserts order here (NoCheck accessors; the
        // uint32 block*MaxHeapTuplesPerPage+off wrap is the comment claim
        // under proof, full domain)
        let r = ok(call2(
            FC_DIST_TID,
            Datum::from_usize(t1.as_ptr() as usize),
            Datum::from_usize(t2.as_ptr() as usize),
        ));
        let c = unsafe { pg_dist_tid(hi1, lo1, pos1, hi2, lo2, pos2) };
        assert!(r.as_f64().to_bits() == c.to_bits());
    }

    // ---------- byte-chain distances (/256 = pow2, full-domain) ----------

    #[kani::proof]
    #[kani::unwind(18)] // exact fit: 16-byte loops + 1 (both sides + fence)
    fn eq_dist_uuid() {
        let d1: [u8; 16] = kani::any();
        let d2: [u8; 16] = kani::any();
        kani::assume(lex_le(&d1, &d2)); // C caller contract (uuid_le Assert)
        let r = ok(call2(
            FC_DIST_UUID,
            Datum::from_usize(d1.as_ptr() as usize),
            Datum::from_usize(d2.as_ptr() as usize),
        ));
        let c = unsafe { pg_dist_uuid(d1.as_ptr(), d2.as_ptr()) };
        assert!(r.as_f64().to_bits() == c.to_bits());
        kani::cover!(d1 == d2);
        kani::cover!(d1 != d2);
    }

    #[kani::proof]
    #[kani::unwind(8)] // exact fit: 6-byte loops + 1
    fn eq_dist_macaddr() {
        let a: [u8; 6] = kani::any();
        let b: [u8; 6] = kani::any();
        kani::assume(lex_le(&a, &b)); // C caller contract (delta >= 0)
        let r = ok(call2(
            FC_DIST_MACADDR,
            Datum::from_usize(a.as_ptr() as usize),
            Datum::from_usize(b.as_ptr() as usize),
        ));
        let c = unsafe { pg_dist_macaddr(a.as_ptr(), b.as_ptr()) };
        assert!(r.as_f64().to_bits() == c.to_bits());
    }

    #[kani::proof]
    #[kani::unwind(10)] // exact fit: 8-byte loops + 1
    fn eq_dist_macaddr8() {
        let a: [u8; 8] = kani::any();
        let b: [u8; 8] = kani::any();
        kani::assume(lex_le(&a, &b));
        let r = ok(call2(
            FC_DIST_MACADDR8,
            Datum::from_usize(a.as_ptr() as usize),
            Datum::from_usize(b.as_ptr() as usize),
        ));
        let c = unsafe { pg_dist_macaddr8(a.as_ptr(), b.as_ptr()) };
        assert!(r.as_f64().to_bits() == c.to_bits());
    }

    // ---------- timetz (order-of-ops parity + wrap-plane probe) ----------

    /// 12-byte on-disk TimeTzADT image {time: i64, zone: i32}, 8-aligned.
    #[repr(C, align(8))]
    struct TimeTzImg([u8; 12]);

    fn timetz_img(time: i64, zone: i32) -> TimeTzImg {
        let mut img = TimeTzImg([0u8; 12]);
        img.0[0..8].copy_from_slice(&time.to_ne_bytes());
        img.0[8..12].copy_from_slice(&zone.to_ne_bytes());
        img
    }

    #[kani::proof]
    fn eq_dist_timetz() {
        let ta_time: i64 = kani::any();
        let tb_time: i64 = kani::any();
        let ta_zone: i32 = kani::any();
        let tb_zone: i32 = kani::any();
        // zone fence: superset of any real tz displacement (timetz_in bounds
        // the zone); OUTSIDE this fence C's int32 zone subtraction wraps
        // where Rust widens first — see probe_dist_timetz_zone_wrap.
        kani::assume(ta_zone >= -86_400 && ta_zone <= 86_400);
        kani::assume(tb_zone >= -86_400 && tb_zone <= 86_400);
        // time fence: validated TimeADT domain — the shipped i64 add/sub
        // wraps in release exactly as C -fwrapv, but Kani's debug overflow
        // checks fire on the out-of-contract plane (check artifact, not a
        // value divergence); that plane lives in tests/dist_wrap_native.rs.
        kani::assume(ta_time >= 0 && ta_time <= USECS_PER_DAY);
        kani::assume(tb_time >= 0 && tb_time <= USECS_PER_DAY);
        let ia = timetz_img(ta_time, ta_zone);
        let ib = timetz_img(tb_time, tb_zone);
        let r = ok(call2(
            FC_DIST_TIMETZ,
            Datum::from_usize(ia.0.as_ptr() as usize),
            Datum::from_usize(ib.0.as_ptr() as usize),
        ));
        let c = unsafe { pg_dist_timetz(ta_time, ta_zone, tb_time, tb_zone) };
        assert!(r.as_f64().to_bits() == c.to_bits());
    }

    /// EXPECTED FAIL (divergence witness, DEFAULT solver): on the zone-wrap
    /// plane (zone diff outside i32) C subtracts zones in int32 (wraps,
    /// -fwrapv) BEFORE the int64 multiply while shipped Rust widens each
    /// zone to i64 first. Unreachable from validated timetz values —
    /// ratified-unreachable candidate, ground-truth (docker postgres:18)
    /// before ANY ledger recording. A PASS here would mean the plane closed
    /// (re-examine both sides).
    #[kani::proof]
    fn probe_dist_timetz_zone_wrap() {
        let ta_zone: i32 = kani::any();
        let tb_zone: i32 = kani::any();
        let ia = timetz_img(0, ta_zone);
        let ib = timetz_img(0, tb_zone);
        let r = ok(call2(
            FC_DIST_TIMETZ,
            Datum::from_usize(ia.0.as_ptr() as usize),
            Datum::from_usize(ib.0.as_ptr() as usize),
        ));
        let c = unsafe { pg_dist_timetz(0, ta_zone, 0, tb_zone) };
        assert!(r.as_f64().to_bits() == c.to_bits());
    }

    // ---------- inet (per-family cells) ----------

    /// 4B-uncompressed varlena inet image: header u32 = (4 + payload) << 2
    /// (LITERAL — prunes arg_varlena_packed's detoast branch, the wrapper's
    /// only fallible seam), payload = [family, bits, ipaddr[len]]. Unused
    /// address bytes stay LITERAL zero (dead-symbolic-bytes law).
    #[repr(C, align(8))]
    struct InetImg([u8; 24]);

    fn inet_img<const LEN: usize>(family: u8, bits: u8, addr: &[u8; LEN]) -> InetImg {
        let mut img = InetImg([0u8; 24]);
        let total: u32 = (4 + 2 + LEN) as u32;
        img.0[0..4].copy_from_slice(&(total << 2).to_ne_bytes());
        img.0[4] = family;
        img.0[5] = bits;
        img.0[6..6 + LEN].copy_from_slice(addr);
        img
    }

    /// Mirror of the body's masking, used ONLY to state the caller-contract
    /// fence (masked first-addresses sorted); never the oracle.
    fn masked<const LEN: usize>(addr: &[u8; LEN], bits: u8) -> [u8; LEN] {
        let mut out = *addr;
        let mut i = 0;
        while i < LEN {
            let nbits = (bits as i32 - (i as i32) * 8).max(0);
            if nbits < 8 {
                out[i] &= (0xFFu32 << (8 - nbits)) as u8;
            }
            i += 1;
        }
        out
    }

    macro_rules! inet_family_harness {
        ($h:ident, $len:literal, $fam:literal) => {
            #[kani::proof]
            #[kani::unwind(18)] // C struct-fill loops are 16 iters even for v4
            fn $h() {
                let addr_a: [u8; $len] = kani::any();
                let addr_b: [u8; $len] = kani::any();
                let bits_a: u8 = kani::any();
                let bits_b: u8 = kani::any();
                // C caller contract: BRIN sorts by masked first address —
                // fences the shipped debug_assert!(0.0..=1.0 contains delta)
                kani::assume(lex_le(&masked(&addr_a, bits_a), &masked(&addr_b, bits_b)));

                let ia = inet_img::<$len>($fam, bits_a, &addr_a);
                let ib = inet_img::<$len>($fam, bits_b, &addr_b);
                let mut ca = [0u8; 16];
                let mut cb = [0u8; 16];
                ca[..$len].copy_from_slice(&addr_a);
                cb[..$len].copy_from_slice(&addr_b);

                let r = ok(call2(
                    FC_DIST_INET,
                    Datum::from_usize(ia.0.as_ptr() as usize),
                    Datum::from_usize(ib.0.as_ptr() as usize),
                ));
                let c = unsafe {
                    pg_dist_inet($fam, bits_a, ca.as_ptr(), $fam, bits_b, cb.as_ptr())
                };
                assert!(r.as_f64().to_bits() == c.to_bits());
            }
        };
    }

    inet_family_harness!(eq_dist_inet_v4, 4, 2u8);
    inet_family_harness!(eq_dist_inet_v6, 16, 3u8);

    /// family-mismatch arm: constant 1.0 both sides, address bytes literal
    /// zero (unread past the family check).
    #[kani::proof]
    #[kani::unwind(18)]
    fn eq_dist_inet_mismatch() {
        let bits_a: u8 = kani::any();
        let bits_b: u8 = kani::any();
        let zero4 = [0u8; 4];
        let zero16 = [0u8; 16];
        let ia = inet_img::<4>(2, bits_a, &zero4);
        let ib = inet_img::<16>(3, bits_b, &zero16);
        let r = ok(call2(
            FC_DIST_INET,
            Datum::from_usize(ia.0.as_ptr() as usize),
            Datum::from_usize(ib.0.as_ptr() as usize),
        ));
        let c = unsafe { pg_dist_inet(2, bits_a, zero16.as_ptr(), 3, bits_b, zero16.as_ptr()) };
        assert!(r.as_f64().to_bits() == c.to_bits());
        assert!(c.to_bits() == 1.0f64.to_bits());
    }

    // ---------- summary_in / summary_recv (error-class-only rows) ----------

    /// Verdict + sqlstate + level parity: shipped .with_sqlstate stays
    /// load-bearing over the stubbed constructor (cash/float-arith
    /// precedent); C's MAKE_SQLSTATE int is bit-compatible with pgrust's
    /// SqlState(i32) (same sixbit encoder, machine-checked here).
    macro_rules! summary_reject_harness {
        ($h:ident, $fc:ident, $pg:ident) => {
            #[kani::proof]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            fn $h() {
                let c = unsafe { $pg() };
                let c_code = unsafe { pg_dist_errcode_get() };
                let r = call::<1, _>($fc, [Datum::from_usize(0)]);
                match r {
                    Ok(_) => panic!("summary reject wrapper returned Ok"),
                    Err(e) => {
                        assert!(c == 1);
                        assert!(e.level == types_error::ERROR);
                        assert!(e.sqlstate.0 == c_code);
                        core::mem::forget(e);
                    }
                }
            }
        };
    }

    summary_reject_harness!(eq_summary_in, FC_SUMMARY_IN, pg_summary_in);
    summary_reject_harness!(eq_summary_recv, FC_SUMMARY_RECV, pg_summary_recv);

    // ---------- negative control (MUST FAIL; DEFAULT solver) ----------

    /// Shipped fc_dist_int4 vs a C body with the subtraction order swapped —
    /// decodable counterexample at any a1 != a2 (rig non-vacuity for the
    /// distance lane; the seam lane keeps its own control_seam_skew).
    #[kani::proof]
    fn control_dist_int4_swapped() {
        let a: i32 = kani::any();
        let b: i32 = kani::any();
        let r = ok(call2(FC_DIST_INT4, Datum::from_i32(a), Datum::from_i32(b)));
        let c = unsafe { pg_dist_int4_wrong(a, b) };
        assert!(r.as_f64().to_bits() == c.to_bits());
    }
}
