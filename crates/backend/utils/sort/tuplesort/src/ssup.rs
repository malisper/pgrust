use ::datum::Datum;
use ::mcx::Mcx;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{direct_function_call2_coll_in, PGFunction};
use ::lsyscache::COMPARE_GT;
use ::types_nbtree::{BTORDER_PROC, BTSORTSUPPORT_PROC};

// pg_proc.dat oids for the sortsupport routines with a live comparator arm.
const F_BTINT2SORTSUPPORT: Oid = 3129;
const F_BTINT4SORTSUPPORT: Oid = 3130;
const F_BTOIDSORTSUPPORT: Oid = 3134;
const F_BTINT8SORTSUPPORT: Oid = 3131;
const F_DATE_SORTSUPPORT: Oid = 3136;
const F_TIMESTAMP_SORTSUPPORT: Oid = 3137;
const F_BTTEXTSORTSUPPORT: Oid = 3255;
const F_UUID_SORTSUPPORT: Oid = 3300;
const F_NETWORK_SORTSUPPORT: Oid = 5033;
const F_BTOIDSORTSUPPORT: Oid = 3134;

/// C's `ssup->comparator` fn pointer as a closed enum: identity is switchable
/// (tuplesort_sort_memtuples specialization dispatch) and calls monomorphize.
#[derive(Clone, Copy, Debug)]
pub enum SortComparator {
    /// `ssup_datum_unsigned_cmp` (abbreviated-key comparisons).
    Unsigned,
    /// `ssup_datum_signed_cmp` (btint8/timestamp sortsupport).
    SignedI64,
    /// `ssup_datum_int32_cmp` (btint4/date sortsupport).
    Int32,
    /// `btint2fastcmp` (btint2sortsupport).
    Int16,
    /// `btoidfastcmp` (btoidsortsupport): unsigned 32-bit.
    Uint32,
    /// `varstrfastcmp_c`, no abbreviation (bttextsortsupport, collate-is-C
    /// only); datums must point at live untoasted varlenas.
    TextC,
    /// `PrepareSortSupportComparisonShim`: the opfamily's BTORDER_PROC,
    /// resolved once, invoked per comparison (C builds an fcinfo per call
    /// too). Needs an mcx-threaded apply; the mcx-less lane panics.
    Shim(ShimCmp),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShimCmp {
    pub fn_addr: PGFunction,
    pub fn_oid: Oid,
}

#[derive(Clone, Copy, Debug)]
pub struct SortSupport {
    pub ssup_collation: Oid,
    pub ssup_reverse: bool,
    pub ssup_nulls_first: bool,
    pub ssup_attno: i16,
    pub comparator: SortComparator,
}

#[inline(always)]
pub fn apply_cmp(cmp: SortComparator, x: Datum, y: Datum) -> i32 {
    match cmp {
        SortComparator::Unsigned => {
            let (x, y) = (x.as_u64(), y.as_u64());
            (x > y) as i32 - (x < y) as i32
        }
        SortComparator::SignedI64 => {
            let (x, y) = (x.as_i64(), y.as_i64());
            (x > y) as i32 - (x < y) as i32
        }
        SortComparator::Int32 => {
            let (x, y) = (x.as_i32(), y.as_i32());
            (x > y) as i32 - (x < y) as i32
        }
        SortComparator::Int16 => {
            let (x, y) = (x.as_i16(), y.as_i16());
            (x > y) as i32 - (x < y) as i32
        }
        SortComparator::Uint32 => {
            let (x, y) = (x.as_u32(), y.as_u32());
            (x > y) as i32 - (x < y) as i32
        }
        // SAFETY: TextC contract (enum doc) — both datums are live untoasted
        // varlena pointers owned by the sort's tuplecontext.
        SortComparator::TextC => unsafe {
            varlena::varstrfastcmp_c(varlena_payload(x), varlena_payload(y))
        },
        SortComparator::Shim(shim) => panic!(
            "comparison shim (proc {}) reached an mcx-less comparator lane \
             (merge join over shim-compared types not ported)",
            shim.fn_oid
        ),
    }
}

#[inline(always)]
pub fn apply_cmp_in(cmp: SortComparator, x: Datum, y: Datum, collation: Oid, mcx: Mcx<'_>) -> i32 {
    match cmp {
        SortComparator::Shim(shim) => {
            match direct_function_call2_coll_in(shim.fn_addr, collation, mcx, x, y) {
                Ok(d) => d.as_i32(),
                // The comparator sits under infallible qsort plumbing; a
                // failing btree proc is C's ereport-out-of-sort, surfaced
                // here as a panic.
                Err(e) => panic!(
                    "sort comparison proc {} failed: {}",
                    shim.fn_oid,
                    e.message()
                ),
            }
        }
        other => apply_cmp(other, x, y),
    }
}

/// # Safety
/// `d` points at a live untoasted varlena (short 1B or full 4B header).
#[inline]
unsafe fn varlena_payload<'a>(d: Datum) -> &'a [u8] {
    use ::types_tuple::varatt::{varatt_is_1b, varatt_is_1b_e, varsize_1b, varsize_4b};
    let p = d.as_usize() as *const u8;
    debug_assert!(!varatt_is_1b_e(p));
    if varatt_is_1b(p) {
        core::slice::from_raw_parts(p.add(1), varsize_1b(p) - 1)
    } else {
        core::slice::from_raw_parts(p.add(4), varsize_4b(p) - 4)
    }
}

/// `ApplySortComparator` (sortsupport.h); `cmp` is passed separately so each
/// qsort specialization instantiates with a constant comparator, as C does.
#[inline(always)]
pub fn apply_sort_comparator_as(
    cmp: SortComparator,
    datum1: Datum,
    isnull1: bool,
    datum2: Datum,
    isnull2: bool,
    ssup: &SortSupport,
) -> i32 {
    if isnull1 {
        if isnull2 {
            0
        } else if ssup.ssup_nulls_first {
            -1
        } else {
            1
        }
    } else if isnull2 {
        if ssup.ssup_nulls_first {
            1
        } else {
            -1
        }
    } else {
        let compare = apply_cmp(cmp, datum1, datum2);
        if ssup.ssup_reverse {
            -compare
        } else {
            compare
        }
    }
}

#[inline(always)]
pub fn apply_sort_comparator(
    datum1: Datum,
    isnull1: bool,
    datum2: Datum,
    isnull2: bool,
    ssup: &SortSupport,
) -> i32 {
    apply_sort_comparator_as(ssup.comparator, datum1, isnull1, datum2, isnull2, ssup)
}

/// `ApplySortComparator` with the comparison-shim arm live (needs an mcx for
/// the shim'd proc's per-call allocations).
#[inline(always)]
pub fn apply_sort_comparator_as_in(
    cmp: SortComparator,
    mcx: Mcx<'_>,
    datum1: Datum,
    isnull1: bool,
    datum2: Datum,
    isnull2: bool,
    ssup: &SortSupport,
) -> i32 {
    if isnull1 {
        if isnull2 {
            0
        } else if ssup.ssup_nulls_first {
            -1
        } else {
            1
        }
    } else if isnull2 {
        if ssup.ssup_nulls_first {
            1
        } else {
            -1
        }
    } else {
        let compare = apply_cmp_in(cmp, datum1, datum2, ssup.ssup_collation, mcx);
        if ssup.ssup_reverse {
            -compare
        } else {
            compare
        }
    }
}

#[inline(always)]
pub fn apply_sort_comparator_in(
    mcx: Mcx<'_>,
    datum1: Datum,
    isnull1: bool,
    datum2: Datum,
    isnull2: bool,
    ssup: &SortSupport,
) -> i32 {
    apply_sort_comparator_as_in(ssup.comparator, mcx, datum1, isnull1, datum2, isnull2, ssup)
}

/// `PrepareSortSupportFromOrderingOp` (sortsupport.c). The comparator set is
/// the closed enum above; a sortsupport routine outside it, the btree-proc
/// shim, and abbreviated keys (bttextsortsupport et al.) all panic loudly.
pub fn prepare_sort_support_from_ordering_op(
    ordering_op: Oid,
    ssup: &SortSupportInit,
) -> PgResult<SortSupport> {
    let Some((opfamily, opcintype, cmptype)) =
        lsyscache::get_ordering_op_properties(ordering_op)?
    else {
        panic!("operator {ordering_op} is not a valid ordering operator");
    };
    let ssup_reverse = cmptype == COMPARE_GT;
    let comparator = comparator_for_opfamily(opfamily, opcintype, opcintype)?;

    Ok(SortSupport {
        ssup_collation: ssup.ssup_collation,
        ssup_reverse,
        ssup_nulls_first: ssup.ssup_nulls_first,
        ssup_attno: ssup.ssup_attno,
        comparator,
    })
}

/// The MJExamineQuals (nodeMergejoin.c) comparator resolve: BTSORTSUPPORT_PROC
/// for (lefttype,righttype), else the BTORDER_PROC shim — which, like every
/// out-of-enum sortsupport routine, panics loudly.
pub fn comparator_for_opfamily(
    opfamily: Oid,
    lefttype: Oid,
    righttype: Oid,
) -> PgResult<SortComparator> {
    let sort_support_function =
        lsyscache::get_opfamily_proc(opfamily, lefttype, righttype, BTSORTSUPPORT_PROC as i16)?;
    Ok(match sort_support_function {
        F_BTINT4SORTSUPPORT | F_DATE_SORTSUPPORT => SortComparator::Int32,
        // btoidfastcmp: unsigned; the zero-extended datum word compares exact.
        F_BTOIDSORTSUPPORT => SortComparator::Unsigned,
        F_BTINT8SORTSUPPORT | F_TIMESTAMP_SORTSUPPORT => SortComparator::SignedI64,
        // C DIVERGENCE: uuid 3300 / network 5033 name abbrev sortsupport
        // routines (unported); the shim on their BTORDER_PROC is
        // order-identical (CATALOG perf watch).
        0 | F_UUID_SORTSUPPORT | F_NETWORK_SORTSUPPORT => {
            // C: PrepareSortSupportComparisonShim — fmgr_info the BTORDER_PROC
            // once; comparisons go through the resolved fn pointer.
            let sort_function =
                lsyscache::get_opfamily_proc(opfamily, lefttype, righttype, BTORDER_PROC as i16)?;
            if sort_function == 0 {
                panic!(
                    "missing support function {}({lefttype},{righttype}) in opfamily {opfamily}",
                    BTORDER_PROC
                );
            }
            let flinfo = ::fmgr_seams::fmgr_info::call(sort_function)?;
            SortComparator::Shim(ShimCmp {
                fn_addr: flinfo.fn_addr,
                fn_oid: sort_function,
            })
        }
        other => panic!(
            "sortsupport routine {other} (opfamily {opfamily}) has no comparator arm; \
             abbreviated-key sortsupport (e.g. bttextsortsupport) not ported"
        ),
    })
}

/// The caller-filled prefix of C's zeroed SortSupportData.
pub struct SortSupportInit {
    pub ssup_collation: Oid,
    pub ssup_nulls_first: bool,
    pub ssup_attno: i16,
}

/// `PrepareSortSupportFromIndexRel` comparator resolve, btree arm; text keys
/// are C-collation only (varstr_sortsupport locale arms unported).
pub fn comparator_for_index_col(
    opfamily: Oid,
    opcintype: Oid,
    collation: Oid,
) -> PgResult<SortComparator> {
    let ssup_proc =
        lsyscache::get_opfamily_proc(opfamily, opcintype, opcintype, BTSORTSUPPORT_PROC as i16)?;
    Ok(match ssup_proc {
        F_BTINT4SORTSUPPORT | F_DATE_SORTSUPPORT => SortComparator::Int32,
        F_BTINT8SORTSUPPORT | F_TIMESTAMP_SORTSUPPORT => SortComparator::SignedI64,
        F_BTINT2SORTSUPPORT => SortComparator::Int16,
        F_BTOIDSORTSUPPORT => SortComparator::Uint32,
        F_BTTEXTSORTSUPPORT => {
            let locale = pg_locale::pg_newlocale_from_collation(collation)?;
            if !locale.collate_is_c {
                panic!(
                    "varstr_sortsupport (varlena.c): non-C collation {collation} \
                     index sort not ported"
                );
            }
            SortComparator::TextC
        }
        0 => {
            let sort_function =
                lsyscache::get_opfamily_proc(opfamily, opcintype, opcintype, BTORDER_PROC as i16)?;
            panic!(
                "PrepareSortSupportComparisonShim (sortsupport.c) not ported: \
                 btree comparison proc {sort_function} for opfamily {opfamily}"
            );
        }
        other => panic!(
            "sortsupport routine {other} (opfamily {opfamily}) has no comparator arm; \
             abbreviated-key sortsupport not ported"
        ),
    })
}
