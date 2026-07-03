use ::datum::Datum;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::lsyscache::COMPARE_GT;
use ::types_nbtree::{BTORDER_PROC, BTSORTSUPPORT_PROC};

// pg_proc.dat oids for the sortsupport routines with a live comparator arm.
const F_BTINT4SORTSUPPORT: Oid = 3130;
const F_BTINT8SORTSUPPORT: Oid = 3131;
const F_DATE_SORTSUPPORT: Oid = 3136;
const F_TIMESTAMP_SORTSUPPORT: Oid = 3137;

/// C's `ssup->comparator` fn pointer as a closed enum: identity is switchable
/// (tuplesort_sort_memtuples specialization dispatch) and calls monomorphize.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SortComparator {
    /// `ssup_datum_unsigned_cmp` (abbreviated-key comparisons).
    Unsigned,
    /// `ssup_datum_signed_cmp` (btint8/timestamp sortsupport).
    SignedI64,
    /// `ssup_datum_int32_cmp` (btint4/date sortsupport).
    Int32,
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
        F_BTINT8SORTSUPPORT | F_TIMESTAMP_SORTSUPPORT => SortComparator::SignedI64,
        0 => {
            let sort_function =
                lsyscache::get_opfamily_proc(opfamily, lefttype, righttype, BTORDER_PROC as i16)?;
            panic!(
                "PrepareSortSupportComparisonShim (sortsupport.c) not ported: \
                 btree comparison proc {sort_function} for opfamily {opfamily}"
            );
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
