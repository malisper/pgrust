//! Port of `src/backend/access/index/amvalidate.c` — support routines for index
//! access methods' `amvalidate` and `amadjustmembers` functions, the
//! signature-check / opclass-lookup library shared by every AM opclass
//! validator (nbtree / hash / GiST / GIN / SP-GiST / BRIN).
//!
//! `init_seams()` installs the six functions declared in
//! `backend-access-index-amvalidate-seams`, which the AM validators call.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use mcx::{vec_with_capacity_in, Mcx, PgVec};
use types_amvalidate::backend_access_index_amvalidate::{
    AmopRow, AmprocRow, OpFamilyOpFuncGroup,
};
use types_core::primitive::OidIsValid;
use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult};

use backend_access_index_amvalidate_seams as sx;
use backend_parser_coerce_seams as coerce;
use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_utils_cache_syscache_seams as syscache;

/// `BTREE_AM_OID` (catalog/pg_am.dat) — the built-in btree access method.
const BTREE_AM_OID: Oid = 403;
/// `VOIDOID` (catalog/pg_type.dat).
const VOIDOID: Oid = 2278;
/// `INTERNALOID` (catalog/pg_type.dat).
const INTERNALOID: Oid = 2281;

/// Install every seam declared in `backend-access-index-amvalidate-seams`.
pub fn init_seams() {
    sx::identify_opfamily_groups::set(identify_opfamily_groups);
    sx::check_amproc_signature::set(check_amproc_signature);
    sx::check_amoptsproc_signature::set(check_amoptsproc_signature);
    sx::check_amop_signature::set(check_amop_signature);
    sx::opclass_for_family_datatype::set(opclass_for_family_datatype);
    sx::opfamily_can_sort_type::set(opfamily_can_sort_type);
}

/// `identify_opfamily_groups(oprlist, proclist)` (amvalidate.c) — return a list
/// of [`OpFamilyOpFuncGroup`]s, one for each lefttype/righttype combination
/// present in the family's operator and support-function lists. If amopstrategy
/// K is present for this datatype combination, set bit `1 << K` in
/// `operatorset`, and similarly for the support functions.
///
/// The given lists are expected in datatype order (the C `CatCList->ordered`
/// invariant from the AMOPSTRATEGY / AMPROCNUM caches); the syscache list
/// projections preserve that order, so the C `!ordered` error path
/// (`cannot validate operator family without ordered data`) cannot arise here.
fn identify_opfamily_groups<'mcx>(
    mcx: Mcx<'mcx>,
    oprlist: &[AmopRow],
    proclist: &[AmprocRow],
) -> PgResult<PgVec<'mcx, OpFamilyOpFuncGroup>> {
    let mut result: PgVec<'mcx, OpFamilyOpFuncGroup> = vec_with_capacity_in(mcx, 0)?;

    // Advance through the lists concurrently. Thanks to the ordering, we should
    // see all operators and functions of a given datatype pair consecutively.
    let mut io = 0usize;
    let mut ip = 0usize;
    let mut oprform: Option<AmopRow> = if io < oprlist.len() {
        let r = oprlist[io];
        io += 1;
        Some(r)
    } else {
        None
    };
    let mut procform: Option<AmprocRow> = if ip < proclist.len() {
        let r = proclist[ip];
        ip += 1;
        Some(r)
    } else {
        None
    };

    // `thisgroup` is the index of the current group in `result` (the C keeps a
    // pointer; here the last-appended element).
    let mut thisgroup: Option<usize> = None;

    while oprform.is_some() || procform.is_some() {
        if let (Some(opr), Some(gi)) = (oprform, thisgroup) {
            if opr.amoplefttype == result[gi].lefttype
                && opr.amoprighttype == result[gi].righttype
            {
                // Operator belongs to current group; include it and advance.
                // Ignore strategy numbers outside supported range.
                if opr.amopstrategy > 0 && opr.amopstrategy < 64 {
                    result[gi].operatorset |= 1u64 << opr.amopstrategy;
                }
                oprform = if io < oprlist.len() {
                    let r = oprlist[io];
                    io += 1;
                    Some(r)
                } else {
                    None
                };
                continue;
            }
        }

        if let (Some(proc), Some(gi)) = (procform, thisgroup) {
            if proc.amproclefttype == result[gi].lefttype
                && proc.amprocrighttype == result[gi].righttype
            {
                // Procedure belongs to current group; include it and advance.
                // Ignore function numbers outside supported range.
                if proc.amprocnum > 0 && proc.amprocnum < 64 {
                    result[gi].functionset |= 1u64 << proc.amprocnum;
                }
                procform = if ip < proclist.len() {
                    let r = proclist[ip];
                    ip += 1;
                    Some(r)
                } else {
                    None
                };
                continue;
            }
        }

        // Time for a new group.
        let (lefttype, righttype) = match (oprform, procform) {
            (Some(opr), None) => (opr.amoplefttype, opr.amoprighttype),
            (Some(opr), Some(proc))
                if opr.amoplefttype < proc.amproclefttype
                    || (opr.amoplefttype == proc.amproclefttype
                        && opr.amoprighttype < proc.amprocrighttype) =>
            {
                (opr.amoplefttype, opr.amoprighttype)
            }
            // The C dereferences `procform` here, which is non-NULL: the `while`
            // guard guarantees at least one of the two is `Some`, and we fell
            // through the `oprform`-wins branches above.
            (_, Some(proc)) => (proc.amproclefttype, proc.amprocrighttype),
            (None, None) => unreachable!("loop guard guarantees a non-empty side"),
        };
        result.try_reserve(1).map_err(|_| {
            mcx.oom(core::mem::size_of::<OpFamilyOpFuncGroup>())
        })?;
        result.push(OpFamilyOpFuncGroup {
            lefttype,
            righttype,
            operatorset: 0,
            functionset: 0,
        });
        thisgroup = Some(result.len() - 1);
    }

    Ok(result)
}

/// `check_amproc_signature(funcid, restype, exact, minargs, maxargs, ...)`
/// (amvalidate.c) — validate the signature (argument and result types) of an
/// opclass support function. `argtypes` carries the C variadic `maxargs`
/// argument-type OIDs. If `exact`, they must match the function arg types
/// exactly, else only binary-coercibly; the function result type must match
/// `restype` exactly.
fn check_amproc_signature(
    funcid: Oid,
    restype: Oid,
    exact: bool,
    minargs: i32,
    maxargs: i32,
    argtypes: &[Oid],
) -> PgResult<bool> {
    let mut result = true;

    // tp = SearchSysCache1(PROCOID, ...); the get_func_* lsyscache helpers raise
    // the C `cache lookup failed for function %u` on a miss.
    let prorettype = lsyscache::get_func_rettype::call(funcid)?;
    let proretset = lsyscache::get_func_retset::call(funcid)?;
    let pronargs = lsyscache::get_func_nargs::call(funcid)?;

    if prorettype != restype || proretset || pronargs < minargs || pronargs > maxargs {
        result = false;
    }

    // proargtypes.values[i] — the function's declared argument types.
    let scratch = mcx::MemoryContext::new("check_amproc_signature argtypes");
    let proargtypes = lsyscache::get_func_signature::call(scratch.mcx(), funcid)?;

    for i in 0..(maxargs as usize) {
        let argtype = argtypes[i];
        if i >= pronargs as usize {
            continue;
        }
        let proargtype = proargtypes[i];
        let mismatch = if exact {
            argtype != proargtype
        } else {
            !coerce::is_binary_coercible::call(argtype, proargtype)?
        };
        if mismatch {
            result = false;
        }
    }

    Ok(result)
}

/// `check_amoptsproc_signature(funcid)` (amvalidate.c) — validate the signature
/// of an opclass options support function, which should be `void(internal)`.
fn check_amoptsproc_signature(funcid: Oid) -> PgResult<bool> {
    check_amproc_signature(funcid, VOIDOID, true, 1, 1, &[INTERNALOID])
}

/// `check_amop_signature(opno, restype, lefttype, righttype)` (amvalidate.c) —
/// validate the signature (argument and result types) of an opclass operator.
/// We hard-wire accepting only binary operators and insist on exact type
/// matches, since the given lefttype/righttype come from pg_amop.
fn check_amop_signature(
    opno: Oid,
    restype: Oid,
    lefttype: Oid,
    righttype: Oid,
) -> PgResult<bool> {
    let mut result = true;

    // tp = SearchSysCache1(OPEROID, ...); shouldn't be a miss in practice.
    let scratch = mcx::MemoryContext::new("check_amop_signature");
    let opform = match syscache::pg_operator_form::call(scratch.mcx(), opno)? {
        Some(f) => f,
        None => {
            return Err(PgError::error(format!(
                "cache lookup failed for operator {opno}"
            )));
        }
    };

    // 'b' (binary/infix).
    if opform.oprresult != restype
        || opform.oprkind != b'b' as i8
        || opform.oprleft != lefttype
        || opform.oprright != righttype
    {
        result = false;
    }

    Ok(result)
}

/// `opclass_for_family_datatype(amoid, opfamilyoid, datatypeoid)`
/// (amvalidate.c) — the OID of the opclass belonging to `opfamilyoid` and
/// accepting `datatypeoid` as input type, or `InvalidOid` if no such opclass.
///
/// We search through all the AM's opclasses
/// (`SearchSysCacheList1(CLAAMNAMENSP, amoid)`); inefficient but there is no
/// better index, and it saves an explicit check that the opfamily belongs to
/// the AM.
fn opclass_for_family_datatype(
    amoid: Oid,
    opfamilyoid: Oid,
    datatypeoid: Oid,
) -> PgResult<Oid> {
    let mut result = InvalidOid;

    let scratch = mcx::MemoryContext::new("opclass_for_family_datatype");
    let opclist = syscache::search_opclass_list_by_am::call(scratch.mcx(), amoid)?;

    for &(oid, opcfamily, opcintype) in opclist.iter() {
        if opcfamily == opfamilyoid && opcintype == datatypeoid {
            result = oid;
            break;
        }
    }

    Ok(result)
}

/// `opfamily_can_sort_type(opfamilyoid, datatypeoid)` (amvalidate.c) — is the
/// datatype a legitimate input type for the btree opfamily?
fn opfamily_can_sort_type(opfamilyoid: Oid, datatypeoid: Oid) -> PgResult<bool> {
    Ok(OidIsValid(opclass_for_family_datatype(
        BTREE_AM_OID,
        opfamilyoid,
        datatypeoid,
    )?))
}
