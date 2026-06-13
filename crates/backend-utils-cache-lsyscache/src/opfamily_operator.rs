//! `opfamily-operator` family — `lsyscache.c` lookups keyed on
//! `pg_operator` / `pg_amop` (operator metadata and opfamily membership).
//!
//! The `SearchSysCache*` probes bottom out in the syscache owner
//! (`backend-utils-cache-syscache`), reached through its per-owner seam crate
//! (`backend-utils-cache-syscache-seams`); the index-AM strategy translation
//! and `amcanorder` reads route through `backend-access-index-amapi-seams`.
//!
//! C entry points covered here: `get_op_opfamily_properties`,
//! `get_opfamily_member`, `get_ordering_op_properties`, `get_op_hash_functions`,
//! `op_input_types`, `op_strict`, `get_opcode`, `get_commutator`.

extern crate alloc;

use alloc::format;

use backend_access_index_amapi_seams as amapi;
use backend_utils_cache_syscache_seams as syscache;
use mcx::MemoryContext;
use types_core::Oid;
use types_error::{PgError, PgResult};
use types_hash::hash::{HASHSTANDARD_PROC, HTEqualStrategyNumber};
use types_opclass::AMOP_SEARCH;

/// `InvalidOid` (`postgres_ext.h`).
const INVALID_OID: Oid = 0;

/// Built-in index access-method OIDs (`catalog/pg_am.dat`), used by
/// `get_opmethod_canorder`'s hardcoded fast paths.
const BTREE_AM_OID: Oid = 403;
const HASH_AM_OID: Oid = 405;
const GIST_AM_OID: Oid = 783;
const GIN_AM_OID: Oid = 2742;
const SPGIST_AM_OID: Oid = 4000;
const BRIN_AM_OID: Oid = 3580;

/// `CompareType` values (`access/cmptype.h`) read off
/// `IndexAmTranslateStrategy`.
const COMPARE_INVALID: i32 = 0;
const COMPARE_LT: i32 = 1;
const COMPARE_EQ: i32 = 3;
const COMPARE_GT: i32 = 5;

/// `elog(ERROR, ...)` for the "should not happen" cache-lookup diagnostics.
fn elog(message: alloc::string::String) -> PgError {
    PgError::error(message)
}

/// `get_opcode(opno)` (lsyscache.c): the regproc id of the routine that
/// implements operator `opno`, or `InvalidOid` if no such operator.
pub fn get_opcode(opno: Oid) -> PgResult<Oid> {
    match syscache::oper_oprcode::call(opno)? {
        Some(oprcode) => Ok(oprcode),
        None => Ok(INVALID_OID),
    }
}

/// `op_input_types(opno, &lefttype, &righttype)` (lsyscache.c): the operator's
/// left and right input datatypes.
pub fn op_input_types(opno: Oid) -> PgResult<(Oid, Oid)> {
    match syscache::oper_input_types::call(opno)? {
        Some((lefttype, righttype)) => Ok((lefttype, righttype)),
        /* shouldn't happen */
        None => Err(elog(format!("cache lookup failed for operator {opno}"))),
    }
}

/// `func_strict(funcid)` (lsyscache.c): the `proisstrict` flag of the
/// function. Used here by `op_strict`.
fn func_strict(funcid: Oid) -> PgResult<bool> {
    match syscache::proc_isstrict::call(funcid)? {
        Some(proisstrict) => Ok(proisstrict),
        None => Err(elog(format!("cache lookup failed for function {funcid}"))),
    }
}

/// `op_strict(opno)` (lsyscache.c): the `proisstrict` flag of the operator's
/// underlying function.
pub fn op_strict(opno: Oid) -> PgResult<bool> {
    let funcid = get_opcode(opno)?;

    if funcid == INVALID_OID {
        return Err(elog(format!("operator {opno} does not exist")));
    }

    func_strict(funcid)
}

/// `get_commutator(opno)` (lsyscache.c): the commutator operator of `opno`, or
/// `InvalidOid` if none.
pub fn get_commutator(opno: Oid) -> PgResult<Oid> {
    match syscache::oper_oprcom::call(opno)? {
        Some(oprcom) => Ok(oprcom),
        None => Ok(INVALID_OID),
    }
}

/// `get_op_opfamily_properties(opno, opfamily, ordering_op, &strategy,
/// &lefttype, &righttype)` (lsyscache.c): the operator's strategy number and
/// declared input data types within the opfamily.
///
/// This unit's seam fixes `ordering_op = false` (the `AMOP_SEARCH` purpose; the
/// only purpose its consumers request). The C caller is expected to have
/// verified membership and so raises an error on a miss; here `missing_ok =
/// true` instead returns `Ok(None)`.
pub fn get_op_opfamily_properties(
    opno: Oid,
    opfamily: Oid,
    missing_ok: bool,
) -> PgResult<Option<(i32, Oid, Oid)>> {
    let amop_tup = syscache::amop_by_opr_purpose_family::call(opno, AMOP_SEARCH, opfamily)?;
    match amop_tup {
        None => {
            if missing_ok {
                Ok(None)
            } else {
                Err(elog(format!(
                    "operator {opno} is not a member of opfamily {opfamily}"
                )))
            }
        }
        Some(amop_tup) => Ok(Some((
            amop_tup.amopstrategy as i32,
            amop_tup.amoplefttype,
            amop_tup.amoprighttype,
        ))),
    }
}

/// `get_opfamily_member(opfamily, lefttype, righttype, strategy)`
/// (lsyscache.c): the OID of the operator that implements the strategy with the
/// datatypes for the opfamily, or `InvalidOid` if no `pg_amop` entry.
pub fn get_opfamily_member(
    opfamily: Oid,
    lefttype: Oid,
    righttype: Oid,
    strategy: i16,
) -> PgResult<Oid> {
    let amop_tup =
        syscache::amop_by_strategy_full::call(opfamily, lefttype, righttype, strategy)?;
    match amop_tup {
        None => Ok(INVALID_OID),
        Some(amop_tup) => Ok(amop_tup.amopopr),
    }
}

/// `get_opmethod_canorder(amoid)` (lsyscache.c): `amcanorder` for the index AM,
/// hardcoding the built-in AMs to avoid the handler call in the common cases.
fn get_opmethod_canorder(amoid: Oid) -> PgResult<bool> {
    match amoid {
        BTREE_AM_OID => Ok(true),
        HASH_AM_OID | GIST_AM_OID | GIN_AM_OID | SPGIST_AM_OID | BRIN_AM_OID => Ok(false),
        _ => {
            let amroutine = amapi::get_index_am_info::call(amoid)?;
            Ok(amroutine.amcanorder)
        }
    }
}

/// `get_ordering_op_properties(opno, &opfamily, &opcintype, &cmptype)`
/// (lsyscache.c): given an ordering operator, determine its opfamily, declared
/// input datatype, and comparison type. `None` means the operator is not a
/// valid ordering operator (the C `false`).
pub fn get_ordering_op_properties(opno: Oid) -> PgResult<Option<(Oid, Oid, i32)>> {
    /* ensure outputs are initialized on failure */
    let mut opfamily = INVALID_OID;
    let mut opcintype = INVALID_OID;
    let mut cmptype = COMPARE_INVALID;
    let mut result = false;

    /*
     * Search pg_amop to see if the target operator is registered as the "<"
     * or ">" operator of any btree opfamily.
     */
    let scratch = MemoryContext::new("get_ordering_op_properties");
    let catlist = syscache::amop_list_by_opr::call(scratch.mcx(), opno)?;

    for aform in &catlist {
        /* must be ordering index */
        if !get_opmethod_canorder(aform.amopmethod)? {
            continue;
        }

        let am_cmptype = amapi::index_am_translate_strategy::call(
            aform.amopstrategy as i32,
            aform.amopmethod,
            aform.amopfamily,
            true,
        )?;

        if am_cmptype == COMPARE_LT || am_cmptype == COMPARE_GT {
            /* Found it ... should have consistent input types */
            if aform.amoplefttype == aform.amoprighttype {
                /* Found a suitable opfamily, return info */
                opfamily = aform.amopfamily;
                opcintype = aform.amoplefttype;
                cmptype = am_cmptype;
                result = true;
                break;
            }
        }
    }

    if result {
        Ok(Some((opfamily, opcintype, cmptype)))
    } else {
        Ok(None)
    }
}

/// `get_op_hash_functions(opno, &lhs_procno, &rhs_procno)` (lsyscache.c):
/// resolve the standard hash support function(s) for the LHS and RHS datatypes
/// of a hashable operator. Both `lhs_procno` and `rhs_procno` are always
/// requested by this seam, so the C code paths gated on a NULL `rhs_procno` do
/// not apply. `None` means the function(s) could not be found (the C `false`).
pub fn get_op_hash_functions(opno: Oid) -> PgResult<Option<(Oid, Oid)>> {
    /* Ensure output args are initialized on failure */
    let mut lhs_procno = INVALID_OID;
    let mut rhs_procno = INVALID_OID;
    let mut result = false;

    /*
     * Search pg_amop to see if the target operator is registered as the "="
     * operator of any hash opfamily.  If the operator is registered in
     * multiple opfamilies, assume we can use any one.
     */
    let scratch = MemoryContext::new("get_op_hash_functions");
    let catlist = syscache::amop_list_by_opr::call(scratch.mcx(), opno)?;

    for aform in &catlist {
        if aform.amopmethod == HASH_AM_OID
            && aform.amopstrategy == HTEqualStrategyNumber as i16
        {
            /*
             * Get the matching support function(s).  Failure probably
             * shouldn't happen --- it implies a bogus opfamily --- but
             * continue looking if so.
             *
             * This seam always requests both the LHS and RHS functions, so the
             * C branches gated on a NULL lhs_procno / rhs_procno do not apply.
             */
            lhs_procno = crate::opclass::get_opfamily_proc(
                aform.amopfamily,
                aform.amoplefttype,
                aform.amoplefttype,
                HASHSTANDARD_PROC as i16,
            )?;
            if lhs_procno == INVALID_OID {
                continue;
            }
            /* Only one lookup needed if given operator is single-type */
            if aform.amoplefttype == aform.amoprighttype {
                rhs_procno = lhs_procno;
                result = true;
                break;
            }
            rhs_procno = crate::opclass::get_opfamily_proc(
                aform.amopfamily,
                aform.amoprighttype,
                aform.amoprighttype,
                HASHSTANDARD_PROC as i16,
            )?;
            if rhs_procno == INVALID_OID {
                /* Forget any LHS function from this opfamily */
                lhs_procno = INVALID_OID;
                continue;
            }
            /* Matching RHS found, so done */
            result = true;
            break;
        }
    }

    if result {
        Ok(Some((lhs_procno, rhs_procno)))
    } else {
        Ok(None)
    }
}
