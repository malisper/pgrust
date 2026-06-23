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
use alloc::vec::Vec;

use amapi_seams as amapi;
use ::lsyscache_seams::OpIndexInterpretation;
use syscache_seams as syscache;
use typcache_seams as typcache;
use ::mcx::{Mcx, MemoryContext, PgVec};
use ::types_core::Oid;
use ::types_error::{PgError, PgResult};
use ::hash::hash::{HASHSTANDARD_PROC, HTEqualStrategyNumber};
use ::opclass::{AMOP_ORDER, AMOP_SEARCH};

/// `InvalidOid` (`postgres_ext.h`).
const INVALID_OID: Oid = 0;

/// `COMPARE_NE` (`access/cmptype.h`).
const COMPARE_NE: i32 = 6;

/// `ARRAY_EQ_OP` / `RECORD_EQ_OP` (`catalog/pg_operator.dat`).
const ARRAY_EQ_OP: Oid = 1070;
const RECORD_EQ_OP: Oid = 2988;
/// `F_BTARRAYCMP` / `F_BTRECORDCMP` / `F_HASH_ARRAY` / `F_HASH_RECORD`
/// (`fmgroids.h`).
const F_BTARRAYCMP: Oid = 382;
const F_BTRECORDCMP: Oid = 2987;
const F_HASH_ARRAY: Oid = 626;
const F_HASH_RECORD: Oid = 6192;
/// `TYPECACHE_CMP_PROC` / `TYPECACHE_HASH_PROC` (`utils/typcache.h`).
const TYPECACHE_CMP_PROC: i32 = 0x8;
const TYPECACHE_HASH_PROC: i32 = 0x10;

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
    ordering_op: bool,
    missing_ok: bool,
) -> PgResult<Option<(i32, Oid, Oid)>> {
    let purpose = if ordering_op { AMOP_ORDER } else { AMOP_SEARCH };
    let amop_tup = syscache::amop_by_opr_purpose_family::call(opno, purpose, opfamily)?;
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

/// `OidIsValid(oid)` (`c.h`): `oid != InvalidOid`.
fn oid_is_valid(oid: Oid) -> bool {
    oid != INVALID_OID
}

/// Copy a scratch `Vec<T>` into a `mcx`-allocated `PgVec<T>` (the C `palloc`'d
/// list result, allocated in the caller's context).
fn pgvec_from<'mcx, T: Copy>(mcx: Mcx<'mcx>, src: &[T]) -> PgResult<PgVec<'mcx, T>> {
    let mut out: PgVec<'mcx, T> = ::mcx::vec_with_capacity_in(mcx, src.len())?;
    for &v in src {
        out.push(v);
    }
    Ok(out)
}

/// `op_in_opfamily(opno, opfamily)` (lsyscache.c): whether `opno` is a search
/// member of `opfamily`.
pub fn op_in_opfamily(opno: Oid, opfamily: Oid) -> PgResult<bool> {
    // SearchSysCacheExists3(AMOPOPID, opno, AMOP_SEARCH, opfamily)
    syscache::amop_search_exists::call(opno, opfamily)
}

/// `get_op_opfamily_strategy(opno, opfamily)` (lsyscache.c): the operator's
/// search strategy number within the opfamily, or 0 if not a member.
pub fn get_op_opfamily_strategy(opno: Oid, opfamily: Oid) -> PgResult<i32> {
    // SearchSysCache3(AMOPOPID, opno, AMOP_SEARCH, opfamily); !valid -> 0
    match syscache::amop_by_opr_purpose::call(opno, AMOP_SEARCH as u8, opfamily)? {
        Some((amopstrategy, _amopsortfamily)) => Ok(amopstrategy as i32),
        None => Ok(0),
    }
}

/// `get_op_opfamily_sortfamily(opno, opfamily)` (lsyscache.c): the
/// `amopsortfamily` of `opno` as an ordering member of `opfamily`, or
/// `InvalidOid`.
pub fn get_op_opfamily_sortfamily(opno: Oid, opfamily: Oid) -> PgResult<Oid> {
    // SearchSysCache3(AMOPOPID, opno, AMOP_ORDER, opfamily); !valid -> InvalidOid
    match syscache::amop_by_opr_purpose::call(opno, AMOP_ORDER as u8, opfamily)? {
        Some((_amopstrategy, amopsortfamily)) => Ok(amopsortfamily),
        None => Ok(INVALID_OID),
    }
}

/// `get_opfamily_member_for_cmptype(opfamily, lefttype, righttype, cmptype)`
/// (lsyscache.c).
///
/// ```c
/// opmethod = get_opfamily_method(opfamily);
/// strategy = IndexAmTranslateCompareType(cmptype, opmethod, opfamily, true);
/// if (!strategy) return InvalidOid;
/// return get_opfamily_member(opfamily, lefttype, righttype, strategy);
/// ```
pub fn get_opfamily_member_for_cmptype(
    opfamily: Oid,
    lefttype: Oid,
    righttype: Oid,
    cmptype: i32,
) -> PgResult<Oid> {
    let opmethod = crate::opclass::get_opfamily_method(opfamily)?;
    let strategy = amapi::index_am_translate_cmptype::call(cmptype, opmethod, opfamily, true)?;
    if strategy == 0 {
        return Ok(INVALID_OID);
    }
    get_opfamily_member(opfamily, lefttype, righttype, strategy)
}

/// `get_equality_op_for_ordering_op(opno, &reverse)` (lsyscache.c).
pub fn get_equality_op_for_ordering_op(opno: Oid) -> PgResult<Option<(Oid, bool)>> {
    // if (get_ordering_op_properties(opno, &opfamily, &opcintype, &cmptype)) {
    //     result = get_opfamily_member_for_cmptype(opfamily, opcintype, opcintype, COMPARE_EQ);
    //     if (reverse) *reverse = (cmptype == COMPARE_GT);
    // }
    match get_ordering_op_properties(opno)? {
        Some((opfamily, opcintype, cmptype)) => {
            let result =
                get_opfamily_member_for_cmptype(opfamily, opcintype, opcintype, COMPARE_EQ)?;
            let reverse = cmptype == COMPARE_GT;
            // C returns the (possibly Invalid) result OID and sets *reverse;
            // callers test OidIsValid(result). We carry both.
            Ok(Some((result, reverse)))
        }
        None => Ok(None),
    }
}

/// `get_ordering_op_for_equality_op(opno, use_lhs_type)` (lsyscache.c).
pub fn get_ordering_op_for_equality_op(opno: Oid, use_lhs_type: bool) -> PgResult<Oid> {
    let mut result = INVALID_OID;

    let scratch = MemoryContext::new("get_ordering_op_for_equality_op");
    let catlist = syscache::amop_list_by_opr::call(scratch.mcx(), opno)?;

    for aform in &catlist {
        /* must be ordering index */
        if !get_opmethod_canorder(aform.amopmethod)? {
            continue;
        }

        let cmptype = amapi::index_am_translate_strategy::call(
            aform.amopstrategy as i32,
            aform.amopmethod,
            aform.amopfamily,
            true,
        )?;
        if cmptype == COMPARE_EQ {
            let typid = if use_lhs_type {
                aform.amoplefttype
            } else {
                aform.amoprighttype
            };
            result = get_opfamily_member_for_cmptype(aform.amopfamily, typid, typid, COMPARE_LT)?;
            if oid_is_valid(result) {
                break;
            }
            /* failure probably shouldn't happen, but keep looking if so */
        }
    }

    Ok(result)
}

/// `get_mergejoin_opfamilies(opno)` (lsyscache.c): the amcanorder opfamily
/// OIDs in which `opno` represents equality.
pub fn get_mergejoin_opfamilies<'mcx>(mcx: Mcx<'mcx>, opno: Oid) -> PgResult<PgVec<'mcx, Oid>> {
    let mut result: Vec<Oid> = Vec::new();

    let scratch = MemoryContext::new("get_mergejoin_opfamilies");
    let catlist = syscache::amop_list_by_opr::call(scratch.mcx(), opno)?;

    for aform in &catlist {
        /* must be ordering index equality */
        if get_opmethod_canorder(aform.amopmethod)?
            && amapi::index_am_translate_strategy::call(
                aform.amopstrategy as i32,
                aform.amopmethod,
                aform.amopfamily,
                true,
            )? == COMPARE_EQ
        {
            result.push(aform.amopfamily);
        }
    }

    pgvec_from(mcx, &result)
}

/// `linitial_oid(get_mergejoin_opfamilies(opno))` (lsyscache.c) — the first
/// btree opfamily `opno` represents equality in, or `None` when
/// `get_mergejoin_opfamilies` returns NIL. relnode.c's
/// `set_joinrel_partition_key_exprs` reaches this through its no-owner ext seam
/// without carrying an `Mcx`; the C builds the whole list then takes `linitial`,
/// so we stop at the first match.
pub fn get_mergejoin_opfamilies_first(opno: Oid) -> PgResult<Option<Oid>> {
    let scratch = MemoryContext::new("get_mergejoin_opfamilies_first");
    let catlist = syscache::amop_list_by_opr::call(scratch.mcx(), opno)?;

    for aform in &catlist {
        if get_opmethod_canorder(aform.amopmethod)?
            && amapi::index_am_translate_strategy::call(
                aform.amopstrategy as i32,
                aform.amopmethod,
                aform.amopfamily,
                true,
            )? == COMPARE_EQ
        {
            return Ok(Some(aform.amopfamily));
        }
    }

    Ok(None)
}

/// `get_compatible_hash_operators(opno, &lhs_opno, &rhs_opno)` (lsyscache.c).
/// This seam always requests both LHS and RHS, so the C branches gated on a
/// NULL `lhs_opno`/`rhs_opno` do not apply.
pub fn get_compatible_hash_operators(opno: Oid) -> PgResult<Option<(Oid, Oid)>> {
    let mut lhs_opno = INVALID_OID;
    let mut rhs_opno = INVALID_OID;
    let mut result = false;

    let scratch = MemoryContext::new("get_compatible_hash_operators");
    let catlist = syscache::amop_list_by_opr::call(scratch.mcx(), opno)?;

    for aform in &catlist {
        if aform.amopmethod == HASH_AM_OID && aform.amopstrategy == HTEqualStrategyNumber as i16 {
            /* No extra lookup needed if given operator is single-type */
            if aform.amoplefttype == aform.amoprighttype {
                lhs_opno = opno;
                rhs_opno = opno;
                result = true;
                break;
            }

            /* Get the matching single-type operator(s). */
            lhs_opno = get_opfamily_member(
                aform.amopfamily,
                aform.amoplefttype,
                aform.amoplefttype,
                HTEqualStrategyNumber as i16,
            )?;
            if !oid_is_valid(lhs_opno) {
                continue;
            }
            rhs_opno = get_opfamily_member(
                aform.amopfamily,
                aform.amoprighttype,
                aform.amoprighttype,
                HTEqualStrategyNumber as i16,
            )?;
            if !oid_is_valid(rhs_opno) {
                /* Forget any LHS operator from this opfamily */
                lhs_opno = INVALID_OID;
                continue;
            }
            /* Matching RHS found, so done */
            result = true;
            break;
        }
    }

    if result {
        Ok(Some((lhs_opno, rhs_opno)))
    } else {
        Ok(None)
    }
}

/// `get_op_index_interpretation(opno)` (lsyscache.c).
pub fn get_op_index_interpretation<'mcx>(
    mcx: Mcx<'mcx>,
    opno: Oid,
) -> PgResult<PgVec<'mcx, OpIndexInterpretation>> {
    let mut result: Vec<OpIndexInterpretation> = Vec::new();

    let scratch = MemoryContext::new("get_op_index_interpretation");
    let catlist = syscache::amop_list_by_opr::call(scratch.mcx(), opno)?;

    for op_form in &catlist {
        /* must be ordering index */
        if !get_opmethod_canorder(op_form.amopmethod)? {
            continue;
        }

        let cmptype = amapi::index_am_translate_strategy::call(
            op_form.amopstrategy as i32,
            op_form.amopmethod,
            op_form.amopfamily,
            true,
        )?;

        /* should not happen */
        if cmptype == COMPARE_INVALID {
            continue;
        }

        result.push(OpIndexInterpretation {
            opfamily_id: op_form.amopfamily,
            cmptype,
            oplefttype: op_form.amoplefttype,
            oprighttype: op_form.amoprighttype,
        });
    }

    /*
     * If we didn't find any btree opfamily containing the operator, perhaps it
     * is a <> operator.  See if it has a negator that is in an opfamily.
     */
    if result.is_empty() {
        let op_negator = get_negator(opno)?;

        if oid_is_valid(op_negator) {
            let scratch2 = MemoryContext::new("get_op_index_interpretation.neg");
            let catlist2 = syscache::amop_list_by_opr::call(scratch2.mcx(), op_negator)?;

            for op_form in &catlist2 {
                let amcanorder = amapi::get_index_am_info::call(op_form.amopmethod)?.amcanorder;
                /* must be ordering index */
                if !amcanorder {
                    continue;
                }

                let cmptype = amapi::index_am_translate_strategy::call(
                    op_form.amopstrategy as i32,
                    op_form.amopmethod,
                    op_form.amopfamily,
                    true,
                )?;

                /* Only consider negators that are = */
                if cmptype != COMPARE_EQ {
                    continue;
                }

                /* OK, report it as COMPARE_NE */
                result.push(OpIndexInterpretation {
                    opfamily_id: op_form.amopfamily,
                    cmptype: COMPARE_NE,
                    oplefttype: op_form.amoplefttype,
                    oprighttype: op_form.amoprighttype,
                });
            }
        }
    }

    pgvec_from(mcx, &result)
}

/// `equality_ops_are_compatible(opno1, opno2)` (lsyscache.c).
pub fn equality_ops_are_compatible(opno1: Oid, opno2: Oid) -> PgResult<bool> {
    /* Easy if they're the same operator */
    if opno1 == opno2 {
        return Ok(true);
    }

    let mut result = false;
    let scratch = MemoryContext::new("equality_ops_are_compatible");
    let catlist = syscache::amop_list_by_opr::call(scratch.mcx(), opno1)?;

    for op_form in &catlist {
        /* op_in_opfamily() is cheaper than the AM-routine load, so check first */
        if op_in_opfamily(opno2, op_form.amopfamily)?
            && amapi::index_am_consistent_equality::call(op_form.amopmethod)?
        {
            result = true;
            break;
        }
    }

    Ok(result)
}

/// `comparison_ops_are_compatible(opno1, opno2)` (lsyscache.c).
pub fn comparison_ops_are_compatible(opno1: Oid, opno2: Oid) -> PgResult<bool> {
    /* Easy if they're the same operator */
    if opno1 == opno2 {
        return Ok(true);
    }

    let mut result = false;
    let scratch = MemoryContext::new("comparison_ops_are_compatible");
    let catlist = syscache::amop_list_by_opr::call(scratch.mcx(), opno1)?;

    for op_form in &catlist {
        if op_in_opfamily(opno2, op_form.amopfamily)?
            && amapi::index_am_consistent_ordering::call(op_form.amopmethod)?
        {
            result = true;
            break;
        }
    }

    Ok(result)
}

// ---- pg_operator scalar reads --------------------------------------------

/// Fetch the `Form_pg_operator` row for `opno`, or `None` on a cache miss.
fn pg_operator_form<'mcx>(mcx: Mcx<'mcx>, opno: Oid) -> PgResult<Option<syscache::PgOperatorForm>> {
    syscache::pg_operator_form::call(mcx, opno)
}

/// `get_opname(opno)` (lsyscache.c): the operator's name, or `None`.
pub fn get_opname<'mcx>(mcx: Mcx<'mcx>, opno: Oid) -> PgResult<Option<::mcx::PgString<'mcx>>> {
    match pg_operator_form(mcx, opno)? {
        Some(optup) => Ok(Some(::mcx::PgString::from_str_in(&optup.oprname, mcx)?)),
        None => Ok(None),
    }
}

/// The `(oprname, oprnamespace, oprkind)` triple of `pg_operator` for `opno`,
/// or `None` on a cache miss. Used by ruleutils' `generate_operator_name` to
/// decide whether the unqualified operator name re-parses to the same operator.
pub fn get_oper_name_namespace_kind(
    opno: Oid,
) -> PgResult<Option<(alloc::string::String, Oid, i8)>> {
    let scratch = MemoryContext::new("get_oper_name_namespace_kind");
    match pg_operator_form(scratch.mcx(), opno)? {
        Some(optup) => Ok(Some((optup.oprname, optup.oprnamespace, optup.oprkind))),
        None => Ok(None),
    }
}

/// The single-byte name of operator `opno` when its `pg_operator.oprname` is
/// exactly one character (one byte) long, else `None`. Used by ruleutils'
/// `isSimpleNode` precedence oracle (its `get_simple_binary_op_name` helper),
/// which only ever inspects single-char operator names (`+ - * / %`); for those
/// the unqualified name equals `generate_operator_name`'s output, so this is a
/// faithful, allocation-free stand-in for the C `strlen(op) == 1 ? op[0]` test.
pub fn get_op_name_single_byte(opno: Oid) -> PgResult<Option<u8>> {
    let scratch = MemoryContext::new("get_op_name_single_byte");
    match pg_operator_form(scratch.mcx(), opno)? {
        Some(optup) => {
            let bytes = optup.oprname.as_bytes();
            if bytes.len() == 1 {
                Ok(Some(bytes[0]))
            } else {
                Ok(None)
            }
        }
        None => Ok(None),
    }
}

/// `get_op_rettype(opno)` (lsyscache.c): the operator's result type, or
/// `InvalidOid`.
pub fn get_op_rettype(opno: Oid) -> PgResult<Oid> {
    let scratch = MemoryContext::new("get_op_rettype");
    match pg_operator_form(scratch.mcx(), opno)? {
        Some(optup) => Ok(optup.oprresult),
        None => Ok(INVALID_OID),
    }
}

/// `get_negator(opno)` (lsyscache.c): the operator's negator, or `InvalidOid`.
pub fn get_negator(opno: Oid) -> PgResult<Oid> {
    let scratch = MemoryContext::new("get_negator");
    match pg_operator_form(scratch.mcx(), opno)? {
        Some(optup) => Ok(optup.oprnegate),
        None => Ok(INVALID_OID),
    }
}

/// `get_oprrest(opno)` (lsyscache.c): the restriction-selectivity estimator, or
/// `InvalidOid`.
pub fn get_oprrest(opno: Oid) -> PgResult<Oid> {
    let scratch = MemoryContext::new("get_oprrest");
    match pg_operator_form(scratch.mcx(), opno)? {
        Some(optup) => Ok(optup.oprrest),
        None => Ok(INVALID_OID),
    }
}

/// `get_oprjoin(opno)` (lsyscache.c): the join-selectivity estimator, or
/// `InvalidOid`.
pub fn get_oprjoin(opno: Oid) -> PgResult<Oid> {
    let scratch = MemoryContext::new("get_oprjoin");
    match pg_operator_form(scratch.mcx(), opno)? {
        Some(optup) => Ok(optup.oprjoin),
        None => Ok(INVALID_OID),
    }
}

/// `op_volatile(opno)` (lsyscache.c): the `provolatile` of the operator's
/// underlying function.
pub fn op_volatile(opno: Oid) -> PgResult<u8> {
    let funcid = get_opcode(opno)?;
    if funcid == INVALID_OID {
        return Err(elog(format!("operator {opno} does not exist")));
    }
    crate::function::func_volatile(funcid)
}

/// `op_mergejoinable(opno, inputtype)` (lsyscache.c).
pub fn op_mergejoinable(opno: Oid, inputtype: Oid) -> PgResult<bool> {
    let mut result = false;

    if opno == ARRAY_EQ_OP {
        // lookup_type_cache(inputtype, TYPECACHE_CMP_PROC)->cmp_proc == F_BTARRAYCMP
        let _ = TYPECACHE_CMP_PROC; // documents the C flag the seam encodes
        let cmp_proc = typcache::lookup_element_cmp_proc::call(inputtype)?;
        if cmp_proc == F_BTARRAYCMP {
            result = true;
        }
    } else if opno == RECORD_EQ_OP {
        let cmp_proc = typcache::lookup_element_cmp_proc::call(inputtype)?;
        if cmp_proc == F_BTRECORDCMP {
            result = true;
        }
    } else {
        /* For all other operators, rely on pg_operator.oprcanmerge */
        let scratch = MemoryContext::new("op_mergejoinable");
        if let Some(optup) = pg_operator_form(scratch.mcx(), opno)? {
            result = optup.oprcanmerge;
        }
    }
    Ok(result)
}

/// `op_hashjoinable(opno, inputtype)` (lsyscache.c).
pub fn op_hashjoinable(opno: Oid, inputtype: Oid) -> PgResult<bool> {
    let mut result = false;

    if opno == ARRAY_EQ_OP {
        let _ = TYPECACHE_HASH_PROC;
        let hash_proc = typcache::lookup_element_hash_proc::call(inputtype)?;
        if hash_proc == F_HASH_ARRAY {
            result = true;
        }
    } else if opno == RECORD_EQ_OP {
        let hash_proc = typcache::lookup_element_hash_proc::call(inputtype)?;
        if hash_proc == F_HASH_RECORD {
            result = true;
        }
    } else {
        /* For all other operators, rely on pg_operator.oprcanhash */
        let scratch = MemoryContext::new("op_hashjoinable");
        if let Some(optup) = pg_operator_form(scratch.mcx(), opno)? {
            result = optup.oprcanhash;
        }
    }
    Ok(result)
}
