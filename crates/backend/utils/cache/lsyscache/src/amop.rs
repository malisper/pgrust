use crate::scratch::with_scratch;
use crate::{
    cache_lookup_error, CompareType, InvalidStrategy, OpIndexInterpretation, StrategyNumber,
    COMPARE_EQ, COMPARE_GT, COMPARE_INVALID, COMPARE_LT, COMPARE_NE,
};
use mcx::{Mcx, PgVec};
use types_core::{InvalidOid, Oid, BTREE_AM_OID};
use types_error::PgResult;

// pg_amop.h
pub const AMOP_SEARCH: u8 = b's';
pub const AMOP_ORDER: u8 = b'o';
// stratnum.h
pub const HTEqualStrategyNumber: i16 = 1;
pub const BTLessStrategyNumber: i16 = 1;
pub const BTLessEqualStrategyNumber: i16 = 2;
pub const BTEqualStrategyNumber: i16 = 3;
pub const BTGreaterEqualStrategyNumber: i16 = 4;
pub const BTGreaterStrategyNumber: i16 = 5;
// hash.h
pub const HASHSTANDARD_PROC: i16 = 1;
// pg_am.dat
pub const HASH_AM_OID: Oid = 405;
pub const GIST_AM_OID: Oid = 783;
pub const GIN_AM_OID: Oid = 2742;
pub const SPGIST_AM_OID: Oid = 4000;
pub const BRIN_AM_OID: Oid = 3580;

// A non-builtin AM (CREATE ACCESS METHOD over a builtin index handler)
// behaves as its handler's builtin AM (amapi.c resolves via GetIndexAmRoutine).
fn canonical_index_am(amoid: Oid) -> Oid {
    match syscache_seams::pg_am_amhandler::call(amoid) {
        Ok(Some(330)) => BTREE_AM_OID,
        Ok(Some(331)) => HASH_AM_OID,
        Ok(Some(333)) => GIN_AM_OID,
        Ok(Some(332)) => GIST_AM_OID,
        Ok(Some(334)) => SPGIST_AM_OID,
        Ok(Some(335)) => BRIN_AM_OID,
        _ => amoid,
    }
}

// amapi.c IndexAmTranslateStrategy over the built-in AMs (bttranslatestrategy
// / hashtranslatestrategy; gist/gin/spgist/brin define no translator).
// Non-core AMs need GetIndexAmRoutineByAmId, unported: loud panic.
fn index_am_translate_strategy(strategy: i16, amoid: Oid, _opfamily: Oid) -> CompareType {
    match amoid {
        BTREE_AM_OID if (1..=5).contains(&strategy) => strategy as CompareType,
        BTREE_AM_OID => COMPARE_INVALID,
        HASH_AM_OID if strategy == HTEqualStrategyNumber => COMPARE_EQ,
        HASH_AM_OID | GIST_AM_OID | GIN_AM_OID | SPGIST_AM_OID | BRIN_AM_OID => COMPARE_INVALID,
        _ => match canonical_index_am(amoid) {
            c if c != amoid => index_am_translate_strategy(strategy, c, _opfamily),
            // hnsw and bloom: amtranslatestrategy == NULL.
            _ => match extension_am_handler_name(amoid).as_deref() {
                Some(b"hnswhandler") | Some(b"blhandler") => COMPARE_INVALID,
                _ => panic!(
                    "IndexAmTranslateStrategy for non-builtin AM {amoid}: amapi.c unported"
                ),
            },
        },
    }
}

// amapi.c IndexAmTranslateCompareType, same built-in coverage.
fn index_am_translate_cmptype(cmptype: CompareType, amoid: Oid, _opfamily: Oid) -> StrategyNumber {
    match amoid {
        BTREE_AM_OID if (COMPARE_LT..=COMPARE_GT).contains(&cmptype) => cmptype as StrategyNumber,
        BTREE_AM_OID => InvalidStrategy,
        HASH_AM_OID if cmptype == COMPARE_EQ => HTEqualStrategyNumber as StrategyNumber,
        HASH_AM_OID | GIST_AM_OID | GIN_AM_OID | SPGIST_AM_OID | BRIN_AM_OID => InvalidStrategy,
        _ => match canonical_index_am(amoid) {
            c if c != amoid => index_am_translate_cmptype(cmptype, c, _opfamily),
            // hnsw and bloom: amtranslatecmptype == NULL.
            _ => match extension_am_handler_name(amoid).as_deref() {
                Some(b"hnswhandler") | Some(b"blhandler") => InvalidStrategy,
                _ => panic!(
                    "IndexAmTranslateCompareType for non-builtin AM {amoid}: amapi.c unported"
                ),
            },
        },
    }
}

// (amconsistentequality, amconsistentordering) from the built-in AM handlers.
pub(crate) fn index_am_consistent_flags(amoid: Oid) -> (bool, bool) {
    match amoid {
        BTREE_AM_OID => (true, true),
        HASH_AM_OID => (true, false),
        GIST_AM_OID | GIN_AM_OID | SPGIST_AM_OID | BRIN_AM_OID => (false, false),
        _ => match canonical_index_am(amoid) {
            c if c != amoid => index_am_consistent_flags(c),
            // hnsw and bloom handlers: both consistency flags false.
            _ => match extension_am_handler_name(amoid).as_deref() {
                Some(b"hnswhandler") | Some(b"blhandler") => (false, false),
                _ => panic!(
                    "GetIndexAmRoutineByAmId for non-builtin AM {amoid}: amapi.c unported"
                ),
            },
        },
    }
}

pub fn op_in_opfamily(opno: Oid, opfamily: Oid) -> PgResult<bool> {
    Ok(syscache_seams::lookup_pg_amop_by_operator::call(opno, AMOP_SEARCH, opfamily)?.is_some())
}

pub fn get_op_opfamily_strategy(opno: Oid, opfamily: Oid) -> PgResult<i32> {
    Ok(
        match syscache_seams::lookup_pg_amop_by_operator::call(opno, AMOP_SEARCH, opfamily)? {
            Some(amop) => amop.amopstrategy as i32,
            None => 0,
        },
    )
}

pub fn get_op_opfamily_sortfamily(opno: Oid, opfamily: Oid) -> PgResult<Oid> {
    Ok(
        match syscache_seams::lookup_pg_amop_by_operator::call(opno, AMOP_ORDER, opfamily)? {
            Some(amop) => amop.amopsortfamily,
            None => InvalidOid,
        },
    )
}

pub fn get_op_opfamily_properties(
    opno: Oid,
    opfamily: Oid,
    ordering_op: bool,
) -> PgResult<(i32, Oid, Oid)> {
    let purpose = if ordering_op { AMOP_ORDER } else { AMOP_SEARCH };
    let amop = syscache_seams::lookup_pg_amop_by_operator::call(opno, purpose, opfamily)?
        .ok_or_else(|| {
            cache_lookup_error(format!(
                "operator {opno} is not a member of opfamily {opfamily}"
            ))
        })?;
    Ok((amop.amopstrategy as i32, amop.amoplefttype, amop.amoprighttype))
}

pub fn get_opfamily_member(
    opfamily: Oid,
    lefttype: Oid,
    righttype: Oid,
    strategy: i16,
) -> PgResult<Oid> {
    syscache_seams::lookup_pg_amop_by_strategy::call(opfamily, lefttype, righttype, strategy)
}

pub fn get_opfamily_member_for_cmptype(
    opfamily: Oid,
    lefttype: Oid,
    righttype: Oid,
    cmptype: CompareType,
) -> PgResult<Oid> {
    let opmethod = crate::misc::get_opfamily_method(opfamily)?;
    let strategy = index_am_translate_cmptype(cmptype, opmethod, opfamily);
    if strategy == InvalidStrategy {
        return Ok(InvalidOid);
    }
    get_opfamily_member(opfamily, lefttype, righttype, strategy as i16)
}

// C hardcodes the built-in AMs and falls back to GetIndexAmRoutineByAmId
// (amapi.c, unported) for others: that arm is a loud panic here.
fn get_opmethod_canorder(amoid: Oid) -> bool {
    match amoid {
        BTREE_AM_OID => true,
        HASH_AM_OID | GIST_AM_OID | GIN_AM_OID | SPGIST_AM_OID | BRIN_AM_OID => false,
        _ => match canonical_index_am(amoid) {
            c if c != amoid => get_opmethod_canorder(c),
            // Extension AMs by handler symbol: hnsw and bloom set
            // amcanorder = false in their handlers.
            _ => match extension_am_handler_name(amoid).as_deref() {
                Some(b"hnswhandler") | Some(b"blhandler") => false,
                _ => panic!(
                    "get_opmethod_canorder for non-builtin AM {amoid}: amapi.c unported"
                ),
            },
        },
    }
}

fn extension_am_handler_name(amoid: Oid) -> Option<Vec<u8>> {
    let handler = syscache_seams::pg_am_amhandler::call(amoid).ok()??;
    let name = syscache_seams::pg_proc_proname::call(handler).ok()??;
    Some(name.name_str().to_vec())
}

pub fn get_ordering_op_properties(opno: Oid) -> PgResult<Option<(Oid, Oid, CompareType)>> {
    with_scratch(|scratch| {
        let members = syscache_seams::lookup_pg_amop_members_by_operator::call(scratch, opno)?;
        for aform in &members {
            if !get_opmethod_canorder(aform.amopmethod) {
                continue;
            }
            let am_cmptype = index_am_translate_strategy(
                aform.amopstrategy,
                aform.amopmethod,
                aform.amopfamily,
            );
            if (am_cmptype == COMPARE_LT || am_cmptype == COMPARE_GT)
                && aform.amoplefttype == aform.amoprighttype
            {
                return Ok(Some((aform.amopfamily, aform.amoplefttype, am_cmptype)));
            }
        }
        Ok(None)
    })
}

pub fn get_equality_op_for_ordering_op(opno: Oid) -> PgResult<Option<(Oid, bool)>> {
    if let Some((opfamily, opcintype, cmptype)) = get_ordering_op_properties(opno)? {
        let result = get_opfamily_member_for_cmptype(opfamily, opcintype, opcintype, COMPARE_EQ)?;
        return Ok(Some((result, cmptype == COMPARE_GT)));
    }
    Ok(None)
}

pub fn get_ordering_op_for_equality_op(opno: Oid, use_lhs_type: bool) -> PgResult<Oid> {
    with_scratch(|scratch| {
        let members = syscache_seams::lookup_pg_amop_members_by_operator::call(scratch, opno)?;
        for aform in &members {
            if !get_opmethod_canorder(aform.amopmethod) {
                continue;
            }
            let cmptype = index_am_translate_strategy(
                aform.amopstrategy,
                aform.amopmethod,
                aform.amopfamily,
            );
            if cmptype == COMPARE_EQ {
                let typid = if use_lhs_type { aform.amoplefttype } else { aform.amoprighttype };
                let result =
                    get_opfamily_member_for_cmptype(aform.amopfamily, typid, typid, COMPARE_LT)?;
                if result != InvalidOid {
                    return Ok(result);
                }
            }
        }
        Ok(InvalidOid)
    })
}

// pg_amop rows for an operator, visitor form (get_op_btree_interpretation's
// membership probe rides this).
pub fn with_amop_members(
    opno: Oid,
    mut f: impl FnMut(&syscache_seams::PgAmopMemberShape),
) -> PgResult<()> {
    with_scratch(|scratch| {
        let members = syscache_seams::lookup_pg_amop_members_by_operator::call(scratch, opno)?;
        for aform in &members {
            f(aform);
        }
        Ok(())
    })
}

// op_is_safe_index_member (lsyscache.c): btree/hash opfamily membership,
// used as a proxy for null-safety and for equality-semantics agreement with
// the family's other members.
pub fn op_is_safe_index_member(opno: Oid) -> PgResult<bool> {
    with_scratch(|scratch| {
        let members = syscache_seams::lookup_pg_amop_members_by_operator::call(scratch, opno)?;
        for aform in &members {
            if aform.amopmethod == BTREE_AM_OID || aform.amopmethod == HASH_AM_OID {
                return Ok(true);
            }
        }
        Ok(false)
    })
}

pub fn get_mergejoin_opfamilies<'mcx>(mcx: Mcx<'mcx>, opno: Oid) -> PgResult<PgVec<'mcx, Oid>> {
    let mut result = PgVec::new_in(mcx);
    with_scratch(|scratch| {
        let members = syscache_seams::lookup_pg_amop_members_by_operator::call(scratch, opno)?;
        for aform in &members {
            if get_opmethod_canorder(aform.amopmethod)
                && index_am_translate_strategy(
                    aform.amopstrategy,
                    aform.amopmethod,
                    aform.amopfamily,
                ) == COMPARE_EQ
            {
                result.push(aform.amopfamily);
            }
        }
        Ok(())
    })?;
    Ok(result)
}

pub fn get_compatible_hash_operators(opno: Oid) -> PgResult<Option<(Oid, Oid)>> {
    with_scratch(|scratch| {
        let members = syscache_seams::lookup_pg_amop_members_by_operator::call(scratch, opno)?;
        for aform in &members {
            if aform.amopmethod == HASH_AM_OID && aform.amopstrategy == HTEqualStrategyNumber {
                if aform.amoplefttype == aform.amoprighttype {
                    return Ok(Some((opno, opno)));
                }
                let lhs = get_opfamily_member(
                    aform.amopfamily,
                    aform.amoplefttype,
                    aform.amoplefttype,
                    HTEqualStrategyNumber,
                )?;
                if lhs == InvalidOid {
                    continue;
                }
                let rhs = get_opfamily_member(
                    aform.amopfamily,
                    aform.amoprighttype,
                    aform.amoprighttype,
                    HTEqualStrategyNumber,
                )?;
                if rhs == InvalidOid {
                    continue;
                }
                return Ok(Some((lhs, rhs)));
            }
        }
        Ok(None)
    })
}

pub fn get_op_hash_functions(opno: Oid) -> PgResult<Option<(Oid, Oid)>> {
    with_scratch(|scratch| {
        let members = syscache_seams::lookup_pg_amop_members_by_operator::call(scratch, opno)?;
        for aform in &members {
            if aform.amopmethod == HASH_AM_OID && aform.amopstrategy == HTEqualStrategyNumber {
                let lhs = get_opfamily_proc(
                    aform.amopfamily,
                    aform.amoplefttype,
                    aform.amoplefttype,
                    HASHSTANDARD_PROC,
                )?;
                if lhs == InvalidOid {
                    continue;
                }
                if aform.amoplefttype == aform.amoprighttype {
                    return Ok(Some((lhs, lhs)));
                }
                let rhs = get_opfamily_proc(
                    aform.amopfamily,
                    aform.amoprighttype,
                    aform.amoprighttype,
                    HASHSTANDARD_PROC,
                )?;
                if rhs == InvalidOid {
                    continue;
                }
                return Ok(Some((lhs, rhs)));
            }
        }
        Ok(None)
    })
}

pub fn get_op_index_interpretation<'mcx>(
    mcx: Mcx<'mcx>,
    opno: Oid,
) -> PgResult<PgVec<'mcx, OpIndexInterpretation>> {
    let mut result = PgVec::new_in(mcx);
    with_scratch(|scratch| {
        let members = syscache_seams::lookup_pg_amop_members_by_operator::call(scratch, opno)?;
        for op_form in &members {
            if !get_opmethod_canorder(op_form.amopmethod) {
                continue;
            }
            let cmptype = index_am_translate_strategy(
                op_form.amopstrategy,
                op_form.amopmethod,
                op_form.amopfamily,
            );
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
        if !result.is_empty() {
            return Ok(());
        }
        let op_negator = crate::operator::get_negator(opno)?;
        if op_negator == InvalidOid {
            return Ok(());
        }
        let members = syscache_seams::lookup_pg_amop_members_by_operator::call(scratch, op_negator)?;
        for op_form in &members {
            // C reads amroutine->amcanorder here; same value for built-ins.
            if !get_opmethod_canorder(op_form.amopmethod) {
                continue;
            }
            let cmptype = index_am_translate_strategy(
                op_form.amopstrategy,
                op_form.amopmethod,
                op_form.amopfamily,
            );
            if cmptype != COMPARE_EQ {
                continue;
            }
            result.push(OpIndexInterpretation {
                opfamily_id: op_form.amopfamily,
                cmptype: COMPARE_NE,
                oplefttype: op_form.amoplefttype,
                oprighttype: op_form.amoprighttype,
            });
        }
        Ok(())
    })?;
    Ok(result)
}

pub fn equality_ops_are_compatible(opno1: Oid, opno2: Oid) -> PgResult<bool> {
    ops_are_compatible(opno1, opno2, false)
}

pub fn comparison_ops_are_compatible(opno1: Oid, opno2: Oid) -> PgResult<bool> {
    ops_are_compatible(opno1, opno2, true)
}

fn ops_are_compatible(opno1: Oid, opno2: Oid, check_ordering: bool) -> PgResult<bool> {
    if opno1 == opno2 {
        return Ok(true);
    }
    with_scratch(|scratch| {
        let members = syscache_seams::lookup_pg_amop_members_by_operator::call(scratch, opno1)?;
        for op_form in &members {
            if op_in_opfamily(opno2, op_form.amopfamily)? {
                let (consistent_eq, consistent_ord) =
                    index_am_consistent_flags(op_form.amopmethod);
                if if check_ordering { consistent_ord } else { consistent_eq } {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    })
}

pub fn get_opfamily_proc(
    opfamily: Oid,
    lefttype: Oid,
    righttype: Oid,
    procnum: i16,
) -> PgResult<Oid> {
    syscache_seams::lookup_pg_amproc::call(opfamily, lefttype, righttype, procnum)
}
