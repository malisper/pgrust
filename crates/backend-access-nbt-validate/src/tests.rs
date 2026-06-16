//! Unit tests for the `nbtvalidate.c` port.
//!
//! Seam slots are process-global `OnceLock`s, so every test shares one install
//! ([`install_test_seams`]); the stubs dispatch on their arguments (the opclass
//! OID selects a scenario, which selects an opfamily, which selects catalog
//! rows). Allocating stubs honor the seam contract: their outputs are built in
//! the `Mcx` the caller passes.

use super::*;
use mcx::{slice_in, MemoryContext, PgString};
use std::string::ToString;
use std::sync::Once;
use types_error::make_sqlstate;

// Scenario opclass OIDs.
const OC_VALID: Oid = 1; // valid single-type int4 opclass (all 5 strats + order proc)
const OC_LOOKUP_ERR: Oid = 2; // search_opclass returns Err
const OC_MISSING: Oid = 3; // search_opclass returns Ok(None)
const OC_EMPTY: Oid = 10; // opfamily with no operators/functions
const OC_BAD_PROCNUM: Oid = 11; // support function with invalid number
const OC_ORDER_BY: Oid = 12; // ORDER BY operator

const FULL_OPSET: u64 = (1u64 << BTLessStrategyNumber)
    | (1u64 << BTLessEqualStrategyNumber)
    | (1u64 << BTEqualStrategyNumber)
    | (1u64 << BTGreaterEqualStrategyNumber)
    | (1u64 << BTGreaterStrategyNumber);

fn opclass_form(mcx: Mcx<'_>, opcfamily: Oid) -> PgResult<OpclassForm<'_>> {
    Ok(OpclassForm {
        opcfamily,
        opcintype: 23,
        opckeytype: 0,
        opcname: PgString::from_str_in("int4_ops", mcx)?,
    })
}

fn search_op(strat: i16, lt: Oid, rt: Oid) -> AmopRow {
    AmopRow {
        amopstrategy: strat,
        amoppurpose: b's' as i8,
        amopopr: 7,
        amopsortfamily: 0,
        amoplefttype: lt,
        amoprighttype: rt,
    }
}

fn std_proc(amprocnum: i16) -> AmprocRow {
    AmprocRow {
        amproclefttype: 23,
        amprocrighttype: 23,
        amprocnum,
        amproc: 8,
    }
}

fn stub_search_opclass(mcx: Mcx<'_>, oc: Oid) -> PgResult<Option<OpclassForm<'_>>> {
    match oc {
        OC_VALID => Ok(Some(opclass_form(mcx, 100)?)),
        OC_LOOKUP_ERR => Err(PgError::error("cache lookup failed")),
        OC_MISSING => Ok(None),
        OC_EMPTY => Ok(Some(opclass_form(mcx, 110)?)),
        OC_BAD_PROCNUM => Ok(Some(opclass_form(mcx, 111)?)),
        OC_ORDER_BY => Ok(Some(opclass_form(mcx, 112)?)),
        _ => Ok(None),
    }
}

fn stub_search_amop_list(mcx: Mcx<'_>, fam: Oid) -> PgResult<PgVec<'_, AmopRow>> {
    let all5: Vec<AmopRow> = (1..=5).map(|s| search_op(s, 23, 23)).collect();
    let rows: Vec<AmopRow> = match fam {
        100 => all5,
        112 => vec![AmopRow {
            amoppurpose: b'o' as i8, // ORDER BY
            ..search_op(BTEqualStrategyNumber as i16, 23, 23)
        }],
        _ => Vec::new(),
    };
    slice_in(mcx, &rows)
}

fn stub_search_amproc_list(mcx: Mcx<'_>, fam: Oid) -> PgResult<PgVec<'_, AmprocRow>> {
    let rows: &[AmprocRow] = match fam {
        100 | 112 => &[std_proc(BTORDER_PROC)],
        111 => &[std_proc(9)], // invalid support number
        _ => &[],
    };
    slice_in(mcx, rows)
}

fn stub_get_opfamily_name(
    mcx: Mcx<'_>,
    _fam: Oid,
    _missing_ok: bool,
) -> PgResult<Option<PgString<'_>>> {
    Ok(Some(PgString::from_str_in("integer_ops", mcx)?))
}

// A faithful miniature of identify_opfamily_groups for the stub: group by
// (lefttype, righttype) and set the presence bitmaps, allocating the group list
// in the caller's context.
fn stub_identify_opfamily_groups<'mcx>(
    mcx: Mcx<'mcx>,
    oprs: &[types_amvalidate::AmopRow],
    procs: &[types_amvalidate::AmprocRow],
) -> PgResult<PgVec<'mcx, types_amvalidate::OpFamilyOpFuncGroup>> {
    let mut groups: Vec<types_amvalidate::OpFamilyOpFuncGroup> = Vec::new();
    fn group_for(
        groups: &mut Vec<types_amvalidate::OpFamilyOpFuncGroup>,
        lt: Oid,
        rt: Oid,
    ) -> usize {
        if let Some(i) = groups
            .iter()
            .position(|g| g.lefttype == lt && g.righttype == rt)
        {
            i
        } else {
            groups.push(types_amvalidate::OpFamilyOpFuncGroup {
                lefttype: lt,
                righttype: rt,
                operatorset: 0,
                functionset: 0,
            });
            groups.len() - 1
        }
    }
    for o in oprs {
        let i = group_for(&mut groups, o.amoplefttype, o.amoprighttype);
        groups[i].operatorset |= 1u64 << o.amopstrategy;
    }
    for p in procs {
        let i = group_for(&mut groups, p.amproclefttype, p.amprocrighttype);
        groups[i].functionset |= 1u64 << p.amprocnum;
    }
    slice_in(mcx, &groups)
}

fn stub_format_oid(mcx: Mcx<'_>, oid: Oid) -> PgResult<PgString<'_>> {
    PgString::from_str_in(&oid.to_string(), mcx)
}

fn install_test_seams() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        backend_utils_error_seams::ereport::set(|_err| Ok(()));
        backend_utils_adt_regproc_seams::format_procedure::set(stub_format_oid);
        backend_utils_adt_regproc_seams::format_operator::set(stub_format_oid);
        backend_utils_adt_format_type_seams::format_type_be::set(stub_format_oid);
        backend_utils_cache_lsyscache_seams::get_opfamily_name::set(stub_get_opfamily_name);
        backend_utils_cache_lsyscache_seams::get_opclass_input_type::set(|_oc| Ok(23));
        backend_access_transam_xact_seams::command_counter_increment::set(|| Ok(()));

        backend_utils_cache_syscache_seams::search_opclass::set(stub_search_opclass);
        backend_utils_cache_syscache_seams::search_amop_list::set(stub_search_amop_list);
        backend_utils_cache_syscache_seams::search_amproc_list::set(stub_search_amproc_list);

        backend_access_index_amvalidate_seams::check_amproc_signature::set(
            |_f, _r, _e, _mn, _mx, _a| Ok(true),
        );
        backend_access_index_amvalidate_seams::check_amoptsproc_signature::set(|_f| Ok(true));
        backend_access_index_amvalidate_seams::check_amop_signature::set(|_o, _r, _l, _rt| {
            Ok(true)
        });
        backend_access_index_amvalidate_seams::identify_opfamily_groups::set(
            stub_identify_opfamily_groups,
        );
        // No opclass for opfamily 999; opclass 555 for opfamily 998.
        backend_access_index_amvalidate_seams::opclass_for_family_datatype::set(
            |_am, fam, _typ| Ok(if fam == 998 { 555 } else { 0 }),
        );
    });
}

fn func_member(number: i16, lefttype: Oid, righttype: Oid) -> OpFamilyMember {
    OpFamilyMember {
        is_func: true,
        object: 0,
        number: number as i32,
        lefttype,
        righttype,
        sortfamily: 0,
        ref_is_hard: false,
        ref_is_family: false,
        refobjid: 0,
    }
}

fn op_member(lefttype: Oid, righttype: Oid) -> OpFamilyMember {
    OpFamilyMember {
        is_func: false,
        object: 0,
        number: BTEqualStrategyNumber as i32,
        lefttype,
        righttype,
        sortfamily: 0,
        ref_is_hard: false,
        ref_is_family: false,
        refobjid: 0,
    }
}

#[test]
fn errcode_invalid_object_definition_is_42p17() {
    assert_eq!(ERRCODE_INVALID_OBJECT_DEFINITION, make_sqlstate(*b"42P17"));
}

#[test]
fn full_opset_matches_strategy_constants() {
    // Sanity: the five btree strategy numbers form the 0b111110 mask.
    assert_eq!(FULL_OPSET, 0b111110);
}

// --- btadjustmembers --------------------------------------------------------

#[test]
fn adjustmembers_optional_proc_is_soft_family_dep() {
    // A support proc other than BTORDER_PROC (e.g. sortsupport) is optional, so
    // always a soft family dependency, regardless of left/right type.
    install_test_seams();
    let mut funcs = vec![func_member(BTSORTSUPPORT_PROC, 23, 23)];
    btadjustmembers(999, 0, &mut [], &mut funcs).unwrap();
    assert!(!funcs[0].ref_is_hard);
    assert!(funcs[0].ref_is_family);
    assert_eq!(funcs[0].refobjid, 999);
}

#[test]
fn adjustmembers_order_proc_noncrosstype_with_opclass_is_hard_dep() {
    // BTORDER_PROC is the required comparison proc; not cross-type with a
    // suitable opclass -> hard dependency on the opclass.
    install_test_seams();
    let mut funcs = vec![func_member(BTORDER_PROC, 23, 23)];
    btadjustmembers(998, 0, &mut [], &mut funcs).unwrap();
    assert!(funcs[0].ref_is_hard);
    assert!(!funcs[0].ref_is_family);
    assert_eq!(funcs[0].refobjid, 555);
}

#[test]
fn adjustmembers_crosstype_operator_is_soft_family_dep() {
    install_test_seams();
    let mut ops = vec![op_member(23, 20)];
    btadjustmembers(999, 0, &mut ops, &mut []).unwrap();
    assert!(!ops[0].ref_is_hard);
    assert!(ops[0].ref_is_family);
    assert_eq!(ops[0].refobjid, 999);
}

#[test]
fn adjustmembers_noncrosstype_without_opclass_is_soft_family_dep() {
    install_test_seams();
    let mut ops = vec![op_member(23, 23)];
    btadjustmembers(999, 0, &mut ops, &mut []).unwrap();
    assert!(!ops[0].ref_is_hard);
    assert!(ops[0].ref_is_family);
    assert_eq!(ops[0].refobjid, 999);
}

#[test]
fn adjustmembers_noncrosstype_with_opclass_is_hard_opclass_dep() {
    install_test_seams();
    let mut ops = vec![op_member(23, 23)];
    btadjustmembers(998, 0, &mut ops, &mut []).unwrap();
    assert!(ops[0].ref_is_hard);
    assert!(!ops[0].ref_is_family);
    assert_eq!(ops[0].refobjid, 555);
}

#[test]
fn adjustmembers_uses_provided_opclass_input_type() {
    // When opclassoid is valid, CCI runs and get_opclass_input_type seeds
    // opcintype (23); a same-type member then matches opcintype directly and a
    // hard dependency on the provided opclass is recorded without consulting
    // opclass_for_family_datatype.
    install_test_seams();
    let mut ops = vec![op_member(23, 23)];
    btadjustmembers(999, 42, &mut ops, &mut []).unwrap();
    assert!(ops[0].ref_is_hard);
    assert!(!ops[0].ref_is_family);
    assert_eq!(ops[0].refobjid, 42);
}

// --- btvalidate -------------------------------------------------------------

#[test]
fn validate_valid_int4_opclass_passes() {
    // All five strategy operators and the BTORDER comparison proc on (23, 23):
    // one complete group with the full operator set and the order function.
    install_test_seams();
    let ctx = MemoryContext::new("test");
    assert_eq!(btvalidate(ctx.mcx(), OC_VALID).unwrap(), true);
}

#[test]
fn validate_missing_opclass_group_reports_false() {
    install_test_seams();
    let ctx = MemoryContext::new("test");
    assert_eq!(btvalidate(ctx.mcx(), OC_EMPTY).unwrap(), false);
}

#[test]
fn validate_invalid_support_number_reports_false() {
    // A support function with an out-of-range support number (9) trips the
    // invalid-support-number branch (and `continue`), forcing result = false.
    install_test_seams();
    let ctx = MemoryContext::new("test");
    assert_eq!(btvalidate(ctx.mcx(), OC_BAD_PROCNUM).unwrap(), false);
}

#[test]
fn validate_orderby_operator_reports_false() {
    // An operator with amoppurpose != 's' is an ORDER BY spec, which btree
    // doesn't support -> result false.
    install_test_seams();
    let ctx = MemoryContext::new("test");
    assert_eq!(btvalidate(ctx.mcx(), OC_ORDER_BY).unwrap(), false);
}

#[test]
fn validate_search_opclass_error_propagates() {
    install_test_seams();
    let ctx = MemoryContext::new("test");
    assert!(btvalidate(ctx.mcx(), OC_LOOKUP_ERR).is_err());
}

#[test]
fn validate_missing_opclass_row_raises_cache_lookup_failed() {
    install_test_seams();
    let ctx = MemoryContext::new("test");
    let err = btvalidate(ctx.mcx(), OC_MISSING).unwrap_err();
    assert_eq!(err.message, "cache lookup failed for operator class 3");
}

// --- context accounting -----------------------------------------------------

#[test]
fn validate_all_bytes_return_on_drop() {
    // Everything btvalidate allocates (catalog projections, the projected amv
    // lists, familytypes, the group list) is charged to the caller's context
    // and dropped before return — the context ends every scenario at zero.
    install_test_seams();
    let ctx = MemoryContext::new("per-validate");
    for oc in [OC_VALID, OC_EMPTY, OC_BAD_PROCNUM, OC_ORDER_BY] {
        let _ = btvalidate(ctx.mcx(), oc).unwrap();
        assert_eq!(ctx.used(), 0, "opclass {oc}: bytes left behind");
    }
    assert!(ctx.peak() > 0, "the valid scenario must have allocated");
}
