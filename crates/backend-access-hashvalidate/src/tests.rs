//! Unit tests for the `hashvalidate.c` port.
//!
//! Seam slots are process-global `OnceLock`s, so every test shares one install
//! ([`install_test_seams`]); the stubs dispatch on their arguments (the opclass
//! OID selects a scenario, which selects an opfamily, which selects catalog
//! rows). Allocating stubs honor the seam contract: their outputs are built in
//! the `Mcx` the caller passes, so the context-accounting tests below see
//! exactly what a real owner crate would charge.

use super::*;
use mcx::{slice_in, MemoryContext, PgString};
use std::string::ToString;
use std::sync::Once;
use types_error::make_sqlstate;

// Scenario opclass OIDs.
const OC_VALID: Oid = 1; // valid single-type int4 opclass
const OC_LOOKUP_ERR: Oid = 2; // search_opclass returns Err
const OC_MISSING: Oid = 3; // search_opclass returns Ok(None)
const OC_EMPTY: Oid = 10; // opfamily with no operators/functions
const OC_BAD_PROCNUM: Oid = 11; // support function with invalid number
const OC_ORDER_BY: Oid = 12; // ORDER BY operator

fn opclass_form(mcx: Mcx<'_>, opcfamily: Oid) -> PgResult<OpclassForm<'_>> {
    Ok(OpclassForm {
        opcfamily,
        opcintype: 23,
        opcname: PgString::from_str_in("int4_ops", mcx)?,
    })
}

fn eq_op() -> AmopRow {
    AmopRow {
        amopstrategy: HTEqualStrategyNumber as i16,
        amoppurpose: b's' as i8,
        amopopr: 7,
        amopsortfamily: 0,
        amoplefttype: 23,
        amoprighttype: 23,
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

// Lifetime-generic seam stubs (the slots store higher-ranked `for<'mcx> fn`
// pointers, so these are plain fn items).

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
    let rows: &[AmopRow] = match fam {
        100 => &[eq_op()],
        112 => &[AmopRow {
            amoppurpose: b'o' as i8, // ORDER BY
            ..eq_op()
        }],
        _ => &[],
    };
    slice_in(mcx, rows)
}

fn stub_search_amproc_list(mcx: Mcx<'_>, fam: Oid) -> PgResult<PgVec<'_, AmprocRow>> {
    let rows: &[AmprocRow] = match fam {
        100 | 112 => &[std_proc(HASHSTANDARD_PROC as i16)],
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
// (lefttype, righttype) and set the presence bitmaps, allocating the group
// list in the caller's context.
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

// Allocating format-name stubs honor the seam contract: output built in the
// caller-passed `Mcx`.
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
        number,
        lefttype,
        righttype,
        ref_is_hard: false,
        ref_is_family: false,
        refobjid: 0,
    }
}

fn op_member(lefttype: Oid, righttype: Oid) -> OpFamilyMember {
    OpFamilyMember {
        is_func: false,
        number: HTEqualStrategyNumber as i16,
        lefttype,
        righttype,
        ref_is_hard: false,
        ref_is_family: false,
        refobjid: 0,
    }
}

#[test]
fn errcode_invalid_object_definition_is_42p17() {
    assert_eq!(ERRCODE_INVALID_OBJECT_DEFINITION, make_sqlstate(*b"42P17"));
}

// --- hashadjustmembers ------------------------------------------------------

#[test]
fn adjustmembers_optional_proc_is_soft_family_dep() {
    // HASHOPTIONS_PROC (not HASHSTANDARD_PROC) is an optional support proc:
    // always a soft family dependency, regardless of left/right type.
    // opclassoid is invalid (0), so no CCI / opclass lookup happens.
    install_test_seams();
    let mut funcs = vec![func_member(HASHOPTIONS_PROC as i16, 23, 23)];
    hashadjustmembers(999, 0, &mut [], &mut funcs).unwrap();
    assert!(!funcs[0].ref_is_hard);
    assert!(funcs[0].ref_is_family);
    assert_eq!(funcs[0].refobjid, 999);
}

#[test]
fn adjustmembers_crosstype_operator_is_soft_family_dep() {
    // A cross-type operator (lefttype != righttype) is always a soft family
    // dependency.
    install_test_seams();
    let mut ops = vec![op_member(23, 20)];
    hashadjustmembers(999, 0, &mut ops, &mut []).unwrap();
    assert!(!ops[0].ref_is_hard);
    assert!(ops[0].ref_is_family);
    assert_eq!(ops[0].refobjid, 999);
}

#[test]
fn adjustmembers_noncrosstype_without_opclass_is_soft_family_dep() {
    // Not cross-type, no suitable opclass (family 999 has none): fall back to
    // a soft dependency on the opfamily.
    install_test_seams();
    let mut ops = vec![op_member(23, 23)];
    hashadjustmembers(999, 0, &mut ops, &mut []).unwrap();
    assert!(!ops[0].ref_is_hard);
    assert!(ops[0].ref_is_family);
    assert_eq!(ops[0].refobjid, 999);
}

#[test]
fn adjustmembers_noncrosstype_with_opclass_is_hard_opclass_dep() {
    // Not cross-type, and family 998 has a suitable opclass (555) for the
    // datatype: hard dependency on that opclass.
    install_test_seams();
    let mut ops = vec![op_member(23, 23)];
    hashadjustmembers(998, 0, &mut ops, &mut []).unwrap();
    assert!(ops[0].ref_is_hard);
    assert!(!ops[0].ref_is_family);
    assert_eq!(ops[0].refobjid, 555);
}

#[test]
fn adjustmembers_uses_provided_opclass_input_type() {
    // When opclassoid is valid, CCI runs and get_opclass_input_type seeds
    // opcintype (23); a same-type member then matches opcintype directly and a
    // hard dependency on the provided opclass is recorded without consulting
    // opclass_for_family_datatype (whose stub for family 999 would return
    // InvalidOid and force a soft family dep instead).
    install_test_seams();
    let mut ops = vec![op_member(23, 23)];
    hashadjustmembers(999, 42, &mut ops, &mut []).unwrap();
    assert!(ops[0].ref_is_hard);
    assert!(!ops[0].ref_is_family);
    assert_eq!(ops[0].refobjid, 42);
}

// --- hashvalidate -----------------------------------------------------------

#[test]
fn validate_valid_int4_opclass_passes() {
    // One standard hash function and one `=` operator on (23, 23): exactly one
    // group with the equality strategy present; everything checks out.
    install_test_seams();
    let ctx = MemoryContext::new("test");
    assert_eq!(hashvalidate(ctx.mcx(), OC_VALID).unwrap(), true);
}

#[test]
fn validate_missing_opclass_group_reports_false() {
    // The opclass's own (opcintype, opcintype) group is absent from the group
    // list, so the missing-operator(s) report fires -> result false.
    install_test_seams();
    let ctx = MemoryContext::new("test");
    assert_eq!(hashvalidate(ctx.mcx(), OC_EMPTY).unwrap(), false);
}

#[test]
fn validate_invalid_support_number_reports_false() {
    // A support function with an out-of-range support number (9) trips the
    // invalid-support-number branch (and `continue`), forcing result = false.
    install_test_seams();
    let ctx = MemoryContext::new("test");
    assert_eq!(hashvalidate(ctx.mcx(), OC_BAD_PROCNUM).unwrap(), false);
}

#[test]
fn validate_orderby_operator_reports_false() {
    // An operator with amoppurpose != 's' is an ORDER BY spec, which hash
    // doesn't support -> result false.
    install_test_seams();
    let ctx = MemoryContext::new("test");
    assert_eq!(hashvalidate(ctx.mcx(), OC_ORDER_BY).unwrap(), false);
}

#[test]
fn validate_search_opclass_error_propagates() {
    // An error raised by the catalog substrate propagates on Err.
    install_test_seams();
    let ctx = MemoryContext::new("test");
    assert!(hashvalidate(ctx.mcx(), OC_LOOKUP_ERR).is_err());
}

#[test]
fn validate_missing_opclass_row_raises_cache_lookup_failed() {
    // An invalid CLAOID tuple is the C elog(ERROR, "cache lookup failed for
    // operator class %u"), raised in-crate.
    install_test_seams();
    let ctx = MemoryContext::new("test");
    let err = hashvalidate(ctx.mcx(), OC_MISSING).unwrap_err();
    assert_eq!(err.message, "cache lookup failed for operator class 3");
}

// --- context accounting -----------------------------------------------------

#[test]
fn catalog_projection_accounting_is_exact() {
    // The syscache projections are charged to the passed context, byte for
    // byte: the opclass form's name string plus each member-row list's
    // capacity.
    install_test_seams();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();

    let form = backend_utils_cache_syscache_seams::search_opclass::call(mcx, OC_VALID)
        .unwrap()
        .unwrap();
    assert_eq!(ctx.used(), form.opcname.capacity_bytes());

    let oprs = backend_utils_cache_syscache_seams::search_amop_list::call(mcx, 100).unwrap();
    let procs = backend_utils_cache_syscache_seams::search_amproc_list::call(mcx, 100).unwrap();
    assert_eq!(
        ctx.used(),
        form.opcname.capacity_bytes()
            + oprs.capacity() * core::mem::size_of::<AmopRow>()
            + procs.capacity() * core::mem::size_of::<AmprocRow>()
    );
}

#[test]
fn validate_all_bytes_return_on_drop() {
    // Everything hashvalidate allocates (catalog projections, the
    // hashable-types list, the group list) is charged to the caller's context
    // and dropped before return — the context ends every scenario at zero.
    install_test_seams();
    let ctx = MemoryContext::new("per-validate");
    for oc in [OC_VALID, OC_EMPTY, OC_BAD_PROCNUM, OC_ORDER_BY] {
        let _ = hashvalidate(ctx.mcx(), oc).unwrap();
        assert_eq!(ctx.used(), 0, "opclass {oc}: bytes left behind");
    }
    assert!(ctx.peak() > 0, "the valid scenario must have allocated");
}
