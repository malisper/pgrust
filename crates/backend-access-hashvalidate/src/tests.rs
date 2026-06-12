//! Unit tests for the `hashvalidate.c` port.
//!
//! Seam slots are process-global `OnceLock`s, so every test shares one install
//! ([`install_test_seams`]); the stubs dispatch on their arguments (the opclass
//! OID selects a scenario, which selects an opfamily, which selects catalog
//! rows).

use super::*;
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

fn opclass_form(opcfamily: Oid) -> OpclassForm {
    OpclassForm {
        opcfamily,
        opcintype: 23,
        opcname: String::from("int4_ops"),
    }
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

fn install_test_seams() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        backend_utils_error_seams::ereport::set(|_err| Ok(()));
        backend_utils_adt_regproc_seams::format_procedure::set(|oid| Ok(oid.to_string()));
        backend_utils_adt_regproc_seams::format_operator::set(|oid| Ok(oid.to_string()));
        backend_utils_adt_format_type_seams::format_type_be::set(|oid| Ok(oid.to_string()));
        backend_utils_cache_lsyscache_seams::get_opfamily_name::set(|_fam, _missing_ok| {
            Ok(Some(String::from("integer_ops")))
        });
        backend_utils_cache_lsyscache_seams::get_opclass_input_type::set(|_oc| Ok(23));
        backend_access_transam_xact_seams::command_counter_increment::set(|| Ok(()));

        backend_utils_cache_syscache_seams::search_opclass::set(|oc| match oc {
            OC_VALID => Ok(Some(opclass_form(100))),
            OC_LOOKUP_ERR => Err(PgError::error("cache lookup failed")),
            OC_MISSING => Ok(None),
            OC_EMPTY => Ok(Some(opclass_form(110))),
            OC_BAD_PROCNUM => Ok(Some(opclass_form(111))),
            OC_ORDER_BY => Ok(Some(opclass_form(112))),
            _ => Ok(None),
        });
        backend_utils_cache_syscache_seams::search_amop_list::set(|fam| {
            Ok(match fam {
                100 => vec![eq_op()],
                112 => vec![AmopRow {
                    amoppurpose: b'o' as i8, // ORDER BY
                    ..eq_op()
                }],
                _ => Vec::new(),
            })
        });
        backend_utils_cache_syscache_seams::search_amproc_list::set(|fam| {
            Ok(match fam {
                100 | 112 => vec![std_proc(HASHSTANDARD_PROC as i16)],
                111 => vec![std_proc(9)], // invalid support number
                _ => Vec::new(),
            })
        });

        backend_access_index_amvalidate_seams::check_amproc_signature::set(
            |_f, _r, _e, _mn, _mx, _a| Ok(true),
        );
        backend_access_index_amvalidate_seams::check_amoptsproc_signature::set(|_f| Ok(true));
        backend_access_index_amvalidate_seams::check_amop_signature::set(|_o, _r, _l, _rt| {
            Ok(true)
        });
        // A faithful miniature of identify_opfamily_groups for the stub: group
        // by (lefttype, righttype) and set the presence bitmaps.
        backend_access_index_amvalidate_seams::identify_opfamily_groups::set(|oprs, procs| {
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
            groups
        });
        // No opclass for opfamily 999; opclass 555 for opfamily 998.
        backend_access_index_amvalidate_seams::opclass_for_family_datatype::set(
            |_am, fam, _typ| if fam == 998 { 555 } else { 0 },
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
    assert_eq!(hashvalidate(OC_VALID).unwrap(), true);
}

#[test]
fn validate_missing_opclass_group_reports_false() {
    // The opclass's own (opcintype, opcintype) group is absent from the group
    // list, so the missing-operator(s) report fires -> result false.
    install_test_seams();
    assert_eq!(hashvalidate(OC_EMPTY).unwrap(), false);
}

#[test]
fn validate_invalid_support_number_reports_false() {
    // A support function with an out-of-range support number (9) trips the
    // invalid-support-number branch (and `continue`), forcing result = false.
    install_test_seams();
    assert_eq!(hashvalidate(OC_BAD_PROCNUM).unwrap(), false);
}

#[test]
fn validate_orderby_operator_reports_false() {
    // An operator with amoppurpose != 's' is an ORDER BY spec, which hash
    // doesn't support -> result false.
    install_test_seams();
    assert_eq!(hashvalidate(OC_ORDER_BY).unwrap(), false);
}

#[test]
fn validate_search_opclass_error_propagates() {
    // An error raised by the catalog substrate propagates on Err.
    install_test_seams();
    assert!(hashvalidate(OC_LOOKUP_ERR).is_err());
}

#[test]
fn validate_missing_opclass_row_raises_cache_lookup_failed() {
    // An invalid CLAOID tuple is the C elog(ERROR, "cache lookup failed for
    // operator class %u"), raised in-crate.
    install_test_seams();
    let err = hashvalidate(OC_MISSING).unwrap_err();
    assert_eq!(err.message, "cache lookup failed for operator class 3");
}
