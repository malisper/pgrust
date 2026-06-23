//! Unit tests for the `brin_validate.c` port.
//!
//! Seam slots are process-global `OnceLock`s, so every test shares one install
//! ([`install_test_seams`]); the stubs dispatch on their arguments (the opclass
//! OID selects a scenario, which selects an opfamily, which selects catalog
//! rows). Allocating stubs honor the seam contract: their outputs are built in
//! the `Mcx` the caller passes.

use super::*;
use ::mcx::{slice_in, MemoryContext, PgString};
use std::string::ToString;
use std::sync::Once;

// Scenario opclass OIDs.
const OC_VALID: Oid = 1; // valid single-type int4 opclass (mandatory procs + 5 strats)
const OC_MISSING: Oid = 2; // search_opclass returns Ok(None)
const OC_BAD_PROCNUM: Oid = 3; // support function with invalid number
const OC_ORDER_BY: Oid = 4; // ORDER BY operator

// All five minmax strategy numbers (1..=5).
const FULL_OPSET: u64 = (1u64 << 1) | (1u64 << 2) | (1u64 << 3) | (1u64 << 4) | (1u64 << 5);

fn opclass_form(mcx: Mcx<'_>, opcfamily: Oid) -> PgResult<OpclassForm<'_>> {
    Ok(OpclassForm {
        opcfamily,
        opcintype: 23,
        opckeytype: 0,
        opcname: PgString::from_str_in("int4_minmax_ops", mcx)?,
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
        OC_MISSING => Ok(None),
        OC_BAD_PROCNUM => Ok(Some(opclass_form(mcx, 111)?)),
        OC_ORDER_BY => Ok(Some(opclass_form(mcx, 112)?)),
        _ => Ok(None),
    }
}

fn stub_search_amop_list(mcx: Mcx<'_>, fam: Oid) -> PgResult<PgVec<'_, AmopRow>> {
    let all5: Vec<AmopRow> = (1..=5).map(|s| search_op(s, 23, 23)).collect();
    let rows: Vec<AmopRow> = match fam {
        100 | 111 => all5,
        112 => vec![AmopRow {
            amoppurpose: b'o' as i8, // ORDER BY
            ..search_op(1, 23, 23)
        }],
        _ => Vec::new(),
    };
    slice_in(mcx, &rows)
}

fn stub_search_amproc_list(mcx: Mcx<'_>, fam: Oid) -> PgResult<PgVec<'_, AmprocRow>> {
    let mandatory: Vec<AmprocRow> = (1..=4).map(std_proc).collect();
    let rows: Vec<AmprocRow> = match fam {
        100 | 112 => mandatory,
        111 => vec![std_proc(9)], // invalid support number
        _ => Vec::new(),
    };
    slice_in(mcx, &rows)
}

fn stub_get_opfamily_name(
    mcx: Mcx<'_>,
    _fam: Oid,
    _missing_ok: bool,
) -> PgResult<Option<PgString<'_>>> {
    Ok(Some(PgString::from_str_in("integer_minmax_ops", mcx)?))
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
        error_seams::ereport::set(|_err| Ok(()));
        regproc_seams::format_procedure::set(stub_format_oid);
        regproc_seams::format_operator::set(stub_format_oid);
        format_type_seams::format_type_be::set(stub_format_oid);
        lsyscache_seams::get_opfamily_name::set(stub_get_opfamily_name);

        syscache_seams::search_opclass::set(stub_search_opclass);
        syscache_seams::search_amop_list::set(stub_search_amop_list);
        syscache_seams::search_amproc_list::set(stub_search_amproc_list);

        amvalidate_seams::check_amproc_signature::set(
            |_f, _r, _e, _mn, _mx, _a| Ok(true),
        );
        amvalidate_seams::check_amoptsproc_signature::set(|_f| Ok(true));
        amvalidate_seams::check_amop_signature::set(|_o, _r, _l, _rt| {
            Ok(true)
        });
        amvalidate_seams::identify_opfamily_groups::set(
            stub_identify_opfamily_groups,
        );
    });
}

#[test]
fn valid_opclass_passes() {
    install_test_seams();
    // Sanity: the mandatory-proc bitmask used below.
    assert_eq!(
        FULL_OPSET,
        (1u64 << 1) | (1u64 << 2) | (1u64 << 3) | (1u64 << 4) | (1u64 << 5)
    );
    let ctx = MemoryContext::new("test");
    let ok = brinvalidate(ctx.mcx(), OC_VALID).expect("brinvalidate should not raise");
    assert!(ok, "a complete minmax-style opclass should validate");
}

#[test]
fn missing_opclass_raises() {
    install_test_seams();
    let ctx = MemoryContext::new("test");
    let res = brinvalidate(ctx.mcx(), OC_MISSING);
    assert!(res.is_err(), "a cache miss on the opclass must raise (Err)");
}

#[test]
fn invalid_support_number_fails() {
    install_test_seams();
    let ctx = MemoryContext::new("test");
    // proc number 9 is below BRIN_FIRST_OPTIONAL_PROCNUM (11): INFO + result=false.
    let ok = brinvalidate(ctx.mcx(), OC_BAD_PROCNUM).expect("INFO never raises");
    assert!(
        !ok,
        "an invalid support number should make the validator return false"
    );
}

#[test]
fn order_by_operator_fails() {
    install_test_seams();
    let ctx = MemoryContext::new("test");
    // amoppurpose = 'o' (ORDER BY): brin rejects it, result=false.
    let ok = brinvalidate(ctx.mcx(), OC_ORDER_BY).expect("INFO never raises");
    assert!(
        !ok,
        "an ORDER BY operator should make the validator return false"
    );
}
