//! `indexcmds.c` — operator-class resolution:
//! [`ResolveOpClass`] and [`GetOperatorFromCompareType`].
//!
//! `GetDefaultOpClass` is a home divergence already merged into the lsyscache
//! owner; [`ResolveOpClass`] reaches it through
//! `backend_utils_cache_lsyscache_seams::get_default_opclass`. The
//! exact / binary-compatible / preferred-type tiebreak therefore lives there,
//! not here.

use alloc::format;
use alloc::string::String;

use mcx::{Mcx, MemoryContext};

use types_amapi::{CompareType, COMPARE_CONTAINED_BY, COMPARE_EQ, COMPARE_OVERLAP};
use types_core::primitive::Oid;
use types_core::{InvalidOid, OidIsValid};
use types_error::PgResult;
use types_nodes::nodes::NodePtr;
use types_scan::scankey::{InvalidStrategy, StrategyNumber};

use mcx::PgVec;

use backend_utils_error::ereport;
use types_error::ERROR;

use backend_access_index_amapi::IndexAmTranslateCompareType;
use backend_catalog_namespace::{
    DeconstructQualifiedName, LookupExplicitNamespace, NameListToString, OpclassnameGetOpcid,
};
use backend_parser_coerce::IsBinaryCoercible;

use backend_utils_adt_format_type_seams as formattype_seam;
use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_utils_cache_syscache_seams as syscache;

use crate::name_list;

// ---------------------------------------------------------------------------
// ResolveOpClass  (indexcmds.c:2259-2336)
// ---------------------------------------------------------------------------

/// `ResolveOpClass(opclass, attrType, accessMethodName, accessMethodId)`.
///
/// Resolves a possibly-defaulted operator-class specification. `opclass` is the
/// (possibly schema-qualified) opclass name as a list of `String` value nodes;
/// an empty list means "use the default for the type". Used for both index and
/// partition-key definitions.
pub fn ResolveOpClass<'mcx>(
    mcx: Mcx<'mcx>,
    opclass: &PgVec<'mcx, NodePtr<'mcx>>,
    attr_type: Oid,
    access_method_name: &str,
    access_method_id: Oid,
) -> PgResult<Oid> {
    if opclass.is_empty() {
        // no operator class specified, so find the default
        let op_class_id = lsyscache::get_default_opclass::call(attr_type, access_method_id)?;
        if !OidIsValid(op_class_id) {
            return Err(ereport(ERROR)
                .errcode(types_error::ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!(
                    "data type {} has no default operator class for access method \"{}\"",
                    formattype_seam::format_type_be_owned::call(attr_type)?,
                    access_method_name
                ))
                .errhint(
                    "You must specify an operator class for the index or define a default operator class for the data type.",
                )
                .into_error());
        }
        return Ok(op_class_id);
    }

    // Specific opclass name given, so look up the opclass.

    // deconstruct the name list
    let names = name_list(opclass);
    let (schemaname, opcname) = DeconstructQualifiedName(mcx, &names)?;
    let opcname = opcname.to_string();

    // op_class_id is determined by either the explicit-schema lookup or the
    // search-path lookup; op_input_type is then read from pg_opclass.
    let op_class_id: Oid = if let Some(schemaname) = schemaname {
        // Look in specific schema only.
        let namespace_id = LookupExplicitNamespace(schemaname, false)?;
        syscache::get_opclass_oid::call(access_method_id, &opcname, namespace_id)?
    } else {
        // Unqualified opclass name, so search the search path.
        let op_class_id = OpclassnameGetOpcid(mcx, access_method_id, &opcname)?;
        if !OidIsValid(op_class_id) {
            return Err(ereport(ERROR)
                .errcode(types_error::ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!(
                    "operator class \"{opcname}\" does not exist for access method \"{access_method_name}\""
                ))
                .into_error());
        }
        op_class_id
    };

    // Read the opclass row for its input type (and to validate existence in the
    // explicit-schema path).
    let opcform = match syscache::search_opclass::call(mcx, op_class_id)? {
        Some(form) if OidIsValid(op_class_id) => form,
        _ => {
            return Err(ereport(ERROR)
                .errcode(types_error::ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!(
                    "operator class \"{}\" does not exist for access method \"{}\"",
                    NameListToString(mcx, &names)?.as_str(),
                    access_method_name
                ))
                .into_error());
        }
    };
    let op_input_type = opcform.opcintype;

    // Verify that the index operator class accepts this datatype. Note we will
    // accept binary compatibility.
    if !IsBinaryCoercible(attr_type, op_input_type)? {
        return Err(ereport(ERROR)
            .errcode(types_error::ERRCODE_DATATYPE_MISMATCH)
            .errmsg(format!(
                "operator class \"{}\" does not accept data type {}",
                NameListToString(mcx, &names)?.as_str(),
                formattype_seam::format_type_be_owned::call(attr_type)?
            ))
            .into_error());
    }

    Ok(op_class_id)
}

// ---------------------------------------------------------------------------
// GetOperatorFromCompareType  (indexcmds.c:2446-2493)
// ---------------------------------------------------------------------------

/// `GetOperatorFromCompareType(opclass, rhstype, cmptype, opid, strat)`.
///
/// Finds an operator from a [`CompareType`]. Used for temporal index
/// constraints (and other temporal features) to look up equality and overlaps
/// operators. Asks an opclass support function to translate from the compare
/// type to the internal strategy numbers. Raises ERROR on search failure.
pub fn GetOperatorFromCompareType(
    opclass: Oid,
    mut rhstype: Oid,
    cmptype: CompareType,
    opid: &mut Oid,
    strat: &mut StrategyNumber,
) -> PgResult<()> {
    debug_assert!(
        cmptype == COMPARE_EQ || cmptype == COMPARE_OVERLAP || cmptype == COMPARE_CONTAINED_BY
    );

    let amid = lsyscache::get_opclass_method::call(opclass)?;

    *opid = InvalidOid;

    // We need opfamily/opcintype for the error messages even on the failure
    // path, so capture them; the C reads them from the same lookup.
    let mut opfamily = InvalidOid;
    let mut opcintype = InvalidOid;

    if let Some((of, oc)) = lsyscache::get_opclass_opfamily_and_input_type::call(opclass)? {
        opfamily = of;
        opcintype = oc;

        // Ask the index AM to translate to its internal stratnum.
        *strat = IndexAmTranslateCompareType(cmptype, amid, opfamily, true)?;
        if *strat == InvalidStrategy {
            return Err(compare_type_error(cmptype, opcintype, opfamily, amid, true)?);
        }

        // We parameterize rhstype so foreign keys can ask for a <@ operator
        // whose rhs matches the aggregate function. For example range_agg
        // returns anymultirange.
        if !OidIsValid(rhstype) {
            rhstype = opcintype;
        }
        *opid = lsyscache::get_opfamily_member::call(opfamily, opcintype, rhstype, *strat as i16)?;
    }

    if !OidIsValid(*opid) {
        return Err(compare_type_error(cmptype, opcintype, opfamily, amid, false)?);
    }

    Ok(())
}

/// The two ereports in `GetOperatorFromCompareType` share the same cmptype-keyed
/// errmsg plus an errdetail; `translate` selects which errdetail.
fn compare_type_error(
    cmptype: CompareType,
    opcintype: Oid,
    opfamily: Oid,
    amid: Oid,
    translate: bool,
) -> PgResult<backend_utils_error::PgError> {
    let typ = formattype_seam::format_type_be_owned::call(opcintype)?;
    let msg = if cmptype == COMPARE_EQ {
        format!("could not identify an equality operator for type {typ}")
    } else if cmptype == COMPARE_OVERLAP {
        format!("could not identify an overlaps operator for type {typ}")
    } else if cmptype == COMPARE_CONTAINED_BY {
        format!("could not identify a contained-by operator for type {typ}")
    } else {
        String::new()
    };
    let opfname = get_opfamily_name_str(opfamily)?;
    let amname = get_am_name_str(amid)?;
    let cmptype_n = cmptype as u32;
    let detail = if translate {
        format!(
            "Could not translate compare type {cmptype_n} for operator family \"{opfname}\" of access method \"{amname}\"."
        )
    } else {
        format!(
            "There is no suitable operator in operator family \"{opfname}\" for access method \"{amname}\"."
        )
    };
    Ok(ereport(ERROR)
        .errcode(types_error::ERRCODE_UNDEFINED_OBJECT)
        .errmsg(msg)
        .errdetail(detail)
        .into_error())
}

/// `get_opfamily_name(opfamily, false)` rendered as an owned `String` for the
/// error-detail interpolation (the seam returns an `mcx`-scoped string; we copy
/// it out using a transient context borrow).
fn get_opfamily_name_str(opfamily: Oid) -> PgResult<String> {
    let tmp = MemoryContext::new("indexcmds:get_opfamily_name");
    let name = lsyscache::get_opfamily_name::call(tmp.mcx(), opfamily, false)?
        .map(|s| s.as_str().to_string())
        .unwrap_or_default();
    Ok(name)
}

/// `get_am_name(amid)` rendered as an owned `String`, like
/// [`get_opfamily_name_str`].
fn get_am_name_str(amid: Oid) -> PgResult<String> {
    let tmp = MemoryContext::new("indexcmds:get_am_name");
    let name = lsyscache::get_am_name::call(tmp.mcx(), amid)?
        .map(|s| s.as_str().to_string())
        .unwrap_or_default();
    Ok(name)
}
