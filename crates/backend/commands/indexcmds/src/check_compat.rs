//! `indexcmds.c` — [`CheckIndexCompatible`]: determine whether an existing index
//! definition is compatible enough with a new `IndexStmt` that the index's
//! storage can be reused (the `ALTER TABLE ... ALTER COLUMN TYPE` rebuild path,
//! `TryReuseIndex`).
//!
//! Branch order, casts, error codes / messages match the C source
//! (`CheckIndexCompatible`, indexcmds.c).

use alloc::format;
use alloc::vec;
use alloc::vec::Vec;

use ::mcx::Mcx;

use ::types_core::primitive::Oid;
use ::types_core::InvalidOid;
use ::types_error::{PgResult, ERROR};

use ::types_storage::lock::{AccessShareLock, NoLock};

use ::index_amapi::GetIndexAmRoutineByAmId;
use indexam_seams as indexam_seam;
use table_seams as table_seam;
use ::index::IndexGetRelation;
use ::utils_error::ereport;

use ::nodes_core::makefuncs::make_index_info;
use lsyscache_seams as lsyscache;
use syscache_seams as syscache;

use crate::vec_oid;
use crate::ComputeIndexAttrs;

// IsPolymorphicTypeFamily1 / Family2 OIDs (catalog/pg_type.h:313).
use ::types_tuple::heaptuple::{
    ANYARRAYOID, ANYCOMPATIBLEARRAYOID, ANYCOMPATIBLEMULTIRANGEOID, ANYCOMPATIBLENONARRAYOID,
    ANYCOMPATIBLEOID, ANYCOMPATIBLERANGEOID, ANYELEMENTOID, ANYENUMOID, ANYMULTIRANGEOID,
    ANYNONARRAYOID, ANYRANGEOID,
};

/// `IsPolymorphicType(typid)` (catalog/pg_type.h:313): a pure OID comparison.
fn is_polymorphic_type(typid: Oid) -> bool {
    typid == ANYELEMENTOID
        || typid == ANYARRAYOID
        || typid == ANYNONARRAYOID
        || typid == ANYENUMOID
        || typid == ANYRANGEOID
        || typid == ANYMULTIRANGEOID
        || typid == ANYCOMPATIBLEOID
        || typid == ANYCOMPATIBLEARRAYOID
        || typid == ANYCOMPATIBLENONARRAYOID
        || typid == ANYCOMPATIBLERANGEOID
        || typid == ANYCOMPATIBLEMULTIRANGEOID
}

/// `CheckIndexCompatible(oldId, accessMethodName, attributeList,
/// exclusionOpNames, isWithoutOverlaps)` (indexcmds.c).
///
/// Determine whether an existing index (`oldId`) is compatible — to the extent
/// that its physical storage can be reused — with the new index that would be
/// produced by the given `IndexStmt` definition. Used by `TryReuseIndex` during
/// an `ALTER TABLE ... ALTER COLUMN TYPE` rebuild so a no-rewrite column change
/// keeps the index's relfilenode (and tablespace) intact.
///
/// We don't assess expressions or predicates; an index with any of those is
/// treated as incompatible. Any change in opclass, collation, opclass options,
/// or (for polymorphic opclasses) the column type breaks compatibility.
pub fn CheckIndexCompatible<'mcx>(
    mcx: Mcx<'mcx>,
    old_id: Oid,
    access_method_name: &str,
    attribute_list: &[&nodes::ddlnodes::IndexElem<'mcx>],
    exclusion_op_names: Option<&::mcx::PgVec<'mcx, nodes::nodes::NodePtr<'mcx>>>,
    is_without_overlaps: bool,
) -> PgResult<bool> {
    // Caller should already have the relation locked in some way.
    let relation_id = IndexGetRelation(old_id, false)?;

    // We can pretend isconstraint = false unconditionally.  It only serves to
    // decide the text of an error message that should never happen for us.
    let isconstraint = false;

    let number_of_attributes = attribute_list.len() as i32;
    debug_assert!(number_of_attributes > 0);
    debug_assert!(number_of_attributes <= crate::INDEX_MAX_KEYS);

    // look up the access method
    let Some(am_info) = syscache::search_am_by_name::call(mcx, access_method_name)? else {
        return Err(ereport(ERROR)
            .errcode(::types_error::ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!(
                "access method \"{access_method_name}\" does not exist"
            ))
            .into_error());
    };
    let access_method_id = am_info.oid;
    let am_routine = GetIndexAmRoutineByAmId(access_method_id)?;

    let amcanorder = am_routine.amcanorder;
    let amsummarizing = am_routine.amsummarizing;

    // Compute the operator classes, collations, and exclusion operators for the
    // new index, so we can test whether it's compatible with the existing one.
    // Note that ComputeIndexAttrs might fail here, but that's OK: DefineIndex
    // would have failed later.  Our attributeList contains only key attributes,
    // thus we're filling ii_NumIndexAttrs and ii_NumIndexKeyAttrs with same
    // value.
    let mut index_info = make_index_info(
        number_of_attributes,
        number_of_attributes,
        access_method_id,
        (),
        (),
        false,
        false,
        false,
        false,
        amsummarizing,
        is_without_overlaps,
    );
    index_info.ii_Context = Some(mcx);

    let mut type_ids = vec_oid(number_of_attributes);
    let mut collation_ids = vec_oid(number_of_attributes);
    let mut opclass_ids = vec_oid(number_of_attributes);
    let mut opclass_options: Vec<::types_tuple::Datum<'mcx>> = (0..number_of_attributes)
        .map(|_| ::types_tuple::Datum::null())
        .collect();
    let mut col_options = vec![0i16; number_of_attributes as usize];

    // Open the table so ComputeIndexAttrs can resolve column attnums/types.
    let rel = table_seam::table_open::call(mcx, relation_id, NoLock)?;
    let mut save_nestlevel = 0i32;
    ComputeIndexAttrs(
        mcx,
        &mut index_info,
        &rel,
        &mut type_ids,
        &mut collation_ids,
        &mut opclass_ids,
        &mut opclass_options,
        &mut col_options,
        attribute_list,
        exclusion_op_names,
        relation_id,
        access_method_name,
        access_method_id,
        amcanorder,
        isconstraint,
        is_without_overlaps,
        InvalidOid,
        0,
        &mut save_nestlevel,
    )?;
    rel.close(NoLock)?;

    // Get the soon-obsolete pg_index tuple.
    let Some(old_index) = syscache::search_pg_index_info::call(mcx, old_id)? else {
        return Err(ereport(ERROR)
            .errmsg_internal(format!("cache lookup failed for index {old_id}"))
            .into_error());
    };

    // We don't assess expressions or predicates; assume incompatibility. Also,
    // if the index is invalid for any reason, treat it as incompatible.
    //   heap_attisnull(indpred) && heap_attisnull(indexprs) && indisvalid
    let has_pred = syscache::pg_index_has_predicate::call(old_id)?.unwrap_or(false);
    let has_exprs = match syscache::pg_index_tid_and_hasexprs::call(old_id)? {
        Some((_, hx)) => hx,
        None => false,
    };
    if has_pred || has_exprs || !old_index.indisvalid {
        return Ok(false);
    }

    // Any change in operator class or collation breaks compatibility.
    let old_natts = old_index.indnkeyatts as usize;
    debug_assert!(old_natts == number_of_attributes as usize);

    let mut ret = true;
    for i in 0..old_natts {
        if old_index.indclass[i] != opclass_ids[i] || old_index.indcollation[i] != collation_ids[i]
        {
            ret = false;
            break;
        }
    }

    if !ret {
        return Ok(false);
    }

    // For polymorphic opcintype, column type changes break compatibility.
    // irel = index_open(oldId, AccessShareLock); caller probably has a lock.
    let irel = indexam_seam::index_open::call(mcx, old_id, AccessShareLock)?;
    for i in 0..old_natts {
        let opcintype = lsyscache::get_opclass_input_type::call(opclass_ids[i])?;
        if is_polymorphic_type(opcintype) && irel.rd_att.attr(i).atttypid != type_ids[i] {
            ret = false;
            break;
        }
    }

    // Any change in opclass options break compatibility.
    if ret {
        let mut old_opclass_options: Vec<::types_tuple::Datum<'mcx>> =
            (0..old_natts).map(|_| ::types_tuple::Datum::null()).collect();
        for (i, slot) in old_opclass_options.iter_mut().enumerate() {
            *slot = match lsyscache::get_attoptions::call(mcx, old_id, (i + 1) as i16)? {
                Some(d) => d,
                None => ::types_tuple::Datum::null(),
            };
        }
        ret = compare_opclass_options(&old_opclass_options, &opclass_options, old_natts);
    }

    // Any change in exclusion operator selections breaks compatibility.
    if ret && index_info.ii_ExclusionOps.is_some() {
        // RelationGetExclusionInfo(irel, ...) is not yet ported with a value
        // return; exclusion indexes never reach here for the column-type rebuild
        // path that drives TryReuseIndex (ALTER COLUMN TYPE on an exclusion
        // constraint forces a rewrite). Be conservative and treat as
        // incompatible rather than reuse storage we can't verify.
        ret = false;
    }

    irel.close(NoLock)?;
    Ok(ret)
}

/// `CheckIndexCompatible(oldId, stmt->accessMethod, stmt->indexParams,
/// stmt->excludeOpNames, stmt->iswithoutoverlaps)` — the `TryReuseIndex` call
/// shape. Extracts the key-column `IndexElem` list from `stmt->indexParams`
/// (INCLUDE columns are not part of the compatibility check) and forwards to
/// [`CheckIndexCompatible`].
pub fn check_index_compatible_stmt<'mcx>(
    mcx: Mcx<'mcx>,
    old_id: Oid,
    stmt: &nodes::ddlnodes::IndexStmt<'mcx>,
) -> PgResult<bool> {
    let access_method_name = stmt
        .accessMethod
        .as_ref()
        .map(|s| s.as_str())
        .expect("CheckIndexCompatible: IndexStmt has no accessMethod");

    let att_list: Vec<&nodes::ddlnodes::IndexElem<'mcx>> = stmt
        .indexParams
        .iter()
        .map(|n| crate::node_as_index_elem(n.as_ref()))
        .collect();

    let excl = if stmt.excludeOpNames.is_empty() {
        None
    } else {
        Some(&stmt.excludeOpNames)
    };

    CheckIndexCompatible(
        mcx,
        old_id,
        access_method_name,
        &att_list,
        excl,
        stmt.iswithoutoverlaps,
    )
}

/// `CompareOpclassOptions(opts1, opts2, natts)` (indexcmds.c): true when every
/// paired opclass-options `text[]` Datum is equal — both NULL (`(Datum) 0`), or
/// binary-equivalent `text[]` arrays (C uses `array_eq` under `C_COLLATION_OID`,
/// i.e. byte equality). The reachable `CheckIndexCompatible` path passes all-NULL
/// options (no `WITH` clause), which compare equal element-wise.
fn compare_opclass_options<'mcx>(
    opts1: &[::types_tuple::Datum<'mcx>],
    opts2: &[::types_tuple::Datum<'mcx>],
    natts: usize,
) -> bool {
    for i in 0..natts {
        let n1 = is_null_datum(&opts1[i]);
        let n2 = is_null_datum(&opts2[i]);
        if n1 {
            if n2 {
                continue;
            }
            return false;
        } else if n2 {
            return false;
        }
        // Both non-NULL `text[]`: compare the by-reference varlena byte images
        // (binary equivalence, matching C `array_eq` under C collation).
        if opts1[i].as_ref_bytes() != opts2[i].as_ref_bytes() {
            return false;
        }
    }
    true
}

/// A `(Datum) 0` sentinel — the "no opclass options" marker, modelled as the
/// by-value zero word (`Datum::null()`).
fn is_null_datum(d: &::types_tuple::Datum<'_>) -> bool {
    matches!(d, ::types_tuple::Datum::ByVal(0))
}
