//! `commands/typecmds.c` — the ALTER DOMAIN ADD/VALIDATE CONSTRAINT validation
//! scans, ported faithfully from PostgreSQL 18.3:
//!
//!   - `validateDomainNotNullConstraint` (typecmds.c:3130) — verify that every
//!     column currently using the domain contains no NULL value.
//!   - `validateDomainCheckConstraint` (typecmds.c:3196) — compile the CHECK
//!     expression and verify that every column currently using the domain
//!     satisfies it.
//!   - `get_rels_with_domain` (typecmds.c:3316) — fetch all relations /
//!     attributes that are using the domain (recursive pg_depend scan, including
//!     attributes of derived sub-domains; errors out if a container type stores
//!     the domain).
//!
//! These live here (not in `backend-commands-typecmds`) because the validation
//! scans need the executor (`CreateExecutorState` / `ExecPrepareExpr` /
//! `ExecEvalExpr`, the table-AM scan) and `find_composite_type_dependencies`
//! (owned by tablecmds), neither reachable from typecmds. The bodies install the
//! `validate_domain_not_null_constraint` / `validate_domain_check_constraint`
//! OUTWARD seams declared on `backend-commands-typecmds-seams`, which the
//! AlterDomain* drivers in typecmds call.

#![allow(non_snake_case)]

use mcx::Mcx;

use types_core::primitive::{AttrNumber, Oid, OidIsValid};
use types_error::{PgResult, ERRCODE_CHECK_VIOLATION, ERRCODE_NOT_NULL_VIOLATION, ERROR};
use rel::Relation;
use types_storage::lock::{LOCKMODE, NoLock, ShareLock};
use types_tuple::access::{RELKIND_MATVIEW, RELKIND_RELATION};

use common_relation::relation_open;
use objectaddress::consts::{RelationRelationId, TypeRelationId};
use pg_depend_seams as pg_depend_seam;
use execTuples::exec_init_slots::ExecDropSingleTupleTableSlot;
use execTuples::slot_deform::slot_getattr;
use lsyscache_seams::get_typtype;
use stack_depth::check_stack_depth;

use types_catalog::pg_type::TYPTYPE_DOMAIN;

use crate::at_altertype::find_composite_type_dependencies;
use crate::helpers::here;

/// result structure for `get_rels_with_domain()` (typecmds.c:78 `RelToCheck`).
struct RelToCheck<'mcx> {
    /// opened and locked relation
    rel: Relation<'mcx>,
    /// attribute numbers of interest, kept in column-number order
    atts: Vec<AttrNumber>,
}

/// `validateDomainNotNullConstraint(Oid domainoid)` (typecmds.c:3130).
///
/// Verify that all columns currently using the domain are not null.
pub fn validate_domain_not_null_constraint<'mcx>(mcx: Mcx<'mcx>, domainoid: Oid) -> PgResult<()> {
    // Fetch relation list with attributes based on this domain.
    // ShareLock is sufficient to prevent concurrent data changes.
    let rels = get_rels_with_domain(mcx, domainoid, ShareLock)?;

    for rtc in rels.into_iter() {
        let RelToCheck { rel: testrel, atts } = rtc;
        let tupdesc = &testrel.rd_att;

        // Scan all tuples in this relation.
        //
        // C does RegisterSnapshot(GetLatestSnapshot()) + table_beginscan; here we
        // reuse the active snapshot already pushed by the utility portal (the
        // established at_verify_not_null idiom), which avoids a private
        // RegisterSnapshot/UnregisterSnapshot pair and its resource-owner leak.
        let snap_rc = snapmgr_seams::get_active_snapshot::call()?
            .expect("ALTER DOMAIN NOT NULL validate scan with no active snapshot");

        let rel_alias = testrel.alias();

        let mut scan =
            table_tableam_seams::table_beginscan::call(mcx, &rel_alias, snap_rc)?;
        let mut slot = table_tableam::table_slot_create(mcx, &rel_alias)?;

        // while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
        while table_tableam_seams::table_scan_getnextslot::call(
            mcx, &mut scan, &mut slot,
        )? {
            // Test attributes that are of the domain.
            for &attnum in atts.iter() {
                let (_d, isnull) = slot_getattr(mcx, &mut slot, attnum)?;
                if isnull {
                    let attr = tupdesc.attr((attnum - 1) as usize);
                    let attname =
                        String::from_utf8_lossy(attr.attname.name_str()).into_owned();
                    let relname = testrel.name().to_string();
                    table_tableam::table_endscan(scan)?;
                    ExecDropSingleTupleTableSlot(slot)?;
                    return utils_error::ereport(ERROR)
                        .errcode(ERRCODE_NOT_NULL_VIOLATION)
                        .errmsg(format!(
                            "column \"{attname}\" of table \"{relname}\" contains null values"
                        ))
                        .finish(here("validate_domain_not_null_constraint"));
                }
            }
        }

        ExecDropSingleTupleTableSlot(slot)?;
        table_tableam::table_endscan(scan)?;

        // Close each rel after processing, but keep lock.
        testrel.close(NoLock)?;
    }

    Ok(())
}

/// `validateDomainCheckConstraint(Oid domainoid, const char *ccbin)`
/// (typecmds.c:3196).
///
/// Verify that all columns currently using the domain satisfy the given check
/// constraint expression.
pub fn validate_domain_check_constraint<'mcx>(
    mcx: Mcx<'mcx>,
    domainoid: Oid,
    ccbin: &str,
) -> PgResult<()> {
    // expr = (Expr *) stringToNode(ccbin);
    let cnode = read_seams::string_to_node::call(mcx, ccbin)?;
    let expr = mcx::PgBox::into_inner(cnode)
        .into_expr()
        .ok_or_else(|| {
            utils_error::ereport(ERROR)
                .errmsg("domain CHECK ccbin did not parse to an Expr".to_string())
                .into_error()
        })?;

    // Need an EState to run ExecEvalExpr.
    let mut estate = execExpr_seams::create_executor_state::call(mcx)?;

    // build execution state for expr (ExecPrepareExpr runs expression_planner).
    let mut exprstate =
        execExpr_seams::exec_prepare_expr::call(&expr, &mut estate)?;

    // econtext = GetPerTupleExprContext(estate);
    let econtext =
        execUtils_seams::get_per_tuple_expr_context::call(&mut estate)?;

    // Fetch relation list with attributes based on this domain.
    // ShareLock is sufficient to prevent concurrent data changes.
    let rels = get_rels_with_domain(mcx, domainoid, ShareLock)?;

    for rtc in rels.into_iter() {
        let RelToCheck { rel: testrel, atts } = rtc;
        let tupdesc = &testrel.rd_att;

        // Scan all tuples in this relation (reuse the active snapshot — see the
        // note in validate_domain_not_null_constraint).
        let snap_rc = snapmgr_seams::get_active_snapshot::call()?
            .expect("ALTER DOMAIN CHECK validate scan with no active snapshot");

        let rel_alias = testrel.alias();

        let mut scan =
            table_tableam_seams::table_beginscan::call(mcx, &rel_alias, snap_rc)?;
        let mut slot = table_tableam::table_slot_create(mcx, &rel_alias)?;

        while table_tableam_seams::table_scan_getnextslot::call(
            mcx, &mut scan, &mut slot,
        )? {
            // Test attributes that are of the domain.
            for &attnum in atts.iter() {
                let (d, mut isnull) = slot_getattr(mcx, &mut slot, attnum)?;

                // econtext->domainValue_datum = d;
                // econtext->domainValue_isNull = isNull;
                {
                    let ecxt = estate.ecxt_mut(econtext);
                    ecxt.domainValue_datum = d;
                    ecxt.domainValue_isNull = isnull;
                }

                // conResult = ExecEvalExprSwitchContext(exprstate, econtext, &isNull);
                let (con_result, con_isnull) =
                    execExpr_seams::exec_eval_expr_switch_context::call(
                        &mut exprstate,
                        econtext,
                        &mut estate,
                    )?;
                isnull = con_isnull;

                // if (!isNull && !DatumGetBool(conResult))
                if !isnull && !con_result.as_bool() {
                    let attr = tupdesc.attr((attnum - 1) as usize);
                    let attname =
                        String::from_utf8_lossy(attr.attname.name_str()).into_owned();
                    let relname = testrel.name().to_string();
                    table_tableam::table_endscan(scan)?;
                    ExecDropSingleTupleTableSlot(slot)?;
                    return utils_error::ereport(ERROR)
                        .errcode(ERRCODE_CHECK_VIOLATION)
                        .errmsg(format!(
                            "column \"{attname}\" of table \"{relname}\" contains values that violate the new constraint"
                        ))
                        .finish(here("validate_domain_check_constraint"));
                }
            }

            // ResetExprContext(econtext);
            estate.ecxt_mut(econtext).ecxt_per_tuple_memory.reset();
        }

        ExecDropSingleTupleTableSlot(slot)?;
        table_tableam::table_endscan(scan)?;

        // Hold relation lock till commit (XXX bad for concurrency).
        testrel.close(NoLock)?;
    }

    // FreeExecutorState(estate).
    drop(exprstate);
    execExpr_seams::free_executor_state::call(estate)?;

    Ok(())
}

/// `get_rels_with_domain(Oid domainOid, LOCKMODE lockmode)` (typecmds.c:3316).
///
/// Fetch all relations / attributes which are using the domain. The result is a
/// list of [`RelToCheck`] structs, one for each distinct relation, each
/// containing one or more attribute numbers that are of the domain type. We have
/// opened each rel and acquired the specified lock type on it.
///
/// Nested domains are supported by including attributes that are of derived
/// domain types.
fn get_rels_with_domain<'mcx>(
    mcx: Mcx<'mcx>,
    domain_oid: Oid,
    lockmode: LOCKMODE,
) -> PgResult<Vec<RelToCheck<'mcx>>> {
    let mut result: Vec<RelToCheck<'mcx>> = Vec::new();

    // char *domainTypeName = format_type_be(domainOid);
    let domain_type_name =
        format_type_seams::format_type_be::call(mcx, domain_oid)?
            .as_str()
            .to_string();

    debug_assert!(lockmode != NoLock);

    // since this function recurses, it could be driven to stack overflow
    check_stack_depth()?;

    // We scan pg_depend to find those things that depend on the domain. (We
    // assume we can ignore refobjsubid for a domain.) The scan filters by
    // (refclassid = pg_type, refobjid = domain_oid).
    let rows = pg_depend_seam::scan_type_referers::call(mcx, domain_oid)?;

    for row in rows.iter() {
        // Check for directly dependent types.
        if row.classid == TypeRelationId {
            if (get_typtype::call(row.objid)? as i8) == TYPTYPE_DOMAIN {
                // This is a sub-domain, so recursively add dependent columns to
                // the output list.
                let mut sub = get_rels_with_domain(mcx, row.objid, lockmode)?;
                result.append(&mut sub);
            } else {
                // Otherwise, it is some container type using the domain, so fail
                // if there are any columns of this type. C passes
                // origRelation = NULL, origTypeName = domainTypeName.
                find_composite_type_dependencies(
                    mcx,
                    row.objid,
                    None,
                    Some(domain_type_name.as_str()),
                )?;
            }
            continue;
        }

        // Else, ignore dependees that aren't user columns of relations.
        // (we assume system columns are never of domain types)
        if row.classid != RelationRelationId || row.objsubid <= 0 {
            continue;
        }

        // See if we already have an entry for this relation.
        let existing = result
            .iter()
            .position(|rt| rt.rel.rd_id == row.objid);

        let idx = match existing {
            Some(i) => i,
            None => {
                // First attribute found for this relation.
                // Acquire requested lock on relation.
                let rel = relation_open(mcx, row.objid, lockmode)?;

                // Check to see if rowtype is stored anyplace as a composite-type
                // column; if so we have to fail, for now anyway.
                if OidIsValid(rel.rd_rel.reltype) {
                    find_composite_type_dependencies(
                        mcx,
                        rel.rd_rel.reltype,
                        None,
                        Some(domain_type_name.as_str()),
                    )?;
                }

                // Otherwise, we can ignore relations except those with both
                // storage and user-chosen column types.
                let relkind = rel.rd_rel.relkind;
                if relkind != RELKIND_RELATION && relkind != RELKIND_MATVIEW {
                    rel.close(lockmode)?;
                    continue;
                }

                result.push(RelToCheck {
                    rel,
                    atts: Vec::new(),
                });
                result.len() - 1
            }
        };

        // Confirm column has not been dropped, and is of the expected type. This
        // defends against an ALTER DROP COLUMN occurring just before we acquired
        // lock ... but if the whole table were dropped, we'd still have a problem.
        let natts = result[idx].rel.rd_att.natts;
        if row.objsubid > natts {
            continue;
        }
        let pg_att = result[idx].rel.rd_att.attr((row.objsubid - 1) as usize);
        if pg_att.attisdropped || pg_att.atttypid != domain_oid {
            continue;
        }

        // Okay, add column to result. We store the columns in column-number
        // order; this is just a hack to improve predictability of regression
        // test output ...
        debug_assert!((result[idx].atts.len() as i32) < natts);
        let objsubid = row.objsubid as AttrNumber;
        let atts = &mut result[idx].atts;
        let mut ptr = atts.len();
        atts.push(objsubid);
        while ptr > 0 && atts[ptr - 1] > objsubid {
            atts[ptr] = atts[ptr - 1];
            ptr -= 1;
        }
        atts[ptr] = objsubid;
    }

    Ok(result)
}
