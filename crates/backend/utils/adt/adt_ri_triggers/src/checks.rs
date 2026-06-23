//! The check driver, error reporting, and the key NULL/equality predicates:
//! `ri_PlanCheck` / `ri_PerformCheck` / `ri_ExtractValues` /
//! `ri_ReportViolation` / `ri_NullCheck` / `ri_KeysEqual` / `ri_CheckTrigger` /
//! `RI_FKey_trigger_type` / `ri_Check_Pk_Match`.

extern crate alloc;
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use ::mcx::Mcx;
// Canonical value type (Datum unification). The fmgr `function_call*` seams
// still carry the bare-word shim (`datum::Datum`), so a by-value scalar
// crosses that ABI edge via `byval_word()` / `Datum::ByVal`.
use types_tuple::heaptuple::Datum;
use types_core::{InvalidOid, Oid};
use types_error::{
    PgError, PgResult, ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED, ERRCODE_FOREIGN_KEY_VIOLATION,
    ERRCODE_INTERNAL_ERROR, ERRCODE_RESTRICT_VIOLATION, PG_DIAG_CONSTRAINT_NAME, PG_DIAG_SCHEMA_NAME,
    PG_DIAG_TABLE_NAME,
};
use types_ri_triggers::{SpiPlanPtr, TriggerDataRef, TupleTableSlotRef};

use crate::cache::{ri_hash_compare_op, ri_hash_prepared_plan};
use crate::querybuild::{append_quoted_relation, quote_one_name, ri_generate_qual, try_extend};
use crate::{
    trigger_fired_after, trigger_fired_by_delete, trigger_fired_by_insert, trigger_fired_by_update,
    trigger_fired_for_row, RelSide, RiConstraintInfo, RiQueryKey, F_RI_FKEY_CASCADE_DEL,
    F_RI_FKEY_CASCADE_UPD, F_RI_FKEY_CHECK_INS, F_RI_FKEY_CHECK_UPD, F_RI_FKEY_NOACTION_DEL,
    F_RI_FKEY_NOACTION_UPD, F_RI_FKEY_RESTRICT_DEL, F_RI_FKEY_RESTRICT_UPD, F_RI_FKEY_SETDEFAULT_DEL,
    F_RI_FKEY_SETDEFAULT_UPD, F_RI_FKEY_SETNULL_DEL, F_RI_FKEY_SETNULL_UPD, ANYMULTIRANGEOID,
    RI_KEYS_ALL_NULL, RI_KEYS_NONE_NULL, RI_KEYS_SOME_NULL, RI_MAX_NUMKEYS, RI_PLAN_CHECK_LOOKUPPK,
    RI_PLAN_CHECK_LOOKUPPK_FROM_PK, RI_PLAN_LAST_ON_PK, RI_TRIGGER_FK, RI_TRIGGER_NONE,
    RI_TRIGGER_PK, RI_TRIGTYPE_DELETE, RI_TRIGTYPE_INSERT, RI_TRIGTYPE_UPDATE,
    SECURITY_LOCAL_USERID_CHANGE, SECURITY_NOFORCE_RLS, SPI_OK_FINISH, SPI_OK_SELECT,
};

/// `ri_CheckTrigger` --- check the RI trigger was called in the expected
/// context (CALLED_AS_TRIGGER + AFTER ROW + the right event).
pub fn ri_check_trigger(trigdata: TriggerDataRef, funcname: &str, tgkind: i32) -> PgResult<()> {
    if !trigger_seams::called_as_trigger::call(trigdata) {
        return Err(PgError::error(format!(
            "function \"{funcname}\" was not called by trigger manager"
        ))
        .with_sqlstate(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED));
    }

    let event = trigger_seams::tg_event::call(trigdata);

    if !trigger_fired_after(event) || !trigger_fired_for_row(event) {
        return Err(
            PgError::error(format!("function \"{funcname}\" must be fired AFTER ROW"))
                .with_sqlstate(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
        );
    }

    match tgkind {
        RI_TRIGTYPE_INSERT => {
            if !trigger_fired_by_insert(event) {
                return Err(PgError::error(format!(
                    "function \"{funcname}\" must be fired for INSERT"
                ))
                .with_sqlstate(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED));
            }
        }
        RI_TRIGTYPE_UPDATE => {
            if !trigger_fired_by_update(event) {
                return Err(PgError::error(format!(
                    "function \"{funcname}\" must be fired for UPDATE"
                ))
                .with_sqlstate(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED));
            }
        }
        RI_TRIGTYPE_DELETE => {
            if !trigger_fired_by_delete(event) {
                return Err(PgError::error(format!(
                    "function \"{funcname}\" must be fired for DELETE"
                ))
                .with_sqlstate(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED));
            }
        }
        _ => {}
    }
    Ok(())
}

/// `ri_PlanCheck` --- prepare and save an SPI plan for an RI query, switching to
/// the query relation owner's UID for the prepare.
pub fn ri_plan_check(
    mcx: Mcx<'_>,
    querystr: &[u8],
    argtypes: &[Oid],
    qkey: &RiQueryKey,
    fk_rel: RelSide<'_, '_>,
    pk_rel: RelSide<'_, '_>,
) -> PgResult<SpiPlanPtr> {
    let query_rel = if qkey.constr_queryno <= RI_PLAN_LAST_ON_PK {
        pk_rel
    } else {
        fk_rel
    };

    let (save_userid, save_sec_context) =
        miscinit_seams::get_user_id_and_sec_context::call();
    let owner = query_rel.owner();
    miscinit_seams::set_user_id_and_sec_context::call(
        owner,
        save_sec_context | SECURITY_LOCAL_USERID_CHANGE | SECURITY_NOFORCE_RLS,
    );

    let qplan = spi_seams::spi_prepare::call(querystr, argtypes)?;

    let qplan = match qplan {
        Some(p) => p,
        None => {
            let code = spi_seams::spi_result_code_string::call(mcx, -1)?;
            miscinit_seams::set_user_id_and_sec_context::call(
                save_userid,
                save_sec_context,
            );
            let querytext = String::from_utf8_lossy(querystr);
            return Err(PgError::error(format!(
                "SPI_prepare returned {} for {querytext}",
                code.as_str()
            )));
        }
    };

    miscinit_seams::set_user_id_and_sec_context::call(
        save_userid,
        save_sec_context,
    );

    spi_seams::spi_keepplan::call(qplan)?;
    ri_hash_prepared_plan(qkey, qplan)?;

    Ok(qplan)
}

/// `ri_ExtractValues` --- extract key fields from a tuple slot into the
/// `vals`/`nulls` arrays. `nulls[i]` is `true` for a NULL (C `'n'`).
pub fn ri_extract_values<'mcx>(
    mcx: Mcx<'mcx>,
    slot: TupleTableSlotRef,
    riinfo: &RiConstraintInfo,
    rel_is_pk: bool,
    vals: &mut [Datum<'mcx>],
    nulls: &mut [bool],
) -> PgResult<()> {
    let attnums = if rel_is_pk {
        &riinfo.pk_attnums
    } else {
        &riinfo.fk_attnums
    };
    for i in 0..riinfo.nkeys as usize {
        let (datum, isnull) =
            trigger_seams::slot_getattr::call(mcx, slot, attnums[i])?;
        vals[i] = datum;
        nulls[i] = isnull;
    }
    Ok(())
}

/// `ri_PerformCheck` --- run an RI query under the right snapshot/role and check
/// the result, reporting a violation if indicated. Returns `SPI_processed != 0`.
pub fn ri_perform_check(
    mcx: Mcx<'_>,
    riinfo: &RiConstraintInfo,
    qkey: &RiQueryKey,
    qplan: SpiPlanPtr,
    fk_rel: RelSide<'_, '_>,
    pk_rel: RelSide<'_, '_>,
    oldslot: Option<TupleTableSlotRef>,
    newslot: Option<TupleTableSlotRef>,
    is_restrict: bool,
    detect_new_rows: bool,
    expect_ok: i32,
) -> PgResult<bool> {
    let query_rel = if qkey.constr_queryno <= RI_PLAN_LAST_ON_PK {
        pk_rel
    } else {
        fk_rel
    };

    let source_is_pk = qkey.constr_queryno != RI_PLAN_CHECK_LOOKUPPK;

    let nkeys = riinfo.nkeys as usize;
    // The canonical `Datum` is not `Copy` (the `ByRef` arm owns bytes), so the
    // value arrays are built with `from_fn` rather than array-repeat init.
    let mut vals: [Datum; RI_MAX_NUMKEYS * 2] = core::array::from_fn(|_| Datum::null());
    let mut nulls = [false; RI_MAX_NUMKEYS * 2];

    if let Some(newslot) = newslot {
        ri_extract_values(mcx, newslot, riinfo, source_is_pk, &mut vals, &mut nulls)?;
        if let Some(oldslot) = oldslot {
            let mut tail_vals: [Datum; RI_MAX_NUMKEYS * 2] =
                core::array::from_fn(|_| Datum::null());
            let mut tail_nulls = [false; RI_MAX_NUMKEYS * 2];
            ri_extract_values(mcx, oldslot, riinfo, source_is_pk, &mut tail_vals, &mut tail_nulls)?;
            for (dst, src) in vals[nkeys..nkeys * 2].iter_mut().zip(&tail_vals[..nkeys]) {
                *dst = src.clone();
            }
            nulls[nkeys..nkeys * 2].copy_from_slice(&tail_nulls[..nkeys]);
        }
    } else {
        let oldslot = oldslot.expect("ri_PerformCheck: oldslot required when newslot is NULL");
        ri_extract_values(mcx, oldslot, riinfo, source_is_pk, &mut vals, &mut nulls)?;
    }

    let (test_snapshot, crosscheck_snapshot) =
        if transam_xact_seams::isolation_uses_xact_snapshot::call() && detect_new_rows
        {
            transam_xact_seams::command_counter_increment::call()?;
            (
                Some(snapmgr_seams::get_latest_snapshot::call()?),
                Some(snapmgr_seams::get_transaction_snapshot::call()?),
            )
        } else {
            (None, None)
        };

    let limit: i64 = if expect_ok == SPI_OK_SELECT { 1 } else { 0 };

    let nparams = if newslot.is_some() && oldslot.is_some() {
        nkeys * 2
    } else {
        nkeys
    };

    let (save_userid, save_sec_context) =
        miscinit_seams::get_user_id_and_sec_context::call();
    let owner = query_rel.owner();
    miscinit_seams::set_user_id_and_sec_context::call(
        owner,
        save_sec_context | SECURITY_LOCAL_USERID_CHANGE | SECURITY_NOFORCE_RLS,
    );

    let exec = spi_seams::spi_execute_snapshot::call(
        qplan,
        &vals[..nparams],
        &nulls[..nparams],
        test_snapshot,
        crosscheck_snapshot,
        false,
        false,
        limit,
    );

    miscinit_seams::set_user_id_and_sec_context::call(
        save_userid,
        save_sec_context,
    );

    let exec = exec?;
    let spi_result = exec.code;
    let spi_processed = exec.processed;

    if spi_result < 0 {
        let code = spi_seams::spi_result_code_string::call(mcx, spi_result)?;
        return Err(PgError::error(format!(
            "SPI_execute_snapshot returned {}",
            code.as_str()
        )));
    }

    if expect_ok >= 0 && spi_result != expect_ok {
        let pkname = String::from_utf8_lossy(&pk_rel.name_bytes(mcx)?).into_owned();
        let fkname = String::from_utf8_lossy(&fk_rel.name_bytes(mcx)?).into_owned();
        return Err(PgError::error(format!(
            "referential integrity query on \"{pkname}\" from constraint \"{}\" on \"{fkname}\" gave unexpected result",
            riinfo.conname_str()
        ))
        .with_sqlstate(ERRCODE_INTERNAL_ERROR)
        .with_hint("This is most likely due to a rule having rewritten the query."));
    }

    if qkey.constr_queryno != RI_PLAN_CHECK_LOOKUPPK_FROM_PK
        && expect_ok == SPI_OK_SELECT
        && (spi_processed == 0) == (qkey.constr_queryno == RI_PLAN_CHECK_LOOKUPPK)
    {
        let violatorslot = newslot.or(oldslot);
        ri_report_violation(
            mcx,
            riinfo,
            pk_rel,
            fk_rel,
            ViolatorSource::Slot(violatorslot.expect("violator slot present")),
            qkey.constr_queryno,
            is_restrict,
            false,
        )?;
    }

    Ok(spi_processed != 0)
}

/// Where `ri_ReportViolation` reads the violating tuple's key columns from.
#[derive(Clone, Copy, Debug)]
pub enum ViolatorSource {
    /// `violatorslot` with C `tupdesc == NULL` (uses fk/pk rel's tupdesc).
    Slot(TupleTableSlotRef),
    /// `tupdesc` was passed: read columns 1..N from the first SPI result tuple.
    SpiResult,
}

/// `ri_ReportViolation` --- produce the FK/RESTRICT violation error report.
/// Byte-identical message/detail text and SQLSTATEs to C; always returns `Err`.
pub fn ri_report_violation(
    mcx: Mcx<'_>,
    riinfo: &RiConstraintInfo,
    pk_rel: RelSide<'_, '_>,
    fk_rel: RelSide<'_, '_>,
    source: ViolatorSource,
    queryno: i32,
    is_restrict: bool,
    partgone: bool,
) -> PgResult<()> {
    let onfk = queryno == RI_PLAN_CHECK_LOOKUPPK;
    let (attnums_vec, rel_oid): (Vec<i16>, Oid) = if onfk {
        (riinfo.fk_attnums[..riinfo.nkeys as usize].to_vec(), fk_rel.oid())
    } else {
        (riinfo.pk_attnums[..riinfo.nkeys as usize].to_vec(), pk_rel.oid())
    };

    let has_perm = if partgone {
        true
    } else {
        report_has_perm(rel_oid, &attnums_vec)?
    };

    let (key_names, key_values) = if has_perm {
        // Get printable versions of the keys involved (name + rendered value).
        let columns = match source {
            ViolatorSource::Slot(slot) => {
                fmgr_seams::render_slot_columns::call(mcx, slot, &attnums_vec)?
            }
            ViolatorSource::SpiResult => {
                spi_seams::spi_first_row_columns::call(mcx, &attnums_vec)?
            }
        };
        let mut names = String::new();
        let mut values = String::new();
        for (idx, col) in columns.iter().enumerate() {
            let name = String::from_utf8_lossy(&col.name);
            let val = match &col.value {
                Some(v) => v.as_str().to_string(),
                None => "null".to_string(),
            };
            if idx > 0 {
                names.push_str(", ");
                values.push_str(", ");
            }
            names.push_str(&name);
            values.push_str(&val);
        }
        (names, values)
    } else {
        (String::new(), String::new())
    };

    let pkname = String::from_utf8_lossy(&pk_rel.name_bytes(mcx)?).into_owned();
    let fkname = String::from_utf8_lossy(&fk_rel.name_bytes(mcx)?).into_owned();
    let conname = riinfo.conname_str();

    let err = if partgone {
        PgError::error(format!(
            "removing partition \"{pkname}\" violates foreign key constraint \"{conname}\""
        ))
        .with_sqlstate(ERRCODE_FOREIGN_KEY_VIOLATION)
        .with_detail(format!(
            "Key ({key_names})=({key_values}) is still referenced from table \"{fkname}\"."
        ))
    } else if onfk {
        let e = PgError::error(format!(
            "insert or update on table \"{fkname}\" violates foreign key constraint \"{conname}\""
        ))
        .with_sqlstate(ERRCODE_FOREIGN_KEY_VIOLATION);
        if has_perm {
            e.with_detail(format!(
                "Key ({key_names})=({key_values}) is not present in table \"{pkname}\"."
            ))
        } else {
            e.with_detail(format!("Key is not present in table \"{pkname}\"."))
        }
    } else if is_restrict {
        let e = PgError::error(format!(
            "update or delete on table \"{pkname}\" violates RESTRICT setting of foreign key constraint \"{conname}\" on table \"{fkname}\""
        ))
        .with_sqlstate(ERRCODE_RESTRICT_VIOLATION);
        if has_perm {
            e.with_detail(format!(
                "Key ({key_names})=({key_values}) is referenced from table \"{fkname}\"."
            ))
        } else {
            e.with_detail(format!("Key is referenced from table \"{fkname}\"."))
        }
    } else {
        let e = PgError::error(format!(
            "update or delete on table \"{pkname}\" violates foreign key constraint \"{conname}\" on table \"{fkname}\""
        ))
        .with_sqlstate(ERRCODE_FOREIGN_KEY_VIOLATION);
        if has_perm {
            e.with_detail(format!(
                "Key ({key_names})=({key_values}) is still referenced from table \"{fkname}\"."
            ))
        } else {
            e.with_detail(format!("Key is still referenced from table \"{fkname}\"."))
        }
    };

    Err(attach_table_constraint(mcx, err, fk_rel, riinfo)?)
}

/// Permission probe for `ri_ReportViolation`'s `has_perm` decision
/// (`ri_triggers.c` lines 2723-2761, the non-`partgone` branch). If the user
/// can't view the key columns we omit the `Key (...)=(...)` detail.
///
/// C: check RLS first — if `check_enable_rls(rel_oid, InvalidOid, true) ==
/// RLS_ENABLED` we return `false` (don't leak data under RLS); otherwise check
/// table-level `pg_class_aclcheck(ACL_SELECT)`, and failing that the per-column
/// `pg_attribute_aclcheck(ACL_SELECT)` loop (any denied column → `false`). All
/// checks use `GetUserId()`.
fn report_has_perm(rel_oid: Oid, key_attnums: &[i16]) -> PgResult<bool> {
    use types_acl::{ACLCHECK_OK, ACL_SELECT, RLS_ENABLED};

    if more_seams::check_enable_rls::call(rel_oid, InvalidOid, true)?
        != RLS_ENABLED
    {
        let userid = miscinit_seams::get_user_id::call();
        let aclresult = aclchk_seams::pg_class_aclcheck::call(
            rel_oid,
            userid,
            ACL_SELECT,
        )?;
        if aclresult != ACLCHECK_OK {
            // Try for column-level permissions.
            for &attnum in key_attnums {
                let aclresult = aclchk_seams::pg_attribute_aclcheck::call(
                    rel_oid,
                    attnum,
                    userid,
                    ACL_SELECT,
                )?;
                if aclresult != ACLCHECK_OK {
                    // No access to the key.
                    return Ok(false);
                }
            }
        }
        Ok(true)
    } else {
        Ok(false)
    }
}

/// `errtableconstraint(rel, NameStr(riinfo->conname))` — attach the
/// schema/table/constraint diagnostic fields.
pub(crate) fn attach_table_constraint(
    mcx: Mcx<'_>,
    err: PgError,
    rel: RelSide<'_, '_>,
    riinfo: &RiConstraintInfo,
) -> PgResult<PgError> {
    let schema = String::from_utf8_lossy(
        &crate::querybuild::relation_namespace_name(mcx, rel.namespace())?,
    )
    .into_owned();
    let table = String::from_utf8_lossy(&rel.name_bytes(mcx)?).into_owned();
    err.with_error_field(PG_DIAG_SCHEMA_NAME, schema)?
        .with_error_field(PG_DIAG_TABLE_NAME, table)?
        .with_error_field(PG_DIAG_CONSTRAINT_NAME, riinfo.conname_str())
}

/// `ri_NullCheck` --- determine the NULL state of all key values in a tuple.
pub fn ri_null_check(
    slot: TupleTableSlotRef,
    riinfo: &RiConstraintInfo,
    rel_is_pk: bool,
) -> PgResult<i32> {
    let attnums = if rel_is_pk {
        &riinfo.pk_attnums
    } else {
        &riinfo.fk_attnums
    };

    let mut allnull = true;
    let mut nonenull = true;

    for i in 0..riinfo.nkeys as usize {
        if trigger_seams::slot_attisnull::call(slot, attnums[i])? {
            nonenull = false;
        } else {
            allnull = false;
        }
    }

    if allnull {
        return Ok(RI_KEYS_ALL_NULL);
    }
    if nonenull {
        return Ok(RI_KEYS_NONE_NULL);
    }
    Ok(RI_KEYS_SOME_NULL)
}

/// `ri_KeysEqual` --- check if all key values in OLD and NEW are "equivalent".
pub fn ri_keys_equal(
    mcx: Mcx<'_>,
    rel: RelSide<'_, '_>,
    oldslot: TupleTableSlotRef,
    newslot: TupleTableSlotRef,
    riinfo: &RiConstraintInfo,
    rel_is_pk: bool,
) -> PgResult<bool> {
    let attnums = if rel_is_pk {
        &riinfo.pk_attnums
    } else {
        &riinfo.fk_attnums
    };

    for i in 0..riinfo.nkeys as usize {
        let (oldvalue, isnull) =
            trigger_seams::slot_getattr::call(mcx, oldslot, attnums[i])?;
        if isnull {
            return Ok(false);
        }
        let (newvalue, isnull) =
            trigger_seams::slot_getattr::call(mcx, newslot, attnums[i])?;
        if isnull {
            return Ok(false);
        }

        if rel_is_pk {
            if !trigger_seams::pk_datum_image_eq::call(
                oldslot, attnums[i], &oldvalue, &newvalue,
            ) {
                return Ok(false);
            }
        } else {
            let eq_opr = if riinfo.hasperiod && i == riinfo.nkeys as usize - 1 {
                riinfo.period_contained_by_oper
            } else {
                riinfo.ff_eq_oprs[i]
            };
            let typeid = rel.att_type(attnums[i]);
            let collid = rel.att_collation(attnums[i]);
            // The comparison edge takes canonical `Datum`s so a by-reference key
            // value (and the by-reference output of a coercion cast, e.g. the
            // `int4 -> numeric` promotion for a `int` FK referencing a `numeric`
            // PK) crosses through the fmgr by-reference side channel intact.
            if !ri_compare_with_cast(mcx, eq_opr, typeid, collid, newvalue, oldvalue)? {
                return Ok(false);
            }
        }
    }

    Ok(true)
}

/// `ri_CompareWithCast` --- call the appropriate comparison operator for two
/// values. Normally this is equality, but for the PERIOD part of foreign keys
/// it is ContainedBy, so the order of lhs vs rhs is significant.
///
/// NB: we have already checked that neither value is null.
fn ri_compare_with_cast<'mcx>(
    mcx: Mcx<'mcx>,
    eq_opr: Oid,
    typeid: Oid,
    collid: Oid,
    mut lhs: Datum<'mcx>,
    mut rhs: Datum<'mcx>,
) -> PgResult<bool> {
    let entry = ri_hash_compare_op(mcx, eq_opr, typeid)?;

    // Do we need to cast the values? The cast result type may be
    // pass-by-reference (e.g. `int4 -> numeric`), so this goes over the canonical
    // `Datum` lane, which carries the by-reference value through the fmgr
    // by-reference side channel.
    if entry.cast_func != InvalidOid {
        lhs = fmgr_seams::function_call3_coll_datum::call(
            mcx,
            entry.cast_func,
            lhs,
            Datum::from_i32(-1),     // typmod
            Datum::from_bool(false), // implicit coercion
        )?;
        rhs = fmgr_seams::function_call3_coll_datum::call(
            mcx,
            entry.cast_func,
            rhs,
            Datum::from_i32(-1),
            Datum::from_bool(false),
        )?;
    }

    // Apply the comparison operator.
    //
    // Note: the comparison here would in principle need the collation of the
    // *other* table; for simplicity we use our own collation, which is fine
    // because both collations are required to share a notion of equality.
    let result = fmgr_seams::function_call2_coll_datum::call(
        mcx,
        entry.eq_opr_func,
        collid,
        lhs,
        rhs,
    )?;
    Ok(result.as_bool())
}

/// `RI_FKey_trigger_type` --- classify a trigger function OID.
pub fn ri_fkey_trigger_type(tgfoid: Oid) -> i32 {
    match tgfoid {
        x if x == F_RI_FKEY_CASCADE_DEL
            || x == F_RI_FKEY_CASCADE_UPD
            || x == F_RI_FKEY_RESTRICT_DEL
            || x == F_RI_FKEY_RESTRICT_UPD
            || x == F_RI_FKEY_SETNULL_DEL
            || x == F_RI_FKEY_SETNULL_UPD
            || x == F_RI_FKEY_SETDEFAULT_DEL
            || x == F_RI_FKEY_SETDEFAULT_UPD
            || x == F_RI_FKEY_NOACTION_DEL
            || x == F_RI_FKEY_NOACTION_UPD =>
        {
            RI_TRIGGER_PK
        }
        x if x == F_RI_FKEY_CHECK_INS || x == F_RI_FKEY_CHECK_UPD => RI_TRIGGER_FK,
        _ => RI_TRIGGER_NONE,
    }
}

/// `ri_Check_Pk_Match` --- check whether another PK row provides the same key
/// values as the modified/deleted `oldslot`. Returns true if a match is found.
pub fn ri_check_pk_match(
    mcx: Mcx<'_>,
    pk_rel: RelSide<'_, '_>,
    fk_rel: RelSide<'_, '_>,
    oldslot: TupleTableSlotRef,
    riinfo: &RiConstraintInfo,
) -> PgResult<bool> {
    use crate::cache::{ri_build_query_key, ri_fetch_prepared_plan};

    spi_seams::spi_connect::call()?;

    let qkey = ri_build_query_key(riinfo, RI_PLAN_CHECK_LOOKUPPK_FROM_PK);

    let qplan = match ri_fetch_prepared_plan(&qkey)? {
        Some(plan) => plan,
        None => {
            let mut queryoids = [0 as Oid; RI_MAX_NUMKEYS];
            let mut querybuf: Vec<u8> = Vec::new();

            let pk_only = if pk_rel.is_partitioned() { "" } else { "ONLY " };
            if riinfo.hasperiod {
                try_extend(mcx, &mut querybuf, b"SELECT 1 FROM (SELECT ")?;
                let quoted = quote_one_name(
                    mcx,
                    &pk_rel.att_name(mcx, riinfo.pk_attnums[riinfo.nkeys as usize - 1])?,
                )?;
                try_extend(mcx, &mut querybuf, &quoted)?;
                try_extend(mcx, &mut querybuf, b" AS r FROM ")?;
                try_extend(mcx, &mut querybuf, pk_only.as_bytes())?;
                append_quoted_relation(mcx, &mut querybuf, pk_rel)?;
                try_extend(mcx, &mut querybuf, b" x")?;
            } else {
                try_extend(mcx, &mut querybuf, b"SELECT 1 FROM ")?;
                try_extend(mcx, &mut querybuf, pk_only.as_bytes())?;
                append_quoted_relation(mcx, &mut querybuf, pk_rel)?;
                try_extend(mcx, &mut querybuf, b" x")?;
            }
            let mut querysep = "WHERE";
            for i in 0..riinfo.nkeys as usize {
                let pk_type = pk_rel.att_type(riinfo.pk_attnums[i]);
                let quoted = quote_one_name(mcx, &pk_rel.att_name(mcx, riinfo.pk_attnums[i])?)?;
                let paramname = format!("${}", i + 1);
                ri_generate_qual(
                    mcx,
                    &mut querybuf,
                    querysep,
                    &quoted,
                    pk_type,
                    riinfo.pp_eq_oprs[i],
                    paramname.as_bytes(),
                    pk_type,
                )?;
                querysep = "AND";
                queryoids[i] = pk_type;
            }
            try_extend(mcx, &mut querybuf, b" FOR KEY SHARE OF x")?;
            if riinfo.hasperiod {
                let fk_type = fk_rel.att_type(riinfo.fk_attnums[riinfo.nkeys as usize - 1]);
                try_extend(mcx, &mut querybuf, b") x1 HAVING ")?;
                let paramname = format!("${}", riinfo.nkeys);
                ri_generate_qual(
                    mcx,
                    &mut querybuf,
                    "",
                    paramname.as_bytes(),
                    fk_type,
                    riinfo.agged_period_contained_by_oper,
                    b"pg_catalog.range_agg",
                    ANYMULTIRANGEOID,
                )?;
                try_extend(mcx, &mut querybuf, b"(x1.r)")?;
            }
            ri_plan_check(
                mcx,
                &querybuf,
                &queryoids[..riinfo.nkeys as usize],
                &qkey,
                fk_rel,
                pk_rel,
            )?
        }
    };

    let result = ri_perform_check(
        mcx,
        riinfo,
        &qkey,
        qplan,
        fk_rel,
        pk_rel,
        Some(oldslot),
        None,
        false,
        true,
        SPI_OK_SELECT,
    )?;

    if spi_seams::spi_finish::call()? != SPI_OK_FINISH {
        return Err(PgError::error("SPI_finish failed"));
    }

    Ok(result)
}
