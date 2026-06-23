//! The RI trigger entry points and bulk-validation queries.
//!
//! Each entry point takes a [`TriggerDataRef`] (the C `TriggerData *`) and
//! returns `PgResult<()>` (the trigger procs return `PointerGetDatum(NULL)`),
//! plus `RI_FKey_pk/fk_upd_check_required` (`bool`) and the bulk
//! `RI_Initial_Check` / `RI_PartitionRemove_Check`.

extern crate alloc;
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

use mcx::Mcx;
use types_core::Oid;
use types_error::{PgError, PgResult, ERRCODE_FOREIGN_KEY_VIOLATION};
use types_storage::lock::{RowExclusiveLock, RowShareLock};
use types_guc::guc::{GucContext, PGC_S_SESSION};
use types_ri_triggers::{TriggerDataRef, TriggerRef, TupleTableSlotRef};

use crate::cache::{ri_build_query_key, ri_fetch_constraint_info, ri_fetch_prepared_plan};
use crate::checks::{
    attach_table_constraint, ri_check_pk_match, ri_check_trigger, ri_keys_equal, ri_null_check,
    ri_perform_check, ri_plan_check, ri_report_violation, ViolatorSource,
};
use crate::querybuild::{
    append_quoted_name, append_quoted_relation, quote_one_name, ri_generate_qual,
    ri_generate_qual_collation, try_extend, try_push,
};
use crate::{
    trigger_fired_by_update, RelSide, RiConstraintInfo, ANYMULTIRANGEOID, FKCONSTR_MATCH_FULL,
    FKCONSTR_MATCH_PARTIAL, FKCONSTR_MATCH_SIMPLE, RI_KEYS_ALL_NULL, RI_KEYS_NONE_NULL,
    RI_KEYS_SOME_NULL, RI_MAX_NUMKEYS, RI_PLAN_CASCADE_ONDELETE, RI_PLAN_CASCADE_ONUPDATE,
    RI_PLAN_CHECK_LOOKUPPK, RI_PLAN_NO_ACTION, RI_PLAN_RESTRICT, RI_PLAN_SETDEFAULT_ONDELETE,
    RI_PLAN_SETDEFAULT_ONUPDATE, RI_PLAN_SETNULL_ONDELETE, RI_PLAN_SETNULL_ONUPDATE,
    RI_TRIGTYPE_DELETE, RI_TRIGTYPE_INSERT, RI_TRIGTYPE_UPDATE, SPI_OK_DELETE, SPI_OK_FINISH,
    SPI_OK_SELECT, SPI_OK_UPDATE,
};

use table::table_open;
use trigger_seams as trig;
use spi_seams as spi;
use guc_seams as guc;

/// `RI_FKey_check` --- check foreign key existence (combined INSERT/UPDATE).
pub fn ri_fkey_check(mcx: Mcx<'_>, trigdata: TriggerDataRef) -> PgResult<()> {
    let trigger = trig::tg_trigger::call(trigdata);
    let trig_rel = RelSide::Trigger(trigdata);
    let riinfo = ri_fetch_constraint_info(mcx, trigger, trig_rel, false)?;

    let event = trig::tg_event::call(trigdata);
    let newslot = if trigger_fired_by_update(event) {
        trig::tg_newslot::call(trigdata)
    } else {
        trig::tg_trigslot::call(trigdata)
    };

    // Skip the row if it is no longer valid per SnapshotSelf.
    if !trig::tg_relation_tuple_satisfies_snapshot_self::call(trigdata, newslot)? {
        return Ok(());
    }

    // fk_rel is the trigger relation; pk_rel opened in RowShareLock.
    let fk_rel = trig_rel;
    let pk_rel = table_open(mcx, riinfo.pk_relid, RowShareLock)?;

    match ri_null_check(newslot, &riinfo, false)? {
        RI_KEYS_ALL_NULL => {
            pk_rel.close(RowShareLock)?;
            return Ok(());
        }
        RI_KEYS_SOME_NULL => match riinfo.confmatchtype {
            FKCONSTR_MATCH_FULL => {
                let fkname = String::from_utf8_lossy(&fk_rel.name_bytes(mcx)?).into_owned();
                let err = PgError::error(format!(
                    "insert or update on table \"{fkname}\" violates foreign key constraint \"{}\"",
                    riinfo.conname_str()
                ))
                .with_sqlstate(ERRCODE_FOREIGN_KEY_VIOLATION)
                .with_detail("MATCH FULL does not allow mixing of null and nonnull key values.");
                let err = attach_table_constraint(mcx, err, fk_rel, &riinfo)?;
                pk_rel.close(RowShareLock)?;
                return Err(err);
            }
            FKCONSTR_MATCH_SIMPLE => {
                pk_rel.close(RowShareLock)?;
                return Ok(());
            }
            _ => {
                // FKCONSTR_MATCH_PARTIAL is rejected earlier; C falls through
                // into the NONE_NULL handling. Continue below.
            }
        },
        RI_KEYS_NONE_NULL => {}
        _ => {}
    }

    finish_fkey_check(mcx, &riinfo, fk_rel, pk_rel, newslot)
}

fn finish_fkey_check<'mcx>(
    mcx: Mcx<'mcx>,
    riinfo: &RiConstraintInfo,
    fk_rel: RelSide<'_, 'mcx>,
    pk_rel: rel::Relation<'mcx>,
    newslot: TupleTableSlotRef,
) -> PgResult<()> {
    let pk_side = RelSide::Open(&pk_rel);
    spi::spi_connect::call()?;

    let qkey = ri_build_query_key(riinfo, RI_PLAN_CHECK_LOOKUPPK);

    let qplan = match ri_fetch_prepared_plan(&qkey)? {
        Some(plan) => plan,
        None => {
            let mut queryoids = [0 as Oid; RI_MAX_NUMKEYS];
            let mut querybuf: Vec<u8> = Vec::new();
            let pk_only = if pk_side.is_partitioned() { "" } else { "ONLY " };
            if riinfo.hasperiod {
                try_extend(mcx, &mut querybuf, b"SELECT 1 FROM (SELECT ")?;
                append_quoted_name(
                    mcx,
                    &mut querybuf,
                    &pk_side.att_name(mcx, riinfo.pk_attnums[riinfo.nkeys as usize - 1])?,
                )?;
                try_extend(mcx, &mut querybuf, b" AS r FROM ")?;
                try_extend(mcx, &mut querybuf, pk_only.as_bytes())?;
                append_quoted_relation(mcx, &mut querybuf, pk_side)?;
                try_extend(mcx, &mut querybuf, b" x")?;
            } else {
                try_extend(mcx, &mut querybuf, b"SELECT 1 FROM ")?;
                try_extend(mcx, &mut querybuf, pk_only.as_bytes())?;
                append_quoted_relation(mcx, &mut querybuf, pk_side)?;
                try_extend(mcx, &mut querybuf, b" x")?;
            }
            let mut querysep = "WHERE";
            for i in 0..riinfo.nkeys as usize {
                let pk_type = pk_side.att_type(riinfo.pk_attnums[i]);
                let fk_type = fk_rel.att_type(riinfo.fk_attnums[i]);
                let quoted = quote_one_name(mcx, &pk_side.att_name(mcx, riinfo.pk_attnums[i])?)?;
                let paramname = format!("${}", i + 1);
                ri_generate_qual(
                    mcx,
                    &mut querybuf,
                    querysep,
                    &quoted,
                    pk_type,
                    riinfo.pf_eq_oprs[i],
                    paramname.as_bytes(),
                    fk_type,
                )?;
                querysep = "AND";
                queryoids[i] = fk_type;
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
                pk_side,
            )?
        }
    };

    let detect_new_rows = pk_side.is_partitioned();
    ri_perform_check(
        mcx,
        riinfo,
        &qkey,
        qplan,
        fk_rel,
        pk_side,
        None,
        Some(newslot),
        false,
        detect_new_rows,
        SPI_OK_SELECT,
    )?;

    if spi::spi_finish::call()? != SPI_OK_FINISH {
        return Err(PgError::error("SPI_finish failed"));
    }

    let _ = pk_side;
    pk_rel.close(RowShareLock)
}

/// `RI_FKey_check_ins`.
pub fn ri_fkey_check_ins(mcx: Mcx<'_>, trigdata: TriggerDataRef) -> PgResult<()> {
    ri_check_trigger(trigdata, "RI_FKey_check_ins", RI_TRIGTYPE_INSERT)?;
    ri_fkey_check(mcx, trigdata)
}

/// `RI_FKey_check_upd`.
pub fn ri_fkey_check_upd(mcx: Mcx<'_>, trigdata: TriggerDataRef) -> PgResult<()> {
    ri_check_trigger(trigdata, "RI_FKey_check_upd", RI_TRIGTYPE_UPDATE)?;
    ri_fkey_check(mcx, trigdata)
}

/// `RI_FKey_noaction_del`.
pub fn ri_fkey_noaction_del(mcx: Mcx<'_>, trigdata: TriggerDataRef) -> PgResult<()> {
    ri_check_trigger(trigdata, "RI_FKey_noaction_del", RI_TRIGTYPE_DELETE)?;
    ri_restrict(mcx, trigdata, true)
}

/// `RI_FKey_restrict_del`.
pub fn ri_fkey_restrict_del(mcx: Mcx<'_>, trigdata: TriggerDataRef) -> PgResult<()> {
    ri_check_trigger(trigdata, "RI_FKey_restrict_del", RI_TRIGTYPE_DELETE)?;
    ri_restrict(mcx, trigdata, false)
}

/// `RI_FKey_noaction_upd`.
pub fn ri_fkey_noaction_upd(mcx: Mcx<'_>, trigdata: TriggerDataRef) -> PgResult<()> {
    ri_check_trigger(trigdata, "RI_FKey_noaction_upd", RI_TRIGTYPE_UPDATE)?;
    ri_restrict(mcx, trigdata, true)
}

/// `RI_FKey_restrict_upd`.
pub fn ri_fkey_restrict_upd(mcx: Mcx<'_>, trigdata: TriggerDataRef) -> PgResult<()> {
    ri_check_trigger(trigdata, "RI_FKey_restrict_upd", RI_TRIGTYPE_UPDATE)?;
    ri_restrict(mcx, trigdata, false)
}

/// `ri_restrict` --- common code for ON DELETE/UPDATE RESTRICT / NO ACTION.
pub fn ri_restrict(mcx: Mcx<'_>, trigdata: TriggerDataRef, is_no_action: bool) -> PgResult<()> {
    let trigger = trig::tg_trigger::call(trigdata);
    let trig_rel = RelSide::Trigger(trigdata);
    let riinfo = ri_fetch_constraint_info(mcx, trigger, trig_rel, true)?;

    let fk_rel = table_open(mcx, riinfo.fk_relid, RowShareLock)?;
    let fk_side = RelSide::Open(&fk_rel);
    let pk_rel = trig_rel;
    let oldslot = trig::tg_trigslot::call(trigdata);

    if is_no_action && !riinfo.hasperiod && ri_check_pk_match(mcx, pk_rel, fk_side, oldslot, &riinfo)? {
        fk_rel.close(RowShareLock)?;
        return Ok(());
    }

    spi::spi_connect::call()?;

    let qkey = ri_build_query_key(
        &riinfo,
        if is_no_action {
            RI_PLAN_NO_ACTION
        } else {
            RI_PLAN_RESTRICT
        },
    );

    let qplan = match ri_fetch_prepared_plan(&qkey)? {
        Some(plan) => plan,
        None => {
            let mut queryoids = [0 as Oid; RI_MAX_NUMKEYS];
            let mut querybuf: Vec<u8> = Vec::new();
            let fk_only = if fk_side.is_partitioned() { "" } else { "ONLY " };
            try_extend(mcx, &mut querybuf, b"SELECT 1 FROM ")?;
            try_extend(mcx, &mut querybuf, fk_only.as_bytes())?;
            append_quoted_relation(mcx, &mut querybuf, fk_side)?;
            try_extend(mcx, &mut querybuf, b" x")?;
            let mut querysep = "WHERE";
            for i in 0..riinfo.nkeys as usize {
                let pk_type = pk_rel.att_type(riinfo.pk_attnums[i]);
                let fk_type = fk_side.att_type(riinfo.fk_attnums[i]);
                let quoted = quote_one_name(mcx, &fk_side.att_name(mcx, riinfo.fk_attnums[i])?)?;
                let paramname = format!("${}", i + 1);
                ri_generate_qual(
                    mcx,
                    &mut querybuf,
                    querysep,
                    paramname.as_bytes(),
                    pk_type,
                    riinfo.pf_eq_oprs[i],
                    &quoted,
                    fk_type,
                )?;
                querysep = "AND";
                queryoids[i] = pk_type;
            }

            // Temporal NO ACTION: the coalesce/range_agg sub-select.
            if riinfo.hasperiod && is_no_action {
                let pk_period_type = pk_rel.att_type(riinfo.pk_attnums[riinfo.nkeys as usize - 1]);
                let fk_period_type = fk_side.att_type(riinfo.fk_attnums[riinfo.nkeys as usize - 1]);
                let pk_only = if pk_rel.is_partitioned() { "" } else { "ONLY " };

                let attname = quote_one_name(
                    mcx,
                    &fk_side.att_name(mcx, riinfo.fk_attnums[riinfo.nkeys as usize - 1])?,
                )?;
                let paramname = format!("${}", riinfo.nkeys);

                try_extend(mcx, &mut querybuf, b" AND NOT coalesce(")?;

                let mut intersectbuf: Vec<u8> = Vec::new();
                try_push(mcx, &mut intersectbuf, b'(')?;
                ri_generate_qual(
                    mcx,
                    &mut intersectbuf,
                    "",
                    &attname,
                    fk_period_type,
                    riinfo.period_intersect_oper,
                    paramname.as_bytes(),
                    pk_period_type,
                )?;
                try_push(mcx, &mut intersectbuf, b')')?;

                let mut replacementsbuf: Vec<u8> = Vec::new();
                try_extend(mcx, &mut replacementsbuf, b"(SELECT pg_catalog.range_agg(r) FROM ")?;
                try_extend(mcx, &mut replacementsbuf, b"(SELECT y.")?;
                append_quoted_name(
                    mcx,
                    &mut replacementsbuf,
                    &pk_rel.att_name(mcx, riinfo.pk_attnums[riinfo.nkeys as usize - 1])?,
                )?;
                try_extend(mcx, &mut replacementsbuf, b" r FROM ")?;
                try_extend(mcx, &mut replacementsbuf, pk_only.as_bytes())?;
                append_quoted_relation(mcx, &mut replacementsbuf, pk_rel)?;
                try_extend(mcx, &mut replacementsbuf, b" y")?;

                let mut querysep2 = "WHERE";
                for i in 0..riinfo.nkeys as usize {
                    let pk_type = pk_rel.att_type(riinfo.pk_attnums[i]);
                    let quoted = quote_one_name(mcx, &pk_rel.att_name(mcx, riinfo.pk_attnums[i])?)?;
                    let paramname = format!("${}", i + 1);
                    ri_generate_qual(
                        mcx,
                        &mut replacementsbuf,
                        querysep2,
                        paramname.as_bytes(),
                        pk_type,
                        riinfo.pp_eq_oprs[i],
                        &quoted,
                        pk_type,
                    )?;
                    querysep2 = "AND";
                    queryoids[i] = pk_type;
                }
                try_extend(mcx, &mut replacementsbuf, b" FOR KEY SHARE OF y) y2)")?;

                ri_generate_qual(
                    mcx,
                    &mut querybuf,
                    "",
                    &intersectbuf,
                    fk_period_type,
                    riinfo.agged_period_contained_by_oper,
                    &replacementsbuf,
                    ANYMULTIRANGEOID,
                )?;
                try_extend(mcx, &mut querybuf, b", false)")?;
            }

            try_extend(mcx, &mut querybuf, b" FOR KEY SHARE OF x")?;
            ri_plan_check(
                mcx,
                &querybuf,
                &queryoids[..riinfo.nkeys as usize],
                &qkey,
                fk_side,
                pk_rel,
            )?
        }
    };

    ri_perform_check(
        mcx,
        &riinfo,
        &qkey,
        qplan,
        fk_side,
        pk_rel,
        Some(oldslot),
        None,
        !is_no_action,
        true,
        SPI_OK_SELECT,
    )?;

    if spi::spi_finish::call()? != SPI_OK_FINISH {
        return Err(PgError::error("SPI_finish failed"));
    }

    fk_rel.close(RowShareLock)
}

/// `RI_FKey_cascade_del` --- cascaded delete at delete event on PK.
pub fn ri_fkey_cascade_del(mcx: Mcx<'_>, trigdata: TriggerDataRef) -> PgResult<()> {
    ri_check_trigger(trigdata, "RI_FKey_cascade_del", RI_TRIGTYPE_DELETE)?;

    let trigger = trig::tg_trigger::call(trigdata);
    let trig_rel = RelSide::Trigger(trigdata);
    let riinfo = ri_fetch_constraint_info(mcx, trigger, trig_rel, true)?;

    let fk_rel = table_open(mcx, riinfo.fk_relid, RowExclusiveLock)?;
    let fk_side = RelSide::Open(&fk_rel);
    let pk_rel = trig_rel;
    let oldslot = trig::tg_trigslot::call(trigdata);

    spi::spi_connect::call()?;

    let qkey = ri_build_query_key(&riinfo, RI_PLAN_CASCADE_ONDELETE);

    let qplan = match ri_fetch_prepared_plan(&qkey)? {
        Some(plan) => plan,
        None => {
            let mut queryoids = [0 as Oid; RI_MAX_NUMKEYS];
            let mut querybuf: Vec<u8> = Vec::new();
            let fk_only = if fk_side.is_partitioned() { "" } else { "ONLY " };
            try_extend(mcx, &mut querybuf, b"DELETE FROM ")?;
            try_extend(mcx, &mut querybuf, fk_only.as_bytes())?;
            append_quoted_relation(mcx, &mut querybuf, fk_side)?;
            let mut querysep = "WHERE";
            for i in 0..riinfo.nkeys as usize {
                let pk_type = pk_rel.att_type(riinfo.pk_attnums[i]);
                let fk_type = fk_side.att_type(riinfo.fk_attnums[i]);
                let quoted = quote_one_name(mcx, &fk_side.att_name(mcx, riinfo.fk_attnums[i])?)?;
                let paramname = format!("${}", i + 1);
                ri_generate_qual(
                    mcx,
                    &mut querybuf,
                    querysep,
                    paramname.as_bytes(),
                    pk_type,
                    riinfo.pf_eq_oprs[i],
                    &quoted,
                    fk_type,
                )?;
                querysep = "AND";
                queryoids[i] = pk_type;
            }
            ri_plan_check(
                mcx,
                &querybuf,
                &queryoids[..riinfo.nkeys as usize],
                &qkey,
                fk_side,
                pk_rel,
            )?
        }
    };

    ri_perform_check(
        mcx,
        &riinfo,
        &qkey,
        qplan,
        fk_side,
        pk_rel,
        Some(oldslot),
        None,
        false,
        true,
        SPI_OK_DELETE,
    )?;

    if spi::spi_finish::call()? != SPI_OK_FINISH {
        return Err(PgError::error("SPI_finish failed"));
    }

    fk_rel.close(RowExclusiveLock)
}

/// `RI_FKey_cascade_upd` --- cascaded update at update event on PK.
pub fn ri_fkey_cascade_upd(mcx: Mcx<'_>, trigdata: TriggerDataRef) -> PgResult<()> {
    ri_check_trigger(trigdata, "RI_FKey_cascade_upd", RI_TRIGTYPE_UPDATE)?;

    let trigger = trig::tg_trigger::call(trigdata);
    let trig_rel = RelSide::Trigger(trigdata);
    let riinfo = ri_fetch_constraint_info(mcx, trigger, trig_rel, true)?;

    let fk_rel = table_open(mcx, riinfo.fk_relid, RowExclusiveLock)?;
    let fk_side = RelSide::Open(&fk_rel);
    let pk_rel = trig_rel;
    let newslot = trig::tg_newslot::call(trigdata);
    let oldslot = trig::tg_trigslot::call(trigdata);

    spi::spi_connect::call()?;

    let qkey = ri_build_query_key(&riinfo, RI_PLAN_CASCADE_ONUPDATE);

    let qplan = match ri_fetch_prepared_plan(&qkey)? {
        Some(plan) => plan,
        None => {
            let mut queryoids = [0 as Oid; RI_MAX_NUMKEYS * 2];
            let mut querybuf: Vec<u8> = Vec::new();
            let mut qualbuf: Vec<u8> = Vec::new();
            let fk_only = if fk_side.is_partitioned() { "" } else { "ONLY " };
            try_extend(mcx, &mut querybuf, b"UPDATE ")?;
            try_extend(mcx, &mut querybuf, fk_only.as_bytes())?;
            append_quoted_relation(mcx, &mut querybuf, fk_side)?;
            try_extend(mcx, &mut querybuf, b" SET")?;
            let mut querysep = "";
            let mut qualsep = "WHERE";
            let nkeys = riinfo.nkeys as usize;
            for i in 0..nkeys {
                let j = nkeys + i;
                let pk_type = pk_rel.att_type(riinfo.pk_attnums[i]);
                let fk_type = fk_side.att_type(riinfo.fk_attnums[i]);
                let quoted = quote_one_name(mcx, &fk_side.att_name(mcx, riinfo.fk_attnums[i])?)?;
                try_extend(mcx, &mut querybuf, format!("{querysep} ").as_bytes())?;
                try_extend(mcx, &mut querybuf, &quoted)?;
                try_extend(mcx, &mut querybuf, format!(" = ${}", i + 1).as_bytes())?;
                let paramname = format!("${}", j + 1);
                ri_generate_qual(
                    mcx,
                    &mut qualbuf,
                    qualsep,
                    paramname.as_bytes(),
                    pk_type,
                    riinfo.pf_eq_oprs[i],
                    &quoted,
                    fk_type,
                )?;
                querysep = ",";
                qualsep = "AND";
                queryoids[i] = pk_type;
                queryoids[j] = pk_type;
            }
            try_extend(mcx, &mut querybuf, &qualbuf)?;
            ri_plan_check(
                mcx,
                &querybuf,
                &queryoids[..riinfo.nkeys as usize * 2],
                &qkey,
                fk_side,
                pk_rel,
            )?
        }
    };

    ri_perform_check(
        mcx,
        &riinfo,
        &qkey,
        qplan,
        fk_side,
        pk_rel,
        Some(oldslot),
        Some(newslot),
        false,
        true,
        SPI_OK_UPDATE,
    )?;

    if spi::spi_finish::call()? != SPI_OK_FINISH {
        return Err(PgError::error("SPI_finish failed"));
    }

    fk_rel.close(RowExclusiveLock)
}

/// `RI_FKey_setnull_del`.
pub fn ri_fkey_setnull_del(mcx: Mcx<'_>, trigdata: TriggerDataRef) -> PgResult<()> {
    ri_check_trigger(trigdata, "RI_FKey_setnull_del", RI_TRIGTYPE_DELETE)?;
    ri_set(mcx, trigdata, true, RI_TRIGTYPE_DELETE)
}

/// `RI_FKey_setnull_upd`.
pub fn ri_fkey_setnull_upd(mcx: Mcx<'_>, trigdata: TriggerDataRef) -> PgResult<()> {
    ri_check_trigger(trigdata, "RI_FKey_setnull_upd", RI_TRIGTYPE_UPDATE)?;
    ri_set(mcx, trigdata, true, RI_TRIGTYPE_UPDATE)
}

/// `RI_FKey_setdefault_del`.
pub fn ri_fkey_setdefault_del(mcx: Mcx<'_>, trigdata: TriggerDataRef) -> PgResult<()> {
    ri_check_trigger(trigdata, "RI_FKey_setdefault_del", RI_TRIGTYPE_DELETE)?;
    ri_set(mcx, trigdata, false, RI_TRIGTYPE_DELETE)
}

/// `RI_FKey_setdefault_upd`.
pub fn ri_fkey_setdefault_upd(mcx: Mcx<'_>, trigdata: TriggerDataRef) -> PgResult<()> {
    ri_check_trigger(trigdata, "RI_FKey_setdefault_upd", RI_TRIGTYPE_UPDATE)?;
    ri_set(mcx, trigdata, false, RI_TRIGTYPE_UPDATE)
}

/// `ri_set` --- common code for ON DELETE/UPDATE SET NULL / SET DEFAULT.
pub fn ri_set(
    mcx: Mcx<'_>,
    trigdata: TriggerDataRef,
    is_set_null: bool,
    tgkind: i32,
) -> PgResult<()> {
    let trigger = trig::tg_trigger::call(trigdata);
    let trig_rel = RelSide::Trigger(trigdata);
    let riinfo = ri_fetch_constraint_info(mcx, trigger, trig_rel, true)?;

    let fk_rel = table_open(mcx, riinfo.fk_relid, RowExclusiveLock)?;
    let fk_side = RelSide::Open(&fk_rel);
    let pk_rel = trig_rel;
    let oldslot = trig::tg_trigslot::call(trigdata);

    spi::spi_connect::call()?;

    let queryno = match tgkind {
        RI_TRIGTYPE_UPDATE => {
            if is_set_null {
                RI_PLAN_SETNULL_ONUPDATE
            } else {
                RI_PLAN_SETDEFAULT_ONUPDATE
            }
        }
        RI_TRIGTYPE_DELETE => {
            if is_set_null {
                RI_PLAN_SETNULL_ONDELETE
            } else {
                RI_PLAN_SETDEFAULT_ONDELETE
            }
        }
        _ => return Err(PgError::error("invalid tgkind passed to ri_set")),
    };

    let qkey = ri_build_query_key(&riinfo, queryno);

    let qplan = match ri_fetch_prepared_plan(&qkey)? {
        Some(plan) => plan,
        None => {
            let mut queryoids = [0 as Oid; RI_MAX_NUMKEYS];

            let (num_cols_to_set, set_cols): (usize, [i16; RI_MAX_NUMKEYS]) = match tgkind {
                RI_TRIGTYPE_UPDATE => (riinfo.nkeys as usize, riinfo.fk_attnums),
                RI_TRIGTYPE_DELETE => {
                    if riinfo.ndelsetcols != 0 {
                        (riinfo.ndelsetcols as usize, riinfo.confdelsetcols)
                    } else {
                        (riinfo.nkeys as usize, riinfo.fk_attnums)
                    }
                }
                _ => return Err(PgError::error("invalid tgkind passed to ri_set")),
            };

            let mut querybuf: Vec<u8> = Vec::new();
            let fk_only = if fk_side.is_partitioned() { "" } else { "ONLY " };
            try_extend(mcx, &mut querybuf, b"UPDATE ")?;
            try_extend(mcx, &mut querybuf, fk_only.as_bytes())?;
            append_quoted_relation(mcx, &mut querybuf, fk_side)?;
            try_extend(mcx, &mut querybuf, b" SET")?;

            let mut querysep = "";
            for &col in set_cols.iter().take(num_cols_to_set) {
                let quoted = quote_one_name(mcx, &fk_side.att_name(mcx, col)?)?;
                let val = if is_set_null { "NULL" } else { "DEFAULT" };
                try_extend(mcx, &mut querybuf, format!("{querysep} ").as_bytes())?;
                try_extend(mcx, &mut querybuf, &quoted)?;
                try_extend(mcx, &mut querybuf, format!(" = {val}").as_bytes())?;
                querysep = ",";
            }

            let mut qualsep = "WHERE";
            for i in 0..riinfo.nkeys as usize {
                let pk_type = pk_rel.att_type(riinfo.pk_attnums[i]);
                let fk_type = fk_side.att_type(riinfo.fk_attnums[i]);
                let quoted = quote_one_name(mcx, &fk_side.att_name(mcx, riinfo.fk_attnums[i])?)?;
                let paramname = format!("${}", i + 1);
                ri_generate_qual(
                    mcx,
                    &mut querybuf,
                    qualsep,
                    paramname.as_bytes(),
                    pk_type,
                    riinfo.pf_eq_oprs[i],
                    &quoted,
                    fk_type,
                )?;
                qualsep = "AND";
                queryoids[i] = pk_type;
            }
            ri_plan_check(
                mcx,
                &querybuf,
                &queryoids[..riinfo.nkeys as usize],
                &qkey,
                fk_side,
                pk_rel,
            )?
        }
    };

    ri_perform_check(
        mcx,
        &riinfo,
        &qkey,
        qplan,
        fk_side,
        pk_rel,
        Some(oldslot),
        None,
        false,
        true,
        SPI_OK_UPDATE,
    )?;

    if spi::spi_finish::call()? != SPI_OK_FINISH {
        return Err(PgError::error("SPI_finish failed"));
    }

    fk_rel.close(RowExclusiveLock)?;

    if is_set_null {
        Ok(())
    } else {
        ri_restrict(mcx, trigdata, true)
    }
}

/// `RI_FKey_pk_upd_check_required`. `newslot == None` for a delete.
pub fn ri_fkey_pk_upd_check_required(
    mcx: Mcx<'_>,
    trigger: TriggerRef,
    pk_rel: TriggerDataRef,
    oldslot: TupleTableSlotRef,
    newslot: Option<TupleTableSlotRef>,
) -> PgResult<bool> {
    let pk_side = RelSide::Trigger(pk_rel);
    let riinfo = ri_fetch_constraint_info(mcx, trigger, pk_side, true)?;

    if ri_null_check(oldslot, &riinfo, true)? != RI_KEYS_NONE_NULL {
        return Ok(false);
    }

    if let Some(newslot) = newslot {
        if ri_keys_equal(mcx, pk_side, oldslot, newslot, &riinfo, true)? {
            return Ok(false);
        }
    }

    Ok(true)
}

/// `RI_FKey_fk_upd_check_required`.
pub fn ri_fkey_fk_upd_check_required(
    mcx: Mcx<'_>,
    trigger: TriggerRef,
    fk_rel: TriggerDataRef,
    oldslot: TupleTableSlotRef,
    newslot: TupleTableSlotRef,
) -> PgResult<bool> {
    let fk_side = RelSide::Trigger(fk_rel);
    let riinfo = ri_fetch_constraint_info(mcx, trigger, fk_side, false)?;

    let ri_nullcheck = ri_null_check(newslot, &riinfo, false)?;

    if ri_nullcheck == RI_KEYS_ALL_NULL {
        return Ok(false);
    } else if ri_nullcheck == RI_KEYS_SOME_NULL {
        match riinfo.confmatchtype {
            FKCONSTR_MATCH_SIMPLE => return Ok(false),
            FKCONSTR_MATCH_PARTIAL => {}
            FKCONSTR_MATCH_FULL => return Ok(true),
            _ => {}
        }
    }

    if trig::slot_is_current_xact_tuple::call(oldslot)? {
        return Ok(true);
    }

    if ri_keys_equal(mcx, fk_side, oldslot, newslot, &riinfo, false)? {
        return Ok(false);
    }

    Ok(true)
}

/// `RI_Initial_Check` --- validate an entire table's existing FK data with one
/// LEFT JOIN query (ALTER TABLE ADD FOREIGN KEY). Returns `false` if the caller
/// must fall back to the trigger method (insufficient permissions / RLS).
pub fn ri_initial_check(
    mcx: Mcx<'_>,
    trigger: TriggerRef,
    fk_rel: &rel::Relation<'_>,
    pk_rel: &rel::Relation<'_>,
) -> PgResult<bool> {
    let fk_side = RelSide::Open(fk_rel);
    let pk_side = RelSide::Open(pk_rel);
    let riinfo = ri_fetch_constraint_info(mcx, trigger, fk_side, false)?;

    // Permission + RLS gate (ExecCheckPermissions / bypassrls / ownercheck).
    let pk_attnums = riinfo.pk_attnums[..riinfo.nkeys as usize].to_vec();
    let fk_attnums = riinfo.fk_attnums[..riinfo.nkeys as usize].to_vec();
    if !execMain_seams::exec_check_permissions_select::call(
        &[
            (pk_rel.rd_id, pk_rel.rd_rel.relkind, &pk_attnums),
            (fk_rel.rd_id, fk_rel.rd_rel.relkind, &fk_attnums),
        ],
        false,
    )? {
        return Ok(false);
    }
    let userid = miscinit_seams::get_user_id::call();
    if !acl_seams::has_bypassrls_privilege::call(userid)?
        && ((pk_rel.rd_rel.relrowsecurity
            && !aclchk_seams::object_ownercheck::call(
                types_catalog::catalog::RELATION_RELATION_ID,
                pk_rel.rd_id,
                userid,
            )?)
            || (fk_rel.rd_rel.relrowsecurity
                && !aclchk_seams::object_ownercheck::call(
                    types_catalog::catalog::RELATION_RELATION_ID,
                    fk_rel.rd_id,
                    userid,
                )?))
    {
        return Ok(false);
    }

    let querybuf = build_join_check_query(mcx, &riinfo, fk_side, pk_side, JoinKind::LeftOuter)?;

    let save_nestlevel = bump_work_mem_for_validation()?;

    spi::spi_connect::call()?;

    let qplan = match spi::spi_prepare::call(&querybuf, &[])? {
        Some(plan) => plan,
        None => {
            let code = spi::spi_result_code_string::call(mcx, -1)?;
            let querytext = String::from_utf8_lossy(&querybuf);
            return Err(PgError::error(format!(
                "SPI_prepare returned {} for {querytext}",
                code.as_str()
            )));
        }
    };

    let exec = spi::spi_execute_snapshot::call(
        qplan,
        &[],
        &[],
        Some(snapmgr_seams::get_latest_snapshot::call()?),
        None,
        true,
        false,
        1,
    )?;

    if exec.code != SPI_OK_SELECT {
        let code = spi::spi_result_code_string::call(mcx, exec.code)?;
        return Err(PgError::error(format!(
            "SPI_execute_snapshot returned {}",
            code.as_str()
        )));
    }

    if exec.processed > 0 {
        let mut fake_riinfo = riinfo;
        for i in 0..fake_riinfo.nkeys as usize {
            fake_riinfo.fk_attnums[i] = (i + 1) as i16;
        }

        if fake_riinfo.confmatchtype == FKCONSTR_MATCH_FULL
            && ri_initial_result_nullcheck(mcx, &fake_riinfo)? != RI_KEYS_NONE_NULL
        {
            let fkname = String::from_utf8_lossy(&fk_side.name_bytes(mcx)?).into_owned();
            let err = PgError::error(format!(
                "insert or update on table \"{fkname}\" violates foreign key constraint \"{}\"",
                fake_riinfo.conname_str()
            ))
            .with_sqlstate(ERRCODE_FOREIGN_KEY_VIOLATION)
            .with_detail("MATCH FULL does not allow mixing of null and nonnull key values.");
            return Err(attach_table_constraint(mcx, err, fk_side, &fake_riinfo)?);
        }

        return ri_report_violation(
            mcx,
            &fake_riinfo,
            pk_side,
            fk_side,
            ViolatorSource::SpiResult,
            RI_PLAN_CHECK_LOOKUPPK,
            false,
            false,
        )
        .map(|_| true);
    }

    if spi::spi_finish::call()? != SPI_OK_FINISH {
        return Err(PgError::error("SPI_finish failed"));
    }

    guc::at_eoxact_guc::call(true, save_nestlevel)?;

    Ok(true)
}

/// `RI_PartitionRemove_Check`.
pub fn ri_partition_remove_check(
    mcx: Mcx<'_>,
    trigger: TriggerRef,
    fk_rel: &rel::Relation<'_>,
    pk_rel: &rel::Relation<'_>,
) -> PgResult<()> {
    let fk_side = RelSide::Open(fk_rel);
    let pk_side = RelSide::Open(pk_rel);
    let riinfo = ri_fetch_constraint_info(mcx, trigger, fk_side, false)?;

    let querybuf = build_join_check_query(mcx, &riinfo, fk_side, pk_side, JoinKind::PartitionInner)?;

    let save_nestlevel = bump_work_mem_for_validation()?;

    spi::spi_connect::call()?;

    let qplan = match spi::spi_prepare::call(&querybuf, &[])? {
        Some(plan) => plan,
        None => {
            let code = spi::spi_result_code_string::call(mcx, -1)?;
            let querytext = String::from_utf8_lossy(&querybuf);
            return Err(PgError::error(format!(
                "SPI_prepare returned {} for {querytext}",
                code.as_str()
            )));
        }
    };

    let exec = spi::spi_execute_snapshot::call(
        qplan,
        &[],
        &[],
        Some(snapmgr_seams::get_latest_snapshot::call()?),
        None,
        true,
        false,
        1,
    )?;

    if exec.code != SPI_OK_SELECT {
        let code = spi::spi_result_code_string::call(mcx, exec.code)?;
        return Err(PgError::error(format!(
            "SPI_execute_snapshot returned {}",
            code.as_str()
        )));
    }

    if exec.processed > 0 {
        let mut fake_riinfo = riinfo;
        for i in 0..fake_riinfo.nkeys as usize {
            fake_riinfo.pk_attnums[i] = (i + 1) as i16;
        }

        return ri_report_violation(
            mcx,
            &fake_riinfo,
            pk_side,
            fk_side,
            ViolatorSource::SpiResult,
            0,
            false,
            true,
        );
    }

    if spi::spi_finish::call()? != SPI_OK_FINISH {
        return Err(PgError::error("SPI_finish failed"));
    }

    guc::at_eoxact_guc::call(true, save_nestlevel)?;
    Ok(())
}

/// `NewGUCNestLevel()` + set work_mem=maintenance_work_mem + hash_mem_multiplier=1.
fn bump_work_mem_for_validation() -> PgResult<i32> {
    let save_nestlevel = guc::new_guc_nest_level::call();
    let workmembuf = format!("{}", guc::maintenance_work_mem::call());
    guc::set_config_option::call("work_mem", &workmembuf, GucContext::PGC_USERSET, PGC_S_SESSION)?;
    guc::set_config_option::call(
        "hash_mem_multiplier",
        "1",
        GucContext::PGC_USERSET,
        PGC_S_SESSION,
    )?;
    Ok(save_nestlevel)
}

/// Which flavor of the bulk validation query to build.
#[derive(Clone, Copy, PartialEq, Eq)]
enum JoinKind {
    /// `RI_Initial_Check`: `LEFT OUTER JOIN [ONLY] pk` + `WHERE pk.col IS NULL`.
    LeftOuter,
    /// `RI_PartitionRemove_Check`: `JOIN pk` + `WHERE (<partition constraint>)`.
    PartitionInner,
}

/// The shared body of the `RI_Initial_Check` / `RI_PartitionRemove_Check` query
/// builders (identical except the JOIN/WHERE preamble).
fn build_join_check_query(
    mcx: Mcx<'_>,
    riinfo: &RiConstraintInfo,
    fk_rel: RelSide<'_, '_>,
    pk_rel: RelSide<'_, '_>,
    kind: JoinKind,
) -> PgResult<Vec<u8>> {
    let mut querybuf: Vec<u8> = Vec::new();
    try_extend(mcx, &mut querybuf, b"SELECT ")?;
    let mut sep = "";
    for i in 0..riinfo.nkeys as usize {
        try_extend(mcx, &mut querybuf, format!("{sep}fk.").as_bytes())?;
        append_quoted_name(mcx, &mut querybuf, &fk_rel.att_name(mcx, riinfo.fk_attnums[i])?)?;
        sep = ", ";
    }

    let fk_only = if fk_rel.is_partitioned() { "" } else { "ONLY " };

    match kind {
        JoinKind::LeftOuter => {
            let pk_only = if pk_rel.is_partitioned() { "" } else { "ONLY " };
            try_extend(mcx, &mut querybuf, b" FROM ")?;
            try_extend(mcx, &mut querybuf, fk_only.as_bytes())?;
            append_quoted_relation(mcx, &mut querybuf, fk_rel)?;
            try_extend(mcx, &mut querybuf, b" fk LEFT OUTER JOIN ")?;
            try_extend(mcx, &mut querybuf, pk_only.as_bytes())?;
            append_quoted_relation(mcx, &mut querybuf, pk_rel)?;
            try_extend(mcx, &mut querybuf, b" pk ON")?;
        }
        JoinKind::PartitionInner => {
            try_extend(mcx, &mut querybuf, b" FROM ")?;
            try_extend(mcx, &mut querybuf, fk_only.as_bytes())?;
            append_quoted_relation(mcx, &mut querybuf, fk_rel)?;
            try_extend(mcx, &mut querybuf, b" fk JOIN ")?;
            append_quoted_relation(mcx, &mut querybuf, pk_rel)?;
            try_extend(mcx, &mut querybuf, b" pk ON")?;
        }
    }

    let mut sep = "(";
    for i in 0..riinfo.nkeys as usize {
        let pk_type = pk_rel.att_type(riinfo.pk_attnums[i]);
        let fk_type = fk_rel.att_type(riinfo.fk_attnums[i]);
        let pk_coll = pk_rel.att_collation(riinfo.pk_attnums[i]);
        let fk_coll = fk_rel.att_collation(riinfo.fk_attnums[i]);

        let mut pkattname: Vec<u8> = Vec::new();
        try_extend(mcx, &mut pkattname, b"pk.")?;
        append_quoted_name(mcx, &mut pkattname, &pk_rel.att_name(mcx, riinfo.pk_attnums[i])?)?;
        let mut fkattname: Vec<u8> = Vec::new();
        try_extend(mcx, &mut fkattname, b"fk.")?;
        append_quoted_name(mcx, &mut fkattname, &fk_rel.att_name(mcx, riinfo.fk_attnums[i])?)?;
        ri_generate_qual(
            mcx,
            &mut querybuf,
            sep,
            &pkattname,
            pk_type,
            riinfo.pf_eq_oprs[i],
            &fkattname,
            fk_type,
        )?;
        if pk_coll != fk_coll {
            ri_generate_qual_collation(mcx, &mut querybuf, pk_coll)?;
        }
        sep = "AND";
    }

    match kind {
        JoinKind::LeftOuter => {
            try_extend(mcx, &mut querybuf, b") WHERE pk.")?;
            append_quoted_name(mcx, &mut querybuf, &pk_rel.att_name(mcx, riinfo.pk_attnums[0])?)?;
            try_extend(mcx, &mut querybuf, b" IS NULL AND (")?;
        }
        JoinKind::PartitionInner => {
            let constraint_def =
                ruleutils_seams::partition_constraint_def::call(mcx, pk_rel.oid())?;
            match constraint_def {
                Some(def) if !def.as_str().is_empty() => {
                    try_extend(mcx, &mut querybuf, b") WHERE ")?;
                    try_extend(mcx, &mut querybuf, def.as_str().as_bytes())?;
                    try_extend(mcx, &mut querybuf, b" AND (")?;
                }
                _ => {
                    try_extend(mcx, &mut querybuf, b") WHERE (")?;
                }
            }
        }
    }

    let mut sep = "";
    for i in 0..riinfo.nkeys as usize {
        try_extend(mcx, &mut querybuf, sep.as_bytes())?;
        try_extend(mcx, &mut querybuf, b"fk.")?;
        append_quoted_name(mcx, &mut querybuf, &fk_rel.att_name(mcx, riinfo.fk_attnums[i])?)?;
        try_extend(mcx, &mut querybuf, b" IS NOT NULL")?;
        sep = match riinfo.confmatchtype {
            FKCONSTR_MATCH_SIMPLE => " AND ",
            FKCONSTR_MATCH_FULL => " OR ",
            _ => sep,
        };
    }
    try_push(mcx, &mut querybuf, b')')?;
    Ok(querybuf)
}

/// `RI_Initial_Check`'s MATCH-FULL null check on the first SPI result row.
fn ri_initial_result_nullcheck(mcx: Mcx<'_>, fake_riinfo: &RiConstraintInfo) -> PgResult<i32> {
    let attnums: Vec<i16> = fake_riinfo.fk_attnums[..fake_riinfo.nkeys as usize].to_vec();
    let columns = spi::spi_first_row_columns::call(mcx, &attnums)?;

    let mut allnull = true;
    let mut nonenull = true;
    for col in columns.iter() {
        if col.value.is_none() {
            nonenull = false;
        } else {
            allnull = false;
        }
    }
    if allnull {
        Ok(RI_KEYS_ALL_NULL)
    } else if nonenull {
        Ok(RI_KEYS_NONE_NULL)
    } else {
        Ok(RI_KEYS_SOME_NULL)
    }
}
