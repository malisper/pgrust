// ri_triggers.c, MATCH SIMPLE/FULL lane: RI_FKey_check (check_ins/check_upd),
// ri_restrict (noaction/restrict del/upd), RI_FKey_cascade_del/upd, ri_set
// (setnull/setdefault del/upd), ri_Check_Pk_Match, RI_Initial_Check, the
// constraint-info and prepared-plan caches, and the upd_check_required skip
// tests. LOUD: PERIOD, cross-type comparison casts, cross-collation
// initial-check quals. Divergences: constraint-info cache has no syscache invalidation
// callback (constraint rows are immutable in the ported DDL surface; DROP
// removes the triggers that key into it) and the violation DETAIL permission
// check is the superuser fast path.
#![allow(non_snake_case, non_upper_case_globals)]

use core::cell::RefCell;
use datum::Datum;
use mcx::{Mcx, MemoryContext, PgHashMap, PgString, PgVec};
use ri_triggers_seams::RiTriggerData;
use types_core::{
    InvalidOid, Oid, INDEX_MAX_KEYS, SECURITY_LOCAL_USERID_CHANGE, SECURITY_NOFORCE_RLS,
};
use types_tuple::NameData;
use types_error::{
    PgError, PgResult, ERRCODE_FOREIGN_KEY_VIOLATION, ERRCODE_RESTRICT_VIOLATION, ERROR,
};
use types_rel::{Relation, RowShareLock, RELKIND_PARTITIONED_TABLE};
use types_trigger::{Trigger, TRIGGER_FIRED_BY_UPDATE};
use types_tuple::HeapTupleData;

const RI_MAX_NUMKEYS: usize = INDEX_MAX_KEYS as usize;
const RI_PLAN_CHECK_LOOKUPPK: i32 = 1;
const RI_PLAN_CHECK_LOOKUPPK_FROM_PK: i32 = 2;
const RI_PLAN_LAST_ON_PK: i32 = RI_PLAN_CHECK_LOOKUPPK_FROM_PK;
const RI_PLAN_CASCADE_ONDELETE: i32 = 3;
const RI_PLAN_CASCADE_ONUPDATE: i32 = 4;
const RI_PLAN_NO_ACTION: i32 = 5;
const RI_PLAN_RESTRICT: i32 = 6;
const RI_PLAN_SETNULL_ONDELETE: i32 = 7;
const RI_PLAN_SETNULL_ONUPDATE: i32 = 8;
const RI_PLAN_SETDEFAULT_ONDELETE: i32 = 9;
const RI_PLAN_SETDEFAULT_ONUPDATE: i32 = 10;

const RI_TRIGTYPE_UPDATE: i32 = 2;
const RI_TRIGTYPE_DELETE: i32 = 3;

const RI_KEYS_ALL_NULL: i32 = 0;
const RI_KEYS_SOME_NULL: i32 = 1;
const RI_KEYS_NONE_NULL: i32 = 2;

const F_RI_FKEY_CHECK_INS: Oid = 1644;
const F_RI_FKEY_CHECK_UPD: Oid = 1645;
const F_RI_FKEY_CASCADE_DEL: Oid = 1646;
const F_RI_FKEY_CASCADE_UPD: Oid = 1647;
const F_RI_FKEY_RESTRICT_DEL: Oid = 1648;
const F_RI_FKEY_RESTRICT_UPD: Oid = 1649;
const F_RI_FKEY_SETNULL_DEL: Oid = 1650;
const F_RI_FKEY_SETNULL_UPD: Oid = 1651;
const F_RI_FKEY_SETDEFAULT_DEL: Oid = 1652;
const F_RI_FKEY_SETDEFAULT_UPD: Oid = 1653;
const F_RI_FKEY_NOACTION_DEL: Oid = 1654;
const F_RI_FKEY_NOACTION_UPD: Oid = 1655;

const FKCONSTR_MATCH_FULL: i8 = b'f' as i8;
const FKCONSTR_MATCH_PARTIAL: i8 = b'p' as i8;
const FKCONSTR_MATCH_SIMPLE: i8 = b's' as i8;

const CONSTRAINT_FOREIGN: i8 = b'f' as i8;

#[derive(Clone)]
struct RiConstraintInfo {
    constraint_id: Oid,
    constraint_root_id: Oid,
    conname: NameData,
    pk_relid: Oid,
    fk_relid: Oid,
    confmatchtype: i8,
    nkeys: usize,
    ndelsetcols: usize,
    confdelsetcols: [i16; RI_MAX_NUMKEYS],
    fk_attnums: [i16; RI_MAX_NUMKEYS],
    pk_attnums: [i16; RI_MAX_NUMKEYS],
    pf_eq_oprs: [Oid; RI_MAX_NUMKEYS],
    pp_eq_oprs: [Oid; RI_MAX_NUMKEYS],
    ff_eq_oprs: [Oid; RI_MAX_NUMKEYS],
    hasperiod: bool,
    period_contained_by_oper: Oid,
    agged_period_contained_by_oper: Oid,
    period_intersect_oper: Oid,
}

fn cache_mcx() -> &'static MemoryContext {
    thread_local! {
        static CTX: &'static MemoryContext = {
            let cx = ::mcx::session_root("RI cache context");
            // LIFO: empty the droppy TLS caches before the context is freed.
            ::mcx::register_session_cleanup(Box::new(|| {
                RI_CONSTRAINT_CACHE.with(|c| drop(c.borrow_mut().take()));
                RI_QUERY_CACHE.with(|c| drop(c.borrow_mut().take()));
            }));
            cx
        };
    }
    CTX.with(|c| *c)
}

thread_local! {
    static RI_CONSTRAINT_CACHE: RefCell<Option<PgHashMap<'static, Oid, RiConstraintInfo>>> =
        const { RefCell::new(None) };
    static RI_QUERY_CACHE: RefCell<Option<PgHashMap<'static, (Oid, i32), spi::SpiPlanPtr>>> =
        const { RefCell::new(None) };
}

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("unported: ri_triggers {what}")
}

pub fn init_seams() {
    ri_triggers_seams::ri_fkey_trigger::set(ri_fkey_trigger);
    ri_triggers_seams::ri_initial_check::set(RI_Initial_Check);
    ri_triggers_seams::ri_fkey_fk_upd_check_required::set(RI_FKey_fk_upd_check_required);
    ri_triggers_seams::ri_fkey_pk_upd_check_required::set(RI_FKey_pk_upd_check_required);
    ri_triggers_seams::ri_partition_remove_check::set(RI_PartitionRemove_Check);
}

fn ri_fkey_trigger<'mcx>(
    mcx: Mcx<'mcx>,
    tgfoid: Oid,
    tgdata: &RiTriggerData<'_, 'mcx>,
) -> PgResult<()> {
    match tgfoid {
        F_RI_FKEY_CHECK_INS | F_RI_FKEY_CHECK_UPD => RI_FKey_check(mcx, tgdata),
        F_RI_FKEY_NOACTION_DEL | F_RI_FKEY_NOACTION_UPD => ri_restrict(mcx, tgdata, true),
        F_RI_FKEY_RESTRICT_DEL | F_RI_FKEY_RESTRICT_UPD => ri_restrict(mcx, tgdata, false),
        F_RI_FKEY_CASCADE_DEL => RI_FKey_cascade_del(mcx, tgdata),
        F_RI_FKEY_CASCADE_UPD => RI_FKey_cascade_upd(mcx, tgdata),
        F_RI_FKEY_SETNULL_DEL => ri_set(mcx, tgdata, true, RI_TRIGTYPE_DELETE),
        F_RI_FKEY_SETNULL_UPD => ri_set(mcx, tgdata, true, RI_TRIGTYPE_UPDATE),
        F_RI_FKEY_SETDEFAULT_DEL => ri_set(mcx, tgdata, false, RI_TRIGTYPE_DELETE),
        F_RI_FKEY_SETDEFAULT_UPD => ri_set(mcx, tgdata, false, RI_TRIGTYPE_UPDATE),
        other => unported(&format!("RI trigger function {other}")),
    }
}

// RI_FKey_check (ri_triggers.c).
fn RI_FKey_check<'mcx>(mcx: Mcx<'mcx>, tgdata: &RiTriggerData<'_, 'mcx>) -> PgResult<()> {
    let riinfo = ri_FetchConstraintInfo(tgdata.tg_trigger, tgdata.tg_relation, false)?;

    let newtup = if TRIGGER_FIRED_BY_UPDATE(tgdata.tg_event) {
        tgdata.tg_newtuple.expect("UPDATE check without new tuple")
    } else {
        tgdata.tg_trigtuple
    };

    // C re-tests liveness under SnapshotSelf before checking (the row may
    // have been updated/deleted later in the same statement or transaction).
    if !tuple_satisfies_self(tgdata.tg_relation, newtup)? {
        return Ok(());
    }

    let fk_rel = tgdata.tg_relation;
    let pk_rel = table::table_open(mcx, riinfo.pk_relid, RowShareLock)?;

    match ri_NullCheck(&fk_rel.rd_att, newtup, &riinfo, false) {
        RI_KEYS_ALL_NULL => {
            return pk_rel.close(RowShareLock);
        }
        RI_KEYS_SOME_NULL => match riinfo.confmatchtype {
            FKCONSTR_MATCH_FULL => {
                return Err(match_full_mixing_error(mcx, fk_rel, &riinfo));
            }
            _ => {
                debug_assert_eq!(riinfo.confmatchtype, FKCONSTR_MATCH_SIMPLE);
                return pk_rel.close(RowShareLock);
            }
        },
        _ => {}
    }

    spi::SPI_connect()?;

    let qkey = ri_build_query_key(&riinfo, RI_PLAN_CHECK_LOOKUPPK);
    let qplan = match ri_FetchPreparedPlan(&qkey) {
        Some(p) => p,
        None => {
            let mut querybuf = PgString::new_in(mcx);
            let mut queryoids = [InvalidOid; RI_MAX_NUMKEYS];
            let pk_only =
                if pk_rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE { "" } else { "ONLY " };
            use core::fmt::Write;
            // Temporal FKs check containment against the aggregated matching
            // PK ranges: SELECT 1 FROM (SELECT pkperiodatt AS r FROM ONLY pk
            // x WHERE keys.. AND pkperiodatt && $n FOR KEY SHARE OF x) x1
            // HAVING $n <@ range_agg(x1.r).
            if riinfo.hasperiod {
                let periodatt =
                    quote_one_name(att_name(&pk_rel, riinfo.pk_attnums[riinfo.nkeys - 1]));
                write!(
                    querybuf,
                    "SELECT 1 FROM (SELECT {periodatt} AS r FROM {pk_only}{} x",
                    quote_relation_name(mcx, &pk_rel)?
                )
                .expect("PgString write");
            } else {
                write!(
                    querybuf,
                    "SELECT 1 FROM {pk_only}{} x",
                    quote_relation_name(mcx, &pk_rel)?
                )
                .expect("PgString write");
            }
            let mut querysep = "WHERE";
            for i in 0..riinfo.nkeys {
                let pk_type = att_type(&pk_rel, riinfo.pk_attnums[i]);
                let fk_type = att_type(fk_rel, riinfo.fk_attnums[i]);
                let attname = quote_one_name(att_name(&pk_rel, riinfo.pk_attnums[i]));
                let paramname = format!("${}", i + 1);
                ri_GenerateQual(
                    &mut querybuf,
                    querysep,
                    &attname,
                    pk_type,
                    riinfo.pf_eq_oprs[i],
                    &paramname,
                    fk_type,
                )?;
                querysep = "AND";
                queryoids[i] = fk_type;
            }
            querybuf.try_push_str(" FOR KEY SHARE OF x")?;
            if riinfo.hasperiod {
                let fk_type = att_type(fk_rel, riinfo.fk_attnums[riinfo.nkeys - 1]);
                querybuf.try_push_str(") x1 HAVING ")?;
                let paramname = format!("${}", riinfo.nkeys);
                ri_GenerateQual(
                    &mut querybuf,
                    "",
                    &paramname,
                    fk_type,
                    riinfo.agged_period_contained_by_oper,
                    "pg_catalog.range_agg",
                    types_core::ANYMULTIRANGEOID,
                )?;
                querybuf.try_push_str("(x1.r)")?;
            }
            ri_PlanCheck(querybuf.as_str(), &queryoids[..riinfo.nkeys], &qkey, &fk_rel, &pk_rel)?
        }
    };

    // C: detectNewRows must be true when a partitioned table is on the
    // referenced side — the snapshot must be fresh for the
    // find_inheritance_children() hack to work.
    ri_PerformCheck(
        mcx,
        &riinfo,
        &qkey,
        qplan,
        fk_rel,
        &pk_rel,
        None,
        Some(newtup),
        false,
        pk_rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE,
        spi::SPI_OK_SELECT,
    )?;

    spi::SPI_finish()?;
    pk_rel.close(RowShareLock)?;
    Ok(())
}

// ri_restrict (ri_triggers.c): shared by NO ACTION and RESTRICT del/upd.
fn ri_restrict<'mcx>(
    mcx: Mcx<'mcx>,
    tgdata: &RiTriggerData<'_, 'mcx>,
    is_no_action: bool,
) -> PgResult<()> {
    let riinfo = ri_FetchConstraintInfo(tgdata.tg_trigger, tgdata.tg_relation, true)?;

    let fk_rel = table::table_open(mcx, riinfo.fk_relid, RowShareLock)?;
    let pk_rel = tgdata.tg_relation;
    let oldtup = tgdata.tg_trigtuple;

    // trigger.c skips queuing PK-side RI triggers whose old key contains a
    // NULL (RI_FKey_pk_upd_check_required); this port queues them, so the
    // equivalent skip lives here — a NULL key cannot be referenced.
    if ri_NullCheck(&pk_rel.rd_att, oldtup, &riinfo, true) != RI_KEYS_NONE_NULL {
        return fk_rel.close(RowShareLock);
    }

    // Temporal FKs fold replacement-row lookup into the main query below.
    if is_no_action
        && !riinfo.hasperiod
        && ri_Check_Pk_Match(mcx, pk_rel, &fk_rel, oldtup, &riinfo)?
    {
        return fk_rel.close(RowShareLock);
    }

    spi::SPI_connect()?;

    let queryno = if is_no_action { RI_PLAN_NO_ACTION } else { RI_PLAN_RESTRICT };
    let qkey = ri_build_query_key(&riinfo, queryno);
    let qplan = match ri_FetchPreparedPlan(&qkey) {
        Some(p) => p,
        None => {
            let mut querybuf = PgString::new_in(mcx);
            let mut queryoids = [InvalidOid; RI_MAX_NUMKEYS];
            let fk_only =
                if fk_rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE { "" } else { "ONLY " };
            use core::fmt::Write;
            write!(querybuf, "SELECT 1 FROM {fk_only}{} x", quote_relation_name(mcx, &fk_rel)?)
                .expect("PgString write");
            let mut querysep = "WHERE";
            for i in 0..riinfo.nkeys {
                let pk_type = att_type(pk_rel, riinfo.pk_attnums[i]);
                let fk_type = att_type(&fk_rel, riinfo.fk_attnums[i]);
                let attname = quote_one_name(att_name(&fk_rel, riinfo.fk_attnums[i]));
                let paramname = format!("${}", i + 1);
                ri_GenerateQual(
                    &mut querybuf,
                    querysep,
                    &paramname,
                    pk_type,
                    riinfo.pf_eq_oprs[i],
                    &attname,
                    fk_type,
                )?;
                querysep = "AND";
                queryoids[i] = pk_type;
            }
            // Temporal NO ACTION: a reference is still valid if the remaining
            // matching PK history covers the intersection of the FK range
            // with the changed PK range (C's coalesce/range_agg subquery).
            if riinfo.hasperiod && is_no_action {
                let pk_period_type = att_type(pk_rel, riinfo.pk_attnums[riinfo.nkeys - 1]);
                let fk_period_type = att_type(&fk_rel, riinfo.fk_attnums[riinfo.nkeys - 1]);
                let attname = quote_one_name(att_name(&fk_rel, riinfo.fk_attnums[riinfo.nkeys - 1]));
                let paramname = format!("${}", riinfo.nkeys);

                querybuf.try_push_str(" AND NOT coalesce(")?;

                let mut intersectbuf = PgString::new_in(mcx);
                intersectbuf.try_push_str("(")?;
                ri_GenerateQual(
                    &mut intersectbuf,
                    "",
                    &attname,
                    fk_period_type,
                    riinfo.period_intersect_oper,
                    &paramname,
                    pk_period_type,
                )?;
                intersectbuf.try_push_str(")")?;

                let mut replacementsbuf = PgString::new_in(mcx);
                replacementsbuf.try_push_str("(SELECT pg_catalog.range_agg(r) FROM ")?;
                let periodattname =
                    quote_one_name(att_name(pk_rel, riinfo.pk_attnums[riinfo.nkeys - 1]));
                let pk_only = if pk_rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
                    ""
                } else {
                    "ONLY "
                };
                write!(
                    replacementsbuf,
                    "(SELECT y.{periodattname} r FROM {pk_only}{} y",
                    quote_relation_name(mcx, pk_rel)?
                )
                .expect("PgString write");
                let mut querysep = "WHERE";
                for i in 0..riinfo.nkeys {
                    let pk_type = att_type(pk_rel, riinfo.pk_attnums[i]);
                    let attname = quote_one_name(att_name(pk_rel, riinfo.pk_attnums[i]));
                    let paramname = format!("${}", i + 1);
                    ri_GenerateQual(
                        &mut replacementsbuf,
                        querysep,
                        &paramname,
                        pk_type,
                        riinfo.pp_eq_oprs[i],
                        &attname,
                        pk_type,
                    )?;
                    querysep = "AND";
                    queryoids[i] = pk_type;
                }
                replacementsbuf.try_push_str(" FOR KEY SHARE OF y) y2)")?;

                ri_GenerateQual(
                    &mut querybuf,
                    "",
                    intersectbuf.as_str(),
                    fk_period_type,
                    riinfo.agged_period_contained_by_oper,
                    replacementsbuf.as_str(),
                    types_core::ANYMULTIRANGEOID,
                )?;
                querybuf.try_push_str(", false)")?;
            }
            querybuf.try_push_str(" FOR KEY SHARE OF x")?;
            ri_PlanCheck(querybuf.as_str(), &queryoids[..riinfo.nkeys], &qkey, &fk_rel, &pk_rel)?
        }
    };

    ri_PerformCheck(
        mcx,
        &riinfo,
        &qkey,
        qplan,
        &fk_rel,
        pk_rel,
        Some(oldtup),
        None,
        !is_no_action,
        true,
        spi::SPI_OK_SELECT,
    )?;

    spi::SPI_finish()?;
    fk_rel.close(RowShareLock)?;
    Ok(())
}

// RI_FKey_cascade_del (ri_triggers.c).
fn RI_FKey_cascade_del<'mcx>(mcx: Mcx<'mcx>, tgdata: &RiTriggerData<'_, 'mcx>) -> PgResult<()> {
    let riinfo = ri_FetchConstraintInfo(tgdata.tg_trigger, tgdata.tg_relation, true)?;

    let fk_rel = table::table_open(mcx, riinfo.fk_relid, types_rel::RowExclusiveLock)?;
    let pk_rel = tgdata.tg_relation;
    let oldtup = tgdata.tg_trigtuple;

    spi::SPI_connect()?;

    let qkey = ri_build_query_key(&riinfo, RI_PLAN_CASCADE_ONDELETE);
    let qplan = match ri_FetchPreparedPlan(&qkey) {
        Some(p) => p,
        None => {
            let mut querybuf = PgString::new_in(mcx);
            let mut queryoids = [InvalidOid; RI_MAX_NUMKEYS];
            let fk_only =
                if fk_rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE { "" } else { "ONLY " };
            use core::fmt::Write;
            write!(querybuf, "DELETE FROM {fk_only}{}", quote_relation_name(mcx, &fk_rel)?)
                .expect("PgString write");
            let mut querysep = "WHERE";
            for i in 0..riinfo.nkeys {
                let pk_type = att_type(pk_rel, riinfo.pk_attnums[i]);
                let fk_type = att_type(&fk_rel, riinfo.fk_attnums[i]);
                let attname = quote_one_name(att_name(&fk_rel, riinfo.fk_attnums[i]));
                let paramname = format!("${}", i + 1);
                ri_GenerateQual(
                    &mut querybuf,
                    querysep,
                    &paramname,
                    pk_type,
                    riinfo.pf_eq_oprs[i],
                    &attname,
                    fk_type,
                )?;
                querysep = "AND";
                queryoids[i] = pk_type;
            }
            ri_PlanCheck(querybuf.as_str(), &queryoids[..riinfo.nkeys], &qkey, &fk_rel, &pk_rel)?
        }
    };

    ri_PerformCheck(
        mcx,
        &riinfo,
        &qkey,
        qplan,
        &fk_rel,
        pk_rel,
        Some(oldtup),
        None,
        false,
        true,
        spi::SPI_OK_DELETE,
    )?;

    spi::SPI_finish()?;
    fk_rel.close(types_rel::RowExclusiveLock)?;
    Ok(())
}

// RI_FKey_cascade_upd (ri_triggers.c).
fn RI_FKey_cascade_upd<'mcx>(mcx: Mcx<'mcx>, tgdata: &RiTriggerData<'_, 'mcx>) -> PgResult<()> {
    let riinfo = ri_FetchConstraintInfo(tgdata.tg_trigger, tgdata.tg_relation, true)?;

    let fk_rel = table::table_open(mcx, riinfo.fk_relid, types_rel::RowExclusiveLock)?;
    let pk_rel = tgdata.tg_relation;
    let newtup = tgdata.tg_newtuple.expect("cascade_upd without new tuple");
    let oldtup = tgdata.tg_trigtuple;

    spi::SPI_connect()?;

    let qkey = ri_build_query_key(&riinfo, RI_PLAN_CASCADE_ONUPDATE);
    let qplan = match ri_FetchPreparedPlan(&qkey) {
        Some(p) => p,
        None => {
            let mut querybuf = PgString::new_in(mcx);
            let mut qualbuf = PgString::new_in(mcx);
            let mut queryoids = [InvalidOid; RI_MAX_NUMKEYS * 2];
            let fk_only =
                if fk_rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE { "" } else { "ONLY " };
            use core::fmt::Write;
            write!(querybuf, "UPDATE {fk_only}{} SET", quote_relation_name(mcx, &fk_rel)?)
                .expect("PgString write");
            let mut querysep = "";
            let mut qualsep = "WHERE";
            for i in 0..riinfo.nkeys {
                let j = riinfo.nkeys + i;
                let pk_type = att_type(pk_rel, riinfo.pk_attnums[i]);
                let fk_type = att_type(&fk_rel, riinfo.fk_attnums[i]);
                let attname = quote_one_name(att_name(&fk_rel, riinfo.fk_attnums[i]));
                write!(querybuf, "{querysep} {attname} = ${}", i + 1).expect("PgString write");
                let paramname = format!("${}", j + 1);
                ri_GenerateQual(
                    &mut qualbuf,
                    qualsep,
                    &paramname,
                    pk_type,
                    riinfo.pf_eq_oprs[i],
                    &attname,
                    fk_type,
                )?;
                querysep = ",";
                qualsep = "AND";
                queryoids[i] = pk_type;
                queryoids[j] = pk_type;
            }
            querybuf.try_push_str(qualbuf.as_str())?;
            ri_PlanCheck(querybuf.as_str(), &queryoids[..riinfo.nkeys * 2], &qkey, &fk_rel, &pk_rel)?
        }
    };

    ri_PerformCheck(
        mcx,
        &riinfo,
        &qkey,
        qplan,
        &fk_rel,
        pk_rel,
        Some(oldtup),
        Some(newtup),
        false,
        true,
        spi::SPI_OK_UPDATE,
    )?;

    spi::SPI_finish()?;
    fk_rel.close(types_rel::RowExclusiveLock)?;
    Ok(())
}

// ri_set (ri_triggers.c): SET NULL / SET DEFAULT, del/upd.
fn ri_set<'mcx>(
    mcx: Mcx<'mcx>,
    tgdata: &RiTriggerData<'_, 'mcx>,
    is_set_null: bool,
    tgkind: i32,
) -> PgResult<()> {
    let riinfo = ri_FetchConstraintInfo(tgdata.tg_trigger, tgdata.tg_relation, true)?;

    let fk_rel = table::table_open(mcx, riinfo.fk_relid, types_rel::RowExclusiveLock)?;
    let pk_rel = tgdata.tg_relation;
    let oldtup = tgdata.tg_trigtuple;

    spi::SPI_connect()?;

    let queryno = match (tgkind, is_set_null) {
        (RI_TRIGTYPE_UPDATE, true) => RI_PLAN_SETNULL_ONUPDATE,
        (RI_TRIGTYPE_UPDATE, false) => RI_PLAN_SETDEFAULT_ONUPDATE,
        (RI_TRIGTYPE_DELETE, true) => RI_PLAN_SETNULL_ONDELETE,
        (RI_TRIGTYPE_DELETE, false) => RI_PLAN_SETDEFAULT_ONDELETE,
        _ => panic!("invalid tgkind passed to ri_set"),
    };
    let qkey = ri_build_query_key(&riinfo, queryno);
    let qplan = match ri_FetchPreparedPlan(&qkey) {
        Some(p) => p,
        None => {
            let (num_cols_to_set, set_cols): (usize, &[i16]) = match tgkind {
                RI_TRIGTYPE_UPDATE => (riinfo.nkeys, &riinfo.fk_attnums),
                RI_TRIGTYPE_DELETE if riinfo.ndelsetcols != 0 => {
                    (riinfo.ndelsetcols, &riinfo.confdelsetcols)
                }
                RI_TRIGTYPE_DELETE => (riinfo.nkeys, &riinfo.fk_attnums),
                _ => panic!("invalid tgkind passed to ri_set"),
            };
            let mut querybuf = PgString::new_in(mcx);
            let mut queryoids = [InvalidOid; RI_MAX_NUMKEYS];
            let fk_only =
                if fk_rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE { "" } else { "ONLY " };
            use core::fmt::Write;
            write!(querybuf, "UPDATE {fk_only}{} SET", quote_relation_name(mcx, &fk_rel)?)
                .expect("PgString write");
            let mut querysep = "";
            for i in 0..num_cols_to_set {
                let attname = quote_one_name(att_name(&fk_rel, set_cols[i]));
                let rhs = if is_set_null { "NULL" } else { "DEFAULT" };
                write!(querybuf, "{querysep} {attname} = {rhs}").expect("PgString write");
                querysep = ",";
            }
            let mut qualsep = "WHERE";
            for i in 0..riinfo.nkeys {
                let pk_type = att_type(pk_rel, riinfo.pk_attnums[i]);
                let fk_type = att_type(&fk_rel, riinfo.fk_attnums[i]);
                let attname = quote_one_name(att_name(&fk_rel, riinfo.fk_attnums[i]));
                let paramname = format!("${}", i + 1);
                ri_GenerateQual(
                    &mut querybuf,
                    qualsep,
                    &paramname,
                    pk_type,
                    riinfo.pf_eq_oprs[i],
                    &attname,
                    fk_type,
                )?;
                qualsep = "AND";
                queryoids[i] = pk_type;
            }
            ri_PlanCheck(querybuf.as_str(), &queryoids[..riinfo.nkeys], &qkey, &fk_rel, &pk_rel)?
        }
    };

    ri_PerformCheck(
        mcx,
        &riinfo,
        &qkey,
        qplan,
        &fk_rel,
        pk_rel,
        Some(oldtup),
        None,
        false,
        true,
        spi::SPI_OK_UPDATE,
    )?;

    spi::SPI_finish()?;
    fk_rel.close(types_rel::RowExclusiveLock)?;

    if is_set_null {
        Ok(())
    } else {
        // C re-runs the NO ACTION check: a SET DEFAULT that rewrites rows to
        // the just-removed key values would otherwise skip its FK check.
        ri_restrict(mcx, tgdata, true)
    }
}

// ri_Check_Pk_Match (ri_triggers.c): true if a replacement PK row exists.
fn ri_Check_Pk_Match<'mcx>(
    mcx: Mcx<'mcx>,
    pk_rel: &Relation<'mcx>,
    fk_rel: &Relation<'mcx>,
    oldtup: &HeapTupleData<'_>,
    riinfo: &RiConstraintInfo,
) -> PgResult<bool> {
    debug_assert_eq!(ri_NullCheck(&pk_rel.rd_att, oldtup, riinfo, true), RI_KEYS_NONE_NULL);

    spi::SPI_connect()?;

    let qkey = ri_build_query_key(&riinfo, RI_PLAN_CHECK_LOOKUPPK_FROM_PK);
    let qplan = match ri_FetchPreparedPlan(&qkey) {
        Some(p) => p,
        None => {
            let mut querybuf = PgString::new_in(mcx);
            let mut queryoids = [InvalidOid; RI_MAX_NUMKEYS];
            let pk_only =
                if pk_rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE { "" } else { "ONLY " };
            use core::fmt::Write;
            if riinfo.hasperiod {
                let periodatt =
                    quote_one_name(att_name(pk_rel, riinfo.pk_attnums[riinfo.nkeys - 1]));
                write!(
                    querybuf,
                    "SELECT 1 FROM (SELECT {periodatt} AS r FROM {pk_only}{} x",
                    quote_relation_name(mcx, pk_rel)?
                )
                .expect("PgString write");
            } else {
                write!(
                    querybuf,
                    "SELECT 1 FROM {pk_only}{} x",
                    quote_relation_name(mcx, pk_rel)?
                )
                .expect("PgString write");
            }
            let mut querysep = "WHERE";
            for i in 0..riinfo.nkeys {
                let pk_type = att_type(pk_rel, riinfo.pk_attnums[i]);
                let attname = quote_one_name(att_name(pk_rel, riinfo.pk_attnums[i]));
                let paramname = format!("${}", i + 1);
                ri_GenerateQual(
                    &mut querybuf,
                    querysep,
                    &attname,
                    pk_type,
                    riinfo.pp_eq_oprs[i],
                    &paramname,
                    pk_type,
                )?;
                querysep = "AND";
                queryoids[i] = pk_type;
            }
            querybuf.try_push_str(" FOR KEY SHARE OF x")?;
            if riinfo.hasperiod {
                let fk_type = att_type(fk_rel, riinfo.fk_attnums[riinfo.nkeys - 1]);
                querybuf.try_push_str(") x1 HAVING ")?;
                let paramname = format!("${}", riinfo.nkeys);
                ri_GenerateQual(
                    &mut querybuf,
                    "",
                    &paramname,
                    fk_type,
                    riinfo.agged_period_contained_by_oper,
                    "pg_catalog.range_agg",
                    types_core::ANYMULTIRANGEOID,
                )?;
                querybuf.try_push_str("(x1.r)")?;
            }
            ri_PlanCheck(querybuf.as_str(), &queryoids[..riinfo.nkeys], &qkey, &fk_rel, &pk_rel)?
        }
    };

    let found = ri_PerformCheck(
        mcx,
        riinfo,
        &qkey,
        qplan,
        fk_rel,
        pk_rel,
        Some(oldtup),
        None,
        false,
        true,
        spi::SPI_OK_SELECT,
    )?;

    spi::SPI_finish()?;
    let _ = fk_rel;
    Ok(found)
}

// RI_Initial_Check (ri_triggers.c): whole-table validation for ALTER TABLE
// ADD FOREIGN KEY. False = caller falls back to the per-row trigger method
// (here only for non-superusers; C also falls back on RLS/permission grounds).
pub fn RI_Initial_Check<'mcx>(
    mcx: Mcx<'mcx>,
    trigger: &Trigger<'mcx>,
    fk_rel: &Relation<'mcx>,
    pk_rel: &Relation<'mcx>,
) -> PgResult<bool> {
    let riinfo = ri_FetchConstraintInfo(trigger, fk_rel, false)?;

    // ExecCheckPermissions + bypassrls/ownercheck walk: superuser fast path.
    if !superuser::superuser()? {
        return Ok(false);
    }

    let mut querybuf = PgString::new_in(mcx);
    use core::fmt::Write;
    querybuf.try_push_str("SELECT ")?;
    let mut sep = "";
    for i in 0..riinfo.nkeys {
        let fkattname = quote_one_name(att_name(fk_rel, riinfo.fk_attnums[i]));
        write!(querybuf, "{sep}fk.{fkattname}").expect("PgString write");
        sep = ", ";
    }
    let fk_only = if fk_rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE { "" } else { "ONLY " };
    let pk_only = if pk_rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE { "" } else { "ONLY " };
    write!(
        querybuf,
        " FROM {fk_only}{} fk LEFT OUTER JOIN {pk_only}{} pk ON",
        quote_relation_name(mcx, fk_rel)?,
        quote_relation_name(mcx, pk_rel)?
    )
    .expect("PgString write");

    let mut sep = "(";
    for i in 0..riinfo.nkeys {
        let pk_type = att_type(pk_rel, riinfo.pk_attnums[i]);
        let fk_type = att_type(fk_rel, riinfo.fk_attnums[i]);
        let pk_coll = att_collation(pk_rel, riinfo.pk_attnums[i]);
        let fk_coll = att_collation(fk_rel, riinfo.fk_attnums[i]);
        let pkattname = format!("pk.{}", quote_one_name(att_name(pk_rel, riinfo.pk_attnums[i])));
        let fkattname = format!("fk.{}", quote_one_name(att_name(fk_rel, riinfo.fk_attnums[i])));
        ri_GenerateQual(
            &mut querybuf,
            sep,
            &pkattname,
            pk_type,
            riinfo.pf_eq_oprs[i],
            &fkattname,
            fk_type,
        )?;
        if pk_coll != fk_coll {
            ri_GenerateQualCollation(mcx, &mut querybuf, pk_coll)?;
        }
        sep = "AND";
    }

    let pkattname = quote_one_name(att_name(pk_rel, riinfo.pk_attnums[0]));
    write!(querybuf, ") WHERE pk.{pkattname} IS NULL AND (").expect("PgString write");
    let mut sep = "";
    for i in 0..riinfo.nkeys {
        let fkattname = quote_one_name(att_name(fk_rel, riinfo.fk_attnums[i]));
        write!(querybuf, "{sep}fk.{fkattname} IS NOT NULL").expect("PgString write");
        sep = match riinfo.confmatchtype {
            FKCONSTR_MATCH_SIMPLE => " AND ",
            FKCONSTR_MATCH_FULL => " OR ",
            _ => sep,
        };
    }
    querybuf.try_push_str(")")?;

    let save_nestlevel = guc::NewGUCNestLevel();
    let workmem = format!("{}", init_small::globals::maintenance_work_mem());
    guc::set_config_option(
        "work_mem",
        Some(&workmem),
        types_guc::PGC_USERSET,
        types_guc::PGC_S_SESSION,
        guc::GUC_ACTION_SAVE,
        true,
        types_error::ErrorLevel(0),
        false,
    )?;
    guc::set_config_option(
        "hash_mem_multiplier",
        Some("1"),
        types_guc::PGC_USERSET,
        types_guc::PGC_S_SESSION,
        guc::GUC_ACTION_SAVE,
        true,
        types_error::ErrorLevel(0),
        false,
    )?;

    spi::SPI_connect()?;
    let qplan = spi::SPI_prepare(querybuf.as_str(), &[])?;
    let snap = snapmgr::GetLatestSnapshot()?;
    let spi_result =
        spi::SPI_execute_snapshot(qplan, &[], &[], Some(snap), None, true, false, 1)?;
    if spi_result != spi::SPI_OK_SELECT {
        panic!("SPI_execute_snapshot returned {spi_result}");
    }

    if spi::SPI_processed() > 0 {
        // Error propagation aborts the transaction; SPI and GUC stacks are
        // reset by the abort path (C's ereport shape).
        let h = spi::SPI_tuptable().expect("SELECT leaves a tuptable");
        let mut fake_riinfo = riinfo.clone();
        for i in 0..fake_riinfo.nkeys {
            fake_riinfo.fk_attnums[i] = i as i16 + 1;
        }
        return Err(spi::tuptable_with(h, |t| {
            // MATCH FULL disallows partially-null FK rows: complain about the
            // null mixing rather than the lack of a match.
            if fake_riinfo.confmatchtype == FKCONSTR_MATCH_FULL
                && ri_NullCheck(&t.tupdesc, &t.vals[0], &fake_riinfo, false) != RI_KEYS_NONE_NULL
            {
                return match_full_mixing_error(mcx, fk_rel, &fake_riinfo);
            }
            ri_ReportViolation(
                mcx,
                &fake_riinfo,
                pk_rel,
                fk_rel,
                &t.vals[0],
                Some(&t.tupdesc),
                RI_PLAN_CHECK_LOOKUPPK,
                false,
                false,
            )
        }));
    }

    spi::SPI_finish()?;
    guc::AtEOXact_GUC(true, save_nestlevel);
    Ok(true)
}

// RI_PartitionRemove_Check (ri_triggers.c): verify no referencing values
// exist when a partition is detached on the referenced side of an FK.
pub fn RI_PartitionRemove_Check<'mcx>(
    mcx: Mcx<'mcx>,
    trigger: &Trigger<'mcx>,
    fk_rel: &Relation<'mcx>,
    pk_rel: &Relation<'mcx>,
) -> PgResult<()> {
    let riinfo = ri_FetchConstraintInfo(trigger, fk_rel, false)?;

    let mut querybuf = PgString::new_in(mcx);
    use core::fmt::Write;
    querybuf.try_push_str("SELECT ")?;
    let mut sep = "";
    for i in 0..riinfo.nkeys {
        let fkattname = quote_one_name(att_name(fk_rel, riinfo.fk_attnums[i]));
        write!(querybuf, "{sep}fk.{fkattname}").expect("PgString write");
        sep = ", ";
    }

    let fk_only =
        if fk_rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE { "" } else { "ONLY " };
    write!(
        querybuf,
        " FROM {}{} fk JOIN {} pk ON",
        fk_only,
        quote_relation_name(mcx, fk_rel)?,
        quote_relation_name(mcx, pk_rel)?
    )
    .expect("PgString write");

    let mut sep = "(";
    for i in 0..riinfo.nkeys {
        let pk_type = att_type(pk_rel, riinfo.pk_attnums[i]);
        let fk_type = att_type(fk_rel, riinfo.fk_attnums[i]);
        let pk_coll = att_collation(pk_rel, riinfo.pk_attnums[i]);
        let fk_coll = att_collation(fk_rel, riinfo.fk_attnums[i]);
        let pkattname = format!("pk.{}", quote_one_name(att_name(pk_rel, riinfo.pk_attnums[i])));
        let fkattname = format!("fk.{}", quote_one_name(att_name(fk_rel, riinfo.fk_attnums[i])));
        ri_GenerateQual(
            &mut querybuf,
            sep,
            &pkattname,
            pk_type,
            riinfo.pf_eq_oprs[i],
            &fkattname,
            fk_type,
        )?;
        if pk_coll != fk_coll {
            ri_GenerateQualCollation(mcx, &mut querybuf, pk_coll)?;
        }
        sep = "AND";
    }

    // Start the WHERE clause with the partition constraint, unless this is a
    // sole default partition (empty constraint).
    match ruleutils::pg_get_partconstrdef_string(mcx, pk_rel.rd_id, "pk")? {
        Some(constraint_def) if !constraint_def.is_empty() => {
            write!(querybuf, ") WHERE {constraint_def} AND (").expect("PgString write");
        }
        _ => querybuf.try_push_str(") WHERE (")?,
    }

    let mut sep = "";
    for i in 0..riinfo.nkeys {
        let fkattname = quote_one_name(att_name(fk_rel, riinfo.fk_attnums[i]));
        write!(querybuf, "{sep}fk.{fkattname} IS NOT NULL").expect("PgString write");
        sep = match riinfo.confmatchtype {
            FKCONSTR_MATCH_SIMPLE => " AND ",
            FKCONSTR_MATCH_FULL => " OR ",
            _ => sep,
        };
    }
    querybuf.try_push_str(")")?;

    let save_nestlevel = guc::NewGUCNestLevel();
    let workmem = format!("{}", init_small::globals::maintenance_work_mem());
    guc::set_config_option(
        "work_mem",
        Some(&workmem),
        types_guc::PGC_USERSET,
        types_guc::PGC_S_SESSION,
        guc::GUC_ACTION_SAVE,
        true,
        types_error::ErrorLevel(0),
        false,
    )?;
    guc::set_config_option(
        "hash_mem_multiplier",
        Some("1"),
        types_guc::PGC_USERSET,
        types_guc::PGC_S_SESSION,
        guc::GUC_ACTION_SAVE,
        true,
        types_error::ErrorLevel(0),
        false,
    )?;

    spi::SPI_connect()?;
    let qplan = spi::SPI_prepare(querybuf.as_str(), &[])?;
    let snap = snapmgr::GetLatestSnapshot()?;
    let spi_result =
        spi::SPI_execute_snapshot(qplan, &[], &[], Some(snap), None, true, false, 1)?;
    if spi_result != spi::SPI_OK_SELECT {
        panic!("SPI_execute_snapshot returned {spi_result}");
    }

    if spi::SPI_processed() > 0 {
        // The result tuple's columns are 1..N; hack up riinfo so
        // ri_ReportViolation reads them (and its tupdesc) correctly.
        let h = spi::SPI_tuptable().expect("SELECT leaves a tuptable");
        let mut fake_riinfo = riinfo.clone();
        for i in 0..fake_riinfo.nkeys {
            fake_riinfo.pk_attnums[i] = i as i16 + 1;
        }
        return Err(spi::tuptable_with(h, |t| {
            ri_ReportViolation(
                mcx,
                &fake_riinfo,
                pk_rel,
                fk_rel,
                &t.vals[0],
                Some(&t.tupdesc),
                0,
                false,
                true,
            )
        }));
    }

    spi::SPI_finish()?;
    guc::AtEOXact_GUC(true, save_nestlevel);
    Ok(())
}

// RI_FKey_pk_upd_check_required (ri_triggers.c).
fn RI_FKey_pk_upd_check_required<'mcx>(
    _mcx: Mcx<'mcx>,
    trigger: &Trigger<'mcx>,
    pk_rel: &Relation<'mcx>,
    old_tup: &HeapTupleData<'_>,
    new_tup: &HeapTupleData<'_>,
) -> PgResult<bool> {
    let riinfo = ri_FetchConstraintInfo(trigger, pk_rel, true)?;
    if ri_NullCheck(&pk_rel.rd_att, old_tup, &riinfo, true) != RI_KEYS_NONE_NULL {
        return Ok(false);
    }
    if ri_KeysEqual(pk_rel, old_tup, new_tup, &riinfo, true)? {
        return Ok(false);
    }
    Ok(true)
}

// RI_FKey_fk_upd_check_required (ri_triggers.c).
fn RI_FKey_fk_upd_check_required<'mcx>(
    _mcx: Mcx<'mcx>,
    trigger: &Trigger<'mcx>,
    fk_rel: &Relation<'mcx>,
    old_tup: &HeapTupleData<'_>,
    new_tup: &HeapTupleData<'_>,
) -> PgResult<bool> {
    let riinfo = ri_FetchConstraintInfo(trigger, fk_rel, false)?;
    match ri_NullCheck(&fk_rel.rd_att, new_tup, &riinfo, false) {
        RI_KEYS_ALL_NULL => return Ok(false),
        RI_KEYS_SOME_NULL => match riinfo.confmatchtype {
            FKCONSTR_MATCH_SIMPLE => return Ok(false),
            FKCONSTR_MATCH_FULL => return Ok(true),
            _ => {}
        },
        _ => {}
    }
    // C: if the old row was inserted by our own transaction, fire anyway
    // (the UPDATE invalidates the INSERT's pending check).
    if xact_seams::transaction_id_is_current_transaction_id::call(old_tup.t_data().xmin()) {
        return Ok(true);
    }
    if ri_KeysEqual(fk_rel, old_tup, new_tup, &riinfo, false)? {
        return Ok(false);
    }
    Ok(true)
}

fn ri_FetchConstraintInfo<'mcx>(
    trigger: &Trigger<'mcx>,
    trig_rel: &Relation<'mcx>,
    rel_is_pk: bool,
) -> PgResult<RiConstraintInfo> {
    let constraint_oid = trigger.tgconstraint;
    assert!(
        constraint_oid != InvalidOid,
        "no pg_constraint entry for trigger \"{}\" on table \"{}\"",
        trigger.tgname.as_str(),
        trig_rel.name()
    );
    let riinfo = ri_LoadConstraintInfo(constraint_oid)?;
    if rel_is_pk {
        assert!(
            riinfo.fk_relid == trigger.tgconstrrelid && riinfo.pk_relid == trig_rel.rd_id,
            "wrong pg_constraint entry for trigger \"{}\"",
            trigger.tgname.as_str()
        );
    } else {
        assert!(
            riinfo.fk_relid == trig_rel.rd_id && riinfo.pk_relid == trigger.tgconstrrelid,
            "wrong pg_constraint entry for trigger \"{}\"",
            trigger.tgname.as_str()
        );
    }
    if riinfo.confmatchtype != FKCONSTR_MATCH_FULL
        && riinfo.confmatchtype != FKCONSTR_MATCH_PARTIAL
        && riinfo.confmatchtype != FKCONSTR_MATCH_SIMPLE
    {
        panic!("unrecognized confmatchtype: {}", riinfo.confmatchtype);
    }
    if riinfo.confmatchtype == FKCONSTR_MATCH_PARTIAL {
        return Err(Box::new(
            PgError::new(ERROR, "MATCH PARTIAL not yet implemented")
                .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }
    Ok(riinfo)
}

fn ri_LoadConstraintInfo(constraint_oid: Oid) -> PgResult<RiConstraintInfo> {
    if let Some(hit) = RI_CONSTRAINT_CACHE.with(|c| {
        c.borrow().as_ref().and_then(|m| m.get(&constraint_oid).cloned())
    }) {
        return Ok(hit);
    }

    use cache_syscache::{SysCacheGetAttr, SearchSysCache1, SysCacheKey, CONSTROID};
    const Anum_conname: i32 = 2;
    const Anum_contype: i32 = 4;
    const Anum_conrelid: i32 = 9;
    const Anum_conparentid: i32 = 12;
    const Anum_confrelid: i32 = 13;
    const Anum_confmatchtype: i32 = 16;
    const Anum_conkey: i32 = 21;
    const Anum_confkey: i32 = 22;
    const Anum_conindid: i32 = 11;
    const Anum_conperiod: i32 = 20;
    const Anum_conpfeqop: i32 = 23;
    const Anum_conppeqop: i32 = 24;
    const Anum_conffeqop: i32 = 25;
    const Anum_confdelsetcols: i32 = 26;

    let tup = SearchSysCache1(CONSTROID, SysCacheKey::Value(Datum::from_oid(constraint_oid)))?
        .unwrap_or_else(|| panic!("cache lookup failed for constraint {constraint_oid}"));
    let req = |attno: i32| -> PgResult<Datum> {
        let (d, isnull) = SysCacheGetAttr(CONSTROID, &tup, attno)?;
        assert!(!isnull, "unexpected null pg_constraint attr {attno}");
        Ok(d)
    };
    assert!(
        req(Anum_contype)?.as_i8() == CONSTRAINT_FOREIGN,
        "constraint {constraint_oid} is not a foreign key constraint"
    );

    let conparentid = req(Anum_conparentid)?.as_oid();
    let mut info = RiConstraintInfo {
        constraint_id: constraint_oid,
        constraint_root_id: if conparentid != InvalidOid {
            get_ri_constraint_root(conparentid)?
        } else {
            constraint_oid
        },
        // SAFETY: conname is a by-ref NAME column of the held syscache tuple.
        conname: unsafe { *(req(Anum_conname)?.as_usize() as *const NameData) },
        pk_relid: req(Anum_confrelid)?.as_oid(),
        fk_relid: req(Anum_conrelid)?.as_oid(),
        confmatchtype: req(Anum_confmatchtype)?.as_i8(),
        nkeys: 0,
        ndelsetcols: 0,
        confdelsetcols: [0; RI_MAX_NUMKEYS],
        fk_attnums: [0; RI_MAX_NUMKEYS],
        pk_attnums: [0; RI_MAX_NUMKEYS],
        pf_eq_oprs: [InvalidOid; RI_MAX_NUMKEYS],
        pp_eq_oprs: [InvalidOid; RI_MAX_NUMKEYS],
        ff_eq_oprs: [InvalidOid; RI_MAX_NUMKEYS],
        hasperiod: req(Anum_conperiod)?.as_bool(),
        period_contained_by_oper: InvalidOid,
        agged_period_contained_by_oper: InvalidOid,
        period_intersect_oper: InvalidOid,
    };

    // DeconstructFkConstraintRow (pg_constraint.c): 1-D no-null int2[]/oid[].
    let scratch = mcx::MemoryContext::new("ri-load-scratch");
    {
        let smcx = scratch.mcx();
        let unpack = |d: Datum| -> PgResult<PgVec<'_, u8>> {
            // DatumGetArrayTypeP: the on-disk image may carry a 1-byte
            // header; rebuild the 4-byte form (relcache_build precedent).
            // SAFETY: not-null array datum of the held syscache tuple.
            let img = unsafe { array_image_bytes(d) };
            let payload = varlena::open_image(smcx, img)?;
            let body = payload.as_bytes();
            let total = body.len() + 4;
            let mut full: PgVec<'_, u8> = mcx::vec_with_capacity_in(smcx, total)?;
            mcx::vec_append_bytes(&mut full, &((total as u32) << 2).to_ne_bytes())?;
            mcx::vec_append_bytes(&mut full, body)?;
            Ok(full)
        };
        let i16_arr = |d: Datum, out: &mut [i16; RI_MAX_NUMKEYS]| -> PgResult<usize> {
            let full = unpack(d)?;
            let vals = datum::array_build::deconstruct_array_image(smcx, &full, 2, true, b's')?;
            for (i, v) in vals.iter().enumerate() {
                out[i] = v.as_i16();
            }
            Ok(vals.len())
        };
        let oid_arr = |d: Datum, out: &mut [Oid; RI_MAX_NUMKEYS]| -> PgResult<usize> {
            let full = unpack(d)?;
            let vals = datum::array_build::deconstruct_array_image(smcx, &full, 4, true, b'i')?;
            for (i, v) in vals.iter().enumerate() {
                out[i] = v.as_oid();
            }
            Ok(vals.len())
        };
        let n = i16_arr(req(Anum_conkey)?, &mut info.fk_attnums)?;
        let n2 = i16_arr(req(Anum_confkey)?, &mut info.pk_attnums)?;
        let n3 = oid_arr(req(Anum_conpfeqop)?, &mut info.pf_eq_oprs)?;
        let n4 = oid_arr(req(Anum_conppeqop)?, &mut info.pp_eq_oprs)?;
        let n5 = oid_arr(req(Anum_conffeqop)?, &mut info.ff_eq_oprs)?;
        assert!(n == n2 && n == n3 && n == n4 && n == n5, "foreign key array length mismatch");
        info.nkeys = n;
        let (dsc, dsc_null) = SysCacheGetAttr(CONSTROID, &tup, Anum_confdelsetcols)?;
        if !dsc_null {
            info.ndelsetcols = i16_arr(dsc, &mut info.confdelsetcols)?;
        }
    }

    // Temporal FKs: the PK element's opclass supplies the containment and
    // intersect operators (cached with the rest of the info).
    if info.hasperiod {
        let opclass = lsyscache::get_index_column_opclass(
            req(Anum_conindid)?.as_oid(),
            info.nkeys as i32,
        )?;
        let (contained_by, agged, intersect) = pg_constraint::FindFKPeriodOpers(opclass)?;
        info.period_contained_by_oper = contained_by;
        info.agged_period_contained_by_oper = agged;
        info.period_intersect_oper = intersect;
    }

    RI_CONSTRAINT_CACHE.with(|c| {
        let mut b = c.borrow_mut();
        let m = b.get_or_insert_with(|| PgHashMap::new_in(cache_mcx().mcx()));
        m.insert(constraint_oid, info.clone());
    });
    Ok(info)
}

// SAFETY contract: d is a not-null, untoasted array datum; returns its full
// varlena image.
unsafe fn array_image_bytes<'a>(d: Datum) -> &'a [u8] {
    let p = d.as_usize() as *const u8;
    // SAFETY: caller contract.
    let len = unsafe { types_tuple::varatt::varsize_any(p) };
    core::slice::from_raw_parts(p, len)
}

// C: a stale plan is rebuilt from scratch (query text too — renames), not
// re-analyzed by the plancache; validity is trustworthy only under the
// caller's FK+PK locks.
fn ri_FetchPreparedPlan(key: &(Oid, i32)) -> Option<spi::SpiPlanPtr> {
    let plan = RI_QUERY_CACHE.with(|c| c.borrow().as_ref().and_then(|m| m.get(key).copied()))?;
    if spi::SPI_plan_is_valid(plan) {
        return Some(plan);
    }
    RI_QUERY_CACHE.with(|c| {
        if let Some(m) = c.borrow_mut().as_mut() {
            m.remove(key);
        }
    });
    spi::SPI_freeplan(plan);
    None
}

// ri_PlanCheck (ri_triggers.c). The C owner-uid switch is the superuser fast
// path repo-wide; plan validity re-checks ride the plancache.
fn ri_PlanCheck(
    querystr: &str,
    argtypes: &[Oid],
    key: &(Oid, i32),
    fk_rel: &Relation<'_>,
    pk_rel: &Relation<'_>,
) -> PgResult<spi::SpiPlanPtr> {
    // The query runs against the PK or FK table per the query type code; both
    // plan and check execute as that table's owner.
    let query_rel = if key.1 <= RI_PLAN_LAST_ON_PK { pk_rel } else { fk_rel };
    let (_, save_sec_context) = miscinit::GetUserIdAndSecContext();
    let guard = miscinit::SecContextGuard::set(
        query_rel.rd_rel.relowner,
        save_sec_context | SECURITY_LOCAL_USERID_CHANGE | SECURITY_NOFORCE_RLS,
    );
    let qplan = spi::SPI_prepare(querystr, argtypes);
    guard.restore();
    let qplan = qplan?;
    spi::SPI_keepplan(qplan);
    RI_QUERY_CACHE.with(|c| {
        let mut b = c.borrow_mut();
        let m = b.get_or_insert_with(|| PgHashMap::new_in(cache_mcx().mcx()));
        m.insert(*key, qplan);
    });
    Ok(qplan)
}

// ri_PerformCheck (ri_triggers.c).
#[allow(clippy::too_many_arguments)]
fn ri_PerformCheck<'mcx>(
    mcx: Mcx<'mcx>,
    riinfo: &RiConstraintInfo,
    qkey: &(Oid, i32),
    qplan: spi::SpiPlanPtr,
    fk_rel: &Relation<'mcx>,
    pk_rel: &Relation<'mcx>,
    oldtup: Option<&HeapTupleData<'_>>,
    newtup: Option<&HeapTupleData<'_>>,
    is_restrict: bool,
    detect_new_rows: bool,
    expect_ok: i32,
) -> PgResult<bool> {
    let (source_rel, source_is_pk) = if qkey.1 == RI_PLAN_CHECK_LOOKUPPK {
        (fk_rel, false)
    } else {
        (pk_rel, true)
    };

    let mut vals = [Datum::null(); RI_MAX_NUMKEYS * 2];
    let mut nulls = [false; RI_MAX_NUMKEYS * 2];
    let nargs;
    if let Some(new_t) = newtup {
        ri_ExtractValues(source_rel, new_t, riinfo, source_is_pk, &mut vals, &mut nulls);
        if let Some(old_t) = oldtup {
            ri_ExtractValues(
                source_rel,
                old_t,
                riinfo,
                source_is_pk,
                &mut vals[riinfo.nkeys..],
                &mut nulls[riinfo.nkeys..],
            );
            nargs = riinfo.nkeys * 2;
        } else {
            nargs = riinfo.nkeys;
        }
    } else {
        let old_t = oldtup.expect("RI check without a source tuple");
        ri_ExtractValues(source_rel, old_t, riinfo, source_is_pk, &mut vals, &mut nulls);
        nargs = riinfo.nkeys;
    }

    // C: in transaction-snapshot mode with detectNewRows, run under a current
    // snapshot and have the executor error out on any row that would be
    // invisible per the transaction snapshot (the crosscheck).
    let (test_snapshot, crosscheck_snapshot) =
        if xact::IsolationUsesXactSnapshot() && detect_new_rows {
            xact::CommandCounterIncrement()?;
            (Some(snapmgr::GetLatestSnapshot()?), Some(snapmgr::GetTransactionSnapshot()?))
        } else {
            (None, None)
        };

    let limit = if expect_ok == spi::SPI_OK_SELECT { 1 } else { 0 };
    let query_rel = if qkey.1 <= RI_PLAN_LAST_ON_PK { pk_rel } else { fk_rel };
    let (_, save_sec_context) = miscinit::GetUserIdAndSecContext();
    let guard = miscinit::SecContextGuard::set(
        query_rel.rd_rel.relowner,
        save_sec_context | SECURITY_LOCAL_USERID_CHANGE | SECURITY_NOFORCE_RLS,
    );
    let spi_result = spi::SPI_execute_snapshot(
        qplan,
        &vals[..nargs],
        &nulls[..nargs],
        test_snapshot,
        crosscheck_snapshot,
        false,
        false,
        limit,
    );
    guard.restore();
    let spi_result = spi_result?;

    if spi_result != expect_ok {
        panic!(
            "referential integrity query on \"{}\" from constraint \"{}\" on \"{}\" gave \
             unexpected result",
            pk_rel.name(),
            conname_str(riinfo),
            fk_rel.name()
        );
    }

    let processed = spi::SPI_processed();
    if qkey.1 != RI_PLAN_CHECK_LOOKUPPK_FROM_PK
        && expect_ok == spi::SPI_OK_SELECT
        && (processed == 0) == (qkey.1 == RI_PLAN_CHECK_LOOKUPPK)
    {
        let src = newtup.or(oldtup).expect("RI check without a source tuple");
        return Err(ri_ReportViolation(
            mcx,
            riinfo,
            pk_rel,
            fk_rel,
            src,
            None,
            qkey.1,
            is_restrict,
            false,
        ));
    }
    Ok(processed != 0)
}

fn ri_ExtractValues(
    rel: &Relation<'_>,
    tup: &HeapTupleData<'_>,
    riinfo: &RiConstraintInfo,
    rel_is_pk: bool,
    vals: &mut [Datum],
    nulls: &mut [bool],
) {
    let attnums = if rel_is_pk { &riinfo.pk_attnums } else { &riinfo.fk_attnums };
    for i in 0..riinfo.nkeys {
        let mut isnull = false;
        // SAFETY: attnums are live user columns of rel's descriptor.
        vals[i] = unsafe { types_tuple::heap_getattr(tup, attnums[i] as i32, &rel.rd_att, &mut isnull) };
        nulls[i] = isnull;
    }
}

// C errtableconstraint(rel, conname): the schema/table/constraint
// ErrorResponse fields that constraint-mapping clients (diesel's
// constraint_name(), sequel's pg_auto_constraint_validations) read.
fn errtableconstraint<'mcx>(
    e: PgError,
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    conname: &str,
) -> PgError {
    let mut e = e
        .with_table_name(rel.name().to_owned())
        .with_constraint_name(conname.to_owned());
    if let Ok(Some(nsp)) = lsyscache::get_namespace_name(mcx, rel.rd_rel.relnamespace) {
        e = e.with_schema_name(nsp.as_str().to_owned());
    }
    e
}

#[track_caller]
#[cold]
#[inline(never)]
fn match_full_mixing_error<'mcx>(
    mcx: Mcx<'mcx>,
    fk_rel: &Relation<'mcx>,
    riinfo: &RiConstraintInfo,
) -> Box<PgError> {
    let conname = conname_str(riinfo);
    let e = PgError::new(
        ERROR,
        format!(
            "insert or update on table \"{}\" violates foreign key constraint \"{conname}\"",
            fk_rel.name()
        ),
    )
    .with_sqlstate(ERRCODE_FOREIGN_KEY_VIOLATION)
    .with_detail("MATCH FULL does not allow mixing of null and nonnull key values.");
    Box::new(errtableconstraint(e, mcx, fk_rel, conname))
}

// aclchk.c ACLCHECK_OK.
const ACLCHECK_OK: i32 = 0;

#[cold]
#[inline(never)]
#[allow(clippy::too_many_arguments)]
fn ri_ReportViolation<'mcx>(
    mcx: Mcx<'mcx>,
    riinfo: &RiConstraintInfo,
    pk_rel: &Relation<'mcx>,
    fk_rel: &Relation<'mcx>,
    violator: &HeapTupleData<'_>,
    tupdesc: Option<&types_tuple::TupleDescData<'_>>,
    queryno: i32,
    is_restrict: bool,
    partgone: bool,
) -> Box<PgError> {
    let onfk = queryno == RI_PLAN_CHECK_LOOKUPPK;
    let (attnums, rel) = if onfk {
        (&riinfo.fk_attnums, fk_rel)
    } else {
        (&riinfo.pk_attnums, pk_rel)
    };
    let desc = tupdesc.unwrap_or(&rel.rd_att);

    // Omit the key values from the detail when the user could not SELECT
    // them: RLS enabled on the rel, or no table- nor column-level SELECT
    // privilege (C ri_triggers.c:2690).
    let has_perm = if partgone {
        true
    } else {
        (|| -> PgResult<bool> {
            if rls_seams::check_enable_rls::call(rel.rd_id, types_core::InvalidOid, true)?
                == rls_seams::CheckEnableRls::RlsEnabled
            {
                return Ok(false);
            }
            let (aclresult, _) = aclchk_seams::pg_class_aclcheck_ext::call(
                rel.rd_id,
                miscinit::GetUserId(),
                types_nodes::parsenodes::ACL_SELECT,
            )?;
            if aclresult == ACLCHECK_OK {
                return Ok(true);
            }
            for idx in 0..riinfo.nkeys {
                let aclresult = aclchk_seams::pg_attribute_aclcheck::call(
                    rel.rd_id,
                    attnums[idx],
                    miscinit::GetUserId(),
                    types_nodes::parsenodes::ACL_SELECT,
                )?;
                if aclresult != ACLCHECK_OK {
                    return Ok(false);
                }
            }
            Ok(true)
        })()
        .unwrap_or(false)
    };
    let mut key_names = String::new();
    let mut key_values = String::new();
    if has_perm {
        for idx in 0..riinfo.nkeys {
            let fnum = attnums[idx];
            let att = desc.attr(fnum as usize - 1);
            let name = core::str::from_utf8(att.attname.name_str()).expect("attname UTF-8");
            let mut isnull = false;
            // SAFETY: live user column of the violator's descriptor.
            let d =
                unsafe { types_tuple::heap_getattr(violator, fnum as i32, desc, &mut isnull) };
            let val = if isnull {
                "null".to_string()
            } else {
                datum_output_text(mcx, att.atttypid, d).unwrap_or_else(|_| "???".to_string())
            };
            if idx > 0 {
                key_names.push_str(", ");
                key_values.push_str(", ");
            }
            key_names.push_str(name);
            key_values.push_str(&val);
        }
    }

    let conname = conname_str(riinfo);
    // C attaches errtableconstraint(fk_rel, conname) on every branch below
    // (ri_triggers.c ri_ReportViolation).
    if partgone {
        let e = PgError::new(
            ERROR,
            format!(
                "removing partition \"{}\" violates foreign key constraint \"{conname}\"",
                pk_rel.name()
            ),
        )
        .with_sqlstate(ERRCODE_FOREIGN_KEY_VIOLATION)
        .with_detail(format!(
            "Key ({key_names})=({key_values}) is still referenced from table \"{}\".",
            fk_rel.name()
        ));
        return Box::new(errtableconstraint(e, mcx, fk_rel, conname));
    }
    if onfk {
        let e = PgError::new(
            ERROR,
            format!(
                "insert or update on table \"{}\" violates foreign key constraint \"{conname}\"",
                fk_rel.name()
            ),
        )
        .with_sqlstate(ERRCODE_FOREIGN_KEY_VIOLATION)
        .with_detail(if has_perm {
            format!(
                "Key ({key_names})=({key_values}) is not present in table \"{}\".",
                pk_rel.name()
            )
        } else {
            format!("Key is not present in table \"{}\".", pk_rel.name())
        });
        Box::new(errtableconstraint(e, mcx, fk_rel, conname))
    } else if is_restrict {
        let e = PgError::new(
            ERROR,
            format!(
                "update or delete on table \"{}\" violates RESTRICT setting of foreign key \
                 constraint \"{conname}\" on table \"{}\"",
                pk_rel.name(),
                fk_rel.name()
            ),
        )
        .with_sqlstate(ERRCODE_RESTRICT_VIOLATION)
        .with_detail(if has_perm {
            format!(
                "Key ({key_names})=({key_values}) is referenced from table \"{}\".",
                fk_rel.name()
            )
        } else {
            format!("Key is referenced from table \"{}\".", fk_rel.name())
        });
        Box::new(errtableconstraint(e, mcx, fk_rel, conname))
    } else {
        let e = PgError::new(
            ERROR,
            format!(
                "update or delete on table \"{}\" violates foreign key constraint \
                 \"{conname}\" on table \"{}\"",
                pk_rel.name(),
                fk_rel.name()
            ),
        )
        .with_sqlstate(ERRCODE_FOREIGN_KEY_VIOLATION)
        .with_detail(if has_perm {
            format!(
                "Key ({key_names})=({key_values}) is still referenced from table \"{}\".",
                fk_rel.name()
            )
        } else {
            format!("Key is still referenced from table \"{}\".", fk_rel.name())
        });
        Box::new(errtableconstraint(e, mcx, fk_rel, conname))
    }
}

fn datum_output_text<'mcx>(mcx: Mcx<'mcx>, typid: Oid, d: Datum) -> PgResult<String> {
    let (typoutput, _typisvarlena) = lsyscache::typ::getTypeOutputInfo(typid)?;
    let mut finfo = fmgr_seams::fmgr_info::call(typoutput)?;
    let mut fcinfo = types_fmgr::LocalFcinfo::<1>::fresh(InvalidOid);
    // range/multirange output fns build the text through the result mcx.
    // SAFETY: mcx outlives this call.
    unsafe { fcinfo.set_result_mcx(mcx) };
    fcinfo.set_arg(0, d);
    let out = finfo.invoke(&mut fcinfo)?;
    // SAFETY: type output functions return a NUL-terminated cstring datum.
    let cs = unsafe { core::ffi::CStr::from_ptr(out.as_usize() as *const core::ffi::c_char) };
    Ok(core::str::from_utf8(cs.to_bytes()).expect("server encoding").to_string())
}

// get_ri_constraint_root (ri_triggers.c): walk conparentid to the topmost
// constraint.
fn get_ri_constraint_root(mut constr_oid: Oid) -> PgResult<Oid> {
    use cache_syscache::{SearchSysCache1, SysCacheGetAttr, SysCacheKey, CONSTROID};
    loop {
        let tup = SearchSysCache1(CONSTROID, SysCacheKey::Value(Datum::from_oid(constr_oid)))?
            .unwrap_or_else(|| panic!("cache lookup failed for constraint {constr_oid}"));
        let (d, isnull) = SysCacheGetAttr(CONSTROID, &tup, 12)?;
        assert!(!isnull, "null conparentid");
        let parent = d.as_oid();
        if parent == InvalidOid {
            return Ok(constr_oid);
        }
        constr_oid = parent;
    }
}

// ri_BuildQueryKey (ri_triggers.c): inherited constraints share plans via the
// root constraint OID, except LOOKUPPK_FROM_PK (which queries the fired-on
// table itself).
fn ri_build_query_key(riinfo: &RiConstraintInfo, queryno: i32) -> (Oid, i32) {
    if queryno != RI_PLAN_CHECK_LOOKUPPK_FROM_PK {
        (riinfo.constraint_root_id, queryno)
    } else {
        (riinfo.constraint_id, queryno)
    }
}

fn ri_NullCheck(
    desc: &types_tuple::TupleDescData<'_>,
    tup: &HeapTupleData<'_>,
    riinfo: &RiConstraintInfo,
    rel_is_pk: bool,
) -> i32 {
    let attnums = if rel_is_pk { &riinfo.pk_attnums } else { &riinfo.fk_attnums };
    let mut allnull = true;
    let mut nonenull = true;
    for i in 0..riinfo.nkeys {
        let mut isnull = false;
        // SAFETY: live user column of the tuple's descriptor.
        unsafe { types_tuple::heap_getattr(tup, attnums[i] as i32, desc, &mut isnull) };
        if isnull {
            nonenull = false;
        } else {
            allnull = false;
        }
    }
    if allnull {
        RI_KEYS_ALL_NULL
    } else if nonenull {
        RI_KEYS_NONE_NULL
    } else {
        RI_KEYS_SOME_NULL
    }
}

// ri_KeysEqual (ri_triggers.c). PK side: datum_image_eq; FK side: the ff
// equality operator (ri_CompareWithCast without the cast lane — cross-type
// FKs whose comparison needs a cast are loud).
fn ri_KeysEqual(
    rel: &Relation<'_>,
    oldtup: &HeapTupleData<'_>,
    newtup: &HeapTupleData<'_>,
    riinfo: &RiConstraintInfo,
    rel_is_pk: bool,
) -> PgResult<bool> {
    let attnums = if rel_is_pk { &riinfo.pk_attnums } else { &riinfo.fk_attnums };
    for i in 0..riinfo.nkeys {
        let mut oldnull = false;
        let mut newnull = false;
        // SAFETY (both): live user columns of rel's descriptor.
        let oldvalue =
            unsafe { types_tuple::heap_getattr(oldtup, attnums[i] as i32, &rel.rd_att, &mut oldnull) };
        let newvalue =
            unsafe { types_tuple::heap_getattr(newtup, attnums[i] as i32, &rel.rd_att, &mut newnull) };
        if oldnull || newnull {
            return Ok(false);
        }
        let att = rel.rd_att.attr(attnums[i] as usize - 1);
        if rel_is_pk {
            if !datum_image_eq(oldvalue, newvalue, att.attbyval, att.attlen) {
                return Ok(false);
            }
        } else {
            // PERIOD column: skip the check whenever the referencing range
            // stayed equal or shrank — contained-by instead of equality.
            let eq_opr = if riinfo.hasperiod && i == riinfo.nkeys - 1 {
                riinfo.period_contained_by_oper
            } else {
                riinfo.ff_eq_oprs[i]
            };
            let opcode = lsyscache::operator::get_opcode(eq_opr)?;
            let (oprleft, _) = lsyscache::operator::op_input_types(eq_opr)?;
            // ri_HashCompareOp (ri_triggers.c): cross-type FK comparisons run
            // both values through the implicit-coercion function to the
            // operator's input type; binary-coercible operands (incl.
            // polymorphic anyrange/anymultirange) relabel without one.
            // Uncached (C caches fmgr state per eq_opr/type pair).
            let mut castfunc = InvalidOid;
            if att.atttypid != oprleft {
                let (pathtype, f) = coerce::find_coercion_pathway(
                    oprleft,
                    att.atttypid,
                    coerce::CoercionContext::COERCION_IMPLICIT,
                )?;
                match pathtype {
                    coerce::CoercionPathType::COERCION_PATH_FUNC => castfunc = f,
                    coerce::CoercionPathType::COERCION_PATH_RELABELTYPE => {}
                    _ => {
                        if !coerce::IsBinaryCoercible(att.atttypid, oprleft)? {
                            panic!(
                                "no conversion function from {} to {}",
                                format_type::format_type_be(att.atttypid)?,
                                format_type::format_type_be(oprleft)?
                            );
                        }
                    }
                }
            }
            let scratch = mcx::MemoryContext::new("ri-keys-cmp");
            let (mut newvalue, mut oldvalue) = (newvalue, oldvalue);
            if castfunc != InvalidOid {
                let mut cast = |v: Datum| -> PgResult<Datum> {
                    let mut finfo = fmgr_seams::fmgr_info::call(castfunc)?;
                    let mut fcinfo = types_fmgr::LocalFcinfo::<3>::fresh(InvalidOid);
                    // SAFETY: scratch outlives this call.
                    unsafe { fcinfo.set_result_mcx(scratch.mcx()) };
                    fcinfo.set_arg(0, v);
                    fcinfo.set_arg(1, Datum::from_i32(-1));
                    fcinfo.set_arg(2, Datum::from_bool(false));
                    finfo.invoke(&mut fcinfo)
                };
                newvalue = cast(newvalue)?;
                oldvalue = cast(oldvalue)?;
            }
            let mut finfo = fmgr_seams::fmgr_info::call(opcode)?;
            let mut fcinfo = types_fmgr::LocalFcinfo::<2>::fresh(att.attcollation);
            // range/multirange operators detoast through the result mcx.
            // SAFETY: scratch outlives this call.
            unsafe { fcinfo.set_result_mcx(scratch.mcx()) };
            fcinfo.set_arg(0, newvalue);
            fcinfo.set_arg(1, oldvalue);
            if !finfo.invoke(&mut fcinfo)?.as_bool() {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

// generate_operator_clause (ruleutils.c) reduced: RI operators come from the
// support index's opfamily; the pg_catalog qualification is C-exact for the
// in-core families this port creates.
fn ri_GenerateQual(
    buf: &mut PgString<'_>,
    sep: &str,
    leftop: &str,
    leftoptype: Oid,
    opoid: Oid,
    rightop: &str,
    rightoptype: Oid,
) -> PgResult<()> {
    let shape = syscache_shape_for_operator(opoid)?;
    use core::fmt::Write;
    write!(buf, " {sep} {leftop}").expect("PgString write");
    if leftoptype != shape.0 {
        add_cast_to(buf, shape.0)?;
    }
    write!(buf, " OPERATOR(pg_catalog.{}) {rightop}", shape.2.as_str()).expect("PgString write");
    if rightoptype != shape.1 {
        add_cast_to(buf, shape.1)?;
    }
    Ok(())
}

// add_cast_to (ruleutils.c): "::nspname.typname".
fn add_cast_to(buf: &mut PgString<'_>, typid: Oid) -> PgResult<()> {
    let (typname, typnamespace) = syscache_seams::pg_type_name_namespace::call(typid)?
        .unwrap_or_else(|| panic!("cache lookup failed for type {typid}"));
    let scratch = mcx::MemoryContext::new("ri-cast-nsp");
    let nsp = lsyscache::get_namespace_name(scratch.mcx(), typnamespace)?
        .unwrap_or_else(|| panic!("cache lookup failed for namespace {typnamespace}"));
    let tn = String::from_utf8_lossy(typname.name_str()).into_owned();
    use core::fmt::Write;
    write!(buf, "::{}.{}", quote_one_name(nsp.as_str()), quote_one_name(&tn))
        .expect("PgString write");
    Ok(())
}

fn syscache_shape_for_operator(opoid: Oid) -> PgResult<(Oid, Oid, String)> {
    let (left, right) = lsyscache::operator::op_input_types(opoid)?;
    let scratch = mcx::MemoryContext::new("ri-opname");
    let name = lsyscache::operator::get_opname(scratch.mcx(), opoid)?
        .unwrap_or_else(|| panic!("cache lookup failed for operator {opoid}"));
    Ok((left, right, name.as_str().to_string()))
}

// ri_GenerateQualCollation (ri_triggers.c): append an always-qualified
// COLLATE spec so the generated query is not search-path-dependent.
fn ri_GenerateQualCollation(
    mcx: Mcx<'_>,
    buf: &mut PgString<'_>,
    collation: Oid,
) -> PgResult<()> {
    use core::fmt::Write;
    if collation == InvalidOid {
        return Ok(());
    }
    let shape = syscache_seams::lookup_pg_collation_shape::call(collation)?
        .unwrap_or_else(|| panic!("cache lookup failed for collation {collation}"));
    let collname =
        core::str::from_utf8(shape.collname.name_str()).expect("collname UTF-8");
    let nsp = lsyscache::get_namespace_name(mcx, shape.collnamespace)?
        .expect("collation namespace exists");
    write!(
        buf,
        " COLLATE {}.{}",
        quote_one_name(nsp.as_str()),
        quote_one_name(collname)
    )
    .expect("PgString write");
    Ok(())
}

fn quote_one_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 2);
    out.push('"');
    for c in name.chars() {
        if c == '"' {
            out.push('"');
        }
        out.push(c);
    }
    out.push('"');
    out
}

fn quote_relation_name<'mcx>(mcx: Mcx<'mcx>, rel: &Relation<'mcx>) -> PgResult<String> {
    let nsp = lsyscache::get_namespace_name(mcx, rel.rd_rel.relnamespace)?
        .unwrap_or_else(|| panic!("cache lookup failed for namespace {}", rel.rd_rel.relnamespace));
    Ok(format!("{}.{}", quote_one_name(nsp.as_str()), quote_one_name(rel.name())))
}

// datumIsEqual (datum.c) image comparison; typlen -2 (cstring) unreachable
// for FK key columns backed by a btree unique index.
fn datum_image_eq(a: Datum, b: Datum, typbyval: bool, typlen: i16) -> bool {
    if typbyval {
        // Compare at typlen width: a formed-then-deformed datum may differ
        // from the original in the upper bits (C 49315de).
        let (x, y) = (a.as_usize(), b.as_usize());
        return match typlen {
            1 => x as u8 == y as u8,
            2 => x as u16 == y as u16,
            4 => x as u32 == y as u32,
            _ => x == y,
        };
    }
    // SAFETY: by-ref datums point at live untoasted images (heap_getattr of
    // pinned-page tuples re-fetched by the trigger machinery).
    unsafe {
        let (pa, pb) = (a.as_usize() as *const u8, b.as_usize() as *const u8);
        let (la, lb) = if typlen > 0 {
            (typlen as usize, typlen as usize)
        } else {
            assert!(typlen == -1, "datum_image_eq: cstring keys unreachable");
            (types_tuple::varatt::varsize_any(pa), types_tuple::varatt::varsize_any(pb))
        };
        la == lb && core::slice::from_raw_parts(pa, la) == core::slice::from_raw_parts(pb, lb)
    }
}

fn att_type(rel: &Relation<'_>, attnum: i16) -> Oid {
    rel.rd_att.attr(attnum as usize - 1).atttypid
}

fn att_collation(rel: &Relation<'_>, attnum: i16) -> Oid {
    rel.rd_att.attr(attnum as usize - 1).attcollation
}

fn att_name<'a>(rel: &'a Relation<'_>, attnum: i16) -> &'a str {
    core::str::from_utf8(rel.rd_att.attr(attnum as usize - 1).attname.name_str())
        .expect("attname UTF-8")
}

fn conname_str(riinfo: &RiConstraintInfo) -> &str {
    core::str::from_utf8(riinfo.conname.name_str()).expect("conname UTF-8")
}

// table_tuple_satisfies_snapshot(SnapshotSelf) over a fresh TID fetch.
fn tuple_satisfies_self(rel: &Relation<'_>, tup: &HeapTupleData<'_>) -> PgResult<bool> {
    let scratch = MemoryContext::new("ri-snapshot-self");
    let snap = types_snapshot::SnapshotData::sentinel(scratch.mcx(), types_snapshot::SNAPSHOT_SELF);
    let mut res = heapam::heap_fetch(rel, &snap, tup.t_self, false)?;
    let found = res.found;
    if let Some(pin) = res.pin.take() {
        pin.release();
    }
    Ok(found)
}
