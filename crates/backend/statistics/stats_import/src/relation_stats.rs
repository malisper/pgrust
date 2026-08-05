use crate::{
    lookup_relation, recovery_in_progress_error, stats_check_required_arg,
    stats_fill_fcinfo_from_arg_pairs, text_datum_string, warn, Arg, StatsArgInfo,
};
use datum::Datum;
use mcx::{Mcx, MemoryContext};
use types_core::catalog::RELATION_RELATION_ID;
use types_core::{FLOAT4OID, INT4OID, TEXTOID};
use types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo};
use types_rel::lock::{RowExclusiveLock, LOCKMODE};

const RELSCHEMA_ARG: usize = 0;
const RELNAME_ARG: usize = 1;
const RELPAGES_ARG: usize = 2;
const RELTUPLES_ARG: usize = 3;
const RELALLVISIBLE_ARG: usize = 4;
const RELALLFROZEN_ARG: usize = 5;
const NUM_RELATION_STATS_ARGS: usize = 6;

static RELARGINFO: [StatsArgInfo; NUM_RELATION_STATS_ARGS] = [
    StatsArgInfo { argname: "schemaname", argtype: TEXTOID },
    StatsArgInfo { argname: "relname", argtype: TEXTOID },
    StatsArgInfo { argname: "relpages", argtype: INT4OID },
    StatsArgInfo { argname: "reltuples", argtype: FLOAT4OID },
    StatsArgInfo { argname: "relallvisible", argtype: INT4OID },
    StatsArgInfo { argname: "relallfrozen", argtype: INT4OID },
];

const Anum_pg_class_relpages: i32 = 10;
const Anum_pg_class_reltuples: i32 = 11;
const Anum_pg_class_relallvisible: i32 = 12;
const Anum_pg_class_relallfrozen: i32 = 13;

fn relation_statistics_update(mcx: Mcx<'_>, args: &[Arg]) -> PgResult<bool> {
    let mut result = true;

    stats_check_required_arg(args, &RELARGINFO, RELSCHEMA_ARG)?;
    stats_check_required_arg(args, &RELARGINFO, RELNAME_ARG)?;

    let nspname = text_datum_string(mcx, args[RELSCHEMA_ARG].value)?;
    let relname = text_datum_string(mcx, args[RELNAME_ARG].value)?;

    if transam_xlog::RecoveryInProgress() {
        return Err(recovery_in_progress_error());
    }

    let reloid = lookup_relation(mcx, &nspname, &relname)?;

    let mut relpages: u32 = 0;
    let mut update_relpages = false;
    let mut reltuples: f32 = 0.0;
    let mut update_reltuples = false;
    let mut relallvisible: u32 = 0;
    let mut update_relallvisible = false;
    let mut relallfrozen: u32 = 0;
    let mut update_relallfrozen = false;

    if !args[RELPAGES_ARG].isnull {
        relpages = args[RELPAGES_ARG].value.as_i32() as u32;
        update_relpages = true;
    }

    if !args[RELTUPLES_ARG].isnull {
        reltuples = args[RELTUPLES_ARG].value.as_f32();
        if reltuples < -1.0 {
            warn(
                "relation_statistics_update",
                "argument \"reltuples\" must not be less than -1.0".to_string(),
                Some(ERRCODE_INVALID_PARAMETER_VALUE),
                None,
                None,
            )?;
            result = false;
        } else {
            update_reltuples = true;
        }
    }

    if !args[RELALLVISIBLE_ARG].isnull {
        relallvisible = args[RELALLVISIBLE_ARG].value.as_i32() as u32;
        update_relallvisible = true;
    }

    if !args[RELALLFROZEN_ARG].isnull {
        relallfrozen = args[RELALLFROZEN_ARG].value.as_i32() as u32;
        update_relallfrozen = true;
    }

    // RowExclusiveLock on pg_class, consistent with vac_update_relstats().
    let crel = table::table_open(mcx, RELATION_RELATION_ID, RowExclusiveLock as LOCKMODE)?;

    let ctup = cache_syscache::SearchSysCache1(
        cache_syscache::cacheinfo::RELOID,
        cache_syscache::SysCacheKey::Value(Datum::from_oid(reloid)),
    )?
    .ok_or_else(|| {
        Box::new(PgError::error(format!(
            "pg_class entry for relid {reloid} not found"
        )))
    })?;

    {
        let old = ctup.tuple();
        let desc = crel.descr();

        let mut replaces: [i32; 4] = [0; 4];
        let mut values: [Datum; 4] = [Datum::null(); 4];
        let nulls: [bool; 4] = [false; 4];
        let mut nreplaces = 0usize;

        if update_relpages && relpages != getattr(&old, Anum_pg_class_relpages, desc).as_i32() as u32
        {
            replaces[nreplaces] = Anum_pg_class_relpages;
            values[nreplaces] = Datum::from_i32(relpages as i32);
            nreplaces += 1;
        }

        if update_reltuples && reltuples != getattr(&old, Anum_pg_class_reltuples, desc).as_f32() {
            replaces[nreplaces] = Anum_pg_class_reltuples;
            values[nreplaces] = Datum::from_f32(reltuples);
            nreplaces += 1;
        }

        if update_relallvisible
            && relallvisible != getattr(&old, Anum_pg_class_relallvisible, desc).as_i32() as u32
        {
            replaces[nreplaces] = Anum_pg_class_relallvisible;
            values[nreplaces] = Datum::from_i32(relallvisible as i32);
            nreplaces += 1;
        }

        if update_relallfrozen
            && relallfrozen != getattr(&old, Anum_pg_class_relallfrozen, desc).as_i32() as u32
        {
            replaces[nreplaces] = Anum_pg_class_relallfrozen;
            values[nreplaces] = Datum::from_i32(relallfrozen as i32);
            nreplaces += 1;
        }

        if nreplaces > 0 {
            let mut newtup = heaptuple::heap_modify_tuple_by_cols(
                mcx,
                &old,
                desc,
                &replaces[..nreplaces],
                &values[..nreplaces],
                &nulls[..nreplaces],
            )?;
            let otid = old.t_self;
            catalog_indexing::CatalogTupleUpdate(mcx, &crel, &otid, &mut newtup)?;
        }
    }

    cache_syscache::ReleaseSysCache(ctup);

    // release the lock, consistent with vac_update_relstats()
    table::table_close(crel, RowExclusiveLock as LOCKMODE)?;

    xact::CommandCounterIncrement()?;

    Ok(result)
}

fn getattr(
    tup: &types_tuple::HeapTupleData<'_>,
    attnum: i32,
    desc: &types_tuple::TupleDescData<'_>,
) -> Datum {
    let mut isnull = false;
    // SAFETY: fixed pg_class columns read under pg_class's descriptor; never null.
    let d = unsafe { types_tuple::heap_getattr(tup, attnum, desc, &mut isnull) };
    debug_assert!(!isnull);
    d
}

pub fn fc_pg_restore_relation_stats(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let cx = MemoryContext::new("pg_restore_relation_stats");
    let mcx = cx.mcx();

    let mut result = true;
    let mut positional = [Arg::NULL; NUM_RELATION_STATS_ARGS];

    if !stats_fill_fcinfo_from_arg_pairs(
        mcx,
        flinfo.as_deref(),
        fcinfo,
        &RELARGINFO,
        &mut positional,
    )? {
        result = false;
    }
    if !relation_statistics_update(mcx, &positional)? {
        result = false;
    }

    Ok(Datum::from_bool(result))
}

pub fn fc_pg_clear_relation_stats(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let cx = MemoryContext::new("pg_clear_relation_stats");
    let mcx = cx.mcx();

    let mut positional = [Arg::NULL; NUM_RELATION_STATS_ARGS];
    positional[RELSCHEMA_ARG] = Arg { value: fcinfo.arg(0), isnull: fcinfo.argisnull(0) };
    positional[RELNAME_ARG] = Arg { value: fcinfo.arg(1), isnull: fcinfo.argisnull(1) };
    positional[RELPAGES_ARG] = Arg::present(Datum::from_i32(0));
    positional[RELTUPLES_ARG] = Arg::present(Datum::from_f32(-1.0));
    positional[RELALLVISIBLE_ARG] = Arg::present(Datum::from_i32(0));
    positional[RELALLFROZEN_ARG] = Arg::present(Datum::from_i32(0));

    relation_statistics_update(mcx, &positional)?;
    Ok(Datum::null())
}
