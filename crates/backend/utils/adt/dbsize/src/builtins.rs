//! fmgr wrappers (`fc_*`) + `DBSIZE_BUILTINS` for fmgr-core.

use ::datum::Datum;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};

pub fn fc_pg_size_bytes(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null text varlena (strict fn).
    let a = unsafe { fcinfo.arg_varlena_packed(0)? };
    let s = String::from_utf8_lossy(a.data());
    Ok(Datum::from_i64(crate::pg_size_bytes(&s)?))
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

// pg_relation_size(regclass, text). C stats the segment files
// (calculate_relation_size); one backend + full-page segments make
// smgrnblocks * BLCKSZ the same number without the fs walk.
pub fn fc_pg_relation_size(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let rel_oid = fcinfo.arg_oid(0);
    // SAFETY: catalog arg 1 is a non-null text varlena (strict fn).
    let forkname_b = unsafe { fcinfo.arg_varlena_packed(1)? };
    let forkname = String::from_utf8_lossy(forkname_b.data()).into_owned();
    let forknum = match forkname.as_str() {
        "main" => types_core::ForkNumber::MAIN_FORKNUM,
        "fsm" => types_core::ForkNumber::FSM_FORKNUM,
        "vm" => types_core::ForkNumber::VISIBILITYMAP_FORKNUM,
        "init" => types_core::ForkNumber::INIT_FORKNUM,
        other => {
            return Err(Box::new(
                ::types_error::PgError::error(format!("invalid fork name: \"{other}\""))
                    .with_sqlstate(::types_error::ERRCODE_INVALID_PARAMETER_VALUE)
                    .with_hint("Valid fork names are \"main\", \"fsm\", \"vm\", and \"init\"."),
            ))
        }
    };
    let mcx = fcinfo.result_mcx();
    let Some(rel) =
        relation_seams::try_relation_open::call(mcx, rel_oid, types_rel::AccessShareLock)?
    else {
        fcinfo.isnull = true;
        return Ok(Datum::null());
    };
    let key = ::types_storage::RelFileLocatorBackend {
        locator: rel.rd_locator.get(),
        backend: rel.rd_backend,
    };
    let size = if smgr_seams::smgr_exists::call(key, forknum)? {
        smgr_seams::smgr_nblocks::call(key, forknum)? as i64 * types_core::BLCKSZ as i64
    } else {
        0
    };
    rel.close(types_rel::AccessShareLock)?;
    Ok(Datum::from_i64(size))
}

pub const DBSIZE_BUILTINS: &[FmgrBuiltin] = &[
    b(3334, "pg_size_bytes", 1, fc_pg_size_bytes),
    b(2332, "pg_relation_size", 2, fc_pg_relation_size),
    b(2168, "pg_database_size_name", 1, fc_pg_database_size_name),
    b(2288, "pg_size_pretty", 1, fc_pg_size_pretty),
    b(2324, "pg_database_size_oid", 1, fc_pg_database_size_oid),
    b(3166, "pg_size_pretty_numeric", 1, fc_pg_size_pretty_numeric),
];

// size_pretty_units (dbsize.c): (name, limit, round, unitbits).
const SIZE_PRETTY_UNITS: &[(&str, u32, bool, u8)] = &[
    ("bytes", 10 * 1024, false, 0),
    ("kB", 20 * 1024 - 1, true, 10),
    ("MB", 20 * 1024 - 1, true, 20),
    ("GB", 20 * 1024 - 1, true, 30),
    ("TB", 20 * 1024 - 1, true, 40),
    ("PB", 20 * 1024 - 1, true, 50),
];

fn half_rounded(x: i64) -> i64 {
    (x + if x < 0 { -1 } else { 1 }) / 2
}

fn text_result(fcinfo: &Fcinfo, s: &str) -> PgResult<Datum> {
    Ok(types_fmgr::varlena_result(varlena::cstring_to_text(fcinfo.result_mcx(), s.as_bytes())?))
}

pub fn fc_pg_size_pretty(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mut size = fcinfo.arg_i64(0);
    let mut buf = String::new();
    for (i, &(name, limit, round, unitbits)) in SIZE_PRETTY_UNITS.iter().enumerate() {
        let next = SIZE_PRETTY_UNITS.get(i + 1);
        let abs_size: u64 =
            if size < 0 { 0u64.wrapping_sub(size as u64) } else { size as u64 };
        if next.is_none() || abs_size < limit as u64 {
            if round {
                size = half_rounded(size);
            }
            buf = format!("{size} {name}");
            break;
        }
        let next = next.unwrap();
        let bits = (next.3 as i32 - unitbits as i32 - (next.2 as i32)) + (round as i32);
        size /= 1i64 << bits;
    }
    text_result(fcinfo, &buf)
}

pub fn fc_pg_size_pretty_numeric(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    use adt_numeric::ops;
    // SAFETY: catalog arg 0 is a non-null numeric varlena (strict fn).
    let v = unsafe { fcinfo.arg_varlena_packed(0)? };
    let payload =
        if v.is_short() { v.data_expanded(fcinfo.result_mcx())? } else { v.data() };
    let mut size =
        adt_numeric::NumericImage::from_num(adt_numeric::Num::from_payload(payload));
    let mut result = String::new();
    for (i, &(name, limit, round, unitbits)) in SIZE_PRETTY_UNITS.iter().enumerate() {
        let next = SIZE_PRETTY_UNITS.get(i + 1);
        let below_limit = match next {
            None => true,
            Some(_) => {
                let abs = ops::numeric_abs(size.num());
                let lim = ops::int64_to_numeric(limit as i64);
                ops::numeric_lt(abs.num(), lim.num())
            }
        };
        if below_limit {
            if round {
                let zero = ops::int64_to_numeric(0);
                let one = ops::int64_to_numeric(1);
                let two = ops::int64_to_numeric(2);
                let adjusted = if ops::numeric_ge(size.num(), zero.num()) {
                    ops::numeric_add_common(size.num(), one.num())?
                } else {
                    ops::numeric_sub_common(size.num(), one.num())?
                };
                size = ops::numeric_div_trunc_common(adjusted.num(), two.num())?;
            }
            let mut out = Vec::new();
            adt_numeric::io::numeric_out_into(size.num(), &mut out);
            result = format!("{} {name}", String::from_utf8_lossy(&out));
            break;
        }
        let next = next.unwrap();
        let shiftby = (next.3 as i32 - unitbits as i32 - (next.2 as i32)) + (round as i32);
        let divisor = ops::int64_to_numeric(1i64 << shiftby);
        size = ops::numeric_div_trunc_common(size.num(), divisor.num())?;
    }
    text_result(fcinfo, &result)
}

// db_dir_size (dbsize.c): physical size of directory contents, 0 if absent.
// Paths are DataDir-relative (the backend chdir's to PGDATA, per C).
fn db_dir_size(path: &str) -> PgResult<i64> {
    let Ok(entries) = std::fs::read_dir(path) else {
        return Ok(0);
    };
    let mut dirsize: i64 = 0;
    for entry in entries {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };
        match entry.metadata() {
            Ok(m) => dirsize += m.len() as i64,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => {
                return Err(Box::new(::types_error::PgError::error(format!(
                    "could not stat file \"{}\": {e}",
                    entry.path().display()
                ))))
            }
        }
    }
    Ok(dirsize)
}

const ACL_CONNECT: u64 = 1 << 11; // acl.h
const ACLCHECK_OK: i32 = 0;
const ROLE_PG_READ_ALL_STATS: Oid = 3375;
const DATABASE_RELATION_ID: Oid = 1262;

fn calculate_database_size(db_oid: Oid) -> PgResult<i64> {
    let uid = miscinit_seams::get_user_id::call();
    let aclresult = aclchk_seams::object_aclcheck::call(DATABASE_RELATION_ID, db_oid, uid, ACL_CONNECT)?;
    if aclresult != ACLCHECK_OK
        && !acl_seams::has_privs_of_role::call(uid, ROLE_PG_READ_ALL_STATS)?
    {
        let datname = dbcommands_seams::get_database_name::call(db_oid)?.unwrap_or_default();
        aclchk_seams::aclcheck_error::call(
            aclresult,
            ::types_nodes::parsenodes::ObjectType::OBJECT_DATABASE as i32,
            &datname,
        )?;
    }

    let mut totalsize = db_dir_size(&format!("base/{db_oid}"))?;

    let tblspc = match std::fs::read_dir("pg_tblspc") {
        Ok(entries) => entries,
        Err(e) => {
            return Err(Box::new(::types_error::PgError::error(format!(
                "could not open directory \"pg_tblspc\": {e}"
            ))))
        }
    };
    for entry in tblspc.flatten() {
        totalsize += db_dir_size(&format!(
            "pg_tblspc/{}/{}/{db_oid}",
            entry.file_name().to_string_lossy(),
            ::types_storage::TABLESPACE_VERSION_DIRECTORY,
        ))?;
    }
    Ok(totalsize)
}

fn database_size_result(fcinfo: &mut Fcinfo, size: i64) -> PgResult<Datum> {
    if size == 0 {
        return Ok(fcinfo.return_null());
    }
    Ok(Datum::from_i64(size))
}

pub fn fc_pg_database_size_oid(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let db_oid = fcinfo.arg_oid(0);
    if !syscache_seams::search_syscache_exists_databaseoid::call(db_oid)? {
        return Err(Box::new(
            ::types_error::PgError::error(format!("database with OID {db_oid} does not exist"))
                .with_sqlstate(::types_error::ERRCODE_UNDEFINED_OBJECT),
        ));
    }
    let size = calculate_database_size(db_oid)?;
    database_size_result(fcinfo, size)
}

pub fn fc_pg_database_size_name(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null Name (strict fn).
    let name = unsafe { fcinfo.arg_name(0) };
    let end = name.iter().position(|&b| b == 0).unwrap_or(name.len());
    let dbname = core::str::from_utf8(&name[..end])
        .expect("database name is valid UTF-8")
        .to_owned();
    let db_oid = dbcommands_seams::get_database_oid::call(fcinfo.result_mcx(), &dbname, false)?;
    let size = calculate_database_size(db_oid)?;
    database_size_result(fcinfo, size)
}
