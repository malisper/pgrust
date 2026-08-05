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

// calculate_relation_size (dbsize.c). C stats the segment files; one backend
// + full-page segments make smgrnblocks * BLCKSZ the same number without the
// fs walk.
fn calculate_relation_size(
    key: ::types_storage::RelFileLocatorBackend,
    forknum: types_core::ForkNumber,
) -> PgResult<i64> {
    // Storage-less rels (views: relfilenumber 0) stat a nonexistent path in C
    // and count 0 bytes; smgropen asserts on RelFileNumber 0 so gate here.
    if key.locator.relNumber == 0 {
        return Ok(0);
    }
    if smgr_seams::smgr_exists::call(key, forknum)? {
        Ok(smgr_seams::smgr_nblocks::call(key, forknum)? as i64 * types_core::BLCKSZ as i64)
    } else {
        Ok(0)
    }
}

fn rel_key(rel: &types_rel::Relation<'_>) -> ::types_storage::RelFileLocatorBackend {
    ::types_storage::RelFileLocatorBackend {
        locator: rel.rd_locator.get(),
        backend: rel.rd_backend,
    }
}

// ForkNumber 0..=MAX_FORKNUM (relpath.h): main, fsm, vm, init.
const ALL_FORKS: [types_core::ForkNumber; 4] = [
    types_core::ForkNumber::MAIN_FORKNUM,
    types_core::ForkNumber::FSM_FORKNUM,
    types_core::ForkNumber::VISIBILITYMAP_FORKNUM,
    types_core::ForkNumber::INIT_FORKNUM,
];

fn calculate_all_forks_size(key: ::types_storage::RelFileLocatorBackend) -> PgResult<i64> {
    let mut size = 0i64;
    for forknum in ALL_FORKS {
        size += calculate_relation_size(key, forknum)?;
    }
    Ok(size)
}

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
        return Ok(fcinfo.return_null());
    };
    let size = calculate_relation_size(rel_key(&rel), forknum)?;
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
    b(2322, "pg_tablespace_size_oid", 1, fc_pg_tablespace_size_oid),
    b(2323, "pg_tablespace_size_name", 1, fc_pg_tablespace_size_name),
    b(2997, "pg_table_size", 1, fc_pg_table_size),
    b(2998, "pg_indexes_size", 1, fc_pg_indexes_size),
    b(2286, "pg_total_relation_size", 1, fc_pg_total_relation_size),
    b(2999, "pg_relation_filenode", 1, fc_pg_relation_filenode),
    b(3454, "pg_filenode_relation", 2, fc_pg_filenode_relation),
    b(3034, "pg_relation_filepath", 1, fc_pg_relation_filepath),
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

const ACL_CREATE: u64 = 1 << 9; // parsenodes.h
const TABLESPACE_RELATION_ID: Oid = 1213;

pub(crate) fn tablespace_dir_path(tblspc_oid: Oid) -> String {
    if tblspc_oid == relpath::DEFAULTTABLESPACE_OID {
        "base".to_string()
    } else if tblspc_oid == relpath::GLOBALTABLESPACE_OID {
        "global".to_string()
    } else {
        format!(
            "{}/{tblspc_oid}/{}",
            ::types_storage::PG_TBLSPC_DIR,
            ::types_storage::TABLESPACE_VERSION_DIRECTORY,
        )
    }
}

fn calculate_tablespace_size(mcx: ::mcx::Mcx<'_>, tblspc_oid: Oid) -> PgResult<i64> {
    let uid = miscinit_seams::get_user_id::call();
    if tblspc_oid != init_small::globals::MyDatabaseTableSpace()
        && !acl_seams::has_privs_of_role::call(uid, ROLE_PG_READ_ALL_STATS)?
    {
        let aclresult =
            aclchk_seams::object_aclcheck::call(TABLESPACE_RELATION_ID, tblspc_oid, uid, ACL_CREATE)?;
        if aclresult != ACLCHECK_OK {
            let spcname = tablespace_seams::get_tablespace_name::call(mcx, tblspc_oid)?;
            let spcname = spcname
                .map(|n| String::from_utf8_lossy(n.name_str()).into_owned())
                .unwrap_or_default();
            aclchk_seams::aclcheck_error::call(
                aclresult,
                ::types_nodes::parsenodes::ObjectType::OBJECT_TABLESPACE as i32,
                &spcname,
            )?;
        }
    }

    let tblspc_path = tablespace_dir_path(tblspc_oid);
    let Ok(entries) = std::fs::read_dir(&tblspc_path) else {
        return Ok(-1);
    };
    let mut totalsize = 0i64;
    for entry in entries {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };
        let pathname = format!("{tblspc_path}/{}", entry.file_name().to_string_lossy());
        // std::fs::metadata follows symlinks like C's stat(); pg_tblspc
        // entries are symlinks to the tablespace directories.
        let fst = match std::fs::metadata(&pathname) {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => {
                return Err(Box::new(::types_error::PgError::error(format!(
                    "could not stat file \"{pathname}\": {e}"
                ))))
            }
        };
        // C adds fst.st_size for every dirent and additionally recurses into
        // directories, so a subdirectory's own inode size is counted too.
        if fst.is_dir() {
            totalsize += db_dir_size(&pathname)?;
        }
        totalsize += fst.len() as i64;
    }
    Ok(totalsize)
}

fn tablespace_size_result(fcinfo: &mut Fcinfo, size: i64) -> PgResult<Datum> {
    if size < 0 {
        return Ok(fcinfo.return_null());
    }
    Ok(Datum::from_i64(size))
}

pub fn fc_pg_tablespace_size_oid(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let tblspc_oid = fcinfo.arg_oid(0);
    if !syscache_seams::search_syscache_exists_tablespaceoid::call(tblspc_oid)? {
        return Err(Box::new(
            ::types_error::PgError::error(format!(
                "tablespace with OID {tblspc_oid} does not exist"
            ))
            .with_sqlstate(::types_error::ERRCODE_UNDEFINED_OBJECT),
        ));
    }
    let size = calculate_tablespace_size(fcinfo.result_mcx(), tblspc_oid)?;
    tablespace_size_result(fcinfo, size)
}

pub fn fc_pg_tablespace_size_name(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null Name (strict fn).
    let name = unsafe { fcinfo.arg_name(0) };
    let end = name.iter().position(|&b| b == 0).unwrap_or(name.len());
    let spcname = core::str::from_utf8(&name[..end])
        .expect("tablespace name is valid UTF-8")
        .to_owned();
    let tblspc_oid =
        tablespace_seams::get_tablespace_oid::call(fcinfo.result_mcx(), &spcname, false)?;
    let size = calculate_tablespace_size(fcinfo.result_mcx(), tblspc_oid)?;
    tablespace_size_result(fcinfo, size)
}

// calculate_toast_table_size (dbsize.c): toast heap + all its indexes,
// all forks. Must not be applied to non-TOAST relations.
fn calculate_toast_table_size(mcx: ::mcx::Mcx<'_>, toastrelid: Oid) -> PgResult<i64> {
    let toast_rel =
        relation_seams::relation_open::call(mcx, toastrelid, types_rel::AccessShareLock)?;
    let mut size = calculate_all_forks_size(rel_key(&toast_rel))?;
    let indexlist = relcache_seams::relation_get_index_list::call(mcx, toast_rel.rd_id)?;
    for &idx_oid in indexlist.iter() {
        let idx_rel =
            relation_seams::relation_open::call(mcx, idx_oid, types_rel::AccessShareLock)?;
        size += calculate_all_forks_size(rel_key(&idx_rel))?;
        idx_rel.close(types_rel::AccessShareLock)?;
    }
    toast_rel.close(types_rel::AccessShareLock)?;
    Ok(size)
}

fn calculate_table_size(mcx: ::mcx::Mcx<'_>, rel: &types_rel::Relation<'_>) -> PgResult<i64> {
    let mut size = calculate_all_forks_size(rel_key(rel))?;
    if rel.rd_rel.reltoastrelid != 0 {
        size += calculate_toast_table_size(mcx, rel.rd_rel.reltoastrelid)?;
    }
    Ok(size)
}

fn calculate_indexes_size(mcx: ::mcx::Mcx<'_>, rel: &types_rel::Relation<'_>) -> PgResult<i64> {
    let mut size = 0i64;
    if rel.rd_rel.relhasindex {
        let index_oids = relcache_seams::relation_get_index_list::call(mcx, rel.rd_id)?;
        for &idx_oid in index_oids.iter() {
            let idx_rel =
                relation_seams::relation_open::call(mcx, idx_oid, types_rel::AccessShareLock)?;
            size += calculate_all_forks_size(rel_key(&idx_rel))?;
            idx_rel.close(types_rel::AccessShareLock)?;
        }
    }
    Ok(size)
}

pub fn fc_pg_table_size(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let rel_oid = fcinfo.arg_oid(0);
    let mcx = fcinfo.result_mcx();
    let Some(rel) =
        relation_seams::try_relation_open::call(mcx, rel_oid, types_rel::AccessShareLock)?
    else {
        return Ok(fcinfo.return_null());
    };
    let size = calculate_table_size(mcx, &rel)?;
    rel.close(types_rel::AccessShareLock)?;
    Ok(Datum::from_i64(size))
}

pub fn fc_pg_indexes_size(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let rel_oid = fcinfo.arg_oid(0);
    let mcx = fcinfo.result_mcx();
    let Some(rel) =
        relation_seams::try_relation_open::call(mcx, rel_oid, types_rel::AccessShareLock)?
    else {
        return Ok(fcinfo.return_null());
    };
    let size = calculate_indexes_size(mcx, &rel)?;
    rel.close(types_rel::AccessShareLock)?;
    Ok(Datum::from_i64(size))
}

pub fn fc_pg_total_relation_size(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let rel_oid = fcinfo.arg_oid(0);
    let mcx = fcinfo.result_mcx();
    let Some(rel) =
        relation_seams::try_relation_open::call(mcx, rel_oid, types_rel::AccessShareLock)?
    else {
        return Ok(fcinfo.return_null());
    };
    // C: table size already includes toast, then index size on top.
    let size = calculate_table_size(mcx, &rel)? + calculate_indexes_size(mcx, &rel)?;
    rel.close(types_rel::AccessShareLock)?;
    Ok(Datum::from_i64(size))
}

pub fn fc_pg_relation_filenode(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let relid = fcinfo.arg_oid(0);
    let Some(relform) = syscache_seams::lookup_pg_class_by_relid::call(relid)? else {
        return Ok(fcinfo.return_null());
    };
    let result = if types_rel::RELKIND_HAS_STORAGE(relform.relkind as u8) {
        if relform.relfilenode != 0 {
            relform.relfilenode
        } else {
            relmapper_seams::relation_map_oid_to_filenumber::call(relid, relform.relisshared)
        }
    } else {
        types_core::InvalidRelFileNumber
    };
    if result == types_core::InvalidRelFileNumber {
        return Ok(fcinfo.return_null());
    }
    Ok(Datum::from_oid(result))
}

pub fn fc_pg_filenode_relation(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let reltablespace = fcinfo.arg_oid(0);
    let relfilenumber = fcinfo.arg_oid(1);
    // C: test needed so RelidByRelfilenumber doesn't misbehave.
    if relfilenumber == types_core::InvalidRelFileNumber {
        return Ok(fcinfo.return_null());
    }
    let heaprel =
        relfilenumbermap_seams::relid_by_relfilenumber::call(reltablespace, relfilenumber)?;
    if heaprel == types_core::InvalidOid {
        return Ok(fcinfo.return_null());
    }
    Ok(Datum::from_oid(heaprel))
}

pub fn fc_pg_relation_filepath(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let relid = fcinfo.arg_oid(0);
    let Some(relform) = syscache_seams::lookup_pg_class_by_relid::call(relid)? else {
        return Ok(fcinfo.return_null());
    };
    // This logic should match RelationInitPhysicalAddr.
    let rlocator = if types_rel::RELKIND_HAS_STORAGE(relform.relkind as u8) {
        let spc_oid = if relform.reltablespace != 0 {
            relform.reltablespace
        } else {
            init_small::globals::MyDatabaseTableSpace()
        };
        let db_oid = if spc_oid == relpath::GLOBALTABLESPACE_OID {
            types_core::InvalidOid
        } else {
            init_small::globals::MyDatabaseId()
        };
        let rel_number = if relform.relfilenode != 0 {
            relform.relfilenode
        } else {
            relmapper_seams::relation_map_oid_to_filenumber::call(relid, relform.relisshared)
        };
        ::types_storage::RelFileLocator::new(spc_oid, db_oid, rel_number)
    } else {
        ::types_storage::RelFileLocator::new(
            types_core::InvalidOid,
            types_core::InvalidOid,
            types_core::InvalidRelFileNumber,
        )
    };
    if rlocator.relNumber == types_core::InvalidRelFileNumber {
        return Ok(fcinfo.return_null());
    }
    let backend = match relform.relpersistence as u8 {
        types_core::RELPERSISTENCE_UNLOGGED | types_core::RELPERSISTENCE_PERMANENT => {
            types_core::INVALID_PROC_NUMBER
        }
        types_core::RELPERSISTENCE_TEMP => {
            if namespace_seams::is_temp_or_temp_toast_namespace::call(relform.relnamespace) {
                init_small::globals::ProcNumberForTempRelations()
            } else {
                let backend = namespace_seams::get_temp_namespace_proc_number::call(
                    relform.relnamespace,
                )?;
                debug_assert!(backend != types_core::INVALID_PROC_NUMBER);
                backend
            }
        }
        other => {
            return Err(Box::new(::types_error::PgError::error(format!(
                "invalid relpersistence: {}",
                other as char
            ))))
        }
    };
    let path = relpath::GetRelationPath(rlocator, backend, types_core::ForkNumber::MAIN_FORKNUM);
    text_result(fcinfo, &path)
}
