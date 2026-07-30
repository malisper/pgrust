//! pg_database catalog reads: the GetDatabaseTuple / GetDatabaseTupleByOid
//! scans and the DATABASEOID syscache decode postinit.c consumes.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use core::slice;

use datum::Datum;
use mcx::{Mcx, PgString, PgVec};
use pg_database_seams::PgDatabaseForm;
use types_core::catalog::{C_COLLATION_OID, DATABASE_RELATION_ID};
use types_core::fmgr::{F_NAMEEQ, F_OIDEQ, NAMEDATALEN};
use types_core::{AttrNumber, Oid};
use types_error::{PgError, PgResult};
use types_rel::{AccessShareLock, Relation};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_tuple::{HeapTupleData, TupleDescData};

#[cfg(test)]
mod tests;

pub const DatabaseNameIndexId: Oid = 2671;
pub const DatabaseOidIndexId: Oid = 2672;

pub const Natts_pg_database: usize = 18;
pub const Anum_pg_database_oid: i32 = 1;
pub const Anum_pg_database_datname: i32 = 2;
pub const Anum_pg_database_datdba: i32 = 3;
pub const Anum_pg_database_encoding: i32 = 4;
pub const Anum_pg_database_datlocprovider: i32 = 5;
pub const Anum_pg_database_datistemplate: i32 = 6;
pub const Anum_pg_database_datallowconn: i32 = 7;
pub const Anum_pg_database_dathasloginevt: i32 = 8;
pub const Anum_pg_database_datconnlimit: i32 = 9;
pub const Anum_pg_database_datfrozenxid: i32 = 10;
pub const Anum_pg_database_datminmxid: i32 = 11;
pub const Anum_pg_database_dattablespace: i32 = 12;
pub const Anum_pg_database_datcollate: i32 = 13;
pub const Anum_pg_database_datctype: i32 = 14;
pub const Anum_pg_database_datlocale: i32 = 15;
pub const Anum_pg_database_daticurules: i32 = 16;
pub const Anum_pg_database_datcollversion: i32 = 17;
pub const Anum_pg_database_datacl: i32 = 18;

pub const DATCONNLIMIT_UNLIMITED: i32 = -1;
pub const DATCONNLIMIT_INVALID_DB: i32 = -2;

const DATABASEOID: i32 = 21;

#[track_caller]
#[cold]
#[inline(never)]
fn non_utf8(col: &'static str) -> Box<PgError> {
    PgError::error(format!("pg_database column {col} is not valid UTF-8")).into()
}

fn eq_key(attno: i32, eqfunc: types_core::primitive::RegProcedure, arg: Datum) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno as AttrNumber;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = C_COLLATION_OID;
    key.sk_func = fmgr_seams::fmgr_info::call(eqfunc)
        .unwrap_or_else(|e| panic!("fmgr_info({eqfunc}) failed: {e:?}"));
    key.sk_argument = arg;
    key
}

// CStringGetDatum(dbname) framed as a full NameData block: the nameeq carrier
// copies NAMEDATALEN bytes from its argument, so a bare cstring would over-read.
fn name_arg<'mcx>(mcx: Mcx<'mcx>, name: &str) -> PgResult<(PgVec<'mcx, u8>, Datum)> {
    let n = NAMEDATALEN as usize;
    let mut buf: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, n)?;
    let take = name.len().min(n - 1);
    mcx::vec_append_bytes(&mut buf, &name.as_bytes()[..take])?;
    mcx::vec_append_bytes(&mut buf, &[0u8; 64][..n - take])?;
    let d = Datum::from_usize(buf.as_ptr() as usize);
    Ok((buf, d))
}

fn name_str<'mcx>(mcx: Mcx<'mcx>, d: Datum) -> PgResult<PgString<'mcx>> {
    // SAFETY: d comes off a NameData column: NAMEDATALEN readable bytes.
    let bytes = unsafe { slice::from_raw_parts(d.as_usize() as *const u8, NAMEDATALEN as usize) };
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    let s = core::str::from_utf8(&bytes[..end]).map_err(|_| non_utf8("datname"))?;
    PgString::from_str_in(s, mcx)
}

// TextDatumGetCString: detoast-if-needed (open_image) + copy out.
fn text_str<'mcx>(mcx: Mcx<'mcx>, d: Datum, col: &'static str) -> PgResult<PgString<'mcx>> {
    let p = d.as_usize() as *const u8;
    // SAFETY: d comes off a not-null text column: a live varlena image
    // readable through its varsize_any extent.
    let image = unsafe { slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
    let payload = varlena::open_image(mcx, image)?;
    let s = core::str::from_utf8(payload.as_bytes()).map_err(|_| non_utf8(col))?;
    PgString::from_str_in(s, mcx)
}

pub(crate) fn decode_form<'mcx>(
    mcx: Mcx<'mcx>,
    mut att: impl FnMut(i32) -> PgResult<(Datum, bool)>,
) -> PgResult<PgDatabaseForm<'mcx>> {
    let mut req = |attno: i32| -> PgResult<Datum> {
        let (d, isnull) = att(attno)?;
        if isnull {
            return Err(PgError::error(format!(
                "unexpected null in pg_database column {attno}"
            ))
            .into());
        }
        Ok(d)
    };

    let oid = req(Anum_pg_database_oid)?.as_oid();
    let datname_d = req(Anum_pg_database_datname)?;
    let datdba = req(Anum_pg_database_datdba)?.as_oid();
    let datistemplate = req(Anum_pg_database_datistemplate)?.as_bool();
    let encoding = req(Anum_pg_database_encoding)?.as_i32();
    let datlocprovider = req(Anum_pg_database_datlocprovider)?.as_u8();
    let datallowconn = req(Anum_pg_database_datallowconn)?.as_bool();
    let dathasloginevt = req(Anum_pg_database_dathasloginevt)?.as_bool();
    let datconnlimit = req(Anum_pg_database_datconnlimit)?.as_i32();
    let datfrozenxid = req(Anum_pg_database_datfrozenxid)?.as_u32();
    let datminmxid = req(Anum_pg_database_datminmxid)?.as_u32();
    let dattablespace = req(Anum_pg_database_dattablespace)?.as_oid();
    let datcollate_d = req(Anum_pg_database_datcollate)?;
    let datctype_d = req(Anum_pg_database_datctype)?;
    drop(req);

    let mut opt = |attno: i32| -> PgResult<Option<Datum>> {
        let (d, isnull) = att(attno)?;
        Ok(if isnull { None } else { Some(d) })
    };
    let datlocale_d = opt(Anum_pg_database_datlocale)?;
    let daticurules_d = opt(Anum_pg_database_daticurules)?;
    let datcollversion_d = opt(Anum_pg_database_datcollversion)?;

    let opt_text = |d: Option<Datum>, col: &'static str| -> PgResult<Option<PgString<'mcx>>> {
        d.map(|d| text_str(mcx, d, col)).transpose()
    };

    Ok(PgDatabaseForm {
        oid,
        datname: name_str(mcx, datname_d)?,
        datdba,
        datistemplate,
        dattablespace,
        datallowconn,
        dathasloginevt,
        datconnlimit,
        datfrozenxid,
        datminmxid,
        encoding,
        datlocprovider,
        datcollate: text_str(mcx, datcollate_d, "datcollate")?,
        datctype: text_str(mcx, datctype_d, "datctype")?,
        datlocale: opt_text(datlocale_d, "datlocale")?,
        daticurules: opt_text(daticurules_d, "daticurules")?,
        datcollversion: opt_text(datcollversion_d, "datcollversion")?,
    })
}

fn decode_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    td: &TupleDescData<'_>,
    tup: &HeapTupleData<'_>,
) -> PgResult<PgDatabaseForm<'mcx>> {
    decode_form(mcx, |attno| {
        let mut isnull = false;
        // SAFETY: tup is a pg_database row read under this relation's
        // descriptor; attno is a declared pg_database column.
        let d = unsafe { types_tuple::heap_getattr(tup, attno, td, &mut isnull) };
        Ok((d, isnull))
    })
}

fn scan_first<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    index_id: Oid,
    index_ok: bool,
    key: ScanKeyData,
) -> PgResult<Option<PgDatabaseForm<'mcx>>> {
    let keys = [key];
    let mut scan = genam::systable_beginscan(mcx, rel, index_id, index_ok, None, &keys)?;
    let decoded = match genam::systable_getnext(mcx, &mut scan)? {
        Some(tup) => Some(decode_tuple(mcx, rel.descr(), tup)?),
        None => None,
    };
    genam::systable_endscan(mcx, scan)?;
    Ok(decoded)
}

/// GetDatabaseTuple(dbname) (postinit.c), decoded.
pub fn get_database_tuple_by_name<'mcx>(
    mcx: Mcx<'mcx>,
    dbname: &str,
) -> PgResult<Option<PgDatabaseForm<'mcx>>> {
    let rel = table::table_open(mcx, DATABASE_RELATION_ID, AccessShareLock)?;
    let (_name_buf, arg) = name_arg(mcx, dbname)?;
    let key = eq_key(Anum_pg_database_datname, F_NAMEEQ, arg);
    let index_ok = relcache::criticalSharedRelcachesBuilt();
    let decoded = scan_first(mcx, &rel, DatabaseNameIndexId, index_ok, key)?;
    rel.close(AccessShareLock)?;
    Ok(decoded)
}

/// GetDatabaseTupleByOid(dboid) (postinit.c), decoded.
pub fn get_database_tuple_by_oid<'mcx>(
    mcx: Mcx<'mcx>,
    dboid: Oid,
) -> PgResult<Option<PgDatabaseForm<'mcx>>> {
    let rel = table::table_open(mcx, DATABASE_RELATION_ID, AccessShareLock)?;
    let key = eq_key(Anum_pg_database_oid, F_OIDEQ, Datum::from_oid(dboid));
    let index_ok = relcache::criticalSharedRelcachesBuilt();
    let decoded = scan_first(mcx, &rel, DatabaseOidIndexId, index_ok, key)?;
    rel.close(AccessShareLock)?;
    Ok(decoded)
}

/// SearchSysCache1(DATABASEOID, dboid) + the GETSTRUCT/SysCacheGetAttr reads
/// of CheckMyDatabase (postinit.c), decoded once.
pub fn search_database_syscache<'mcx>(
    mcx: Mcx<'mcx>,
    dboid: Oid,
) -> PgResult<Option<PgDatabaseForm<'mcx>>> {
    let Some(tup) = cache_syscache::SearchSysCache1(
        DATABASEOID,
        cache_syscache::SysCacheKey::Value(Datum::from_oid(dboid)),
    )?
    else {
        return Ok(None);
    };
    let decoded =
        decode_form(mcx, |attno| cache_syscache::SysCacheGetAttr(DATABASEOID, &tup, attno));
    cache_syscache::ReleaseSysCache(tup);
    decoded.map(Some)
}

pub fn init_seams() {
    pg_database_seams::get_database_tuple_by_name::set(get_database_tuple_by_name);
    pg_database_seams::get_database_tuple_by_oid::set(get_database_tuple_by_oid);
    pg_database_seams::search_database_syscache::set(search_database_syscache);
}
