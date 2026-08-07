//! misc.c + version.c slices; obj_description/col_description are C
//! SQL-language pg_proc rows (system_functions.sql) implemented natively.


use datum::{Datum, Varlena};
use mcx::Mcx;
use types_core::{AttrNumber, Oid};
use types_error::PgResult;
use types_rel::AccessShareLock;
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_tuple::NameData;

// Shaped like C's PG_VERSION_STR ("... on <triple>, ..."): collate.linux.utf8
// and infinite_recurse gate on version() ~ platform regexes ('linux-gnu'), so
// the target triple must appear in BOTH renderings. Which identity leads is
// GUC-controlled (pgrust.version_string_style, ruling 2026-08-06): the
// default postgres_first matches C PostgreSQL's own "PostgreSQL <ver> on ..."
// shape, so clients that parse the FIRST version number out of version()
// (duckdb-postgres's ExtractPostgresVersion took the legacy leading "0.3"
// for a pre-8.3 PostgreSQL and silently degraded) read the compatibility
// version, with pgrust's identity parenthesized; pgrust_first restores the
// legacy pgrust-led banner byte-for-byte. Both version numbers come from
// guc_tables::consts — the single source of truth, never re-hardcoded here.
// The `server_version` GUC stays exactly the plain compat number: that is
// what drivers parse for feature detection.
static VERSION_STR_POSTGRES_FIRST: std::sync::LazyLock<String> =
    std::sync::LazyLock::new(|| {
        format!(
            "PostgreSQL {} (pgrust {}) on {}, 64-bit",
            guc_tables::consts::PG_COMPAT_VERSION,
            guc_tables::consts::PGRUST_VERSION,
            env!("PGRUST_TARGET_TRIPLE"),
        )
    });
static VERSION_STR_PGRUST_FIRST: std::sync::LazyLock<String> =
    std::sync::LazyLock::new(|| {
        format!(
            "pgrust {} (PostgreSQL {} compatible) on {}, 64-bit",
            guc_tables::consts::PGRUST_VERSION,
            guc_tables::consts::PG_COMPAT_VERSION,
            env!("PGRUST_TARGET_TRIPLE"),
        )
    });

/// version()'s banner under the session's pgrust.version_string_style. Reads
/// the registered GUC's backing cell directly (the runtime_pool precedent:
/// always installed, never panics in unit tests).
pub fn pg_version_str() -> &'static str {
    if guc_tables::backing::pgrust_version_string_style()
        == guc_tables::consts::VERSION_STRING_PGRUST_FIRST
    {
        &VERSION_STR_PGRUST_FIRST
    } else {
        &VERSION_STR_POSTGRES_FIRST
    }
}

const DESCRIPTION_RELATION_ID: Oid = 2609;
const DESCRIPTION_OBJ_INDEX_ID: Oid = 2675;
const ANUM_PG_DESCRIPTION_DESCRIPTION: i32 = 4;
const SHARED_DESCRIPTION_RELATION_ID: Oid = 2396;
const SHARED_DESCRIPTION_OBJ_INDEX_ID: Oid = 2397;
const ANUM_PG_SHDESCRIPTION_DESCRIPTION: i32 = 3;

pub fn current_database() -> PgResult<NameData> {
    let dbname = dbcommands::get_database_name(init_small::globals::MyDatabaseId())?
        .expect("current database has a pg_database row");
    let mut db = NameData::default();
    db.namestrcpy(&dbname);
    Ok(db)
}

fn scankey(attno: usize, func: types_core::primitive::RegProcedure, arg: Datum) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno as AttrNumber;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(func)
        .unwrap_or_else(|e| panic!("fmgr_info({func}) failed: {e:?}"));
    key.sk_argument = arg;
    key
}

fn description_scan<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    indexid: Oid,
    desc_attno: i32,
    keys: &[ScanKeyData],
) -> PgResult<Option<Varlena<'mcx>>> {
    let rel = table::table_open(mcx, relid, AccessShareLock)?;
    let mut result: Option<Varlena<'mcx>> = None;
    genam_seams::systable_scan_catalog::call(&rel, indexid, true, keys, &mut |tup| {
        let mut isnull = false;
        // SAFETY: NOT NULL text column under the relation's own descriptor.
        let d = unsafe { types_tuple::heap_getattr(tup, desc_attno, rel.descr(), &mut isnull) };
        debug_assert!(!isnull);
        // SAFETY: in-tuple varlena, live for this callback; from_ptr
        // panics loudly on external/compressed images.
        let payload =
            unsafe { types_fmgr::PackedVarlena::from_ptr(d.as_usize() as *const u8) }.data();
        let mut image =
            mcx::vec_with_capacity_in(mcx, datum::varlena::VARHDRSZ + payload.len())?;
        image.resize(datum::varlena::VARHDRSZ, 0);
        mcx::vec_append_bytes(&mut image, payload)?;
        result = Some(Varlena::from_image(image));
        Ok(false)
    })?;
    rel.close(AccessShareLock)?;
    Ok(result)
}

pub fn get_description<'mcx>(
    mcx: Mcx<'mcx>,
    objoid: Oid,
    classoid: Oid,
    objsubid: i32,
) -> PgResult<Option<Varlena<'mcx>>> {
    let keys = [
        scankey(1, types_core::fmgr::F_OIDEQ, Datum::from_oid(objoid)),
        scankey(2, types_core::fmgr::F_OIDEQ, Datum::from_oid(classoid)),
        scankey(3, types_core::fmgr::F_INT4EQ, Datum::from_i32(objsubid)),
    ];
    description_scan(
        mcx,
        DESCRIPTION_RELATION_ID,
        DESCRIPTION_OBJ_INDEX_ID,
        ANUM_PG_DESCRIPTION_DESCRIPTION,
        &keys,
    )
}

pub fn get_shared_description<'mcx>(
    mcx: Mcx<'mcx>,
    objoid: Oid,
    classoid: Oid,
) -> PgResult<Option<Varlena<'mcx>>> {
    let keys = [
        scankey(1, types_core::fmgr::F_OIDEQ, Datum::from_oid(objoid)),
        scankey(2, types_core::fmgr::F_OIDEQ, Datum::from_oid(classoid)),
    ];
    description_scan(
        mcx,
        SHARED_DESCRIPTION_RELATION_ID,
        SHARED_DESCRIPTION_OBJ_INDEX_ID,
        ANUM_PG_SHDESCRIPTION_DESCRIPTION,
        &keys,
    )
}
