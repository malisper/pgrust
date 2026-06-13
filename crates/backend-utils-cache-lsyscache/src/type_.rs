//! `type` family — `lsyscache.c` lookups keyed on `pg_type` / `pg_range`
//! (`TYPEOID` / `RANGETYPE` syscaches and the type-I/O helpers).
//!
//! SCAFFOLD STAGE: signatures mirror the seam declarations; bodies are
//! `todo!()` until the SearchSysCache logic (catcache seam) lands.
//!
//! C entry points covered here: `get_typlenbyvalalign`, `get_type_io_data`,
//! `getTypeOutputInfo`, `getTypeInputInfo`, `getTypeBinaryOutputInfo`,
//! `getBaseType`, `getBaseTypeAndTypmod`, `get_base_element_type`,
//! `get_element_type`, `get_array_type`, `get_multirange_range`, plus the
//! typcache-driven `RANGETYPE` / `TYPEOID` row probes and the arrayfuncs.c
//! element-I/O projection.

use backend_utils_cache_lsyscache_seams::{IOFuncSelector, TypLenByValAlign, TypeIoData};
use types_array::{ArrayElementIoData, ArrayIoFuncSelector};
use types_cache::typcache::{PgRangeRow, PgTypeRow};
use types_core::Oid;
use types_error::PgResult;

/// `get_typlenbyvalalign(typid, &typlen, &typbyval, &typalign)` (lsyscache.c).
pub fn get_typlenbyvalalign(_typid: Oid) -> PgResult<TypLenByValAlign> {
    todo!("get_typlenbyvalalign: SearchSysCache(TYPEOID) -> (typlen, typbyval, typalign)")
}

/// `get_type_io_data(typid, which_func, ...)` (lsyscache.c) — canonical
/// `IOFuncSelector` / `TypeIoData` shape.
pub fn get_type_io_data(_typid: Oid, _which_func: IOFuncSelector) -> PgResult<TypeIoData> {
    todo!("get_type_io_data: SearchSysCache(TYPEOID) -> storage + selected I/O proc")
}

/// `getTypeOutputInfo(type, &typOutput, &typIsVarlena)` (lsyscache.c).
pub fn get_type_output_info(_typid: Oid) -> PgResult<(Oid, bool)> {
    todo!("get_type_output_info: SearchSysCache(TYPEOID) -> (typoutput, typisvarlena)")
}

/// `getTypeInputInfo(type, &typInput, &typIOParam)` (lsyscache.c).
pub fn get_type_input_info(_typ: Oid) -> PgResult<(Oid, Oid)> {
    todo!("get_type_input_info: SearchSysCache(TYPEOID) -> (typinput, typioparam)")
}

/// `getTypeBinaryOutputInfo(type, &typSend, &typIsVarlena)` (lsyscache.c).
pub fn get_type_binary_output_info(_type_oid: Oid) -> PgResult<(Oid, bool)> {
    todo!("get_type_binary_output_info: SearchSysCache(TYPEOID) -> (typsend, typisvarlena)")
}

/// `getBaseType(typid)` (lsyscache.c).
pub fn get_base_type(_typid: Oid) -> PgResult<Oid> {
    todo!("get_base_type: getBaseTypeAndTypmod with typmod = -1")
}

/// `getBaseTypeAndTypmod(type_id, &typmod)` (lsyscache.c).
pub fn get_base_type_and_typmod(_type_id: Oid) -> PgResult<(Oid, i32)> {
    todo!("get_base_type_and_typmod: walk domain chain via TYPEOID")
}

/// `get_base_element_type(type_id)` (lsyscache.c).
pub fn get_base_element_type(_type_id: Oid) -> PgResult<Oid> {
    todo!("get_base_element_type: element type of getBaseType(type_id)")
}

/// `get_element_type(array_type)` (lsyscache.c).
pub fn get_element_type(_array_type: Oid) -> PgResult<Option<Oid>> {
    todo!("get_element_type: SearchSysCache(TYPEOID) -> typelem when typlen == -1")
}

/// `get_array_type(input_type)` (lsyscache.c).
pub fn get_array_type(_input_type: Oid) -> PgResult<Option<Oid>> {
    todo!("get_array_type: SearchSysCache(TYPEOID) -> typarray")
}

/// `get_type_io_data(...)` array-element projection consumed by arrayfuncs.c.
pub fn get_array_element_io_data(
    _element_type: Oid,
    _which: ArrayIoFuncSelector,
) -> PgResult<ArrayElementIoData> {
    todo!("get_array_element_io_data: get_type_io_data projected to the array-element shape")
}

/// `get_multirange_range(multirange_type_id)` (lsyscache.c).
pub fn get_multirange_range(_multirange_type_id: Oid) -> PgResult<Oid> {
    todo!("get_multirange_range: SearchSysCache(RANGEMULTIRANGE) -> rngtypid")
}

/// `SearchSysCache1(RANGETYPE, ...)` row probe for `load_rangetype_info`.
pub fn lookup_pg_range(_range_type_id: Oid) -> PgResult<Option<PgRangeRow>> {
    todo!("lookup_pg_range: SearchSysCache(RANGETYPE) -> Form_pg_range fields")
}

/// `SearchSysCache1(TYPEOID, ...)` row probe for `TypeCacheEntry` build.
pub fn lookup_pg_type(_type_id: Oid) -> PgResult<Option<PgTypeRow>> {
    todo!("lookup_pg_type: SearchSysCache(TYPEOID) -> Form_pg_type fields + typname")
}

/// `GetSysCacheHashValue1(TYPEOID, ObjectIdGetDatum(type_id))` (typcache).
pub fn syscache_hash_value_typeoid(_type_id: Oid) -> PgResult<u32> {
    todo!("syscache_hash_value_typeoid: GetSysCacheHashValue1(TYPEOID)")
}
