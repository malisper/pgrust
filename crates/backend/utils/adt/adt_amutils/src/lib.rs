//! amutils.c over the closed in-core AM set (IndexAmKind); AM flags are the
//! handler-routine constants from nbtree.c/hash.c/gist.c/ginutil.c/
//! spgutils.c/brin.c 18.3.

use ::datum::Datum;
use ::mcx::Mcx;
use ::types_core::{Oid, BTREE_AM_OID, GIN_AM_OID, GIST_AM_OID, HASH_AM_OID, SPGIST_AM_OID};
use ::types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED};
use ::types_fmgr::{
    varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction,
};

const BRIN_AM_OID: Oid = 3580;

const RELKIND_INDEX: i8 = b'i' as i8;
const RELKIND_PARTITIONED_INDEX: i8 = b'I' as i8;
const INDOPTION_DESC: i16 = 0x0001;
const INDOPTION_NULLS_FIRST: i16 = 0x0002;

const GIST_COMPRESS_PROC: i16 = 3;
const GIST_DISTANCE_PROC: i16 = 8;
const GIST_FETCH_PROC: i16 = 9;
const AMOP_ORDER: i8 = b'o' as i8;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Prop {
    Asc,
    Desc,
    NullsFirst,
    NullsLast,
    Orderable,
    DistanceOrderable,
    Returnable,
    SearchArray,
    SearchNulls,
    Clusterable,
    IndexScan,
    BitmapScan,
    BackwardScan,
    CanOrder,
    CanUnique,
    CanMultiCol,
    CanExclude,
    CanInclude,
    Unknown,
}

const AM_PROPNAMES: &[(&str, Prop)] = &[
    ("asc", Prop::Asc),
    ("desc", Prop::Desc),
    ("nulls_first", Prop::NullsFirst),
    ("nulls_last", Prop::NullsLast),
    ("orderable", Prop::Orderable),
    ("distance_orderable", Prop::DistanceOrderable),
    ("returnable", Prop::Returnable),
    ("search_array", Prop::SearchArray),
    ("search_nulls", Prop::SearchNulls),
    ("clusterable", Prop::Clusterable),
    ("index_scan", Prop::IndexScan),
    ("bitmap_scan", Prop::BitmapScan),
    ("backward_scan", Prop::BackwardScan),
    ("can_order", Prop::CanOrder),
    ("can_unique", Prop::CanUnique),
    ("can_multi_col", Prop::CanMultiCol),
    ("can_exclude", Prop::CanExclude),
    ("can_include", Prop::CanInclude),
];

fn lookup_prop_name(name: &[u8]) -> Prop {
    for &(n, p) in AM_PROPNAMES {
        if n.len() == name.len() && n.bytes().zip(name).all(|(a, &b)| a == b.to_ascii_lowercase())
        {
            return p;
        }
    }
    // no error, so AMs can define their own properties
    Prop::Unknown
}

struct AmFlags {
    amcanorder: bool,
    amcanorderbyop: bool,
    amcanbackward: bool,
    amcanunique: bool,
    amcanmulticol: bool,
    amsearcharray: bool,
    amsearchnulls: bool,
    amclusterable: bool,
    amcaninclude: bool,
    has_amgettuple: bool,
    has_amcanreturn: bool,
    has_amproperty: bool,
    has_ambuildphasename: bool,
}

fn am_flags(amoid: Oid) -> Option<&'static AmFlags> {
    const BT: AmFlags = AmFlags {
        amcanorder: true,
        amcanorderbyop: false,
        amcanbackward: true,
        amcanunique: true,
        amcanmulticol: true,
        amsearcharray: true,
        amsearchnulls: true,
        amclusterable: true,
        amcaninclude: true,
        has_amgettuple: true,
        has_amcanreturn: true,
        has_amproperty: true,
        has_ambuildphasename: true,
    };
    const HASH: AmFlags = AmFlags {
        amcanorder: false,
        amcanorderbyop: false,
        amcanbackward: true,
        amcanunique: false,
        amcanmulticol: false,
        amsearcharray: false,
        amsearchnulls: false,
        amclusterable: false,
        amcaninclude: false,
        has_amgettuple: true,
        has_amcanreturn: false,
        has_amproperty: false,
        has_ambuildphasename: false,
    };
    const GIST: AmFlags = AmFlags {
        amcanorder: false,
        amcanorderbyop: true,
        amcanbackward: false,
        amcanunique: false,
        amcanmulticol: true,
        amsearcharray: false,
        amsearchnulls: true,
        amclusterable: true,
        amcaninclude: true,
        has_amgettuple: true,
        has_amcanreturn: true,
        has_amproperty: true,
        has_ambuildphasename: false,
    };
    const GIN: AmFlags = AmFlags {
        amcanorder: false,
        amcanorderbyop: false,
        amcanbackward: false,
        amcanunique: false,
        amcanmulticol: true,
        amsearcharray: false,
        amsearchnulls: false,
        amclusterable: false,
        amcaninclude: false,
        has_amgettuple: false,
        has_amcanreturn: false,
        has_amproperty: false,
        has_ambuildphasename: true,
    };
    const SPGIST: AmFlags = AmFlags {
        amcanorder: false,
        amcanorderbyop: true,
        amcanbackward: false,
        amcanunique: false,
        amcanmulticol: false,
        amsearcharray: false,
        amsearchnulls: true,
        amclusterable: false,
        amcaninclude: true,
        has_amgettuple: true,
        has_amcanreturn: true,
        has_amproperty: true,
        has_ambuildphasename: false,
    };
    const BRIN: AmFlags = AmFlags {
        amcanorder: false,
        amcanorderbyop: false,
        amcanbackward: false,
        amcanunique: false,
        amcanmulticol: true,
        amsearcharray: false,
        amsearchnulls: true,
        amclusterable: false,
        amcaninclude: false,
        has_amgettuple: false,
        has_amcanreturn: false,
        has_amproperty: false,
        has_ambuildphasename: false,
    };
    // contrib/bloom blhandler flags: multicolumn yes, everything else no;
    // bitmap-only (no amgettuple), amcanreturn/amproperty/ambuildphasename NULL.
    const BLOOM: AmFlags = AmFlags {
        amcanorder: false,
        amcanorderbyop: false,
        amcanbackward: false,
        amcanunique: false,
        amcanmulticol: true,
        amsearcharray: false,
        amsearchnulls: false,
        amclusterable: false,
        amcaninclude: false,
        has_amgettuple: false,
        has_amcanreturn: false,
        has_amproperty: false,
        has_ambuildphasename: false,
    };
    const HNSW: AmFlags = AmFlags {
        amcanorder: false,
        amcanorderbyop: true,
        amcanbackward: false,
        amcanunique: false,
        amcanmulticol: false,
        amsearcharray: false,
        amsearchnulls: false,
        amclusterable: false,
        amcaninclude: false,
        has_amgettuple: true,
        has_amcanreturn: false,
        has_amproperty: false,
        has_ambuildphasename: true,
    };
    match canonical_index_am(amoid) {
        BTREE_AM_OID => Some(&BT),
        HASH_AM_OID => Some(&HASH),
        GIST_AM_OID => Some(&GIST),
        GIN_AM_OID => Some(&GIN),
        SPGIST_AM_OID => Some(&SPGIST),
        BRIN_AM_OID => Some(&BRIN),
        other => match extension_am_handler(other).as_deref() {
            Some(b"hnswhandler") => Some(&HNSW),
            Some(b"blhandler") => Some(&BLOOM),
            _ => None,
        },
    }
}

fn extension_am_handler(amoid: Oid) -> Option<Vec<u8>> {
    let handler = syscache_seams::pg_am_amhandler::call(amoid).ok()??;
    let name = syscache_seams::pg_proc_proname::call(handler).ok()??;
    Some(name.name_str().to_vec())
}

// pg_am.amhandler -> the handler's builtin AM (C reads the IndexAmRoutine, so
// a non-builtin AM over a builtin handler shares its arms).
fn canonical_index_am(amoid: Oid) -> Oid {
    if matches!(
        amoid,
        BTREE_AM_OID | HASH_AM_OID | GIN_AM_OID | GIST_AM_OID | SPGIST_AM_OID | BRIN_AM_OID
    ) {
        return amoid;
    }
    match syscache_seams::pg_am_amhandler::call(amoid) {
        Ok(Some(330)) => BTREE_AM_OID,
        Ok(Some(331)) => HASH_AM_OID,
        Ok(Some(333)) => GIN_AM_OID,
        Ok(Some(332)) => GIST_AM_OID,
        Ok(Some(334)) => SPGIST_AM_OID,
        Ok(Some(335)) => BRIN_AM_OID,
        _ => amoid,
    }
}

// opfamily_can_sort_type (amvalidate.c): a btree opclass exists for the
// family+datatype pair.
fn opfamily_can_sort_type(mcx: Mcx<'_>, opfamilyoid: Oid, datatypeoid: Oid) -> PgResult<bool> {
    let rows = syscache_seams::lookup_pg_opclass_rows_by_am::call(mcx, BTREE_AM_OID)?;
    for &(_oid, opcfamily, opcintype, _opcdefault, _name) in rows.iter() {
        if opcfamily == opfamilyoid && opcintype == datatypeoid {
            return Ok(true);
        }
    }
    Ok(false)
}

// The amproperty override; None = the routine punted to the generic code.
fn am_property(
    mcx: Mcx<'_>,
    amoid: Oid,
    index_oid: Oid,
    attno: i32,
    prop: Prop,
) -> PgResult<Option<Option<bool>>> {
    match canonical_index_am(amoid) {
        // btproperty (nbtutils.c)
        BTREE_AM_OID => Ok(match prop {
            Prop::Returnable if attno != 0 => Some(Some(true)),
            _ => None,
        }),
        // gistproperty (gistutil.c)
        GIST_AM_OID => {
            if attno == 0 {
                return Ok(None);
            }
            let procno = match prop {
                Prop::DistanceOrderable => GIST_DISTANCE_PROC,
                Prop::Returnable => GIST_FETCH_PROC,
                _ => return Ok(None),
            };
            let opclass = lsyscache::get_index_column_opclass(index_oid, attno)?;
            if opclass == 0 {
                return Ok(Some(None));
            }
            let Some((opfamily, opcintype)) =
                lsyscache::get_opclass_opfamily_and_input_type(opclass)?
            else {
                return Ok(Some(None));
            };
            let mut res =
                lsyscache::get_opfamily_proc(opfamily, opcintype, opcintype, procno)? != 0;
            if prop == Prop::Returnable && !res {
                res = lsyscache::get_opfamily_proc(
                    opfamily,
                    opcintype,
                    opcintype,
                    GIST_COMPRESS_PROC,
                )? == 0;
            }
            Ok(Some(Some(res)))
        }
        // spgproperty (spgutils.c)
        SPGIST_AM_OID => {
            if attno == 0 || prop != Prop::DistanceOrderable {
                return Ok(None);
            }
            let opclass = lsyscache::get_index_column_opclass(index_oid, attno)?;
            if opclass == 0 {
                return Ok(Some(None));
            }
            let Some((opfamily, opcintype)) =
                lsyscache::get_opclass_opfamily_and_input_type(opclass)?
            else {
                return Ok(Some(None));
            };
            let (rows, _) = syscache_seams::lookup_pg_amop_rows::call(mcx, opfamily)?;
            let mut res = false;
            for row in rows.iter() {
                if row.amoppurpose == AMOP_ORDER
                    && (row.amoplefttype == opcintype || row.amoprighttype == opcintype)
                    && opfamily_can_sort_type(
                        mcx,
                        row.amopsortfamily,
                        lsyscache::get_op_rettype(row.amopopr)?,
                    )?
                {
                    res = true;
                    break;
                }
            }
            Ok(Some(Some(res)))
        }
        _ => Ok(None),
    }
}

struct IndexRow {
    indnatts: i16,
    indnkeyatts: i16,
}

fn index_row(index_oid: Oid) -> PgResult<Option<IndexRow>> {
    Ok(syscache_seams::lookup_pg_index_ls_shape::call(index_oid)?.map(|s| IndexRow {
        indnatts: s.indnatts,
        indnkeyatts: s.indnkeyatts,
    }))
}

fn test_indoption(
    index_oid: Oid,
    attno: i32,
    guard: bool,
    mask: i16,
    expect: i16,
) -> PgResult<Option<bool>> {
    if !guard {
        return Ok(Some(false));
    }
    let val = syscache_seams::pg_index_indoption_element::call(index_oid, attno - 1)?
        .unwrap_or_else(|| panic!("cache lookup failed for index {index_oid}"));
    Ok(Some((val & mask) == expect))
}

fn indexam_property(
    mcx: Mcx<'_>,
    propname: &[u8],
    mut amoid: Oid,
    index_oid: Oid,
    attno: i32,
) -> PgResult<Option<bool>> {
    let prop = lookup_prop_name(propname);

    let mut natts = 0i32;
    if index_oid != 0 {
        debug_assert_eq!(amoid, 0);
        let Some(rel) = syscache_seams::lookup_pg_class_ls_shape::call(index_oid)? else {
            return Ok(None);
        };
        if rel.relkind != RELKIND_INDEX && rel.relkind != RELKIND_PARTITIONED_INDEX {
            return Ok(None);
        }
        amoid = rel.relam;
        natts = rel.relnatts as i32;
    }

    if attno < 0 || attno > natts {
        return Ok(None);
    }

    let Some(routine) = am_flags(amoid) else {
        // C returns NULL for an invalid/non-index AM OID; a real out-of-core
        // index AM is an unported lane, not a NULL.
        if amoid != 0 && is_index_am(amoid)? {
            // unported: out-of-core index AM property lane.
            return Err(Box::new(
                PgError::error(format!(
                    "index AM properties for non-core access method {amoid} \
                     are not yet implemented"
                ))
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
            ));
        }
        return Ok(None);
    };

    if routine.has_amproperty {
        if let Some(over) = am_property(mcx, amoid, index_oid, attno, prop)? {
            return Ok(over);
        }
    }

    if attno > 0 {
        let Some(ind) = index_row(index_oid)? else {
            return Ok(None);
        };
        debug_assert!(attno > 0 && attno <= ind.indnatts as i32);
        let iskey = !(routine.amcaninclude && attno > ind.indnkeyatts as i32);

        let out: Option<bool> = match prop {
            Prop::Asc if iskey => {
                test_indoption(index_oid, attno, routine.amcanorder, INDOPTION_DESC, 0)?
            }
            Prop::Desc if iskey => test_indoption(
                index_oid,
                attno,
                routine.amcanorder,
                INDOPTION_DESC,
                INDOPTION_DESC,
            )?,
            Prop::NullsFirst if iskey => test_indoption(
                index_oid,
                attno,
                routine.amcanorder,
                INDOPTION_NULLS_FIRST,
                INDOPTION_NULLS_FIRST,
            )?,
            Prop::NullsLast if iskey => {
                test_indoption(index_oid, attno, routine.amcanorder, INDOPTION_NULLS_FIRST, 0)?
            }
            Prop::Orderable => Some(if iskey { routine.amcanorder } else { false }),
            Prop::DistanceOrderable => {
                if !iskey || !routine.amcanorderbyop {
                    Some(false)
                } else {
                    None
                }
            }
            Prop::Returnable => Some(if routine.has_amcanreturn {
                indexam_seams::index_can_return::call(mcx, index_oid, attno)?
            } else {
                false
            }),
            Prop::SearchArray if iskey => Some(routine.amsearcharray),
            Prop::SearchNulls if iskey => Some(routine.amsearchnulls),
            _ => None,
        };
        return Ok(out);
    }

    if index_oid != 0 {
        return Ok(match prop {
            Prop::Clusterable => Some(routine.amclusterable),
            Prop::IndexScan => Some(routine.has_amgettuple),
            Prop::BitmapScan => Some(true),
            Prop::BackwardScan => Some(routine.amcanbackward),
            _ => None,
        });
    }

    Ok(match prop {
        Prop::CanOrder => Some(routine.amcanorder),
        Prop::CanUnique => Some(routine.amcanunique),
        Prop::CanMultiCol => Some(routine.amcanmulticol),
        Prop::CanExclude => Some(routine.has_amgettuple),
        Prop::CanInclude => Some(routine.amcaninclude),
        _ => None,
    })
}

// GetIndexAmRoutineByAmId(amoid, noerror=true) NULL-vs-error split: NULL for
// a nonexistent/non-index AM; a real out-of-core index AM is unported.
fn is_index_am(amoid: Oid) -> PgResult<bool> {
    Ok(syscache_seams::pg_am_amtype::call(amoid)? == Some(b'i' as i8))
}

fn bool_or_null(fcinfo: &mut Fcinfo, v: Option<bool>) -> Datum {
    match v {
        Some(b) => Datum::from_bool(b),
        None => fcinfo.return_null(),
    }
}

pub fn fc_pg_indexam_has_property(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let amoid = fcinfo.arg_oid(0);
    // SAFETY: catalog arg 1 is text; strict fn.
    let prop = unsafe { fcinfo.arg_varlena_packed(1)? };
    let r = indexam_property(fcinfo.result_mcx(), prop.data(), amoid, 0, 0)?;
    Ok(bool_or_null(fcinfo, r))
}

pub fn fc_pg_index_has_property(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let relid = fcinfo.arg_oid(0);
    // SAFETY: catalog arg 1 is text; strict fn.
    let prop = unsafe { fcinfo.arg_varlena_packed(1)? };
    let r = indexam_property(fcinfo.result_mcx(), prop.data(), 0, relid, 0)?;
    Ok(bool_or_null(fcinfo, r))
}

pub fn fc_pg_index_column_has_property(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let relid = fcinfo.arg_oid(0);
    let attno = fcinfo.arg_i32(1);
    // SAFETY: catalog arg 2 is text; strict fn.
    let prop = unsafe { fcinfo.arg_varlena_packed(2)? };
    if attno <= 0 {
        return Ok(bool_or_null(fcinfo, None));
    }
    let r = indexam_property(fcinfo.result_mcx(), prop.data(), 0, relid, attno)?;
    Ok(bool_or_null(fcinfo, r))
}

// PG_GETARG_INT32 truncation of the int8 arg is C-exact.
pub fn fc_pg_indexam_progress_phasename(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let amoid = fcinfo.arg_oid(0);
    let phasenum = fcinfo.arg_i64(1) as i32 as i64;
    let name = match canonical_index_am(amoid) {
        BTREE_AM_OID => bt_phasename(phasenum),
        GIN_AM_OID => gin_phasename(phasenum),
        _ => match am_flags(amoid) {
            Some(_) => None,
            None if is_index_am(amoid)? => {
                // unported: out-of-core index AM build-phase-name lane.
                return Err(Box::new(
                    PgError::error(format!(
                        "build phase names for non-core access method {amoid} \
                         are not yet implemented"
                    ))
                    .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
                ));
            }
            None => None,
        },
    };
    match name {
        Some(s) => Ok(varlena_result(varlena::cstring_to_text(
            fcinfo.result_mcx(),
            s.as_bytes(),
        )?)),
        None => Ok(fcinfo.return_null()),
    }
}

// btbuildphasename (nbtutils.c); PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE = 1.
fn bt_phasename(phasenum: i64) -> Option<&'static str> {
    match phasenum {
        1 => Some("initializing"),
        2 => Some("scanning table"),
        3 => Some("sorting live tuples"),
        4 => Some("sorting dead tuples"),
        5 => Some("loading tuples in tree"),
        _ => None,
    }
}

// ginbuildphasename (ginutil.c).
fn gin_phasename(phasenum: i64) -> Option<&'static str> {
    match phasenum {
        1 => Some("initializing"),
        2 => Some("scanning table"),
        3 => Some("sorting tuples (workers)"),
        4 => Some("merging tuples (workers)"),
        5 => Some("sorting tuples"),
        6 => Some("merging tuples"),
        _ => None,
    }
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

pub const AMUTILS_BUILTINS: &[FmgrBuiltin] = &[
    b(636, "pg_indexam_has_property", 2, fc_pg_indexam_has_property),
    b(637, "pg_index_has_property", 2, fc_pg_index_has_property),
    b(638, "pg_index_column_has_property", 3, fc_pg_index_column_has_property),
    b(676, "pg_indexam_progress_phasename", 2, fc_pg_indexam_progress_phasename),
];

#[cfg(test)]
mod tests;
