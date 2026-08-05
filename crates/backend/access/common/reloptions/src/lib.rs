//! reloptions.c — pg_class.reloptions parse/transform machinery. Also hosts
//! the per-AM amoptions parse tables (C: nbtutils.c btoptions, hashutil.c
//! hashoptions, ginutil.c ginoptions, gistutil.c gistoptions, spgutils.c
//! spgoptions, brin.c brinoptions): the AM set is closed, so the dispatch is
//! a match on relam instead of C's IndexAmRoutine fn pointer.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

#[cfg(test)]
mod tests;

mod local;
pub use local::{build_local_reloptions, LocalOptDef, LocalRelopts};

use core::fmt::Write;

use ::datum::Datum;
use ::elog::ereport;
use ::mcx::{Mcx, PgString, PgVec};
use ::types_core::{
    Oid, BRIN_AM_OID, BTREE_AM_OID, GIN_AM_OID, GIST_AM_OID, HASH_AM_OID, SPGIST_AM_OID, TEXTOID,
};
use ::types_error::{
    PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_SYNTAX_ERROR, ERRCODE_WRONG_OBJECT_TYPE, ERROR,
};
use ::types_nodes::parsenodes::DefElem;
use ::types_nodes::NodeList;
use ::types_rel::{
    AutoVacOpts, BTOptions, BrinOptions, GinOptions, GistOptBufferingMode, GistOptions,
    HashOptions, HnswOptions, RdOptions, SpGistOptions, StdRdOptIndexCleanup, StdRdOptions,
    ViewOptCheckOption,
    ViewOptions, LOCKMODE, RELKIND_INDEX, RELKIND_MATVIEW, RELKIND_PARTITIONED_INDEX,
    RELKIND_PARTITIONED_TABLE, RELKIND_RELATION, RELKIND_TOASTVALUE, RELKIND_VIEW,
};

use ::types_rel::{AccessExclusiveLock, NoLock, ShareUpdateExclusiveLock};

pub type relopt_kind = u32;
pub const RELOPT_KIND_HEAP: relopt_kind = 1 << 0;
pub const RELOPT_KIND_TOAST: relopt_kind = 1 << 1;
pub const RELOPT_KIND_BTREE: relopt_kind = 1 << 2;
pub const RELOPT_KIND_HASH: relopt_kind = 1 << 3;
pub const RELOPT_KIND_GIN: relopt_kind = 1 << 4;
pub const RELOPT_KIND_GIST: relopt_kind = 1 << 5;
pub const RELOPT_KIND_ATTRIBUTE: relopt_kind = 1 << 6;
pub const RELOPT_KIND_TABLESPACE: relopt_kind = 1 << 7;
pub const RELOPT_KIND_SPGIST: relopt_kind = 1 << 8;
pub const RELOPT_KIND_VIEW: relopt_kind = 1 << 9;
pub const RELOPT_KIND_BRIN: relopt_kind = 1 << 10;
pub const RELOPT_KIND_PARTITIONED: relopt_kind = 1 << 11;
// pgvector hnsw: C add_reloption_kind() at module load; static here.
pub const RELOPT_KIND_HNSW: relopt_kind = 1 << 12;
// contrib/bloom: C add_reloption_kind() in _PG_init; static here.
pub const RELOPT_KIND_BLOOM: relopt_kind = 1 << 13;

pub const HEAP_RELOPT_NAMESPACES: &[&str] = &["toast"];

const HEAP_MIN_FILLFACTOR: i32 = 10;
const HEAP_DEFAULT_FILLFACTOR: i32 = 100;
const BTREE_MIN_FILLFACTOR: i32 = 10;
const BTREE_DEFAULT_FILLFACTOR: i32 = 90;
const HASH_MIN_FILLFACTOR: i32 = 10;
const HASH_DEFAULT_FILLFACTOR: i32 = 75;
const GIST_MIN_FILLFACTOR: i32 = 10;
const GIST_DEFAULT_FILLFACTOR: i32 = 90;
const SPGIST_MIN_FILLFACTOR: i32 = 10;
const SPGIST_DEFAULT_FILLFACTOR: i32 = 80;
const MAX_KILOBYTES: i32 = i32::MAX;
const MAX_IO_CONCURRENCY: i32 = 1000;

const BLCKSZ: usize = 8192;
const fn maxalign(len: usize) -> usize {
    (len + 7) & !7
}
const fn maximum_bytes_per_tuple(tuples_per_page: usize) -> i32 {
    ((BLCKSZ - maxalign(24 + tuples_per_page * 4)) / tuples_per_page & !7) as i32
}
const TOAST_TUPLE_TARGET: i32 = maximum_bytes_per_tuple(4);
const TOAST_TUPLE_TARGET_MAIN: i32 = maximum_bytes_per_tuple(1);
const _: () = assert!(TOAST_TUPLE_TARGET == 2032 && TOAST_TUPLE_TARGET_MAIN == 8160);

use StdRdOptIndexCleanup::*;

const VACUUM_INDEX_CLEANUP_VALUES: &[(&str, i32)] = &[
    ("auto", STDRD_OPTION_VACUUM_INDEX_CLEANUP_AUTO as i32),
    ("on", STDRD_OPTION_VACUUM_INDEX_CLEANUP_ON as i32),
    ("off", STDRD_OPTION_VACUUM_INDEX_CLEANUP_OFF as i32),
    ("true", STDRD_OPTION_VACUUM_INDEX_CLEANUP_ON as i32),
    ("false", STDRD_OPTION_VACUUM_INDEX_CLEANUP_OFF as i32),
    ("yes", STDRD_OPTION_VACUUM_INDEX_CLEANUP_ON as i32),
    ("no", STDRD_OPTION_VACUUM_INDEX_CLEANUP_OFF as i32),
    ("1", STDRD_OPTION_VACUUM_INDEX_CLEANUP_ON as i32),
    ("0", STDRD_OPTION_VACUUM_INDEX_CLEANUP_OFF as i32),
];

const GIST_BUFFERING_VALUES: &[(&str, i32)] = &[
    ("auto", GistOptBufferingMode::GIST_OPTION_BUFFERING_AUTO as i32),
    ("on", GistOptBufferingMode::GIST_OPTION_BUFFERING_ON as i32),
    ("off", GistOptBufferingMode::GIST_OPTION_BUFFERING_OFF as i32),
];

const VIEW_CHECK_OPTION_VALUES: &[(&str, i32)] = &[
    ("local", ViewOptCheckOption::VIEW_OPTION_CHECK_OPTION_LOCAL as i32),
    ("cascaded", ViewOptCheckOption::VIEW_OPTION_CHECK_OPTION_CASCADED as i32),
];

pub enum OptData {
    Bool { default_val: bool },
    Int { default_val: i32, min: i32, max: i32 },
    Real { default_val: f64, min: f64, max: f64 },
    Enum { members: &'static [(&'static str, i32)], default_val: i32, detailmsg: &'static str },
}

pub struct OptDef {
    pub name: &'static str,
    pub kinds: relopt_kind,
    pub lockmode: LOCKMODE,
    pub data: OptData,
}

use OptData::*;

const fn b(name: &'static str, kinds: relopt_kind, lockmode: LOCKMODE, default_val: bool) -> OptDef {
    OptDef { name, kinds, lockmode, data: Bool { default_val } }
}
const fn i(
    name: &'static str,
    kinds: relopt_kind,
    lockmode: LOCKMODE,
    default_val: i32,
    min: i32,
    max: i32,
) -> OptDef {
    OptDef { name, kinds, lockmode, data: Int { default_val, min, max } }
}
const fn r(
    name: &'static str,
    kinds: relopt_kind,
    lockmode: LOCKMODE,
    default_val: f64,
    min: f64,
    max: f64,
) -> OptDef {
    OptDef { name, kinds, lockmode, data: Real { default_val, min, max } }
}

const HT: relopt_kind = RELOPT_KIND_HEAP | RELOPT_KIND_TOAST;
const AEL: LOCKMODE = AccessExclusiveLock;
const SUEL: LOCKMODE = ShareUpdateExclusiveLock;

// boolRelOpts / intRelOpts / realRelOpts / enumRelOpts, in C's table order
// (parse scans in order; keep it for tie-breaking parity).
static RELOPTS: &[OptDef] = &[
    b("autosummarize", RELOPT_KIND_BRIN, AEL, false),
    b("autovacuum_enabled", HT, SUEL, true),
    b("user_catalog_table", RELOPT_KIND_HEAP, AEL, false),
    b("fastupdate", RELOPT_KIND_GIN, AEL, true),
    b("security_barrier", RELOPT_KIND_VIEW, AEL, false),
    b("security_invoker", RELOPT_KIND_VIEW, AEL, false),
    b("vacuum_truncate", HT, SUEL, true),
    b("deduplicate_items", RELOPT_KIND_BTREE, SUEL, true),
    i("fillfactor", RELOPT_KIND_HEAP, SUEL, HEAP_DEFAULT_FILLFACTOR, HEAP_MIN_FILLFACTOR, 100),
    i("fillfactor", RELOPT_KIND_BTREE, SUEL, BTREE_DEFAULT_FILLFACTOR, BTREE_MIN_FILLFACTOR, 100),
    i("fillfactor", RELOPT_KIND_HASH, SUEL, HASH_DEFAULT_FILLFACTOR, HASH_MIN_FILLFACTOR, 100),
    i("fillfactor", RELOPT_KIND_GIST, SUEL, GIST_DEFAULT_FILLFACTOR, GIST_MIN_FILLFACTOR, 100),
    i(
        "fillfactor",
        RELOPT_KIND_SPGIST,
        SUEL,
        SPGIST_DEFAULT_FILLFACTOR,
        SPGIST_MIN_FILLFACTOR,
        100,
    ),
    i("m", RELOPT_KIND_HNSW, AEL, 16, 2, 100),
    i("ef_construction", RELOPT_KIND_HNSW, AEL, 64, 4, 1000),
    // contrib/bloom _PG_init: "length" in bits, then col1..col32 bits-per-key.
    i("length", RELOPT_KIND_BLOOM, AEL, 80, 1, 4096),
    i("col1", RELOPT_KIND_BLOOM, AEL, 2, 1, 4095),
    i("col2", RELOPT_KIND_BLOOM, AEL, 2, 1, 4095),
    i("col3", RELOPT_KIND_BLOOM, AEL, 2, 1, 4095),
    i("col4", RELOPT_KIND_BLOOM, AEL, 2, 1, 4095),
    i("col5", RELOPT_KIND_BLOOM, AEL, 2, 1, 4095),
    i("col6", RELOPT_KIND_BLOOM, AEL, 2, 1, 4095),
    i("col7", RELOPT_KIND_BLOOM, AEL, 2, 1, 4095),
    i("col8", RELOPT_KIND_BLOOM, AEL, 2, 1, 4095),
    i("col9", RELOPT_KIND_BLOOM, AEL, 2, 1, 4095),
    i("col10", RELOPT_KIND_BLOOM, AEL, 2, 1, 4095),
    i("col11", RELOPT_KIND_BLOOM, AEL, 2, 1, 4095),
    i("col12", RELOPT_KIND_BLOOM, AEL, 2, 1, 4095),
    i("col13", RELOPT_KIND_BLOOM, AEL, 2, 1, 4095),
    i("col14", RELOPT_KIND_BLOOM, AEL, 2, 1, 4095),
    i("col15", RELOPT_KIND_BLOOM, AEL, 2, 1, 4095),
    i("col16", RELOPT_KIND_BLOOM, AEL, 2, 1, 4095),
    i("col17", RELOPT_KIND_BLOOM, AEL, 2, 1, 4095),
    i("col18", RELOPT_KIND_BLOOM, AEL, 2, 1, 4095),
    i("col19", RELOPT_KIND_BLOOM, AEL, 2, 1, 4095),
    i("col20", RELOPT_KIND_BLOOM, AEL, 2, 1, 4095),
    i("col21", RELOPT_KIND_BLOOM, AEL, 2, 1, 4095),
    i("col22", RELOPT_KIND_BLOOM, AEL, 2, 1, 4095),
    i("col23", RELOPT_KIND_BLOOM, AEL, 2, 1, 4095),
    i("col24", RELOPT_KIND_BLOOM, AEL, 2, 1, 4095),
    i("col25", RELOPT_KIND_BLOOM, AEL, 2, 1, 4095),
    i("col26", RELOPT_KIND_BLOOM, AEL, 2, 1, 4095),
    i("col27", RELOPT_KIND_BLOOM, AEL, 2, 1, 4095),
    i("col28", RELOPT_KIND_BLOOM, AEL, 2, 1, 4095),
    i("col29", RELOPT_KIND_BLOOM, AEL, 2, 1, 4095),
    i("col30", RELOPT_KIND_BLOOM, AEL, 2, 1, 4095),
    i("col31", RELOPT_KIND_BLOOM, AEL, 2, 1, 4095),
    i("col32", RELOPT_KIND_BLOOM, AEL, 2, 1, 4095),
    i("autovacuum_vacuum_threshold", HT, SUEL, -1, 0, i32::MAX),
    i("autovacuum_vacuum_max_threshold", HT, SUEL, -2, -1, i32::MAX),
    i("autovacuum_vacuum_insert_threshold", HT, SUEL, -2, -1, i32::MAX),
    i("autovacuum_analyze_threshold", RELOPT_KIND_HEAP, SUEL, -1, 0, i32::MAX),
    i("autovacuum_vacuum_cost_limit", HT, SUEL, -1, 1, 10000),
    i("autovacuum_freeze_min_age", HT, SUEL, -1, 0, 1000000000),
    i("autovacuum_multixact_freeze_min_age", HT, SUEL, -1, 0, 1000000000),
    i("autovacuum_freeze_max_age", HT, SUEL, -1, 100000, 2000000000),
    i("autovacuum_multixact_freeze_max_age", HT, SUEL, -1, 10000, 2000000000),
    i("autovacuum_freeze_table_age", HT, SUEL, -1, 0, 2000000000),
    i("autovacuum_multixact_freeze_table_age", HT, SUEL, -1, 0, 2000000000),
    i("log_autovacuum_min_duration", HT, SUEL, -1, -1, i32::MAX),
    i("toast_tuple_target", RELOPT_KIND_HEAP, SUEL, TOAST_TUPLE_TARGET, 128, TOAST_TUPLE_TARGET_MAIN),
    i("pages_per_range", RELOPT_KIND_BRIN, AEL, 128, 1, 131072),
    i("gin_pending_list_limit", RELOPT_KIND_GIN, AEL, -1, 64, MAX_KILOBYTES),
    i("effective_io_concurrency", RELOPT_KIND_TABLESPACE, SUEL, -1, 0, MAX_IO_CONCURRENCY),
    i("maintenance_io_concurrency", RELOPT_KIND_TABLESPACE, SUEL, -1, 0, MAX_IO_CONCURRENCY),
    i("parallel_workers", RELOPT_KIND_HEAP, SUEL, -1, 0, 1024),
    r("autovacuum_vacuum_cost_delay", HT, SUEL, -1.0, 0.0, 100.0),
    r("autovacuum_vacuum_scale_factor", HT, SUEL, -1.0, 0.0, 100.0),
    r("autovacuum_vacuum_insert_scale_factor", HT, SUEL, -1.0, 0.0, 100.0),
    r("autovacuum_analyze_scale_factor", RELOPT_KIND_HEAP, SUEL, -1.0, 0.0, 100.0),
    r("vacuum_max_eager_freeze_failure_rate", HT, SUEL, -1.0, 0.0, 1.0),
    r("seq_page_cost", RELOPT_KIND_TABLESPACE, SUEL, -1.0, 0.0, f64::MAX),
    r("random_page_cost", RELOPT_KIND_TABLESPACE, SUEL, -1.0, 0.0, f64::MAX),
    r("n_distinct", RELOPT_KIND_ATTRIBUTE, SUEL, 0.0, -1.0, f64::MAX),
    r("n_distinct_inherited", RELOPT_KIND_ATTRIBUTE, SUEL, 0.0, -1.0, f64::MAX),
    r("vacuum_cleanup_index_scale_factor", RELOPT_KIND_BTREE, SUEL, -1.0, 0.0, 1e10),
    OptDef {
        name: "vacuum_index_cleanup",
        kinds: HT,
        lockmode: SUEL,
        data: Enum {
            members: VACUUM_INDEX_CLEANUP_VALUES,
            default_val: STDRD_OPTION_VACUUM_INDEX_CLEANUP_AUTO as i32,
            detailmsg: "Valid values are \"on\", \"off\", and \"auto\".",
        },
    },
    OptDef {
        name: "buffering",
        kinds: RELOPT_KIND_GIST,
        lockmode: AEL,
        data: Enum {
            members: GIST_BUFFERING_VALUES,
            default_val: GistOptBufferingMode::GIST_OPTION_BUFFERING_AUTO as i32,
            detailmsg: "Valid values are \"on\", \"off\", and \"auto\".",
        },
    },
    OptDef {
        name: "check_option",
        kinds: RELOPT_KIND_VIEW,
        lockmode: AEL,
        data: Enum {
            members: VIEW_CHECK_OPTION_VALUES,
            default_val: ViewOptCheckOption::VIEW_OPTION_CHECK_OPTION_NOT_SET as i32,
            detailmsg: "Valid values are \"local\" and \"cascaded\".",
        },
    },
];

#[derive(Clone, Copy)]
pub enum OptVal {
    Bool(bool),
    Int(i32),
    Real(f64),
    Enum(i32),
}

pub struct RelOptValue {
    pub def: &'static OptDef,
    pub isset: bool,
    pub val: OptVal,
}

impl RelOptValue {
    fn bool_val(&self) -> bool {
        match (self.isset, &self.val, &self.def.data) {
            (true, OptVal::Bool(v), _) => *v,
            (false, _, Bool { default_val }) => *default_val,
            _ => unreachable!("reloption type confusion for {}", self.def.name),
        }
    }
    fn int_val(&self) -> i32 {
        match (self.isset, &self.val, &self.def.data) {
            (true, OptVal::Int(v), _) => *v,
            (false, _, Int { default_val, .. }) => *default_val,
            _ => unreachable!("reloption type confusion for {}", self.def.name),
        }
    }
    fn real_val(&self) -> f64 {
        match (self.isset, &self.val, &self.def.data) {
            (true, OptVal::Real(v), _) => *v,
            (false, _, Real { default_val, .. }) => *default_val,
            _ => unreachable!("reloption type confusion for {}", self.def.name),
        }
    }
    fn enum_val(&self) -> i32 {
        match (self.isset, &self.val, &self.def.data) {
            (true, OptVal::Enum(v), _) => *v,
            (false, _, Enum { default_val, .. }) => *default_val,
            _ => unreachable!("reloption type confusion for {}", self.def.name),
        }
    }
}

fn text_payload<'a>(image: &'a [u8]) -> &'a [u8] {
    // Array elements are never toasted; only 1B (packed) or 4B headers occur.
    if image[0] & 0x01 != 0 {
        &image[1..(image[0] >> 1) as usize]
    } else {
        let raw = u32::from_ne_bytes(image[0..4].try_into().unwrap()) >> 2;
        &image[4..raw as usize]
    }
}

// SAFETY-boundary helper: turn a text[] datum off a live catalog tuple into a
// bounded 4-byte-header image (DatumGetArrayTypeP over an untoasted column).
pub fn text_array_image<'mcx>(mcx: Mcx<'mcx>, d: Datum) -> PgResult<PgVec<'mcx, u8>> {
    let p = d.as_usize() as *const u8;
    // SAFETY: caller passes a not-null text[] column datum: a live varlena
    // image readable through its varsize_any extent.
    let image =
        unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
    let payload = varlena::open_image(mcx, image)?;
    let body = payload.as_bytes();
    let total = body.len() + 4;
    let mut full: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, total)?;
    mcx::vec_append_bytes(&mut full, &(((total as u32) << 2).to_ne_bytes()))?;
    mcx::vec_append_bytes(&mut full, body)?;
    Ok(full)
}

// DatumGetArrayTypeP's short-header expansion: catalog reads hand back
// 1-byte-header images for small arrays; the array walkers assume 4-byte.
// Returns None when the image is already 4-byte-headed (or absent).
fn expand_short_image<'mcx>(
    mcx: Mcx<'mcx>,
    options: Option<&[u8]>,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    use types_tuple::varatt;
    let Some(img) = options else { return Ok(None) };
    // SAFETY: img starts at a live varlena header.
    if img.is_empty() || !unsafe { varatt::varatt_is_1b(img.as_ptr()) } {
        return Ok(None);
    }
    // SAFETY: 1-byte-header varlena checked above.
    let total = unsafe { varatt::varsize_1b(img.as_ptr()) };
    let payload = &img[varatt::VARHDRSZ_SHORT..total];
    let len = varatt::VARHDRSZ + payload.len();
    let mut out: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, len)?;
    mcx::vec_append_bytes(&mut out, &varatt::set_varsize_4b_word(len as u32).to_ne_bytes())?;
    mcx::vec_append_bytes(&mut out, payload)?;
    Ok(Some(out))
}

fn option_text_strs<'mcx>(mcx: Mcx<'mcx>, options: &[u8]) -> PgResult<PgVec<'mcx, &'mcx str>> {
    let elems = datum::array_build::deconstruct_array_image(mcx, options, -1, false, b'i')?;
    let mut out: PgVec<'mcx, &'mcx str> = mcx::vec_with_capacity_in(mcx, elems.len())?;
    for &e in elems.iter() {
        let p = e.as_usize() as *const u8;
        // SAFETY: element datums point into the options image passed in.
        let img = unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
        let payload = text_payload(img);
        let s = core::str::from_utf8(payload)
            .unwrap_or_else(|_| panic!("non-UTF-8 reloptions text"));
        let bytes = mcx::slice_borrow_in(mcx, s.as_bytes())?;
        // SAFETY: byte-for-byte copy of a &str.
        out.push(unsafe { core::str::from_utf8_unchecked(bytes) });
    }
    Ok(out)
}

pub fn parseRelOptions<'mcx>(
    mcx: Mcx<'mcx>,
    options: Option<&[u8]>,
    validate: bool,
    kind: relopt_kind,
) -> PgResult<PgVec<'mcx, RelOptValue>> {
    let mut values: PgVec<'mcx, RelOptValue> = PgVec::new_in(mcx);
    for def in RELOPTS {
        if def.kinds & kind != 0 {
            values.push(RelOptValue { def, isset: false, val: OptVal::Bool(false) });
        }
    }
    let Some(options) = options else { return Ok(values) };
    let expanded = expand_short_image(mcx, Some(options))?;
    let options = match &expanded {
        Some(v) => &v[..],
        None => options,
    };
    for text_str in option_text_strs(mcx, options)?.iter() {
        let mut found = false;
        for opt in values.iter_mut() {
            let kw = opt.def.name;
            if text_str.len() > kw.len()
                && text_str.as_bytes()[kw.len()] == b'='
                && text_str.as_bytes()[..kw.len()] == *kw.as_bytes()
            {
                parse_one_reloption(opt, &text_str[kw.len() + 1..], validate)?;
                found = true;
                break;
            }
        }
        if !found && validate {
            let name = match text_str.find('=') {
                Some(p) => &text_str[..p],
                None => text_str,
            };
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!("unrecognized parameter \"{name}\""))
                .into_error()
                .into());
        }
    }
    Ok(values)
}

fn parse_one_reloption(option: &mut RelOptValue, value: &str, validate: bool) -> PgResult<()> {
    if option.isset && validate {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!("parameter \"{}\" specified more than once", option.def.name))
            .into_error()
            .into());
    }
    if let Some(v) = parse_opt_value(option.def.name, &option.def.data, value, validate)? {
        option.val = v;
        option.isset = true;
    } else if let Enum { default_val, .. } = &option.def.data {
        option.val = OptVal::Enum(*default_val);
    }
    Ok(())
}

// parse_one_reloption's value leg, shared with the local (opclass) options
// path; Ok(None) = unparsable under !validate.
fn parse_opt_value(
    name: &str,
    data: &OptData,
    value: &str,
    validate: bool,
) -> PgResult<Option<OptVal>> {
    Ok(match data {
        Bool { .. } => match adt_bool::parse_bool(value) {
            Some(v) => Some(OptVal::Bool(v)),
            None => {
                if validate {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                        .errmsg(format!("invalid value for boolean option \"{name}\": {value}"))
                        .into_error()
                        .into());
                }
                None
            }
        },
        Int { min, max, .. } => match guc::units::parse_int(value, 0) {
            guc::units::ParseNum::Ok(v) => {
                if validate && (v < *min || v > *max) {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                        .errmsg(format!("value {value} out of bounds for option \"{name}\""))
                        .errdetail(format!(
                            "Valid values are between \"{min}\" and \"{max}\"."
                        ))
                        .into_error()
                        .into());
                }
                Some(OptVal::Int(v))
            }
            guc::units::ParseNum::Err { .. } => {
                if validate {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                        .errmsg(format!("invalid value for integer option \"{name}\": {value}"))
                        .into_error()
                        .into());
                }
                None
            }
        },
        Real { min, max, .. } => match guc::units::parse_real(value, 0) {
            guc::units::ParseNum::Ok(v) => {
                if validate && (v < *min || v > *max) {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                        .errmsg(format!("value {value} out of bounds for option \"{name}\""))
                        .errdetail(format!(
                            "Valid values are between \"{min:.6}\" and \"{max:.6}\"."
                        ))
                        .into_error()
                        .into());
                }
                Some(OptVal::Real(v))
            }
            guc::units::ParseNum::Err { .. } => {
                if validate {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                        .errmsg(format!(
                            "invalid value for floating point option \"{name}\": {value}"
                        ))
                        .into_error()
                        .into());
                }
                None
            }
        },
        Enum { members, detailmsg, .. } => {
            match members.iter().find(|(s, _)| s.eq_ignore_ascii_case(value)) {
                Some((_, sym)) => Some(OptVal::Enum(*sym)),
                None => {
                    if validate {
                        return Err(ereport(ERROR)
                            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                            .errmsg(format!("invalid value for enum option \"{name}\": {value}"))
                            .errdetail(detailmsg.to_string())
                            .into_error()
                            .into());
                    }
                    None
                }
            }
        }
    })
}

pub fn transformRelOptions<'mcx>(
    mcx: Mcx<'mcx>,
    old_options: Option<&[u8]>,
    def_list: &NodeList<'_>,
    namspace: Option<&str>,
    validnsps: &[&str],
    accept_oids_off: bool,
    is_reset: bool,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let expanded = expand_short_image(mcx, old_options)?;
    let old_options = match &expanded {
        Some(v) => Some(&v[..]),
        None => old_options,
    };
    if def_list.is_nil() {
        return match old_options {
            Some(old) => {
                let mut copy: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, old.len())?;
                mcx::vec_append_bytes(&mut copy, old)?;
                Ok(Some(copy))
            }
            None => Ok(None),
        };
    }

    let mut texts: PgVec<'mcx, Datum> = PgVec::new_in(mcx);

    let same_namespace = |def: &DefElem<'_>| -> bool {
        match (namspace, def.defnamespace) {
            (None, None) => true,
            (Some(ns), Some(dns)) => ns == dns,
            _ => false,
        }
    };

    if let Some(old) = old_options {
        for text_str in option_text_strs(mcx, old)?.iter() {
            let mut replaced = false;
            for dnode in def_list.iter() {
                let def = dnode.as_def_elem().expect("DefElem");
                if !same_namespace(def) {
                    continue;
                }
                let kw = def.defname.expect("DefElem.defname");
                if text_str.len() > kw.len()
                    && text_str.as_bytes()[kw.len()] == b'='
                    && text_str.as_bytes()[..kw.len()] == *kw.as_bytes()
                {
                    replaced = true;
                    break;
                }
            }
            if !replaced {
                texts.push(make_text_datum(mcx, text_str)?);
            }
        }
    }

    for dnode in def_list.iter() {
        let def = dnode.as_def_elem().expect("DefElem");
        if is_reset {
            if def.arg.is_some() {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg("RESET must not include values for parameters".to_string())
                    .into_error()
                    .into());
            }
        } else {
            if let Some(dns) = def.defnamespace {
                if !validnsps.contains(&dns) {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                        .errmsg(format!("unrecognized parameter namespace \"{dns}\""))
                        .into_error()
                        .into());
                }
            }
            if !same_namespace(def) {
                continue;
            }
            let name = def.defname.expect("DefElem.defname");
            let value = match def.arg {
                Some(_) => define::defGetString(mcx, def)?,
                None => "true",
            };
            if name.contains('=') {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg(format!("invalid option name \"{name}\": must not contain \"=\""))
                    .into_error()
                    .into());
            }
            if accept_oids_off && def.defnamespace.is_none() && name == "oids" {
                if define::defGetBoolean(def)? {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                        .errmsg("tables declared WITH OIDS are not supported".to_string())
                        .into_error()
                        .into());
                }
                continue;
            }
            let mut s = PgString::new_in(mcx);
            write!(s, "{name}={value}").expect("PgString write");
            texts.push(make_text_datum(mcx, s.as_str())?);
        }
    }

    if texts.is_empty() {
        return Ok(None);
    }
    Ok(Some(datum::array_build::construct_array_image(mcx, &texts, TEXTOID, -1, false, b'i')?))
}

fn make_text_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum> {
    let len = 4 + s.len();
    let mut image: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, len)?;
    mcx::vec_append_bytes(&mut image, &(((len as u32) << 2).to_ne_bytes()))?;
    mcx::vec_append_bytes(&mut image, s.as_bytes())?;
    Ok(Datum::from_usize(image.leak().as_ptr() as usize))
}

fn build_std<'mcx>(
    mcx: Mcx<'mcx>,
    options: Option<&[u8]>,
    validate: bool,
    kind: relopt_kind,
) -> PgResult<Option<StdRdOptions>> {
    let values = parseRelOptions(mcx, options, validate, kind)?;
    if values.is_empty() {
        return Ok(None);
    }
    let mut out = StdRdOptions {
        fillfactor: HEAP_DEFAULT_FILLFACTOR,
        toast_tuple_target: TOAST_TUPLE_TARGET,
        autovacuum: AutoVacOpts {
            enabled: true,
            vacuum_threshold: -1,
            vacuum_max_threshold: -2,
            vacuum_ins_threshold: -2,
            analyze_threshold: -1,
            vacuum_cost_limit: -1,
            freeze_min_age: -1,
            freeze_max_age: -1,
            freeze_table_age: -1,
            multixact_freeze_min_age: -1,
            multixact_freeze_max_age: -1,
            multixact_freeze_table_age: -1,
            log_min_duration: -1,
            vacuum_cost_delay: -1.0,
            vacuum_scale_factor: -1.0,
            vacuum_ins_scale_factor: -1.0,
            analyze_scale_factor: -1.0,
        },
        user_catalog_table: false,
        parallel_workers: -1,
        vacuum_index_cleanup: STDRD_OPTION_VACUUM_INDEX_CLEANUP_AUTO,
        vacuum_truncate: true,
        vacuum_truncate_set: false,
        vacuum_max_eager_freeze_failure_rate: -1.0,
    };
    for v in values.iter() {
        let av = &mut out.autovacuum;
        match v.def.name {
            "fillfactor" => out.fillfactor = v.int_val(),
            "autovacuum_enabled" => av.enabled = v.bool_val(),
            "autovacuum_vacuum_threshold" => av.vacuum_threshold = v.int_val(),
            "autovacuum_vacuum_max_threshold" => av.vacuum_max_threshold = v.int_val(),
            "autovacuum_vacuum_insert_threshold" => av.vacuum_ins_threshold = v.int_val(),
            "autovacuum_analyze_threshold" => av.analyze_threshold = v.int_val(),
            "autovacuum_vacuum_cost_limit" => av.vacuum_cost_limit = v.int_val(),
            "autovacuum_freeze_min_age" => av.freeze_min_age = v.int_val(),
            "autovacuum_freeze_max_age" => av.freeze_max_age = v.int_val(),
            "autovacuum_freeze_table_age" => av.freeze_table_age = v.int_val(),
            "autovacuum_multixact_freeze_min_age" => av.multixact_freeze_min_age = v.int_val(),
            "autovacuum_multixact_freeze_max_age" => av.multixact_freeze_max_age = v.int_val(),
            "autovacuum_multixact_freeze_table_age" => {
                av.multixact_freeze_table_age = v.int_val()
            }
            "log_autovacuum_min_duration" => av.log_min_duration = v.int_val(),
            "toast_tuple_target" => out.toast_tuple_target = v.int_val(),
            "autovacuum_vacuum_cost_delay" => av.vacuum_cost_delay = v.real_val(),
            "autovacuum_vacuum_scale_factor" => av.vacuum_scale_factor = v.real_val(),
            "autovacuum_vacuum_insert_scale_factor" => {
                av.vacuum_ins_scale_factor = v.real_val()
            }
            "autovacuum_analyze_scale_factor" => av.analyze_scale_factor = v.real_val(),
            "user_catalog_table" => out.user_catalog_table = v.bool_val(),
            "parallel_workers" => out.parallel_workers = v.int_val(),
            "vacuum_index_cleanup" => {
                out.vacuum_index_cleanup = match v.enum_val() {
                    0 => STDRD_OPTION_VACUUM_INDEX_CLEANUP_AUTO,
                    1 => STDRD_OPTION_VACUUM_INDEX_CLEANUP_OFF,
                    2 => STDRD_OPTION_VACUUM_INDEX_CLEANUP_ON,
                    other => unreachable!("vacuum_index_cleanup enum value {other}"),
                }
            }
            "vacuum_truncate" => {
                out.vacuum_truncate = v.bool_val();
                out.vacuum_truncate_set = v.isset;
            }
            "vacuum_max_eager_freeze_failure_rate" => {
                out.vacuum_max_eager_freeze_failure_rate = v.real_val()
            }
            other => {
                if validate {
                    panic!("reloption \"{other}\" not found in parse table");
                }
            }
        }
    }
    Ok(Some(out))
}

pub fn default_reloptions<'mcx>(
    mcx: Mcx<'mcx>,
    options: Option<&[u8]>,
    validate: bool,
    kind: relopt_kind,
) -> PgResult<Option<StdRdOptions>> {
    build_std(mcx, options, validate, kind)
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct AttributeOpts {
    pub n_distinct: f64,
    pub n_distinct_inherited: f64,
}

pub fn attribute_reloptions<'mcx>(
    mcx: Mcx<'mcx>,
    options: Option<&[u8]>,
    validate: bool,
) -> PgResult<Option<AttributeOpts>> {
    let values = parseRelOptions(mcx, options, validate, RELOPT_KIND_ATTRIBUTE)?;
    if values.is_empty() {
        return Ok(None);
    }
    let mut out = AttributeOpts { n_distinct: 0.0, n_distinct_inherited: 0.0 };
    for v in values.iter() {
        match v.def.name {
            "n_distinct" => out.n_distinct = v.real_val(),
            "n_distinct_inherited" => out.n_distinct_inherited = v.real_val(),
            other => {
                if validate {
                    panic!("reloption \"{other}\" not found in parse table");
                }
            }
        }
    }
    Ok(Some(out))
}

pub fn heap_reloptions<'mcx>(
    mcx: Mcx<'mcx>,
    relkind: u8,
    options: Option<&[u8]>,
    validate: bool,
) -> PgResult<Option<StdRdOptions>> {
    match relkind {
        RELKIND_TOASTVALUE => {
            let mut rdopts = build_std(mcx, options, validate, RELOPT_KIND_TOAST)?;
            if let Some(o) = rdopts.as_mut() {
                o.fillfactor = 100;
                o.autovacuum.analyze_threshold = -1;
                o.autovacuum.analyze_scale_factor = -1.0;
            }
            Ok(rdopts)
        }
        RELKIND_RELATION | RELKIND_MATVIEW => build_std(mcx, options, validate, RELOPT_KIND_HEAP),
        // C DefineRelation dispatches RELKIND_VIEW to view_reloptions before
        // reaching heap_reloptions; this repo's DefineRelation routes all
        // non-partitioned relkinds here, so validate views in place.
        RELKIND_VIEW => {
            view_reloptions(mcx, options, validate)?;
            Ok(None)
        }
        _ => Ok(None),
    }
}

// Is relam the pgrcolumnar table AM? The tableam_vocab registry only fills when
// a pgrcolumnar relation is first built into the relcache, so fall back to the
// pg_am.amname probe (the same identity rule relcache build uses) — this
// runs only for non-heap relam values with reloptions present, so the
// syscache probe is off every hot path.
pub fn relam_is_pgrcolumnar(relam: Oid) -> bool {
    const HEAP_TABLE_AM_OID: Oid = 2; // pg_am.dat (relcache build carries it too)
    relam != ::types_core::InvalidOid
        && relam != HEAP_TABLE_AM_OID
        && (tableam_vocab::is_pgrcolumnar_am_oid(relam)
            || matches!(
                syscache_seams::pg_am_amname::call(relam),
                Ok(Some(ref n)) if n == "cbstore"
            ))
}

// pgrcolumnar AM storage options (CREATE TABLE ... USING cbstore WITH (...)).
// A closed hand-rolled parse table (no C counterpart — pgrcolumnar is native):
// cluster_key='col,...' (sort-on-ingest key), codec=auto|lz4|zstd|plain,
// zstd_level=1..22, codec_cols='col=codec,...' (per-column overrides).
// Column-name resolution happens at writer open, not here (parse-time has no
// tupdesc); unknown/invalid values error under validate, are skipped else.
pub fn pgrcolumnar_reloptions<'mcx>(
    mcx: Mcx<'mcx>,
    options: Option<&[u8]>,
    validate: bool,
) -> PgResult<Option<::types_rel::PgrcolumnarOptions>> {
    use ::types_rel::{PgrcolumnarCodec, PgrcolumnarOptions};
    let Some(options) = options else { return Ok(None) };
    let expanded = expand_short_image(mcx, Some(options))?;
    let options = match &expanded {
        Some(v) => &v[..],
        None => options,
    };
    let bad = |msg: String| -> Box<::types_error::PgError> {
        ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(msg)
            .into_error()
            .into()
    };
    let mut out = PgrcolumnarOptions::default();
    for text_str in option_text_strs(mcx, options)?.iter() {
        let (name, value) = match text_str.find('=') {
            Some(p) => (&text_str[..p], &text_str[p + 1..]),
            None => (&text_str[..], ""),
        };
        match name {
            "cluster_key" => {
                if !out.set_cluster_key(value) && validate {
                    return Err(bad(format!("value for \"cluster_key\" is too long")));
                }
            }
            "codec_cols" => {
                if !out.set_codec_cols(value) && validate {
                    return Err(bad(format!("value for \"codec_cols\" is too long")));
                }
            }
            "codec" => match value {
                v if v.eq_ignore_ascii_case("auto") => out.codec = PgrcolumnarCodec::Auto,
                v if v.eq_ignore_ascii_case("lz4") => out.codec = PgrcolumnarCodec::Lz4,
                v if v.eq_ignore_ascii_case("zstd") => out.codec = PgrcolumnarCodec::Zstd,
                v if v.eq_ignore_ascii_case("plain") => out.codec = PgrcolumnarCodec::Plain,
                other => {
                    if validate {
                        return Err(ereport(ERROR)
                            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                            .errmsg(format!("invalid value for enum option \"codec\": {other}"))
                            .errdetail(
                                "Valid values are \"auto\", \"lz4\", \"zstd\", and \"plain\"."
                                    .to_string(),
                            )
                            .into_error()
                            .into());
                    }
                }
            },
            "zstd_level" => match guc::units::parse_int(value, 0) {
                guc::units::ParseNum::Ok(v) if (1..=22).contains(&v) => out.zstd_level = v,
                _ => {
                    if validate {
                        return Err(bad(format!(
                            "invalid value for integer option \"zstd_level\": {value} (valid: 1..22)"
                        )));
                    }
                }
            },
            // Same name/range as the heap reloption (RELOPTS: 0..1024); the
            // AlterTableGetRelOptionsLockLevel name lookup already grants it
            // ShareUpdateExclusiveLock from the heap row.
            "parallel_workers" => match guc::units::parse_int(value, 0) {
                guc::units::ParseNum::Ok(v) if (0..=1024).contains(&v) => {
                    out.parallel_workers = v
                }
                _ => {
                    if validate {
                        return Err(bad(format!(
                            "invalid value for integer option \"parallel_workers\": {value} (valid: 0..1024)"
                        )));
                    }
                }
            },
            other => {
                if validate {
                    return Err(bad(format!("unrecognized parameter \"{other}\"")));
                }
            }
        }
    }
    Ok(Some(out))
}

pub fn partitioned_table_reloptions(options: Option<&[u8]>, validate: bool) -> PgResult<()> {
    if validate && options.is_some() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg("cannot specify storage parameters for a partitioned table".to_string())
            .errhint("Specify storage parameters for its leaf partitions instead.".to_string())
            .into_error()
            .into());
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TableSpaceOpts {
    pub random_page_cost: f64,
    pub seq_page_cost: f64,
    pub effective_io_concurrency: i32,
    pub maintenance_io_concurrency: i32,
}

pub fn tablespace_reloptions<'mcx>(
    mcx: Mcx<'mcx>,
    options: Option<&[u8]>,
    validate: bool,
) -> PgResult<Option<TableSpaceOpts>> {
    let values = parseRelOptions(mcx, options, validate, RELOPT_KIND_TABLESPACE)?;
    if options.is_none() {
        return Ok(None);
    }
    let mut out = TableSpaceOpts {
        random_page_cost: -1.0,
        seq_page_cost: -1.0,
        effective_io_concurrency: -1,
        maintenance_io_concurrency: -1,
    };
    for v in values.iter() {
        match v.def.name {
            "random_page_cost" => out.random_page_cost = v.real_val(),
            "seq_page_cost" => out.seq_page_cost = v.real_val(),
            "effective_io_concurrency" => out.effective_io_concurrency = v.int_val(),
            "maintenance_io_concurrency" => out.maintenance_io_concurrency = v.int_val(),
            other => {
                if validate {
                    panic!("reloption \"{other}\" not found in parse table");
                }
            }
        }
    }
    Ok(Some(out))
}

pub fn view_reloptions<'mcx>(
    mcx: Mcx<'mcx>,
    options: Option<&[u8]>,
    validate: bool,
) -> PgResult<Option<ViewOptions>> {
    let values = parseRelOptions(mcx, options, validate, RELOPT_KIND_VIEW)?;
    if values.is_empty() {
        return Ok(None);
    }
    let mut out = ViewOptions {
        security_barrier: false,
        security_invoker: false,
        check_option: ViewOptCheckOption::VIEW_OPTION_CHECK_OPTION_NOT_SET,
    };
    for v in values.iter() {
        match v.def.name {
            "security_barrier" => out.security_barrier = v.bool_val(),
            "security_invoker" => out.security_invoker = v.bool_val(),
            "check_option" => {
                out.check_option = match v.enum_val() {
                    0 => ViewOptCheckOption::VIEW_OPTION_CHECK_OPTION_NOT_SET,
                    1 => ViewOptCheckOption::VIEW_OPTION_CHECK_OPTION_LOCAL,
                    2 => ViewOptCheckOption::VIEW_OPTION_CHECK_OPTION_CASCADED,
                    other => unreachable!("check_option enum value {other}"),
                }
            }
            other => {
                if validate {
                    panic!("reloption \"{other}\" not found in parse table");
                }
            }
        }
    }
    Ok(Some(out))
}

pub fn index_reloptions<'mcx>(
    mcx: Mcx<'mcx>,
    relam: Oid,
    options: Option<&[u8]>,
    validate: bool,
) -> PgResult<Option<RdOptions>> {
    if options.is_none() {
        return Ok(None);
    }
    // amoptions comes off the handler's IndexAmRoutine; a non-builtin AM
    // (CREATE ACCESS METHOD over a builtin handler) uses its handler's arm.
    match canonical_index_am(relam) {
        BTREE_AM_OID => btoptions(mcx, options, validate),
        HASH_AM_OID => hashoptions(mcx, options, validate),
        GIN_AM_OID => ginoptions(mcx, options, validate),
        GIST_AM_OID => gistoptions(mcx, options, validate),
        SPGIST_AM_OID => spgoptions(mcx, options, validate),
        BRIN_AM_OID => brinoptions(mcx, options, validate),
        other => match extension_am_handler_symbol(other).as_deref() {
            Some("hnswhandler") => hnswoptions(mcx, options, validate),
            Some("blhandler") => bloomoptions(mcx, options, validate),
            _ => panic!("index_reloptions: no amoptions for access method {other}"),
        },
    }
}

// Extension AM (dynamic pg_am row): identify by the handler proc's C symbol.
fn extension_am_handler_symbol(amoid: Oid) -> Option<String> {
    let handler = syscache_seams::pg_am_amhandler::call(amoid).ok()??;
    let name = syscache_seams::pg_proc_proname::call(handler).ok()??;
    Some(String::from_utf8_lossy(name.name_str()).into_owned())
}

// pg_am.amhandler -> the handler's builtin AM (amapi.c GetIndexAmRoutine).
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

fn btoptions<'mcx>(
    mcx: Mcx<'mcx>,
    options: Option<&[u8]>,
    validate: bool,
) -> PgResult<Option<RdOptions>> {
    let values = parseRelOptions(mcx, options, validate, RELOPT_KIND_BTREE)?;
    if values.is_empty() {
        return Ok(None);
    }
    let mut out = BTOptions {
        fillfactor: BTREE_DEFAULT_FILLFACTOR,
        vacuum_cleanup_index_scale_factor: -1.0,
        deduplicate_items: true,
    };
    for v in values.iter() {
        match v.def.name {
            "fillfactor" => out.fillfactor = v.int_val(),
            "vacuum_cleanup_index_scale_factor" => {
                out.vacuum_cleanup_index_scale_factor = v.real_val()
            }
            "deduplicate_items" => out.deduplicate_items = v.bool_val(),
            other => {
                if validate {
                    panic!("reloption \"{other}\" not found in parse table");
                }
            }
        }
    }
    Ok(Some(RdOptions::BTree(out)))
}

fn hashoptions<'mcx>(
    mcx: Mcx<'mcx>,
    options: Option<&[u8]>,
    validate: bool,
) -> PgResult<Option<RdOptions>> {
    let values = parseRelOptions(mcx, options, validate, RELOPT_KIND_HASH)?;
    if values.is_empty() {
        return Ok(None);
    }
    let mut out = HashOptions { fillfactor: HASH_DEFAULT_FILLFACTOR };
    for v in values.iter() {
        match v.def.name {
            "fillfactor" => out.fillfactor = v.int_val(),
            other => {
                if validate {
                    panic!("reloption \"{other}\" not found in parse table");
                }
            }
        }
    }
    Ok(Some(RdOptions::Hash(out)))
}

fn ginoptions<'mcx>(
    mcx: Mcx<'mcx>,
    options: Option<&[u8]>,
    validate: bool,
) -> PgResult<Option<RdOptions>> {
    let values = parseRelOptions(mcx, options, validate, RELOPT_KIND_GIN)?;
    if values.is_empty() {
        return Ok(None);
    }
    let mut out = GinOptions { use_fast_update: true, pending_list_cleanup_size: -1 };
    for v in values.iter() {
        match v.def.name {
            "fastupdate" => out.use_fast_update = v.bool_val(),
            "gin_pending_list_limit" => out.pending_list_cleanup_size = v.int_val(),
            other => {
                if validate {
                    panic!("reloption \"{other}\" not found in parse table");
                }
            }
        }
    }
    Ok(Some(RdOptions::Gin(out)))
}

fn gistoptions<'mcx>(
    mcx: Mcx<'mcx>,
    options: Option<&[u8]>,
    validate: bool,
) -> PgResult<Option<RdOptions>> {
    let values = parseRelOptions(mcx, options, validate, RELOPT_KIND_GIST)?;
    if values.is_empty() {
        return Ok(None);
    }
    let mut out = GistOptions {
        fillfactor: GIST_DEFAULT_FILLFACTOR,
        buffering_mode: GistOptBufferingMode::GIST_OPTION_BUFFERING_AUTO,
    };
    for v in values.iter() {
        match v.def.name {
            "fillfactor" => out.fillfactor = v.int_val(),
            "buffering" => {
                out.buffering_mode = match v.enum_val() {
                    0 => GistOptBufferingMode::GIST_OPTION_BUFFERING_AUTO,
                    1 => GistOptBufferingMode::GIST_OPTION_BUFFERING_ON,
                    2 => GistOptBufferingMode::GIST_OPTION_BUFFERING_OFF,
                    other => unreachable!("gist buffering enum value {other}"),
                }
            }
            other => {
                if validate {
                    panic!("reloption \"{other}\" not found in parse table");
                }
            }
        }
    }
    Ok(Some(RdOptions::Gist(out)))
}

fn spgoptions<'mcx>(
    mcx: Mcx<'mcx>,
    options: Option<&[u8]>,
    validate: bool,
) -> PgResult<Option<RdOptions>> {
    let values = parseRelOptions(mcx, options, validate, RELOPT_KIND_SPGIST)?;
    if values.is_empty() {
        return Ok(None);
    }
    let mut out = SpGistOptions { fillfactor: SPGIST_DEFAULT_FILLFACTOR };
    for v in values.iter() {
        match v.def.name {
            "fillfactor" => out.fillfactor = v.int_val(),
            other => {
                if validate {
                    panic!("reloption \"{other}\" not found in parse table");
                }
            }
        }
    }
    Ok(Some(RdOptions::SpGist(out)))
}

fn brinoptions<'mcx>(
    mcx: Mcx<'mcx>,
    options: Option<&[u8]>,
    validate: bool,
) -> PgResult<Option<RdOptions>> {
    let values = parseRelOptions(mcx, options, validate, RELOPT_KIND_BRIN)?;
    if values.is_empty() {
        return Ok(None);
    }
    let mut out = BrinOptions { pages_per_range: 128, autosummarize: false };
    for v in values.iter() {
        match v.def.name {
            "pages_per_range" => out.pages_per_range = v.int_val(),
            "autosummarize" => out.autosummarize = v.bool_val(),
            other => {
                if validate {
                    panic!("reloption \"{other}\" not found in parse table");
                }
            }
        }
    }
    Ok(Some(RdOptions::Brin(out)))
}

// hnswoptions (pgvector hnsw.c).
fn hnswoptions<'mcx>(
    mcx: Mcx<'mcx>,
    options: Option<&[u8]>,
    validate: bool,
) -> PgResult<Option<RdOptions>> {
    let values = parseRelOptions(mcx, options, validate, RELOPT_KIND_HNSW)?;
    if values.is_empty() {
        return Ok(None);
    }
    let mut out = HnswOptions { m: 16, ef_construction: 64 };
    for v in values.iter() {
        match v.def.name {
            "m" => out.m = v.int_val(),
            "ef_construction" => out.ef_construction = v.int_val(),
            other => {
                if validate {
                    panic!("reloption \"{other}\" not found in parse table");
                }
            }
        }
    }
    Ok(Some(RdOptions::Hnsw(out)))
}

// bloptions (contrib/bloom blutils.c): parse, then convert the signature
// length from bits to words, rounding up.
fn bloomoptions<'mcx>(
    mcx: Mcx<'mcx>,
    options: Option<&[u8]>,
    validate: bool,
) -> PgResult<Option<RdOptions>> {
    let values = parseRelOptions(mcx, options, validate, RELOPT_KIND_BLOOM)?;
    if values.is_empty() {
        return Ok(None);
    }
    let mut length_bits: i32 = 80;
    let mut bit_size = [2i32; 32];
    for v in values.iter() {
        match v.def.name {
            "length" => length_bits = v.int_val(),
            name => match name.strip_prefix("col").and_then(|n| n.parse::<usize>().ok()) {
                Some(n) if (1..=32).contains(&n) => bit_size[n - 1] = v.int_val(),
                _ => {
                    if validate {
                        panic!("reloption \"{name}\" not found in parse table");
                    }
                }
            },
        }
    }
    Ok(Some(RdOptions::Bloom(types_rel::reloptions::BloomOptions {
        bloom_length: (length_bits + 15) / 16,
        bit_size,
    })))
}

// extractRelOptions over the already-fetched reloptions datum; the caller
// (relcache) supplies relkind/relam from the pg_class form it decoded.
pub fn extractRelOptions<'mcx>(
    mcx: Mcx<'mcx>,
    relkind: u8,
    relam: Oid,
    options_datum: Option<Datum>,
) -> PgResult<Option<RdOptions>> {
    let Some(d) = options_datum else { return Ok(None) };
    let image = text_array_image(mcx, d)?;
    let options = Some(image.as_slice());
    match relkind {
        RELKIND_RELATION if relam_is_pgrcolumnar(relam) => {
            Ok(pgrcolumnar_reloptions(mcx, options, false)?.map(RdOptions::Pgrcolumnar))
        }
        RELKIND_RELATION | RELKIND_TOASTVALUE | RELKIND_MATVIEW => {
            Ok(heap_reloptions(mcx, relkind, options, false)?.map(RdOptions::Std))
        }
        RELKIND_PARTITIONED_TABLE => {
            partitioned_table_reloptions(options, false)?;
            Ok(None)
        }
        RELKIND_VIEW => Ok(view_reloptions(mcx, options, false)?.map(RdOptions::View)),
        RELKIND_INDEX | RELKIND_PARTITIONED_INDEX => index_reloptions(mcx, relam, options, false),
        _ => Ok(None),
    }
}

pub fn AlterTableGetRelOptionsLockLevel(def_list: &NodeList<'_>) -> LOCKMODE {
    if def_list.is_nil() {
        return AccessExclusiveLock;
    }
    let mut lockmode = NoLock;
    for dnode in def_list.iter() {
        let def = dnode.as_def_elem().expect("DefElem");
        let name = def.defname.unwrap_or("");
        for opt in RELOPTS {
            if opt.name == name && lockmode < opt.lockmode {
                lockmode = opt.lockmode;
            }
        }
    }
    lockmode
}
