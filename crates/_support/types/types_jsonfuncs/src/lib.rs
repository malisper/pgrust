//! Signature types for `backend-utils-adt-jsonfuncs` — the in-memory working
//! structs private to `jsonfuncs.c` (the type-IO populate caches, the
//! generalized json/jsonb value carriers `JsValue`/`JsObject`, and the per-call
//! SAX-callback state structs). They never cross a C ABI, so they are idiomatic
//! owned Rust with no raw pointers.
//!
//! The on-disk `jsonb` vocabulary (`Jsonb`/`JsonbContainer`/`JsonbValue`) lives
//! in `types-jsonb`; the lexer/category/SAX vocabulary (`JsonLexContext`,
//! `JsonSemAction`, `JsonTokenType`, `JsonTypeCategory`) in `types-json`. These
//! structs reference those.

#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

extern crate alloc;

use alloc::boxed::Box;
use alloc::collections::BTreeMap;
use alloc::vec::Vec;

use types_core::{InvalidOid, Oid};
use fmgr::resolution::FmgrResolution;
use fmgr::FmgrInfo;
use types_json::{JsonTokenType, JsonTypeCategory};
use types_jsonb::jsonb_util::JsonbValue;
use types_jsonb::jsonb::JsonbContainer;
use types_tuple::heaptuple::TupleDesc;

/// `NAMEDATALEN` — pg name length (the `JsonHashEntry.fname` key width).
pub const NAMEDATALEN: usize = 64;

// ---------------------------------------------------------------------------
// TypeCat (jsonfuncs.c:199) — enumeration type categories.
// ---------------------------------------------------------------------------

/// C: `enum TypeCat` (jsonfuncs.c:199). The discriminant bytes match the C
/// char values exactly (they are stored in the IO cache and switched on).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum TypeCat {
    /// `TYPECAT_SCALAR = 's'`.
    Scalar = b's',
    /// `TYPECAT_ARRAY = 'a'`.
    Array = b'a',
    /// `TYPECAT_COMPOSITE = 'c'`.
    Composite = b'c',
    /// `TYPECAT_COMPOSITE_DOMAIN = 'C'`.
    CompositeDomain = b'C',
    /// `TYPECAT_DOMAIN = 'd'`.
    Domain = b'd',
}

// ---------------------------------------------------------------------------
// Type-IO metadata caches (jsonfuncs.c:155-242). In C these recursively cache
// fmgr/typcache lookups across populate_record calls keyed off fn_extra; the
// faithful owned model holds the same fields. A resolved input function is the
// real fmgr `FmgrInfo` + its `FmgrResolution` (what `fmgr_info` returns).
// ---------------------------------------------------------------------------

/// C: `ResolvedFmgrInfo`-equivalent pair for a cached input function — the
/// `FmgrInfo typiofunc` field of `ScalarIOData` carries this. Held by value;
/// `None` means "not yet looked up" (the C `flinfo->fn_oid == InvalidOid`
/// freshness test in `prepare_column_cache`).
#[derive(Clone)]
pub struct CachedFmgrInfo {
    pub finfo: FmgrInfo,
    pub resolution: FmgrResolution,
}

/// C: `struct ScalarIOData` (jsonfuncs.c:155) — type-IO metadata for
/// `populate_scalar()`.
#[derive(Clone, Default)]
pub struct ScalarIOData {
    /// `Oid typioparam`.
    pub typioparam: Oid,
    /// `FmgrInfo typiofunc` — resolved input function (`None` until resolved).
    pub typiofunc: Option<CachedFmgrInfo>,
}

/// C: `struct ArrayIOData` (jsonfuncs.c:166) — metadata for `populate_array()`.
pub struct ArrayIOData<'mcx> {
    /// `ColumnIOData *element_info`.
    pub element_info: Box<ColumnIOData<'mcx>>,
    /// `Oid element_type`.
    pub element_type: Oid,
    /// `int32 element_typmod`.
    pub element_typmod: i32,
}

/// C: `struct CompositeIOData` (jsonfuncs.c:174) — metadata for
/// `populate_composite()`.
pub struct CompositeIOData<'mcx> {
    /// `RecordIOData *record_io`.
    pub record_io: Option<Box<RecordIOData<'mcx>>>,
    /// `TupleDesc tupdesc`.
    pub tupdesc: TupleDesc<'mcx>,
    /// `Oid base_typid`.
    pub base_typid: Oid,
    /// `int32 base_typmod`.
    pub base_typmod: i32,
    /// `void *domain_info` — opaque cache for domain checks (the C
    /// `domain_check`'s cached `DomainConstraintRef`). Held by the real
    /// owned domain-check cache when domain-over-composite.
    pub domain_info: Option<Box<DomainCheckCache>>,
}

/// C: `struct DomainIOData` (jsonfuncs.c:190) — metadata for `populate_domain()`.
pub struct DomainIOData<'mcx> {
    /// `ColumnIOData *base_io`.
    pub base_io: Box<ColumnIOData<'mcx>>,
    /// `Oid base_typid`.
    pub base_typid: Oid,
    /// `int32 base_typmod`.
    pub base_typmod: i32,
    /// `void *domain_info` — opaque cache for domain checks.
    pub domain_info: Option<Box<DomainCheckCache>>,
}

/// The `ColumnIOData.io` union (jsonfuncs.c:218) — array / composite / domain
/// metadata, selected by `typcat`.
pub enum ColumnIOUnion<'mcx> {
    /// `ArrayIOData array`.
    Array(ArrayIOData<'mcx>),
    /// `CompositeIOData composite`.
    Composite(CompositeIOData<'mcx>),
    /// `DomainIOData domain`.
    Domain(DomainIOData<'mcx>),
    /// No `io` arm populated yet (scalar, or freshly-zeroed cache).
    None,
}

/// C: `struct ColumnIOData` (jsonfuncs.c:211) — record metadata cache for
/// `populate_record_field()`.
pub struct ColumnIOData<'mcx> {
    /// `Oid typid`.
    pub typid: Oid,
    /// `int32 typmod`.
    pub typmod: i32,
    /// `TypeCat typcat`.
    pub typcat: TypeCat,
    /// `ScalarIOData scalar_io`.
    pub scalar_io: ScalarIOData,
    /// `union { ... } io`.
    pub io: ColumnIOUnion<'mcx>,
}

impl Default for ColumnIOData<'_> {
    fn default() -> Self {
        // C: the cache is `palloc0`'d — a zeroed `ColumnIOData` has typid = 0,
        // typcat = 0. `TypeCat` has no zero discriminant; the freshness test in
        // `prepare_column_cache` keys off `typid != column->typid`, so the
        // initial sentinel typcat is never read before being overwritten.
        ColumnIOData {
            typid: InvalidOid,
            typmod: -1,
            typcat: TypeCat::Scalar,
            scalar_io: ScalarIOData::default(),
            io: ColumnIOUnion::None,
        }
    }
}

/// C: `struct RecordIOData` (jsonfuncs.c:228) — metadata cache for
/// `populate_record()`.
pub struct RecordIOData<'mcx> {
    /// `Oid record_type`.
    pub record_type: Oid,
    /// `int32 record_typmod`.
    pub record_typmod: i32,
    /// `int ncolumns`.
    pub ncolumns: i32,
    /// `ColumnIOData columns[FLEXIBLE_ARRAY_MEMBER]`.
    pub columns: Vec<ColumnIOData<'mcx>>,
}

/// C: `struct PopulateRecordCache` (jsonfuncs.c:237) — per-query cache for
/// `populate_record_worker` / `populate_recordset_worker`.
pub struct PopulateRecordCache<'mcx> {
    /// `Oid argtype` — declared type of the record argument.
    pub argtype: Oid,
    /// `ColumnIOData c` — metadata cache for `populate_composite()`.
    pub c: ColumnIOData<'mcx>,
}

/// The owned domain-check cache (`void *domain_info`): the resolved domain
/// constraint set used by `domain_check`. Held opaquely here; the populate
/// machinery threads it back into the `domain_check` call. The repo's
/// `domain_check` owns the resolved-constraint representation, so this carries
/// the cached base-type id and a one-shot "constraints resolved" marker.
#[derive(Clone, Default)]
pub struct DomainCheckCache {
    /// The domain OID the cache was built for (freshness check).
    pub domain_oid: Oid,
}

// ---------------------------------------------------------------------------
// JsValue / JsObject (jsonfuncs.c:293-317) — generalized json/jsonb value
// passing through the populate machinery.
// ---------------------------------------------------------------------------

/// C: `struct JsValue` (jsonfuncs.c:293) — a single json *or* jsonb value the
/// populate recursion processes. The C tagged union over `is_json` becomes a
/// Rust enum.
#[derive(Clone, Debug)]
pub enum JsValue {
    /// The `is_json` arm: `struct { const char *str; int len; JsonTokenType
    /// type; }`. `str = None` is the C NULL pointer (a JSON null / absent
    /// field); `len` is the string length or `-1` if NUL-terminated.
    Json {
        str: Option<Vec<u8>>,
        type_: JsonTokenType,
    },
    /// The jsonb arm: `JsonbValue *jsonb`. `None` is the C NULL pointer.
    Jsonb(Option<Box<JsonbValue>>),
}

/// C: `struct JsObject` (jsonfuncs.c:309) — a json object as either a hash of
/// field bytes (text path) or a jsonb container (binary path).
pub enum JsObject {
    /// `HTAB *json_hash` — the field-name → (value bytes, token type) map the
    /// text path builds with `get_json_object_as_hash`. `None` is the C NULL
    /// (an empty / non-object input).
    JsonHash(Option<BTreeMap<Vec<u8>, JsonHashEntry>>),
    /// `JsonbContainer *jsonb_cont` — the binary object container. `None` is
    /// the C NULL pointer.
    JsonbCont(Option<Box<JsonbContainer>>),
}

/// C: `struct JsonHashEntry` (jsonfuncs.c:147) — a `get_json_object_as_hash`
/// hashtable element. The C key `char fname[NAMEDATALEN]` is the map key; this
/// carries the value payload.
#[derive(Clone, Debug)]
pub struct JsonHashEntry {
    /// `char *val` — the field's json value bytes (`None` is the C NULL,
    /// a JSON null).
    pub val: Option<Vec<u8>>,
    /// `JsonTokenType type`.
    pub type_: JsonTokenType,
}

// ---------------------------------------------------------------------------
// Result of json_categorize_type for the inward seam (matches the
// jsonfuncs-seams::categorize_type return). Re-exported for callers.
// ---------------------------------------------------------------------------

/// The `(JsonTypeCategory, outfuncoid)` pair `json_categorize_type` yields.
pub type CategorizeResult = (JsonTypeCategory, Oid);
