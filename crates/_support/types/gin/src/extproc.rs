//! Generic GIN opclass support-function call protocol — the owned-model
//! analogue of C's `index_getprocinfo` → `FunctionCallNColl(&ginstate->…Fn, …)`
//! for an ARBITRARY (extension-provided) opclass.
//!
//! ## Why this exists
//!
//! The built-in GIN opclasses (`anyarray_ops` / `tsvector_ops` / `jsonb_ops`)
//! are dispatched by a TYPED, by-OID match in `gin-core-probe` (`dispatch.rs`).
//! That match cannot reach an extension opclass (`gin_trgm_ops`, `btree_gin`,
//! `hstore`, …): those support functions are `prolang => c` rows that
//! `fmgr_info` resolves to a real `FmgrInfo.fn_addr` (the extension's
//! [`::fmgr::PGFunction`] body, registered through the dynamic-loader
//! ported-library registry), but the GIN AM dispatched by `fn_oid` instead of
//! calling `fn_addr`, so an unknown OID bottomed out loudly.
//!
//! The generic path restores C's `FunctionCallNColl(&flinfo, …)`: when the
//! by-OID match misses, the dispatch invokes `flinfo.fn_addr` through a real
//! fmgr frame. The C `internal`-typed out-parameters (`*nentries`,
//! `**nullFlags`, `*searchMode`, `*recheck`, the returned `Datum*` key array)
//! cannot ride the by-word `Datum` lane, so they cross through the fmgr frame's
//! `internal` side-channel ([`FunctionCallInfoBaseData::internal_args`]) as one
//! of the owned protocol structs defined here. The extension's body downcasts
//! the protocol struct out of the frame, reads its typed inputs, and writes its
//! typed outputs back into it — exactly the role C's by-pointer `internal`
//! arguments play.
//!
//! This module is plain data (no `fmgr` / GIN-core dependency), so both the GIN
//! dispatch (`gin-core-probe`) and an extension opclass crate (`pg_trgm`, …) can
//! name the same protocol without a layering cycle.

extern crate alloc;

use alloc::vec::Vec;

use crate::{GinNullCategory, GinTernaryValue};

/// The internal-lane slot index the GIN generic dispatch uses to pass the
/// protocol struct (`fcinfo->args[GIN_EXTPROC_INTERNAL_SLOT]` is the C
/// `internal` out-parameter the support function scribbles through). The value
/// argument (the `text`/`bytea` being indexed/queried) rides the ordinary
/// by-ref lane at slot 0, mirroring C's `PG_GETARG_*(0)`.
pub const GIN_EXTPROC_INTERNAL_SLOT: usize = 1;

/// One extracted GIN key, in the canonical on-disk form the opclass produces.
///
/// C's `extractValue`/`extractQuery` return a `Datum *` whose element type is
/// the opclass `opckeytype`. The two shapes that occur in practice cross here:
/// a pass-by-value 4-byte key (`int4` — pg_trgm's packed trigram, jsonb_path's
/// hash) or a by-reference varlena key (`text`/`bytea`). The dispatch wraps each
/// back into the canonical `::types_tuple::Datum` the GIN core indexes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GinKey {
    /// A pass-by-value 4-byte key word (C `Int32GetDatum`/`UInt32GetDatum`).
    Int4(i32),
    /// A by-reference varlena key — the HEADER-FUL image (`VARHDRSZ` + payload).
    Varlena(Vec<u8>),
}

/// The protocol struct for `extractValue` (C `gin_extract_value_*(value,
/// internal)`): the dispatch supplies the detoasted `value` payload through the
/// by-ref lane and an empty [`GinExtractValueOut`] in the internal lane; the
/// body fills `keys` (and optionally `null_flags`).
#[derive(Default, Debug)]
pub struct GinExtractValueOut {
    /// `*nentries` + the returned `Datum*` — the extracted index keys.
    pub keys: Vec<GinKey>,
    /// `**nullFlags` — per-key null flags (empty == C `NULL`, all non-null).
    pub null_flags: Vec<bool>,
}

/// The protocol struct for `extractQuery` (C `gin_extract_query_*(query,
/// internal, int2, internal, internal, internal, internal)`).
///
/// Inputs: the detoasted `query` payload (by-ref lane) + [`strategy`]. Outputs:
/// the keys, the optional per-key partial-match / null flags, the per-key
/// opclass-private `extra_data`, and `*searchMode`.
#[derive(Debug)]
pub struct GinExtractQueryOut {
    /// `StrategyNumber strategy` (input).
    pub strategy: u16,
    /// `*nentries` + the returned `Datum*` query keys (output).
    pub keys: Vec<GinKey>,
    /// `**nullFlags` — per-key null flags (empty == C `NULL`).
    pub null_flags: Vec<bool>,
    /// `**pmatch` — per-key partial-match flags (empty == C `NULL`).
    pub partial_matches: Vec<bool>,
    /// `**extra_data` — per-key opclass-private blob (`None` == C `NULL`).
    pub extra_data: Vec<Option<Vec<u8>>>,
    /// `*searchMode`.
    pub search_mode: i32,
}

impl GinExtractQueryOut {
    /// Build the input form (strategy set, outputs empty).
    pub fn new(strategy: u16) -> Self {
        GinExtractQueryOut {
            strategy,
            keys: Vec::new(),
            null_flags: Vec::new(),
            partial_matches: Vec::new(),
            extra_data: Vec::new(),
            search_mode: crate::GIN_SEARCH_MODE_DEFAULT,
        }
    }
}

/// The protocol struct for the boolean `consistent`
/// (C `gin_*_consistent(internal check, int2 strategy, <query>, int4 nkeys,
/// internal extra_data, internal recheck, internal queryKeys,
/// internal nullFlags)`).
///
/// Inputs: `check` (one `bool` per user entry), `strategy`, `nkeys`, the
/// per-key `extra_data`, the per-key `query_categories` (the `nullFlags` C
/// reads), and the detoasted `query` payload (by-ref lane). Output: `matched`
/// + `recheck`.
#[derive(Debug)]
pub struct GinConsistentInOut {
    /// `bool *check` (input).
    pub check: Vec<bool>,
    /// `StrategyNumber strategy` (input).
    pub strategy: u16,
    /// `int32 nkeys` (input == `nuserentries`).
    pub nkeys: i32,
    /// `Pointer *extra_data` (input, per key; `None` where C had NULL).
    pub extra_data: Vec<Option<Vec<u8>>>,
    /// `GinNullCategory *queryCategories` — the `bool *nullFlags` C reads (input).
    pub query_categories: Vec<GinNullCategory>,
    /// `*recheck` (output; pre-seeded `true`, mirroring `directBoolConsistentFn`).
    pub recheck: bool,
    /// `PG_RETURN_BOOL(...)` (output).
    pub matched: bool,
}

/// The protocol struct for the ternary `triConsistent` (C
/// `gin_*_triconsistent(internal check, int2 strategy, <query>, int4 nkeys,
/// internal extra_data, internal queryKeys, internal nullFlags)`).
///
/// Same inputs as [`GinConsistentInOut`] but `check` carries the ternary
/// values; the output is the [`GinTernaryValue`] (the `GIN_MAYBE` return IS the
/// recheck signal, so there is no separate `recheck` out-param).
#[derive(Debug)]
pub struct GinTriConsistentInOut {
    /// `GinTernaryValue *check` (input).
    pub check: Vec<GinTernaryValue>,
    /// `StrategyNumber strategy` (input).
    pub strategy: u16,
    /// `int32 nkeys` (input).
    pub nkeys: i32,
    /// `Pointer *extra_data` (input, per key).
    pub extra_data: Vec<Option<Vec<u8>>>,
    /// `GinNullCategory *queryCategories` (input).
    pub query_categories: Vec<GinNullCategory>,
    /// `PG_RETURN_GIN_TERNARY_VALUE(...)` (output; pre-seeded `GIN_MAYBE`).
    pub result: GinTernaryValue,
}
