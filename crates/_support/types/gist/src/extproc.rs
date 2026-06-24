//! Generic GiST opclass support-function call protocol — the owned-model
//! analogue of C's `index_getprocinfo` → `FunctionCall5Coll(&giststate->…Fn, …)`
//! for an ARBITRARY (extension-provided) opclass, mirroring [`::gin::extproc`].
//!
//! ## Why this exists
//!
//! The built-in GiST opclasses (`box`/`point`/`poly`/`circle`/`range`/`inet`/
//! `tsvector`/`tsquery`) are dispatched by a TYPED, by-OID match in
//! `gist-proc` (`dispatch_*`). That match cannot reach an extension opclass
//! (`gist_trgm_ops`, `btree_gist`, `hstore`, …): those support functions are
//! `prolang => c` rows that `fmgr_info` resolves to a real `FmgrInfo.fn_addr`
//! (the extension's [`::fmgr::PGFunction`] body, registered through the
//! dynamic-loader ported-library registry), but the GiST AM dispatched by
//! `fn_oid` instead of calling `fn_addr`, so an unknown OID bottomed out loudly.
//!
//! The generic path restores C's `FunctionCallNColl(&flinfo, …)`: when the
//! by-OID match misses, the dispatch invokes `flinfo.fn_addr` through a real
//! fmgr frame. The C by-pointer `internal`-typed arguments
//! (`GISTENTRY *entry`, `GistEntryVector *entryvec`, `GIST_SPLITVEC *v`,
//! `bool *recheck`, the `*size`/`*penalty` out-params, and the returned
//! `GISTENTRY *`) cannot ride the by-word `Datum` lane, so they cross through
//! the fmgr frame's `internal` side-channel
//! ([`FunctionCallInfoBaseData::internal_args`]) as one of the owned protocol
//! structs defined here. The extension's body downcasts the protocol struct out
//! of the frame, reads its typed inputs, and writes its typed outputs back into
//! it — exactly the role C's by-pointer `internal` arguments play.
//!
//! ## Key marshaling
//!
//! GiST keys (`entry->key`) are opclass-private values. In the typed dispatch
//! they ride the [`crate::GISTENTRY`] `Datum<'mcx>` lane; here they cross as
//! their raw HEADER-FUL varlena byte image (`Vec<u8>` — what
//! `DatumGetPointer(entry->key)` points at). The body reads its `TRGM`/whatever
//! off the bytes and produces a new key as bytes; the dispatch re-wraps the
//! bytes into the by-reference `Datum<'mcx>` the GiST core indexes. This is the
//! same shape `gin::extproc::GinKey::Varlena` uses, and it keeps this module a
//! plain-data crate (no `fmgr` / GiST-core dependency), so both the GiST
//! dispatch (`gist-proc`) and an extension opclass crate (`pg_trgm`, …) can name
//! the same protocol without a layering cycle.

extern crate alloc;

use alloc::vec::Vec;

/// The internal-lane slot index the GiST generic dispatch uses to pass the
/// protocol struct. For procs that ALSO take a by-ref `text`/varlena query
/// argument (consistent/distance), that query rides the ordinary by-ref lane at
/// slot 1 (C's `PG_GETARG_TEXT_P(1)`), while slot 0 (the C `GISTENTRY *`) is
/// represented entirely by the protocol struct in the internal lane. For the
/// other procs the protocol struct is the sole carrier.
pub const GIST_EXTPROC_INTERNAL_SLOT: usize = 0;

/// One GiST entry as it crosses the internal lane: the opclass-private key (its
/// HEADER-FUL varlena image, or empty when `DatumGetPointer(key) == NULL`), plus
/// the entry's leaf-ness. Mirror of [`crate::GISTENTRY`] minus the typed
/// `Datum`.
#[derive(Clone, Debug, Default)]
pub struct GistEntryImage {
    /// `DatumGetPointer(entry->key)` as a varlena image. Empty when `key_is_null`.
    pub key: Vec<u8>,
    /// `bool` — whether `key` is non-NULL (distinguishes an empty varlena from
    /// a genuinely NULL pointer key).
    pub key_is_null: bool,
    /// `bool GIST_LEAF(entry)` / `entry->leafkey`.
    pub leafkey: bool,
}

impl GistEntryImage {
    /// Build from a (possibly absent) key image.
    pub fn new(key: Option<Vec<u8>>, leafkey: bool) -> Self {
        match key {
            Some(bytes) => GistEntryImage {
                key: bytes,
                key_is_null: false,
                leafkey,
            },
            None => GistEntryImage {
                key: Vec::new(),
                key_is_null: true,
                leafkey,
            },
        }
    }
}

/// Protocol struct for `compress`/`decompress`/`fetch` (C:
/// `FunctionCall1Coll(fn, coll, PointerGetDatum(entry))` returning a
/// `GISTENTRY *`). The body reads `entry` and writes `retval_key` (the new key
/// image) + `retval_leafkey`; if `passthrough` stays `true` the dispatch
/// returns the entry unchanged.
#[derive(Debug)]
pub struct GistEntryInOut {
    /// The input entry.
    pub entry: GistEntryImage,
    /// `true` (default) == return the entry unchanged (C `retval = entry`);
    /// the body sets this `false` and fills `retval_*` when it produces a new
    /// entry.
    pub passthrough: bool,
    /// New key image (when `!passthrough`).
    pub retval_key: Vec<u8>,
    /// New `leafkey` flag (when `!passthrough`).
    pub retval_leafkey: bool,
}

impl GistEntryInOut {
    pub fn new(entry: GistEntryImage) -> Self {
        GistEntryInOut {
            entry,
            passthrough: true,
            retval_key: Vec::new(),
            retval_leafkey: false,
        }
    }
}

/// Protocol struct for `consistent` (C:
/// `FunctionCall5Coll(fn, coll, entry, query, strategy, subtype, &recheck)`).
/// The query rides the by-ref lane (arg 1); everything else crosses here.
#[derive(Debug)]
pub struct GistConsistentInOut {
    /// The index entry (`GISTENTRY *`).
    pub entry: GistEntryImage,
    /// `StrategyNumber strategy` (input).
    pub strategy: u16,
    /// `Oid subtype` (input; usually unused).
    pub subtype: u32,
    /// `*recheck` (output; the body sets it).
    pub recheck: bool,
    /// `PG_RETURN_BOOL(...)` (output).
    pub matched: bool,
}

impl GistConsistentInOut {
    pub fn new(entry: GistEntryImage, strategy: u16, subtype: u32) -> Self {
        GistConsistentInOut {
            entry,
            strategy,
            subtype,
            recheck: false,
            matched: false,
        }
    }
}

/// Protocol struct for `distance` (C:
/// `FunctionCall5Coll(fn, coll, entry, query, strategy, subtype, &recheck)`
/// RETURNS `float8`).
#[derive(Debug)]
pub struct GistDistanceInOut {
    /// The index entry (`GISTENTRY *`).
    pub entry: GistEntryImage,
    /// `StrategyNumber strategy` (input).
    pub strategy: u16,
    /// `Oid subtype` (input).
    pub subtype: u32,
    /// `*recheck` (output).
    pub recheck: bool,
    /// `PG_RETURN_FLOAT8(...)` (output).
    pub distance: f64,
}

impl GistDistanceInOut {
    pub fn new(entry: GistEntryImage, strategy: u16, subtype: u32) -> Self {
        GistDistanceInOut {
            entry,
            strategy,
            subtype,
            recheck: false,
            distance: 0.0,
        }
    }
}

/// Protocol struct for `union` (C:
/// `FunctionCall2Coll(fn, coll, PointerGetDatum(entryvec), &size)`). The body
/// reads `entries` (every member's key image) and writes the union key image.
#[derive(Debug, Default)]
pub struct GistUnionInOut {
    /// `entryvec->vector[0..n]` — each member's key image.
    pub entries: Vec<GistEntryImage>,
    /// The union key (`PG_RETURN_POINTER(result)`); `*size` is its length.
    pub result: Vec<u8>,
}

/// Protocol struct for `same`/`equal` (C:
/// `FunctionCall3Coll(fn, coll, a, b, &result)`). Both keys cross as images.
#[derive(Debug, Default)]
pub struct GistSameInOut {
    /// `a` key image.
    pub a: Vec<u8>,
    /// `b` key image.
    pub b: Vec<u8>,
    /// `*result` (output).
    pub equal: bool,
}

/// Protocol struct for `penalty` (C:
/// `FunctionCall3Coll(fn, coll, origentry, newentry, &penalty)`).
#[derive(Debug, Default)]
pub struct GistPenaltyInOut {
    /// `origentry->key` image (C: always ISSIGNKEY for trgm).
    pub orig_key: Vec<u8>,
    /// `newentry->key` image.
    pub new_key: Vec<u8>,
    /// `*penalty` (output).
    pub penalty: f32,
}

/// Protocol struct for `picksplit` (C:
/// `FunctionCall2Coll(fn, coll, PointerGetDatum(entryvec), &splitvec)`). Index
/// 0 of `entries` is the C placeholder slot (`entryvec->vector[0]`, never read —
/// the bodies index 1-based from `FirstOffsetNumber`); the dispatch keeps it
/// present so the offset numbers the body writes into `spl_left`/`spl_right`
/// match the GiST core's 1-based convention.
#[derive(Debug, Default)]
pub struct GistPicksplitInOut {
    /// `entryvec->vector[0..n]` — each member's key image (index 0 = placeholder).
    pub entries: Vec<GistEntryImage>,
    /// `v->spl_left[0..spl_nleft]` (1-based offset numbers).
    pub spl_left: Vec<u16>,
    /// `v->spl_right[0..spl_nright]` (1-based offset numbers).
    pub spl_right: Vec<u16>,
    /// `v->spl_ldatum` — union key image of the left group.
    pub spl_ldatum: Vec<u8>,
    /// `v->spl_rdatum` — union key image of the right group.
    pub spl_rdatum: Vec<u8>,
}
