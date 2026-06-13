//! Array-accumulation builder vocabulary (`utils/array.h`).
//!
//! `ArrayBuildStateAny` is the polymorphic accumulator that
//! `initArrayResultAny`/`accumArrayResultAny`/`makeArrayResultAny` (arrayfuncs.c)
//! pass between calls. nodeSubplan only threads it opaquely — it never inspects
//! the accumulated element/dimension state. The arrayfuncs unit (the owner)
//! fills the scalar / array sub-states; this module declares them
//! field-for-field against `utils/array.h`.

use alloc::vec::Vec;
use types_core::Oid;

use crate::datum::Datum;

/// `MAXDIM` (array.h:75) — maximum number of array subscripts. Mirrored here so
/// the build-state fixed arrays match the C struct width without depending on
/// the array-ABI crate.
pub const MAXDIM: usize = 6;

/// `ArrayBuildState` (utils/array.h:187) — working state for
/// `accumArrayResult()` and friends (scalar element accumulation).
///
/// In C the `dvalues`/`dnulls` arrays and the struct itself are kept in
/// `mcontext` (a private subcontext or the caller's `rcontext`). The owned
/// model holds the accumulated columns as `Vec`s on the global allocator,
/// carried inside the [`ArrayBuildStateAny`] slot; `MemoryContextDelete` of the
/// private subcontext is modeled by dropping this state. The `int alen` field
/// is implicit in the `Vec` capacity.
#[derive(Debug, Default)]
pub struct ArrayBuildState {
    /// `Datum *dvalues` — array of accumulated Datums.
    pub dvalues: Vec<Datum>,
    /// `bool *dnulls` — array of is-null flags for the Datums.
    pub dnulls: Vec<bool>,
    /// `int nelems` — number of valid entries in the arrays above.
    pub nelems: i32,
    /// `Oid element_type` — data type of the Datums.
    pub element_type: Oid,
    /// `int16 typlen` — needed datatype info.
    pub typlen: i16,
    /// `bool typbyval`.
    pub typbyval: bool,
    /// `char typalign`.
    pub typalign: u8,
    /// `bool private_cxt` — whether a private memory context is used.
    pub private_cxt: bool,
}

/// `ArrayBuildStateArr` (utils/array.h:205) — working state for
/// `accumArrayResultArr()` and friends (sub-array accumulation; the inputs are
/// arrays and the same array type is returned).
///
/// `data` / `nullbitmap` are kept in `mcontext` in C; here they are global-
/// allocator `Vec`s carried inside [`ArrayBuildStateAny`]. The `int abytes` /
/// `int aitems` allocated-length fields are implicit in the `Vec` capacities.
#[derive(Debug, Default)]
pub struct ArrayBuildStateArr {
    /// `char *data` — accumulated element data bytes.
    pub data: Vec<u8>,
    /// `bits8 *nullbitmap` — bitmap of is-null flags, or empty if none.
    pub nullbitmap: Option<Vec<u8>>,
    /// `int nbytes` — number of bytes used so far in `data`.
    pub nbytes: i32,
    /// `int nitems` — total number of elements in the result.
    pub nitems: i32,
    /// `int ndims` — current number of dimensions of the result.
    pub ndims: i32,
    /// `int dims[MAXDIM]`.
    pub dims: [i32; MAXDIM],
    /// `int lbs[MAXDIM]`.
    pub lbs: [i32; MAXDIM],
    /// `Oid array_type` — data type of the arrays.
    pub array_type: Oid,
    /// `Oid element_type` — data type of the array elements.
    pub element_type: Oid,
    /// `bool private_cxt` — whether a private memory context is used.
    pub private_cxt: bool,
}

/// `ArrayBuildStateAny *` (utils/array.h:226) — the polymorphic array
/// accumulator. Exactly one of the two sub-states is populated (the other is
/// `None`), mirroring the C `scalarstate`/`arraystate` discriminated pair.
#[derive(Debug, Default)]
pub struct ArrayBuildStateAny {
    /// `ArrayBuildState *scalarstate` — set for element (scalar) inputs.
    pub scalarstate: Option<ArrayBuildState>,
    /// `ArrayBuildStateArr *arraystate` — set for array (sub-array) inputs.
    pub arraystate: Option<ArrayBuildStateArr>,
}
