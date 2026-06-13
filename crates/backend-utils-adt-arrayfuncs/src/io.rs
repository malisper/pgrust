//! I/O family: text I/O (`array_in` + the `ReadArray*` recursive-descent
//! parser, `array_out`) and binary I/O (`array_recv` / `array_send` +
//! `ReadArrayBinary`).
//!
//! The element-type I/O functions are reached through the fmgr owner's seams
//! (`input_function_call_safe` / `array_output_function_call` /
//! `array_receive_function_call` / `array_send_function_call`); the element
//! type's storage metadata + I/O func OID come from
//! `get_type_io_data` (lsyscache owner). Detoasting an element value uses the
//! detoast owner's `detoast_attr`.

use mcx::{Mcx, PgVec};
use types_core::Oid;
use types_error::PgResult;

// ---------------------------------------------------------------------------
// Text input: array_in + ReadArray parser (arrayfuncs.c).
// ---------------------------------------------------------------------------

/// `array_in(string, element_type, typmod)` (arrayfuncs.c): parse the external
/// text representation of an array into the on-disk `ArrayType` bytes.
pub fn array_in<'mcx>(
    mcx: Mcx<'mcx>,
    string: &str,
    element_type: Oid,
    typmod: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    todo!("io: array_in")
}

/// `ReadArrayStr`/`ReadArrayDimensions`/`ReadDimensionInt` machinery — the
/// recursive-descent text parser invoked by `array_in`. (Internal; exact
/// shape lands with the implementation.)
pub fn read_array_str<'mcx>(mcx: Mcx<'mcx>) -> PgResult<()> {
    todo!("io: ReadArrayStr parser")
}

// ---------------------------------------------------------------------------
// Text output: array_out (arrayfuncs.c).
// ---------------------------------------------------------------------------

/// `array_out(v)` (arrayfuncs.c): render an array's external text form.
pub fn array_out<'mcx>(mcx: Mcx<'mcx>, array: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    todo!("io: array_out")
}

// ---------------------------------------------------------------------------
// Binary I/O: array_recv / array_send + ReadArrayBinary (arrayfuncs.c).
// ---------------------------------------------------------------------------

/// `array_recv(buf, spec_element_type, typmod)` (arrayfuncs.c): decode the
/// binary wire form of an array into on-disk `ArrayType` bytes.
pub fn array_recv<'mcx>(
    mcx: Mcx<'mcx>,
    buf: &[u8],
    spec_element_type: Oid,
    typmod: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    todo!("io: array_recv")
}

/// `ReadArrayBinary(buf, nitems, ...)` (arrayfuncs.c): the per-element binary
/// reader invoked by `array_recv`. (Internal.)
pub fn read_array_binary<'mcx>(mcx: Mcx<'mcx>) -> PgResult<()> {
    todo!("io: ReadArrayBinary")
}

/// `array_send(v)` (arrayfuncs.c): encode an array into its binary wire form.
pub fn array_send<'mcx>(mcx: Mcx<'mcx>, array: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    todo!("io: array_send")
}
