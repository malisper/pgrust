//! Port of the `tsquery` core ADT translation units (PostgreSQL 18.3):
//!
//!  * `tsquery.c` — `tsquery` I/O (`tsqueryin`/`tsqueryout`/`tsquerysend`/
//!    `tsqueryrecv`/`tsquerytree`) and the shared `parse_tsquery` parser
//!    machine (tokenizers, operator-precedence `makepol`, `findoprnd`, the
//!    `infix` printer, and the `push*` helpers used by `PushFunction`s);
//!  * `tsquery_op.c` — `tsquery` operations (`numnode`, the `&`/`|`/`<->`/`!`
//!    constructors, the comparison family, `makeTSQuerySign`, and the
//!    `@>`/`<@` "mcontains" operators);
//!  * `tsquery_gist.c` — the GiST `gtsquery` opclass support functions over a
//!    [`TSQuerySign`] bit signature.
//!
//! Memory model: a `tsquery` value is its flat varlena image (`&[u8]` in,
//! `Vec<u8>` out — the `palloc`-into-caller's-context analog), exactly as the
//! sibling `backend-utils-adt-ts-small` crate models it. The `QTNode`
//! expression-tree toolkit (`QT2QTN`/`QTN2QT`/`QTNFree`/`QTNodeCompare`/…) and
//! the cleanup helpers (`clean_NOT`/`cleanup_tsquery_stopwords`) live in that
//! crate and are reused here. Transient working buffers (the parser's operand
//! store, the `OperatorElement` stack, the `INFIX` print buffer, the `QTNode`
//! trees) are charged to a caller-supplied [`mcx::Mcx`].
//!
//! Genuine externals (mirror-and-panic until their owner lands):
//!  * the stateful `tsvector_parser.c` engine (`init`/`reset`/`gettoken`/
//!    `close_tsvector_parser`), via `backend-utils-adt-tsvector-core-seams`;
//!  * the legacy CRC32 of an operand (`common/pg_crc.h`), via
//!    `backend-utils-hash-small-seams`;
//!  * `pg_mblen` / `pg_database_encoding_max_length` (`mbutils.c`), via
//!    `backend-utils-mb-mbutils-seams`;
//!  * `t_isalnum` (`ts_locale.c`), via `backend-tsearch-ts-locale-seams`;
//!  * `check_stack_depth` (`tcop/postgres.c`), via `backend-tcop-postgres-seams`.
//!
//! The fmgr/`Datum` wrappers (`PG_FUNCTION_ARGS`, `DirectFunctionCall*`) and the
//! `cstring`/`text`/`bytea` datum framing are a project-wide deferral: each
//! built-in here takes the already-detoasted `tsquery` image as `&[u8]` and
//! returns the bare result bytes.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]
#![allow(clippy::collapsible_else_if)]
#![allow(clippy::too_many_arguments)]

pub mod gist;
pub mod op;
pub mod tsquery;

/// Install this crate's seams. This unit declares no inward seam crate (no
/// other crate calls back into the `tsquery` core across a cycle — the
/// `QTNode` toolkit it shares lives in `backend-utils-adt-ts-small`), so
/// `init_seams()` is a no-op, present for the uniform `seams-init` call shape.
pub fn init_seams() {}

#[cfg(test)]
mod tests;
