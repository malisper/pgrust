//! Port of the "small" `tsquery` ADT translation units (PostgreSQL 18.3):
//!
//!  * `tsquery_util.c` — the `QTNode` expression-tree toolkit every `tsquery`
//!    transformation builds on (`QT2QTN` / `QTN2QT` / `QTNSort` / `QTNTernary` /
//!    `QTNBinary` / `QTNodeCompare` / `QTNEq` / `QTNClearFlags` / `QTNCopy`);
//!  * `tsquery_cleanup.c` — `clean_NOT` (drop every `!` subtree) and
//!    `cleanup_tsquery_stopwords` (remove `QI_VALSTOP` stopword nodes, fixing
//!    adjacent `OP_PHRASE` distances);
//!  * `tsquery_rewrite.c` — the `ts_rewrite` family (substitute a sub-query for
//!    every occurrence of a target sub-query).
//!
//! Memory model: the C code hand-manages `QTNode *` / `NODE *` trees with
//! `palloc`/`pfree` and the `QTN_NEEDFREE`/`QTN_WORDFREE` freeing hints. Here
//! the working trees and the transient codec buffers are charged to a
//! caller-supplied [`Mcx`] (`PgVec`/`PgBox`), which release their charge on
//! drop, so the `pfree`/`QTNFree` plumbing dissolves into ownership and the
//! `QTN_NEEDFREE`/`QTN_WORDFREE` hints are inert. The one flag that drives
//! control flow, [`QTN_NOCHANGE`], is preserved. The OWNED result datum is a
//! plain `Vec<u8>` (the `palloc`-into-caller's-context analog), deliberately
//! uncharged.
//!
//! Genuine externals: `check_stack_depth` / `CHECK_FOR_INTERRUPTS` (owned by
//! `tcop/postgres.c`, via `backend-tcop-postgres-seams`); the
//! `ts_rewrite(query, text)` SPI execution (via this unit's own seam crate,
//! installed by the SPI owner). The fmgr `Datum` wrappers and `gettext _()`
//! are project-wide deferrals; the cores take fully-detoasted `tsquery` datums
//! as `&[u8]` and return `Vec<u8>`.

#![no_std]
#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]
#![allow(clippy::collapsible_else_if)]

extern crate alloc;

pub mod cleanup;
pub mod rewrite;
pub mod util;

/// Install this crate's seams. This crate provides no seams of its own — its
/// inward seam crate (`backend-utils-adt-ts-small-seams`) is installed by the
/// SPI owner when it lands — so `init_seams()` is a no-op, present for the
/// uniform `seams-init` call shape.
pub fn init_seams() {}

#[cfg(test)]
extern crate std;

#[cfg(test)]
mod tests;
