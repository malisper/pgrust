//! Carrier for the block-reference-table handle (`common/blkreftable.c`).
//!
//! `BlockRefTable` is an incomplete type in `common/blkreftable.h` (its
//! definition is private to `blkreftable.c`): callers only ever hold an opaque
//! `BlockRefTable *`. The owning unit is not ported yet, so the handle is a
//! registry token the owner maps to the live, context-allocated table; the
//! genuine struct is defined when `blkreftable.c` lands.

#![no_std]

/// Opaque handle to a `BlockRefTable` (`BlockRefTable *`). A registry token,
/// not a pointer the consumer dereferences.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct BlockRefTableHandle(pub u64);

/// Opaque handle to a `BlockRefTableReader` (`BlockRefTableReader *`,
/// `common/blkreftable.h` — an incomplete type whose definition is private to
/// `blkreftable.c`): the incremental on-disk reader callers drive via
/// `BlockRefTableReaderNextRelation` / `BlockRefTableReaderGetBlocks` /
/// `DestroyBlockRefTableReader`. A registry token the owner maps to the live
/// reader; the genuine struct is defined when `blkreftable.c` lands.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct BlockRefTableReaderHandle(pub u64);
