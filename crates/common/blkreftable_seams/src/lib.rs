//! Seam declarations for the block-reference-table builder
//! (`common/blkreftable.c`).
//!
//! These outward seams were removed as faithful de-indirection: the owning unit
//! `common-blkreftable` is a clean single-owner leaf (no consumer is in its
//! dependency closure), so its `BlockRefTable*` / reader routines are now called
//! directly by their consumers (backup-incremental, walsummaryfuncs,
//! walsummarizer) instead of through a fn-ptr seam. A direct call replaces the
//! seam call — behavior is identical. The crate is retained as an empty shell so
//! existing workspace/dependency wiring stays valid.
