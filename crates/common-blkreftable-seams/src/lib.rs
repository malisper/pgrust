//! Seam declarations for the block-reference-table builder
//! (`common/blkreftable.c`).
//!
//! This unit no longer exports any outward seams: its functions are pure,
//! single-owner leaf code (`common-blkreftable`), so the (few) consumers call
//! them directly rather than through a function-pointer seam. The crate is kept
//! as an empty placeholder to preserve the workspace layout.

extern crate alloc;
