//! Shared types, populated incrementally from ../pgrust/src-idiomatic/crates/types
//! as ports need them. Keep the source module structure so later copies land in
//! predictable places. Seam signatures may only use `types`, `std`, and primitives.

pub mod primitive;

pub use primitive::*;
