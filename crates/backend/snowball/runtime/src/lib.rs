//! The Snowball **libstemmer runtime** plus every generated per-language
//! stemmer module shipped with PostgreSQL 18.3
//! (`src/backend/snowball/libstemmer/`).
//!
//! This crate has two layers:
//!
//!  * The hand-ported runtime substrate every stemmer links against —
//!    `api.c` ([`api`]), `utilities.c` ([`utilities`]), the runtime header
//!    structs `symbol`/`SN_env`/`among` ([`types`]) — a 1:1 faithful
//!    translation that preserves the byte-for-byte state-machine behaviour and
//!    the hidden `[capacity, length]` buffer header.
//!  * The generated stemmer modules ([`stemmers`]), a c2rust translation of the
//!    Snowball-compiler output (`stem_*.c`), adapted to call the runtime above.
//!    Each module exposes `<lang>_<enc>_create_env` / `_close_env` / `_stem`.
//!
//! [`STEMMER_MODULES`] is the dispatch table `dict_snowball.c` walks to locate a
//! stemmer by language + database encoding.
//!
//! ## Memory model
//!
//! A Snowball working buffer is a raw `symbol*` whose two `int`s immediately
//! *before* the returned pointer hold `[capacity, length]` (the `HEAD` header,
//! see [`utilities::HEAD`]). The runtime reads and writes those words via
//! negative offsets, so the buffer must stay a raw pointer with a stable
//! address; it cannot be re-modelled as an owned `Vec`/`Box` without breaking
//! both the byte-for-byte algorithm and the ABI the stemmers compile against.
//!
//! PostgreSQL builds Snowball with `malloc`/`calloc`/`realloc`/`free` redefined
//! to `palloc`/`palloc0`/`repalloc`/`pfree` (`src/include/snowball/header.h`)
//! so the buffers live in a backend memory context. The four allocation
//! primitives are therefore the crate's one external dependency; they are
//! routed through the in-crate [`mem`] seam, which **loud-panics until a host
//! installs a provider** (it never fabricates an allocation). The host must
//! expose real `*mut` addresses, which the handle-based workspace allocator
//! cannot yet do — so the runtime is reachable only once a raw-pointer palloc
//! provider is installed, exactly as the seam contract requires.

#![no_std]
// C-faithful Snowball identifiers (`SN_env.S`/`.I`, `in_grouping_U`, …) keep
// their original casing to mirror the runtime headers and the generated stemmers.
#![allow(non_snake_case)]

pub mod api;
pub mod mem;
pub mod stemmers;
pub mod types;
pub mod utilities;

mod modules;

// Re-export the C-faithful public surface at the crate root so callers (the
// generated stemmers, `dict_snowball.c`) can refer to the runtime symbols by
// their original names.
pub use api::{SN_close_env, SN_create_env, SN_set_current};
pub use modules::{StemmerModule, STEMMER_MODULES, PG_KOI8R, PG_LATIN1, PG_LATIN2, PG_SQL_ASCII, PG_UTF8};
pub use types::{among, symbol, SN_env};
pub use utilities::{
    assign_to, eq_s, eq_s_b, eq_v, eq_v_b, find_among, find_among_b, in_grouping, in_grouping_U,
    in_grouping_b, in_grouping_b_U, insert_s, insert_v, len_utf8, out_grouping, out_grouping_U,
    out_grouping_b, out_grouping_b_U, replace_s, skip_b_utf8, skip_utf8, slice_del, slice_from_s,
    slice_from_v, slice_to, CREATE_SIZE, HEAD,
};
