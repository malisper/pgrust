//! `backend-utils-adt-jsonfuncs` — port of `src/backend/utils/adt/jsonfuncs.c`
//! (PostgreSQL 18.3): the SQL-callable JSON/JSONB function surface.
//!
//! Field/element/path extraction; object-keys; array length/elements; `each`;
//! `populate_record(set)` / `to_record(set)`; `strip_nulls`; the jsonb mutation
//! operators (`concat`/`delete`/`set`/`insert`/`delete_path`); the GIN value
//! iteration (`iterate_*`); the string-value transform machinery
//! (`transform_*`); and the `json_categorize_type` classifier (json's cycle
//! partner — this crate owns and installs the `backend-utils-adt-jsonfuncs-seams`
//! inward seams).
//!
//! Module layout mirrors the C clusters (see each module's header). The jsonb
//! (binary) paths call the landed `jsonb_util` value API directly. The json
//! (text) paths drive the recursive-descent parser through the
//! `common-jsonapi-seams::pg_parse_json` SAX-driver seam over a real
//! [`types_json::JsonSemAction`] callback table; the parser is `common/jsonapi.c`
//! (unported), so those paths panic loudly until it lands — the parse driver is
//! the only seamed gap; every callback and worker is real in-crate logic.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::type_complexity)]
#![allow(clippy::too_many_arguments)]

extern crate alloc;

pub mod categorize;
pub mod common;
pub mod each;
pub mod elements;
pub mod getfield;
pub mod iterate;
pub mod json_render;
pub mod keys;
pub mod length;
pub mod lex;
pub mod populate;
pub mod recordset;
pub mod seams_install;
pub mod setops;
pub mod strip;

pub use seams_install::init_seams;
