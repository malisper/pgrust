//! Crate-local re-export shim for the seams of `wparser_def.c` / `ts_parse.c`.
//!
//! The seam *declarations* live in [`parse_seams`] (the unit's
//! `-seams` crate); this module re-exports them so the in-crate call sites can
//! keep using `crate::seam::<name>::call(..)`. The genuinely-external owning
//! subsystems (multibyte/locale/ts-config/tsvector-op) install them from their
//! own `init_seams()`.

pub use ::parse_seams::{
    char2wchar, config_dict_ids, config_lenmap, database_ctype_is_c, dict_lexize,
    get_database_encoding, isalnum, isalpha, isdigit, isspace, isxdigit, iswalnum, iswalpha,
    iswdigit, iswspace, iswxdigit, pg_database_encoding_max_length, pg_dsplen, pg_mb2wchar_with_len,
    pg_mblen_range, ts_execute_hl, ts_execute_locations_hl,
};
