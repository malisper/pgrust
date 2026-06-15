//! Seam declarations for the `common-extra-encnames` unit (`common/encnames.c`).
//!
//! The owning unit (the encoding-name alias table) installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

seam_core::seam!(
    /// `pg_char_to_encoding(name)` (encnames.c): map an encoding name to its
    /// `pg_enc` id (the sorted alias-table lookup, after `clean_encoding_name`),
    /// or `-1` for an empty/oversize/unknown name. A pure table lookup — never
    /// raises.
    pub fn pg_char_to_encoding(name: &str) -> i32
);

seam_core::seam!(
    /// `pg_encoding_to_char(encoding)` (encnames.c): the canonical name of the
    /// given `pg_enc` id (the `pg_enc2name_tbl` reverse lookup), or `""` for an
    /// out-of-range id. A pure table lookup — never raises.
    pub fn pg_encoding_to_char(encoding: i32) -> &'static str
);
