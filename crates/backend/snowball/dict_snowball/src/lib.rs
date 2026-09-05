#![allow(non_snake_case, non_upper_case_globals, non_camel_case_types)]

pub mod api;
pub mod builtins;
pub mod dict;
pub mod mem;
pub mod types;
pub mod utilities;

// Generated Snowball stemmer modules (c2rust translation of PostgreSQL 18.3
// `src/backend/snowball/libstemmer/stem_*.c`), linking against this crate's
// runtime (`api`/`utilities`/`types`). The non-english modules keep the raw
// transpiled shape, hence the broad per-module allow list.
#[allow(
    dead_code,
    unused_mut,
    unused_assignments,
    static_mut_refs,
    unused_variables,
    unused_parens,
    unused_unsafe,
    clippy::all
)]
pub mod stemmers {
    pub mod stem_iso_8859_1_basque;
    pub mod stem_iso_8859_1_catalan;
    pub mod stem_iso_8859_1_danish;
    pub mod stem_iso_8859_1_dutch;
    pub mod stem_iso_8859_1_english;
    pub mod stem_iso_8859_1_finnish;
    pub mod stem_iso_8859_1_french;
    pub mod stem_iso_8859_1_german;
    pub mod stem_iso_8859_1_indonesian;
    pub mod stem_iso_8859_1_irish;
    pub mod stem_iso_8859_1_italian;
    pub mod stem_iso_8859_1_norwegian;
    pub mod stem_iso_8859_1_porter;
    pub mod stem_iso_8859_1_portuguese;
    pub mod stem_iso_8859_1_spanish;
    pub mod stem_iso_8859_1_swedish;
    pub mod stem_iso_8859_2_hungarian;
    pub mod stem_koi8_r_russian;
    pub mod stem_utf8_arabic;
    pub mod stem_utf8_armenian;
    pub mod stem_utf8_basque;
    pub mod stem_utf8_catalan;
    pub mod stem_utf8_danish;
    pub mod stem_utf8_dutch;
    pub mod stem_utf8_english;
    pub mod stem_utf8_estonian;
    pub mod stem_utf8_finnish;
    pub mod stem_utf8_french;
    pub mod stem_utf8_german;
    pub mod stem_utf8_greek;
    pub mod stem_utf8_hindi;
    pub mod stem_utf8_hungarian;
    pub mod stem_utf8_indonesian;
    pub mod stem_utf8_irish;
    pub mod stem_utf8_italian;
    pub mod stem_utf8_lithuanian;
    pub mod stem_utf8_nepali;
    pub mod stem_utf8_norwegian;
    pub mod stem_utf8_porter;
    pub mod stem_utf8_portuguese;
    pub mod stem_utf8_romanian;
    pub mod stem_utf8_russian;
    pub mod stem_utf8_serbian;
    pub mod stem_utf8_spanish;
    pub mod stem_utf8_swedish;
    pub mod stem_utf8_tamil;
    pub mod stem_utf8_turkish;
    pub mod stem_utf8_yiddish;
}

#[cfg(test)]
mod tests;

// dict_snowball.so's entry points (PG_MODULE_MAGIC_EXT, no _PG_init):
// snowball_create.sql declares dsnowball_init/dsnowball_lexize as LANGUAGE C
// AS '$libdir/dict_snowball', so fmgr's C-language leg resolves them through
// the dfmgr library registry (fmgr.c fmgr_info_C_lang -> dfmgr.c
// load_external_function), which records the library's first use in the
// session for pg_get_loaded_modules.
pub fn init_seams() {
    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: builtins::SNOWBALL_LIBRARY,
        lookup: |symbol| {
            builtins::SNOWBALL_CLANG
                .iter()
                .find(|(name, _, _)| *name == symbol)
                .map(|&(_, _, func)| func)
        },
        pg_init: None,
    });
}
