//! Generated Snowball stemmer modules (c2rust translation of
//! `src/backend/snowball/libstemmer/stem_*.c`), adapted to link against
//! this crate's faithful runtime (`crate::api`/`utilities`/`types`).
//!
//! Each module exposes `<lang>_<enc>_create_env`, `<lang>_<enc>_close_env`,
//! and `<lang>_<enc>_stem`, exactly as the Snowball compiler emits them.

#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_ISO_8859_1_basque;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_ISO_8859_1_catalan;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_ISO_8859_1_danish;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_ISO_8859_1_dutch;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_ISO_8859_1_english;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_ISO_8859_1_finnish;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_ISO_8859_1_french;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_ISO_8859_1_german;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_ISO_8859_1_indonesian;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_ISO_8859_1_irish;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_ISO_8859_1_italian;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_ISO_8859_1_norwegian;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_ISO_8859_1_porter;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_ISO_8859_1_portuguese;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_ISO_8859_1_spanish;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_ISO_8859_1_swedish;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_ISO_8859_2_hungarian;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_KOI8_R_russian;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_UTF_8_arabic;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_UTF_8_armenian;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_UTF_8_basque;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_UTF_8_catalan;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_UTF_8_danish;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_UTF_8_dutch;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_UTF_8_english;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_UTF_8_estonian;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_UTF_8_finnish;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_UTF_8_french;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_UTF_8_german;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_UTF_8_greek;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_UTF_8_hindi;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_UTF_8_hungarian;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_UTF_8_indonesian;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_UTF_8_irish;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_UTF_8_italian;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_UTF_8_lithuanian;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_UTF_8_nepali;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_UTF_8_norwegian;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_UTF_8_porter;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_UTF_8_portuguese;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_UTF_8_romanian;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_UTF_8_russian;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_UTF_8_serbian;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_UTF_8_spanish;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_UTF_8_swedish;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_UTF_8_tamil;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_UTF_8_turkish;
#[allow(non_upper_case_globals, non_snake_case, non_camel_case_types, dead_code, unused_mut, unused_assignments, static_mut_refs, unused_variables, unused_parens, unused_unsafe, unused_imports)]
pub mod stem_UTF_8_yiddish;
