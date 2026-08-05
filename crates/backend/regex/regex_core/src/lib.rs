#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(dead_code)]
#![allow(clippy::result_large_err)]


extern crate alloc;

pub mod regex_consts;
pub mod regex_error;
pub mod regguts;

pub mod regex_compile;
pub(crate) mod regex_dfa_kernel;
pub mod regex_exec;
pub mod regex_export_free_error;
pub mod regex_foundation;
pub mod regex_locale;
pub mod regex_nfa;

pub fn init_seams() {
    use regex_core_seams as seams;

    seams::pg_regcomp::set(regex_export_free_error::seam_pg_regcomp);
    seams::pg_regexec::set(regex_export_free_error::seam_pg_regexec);
    seams::pg_regprefix::set(regex_export_free_error::seam_pg_regprefix);
    seams::pg_regfree::set(regex_export_free_error::seam_pg_regfree);
}
