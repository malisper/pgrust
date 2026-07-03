#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::result_large_err)]
#![allow(clippy::manual_range_contains)]
#![allow(clippy::needless_range_loop)]

//! `to_char` / `to_number` / `to_date` / `to_timestamp` — the DCH (datetime)
//! and NUM (numeric) format engines. Port of formatting.c (PG 18.3).

pub mod cache;
pub mod case;
pub mod dch;
pub mod dch_entry;
pub mod dch_fromchar;
pub mod fromchar;
pub mod isoweek;
pub mod num;
pub mod num_entry;
pub mod parse;
pub mod tables;

pub use case::{
    asc_initcap, asc_tolower, asc_toupper, get_th, index_seq_search, is_separator_char, str_numth,
    str_tolower, suff_search,
};
pub use parse::{numdesc_prepare, parse_format};
pub use tables::{FormatNode, FromCharDateMode, KeySuffix, KeyWord, NUMDesc};

/// Install this crate's inward seams.
pub fn init_seams() {
    formatting_seams::str_tolower::set(case::str_tolower);
}
