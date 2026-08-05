#![allow(non_snake_case, non_upper_case_globals, non_camel_case_types)]

pub mod api;
pub mod builtins;
pub mod dict;
pub mod mem;
pub mod types;
pub mod utilities;

pub mod stemmers {
    pub mod stem_iso_8859_1_english;
    pub mod stem_utf8_english;
}

#[cfg(test)]
mod tests;
