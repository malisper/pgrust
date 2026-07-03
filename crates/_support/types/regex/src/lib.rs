//! Regular-expression engine vocabulary (`regex/regex.h`), shared between the
//! engine (`backend-regex-core`, i.e. `backend/regex/*`) and its SQL-level
//! consumers (`utils/adt/regexp.c`, `utils/adt/varlena.c`,
//! `utils/adt/like_support.c`, ...).

#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

pub mod regex;

pub use regex::*;
