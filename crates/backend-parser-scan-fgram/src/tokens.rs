//! Grammar token codes, generated from the bison `gram.h` `enum yytokentype`
//! for PostgreSQL 18.3's `gram.y` (see `build.rs`).
//!
//! These are the integer token codes the core scanner returns and that the
//! bison grammar built atop it consumes.  Single-character tokens (e.g. `'+'`,
//! `'('`) are returned as their ASCII byte value and are *not* listed here;
//! only the named multi-character/keyword tokens get explicit codes, starting
//! at `IDENT = 258` exactly as `parser/scanner.h` documents.

#![allow(non_upper_case_globals)]

include!(concat!(env!("OUT_DIR"), "/tokens.rs"));
