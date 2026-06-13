//! COPY command option vocabulary (`commands/copy.h`'s `CopyFormatOptions` and
//! its enums), trimmed to the fields the COPY drivers consume.
//!
//! The owning unit is `commands/copy.c` (`ProcessCopyOptions` fills the struct);
//! until that unit lands, copyto/copyfrom obtain a filled `CopyFormatOptions`
//! through the copy unit's seam. The struct holds context-allocated owned
//! storage for the string / per-column-flag members C `palloc`s.

#![no_std]
#![allow(non_camel_case_types)]

extern crate alloc;

use mcx::{PgString, PgVec};

/// `typedef enum CopyHeaderChoice` (`commands/copy.h`). For COPY TO only
/// `False`/`True` occur (`Match` is rejected during option processing); the
/// driver tests `header_line != False`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum CopyHeaderChoice {
    /// `COPY_HEADER_FALSE = 0`.
    False = 0,
    /// `COPY_HEADER_TRUE`.
    True = 1,
    /// `COPY_HEADER_MATCH`.
    Match = 2,
}

/// `CopyFormatOptions` (`commands/copy.h`), trimmed to the members the COPY
/// drivers read. The C struct's `char *` members carry NUL-terminated server-
/// or file-encoding strings; the owned model keeps them as context-allocated
/// [`PgString`]s (`null_print_len` collapses into the string length). The
/// per-column `bool *force_quote_flags` is a context-allocated [`PgVec`].
pub struct CopyFormatOptions<'mcx> {
    /// `int file_encoding` — file/remote side's encoding, -1 if unspecified.
    pub file_encoding: i32,
    /// `bool binary` — binary format?
    pub binary: bool,
    /// `bool csv_mode` — CSV format?
    pub csv_mode: bool,
    /// `CopyHeaderChoice header_line` — emit a header line?
    pub header_line: CopyHeaderChoice,
    /// `char *null_print` — NULL marker string (server encoding).
    pub null_print: PgString<'mcx>,
    /// `char *null_print_client` — `null_print` converted to file encoding.
    pub null_print_client: PgString<'mcx>,
    /// `char *delim` — column delimiter (1 byte).
    pub delim: u8,
    /// `char *quote` — CSV quote char (1 byte).
    pub quote: u8,
    /// `char *escape` — CSV escape char (1 byte).
    pub escape: u8,
    /// `List *force_quote` — column names for FORCE_QUOTE (`None` ⇒ NIL).
    pub force_quote: Option<PgVec<'mcx, PgString<'mcx>>>,
    /// `bool force_quote_all` — FORCE_QUOTE *?
    pub force_quote_all: bool,
    /// `bool *force_quote_flags` — per-physical-column FORCE_QUOTE flags.
    /// Empty until `BeginCopyTo` allocates it (`num_phys_attrs` entries).
    pub force_quote_flags: PgVec<'mcx, bool>,
}
