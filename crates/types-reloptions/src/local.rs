//! Local (opclass) reloption vocabulary (`access/reloptions.h`), consumed by
//! index-AM `options` support functions that register per-opclass options
//! (e.g. `gtsvector_options`'s `siglen`).
//!
//! The owning unit (`access/common/reloptions.c`) builds these via
//! `init_local_reloptions` / `add_local_int_reloption`; consumers only hand a
//! `&mut local_relopts` to those routines, so the structs are carried here as
//! the real (trimmed) shapes rather than a stand-in.

use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;

/// `bits32` (c.h) — a 32-bit bit-string.
pub type bits32 = u32;
/// `LOCKMODE` (storage/lockdefs.h) — a lock-level code.
pub type LOCKMODE = i32;

/// `relopt_type` (reloptions.h).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum relopt_type {
    RELOPT_TYPE_BOOL = 0,
    RELOPT_TYPE_INT = 1,
    RELOPT_TYPE_REAL = 2,
    RELOPT_TYPE_ENUM = 3,
    RELOPT_TYPE_STRING = 4,
}

/// Type-specific payload of a reloption definition. In C the typed option
/// structs (`relopt_bool`, `relopt_int`, `relopt_real`, `relopt_enum`,
/// `relopt_string`; reloptions.c) embed `relopt_gen` as their first member and
/// add the default/range fields; here that tail is carried as this variant,
/// selected by [`relopt_gen::type_`]. Without it the default/min/max an option
/// registers (e.g. `add_local_int_reloption`) would be lost.
#[derive(Clone, Debug)]
pub enum relopt_typed {
    /// `relopt_bool`.
    Bool { default_val: bool },
    /// `relopt_int`.
    Int { default_val: i32, min: i32, max: i32 },
    /// `relopt_real`.
    Real { default_val: f64, min: f64, max: f64 },
    /// `relopt_enum` — `default_val` is the resolved symbol value.
    Enum { default_val: i32 },
    /// `relopt_string`.
    Str {
        default_val: Option<String>,
        default_isnull: bool,
    },
}

/// `relopt_gen` (reloptions.h) — the generic option definition header plus its
/// type-specific tail ([`relopt_typed`], the embedding typed struct's fields).
#[derive(Clone, Debug)]
pub struct relopt_gen {
    /// must be first (used as list termination marker)
    pub name: Option<String>,
    pub desc: Option<String>,
    pub kinds: bits32,
    pub lockmode: LOCKMODE,
    pub namelen: i32,
    pub type_: relopt_type,
    /// type-specific default/range payload
    pub data: relopt_typed,
}

/// `relopt_value` (reloptions.h) — a parsed option value handed to a
/// [`relopts_validator`]. The variant is selected by the generic header's
/// `type`.
#[derive(Clone, Debug)]
pub struct relopt_value {
    pub gen: Option<Box<relopt_gen>>,
    pub isset: bool,
}

/// `relopts_validator` (reloptions.h:137) —
/// `void (*)(void *parsed_options, relopt_value *vals, int nvals)`.
pub type relopts_validator = fn(parsed_options: &mut (), vals: &mut [relopt_value]);

/// `local_relopt` (reloptions.h:171) — one local option definition plus the
/// offset of its parsed value in the result bytea.
#[derive(Clone, Debug)]
pub struct local_relopt {
    /// option definition
    pub option: Option<Box<relopt_gen>>,
    /// offset of parsed value in bytea structure
    pub offset: i32,
}

/// `local_relopts` (reloptions.h:178) — local reloption data for
/// `build_local_reloptions()`.
#[derive(Clone, Debug, Default)]
pub struct local_relopts {
    /// list of `local_relopt` definitions
    pub options: Vec<local_relopt>,
    /// list of `relopts_validator` callbacks
    pub validators: Vec<relopts_validator>,
    /// size of parsed bytea structure (C `Size`)
    pub relopt_struct_size: usize,
}
