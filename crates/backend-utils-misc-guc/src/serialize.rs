//! Parallel-worker GUC-state transfer (`guc.c`): `EstimateGUCStateSpace`,
//! `SerializeGUCState`, `RestoreGUCState`, plus their per-variable helpers
//! `can_skip_gucvar`, `estimate_variable_size`, `serialize_variable`, and the
//! `read_gucstate` cursor.
//!
//! When a parallel query launches, the leader dumps its non-default GUC values
//! into the DSM segment (`SerializeGUCState`) and each worker reads them back
//! (`RestoreGUCState`) so the worker computes with the same settings. The wire
//! format is a private byte stream (NUL-terminated strings + native-endian
//! `source`/`scontext`/`srole`), identical on leader and worker (same backend
//! build), exactly as `guc.c` lays it out.
//!
//! The functions here operate on a byte slice (`&mut [u8]` / `&[u8]`); the
//! `space: usize` raw DSM address the `parallel-rt` seams carry is bridged into
//! a slice at the install site (the audited DSM-pointer primitive), mirroring
//! the sibling combocid/snapshot serializers.

use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR, ERROR};
use types_guc::{
    GucContext, GucSource, PGC_BOOL, PGC_ENUM, PGC_INT, PGC_INTERNAL, PGC_POSTMASTER, PGC_REAL,
    PGC_STRING, PGC_S_DEFAULT,
};

use crate::enum_lookup::config_enum_lookup_by_value;
use crate::registry::{GucRegistry, GucVariable};
use crate::units::fmt_e;
use crate::GUC_ACTION_SET;

/// `#define REALTYPE_PRECISION 17` (guc.c:68): the `%.*e` fractional-digit count
/// used to serialize `PGC_REAL` GUCs so they round-trip exactly.
const REALTYPE_PRECISION: usize = 17;

/// Native size of a serialized `GucSource` / `GucContext` (C `enum`, `int`).
const SIZEOF_ENUM: usize = core::mem::size_of::<i32>();
/// Native size of a serialized `Oid` (the `srole`).
const SIZEOF_OID: usize = core::mem::size_of::<u32>();
/// Native size of the serialized `sourceline` (`int`).
const SIZEOF_SOURCELINE: usize = core::mem::size_of::<i32>();
/// Native size of the leading length prefix (`Size`).
const SIZEOF_SIZE: usize = core::mem::size_of::<usize>();

/// `can_skip_gucvar(gconf)` (guc.c:5818): true for GUCs guaranteed to have the
/// same value in leader and workers — `PGC_POSTMASTER`, `PGC_INTERNAL`, or any
/// GUC still at its compiled-in default (`source == PGC_S_DEFAULT`). The same
/// test gates both serialize (skip sending) and restore (skip resetting).
fn can_skip_gucvar(record: &GucVariable) -> bool {
    let gen = record.gen();
    gen.context == PGC_POSTMASTER || gen.context == PGC_INTERNAL || gen.source == PGC_S_DEFAULT
}

/// The current value of a GUC rendered as the serialize wire string, matching
/// `serialize_variable`'s per-type `do_serialize` (C reads `*conf->variable`;
/// here the live value tracked in the record — read through the owner's storage
/// slot when installed, else the record's cached value/reset). NULL strings
/// become the empty string, as `estimate_variable_size`/`serialize_variable`
/// document.
fn serialized_value(record: &GucVariable) -> String {
    match record {
        GucVariable::Bool(c) => {
            let v = if c.variable.installed() {
                c.variable.read()
            } else {
                c.value.unwrap_or(c.reset_val)
            };
            if v { "true".to_string() } else { "false".to_string() }
        }
        GucVariable::Int(c) => {
            let v = if c.variable.installed() {
                c.variable.read()
            } else {
                c.value.unwrap_or(c.reset_val)
            };
            format!("{v}")
        }
        GucVariable::Real(c) => {
            let v = if c.variable.installed() {
                c.variable.read()
            } else {
                c.value.unwrap_or(c.reset_val)
            };
            // C: do_serialize(..., "%.*e", REALTYPE_PRECISION, *conf->variable)
            fmt_e(v, REALTYPE_PRECISION)
        }
        GucVariable::String(c) => {
            let v = if c.variable.installed() {
                c.variable.read()
            } else {
                c.value.clone().unwrap_or_else(|| c.reset_val.clone())
            };
            // NULL becomes empty string (see estimate_variable_size()).
            v.unwrap_or_default()
        }
        GucVariable::Enum(c) => {
            let v = if c.variable.installed() {
                c.variable.read()
            } else {
                c.value.unwrap_or(c.reset_val)
            };
            // C: config_enum_lookup_by_value(conf, *conf->variable). Unknown
            // encodings would be a backend bug; "" keeps us within budget.
            config_enum_lookup_by_value(c, v).unwrap_or("").to_string()
        }
    }
}

/// `estimate_variable_size(gconf)` (guc.c:5848): the space needed to dump one
/// GUC. Overestimating is fine, underestimating is not — so the int/real cases
/// use the maximum display length, mirroring C exactly.
fn estimate_variable_size(record: &GucVariable) -> usize {
    // Skippable GUCs consume zero space.
    if can_skip_gucvar(record) {
        return 0;
    }

    let gen = record.gen();

    // Name, plus trailing zero byte.
    let mut size = gen.name.len() + 1;

    // Maximum display length of the GUC value.
    let valsize: usize = match gen.vartype {
        // max(strlen("true"), strlen("false"))
        PGC_BOOL => 5,
        PGC_INT => {
            // Max length, reduced for typical small values. Max is 2147483647
            // (10 chars), plus one byte for sign.
            let v = match record {
                GucVariable::Int(c) => {
                    if c.variable.installed() {
                        c.variable.read()
                    } else {
                        c.value.unwrap_or(c.reset_val)
                    }
                }
                _ => 0,
            };
            if v.unsigned_abs() < 1000 {
                3 + 1
            } else {
                10 + 1
            }
        }
        // %.*e with REALTYPE_PRECISION digits: sign + leading digit + '.' +
        // fractional digits + exponent with up to 3 digits ("e+110").
        PGC_REAL => 1 + 1 + 1 + REALTYPE_PRECISION + 5,
        // strlen of the (possibly-NULL -> empty) string value.
        PGC_STRING => match record {
            GucVariable::String(c) => {
                let v = if c.variable.installed() {
                    c.variable.read()
                } else {
                    c.value.clone().unwrap_or_else(|| c.reset_val.clone())
                };
                v.map(|s| s.len()).unwrap_or(0)
            }
            _ => 0,
        },
        // strlen of the enum label.
        PGC_ENUM => serialized_value(record).len(),
    };

    // Terminating zero-byte for the value.
    size += valsize + 1;

    // sourcefile string + its terminating zero byte.
    if let Some(sf) = gen.sourcefile.as_deref() {
        size += sf.len();
    }
    size += 1;

    // sourceline is included only when sourcefile is nonempty.
    if gen.sourcefile.as_deref().is_some_and(|s| !s.is_empty()) {
        size += SIZEOF_SOURCELINE;
    }

    // source, scontext, srole.
    size += SIZEOF_ENUM + SIZEOF_ENUM + SIZEOF_OID;

    size
}

/// `EstimateGUCStateSpace()` (guc.c:5934): total space to store this process's
/// non-default GUC state, including the leading length prefix. C walks
/// `guc_nondef_list`; iterating the whole registry and skipping via
/// `can_skip_gucvar` (which already excludes `PGC_S_DEFAULT`) selects exactly
/// the same set.
pub fn estimate_guc_state_space(reg: &GucRegistry) -> usize {
    // Space for saving the data size of the guc state.
    let mut size = SIZEOF_SIZE;
    for var in reg.iter() {
        size += estimate_variable_size(var);
    }
    size
}

/// `do_serialize(destptr, maxbytes, "%s", s)` (guc.c:5957): copy a string plus
/// its NUL terminator into the cursor, erroring if it does not fit.
fn do_serialize(buf: &mut [u8], cursor: &mut usize, s: &str) -> PgResult<()> {
    let bytes = s.as_bytes();
    // Need the string bytes + one NUL terminator.
    if *cursor + bytes.len() + 1 > buf.len() {
        return Err(serialize_overflow());
    }
    buf[*cursor..*cursor + bytes.len()].copy_from_slice(bytes);
    *cursor += bytes.len();
    buf[*cursor] = 0;
    *cursor += 1;
    Ok(())
}

/// `do_serialize_binary(destptr, maxbytes, val, valsize)` (guc.c:5985): copy
/// raw bytes into the cursor, erroring if they do not fit.
fn do_serialize_binary(buf: &mut [u8], cursor: &mut usize, val: &[u8]) -> PgResult<()> {
    if *cursor + val.len() > buf.len() {
        return Err(serialize_overflow());
    }
    buf[*cursor..*cursor + val.len()].copy_from_slice(val);
    *cursor += val.len();
    Ok(())
}

/// `serialize_variable(destptr, maxbytes, gconf)` (guc.c:5996): dump name, value
/// and provenance of one GUC into the cursor. Skippable GUCs emit nothing.
fn serialize_variable(buf: &mut [u8], cursor: &mut usize, record: &GucVariable) -> PgResult<()> {
    if can_skip_gucvar(record) {
        return Ok(());
    }
    let gen = record.gen();

    do_serialize(buf, cursor, gen.name)?;
    do_serialize(buf, cursor, &serialized_value(record))?;
    do_serialize(buf, cursor, gen.sourcefile.as_deref().unwrap_or(""))?;

    if gen.sourcefile.as_deref().is_some_and(|s| !s.is_empty()) {
        do_serialize_binary(buf, cursor, &gen.sourceline.to_ne_bytes())?;
    }

    do_serialize_binary(buf, cursor, &(gen.source as i32).to_ne_bytes())?;
    do_serialize_binary(buf, cursor, &(gen.scontext as i32).to_ne_bytes())?;
    do_serialize_binary(buf, cursor, &gen.srole.to_ne_bytes())?;

    Ok(())
}

/// `SerializeGUCState(maxsize, start_address)` (guc.c:6095): dump the complete
/// non-default GUC state into `buf`. The leading `Size` slot records the actual
/// payload length (so `RestoreGUCState` knows where the data ends without
/// assuming the whole buffer is filled).
pub fn serialize_guc_state(reg: &GucRegistry, buf: &mut [u8]) -> PgResult<()> {
    // Reserve space for saving the actual size of the guc state.
    if buf.len() <= SIZEOF_SIZE {
        return Err(serialize_overflow());
    }
    let mut cursor = SIZEOF_SIZE;

    for var in reg.iter() {
        serialize_variable(buf, &mut cursor, var)?;
    }

    // Store actual payload size (everything after the length prefix).
    let actual_size = cursor - SIZEOF_SIZE;
    buf[0..SIZEOF_SIZE].copy_from_slice(&actual_size.to_ne_bytes());
    Ok(())
}

/// `read_gucstate(srcptr, srcend)` (guc.c:6133): return the NUL-terminated
/// string at the cursor and advance the cursor past its terminator.
fn read_gucstate<'a>(buf: &'a [u8], cursor: &mut usize, end: usize) -> PgResult<&'a str> {
    if *cursor >= end {
        return Err(restore_err("incomplete GUC state"));
    }
    let start = *cursor;
    let mut ptr = *cursor;
    while ptr < end && buf[ptr] != 0 {
        ptr += 1;
    }
    if ptr >= end {
        return Err(restore_err("could not find null terminator in GUC state"));
    }
    *cursor = ptr + 1;
    core::str::from_utf8(&buf[start..ptr]).map_err(|_| restore_err("invalid UTF-8 in GUC state"))
}

/// `read_gucstate_binary(srcptr, srcend, dest, size)` (guc.c:6160): read `n`
/// raw bytes at the cursor and advance.
fn read_gucstate_binary<'a>(
    buf: &'a [u8],
    cursor: &mut usize,
    end: usize,
    n: usize,
) -> PgResult<&'a [u8]> {
    if *cursor + n > end {
        return Err(restore_err("incomplete GUC state"));
    }
    let out = &buf[*cursor..*cursor + n];
    *cursor += n;
    Ok(out)
}

/// `RestoreGUCState(gucstate)` (guc.c:6191): read the serialized GUC state in
/// `buf` and set this process's GUCs to match.
///
/// As in C, every potentially-shippable GUC is first reset to its default
/// (here: `ResetAllOptions`-style reset on those that are not `can_skip_gucvar`),
/// then each serialized entry is applied with `set_config_option_ext(...,
/// GUC_ACTION_SET, changeVal=true, elevel=ERROR, is_reload=true)`.
///
/// The C "free subsidiary data + `InitializeOneGUCOption`" reset is the
/// safe-Rust reset of the record's live value/source back to its boot/default
/// state; the `Assert(gconf->stack == NULL)` precondition holds because a
/// parallel worker starts with an empty transactional GUC stack.
pub fn restore_guc_state(reg: &mut GucRegistry, buf: &[u8]) -> PgResult<()> {
    // First, reset all potentially-shippable GUCs to their default values, so
    // that applying the leader's non-default values lands us at exactly the
    // leader's state (and set_config_option won't refuse due to source-priority
    // comparisons). Same test that serialize uses.
    let indices: Vec<usize> = (0..reg.len()).collect();
    for idx in indices {
        if can_skip_gucvar(&reg[idx]) {
            continue;
        }
        reset_to_default(&mut reg[idx]);
    }

    // First item is the length of the subsequent data.
    if buf.len() < SIZEOF_SIZE {
        return Err(restore_err("incomplete GUC state"));
    }
    let len = usize::from_ne_bytes(
        buf[0..SIZEOF_SIZE]
            .try_into()
            .expect("SIZEOF_SIZE bytes"),
    );
    let mut cursor = SIZEOF_SIZE;
    let end = SIZEOF_SIZE
        .checked_add(len)
        .filter(|e| *e <= buf.len())
        .ok_or_else(|| restore_err("incomplete GUC state"))?;

    // Restore all the listed GUCs.
    while cursor < end {
        let varname = read_gucstate(buf, &mut cursor, end)?.to_string();
        let varvalue = read_gucstate(buf, &mut cursor, end)?.to_string();
        let varsourcefile = read_gucstate(buf, &mut cursor, end)?.to_string();

        let varsourceline: i32 = if !varsourcefile.is_empty() {
            i32::from_ne_bytes(
                read_gucstate_binary(buf, &mut cursor, end, SIZEOF_SOURCELINE)?
                    .try_into()
                    .expect("4 bytes"),
            )
        } else {
            0
        };
        let varsource = source_from_i32(i32::from_ne_bytes(
            read_gucstate_binary(buf, &mut cursor, end, SIZEOF_ENUM)?
                .try_into()
                .expect("4 bytes"),
        ))?;
        let varscontext = context_from_i32(i32::from_ne_bytes(
            read_gucstate_binary(buf, &mut cursor, end, SIZEOF_ENUM)?
                .try_into()
                .expect("4 bytes"),
        ))?;
        let varsrole = u32::from_ne_bytes(
            read_gucstate_binary(buf, &mut cursor, end, SIZEOF_OID)?
                .try_into()
                .expect("4 bytes"),
        );

        // set_config_option_ext(varname, varvalue, varscontext, varsource,
        // varsrole, GUC_ACTION_SET, true, ERROR, true).
        let result = crate::registry::set_config_option(
            reg,
            &varname,
            Some(&varvalue),
            varscontext,
            varsource,
            varsrole,
            GUC_ACTION_SET,
            true,
            ERROR,
            true,
        )?;
        if result <= 0 {
            return Err(PgError::error(format!(
                "parameter \"{varname}\" could not be set"
            ))
            .with_sqlstate(ERRCODE_INTERNAL_ERROR));
        }
        // set_config_sourcefile(varname, varsourcefile, varsourceline).
        if !varsourcefile.is_empty() {
            if let Some(var) = reg.find_option_mut(&varname) {
                let gen = var.gen_mut();
                gen.sourcefile = Some(varsourcefile);
                gen.sourceline = varsourceline;
            }
        }
    }
    Ok(())
}

/// The `InitializeOneGUCOption`-style reset of one GUC back to its default
/// (boot) value, used by `RestoreGUCState` before applying the leader's values.
/// Resets the live value to `reset_val` and the provenance to the default
/// (`PGC_S_DEFAULT` / `PGC_INTERNAL` / boot superuser), and runs the assign hook
/// and storage write, mirroring the per-type reset already used by
/// `reset_all_options`.
fn reset_to_default(var: &mut GucVariable) {
    use types_core::BOOTSTRAP_SUPERUSERID;

    match var {
        GucVariable::Bool(c) => {
            c.value = Some(c.reset_val);
            if let Some(slot) = c.assign_hook {
                (slot.get())(c.reset_val, None);
            }
            if c.variable.installed() {
                c.variable.write(c.reset_val);
            }
        }
        GucVariable::Int(c) => {
            c.value = Some(c.reset_val);
            if let Some(slot) = c.assign_hook {
                (slot.get())(c.reset_val, None);
            }
            if c.variable.installed() {
                c.variable.write(c.reset_val);
            }
        }
        GucVariable::Real(c) => {
            c.value = Some(c.reset_val);
            if let Some(slot) = c.assign_hook {
                (slot.get())(c.reset_val, None);
            }
            if c.variable.installed() {
                c.variable.write(c.reset_val);
            }
        }
        GucVariable::String(c) => {
            if let Some(slot) = c.assign_hook {
                (slot.get())(c.reset_val.as_deref(), None);
            }
            if c.variable.installed() {
                c.variable.write(c.reset_val.clone());
            }
            c.value = Some(c.reset_val.clone());
        }
        GucVariable::Enum(c) => {
            c.value = Some(c.reset_val);
            if let Some(slot) = c.assign_hook {
                (slot.get())(c.reset_val, None);
            }
            if c.variable.installed() {
                c.variable.write(c.reset_val);
            }
        }
    }
    let gen = var.gen_mut();
    gen.source = PGC_S_DEFAULT;
    gen.scontext = PGC_INTERNAL;
    gen.srole = BOOTSTRAP_SUPERUSERID;
    gen.sourcefile = None;
    gen.sourceline = 0;
}

/// Map a serialized `int` back to a `GucSource` (the inverse of `source as i32`).
fn source_from_i32(v: i32) -> PgResult<GucSource> {
    use types_guc::*;
    Ok(match v {
        x if x == PGC_S_DEFAULT as i32 => PGC_S_DEFAULT,
        x if x == PGC_S_DYNAMIC_DEFAULT as i32 => PGC_S_DYNAMIC_DEFAULT,
        x if x == PGC_S_ENV_VAR as i32 => PGC_S_ENV_VAR,
        x if x == PGC_S_FILE as i32 => PGC_S_FILE,
        x if x == PGC_S_ARGV as i32 => PGC_S_ARGV,
        x if x == PGC_S_GLOBAL as i32 => PGC_S_GLOBAL,
        x if x == PGC_S_DATABASE as i32 => PGC_S_DATABASE,
        x if x == PGC_S_USER as i32 => PGC_S_USER,
        x if x == PGC_S_DATABASE_USER as i32 => PGC_S_DATABASE_USER,
        x if x == PGC_S_CLIENT as i32 => PGC_S_CLIENT,
        x if x == PGC_S_OVERRIDE as i32 => PGC_S_OVERRIDE,
        x if x == PGC_S_INTERACTIVE as i32 => PGC_S_INTERACTIVE,
        x if x == PGC_S_TEST as i32 => PGC_S_TEST,
        x if x == PGC_S_SESSION as i32 => PGC_S_SESSION,
        _ => return Err(restore_err("invalid GUC source in serialized state")),
    })
}

/// Map a serialized `int` back to a `GucContext` (the inverse of `ctx as i32`).
fn context_from_i32(v: i32) -> PgResult<GucContext> {
    use types_guc::*;
    Ok(match v {
        x if x == PGC_INTERNAL as i32 => PGC_INTERNAL,
        x if x == PGC_POSTMASTER as i32 => PGC_POSTMASTER,
        x if x == PGC_SIGHUP as i32 => PGC_SIGHUP,
        x if x == PGC_SU_BACKEND as i32 => PGC_SU_BACKEND,
        x if x == PGC_BACKEND as i32 => PGC_BACKEND,
        x if x == PGC_SUSET as i32 => PGC_SUSET,
        x if x == PGC_USERSET as i32 => PGC_USERSET,
        _ => return Err(restore_err("invalid GUC context in serialized state")),
    })
}

/// `set_config_sourcefile`-equivalent provenance touch is folded into
/// [`restore_guc_state`]; no separate seam is needed.
const _SOURCEFILE_FOLDED: () = ();

/// `elog(ERROR, "not enough space to serialize GUC state")` (guc.c:5963).
fn serialize_overflow() -> PgError {
    PgError::error("not enough space to serialize GUC state").with_sqlstate(ERRCODE_INTERNAL_ERROR)
}

/// `elog(ERROR, ...)` for a malformed serialized GUC state on the restore path.
fn restore_err(msg: &'static str) -> PgError {
    PgError::error(msg).with_sqlstate(ERRCODE_INTERNAL_ERROR)
}
