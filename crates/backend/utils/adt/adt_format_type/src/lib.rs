// NB: not `#![no_std]` — the fmgr builtin registration layer (`fmgr_builtins`)
// registers the `format_type.c` builtins into the fmgr-core table (C:
// `fmgr_builtins[]`), which uses `String`/`std`.
#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

//! Idiomatic Rust port of PostgreSQL 18.3 `src/backend/utils/adt/format_type.c`
//! — "display type names nicely".
//!
//! Every C function is ported in full. The fmgr / Datum plumbing of the SQL
//! entry points (`PG_GETARG_*`, `PG_RETURN_TEXT_P`, `cstring_to_text`,
//! `check_valid_oidvector`) is the project-wide fmgr boundary, so [`format_type`]
//! and [`oidvectortypes`] expose the decoded-argument / `PgString`-result shape.
//!
//! The catalog / namespace / deparser / fmgr / encoding / numeric call-outs each
//! cross the owning unit's seam crate (they return the *raw* catalog row fields
//! and *raw* call results, so the entire formatting decision logic is ported
//! 1:1 and lives here).

extern crate alloc;

use core::fmt::Write;

use mcx::{Mcx, PgString};
use types_core::{InvalidOid, OidIsValid, Oid, BITS_PER_BYTE};
use ::datum::VARHDRSZ;
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use ::types_format_type::TypeFormInfo;
use ::types_tuple::heaptuple::{
    BITOID, BOOLOID, BPCHAROID, FLOAT4OID, FLOAT8OID, INT2OID, INT4OID, INT8OID, INTERVALOID,
    JSONOID, NUMERICOID, TIMEOID, TIMESTAMPOID, TIMESTAMPTZOID, TIMETZOID, TYPSTORAGE_PLAIN,
    VARBITOID, VARCHAROID,
};

use ::namespace_seams::type_is_visible;
use ::numeric_seams::numeric_maximum_size;
use ::ruleutils_seams::quote_qualified_identifier;
use ::lsyscache_seams::get_namespace_name_or_temp;
use ::syscache_seams::type_form;
use ::fmgr_seams::typmod_out;
use ::mbutils_seams::pg_database_encoding_max_length;

/// `utils/fmgroids.h`: `F_ARRAY_SUBSCRIPT_HANDLER` — proc OID of the generic
/// array subscript handler (`pg_proc.dat`, oid 6179). Used by
/// [`is_true_array_type`].
pub const F_ARRAY_SUBSCRIPT_HANDLER: Oid = 6179;

/// `utils/builtins.h`: `FORMAT_TYPE_TYPEMOD_GIVEN` — include the typmod in the
/// output (typmod could still be -1 though).
pub const FORMAT_TYPE_TYPEMOD_GIVEN: u16 = 0x01;
/// `utils/builtins.h`: `FORMAT_TYPE_ALLOW_INVALID` — allow invalid types (return
/// `???` / `-` instead of erroring).
pub const FORMAT_TYPE_ALLOW_INVALID: u16 = 0x02;
/// `utils/builtins.h`: `FORMAT_TYPE_FORCE_QUALIFY` — force schema-qualification
/// of type names, regardless of `search_path`.
pub const FORMAT_TYPE_FORCE_QUALIFY: u16 = 0x04;
/// `utils/builtins.h`: `FORMAT_TYPE_INVALID_AS_NULL` — return NULL if the type
/// OID is undefined.
pub const FORMAT_TYPE_INVALID_AS_NULL: u16 = 0x08;

pub mod fmgr_builtins;
mod seams;
pub use seams::init_seams;

/// `pg_type.h`: `IsTrueArrayType(typeForm)` —
/// `OidIsValid(typelem) && typsubscript == F_ARRAY_SUBSCRIPT_HANDLER`.
#[inline]
fn is_true_array_type(typeform: &TypeFormInfo) -> bool {
    OidIsValid(typeform.typelem) && typeform.typsubscript == F_ARRAY_SUBSCRIPT_HANDLER
}

/// `elog(ERROR, "cache lookup failed for type %u", type_oid)` — the internal
/// catalog-corruption error (SQLSTATE `XX000`).
fn cache_lookup_failed(type_oid: Oid) -> PgError {
    PgError::error(alloc::format!("cache lookup failed for type {type_oid}"))
        .with_sqlstate(ERRCODE_INTERNAL_ERROR)
}

/// A `&str` copied into `mcx` as a `PgString` (the `pstrdup` of a literal name).
fn pstrdup<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<PgString<'mcx>> {
    PgString::from_str_in(s, mcx)
}

/// SQL function: `format_type(type_oid, typemod)`.
///
/// `type_oid` is from `pg_type.oid`, `typemod` is from `pg_attribute.atttypmod`.
/// Gets the type name and formats it and the modifier to canonical SQL format
/// if the type is a standard type; otherwise you get `pg_type.typname` back,
/// double-quoted if it contains funny characters or matches a keyword.
///
/// `type_oid == None` mirrors C's `PG_ARGISNULL(0)` (the not-strict NULL
/// passthrough), yielding `Ok(None)`. `typemod == None` mirrors
/// `PG_ARGISNULL(1)`: no typemod available, a slightly different result from
/// `Some(-1)` in some cases (see the long C comment above `format_type`).
pub fn format_type<'mcx>(
    mcx: Mcx<'mcx>,
    type_oid: Option<Oid>,
    typemod: Option<i32>,
) -> PgResult<Option<PgString<'mcx>>> {
    let flags: u16 = FORMAT_TYPE_ALLOW_INVALID;

    // Since this function is not strict, we must test for null args.
    let type_oid = match type_oid {
        None => return Ok(None),
        Some(oid) => oid,
    };

    let (typemod, flags) = match typemod {
        None => (-1, flags),
        Some(tm) => (tm, flags | FORMAT_TYPE_TYPEMOD_GIVEN),
    };

    format_type_extended(mcx, type_oid, typemod, flags)
}

/// `format_type_extended` — generate a possibly-qualified type name.
///
/// The default behavior is to only qualify if the type is not in the search
/// path, to ignore the given typmod, and to raise an error if a non-existent
/// `type_oid` is given. See the `FORMAT_TYPE_*` flags for the modifiers.
///
/// Returns a (palloc'd) string, or `None` (C's `return NULL`).
pub fn format_type_extended<'mcx>(
    mcx: Mcx<'mcx>,
    type_oid: Oid,
    typemod: i32,
    flags: u16,
) -> PgResult<Option<PgString<'mcx>>> {
    // Local mutable copy mirroring the C `Oid type_oid` parameter, reassigned
    // to `array_base_type` in the true-array branch.
    let mut type_oid = type_oid;

    if type_oid == InvalidOid {
        if (flags & FORMAT_TYPE_INVALID_AS_NULL) != 0 {
            return Ok(None);
        } else if (flags & FORMAT_TYPE_ALLOW_INVALID) != 0 {
            return Ok(Some(pstrdup(mcx, "-")?));
        }
    }

    // tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type_oid));
    let mut typeform = match type_form::call(mcx, type_oid)? {
        None => {
            if (flags & FORMAT_TYPE_INVALID_AS_NULL) != 0 {
                return Ok(None);
            } else if (flags & FORMAT_TYPE_ALLOW_INVALID) != 0 {
                return Ok(Some(pstrdup(mcx, "???")?));
            } else {
                return Err(cache_lookup_failed(type_oid));
            }
        }
        Some(form) => form,
    };

    // Check if it's a "true" array type. Pseudo-array types such as "name"
    // shouldn't get deconstructed. Also check the toast property, and don't
    // deconstruct "plain storage" array types --- this is because we don't want
    // to show oidvector as oid[].
    let array_base_type = typeform.typelem;

    let is_array;
    if is_true_array_type(&typeform) && typeform.typstorage != TYPSTORAGE_PLAIN {
        // Switch our attention to the array element type.
        typeform = match type_form::call(mcx, array_base_type)? {
            None => {
                if (flags & FORMAT_TYPE_INVALID_AS_NULL) != 0 {
                    return Ok(None);
                } else if (flags & FORMAT_TYPE_ALLOW_INVALID) != 0 {
                    return Ok(Some(pstrdup(mcx, "???[]")?));
                } else {
                    return Err(cache_lookup_failed(type_oid));
                }
            }
            Some(form) => form,
        };
        type_oid = array_base_type;
        is_array = true;
    } else {
        is_array = false;
    }

    let with_typemod = (flags & FORMAT_TYPE_TYPEMOD_GIVEN) != 0 && (typemod >= 0);

    // See if we want to special-case the output for certain built-in types.
    // Note that these special cases should all correspond to special productions
    // in gram.y, to ensure that the type name will be taken as a system type,
    // not a user type of the same name.
    //
    // If we do not provide a special-case output here, the type name is handled
    // the same way as a user type name --- in particular, it will be
    // double-quoted if it matches any lexer keyword. This behavior is essential
    // for some cases, such as types "bit" and "char".
    let mut buf: Option<PgString<'mcx>> = None; // flag for no special case

    match type_oid {
        BITOID => {
            if with_typemod {
                buf = Some(print_typmod(mcx, "bit", typemod, typeform.typmodout)?);
            } else if (flags & FORMAT_TYPE_TYPEMOD_GIVEN) != 0 {
                // bit with typmod -1 is not the same as BIT, which means BIT(1)
                // per SQL spec. Report it as the quoted typename so that parser
                // will not assign a bogus typmod.
            } else {
                buf = Some(pstrdup(mcx, "bit")?);
            }
        }

        BOOLOID => {
            buf = Some(pstrdup(mcx, "boolean")?);
        }

        BPCHAROID => {
            if with_typemod {
                buf = Some(print_typmod(mcx, "character", typemod, typeform.typmodout)?);
            } else if (flags & FORMAT_TYPE_TYPEMOD_GIVEN) != 0 {
                // bpchar with typmod -1 is not the same as CHARACTER, which means
                // CHARACTER(1) per SQL spec. Report it as bpchar so that parser
                // will not assign a bogus typmod.
            } else {
                buf = Some(pstrdup(mcx, "character")?);
            }
        }

        FLOAT4OID => {
            buf = Some(pstrdup(mcx, "real")?);
        }

        FLOAT8OID => {
            buf = Some(pstrdup(mcx, "double precision")?);
        }

        INT2OID => {
            buf = Some(pstrdup(mcx, "smallint")?);
        }

        INT4OID => {
            buf = Some(pstrdup(mcx, "integer")?);
        }

        INT8OID => {
            buf = Some(pstrdup(mcx, "bigint")?);
        }

        NUMERICOID => {
            if with_typemod {
                buf = Some(print_typmod(mcx, "numeric", typemod, typeform.typmodout)?);
            } else {
                buf = Some(pstrdup(mcx, "numeric")?);
            }
        }

        INTERVALOID => {
            if with_typemod {
                buf = Some(print_typmod(mcx, "interval", typemod, typeform.typmodout)?);
            } else {
                buf = Some(pstrdup(mcx, "interval")?);
            }
        }

        TIMEOID => {
            if with_typemod {
                buf = Some(print_typmod(mcx, "time", typemod, typeform.typmodout)?);
            } else {
                buf = Some(pstrdup(mcx, "time without time zone")?);
            }
        }

        TIMETZOID => {
            if with_typemod {
                buf = Some(print_typmod(mcx, "time", typemod, typeform.typmodout)?);
            } else {
                buf = Some(pstrdup(mcx, "time with time zone")?);
            }
        }

        TIMESTAMPOID => {
            if with_typemod {
                buf = Some(print_typmod(mcx, "timestamp", typemod, typeform.typmodout)?);
            } else {
                buf = Some(pstrdup(mcx, "timestamp without time zone")?);
            }
        }

        TIMESTAMPTZOID => {
            if with_typemod {
                buf = Some(print_typmod(mcx, "timestamp", typemod, typeform.typmodout)?);
            } else {
                buf = Some(pstrdup(mcx, "timestamp with time zone")?);
            }
        }

        VARBITOID => {
            if with_typemod {
                buf = Some(print_typmod(mcx, "bit varying", typemod, typeform.typmodout)?);
            } else {
                buf = Some(pstrdup(mcx, "bit varying")?);
            }
        }

        VARCHAROID => {
            if with_typemod {
                buf = Some(print_typmod(
                    mcx,
                    "character varying",
                    typemod,
                    typeform.typmodout,
                )?);
            } else {
                buf = Some(pstrdup(mcx, "character varying")?);
            }
        }

        JSONOID => {
            buf = Some(pstrdup(mcx, "json")?);
        }

        _ => {}
    }

    let mut buf = match buf {
        Some(b) => b,
        None => {
            // Default handling: report the name as it appears in the catalog.
            // Here, we must qualify the name if it is not visible in the search
            // path or if caller requests it; and we must double-quote it if it's
            // not a standard identifier or if it matches any keyword.
            let nspname: Option<PgString<'mcx>> = if (flags & FORMAT_TYPE_FORCE_QUALIFY) == 0
                && type_is_visible::call(mcx, type_oid)?
            {
                None
            } else {
                // get_namespace_name_or_temp returns NULL only for a missing
                // namespace, which can't happen for a type whose tuple we hold;
                // mirror C, which passes the result straight to
                // quote_qualified_identifier.
                get_namespace_name_or_temp::call(mcx, typeform.typnamespace)?
            };

            let mut buf = quote_qualified_identifier::call(
                mcx,
                nspname.as_ref().map(|s| s.as_str()),
                typeform.typname.as_str(),
            )?;

            if with_typemod {
                buf = print_typmod(mcx, buf.as_str(), typemod, typeform.typmodout)?;
            }

            buf
        }
    };

    if is_array {
        // psprintf("%s[]", buf)
        buf.try_push_str("[]")?;
    }

    Ok(Some(buf))
}

/// This version is for use within the backend in error messages, etc. One
/// difference is that it will fail for an invalid type.
///
/// The result is always a (palloc'd) string.
pub fn format_type_be<'mcx>(mcx: Mcx<'mcx>, type_oid: Oid) -> PgResult<PgString<'mcx>> {
    // With flags == 0 the only `None` paths (INVALID_AS_NULL) are unreachable;
    // an unexpected `None` is the same cache-lookup failure C would have raised.
    format_type_extended(mcx, type_oid, -1, 0)?.ok_or_else(|| cache_lookup_failed(type_oid))
}

/// [`format_type_be`] for callers that thread no `Mcx` (e.g. error-message
/// sites whose C signature takes no memory context and render the type name
/// only into the `ereport(ERROR)` text). The C result is a transient palloc'd
/// cstring that the caller immediately copies into the error string; here we
/// format into a throwaway scratch context and return that copy as an owned
/// `String`, freeing the scratch context on return. It carries no formatting
/// logic of its own — a thin owned-result wrapper over [`format_type_be`].
pub fn format_type_be_str(type_oid: Oid) -> PgResult<alloc::string::String> {
    let scratch = ::mcx::MemoryContext::new("format_type_be_str");
    let name = format_type_be(scratch.mcx(), type_oid)?;
    Ok(alloc::string::String::from(name.as_str()))
}

/// [`format_type_be`] for callers that need the printable name only to
/// interpolate into an owned `errmsg(...)` string with no `Mcx` in scope (the
/// funcapi polymorphic resolvers' `"... but type %s"` messages). The C result
/// is a transient palloc'd cstring that the caller copies into the error text;
/// here we format into a throwaway scratch context and return the copy as an
/// owned `String`. Identical to [`format_type_be_str`] — a thin owned-result
/// wrapper over [`format_type_be`] with no formatting logic of its own.
pub fn format_type_be_owned(type_oid: Oid) -> PgResult<alloc::string::String> {
    let scratch = ::mcx::MemoryContext::new("format_type_be_owned");
    let name = format_type_be(scratch.mcx(), type_oid)?;
    Ok(alloc::string::String::from(name.as_str()))
}

/// This version returns a name that is always qualified (unless it's one of the
/// SQL-keyword type names, such as TIMESTAMP WITH TIME ZONE).
pub fn format_type_be_qualified<'mcx>(mcx: Mcx<'mcx>, type_oid: Oid) -> PgResult<PgString<'mcx>> {
    format_type_extended(mcx, type_oid, -1, FORMAT_TYPE_FORCE_QUALIFY)?
        .ok_or_else(|| cache_lookup_failed(type_oid))
}

/// This version allows a nondefault typemod to be specified.
pub fn format_type_with_typemod<'mcx>(
    mcx: Mcx<'mcx>,
    type_oid: Oid,
    typemod: i32,
) -> PgResult<PgString<'mcx>> {
    format_type_extended(mcx, type_oid, typemod, FORMAT_TYPE_TYPEMOD_GIVEN)?
        .ok_or_else(|| cache_lookup_failed(type_oid))
}

/// `printTypmod` — add typmod decoration to the basic type name.
fn print_typmod<'mcx>(
    mcx: Mcx<'mcx>,
    typname: &str,
    typmod: i32,
    typmodout: Oid,
) -> PgResult<PgString<'mcx>> {
    // Shouldn't be called if typmod is -1.
    debug_assert!(typmod >= 0);

    let mut res = PgString::from_str_in(typname, mcx)?;
    if typmodout == InvalidOid {
        // Default behavior: just print the integer typmod with parens.
        // psprintf("%s(%d)", typname, (int) typmod)
        res.try_push('(')?;
        push_i32(&mut res, typmod)?;
        res.try_push(')')?;
    } else {
        // Use the type-specific typmodout procedure.
        // psprintf("%s%s", typname, tmstr)
        let tmstr = typmod_out::call(mcx, typmodout, typmod)?;
        res.try_push_str(tmstr.as_str())?;
    }

    Ok(res)
}

/// Append the base-10 representation of an `i32` to a `PgString`, fallibly.
///
/// The "%d" of `psprintf`; only the `try_push_str` into the context-allocated
/// `PgString` can fail (OOM). The decimal digits are formed in a stack buffer
/// (an `i32` is at most 11 chars including a sign), so there is no intermediate
/// heap allocation.
fn push_i32(buf: &mut PgString<'_>, value: i32) -> PgResult<()> {
    struct Stack {
        bytes: [u8; 11],
        len: usize,
    }
    impl Write for Stack {
        fn write_str(&mut self, s: &str) -> core::fmt::Result {
            let b = s.as_bytes();
            let end = self.len + b.len();
            self.bytes.get_mut(self.len..end).ok_or(core::fmt::Error)?.copy_from_slice(b);
            self.len = end;
            Ok(())
        }
    }
    let mut scratch = Stack { bytes: [0; 11], len: 0 };
    // i32 always fits; the formatter only writes digits and an optional sign.
    let _ = write!(scratch, "{value}");
    let s = core::str::from_utf8(&scratch.bytes[..scratch.len]).unwrap_or("");
    buf.try_push_str(s)
}

/// `type_maximum_size` — determine maximum width of a variable-width column.
///
/// If the max width is indeterminate, return -1 (in particular, for any type
/// not known to this routine). The caller has already determined that the type
/// is variable-width, so we don't look up the `pg_type` tuple here.
pub fn type_maximum_size(type_oid: Oid, typemod: i32) -> PgResult<i32> {
    if typemod < 0 {
        return Ok(-1);
    }

    match type_oid {
        BPCHAROID | VARCHAROID => {
            // typemod includes varlena header; typemod is in characters not bytes.
            // return (typemod - VARHDRSZ) *
            //     pg_encoding_max_length(GetDatabaseEncoding()) + VARHDRSZ;
            let max_len = pg_database_encoding_max_length::call();
            Ok((typemod - VARHDRSZ as i32) * max_len + VARHDRSZ as i32)
        }

        NUMERICOID => Ok(numeric_maximum_size::call(typemod)),

        VARBITOID | BITOID => {
            // typemod is the (max) number of bits
            // return (typemod + (BITS_PER_BYTE - 1)) / BITS_PER_BYTE
            //     + 2 * sizeof(int32);
            Ok((typemod + (BITS_PER_BYTE - 1)) / BITS_PER_BYTE
                + 2 * core::mem::size_of::<i32>() as i32)
        }

        // Unknown type, or unlimited-width type such as 'text'.
        _ => Ok(-1),
    }
}

/// `oidvectortypes` — converts a vector of type OIDs to a "typname" list.
///
/// SQL `oidvectortypes(oidvector)`. The `PG_GETARG_POINTER` /
/// `check_valid_oidvector` / `PG_RETURN_TEXT_P` Datum plumbing is the fmgr
/// boundary; this entry point takes the validated `values` slice
/// (`oidArray->values[0 .. oidArray->dim1]`). The growing palloc'd buffer of
/// the C body is a `PgString` here (buffer-growth bookkeeping is the
/// allocator's job); the per-element formatting and the `", "` separator from
/// the second element on are ported 1:1.
// The indexed `for (num = 0; num < numargs; num++)` loop and the `values[num]`
// double-access mirror the C `for` loop and `oidArray->values[num]` exactly.
#[allow(clippy::needless_range_loop)]
pub fn oidvectortypes<'mcx>(mcx: Mcx<'mcx>, values: &[Oid]) -> PgResult<PgString<'mcx>> {
    let numargs = values.len();

    let mut result = PgString::new_in(mcx);

    for num in 0..numargs {
        // format_type_extended(oidArray->values[num], -1, FORMAT_TYPE_ALLOW_INVALID)
        let typename = format_type_extended(mcx, values[num], -1, FORMAT_TYPE_ALLOW_INVALID)?
            // FORMAT_TYPE_ALLOW_INVALID never returns NULL (only `???`/`-`), so
            // this is unreachable; mirror C, which dereferences the result.
            .ok_or_else(|| cache_lookup_failed(values[num]))?;

        if num > 0 {
            result.try_push_str(", ")?;
        }
        result.try_push_str(typename.as_str())?;
    }

    Ok(result)
}

#[cfg(test)]
mod tests;
