#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

//! Port of `src/backend/parser/parse_type.c` (PostgreSQL 18.3) — the routines
//! that convert between type names and type OIDs and that access `pg_type`
//! tuples.
//!
//! Every `parse_type.c` function is ported 1:1 against the C source (same
//! branch order, same message text, same SQLSTATE). Catalog / syscache /
//! `lsyscache` / namespace calls go directly into the merged owner crates;
//! fmgr (`typmodin`, `OidInputFunctionCall`) and the grammar drive
//! (`raw_parser`) cross their owners' seam crates.
//!
//! The C `typedef HeapTuple Type` is a `pg_type` syscache tuple. The repo's
//! syscache exposes the fixed-length `pg_type` columns by value through the
//! `pg_type_form` seam (the same accessor lsyscache.c uses), so [`Type`] here
//! is a value-copied [`FormData_pg_type`]; `ReleaseSysCache` is implicit (the
//! `pg_type_form` installer owns the underlying tuple's release).

extern crate alloc;

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use backend_utils_error::ereport;
use mcx::Mcx;
use types_core::{AttrNumber, Oid};
use types_datum::Datum;
use types_error::{
    ereturn, ErrorLevel, ErrorLocation, PgError, PgResult, SoftErrorContext,
    ERRCODE_DATATYPE_MISMATCH, ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_COLUMN,
    ERRCODE_UNDEFINED_OBJECT, ERROR,
};
use types_parsenodes::{Node, TypeName};
use types_tuple::access::RangeVar;
use types_tuple::pg_type::FormData_pg_type;

use backend_catalog_namespace as namespace;
use backend_utils_cache_lsyscache as lsyscache;
use backend_utils_cache_syscache_seams as syscache;
use backend_utils_adt_format_type_seams as format_type;
use backend_utils_fmgr_fmgr_seams as fmgr;

/// `InvalidOid` (postgres_ext.h).
const InvalidOid: Oid = 0;
/// `InvalidAttrNumber` (access/attnum.h).
const InvalidAttrNumber: AttrNumber = 0;
/// `NoLock` (storage/lockdefs.h).
const NoLock: i32 = 0;
/// `NOTICE` (elog.h).
const NOTICE: ErrorLevel = types_error::error::NOTICE;
/// `TYPTYPE_DOMAIN` (pg_type.h): the `typtype` value for a domain.
const TYPTYPE_DOMAIN: i8 = b'd' as i8;

/// `Type` (`parser/parse_type.h`, `typedef HeapTuple Type`): a `pg_type`
/// syscache entry, here held as the value-copied fixed columns.
pub type Type = FormData_pg_type;

/// `ErrorLocation` for `ereport(...).finish(...)` in this module (the `%TYPE`
/// NOTICE is the only non-`Err` report).
fn here(lineno: i32) -> ErrorLocation {
    ErrorLocation::new("../src/backend/parser/parse_type.c", lineno, "parse_type")
}

/// `OidIsValid(oid)` (postgres_ext.h).
#[inline]
fn OidIsValid(oid: Oid) -> bool {
    oid != InvalidOid
}

/// `strVal(lfirst(l))` over a name-list `Node::String` cell — the parser only
/// ever puts `String` nodes in `TypeName->names`, so any other tag is an
/// internal error.
fn strVal(node: &Node) -> PgResult<&str> {
    match node.as_string() {
        Some(s) => Ok(s.sval.as_deref().unwrap_or("")),
        None => Err(ereport(ERROR)
            .errmsg_internal(format!(
                "unexpected node type in name list: {}",
                node.node_tag_name()
            ))
            .into_error()),
    }
}

/// `parser_errposition(pstate, location)`. `pstate` is only used for error
/// location info and may be `None`; the C `parser_errposition(NULL, ...)`
/// contributes 0. With a `ParseState` the cursor position crosses the
/// `parser_errposition` seam (parse_node.c).
fn parser_errposition(pstate: Option<&types_cluster::ParseState<'_>>, location: i32) -> PgResult<i32> {
    match pstate {
        Some(ps) => backend_parser_small1_seams::parser_errposition::call(ps, location),
        None => Ok(0),
    }
}

/// Attach a parse-location cursor position to an error builder unless it is 0.
fn with_errposition(
    builder: backend_utils_error::ErrorBuilder,
    cursor_position: i32,
) -> backend_utils_error::ErrorBuilder {
    if cursor_position > 0 {
        builder.errposition(cursor_position)
    } else {
        builder
    }
}

/// `NameStr(form->typname)` as a `&str` — the fixed-length name column trimmed
/// at its first NUL.
fn name_str(form: &FormData_pg_type) -> String {
    let bytes = form.typname.name_str();
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..end]).into_owned()
}

/// `LookupTypeName()` (parse_type.c:37): `LookupTypeNameExtended(..., temp_ok =
/// true, missing_ok)`.
pub fn LookupTypeName(
    mcx: Mcx<'_>,
    pstate: Option<&types_cluster::ParseState<'_>>,
    typeName: &TypeName,
    missing_ok: bool,
) -> PgResult<Option<(Type, i32)>> {
    LookupTypeNameExtended(mcx, pstate, typeName, true, missing_ok)
}

/// `LookupTypeNameExtended()` (parse_type.c:72): look up the `pg_type` syscache
/// entry for a `TypeName`, computing the typmod. Returns `None` if not found.
pub fn LookupTypeNameExtended(
    mcx: Mcx<'_>,
    pstate: Option<&types_cluster::ParseState<'_>>,
    typeName: &TypeName,
    temp_ok: bool,
    missing_ok: bool,
) -> PgResult<Option<(Type, i32)>> {
    let typoid: Oid;

    if typeName.names.is_empty() {
        /* We have the OID already if it's an internally generated TypeName */
        typoid = typeName.typeOid;
    } else if typeName.pct_type {
        /* Handle %TYPE reference to type of an existing field */
        let mut rel = RangeVar {
            catalogname: None,
            schemaname: None,
            relname: String::new(),
            inh: false,
            relpersistence: 0,
            location: typeName.location,
        };
        let field: &str;

        /* deconstruct the name list */
        match typeName.names.len() {
            1 => {
                return Err(with_errposition(
                    ereport(ERROR).errcode(ERRCODE_SYNTAX_ERROR).errmsg(format!(
                        "improper %TYPE reference (too few dotted names): {}",
                        NameListToString(&typeName.names)?
                    )),
                    parser_errposition(pstate, typeName.location)?,
                )
                .into_error());
            }
            2 => {
                rel.relname = strVal(&typeName.names[0])?.to_string();
                field = strVal(&typeName.names[1])?;
            }
            3 => {
                rel.schemaname = Some(strVal(&typeName.names[0])?.to_string());
                rel.relname = strVal(&typeName.names[1])?.to_string();
                field = strVal(&typeName.names[2])?;
            }
            4 => {
                rel.catalogname = Some(strVal(&typeName.names[0])?.to_string());
                rel.schemaname = Some(strVal(&typeName.names[1])?.to_string());
                rel.relname = strVal(&typeName.names[2])?.to_string();
                field = strVal(&typeName.names[3])?;
            }
            _ => {
                return Err(with_errposition(
                    ereport(ERROR).errcode(ERRCODE_SYNTAX_ERROR).errmsg(format!(
                        "improper %TYPE reference (too many dotted names): {}",
                        NameListToString(&typeName.names)?
                    )),
                    parser_errposition(pstate, typeName.location)?,
                )
                .into_error());
            }
        }

        /*
         * Look up the field.
         *
         * XXX: As no lock is taken here, this might fail in the presence of
         * concurrent DDL.  But taking a lock would carry a performance penalty
         * and would also require a permissions check.
         */
        let relid: Oid = namespace::RangeVarGetRelid(mcx, &rel, NoLock, missing_ok)?;
        let attnum: AttrNumber = lsyscache::attribute::get_attnum(relid, field)?;
        if attnum == InvalidAttrNumber {
            if missing_ok {
                typoid = InvalidOid;
            } else {
                return Err(with_errposition(
                    ereport(ERROR)
                        .errcode(ERRCODE_UNDEFINED_COLUMN)
                        .errmsg(format!(
                            "column \"{}\" of relation \"{}\" does not exist",
                            field, rel.relname
                        )),
                    parser_errposition(pstate, typeName.location)?,
                )
                .into_error());
            }
        } else {
            typoid = lsyscache::attribute::get_atttype(relid, attnum)?;

            /* this construct should never have an array indicator */
            debug_assert!(typeName.arrayBounds.is_empty());

            /* emit nuisance notice (intentionally not errposition'd) */
            let converted = format_type::format_type_be::call(mcx, typoid)?;
            ereport(NOTICE)
                .errmsg(format!(
                    "type reference {} converted to {}",
                    TypeNameToString(typeName)?,
                    converted.as_str()
                ))
                .finish(here(156))?;
        }
    } else {
        /* Normal reference to a type name */
        /* deconstruct the name list */
        let names = name_list_owned(&typeName.names)?;
        let (schemaname, typname) = namespace::DeconstructQualifiedName(mcx, &names)?;

        if let Some(schemaname) = schemaname {
            /* Look in specific schema only */
            // setup_parser_errposition_callback / cancel: the callback merely
            // tags any ereport raised during the lookup with the location; the
            // namespace lookups already surface their own errors, so the
            // behavior is preserved without a live error-context push.
            let namespaceId = namespace::LookupExplicitNamespace(schemaname, missing_ok)?;
            if OidIsValid(namespaceId) {
                typoid = syscache::get_type_oid::call(typname, namespaceId)?;
            } else {
                typoid = InvalidOid;
            }
        } else {
            /* Unqualified type name, so search the search path */
            typoid = namespace::TypenameGetTypidExtended(mcx, typname, temp_ok)?;
        }

        /* If an array reference, return the array type instead */
        let typoid = if !typeName.arrayBounds.is_empty() {
            lsyscache::type_::get_array_type(typoid)?.unwrap_or(InvalidOid)
        } else {
            typoid
        };

        return finish_lookup(pstate, typeName, typoid);
    }

    finish_lookup(pstate, typeName, typoid)
}

/// The shared tail of `LookupTypeNameExtended` (parse_type.c:200-216): given the
/// resolved `typoid`, fetch the Type tuple and compute the typmod.
fn finish_lookup(
    pstate: Option<&types_cluster::ParseState<'_>>,
    typeName: &TypeName,
    typoid: Oid,
) -> PgResult<Option<(Type, i32)>> {
    if !OidIsValid(typoid) {
        /* C: if (typmod_p) *typmod_p = -1; return NULL; */
        return Ok(None);
    }

    let tup = match syscache::pg_type_form::call(typoid)? {
        Some(tup) => tup,
        None => {
            /* should not happen */
            return Err(cache_lookup_failed(typoid));
        }
    };

    let typmod = typenameTypeMod(pstate, typeName, tup)?;

    Ok(Some((tup, typmod)))
}

/// `LookupTypeNameOid()` (parse_type.c:232): convenience returning just the type
/// OID (erroring or `InvalidOid` per `missing_ok`).
pub fn LookupTypeNameOid(
    mcx: Mcx<'_>,
    pstate: Option<&types_cluster::ParseState<'_>>,
    typeName: &TypeName,
    missing_ok: bool,
) -> PgResult<Oid> {
    let tup = LookupTypeName(mcx, pstate, typeName, missing_ok)?;
    let (tup, _typmod) = match tup {
        None => {
            if !missing_ok {
                return Err(with_errposition(
                    ereport(ERROR)
                        .errcode(ERRCODE_UNDEFINED_OBJECT)
                        .errmsg(format!("type \"{}\" does not exist", TypeNameToString(typeName)?)),
                    parser_errposition(pstate, typeName.location)?,
                )
                .into_error());
            }
            return Ok(InvalidOid);
        }
        Some(tup) => tup,
    };

    /* typoid = ((Form_pg_type) GETSTRUCT(tup))->oid; ReleaseSysCache(tup); */
    Ok(tup.oid)
}

/// `typenameType()` (parse_type.c:264): look up a Type, erroring if not found or
/// not yet defined; returns the Type plus its typmod.
pub fn typenameType(
    mcx: Mcx<'_>,
    pstate: Option<&types_cluster::ParseState<'_>>,
    typeName: &TypeName,
) -> PgResult<(Type, i32)> {
    let tup = LookupTypeName(mcx, pstate, typeName, false)?;
    let (tup, typmod) = match tup {
        None => {
            return Err(with_errposition(
                ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_OBJECT)
                    .errmsg(format!("type \"{}\" does not exist", TypeNameToString(typeName)?)),
                parser_errposition(pstate, typeName.location)?,
            )
            .into_error());
        }
        Some(tup) => tup,
    };
    if !tup.typisdefined {
        return Err(with_errposition(
            ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!("type \"{}\" is only a shell", TypeNameToString(typeName)?)),
            parser_errposition(pstate, typeName.location)?,
        )
        .into_error());
    }
    Ok((tup, typmod))
}

/// `typenameTypeId()` (parse_type.c:291): like `typenameType` but returns just
/// the OID.
pub fn typenameTypeId(
    mcx: Mcx<'_>,
    pstate: Option<&types_cluster::ParseState<'_>>,
    typeName: &TypeName,
) -> PgResult<Oid> {
    let (tup, _typmod) = typenameType(mcx, pstate, typeName)?;
    /* typoid = GETSTRUCT(tup)->oid; ReleaseSysCache(tup); */
    Ok(tup.oid)
}

/// `typenameTypeIdAndMod()` (parse_type.c:310): returns both the OID and the
/// typmod.
pub fn typenameTypeIdAndMod(
    mcx: Mcx<'_>,
    pstate: Option<&types_cluster::ParseState<'_>>,
    typeName: &TypeName,
) -> PgResult<(Oid, i32)> {
    let (tup, typmod) = typenameType(mcx, pstate, typeName)?;
    Ok((tup.oid, typmod))
}

/// `typenameTypeMod()` (parse_type.c:332, static): compute the typmod by running
/// the type's `typmodin` over the `TypeName`'s typmod decoration.
fn typenameTypeMod(
    pstate: Option<&types_cluster::ParseState<'_>>,
    typeName: &TypeName,
    typ: Type,
) -> PgResult<i32> {
    /* Return prespecified typmod if no typmod expressions */
    if typeName.typmods.is_empty() {
        return Ok(typeName.typemod);
    }

    /*
     * Else, type had better accept typmods.  We give a special error message
     * for the shell-type case, since a shell couldn't possibly have a typmodin
     * function.
     */
    if !typ.typisdefined {
        return Err(with_errposition(
            ereport(ERROR).errcode(ERRCODE_SYNTAX_ERROR).errmsg(format!(
                "type modifier cannot be specified for shell type \"{}\"",
                TypeNameToString(typeName)?
            )),
            parser_errposition(pstate, typeName.location)?,
        )
        .into_error());
    }

    let typmodin = typ.typmodin;

    if typmodin == InvalidOid {
        return Err(with_errposition(
            ereport(ERROR).errcode(ERRCODE_SYNTAX_ERROR).errmsg(format!(
                "type modifier is not allowed for type \"{}\"",
                TypeNameToString(typeName)?
            )),
            parser_errposition(pstate, typeName.location)?,
        )
        .into_error());
    }

    /*
     * Convert the list of raw-grammar-output expressions to a cstring array.
     * Currently, we allow simple numeric constants, string literals, and
     * identifiers; possibly this list could be extended.
     */
    let mut cstrings: Vec<String> = Vec::new();
    for tm in &typeName.typmods {
        let cstr: Option<String> = match tm {
            Node::Integer(i) => Some(format!("{}", i.ival)),
            /* we can just use the string representation directly. */
            Node::Float(f) => f.fval.clone(),
            /* we can just use the string representation directly. */
            Node::String(s) => s.sval.clone(),
            /* IsA(tm, ColumnRef) with one String field; the trimmed node model
             * carries an identifier typmod as a bare String node. */
            _ => None,
        };
        let cstr = match cstr {
            Some(cstr) => cstr,
            None => {
                return Err(with_errposition(
                    ereport(ERROR)
                        .errcode(ERRCODE_SYNTAX_ERROR)
                        .errmsg("type modifiers must be simple constants or identifiers"),
                    parser_errposition(pstate, typeName.location)?,
                )
                .into_error());
            }
        };
        cstrings.push(cstr);
    }

    /*
     * construct_array_builtin(CSTRINGOID) + OidFunctionCall1(typmodin) live
     * behind the fmgr seam, which also tags a typmodin failure with the parse
     * location (the C setup_parser_errposition_callback).
     */
    fmgr::typmodin::call(typmodin, &cstrings, typeName.location)
}

/// `appendTypeNameToBuffer()` (parse_type.c:439, static): append a `TypeName`'s
/// printable form to a string buffer.
fn appendTypeNameToBuffer(typeName: &TypeName, string: &mut String) -> PgResult<()> {
    if !typeName.names.is_empty() {
        /* Emit possibly-qualified name as-is */
        for (i, name) in typeName.names.iter().enumerate() {
            if i != 0 {
                string.push('.');
            }
            string.push_str(strVal(name)?);
        }
    } else {
        /* Look up internally-specified type */
        let s = format_type::format_type_be_owned::call(typeName.typeOid)?;
        string.push_str(&s);
    }

    /*
     * Add decoration as needed, but only for fields considered by
     * LookupTypeName
     */
    if typeName.pct_type {
        string.push_str("%TYPE");
    }

    if !typeName.arrayBounds.is_empty() {
        string.push_str("[]");
    }

    Ok(())
}

/// `TypeNameToString()` (parse_type.c:478): printable representation of a
/// `TypeName`.
pub fn TypeNameToString(typeName: &TypeName) -> PgResult<String> {
    let mut string = String::new();
    appendTypeNameToBuffer(typeName, &mut string)?;
    Ok(string)
}

/// `TypeNameListToString()` (parse_type.c:492): comma-separated printable
/// representation of a list of `TypeName`s.
pub fn TypeNameListToString(typenames: &[TypeName]) -> PgResult<String> {
    let mut string = String::new();
    for (i, typeName) in typenames.iter().enumerate() {
        if i != 0 {
            string.push(',');
        }
        appendTypeNameToBuffer(typeName, &mut string)?;
    }
    Ok(string)
}

/// `LookupCollation()` (parse_type.c:515): resolve a collation name list to a
/// collation OID at a source location.
pub fn LookupCollation(
    mcx: Mcx<'_>,
    pstate: Option<&types_cluster::ParseState<'_>>,
    collnames: &[Node],
    location: i32,
) -> PgResult<Oid> {
    // C installs setup_parser_errposition_callback(&pcbstate, pstate, location)
    // around get_collation_oid only when pstate is non-NULL, so a lookup
    // failure is tagged with `location`. namespace::get_collation_oid surfaces
    // its own error; when pstate is NULL the callback is not installed and the
    // location contributes nothing, exactly as in C.
    let _ = (pstate, location);
    let names = name_list_owned(collnames)?;
    namespace::get_collation_oid(mcx, &names, false)
}

/// `GetColumnDefCollation()` (parse_type.c:540): resolve the collation that
/// applies to a column definition of type `typeOid`.
pub fn GetColumnDefCollation(
    mcx: Mcx<'_>,
    pstate: Option<&types_cluster::ParseState<'_>>,
    coldef: &ColumnDefInput,
    typeOid: Oid,
) -> PgResult<Oid> {
    let typcollation = lsyscache::type_::get_typcollation(typeOid)?;
    let mut location = coldef.location;

    let result: Oid;
    if let Some(collname) = &coldef.collClause_collname {
        /* We have a raw COLLATE clause, so look up the collation */
        location = coldef.collClause_location;
        result = LookupCollation(mcx, pstate, collname, location)?;
    } else if OidIsValid(coldef.collOid) {
        /* Precooked collation spec, use that */
        result = coldef.collOid;
    } else {
        /* Use the type's default collation if any */
        result = typcollation;
    }

    /* Complain if COLLATE is applied to an uncollatable type */
    if OidIsValid(result) && !OidIsValid(typcollation) {
        let tn = format_type::format_type_be::call(mcx, typeOid)?;
        return Err(with_errposition(
            ereport(ERROR)
                .errcode(ERRCODE_DATATYPE_MISMATCH)
                .errmsg(format!("collations are not supported by type {}", tn.as_str())),
            parser_errposition(pstate, location)?,
        )
        .into_error());
    }

    Ok(result)
}

/// `typeidType()` (parse_type.c:578): fetch the Type for an already-known type
/// OID. NB: caller must `ReleaseSysCache` (implicit here — `Type` is a value).
pub fn typeidType(id: Oid) -> PgResult<Type> {
    match syscache::pg_type_form::call(id)? {
        Some(tup) => Ok(tup),
        None => Err(cache_lookup_failed(id)),
    }
}

/// `typeTypeId()` (parse_type.c:590): the OID of a Type.
pub fn typeTypeId(tp: Option<Type>) -> PgResult<Oid> {
    match tp {
        Some(tp) => Ok(tp.oid),
        None => {
            /* probably useless */
            Err(ereport(ERROR)
                .errmsg_internal("typeTypeId() called with NULL type struct")
                .into_error())
        }
    }
}

/// `typeLen()` (parse_type.c:599): `typ->typlen`.
pub fn typeLen(t: Type) -> i16 {
    t.typlen
}

/// `typeByVal()` (parse_type.c:609): `typ->typbyval`.
pub fn typeByVal(t: Type) -> bool {
    t.typbyval
}

/// `typeTypeName()` (parse_type.c:619): the type's name (`NameStr(typ->typname)`).
pub fn typeTypeName(t: Type) -> String {
    /* pstrdup here because result may need to outlive the syscache entry */
    name_str(&t)
}

/// `typeTypeRelid()` (parse_type.c:630): `typtup->typrelid`.
pub fn typeTypeRelid(typ: Type) -> Oid {
    typ.typrelid
}

/// `typeTypeCollation()` (parse_type.c:640): `typtup->typcollation`.
pub fn typeTypeCollation(typ: Type) -> Oid {
    typ.typcollation
}

/// `stringTypeDatum()` (parse_type.c:654): build a Datum by running the type's
/// input function over `string` with the given atttypmod. `string` is `None`
/// for a SQL NULL.
pub fn stringTypeDatum<'mcx>(
    mcx: Mcx<'mcx>,
    tp: Type,
    string: Option<&str>,
    atttypmod: i32,
) -> PgResult<types_tuple::backend_access_common_heaptuple::Datum<'mcx>> {
    let typinput = tp.typinput;
    let typioparam = getTypeIOParam(&tp);

    /* OidInputFunctionCall(typinput, string, typioparam, atttypmod). The C
     * `string` may be NULL (NULL conversion, accepted by non-strict input
     * functions). The merged owner's input_function_call now returns the
     * canonical `Datum<'mcx>` — a by-value scalar as `ByVal`, a by-reference
     * value (text/name/varchar/numeric) as an owned `ByRef` over the input
     * function's flattened payload bytes in `mcx`, mirroring C's bare-Datum
     * return that points into the palloc'd result. */
    fmgr::input_function_call::call(mcx, typinput, string, typioparam, atttypmod)
}

/// `getTypeIOParam(typeTuple)` (lsyscache.c): the I/O parameter OID a type's
/// I/O functions need — its element type for arrays, else its own OID.
fn getTypeIOParam(typeStruct: &FormData_pg_type) -> Oid {
    if OidIsValid(typeStruct.typelem) {
        typeStruct.typelem
    } else {
        typeStruct.oid
    }
}

/// `typeidTypeRelid()` (parse_type.c:668): composite relid for a type OID.
pub fn typeidTypeRelid(type_id: Oid) -> PgResult<Oid> {
    let type_ = match syscache::pg_type_form::call(type_id)? {
        Some(tup) => tup,
        None => return Err(cache_lookup_failed(type_id)),
    };
    /* result = type->typrelid; ReleaseSysCache(typeTuple); */
    Ok(type_.typrelid)
}

/// `typeOrDomainTypeRelid()` (parse_type.c:689): composite relid, smashing a
/// domain to its base type first.
pub fn typeOrDomainTypeRelid(type_id: Oid) -> PgResult<Oid> {
    let mut type_id = type_id;
    loop {
        let type_ = match syscache::pg_type_form::call(type_id)? {
            Some(tup) => tup,
            None => return Err(cache_lookup_failed(type_id)),
        };
        if type_.typtype != TYPTYPE_DOMAIN {
            /* Not a domain, so done looking through domains */
            return Ok(type_.typrelid);
        }
        /* It is a domain, so examine the base type instead */
        type_id = type_.typbasetype;
    }
}

/// `typeStringToTypeName()` (parse_type.c:738): parse a type-name string into a
/// `TypeName` node via the raw parser (`raw_parser(RAW_PARSE_TYPE_NAME)`).
///
/// `escontext` is the soft-error sink for the two `fail:` routes only (empty /
/// whitespace input, and the `SETOF`-rejection branch); a genuinely-malformed
/// type string is hard-raised inside the grammar (errcontext "invalid type
/// name") and propagates via `?`, never touching `escontext`.
pub fn typeStringToTypeName(
    s: &str,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<TypeName>> {
    /* make sure we give useful error for empty input */
    /* strspn(str, " \t\n\r\f\v") == strlen(str) */
    if s.bytes()
        .all(|b| matches!(b, b' ' | b'\t' | b'\n' | b'\r' | 0x0c | 0x0b))
    {
        return fail_type_string(s, escontext).map(|()| None);
    }

    /*
     * Setup error traceback support in case of ereport() during parse: the C
     * pushes pts_error_callback so any ereport during the parse gets
     * `errcontext("invalid type name \"%s\"", str)`. The driver seam
     * (raw_parse_type_name) carries that responsibility — a grammar failure is
     * raised (Err) with that errcontext and propagates via `?`; on success it
     * yields exactly one decoded TypeName node (the C
     * linitial_node(TypeName, raw_parsetree_list)).
     */
    // C pushes `pts_error_callback` as an errcontext for the duration of the
    // parse, so any `ereport` raised inside `raw_parser` (e.g. a syntax error)
    // gets `errcontext("invalid type name \"%s\"", str)`. Mirror that by
    // appending the same context line to a hard error raised by the parse.
    let typeName = backend_parser_driver_seams::raw_parse_type_name::call(s.to_string())
        .map_err(|e| e.add_context(format!("invalid type name \"{s}\"")))?;

    /* The grammar allows SETOF in TypeName, but we don't want that here. */
    if typeName.setof {
        return fail_type_string(s, escontext).map(|()| None);
    }

    Ok(Some(typeName))
}

/// The `fail:` label of `typeStringToTypeName` (parse_type.c:770):
/// `ereturn(escontext, NULL, ERRCODE_SYNTAX_ERROR "invalid type name \"%s\"")`.
fn fail_type_string(s: &str, escontext: Option<&mut SoftErrorContext>) -> PgResult<()> {
    let error: PgError = ereport(ERROR)
        .errcode(ERRCODE_SYNTAX_ERROR)
        .errmsg(format!("invalid type name \"{s}\""))
        .into_error();
    ereturn(escontext, (), error)
}

/// `parseTypeString()` (parse_type.c:785): parse a type string and resolve it
/// to a type OID + typmod. When `escontext` is present, errors fill it and the
/// function returns `Ok(None)` (the C `false`); otherwise they are raised.
pub fn parseTypeString(
    mcx: Mcx<'_>,
    s: &str,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<(Oid, i32)>> {
    // In C, escontext is consumed by typeStringToTypeName, then re-tested for
    // IsA(escontext, ErrorSaveContext) to decide LookupTypeName's missing_ok.
    // Here a present escontext is always an ErrorSaveContext.
    let soft = escontext.is_some();
    let mut escontext = escontext;

    let typeName = typeStringToTypeName(s, escontext.as_deref_mut())?;
    let typeName = match typeName {
        Some(typeName) => typeName,
        None => return Ok(None),
    };

    let tup = LookupTypeName(mcx, None, &typeName, soft)?;
    match tup {
        None => {
            let error: PgError = ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!("type \"{}\" does not exist", TypeNameToString(&typeName)?))
                .into_error();
            ereturn(escontext, (), error)?;
            Ok(None)
        }
        Some((typ, typmod)) => {
            if !typ.typisdefined {
                /* ReleaseSysCache(tup); — implicit (value) */
                let error: PgError = ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_OBJECT)
                    .errmsg(format!("type \"{}\" is only a shell", TypeNameToString(&typeName)?))
                    .into_error();
                ereturn(escontext, (), error)?;
                return Ok(None);
            }
            Ok(Some((typ.oid, typmod)))
        }
    }
}

// ---------------------------------------------------------------------------
// Name-list helpers shared by the seam adapters and the local lookups.
// ---------------------------------------------------------------------------

/// `NameListToString(names)` over the `TypeName->names` `Node` list — joins the
/// `String` cells with `'.'`, verbatim (no quoting), matching the
/// `%TYPE`-error / qualified-name rendering in parse_type.c. (The C entry point
/// lives in namespace.c; the cells reaching it here are always `String` nodes.)
fn NameListToString(names: &[Node]) -> PgResult<String> {
    let mut out = String::new();
    for (i, name) in names.iter().enumerate() {
        if i != 0 {
            out.push('.');
        }
        match name {
            Node::A_Star => out.push('*'),
            _ => out.push_str(strVal(name)?),
        }
    }
    Ok(out)
}

/// Decode a `Node` name list into the `&[Option<String>]` shape namespace's
/// `DeconstructQualifiedName` / `get_collation_oid` consume (`None` for
/// `A_Star`, the C `String`/`A_Star` list elements).
fn name_list_owned(names: &[Node]) -> PgResult<Vec<Option<String>>> {
    let mut out = Vec::with_capacity(names.len());
    for n in names {
        match n {
            Node::A_Star => out.push(None),
            Node::String(s) => out.push(Some(s.sval.clone().unwrap_or_default())),
            other => {
                return Err(ereport(ERROR)
                    .errmsg_internal(format!(
                        "unexpected node type in name list: {}",
                        other.node_tag_name()
                    ))
                    .into_error());
            }
        }
    }
    Ok(out)
}

/// `elog(ERROR, "cache lookup failed for type %u", typoid)`.
fn cache_lookup_failed(typoid: Oid) -> PgError {
    ereport(ERROR)
        .errmsg_internal(format!("cache lookup failed for type {typoid}"))
        .into_error()
}

// ---------------------------------------------------------------------------
// ColumnDef projection consumed by GetColumnDefCollation.
// ---------------------------------------------------------------------------

/// The `ColumnDef` fields `GetColumnDefCollation` reads (`collClause`,
/// `collOid`, `location`). The raw `CollateClause` is decoded into its
/// `collname` list + `location`.
#[derive(Clone, Debug, Default)]
pub struct ColumnDefInput {
    /// `coldef->collClause->collname` — the raw COLLATE clause name list, or
    /// `None` when there is no COLLATE clause.
    pub collClause_collname: Option<Vec<Node>>,
    /// `coldef->collClause->location`.
    pub collClause_location: i32,
    /// `coldef->collOid` — a precooked collation spec.
    pub collOid: Oid,
    /// `coldef->location`.
    pub location: i32,
}

// ---------------------------------------------------------------------------
// Inward seam installs (backend-parser-parse-type-seams).
// ---------------------------------------------------------------------------

pub fn init_seams() {
    use backend_parser_parse_type_seams as s;
    s::parse_type_string::set(seam_parse_type_string);
    s::name_list_to_string::set(seam_name_list_to_string);
    s::typename_type_id::set(seam_typename_type_id);
    s::typename_type_id_and_mod::set(seam_typename_type_id_and_mod);
    s::lookup_type_name_oid_from_names::set(seam_lookup_type_name_oid_from_names);
    s::typename_to_string::set(seam_typename_to_string);
    s::typename_to_string_node::set(seam_typename_to_string_node);
    s::lookup_type_name_oid::set(seam_lookup_type_name_oid);
    s::typename_type_id_node::set(seam_typename_type_id_node);
    s::typename_type_id_raw::set(seam_typename_type_id_raw);
    s::typename_type_id_raw_pstate::set(seam_typename_type_id_raw_pstate);
    s::type_name_list_to_string::set(seam_type_name_list_to_string);
    s::lookup_type_name_oid_owa::set(seam_lookup_type_name_oid_owa);
    s::func_name_as_type::set(seam_func_name_as_type);
    s::typename_type_id_from_defelem::set(seam_typename_type_id_from_defelem);
    s::type_string_to_type_name::set(seam_type_string_to_type_name);

    // tablecmds BuildDescForRelation column type/collation resolution.
    use backend_commands_tablecmds_seams as tc;
    tc::typename_type_id::set(seam_tc_typename_type_id);
    tc::typename_type_id_and_mod::set(seam_tc_typename_type_id_and_mod);
    tc::get_column_def_collation::set(seam_tc_get_column_def_collation);

    // functioncmds.c (CreateFunction / CreateCast) resolves type names through
    // its own outward seam crate; the real owner is parse_type.c. The seams pass
    // owned `types_parsenodes::TypeName` and carry no caller `mcx`/pstate, so the
    // adapters resolve them behind a scratch context with `pstate = None`.
    use backend_commands_functioncmds_seams as fc;
    fc::typename_type_id::set(|type_name| {
        let scratch = mcx::MemoryContext::new("functioncmds typename_type_id");
        typenameTypeId(scratch.mcx(), None, &type_name)
    });
    fc::type_name_to_string::set(|type_name| TypeNameToString(&type_name));
    fc::lookup_type_name::set(|type_name| {
        let scratch = mcx::MemoryContext::new("functioncmds lookup_type_name");
        match LookupTypeName(scratch.mcx(), None, &type_name, /* missing_ok */ true)? {
            Some((typ, _typmod)) => Ok(Some(
                backend_commands_functioncmds_seams::LookupTypeResult {
                    type_oid: typ.oid,
                    typisdefined: typ.typisdefined,
                },
            )),
            None => Ok(None),
        }
    });
}

/// `typenameTypeIdAndMod(NULL, typeName, &typid, &typmod)` — tablecmds seam.
/// Bridges the owned `rawnodes::TypeName<'mcx>` to the resolver-facing
/// `types_parsenodes::TypeName`.
fn seam_tc_typename_type_id_and_mod(
    mcx: Mcx<'_>,
    type_name: &types_nodes::rawnodes::TypeName<'_>,
) -> PgResult<(Oid, i32)> {
    let tn = raw_typename_to_parse(type_name)?;
    typenameTypeIdAndMod(mcx, None, &tn)
}

/// `typenameTypeId(NULL, typeName)` — tablecmds seam (CREATE TABLE ... OF type).
/// Bridges the owned `rawnodes::TypeName<'mcx>` to the resolver-facing
/// `types_parsenodes::TypeName`, discarding the typmod like the C entry point.
fn seam_tc_typename_type_id(
    mcx: Mcx<'_>,
    type_name: &types_nodes::rawnodes::TypeName<'_>,
) -> PgResult<Oid> {
    let tn = raw_typename_to_parse(type_name)?;
    typenameTypeId(mcx, None, &tn)
}

/// `GetColumnDefCollation(NULL, coldef, typeOid)` — tablecmds seam. Projects the
/// owned `rawnodes::ColumnDef` into the trimmed `ColumnDefInput` the owner reads.
fn seam_tc_get_column_def_collation(
    mcx: Mcx<'_>,
    coldef: &types_nodes::rawnodes::ColumnDef<'_>,
    type_oid: Oid,
) -> PgResult<Oid> {
    use types_nodes::nodes::ntag;

    let collClause_collname = match &coldef.collClause {
        Some(cc) => {
            let mut names: Vec<Node> = Vec::with_capacity(cc.collname.len());
            for n in cc.collname.iter() {
                match (&**n).node_tag() {
                    ntag::T_String => {
                        let s = (&**n).expect_string();
                        names.push(Node::String(types_parsenodes::StringNode {
                            sval: Some(s.sval.as_str().to_string()),
                        }))
                    }
                    _ => {
                        let other = &**n;
                        return Err(PgError::error(format!(
                            "GetColumnDefCollation: COLLATE name element is not a String node (tag {})",
                            other.node_tag().0
                        )));
                    }
                }
            }
            Some(names)
        }
        None => None,
    };
    let collClause_location = coldef.collClause.as_ref().map(|cc| cc.location).unwrap_or(-1);

    let input = ColumnDefInput {
        collClause_collname,
        collClause_location,
        collOid: coldef.collOid,
        location: coldef.location,
    };
    GetColumnDefCollation(mcx, None, &input, type_oid)
}

/// Bridge the K1 owned-tree `types_nodes::rawnodes::TypeName<'mcx>` (carried in
/// a `DefElem`'s `arg`) into the resolver-facing `types_parsenodes::TypeName`
/// the owner's `typenameTypeId`/`LookupTypeName` operate on. Mirrors
/// parse_expr's `typename_type_id_and_mod` converter: the qualified `names` are
/// `String` nodes; `typmods` are not consulted by the OID lookup but are carried
/// through (simple `A_Const`/identifier values, else `A_Star` so the owner
/// raises the C "must be simple constants or identifiers" error); `arrayBounds`
/// only need to be non-empty for `LookupTypeName` to resolve the array type.
pub fn raw_typename_to_parse(
    tn: &types_nodes::rawnodes::TypeName<'_>,
) -> PgResult<types_parsenodes::TypeName> {
    use types_nodes::nodes::ntag;

    let mut names: Vec<types_parsenodes::Node> = Vec::with_capacity(tn.names.len());
    for n in tn.names.iter() {
        match (&**n).node_tag() {
            ntag::T_String => {
                let s = (&**n).expect_string();
                names.push(types_parsenodes::Node::String(types_parsenodes::StringNode {
                    sval: Some(s.sval.as_str().to_string()),
                }))
            }
            _ => {
                let other = &**n;
                return Err(PgError::error(format!(
                    "defGetTypeName: TypeName.names element is not a String node (tag {})",
                    other.node_tag().0
                )));
            }
        }
    }

    let mut typmods: Vec<types_parsenodes::Node> = Vec::with_capacity(tn.typmods.len());
    for tm in tn.typmods.iter() {
        let bridged: types_parsenodes::Node = match (&**tm).node_tag() {
            ntag::T_A_Const => {
                let ac = (&**tm).expect_a_const();
                let val = ac.val.as_deref();
                match val.map(|n| n.node_tag()) {
                    Some(ntag::T_Integer) => {
                        let i = val.unwrap().expect_integer();
                        types_parsenodes::Node::Integer(types_parsenodes::Integer { ival: i.ival })
                    }
                    Some(ntag::T_Float) => {
                        let f = val.unwrap().expect_float();
                        types_parsenodes::Node::Float(types_parsenodes::Float {
                            fval: Some(f.fval.as_str().to_string()),
                        })
                    }
                    Some(ntag::T_String) => {
                        let s = val.unwrap().expect_string();
                        types_parsenodes::Node::String(types_parsenodes::StringNode {
                            sval: Some(s.sval.as_str().to_string()),
                        })
                    }
                    Some(ntag::T_Boolean) => {
                        let b = val.unwrap().expect_boolean();
                        types_parsenodes::Node::Boolean(types_parsenodes::Boolean { boolval: b.boolval })
                    }
                    Some(ntag::T_BitString) => {
                        let b = val.unwrap().expect_bitstring();
                        types_parsenodes::Node::BitString(types_parsenodes::BitString {
                            bsval: Some(b.bsval.as_str().to_string()),
                        })
                    }
                    _ => types_parsenodes::Node::A_Star,
                }
            }
            ntag::T_ColumnRef => {
                let cr = (&**tm).expect_columnref();
                if cr.fields.len() == 1 {
                    if let Some(s) = cr.fields[0].as_string() {
                        types_parsenodes::Node::String(types_parsenodes::StringNode {
                            sval: Some(s.sval.as_str().to_string()),
                        })
                    } else {
                        types_parsenodes::Node::A_Star
                    }
                } else {
                    types_parsenodes::Node::A_Star
                }
            }
            _ => types_parsenodes::Node::A_Star,
        };
        typmods.push(bridged);
    }

    let mut array_bounds: Vec<types_parsenodes::Node> = Vec::with_capacity(tn.arrayBounds.len());
    for n in tn.arrayBounds.iter() {
        match (&**n).node_tag() {
            ntag::T_Integer => {
                let i = (&**n).expect_integer();
                array_bounds.push(types_parsenodes::Node::Integer(types_parsenodes::Integer {
                    ival: i.ival,
                }))
            }
            _ => array_bounds
                .push(types_parsenodes::Node::Integer(types_parsenodes::Integer { ival: -1 })),
        }
    }

    Ok(types_parsenodes::TypeName {
        names,
        typeOid: tn.typeOid,
        setof: tn.setof,
        pct_type: tn.pct_type,
        typmods,
        typemod: tn.typemod,
        arrayBounds: array_bounds,
        location: tn.location,
    })
}

/// Convert one rich owned [`types_nodes::nodes::Node`] into the flat
/// [`types_parsenodes::Node`] the command bodies (`DefineType`,
/// `CreateFunction`, `CreateCast`, `RemoveObjects`) consume. Handles the leaf /
/// value / DDL-vocabulary node kinds that appear inside DEFINE / CREATE
/// FUNCTION / CREATE CAST / DROP statements: the value literals, `TypeName`
/// (through [`raw_typename_to_parse`]), `DefElem`, `ObjectWithArgs`,
/// `FunctionParameter`, and `List`. Recurses through nested args. Arbitrary
/// expression nodes (e.g. a parameter `DEFAULT` expression, or a non-value
/// `DefElem.arg`) are NOT yet expressible as a flat `parsenodes::Node`; those
/// raise loudly — they do not occur in the base-type / C-language DDL paths.
pub fn rich_node_to_parse(
    n: &types_nodes::nodes::Node<'_>,
) -> PgResult<types_parsenodes::Node> {
    use types_nodes::nodes::ntag;

    let out = match n.node_tag() {
        ntag::T_Integer => {
            let i = n.expect_integer();
            types_parsenodes::Node::Integer(types_parsenodes::Integer { ival: i.ival })
        }
        ntag::T_Float => {
            let f = n.expect_float();
            types_parsenodes::Node::Float(types_parsenodes::Float {
                fval: Some(f.fval.as_str().to_string()),
            })
        }
        ntag::T_Boolean => {
            let b = n.expect_boolean();
            types_parsenodes::Node::Boolean(types_parsenodes::Boolean {
                boolval: b.boolval,
            })
        }
        ntag::T_String => {
            let s = n.expect_string();
            types_parsenodes::Node::String(types_parsenodes::StringNode {
                sval: Some(s.sval.as_str().to_string()),
            })
        }
        ntag::T_BitString => {
            let b = n.expect_bitstring();
            types_parsenodes::Node::BitString(types_parsenodes::BitString {
                bsval: Some(b.bsval.as_str().to_string()),
            })
        }
        ntag::T_TypeName => {
            let tn = n.expect_typename();
            types_parsenodes::Node::TypeName(raw_typename_to_parse(tn)?)
        }
        ntag::T_DefElem => {
            let de = n.expect_defelem();
            types_parsenodes::Node::DefElem(rich_defelem_to_parse(de)?)
        }
        ntag::T_VariableSetStmt => {
            let vss = n.expect_variablesetstmt();
            types_parsenodes::Node::VariableSetStmt(rich_variablesetstmt_to_parse(vss)?)
        }
        ntag::T_ObjectWithArgs => {
            let owa = n.expect_objectwithargs();
            types_parsenodes::Node::ObjectWithArgs(rich_objectwithargs_to_parse(owa)?)
        }
        ntag::T_FunctionParameter => {
            let fp = n.expect_functionparameter();
            types_parsenodes::Node::FunctionParameter(rich_functionparameter_to_parse(fp)?)
        }
        ntag::T_List => {
            let l = n.expect_list();
            let mut out = Vec::with_capacity(l.len());
            for e in l.iter() {
                out.push(rich_node_to_parse(e)?);
            }
            types_parsenodes::Node::List(out)
        }
        other => {
            return Err(PgError::error(format!(
                "rich_node_to_parse: node tag {} not convertible to parsenodes",
                other.0
            )))
        }
    };
    Ok(out)
}

/// `DefElem` (rich → flat). `arg` (when present) is recursively converted.
pub fn rich_defelem_to_parse(
    de: &types_nodes::ddlnodes::DefElem<'_>,
) -> PgResult<types_parsenodes::DefElem> {
    let arg = match de.arg.as_deref() {
        Some(a) => Some(Box::new(rich_node_to_parse(a)?)),
        None => None,
    };
    Ok(types_parsenodes::DefElem {
        defnamespace: de.defnamespace.as_ref().map(|s| s.as_str().to_string()),
        defname: de.defname.as_ref().map(|s| s.as_str().to_string()),
        arg,
        defaction: de.defaction,
        location: de.location,
    })
}

/// `VariableSetStmt` (rich → flat). The `args` list (`A_Const` / `DefElem`
/// members) is recursively converted; the `jumble_args` field is not carried by
/// the flat node. This lets a `SET`/`RESET` subcommand embedded in another
/// statement (e.g. `CREATE FUNCTION ... SET x = y`, whose proconfig items are
/// `VariableSetStmt` nodes) round-trip through `rich_node_to_parse`.
pub fn rich_variablesetstmt_to_parse(
    vss: &types_nodes::ddlnodes::VariableSetStmt<'_>,
) -> PgResult<types_parsenodes::VariableSetStmt> {
    use types_nodes::ddlnodes::VariableSetKind as RichKind;
    use types_parsenodes::VariableSetKind as FlatKind;

    let kind = match vss.kind {
        RichKind::VAR_SET_VALUE => FlatKind::SetValue,
        RichKind::VAR_SET_DEFAULT => FlatKind::SetDefault,
        RichKind::VAR_SET_CURRENT => FlatKind::SetCurrent,
        RichKind::VAR_SET_MULTI => FlatKind::SetMulti,
        RichKind::VAR_RESET => FlatKind::Reset,
        RichKind::VAR_RESET_ALL => FlatKind::ResetAll,
    };
    // Each `args` member is an `A_Const` literal (or a `DefElem` for
    // VAR_SET_MULTI). The flat parsenodes universe has no `A_Const`; by
    // convention the inner value node is carried directly (mirrors the GUC
    // owner's `set_arg_from_nodes` flattener). Unwrap A_Const to its `val`.
    let mut args: Vec<types_parsenodes::Node> = Vec::with_capacity(vss.args.len());
    for n in vss.args.iter() {
        if n.node_tag() == types_nodes::nodes::ntag::T_A_Const {
            let c = n.expect_a_const();
            match c.val.as_deref() {
                Some(v) => args.push(rich_node_to_parse(v)?),
                // A NULL A_Const has no value node; SET literals are never NULL,
                // but carry an empty String to stay total.
                None => args.push(types_parsenodes::Node::String(
                    types_parsenodes::StringNode { sval: None },
                )),
            }
        } else {
            args.push(rich_node_to_parse(n)?);
        }
    }
    Ok(types_parsenodes::VariableSetStmt {
        kind,
        name: vss.name.as_ref().map(|s| s.as_str().to_string()),
        args,
        is_local: vss.is_local,
        location: vss.location,
    })
}

/// `ObjectWithArgs` (rich → flat). `objname` is a `List` of `String`s flattened
/// to `Vec<String>`; `objargs` / `objfuncargs` are node lists.
pub fn rich_objectwithargs_to_parse(
    owa: &types_nodes::ddlnodes::ObjectWithArgs<'_>,
) -> PgResult<types_parsenodes::ObjectWithArgs> {
    let mut objname: Vec<String> = Vec::with_capacity(owa.objname.len());
    for n in owa.objname.iter() {
        match n.as_string() {
            Some(s) => objname.push(s.sval.as_str().to_string()),
            None => {
                return Err(PgError::error(
                    "rich_objectwithargs_to_parse: objname element is not a String",
                ))
            }
        }
    }
    let mut objargs: Vec<types_parsenodes::Node> = Vec::with_capacity(owa.objargs.len());
    for n in owa.objargs.iter() {
        objargs.push(rich_node_to_parse(n)?);
    }
    let mut objfuncargs: Vec<types_parsenodes::Node> = Vec::with_capacity(owa.objfuncargs.len());
    for n in owa.objfuncargs.iter() {
        objfuncargs.push(rich_node_to_parse(n)?);
    }
    Ok(types_parsenodes::ObjectWithArgs {
        objname,
        objargs,
        objfuncargs,
        args_unspecified: owa.args_unspecified,
    })
}

/// `FunctionParameter` (rich → flat). `argType` is a `TypeName`; `defexpr` (a
/// `DEFAULT` expression) is not yet expressible as a flat node and raises.
pub fn rich_functionparameter_to_parse(
    fp: &types_nodes::ddlnodes::FunctionParameter<'_>,
) -> PgResult<types_parsenodes::FunctionParameter> {
    let argType = match fp.argType.as_deref() {
        Some(a) => Some(Box::new(rich_node_to_parse(a)?)),
        None => None,
    };
    let defexpr = match fp.defexpr.as_deref() {
        Some(a) => Some(Box::new(rich_node_to_parse(a)?)),
        None => None,
    };
    Ok(types_parsenodes::FunctionParameter {
        name: fp.name.as_ref().map(|s| s.as_str().to_string()),
        argType,
        mode: fp.mode as i8,
        defexpr,
        location: fp.location,
    })
}

/// `typenameTypeId(pstate, defGetTypeName(def))` (sequence.c `init_params`
/// AS-type leg). `defGetTypeName` requires the `DefElem`'s `arg` to be a
/// `TypeName` node; anything else raises (the C `elog`). The resolved
/// `TypeName` is looked up to a type OID, erroring if absent or shell-only.
fn seam_typename_type_id_from_defelem(
    def: &types_nodes::ddlnodes::DefElem<'_>,
) -> PgResult<Oid> {
    // defGetTypeName: the value must be an IsA(arg, TypeName) node.
    let tn = match def.arg.as_deref().and_then(|n| n.as_typename()) {
        Some(tn) => tn,
        _ => {
            let name = def.defname.as_ref().map(|s| s.as_str()).unwrap_or("");
            return Err(PgError::error(format!(
                "argument of \"{}\" must be a type name",
                name
            )));
        }
    };

    let tn_pn = raw_typename_to_parse(tn)?;
    let scratch = mcx::MemoryContext::new("typenameTypeIdFromDefElem");
    typenameTypeId(scratch.mcx(), None, &tn_pn)
}

/// `parse_type_string(str, soft)` — `parseTypeString(str, &typeid, &typmod,
/// escontext)` with the out-params/boolean folded into the result.
fn seam_parse_type_string(
    string: &str,
    soft: bool,
) -> PgResult<Result<(Oid, i32), types_error::PgError>> {
    let scratch = mcx::MemoryContext::new("parse_type_string");
    if soft {
        // C threads the caller's escontext straight through; here the boundary
        // takes only `soft`, so capture the soft `ereturn` into a local
        // ErrorSaveContext and hand the recorded PgError back to the caller, who
        // reflects it into its own sink (preserving message/detail/hint/sqlstate).
        let mut escontext = SoftErrorContext::new(true);
        match parseTypeString(scratch.mcx(), string, Some(&mut escontext))? {
            Some(pair) => Ok(Ok(pair)),
            None => {
                // A soft failure recorded its error into escontext. Surface it so
                // the caller carries the real message (e.g. `pg_input_error_info`).
                let err = escontext.take_error().unwrap_or_else(|| {
                    types_error::PgError::error("invalid type name").with_sqlstate(
                        types_error::ERRCODE_SYNTAX_ERROR,
                    )
                });
                Ok(Err(err))
            }
        }
    } else {
        match parseTypeString(scratch.mcx(), string, None)? {
            Some(pair) => Ok(Ok(pair)),
            // With escontext = None, parseTypeString hard-raises on failure
            // (Err), so the Ok(None) arm is unreachable.
            None => unreachable!("parseTypeString(str, NULL) never returns Ok(None)"),
        }
    }
}

/// `typeStringToTypeName(str, NULL)` — parse a type-name string into a
/// raw-parser `TypeName` node, rejecting `SETOF`. With `escontext = NULL` the
/// `fail:` routes hard-raise, so the result is always a real `TypeName` (or an
/// `Err`); the `Ok(None)` arm is unreachable here.
fn seam_type_string_to_type_name(string: &str) -> PgResult<TypeName> {
    match typeStringToTypeName(string, None)? {
        Some(tn) => Ok(tn),
        // Unreachable: with escontext = None the fail_type_string route raises
        // (Err) instead of returning Ok(None). Mirror the C: a NULL TypeName out
        // of typeStringToTypeName with no soft-error sink cannot occur.
        None => unreachable!("typeStringToTypeName(str, NULL) never returns NULL"),
    }
}

/// `NameListToString(names)` — render a possibly-qualified name into a dotted
/// string, allocated in `mcx`.
fn seam_name_list_to_string<'mcx>(
    mcx: Mcx<'mcx>,
    names: &[mcx::PgString<'_>],
) -> PgResult<mcx::PgString<'mcx>> {
    let mut out = mcx::PgString::new_in(mcx);
    for (i, name) in names.iter().enumerate() {
        if i != 0 {
            out.try_push('.')?;
        }
        out.try_push_str(name.as_str())?;
    }
    Ok(out)
}

/// `typenameTypeId(NULL, typeName)` over the trimmed `types_opclass::TypeName`.
fn seam_typename_type_id(type_name: &types_opclass::TypeName) -> PgResult<Oid> {
    let scratch = mcx::MemoryContext::new("typenameTypeId");
    let tn = from_opclass_typename(type_name);
    typenameTypeId(scratch.mcx(), None, &tn)
}

/// `typenameTypeIdAndMod(NULL, typeName, ...)` over the full
/// `types_parsenodes::TypeName` (carries the `typmods` decoration the typmod
/// resolution needs).
fn seam_typename_type_id_and_mod(type_name: &TypeName) -> PgResult<(Oid, i32)> {
    let scratch = mcx::MemoryContext::new("typenameTypeIdAndMod");
    typenameTypeIdAndMod(scratch.mcx(), None, type_name)
}

/// `typeTypeId(LookupTypeName(NULL, typeName, NULL, false))` over the trimmed
/// `types_opclass::TypeName` — the shell-allowing OID resolver `AlterTypeOwner`
/// uses (returns a shell type rather than rejecting it).
fn seam_lookup_type_name_oid_from_names(type_name: &types_opclass::TypeName) -> PgResult<Oid> {
    let scratch = mcx::MemoryContext::new("LookupTypeNameOid");
    let tn = from_opclass_typename(type_name);
    LookupTypeNameOid(scratch.mcx(), None, &tn, false)
}

/// `TypeNameToString(typeName)` over the trimmed `types_opclass::TypeName`.
fn seam_typename_to_string<'mcx>(
    mcx: Mcx<'mcx>,
    type_name: &types_opclass::TypeName,
) -> PgResult<mcx::PgString<'mcx>> {
    let tn = from_opclass_typename(type_name);
    let s = TypeNameToString(&tn)?;
    let mut out = mcx::PgString::new_in(mcx);
    out.try_push_str(&s)?;
    Ok(out)
}

/// `TypeNameToString(typeName)` over the raw-parser `types_parsenodes::TypeName`.
fn seam_typename_to_string_node<'mcx>(
    mcx: Mcx<'mcx>,
    type_name: &TypeName,
) -> PgResult<mcx::PgString<'mcx>> {
    let s = TypeNameToString(type_name)?;
    let mut out = mcx::PgString::new_in(mcx);
    out.try_push_str(&s)?;
    Ok(out)
}

/// `LookupTypeNameOid(NULL, typeName, missing_ok)` over the raw-parser node.
fn seam_lookup_type_name_oid(type_name: &TypeName, missing_ok: bool) -> PgResult<Oid> {
    let scratch = mcx::MemoryContext::new("LookupTypeNameOid");
    LookupTypeNameOid(scratch.mcx(), None, type_name, missing_ok)
}

/// `typenameTypeId(NULL, typeName)` over the raw-parser node.
fn seam_typename_type_id_node(type_name: &TypeName) -> PgResult<Oid> {
    let scratch = mcx::MemoryContext::new("typenameTypeId");
    typenameTypeId(scratch.mcx(), None, type_name)
}

/// `typenameTypeId(NULL, typeName)` over the owned-tree `rawnodes::TypeName`
/// (PREPARE's `argtypes`); bridges through `raw_typename_to_parse`.
fn seam_typename_type_id_raw(type_name: &types_nodes::rawnodes::TypeName<'_>) -> PgResult<Oid> {
    let tn = raw_typename_to_parse(type_name)?;
    let scratch = mcx::MemoryContext::new("typenameTypeId");
    typenameTypeId(scratch.mcx(), None, &tn)
}

/// `typenameTypeId(pstate, typeName)` over a raw-tree `TypeName`, threading the
/// active `ParseState` so a "type does not exist" error carries the cursor
/// position (`parser_errposition(pstate, typeName->location)`).
fn seam_typename_type_id_raw_pstate(
    pstate: &types_cluster::ParseState<'_>,
    type_name: &types_nodes::rawnodes::TypeName<'_>,
) -> PgResult<Oid> {
    let tn = raw_typename_to_parse(type_name)?;
    let scratch = mcx::MemoryContext::new("typenameTypeId");
    typenameTypeId(scratch.mcx(), Some(pstate), &tn)
}

/// `TypeNameListToString(typenames)` over a list of raw-parser nodes.
fn seam_type_name_list_to_string<'mcx>(
    mcx: Mcx<'mcx>,
    typenames: &[TypeName],
) -> PgResult<mcx::PgString<'mcx>> {
    let s = TypeNameListToString(typenames)?;
    let mut out = mcx::PgString::new_in(mcx);
    out.try_push_str(&s)?;
    Ok(out)
}

/// `LookupTypeNameOid(NULL, typeName, missing_ok)` (parse_type.c:232) over the
/// opclasscmds/function `types_opclass::TypeName` carrier — the `ObjectWithArgs`
/// `objargs` path of `LookupFuncWithArgs` (parse_func.c).
fn seam_lookup_type_name_oid_owa(
    type_name: &types_opclass::TypeName,
    missing_ok: bool,
) -> PgResult<Oid> {
    let scratch = mcx::MemoryContext::new("LookupTypeNameOid");
    let tn = from_opclass_typename(type_name);
    LookupTypeNameOid(scratch.mcx(), None, &tn, missing_ok)
}

/// `FuncNameAsType(funcname)` (parse_func.c:1881): treat a function name as a
/// type-coercion target. `LookupTypeNameExtended(NULL,
/// makeTypeNameFromNameList(funcname), NULL, false, false)` then keep the OID
/// only for a fully-defined, non-composite (scalar/domain) type.
fn seam_func_name_as_type(funcname: &[mcx::PgString<'_>]) -> PgResult<Oid> {
    let scratch = mcx::MemoryContext::new("FuncNameAsType");

    /*
     * temp_ok=false protects the contract for writing SECURITY DEFINER
     * functions safely.
     */
    let typeName = makeTypeNameFromNameList(funcname);
    let typtup = LookupTypeNameExtended(scratch.mcx(), None, &typeName, false, false)?;
    let (typtup, _typmod) = match typtup {
        Some(t) => t,
        None => return Ok(InvalidOid),
    };

    if typtup.typisdefined && !OidIsValid(typeTypeRelid(typtup)) {
        typeTypeId(Some(typtup))
    } else {
        Ok(InvalidOid)
    }
}

/// `makeTypeNameFromNameList(names)` (makefuncs.c) over a `String`-list function
/// name: a `TypeName` carrying just the name components (no typmods/array
/// bounds; `typemod = -1`, `location = -1`).
fn makeTypeNameFromNameList(names: &[mcx::PgString<'_>]) -> TypeName {
    TypeName {
        names: names
            .iter()
            .map(|n| {
                Node::String(types_parsenodes::StringNode {
                    sval: Some(n.as_str().to_string()),
                })
            })
            .collect(),
        typeOid: InvalidOid,
        setof: false,
        pct_type: false,
        typmods: Vec::new(),
        typemod: -1,
        arrayBounds: Vec::new(),
        location: -1,
    }
}

/// Convert the trimmed `types_opclass::TypeName` (names as `Vec<String>`) into
/// the canonical raw-parser `types_parsenodes::TypeName` (names as `Vec<Node>`
/// of `String` nodes). The trimmed node carries no typmods/arrayBounds, so the
/// rendering / lookup behaves as a plain qualified name.
fn from_opclass_typename(tn: &types_opclass::TypeName) -> TypeName {
    TypeName {
        names: tn
            .names
            .iter()
            .map(|n| Node::String(types_parsenodes::StringNode { sval: Some(n.clone()) }))
            .collect(),
        typeOid: tn.typeOid,
        setof: tn.setof,
        pct_type: tn.pct_type,
        typmods: Vec::new(),
        typemod: tn.typemod,
        // Rebuild the `List *arrayBounds` (Integer A_Const nodes) so
        // LookupTypeName takes its `get_array_type` branch for array types
        // (e.g. a domain over `int[]`). C carries the raw A_Const dims; only
        // emptiness drives resolution, but we round-trip the integer bound.
        arrayBounds: tn
            .arrayBounds
            .iter()
            .map(|&b| Node::Integer(types_parsenodes::Integer { ival: b }))
            .collect(),
        location: tn.location,
    }
}

#[cfg(test)]
mod tests;
