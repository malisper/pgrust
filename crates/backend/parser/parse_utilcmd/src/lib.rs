// CREATE TABLE plain-column + LIKE lanes + the parse_type.c slice they need,
// plus the SERIAL expansion (generateSerialExtraStmts) and its
// ruleutils/indexcmds helpers (quote_identifier, makeObjectName,
// ChooseRelationName).
#![allow(non_snake_case)]

mod like;
pub use like::{expandTableLikeClause, generateClonedIndexStmt};

use mcx::{Mcx, PgString};
use types_core::{InvalidOid, Oid, INT2OID, INT4OID, INT8OID, NAMEDATALEN};
use types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_TABLE_DEFINITION, ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_OBJECT,
    ERRCODE_UNDEFINED_SCHEMA, ERROR,
};
use types_nodes::rawnodes::{
    ColumnDef, Constraint, ConstrType, CreateSeqStmt, CreateStmt, IndexElem, IndexStmt, SortByDir,
    SortByNulls, TypeName,
};
use types_nodes::parsenodes::{DefElem, DefElemAction};
use types_nodes::{
    AlterSeqStmt, CoercionForm, FuncCall, Node, NodeList, NodeTag, RangeVar, TypeCast, ValUnion,
};

#[cold]
#[inline(never)]
pub(crate) fn unported(what: &str) -> ! {
    panic!("unported: parse_utilcmd {what}")
}

#[track_caller]
#[cold]
#[inline(never)]
fn type_does_not_exist(name: &str) -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, format!("type \"{name}\" does not exist"))
            .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn type_is_only_a_shell(name: &str) -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, format!("type \"{name}\" is only a shell"))
            .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
    )
}

// Clean 0A000 for unported-feature lanes (user-reachable stubs must raise,
// not panic); errposition attaches when the surrounding code has a pstate.
#[track_caller]
#[cold]
#[inline(never)]
fn unported_feature_at(
    pstate: Option<&parser_small1::ParseState<'_, '_>>,
    location: i32,
    what: &str,
) -> Box<PgError> {
    let mut e = Box::new(
        PgError::new(ERROR, format!("{what} is not supported yet"))
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    );
    if let Some(ps) = pstate {
        let pos =
            parser_small1::parser_errposition(ps, location, mbutils::GetDatabaseEncoding());
        if pos > 0 {
            e.cursor_position = Some(pos);
        }
    }
    e
}

// typenameTypeIdAndMod (parse_type.c); pstate feeds errposition around the
// typmodin call (C's setup_parser_errposition_callback).
pub fn typenameTypeIdAndMod<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: Option<&parser_small1::ParseState<'_, '_>>,
    tn: &TypeName<'_>,
) -> PgResult<(Oid, i32)> {
    typename_type_id_and_mod(mcx, pstate, tn)
}

// Alias kept for composite-consumer call sites; C's typenameTypeIdAndMod
// never had a typtype gate, so both entry points are the same lane.
pub fn typenameTypeIdAndModAllowComposite<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: Option<&parser_small1::ParseState<'_, '_>>,
    tn: &TypeName<'_>,
) -> PgResult<(Oid, i32)> {
    typename_type_id_and_mod(mcx, pstate, tn)
}

fn typename_type_id_and_mod<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: Option<&parser_small1::ParseState<'_, '_>>,
    tn: &TypeName<'_>,
) -> PgResult<(Oid, i32)> {
    if tn.pct_type || tn.setof {
        // unported: C's LookupTypeName resolves %TYPE to the referenced
        // column's type and ignores SETOF here; clean 0A000 until ported.
        return Err(unported_feature_at(
            pstate,
            tn.location,
            "%TYPE and SETOF type references",
        ));
    }
    if tn.names.is_nil() {
        // LookupTypeName pre-resolved arm (makeTypeNameFromOid; LIKE / OF type).
        assert!(tn.typeOid != InvalidOid, "TypeName without names or typeOid");
        match syscache_seams::pg_type_isdefined::call(tn.typeOid)? {
            Some(true) => {}
            _ => unported("shell types (typisdefined = false)"),
        }
        // C typenameTypeIdAndMod applies no typtype gate on the pre-resolved
        // lane (LIKE / OF type); column legality is CheckAttributeType's job.
        let typmod = typenameTypeMod(mcx, pstate, tn, tn.typeOid)?;
        return Ok((tn.typeOid, typmod));
    }
    if tn.typeOid != InvalidOid {
        debug_assert!(tn.names.is_nil());
        return Ok((tn.typeOid, -1));
    }

    // C typenameType attaches parser_errposition(pstate, typeName->location)
    // to every lookup error on this path.
    let at_tn = |mut e: Box<PgError>| {
        if let Some(ps) = pstate {
            let pos =
                parser_small1::parser_errposition(ps, tn.location, mbutils::GetDatabaseEncoding());
            if e.cursor_position.is_none() && pos > 0 {
                e.cursor_position = Some(pos);
            }
        }
        e
    };
    let (typoid, typname) = resolveTypeNames(mcx, tn)?;
    if typoid == InvalidOid {
        return Err(at_tn(type_does_not_exist(typname)));
    }
    // C LookupTypeNameExtended: array bounds convert to the array type.
    let typoid = if tn.arrayBounds.is_nil() {
        typoid
    } else {
        let arr = syscache_seams::pg_type_typarray::call(typoid)?.unwrap_or(InvalidOid);
        if arr == InvalidOid {
            return Err(at_tn(type_does_not_exist(typname)));
        }
        arr
    };
    match syscache_seams::pg_type_isdefined::call(typoid)? {
        Some(true) => {}
        _ => return Err(at_tn(type_is_only_a_shell(typname))),
    }
    // C typenameTypeIdAndMod has no typtype gate; column legality is
    // CheckAttributeType's job (heap.c: column "u" has pseudo-type unknown).
    match syscache_seams::pg_type_typtype::call(typoid)? {
        Some(_) => {}
        None => return Err(type_does_not_exist(typname)),
    }
    let typmod = typenameTypeMod(mcx, pstate, tn, typoid)?;
    Ok((typoid, typmod))
}

// typenameTypeId (parse_type.c): PREPARE/DDL argument types — no column-lane
// typtype gate (pseudo-types like unknown are legal); pstate feeds errposition.
pub fn typenameTypeId<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: Option<&parser_small1::ParseState<'_, '_>>,
    tn: &TypeName<'_>,
) -> PgResult<Oid> {
    if tn.pct_type || tn.setof {
        // unported: C's LookupTypeName resolves %TYPE to the referenced
        // column's type and ignores SETOF here; clean 0A000 until ported.
        return Err(unported_feature_at(
            pstate,
            tn.location,
            "%TYPE and SETOF type references",
        ));
    }
    if tn.names.is_nil() || tn.typeOid != InvalidOid {
        unported("pre-resolved TypeName.typeOid lane");
    }
    let (typoid, typname) = resolveTypeNames(mcx, tn)?;
    let at_tn = |mut e: Box<PgError>| {
        if let Some(ps) = pstate {
            let pos =
                parser_small1::parser_errposition(ps, tn.location, mbutils::GetDatabaseEncoding());
            if pos > 0 {
                e.cursor_position = Some(pos);
            }
        }
        e
    };
    let not_exist = |typname: &str| at_tn(type_does_not_exist(typname));
    if typoid == InvalidOid {
        return Err(not_exist(typname));
    }
    let typoid = if tn.arrayBounds.is_nil() {
        typoid
    } else {
        let arr = syscache_seams::pg_type_typarray::call(typoid)?.unwrap_or(InvalidOid);
        if arr == InvalidOid {
            return Err(not_exist(typname));
        }
        arr
    };
    match syscache_seams::pg_type_isdefined::call(typoid)? {
        Some(true) => {}
        // unported: C's typenameTypeId path (typenameType) raises exactly
        // this shell-type error; the shell-type USE lanes stay unported.
        _ => return Err(at_tn(type_is_only_a_shell(typname))),
    }
    Ok(typoid)
}

// LookupTypeNameOid (parse_type.c): plain resolution, no column-lane typtype
// restriction (operator/opclass DDL accepts pseudo-types like internal).
pub fn LookupTypeNameOid<'mcx>(mcx: Mcx<'mcx>, tn: &TypeName<'_>) -> PgResult<Oid> {
    LookupTypeNameOidExtended(mcx, tn, false)
}

pub fn LookupTypeNameOidExtended<'mcx>(
    mcx: Mcx<'mcx>,
    tn: &TypeName<'_>,
    missing_ok: bool,
) -> PgResult<Oid> {
    if tn.pct_type || tn.setof {
        // unported: C's LookupTypeName resolves %TYPE to the referenced
        // column's type and ignores SETOF here; clean 0A000 until ported
        // (reachable via CREATE AGGREGATE/OPERATOR argument types).
        return Err(unported_feature_at(
            None,
            tn.location,
            "%TYPE and SETOF type references",
        ));
    }
    if tn.names.is_nil() || tn.typeOid != InvalidOid {
        unported("pre-resolved TypeName.typeOid lane");
    }
    let (typoid, typname) = resolve_type_names_ext(mcx, tn, missing_ok)?;
    if typoid == InvalidOid {
        if missing_ok {
            return Ok(InvalidOid);
        }
        return Err(type_does_not_exist(typname));
    }
    let typoid = if tn.arrayBounds.is_nil() {
        typoid
    } else {
        let arr = syscache_seams::pg_type_typarray::call(typoid)?.unwrap_or(InvalidOid);
        if arr == InvalidOid {
            return Err(type_does_not_exist(typname));
        }
        arr
    };
    match syscache_seams::pg_type_isdefined::call(typoid)? {
        Some(true) => {}
        // unported: C's LookupTypeNameOid returns shell types (their DDL
        // consumers accept them); pgrust's shell-type USE lanes are
        // unported, so raise the typenameType-shaped error cleanly.
        _ => return Err(type_is_only_a_shell(typname)),
    }
    Ok(typoid)
}

// The names→Oid walk shared by typenameTypeIdAndMod and parseTypeString
// (LookupTypeNameExtended's "normal reference" arm, pre array-bounds).
fn resolveTypeNames<'mcx, 'tn>(
    mcx: Mcx<'mcx>,
    tn: &TypeName<'tn>,
) -> PgResult<(Oid, &'tn str)> {
    resolve_type_names_ext(mcx, tn, false)
}

// LookupTypeNameExtended's missing_ok also covers the explicit-schema
// lookup: a missing schema yields InvalidOid ("type does not exist" at the
// caller) instead of a schema error.
fn resolve_type_names_ext<'mcx, 'tn>(
    mcx: Mcx<'mcx>,
    tn: &TypeName<'tn>,
    missing_ok: bool,
) -> PgResult<(Oid, &'tn str)> {
    let mut names: [&str; 4] = [""; 4];
    let nnames = tn.names.len();
    if nnames == 0 || nnames > 3 {
        // C DeconstructQualifiedName's default arm (namespace.c).
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!(
                    "improper qualified name (too many dotted names): {}",
                    typename_to_string(tn)
                ),
            )
            .with_sqlstate(ERRCODE_SYNTAX_ERROR),
        ));
    }
    for (i, n) in tn.names.iter().enumerate() {
        names[i] = n.as_string().expect("TypeName names").sval;
    }
    let (schemaname, typname) = catalog_namespace::DeconstructQualifiedName(&names[..nnames])?;

    let typoid = match schemaname {
        Some(schemaname) => {
            let namespace_id = catalog_namespace::LookupExplicitNamespace(schemaname, missing_ok)?;
            if namespace_id == InvalidOid {
                return Ok((InvalidOid, typname));
            }
            syscache_seams::lookup_pg_type_oid_by_name::call(typname, namespace_id)?
        }
        None => {
            // TypenameGetTypidExtended walk; temp_ok arm unreachable (no temp rels).
            let mut found = InvalidOid;
            for &namespace_id in catalog_namespace::fetch_search_path(mcx, true)?.iter() {
                found = syscache_seams::lookup_pg_type_oid_by_name::call(typname, namespace_id)?;
                if found != InvalidOid {
                    break;
                }
            }
            found
        }
    };
    Ok((typoid, typname))
}

// TypeNameToString (parse_type.c), error-message shape only ("[]" appended
// for array bounds, per appendTypeNameToBuffer).
fn typeNameToString(tn: &TypeName<'_>) -> String {
    let mut s = typename_to_string(tn);
    if !tn.arrayBounds.is_nil() {
        s.push_str("[]");
    }
    s
}

#[track_caller]
#[cold]
#[inline(never)]
fn invalid_type_name(s: &str) -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, format!("invalid type name \"{s}\""))
            .with_sqlstate(ERRCODE_SYNTAX_ERROR),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn shell_type(name: &str) -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, format!("type \"{name}\" is only a shell"))
            .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
    )
}

// typeStringToTypeName (parse_type.c). Only the function's own "invalid type
// name" arms are escontext-soft; raw-parse errors are hard with
// pts_error_callback riding as with_context, matching the C callback's span.
pub fn typeStringToTypeNameEsc<'mcx>(
    mcx: Mcx<'mcx>,
    s: &str,
    esc: Option<&mut SoftErrorContext>,
) -> PgResult<Option<&'mcx TypeName<'mcx>>> {
    if s.bytes().all(|c| matches!(c, b' ' | b'\t' | b'\n' | b'\r' | 0x0c | 0x0b)) {
        return ereturn(esc, None, *invalid_type_name(s));
    }
    let list = gram_core::raw_parser(mcx, s, parser_seams::RawParseMode::RAW_PARSE_TYPE_NAME)
        .map_err(|e| Box::new((*e).with_context(format!("invalid type name \"{s}\""))))?;
    debug_assert_eq!(list.len(), 1);
    let node = list.first().expect("TYPE_NAME parse yields one node");
    let tn = node.as_type_name().expect("TYPE_NAME parse yields TypeName");
    if tn.setof {
        return ereturn(esc, None, *invalid_type_name(s));
    }
    Ok(Some(tn))
}

pub fn typeStringToTypeName<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx TypeName<'mcx>> {
    Ok(typeStringToTypeNameEsc(mcx, s, None)?.expect("escontext=NULL errors instead of None"))
}

/// C `parseTypeString`: (type Oid, typmod) for a standalone type-name string;
/// None when a soft error was captured into `esc`. Soft arms are exactly C's
/// ereturn sites (invalid type name, type does not exist, shell type); name
/// deconstruction and typmodin errors stay hard. No typtype restriction
/// (unlike the CREATE TABLE lane above): any resolvable non-shell type
/// passes, per C.
pub fn parseTypeStringEsc<'mcx>(
    mcx: Mcx<'mcx>,
    s: &str,
    mut esc: Option<&mut SoftErrorContext>,
) -> PgResult<Option<(Oid, i32)>> {
    let Some(tn) = typeStringToTypeNameEsc(mcx, s, esc.as_deref_mut())? else {
        return Ok(None);
    };
    if tn.pct_type {
        unported("LookupTypeName %TYPE");
    }
    if tn.typeOid != InvalidOid {
        unported("pre-resolved TypeName.typeOid lane");
    }

    // C: LookupTypeName(NULL, typeName, ..., missing_ok = escontext is an
    // ErrorSaveContext) — a missing schema soft-NULLs under a soft context.
    let (mut typoid, _typname) = resolve_type_names_ext(mcx, tn, esc.is_some())?;
    if typoid != InvalidOid && !tn.arrayBounds.is_nil() {
        typoid = syscache_seams::pg_type_typarray::call(typoid)?.unwrap_or(InvalidOid);
    }
    if typoid == InvalidOid {
        return ereturn(esc, None, *type_does_not_exist(&typeNameToString(tn)));
    }

    match syscache_seams::pg_type_isdefined::call(typoid)? {
        Some(true) => {}
        Some(false) => return ereturn(esc, None, *shell_type(&typeNameToString(tn))),
        None => return ereturn(esc, None, *type_does_not_exist(&typeNameToString(tn))),
    }

    let typmod = typenameTypeMod(mcx, None, tn, typoid)?;
    Ok(Some((typoid, typmod)))
}

pub fn parseTypeString<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<(Oid, i32)> {
    Ok(parseTypeStringEsc(mcx, s, None)?.expect("escontext=NULL errors instead of None"))
}

fn typename_to_string(tn: &TypeName<'_>) -> String {
    let mut s = String::new();
    for n in tn.names.iter() {
        if !s.is_empty() {
            s.push('.');
        }
        s.push_str(n.as_string().map(|v| v.sval).unwrap_or("?"));
    }
    s
}

// typenameTypeMod (parse_type.c): raw typmods -> cstring[] -> typmodin.
pub fn typenameTypeMod<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: Option<&parser_small1::ParseState<'_, '_>>,
    tn: &TypeName<'_>,
    typoid: Oid,
) -> PgResult<i32> {
    use types_nodes::rawnodes::{ColumnRef, ValUnion};

    if tn.typmods.is_nil() {
        return Ok(tn.typemod);
    }

    let io = syscache_seams::pg_type_io_shape::call(typoid)?
        .unwrap_or_else(|| unported("typmod on a type without an io shape row"));
    if !io.typisdefined {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!(
                    "type modifier cannot be specified for shell type \"{}\"",
                    typename_to_string(tn)
                ),
            )
            .with_sqlstate(ERRCODE_SYNTAX_ERROR),
        ));
    }
    if io.typmodin == InvalidOid {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!("type modifier is not allowed for type \"{}\"", typename_to_string(tn)),
            )
            .with_sqlstate(ERRCODE_SYNTAX_ERROR),
        ));
    }

    #[track_caller]
    #[cold]
    fn bad_typmod_expr() -> Box<PgError> {
        Box::new(
            PgError::new(ERROR, "type modifiers must be simple constants or identifiers")
                .with_sqlstate(ERRCODE_SYNTAX_ERROR),
        )
    }

    let mut cstrings: Vec<mcx::PgVec<'mcx, u8>> = Vec::with_capacity(tn.typmods.len());
    for tm in tn.typmods.iter() {
        let cstr: Option<String> = if let Some(ac) = tm.as_a_const() {
            match ac.val {
                Some(ValUnion::Integer(i)) => Some(i.ival.to_string()),
                Some(ValUnion::Float(f)) => Some(f.fval.to_string()),
                Some(ValUnion::String(s)) => Some(s.sval.to_string()),
                _ => None,
            }
        } else if let Some(cr) = tm.as_variant::<ColumnRef>() {
            match (cr.fields.len(), cr.fields.first().and_then(|f| f.as_string())) {
                (1, Some(s)) => Some(s.sval.to_string()),
                _ => None,
            }
        } else {
            None
        };
        let Some(cstr) = cstr else {
            return Err(bad_typmod_expr());
        };
        let mut v = mcx::vec_with_capacity_in(mcx, cstr.len() + 1)?;
        mcx::vec_append_bytes(&mut v, cstr.as_bytes())?;
        mcx::vec_append_bytes(&mut v, &[0u8])?;
        cstrings.push(v);
    }
    let datums: Vec<datum::Datum> = cstrings
        .iter()
        .map(|v| datum::Datum::from_usize(v.as_ptr() as usize))
        .collect();
    let img = datum::array_build::construct_array_image(mcx, &datums, types_core::CSTRINGOID, -2, false, b'c')?;

    let mut flinfo = types_fmgr::FmgrInfo::unresolved();
    fmgr_core::fmgr_info_into(io.typmodin, &mut flinfo)?;

    // setup_parser_errposition_callback: reports emitted inside the typmodin
    // call (e.g. intervaltypmodin's precision WARNING) carry the cursor.
    let cb = pstate.map(|ps| {
        let pos = parser_small1::parser_errposition(ps, tn.location, mbutils::GetDatabaseEncoding());
        elog::push_emit_context_callback(Box::new(move |err| {
            if err.cursor_position.is_none() && pos > 0 {
                err.cursor_position = Some(pos);
            }
        }))
    });
    let d = fmgr_core::function_call1_coll_in(
        &mut flinfo,
        InvalidOid,
        mcx,
        datum::Datum::from_usize(img.as_ptr() as usize),
    );
    if let Some(id) = cb {
        elog::pop_emit_context_callback(id);
    }
    match d {
        Ok(v) => Ok(v.as_i32()),
        Err(mut e) => {
            if let Some(ps) = pstate {
                if e.cursor_position.is_none() {
                    let pos = parser_small1::parser_errposition(
                        ps,
                        tn.location,
                        mbutils::GetDatabaseEncoding(),
                    );
                    if pos > 0 {
                        e.cursor_position = Some(pos);
                    }
                }
            }
            Err(e)
        }
    }
}

pub struct CreateStmtCxt<'mcx> {
    // C cxt->stmtType: "CREATE [FOREIGN] TABLE" or "ALTER [FOREIGN] TABLE",
    // for the implicit-sequence DEBUG1 report (and error messages).
    pub stmt_type: &'static str,
    pub blist: NodeList<'mcx>,
    pub alist: NodeList<'mcx>,
    pub ckconstraints: NodeList<'mcx>,
    pub nnconstraints: NodeList<'mcx>,
    // ALTER path only: IndexStmts from transformIndexConstraints (C folds
    // them into AT_AddIndex[Constraint] cmds, parse_utilcmd.c:3817-3838) and
    // FK Constraints for AT_AddConstraint cmds (parse_utilcmd.c:3857-3863).
    pub ixstmts: NodeList<'mcx>,
    pub fkconstraints: NodeList<'mcx>,
}

impl<'mcx> CreateStmtCxt<'mcx> {
    fn new(stmt_type: &'static str) -> Self {
        CreateStmtCxt {
            stmt_type,
            blist: NodeList::nil(),
            alist: NodeList::nil(),
            ckconstraints: NodeList::nil(),
            nnconstraints: NodeList::nil(),
            ixstmts: NodeList::nil(),
            fkconstraints: NodeList::nil(),
        }
    }
}

// setSchemaName (parse_utilcmd.c). RangeVars sit behind shared refs, so a
// missing schemaname is fixed by swapping in a stamped copy (C scribbles).
fn set_schema_name<'mcx>(
    mcx: Mcx<'mcx>,
    context_schema: &str,
    rv: Option<&'mcx RangeVar<'mcx>>,
) -> PgResult<Option<&'mcx RangeVar<'mcx>>> {
    let rv = rv.expect("schema element names a relation");
    match rv.schemaname {
        None => {
            let stamped = RangeVar {
                catalogname: rv.catalogname,
                schemaname: Some(crate::like::str_in(mcx, context_schema)?),
                relname: rv.relname,
                inh: rv.inh,
                relpersistence: rv.relpersistence,
                alias: rv.alias,
                location: rv.location,
            };
            Ok(Some(Node::mk_mut(mcx, stamped)?.seal_ref()))
        }
        Some(s) if s != context_schema => Err(Box::new(
            PgError::new(
                ERROR,
                format!(
                    "CREATE specifies a schema ({s}) different from the one being created ({context_schema})"
                ),
            )
            .with_sqlstate(types_error::ERRCODE_INVALID_SCHEMA_DEFINITION),
        )),
        _ => Ok(None),
    }
}

// transformCreateSchemaStmtElements (parse_utilcmd.c): reorganize CREATE
// SCHEMA elements into a sequentially executable order and stamp the schema.
pub fn transformCreateSchemaStmtElements<'mcx>(
    mcx: Mcx<'mcx>,
    schema_elts: &NodeList<'mcx>,
    schema_name: &str,
) -> PgResult<NodeList<'mcx>> {
    use types_nodes::rawnodes::{CreateTrigStmt, ViewStmt};
    let mut sequences = NodeList::nil();
    let mut tables = NodeList::nil();
    let mut views = NodeList::nil();
    let mut indexes = NodeList::nil();
    let mut triggers = NodeList::nil();
    let mut grants = NodeList::nil();
    for element in schema_elts.iter() {
        // SAFETY (each with_mut): parse tree is analyze-owned; no derived
        // refs live across the write.
        match element.node_tag() {
            NodeTag::T_CreateSeqStmt => {
                let s = element.as_variant::<CreateSeqStmt>().expect("CreateSeqStmt");
                if let Some(rv) = set_schema_name(mcx, schema_name, s.sequence)? {
                    unsafe {
                        element
                            .with_mut::<CreateSeqStmt, _>(|s| s.sequence = Some(rv))
                            .expect("CreateSeqStmt");
                    }
                }
                sequences.lappend(mcx, element)?;
            }
            NodeTag::T_CreateStmt => {
                let s = element.as_variant::<CreateStmt>().expect("CreateStmt");
                if let Some(rv) = set_schema_name(mcx, schema_name, s.relation)? {
                    unsafe {
                        element
                            .with_mut::<CreateStmt, _>(|s| s.relation = Some(rv))
                            .expect("CreateStmt");
                    }
                }
                tables.lappend(mcx, element)?;
            }
            NodeTag::T_ViewStmt => {
                let s = element.as_variant::<ViewStmt>().expect("ViewStmt");
                if let Some(rv) = set_schema_name(mcx, schema_name, s.view)? {
                    unsafe {
                        element
                            .with_mut::<ViewStmt, _>(|s| s.view = Some(rv))
                            .expect("ViewStmt");
                    }
                }
                views.lappend(mcx, element)?;
            }
            NodeTag::T_IndexStmt => {
                let s = element.as_variant::<IndexStmt>().expect("IndexStmt");
                if let Some(rv) = set_schema_name(mcx, schema_name, s.relation)? {
                    unsafe {
                        element
                            .with_mut::<IndexStmt, _>(|s| s.relation = Some(rv))
                            .expect("IndexStmt");
                    }
                }
                indexes.lappend(mcx, element)?;
            }
            NodeTag::T_CreateTrigStmt => {
                let s = element.as_variant::<CreateTrigStmt>().expect("CreateTrigStmt");
                if let Some(rv) = set_schema_name(mcx, schema_name, s.relation)? {
                    unsafe {
                        element
                            .with_mut::<CreateTrigStmt, _>(|s| s.relation = Some(rv))
                            .expect("CreateTrigStmt");
                    }
                }
                triggers.lappend(mcx, element)?;
            }
            NodeTag::T_GrantStmt => grants.lappend(mcx, element)?,
            other => panic!("unrecognized node type: {other:?}"),
        }
    }
    let mut result = sequences;
    for l in [&tables, &views, &indexes, &triggers, &grants] {
        for n in l.iter() {
            result.lappend(mcx, n)?;
        }
    }
    Ok(result)
}

// transformConstraintAttrs (parse_utilcmd.c): fold CONSTR_ATTR_* markers onto
// the preceding constraint. Deferrable UNIQUE/PK louds downstream in the
// transformIndexConstraint lanes (deferred-unique lane owns them).
fn transformConstraintAttrs<'mcx>(
    constraints: &NodeList<'mcx>,
    src: Option<&str>,
) -> PgResult<()> {
    let supports_attrs = |c: Option<&Constraint<'_>>| {
        matches!(
            c.map(|c| c.contype),
            Some(
                ConstrType::CONSTR_PRIMARY
                    | ConstrType::CONSTR_UNIQUE
                    | ConstrType::CONSTR_EXCLUSION
                    | ConstrType::CONSTR_FOREIGN
            )
        )
    };
    let misplaced = |clause: &str, location: i32| {
        column_syntax_error(format_args!("misplaced {clause} clause"), src, location)
    };
    let multiple = |what: &str, location: i32| {
        column_syntax_error(
            format_args!("multiple {what} clauses not allowed"),
            src,
            location,
        )
    };
    let initially_deferred_not_deferrable = |location: i32| {
        column_syntax_error(
            format_args!("constraint declared INITIALLY DEFERRED must be DEFERRABLE"),
            src,
            location,
        )
    };
    let mut lastprimarycon: Option<Node<'mcx>> = None;
    let mut saw_deferrability = false;
    let mut saw_initially = false;
    let mut saw_enforced = false;
    for cnode in constraints.iter() {
        let con = cnode.as_variant::<Constraint>().expect("column constraint");
        let last = lastprimarycon
            .map(|n| n.as_variant::<Constraint>().expect("Constraint"));
        // SAFETY (each with_mut): parse tree is analyze-owned; `last` is not
        // read again after the write.
        match con.contype {
            ConstrType::CONSTR_ATTR_DEFERRABLE => {
                if !supports_attrs(last) {
                    return Err(misplaced("DEFERRABLE", con.location));
                }
                if saw_deferrability {
                    return Err(multiple("DEFERRABLE/NOT DEFERRABLE", con.location));
                }
                saw_deferrability = true;
                unsafe {
                    lastprimarycon
                        .expect("SUPPORTS_ATTRS checked")
                        .with_mut::<Constraint, _>(|c| c.deferrable = true)
                        .expect("Constraint");
                }
            }
            ConstrType::CONSTR_ATTR_NOT_DEFERRABLE => {
                if !supports_attrs(last) {
                    return Err(misplaced("NOT DEFERRABLE", con.location));
                }
                if saw_deferrability {
                    return Err(multiple("DEFERRABLE/NOT DEFERRABLE", con.location));
                }
                saw_deferrability = true;
                let last_node = lastprimarycon.expect("SUPPORTS_ATTRS checked");
                unsafe {
                    last_node
                        .with_mut::<Constraint, _>(|c| c.deferrable = false)
                        .expect("Constraint");
                }
                if saw_initially
                    && last_node.as_variant::<Constraint>().expect("Constraint").initdeferred
                {
                    return Err(initially_deferred_not_deferrable(con.location));
                }
            }
            ConstrType::CONSTR_ATTR_DEFERRED => {
                if !supports_attrs(last) {
                    return Err(misplaced("INITIALLY DEFERRED", con.location));
                }
                if saw_initially {
                    return Err(multiple("INITIALLY IMMEDIATE/DEFERRED", con.location));
                }
                saw_initially = true;
                let last_node = lastprimarycon.expect("SUPPORTS_ATTRS checked");
                unsafe {
                    last_node
                        .with_mut::<Constraint, _>(|c| {
                            c.initdeferred = true;
                            // If only INITIALLY DEFERRED appears, assume DEFERRABLE
                            if !saw_deferrability {
                                c.deferrable = true;
                            }
                        })
                        .expect("Constraint");
                }
                if saw_deferrability
                    && !last_node.as_variant::<Constraint>().expect("Constraint").deferrable
                {
                    return Err(initially_deferred_not_deferrable(con.location));
                }
            }
            ConstrType::CONSTR_ATTR_IMMEDIATE => {
                if !supports_attrs(last) {
                    return Err(misplaced("INITIALLY IMMEDIATE", con.location));
                }
                if saw_initially {
                    return Err(multiple("INITIALLY IMMEDIATE/DEFERRED", con.location));
                }
                saw_initially = true;
                unsafe {
                    lastprimarycon
                        .expect("SUPPORTS_ATTRS checked")
                        .with_mut::<Constraint, _>(|c| c.initdeferred = false)
                        .expect("Constraint");
                }
            }
            ConstrType::CONSTR_ATTR_ENFORCED => {
                if !matches!(
                    last.map(|c| c.contype),
                    Some(ConstrType::CONSTR_CHECK | ConstrType::CONSTR_FOREIGN)
                ) {
                    return Err(misplaced("ENFORCED", con.location));
                }
                if saw_enforced {
                    return Err(multiple("ENFORCED/NOT ENFORCED", con.location));
                }
                saw_enforced = true;
                unsafe {
                    lastprimarycon
                        .expect("contype checked")
                        .with_mut::<Constraint, _>(|c| c.is_enforced = true)
                        .expect("Constraint");
                }
            }
            ConstrType::CONSTR_ATTR_NOT_ENFORCED => {
                if !matches!(
                    last.map(|c| c.contype),
                    Some(ConstrType::CONSTR_CHECK | ConstrType::CONSTR_FOREIGN)
                ) {
                    return Err(misplaced("NOT ENFORCED", con.location));
                }
                if saw_enforced {
                    return Err(multiple("ENFORCED/NOT ENFORCED", con.location));
                }
                saw_enforced = true;
                unsafe {
                    lastprimarycon
                        .expect("contype checked")
                        .with_mut::<Constraint, _>(|c| {
                            c.is_enforced = false;
                            // A NOT ENFORCED constraint must be marked as invalid.
                            c.skip_validation = true;
                            c.initially_valid = false;
                        })
                        .expect("Constraint");
                }
            }
            _ => {
                lastprimarycon = Some(cnode);
                saw_deferrability = false;
                saw_initially = false;
                saw_enforced = false;
            }
        }
    }
    Ok(())
}

fn transformColumnDefinition<'mcx>(
    mcx: Mcx<'mcx>,
    column_node: Node<'mcx>,
    relation: &RangeVar<'mcx>,
    // C cxt->rel: set only on the ALTER path; serial/identity sequences take
    // namespace/persistence/owner from the existing table.
    rel: Option<&types_rel::Relation<'_>>,
    src: Option<&str>,
    cxt: &mut CreateStmtCxt<'mcx>,
    ckconstraints: &mut NodeList<'mcx>,
    nnconstraints: &mut NodeList<'mcx>,
    ixconstraints: &mut NodeList<'mcx>,
    fkconstraints: &mut NodeList<'mcx>,
    is_foreign: bool,
    of_type: bool,
    partbound: bool,
    // C cxt->ispartitioned: CREATE ... PARTITION BY / ALTER on a
    // partitioned table.
    ispartitioned: bool,
) -> PgResult<()> {
    let relname = relation.relname.unwrap_or("");
    // The ColumnDef is mutated through column_node throughout; every read
    // re-derives so no shared ref is held across a with_mut.
    macro_rules! col {
        () => {
            column_node.as_variant::<ColumnDef>().expect("ColumnDef")
        };
    }
    if col!().raw_default.is_some() || col!().cooked_default.is_some() {
        unported("pre-split column defaults");
    }

    // SERIAL pseudo-types (transformColumnDefinition's is_serial arm).
    let mut is_serial_oid = InvalidOid;
    if let Some(tn_node) = col!().typeName {
        let tn = tn_node.as_variant::<TypeName>().expect("TypeName");
        if tn.names.len() == 1 && !tn.pct_type {
            let typname = tn.names.nth(0).as_string().expect("TypeName name").sval;
            is_serial_oid = match typname {
                "smallserial" | "serial2" => INT2OID,
                "serial" | "serial4" => INT4OID,
                "bigserial" | "serial8" => INT8OID,
                _ => InvalidOid,
            };
            if is_serial_oid != InvalidOid {
                if !tn.arrayBounds.is_nil() {
                    return Err(Box::new(
                        PgError::new(ERROR, "array of serial is not implemented".to_string())
                            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ));
                }
                // SAFETY: parse tree is analyze-owned; no derived refs live.
                unsafe {
                    tn_node
                        .with_mut::<TypeName, _>(|t| {
                            t.names = NodeList::nil();
                            t.typeOid = is_serial_oid;
                        })
                        .expect("TypeName");
                }
            }
        }
    }

    let mut need_notnull = false;
    let mut disallow_noinherit_notnull = false;
    if is_serial_oid != InvalidOid {
        let (snamespace, sname) = generateSerialExtraStmts(
            mcx,
            relation,
            column_node,
            is_serial_oid,
            NodeList::nil(),
            false,
            rel,
            false,
            cxt,
        )?;

        // DEFAULT nextval('snamespace.sname'::regclass), raw form.
        let qstring = leak_str(quote_qualified_identifier(mcx, Some(snamespace), sname)?);
        let snamenode = Node::mk_a_const(
            mcx,
            Some(ValUnion::String(types_nodes::String { sval: qstring })),
            -1,
        )?;
        let mut regclass_tn = Node::build::<TypeName>(mcx)?;
        let mut names = NodeList::make1(mcx, Node::mk_string(mcx, "pg_catalog")?)?;
        names.lappend(mcx, Node::mk_string(mcx, "regclass")?)?;
        regclass_tn.names = names;
        regclass_tn.typemod = -1;
        regclass_tn.location = -1;
        let castnode = Node::mk(
            mcx,
            TypeCast { arg: Some(snamenode), typeName: Some(regclass_tn.seal()), location: -1 },
        )?;
        let mut funcname = NodeList::make1(mcx, Node::mk_string(mcx, "pg_catalog")?)?;
        funcname.lappend(mcx, Node::mk_string(mcx, "nextval")?)?;
        let mut fc = Node::build::<FuncCall>(mcx)?;
        fc.funcname = funcname;
        fc.args = NodeList::make1(mcx, castnode)?;
        fc.funcformat = CoercionForm::COERCE_EXPLICIT_CALL;
        fc.location = -1;
        let mut cons = Node::build::<Constraint>(mcx)?;
        cons.contype = ConstrType::CONSTR_DEFAULT;
        cons.location = -1;
        cons.raw_expr = Some(fc.seal());
        let cons = cons.seal();
        // SAFETY: parse tree is analyze-owned; no derived refs live.
        unsafe {
            column_node
                .with_mut::<ColumnDef, _>(|c| c.constraints.lappend(mcx, cons))
                .expect("ColumnDef")?;
        }
        need_notnull = true;
        disallow_noinherit_notnull = true;
    }

    // SERIAL implies a not-null that must not be NO INHERIT; PRIMARY KEY and
    // IDENTITY column constraints do too (pre-scan mirrors C).
    let mut disallow_noinherit_notnull = is_serial_oid != InvalidOid;

    transformConstraintAttrs(&col!().constraints, src)?;

    if !disallow_noinherit_notnull {
        for i in 0..col!().constraints.len() {
            let c = col!().constraints.nth(i).as_variant::<Constraint>().expect("column constraint");
            if matches!(c.contype, ConstrType::CONSTR_IDENTITY | ConstrType::CONSTR_PRIMARY) {
                disallow_noinherit_notnull = true;
            }
        }
    }

    let mut saw_nullable = false;
    let mut saw_default = false;
    let mut col_not_null = col!().is_not_null;
    let mut saw_identity = false;
    let mut saw_generated = false;
    let mut notnull_constraint: Option<Node<'mcx>> = None;
    let mut ci = 0;
    while ci < col!().constraints.len() {
        let cnode = col!().constraints.nth(ci);
        ci += 1;
        let constraint = cnode.as_variant::<Constraint>().expect("column constraint");
        // Arms below may with_mut cnode; the trailing checks use this copy.
        let con_location = constraint.location;
        match constraint.contype {
            ConstrType::CONSTR_DEFAULT => {
                if saw_default {
                    return Err(multiple_defaults(
                        col!().colname.unwrap_or(""),
                        relname,
                    ));
                }
                let raw_expr = constraint.raw_expr;
                debug_assert!(constraint.cooked_expr.is_none());
                // SAFETY: parse tree is analyze-owned; no derived refs live.
                unsafe {
                    column_node
                        .with_mut::<ColumnDef, _>(|c| c.raw_default = raw_expr)
                        .expect("ColumnDef");
                }
                saw_default = true;
            }
            ConstrType::CONSTR_IDENTITY => {
                if of_type {
                    return Err(Box::new(
                        PgError::new(
                            ERROR,
                            "identity columns are not supported on typed tables".to_string(),
                        )
                        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ));
                }
                if partbound {
                    return Err(Box::new(
                        PgError::new(
                            ERROR,
                            "identity columns are not supported on partitions".to_string(),
                        )
                        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ));
                }
                let tn = col!()
                    .typeName
                    .expect("ColumnDef.typeName")
                    .as_variant::<TypeName>()
                    .expect("TypeName");
                // C typenameType(cxt->pstate, ...) attaches errposition here.
                let (type_oid, _typmod) = typenameTypeIdAndMod(mcx, None, tn)
                    .map_err(|e| position_on_src(e, src, tn.location))?;
                if saw_identity {
                    return Err(column_syntax_error(
                        format_args!(
                            "multiple identity specifications for column \"{}\" of table \"{}\"",
                            col!().colname.unwrap_or(""),
                            relname
                        ),
                        src,
                        constraint.location,
                    ));
                }
                generateSerialExtraStmts(
                    mcx,
                    relation,
                    column_node,
                    type_oid,
                    // C list_copy: generateSerialExtraStmts prepends AS.
                    constraint.options.clone_in(mcx)?,
                    true,
                    rel,
                    false,
                    cxt,
                )?;
                let when = constraint.generated_when;
                // SAFETY: parse tree is analyze-owned; no derived refs live.
                unsafe {
                    column_node
                        .with_mut::<ColumnDef, _>(|c| c.identity = when)
                        .expect("ColumnDef");
                }
                saw_identity = true;
                if !saw_nullable {
                    need_notnull = true;
                } else if !col_not_null {
                    return Err(column_syntax_error(
                        format_args!(
                            "conflicting NULL/NOT NULL declarations for column \"{}\" of table \"{}\"",
                            col!().colname.unwrap_or(""),
                            relname
                        ),
                        src,
                        constraint.location,
                    ));
                }
            }
            ConstrType::CONSTR_GENERATED => {
                if of_type {
                    return Err(Box::new(
                        PgError::new(
                            ERROR,
                            "generated columns are not supported on typed tables".to_string(),
                        )
                        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ));
                }
                if saw_generated {
                    return Err(column_syntax_error(
                        format_args!(
                            "multiple generation clauses specified for column \"{}\" of table \"{}\"",
                            col!().colname.unwrap_or(""),
                            relname
                        ),
                        src,
                        constraint.location,
                    ));
                }
                let kind = constraint.generated_kind;
                let raw_expr = constraint.raw_expr;
                debug_assert!(constraint.cooked_expr.is_none());
                // SAFETY: parse tree is analyze-owned; no derived refs live.
                unsafe {
                    column_node
                        .with_mut::<ColumnDef, _>(|c| {
                            c.generated = kind;
                            c.raw_default = raw_expr;
                        })
                        .expect("ColumnDef");
                }
                saw_generated = true;
            }
            ConstrType::CONSTR_CHECK => ckconstraints.lappend(mcx, cnode)?,
            ConstrType::CONSTR_NOTNULL => {
                let colname = col!().colname.expect("ColumnDef.colname");
                if ispartitioned && constraint.is_no_inherit {
                    return Err(Box::new(
                        PgError::new(
                            ERROR,
                            "not-null constraints on partitioned tables cannot be NO INHERIT"
                                .to_string(),
                        )
                        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ));
                }
                if saw_nullable && !col_not_null {
                    // C attaches parser_errposition at every conflicting-
                    // declarations site (parse_utilcmd.c:747,765,873,906).
                    return Err(column_syntax_error(
                        format_args!(
                            "conflicting NULL/NOT NULL declarations for column \"{colname}\" of table \"{relname}\""
                        ),
                        src,
                        constraint.location,
                    ));
                }
                if disallow_noinherit_notnull && constraint.is_no_inherit {
                    return Err(Box::new(
                        PgError::new(
                            ERROR,
                            format!(
                                "conflicting NO INHERIT declarations for not-null constraints on column \"{colname}\""
                            ),
                        )
                        .with_sqlstate(types_error::ERRCODE_SYNTAX_ERROR),
                    ));
                }
                if !col_not_null {
                    saw_nullable = true;
                    col_not_null = true;
                    need_notnull = false;
                    let keys = NodeList::make1(mcx, Node::mk_string(mcx, colname)?)?;
                    // SAFETY (both): parse tree is analyze-owned; no derived
                    // refs.
                    unsafe {
                        column_node
                            .with_mut::<ColumnDef, _>(|c| c.is_not_null = true)
                            .expect("ColumnDef");
                        cnode
                            .with_mut::<Constraint, _>(|c| c.keys = keys)
                            .expect("Constraint");
                    }
                    notnull_constraint = Some(cnode);
                    nnconstraints.lappend(mcx, cnode)?;
                } else if let Some(first_node) = notnull_constraint {
                    // Redundant specification: merge onto the first one.
                    let first =
                        first_node.as_variant::<Constraint>().expect("Constraint");
                    if let (Some(a), Some(b)) = (first.conname, constraint.conname) {
                        if a != b {
                            return Err(Box::new(PgError::new(
                                ERROR,
                                format!(
                                    "conflicting not-null constraint names \"{a}\" and \"{b}\""
                                ),
                            )));
                        }
                    }
                    if first.is_no_inherit != constraint.is_no_inherit {
                        return Err(Box::new(
                            PgError::new(
                                ERROR,
                                format!(
                                    "conflicting NO INHERIT declarations for not-null constraints on column \"{colname}\""
                                ),
                            )
                            .with_sqlstate(types_error::ERRCODE_SYNTAX_ERROR),
                        ));
                    }
                    if first.conname.is_none() && constraint.conname.is_some() {
                        let adopted = constraint.conname;
                        // SAFETY: parse tree is analyze-owned; no derived refs.
                        unsafe {
                            first_node
                                .with_mut::<Constraint, _>(|c| c.conname = adopted)
                                .expect("Constraint");
                        }
                    }
                }
            }
            ConstrType::CONSTR_NULL => {
                if (saw_nullable && col_not_null) || need_notnull {
                    return Err(column_syntax_error(
                        format_args!(
                            "conflicting NULL/NOT NULL declarations for column \"{}\" of table \"{}\"",
                            col!().colname.unwrap_or(""),
                            relname
                        ),
                        src,
                        constraint.location,
                    ));
                }
                col_not_null = false;
                saw_nullable = true;
                // SAFETY: parse tree is analyze-owned; no derived refs.
                unsafe {
                    column_node
                        .with_mut::<ColumnDef, _>(|c| c.is_not_null = false)
                        .expect("ColumnDef");
                }
            }
            ConstrType::CONSTR_PRIMARY | ConstrType::CONSTR_UNIQUE => {
                if constraint.contype == ConstrType::CONSTR_PRIMARY {
                    if saw_nullable && !col_not_null {
                        return Err(column_syntax_error(
                            format_args!(
                                "conflicting NULL/NOT NULL declarations for column \"{}\" of table \"{}\"",
                                col!().colname.unwrap_or(""),
                                relname
                            ),
                            src,
                            constraint.location,
                        ));
                    }
                    need_notnull = true;
                    if is_foreign {
                        return Err(not_supported_on_foreign_tables(
                            "primary key",
                            src,
                            constraint.location,
                        ));
                    }
                }
                if constraint.contype == ConstrType::CONSTR_UNIQUE && is_foreign {
                    return Err(not_supported_on_foreign_tables(
                        "unique",
                        src,
                        constraint.location,
                    ));
                }
                if constraint.keys.is_nil() {
                    let colname = col!().colname.expect("ColumnDef.colname");
                    let keys = NodeList::make1(mcx, Node::mk_string(mcx, colname)?)?;
                    // SAFETY: parse tree is analyze-owned; no derived refs.
                    unsafe {
                        cnode.with_mut::<Constraint, _>(|c| c.keys = keys).expect("Constraint");
                    }
                }
                ixconstraints.lappend(mcx, cnode)?;
            }
            ConstrType::CONSTR_FOREIGN => {
                if is_foreign {
                    return Err(not_supported_on_foreign_tables(
                        "foreign key",
                        src,
                        constraint.location,
                    ));
                }
                let colname = col!().colname.expect("ColumnDef.colname");
                let fk_attrs = NodeList::make1(mcx, Node::mk_string(mcx, colname)?)?;
                // SAFETY: parse tree is analyze-owned; no derived refs.
                unsafe {
                    cnode
                        .with_mut::<Constraint, _>(|c| c.fk_attrs = fk_attrs)
                        .expect("Constraint");
                }
                fkconstraints.lappend(mcx, cnode)?;
            }
            ConstrType::CONSTR_ATTR_DEFERRABLE
            | ConstrType::CONSTR_ATTR_NOT_DEFERRABLE
            | ConstrType::CONSTR_ATTR_DEFERRED
            | ConstrType::CONSTR_ATTR_IMMEDIATE
            | ConstrType::CONSTR_ATTR_ENFORCED
            | ConstrType::CONSTR_ATTR_NOT_ENFORCED => {
                // transformConstraintAttrs took care of these
            }
            _ => unported("unexpected column constraint type"),
        }
        if saw_default && saw_identity {
            return Err(column_syntax_error(
                format_args!(
                    "both default and identity specified for column \"{}\" of table \"{}\"",
                    col!().colname.unwrap_or(""),
                    relname
                ),
                src,
                con_location,
            ));
        }
        if saw_default && saw_generated {
            return Err(column_syntax_error(
                format_args!(
                    "both default and generation expression specified for column \"{}\" of table \"{}\"",
                    col!().colname.unwrap_or(""),
                    relname
                ),
                src,
                con_location,
            ));
        }
        if saw_identity && saw_generated {
            return Err(column_syntax_error(
                format_args!(
                    "both identity and generation expression specified for column \"{}\" of table \"{}\"",
                    col!().colname.unwrap_or(""),
                    relname
                ),
                src,
                con_location,
            ));
        }
    }
    if need_notnull && !(saw_nullable && col_not_null) {
        // SAFETY: parse tree is analyze-owned; no derived refs.
        unsafe {
            column_node
                .with_mut::<ColumnDef, _>(|c| c.is_not_null = true)
                .expect("ColumnDef");
        }
        let colname = col!().colname.expect("ColumnDef.colname");
        nnconstraints.lappend(mcx, make_not_null_constraint(mcx, colname)?)?;
    }
    // Per-column FDW options become a post-create ALTER FOREIGN TABLE ALTER
    // COLUMN ... OPTIONS statement (parse_utilcmd.c:1008-1033).
    if !col!().fdwoptions.is_nil() {
        use types_nodes::parsenodes::{
            AlterTableCmd, AlterTableStmt, AlterTableType, DropBehavior, ObjectType,
        };
        let mut cmd = Node::build::<AlterTableCmd>(mcx)?;
        cmd.subtype = AlterTableType::AT_AlterColumnGenericOptions;
        cmd.name = col!().colname;
        let mut opts = NodeList::nil();
        for o in col!().fdwoptions.iter() {
            opts.lappend(mcx, o)?;
        }
        cmd.def = Some(Node::mk_list(mcx, opts)?);
        cmd.behavior = DropBehavior::DROP_RESTRICT;
        cmd.missing_ok = false;
        let mut cmds = NodeList::nil();
        cmds.lappend(mcx, cmd.seal())?;
        let rv = Node::mk_mut(
            mcx,
            RangeVar {
                catalogname: relation.catalogname,
                schemaname: relation.schemaname,
                relname: relation.relname,
                inh: relation.inh,
                relpersistence: relation.relpersistence,
                alias: relation.alias,
                location: relation.location,
            },
        )?
        .seal_ref();
        let mut stmt = Node::build::<AlterTableStmt>(mcx)?;
        stmt.relation = Some(rv);
        stmt.cmds = cmds;
        stmt.objtype = ObjectType::OBJECT_FOREIGN_TABLE;
        cxt.alist.lappend(mcx, stmt.seal())?;
    }
    // Typed-table/partition column options carry no typeName; C skips
    // transformColumnType for them (parse_utilcmd.c:1055).
    let Some(tn_node) = col!().typeName else {
        return Ok(());
    };
    let tn = tn_node.as_variant::<TypeName>().expect("TypeName");
    // transformColumnType: validate the type reference and any COLLATE spec.
    // C typenameType(cxt->pstate, ...) attaches errposition at tn.location.
    let (type_oid, _typmod) = typenameTypeIdAndMod(mcx, None, tn)
        .map_err(|e| position_on_src(e, src, tn.location))?;
    if let Some(cc) = col!().collClause {
        let cc = cc.as_variant::<types_nodes::CollateClause>().expect("CollateClause");
        catalog_namespace::get_collation_oid_list(&cc.collname, false)
            .map_err(|e| position_on_src(e, src, cc.location))?;
        let typcollation = syscache_seams::lookup_pg_type_shape::call(type_oid)?
            .expect("pg_type row vanished")
            .typcollation;
        if typcollation == InvalidOid {
            return Err(position_on_src(
                Box::new(
                    types_error::PgError::error(format!(
                        "collations are not supported by type {}",
                        format_type::format_type_be(type_oid)?
                    ))
                    .with_sqlstate(types_error::ERRCODE_DATATYPE_MISMATCH),
                ),
                src,
                cc.location,
            ));
        }
    }
    Ok(())
}

#[cold]
fn position_on_src(
    e: Box<types_error::PgError>,
    src: Option<&str>,
    location: types_core::ParseLoc,
) -> Box<types_error::PgError> {
    if e.cursor_position().is_some() {
        return e;
    }
    Box::new((*e).with_cursor_position(parser_small1::parser_errposition_source(
        src.map(str::as_bytes),
        location,
        mbutils::GetDatabaseEncoding(),
    )))
}

// transformOfType (parse_utilcmd.c:1638): derive is_from_type ColumnDefs from
// the composite type's rowtype, prepended to the column list.
fn transformOfType<'mcx>(
    mcx: Mcx<'mcx>,
    of_tn_node: Node<'mcx>,
    columns: &mut NodeList<'mcx>,
    src: Option<&str>,
) -> PgResult<()> {
    let tn = of_tn_node.as_variant::<TypeName>().expect("TypeName");
    let (of_type_id, _typmod) = typenameTypeIdAndModAllowComposite(mcx, None, tn)
        .map_err(|e| position_on_src(e, src, tn.location))?;
    tablecmds_seams::check_of_type::call(mcx, of_type_id)?;
    // SAFETY: parse tree is analyze-owned; no derived refs live.
    unsafe {
        of_tn_node.with_mut::<TypeName, _>(|t| t.typeOid = of_type_id).expect("TypeName");
    }
    let tupdesc = typcache_seams::lookup_rowtype_tupdesc_copy::call(mcx, of_type_id, -1)?;
    for i in 0..tupdesc.natts as usize {
        let attr = tupdesc.attr(i);
        if attr.attisdropped {
            continue;
        }
        let attname = {
            let mut v: mcx::PgVec<'mcx, u8> =
                mcx::vec_with_capacity_in(mcx, attr.attname.name_str().len())?;
            mcx::vec_append_bytes(&mut v, attr.attname.name_str())?;
            core::str::from_utf8(v.leak()).expect("attname UTF-8")
        };
        let coltn = TypeName {
            typeOid: attr.atttypid,
            typemod: attr.atttypmod,
            location: -1,
            ..TypeName::default()
        };
        let def = ColumnDef {
            colname: Some(attname),
            typeName: Some(Node::mk(mcx, coltn)?),
            is_local: true,
            is_from_type: true,
            collOid: attr.attcollation,
            location: -1,
            ..ColumnDef::default()
        };
        columns.lappend(mcx, Node::mk(mcx, def)?)?;
    }
    Ok(())
}

pub fn transformCreateStmt<'mcx>(
    mcx: Mcx<'mcx>,
    stmt_node: Node<'mcx>,
    query_string: &str,
) -> PgResult<NodeList<'mcx>> {
    let is_foreign = stmt_node.node_tag() == NodeTag::T_CreateForeignTableStmt;
    let stmt: &CreateStmt<'mcx> = if is_foreign {
        &stmt_node
            .as_variant::<types_nodes::rawnodes::CreateForeignTableStmt>()
            .expect("transformCreateStmt on non-CreateStmt")
            .base
    } else {
        stmt_node.as_variant::<CreateStmt>().expect("transformCreateStmt on non-CreateStmt")
    };

    debug_assert!(stmt.constraints.is_nil() && stmt.nnconstraints.is_nil());
    debug_assert!(stmt.ofTypename.is_none() || stmt.inhRelations.is_nil());

    let relation = stmt.relation.expect("CreateStmt.relation");
    let relname = relation.relname.unwrap_or("");
    // RangeVarGetAndCheckCreationNamespace at analysis (parse_utilcmd.c:
    // 215-217, under a parser errposition callback): namespace/persistence
    // errors carry the relation name's position, and a temp creation
    // namespace flips a PERMANENT target to TEMP. The namespace CREATE ACL
    // check and lock-retry loop ride with the aclchk lane.
    let at_rel = |mut e: Box<PgError>| {
        let pos = parser_small1::parser_errposition_source(
            Some(query_string.as_bytes()),
            relation.location,
            mbutils::GetDatabaseEncoding(),
        );
        if e.cursor_position.is_none() && pos > 0 {
            e.cursor_position = Some(pos);
        }
        e
    };
    // Unqualified TEMP targets skip the analysis-time probe: C's call is a
    // no-op for them beyond creating the temp namespace, a side effect our
    // cached-plan revalidation lane cannot absorb yet (plancache loud); it
    // still happens at execution (DefineRelation).
    let probe = relation.schemaname.is_some()
        || relation.relpersistence != types_core::RELPERSISTENCE_TEMP;
    let mut nspid = InvalidOid;
    let mut existing_relid = InvalidOid;
    let mut adjusted_persistence = relation.relpersistence;
    if probe {
        let (n, e, p) =
            RangeVarGetAndCheckCreationNamespace(mcx, relation, types_rel::NoLock, true).map_err(at_rel)?;
        nspid = n;
        existing_relid = e;
        adjusted_persistence = p;
    }
    if stmt.if_not_exists {
        let existing_relid = if probe {
            existing_relid
        } else {
            RangeVarGetAndCheckCreationNamespace(mcx, relation, types_rel::NoLock, true)?.1
        };
        if existing_relid != InvalidOid {
            // unported: checkMembershipInCurrentExtension only bites inside
            // an extension script (needs getObjectDescription for its
            // report); clean 0A000 until that lane is ported.
            if pg_depend::creating_extension() {
                return Err(unported_feature_at(
                    None,
                    -1,
                    "CREATE TABLE IF NOT EXISTS inside an extension script",
                ));
            }
            elog_seams::ereport::call(
                PgError::new(
                    types_error::NOTICE,
                    format!("relation \"{relname}\" already exists, skipping"),
                )
                .with_sqlstate(types_error::ERRCODE_DUPLICATE_TABLE),
            )?;
            return Ok(NodeList::nil());
        }
    }
    // Qualify an unqualified non-temp target (parse_utilcmd.c:215-224) so
    // added-on commands (LIKE expansion) can't latch onto a same-named
    // relation earlier in the search path; the persistence adjustment above
    // is stamped alongside (C mutates stmt->relation in place).
    let qualify = relation.schemaname.is_none()
        && adjusted_persistence != types_core::RELPERSISTENCE_TEMP;
    let relation = if qualify || adjusted_persistence != relation.relpersistence {
        let schemaname = if qualify {
            Some(leak_str(
                lsyscache::get_namespace_name(mcx, nspid)?
                    .unwrap_or_else(|| panic!("cache lookup failed for namespace {nspid}")),
            ))
        } else {
            relation.schemaname
        };
        let stamped: &'mcx RangeVar<'mcx> = Node::mk_mut(
            mcx,
            RangeVar {
                catalogname: relation.catalogname,
                schemaname,
                relname: relation.relname,
                inh: relation.inh,
                relpersistence: adjusted_persistence,
                alias: relation.alias,
                location: relation.location,
            },
        )?
        .seal_ref();
        // SAFETY: parse tree is analyze-owned; no derived refs live.
        unsafe {
            if is_foreign {
                stmt_node
                    .with_mut::<types_nodes::rawnodes::CreateForeignTableStmt, _>(|s| {
                        s.base.relation = Some(stamped)
                    })
                    .expect("CreateForeignTableStmt");
            } else {
                stmt_node
                    .with_mut::<CreateStmt, _>(|s| s.relation = Some(stamped))
                    .expect("CreateStmt");
            }
        }
        stamped
    } else {
        relation
    };
    // Re-derive: the stamp above mutated the statement node.
    let stmt: &CreateStmt<'mcx> = if is_foreign {
        &stmt_node
            .as_variant::<types_nodes::rawnodes::CreateForeignTableStmt>()
            .expect("transformCreateStmt on non-CreateStmt")
            .base
    } else {
        stmt_node.as_variant::<CreateStmt>().expect("transformCreateStmt on non-CreateStmt")
    };
    let mut columns = NodeList::nil();
    if let Some(of_tn) = stmt.ofTypename {
        transformOfType(mcx, of_tn, &mut columns, Some(query_string))?;
    }
    // C raises this at analysis (parse_utilcmd.c:262-266), before any parent
    // lookup can report a missing inheritance parent instead.
    if stmt.partspec.is_some() && !stmt.inhRelations.is_nil() && stmt.partbound.is_none() {
        return Err(Box::new(
            PgError::new(ERROR, "cannot create partitioned table as inheritance child".to_string())
                .with_sqlstate(types_error::ERRCODE_INVALID_OBJECT_DEFINITION),
        ));
    }
    let mut cxt = CreateStmtCxt::new(if is_foreign {
        "CREATE FOREIGN TABLE"
    } else {
        "CREATE TABLE"
    });
    let mut ckconstraints = NodeList::nil();
    let mut nnconstraints = NodeList::nil();
    let mut ixconstraints = NodeList::nil();
    let mut fkconstraints = NodeList::nil();
    let mut alist = NodeList::nil();
    let mut likeclauses = NodeList::nil();
    let mut save_alist = NodeList::nil();
    for elt in stmt.tableElts.iter() {
        match elt.node_tag() {
            NodeTag::T_ColumnDef => {
                transformColumnDefinition(
                    mcx,
                    elt,
                    relation,
                    None,
                    Some(query_string),
                    &mut cxt,
                    &mut ckconstraints,
                    &mut nnconstraints,
                    &mut ixconstraints,
                    &mut fkconstraints,
                    is_foreign,
                    stmt.ofTypename.is_some(),
                    stmt.partbound.is_some(),
                    stmt.partspec.is_some(),
                )?;
                columns.lappend(mcx, elt)?;
            }
            NodeTag::T_TableLikeClause => {
                let mut likecxt = like::LikeCxt {
                    relation,
                    columns: &mut columns,
                    nnconstraints: &mut nnconstraints,
                    likeclauses: &mut likeclauses,
                    alist: &mut save_alist,
                    is_foreign,
                };
                like::transformTableLikeClause(mcx, &mut likecxt, &mut cxt, elt, query_string)?;
            }
            NodeTag::T_Constraint => {
                let c = elt.as_variant::<Constraint>().expect("Constraint");
                match c.contype {
                    ConstrType::CONSTR_PRIMARY
                    | ConstrType::CONSTR_UNIQUE
                    | ConstrType::CONSTR_EXCLUSION => {
                        if is_foreign {
                            let what = match c.contype {
                                ConstrType::CONSTR_PRIMARY => "primary key",
                                ConstrType::CONSTR_UNIQUE => "unique",
                                _ => "exclusion",
                            };
                            return Err(not_supported_on_foreign_tables(
                                what,
                                Some(query_string),
                                c.location,
                            ));
                        }
                        ixconstraints.lappend(mcx, elt)?
                    }
                    ConstrType::CONSTR_CHECK => ckconstraints.lappend(mcx, elt)?,
                    ConstrType::CONSTR_NOTNULL => {
                        // transformTableConstraint (parse_utilcmd.c:1074-1078).
                        if stmt.partspec.is_some() && c.is_no_inherit {
                            return Err(Box::new(
                                PgError::new(
                                    ERROR,
                                    "not-null constraints on partitioned tables cannot be NO INHERIT"
                                        .to_string(),
                                )
                                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
                            ));
                        }
                        nnconstraints.lappend(mcx, elt)?
                    }
                    ConstrType::CONSTR_FOREIGN => {
                        if is_foreign {
                            return Err(not_supported_on_foreign_tables(
                                "foreign key",
                                Some(query_string),
                                c.location,
                            ));
                        }
                        fkconstraints.lappend(mcx, elt)?
                    }
                    other => unported(&format!("transformTableConstraint {other:?} arm")),
                }
            }
            other => panic!("unrecognized node type in tableElts: {other:?}"),
        }
    }

    // Table-level NOT NULL propagation (C parse_utilcmd.c:310-333).
    for nn in nnconstraints.iter() {
        let nnc = nn.as_variant::<Constraint>().expect("Constraint");
        let colname = nnc.keys.nth(0).as_string().expect("not-null keys").sval;
        for cn in columns.iter() {
            let cd = cn.as_variant::<ColumnDef>().expect("ColumnDef");
            if cd.colname != Some(colname) {
                continue;
            }
            if !cd.is_not_null {
                // SAFETY: parse tree is analyze-owned; no derived refs.
                unsafe {
                    cn.with_mut::<ColumnDef, _>(|c| c.is_not_null = true).expect("ColumnDef");
                }
            }
            break;
        }
    }

    transform_index_constraints(
        mcx,
        relname,
        Some(relation),
        &columns,
        &stmt.inhRelations,
        &mut nnconstraints,
        &ixconstraints,
        &mut alist,
        query_string,
        false,
    )?;

    // LIKE re-consideration runs after index creation, before FK creation
    // (parse_utilcmd.c:337-351): a LIKE-cloned pkey behaves like ALTER TABLE
    // ADD and must be the one that hits the duplicate-pkey check.
    alist.concat(mcx, &likeclauses)?;

    // transformFKConstraints(skipValidation=true, isAddConstraint=false).
    if !fkconstraints.is_nil() {
        for cnode in fkconstraints.iter() {
            // SAFETY: parse tree is analyze-owned; no derived refs live.
            unsafe {
                cnode
                    .with_mut::<Constraint, _>(|c| {
                        c.skip_validation = true;
                        c.initially_valid = c.is_enforced;
                    })
                    .expect("Constraint");
            }
        }
        use types_nodes::parsenodes::{AlterTableCmd, AlterTableStmt, AlterTableType, ObjectType};
        let mut cmds = NodeList::nil();
        for cnode in fkconstraints.iter() {
            let mut cmd = Node::build::<AlterTableCmd>(mcx)?;
            cmd.subtype = AlterTableType::AT_AddConstraint;
            cmd.def = Some(cnode);
            cmds.lappend(mcx, cmd.seal())?;
        }
        let mut alterstmt = Node::build::<AlterTableStmt>(mcx)?;
        alterstmt.relation = Some(relation);
        alterstmt.cmds = cmds;
        alterstmt.objtype = ObjectType::OBJECT_TABLE;
        alist.lappend(mcx, alterstmt.seal())?;
    }

    // transformCheckConstraints(!isforeign): a new plain table is empty, so
    // its CHECKs are immediately valid; not so for foreign tables.
    if !is_foreign {
        for cnode in ckconstraints.iter() {
            // SAFETY: parse tree is analyze-owned; no derived refs live.
            unsafe {
                cnode
                    .with_mut::<Constraint, _>(|c| {
                        c.skip_validation = true;
                        c.initially_valid = c.is_enforced;
                    })
                    .expect("Constraint");
            }
        }
    }
    // SAFETY (both arms): parse tree is analyze-owned; no derived refs live.
    if is_foreign {
        unsafe {
            stmt_node
                .with_mut::<types_nodes::rawnodes::CreateForeignTableStmt, _>(|s| {
                    s.base.tableElts = columns;
                    s.base.constraints = ckconstraints;
                    s.base.nnconstraints = nnconstraints;
                })
                .expect("CreateForeignTableStmt");
        }
    } else {
        unsafe {
            stmt_node
                .with_mut::<CreateStmt, _>(|s| {
                    s.tableElts = columns;
                    s.constraints = ckconstraints;
                    s.nnconstraints = nnconstraints;
                })
                .expect("CreateStmt");
        }
    }

    // C: result = blist ++ [stmt] ++ indexes ++ likeclauses ++ fk ++
    // save_alist; C's save_alist captured the element-loop alist (serial
    // OWNED BY + LIKE statements) before transformIndexConstraints
    // (parse_utilcmd.c:390-395, 465-471).
    let mut result = cxt.blist;
    result.lappend(mcx, stmt_node)?;
    for a in alist.iter() {
        result.lappend(mcx, a)?;
    }
    for n in cxt.alist.iter() {
        result.lappend(mcx, n)?;
    }
    result.concat(mcx, &save_alist)?;
    Ok(result)
}

// Returns the inherited column's type oid when found (C threads it into the
// WITHOUT OVERLAPS range check, parse_utilcmd.c:2712-2715).
fn key_found_in_inh_relations<'mcx>(
    mcx: Mcx<'mcx>,
    key: &str,
    add_not_null: bool,
    inh_relations: &NodeList<'mcx>,
    nnconstraints: &mut NodeList<'mcx>,
) -> PgResult<Option<Oid>> {
    for inode in inh_relations.iter() {
        let prv = inode.as_variant::<RangeVar>().expect("inhRelations RangeVar");
        let relname = prv.relname.expect("RangeVar.relname");
        let rv = rel_vocab::RangeVar {
            catalogname: prv.catalogname,
            schemaname: prv.schemaname,
            relname,
            inh: prv.inh,
            relpersistence: prv.relpersistence,
            location: prv.location,
        };
        let relid = catalog_namespace::RangeVarGetRelid(&rv, types_rel::AccessShareLock, false)?;
        let rel = table::table_open(mcx, relid, types_rel::NoLock)?;
        if rel.rd_rel.relkind != types_rel::RELKIND_RELATION
            && rel.rd_rel.relkind != types_rel::RELKIND_FOREIGN_TABLE
            && rel.rd_rel.relkind != types_rel::RELKIND_PARTITIONED_TABLE
        {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!("inherited relation \"{relname}\" is not a table or foreign table"),
                )
                .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE),
            ));
        }
        let mut found: Option<Oid> = None;
        for i in 0..rel.rd_att.natts as usize {
            let att = rel.rd_att.attr(i);
            if att.attisdropped {
                continue;
            }
            if att.attname.name_str() == key.as_bytes() {
                found = Some(att.atttypid);
                if add_not_null {
                    let inhname = {
                        let mut s = PgString::new_in(mcx);
                        s.try_push_str(key)?;
                        leak_str(s)
                    };
                    nnconstraints.lappend(mcx, make_not_null_constraint(mcx, inhname)?)?;
                }
                break;
            }
        }
        rel.close(types_rel::NoLock)?;
        if found.is_some() {
            return Ok(found);
        }
    }
    Ok(None)
}

fn make_not_null_constraint<'mcx>(mcx: Mcx<'mcx>, colname: &'mcx str) -> PgResult<Node<'mcx>> {
    let mut n = Node::build::<Constraint>(mcx)?;
    n.contype = ConstrType::CONSTR_NOTNULL;
    n.keys = NodeList::make1(mcx, Node::mk_string(mcx, colname)?)?;
    n.is_enforced = true;
    n.skip_validation = false;
    n.initially_valid = true;
    n.location = -1;
    Ok(n.seal())
}

// transformIndexConstraints + transformIndexConstraint (CREATE TABLE +
// ALTER ADD COLUMN lanes; USING INDEX is loud). isalter: keys absent from
// `columns` are left for DefineIndex, and PK not-null forcing is skipped
// (ATPrepAddPrimaryKey / transformColumnDefinition already handled it,
// parse_utilcmd.c:2634-2668,2727-2733).
fn transform_index_constraints<'mcx>(
    mcx: Mcx<'mcx>,
    relname: &str,
    relation: Option<&'mcx types_nodes::RangeVar<'mcx>>,
    columns: &NodeList<'mcx>,
    inh_relations: &NodeList<'mcx>,
    nnconstraints: &mut NodeList<'mcx>,
    ixconstraints: &NodeList<'mcx>,
    alist: &mut NodeList<'mcx>,
    src: &str,
    isalter: bool,
) -> PgResult<()> {
    let mut indexlist = NodeList::nil();
    let mut pkey: Option<Node<'mcx>> = None;
    for cnode in ixconstraints.iter() {
        let constraint = cnode.as_variant::<Constraint>().expect("Constraint");
        debug_assert!(matches!(
            constraint.contype,
            ConstrType::CONSTR_PRIMARY
                | ConstrType::CONSTR_UNIQUE
                | ConstrType::CONSTR_EXCLUSION
        ));
        let is_exclusion = constraint.contype == ConstrType::CONSTR_EXCLUSION;
        if constraint.indexname.is_some() {
            return Err(cursor_at(
                Box::new(
                    PgError::new(
                        ERROR,
                        "cannot use an existing index in CREATE TABLE".to_string(),
                    )
                    .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
                ),
                Some(src.as_bytes()),
                constraint.location,
            ));
        }
        let mut index = Node::build::<IndexStmt>(mcx)?;
        index.unique = !is_exclusion;
        index.primary = constraint.contype == ConstrType::CONSTR_PRIMARY;
        if index.primary {
            if pkey.is_some() {
                return Err(multiple_pkeys(relname, constraint.location));
            }
        }
        index.nulls_not_distinct = constraint.nulls_not_distinct;
        index.isconstraint = true;
        index.iswithoutoverlaps = constraint.without_overlaps;
        index.deferrable = constraint.deferrable;
        index.initdeferred = constraint.initdeferred;
        index.idxname = constraint.conname;
        index.relation = relation;
        index.accessMethod = Some(constraint.access_method.unwrap_or("btree"));
        index.whereClause = constraint.where_clause;
        // SAFETY: parse tree is analyze-owned; the constraint node's options
        // list moves onto the IndexStmt (C shares the pointer).
        index.options =
            unsafe { cnode.with_mut::<Constraint, _>(|c| core::mem::take(&mut c.options)) }
                .expect("Constraint");
        index.tableSpace = constraint.indexspace;
        // Included columns (parse_utilcmd.c:2841-2929): no NOT NULL forcing,
        // no duplicate complaints.
        let mut including_params = NodeList::nil();
        for keynode in constraint.including.iter() {
            let key = keynode.as_string().expect("constraint including").sval;
            let mut found = columns
                .iter()
                .any(|cn| cn.as_variant::<ColumnDef>().expect("ColumnDef").colname == Some(key));
            if !found {
                if catalog_heap::SystemAttributeByName(key).is_some() {
                    found = true;
                } else if !inh_relations.is_nil() {
                    found = key_found_in_inh_relations(
                        mcx,
                        key,
                        false,
                        inh_relations,
                        nnconstraints,
                    )?
                    .is_some();
                }
            }
            if !found && !isalter {
                return Err(cursor_at(
                    key_column_missing(key, constraint.location),
                    Some(src.as_bytes()),
                    constraint.location,
                ));
            }
            let mut iparam = Node::build::<IndexElem>(mcx)?;
            iparam.name = Some(key);
            iparam.ordering = SortByDir::SORTBY_DEFAULT;
            iparam.nulls_ordering = SortByNulls::SORTBY_NULLS_DEFAULT;
            including_params.lappend(mcx, iparam.seal())?;
        }
        index.indexIncludingParams = including_params;

        if is_exclusion {
            let mut index_params = NodeList::nil();
            let mut exclude_op_names = NodeList::nil();
            for pair in constraint.exclusions.iter() {
                let pair = pair.as_list().expect("exclusions pair");
                index_params.lappend(mcx, pair.nth(0))?;
                exclude_op_names.lappend(mcx, pair.nth(1))?;
            }
            index.indexParams = index_params;
            index.excludeOpNames = exclude_op_names;
            indexlist.lappend(mcx, index.seal())?;
            continue;
        }

        let is_primary = index.primary;
        let mut index_params = NodeList::nil();
        let nkeys = constraint.keys.len();
        for (keyidx, keynode) in constraint.keys.iter().enumerate() {
            let key = keynode.as_string().expect("constraint keys").sval;
            let mut found = false;
            let mut found_column: Option<Node<'mcx>> = None;
            let mut typid = InvalidOid;
            for cn in columns.iter() {
                let cd = cn.as_variant::<ColumnDef>().expect("ColumnDef");
                if cd.colname != Some(key) {
                    continue;
                }
                found = true;
                found_column = Some(cn);
                // C: ALTER never needs the PK not-null forcing here
                // (parse_utilcmd.c:2634-2643).
                if is_primary && !isalter {
                    if cd.is_not_null {
                        for nn in nnconstraints.iter() {
                            let nnc = nn.as_variant::<Constraint>().expect("Constraint");
                            if nnc.keys.nth(0).as_string().expect("nn keys").sval == key {
                                if nnc.is_no_inherit {
                                    return Err(conflicting_no_inherit(key));
                                }
                                break;
                            }
                        }
                    } else {
                        // SAFETY: parse tree is analyze-owned; no derived refs.
                        unsafe {
                            cn.with_mut::<ColumnDef, _>(|c| c.is_not_null = true)
                                .expect("ColumnDef");
                        }
                        nnconstraints.lappend(mcx, make_not_null_constraint(mcx, key)?)?;
                    }
                }
                break;
            }
            if !found {
                if catalog_heap::SystemAttributeByName(key).is_some() {
                    // System columns are never null; no PK forcing needed
                    // (parse_utilcmd.c:2672-2680).
                    found = true;
                } else if !inh_relations.is_nil() {
                    if let Some(inh_typid) = key_found_in_inh_relations(
                        mcx,
                        key,
                        is_primary,
                        inh_relations,
                        nnconstraints,
                    )? {
                        found = true;
                        typid = inh_typid;
                    }
                }
            }
            // C: on the ALTER path missing keys may exist in the table
            // already; DefineIndex complains if not (parse_utilcmd.c:2734).
            if !found && !isalter {
                return Err(cursor_at(
                    key_column_missing(key, constraint.location),
                    Some(src.as_bytes()),
                    constraint.location,
                ));
            }
            for ip in index_params.iter() {
                let iparam = ip.as_variant::<IndexElem>().expect("IndexElem");
                if iparam.name == Some(key) {
                    return Err(duplicate_key_column(key, is_primary, constraint.location));
                }
            }
            // C: the WITHOUT OVERLAPS part must be a range or multirange type.
            if constraint.without_overlaps && keyidx == nkeys - 1 {
                if found {
                    if typid == InvalidOid {
                        if let Some(cn) = found_column {
                            let cd = cn.as_variant::<ColumnDef>().expect("ColumnDef");
                            if let Some(tn_node) = cd.typeName {
                                let tn = tn_node.as_variant::<TypeName>().expect("TypeName");
                                typid = typenameTypeIdAndMod(mcx, None, tn)?.0;
                            }
                        }
                    }
                    if typid == InvalidOid
                        || !(lsyscache::type_is_range(typid)?
                            || lsyscache::type_is_multirange(typid)?)
                    {
                        return Err(cursor_at(
                            Box::new(
                                PgError::new(
                                    ERROR,
                                    format!(
                                        "column \"{key}\" in WITHOUT OVERLAPS is not a range or multirange type"
                                    ),
                                )
                                .with_sqlstate(types_error::ERRCODE_DATATYPE_MISMATCH),
                            ),
                            Some(src.as_bytes()),
                            constraint.location,
                        ));
                    }
                }
            }
            let mut iparam = Node::build::<IndexElem>(mcx)?;
            iparam.name = Some(key);
            iparam.ordering = SortByDir::SORTBY_DEFAULT;
            iparam.nulls_ordering = SortByNulls::SORTBY_NULLS_DEFAULT;
            index_params.lappend(mcx, iparam.seal())?;
        }
        if constraint.without_overlaps {
            // Per SQL standard: at least one equality column besides the
            // WITHOUT OVERLAPS column.
            if constraint.keys.len() < 2 {
                return Err(Box::new(
                    PgError::new(
                        ERROR,
                        "constraint using WITHOUT OVERLAPS needs at least two columns"
                            .to_string(),
                    )
                    .with_sqlstate(types_error::ERRCODE_SYNTAX_ERROR),
                ));
            }
            // WITHOUT OVERLAPS requires a GiST index.
            index.accessMethod = Some("gist");
        }
        let index_node = {
            index.indexParams = index_params;
            index.seal()
        };
        if is_primary {
            pkey = Some(index_node);
        }
        indexlist.lappend(mcx, index_node)?;
    }

    // Redundant-specification dedup (e.g. UNIQUE + PRIMARY KEY on one column).
    let mut finalindexlist = NodeList::nil();
    if let Some(pk) = pkey {
        finalindexlist.lappend(mcx, pk)?;
    }
    for inode in indexlist.iter() {
        if let Some(pk) = pkey {
            if inode.as_raw() == pk.as_raw() {
                continue;
            }
        }
        let index = inode.as_variant::<IndexStmt>().expect("IndexStmt");
        let mut keep = true;
        for pnode in finalindexlist.iter() {
            let prior = pnode.as_variant::<IndexStmt>().expect("IndexStmt");
            // C compares whereClause with equal(); predicate exclusions
            // conservatively never merge (only identical duplicate
            // constraints diverge: both indexes get built).
            if index_params_equal(&index.indexParams, &prior.indexParams)
                && index_params_equal(&index.indexIncludingParams, &prior.indexIncludingParams)
                && exclude_op_names_equal(&index.excludeOpNames, &prior.excludeOpNames)
                && index.accessMethod == prior.accessMethod
                && index.whereClause.is_none()
                && prior.whereClause.is_none()
                && index.nulls_not_distinct == prior.nulls_not_distinct
                && index.deferrable == prior.deferrable
                && index.initdeferred == prior.initdeferred
            {
                let idxname = index.idxname;
                // SAFETY: parse tree is analyze-owned; no derived refs.
                unsafe {
                    pnode
                        .with_mut::<IndexStmt, _>(|p| {
                            p.unique |= index.unique;
                            if p.idxname.is_none() {
                                p.idxname = idxname;
                            }
                        })
                        .expect("IndexStmt");
                }
                keep = false;
                break;
            }
        }
        if keep {
            finalindexlist.lappend(mcx, inode)?;
        }
    }
    for inode in finalindexlist.iter() {
        alist.lappend(mcx, inode)?;
    }
    Ok(())
}

// transformIndexConstraint, isalter slice: keys are not resolved against any
// column list (DefineIndex complains about missing ones); the PK not-null
// forcing happened in ATPrepAddPrimaryKey. USING INDEX / DEFERRABLE /
// WITHOUT OVERLAPS are loud, as in the CREATE lane.
pub fn transformIndexConstraintForAlter<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &types_rel::Relation<'_>,
    cnode: Node<'mcx>,
    query_string: &str,
) -> PgResult<(Node<'mcx>, NodeList<'mcx>)> {
    let constraint = cnode.as_variant::<Constraint>().expect("Constraint");
    debug_assert!(matches!(
        constraint.contype,
        ConstrType::CONSTR_PRIMARY
            | ConstrType::CONSTR_UNIQUE
            | ConstrType::CONSTR_EXCLUSION
    ));
    let is_exclusion = constraint.contype == ConstrType::CONSTR_EXCLUSION;
    // transformTableConstraint isforeign guards (parse_utilcmd.c:1043-1066).
    if rel.rd_rel.relkind == types_rel::RELKIND_FOREIGN_TABLE {
        let what = match constraint.contype {
            ConstrType::CONSTR_PRIMARY => "primary key",
            ConstrType::CONSTR_UNIQUE => "unique",
            _ => "exclusion",
        };
        return Err(not_supported_on_foreign_tables(
            what,
            Some(query_string),
            constraint.location,
        ));
    }
    if constraint.indexname.is_some() {
        return transform_existing_index_constraint(mcx, rel, cnode, query_string);
    }
    let mut index = Node::build::<IndexStmt>(mcx)?;
    index.unique = !is_exclusion;
    index.primary = constraint.contype == ConstrType::CONSTR_PRIMARY;
    index.nulls_not_distinct = constraint.nulls_not_distinct;
    index.isconstraint = true;
    index.iswithoutoverlaps = constraint.without_overlaps;
    index.deferrable = constraint.deferrable;
    index.initdeferred = constraint.initdeferred;
    index.idxname = constraint.conname;
    index.accessMethod = Some(constraint.access_method.unwrap_or("btree"));
    index.whereClause = constraint.where_clause;
    // SAFETY: parse tree is statement-owned; the constraint node's options
    // list moves onto the IndexStmt (C shares the pointer).
    index.options =
        unsafe { cnode.with_mut::<Constraint, _>(|c| core::mem::take(&mut c.options)) }
            .expect("Constraint");
    index.tableSpace = constraint.indexspace;

    if is_exclusion {
        let mut index_params = NodeList::nil();
        let mut exclude_op_names = NodeList::nil();
        for pair in constraint.exclusions.iter() {
            let pair = pair.as_list().expect("exclusions pair");
            index_params.lappend(mcx, pair.nth(0))?;
            exclude_op_names.lappend(mcx, pair.nth(1))?;
        }
        index.indexParams = index_params;
        index.excludeOpNames = exclude_op_names;
        return Ok((index.seal(), NodeList::nil()));
    }

    let is_primary = index.primary;
    let mut index_params = NodeList::nil();
    let nkeys = constraint.keys.len();
    for (keyidx, keynode) in constraint.keys.iter().enumerate() {
        let key = keynode.as_string().expect("constraint keys").sval;
        for ip in index_params.iter() {
            let iparam = ip.as_variant::<IndexElem>().expect("IndexElem");
            if iparam.name == Some(key) {
                return Err(duplicate_key_column(key, is_primary, constraint.location));
            }
        }
        // C isalter: resolve the WITHOUT OVERLAPS column's type on the
        // existing table; if absent, DefineIndex complains later.
        if constraint.without_overlaps && keyidx == nkeys - 1 {
            let desc = rel.descr();
            for i in 0..desc.natts as usize {
                let att = desc.attr(i);
                if att.attisdropped {
                    break;
                }
                if att.attname.name_str() == key.as_bytes() {
                    let typid = att.atttypid;
                    if typid == InvalidOid
                        || !(lsyscache::type_is_range(typid)?
                            || lsyscache::type_is_multirange(typid)?)
                    {
                        return Err(cursor_at(
                            Box::new(
                                PgError::new(
                                    ERROR,
                                    format!(
                                        "column \"{key}\" in WITHOUT OVERLAPS is not a range or multirange type"
                                    ),
                                )
                                .with_sqlstate(types_error::ERRCODE_DATATYPE_MISMATCH),
                            ),
                            Some(query_string.as_bytes()),
                            constraint.location,
                        ));
                    }
                    break;
                }
            }
        }
        let mut iparam = Node::build::<IndexElem>(mcx)?;
        iparam.name = Some(key);
        iparam.ordering = SortByDir::SORTBY_DEFAULT;
        iparam.nulls_ordering = SortByNulls::SORTBY_NULLS_DEFAULT;
        index_params.lappend(mcx, iparam.seal())?;
    }
    if constraint.without_overlaps {
        if constraint.keys.len() < 2 {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    "constraint using WITHOUT OVERLAPS needs at least two columns".to_string(),
                )
                .with_sqlstate(types_error::ERRCODE_SYNTAX_ERROR),
            ));
        }
        index.accessMethod = Some("gist");
    }
    index.indexParams = index_params;
    // Included columns, isalter slice: keys may exist already, so no
    // missing-column complaint here; DefineIndex raises it (2915-2919).
    let mut including_params = NodeList::nil();
    for keynode in constraint.including.iter() {
        let key = keynode.as_string().expect("constraint including").sval;
        let mut iparam = Node::build::<IndexElem>(mcx)?;
        iparam.name = Some(key);
        iparam.ordering = SortByDir::SORTBY_DEFAULT;
        iparam.nulls_ordering = SortByNulls::SORTBY_NULLS_DEFAULT;
        including_params.lappend(mcx, iparam.seal())?;
    }
    index.indexIncludingParams = including_params;
    Ok((index.seal(), NodeList::nil()))
}

// transformIndexConstraint's USING INDEX arm (parse_utilcmd.c:2397-2574);
// returns the IndexStmt (indexOid set) + the PK not-null Constraint nodes C
// puts in cxt->nnconstraints.
fn transform_existing_index_constraint<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &types_rel::Relation<'_>,
    cnode: Node<'mcx>,
    src: &str,
) -> PgResult<(Node<'mcx>, NodeList<'mcx>)> {
    let constraint = cnode.as_variant::<Constraint>().expect("Constraint");
    let index_name = constraint.indexname.expect("Constraint.indexname");
    debug_assert!(constraint.keys.is_nil());
    let at = |e: Box<PgError>| cursor_at(e, Some(src.as_bytes()), constraint.location);

    let mut index = Node::build::<IndexStmt>(mcx)?;
    index.unique = true;
    index.primary = constraint.contype == ConstrType::CONSTR_PRIMARY;
    index.nulls_not_distinct = constraint.nulls_not_distinct;
    index.isconstraint = true;
    index.deferrable = constraint.deferrable;
    index.initdeferred = constraint.initdeferred;
    index.idxname = constraint.conname;
    index.accessMethod = Some("btree");
    index.tableSpace = constraint.indexspace;

    let index_oid = lsyscache::get_relname_relid(index_name, rel.rd_rel.relnamespace)?;
    if index_oid == InvalidOid {
        return Err(at(Box::new(
            PgError::new(ERROR, format!("index \"{index_name}\" does not exist"))
                .with_sqlstate(types_error::ERRCODE_UNDEFINED_OBJECT),
        )));
    }
    let index_rel = indexam::index_open(mcx, index_oid, types_rel::AccessShareLock)?;
    let existing_err = |msg: String| -> Box<PgError> {
        at(Box::new(
            PgError::new(ERROR, msg)
                .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
        ))
    };
    let wrong_type = |msg: String, detail: bool| -> Box<PgError> {
        let mut e = PgError::new(ERROR, msg)
            .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE);
        if detail {
            e = e.with_detail(
                "Cannot create a primary key or unique constraint using such an index."
                    .to_string(),
            );
        }
        at(Box::new(e))
    };
    if pg_depend::get_index_constraint(mcx, index_oid)? != InvalidOid {
        return Err(existing_err(format!(
            "index \"{index_name}\" is already associated with a constraint"
        )));
    }
    let index_form = index_rel.rd_index.as_ref().expect("rd_index");
    if index_form.indrelid != rel.rd_id {
        return Err(existing_err(format!(
            "index \"{index_name}\" does not belong to table \"{}\"",
            rel.name()
        )));
    }
    if !index_form.indisvalid {
        return Err(existing_err(format!("index \"{index_name}\" is not valid")));
    }
    if !index_form.indisunique {
        return Err(wrong_type(format!("\"{index_name}\" is not a unique index"), true));
    }
    if index_form.indexprs_src.is_some() {
        return Err(wrong_type(format!("index \"{index_name}\" contains expressions"), true));
    }
    if index_form.has_indpred {
        return Err(wrong_type(format!("\"{index_name}\" is a partial index"), true));
    }
    if !index_form.indimmediate && !constraint.deferrable {
        let mut e = PgError::new(ERROR, format!("\"{index_name}\" is a deferrable index"))
            .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE);
        e = e.with_detail(
            "Cannot create a non-deferrable constraint using a deferrable index.".to_string(),
        );
        return Err(at(Box::new(e)));
    }
    if index_rel.rd_rel.relam != types_core::BTREE_AM_OID {
        return Err(wrong_type(format!("index \"{index_name}\" is not a btree"), false));
    }

    let (indcollation, indclass, indoption) = pg_index_vectors(mcx, index_oid)?;
    let mut nnconstraints = NodeList::nil();
    let mut keys = NodeList::nil();
    let mut including = NodeList::nil();
    for i in 0..index_form.indnatts as usize {
        let attnum = index_form.indkey[i];
        if attnum <= 0 {
            // Expression columns were rejected above; system columns can't be
            // indexed here (index creation on them is an earlier error).
            unported("transformIndexConstraint: USING INDEX over a system column");
        }
        let attform = rel.rd_att.attr(attnum as usize - 1);
        let attname = {
            let mut s = PgString::new_in(mcx);
            s.try_push_str(
                core::str::from_utf8(attform.attname.name_str()).expect("attname UTF-8"),
            )?;
            leak_str(s)
        };
        if i < index_form.indnkeyatts as usize {
            let attoptions_set = index_attoptions_set(mcx, index_oid, (i + 1) as i16)?;
            let defopclass = indexcmds_seams::get_default_opclass::call(
                attform.atttypid,
                index_rel.rd_rel.relam,
            )?;
            if indclass[i] != defopclass
                || attform.attcollation != indcollation[i]
                || attoptions_set
                || indoption[i] != 0
            {
                return Err(wrong_type(
                    format!(
                        "index \"{index_name}\" column number {} does not have default sorting behavior",
                        i + 1
                    ),
                    true,
                ));
            }
            keys.lappend(mcx, Node::mk_string(mcx, attname)?)?;
            if constraint.contype == ConstrType::CONSTR_PRIMARY {
                nnconstraints.lappend(mcx, make_not_null_constraint(mcx, attname)?)?;
            }
        } else {
            including.lappend(mcx, Node::mk_string(mcx, attname)?)?;
        }
    }
    // SAFETY: parse tree is statement-owned; no derived refs live.
    unsafe {
        cnode
            .with_mut::<Constraint, _>(|c| {
                c.keys = keys;
                c.including = including;
            })
            .expect("Constraint");
    }
    index_rel.close(types_rel::NoLock)?;
    index.indexOid = index_oid;
    Ok((index.seal(), nnconstraints))
}

const IndexRelidIndexId: Oid = 2679;
const AttributeRelidNumIndexId: Oid = 2659;
const Anum_pg_index_indcollation: usize = 17;
const Anum_pg_index_indclass: usize = 18;
const Anum_pg_index_indoption: usize = 19;
const Anum_pg_attribute_attoptions: usize = 23;

fn catalog_oid_key(attno: usize, oid: Oid) -> types_scan::scankey::ScanKeyData {
    let mut key = types_scan::scankey::ScanKeyData::empty();
    key.sk_attno = attno as i16;
    key.sk_strategy = types_scan::scankey::BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = datum::Datum::from_oid(oid);
    key
}

fn pg_index_vectors<'mcx>(
    mcx: Mcx<'mcx>,
    indexoid: Oid,
) -> PgResult<(mcx::PgVec<'mcx, Oid>, mcx::PgVec<'mcx, Oid>, mcx::PgVec<'mcx, i16>)> {
    let pg_index =
        table::table_open(mcx, types_core::INDEX_RELATION_ID, types_rel::AccessShareLock)?;
    let key = catalog_oid_key(1, indexoid);
    let mut scan =
        genam::systable_beginscan(mcx, &pg_index, IndexRelidIndexId, true, None, &[key])?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for index {indexoid}"));
    let desc = pg_index.descr();
    let mut vector_image = |attnum: usize| {
        let mut isnull = false;
        // SAFETY: fixed NOT NULL pg_index vector columns under its descriptor.
        let d = unsafe { types_tuple::heap_getattr(tup, attnum as i32, desc, &mut isnull) };
        assert!(!isnull, "unexpected null pg_index attnum {attnum}");
        let p = d.as_usize() as *const u8;
        // SAFETY: NOT NULL varlena, live through the scan.
        unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) }
    };
    let coll_elems =
        datum::array_build::deconstruct_array_image(mcx, vector_image(Anum_pg_index_indcollation), 4, true, b'i')?;
    let class_elems =
        datum::array_build::deconstruct_array_image(mcx, vector_image(Anum_pg_index_indclass), 4, true, b'i')?;
    let opt_elems =
        datum::array_build::deconstruct_array_image(mcx, vector_image(Anum_pg_index_indoption), 2, true, b's')?;
    let mut indcollation: mcx::PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, coll_elems.len())?;
    indcollation.extend(coll_elems.iter().map(|d| d.as_oid()));
    let mut indclass: mcx::PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, class_elems.len())?;
    indclass.extend(class_elems.iter().map(|d| d.as_oid()));
    let mut indoption: mcx::PgVec<'mcx, i16> = mcx::vec_with_capacity_in(mcx, opt_elems.len())?;
    indoption.extend(opt_elems.iter().map(|d| d.as_i16()));
    genam::systable_endscan(mcx, scan)?;
    pg_index.close(types_rel::AccessShareLock)?;
    Ok((indcollation, indclass, indoption))
}

// get_attoptions(relid, attnum) != (Datum) 0 — only the null test is needed.
fn index_attoptions_set(mcx: Mcx<'_>, relid: Oid, attnum: i16) -> PgResult<bool> {
    let pg_attribute =
        table::table_open(mcx, types_core::ATTRIBUTE_RELATION_ID, types_rel::AccessShareLock)?;
    let key1 = catalog_oid_key(1, relid);
    let mut key2 = types_scan::scankey::ScanKeyData::empty();
    key2.sk_attno = 5;
    key2.sk_strategy = types_scan::scankey::BTEqualStrategyNumber;
    key2.sk_collation = 0;
    key2.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_INT2EQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_INT2EQ) failed: {e:?}"));
    key2.sk_argument = datum::Datum::from_i16(attnum);
    let mut scan = genam::systable_beginscan(
        mcx,
        &pg_attribute,
        AttributeRelidNumIndexId,
        true,
        None,
        &[key1, key2],
    )?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for attribute {attnum} of relation {relid}"));
    let mut isnull = false;
    // SAFETY: attoptions under pg_attribute's descriptor; null checked.
    let _ = unsafe {
        types_tuple::heap_getattr(
            tup,
            Anum_pg_attribute_attoptions as i32,
            pg_attribute.descr(),
            &mut isnull,
        )
    };
    genam::systable_endscan(mcx, scan)?;
    pg_attribute.close(types_rel::AccessShareLock)?;
    Ok(!isnull)
}

fn index_params_equal(a: &NodeList<'_>, b: &NodeList<'_>) -> bool {
    if a.len() != b.len() {
        return false;
    }
    for (x, y) in a.iter().zip(b.iter()) {
        let xe = x.as_variant::<IndexElem>().expect("IndexElem");
        let ye = y.as_variant::<IndexElem>().expect("IndexElem");
        // Expression elems: C uses equal(); never merged here (see dedup note).
        if xe.expr.is_some() || ye.expr.is_some() {
            return false;
        }
        if xe.name != ye.name
            || xe.ordering != ye.ordering
            || xe.nulls_ordering != ye.nulls_ordering
        {
            return false;
        }
    }
    true
}

fn exclude_op_names_equal(a: &NodeList<'_>, b: &NodeList<'_>) -> bool {
    if a.len() != b.len() {
        return false;
    }
    for (x, y) in a.iter().zip(b.iter()) {
        let xs = x.as_list().expect("op name list");
        let ys = y.as_list().expect("op name list");
        if xs.len() != ys.len() {
            return false;
        }
        for (xn, yn) in xs.iter().zip(ys.iter()) {
            if xn.as_string().expect("op name").sval != yn.as_string().expect("op name").sval {
                return false;
            }
        }
    }
    true
}

// Serial names live as long as the parse arena (C pallocs likewise).
fn leak_str(s: PgString<'_>) -> &str {
    // SAFETY: PgString invariant — bytes are valid UTF-8.
    unsafe { core::str::from_utf8_unchecked(s.into_bytes().leak()) }
}

// generateSerialExtraStmts, CREATE TABLE + ALTER serial/identity arms
// (SEQUENCE NAME/LOGGED/UNLOGGED options ported). rel = C's cxt->rel
// (ALTER TABLE: namespace/persistence/owner come from the existing table);
// col_exists routes the OWNED BY AlterSeqStmt to blist (AT_AddIdentity).
// Takes the node handle only: it mutates the ColumnDef (identitySequence), so
// no caller-held &ColumnDef may cross this call.
pub(crate) fn generateSerialExtraStmts<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &RangeVar<'mcx>,
    column_node: Node<'mcx>,
    seqtypid: Oid,
    seqoptions: NodeList<'mcx>,
    for_identity: bool,
    rel: Option<&types_rel::Relation<'_>>,
    col_exists: bool,
    cxt: &mut CreateStmtCxt<'mcx>,
) -> PgResult<(&'mcx str, &'mcx str)> {
    // C strips the non-CREATE-SEQUENCE options (SEQUENCE NAME, LOGGED/
    // UNLOGGED) from the list before handing it to CREATE SEQUENCE (they'd
    // be redundant there), erroring on duplicates (errorConflictingDefElem;
    // no pstate here, so no errposition — C attaches cxt->pstate's).
    let conflicting = || -> Box<PgError> {
        Box::new(
            PgError::new(ERROR, "conflicting or redundant options".to_string())
                .with_sqlstate(ERRCODE_SYNTAX_ERROR),
        )
    };
    let mut name_el: Option<&DefElem<'mcx>> = None;
    // Some(true) = UNLOGGED, Some(false) = LOGGED.
    let mut logged_el_unlogged: Option<bool> = None;
    let mut filtered = NodeList::nil();
    for opt in seqoptions.iter() {
        let defel = opt.as_variant::<DefElem>().expect("DefElem in seqoptions");
        match defel.defname {
            Some("sequence_name") => {
                if name_el.is_some() {
                    return Err(conflicting());
                }
                name_el = Some(defel);
            }
            Some(d @ ("logged" | "unlogged")) => {
                if logged_el_unlogged.is_some() {
                    return Err(conflicting());
                }
                logged_el_unlogged = Some(d == "unlogged");
            }
            _ => filtered.lappend(mcx, opt)?,
        }
    }
    let seqoptions = filtered;

    let snamespaceid = match rel {
        Some(r) => r.rd_rel.relnamespace,
        None => RangeVarGetCreationNamespace(mcx, relation)?,
    };
    let snamespace_default = leak_str(
        lsyscache::get_namespace_name(mcx, snamespaceid)?
            .unwrap_or_else(|| panic!("cache lookup failed for namespace {snamespaceid}")),
    );
    let relname = relation.relname.expect("RangeVar.relname");
    let colname = column_node
        .as_variant::<ColumnDef>()
        .expect("ColumnDef")
        .colname
        .expect("ColumnDef.colname");
    // SEQUENCE NAME picks the user-specified name (C's
    // makeRangeVarFromNameList arm: only schema+name are consumed — a
    // catalog part is dropped; unqualified names take the table's
    // namespace); otherwise generate one with ChooseRelationName.
    let (snamespace, sname) = if let Some(nel) = name_el {
        let names = nel
            .arg
            .expect("SEQUENCE NAME arg")
            .as_list()
            .expect("SEQUENCE NAME is a name list");
        let parts: Vec<&str> = names
            .iter()
            .map(|n| n.as_string().expect("qualified name component is a String node").sval)
            .collect();
        let (nschema, nname) = match parts.as_slice() {
            [r] => (None, *r),
            [s, r] => (Some(*s), *r),
            [_catalog, s, r] => (Some(*s), *r),
            _ => {
                return Err(Box::new(
                    PgError::new(
                        ERROR,
                        format!(
                            "improper qualified name (too many dotted names): {}",
                            parts.join(".")
                        ),
                    )
                    .with_sqlstate(ERRCODE_SYNTAX_ERROR),
                ))
            }
        };
        (nschema.unwrap_or(snamespace_default), nname)
    } else {
        let sname =
            leak_str(ChooseRelationName(mcx, relname, Some(colname), "seq", snamespaceid)?);
        (snamespace_default, sname)
    };

    // C parse_utilcmd.c:483-486: report the implicit sequence.
    // errmsg_internal: not translated.
    elog_seams::ereport::call(
        PgError::new(
            types_error::DEBUG1,
            format!(
                "{} will create implicit sequence \"{}\" for serial column \"{}.{}\"",
                cxt.stmt_type, sname, relname, colname
            ),
        )
        .with_location("parse_utilcmd.c", 483, "generateSerialExtraStmts"),
    )?;

    // C: the sequence copies the table's persistence, LOGGED/UNLOGGED
    // override it (rejected on TEMP tables).
    let mut seqpersistence =
        rel.map_or(relation.relpersistence, |r| r.rd_rel.relpersistence);
    if let Some(unlogged) = logged_el_unlogged {
        if seqpersistence == types_core::RELPERSISTENCE_TEMP {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    "cannot set logged status of a temporary sequence".to_string(),
                )
                .with_sqlstate(ERRCODE_INVALID_TABLE_DEFINITION),
            ));
        }
        seqpersistence = if unlogged {
            types_core::RELPERSISTENCE_UNLOGGED
        } else {
            types_core::RELPERSISTENCE_PERMANENT
        };
    }

    let seq_rv = Node::mk_mut(
        mcx,
        RangeVar {
            catalogname: None,
            schemaname: Some(snamespace),
            relname: Some(sname),
            inh: true,
            relpersistence: seqpersistence,
            alias: None,
            location: -1,
        },
    )?
    .seal_ref();

    // AS seqtypid, prepended so a user AS lands the redundant-option error;
    // skipped when no sequence data type was specified (LIKE INCLUDING
    // IDENTITY passes InvalidOid, parse_utilcmd.c:523-529).
    let mut options = seqoptions;
    if seqtypid != InvalidOid {
        let mut as_tn = Node::build::<TypeName>(mcx)?;
        as_tn.typeOid = seqtypid;
        as_tn.typemod = -1;
        as_tn.location = -1;
        let as_defel = Node::mk(
            mcx,
            DefElem {
                defnamespace: None,
                defname: Some("as"),
                arg: Some(as_tn.seal()),
                defaction: DefElemAction::DEFELEM_UNSPEC,
                location: -1,
            },
        )?;
        options.lcons(mcx, as_defel)?;
    }
    let mut seqstmt = Node::build::<CreateSeqStmt>(mcx)?;
    seqstmt.for_identity = for_identity;
    seqstmt.sequence = Some(seq_rv);
    seqstmt.options = options;
    seqstmt.ownerId = rel.map_or(InvalidOid, |r| r.rd_rel.relowner);
    cxt.blist.lappend(mcx, seqstmt.seal())?;

    // SAFETY: parse tree is analyze-owned; no derived refs live.
    unsafe {
        column_node
            .with_mut::<ColumnDef, _>(|c| c.identitySequence = Some(seq_rv))
            .expect("ColumnDef");
    }

    let mut attnamelist = NodeList::make1(mcx, Node::mk_string(mcx, snamespace)?)?;
    attnamelist.lappend(mcx, Node::mk_string(mcx, relname)?)?;
    attnamelist.lappend(mcx, Node::mk_string(mcx, colname)?)?;
    let owned_defel = Node::mk(
        mcx,
        DefElem {
            defnamespace: None,
            defname: Some("owned_by"),
            arg: Some(Node::mk_list(mcx, attnamelist)?),
            defaction: DefElemAction::DEFELEM_UNSPEC,
            location: -1,
        },
    )?;
    let mut altseqstmt = Node::build::<AlterSeqStmt>(mcx)?;
    altseqstmt.sequence = Some(seq_rv);
    altseqstmt.options = NodeList::make1(mcx, owned_defel)?;
    altseqstmt.for_identity = for_identity;
    if col_exists {
        cxt.blist.lappend(mcx, altseqstmt.seal())?;
    } else {
        cxt.alist.lappend(mcx, altseqstmt.seal())?;
    }

    Ok((snamespace, sname))
}

fn RangeVarGetCreationNamespace<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &RangeVar<'_>,
) -> PgResult<Oid> {
    let rv = rel_vocab::RangeVar {
        catalogname: relation.catalogname,
        schemaname: relation.schemaname,
        relname: relation.relname.expect("RangeVar.relname"),
        inh: relation.inh,
        relpersistence: relation.relpersistence,
        location: relation.location,
    };
    catalog_namespace::RangeVarGetCreationNamespace(mcx, &rv)
}

// RangeVarGetAndCheckCreationNamespace (namespace.c) over a primnodes RangeVar;
// same marshal as the plain variant above.
fn RangeVarGetAndCheckCreationNamespace<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &RangeVar<'_>,
    lockmode: types_rel::LOCKMODE,
    want_existing: bool,
) -> PgResult<(Oid, Oid, u8)> {
    let rv = rel_vocab::RangeVar {
        catalogname: relation.catalogname,
        schemaname: relation.schemaname,
        relname: relation.relname.expect("RangeVar.relname"),
        inh: relation.inh,
        relpersistence: relation.relpersistence,
        location: relation.location,
    };
    catalog_namespace::RangeVarGetAndCheckCreationNamespace(mcx, &rv, lockmode, want_existing)
}

// C has exactly ONE ChooseRelationName (indexcmds.c:2606, exported via
// defrem.h) and parse_utilcmd.c:476 calls it. This port had grown a second,
// weaker copy here: it probed via the RELNAMENSP syscache instead of a
// systable scan -- which can never be made dirty-snapshot, so it could not be
// fixed in place -- and it dropped the isconstraint parameter entirely. Both
// copies are now the one in indexcmds, reached through a seam because indexcmds
// already depends on this crate. C passes isconstraint = false here
// (parse_utilcmd.c:480).
fn ChooseRelationName<'mcx>(
    mcx: Mcx<'mcx>,
    name1: &str,
    name2: Option<&str>,
    label: &str,
    namespaceid: Oid,
) -> PgResult<PgString<'mcx>> {
    indexcmds_seams::choose_relation_name::call(mcx, name1, name2, label, namespaceid, false)
}

// quote_identifier + quote_qualified_identifier (ruleutils.c).
// quote_all_identifiers GUC is unported (default off).
fn ident_needs_quotes(ident: &str) -> bool {
    let b = ident.as_bytes();
    if b.is_empty() {
        return true;
    }
    let safe_first = b[0].is_ascii_lowercase() || b[0] == b'_';
    let safe = safe_first
        && b.iter().all(|&c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_');
    if !safe {
        return true;
    }
    let kwnum = keywords::ScanKeywordLookup(b, &keywords::ScanKeywords);
    if kwnum >= 0 {
        return keywords::ScanKeywordCategories[kwnum as usize]
            != keywords::KeywordCategory::Unreserved;
    }
    false
}

pub fn quote_identifier<'mcx>(mcx: Mcx<'mcx>, ident: &str) -> PgResult<PgString<'mcx>> {
    let mut out = mcx::PgString::new_in(mcx);
    if !ident_needs_quotes(ident) {
        out.try_push_str(ident)?;
        return Ok(out);
    }
    out.try_push_str("\"")?;
    for c in ident.chars() {
        if c == '"' {
            out.try_push_str("\"")?;
        }
        out.try_push(c)?;
    }
    out.try_push_str("\"")?;
    Ok(out)
}

pub fn quote_qualified_identifier<'mcx>(
    mcx: Mcx<'mcx>,
    qualifier: Option<&str>,
    ident: &str,
) -> PgResult<PgString<'mcx>> {
    let mut out = mcx::PgString::new_in(mcx);
    if let Some(q) = qualifier {
        out.try_push_str(&quote_identifier(mcx, q)?)?;
        out.try_push_str(".")?;
    }
    out.try_push_str(&quote_identifier(mcx, ident)?)?;
    Ok(out)
}

// transformAlterTableStmt's per-subcommand slice (ATParseTransformCmd's
// working half): reuses the CREATE-lane transformColumnDefinition. The
// subcommand is transformed in place (C rebuilds an equal newcmds list);
// generated CHECK/NOT NULL constraints, IndexStmts and FK constraints come
// back in cxt for the caller to schedule as AT_AddIndex[Constraint]/
// AT_AddConstraint subcommands; blist/alist carry the serial/identity
// sequence statements (C beforeStmts/afterStmts).
pub fn transformAlterTableCmd<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &types_rel::Relation<'_>,
    relname: &str,
    cnode: Node<'mcx>,
    query_string: &str,
) -> PgResult<CreateStmtCxt<'mcx>> {
    use types_nodes::parsenodes::{AlterTableCmd, AlterTableType};
    let cmd = cnode.as_variant::<AlterTableCmd>().expect("AlterTableCmd");
    let mut ckconstraints = NodeList::nil();
    let mut nnconstraints = NodeList::nil();
    let mut cxt = CreateStmtCxt::new(
        if rel.rd_rel.relkind == types_rel::RELKIND_FOREIGN_TABLE {
            "ALTER FOREIGN TABLE"
        } else {
            "ALTER TABLE"
        },
    );
    let arena_relname =|| -> PgResult<&'mcx str> {
        let mut s = PgString::new_in(mcx);
        s.try_push_str(relname)?;
        Ok(leak_str(s))
    };
    match cmd.subtype {
        AlterTableType::AT_AddColumn => {
            let defnode = cmd.def.expect("AT_AddColumn ColumnDef");
            let mut ixconstraints = NodeList::nil();
            let mut fkconstraints = NodeList::nil();
            let mut rv = RangeVar::default();
            rv.relname = Some(arena_relname()?);
            rv.inh = true;
            rv.relpersistence = types_core::RELPERSISTENCE_PERMANENT;
            rv.location = -1;
            transformColumnDefinition(
                mcx,
                defnode,
                &rv,
                Some(rel),
                Some(query_string),
                &mut cxt,
                &mut ckconstraints,
                &mut nnconstraints,
                &mut ixconstraints,
                &mut fkconstraints,
                false,
                false,
                false,
                rel.rd_rel.relkind == types_rel::RELKIND_PARTITIONED_TABLE,
            )?;
            // transformIndexConstraints (parse_utilcmd.c:3806): the isalter
            // lane resolves keys against the single new column and leaves
            // absent columns to DefineIndex.
            if !ixconstraints.is_nil() {
                let columns = NodeList::make1(mcx, defnode)?;
                // C: cxt.inhRelations = NIL on the ALTER path
                // (parse_utilcmd.c:3570).
                transform_index_constraints(
                    mcx,
                    relname,
                    None,
                    &columns,
                    &NodeList::nil(),
                    &mut nnconstraints,
                    &ixconstraints,
                    &mut cxt.ixstmts,
                    query_string,
                    true,
                )?;
            }
            // transformFKConstraints(skipValidation = no non-null default,
            // isAddConstraint = true): the new column has no rows to check.
            let cd = defnode.as_variant::<ColumnDef>().expect("ColumnDef");
            if cd.raw_default.is_none() {
                for fknode in fkconstraints.iter() {
                    // SAFETY: parse tree is statement-owned; no derived refs.
                    unsafe {
                        fknode
                            .with_mut::<Constraint, _>(|c| {
                                c.skip_validation = true;
                                c.initially_valid = c.is_enforced;
                            })
                            .expect("Constraint");
                    }
                }
            }
            // SAFETY: parse tree is analyze-owned; no derived refs live.
            unsafe {
                defnode
                    .with_mut::<ColumnDef, _>(|c| c.constraints = NodeList::nil())
                    .expect("ColumnDef");
            }
            cxt.fkconstraints = fkconstraints;
        }
        AlterTableType::AT_AddIdentity => {
            let defnode = cmd.def.expect("AT_AddIdentity Constraint");
            let con = defnode
                .as_variant::<types_nodes::rawnodes::Constraint>()
                .expect("Constraint");
            let colname = cmd.name.expect("AT_AddIdentity name");
            let when = con.generated_when;
            let mut newdef = Node::build::<ColumnDef>(mcx)?;
            newdef.colname = Some({
                let mut s = PgString::new_in(mcx);
                s.try_push_str(colname)?;
                leak_str(s)
            });
            newdef.identity = when;
            newdef.location = -1;
            let newdef_node = newdef.seal();
            let attnum = lsyscache::get_attnum(rel.rd_id, colname)?;
            if attnum == 0 {
                return Err(alter_undefined_column(colname, relname));
            }
            let mut rv = RangeVar::default();
            rv.relname = Some(arena_relname()?);
            rv.inh = true;
            rv.relpersistence = rel.rd_rel.relpersistence;
            rv.location = -1;
            generateSerialExtraStmts(
                mcx,
                &rv,
                newdef_node,
                lsyscache::get_atttype(rel.rd_id, attnum)?,
                con.options.clone_in(mcx)?,
                true,
                Some(rel),
                true,
                &mut cxt,
            )?;
            // SAFETY: parse tree is statement-owned; no derived refs live.
            unsafe {
                cnode
                    .with_mut::<AlterTableCmd, _>(|c| c.def = Some(newdef_node))
                    .expect("AlterTableCmd");
            }
        }
        AlterTableType::AT_SetIdentity => {
            let mut newseqopts = NodeList::nil();
            let mut newdef = NodeList::nil();
            for opt in cmd.def.expect("AT_SetIdentity options").as_list().expect("DefElem list").iter()
            {
                let defel = opt.as_variant::<DefElem>().expect("DefElem");
                if defel.defname == Some("generated") {
                    newdef.lappend(mcx, opt)?;
                } else {
                    newseqopts.lappend(mcx, opt)?;
                }
            }
            let colname = cmd.name.expect("AT_SetIdentity name");
            let attnum = lsyscache::get_attnum(rel.rd_id, colname)?;
            if attnum == 0 {
                return Err(alter_undefined_column(colname, relname));
            }
            let seq_relid = pg_depend::getIdentitySequence(mcx, rel.rd_id, attnum as i32, true)?;
            if seq_relid != InvalidOid {
                let snamespaceid = lsyscache::get_rel_namespace(seq_relid)?;
                let snamespace = leak_str(
                    lsyscache::get_namespace_name(mcx, snamespaceid)?
                        .unwrap_or_else(|| panic!("cache lookup failed for namespace {snamespaceid}")),
                );
                let sname = leak_str(
                    lsyscache::get_rel_name(mcx, seq_relid)?
                        .unwrap_or_else(|| panic!("cache lookup failed for relation {seq_relid}")),
                );
                let mut seq_rv = RangeVar::default();
                seq_rv.schemaname = Some(snamespace);
                seq_rv.relname = Some(sname);
                seq_rv.inh = true;
                seq_rv.relpersistence = types_core::RELPERSISTENCE_PERMANENT;
                seq_rv.location = -1;
                let mut seqstmt = Node::build::<AlterSeqStmt>(mcx)?;
                seqstmt.sequence = Some(Node::mk_mut(mcx, seq_rv)?.seal_ref());
                seqstmt.options = newseqopts;
                seqstmt.for_identity = true;
                seqstmt.missing_ok = false;
                cxt.blist.lappend(mcx, seqstmt.seal())?;
            }
            // A non-identity column errors in ATExecSetIdentity, per C.
            let newdef_node =
                if newdef.is_nil() { None } else { Some(Node::mk_list(mcx, newdef)?) };
            // SAFETY: parse tree is statement-owned; no derived refs live.
            unsafe {
                cnode
                    .with_mut::<AlterTableCmd, _>(|c| c.def = newdef_node)
                    .expect("AlterTableCmd");
            }
        }
        AlterTableType::AT_DropColumn
        | AlterTableType::AT_ColumnDefault
        | AlterTableType::AT_DropIdentity
        | AlterTableType::AT_DropNotNull
        | AlterTableType::AT_SetNotNull
        | AlterTableType::AT_DropConstraint
        | AlterTableType::AT_SetStatistics
        | AlterTableType::AT_SetStorage => {}
        AlterTableType::AT_AlterColumnType => {
            // The USING transform stays raw here; tablecmds'
            // ATPrepAlterColumnType cooks it (C cooks in this arm).
            let defnode = cmd.def.expect("AT_AlterColumnType ColumnDef");
            let cd = defnode.as_variant::<ColumnDef>().expect("ColumnDef");
            // Identity sequences hang off the top-level partitioned table.
            if !rel.rd_rel.relispartition {
                let colname = cmd.name.expect("AT_AlterColumnType name");
                let attnum = lsyscache::get_attnum(rel.rd_id, colname)?;
                if attnum == 0 {
                    return Err(alter_undefined_column(colname, relname));
                }
                if attnum > 0 && rel.rd_att.attr(attnum as usize - 1).attidentity != 0 {
                    let seq_relid =
                        pg_depend::getIdentitySequence(mcx, rel.rd_id, attnum as i32, false)?;
                    let tn = cd
                        .typeName
                        .expect("ColumnDef.typeName")
                        .as_variant::<TypeName>()
                        .expect("TypeName");
                    let (type_oid, _) = typenameTypeIdAndMod(mcx, None, tn)?;
                    let snamespaceid = lsyscache::get_rel_namespace(seq_relid)?;
                    let snamespace = leak_str(
                        lsyscache::get_namespace_name(mcx, snamespaceid)?.unwrap_or_else(
                            || panic!("cache lookup failed for namespace {snamespaceid}"),
                        ),
                    );
                    let sname = leak_str(
                        lsyscache::get_rel_name(mcx, seq_relid)?.unwrap_or_else(|| {
                            panic!("cache lookup failed for relation {seq_relid}")
                        }),
                    );
                    let mut seq_rv = RangeVar::default();
                    seq_rv.schemaname = Some(snamespace);
                    seq_rv.relname = Some(sname);
                    seq_rv.inh = true;
                    seq_rv.relpersistence = types_core::RELPERSISTENCE_PERMANENT;
                    seq_rv.location = -1;
                    let mut newtn = Node::build::<TypeName>(mcx)?;
                    newtn.typeOid = type_oid;
                    newtn.typemod = -1;
                    newtn.location = -1;
                    let asdef = Node::mk(
                        mcx,
                        DefElem {
                            defnamespace: None,
                            defname: Some("as"),
                            arg: Some(newtn.seal()),
                            defaction: DefElemAction::DEFELEM_UNSPEC,
                            location: -1,
                        },
                    )?;
                    let mut altseqstmt = Node::build::<AlterSeqStmt>(mcx)?;
                    altseqstmt.sequence = Some(Node::mk_mut(mcx, seq_rv)?.seal_ref());
                    altseqstmt.options = NodeList::make1(mcx, asdef)?;
                    altseqstmt.for_identity = true;
                    cxt.blist.lappend(mcx, altseqstmt.seal())?;
                }
            }
        }
        AlterTableType::AT_AddConstraint => {
            // transformTableConstraint: CHECK/FOREIGN pass through untouched;
            // index-backed contypes are unported lanes.
            let defnode = cmd.def.expect("AT_AddConstraint Constraint");
            let c = defnode
                .as_variant::<types_nodes::rawnodes::Constraint>()
                .expect("Constraint");
            match c.contype {
                types_nodes::rawnodes::ConstrType::CONSTR_CHECK
                | types_nodes::rawnodes::ConstrType::CONSTR_FOREIGN
                | types_nodes::rawnodes::ConstrType::CONSTR_PRIMARY
                | types_nodes::rawnodes::ConstrType::CONSTR_UNIQUE
                | types_nodes::rawnodes::ConstrType::CONSTR_NOTNULL => {}
                other => unported(&format!("transformTableConstraint {other:?} arm")),
            }
        }
        // unported: subcommands whose transformAlterTableStmt analysis isn't
        // wired yet; clean 0A000 (the panic was user-reachable).
        other => {
            return Err(unported_feature_at(
                None,
                -1,
                &format!("this ALTER TABLE subcommand ({other:?})"),
            ))
        }
    }
    cxt.ckconstraints = ckconstraints;
    cxt.nnconstraints = nnconstraints;
    Ok(cxt)
}

#[track_caller]
#[cold]
#[inline(never)]
fn alter_undefined_column(colname: &str, relname: &str) -> Box<PgError> {
    Box::new(
        PgError::new(
            ERROR,
            format!("column \"{colname}\" of relation \"{relname}\" does not exist"),
        )
        .with_sqlstate(types_error::ERRCODE_UNDEFINED_COLUMN),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
#[cold]
#[inline(never)]
fn cursor_at(mut e: Box<PgError>, src: Option<&[u8]>, location: i32) -> Box<PgError> {
    let pos =
        parser_small1::parser_errposition_source(src, location, mbutils::GetDatabaseEncoding());
    if pos > 0 {
        e.cursor_position = Some(pos);
    }
    e
}

#[track_caller]
#[cold]
#[inline(never)]
fn multiple_pkeys(relname: &str, _location: i32) -> Box<PgError> {
    Box::new(
        PgError::new(
            ERROR,
            format!("multiple primary keys for table \"{relname}\" are not allowed"),
        )
        .with_sqlstate(types_error::ERRCODE_INVALID_TABLE_DEFINITION),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn conflicting_no_inherit(colname: &str) -> Box<PgError> {
    Box::new(
        PgError::new(
            ERROR,
            format!(
                "conflicting NO INHERIT declaration for not-null constraint on column \
                 \"{colname}\""
            ),
        )
        .with_sqlstate(ERRCODE_SYNTAX_ERROR),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn key_column_missing(colname: &str, _location: i32) -> Box<PgError> {
    Box::new(
        PgError::new(
            ERROR,
            format!("column \"{colname}\" named in key does not exist"),
        )
        .with_sqlstate(types_error::ERRCODE_UNDEFINED_COLUMN),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn duplicate_key_column(colname: &str, primary: bool, _location: i32) -> Box<PgError> {
    let what = if primary { "primary key" } else { "unique" };
    Box::new(
        PgError::new(
            ERROR,
            format!("column \"{colname}\" appears twice in {what} constraint"),
        )
        .with_sqlstate(types_error::ERRCODE_DUPLICATE_COLUMN),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn not_supported_on_foreign_tables(what: &str, src: Option<&str>, location: i32) -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, format!("{what} constraints are not supported on foreign tables"))
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
            .with_cursor_position(parser_small1::parser_errposition_source(
                src.map(str::as_bytes),
                location,
                mbutils::GetDatabaseEncoding(),
            )),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn column_syntax_error(
    msg: core::fmt::Arguments<'_>,
    src: Option<&str>,
    location: i32,
) -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, msg.to_string())
            .with_sqlstate(ERRCODE_SYNTAX_ERROR)
            .with_cursor_position(parser_small1::parser_errposition_source(
                src.map(str::as_bytes),
                location,
                mbutils::GetDatabaseEncoding(),
            )),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn multiple_defaults(colname: &str, relname: &str) -> Box<PgError> {
    Box::new(
        PgError::new(
            ERROR,
            format!(
                "multiple default values specified for column \"{colname}\" of \
                 table \"{relname}\""
            ),
        )
        .with_sqlstate(ERRCODE_SYNTAX_ERROR),
    )
}

pub fn init_seams() {
    parse_utilcmd_seams::LookupTypeNameOid::set(LookupTypeNameOid);
    parse_utilcmd_seams::parseTypeString::set(parseTypeString);
    parse_utilcmd_seams::typename_type_id_and_mod::set(typenameTypeIdAndMod);
    parse_utilcmd_seams::typename_type_id_and_mod_any::set(typenameTypeIdAndModAllowComposite);
    regproc_seams::parse_type_string::set(parseTypeStringEsc);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> &'static mcx::MemoryContext {
        Box::leak(Box::new(mcx::MemoryContext::new("utilcmd-test")))
    }

    #[test]
    fn parse_type_string_soft_hard_split() {
        let mcx = ctx().mcx();
        // Whitespace-only input: typeStringToTypeName's own arm is soft.
        let mut soft = SoftErrorContext::new(true);
        assert!(typeStringToTypeNameEsc(mcx, " \t ", Some(&mut soft)).unwrap().is_none());
        assert_eq!(soft.error().unwrap().message(), "invalid type name \" \t \"");
        // >3 dotted names stay hard even with a soft context
        // (DeconstructQualifiedName's default arm, namespace.c).
        let mut soft = SoftErrorContext::new(true);
        let err = parseTypeStringEsc(mcx, "way.too.many.names", Some(&mut soft)).unwrap_err();
        assert_eq!(
            err.message(),
            "improper qualified name (too many dotted names): way.too.many.names"
        );
        // Raw-parser syntax errors stay hard, with pts_error_callback's
        // context line.
        let mut soft = SoftErrorContext::new(true);
        let err =
            parseTypeStringEsc(mcx, "incorrect type name syntax", Some(&mut soft)).unwrap_err();
        assert_eq!(err.message(), "syntax error at or near \"type\"");
        assert_eq!(err.context(), Some("invalid type name \"incorrect type name syntax\""));
    }

    #[test]
    fn quote_identifier_matches_c() {
        let mcx = ctx().mcx();
        assert_eq!(quote_identifier(mcx, "st_id_seq").unwrap().as_str(), "st_id_seq");
        assert_eq!(quote_identifier(mcx, "MiXed").unwrap().as_str(), "\"MiXed\"");
        assert_eq!(quote_identifier(mcx, "se\"q").unwrap().as_str(), "\"se\"\"q\"");
        // reserved keyword quoted; unreserved keyword bare.
        assert_eq!(quote_identifier(mcx, "select").unwrap().as_str(), "\"select\"");
        assert_eq!(quote_identifier(mcx, "between").unwrap().as_str(), "\"between\"");
        assert_eq!(quote_identifier(mcx, "cache").unwrap().as_str(), "cache");
        assert_eq!(
            quote_qualified_identifier(mcx, Some("public"), "t_id_seq").unwrap().as_str(),
            "public.t_id_seq"
        );
    }

    fn mk_columns<'mcx>(mcx: Mcx<'mcx>, names: &[&'static str]) -> NodeList<'mcx> {
        let mut columns = NodeList::nil();
        for name in names {
            let mut cd = Node::build::<ColumnDef>(mcx).unwrap();
            cd.colname = Some(name);
            columns.lappend(mcx, cd.seal()).unwrap();
        }
        columns
    }

    fn mk_unique_constraint<'mcx>(
        mcx: Mcx<'mcx>,
        contype: ConstrType,
        keys: &[&'static str],
        including: &[&'static str],
    ) -> Node<'mcx> {
        let mut con = Node::build::<Constraint>(mcx).unwrap();
        con.contype = contype;
        con.location = -1;
        let mut keylist = NodeList::nil();
        for k in keys {
            keylist.lappend(mcx, Node::mk_string(mcx, k).unwrap()).unwrap();
        }
        con.keys = keylist;
        let mut inclist = NodeList::nil();
        for k in including {
            inclist.lappend(mcx, Node::mk_string(mcx, k).unwrap()).unwrap();
        }
        con.including = inclist;
        con.seal()
    }

    fn elem_names(params: &NodeList<'_>) -> Vec<String> {
        params
            .iter()
            .map(|n| {
                n.as_variant::<IndexElem>().expect("IndexElem").name.expect("named").to_string()
            })
            .collect()
    }

    fn run_transform<'mcx>(
        mcx: Mcx<'mcx>,
        columns: &NodeList<'mcx>,
        ixconstraints: &NodeList<'mcx>,
    ) -> PgResult<NodeList<'mcx>> {
        let relation = Node::mk(
            mcx,
            RangeVar {
                catalogname: None,
                schemaname: None,
                relname: Some("t"),
                inh: false,
                relpersistence: b'p',
                alias: None,
                location: -1,
            },
        )?
        .as_range_var()
        .expect("RangeVar");
        let mut nnconstraints = NodeList::nil();
        let mut alist = NodeList::nil();
        transform_index_constraints(
            mcx,
            "t",
            Some(relation),
            columns,
            &NodeList::nil(),
            &mut nnconstraints,
            ixconstraints,
            &mut alist,
            "",
            false,
        )?;
        Ok(alist)
    }

    #[test]
    fn transform_index_constraint_include_lowering() {
        let mcx = ctx().mcx();
        let columns = mk_columns(mcx, &["a", "b", "c"]);
        let con =
            mk_unique_constraint(mcx, ConstrType::CONSTR_UNIQUE, &["a"], &["b", "c"]);
        let ix = NodeList::make1(mcx, con).unwrap();
        let alist = run_transform(mcx, &columns, &ix).unwrap();
        assert_eq!(alist.len(), 1);
        let stmt = alist.nth(0).as_variant::<IndexStmt>().expect("IndexStmt");
        assert!(stmt.unique);
        assert_eq!(elem_names(&stmt.indexParams), ["a"]);
        assert_eq!(elem_names(&stmt.indexIncludingParams), ["b", "c"]);
    }

    #[test]
    fn transform_index_constraint_include_missing_column() {
        let mcx = ctx().mcx();
        let columns = mk_columns(mcx, &["a"]);
        let con = mk_unique_constraint(mcx, ConstrType::CONSTR_UNIQUE, &["a"], &["z"]);
        let ix = NodeList::make1(mcx, con).unwrap();
        let e = run_transform(mcx, &columns, &ix).unwrap_err();
        assert_eq!(e.message(), "column \"z\" named in key does not exist");
    }

    #[test]
    fn transform_index_constraint_include_blocks_dedup() {
        // PRIMARY KEY (a) and UNIQUE (a) INCLUDE (b) differ in included
        // columns, so both indexes survive (parse_utilcmd.c:2296).
        let mcx = ctx().mcx();
        let columns = mk_columns(mcx, &["a", "b"]);
        let mut ix = NodeList::nil();
        ix.lappend(mcx, mk_unique_constraint(mcx, ConstrType::CONSTR_PRIMARY, &["a"], &[]))
            .unwrap();
        ix.lappend(
            mcx,
            mk_unique_constraint(mcx, ConstrType::CONSTR_UNIQUE, &["a"], &["b"]),
        )
        .unwrap();
        let alist = run_transform(mcx, &columns, &ix).unwrap();
        assert_eq!(alist.len(), 2);
    }

    fn mk_con(mcx: Mcx<'_>, contype: ConstrType) -> Node<'_> {
        let mut c = Node::build::<Constraint>(mcx).unwrap();
        c.contype = contype;
        c.location = -1;
        c.seal()
    }

    #[test]
    fn constraint_attrs_not_enforced_marks_invalid() {
        let mcx = ctx().mcx();
        let check = mk_con(mcx, ConstrType::CONSTR_CHECK);
        // CHECK constraints start enforced/valid at parse time (gram rule 503).
        unsafe {
            check
                .with_mut::<Constraint, _>(|c| {
                    c.is_enforced = true;
                    c.initially_valid = true;
                })
                .unwrap();
        }
        let attr = mk_con(mcx, ConstrType::CONSTR_ATTR_NOT_ENFORCED);
        let mut list = NodeList::make1(mcx, check).unwrap();
        list.lappend(mcx, attr).unwrap();
        transformConstraintAttrs(&list, None).unwrap();
        let c = check.as_variant::<Constraint>().unwrap();
        assert!(!c.is_enforced);
        assert!(c.skip_validation);
        assert!(!c.initially_valid);
    }

    #[test]
    fn constraint_attrs_double_enforced_errors() {
        let mcx = ctx().mcx();
        let check = mk_con(mcx, ConstrType::CONSTR_CHECK);
        let mut list = NodeList::make1(mcx, check).unwrap();
        list.lappend(mcx, mk_con(mcx, ConstrType::CONSTR_ATTR_NOT_ENFORCED)).unwrap();
        list.lappend(mcx, mk_con(mcx, ConstrType::CONSTR_ATTR_ENFORCED)).unwrap();
        let err = transformConstraintAttrs(&list, None).unwrap_err();
        assert_eq!(err.message(), "multiple ENFORCED/NOT ENFORCED clauses not allowed");
    }
}
