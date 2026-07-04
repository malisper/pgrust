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
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_SYNTAX_ERROR,
    ERRCODE_UNDEFINED_OBJECT, ERRCODE_UNDEFINED_SCHEMA, ERROR,
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

#[cold]
#[inline(never)]
fn type_does_not_exist(name: &str) -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, format!("type \"{name}\" does not exist"))
            .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
    )
}

// typenameTypeIdAndMod (parse_type.c); pstate feeds errposition around the
// typmodin call (C's setup_parser_errposition_callback).
pub fn typenameTypeIdAndMod<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: Option<&parser_small1::ParseState<'_, '_>>,
    tn: &TypeName<'_>,
) -> PgResult<(Oid, i32)> {
    typename_type_id_and_mod(mcx, pstate, tn, false)
}

// C's typenameTypeIdAndMod has no typtype gate; the column lanes here narrow
// it, so composite-consumers (ALTER TABLE OF) use the allowing variant.
pub fn typenameTypeIdAndModAllowComposite<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: Option<&parser_small1::ParseState<'_, '_>>,
    tn: &TypeName<'_>,
) -> PgResult<(Oid, i32)> {
    typename_type_id_and_mod(mcx, pstate, tn, true)
}

fn typename_type_id_and_mod<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: Option<&parser_small1::ParseState<'_, '_>>,
    tn: &TypeName<'_>,
    allow_composite: bool,
) -> PgResult<(Oid, i32)> {
    if tn.pct_type || tn.setof {
        unported("LookupTypeName %TYPE / SETOF");
    }
    if tn.names.is_nil() {
        // LookupTypeName pre-resolved arm (makeTypeNameFromOid; LIKE / OF type).
        assert!(tn.typeOid != InvalidOid, "TypeName without names or typeOid");
        match syscache_seams::pg_type_isdefined::call(tn.typeOid)? {
            Some(true) => {}
            _ => unported("shell types (typisdefined = false)"),
        }
        match syscache_seams::pg_type_typtype::call(tn.typeOid)? {
            Some(t) if t == b'b' as i8 || t == b'e' as i8 => {}
            _ => unported("non-base/enum pre-resolved column types"),
        }
        let typmod = typenameTypeMod(mcx, pstate, tn, tn.typeOid)?;
        return Ok((tn.typeOid, typmod));
    }
    if tn.typeOid != InvalidOid {
        debug_assert!(tn.names.is_nil());
        return Ok((tn.typeOid, -1));
    }

    let (typoid, typname) = resolveTypeNames(mcx, tn)?;
    if typoid == InvalidOid {
        return Err(type_does_not_exist(typname));
    }
    // C LookupTypeNameExtended: array bounds convert to the array type.
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
        _ => unported("shell types (typisdefined = false)"),
    }
    match syscache_seams::pg_type_typtype::call(typoid)? {
        Some(t)
            if t == b'b' as i8
                || t == b'e' as i8
                || t == b'r' as i8
                || t == b'm' as i8
                || t == b'd' as i8 => {}
        Some(t) if t == b'c' as i8 && allow_composite => {}
        Some(t) => unported(match t as u8 {
            b'c' => "composite column types",
            b'p' => "pseudo-type columns",
            _ => "unknown typtype",
        }),
        None => return Err(type_does_not_exist(typname)),
    }
    let typmod = typenameTypeMod(mcx, pstate, tn, typoid)?;
    Ok((typoid, typmod))
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
        unported("LookupTypeName %TYPE / SETOF");
    }
    if tn.names.is_nil() || tn.typeOid != InvalidOid {
        unported("pre-resolved TypeName.typeOid lane");
    }
    let (typoid, typname) = resolveTypeNames(mcx, tn)?;
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
        _ => unported("shell types (typisdefined = false)"),
    }
    Ok(typoid)
}

// The names→Oid walk shared by typenameTypeIdAndMod and parseTypeString
// (LookupTypeNameExtended's "normal reference" arm, pre array-bounds).
fn resolveTypeNames<'mcx, 'tn>(
    mcx: Mcx<'mcx>,
    tn: &TypeName<'tn>,
) -> PgResult<(Oid, &'tn str)> {
    let mut names: [&str; 4] = [""; 4];
    let nnames = tn.names.len();
    if nnames == 0 || nnames > 3 {
        unported("improper TypeName names length");
    }
    for (i, n) in tn.names.iter().enumerate() {
        names[i] = n.as_string().expect("TypeName names").sval;
    }
    let (schemaname, typname) = catalog_namespace::DeconstructQualifiedName(&names[..nnames])?;

    let typoid = match schemaname {
        Some(schemaname) => {
            let namespace_id = catalog_namespace::LookupExplicitNamespace(schemaname, false)?;
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

#[cold]
#[inline(never)]
fn invalid_type_name(s: &str) -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, format!("invalid type name \"{s}\""))
            .with_sqlstate(ERRCODE_SYNTAX_ERROR),
    )
}

#[cold]
#[inline(never)]
fn shell_type(name: &str) -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, format!("type \"{name}\" is only a shell"))
            .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
    )
}

// typeStringToTypeName (parse_type.c); escontext=NULL shape (hard errors) —
// misc.c's pg_input_* callers pass NULL there too. pts_error_callback rides
// as with_context on raw-parse errors only, matching the C callback's span.
fn typeStringToTypeName<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx TypeName<'mcx>> {
    if s.bytes().all(|c| matches!(c, b' ' | b'\t' | b'\n' | b'\r' | 0x0c | 0x0b)) {
        return Err(invalid_type_name(s));
    }
    let list = gram_core::raw_parser(mcx, s, parser_seams::RawParseMode::RAW_PARSE_TYPE_NAME)
        .map_err(|e| Box::new((*e).with_context(format!("invalid type name \"{s}\""))))?;
    debug_assert_eq!(list.len(), 1);
    let node = list.first().expect("TYPE_NAME parse yields one node");
    let tn = node.as_type_name().expect("TYPE_NAME parse yields TypeName");
    if tn.setof {
        return Err(invalid_type_name(s));
    }
    Ok(tn)
}

/// C `parseTypeString` with a NULL escontext: (type Oid, typmod) for a
/// standalone type-name string. No typtype restriction (unlike the CREATE
/// TABLE lane above): any resolvable non-shell type passes, per C.
pub fn parseTypeString<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<(Oid, i32)> {
    let tn = typeStringToTypeName(mcx, s)?;
    if tn.pct_type {
        unported("LookupTypeName %TYPE");
    }
    if tn.typeOid != InvalidOid {
        unported("pre-resolved TypeName.typeOid lane");
    }

    let (mut typoid, _typname) = resolveTypeNames(mcx, tn)?;
    if typoid != InvalidOid && !tn.arrayBounds.is_nil() {
        typoid = syscache_seams::pg_type_typarray::call(typoid)?.unwrap_or(InvalidOid);
    }
    if typoid == InvalidOid {
        return Err(type_does_not_exist(&typeNameToString(tn)));
    }

    match syscache_seams::pg_type_isdefined::call(typoid)? {
        Some(true) => {}
        Some(false) => return Err(shell_type(&typeNameToString(tn))),
        None => return Err(type_does_not_exist(&typeNameToString(tn))),
    }

    let typmod = typenameTypeMod(mcx, None, tn, typoid)?;
    Ok((typoid, typmod))
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
fn typenameTypeMod<'mcx>(
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

struct CreateStmtCxt<'mcx> {
    blist: NodeList<'mcx>,
    alist: NodeList<'mcx>,
}

fn transformColumnDefinition<'mcx>(
    mcx: Mcx<'mcx>,
    column_node: Node<'mcx>,
    column: &ColumnDef<'mcx>,
    relation: &RangeVar<'mcx>,
    src: Option<&str>,
    cxt: &mut CreateStmtCxt<'mcx>,
    ckconstraints: &mut NodeList<'mcx>,
    nnconstraints: &mut NodeList<'mcx>,
    ixconstraints: &mut NodeList<'mcx>,
    fkconstraints: &mut NodeList<'mcx>,
) -> PgResult<()> {
    let relname = relation.relname.unwrap_or("");
    if column.raw_default.is_some() || column.cooked_default.is_some() {
        unported("pre-split column defaults");
    }

    // SERIAL pseudo-types (transformColumnDefinition's is_serial arm).
    let mut is_serial_oid = InvalidOid;
    if let Some(tn_node) = column.typeName {
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
    if is_serial_oid != InvalidOid {
        let (snamespace, sname) = generateSerialExtraStmts(
            mcx,
            relation,
            column_node,
            column,
            is_serial_oid,
            NodeList::nil(),
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
    }

    let mut saw_nullable = false;
    let mut saw_default = false;
    let mut col_not_null = column.is_not_null;
    let mut saw_identity = false;
    let mut saw_generated = false;
    for cnode in column.constraints.iter() {
        let constraint = cnode.as_variant::<Constraint>().expect("column constraint");
        match constraint.contype {
            ConstrType::CONSTR_DEFAULT => {
                if saw_default {
                    return Err(multiple_defaults(
                        column.colname.unwrap_or(""),
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
                let tn = column
                    .typeName
                    .expect("ColumnDef.typeName")
                    .as_variant::<TypeName>()
                    .expect("TypeName");
                let (type_oid, _typmod) = typenameTypeIdAndMod(mcx, None, tn)?;
                if saw_identity {
                    return Err(column_syntax_error(
                        format_args!(
                            "multiple identity specifications for column \"{}\" of table \"{}\"",
                            column.colname.unwrap_or(""),
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
                    column,
                    type_oid,
                    // C list_copy: generateSerialExtraStmts prepends AS.
                    constraint.options.clone_in(mcx)?,
                    true,
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
                } else if !column.is_not_null {
                    return Err(column_syntax_error(
                        format_args!(
                            "conflicting NULL/NOT NULL declarations for column \"{}\" of table \"{}\"",
                            column.colname.unwrap_or(""),
                            relname
                        ),
                        src,
                        constraint.location,
                    ));
                }
            }
            ConstrType::CONSTR_GENERATED => {
                if saw_generated {
                    return Err(column_syntax_error(
                        format_args!(
                            "multiple generation clauses specified for column \"{}\" of table \"{}\"",
                            column.colname.unwrap_or(""),
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
                if col_not_null {
                    unported("redundant NOT NULL merge (notnull_constraint conname)");
                }
                saw_nullable = true;
                col_not_null = true;
                let colname = column.colname.expect("ColumnDef.colname");
                let keys = NodeList::make1(mcx, Node::mk_string(mcx, colname)?)?;
                // SAFETY (both): parse tree is analyze-owned; no derived refs.
                unsafe {
                    column_node
                        .with_mut::<ColumnDef, _>(|c| c.is_not_null = true)
                        .expect("ColumnDef");
                    cnode
                        .with_mut::<Constraint, _>(|c| c.keys = keys)
                        .expect("Constraint");
                }
                nnconstraints.lappend(mcx, cnode)?;
            }
            ConstrType::CONSTR_PRIMARY | ConstrType::CONSTR_UNIQUE => {
                if constraint.contype == ConstrType::CONSTR_PRIMARY {
                    if saw_nullable && !col_not_null {
                        return Err(conflicting_null_decls(
                            column.colname.unwrap_or(""),
                            relname,
                        ));
                    }
                    need_notnull = true;
                }
                if constraint.keys.is_nil() {
                    let colname = column.colname.expect("ColumnDef.colname");
                    let keys = NodeList::make1(mcx, Node::mk_string(mcx, colname)?)?;
                    // SAFETY: parse tree is analyze-owned; no derived refs.
                    unsafe {
                        cnode.with_mut::<Constraint, _>(|c| c.keys = keys).expect("Constraint");
                    }
                }
                ixconstraints.lappend(mcx, cnode)?;
            }
            ConstrType::CONSTR_FOREIGN => {
                let colname = column.colname.expect("ColumnDef.colname");
                let fk_attrs = NodeList::make1(mcx, Node::mk_string(mcx, colname)?)?;
                // SAFETY: parse tree is analyze-owned; no derived refs.
                unsafe {
                    cnode
                        .with_mut::<Constraint, _>(|c| c.fk_attrs = fk_attrs)
                        .expect("Constraint");
                }
                fkconstraints.lappend(mcx, cnode)?;
            }
            other => unported(match other {
                ConstrType::CONSTR_NULL => "NULL column constraints",
                _ => "constraint attributes (transformConstraintAttrs)",
            }),
        }
        if saw_default && saw_identity {
            return Err(column_syntax_error(
                format_args!(
                    "both default and identity specified for column \"{}\" of table \"{}\"",
                    column.colname.unwrap_or(""),
                    relname
                ),
                src,
                constraint.location,
            ));
        }
        if saw_default && saw_generated {
            return Err(column_syntax_error(
                format_args!(
                    "both default and generation expression specified for column \"{}\" of table \"{}\"",
                    column.colname.unwrap_or(""),
                    relname
                ),
                src,
                constraint.location,
            ));
        }
        if saw_identity && saw_generated {
            return Err(column_syntax_error(
                format_args!(
                    "both identity and generation expression specified for column \"{}\" of table \"{}\"",
                    column.colname.unwrap_or(""),
                    relname
                ),
                src,
                constraint.location,
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
        let colname = column.colname.expect("ColumnDef.colname");
        nnconstraints.lappend(mcx, make_not_null_constraint(mcx, colname)?)?;
    }
    if column.is_from_type {
        unported("is_from_type columns (OF type / LIKE)");
    }
    let tn = column
        .typeName
        .expect("ColumnDef.typeName")
        .as_variant::<TypeName>()
        .expect("TypeName");
    // transformColumnType: validate the type reference and any COLLATE spec.
    let (type_oid, _typmod) = typenameTypeIdAndMod(mcx, None, tn)?;
    if let Some(cc) = column.collClause {
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

pub fn transformCreateStmt<'mcx>(
    mcx: Mcx<'mcx>,
    stmt_node: Node<'mcx>,
    query_string: &str,
) -> PgResult<NodeList<'mcx>> {
    let stmt = stmt_node
        .as_variant::<CreateStmt>()
        .expect("transformCreateStmt on non-CreateStmt");

    if stmt.if_not_exists {
        unported("IF NOT EXISTS");
    }
    if stmt.partbound.is_some() && !stmt.tableElts.is_nil() {
        unported("PARTITION OF with a column/constraint list");
    }
    if stmt.ofTypename.is_some() {
        unported("typed tables (OF type)");
    }
    debug_assert!(stmt.constraints.is_nil() && stmt.nnconstraints.is_nil());

    let relation = stmt.relation.expect("CreateStmt.relation");
    let relname = relation.relname.unwrap_or("");
    let mut columns = NodeList::nil();
    let mut cxt = CreateStmtCxt { blist: NodeList::nil(), alist: NodeList::nil() };
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
                let cd = elt.as_variant::<ColumnDef>().expect("ColumnDef");
                transformColumnDefinition(
                    mcx,
                    elt,
                    cd,
                    relation,
                    Some(query_string),
                    &mut cxt,
                    &mut ckconstraints,
                    &mut nnconstraints,
                    &mut ixconstraints,
                    &mut fkconstraints,
                )?;
                columns.lappend(mcx, elt)?;
            }
            NodeTag::T_TableLikeClause => {
                let mut cxt = like::LikeCxt {
                    relation,
                    columns: &mut columns,
                    nnconstraints: &mut nnconstraints,
                    likeclauses: &mut likeclauses,
                    alist: &mut save_alist,
                };
                like::transformTableLikeClause(mcx, &mut cxt, elt, query_string)?;
            }
            NodeTag::T_Constraint => {
                let c = elt.as_variant::<Constraint>().expect("Constraint");
                match c.contype {
                    ConstrType::CONSTR_PRIMARY
                    | ConstrType::CONSTR_UNIQUE
                    | ConstrType::CONSTR_EXCLUSION => ixconstraints.lappend(mcx, elt)?,
                    ConstrType::CONSTR_CHECK => ckconstraints.lappend(mcx, elt)?,
                    ConstrType::CONSTR_NOTNULL => nnconstraints.lappend(mcx, elt)?,
                    ConstrType::CONSTR_FOREIGN => fkconstraints.lappend(mcx, elt)?,
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
        relation,
        &columns,
        &mut nnconstraints,
        &ixconstraints,
        &mut alist,
        query_string,
    )?;

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

    // transformCheckConstraints(skipValidation=true): new plain table.
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
    // SAFETY: parse tree is analyze-owned; no derived refs live.
    unsafe {
        stmt_node
            .with_mut::<CreateStmt, _>(|s| {
                s.tableElts = columns;
                s.constraints = ckconstraints;
                s.nnconstraints = nnconstraints;
            })
            .expect("CreateStmt");
    }

    // C: result = blist ++ [stmt] ++ likeclauses ++ alist ++ save_alist
    // (serial's OWNED BY precedes index stmts).
    let mut result = cxt.blist;
    result.lappend(mcx, stmt_node)?;
    result.concat(mcx, &likeclauses)?;
    for n in cxt.alist.iter() {
        result.lappend(mcx, n)?;
    }
    for a in alist.iter() {
        result.lappend(mcx, a)?;
    }
    result.concat(mcx, &save_alist)?;
    Ok(result)
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

// transformIndexConstraints + transformIndexConstraint (CREATE TABLE lane;
// isalter/EXCLUSION/USING INDEX are loud).
fn transform_index_constraints<'mcx>(
    mcx: Mcx<'mcx>,
    relname: &str,
    relation: &'mcx types_nodes::RangeVar<'mcx>,
    columns: &NodeList<'mcx>,
    nnconstraints: &mut NodeList<'mcx>,
    ixconstraints: &NodeList<'mcx>,
    alist: &mut NodeList<'mcx>,
    src: &str,
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
        if (constraint.deferrable || constraint.initdeferred) && !is_exclusion {
            unported("transformIndexConstraint: DEFERRABLE unique/pk constraint indexes");
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
        index.deferrable = constraint.deferrable;
        index.initdeferred = constraint.initdeferred;
        index.idxname = constraint.conname;
        index.relation = Some(relation);
        index.accessMethod = Some(constraint.access_method.unwrap_or("btree"));
        index.whereClause = constraint.where_clause;
        // SAFETY: parse tree is analyze-owned; the constraint node's options
        // list moves onto the IndexStmt (C shares the pointer).
        index.options =
            unsafe { cnode.with_mut::<Constraint, _>(|c| core::mem::take(&mut c.options)) }
                .expect("Constraint");
        index.tableSpace = constraint.indexspace;
        if !constraint.including.is_nil() {
            unported("transformIndexConstraint: INCLUDE columns");
        }

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
        for keynode in constraint.keys.iter() {
            let key = keynode.as_string().expect("constraint keys").sval;
            let mut found = false;
            for cn in columns.iter() {
                let cd = cn.as_variant::<ColumnDef>().expect("ColumnDef");
                if cd.colname != Some(key) {
                    continue;
                }
                found = true;
                if is_primary {
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
                return Err(key_column_missing(key, constraint.location));
            }
            for ip in index_params.iter() {
                let iparam = ip.as_variant::<IndexElem>().expect("IndexElem");
                if iparam.name == Some(key) {
                    return Err(duplicate_key_column(key, is_primary, constraint.location));
                }
            }
            let mut iparam = Node::build::<IndexElem>(mcx)?;
            iparam.name = Some(key);
            iparam.ordering = SortByDir::SORTBY_DEFAULT;
            iparam.nulls_ordering = SortByNulls::SORTBY_NULLS_DEFAULT;
            index_params.lappend(mcx, iparam.seal())?;
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
// INCLUDE / WITHOUT OVERLAPS are loud, as in the CREATE lane.
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
    if constraint.indexname.is_some() {
        return transform_existing_index_constraint(mcx, rel, cnode, query_string);
    }
    if (constraint.deferrable || constraint.initdeferred) && !is_exclusion {
        unported("transformIndexConstraint: DEFERRABLE unique/pk constraint indexes");
    }
    if !constraint.including.is_nil() {
        unported("transformIndexConstraint: INCLUDE columns");
    }
    let mut index = Node::build::<IndexStmt>(mcx)?;
    index.unique = !is_exclusion;
    index.primary = constraint.contype == ConstrType::CONSTR_PRIMARY;
    index.nulls_not_distinct = constraint.nulls_not_distinct;
    index.isconstraint = true;
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
    for keynode in constraint.keys.iter() {
        let key = keynode.as_string().expect("constraint keys").sval;
        for ip in index_params.iter() {
            let iparam = ip.as_variant::<IndexElem>().expect("IndexElem");
            if iparam.name == Some(key) {
                return Err(duplicate_key_column(key, is_primary, constraint.location));
            }
        }
        let mut iparam = Node::build::<IndexElem>(mcx)?;
        iparam.name = Some(key);
        iparam.ordering = SortByDir::SORTBY_DEFAULT;
        iparam.nulls_ordering = SortByNulls::SORTBY_NULLS_DEFAULT;
        index_params.lappend(mcx, iparam.seal())?;
    }
    index.indexParams = index_params;
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

// generateSerialExtraStmts, CREATE TABLE serial+identity arms (ALTER lanes
// and SEQUENCE NAME/LOGGED/UNLOGGED options are loud).
fn generateSerialExtraStmts<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &RangeVar<'mcx>,
    column_node: Node<'mcx>,
    column: &ColumnDef<'mcx>,
    seqtypid: Oid,
    seqoptions: NodeList<'mcx>,
    for_identity: bool,
    cxt: &mut CreateStmtCxt<'mcx>,
) -> PgResult<(&'mcx str, &'mcx str)> {
    for opt in seqoptions.iter() {
        let defel = opt.as_variant::<DefElem>().expect("DefElem in seqoptions");
        match defel.defname {
            Some("sequence_name") => unported("identity SEQUENCE NAME option"),
            Some("logged") | Some("unlogged") => unported("identity LOGGED/UNLOGGED options"),
            _ => {}
        }
    }
    let snamespaceid = RangeVarGetCreationNamespace(mcx, relation)?;
    let snamespace = leak_str(
        lsyscache::get_namespace_name(mcx, snamespaceid)?
            .unwrap_or_else(|| panic!("cache lookup failed for namespace {snamespaceid}")),
    );
    let relname = relation.relname.expect("RangeVar.relname");
    let colname = column.colname.expect("ColumnDef.colname");
    let sname = leak_str(ChooseRelationName(mcx, relname, Some(colname), "seq", snamespaceid)?);

    let seq_rv = Node::mk_mut(
        mcx,
        RangeVar {
            catalogname: None,
            schemaname: Some(snamespace),
            relname: Some(sname),
            inh: true,
            relpersistence: relation.relpersistence,
            alias: None,
            location: -1,
        },
    )?
    .seal_ref();

    // AS seqtypid, prepended so a user AS lands the redundant-option error.
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

    let mut options = seqoptions;
    options.lcons(mcx, as_defel)?;
    let mut seqstmt = Node::build::<CreateSeqStmt>(mcx)?;
    seqstmt.for_identity = for_identity;
    seqstmt.sequence = Some(seq_rv);
    seqstmt.options = options;
    seqstmt.ownerId = InvalidOid;
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
    cxt.alist.lappend(mcx, altseqstmt.seal())?;

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

// makeObjectName (indexcmds.c); names here are valid UTF-8, so pg_mbcliplen
// is a char-boundary clip.
fn makeObjectName<'mcx>(
    mcx: Mcx<'mcx>,
    name1: &str,
    name2: Option<&str>,
    label: &str,
) -> PgResult<PgString<'mcx>> {
    let mut overhead = label.len() + 1;
    if name2.is_some() {
        overhead += 1;
    }
    let availchars = NAMEDATALEN as usize - 1 - overhead;
    let mut name1chars = name1.len();
    let mut name2chars = name2.map_or(0, str::len);
    while name1chars + name2chars > availchars {
        if name1chars > name2chars {
            name1chars -= 1;
        } else {
            name2chars -= 1;
        }
    }
    let clip = |s: &str, mut n: usize| {
        while !s.is_char_boundary(n) {
            n -= 1;
        }
        n
    };
    let mut out = mcx::PgString::new_in(mcx);
    out.try_push_str(&name1[..clip(name1, name1chars)])?;
    if let Some(name2) = name2 {
        out.try_push_str("_")?;
        out.try_push_str(&name2[..clip(name2, name2chars)])?;
    }
    out.try_push_str("_")?;
    out.try_push_str(label)?;
    Ok(out)
}

// ChooseRelationName (indexcmds.c). DIVERGENCE: C probes pg_class under a
// dirty snapshot; this uses the MVCC get_relname_relid probe (single-backend,
// and DDL CCIs before the next probe can matter).
pub fn ChooseRelationName<'mcx>(
    mcx: Mcx<'mcx>,
    name1: &str,
    name2: Option<&str>,
    label: &str,
    namespaceid: Oid,
) -> PgResult<PgString<'mcx>> {
    let mut pass = 0u32;
    let mut modlabel = std::string::String::from(label);
    loop {
        let relname = makeObjectName(mcx, name1, name2, &modlabel)?;
        if lsyscache::get_relname_relid(&relname, namespaceid)? == InvalidOid {
            return Ok(relname);
        }
        pass += 1;
        modlabel = format!("{label}{pass}");
    }
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
// working half): reuses the CREATE-lane transformColumnDefinition; any
// queued constraint subcommand (CHECK / NOT NULL / index) is an unported
// ALTER lane. The subcommand is transformed in place (C rebuilds an equal
// newcmds list).
pub fn transformAlterTableCmd<'mcx>(
    mcx: Mcx<'mcx>,
    relname: &str,
    cnode: Node<'mcx>,
) -> PgResult<()> {
    use types_nodes::parsenodes::{AlterTableCmd, AlterTableType};
    let cmd = cnode.as_variant::<AlterTableCmd>().expect("AlterTableCmd");
    let mut ckconstraints = NodeList::nil();
    let mut nnconstraints = NodeList::nil();
    match cmd.subtype {
        AlterTableType::AT_AddColumn => {
            let defnode = cmd.def.expect("AT_AddColumn ColumnDef");
            let cd = defnode.as_variant::<ColumnDef>().expect("ColumnDef");
            let mut ixconstraints = NodeList::nil();
            let mut fkconstraints = NodeList::nil();
            let mut rv = RangeVar::default();
            rv.relname = Some({
                let mut s = PgString::new_in(mcx);
                s.try_push_str(relname)?;
                leak_str(s)
            });
            rv.inh = true;
            rv.relpersistence = types_core::RELPERSISTENCE_PERMANENT;
            rv.location = -1;
            let mut cxt = CreateStmtCxt { blist: NodeList::nil(), alist: NodeList::nil() };
            transformColumnDefinition(
                mcx,
                defnode,
                cd,
                &rv,
                None,
                &mut cxt,
                &mut ckconstraints,
                &mut nnconstraints,
                &mut ixconstraints,
                &mut fkconstraints,
            )?;
            if !ixconstraints.is_nil() || !fkconstraints.is_nil() {
                unported("ALTER TABLE ADD COLUMN with PRIMARY KEY/UNIQUE/REFERENCES");
            }
            if !cxt.blist.is_nil() || !cxt.alist.is_nil() {
                unported("ALTER TABLE ADD COLUMN serial/identity (extra statements)");
            }
            // SAFETY: parse tree is analyze-owned; no derived refs live.
            unsafe {
                defnode
                    .with_mut::<ColumnDef, _>(|c| c.constraints = NodeList::nil())
                    .expect("ColumnDef");
            }
        }
        AlterTableType::AT_DropColumn
        | AlterTableType::AT_ColumnDefault
        | AlterTableType::AT_DropNotNull
        | AlterTableType::AT_SetNotNull
        | AlterTableType::AT_DropConstraint
        | AlterTableType::AT_SetStatistics
        | AlterTableType::AT_SetStorage => {}
        AlterTableType::AT_AlterColumnType => {
            let defnode = cmd.def.expect("AT_AlterColumnType ColumnDef");
            let cd = defnode.as_variant::<ColumnDef>().expect("ColumnDef");
            if cd.raw_default.is_some() {
                unported("transformAlterTableStmt AT_AlterColumnType USING transform");
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
        other => unported(&format!("transformAlterTableStmt {other:?} arm")),
    }
    if !ckconstraints.is_nil() || !nnconstraints.is_nil() {
        unported("ALTER TABLE ADD COLUMN with CHECK/NOT NULL (AT_AddConstraint lane)");
    }
    Ok(())
}

#[cold]
#[inline(never)]
fn conflicting_null_decls(colname: &str, relname: &str) -> Box<PgError> {
    Box::new(
        PgError::new(
            ERROR,
            format!(
                "conflicting NULL/NOT NULL declarations for column \"{colname}\" of \
                 table \"{relname}\""
            ),
        )
        .with_sqlstate(ERRCODE_SYNTAX_ERROR),
    )
}

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
    regproc_seams::parse_type_string::set(parseTypeString);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> &'static mcx::MemoryContext {
        Box::leak(Box::new(mcx::MemoryContext::new("utilcmd-test")))
    }

    #[test]
    fn make_object_name_matches_c() {
        let mcx = ctx().mcx();
        assert_eq!(makeObjectName(mcx, "st", Some("id"), "seq").unwrap().as_str(), "st_id_seq");
        let long_a = "a".repeat(60);
        let long_b = "b".repeat(60);
        let n = makeObjectName(mcx, &long_a, Some(&long_b), "seq").unwrap();
        assert_eq!(n.len(), NAMEDATALEN as usize - 1);
        assert_eq!(n.as_str(), format!("{}_{}_seq", "a".repeat(29), "b".repeat(29)));
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
}
