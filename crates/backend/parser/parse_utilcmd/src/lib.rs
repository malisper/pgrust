// CREATE TABLE plain-column lane + the parse_type.c slice it needs.
#![allow(non_snake_case)]

use mcx::Mcx;
use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult, ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_OBJECT, ERROR};
use types_nodes::rawnodes::{ColumnDef, Constraint, ConstrType, CreateStmt, TypeName};
use types_nodes::{Node, NodeList, NodeTag};

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
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
    if tn.pct_type || tn.setof {
        unported("LookupTypeName %TYPE / SETOF");
    }
    if tn.typeOid != InvalidOid {
        unported("pre-resolved TypeName.typeOid lane");
    }

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
        Some(t) if t == b'b' as i8 || t == b'e' as i8 => {}
        Some(t) => unported(match t as u8 {
            b'c' => "composite column types",
            b'd' => "domain column types",
            b'p' => "pseudo-type columns",
            b'r' | b'm' => "range/multirange column types",
            _ => "unknown typtype",
        }),
        None => return Err(type_does_not_exist(typname)),
    }
    let typmod = typenameTypeMod(mcx, pstate, tn, typoid)?;
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

fn transformColumnDefinition<'mcx>(
    mcx: Mcx<'mcx>,
    column_node: Node<'mcx>,
    column: &ColumnDef<'mcx>,
    relname: &str,
    ckconstraints: &mut NodeList<'mcx>,
    nnconstraints: &mut NodeList<'mcx>,
) -> PgResult<()> {
    if column.raw_default.is_some() || column.cooked_default.is_some() {
        unported("pre-split column defaults");
    }
    let mut saw_default = false;
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
            ConstrType::CONSTR_CHECK => ckconstraints.lappend(mcx, cnode)?,
            ConstrType::CONSTR_NOTNULL => {
                if column.is_not_null {
                    unported("redundant NOT NULL merge (notnull_constraint conname)");
                }
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
            other => unported(match other {
                ConstrType::CONSTR_NULL => "NULL column constraints",
                ConstrType::CONSTR_IDENTITY | ConstrType::CONSTR_GENERATED => {
                    "identity/generated column constraints"
                }
                ConstrType::CONSTR_PRIMARY | ConstrType::CONSTR_UNIQUE => {
                    "PRIMARY KEY/UNIQUE column constraints"
                }
                ConstrType::CONSTR_FOREIGN => "REFERENCES column constraints",
                _ => "constraint attributes (transformConstraintAttrs)",
            }),
        }
    }
    if column.collClause.is_some() || column.collOid != InvalidOid {
        unported("COLLATE clauses");
    }
    if column.identity != 0 || column.generated != 0 {
        unported("identity/generated columns");
    }
    if column.is_from_type {
        unported("is_from_type columns (OF type / LIKE)");
    }
    let tn = column
        .typeName
        .expect("ColumnDef.typeName")
        .as_variant::<TypeName>()
        .expect("TypeName");
    // transformColumnType: validate the type reference.
    typenameTypeIdAndMod(mcx, None, tn)?;
    Ok(())
}

pub fn transformCreateStmt<'mcx>(
    mcx: Mcx<'mcx>,
    stmt_node: Node<'mcx>,
    _query_string: &str,
) -> PgResult<NodeList<'mcx>> {
    let stmt = stmt_node
        .as_variant::<CreateStmt>()
        .expect("transformCreateStmt on non-CreateStmt");

    if stmt.if_not_exists {
        unported("IF NOT EXISTS");
    }
    if !stmt.inhRelations.is_nil() {
        unported("inheritance (inhRelations)");
    }
    if stmt.partbound.is_some() || stmt.partspec.is_some() {
        unported("partitioning");
    }
    if stmt.ofTypename.is_some() {
        unported("typed tables (OF type)");
    }
    if !stmt.constraints.is_nil() || !stmt.nnconstraints.is_nil() {
        unported("table constraints");
    }

    let relname = stmt.relation.expect("CreateStmt.relation").relname.unwrap_or("");
    let mut ckconstraints = NodeList::nil();
    let mut nnconstraints = NodeList::nil();
    for elt in stmt.tableElts.iter() {
        match elt.node_tag() {
            NodeTag::T_ColumnDef => {
                let cd = elt.as_variant::<ColumnDef>().expect("ColumnDef");
                transformColumnDefinition(
                    mcx,
                    elt,
                    cd,
                    relname,
                    &mut ckconstraints,
                    &mut nnconstraints,
                )?;
            }
            NodeTag::T_TableLikeClause => unported("LIKE clauses"),
            NodeTag::T_Constraint => unported("table constraints"),
            other => panic!("unrecognized node type in tableElts: {other:?}"),
        }
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
                s.constraints = ckconstraints;
                s.nnconstraints = nnconstraints;
            })
            .expect("CreateStmt");
    }

    NodeList::make1(mcx, stmt_node)
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
            transformColumnDefinition(
                mcx,
                defnode,
                cd,
                relname,
                &mut ckconstraints,
                &mut nnconstraints,
            )?;
            // SAFETY: parse tree is analyze-owned; no derived refs live.
            unsafe {
                defnode
                    .with_mut::<ColumnDef, _>(|c| c.constraints = NodeList::nil())
                    .expect("ColumnDef");
            }
        }
        AlterTableType::AT_DropColumn => {}
        other => unported(&format!("transformAlterTableStmt {other:?} arm")),
    }
    if !ckconstraints.is_nil() || !nnconstraints.is_nil() {
        unported("ALTER TABLE ADD COLUMN with CHECK/NOT NULL (AT_AddConstraint lane)");
    }
    Ok(())
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

/// transformIndexStmt: a no-op for plain-column, no-predicate statements
/// (C only transforms index expressions and WHERE); those lanes are loud.
pub fn transformIndexStmt(
    _relid: Oid,
    stmt: &types_nodes::rawnodes::IndexStmt<'_>,
    _query_string: &str,
) -> PgResult<()> {
    if stmt.transformed {
        return Ok(());
    }
    if stmt.whereClause.is_some() {
        unported("transformIndexStmt: WHERE predicates");
    }
    for node in stmt.indexParams.iter() {
        let elem = node
            .as_variant::<types_nodes::rawnodes::IndexElem>()
            .expect("IndexElem in indexParams");
        if elem.expr.is_some() {
            unported("transformIndexStmt: expression index columns");
        }
    }
    Ok(())
}
