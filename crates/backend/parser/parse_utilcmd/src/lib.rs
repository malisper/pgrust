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

// typenameTypeIdAndMod (parse_type.c): plain unparameterized types only.
pub fn typenameTypeIdAndMod<'mcx>(mcx: Mcx<'mcx>, tn: &TypeName<'_>) -> PgResult<(Oid, i32)> {
    if tn.pct_type || tn.setof {
        unported("LookupTypeName %TYPE / SETOF");
    }
    if !tn.typmods.is_nil() || tn.typemod != -1 {
        unported("typenameTypeMod (type modifiers)");
    }
    if !tn.arrayBounds.is_nil() {
        unported("array types (arrayBounds)");
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
    Ok((typoid, -1))
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
    typenameTypeIdAndMod(mcx, tn)?;
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
