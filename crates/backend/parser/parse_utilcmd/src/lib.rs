// CREATE TABLE plain-column lane + the parse_type.c slice it needs.
#![allow(non_snake_case)]

use mcx::Mcx;
use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult, ERRCODE_UNDEFINED_OBJECT, ERROR};
use types_nodes::rawnodes::{ColumnDef, CreateStmt, TypeName};
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

fn transformColumnDefinition<'mcx>(mcx: Mcx<'mcx>, column: &ColumnDef<'_>) -> PgResult<()> {
    if !column.constraints.is_nil() {
        unported("transformConstraintAttrs / column constraints");
    }
    if column.raw_default.is_some() || column.cooked_default.is_some() {
        unported("column defaults");
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

    for elt in stmt.tableElts.iter() {
        match elt.node_tag() {
            NodeTag::T_ColumnDef => {
                let cd = elt.as_variant::<ColumnDef>().expect("ColumnDef");
                transformColumnDefinition(mcx, cd)?;
            }
            NodeTag::T_TableLikeClause => unported("LIKE clauses"),
            NodeTag::T_Constraint => unported("table constraints"),
            other => panic!("unrecognized node type in tableElts: {other:?}"),
        }
    }

    NodeList::make1(mcx, stmt_node)
}
