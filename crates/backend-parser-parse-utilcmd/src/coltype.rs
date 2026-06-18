//! `transformColumnType` (`parse_utilcmd.c`) — verify a column's declared type
//! (and any COLLATE clause) against the catalog.
//!
//! This is the type/syscache leaf of column processing. It is fully reachable:
//! `typenameType` / `LookupCollation` / `typeTypeCollation` live in
//! `backend-parser-parse-type` and `format_type_be` in
//! `backend-utils-adt-format-type`, all cycle-free direct dependencies.

use mcx::Mcx;

use backend_parser_parse_type::{typenameType, LookupCollation, typeTypeCollation, Type};
use backend_utils_adt_format_type::format_type_be;
use backend_utils_error::ereport;
use types_core::{Oid, OidIsValid};
use types_error::{PgResult, ERRCODE_DATATYPE_MISMATCH, ERROR};
use types_nodes::nodes::Node;
use types_nodes::parsestmt::ParseState;

use crate::errpos::parser_errposition;

/// `transformColumnType(cxt, column)` (parse_utilcmd.c:4045): verify that the
/// column's declared type is valid, including any COLLATE spec. Returns the
/// resolved type OID (`((Form_pg_type) GETSTRUCT(ctype))->oid`); the IDENTITY
/// path consumes it.
pub fn transformColumnType<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'mcx>,
    column: &Node<'mcx>,
) -> PgResult<Oid> {
    let column = match column {
        Node::ColumnDef(c) => c,
        other => {
            return Err(ereport(ERROR)
                .errmsg_internal(alloc::format!(
                    "transformColumnType: not a ColumnDef node: {}",
                    other.node_tag()
                ))
                .into_error());
        }
    };

    let type_name = column.typeName.as_deref().ok_or_else(|| {
        ereport(ERROR)
            .errmsg_internal("transformColumnType: column has no typeName")
            .into_error()
    })?;

    // All we really need to do here is verify that the type is valid, including
    // any collation spec that might be present.
    let type_name_pn = raw_typename_to_parse(type_name)?;
    let (ctype, _typmod): (Type, i32) = typenameType(mcx, Some(pstate), &type_name_pn)?;

    if let Some(coll_clause) = column.collClause.as_deref() {
        // `LookupCollation` consumes the parser's own node vocabulary
        // (`types_parsenodes::Node`), distinct from the raw-grammar
        // `types_nodes::Node` this crate carries; bridge the String-only
        // collname list (the only node kind a collation name list contains).
        let mut collname_pn: alloc::vec::Vec<types_parsenodes::Node> =
            alloc::vec::Vec::with_capacity(coll_clause.collname.len());
        for n in coll_clause.collname.iter() {
            match n.as_ref() {
                Node::String(s) => {
                    collname_pn.push(types_parsenodes::Node::String(
                        types_parsenodes::StringNode {
                            sval: Some(alloc::string::String::from(s.sval.as_str())),
                        },
                    ));
                }
                other => {
                    return Err(ereport(ERROR)
                        .errmsg_internal(alloc::format!(
                            "transformColumnType: collname element is not a String value node (tag {})",
                            other.node_tag().0
                        ))
                        .into_error());
                }
            }
        }
        LookupCollation(mcx, Some(pstate), &collname_pn, coll_clause.location)?;
        // Complain if COLLATE is applied to an uncollatable type.
        if !OidIsValid(typeTypeCollation(ctype)) {
            let type_oid = ctype.oid;
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DATATYPE_MISMATCH)
                .errmsg(alloc::format!(
                    "collations are not supported by type {}",
                    format_type_be(mcx, type_oid)?.as_str()
                ))
                .errposition(parser_errposition(pstate, coll_clause.location))
                .into_error());
        }
    }

    // C: ((Form_pg_type) GETSTRUCT(ctype))->oid. (ReleaseSysCache is implicit —
    // `Type` is a value copy of FormData_pg_type.)
    Ok(ctype.oid)
}

/// Bridge the owned-tree raw-grammar `types_nodes::rawnodes::TypeName<'mcx>`
/// (carried in a `ColumnDef`) into the resolver-facing
/// `types_parsenodes::TypeName` that `typenameType`/`LookupTypeName` consume.
/// Mirrors parse_type's `raw_typename_to_parse` / parse_expr's
/// `typename_type_id_and_mod` converter: qualified `names` are `String` nodes;
/// each `typmod` reduces to the value Node it carries (an `A_Const`'s literal,
/// or a single-field `ColumnRef` identifier, else `A_Star` so the owner raises
/// the C "must be simple constants or identifiers" error); `arrayBounds` only
/// need their `Integer` bound (else `-1`) for `LookupTypeName` to resolve the
/// array type.
fn raw_typename_to_parse(
    tn: &types_nodes::rawnodes::TypeName<'_>,
) -> PgResult<types_parsenodes::TypeName> {
    use alloc::string::ToString;
    use alloc::vec::Vec;

    let mut names: Vec<types_parsenodes::Node> = Vec::with_capacity(tn.names.len());
    for n in tn.names.iter() {
        match n.as_ref() {
            Node::String(s) => names.push(types_parsenodes::Node::String(
                types_parsenodes::StringNode {
                    sval: Some(s.sval.as_str().to_string()),
                },
            )),
            other => {
                return Err(ereport(ERROR)
                    .errmsg_internal(alloc::format!(
                        "transformColumnType: TypeName.names element is not a String node (tag {})",
                        other.node_tag().0
                    ))
                    .into_error());
            }
        }
    }

    let mut typmods: Vec<types_parsenodes::Node> = Vec::with_capacity(tn.typmods.len());
    for tm in tn.typmods.iter() {
        let bridged: types_parsenodes::Node = match tm.as_ref() {
            Node::A_Const(ac) => match ac.val.as_deref() {
                Some(Node::Integer(i)) => {
                    types_parsenodes::Node::Integer(types_parsenodes::Integer { ival: i.ival })
                }
                Some(Node::Float(f)) => types_parsenodes::Node::Float(types_parsenodes::Float {
                    fval: Some(f.fval.as_str().to_string()),
                }),
                Some(Node::String(s)) => types_parsenodes::Node::String(
                    types_parsenodes::StringNode {
                        sval: Some(s.sval.as_str().to_string()),
                    },
                ),
                Some(Node::Boolean(b)) => {
                    types_parsenodes::Node::Boolean(types_parsenodes::Boolean {
                        boolval: b.boolval,
                    })
                }
                Some(Node::BitString(b)) => types_parsenodes::Node::BitString(
                    types_parsenodes::BitString {
                        bsval: Some(b.bsval.as_str().to_string()),
                    },
                ),
                _ => types_parsenodes::Node::A_Star,
            },
            Node::ColumnRef(cr) => {
                if cr.fields.len() == 1 {
                    if let Node::String(s) = cr.fields[0].as_ref() {
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
        match n.as_ref() {
            Node::Integer(i) => array_bounds.push(types_parsenodes::Node::Integer(
                types_parsenodes::Integer { ival: i.ival },
            )),
            _ => array_bounds.push(types_parsenodes::Node::Integer(types_parsenodes::Integer {
                ival: -1,
            })),
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
