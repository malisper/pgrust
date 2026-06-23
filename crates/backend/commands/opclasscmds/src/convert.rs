//! Resolve the `'mcx` parse-node statements that `ProcessUtilitySlow`
//! (`utility.c`) dispatches — `nodes::ddlnodes::{CreateOpClassStmt,
//! CreateOpFamilyStmt, AlterOpFamilyStmt}` — into the flattened
//! `opclass` images the in-crate command logic consumes.
//!
//! The C code reads these `Node *` lists in place via `castNode`/`lfirst`; the
//! owned node tree carries the same payloads as `PgVec<NodePtr>` plus typed
//! sub-nodes, and the grammar always fills these fields with the node types the
//! `opclass` structs name (`String`, `TypeName`, `ObjectWithArgs`,
//! `CreateOpClassItem`), so a mismatch is an internal error.

use utils_error::ereport;
use types_error::{PgResult, ERROR};
use nodes::ddlnodes as pnode;
use nodes::nodes::Node;
use nodes::rawnodes::TypeName as PTypeName;
use opclass::{
    AlterOpFamilyStmt, CreateOpClassItem, CreateOpClassStmt, CreateOpFamilyStmt, ObjectWithArgs,
    StringNode, TypeName,
};

/// `strVal(node)` — the text of a `String` value node, mirroring C's
/// `castNode(String, node)->sval`. A non-`String` node is an internal error.
fn string_node(node: &Node<'_>, ctx: &str) -> PgResult<StringNode> {
    match node.as_string() {
        Some(s) => Ok(StringNode {
            sval: Some(s.sval.as_str().to_string()),
        }),
        None => Err(ereport(ERROR).errmsg_internal(ctx.to_string()).into_error()),
    }
}

/// Resolve a `List *` of `String` value nodes into `Vec<StringNode>`
/// (a qualified name).
fn string_list(nodes: &[nodes::nodes::NodePtr<'_>], ctx: &str) -> PgResult<Vec<StringNode>> {
    nodes.iter().map(|n| string_node(n, ctx)).collect()
}

/// Flatten a parse-node `TypeName` (`rawnodes::TypeName`) into the
/// `opclass::TypeName` the type-resolver seams consume. `names` is the
/// `List *` of `String` nodes; the remaining fields are scalar copies.
fn type_name(tn: &PTypeName<'_>) -> PgResult<TypeName> {
    let names: Vec<String> = tn
        .names
        .iter()
        .map(|n| match (**n).as_string() {
            Some(s) => Ok(s.sval.as_str().to_string()),
            None => Err(ereport(ERROR)
                .errmsg_internal("opclasscmds: TypeName.names element is not a String")
                .into_error()),
        })
        .collect::<PgResult<_>>()?;
    Ok(TypeName {
        names,
        typeOid: tn.typeOid,
        setof: tn.setof,
        pct_type: tn.pct_type,
        typemod: tn.typemod,
        arrayBounds: tn
            .arrayBounds
            .iter()
            .map(|n| (**n).as_integer().map(|i| i.ival).unwrap_or(-1))
            .collect(),
        location: tn.location,
    })
}

/// Resolve a `Node *` that must be a `TypeName`.
fn node_type_name(node: &Node<'_>, ctx: &str) -> PgResult<TypeName> {
    match node.as_typename() {
        Some(tn) => type_name(tn),
        None => Err(ereport(ERROR).errmsg_internal(ctx.to_string()).into_error()),
    }
}

/// Resolve a `List *` of `TypeName` nodes (`class_args` / `objargs`).
fn type_name_list(
    nodes: &[nodes::nodes::NodePtr<'_>],
    ctx: &str,
) -> PgResult<Vec<TypeName>> {
    nodes.iter().map(|n| node_type_name(n, ctx)).collect()
}

/// Flatten a parse-node `ObjectWithArgs` (`ddlnodes::ObjectWithArgs`).
fn object_with_args(owa: &pnode::ObjectWithArgs<'_>) -> PgResult<ObjectWithArgs> {
    Ok(ObjectWithArgs {
        objname: string_list(&owa.objname, "opclasscmds: ObjectWithArgs.objname element is not a String")?
            .into_iter()
            .map(|s| s.sval.unwrap_or_default())
            .collect(),
        objargs: type_name_list(&owa.objargs, "opclasscmds: ObjectWithArgs.objargs element is not a TypeName")?,
        args_unspecified: owa.args_unspecified,
    })
}

/// Resolve a `Node *` that must be an `ObjectWithArgs`.
fn node_object_with_args(node: &Node<'_>) -> PgResult<ObjectWithArgs> {
    match node.as_objectwithargs() {
        Some(owa) => object_with_args(owa),
        None => Err(ereport(ERROR)
            .errmsg_internal("opclasscmds: item->name is not an ObjectWithArgs")
            .into_error()),
    }
}

/// Flatten a parse-node `CreateOpClassItem`.
fn opclass_item(node: &Node<'_>) -> PgResult<CreateOpClassItem> {
    let item = match node.as_createopclassitem() {
        Some(it) => it,
        None => {
            return Err(ereport(ERROR)
                .errmsg_internal("opclasscmds: items list element is not a CreateOpClassItem")
                .into_error());
        }
    };
    let name = match item.name.as_deref() {
        Some(n) => Some(node_object_with_args(n)?),
        None => None,
    };
    let storedtype = match item.storedtype.as_deref() {
        Some(n) => Some(node_type_name(n, "opclasscmds: CreateOpClassItem.storedtype is not a TypeName")?),
        None => None,
    };
    Ok(CreateOpClassItem {
        itemtype: item.itemtype,
        name,
        number: item.number,
        order_family: string_list(
            &item.order_family,
            "opclasscmds: CreateOpClassItem.order_family element is not a String",
        )?,
        class_args: type_name_list(
            &item.class_args,
            "opclasscmds: CreateOpClassItem.class_args element is not a TypeName",
        )?,
        storedtype,
    })
}

/// Resolve `CreateOpClassStmt` from its `'mcx` parse-node form.
pub(crate) fn create_op_class_stmt(stmt: &pnode::CreateOpClassStmt<'_>) -> PgResult<CreateOpClassStmt> {
    let datatype = match stmt.datatype.as_deref() {
        Some(n) => Some(node_type_name(n, "DefineOpClass: datatype is not a TypeName")?),
        None => None,
    };
    let items = stmt
        .items
        .iter()
        .map(|n| opclass_item(n))
        .collect::<PgResult<_>>()?;
    Ok(CreateOpClassStmt {
        opclassname: string_list(&stmt.opclassname, "opclasscmds: opclassname element is not a String")?,
        opfamilyname: string_list(&stmt.opfamilyname, "opclasscmds: opfamilyname element is not a String")?,
        amname: stmt.amname.as_ref().map(|s| s.as_str().to_string()),
        datatype,
        items,
        isDefault: stmt.isDefault,
    })
}

/// Resolve `CreateOpFamilyStmt` from its `'mcx` parse-node form.
pub(crate) fn create_op_family_stmt(
    stmt: &pnode::CreateOpFamilyStmt<'_>,
) -> PgResult<CreateOpFamilyStmt> {
    Ok(CreateOpFamilyStmt {
        opfamilyname: string_list(&stmt.opfamilyname, "opclasscmds: opfamilyname element is not a String")?,
        amname: stmt.amname.as_ref().map(|s| s.as_str().to_string()),
    })
}

/// Resolve `AlterOpFamilyStmt` from its `'mcx` parse-node form.
pub(crate) fn alter_op_family_stmt(
    stmt: &pnode::AlterOpFamilyStmt<'_>,
) -> PgResult<AlterOpFamilyStmt> {
    let items = stmt
        .items
        .iter()
        .map(|n| opclass_item(n))
        .collect::<PgResult<_>>()?;
    Ok(AlterOpFamilyStmt {
        opfamilyname: string_list(&stmt.opfamilyname, "opclasscmds: opfamilyname element is not a String")?,
        amname: stmt.amname.as_ref().map(|s| s.as_str().to_string()),
        isDrop: stmt.isDrop,
        items,
    })
}
