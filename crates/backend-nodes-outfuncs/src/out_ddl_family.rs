//! `_out<Type>` writers for the raw-grammar DDL statement family
//! (`crate::ddlnodes`). Generated field-for-field from `outfuncs.funcs.c`.
//! `try_out` returns `true` iff it claimed and wrote `node`.

use alloc::string::String;

use types_nodes::nodes::{ntag, Node};
use types_nodes::ddlnodes as dn;

use crate::{
    framed, out_node_inner, write_bool_field, write_char_field, write_enum_field,
    write_int_field, write_location_field, write_node_field, write_oid_field,
    write_string_field, write_uint_field, write_int64_field,
};

/// Write a `List *` of `Node *` (`WRITE_NODE_FIELD` of a `List`): `(child ...)`,
/// `<>` when NIL/empty (C `outNode(NULL)`).
fn write_node_list(buf: &mut String, name: &str, list: &[types_nodes::nodes::NodePtr<'_>], wl: bool) {
    use core::fmt::Write as _;
    let _ = write!(buf, " :{} ", name);
    if list.is_empty() {
        buf.push_str("<>");
        return;
    }
    buf.push('(');
    let mut first = true;
    for e in list {
        if !first { buf.push(' '); }
        first = false;
        out_node_inner(buf, e, wl);
    }
    buf.push(')');
}

/// `_outIntoClause` (outfuncs.funcs.c).
fn out_into_clause(buf: &mut String, n: &dn::IntoClause<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("INTOCLAUSE");
    write_node_field(buf, "rel", n.rel.as_deref(), wl);
    write_node_list(buf, "colNames", n.colNames.as_slice(), wl);
    write_string_field(buf, "accessMethod", n.accessMethod.as_ref().map(|s| s.as_str()));
    write_node_list(buf, "options", n.options.as_slice(), wl);
    write_enum_field(buf, "onCommit", n.onCommit as i32);
    write_string_field(buf, "tableSpaceName", n.tableSpaceName.as_ref().map(|s| s.as_str()));
    write_node_field(buf, "viewQuery", n.viewQuery.as_deref(), wl);
    write_bool_field(buf, "skipData", n.skipData);
}

/// `_outRoleSpec` (outfuncs.funcs.c).
fn out_role_spec(buf: &mut String, n: &dn::RoleSpec<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("ROLESPEC");
    write_enum_field(buf, "roletype", n.roletype as i32);
    write_string_field(buf, "rolename", n.rolename.as_ref().map(|s| s.as_str()));
    write_location_field(buf, "location", n.location, wl);
}

/// `_outTableLikeClause` (outfuncs.funcs.c).
fn out_table_like_clause(buf: &mut String, n: &dn::TableLikeClause<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("TABLELIKECLAUSE");
    write_node_field(buf, "relation", n.relation.as_deref(), wl);
    write_uint_field(buf, "options", n.options as u32);
    write_oid_field(buf, "relationOid", n.relationOid);
}

/// `_outIndexElem` (outfuncs.funcs.c).
fn out_index_elem(buf: &mut String, n: &dn::IndexElem<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("INDEXELEM");
    write_string_field(buf, "name", n.name.as_ref().map(|s| s.as_str()));
    write_node_field(buf, "expr", n.expr.as_deref(), wl);
    write_string_field(buf, "indexcolname", n.indexcolname.as_ref().map(|s| s.as_str()));
    write_node_list(buf, "collation", n.collation.as_slice(), wl);
    write_node_list(buf, "opclass", n.opclass.as_slice(), wl);
    write_node_list(buf, "opclassopts", n.opclassopts.as_slice(), wl);
    write_enum_field(buf, "ordering", n.ordering as i32);
    write_enum_field(buf, "nulls_ordering", n.nulls_ordering as i32);
}

/// `_outDefElem` (outfuncs.funcs.c).
fn out_def_elem(buf: &mut String, n: &dn::DefElem<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("DEFELEM");
    write_string_field(buf, "defnamespace", n.defnamespace.as_ref().map(|s| s.as_str()));
    write_string_field(buf, "defname", n.defname.as_ref().map(|s| s.as_str()));
    write_node_field(buf, "arg", n.arg.as_deref(), wl);
    write_enum_field(buf, "defaction", n.defaction as i32);
    write_location_field(buf, "location", n.location, wl);
}

/// `_outPartitionElem` (outfuncs.funcs.c).
fn out_partition_elem(buf: &mut String, n: &dn::PartitionElem<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("PARTITIONELEM");
    write_string_field(buf, "name", n.name.as_ref().map(|s| s.as_str()));
    write_node_field(buf, "expr", n.expr.as_deref(), wl);
    write_node_list(buf, "collation", n.collation.as_slice(), wl);
    write_node_list(buf, "opclass", n.opclass.as_slice(), wl);
    write_location_field(buf, "location", n.location, wl);
}

/// `_outPartitionSpec` (outfuncs.funcs.c).
fn out_partition_spec(buf: &mut String, n: &dn::PartitionSpec<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("PARTITIONSPEC");
    write_enum_field(buf, "strategy", n.strategy as i32);
    write_node_list(buf, "partParams", n.partParams.as_slice(), wl);
    write_location_field(buf, "location", n.location, wl);
}

/// `_outPartitionBoundSpec` (outfuncs.funcs.c).
fn out_partition_bound_spec(buf: &mut String, n: &dn::PartitionBoundSpec<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("PARTITIONBOUNDSPEC");
    write_char_field(buf, "strategy", n.strategy as u8);
    write_bool_field(buf, "is_default", n.is_default);
    write_int_field(buf, "modulus", n.modulus as i32);
    write_int_field(buf, "remainder", n.remainder as i32);
    write_node_list(buf, "listdatums", n.listdatums.as_slice(), wl);
    write_node_list(buf, "lowerdatums", n.lowerdatums.as_slice(), wl);
    write_node_list(buf, "upperdatums", n.upperdatums.as_slice(), wl);
    write_location_field(buf, "location", n.location, wl);
}

/// `_outPartitionRangeDatum` (outfuncs.funcs.c).
fn out_partition_range_datum(buf: &mut String, n: &dn::PartitionRangeDatum<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("PARTITIONRANGEDATUM");
    write_enum_field(buf, "kind", n.kind as i32);
    write_node_field(buf, "value", n.value.as_deref(), wl);
    write_location_field(buf, "location", n.location, wl);
}

/// `_outPartitionCmd` (outfuncs.funcs.c).
fn out_partition_cmd(buf: &mut String, n: &dn::PartitionCmd<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("PARTITIONCMD");
    write_node_field(buf, "name", n.name.as_deref(), wl);
    write_node_field(buf, "bound", n.bound.as_deref(), wl);
    write_bool_field(buf, "concurrent", n.concurrent);
}

/// `_outReturnStmt` (outfuncs.funcs.c).
fn out_return_stmt(buf: &mut String, n: &dn::ReturnStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("RETURNSTMT");
    write_node_field(buf, "returnval", n.returnval.as_deref(), wl);
}

/// `_outPLAssignStmt` (outfuncs.funcs.c).
fn out_p_l_assign_stmt(buf: &mut String, n: &dn::PLAssignStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("PLASSIGNSTMT");
    write_string_field(buf, "name", n.name.as_ref().map(|s| s.as_str()));
    write_node_list(buf, "indirection", n.indirection.as_slice(), wl);
    write_int_field(buf, "nnames", n.nnames as i32);
    write_node_field(buf, "val", n.val.as_deref(), wl);
    write_location_field(buf, "location", n.location, wl);
}

/// `_outCreateSchemaStmt` (outfuncs.funcs.c).
fn out_create_schema_stmt(buf: &mut String, n: &dn::CreateSchemaStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("CREATESCHEMASTMT");
    write_string_field(buf, "schemaname", n.schemaname.as_ref().map(|s| s.as_str()));
    write_node_field(buf, "authrole", n.authrole.as_deref(), wl);
    write_node_list(buf, "schemaElts", n.schemaElts.as_slice(), wl);
    write_bool_field(buf, "if_not_exists", n.if_not_exists);
}

/// `_outAlterTableStmt` (outfuncs.funcs.c).
fn out_alter_table_stmt(buf: &mut String, n: &dn::AlterTableStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("ALTERTABLESTMT");
    write_node_field(buf, "relation", n.relation.as_deref(), wl);
    write_node_list(buf, "cmds", n.cmds.as_slice(), wl);
    write_enum_field(buf, "objtype", n.objtype as i32);
    write_bool_field(buf, "missing_ok", n.missing_ok);
}

/// `_outAlterTableCmd` (outfuncs.funcs.c).
fn out_alter_table_cmd(buf: &mut String, n: &dn::AlterTableCmd<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("ALTERTABLECMD");
    write_enum_field(buf, "subtype", n.subtype as i32);
    write_string_field(buf, "name", n.name.as_ref().map(|s| s.as_str()));
    write_int_field(buf, "num", n.num as i32);
    write_node_field(buf, "newowner", n.newowner.as_deref(), wl);
    write_node_field(buf, "def", n.def.as_deref(), wl);
    write_enum_field(buf, "behavior", n.behavior as i32);
    write_bool_field(buf, "missing_ok", n.missing_ok);
    write_bool_field(buf, "recurse", n.recurse);
}

/// `_outATAlterConstraint` (outfuncs.funcs.c).
fn out_a_t_alter_constraint(buf: &mut String, n: &dn::ATAlterConstraint<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("ATALTERCONSTRAINT");
    write_string_field(buf, "conname", n.conname.as_ref().map(|s| s.as_str()));
    write_bool_field(buf, "alterEnforceability", n.alterEnforceability);
    write_bool_field(buf, "is_enforced", n.is_enforced);
    write_bool_field(buf, "alterDeferrability", n.alterDeferrability);
    write_bool_field(buf, "deferrable", n.deferrable);
    write_bool_field(buf, "initdeferred", n.initdeferred);
    write_bool_field(buf, "alterInheritability", n.alterInheritability);
    write_bool_field(buf, "noinherit", n.noinherit);
}

/// `_outReplicaIdentityStmt` (outfuncs.funcs.c).
fn out_replica_identity_stmt(buf: &mut String, n: &dn::ReplicaIdentityStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("REPLICAIDENTITYSTMT");
    write_char_field(buf, "identity_type", n.identity_type as u8);
    write_string_field(buf, "name", n.name.as_ref().map(|s| s.as_str()));
}

/// `_outAlterCollationStmt` (outfuncs.funcs.c).
fn out_alter_collation_stmt(buf: &mut String, n: &dn::AlterCollationStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("ALTERCOLLATIONSTMT");
    write_node_list(buf, "collname", n.collname.as_slice(), wl);
}

/// `_outAlterDomainStmt` (outfuncs.funcs.c).
fn out_alter_domain_stmt(buf: &mut String, n: &dn::AlterDomainStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("ALTERDOMAINSTMT");
    write_char_field(buf, "subtype", n.subtype as u8);
    write_node_list(buf, "typeName", n.typeName.as_slice(), wl);
    write_string_field(buf, "name", n.name.as_ref().map(|s| s.as_str()));
    write_node_field(buf, "def", n.def.as_deref(), wl);
    write_enum_field(buf, "behavior", n.behavior as i32);
    write_bool_field(buf, "missing_ok", n.missing_ok);
}

/// `_outGrantStmt` (outfuncs.funcs.c).
fn out_grant_stmt(buf: &mut String, n: &dn::GrantStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("GRANTSTMT");
    write_bool_field(buf, "is_grant", n.is_grant);
    write_enum_field(buf, "targtype", n.targtype as i32);
    write_enum_field(buf, "objtype", n.objtype as i32);
    write_node_list(buf, "objects", n.objects.as_slice(), wl);
    write_node_list(buf, "privileges", n.privileges.as_slice(), wl);
    write_node_list(buf, "grantees", n.grantees.as_slice(), wl);
    write_bool_field(buf, "grant_option", n.grant_option);
    write_node_field(buf, "grantor", n.grantor.as_deref(), wl);
    write_enum_field(buf, "behavior", n.behavior as i32);
}

/// `_outObjectWithArgs` (outfuncs.funcs.c).
fn out_object_with_args(buf: &mut String, n: &dn::ObjectWithArgs<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("OBJECTWITHARGS");
    write_node_list(buf, "objname", n.objname.as_slice(), wl);
    write_node_list(buf, "objargs", n.objargs.as_slice(), wl);
    write_node_list(buf, "objfuncargs", n.objfuncargs.as_slice(), wl);
    write_bool_field(buf, "args_unspecified", n.args_unspecified);
}

/// `_outAccessPriv` (outfuncs.funcs.c).
fn out_access_priv(buf: &mut String, n: &dn::AccessPriv<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("ACCESSPRIV");
    write_string_field(buf, "priv_name", n.priv_name.as_ref().map(|s| s.as_str()));
    write_node_list(buf, "cols", n.cols.as_slice(), wl);
}

/// `_outGrantRoleStmt` (outfuncs.funcs.c).
fn out_grant_role_stmt(buf: &mut String, n: &dn::GrantRoleStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("GRANTROLESTMT");
    write_node_list(buf, "granted_roles", n.granted_roles.as_slice(), wl);
    write_node_list(buf, "grantee_roles", n.grantee_roles.as_slice(), wl);
    write_bool_field(buf, "is_grant", n.is_grant);
    write_node_list(buf, "opt", n.opt.as_slice(), wl);
    write_node_field(buf, "grantor", n.grantor.as_deref(), wl);
    write_enum_field(buf, "behavior", n.behavior as i32);
}

/// `_outAlterDefaultPrivilegesStmt` (outfuncs.funcs.c).
fn out_alter_default_privileges_stmt(buf: &mut String, n: &dn::AlterDefaultPrivilegesStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("ALTERDEFAULTPRIVILEGESSTMT");
    write_node_list(buf, "options", n.options.as_slice(), wl);
    write_node_field(buf, "action", n.action.as_deref(), wl);
}

/// `_outCopyStmt` (outfuncs.funcs.c).
fn out_copy_stmt(buf: &mut String, n: &dn::CopyStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("COPYSTMT");
    write_node_field(buf, "relation", n.relation.as_deref(), wl);
    write_node_field(buf, "query", n.query.as_deref(), wl);
    write_node_list(buf, "attlist", n.attlist.as_slice(), wl);
    write_bool_field(buf, "is_from", n.is_from);
    write_bool_field(buf, "is_program", n.is_program);
    write_string_field(buf, "filename", n.filename.as_ref().map(|s| s.as_str()));
    write_node_list(buf, "options", n.options.as_slice(), wl);
    write_node_field(buf, "whereClause", n.where_clause.as_deref(), wl);
}

/// `_outVariableSetStmt` (outfuncs.funcs.c).
fn out_variable_set_stmt(buf: &mut String, n: &dn::VariableSetStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("VARIABLESETSTMT");
    write_enum_field(buf, "kind", n.kind as i32);
    write_string_field(buf, "name", n.name.as_ref().map(|s| s.as_str()));
    write_node_list(buf, "args", n.args.as_slice(), wl);
    write_bool_field(buf, "jumble_args", n.jumble_args);
    write_bool_field(buf, "is_local", n.is_local);
    write_location_field(buf, "location", n.location, wl);
}

/// `_outVariableShowStmt` (outfuncs.funcs.c).
fn out_variable_show_stmt(buf: &mut String, n: &dn::VariableShowStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("VARIABLESHOWSTMT");
    write_string_field(buf, "name", n.name.as_ref().map(|s| s.as_str()));
}

/// `_outCreateStmt` (outfuncs.funcs.c).
fn out_create_stmt(buf: &mut String, n: &dn::CreateStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("CREATESTMT");
    write_node_field(buf, "relation", n.relation.as_deref(), wl);
    write_node_list(buf, "tableElts", n.tableElts.as_slice(), wl);
    write_node_list(buf, "inhRelations", n.inhRelations.as_slice(), wl);
    write_node_field(buf, "partbound", n.partbound.as_deref(), wl);
    write_node_field(buf, "partspec", n.partspec.as_deref(), wl);
    write_node_field(buf, "ofTypename", n.ofTypename.as_deref(), wl);
    write_node_list(buf, "constraints", n.constraints.as_slice(), wl);
    write_node_list(buf, "nnconstraints", n.nnconstraints.as_slice(), wl);
    write_node_list(buf, "options", n.options.as_slice(), wl);
    write_enum_field(buf, "oncommit", n.oncommit as i32);
    write_string_field(buf, "tablespacename", n.tablespacename.as_ref().map(|s| s.as_str()));
    write_string_field(buf, "accessMethod", n.accessMethod.as_ref().map(|s| s.as_str()));
    write_bool_field(buf, "if_not_exists", n.if_not_exists);
}

/// `_outConstraint` (outfuncs.funcs.c).
fn out_constraint(buf: &mut String, n: &dn::Constraint<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("CONSTRAINT");
    write_enum_field(buf, "contype", n.contype as i32);
    write_string_field(buf, "conname", n.conname.as_ref().map(|s| s.as_str()));
    write_bool_field(buf, "deferrable", n.deferrable);
    write_bool_field(buf, "initdeferred", n.initdeferred);
    write_bool_field(buf, "is_enforced", n.is_enforced);
    write_bool_field(buf, "skip_validation", n.skip_validation);
    write_bool_field(buf, "initially_valid", n.initially_valid);
    write_bool_field(buf, "is_no_inherit", n.is_no_inherit);
    write_node_field(buf, "raw_expr", n.raw_expr.as_deref(), wl);
    write_string_field(buf, "cooked_expr", n.cooked_expr.as_ref().map(|s| s.as_str()));
    write_char_field(buf, "generated_when", n.generated_when as u8);
    write_char_field(buf, "generated_kind", n.generated_kind as u8);
    write_bool_field(buf, "nulls_not_distinct", n.nulls_not_distinct);
    write_node_list(buf, "keys", n.keys.as_slice(), wl);
    write_bool_field(buf, "without_overlaps", n.without_overlaps);
    write_node_list(buf, "including", n.including.as_slice(), wl);
    write_node_list(buf, "exclusions", n.exclusions.as_slice(), wl);
    write_node_list(buf, "options", n.options.as_slice(), wl);
    write_string_field(buf, "indexname", n.indexname.as_ref().map(|s| s.as_str()));
    write_string_field(buf, "indexspace", n.indexspace.as_ref().map(|s| s.as_str()));
    write_bool_field(buf, "reset_default_tblspc", n.reset_default_tblspc);
    write_string_field(buf, "access_method", n.access_method.as_ref().map(|s| s.as_str()));
    write_node_field(buf, "where_clause", n.where_clause.as_deref(), wl);
    write_node_field(buf, "pktable", n.pktable.as_deref(), wl);
    write_node_list(buf, "fk_attrs", n.fk_attrs.as_slice(), wl);
    write_node_list(buf, "pk_attrs", n.pk_attrs.as_slice(), wl);
    write_bool_field(buf, "fk_with_period", n.fk_with_period);
    write_bool_field(buf, "pk_with_period", n.pk_with_period);
    write_char_field(buf, "fk_matchtype", n.fk_matchtype as u8);
    write_char_field(buf, "fk_upd_action", n.fk_upd_action as u8);
    write_char_field(buf, "fk_del_action", n.fk_del_action as u8);
    write_node_list(buf, "fk_del_set_cols", n.fk_del_set_cols.as_slice(), wl);
    write_node_list(buf, "old_conpfeqop", n.old_conpfeqop.as_slice(), wl);
    write_oid_field(buf, "old_pktable_oid", n.old_pktable_oid);
    write_location_field(buf, "location", n.location, wl);
}

/// `_outCreateTableSpaceStmt` (outfuncs.funcs.c).
fn out_create_table_space_stmt(buf: &mut String, n: &dn::CreateTableSpaceStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("CREATETABLESPACESTMT");
    write_string_field(buf, "tablespacename", n.tablespacename.as_ref().map(|s| s.as_str()));
    write_node_field(buf, "owner", n.owner.as_deref(), wl);
    write_string_field(buf, "location", n.location.as_ref().map(|s| s.as_str()));
    write_node_list(buf, "options", n.options.as_slice(), wl);
}

/// `_outDropTableSpaceStmt` (outfuncs.funcs.c).
fn out_drop_table_space_stmt(buf: &mut String, n: &dn::DropTableSpaceStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("DROPTABLESPACESTMT");
    write_string_field(buf, "tablespacename", n.tablespacename.as_ref().map(|s| s.as_str()));
    write_bool_field(buf, "missing_ok", n.missing_ok);
}

/// `_outAlterTableSpaceOptionsStmt` (outfuncs.funcs.c).
fn out_alter_table_space_options_stmt(buf: &mut String, n: &dn::AlterTableSpaceOptionsStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("ALTERTABLESPACEOPTIONSSTMT");
    write_string_field(buf, "tablespacename", n.tablespacename.as_ref().map(|s| s.as_str()));
    write_node_list(buf, "options", n.options.as_slice(), wl);
    write_bool_field(buf, "isReset", n.isReset);
}

/// `_outAlterTableMoveAllStmt` (outfuncs.funcs.c).
fn out_alter_table_move_all_stmt(buf: &mut String, n: &dn::AlterTableMoveAllStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("ALTERTABLEMOVEALLSTMT");
    write_string_field(buf, "orig_tablespacename", n.orig_tablespacename.as_ref().map(|s| s.as_str()));
    write_enum_field(buf, "objtype", n.objtype as i32);
    write_node_list(buf, "roles", n.roles.as_slice(), wl);
    write_string_field(buf, "new_tablespacename", n.new_tablespacename.as_ref().map(|s| s.as_str()));
    write_bool_field(buf, "nowait", n.nowait);
}

/// `_outCreateExtensionStmt` (outfuncs.funcs.c).
fn out_create_extension_stmt(buf: &mut String, n: &dn::CreateExtensionStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("CREATEEXTENSIONSTMT");
    write_string_field(buf, "extname", n.extname.as_ref().map(|s| s.as_str()));
    write_bool_field(buf, "if_not_exists", n.if_not_exists);
    write_node_list(buf, "options", n.options.as_slice(), wl);
}

/// `_outAlterExtensionStmt` (outfuncs.funcs.c).
fn out_alter_extension_stmt(buf: &mut String, n: &dn::AlterExtensionStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("ALTEREXTENSIONSTMT");
    write_string_field(buf, "extname", n.extname.as_ref().map(|s| s.as_str()));
    write_node_list(buf, "options", n.options.as_slice(), wl);
}

/// `_outAlterExtensionContentsStmt` (outfuncs.funcs.c).
fn out_alter_extension_contents_stmt(buf: &mut String, n: &dn::AlterExtensionContentsStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("ALTEREXTENSIONCONTENTSSTMT");
    write_string_field(buf, "extname", n.extname.as_ref().map(|s| s.as_str()));
    write_int_field(buf, "action", n.action as i32);
    write_enum_field(buf, "objtype", n.objtype as i32);
    write_node_field(buf, "object", n.object.as_deref(), wl);
}

/// `_outCreateFdwStmt` (outfuncs.funcs.c).
fn out_create_fdw_stmt(buf: &mut String, n: &dn::CreateFdwStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("CREATEFDWSTMT");
    write_string_field(buf, "fdwname", n.fdwname.as_ref().map(|s| s.as_str()));
    write_node_list(buf, "func_options", n.func_options.as_slice(), wl);
    write_node_list(buf, "options", n.options.as_slice(), wl);
}

/// `_outAlterFdwStmt` (outfuncs.funcs.c).
fn out_alter_fdw_stmt(buf: &mut String, n: &dn::AlterFdwStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("ALTERFDWSTMT");
    write_string_field(buf, "fdwname", n.fdwname.as_ref().map(|s| s.as_str()));
    write_node_list(buf, "func_options", n.func_options.as_slice(), wl);
    write_node_list(buf, "options", n.options.as_slice(), wl);
}

/// `_outCreateForeignServerStmt` (outfuncs.funcs.c).
fn out_create_foreign_server_stmt(buf: &mut String, n: &dn::CreateForeignServerStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("CREATEFOREIGNSERVERSTMT");
    write_string_field(buf, "servername", n.servername.as_ref().map(|s| s.as_str()));
    write_string_field(buf, "servertype", n.servertype.as_ref().map(|s| s.as_str()));
    write_string_field(buf, "version", n.version.as_ref().map(|s| s.as_str()));
    write_string_field(buf, "fdwname", n.fdwname.as_ref().map(|s| s.as_str()));
    write_bool_field(buf, "if_not_exists", n.if_not_exists);
    write_node_list(buf, "options", n.options.as_slice(), wl);
}

/// `_outAlterForeignServerStmt` (outfuncs.funcs.c).
fn out_alter_foreign_server_stmt(buf: &mut String, n: &dn::AlterForeignServerStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("ALTERFOREIGNSERVERSTMT");
    write_string_field(buf, "servername", n.servername.as_ref().map(|s| s.as_str()));
    write_string_field(buf, "version", n.version.as_ref().map(|s| s.as_str()));
    write_node_list(buf, "options", n.options.as_slice(), wl);
    write_bool_field(buf, "has_version", n.has_version);
}

/// `_outCreateForeignTableStmt` (outfuncs.funcs.c).
fn out_create_foreign_table_stmt(buf: &mut String, n: &dn::CreateForeignTableStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("CREATEFOREIGNTABLESTMT");
    write_node_field(buf, "base.relation", n.base.relation.as_deref(), wl);
    write_node_list(buf, "base.tableElts", n.base.tableElts.as_slice(), wl);
    write_node_list(buf, "base.inhRelations", n.base.inhRelations.as_slice(), wl);
    write_node_field(buf, "base.partbound", n.base.partbound.as_deref(), wl);
    write_node_field(buf, "base.partspec", n.base.partspec.as_deref(), wl);
    write_node_field(buf, "base.ofTypename", n.base.ofTypename.as_deref(), wl);
    write_node_list(buf, "base.constraints", n.base.constraints.as_slice(), wl);
    write_node_list(buf, "base.nnconstraints", n.base.nnconstraints.as_slice(), wl);
    write_node_list(buf, "base.options", n.base.options.as_slice(), wl);
    write_enum_field(buf, "base.oncommit", n.base.oncommit as i32);
    write_string_field(buf, "base.tablespacename", n.base.tablespacename.as_ref().map(|s| s.as_str()));
    write_string_field(buf, "base.accessMethod", n.base.accessMethod.as_ref().map(|s| s.as_str()));
    write_bool_field(buf, "base.if_not_exists", n.base.if_not_exists);
    write_string_field(buf, "servername", n.servername.as_ref().map(|s| s.as_str()));
    write_node_list(buf, "options", n.options.as_slice(), wl);
}

/// `_outCreateUserMappingStmt` (outfuncs.funcs.c).
fn out_create_user_mapping_stmt(buf: &mut String, n: &dn::CreateUserMappingStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("CREATEUSERMAPPINGSTMT");
    write_node_field(buf, "user", n.user.as_deref(), wl);
    write_string_field(buf, "servername", n.servername.as_ref().map(|s| s.as_str()));
    write_bool_field(buf, "if_not_exists", n.if_not_exists);
    write_node_list(buf, "options", n.options.as_slice(), wl);
}

/// `_outAlterUserMappingStmt` (outfuncs.funcs.c).
fn out_alter_user_mapping_stmt(buf: &mut String, n: &dn::AlterUserMappingStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("ALTERUSERMAPPINGSTMT");
    write_node_field(buf, "user", n.user.as_deref(), wl);
    write_string_field(buf, "servername", n.servername.as_ref().map(|s| s.as_str()));
    write_node_list(buf, "options", n.options.as_slice(), wl);
}

/// `_outDropUserMappingStmt` (outfuncs.funcs.c).
fn out_drop_user_mapping_stmt(buf: &mut String, n: &dn::DropUserMappingStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("DROPUSERMAPPINGSTMT");
    write_node_field(buf, "user", n.user.as_deref(), wl);
    write_string_field(buf, "servername", n.servername.as_ref().map(|s| s.as_str()));
    write_bool_field(buf, "missing_ok", n.missing_ok);
}

/// `_outImportForeignSchemaStmt` (outfuncs.funcs.c).
fn out_import_foreign_schema_stmt(buf: &mut String, n: &dn::ImportForeignSchemaStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("IMPORTFOREIGNSCHEMASTMT");
    write_string_field(buf, "server_name", n.server_name.as_ref().map(|s| s.as_str()));
    write_string_field(buf, "remote_schema", n.remote_schema.as_ref().map(|s| s.as_str()));
    write_string_field(buf, "local_schema", n.local_schema.as_ref().map(|s| s.as_str()));
    write_enum_field(buf, "list_type", n.list_type as i32);
    write_node_list(buf, "table_list", n.table_list.as_slice(), wl);
    write_node_list(buf, "options", n.options.as_slice(), wl);
}

/// `_outCreatePolicyStmt` (outfuncs.funcs.c).
fn out_create_policy_stmt(buf: &mut String, n: &dn::CreatePolicyStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("CREATEPOLICYSTMT");
    write_string_field(buf, "policy_name", n.policy_name.as_ref().map(|s| s.as_str()));
    write_node_field(buf, "table", n.table.as_deref(), wl);
    write_string_field(buf, "cmd_name", n.cmd_name.as_ref().map(|s| s.as_str()));
    write_bool_field(buf, "permissive", n.permissive);
    write_node_list(buf, "roles", n.roles.as_slice(), wl);
    write_node_field(buf, "qual", n.qual.as_deref(), wl);
    write_node_field(buf, "with_check", n.with_check.as_deref(), wl);
}

/// `_outAlterPolicyStmt` (outfuncs.funcs.c).
fn out_alter_policy_stmt(buf: &mut String, n: &dn::AlterPolicyStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("ALTERPOLICYSTMT");
    write_string_field(buf, "policy_name", n.policy_name.as_ref().map(|s| s.as_str()));
    write_node_field(buf, "table", n.table.as_deref(), wl);
    write_node_list(buf, "roles", n.roles.as_slice(), wl);
    write_node_field(buf, "qual", n.qual.as_deref(), wl);
    write_node_field(buf, "with_check", n.with_check.as_deref(), wl);
}

/// `_outCreateAmStmt` (outfuncs.funcs.c).
fn out_create_am_stmt(buf: &mut String, n: &dn::CreateAmStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("CREATEAMSTMT");
    write_string_field(buf, "amname", n.amname.as_ref().map(|s| s.as_str()));
    write_node_list(buf, "handler_name", n.handler_name.as_slice(), wl);
    write_char_field(buf, "amtype", n.amtype as u8);
}

/// `_outCreateTrigStmt` (outfuncs.funcs.c).
fn out_create_trig_stmt(buf: &mut String, n: &dn::CreateTrigStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("CREATETRIGSTMT");
    write_bool_field(buf, "replace", n.replace);
    write_bool_field(buf, "isconstraint", n.isconstraint);
    write_string_field(buf, "trigname", n.trigname.as_ref().map(|s| s.as_str()));
    write_node_field(buf, "relation", n.relation.as_deref(), wl);
    write_node_list(buf, "funcname", n.funcname.as_slice(), wl);
    write_node_list(buf, "args", n.args.as_slice(), wl);
    write_bool_field(buf, "row", n.row);
    write_int_field(buf, "timing", n.timing as i32);
    write_int_field(buf, "events", n.events as i32);
    write_node_list(buf, "columns", n.columns.as_slice(), wl);
    write_node_field(buf, "whenClause", n.whenClause.as_deref(), wl);
    write_node_list(buf, "transitionRels", n.transitionRels.as_slice(), wl);
    write_bool_field(buf, "deferrable", n.deferrable);
    write_bool_field(buf, "initdeferred", n.initdeferred);
    write_node_field(buf, "constrrel", n.constrrel.as_deref(), wl);
}

/// `_outCreateEventTrigStmt` (outfuncs.funcs.c).
fn out_create_event_trig_stmt(buf: &mut String, n: &dn::CreateEventTrigStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("CREATEEVENTTRIGSTMT");
    write_string_field(buf, "trigname", n.trigname.as_ref().map(|s| s.as_str()));
    write_string_field(buf, "eventname", n.eventname.as_ref().map(|s| s.as_str()));
    write_node_list(buf, "whenclause", n.whenclause.as_slice(), wl);
    write_node_list(buf, "funcname", n.funcname.as_slice(), wl);
}

/// `_outAlterEventTrigStmt` (outfuncs.funcs.c).
fn out_alter_event_trig_stmt(buf: &mut String, n: &dn::AlterEventTrigStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("ALTEREVENTTRIGSTMT");
    write_string_field(buf, "trigname", n.trigname.as_ref().map(|s| s.as_str()));
    write_char_field(buf, "tgenabled", n.tgenabled as u8);
}

/// `_outCreatePLangStmt` (outfuncs.funcs.c).
fn out_create_p_lang_stmt(buf: &mut String, n: &dn::CreatePLangStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("CREATEPLANGSTMT");
    write_bool_field(buf, "replace", n.replace);
    write_string_field(buf, "plname", n.plname.as_ref().map(|s| s.as_str()));
    write_node_list(buf, "plhandler", n.plhandler.as_slice(), wl);
    write_node_list(buf, "plinline", n.plinline.as_slice(), wl);
    write_node_list(buf, "plvalidator", n.plvalidator.as_slice(), wl);
    write_bool_field(buf, "pltrusted", n.pltrusted);
}

/// `_outCreateRoleStmt` (outfuncs.funcs.c).
fn out_create_role_stmt(buf: &mut String, n: &dn::CreateRoleStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("CREATEROLESTMT");
    write_enum_field(buf, "stmt_type", n.stmt_type as i32);
    write_string_field(buf, "role", n.role.as_ref().map(|s| s.as_str()));
    write_node_list(buf, "options", n.options.as_slice(), wl);
}

/// `_outAlterRoleStmt` (outfuncs.funcs.c).
fn out_alter_role_stmt(buf: &mut String, n: &dn::AlterRoleStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("ALTERROLESTMT");
    write_node_field(buf, "role", n.role.as_deref(), wl);
    write_node_list(buf, "options", n.options.as_slice(), wl);
    write_int_field(buf, "action", n.action as i32);
}

/// `_outAlterRoleSetStmt` (outfuncs.funcs.c).
fn out_alter_role_set_stmt(buf: &mut String, n: &dn::AlterRoleSetStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("ALTERROLESETSTMT");
    write_node_field(buf, "role", n.role.as_deref(), wl);
    write_string_field(buf, "database", n.database.as_ref().map(|s| s.as_str()));
    write_node_field(buf, "setstmt", n.setstmt.as_deref(), wl);
}

/// `_outDropRoleStmt` (outfuncs.funcs.c).
fn out_drop_role_stmt(buf: &mut String, n: &dn::DropRoleStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("DROPROLESTMT");
    write_node_list(buf, "roles", n.roles.as_slice(), wl);
    write_bool_field(buf, "missing_ok", n.missing_ok);
}

/// `_outCreateSeqStmt` (outfuncs.funcs.c).
fn out_create_seq_stmt(buf: &mut String, n: &dn::CreateSeqStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("CREATESEQSTMT");
    write_node_field(buf, "sequence", n.sequence.as_deref(), wl);
    write_node_list(buf, "options", n.options.as_slice(), wl);
    write_oid_field(buf, "ownerId", n.ownerId);
    write_bool_field(buf, "for_identity", n.for_identity);
    write_bool_field(buf, "if_not_exists", n.if_not_exists);
}

/// `_outAlterSeqStmt` (outfuncs.funcs.c).
fn out_alter_seq_stmt(buf: &mut String, n: &dn::AlterSeqStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("ALTERSEQSTMT");
    write_node_field(buf, "sequence", n.sequence.as_deref(), wl);
    write_node_list(buf, "options", n.options.as_slice(), wl);
    write_bool_field(buf, "for_identity", n.for_identity);
    write_bool_field(buf, "missing_ok", n.missing_ok);
}

/// `_outDefineStmt` (outfuncs.funcs.c).
fn out_define_stmt(buf: &mut String, n: &dn::DefineStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("DEFINESTMT");
    write_enum_field(buf, "kind", n.kind as i32);
    write_bool_field(buf, "oldstyle", n.oldstyle);
    write_node_list(buf, "defnames", n.defnames.as_slice(), wl);
    write_node_list(buf, "args", n.args.as_slice(), wl);
    write_node_list(buf, "definition", n.definition.as_slice(), wl);
    write_bool_field(buf, "if_not_exists", n.if_not_exists);
    write_bool_field(buf, "replace", n.replace);
}

/// `_outCreateDomainStmt` (outfuncs.funcs.c).
fn out_create_domain_stmt(buf: &mut String, n: &dn::CreateDomainStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("CREATEDOMAINSTMT");
    write_node_list(buf, "domainname", n.domainname.as_slice(), wl);
    write_node_field(buf, "typeName", n.typeName.as_deref(), wl);
    write_node_field(buf, "collClause", n.collClause.as_deref(), wl);
    write_node_list(buf, "constraints", n.constraints.as_slice(), wl);
}

/// `_outCreateOpClassStmt` (outfuncs.funcs.c).
fn out_create_op_class_stmt(buf: &mut String, n: &dn::CreateOpClassStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("CREATEOPCLASSSTMT");
    write_node_list(buf, "opclassname", n.opclassname.as_slice(), wl);
    write_node_list(buf, "opfamilyname", n.opfamilyname.as_slice(), wl);
    write_string_field(buf, "amname", n.amname.as_ref().map(|s| s.as_str()));
    write_node_field(buf, "datatype", n.datatype.as_deref(), wl);
    write_node_list(buf, "items", n.items.as_slice(), wl);
    write_bool_field(buf, "isDefault", n.isDefault);
}

/// `_outCreateOpClassItem` (outfuncs.funcs.c).
fn out_create_op_class_item(buf: &mut String, n: &dn::CreateOpClassItem<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("CREATEOPCLASSITEM");
    write_int_field(buf, "itemtype", n.itemtype as i32);
    write_node_field(buf, "name", n.name.as_deref(), wl);
    write_int_field(buf, "number", n.number as i32);
    write_node_list(buf, "order_family", n.order_family.as_slice(), wl);
    write_node_list(buf, "class_args", n.class_args.as_slice(), wl);
    write_node_field(buf, "storedtype", n.storedtype.as_deref(), wl);
}

/// `_outCreateOpFamilyStmt` (outfuncs.funcs.c).
fn out_create_op_family_stmt(buf: &mut String, n: &dn::CreateOpFamilyStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("CREATEOPFAMILYSTMT");
    write_node_list(buf, "opfamilyname", n.opfamilyname.as_slice(), wl);
    write_string_field(buf, "amname", n.amname.as_ref().map(|s| s.as_str()));
}

/// `_outAlterOpFamilyStmt` (outfuncs.funcs.c).
fn out_alter_op_family_stmt(buf: &mut String, n: &dn::AlterOpFamilyStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("ALTEROPFAMILYSTMT");
    write_node_list(buf, "opfamilyname", n.opfamilyname.as_slice(), wl);
    write_string_field(buf, "amname", n.amname.as_ref().map(|s| s.as_str()));
    write_bool_field(buf, "isDrop", n.isDrop);
    write_node_list(buf, "items", n.items.as_slice(), wl);
}

/// `_outDropStmt` (outfuncs.funcs.c).
fn out_drop_stmt(buf: &mut String, n: &dn::DropStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("DROPSTMT");
    write_node_list(buf, "objects", n.objects.as_slice(), wl);
    write_enum_field(buf, "removeType", n.removeType as i32);
    write_enum_field(buf, "behavior", n.behavior as i32);
    write_bool_field(buf, "missing_ok", n.missing_ok);
    write_bool_field(buf, "concurrent", n.concurrent);
}

/// `_outTruncateStmt` (outfuncs.funcs.c).
fn out_truncate_stmt(buf: &mut String, n: &dn::TruncateStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("TRUNCATESTMT");
    write_node_list(buf, "relations", n.relations.as_slice(), wl);
    write_bool_field(buf, "restart_seqs", n.restart_seqs);
    write_enum_field(buf, "behavior", n.behavior as i32);
}

/// `_outCommentStmt` (outfuncs.funcs.c).
fn out_comment_stmt(buf: &mut String, n: &dn::CommentStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("COMMENTSTMT");
    write_enum_field(buf, "objtype", n.objtype as i32);
    write_node_field(buf, "object", n.object.as_deref(), wl);
    write_string_field(buf, "comment", n.comment.as_ref().map(|s| s.as_str()));
}

/// `_outSecLabelStmt` (outfuncs.funcs.c).
fn out_sec_label_stmt(buf: &mut String, n: &dn::SecLabelStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("SECLABELSTMT");
    write_enum_field(buf, "objtype", n.objtype as i32);
    write_node_field(buf, "object", n.object.as_deref(), wl);
    write_string_field(buf, "provider", n.provider.as_ref().map(|s| s.as_str()));
    write_string_field(buf, "label", n.label.as_ref().map(|s| s.as_str()));
}

/// `_outDeclareCursorStmt` (outfuncs.funcs.c).
fn out_declare_cursor_stmt(buf: &mut String, n: &dn::DeclareCursorStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("DECLARECURSORSTMT");
    write_string_field(buf, "portalname", n.portalname.as_ref().map(|s| s.as_str()));
    write_int_field(buf, "options", n.options as i32);
    write_node_field(buf, "query", n.query.as_deref(), wl);
}

/// `_outClosePortalStmt` (outfuncs.funcs.c).
fn out_close_portal_stmt(buf: &mut String, n: &dn::ClosePortalStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("CLOSEPORTALSTMT");
    write_string_field(buf, "portalname", n.portalname.as_ref().map(|s| s.as_str()));
}

/// `_outFetchStmt` (outfuncs.funcs.c).
fn out_fetch_stmt(buf: &mut String, n: &dn::FetchStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("FETCHSTMT");
    write_enum_field(buf, "direction", n.direction as i32);
    write_int64_field(buf, "howMany", n.how_many as i64);
    write_string_field(buf, "portalname", n.portalname.as_ref().map(|s| s.as_str()));
    write_bool_field(buf, "ismove", n.ismove);
}

/// `_outIndexStmt` (outfuncs.funcs.c).
fn out_index_stmt(buf: &mut String, n: &dn::IndexStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("INDEXSTMT");
    write_string_field(buf, "idxname", n.idxname.as_ref().map(|s| s.as_str()));
    write_node_field(buf, "relation", n.relation.as_deref(), wl);
    write_string_field(buf, "accessMethod", n.accessMethod.as_ref().map(|s| s.as_str()));
    write_string_field(buf, "tableSpace", n.tableSpace.as_ref().map(|s| s.as_str()));
    write_node_list(buf, "indexParams", n.indexParams.as_slice(), wl);
    write_node_list(buf, "indexIncludingParams", n.indexIncludingParams.as_slice(), wl);
    write_node_list(buf, "options", n.options.as_slice(), wl);
    write_node_field(buf, "whereClause", n.whereClause.as_deref(), wl);
    write_node_list(buf, "excludeOpNames", n.excludeOpNames.as_slice(), wl);
    write_string_field(buf, "idxcomment", n.idxcomment.as_ref().map(|s| s.as_str()));
    write_oid_field(buf, "indexOid", n.indexOid);
    write_oid_field(buf, "oldNumber", n.oldNumber);
    write_uint_field(buf, "oldCreateSubid", n.oldCreateSubid as u32);
    write_uint_field(buf, "oldFirstRelfilelocatorSubid", n.oldFirstRelfilelocatorSubid as u32);
    write_bool_field(buf, "unique", n.unique);
    write_bool_field(buf, "nulls_not_distinct", n.nulls_not_distinct);
    write_bool_field(buf, "primary", n.primary);
    write_bool_field(buf, "isconstraint", n.isconstraint);
    write_bool_field(buf, "iswithoutoverlaps", n.iswithoutoverlaps);
    write_bool_field(buf, "deferrable", n.deferrable);
    write_bool_field(buf, "initdeferred", n.initdeferred);
    write_bool_field(buf, "transformed", n.transformed);
    write_bool_field(buf, "concurrent", n.concurrent);
    write_bool_field(buf, "if_not_exists", n.if_not_exists);
    write_bool_field(buf, "reset_default_tblspc", n.reset_default_tblspc);
}

/// `_outCreateStatsStmt` (outfuncs.funcs.c).
fn out_create_stats_stmt(buf: &mut String, n: &dn::CreateStatsStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("CREATESTATSSTMT");
    write_node_list(buf, "defnames", n.defnames.as_slice(), wl);
    write_node_list(buf, "stat_types", n.stat_types.as_slice(), wl);
    write_node_list(buf, "exprs", n.exprs.as_slice(), wl);
    write_node_list(buf, "relations", n.relations.as_slice(), wl);
    write_string_field(buf, "stxcomment", n.stxcomment.as_ref().map(|s| s.as_str()));
    write_bool_field(buf, "transformed", n.transformed);
    write_bool_field(buf, "if_not_exists", n.if_not_exists);
}

/// `_outStatsElem` (outfuncs.funcs.c).
fn out_stats_elem(buf: &mut String, n: &dn::StatsElem<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("STATSELEM");
    write_string_field(buf, "name", n.name.as_ref().map(|s| s.as_str()));
    write_node_field(buf, "expr", n.expr.as_deref(), wl);
}

/// `_outAlterStatsStmt` (outfuncs.funcs.c).
fn out_alter_stats_stmt(buf: &mut String, n: &dn::AlterStatsStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("ALTERSTATSSTMT");
    write_node_list(buf, "defnames", n.defnames.as_slice(), wl);
    write_node_field(buf, "stxstattarget", n.stxstattarget.as_deref(), wl);
    write_bool_field(buf, "missing_ok", n.missing_ok);
}

/// `_outCreateFunctionStmt` (outfuncs.funcs.c).
fn out_create_function_stmt(buf: &mut String, n: &dn::CreateFunctionStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("CREATEFUNCTIONSTMT");
    write_bool_field(buf, "is_procedure", n.is_procedure);
    write_bool_field(buf, "replace", n.replace);
    write_node_list(buf, "funcname", n.funcname.as_slice(), wl);
    write_node_list(buf, "parameters", n.parameters.as_slice(), wl);
    write_node_field(buf, "returnType", n.returnType.as_deref(), wl);
    write_node_list(buf, "options", n.options.as_slice(), wl);
    write_node_field(buf, "sql_body", n.sql_body.as_deref(), wl);
}

/// `_outFunctionParameter` (outfuncs.funcs.c).
fn out_function_parameter(buf: &mut String, n: &dn::FunctionParameter<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("FUNCTIONPARAMETER");
    write_string_field(buf, "name", n.name.as_ref().map(|s| s.as_str()));
    write_node_field(buf, "argType", n.argType.as_deref(), wl);
    write_enum_field(buf, "mode", n.mode as i32);
    write_node_field(buf, "defexpr", n.defexpr.as_deref(), wl);
    write_location_field(buf, "location", n.location, wl);
}

/// `_outAlterFunctionStmt` (outfuncs.funcs.c).
fn out_alter_function_stmt(buf: &mut String, n: &dn::AlterFunctionStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("ALTERFUNCTIONSTMT");
    write_enum_field(buf, "objtype", n.objtype as i32);
    write_node_field(buf, "func", n.func.as_deref(), wl);
    write_node_list(buf, "actions", n.actions.as_slice(), wl);
}

/// `_outDoStmt` (outfuncs.funcs.c).
fn out_do_stmt(buf: &mut String, n: &dn::DoStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("DOSTMT");
    write_node_list(buf, "args", n.args.as_slice(), wl);
}

/// `_outCallStmt` (outfuncs.funcs.c).
fn out_call_stmt(buf: &mut String, n: &dn::CallStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("CALLSTMT");
    write_node_field(buf, "funccall", n.funccall.as_deref(), wl);
    write_node_field(buf, "funcexpr", n.funcexpr.as_deref(), wl);
    write_node_list(buf, "outargs", n.outargs.as_slice(), wl);
}

/// `_outRenameStmt` (outfuncs.funcs.c).
fn out_rename_stmt(buf: &mut String, n: &dn::RenameStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("RENAMESTMT");
    write_enum_field(buf, "renameType", n.renameType as i32);
    write_enum_field(buf, "relationType", n.relationType as i32);
    write_node_field(buf, "relation", n.relation.as_deref(), wl);
    write_node_field(buf, "object", n.object.as_deref(), wl);
    write_string_field(buf, "subname", n.subname.as_ref().map(|s| s.as_str()));
    write_string_field(buf, "newname", n.newname.as_ref().map(|s| s.as_str()));
    write_enum_field(buf, "behavior", n.behavior as i32);
    write_bool_field(buf, "missing_ok", n.missing_ok);
}

/// `_outAlterObjectDependsStmt` (outfuncs.funcs.c).
fn out_alter_object_depends_stmt(buf: &mut String, n: &dn::AlterObjectDependsStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("ALTEROBJECTDEPENDSSTMT");
    write_enum_field(buf, "objectType", n.objectType as i32);
    write_node_field(buf, "relation", n.relation.as_deref(), wl);
    write_node_field(buf, "object", n.object.as_deref(), wl);
    write_node_field(buf, "extname", n.extname.as_deref(), wl);
    write_bool_field(buf, "remove", n.remove);
}

/// `_outAlterObjectSchemaStmt` (outfuncs.funcs.c).
fn out_alter_object_schema_stmt(buf: &mut String, n: &dn::AlterObjectSchemaStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("ALTEROBJECTSCHEMASTMT");
    write_enum_field(buf, "objectType", n.objectType as i32);
    write_node_field(buf, "relation", n.relation.as_deref(), wl);
    write_node_field(buf, "object", n.object.as_deref(), wl);
    write_string_field(buf, "newschema", n.newschema.as_ref().map(|s| s.as_str()));
    write_bool_field(buf, "missing_ok", n.missing_ok);
}

/// `_outAlterOwnerStmt` (outfuncs.funcs.c).
fn out_alter_owner_stmt(buf: &mut String, n: &dn::AlterOwnerStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("ALTEROWNERSTMT");
    write_enum_field(buf, "objectType", n.objectType as i32);
    write_node_field(buf, "relation", n.relation.as_deref(), wl);
    write_node_field(buf, "object", n.object.as_deref(), wl);
    write_node_field(buf, "newowner", n.newowner.as_deref(), wl);
}

/// `_outAlterOperatorStmt` (outfuncs.funcs.c).
fn out_alter_operator_stmt(buf: &mut String, n: &dn::AlterOperatorStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("ALTEROPERATORSTMT");
    write_node_field(buf, "opername", n.opername.as_deref(), wl);
    write_node_list(buf, "options", n.options.as_slice(), wl);
}

/// `_outAlterTypeStmt` (outfuncs.funcs.c).
fn out_alter_type_stmt(buf: &mut String, n: &dn::AlterTypeStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("ALTERTYPESTMT");
    write_node_list(buf, "typeName", n.typeName.as_slice(), wl);
    write_node_list(buf, "options", n.options.as_slice(), wl);
}

/// `_outRuleStmt` (outfuncs.funcs.c).
fn out_rule_stmt(buf: &mut String, n: &dn::RuleStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("RULESTMT");
    write_node_field(buf, "relation", n.relation.as_deref(), wl);
    write_string_field(buf, "rulename", n.rulename.as_ref().map(|s| s.as_str()));
    write_node_field(buf, "whereClause", n.where_clause.as_deref(), wl);
    write_enum_field(buf, "event", n.event as i32);
    write_bool_field(buf, "instead", n.instead);
    write_node_list(buf, "actions", n.actions.as_slice(), wl);
    write_bool_field(buf, "replace", n.replace);
}

/// `_outNotifyStmt` (outfuncs.funcs.c).
fn out_notify_stmt(buf: &mut String, n: &dn::NotifyStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("NOTIFYSTMT");
    write_string_field(buf, "conditionname", n.conditionname.as_ref().map(|s| s.as_str()));
    write_string_field(buf, "payload", n.payload.as_ref().map(|s| s.as_str()));
}

/// `_outListenStmt` (outfuncs.funcs.c).
fn out_listen_stmt(buf: &mut String, n: &dn::ListenStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("LISTENSTMT");
    write_string_field(buf, "conditionname", n.conditionname.as_ref().map(|s| s.as_str()));
}

/// `_outUnlistenStmt` (outfuncs.funcs.c).
fn out_unlisten_stmt(buf: &mut String, n: &dn::UnlistenStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("UNLISTENSTMT");
    write_string_field(buf, "conditionname", n.conditionname.as_ref().map(|s| s.as_str()));
}

/// `_outTransactionStmt` (outfuncs.funcs.c).
fn out_transaction_stmt(buf: &mut String, n: &dn::TransactionStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("TRANSACTIONSTMT");
    write_enum_field(buf, "kind", n.kind as i32);
    write_node_list(buf, "options", n.options.as_slice(), wl);
    write_string_field(buf, "savepoint_name", n.savepoint_name.as_ref().map(|s| s.as_str()));
    write_string_field(buf, "gid", n.gid.as_ref().map(|s| s.as_str()));
    write_bool_field(buf, "chain", n.chain);
    write_location_field(buf, "location", n.location, wl);
}

/// `_outCompositeTypeStmt` (outfuncs.funcs.c).
fn out_composite_type_stmt(buf: &mut String, n: &dn::CompositeTypeStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("COMPOSITETYPESTMT");
    write_node_field(buf, "typevar", n.typevar.as_deref(), wl);
    write_node_list(buf, "coldeflist", n.coldeflist.as_slice(), wl);
}

/// `_outCreateEnumStmt` (outfuncs.funcs.c).
fn out_create_enum_stmt(buf: &mut String, n: &dn::CreateEnumStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("CREATEENUMSTMT");
    write_node_list(buf, "typeName", n.typeName.as_slice(), wl);
    write_node_list(buf, "vals", n.vals.as_slice(), wl);
}

/// `_outCreateRangeStmt` (outfuncs.funcs.c).
fn out_create_range_stmt(buf: &mut String, n: &dn::CreateRangeStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("CREATERANGESTMT");
    write_node_list(buf, "typeName", n.typeName.as_slice(), wl);
    write_node_list(buf, "params", n.params.as_slice(), wl);
}

/// `_outAlterEnumStmt` (outfuncs.funcs.c).
fn out_alter_enum_stmt(buf: &mut String, n: &dn::AlterEnumStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("ALTERENUMSTMT");
    write_node_list(buf, "typeName", n.typeName.as_slice(), wl);
    write_string_field(buf, "oldVal", n.oldVal.as_ref().map(|s| s.as_str()));
    write_string_field(buf, "newVal", n.newVal.as_ref().map(|s| s.as_str()));
    write_string_field(buf, "newValNeighbor", n.newValNeighbor.as_ref().map(|s| s.as_str()));
    write_bool_field(buf, "newValIsAfter", n.newValIsAfter);
    write_bool_field(buf, "skipIfNewValExists", n.skipIfNewValExists);
}

/// `_outViewStmt` (outfuncs.funcs.c).
fn out_view_stmt(buf: &mut String, n: &dn::ViewStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("VIEWSTMT");
    write_node_field(buf, "view", n.view.as_deref(), wl);
    write_node_list(buf, "aliases", n.aliases.as_slice(), wl);
    write_node_field(buf, "query", n.query.as_deref(), wl);
    write_bool_field(buf, "replace", n.replace);
    write_node_list(buf, "options", n.options.as_slice(), wl);
    write_enum_field(buf, "withCheckOption", n.withCheckOption as i32);
}

/// `_outLoadStmt` (outfuncs.funcs.c).
fn out_load_stmt(buf: &mut String, n: &dn::LoadStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("LOADSTMT");
    write_string_field(buf, "filename", n.filename.as_ref().map(|s| s.as_str()));
}

/// `_outCreatedbStmt` (outfuncs.funcs.c).
fn out_createdb_stmt(buf: &mut String, n: &dn::CreatedbStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("CREATEDBSTMT");
    write_string_field(buf, "dbname", n.dbname.as_ref().map(|s| s.as_str()));
    write_node_list(buf, "options", n.options.as_slice(), wl);
}

/// `_outAlterDatabaseStmt` (outfuncs.funcs.c).
fn out_alter_database_stmt(buf: &mut String, n: &dn::AlterDatabaseStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("ALTERDATABASESTMT");
    write_string_field(buf, "dbname", n.dbname.as_ref().map(|s| s.as_str()));
    write_node_list(buf, "options", n.options.as_slice(), wl);
}

/// `_outAlterDatabaseRefreshCollStmt` (outfuncs.funcs.c).
fn out_alter_database_refresh_coll_stmt(buf: &mut String, n: &dn::AlterDatabaseRefreshCollStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("ALTERDATABASEREFRESHCOLLSTMT");
    write_string_field(buf, "dbname", n.dbname.as_ref().map(|s| s.as_str()));
}

/// `_outAlterDatabaseSetStmt` (outfuncs.funcs.c).
fn out_alter_database_set_stmt(buf: &mut String, n: &dn::AlterDatabaseSetStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("ALTERDATABASESETSTMT");
    write_string_field(buf, "dbname", n.dbname.as_ref().map(|s| s.as_str()));
    write_node_field(buf, "setstmt", n.setstmt.as_deref(), wl);
}

/// `_outDropdbStmt` (outfuncs.funcs.c).
fn out_dropdb_stmt(buf: &mut String, n: &dn::DropdbStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("DROPDBSTMT");
    write_string_field(buf, "dbname", n.dbname.as_ref().map(|s| s.as_str()));
    write_bool_field(buf, "missing_ok", n.missing_ok);
    write_node_list(buf, "options", n.options.as_slice(), wl);
}

/// `_outAlterSystemStmt` (outfuncs.funcs.c).
fn out_alter_system_stmt(buf: &mut String, n: &dn::AlterSystemStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("ALTERSYSTEMSTMT");
    write_node_field(buf, "setstmt", n.setstmt.as_deref(), wl);
}

/// `_outClusterStmt` (outfuncs.funcs.c).
fn out_cluster_stmt(buf: &mut String, n: &dn::ClusterStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("CLUSTERSTMT");
    write_node_field(buf, "relation", n.relation.as_deref(), wl);
    write_string_field(buf, "indexname", n.indexname.as_ref().map(|s| s.as_str()));
    write_node_list(buf, "params", n.params.as_slice(), wl);
}

/// `_outVacuumStmt` (outfuncs.funcs.c).
fn out_vacuum_stmt(buf: &mut String, n: &dn::VacuumStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("VACUUMSTMT");
    write_node_list(buf, "options", n.options.as_slice(), wl);
    write_node_list(buf, "rels", n.rels.as_slice(), wl);
    write_bool_field(buf, "is_vacuumcmd", n.is_vacuumcmd);
}

/// `_outVacuumRelation` (outfuncs.funcs.c).
fn out_vacuum_relation(buf: &mut String, n: &dn::VacuumRelation<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("VACUUMRELATION");
    write_node_field(buf, "relation", n.relation.as_deref(), wl);
    write_oid_field(buf, "oid", n.oid);
    write_node_list(buf, "va_cols", n.va_cols.as_slice(), wl);
}

/// `_outExplainStmt` (outfuncs.funcs.c).
fn out_explain_stmt(buf: &mut String, n: &dn::ExplainStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("EXPLAINSTMT");
    write_node_field(buf, "query", n.query.as_deref(), wl);
    write_node_list(buf, "options", n.options.as_slice(), wl);
}

/// `_outCreateTableAsStmt` (outfuncs.funcs.c).
fn out_create_table_as_stmt(buf: &mut String, n: &dn::CreateTableAsStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("CREATETABLEASSTMT");
    write_node_field(buf, "query", n.query.as_deref(), wl);
    write_node_field(buf, "into", n.into.as_deref(), wl);
    write_enum_field(buf, "objtype", n.objtype as i32);
    write_bool_field(buf, "is_select_into", n.is_select_into);
    write_bool_field(buf, "if_not_exists", n.if_not_exists);
}

/// `_outRefreshMatViewStmt` (outfuncs.funcs.c).
fn out_refresh_mat_view_stmt(buf: &mut String, n: &dn::RefreshMatViewStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("REFRESHMATVIEWSTMT");
    write_bool_field(buf, "concurrent", n.concurrent);
    write_bool_field(buf, "skipData", n.skip_data);
    write_node_field(buf, "relation", n.relation.as_deref(), wl);
}

/// `_outCheckPointStmt` (outfuncs.funcs.c).
fn out_check_point_stmt(buf: &mut String, n: &dn::CheckPointStmt, wl: bool) {
    let _ = wl;
    buf.push_str("CHECKPOINTSTMT");
}

/// `_outDiscardStmt` (outfuncs.funcs.c).
fn out_discard_stmt(buf: &mut String, n: &dn::DiscardStmt, wl: bool) {
    let _ = wl;
    buf.push_str("DISCARDSTMT");
    write_enum_field(buf, "target", n.target as i32);
}

/// `_outLockStmt` (outfuncs.funcs.c).
fn out_lock_stmt(buf: &mut String, n: &dn::LockStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("LOCKSTMT");
    write_node_list(buf, "relations", n.relations.as_slice(), wl);
    write_int_field(buf, "mode", n.mode as i32);
    write_bool_field(buf, "nowait", n.nowait);
}

/// `_outConstraintsSetStmt` (outfuncs.funcs.c).
fn out_constraints_set_stmt(buf: &mut String, n: &dn::ConstraintsSetStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("CONSTRAINTSSETSTMT");
    write_node_list(buf, "constraints", n.constraints.as_slice(), wl);
    write_bool_field(buf, "deferred", n.deferred);
}

/// `_outReindexStmt` (outfuncs.funcs.c).
fn out_reindex_stmt(buf: &mut String, n: &dn::ReindexStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("REINDEXSTMT");
    write_enum_field(buf, "kind", n.kind as i32);
    write_node_field(buf, "relation", n.relation.as_deref(), wl);
    write_string_field(buf, "name", n.name.as_ref().map(|s| s.as_str()));
    write_node_list(buf, "params", n.params.as_slice(), wl);
}

/// `_outCreateConversionStmt` (outfuncs.funcs.c).
fn out_create_conversion_stmt(buf: &mut String, n: &dn::CreateConversionStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("CREATECONVERSIONSTMT");
    write_node_list(buf, "conversion_name", n.conversion_name.as_slice(), wl);
    write_string_field(buf, "for_encoding_name", n.for_encoding_name.as_ref().map(|s| s.as_str()));
    write_string_field(buf, "to_encoding_name", n.to_encoding_name.as_ref().map(|s| s.as_str()));
    write_node_list(buf, "func_name", n.func_name.as_slice(), wl);
    write_bool_field(buf, "def", n.def);
}

/// `_outCreateCastStmt` (outfuncs.funcs.c).
fn out_create_cast_stmt(buf: &mut String, n: &dn::CreateCastStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("CREATECASTSTMT");
    write_node_field(buf, "sourcetype", n.sourcetype.as_deref(), wl);
    write_node_field(buf, "targettype", n.targettype.as_deref(), wl);
    write_node_field(buf, "func", n.func.as_deref(), wl);
    write_enum_field(buf, "context", n.context as i32);
    write_bool_field(buf, "inout", n.inout);
}

/// `_outCreateTransformStmt` (outfuncs.funcs.c).
fn out_create_transform_stmt(buf: &mut String, n: &dn::CreateTransformStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("CREATETRANSFORMSTMT");
    write_bool_field(buf, "replace", n.replace);
    write_node_field(buf, "type_name", n.type_name.as_deref(), wl);
    write_string_field(buf, "lang", n.lang.as_ref().map(|s| s.as_str()));
    write_node_field(buf, "fromsql", n.fromsql.as_deref(), wl);
    write_node_field(buf, "tosql", n.tosql.as_deref(), wl);
}

/// `_outPrepareStmt` (outfuncs.funcs.c).
fn out_prepare_stmt(buf: &mut String, n: &dn::PrepareStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("PREPARESTMT");
    write_string_field(buf, "name", n.name.as_ref().map(|s| s.as_str()));
    write_node_list(buf, "argtypes", n.argtypes.as_slice(), wl);
    write_node_field(buf, "query", n.query.as_deref(), wl);
}

/// `_outExecuteStmt` (outfuncs.funcs.c).
fn out_execute_stmt(buf: &mut String, n: &dn::ExecuteStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("EXECUTESTMT");
    write_string_field(buf, "name", n.name.as_ref().map(|s| s.as_str()));
    write_node_list(buf, "params", n.params.as_slice(), wl);
}

/// `_outDeallocateStmt` (outfuncs.funcs.c).
fn out_deallocate_stmt(buf: &mut String, n: &dn::DeallocateStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("DEALLOCATESTMT");
    write_string_field(buf, "name", n.name.as_ref().map(|s| s.as_str()));
    write_bool_field(buf, "isall", n.isall);
    write_location_field(buf, "location", n.location, wl);
}

/// `_outDropOwnedStmt` (outfuncs.funcs.c).
fn out_drop_owned_stmt(buf: &mut String, n: &dn::DropOwnedStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("DROPOWNEDSTMT");
    write_node_list(buf, "roles", n.roles.as_slice(), wl);
    write_enum_field(buf, "behavior", n.behavior as i32);
}

/// `_outReassignOwnedStmt` (outfuncs.funcs.c).
fn out_reassign_owned_stmt(buf: &mut String, n: &dn::ReassignOwnedStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("REASSIGNOWNEDSTMT");
    write_node_list(buf, "roles", n.roles.as_slice(), wl);
    write_node_field(buf, "newrole", n.newrole.as_deref(), wl);
}

/// `_outAlterTSDictionaryStmt` (outfuncs.funcs.c).
fn out_alter_t_s_dictionary_stmt(buf: &mut String, n: &dn::AlterTSDictionaryStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("ALTERTSDICTIONARYSTMT");
    write_node_list(buf, "dictname", n.dictname.as_slice(), wl);
    write_node_list(buf, "options", n.options.as_slice(), wl);
}

/// `_outAlterTSConfigurationStmt` (outfuncs.funcs.c).
fn out_alter_t_s_configuration_stmt(buf: &mut String, n: &dn::AlterTSConfigurationStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("ALTERTSCONFIGURATIONSTMT");
    write_enum_field(buf, "kind", n.kind as i32);
    write_node_list(buf, "cfgname", n.cfgname.as_slice(), wl);
    write_node_list(buf, "tokentype", n.tokentype.as_slice(), wl);
    write_node_list(buf, "dicts", n.dicts.as_slice(), wl);
    write_bool_field(buf, "override", n.override_);
    write_bool_field(buf, "replace", n.replace);
    write_bool_field(buf, "missing_ok", n.missing_ok);
}

/// `_outPublicationTable` (outfuncs.funcs.c).
fn out_publication_table(buf: &mut String, n: &dn::PublicationTable<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("PUBLICATIONTABLE");
    write_node_field(buf, "relation", n.relation.as_deref(), wl);
    write_node_field(buf, "whereClause", n.where_clause.as_deref(), wl);
    write_node_list(buf, "columns", n.columns.as_slice(), wl);
}

/// `_outPublicationObjSpec` (outfuncs.funcs.c).
fn out_publication_obj_spec(buf: &mut String, n: &dn::PublicationObjSpec<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("PUBLICATIONOBJSPEC");
    write_enum_field(buf, "pubobjtype", n.pubobjtype as i32);
    write_string_field(buf, "name", n.name.as_ref().map(|s| s.as_str()));
    buf.push_str(" :pubtable ");
    match n.pubtable.as_deref() {
        None => buf.push_str("<>"),
        Some(t) => framed(buf, |b| out_publication_table(b, t, wl)),
    }
    write_location_field(buf, "location", n.location, wl);
}

/// `_outCreatePublicationStmt` (outfuncs.funcs.c).
fn out_create_publication_stmt(buf: &mut String, n: &dn::CreatePublicationStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("CREATEPUBLICATIONSTMT");
    write_string_field(buf, "pubname", n.pubname.as_ref().map(|s| s.as_str()));
    write_node_list(buf, "options", n.options.as_slice(), wl);
    write_node_list(buf, "pubobjects", n.pubobjects.as_slice(), wl);
    write_bool_field(buf, "for_all_tables", n.for_all_tables);
}

/// `_outAlterPublicationStmt` (outfuncs.funcs.c).
fn out_alter_publication_stmt(buf: &mut String, n: &dn::AlterPublicationStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("ALTERPUBLICATIONSTMT");
    write_string_field(buf, "pubname", n.pubname.as_ref().map(|s| s.as_str()));
    write_node_list(buf, "options", n.options.as_slice(), wl);
    write_node_list(buf, "pubobjects", n.pubobjects.as_slice(), wl);
    write_bool_field(buf, "for_all_tables", n.for_all_tables);
    write_enum_field(buf, "action", n.action as i32);
}

/// `_outCreateSubscriptionStmt` (outfuncs.funcs.c).
fn out_create_subscription_stmt(buf: &mut String, n: &dn::CreateSubscriptionStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("CREATESUBSCRIPTIONSTMT");
    write_string_field(buf, "subname", n.subname.as_ref().map(|s| s.as_str()));
    write_string_field(buf, "conninfo", n.conninfo.as_ref().map(|s| s.as_str()));
    write_node_list(buf, "publication", n.publication.as_slice(), wl);
    write_node_list(buf, "options", n.options.as_slice(), wl);
}

/// `_outAlterSubscriptionStmt` (outfuncs.funcs.c).
fn out_alter_subscription_stmt(buf: &mut String, n: &dn::AlterSubscriptionStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("ALTERSUBSCRIPTIONSTMT");
    write_enum_field(buf, "kind", n.kind as i32);
    write_string_field(buf, "subname", n.subname.as_ref().map(|s| s.as_str()));
    write_string_field(buf, "conninfo", n.conninfo.as_ref().map(|s| s.as_str()));
    write_node_list(buf, "publication", n.publication.as_slice(), wl);
    write_node_list(buf, "options", n.options.as_slice(), wl);
}

/// `_outDropSubscriptionStmt` (outfuncs.funcs.c).
fn out_drop_subscription_stmt(buf: &mut String, n: &dn::DropSubscriptionStmt<'_>, wl: bool) {
    let _ = wl;
    buf.push_str("DROPSUBSCRIPTIONSTMT");
    write_string_field(buf, "subname", n.subname.as_ref().map(|s| s.as_str()));
    write_bool_field(buf, "missing_ok", n.missing_ok);
    write_enum_field(buf, "behavior", n.behavior as i32);
}

pub(crate) fn try_out(buf: &mut String, node: &Node<'_>, wl: bool) -> bool {
    match node.node_tag() {
        ntag::T_IntoClause => { let n = node.expect_intoclause(); crate::framed(buf, |b| out_into_clause(b, n, wl)) },
        ntag::T_RoleSpec => { let n = node.expect_rolespec(); crate::framed(buf, |b| out_role_spec(b, n, wl)) },
        ntag::T_TableLikeClause => { let n = node.expect_tablelikeclause(); crate::framed(buf, |b| out_table_like_clause(b, n, wl)) },
        ntag::T_IndexElem => { let n = node.expect_indexelem(); crate::framed(buf, |b| out_index_elem(b, n, wl)) },
        ntag::T_DefElem => { let n = node.expect_defelem(); crate::framed(buf, |b| out_def_elem(b, n, wl)) },
        ntag::T_PartitionElem => { let n = node.expect_partitionelem(); crate::framed(buf, |b| out_partition_elem(b, n, wl)) },
        ntag::T_PartitionSpec => { let n = node.expect_partitionspec(); crate::framed(buf, |b| out_partition_spec(b, n, wl)) },
        ntag::T_PartitionBoundSpec => { let n = node.expect_partitionboundspec(); crate::framed(buf, |b| out_partition_bound_spec(b, n, wl)) },
        ntag::T_PartitionRangeDatum => { let n = node.expect_partitionrangedatum(); crate::framed(buf, |b| out_partition_range_datum(b, n, wl)) },
        ntag::T_PartitionCmd => { let n = node.expect_partitioncmd(); crate::framed(buf, |b| out_partition_cmd(b, n, wl)) },
        ntag::T_ReturnStmt => { let n = node.expect_returnstmt(); crate::framed(buf, |b| out_return_stmt(b, n, wl)) },
        ntag::T_PLAssignStmt => { let n = node.expect_plassignstmt(); crate::framed(buf, |b| out_p_l_assign_stmt(b, n, wl)) },
        ntag::T_CreateSchemaStmt => { let n = node.expect_createschemastmt(); crate::framed(buf, |b| out_create_schema_stmt(b, n, wl)) },
        ntag::T_AlterTableStmt => { let n = node.expect_altertablestmt(); crate::framed(buf, |b| out_alter_table_stmt(b, n, wl)) },
        ntag::T_AlterTableCmd => { let n = node.expect_altertablecmd(); crate::framed(buf, |b| out_alter_table_cmd(b, n, wl)) },
        ntag::T_ATAlterConstraint => { let n = node.expect_atalterconstraint(); crate::framed(buf, |b| out_a_t_alter_constraint(b, n, wl)) },
        ntag::T_ReplicaIdentityStmt => { let n = node.expect_replicaidentitystmt(); crate::framed(buf, |b| out_replica_identity_stmt(b, n, wl)) },
        ntag::T_AlterCollationStmt => { let n = node.expect_altercollationstmt(); crate::framed(buf, |b| out_alter_collation_stmt(b, n, wl)) },
        ntag::T_AlterDomainStmt => { let n = node.expect_alterdomainstmt(); crate::framed(buf, |b| out_alter_domain_stmt(b, n, wl)) },
        ntag::T_GrantStmt => { let n = node.expect_grantstmt(); crate::framed(buf, |b| out_grant_stmt(b, n, wl)) },
        ntag::T_ObjectWithArgs => { let n = node.expect_objectwithargs(); crate::framed(buf, |b| out_object_with_args(b, n, wl)) },
        ntag::T_AccessPriv => { let n = node.expect_accesspriv(); crate::framed(buf, |b| out_access_priv(b, n, wl)) },
        ntag::T_GrantRoleStmt => { let n = node.expect_grantrolestmt(); crate::framed(buf, |b| out_grant_role_stmt(b, n, wl)) },
        ntag::T_AlterDefaultPrivilegesStmt => { let n = node.expect_alterdefaultprivilegesstmt(); crate::framed(buf, |b| out_alter_default_privileges_stmt(b, n, wl)) },
        ntag::T_CopyStmt => { let n = node.expect_copystmt(); crate::framed(buf, |b| out_copy_stmt(b, n, wl)) },
        ntag::T_VariableSetStmt => { let n = node.expect_variablesetstmt(); crate::framed(buf, |b| out_variable_set_stmt(b, n, wl)) },
        ntag::T_VariableShowStmt => { let n = node.expect_variableshowstmt(); crate::framed(buf, |b| out_variable_show_stmt(b, n, wl)) },
        ntag::T_CreateStmt => { let n = node.expect_createstmt(); crate::framed(buf, |b| out_create_stmt(b, n, wl)) },
        ntag::T_Constraint => { let n = node.expect_constraint(); crate::framed(buf, |b| out_constraint(b, n, wl)) },
        ntag::T_CreateTableSpaceStmt => { let n = node.expect_createtablespacestmt(); crate::framed(buf, |b| out_create_table_space_stmt(b, n, wl)) },
        ntag::T_DropTableSpaceStmt => { let n = node.expect_droptablespacestmt(); crate::framed(buf, |b| out_drop_table_space_stmt(b, n, wl)) },
        ntag::T_AlterTableSpaceOptionsStmt => { let n = node.expect_altertablespaceoptionsstmt(); crate::framed(buf, |b| out_alter_table_space_options_stmt(b, n, wl)) },
        ntag::T_AlterTableMoveAllStmt => { let n = node.expect_altertablemoveallstmt(); crate::framed(buf, |b| out_alter_table_move_all_stmt(b, n, wl)) },
        ntag::T_CreateExtensionStmt => { let n = node.expect_createextensionstmt(); crate::framed(buf, |b| out_create_extension_stmt(b, n, wl)) },
        ntag::T_AlterExtensionStmt => { let n = node.expect_alterextensionstmt(); crate::framed(buf, |b| out_alter_extension_stmt(b, n, wl)) },
        ntag::T_AlterExtensionContentsStmt => { let n = node.expect_alterextensioncontentsstmt(); crate::framed(buf, |b| out_alter_extension_contents_stmt(b, n, wl)) },
        ntag::T_CreateFdwStmt => { let n = node.expect_createfdwstmt(); crate::framed(buf, |b| out_create_fdw_stmt(b, n, wl)) },
        ntag::T_AlterFdwStmt => { let n = node.expect_alterfdwstmt(); crate::framed(buf, |b| out_alter_fdw_stmt(b, n, wl)) },
        ntag::T_CreateForeignServerStmt => { let n = node.expect_createforeignserverstmt(); crate::framed(buf, |b| out_create_foreign_server_stmt(b, n, wl)) },
        ntag::T_AlterForeignServerStmt => { let n = node.expect_alterforeignserverstmt(); crate::framed(buf, |b| out_alter_foreign_server_stmt(b, n, wl)) },
        ntag::T_CreateForeignTableStmt => { let n = node.expect_createforeigntablestmt(); crate::framed(buf, |b| out_create_foreign_table_stmt(b, n, wl)) },
        ntag::T_CreateUserMappingStmt => { let n = node.expect_createusermappingstmt(); crate::framed(buf, |b| out_create_user_mapping_stmt(b, n, wl)) },
        ntag::T_AlterUserMappingStmt => { let n = node.expect_alterusermappingstmt(); crate::framed(buf, |b| out_alter_user_mapping_stmt(b, n, wl)) },
        ntag::T_DropUserMappingStmt => { let n = node.expect_dropusermappingstmt(); crate::framed(buf, |b| out_drop_user_mapping_stmt(b, n, wl)) },
        ntag::T_ImportForeignSchemaStmt => { let n = node.expect_importforeignschemastmt(); crate::framed(buf, |b| out_import_foreign_schema_stmt(b, n, wl)) },
        ntag::T_CreatePolicyStmt => { let n = node.expect_createpolicystmt(); crate::framed(buf, |b| out_create_policy_stmt(b, n, wl)) },
        ntag::T_AlterPolicyStmt => { let n = node.expect_alterpolicystmt(); crate::framed(buf, |b| out_alter_policy_stmt(b, n, wl)) },
        ntag::T_CreateAmStmt => { let n = node.expect_createamstmt(); crate::framed(buf, |b| out_create_am_stmt(b, n, wl)) },
        ntag::T_CreateTrigStmt => { let n = node.expect_createtrigstmt(); crate::framed(buf, |b| out_create_trig_stmt(b, n, wl)) },
        ntag::T_CreateEventTrigStmt => { let n = node.expect_createeventtrigstmt(); crate::framed(buf, |b| out_create_event_trig_stmt(b, n, wl)) },
        ntag::T_AlterEventTrigStmt => { let n = node.expect_altereventtrigstmt(); crate::framed(buf, |b| out_alter_event_trig_stmt(b, n, wl)) },
        ntag::T_CreatePLangStmt => { let n = node.expect_createplangstmt(); crate::framed(buf, |b| out_create_p_lang_stmt(b, n, wl)) },
        ntag::T_CreateRoleStmt => { let n = node.expect_createrolestmt(); crate::framed(buf, |b| out_create_role_stmt(b, n, wl)) },
        ntag::T_AlterRoleStmt => { let n = node.expect_alterrolestmt(); crate::framed(buf, |b| out_alter_role_stmt(b, n, wl)) },
        ntag::T_AlterRoleSetStmt => { let n = node.expect_alterrolesetstmt(); crate::framed(buf, |b| out_alter_role_set_stmt(b, n, wl)) },
        ntag::T_DropRoleStmt => { let n = node.expect_droprolestmt(); crate::framed(buf, |b| out_drop_role_stmt(b, n, wl)) },
        ntag::T_CreateSeqStmt => { let n = node.expect_createseqstmt(); crate::framed(buf, |b| out_create_seq_stmt(b, n, wl)) },
        ntag::T_AlterSeqStmt => { let n = node.expect_alterseqstmt(); crate::framed(buf, |b| out_alter_seq_stmt(b, n, wl)) },
        ntag::T_DefineStmt => { let n = node.expect_definestmt(); crate::framed(buf, |b| out_define_stmt(b, n, wl)) },
        ntag::T_CreateDomainStmt => { let n = node.expect_createdomainstmt(); crate::framed(buf, |b| out_create_domain_stmt(b, n, wl)) },
        ntag::T_CreateOpClassStmt => { let n = node.expect_createopclassstmt(); crate::framed(buf, |b| out_create_op_class_stmt(b, n, wl)) },
        ntag::T_CreateOpClassItem => { let n = node.expect_createopclassitem(); crate::framed(buf, |b| out_create_op_class_item(b, n, wl)) },
        ntag::T_CreateOpFamilyStmt => { let n = node.expect_createopfamilystmt(); crate::framed(buf, |b| out_create_op_family_stmt(b, n, wl)) },
        ntag::T_AlterOpFamilyStmt => { let n = node.expect_alteropfamilystmt(); crate::framed(buf, |b| out_alter_op_family_stmt(b, n, wl)) },
        ntag::T_DropStmt => { let n = node.expect_dropstmt(); crate::framed(buf, |b| out_drop_stmt(b, n, wl)) },
        ntag::T_TruncateStmt => { let n = node.expect_truncatestmt(); crate::framed(buf, |b| out_truncate_stmt(b, n, wl)) },
        ntag::T_CommentStmt => { let n = node.expect_commentstmt(); crate::framed(buf, |b| out_comment_stmt(b, n, wl)) },
        ntag::T_SecLabelStmt => { let n = node.expect_seclabelstmt(); crate::framed(buf, |b| out_sec_label_stmt(b, n, wl)) },
        ntag::T_DeclareCursorStmt => { let n = node.expect_declarecursorstmt(); crate::framed(buf, |b| out_declare_cursor_stmt(b, n, wl)) },
        ntag::T_ClosePortalStmt => { let n = node.expect_closeportalstmt(); crate::framed(buf, |b| out_close_portal_stmt(b, n, wl)) },
        ntag::T_FetchStmt => { let n = node.expect_fetchstmt(); crate::framed(buf, |b| out_fetch_stmt(b, n, wl)) },
        ntag::T_IndexStmt => { let n = node.expect_indexstmt(); crate::framed(buf, |b| out_index_stmt(b, n, wl)) },
        ntag::T_CreateStatsStmt => { let n = node.expect_createstatsstmt(); crate::framed(buf, |b| out_create_stats_stmt(b, n, wl)) },
        ntag::T_StatsElem => { let n = node.expect_statselem(); crate::framed(buf, |b| out_stats_elem(b, n, wl)) },
        ntag::T_AlterStatsStmt => { let n = node.expect_alterstatsstmt(); crate::framed(buf, |b| out_alter_stats_stmt(b, n, wl)) },
        ntag::T_CreateFunctionStmt => { let n = node.expect_createfunctionstmt(); crate::framed(buf, |b| out_create_function_stmt(b, n, wl)) },
        ntag::T_FunctionParameter => { let n = node.expect_functionparameter(); crate::framed(buf, |b| out_function_parameter(b, n, wl)) },
        ntag::T_AlterFunctionStmt => { let n = node.expect_alterfunctionstmt(); crate::framed(buf, |b| out_alter_function_stmt(b, n, wl)) },
        ntag::T_DoStmt => { let n = node.expect_dostmt(); crate::framed(buf, |b| out_do_stmt(b, n, wl)) },
        ntag::T_CallStmt => { let n = node.expect_callstmt(); crate::framed(buf, |b| out_call_stmt(b, n, wl)) },
        ntag::T_RenameStmt => { let n = node.expect_renamestmt(); crate::framed(buf, |b| out_rename_stmt(b, n, wl)) },
        ntag::T_AlterObjectDependsStmt => { let n = node.expect_alterobjectdependsstmt(); crate::framed(buf, |b| out_alter_object_depends_stmt(b, n, wl)) },
        ntag::T_AlterObjectSchemaStmt => { let n = node.expect_alterobjectschemastmt(); crate::framed(buf, |b| out_alter_object_schema_stmt(b, n, wl)) },
        ntag::T_AlterOwnerStmt => { let n = node.expect_alterownerstmt(); crate::framed(buf, |b| out_alter_owner_stmt(b, n, wl)) },
        ntag::T_AlterOperatorStmt => { let n = node.expect_alteroperatorstmt(); crate::framed(buf, |b| out_alter_operator_stmt(b, n, wl)) },
        ntag::T_AlterTypeStmt => { let n = node.expect_altertypestmt(); crate::framed(buf, |b| out_alter_type_stmt(b, n, wl)) },
        ntag::T_RuleStmt => { let n = node.expect_rulestmt(); crate::framed(buf, |b| out_rule_stmt(b, n, wl)) },
        ntag::T_NotifyStmt => { let n = node.expect_notifystmt(); crate::framed(buf, |b| out_notify_stmt(b, n, wl)) },
        ntag::T_ListenStmt => { let n = node.expect_listenstmt(); crate::framed(buf, |b| out_listen_stmt(b, n, wl)) },
        ntag::T_UnlistenStmt => { let n = node.expect_unlistenstmt(); crate::framed(buf, |b| out_unlisten_stmt(b, n, wl)) },
        ntag::T_TransactionStmt => { let n = node.expect_transactionstmt(); crate::framed(buf, |b| out_transaction_stmt(b, n, wl)) },
        ntag::T_CompositeTypeStmt => { let n = node.expect_compositetypestmt(); crate::framed(buf, |b| out_composite_type_stmt(b, n, wl)) },
        ntag::T_CreateEnumStmt => { let n = node.expect_createenumstmt(); crate::framed(buf, |b| out_create_enum_stmt(b, n, wl)) },
        ntag::T_CreateRangeStmt => { let n = node.expect_createrangestmt(); crate::framed(buf, |b| out_create_range_stmt(b, n, wl)) },
        ntag::T_AlterEnumStmt => { let n = node.expect_alterenumstmt(); crate::framed(buf, |b| out_alter_enum_stmt(b, n, wl)) },
        ntag::T_ViewStmt => { let n = node.expect_viewstmt(); crate::framed(buf, |b| out_view_stmt(b, n, wl)) },
        ntag::T_LoadStmt => { let n = node.expect_loadstmt(); crate::framed(buf, |b| out_load_stmt(b, n, wl)) },
        ntag::T_CreatedbStmt => { let n = node.expect_createdbstmt(); crate::framed(buf, |b| out_createdb_stmt(b, n, wl)) },
        ntag::T_AlterDatabaseStmt => { let n = node.expect_alterdatabasestmt(); crate::framed(buf, |b| out_alter_database_stmt(b, n, wl)) },
        ntag::T_AlterDatabaseRefreshCollStmt => { let n = node.expect_alterdatabaserefreshcollstmt(); crate::framed(buf, |b| out_alter_database_refresh_coll_stmt(b, n, wl)) },
        ntag::T_AlterDatabaseSetStmt => { let n = node.expect_alterdatabasesetstmt(); crate::framed(buf, |b| out_alter_database_set_stmt(b, n, wl)) },
        ntag::T_DropdbStmt => { let n = node.expect_dropdbstmt(); crate::framed(buf, |b| out_dropdb_stmt(b, n, wl)) },
        ntag::T_AlterSystemStmt => { let n = node.expect_altersystemstmt(); crate::framed(buf, |b| out_alter_system_stmt(b, n, wl)) },
        ntag::T_ClusterStmt => { let n = node.expect_clusterstmt(); crate::framed(buf, |b| out_cluster_stmt(b, n, wl)) },
        ntag::T_VacuumStmt => { let n = node.expect_vacuumstmt(); crate::framed(buf, |b| out_vacuum_stmt(b, n, wl)) },
        ntag::T_VacuumRelation => { let n = node.expect_vacuumrelation(); crate::framed(buf, |b| out_vacuum_relation(b, n, wl)) },
        ntag::T_ExplainStmt => { let n = node.expect_explainstmt(); crate::framed(buf, |b| out_explain_stmt(b, n, wl)) },
        ntag::T_CreateTableAsStmt => { let n = node.expect_createtableasstmt(); crate::framed(buf, |b| out_create_table_as_stmt(b, n, wl)) },
        ntag::T_RefreshMatViewStmt => { let n = node.expect_refreshmatviewstmt(); crate::framed(buf, |b| out_refresh_mat_view_stmt(b, n, wl)) },
        ntag::T_CheckPointStmt => { let n = node.expect_checkpointstmt(); crate::framed(buf, |b| out_check_point_stmt(b, n, wl)) },
        ntag::T_DiscardStmt => { let n = node.expect_discardstmt(); crate::framed(buf, |b| out_discard_stmt(b, n, wl)) },
        ntag::T_LockStmt => { let n = node.expect_lockstmt(); crate::framed(buf, |b| out_lock_stmt(b, n, wl)) },
        ntag::T_ConstraintsSetStmt => { let n = node.expect_constraintssetstmt(); crate::framed(buf, |b| out_constraints_set_stmt(b, n, wl)) },
        ntag::T_ReindexStmt => { let n = node.expect_reindexstmt(); crate::framed(buf, |b| out_reindex_stmt(b, n, wl)) },
        ntag::T_CreateConversionStmt => { let n = node.expect_createconversionstmt(); crate::framed(buf, |b| out_create_conversion_stmt(b, n, wl)) },
        ntag::T_CreateCastStmt => { let n = node.expect_createcaststmt(); crate::framed(buf, |b| out_create_cast_stmt(b, n, wl)) },
        ntag::T_CreateTransformStmt => { let n = node.expect_createtransformstmt(); crate::framed(buf, |b| out_create_transform_stmt(b, n, wl)) },
        ntag::T_PrepareStmt => { let n = node.expect_preparestmt(); crate::framed(buf, |b| out_prepare_stmt(b, n, wl)) },
        ntag::T_ExecuteStmt => { let n = node.expect_executestmt(); crate::framed(buf, |b| out_execute_stmt(b, n, wl)) },
        ntag::T_DeallocateStmt => { let n = node.expect_deallocatestmt(); crate::framed(buf, |b| out_deallocate_stmt(b, n, wl)) },
        ntag::T_DropOwnedStmt => { let n = node.expect_dropownedstmt(); crate::framed(buf, |b| out_drop_owned_stmt(b, n, wl)) },
        ntag::T_ReassignOwnedStmt => { let n = node.expect_reassignownedstmt(); crate::framed(buf, |b| out_reassign_owned_stmt(b, n, wl)) },
        ntag::T_AlterTSDictionaryStmt => { let n = node.expect_altertsdictionarystmt(); crate::framed(buf, |b| out_alter_t_s_dictionary_stmt(b, n, wl)) },
        ntag::T_AlterTSConfigurationStmt => { let n = node.expect_altertsconfigurationstmt(); crate::framed(buf, |b| out_alter_t_s_configuration_stmt(b, n, wl)) },
        ntag::T_PublicationTable => { let n = node.expect_publicationtable(); crate::framed(buf, |b| out_publication_table(b, n, wl)) },
        ntag::T_PublicationObjSpec => { let n = node.expect_publicationobjspec(); crate::framed(buf, |b| out_publication_obj_spec(b, n, wl)) },
        ntag::T_CreatePublicationStmt => { let n = node.expect_createpublicationstmt(); crate::framed(buf, |b| out_create_publication_stmt(b, n, wl)) },
        ntag::T_AlterPublicationStmt => { let n = node.expect_alterpublicationstmt(); crate::framed(buf, |b| out_alter_publication_stmt(b, n, wl)) },
        ntag::T_CreateSubscriptionStmt => { let n = node.expect_createsubscriptionstmt(); crate::framed(buf, |b| out_create_subscription_stmt(b, n, wl)) },
        ntag::T_AlterSubscriptionStmt => { let n = node.expect_altersubscriptionstmt(); crate::framed(buf, |b| out_alter_subscription_stmt(b, n, wl)) },
        ntag::T_DropSubscriptionStmt => { let n = node.expect_dropsubscriptionstmt(); crate::framed(buf, |b| out_drop_subscription_stmt(b, n, wl)) },
        _ => return false,
    }
    true
}
