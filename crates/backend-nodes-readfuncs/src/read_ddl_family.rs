//! `_read<Type>` readers for the raw-grammar DDL statement family.
//! Generated field-for-field from `readfuncs.funcs.c`; reads fields in the
//! exact order the OUT side wrote them.

use mcx::{Mcx, PgBox, PgString, PgVec};
use types_error::PgResult;
use types_nodes::nodes::{Node, NodePtr};
use types_nodes::ddlnodes as dn;

use crate::{
    elog_error, read_bool_field, read_char_field, read_enum_field, read_int64_field,
    read_int_field, read_location_field, read_oid_field, read_uint_field,
};
use backend_nodes_core::read;

/// `READ_NODE_FIELD` of an `Option<NodePtr>`: `crate::read_node_field` skips the
/// label and node_reads; `<>` -> None.
fn read_opt_node<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Option<NodePtr<'mcx>>> {
    crate::read_node_field(mcx)
}
/// `READ_NODE_FIELD` of a `List *`: `crate::read_node_list_field` skips the label
/// and node_reads a `(...)` list; `<>` -> empty.
fn read_node_vec<'mcx>(mcx: Mcx<'mcx>) -> PgResult<PgVec<'mcx, NodePtr<'mcx>>> {
    let v = crate::read_node_list_field(mcx)?;
    let mut out = PgVec::new_in(mcx);
    for e in v { out.push(e); }
    Ok(out)
}
/// Skip the `:fldname` label token off the shared cursor.
fn skip_label() -> PgResult<()> {
    read::pg_strtok().ok_or_else(|| elog_error("unexpected end of node string"))?;
    Ok(())
}
/// `READ_STRING_FIELD` (readfuncs.c nullable_string): `<>` -> None, `""` -> empty,
/// else debackslash. Mirrors lib::read_string_field (private there).
fn read_str<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Option<PgString<'mcx>>> {
    skip_label()?; // :fldname
    let v = read::pg_strtok().ok_or_else(|| elog_error("unexpected end of node string"))?;
    if v.bytes.is_empty() {
        return Ok(None);
    }
    if v.bytes == b"\"\"" {
        return Ok(Some(PgString::from_str_in("", mcx)?));
    }
    let s = read::debackslash(v.bytes);
    Ok(Some(PgString::from_str_in(&s, mcx)?))
}

/// `_readIntoClause` (readfuncs.funcs.c).
fn read_into_clause<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::IntoClause<'mcx>> {
    let _ = mcx;
    let rel = read_opt_node(mcx)?;
    let colNames = read_node_vec(mcx)?;
    let accessMethod = read_str(mcx)?;
    let options = read_node_vec(mcx)?;
    let onCommit = on_commit_action_from(read_enum_field()?);
    let tableSpaceName = read_str(mcx)?;
    let viewQuery = read_opt_node(mcx)?;
    let skipData = read_bool_field()?;
    Ok(dn::IntoClause {
        rel: rel,
        colNames: colNames,
        accessMethod: accessMethod,
        options: options,
        onCommit: onCommit,
        tableSpaceName: tableSpaceName,
        viewQuery: viewQuery,
        skipData: skipData,
    })
}

/// `_readRoleSpec` (readfuncs.funcs.c).
fn read_role_spec<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::RoleSpec<'mcx>> {
    let _ = mcx;
    let roletype = role_spec_type_from(read_enum_field()?);
    let rolename = read_str(mcx)?;
    let location = read_location_field()?;
    Ok(dn::RoleSpec {
        roletype: roletype,
        rolename: rolename,
        location: location,
    })
}

/// `_readTableLikeClause` (readfuncs.funcs.c).
fn read_table_like_clause<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::TableLikeClause<'mcx>> {
    let _ = mcx;
    let relation = read_opt_node(mcx)?;
    let options = read_uint_field()?;
    let relationOid = read_oid_field()?;
    Ok(dn::TableLikeClause {
        relation: relation,
        options: options,
        relationOid: relationOid,
    })
}

/// `_readIndexElem` (readfuncs.funcs.c).
fn read_index_elem<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::IndexElem<'mcx>> {
    let _ = mcx;
    let name = read_str(mcx)?;
    let expr = read_opt_node(mcx)?;
    let indexcolname = read_str(mcx)?;
    let collation = read_node_vec(mcx)?;
    let opclass = read_node_vec(mcx)?;
    let opclassopts = read_node_vec(mcx)?;
    let ordering = sort_by_dir_from(read_enum_field()?);
    let nulls_ordering = sort_by_nulls_from(read_enum_field()?);
    Ok(dn::IndexElem {
        name: name,
        expr: expr,
        indexcolname: indexcolname,
        collation: collation,
        opclass: opclass,
        opclassopts: opclassopts,
        ordering: ordering,
        nulls_ordering: nulls_ordering,
    })
}

/// `_readDefElem` (readfuncs.funcs.c).
fn read_def_elem<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::DefElem<'mcx>> {
    let _ = mcx;
    let defnamespace = read_str(mcx)?;
    let defname = read_str(mcx)?;
    let arg = read_opt_node(mcx)?;
    let defaction = def_elem_action_from(read_enum_field()?);
    let location = read_location_field()?;
    Ok(dn::DefElem {
        defnamespace: defnamespace,
        defname: defname,
        arg: arg,
        defaction: defaction,
        location: location,
    })
}

/// `_readPartitionElem` (readfuncs.funcs.c).
fn read_partition_elem<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::PartitionElem<'mcx>> {
    let _ = mcx;
    let name = read_str(mcx)?;
    let expr = read_opt_node(mcx)?;
    let collation = read_node_vec(mcx)?;
    let opclass = read_node_vec(mcx)?;
    let location = read_location_field()?;
    Ok(dn::PartitionElem {
        name: name,
        expr: expr,
        collation: collation,
        opclass: opclass,
        location: location,
    })
}

/// `_readPartitionSpec` (readfuncs.funcs.c).
fn read_partition_spec<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::PartitionSpec<'mcx>> {
    let _ = mcx;
    let strategy = partition_strategy_from(read_enum_field()?);
    let partParams = read_node_vec(mcx)?;
    let location = read_location_field()?;
    Ok(dn::PartitionSpec {
        strategy: strategy,
        partParams: partParams,
        location: location,
    })
}

/// `_readPartitionBoundSpec` (readfuncs.funcs.c).
fn read_partition_bound_spec<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::PartitionBoundSpec<'mcx>> {
    let _ = mcx;
    let strategy = read_char_field()? as i8;
    let is_default = read_bool_field()?;
    let modulus = read_int_field()?;
    let remainder = read_int_field()?;
    let listdatums = read_node_vec(mcx)?;
    let lowerdatums = read_node_vec(mcx)?;
    let upperdatums = read_node_vec(mcx)?;
    let location = read_location_field()?;
    Ok(dn::PartitionBoundSpec {
        strategy: strategy,
        is_default: is_default,
        modulus: modulus,
        remainder: remainder,
        listdatums: listdatums,
        lowerdatums: lowerdatums,
        upperdatums: upperdatums,
        location: location,
    })
}

/// `_readPartitionRangeDatum` (readfuncs.funcs.c).
fn read_partition_range_datum<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::PartitionRangeDatum<'mcx>> {
    let _ = mcx;
    let kind = partition_range_datum_kind_from(read_enum_field()?);
    let value = read_opt_node(mcx)?;
    let location = read_location_field()?;
    Ok(dn::PartitionRangeDatum {
        kind: kind,
        value: value,
        location: location,
    })
}

/// `_readPartitionCmd` (readfuncs.funcs.c).
fn read_partition_cmd<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::PartitionCmd<'mcx>> {
    let _ = mcx;
    let name = read_opt_node(mcx)?;
    let bound = read_opt_node(mcx)?;
    let concurrent = read_bool_field()?;
    Ok(dn::PartitionCmd {
        name: name,
        bound: bound,
        concurrent: concurrent,
    })
}

/// `_readReturnStmt` (readfuncs.funcs.c).
fn read_return_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::ReturnStmt<'mcx>> {
    let _ = mcx;
    let returnval = read_opt_node(mcx)?;
    Ok(dn::ReturnStmt {
        returnval: returnval,
    })
}

/// `_readPLAssignStmt` (readfuncs.funcs.c).
fn read_p_l_assign_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::PLAssignStmt<'mcx>> {
    let _ = mcx;
    let name = read_str(mcx)?;
    let indirection = read_node_vec(mcx)?;
    let nnames = read_int_field()?;
    let val = read_opt_node(mcx)?;
    let location = read_location_field()?;
    Ok(dn::PLAssignStmt {
        name: name,
        indirection: indirection,
        nnames: nnames,
        val: val,
        location: location,
    })
}

/// `_readCreateSchemaStmt` (readfuncs.funcs.c).
fn read_create_schema_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::CreateSchemaStmt<'mcx>> {
    let _ = mcx;
    let schemaname = read_str(mcx)?;
    let authrole = read_opt_node(mcx)?;
    let schemaElts = read_node_vec(mcx)?;
    let if_not_exists = read_bool_field()?;
    Ok(dn::CreateSchemaStmt {
        schemaname: schemaname,
        authrole: authrole,
        schemaElts: schemaElts,
        if_not_exists: if_not_exists,
    })
}

/// `_readAlterTableStmt` (readfuncs.funcs.c).
fn read_alter_table_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::AlterTableStmt<'mcx>> {
    let _ = mcx;
    let relation = read_opt_node(mcx)?;
    let cmds = read_node_vec(mcx)?;
    let objtype = object_type_from(read_enum_field()?);
    let missing_ok = read_bool_field()?;
    Ok(dn::AlterTableStmt {
        relation: relation,
        cmds: cmds,
        objtype: objtype,
        missing_ok: missing_ok,
    })
}

/// `_readAlterTableCmd` (readfuncs.funcs.c).
fn read_alter_table_cmd<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::AlterTableCmd<'mcx>> {
    let _ = mcx;
    let subtype = alter_table_type_from(read_enum_field()?);
    let name = read_str(mcx)?;
    let num = read_int_field()? as i16;
    let newowner = read_opt_node(mcx)?;
    let def = read_opt_node(mcx)?;
    let behavior = drop_behavior_from(read_enum_field()?);
    let missing_ok = read_bool_field()?;
    let recurse = read_bool_field()?;
    Ok(dn::AlterTableCmd {
        subtype: subtype,
        name: name,
        num: num,
        newowner: newowner,
        def: def,
        behavior: behavior,
        missing_ok: missing_ok,
        recurse: recurse,
    })
}

/// `_readATAlterConstraint` (readfuncs.funcs.c).
fn read_a_t_alter_constraint<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::ATAlterConstraint<'mcx>> {
    let _ = mcx;
    let conname = read_str(mcx)?;
    let alterEnforceability = read_bool_field()?;
    let is_enforced = read_bool_field()?;
    let alterDeferrability = read_bool_field()?;
    let deferrable = read_bool_field()?;
    let initdeferred = read_bool_field()?;
    let alterInheritability = read_bool_field()?;
    let noinherit = read_bool_field()?;
    Ok(dn::ATAlterConstraint {
        conname: conname,
        alterEnforceability: alterEnforceability,
        is_enforced: is_enforced,
        alterDeferrability: alterDeferrability,
        deferrable: deferrable,
        initdeferred: initdeferred,
        alterInheritability: alterInheritability,
        noinherit: noinherit,
    })
}

/// `_readReplicaIdentityStmt` (readfuncs.funcs.c).
fn read_replica_identity_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::ReplicaIdentityStmt<'mcx>> {
    let _ = mcx;
    let identity_type = read_char_field()? as i8;
    let name = read_str(mcx)?;
    Ok(dn::ReplicaIdentityStmt {
        identity_type: identity_type,
        name: name,
    })
}

/// `_readAlterCollationStmt` (readfuncs.funcs.c).
fn read_alter_collation_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::AlterCollationStmt<'mcx>> {
    let _ = mcx;
    let collname = read_node_vec(mcx)?;
    Ok(dn::AlterCollationStmt {
        collname: collname,
    })
}

/// `_readAlterDomainStmt` (readfuncs.funcs.c).
fn read_alter_domain_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::AlterDomainStmt<'mcx>> {
    let _ = mcx;
    let subtype = read_char_field()? as i8;
    let typeName = read_node_vec(mcx)?;
    let name = read_str(mcx)?;
    let def = read_opt_node(mcx)?;
    let behavior = drop_behavior_from(read_enum_field()?);
    let missing_ok = read_bool_field()?;
    Ok(dn::AlterDomainStmt {
        subtype: subtype,
        typeName: typeName,
        name: name,
        def: def,
        behavior: behavior,
        missing_ok: missing_ok,
    })
}

/// `_readGrantStmt` (readfuncs.funcs.c).
fn read_grant_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::GrantStmt<'mcx>> {
    let _ = mcx;
    let is_grant = read_bool_field()?;
    let targtype = grant_target_type_from(read_enum_field()?);
    let objtype = object_type_from(read_enum_field()?);
    let objects = read_node_vec(mcx)?;
    let privileges = read_node_vec(mcx)?;
    let grantees = read_node_vec(mcx)?;
    let grant_option = read_bool_field()?;
    let grantor = read_opt_node(mcx)?;
    let behavior = drop_behavior_from(read_enum_field()?);
    Ok(dn::GrantStmt {
        is_grant: is_grant,
        targtype: targtype,
        objtype: objtype,
        objects: objects,
        privileges: privileges,
        grantees: grantees,
        grant_option: grant_option,
        grantor: grantor,
        behavior: behavior,
    })
}

/// `_readObjectWithArgs` (readfuncs.funcs.c).
fn read_object_with_args<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::ObjectWithArgs<'mcx>> {
    let _ = mcx;
    let objname = read_node_vec(mcx)?;
    let objargs = read_node_vec(mcx)?;
    let objfuncargs = read_node_vec(mcx)?;
    let args_unspecified = read_bool_field()?;
    Ok(dn::ObjectWithArgs {
        objname: objname,
        objargs: objargs,
        objfuncargs: objfuncargs,
        args_unspecified: args_unspecified,
    })
}

/// `_readAccessPriv` (readfuncs.funcs.c).
fn read_access_priv<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::AccessPriv<'mcx>> {
    let _ = mcx;
    let priv_name = read_str(mcx)?;
    let cols = read_node_vec(mcx)?;
    Ok(dn::AccessPriv {
        priv_name: priv_name,
        cols: cols,
    })
}

/// `_readGrantRoleStmt` (readfuncs.funcs.c).
fn read_grant_role_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::GrantRoleStmt<'mcx>> {
    let _ = mcx;
    let granted_roles = read_node_vec(mcx)?;
    let grantee_roles = read_node_vec(mcx)?;
    let is_grant = read_bool_field()?;
    let opt = read_node_vec(mcx)?;
    let grantor = read_opt_node(mcx)?;
    let behavior = drop_behavior_from(read_enum_field()?);
    Ok(dn::GrantRoleStmt {
        granted_roles: granted_roles,
        grantee_roles: grantee_roles,
        is_grant: is_grant,
        opt: opt,
        grantor: grantor,
        behavior: behavior,
    })
}

/// `_readAlterDefaultPrivilegesStmt` (readfuncs.funcs.c).
fn read_alter_default_privileges_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::AlterDefaultPrivilegesStmt<'mcx>> {
    let _ = mcx;
    let options = read_node_vec(mcx)?;
    let action = read_opt_node(mcx)?;
    Ok(dn::AlterDefaultPrivilegesStmt {
        options: options,
        action: action,
    })
}

/// `_readCopyStmt` (readfuncs.funcs.c).
fn read_copy_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::CopyStmt<'mcx>> {
    let _ = mcx;
    let relation = read_opt_node(mcx)?;
    let query = read_opt_node(mcx)?;
    let attlist = read_node_vec(mcx)?;
    let is_from = read_bool_field()?;
    let is_program = read_bool_field()?;
    let filename = read_str(mcx)?;
    let options = read_node_vec(mcx)?;
    let where_clause = read_opt_node(mcx)?;
    Ok(dn::CopyStmt {
        relation: relation,
        query: query,
        attlist: attlist,
        is_from: is_from,
        is_program: is_program,
        filename: filename,
        options: options,
        where_clause: where_clause,
    })
}

/// `_readVariableSetStmt` (readfuncs.funcs.c).
fn read_variable_set_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::VariableSetStmt<'mcx>> {
    let _ = mcx;
    let kind = variable_set_kind_from(read_enum_field()?);
    let name = read_str(mcx)?;
    let args = read_node_vec(mcx)?;
    let jumble_args = read_bool_field()?;
    let is_local = read_bool_field()?;
    let location = read_location_field()?;
    Ok(dn::VariableSetStmt {
        kind: kind,
        name: name,
        args: args,
        jumble_args: jumble_args,
        is_local: is_local,
        location: location,
    })
}

/// `_readVariableShowStmt` (readfuncs.funcs.c).
fn read_variable_show_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::VariableShowStmt<'mcx>> {
    let _ = mcx;
    let name = read_str(mcx)?;
    Ok(dn::VariableShowStmt {
        name: name,
    })
}

/// `_readCreateStmt` (readfuncs.funcs.c).
fn read_create_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::CreateStmt<'mcx>> {
    let _ = mcx;
    let relation = read_opt_node(mcx)?;
    let tableElts = read_node_vec(mcx)?;
    let inhRelations = read_node_vec(mcx)?;
    let partbound = read_opt_node(mcx)?;
    let partspec = read_opt_node(mcx)?;
    let ofTypename = read_opt_node(mcx)?;
    let constraints = read_node_vec(mcx)?;
    let nnconstraints = read_node_vec(mcx)?;
    let options = read_node_vec(mcx)?;
    let oncommit = on_commit_action_from(read_enum_field()?);
    let tablespacename = read_str(mcx)?;
    let accessMethod = read_str(mcx)?;
    let if_not_exists = read_bool_field()?;
    Ok(dn::CreateStmt {
        relation: relation,
        tableElts: tableElts,
        inhRelations: inhRelations,
        partbound: partbound,
        partspec: partspec,
        ofTypename: ofTypename,
        constraints: constraints,
        nnconstraints: nnconstraints,
        options: options,
        oncommit: oncommit,
        tablespacename: tablespacename,
        accessMethod: accessMethod,
        if_not_exists: if_not_exists,
    })
}

/// `_readConstraint` (readfuncs.funcs.c).
fn read_constraint<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::Constraint<'mcx>> {
    let _ = mcx;
    let contype = constr_type_from(read_enum_field()?);
    let conname = read_str(mcx)?;
    let deferrable = read_bool_field()?;
    let initdeferred = read_bool_field()?;
    let is_enforced = read_bool_field()?;
    let skip_validation = read_bool_field()?;
    let initially_valid = read_bool_field()?;
    let is_no_inherit = read_bool_field()?;
    let raw_expr = read_opt_node(mcx)?;
    let cooked_expr = read_str(mcx)?;
    let generated_when = read_char_field()? as i8;
    let generated_kind = read_char_field()? as i8;
    let nulls_not_distinct = read_bool_field()?;
    let keys = read_node_vec(mcx)?;
    let without_overlaps = read_bool_field()?;
    let including = read_node_vec(mcx)?;
    let exclusions = read_node_vec(mcx)?;
    let options = read_node_vec(mcx)?;
    let indexname = read_str(mcx)?;
    let indexspace = read_str(mcx)?;
    let reset_default_tblspc = read_bool_field()?;
    let access_method = read_str(mcx)?;
    let where_clause = read_opt_node(mcx)?;
    let pktable = read_opt_node(mcx)?;
    let fk_attrs = read_node_vec(mcx)?;
    let pk_attrs = read_node_vec(mcx)?;
    let fk_with_period = read_bool_field()?;
    let pk_with_period = read_bool_field()?;
    let fk_matchtype = read_char_field()? as i8;
    let fk_upd_action = read_char_field()? as i8;
    let fk_del_action = read_char_field()? as i8;
    let fk_del_set_cols = read_node_vec(mcx)?;
    let old_conpfeqop = read_node_vec(mcx)?;
    let old_pktable_oid = read_oid_field()?;
    let location = read_location_field()?;
    Ok(dn::Constraint {
        contype: contype,
        conname: conname,
        deferrable: deferrable,
        initdeferred: initdeferred,
        is_enforced: is_enforced,
        skip_validation: skip_validation,
        initially_valid: initially_valid,
        is_no_inherit: is_no_inherit,
        raw_expr: raw_expr,
        cooked_expr: cooked_expr,
        generated_when: generated_when,
        generated_kind: generated_kind,
        nulls_not_distinct: nulls_not_distinct,
        keys: keys,
        without_overlaps: without_overlaps,
        including: including,
        exclusions: exclusions,
        options: options,
        indexname: indexname,
        indexspace: indexspace,
        reset_default_tblspc: reset_default_tblspc,
        access_method: access_method,
        where_clause: where_clause,
        pktable: pktable,
        fk_attrs: fk_attrs,
        pk_attrs: pk_attrs,
        fk_with_period: fk_with_period,
        pk_with_period: pk_with_period,
        fk_matchtype: fk_matchtype,
        fk_upd_action: fk_upd_action,
        fk_del_action: fk_del_action,
        fk_del_set_cols: fk_del_set_cols,
        old_conpfeqop: old_conpfeqop,
        old_pktable_oid: old_pktable_oid,
        location: location,
    })
}

/// `_readCreateTableSpaceStmt` (readfuncs.funcs.c).
fn read_create_table_space_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::CreateTableSpaceStmt<'mcx>> {
    let _ = mcx;
    let tablespacename = read_str(mcx)?;
    let owner = read_opt_node(mcx)?;
    let location = read_str(mcx)?;
    let options = read_node_vec(mcx)?;
    Ok(dn::CreateTableSpaceStmt {
        tablespacename: tablespacename,
        owner: owner,
        location: location,
        options: options,
    })
}

/// `_readDropTableSpaceStmt` (readfuncs.funcs.c).
fn read_drop_table_space_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::DropTableSpaceStmt<'mcx>> {
    let _ = mcx;
    let tablespacename = read_str(mcx)?;
    let missing_ok = read_bool_field()?;
    Ok(dn::DropTableSpaceStmt {
        tablespacename: tablespacename,
        missing_ok: missing_ok,
    })
}

/// `_readAlterTableSpaceOptionsStmt` (readfuncs.funcs.c).
fn read_alter_table_space_options_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::AlterTableSpaceOptionsStmt<'mcx>> {
    let _ = mcx;
    let tablespacename = read_str(mcx)?;
    let options = read_node_vec(mcx)?;
    let isReset = read_bool_field()?;
    Ok(dn::AlterTableSpaceOptionsStmt {
        tablespacename: tablespacename,
        options: options,
        isReset: isReset,
    })
}

/// `_readAlterTableMoveAllStmt` (readfuncs.funcs.c).
fn read_alter_table_move_all_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::AlterTableMoveAllStmt<'mcx>> {
    let _ = mcx;
    let orig_tablespacename = read_str(mcx)?;
    let objtype = object_type_from(read_enum_field()?);
    let roles = read_node_vec(mcx)?;
    let new_tablespacename = read_str(mcx)?;
    let nowait = read_bool_field()?;
    Ok(dn::AlterTableMoveAllStmt {
        orig_tablespacename: orig_tablespacename,
        objtype: objtype,
        roles: roles,
        new_tablespacename: new_tablespacename,
        nowait: nowait,
    })
}

/// `_readCreateExtensionStmt` (readfuncs.funcs.c).
fn read_create_extension_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::CreateExtensionStmt<'mcx>> {
    let _ = mcx;
    let extname = read_str(mcx)?;
    let if_not_exists = read_bool_field()?;
    let options = read_node_vec(mcx)?;
    Ok(dn::CreateExtensionStmt {
        extname: extname,
        if_not_exists: if_not_exists,
        options: options,
    })
}

/// `_readAlterExtensionStmt` (readfuncs.funcs.c).
fn read_alter_extension_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::AlterExtensionStmt<'mcx>> {
    let _ = mcx;
    let extname = read_str(mcx)?;
    let options = read_node_vec(mcx)?;
    Ok(dn::AlterExtensionStmt {
        extname: extname,
        options: options,
    })
}

/// `_readAlterExtensionContentsStmt` (readfuncs.funcs.c).
fn read_alter_extension_contents_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::AlterExtensionContentsStmt<'mcx>> {
    let _ = mcx;
    let extname = read_str(mcx)?;
    let action = read_int_field()?;
    let objtype = object_type_from(read_enum_field()?);
    let object = read_opt_node(mcx)?;
    Ok(dn::AlterExtensionContentsStmt {
        extname: extname,
        action: action,
        objtype: objtype,
        object: object,
    })
}

/// `_readCreateFdwStmt` (readfuncs.funcs.c).
fn read_create_fdw_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::CreateFdwStmt<'mcx>> {
    let _ = mcx;
    let fdwname = read_str(mcx)?;
    let func_options = read_node_vec(mcx)?;
    let options = read_node_vec(mcx)?;
    Ok(dn::CreateFdwStmt {
        fdwname: fdwname,
        func_options: func_options,
        options: options,
    })
}

/// `_readAlterFdwStmt` (readfuncs.funcs.c).
fn read_alter_fdw_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::AlterFdwStmt<'mcx>> {
    let _ = mcx;
    let fdwname = read_str(mcx)?;
    let func_options = read_node_vec(mcx)?;
    let options = read_node_vec(mcx)?;
    Ok(dn::AlterFdwStmt {
        fdwname: fdwname,
        func_options: func_options,
        options: options,
    })
}

/// `_readCreateForeignServerStmt` (readfuncs.funcs.c).
fn read_create_foreign_server_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::CreateForeignServerStmt<'mcx>> {
    let _ = mcx;
    let servername = read_str(mcx)?;
    let servertype = read_str(mcx)?;
    let version = read_str(mcx)?;
    let fdwname = read_str(mcx)?;
    let if_not_exists = read_bool_field()?;
    let options = read_node_vec(mcx)?;
    Ok(dn::CreateForeignServerStmt {
        servername: servername,
        servertype: servertype,
        version: version,
        fdwname: fdwname,
        if_not_exists: if_not_exists,
        options: options,
    })
}

/// `_readAlterForeignServerStmt` (readfuncs.funcs.c).
fn read_alter_foreign_server_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::AlterForeignServerStmt<'mcx>> {
    let _ = mcx;
    let servername = read_str(mcx)?;
    let version = read_str(mcx)?;
    let options = read_node_vec(mcx)?;
    let has_version = read_bool_field()?;
    Ok(dn::AlterForeignServerStmt {
        servername: servername,
        version: version,
        options: options,
        has_version: has_version,
    })
}

/// `_readCreateForeignTableStmt` (readfuncs.funcs.c).
fn read_create_foreign_table_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::CreateForeignTableStmt<'mcx>> {
    let _ = mcx;
    let base_relation = read_opt_node(mcx)?;
    let base_tableElts = read_node_vec(mcx)?;
    let base_inhRelations = read_node_vec(mcx)?;
    let base_partbound = read_opt_node(mcx)?;
    let base_partspec = read_opt_node(mcx)?;
    let base_ofTypename = read_opt_node(mcx)?;
    let base_constraints = read_node_vec(mcx)?;
    let base_nnconstraints = read_node_vec(mcx)?;
    let base_options = read_node_vec(mcx)?;
    let base_oncommit = on_commit_action_from(read_enum_field()?);
    let base_tablespacename = read_str(mcx)?;
    let base_accessMethod = read_str(mcx)?;
    let base_if_not_exists = read_bool_field()?;
    let servername = read_str(mcx)?;
    let options = read_node_vec(mcx)?;
    let base = mcx::alloc_in(mcx, dn::CreateStmt {
        relation: base_relation,
        tableElts: base_tableElts,
        inhRelations: base_inhRelations,
        partbound: base_partbound,
        partspec: base_partspec,
        ofTypename: base_ofTypename,
        constraints: base_constraints,
        nnconstraints: base_nnconstraints,
        options: base_options,
        oncommit: base_oncommit,
        tablespacename: base_tablespacename,
        accessMethod: base_accessMethod,
        if_not_exists: base_if_not_exists,
    })?;
    Ok(dn::CreateForeignTableStmt {
        base,
        servername: servername,
        options: options,
    })
}

/// `_readCreateUserMappingStmt` (readfuncs.funcs.c).
fn read_create_user_mapping_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::CreateUserMappingStmt<'mcx>> {
    let _ = mcx;
    let user = read_opt_node(mcx)?;
    let servername = read_str(mcx)?;
    let if_not_exists = read_bool_field()?;
    let options = read_node_vec(mcx)?;
    Ok(dn::CreateUserMappingStmt {
        user: user,
        servername: servername,
        if_not_exists: if_not_exists,
        options: options,
    })
}

/// `_readAlterUserMappingStmt` (readfuncs.funcs.c).
fn read_alter_user_mapping_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::AlterUserMappingStmt<'mcx>> {
    let _ = mcx;
    let user = read_opt_node(mcx)?;
    let servername = read_str(mcx)?;
    let options = read_node_vec(mcx)?;
    Ok(dn::AlterUserMappingStmt {
        user: user,
        servername: servername,
        options: options,
    })
}

/// `_readDropUserMappingStmt` (readfuncs.funcs.c).
fn read_drop_user_mapping_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::DropUserMappingStmt<'mcx>> {
    let _ = mcx;
    let user = read_opt_node(mcx)?;
    let servername = read_str(mcx)?;
    let missing_ok = read_bool_field()?;
    Ok(dn::DropUserMappingStmt {
        user: user,
        servername: servername,
        missing_ok: missing_ok,
    })
}

/// `_readImportForeignSchemaStmt` (readfuncs.funcs.c).
fn read_import_foreign_schema_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::ImportForeignSchemaStmt<'mcx>> {
    let _ = mcx;
    let server_name = read_str(mcx)?;
    let remote_schema = read_str(mcx)?;
    let local_schema = read_str(mcx)?;
    let list_type = import_foreign_schema_type_from(read_enum_field()?);
    let table_list = read_node_vec(mcx)?;
    let options = read_node_vec(mcx)?;
    Ok(dn::ImportForeignSchemaStmt {
        server_name: server_name,
        remote_schema: remote_schema,
        local_schema: local_schema,
        list_type: list_type,
        table_list: table_list,
        options: options,
    })
}

/// `_readCreatePolicyStmt` (readfuncs.funcs.c).
fn read_create_policy_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::CreatePolicyStmt<'mcx>> {
    let _ = mcx;
    let policy_name = read_str(mcx)?;
    let table = read_opt_node(mcx)?;
    let cmd_name = read_str(mcx)?;
    let permissive = read_bool_field()?;
    let roles = read_node_vec(mcx)?;
    let qual = read_opt_node(mcx)?;
    let with_check = read_opt_node(mcx)?;
    Ok(dn::CreatePolicyStmt {
        policy_name: policy_name,
        table: table,
        cmd_name: cmd_name,
        permissive: permissive,
        roles: roles,
        qual: qual,
        with_check: with_check,
    })
}

/// `_readAlterPolicyStmt` (readfuncs.funcs.c).
fn read_alter_policy_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::AlterPolicyStmt<'mcx>> {
    let _ = mcx;
    let policy_name = read_str(mcx)?;
    let table = read_opt_node(mcx)?;
    let roles = read_node_vec(mcx)?;
    let qual = read_opt_node(mcx)?;
    let with_check = read_opt_node(mcx)?;
    Ok(dn::AlterPolicyStmt {
        policy_name: policy_name,
        table: table,
        roles: roles,
        qual: qual,
        with_check: with_check,
    })
}

/// `_readCreateAmStmt` (readfuncs.funcs.c).
fn read_create_am_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::CreateAmStmt<'mcx>> {
    let _ = mcx;
    let amname = read_str(mcx)?;
    let handler_name = read_node_vec(mcx)?;
    let amtype = read_char_field()? as i8;
    Ok(dn::CreateAmStmt {
        amname: amname,
        handler_name: handler_name,
        amtype: amtype,
    })
}

/// `_readCreateTrigStmt` (readfuncs.funcs.c).
fn read_create_trig_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::CreateTrigStmt<'mcx>> {
    let _ = mcx;
    let replace = read_bool_field()?;
    let isconstraint = read_bool_field()?;
    let trigname = read_str(mcx)?;
    let relation = read_opt_node(mcx)?;
    let funcname = read_node_vec(mcx)?;
    let args = read_node_vec(mcx)?;
    let row = read_bool_field()?;
    let timing = read_int_field()? as i16;
    let events = read_int_field()? as i16;
    let columns = read_node_vec(mcx)?;
    let whenClause = read_opt_node(mcx)?;
    let transitionRels = read_node_vec(mcx)?;
    let deferrable = read_bool_field()?;
    let initdeferred = read_bool_field()?;
    let constrrel = read_opt_node(mcx)?;
    Ok(dn::CreateTrigStmt {
        replace: replace,
        isconstraint: isconstraint,
        trigname: trigname,
        relation: relation,
        funcname: funcname,
        args: args,
        row: row,
        timing: timing,
        events: events,
        columns: columns,
        whenClause: whenClause,
        transitionRels: transitionRels,
        deferrable: deferrable,
        initdeferred: initdeferred,
        constrrel: constrrel,
    })
}

/// `_readCreateEventTrigStmt` (readfuncs.funcs.c).
fn read_create_event_trig_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::CreateEventTrigStmt<'mcx>> {
    let _ = mcx;
    let trigname = read_str(mcx)?;
    let eventname = read_str(mcx)?;
    let whenclause = read_node_vec(mcx)?;
    let funcname = read_node_vec(mcx)?;
    Ok(dn::CreateEventTrigStmt {
        trigname: trigname,
        eventname: eventname,
        whenclause: whenclause,
        funcname: funcname,
    })
}

/// `_readAlterEventTrigStmt` (readfuncs.funcs.c).
fn read_alter_event_trig_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::AlterEventTrigStmt<'mcx>> {
    let _ = mcx;
    let trigname = read_str(mcx)?;
    let tgenabled = read_char_field()? as i8;
    Ok(dn::AlterEventTrigStmt {
        trigname: trigname,
        tgenabled: tgenabled,
    })
}

/// `_readCreatePLangStmt` (readfuncs.funcs.c).
fn read_create_p_lang_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::CreatePLangStmt<'mcx>> {
    let _ = mcx;
    let replace = read_bool_field()?;
    let plname = read_str(mcx)?;
    let plhandler = read_node_vec(mcx)?;
    let plinline = read_node_vec(mcx)?;
    let plvalidator = read_node_vec(mcx)?;
    let pltrusted = read_bool_field()?;
    Ok(dn::CreatePLangStmt {
        replace: replace,
        plname: plname,
        plhandler: plhandler,
        plinline: plinline,
        plvalidator: plvalidator,
        pltrusted: pltrusted,
    })
}

/// `_readCreateRoleStmt` (readfuncs.funcs.c).
fn read_create_role_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::CreateRoleStmt<'mcx>> {
    let _ = mcx;
    let stmt_type = role_stmt_type_from(read_enum_field()?);
    let role = read_str(mcx)?;
    let options = read_node_vec(mcx)?;
    Ok(dn::CreateRoleStmt {
        stmt_type: stmt_type,
        role: role,
        options: options,
    })
}

/// `_readAlterRoleStmt` (readfuncs.funcs.c).
fn read_alter_role_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::AlterRoleStmt<'mcx>> {
    let _ = mcx;
    let role = read_opt_node(mcx)?;
    let options = read_node_vec(mcx)?;
    let action = read_int_field()?;
    Ok(dn::AlterRoleStmt {
        role: role,
        options: options,
        action: action,
    })
}

/// `_readAlterRoleSetStmt` (readfuncs.funcs.c).
fn read_alter_role_set_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::AlterRoleSetStmt<'mcx>> {
    let _ = mcx;
    let role = read_opt_node(mcx)?;
    let database = read_str(mcx)?;
    let setstmt = read_opt_node(mcx)?;
    Ok(dn::AlterRoleSetStmt {
        role: role,
        database: database,
        setstmt: setstmt,
    })
}

/// `_readDropRoleStmt` (readfuncs.funcs.c).
fn read_drop_role_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::DropRoleStmt<'mcx>> {
    let _ = mcx;
    let roles = read_node_vec(mcx)?;
    let missing_ok = read_bool_field()?;
    Ok(dn::DropRoleStmt {
        roles: roles,
        missing_ok: missing_ok,
    })
}

/// `_readCreateSeqStmt` (readfuncs.funcs.c).
fn read_create_seq_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::CreateSeqStmt<'mcx>> {
    let _ = mcx;
    let sequence = read_opt_node(mcx)?;
    let options = read_node_vec(mcx)?;
    let ownerId = read_oid_field()?;
    let for_identity = read_bool_field()?;
    let if_not_exists = read_bool_field()?;
    Ok(dn::CreateSeqStmt {
        sequence: sequence,
        options: options,
        ownerId: ownerId,
        for_identity: for_identity,
        if_not_exists: if_not_exists,
    })
}

/// `_readAlterSeqStmt` (readfuncs.funcs.c).
fn read_alter_seq_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::AlterSeqStmt<'mcx>> {
    let _ = mcx;
    let sequence = read_opt_node(mcx)?;
    let options = read_node_vec(mcx)?;
    let for_identity = read_bool_field()?;
    let missing_ok = read_bool_field()?;
    Ok(dn::AlterSeqStmt {
        sequence: sequence,
        options: options,
        for_identity: for_identity,
        missing_ok: missing_ok,
    })
}

/// `_readDefineStmt` (readfuncs.funcs.c).
fn read_define_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::DefineStmt<'mcx>> {
    let _ = mcx;
    let kind = object_type_from(read_enum_field()?);
    let oldstyle = read_bool_field()?;
    let defnames = read_node_vec(mcx)?;
    let args = read_node_vec(mcx)?;
    let definition = read_node_vec(mcx)?;
    let if_not_exists = read_bool_field()?;
    let replace = read_bool_field()?;
    Ok(dn::DefineStmt {
        kind: kind,
        oldstyle: oldstyle,
        defnames: defnames,
        args: args,
        definition: definition,
        if_not_exists: if_not_exists,
        replace: replace,
    })
}

/// `_readCreateDomainStmt` (readfuncs.funcs.c).
fn read_create_domain_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::CreateDomainStmt<'mcx>> {
    let _ = mcx;
    let domainname = read_node_vec(mcx)?;
    let typeName = read_opt_node(mcx)?;
    let collClause = read_opt_node(mcx)?;
    let constraints = read_node_vec(mcx)?;
    Ok(dn::CreateDomainStmt {
        domainname: domainname,
        typeName: typeName,
        collClause: collClause,
        constraints: constraints,
    })
}

/// `_readCreateOpClassStmt` (readfuncs.funcs.c).
fn read_create_op_class_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::CreateOpClassStmt<'mcx>> {
    let _ = mcx;
    let opclassname = read_node_vec(mcx)?;
    let opfamilyname = read_node_vec(mcx)?;
    let amname = read_str(mcx)?;
    let datatype = read_opt_node(mcx)?;
    let items = read_node_vec(mcx)?;
    let isDefault = read_bool_field()?;
    Ok(dn::CreateOpClassStmt {
        opclassname: opclassname,
        opfamilyname: opfamilyname,
        amname: amname,
        datatype: datatype,
        items: items,
        isDefault: isDefault,
    })
}

/// `_readCreateOpClassItem` (readfuncs.funcs.c).
fn read_create_op_class_item<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::CreateOpClassItem<'mcx>> {
    let _ = mcx;
    let itemtype = read_int_field()?;
    let name = read_opt_node(mcx)?;
    let number = read_int_field()?;
    let order_family = read_node_vec(mcx)?;
    let class_args = read_node_vec(mcx)?;
    let storedtype = read_opt_node(mcx)?;
    Ok(dn::CreateOpClassItem {
        itemtype: itemtype,
        name: name,
        number: number,
        order_family: order_family,
        class_args: class_args,
        storedtype: storedtype,
    })
}

/// `_readCreateOpFamilyStmt` (readfuncs.funcs.c).
fn read_create_op_family_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::CreateOpFamilyStmt<'mcx>> {
    let _ = mcx;
    let opfamilyname = read_node_vec(mcx)?;
    let amname = read_str(mcx)?;
    Ok(dn::CreateOpFamilyStmt {
        opfamilyname: opfamilyname,
        amname: amname,
    })
}

/// `_readAlterOpFamilyStmt` (readfuncs.funcs.c).
fn read_alter_op_family_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::AlterOpFamilyStmt<'mcx>> {
    let _ = mcx;
    let opfamilyname = read_node_vec(mcx)?;
    let amname = read_str(mcx)?;
    let isDrop = read_bool_field()?;
    let items = read_node_vec(mcx)?;
    Ok(dn::AlterOpFamilyStmt {
        opfamilyname: opfamilyname,
        amname: amname,
        isDrop: isDrop,
        items: items,
    })
}

/// `_readDropStmt` (readfuncs.funcs.c).
fn read_drop_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::DropStmt<'mcx>> {
    let _ = mcx;
    let objects = read_node_vec(mcx)?;
    let removeType = object_type_from(read_enum_field()?);
    let behavior = drop_behavior_from(read_enum_field()?);
    let missing_ok = read_bool_field()?;
    let concurrent = read_bool_field()?;
    Ok(dn::DropStmt {
        objects: objects,
        removeType: removeType,
        behavior: behavior,
        missing_ok: missing_ok,
        concurrent: concurrent,
    })
}

/// `_readTruncateStmt` (readfuncs.funcs.c).
fn read_truncate_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::TruncateStmt<'mcx>> {
    let _ = mcx;
    let relations = read_node_vec(mcx)?;
    let restart_seqs = read_bool_field()?;
    let behavior = drop_behavior_from(read_enum_field()?);
    Ok(dn::TruncateStmt {
        relations: relations,
        restart_seqs: restart_seqs,
        behavior: behavior,
    })
}

/// `_readCommentStmt` (readfuncs.funcs.c).
fn read_comment_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::CommentStmt<'mcx>> {
    let _ = mcx;
    let objtype = object_type_from(read_enum_field()?);
    let object = read_opt_node(mcx)?;
    let comment = read_str(mcx)?;
    Ok(dn::CommentStmt {
        objtype: objtype,
        object: object,
        comment: comment,
    })
}

/// `_readSecLabelStmt` (readfuncs.funcs.c).
fn read_sec_label_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::SecLabelStmt<'mcx>> {
    let _ = mcx;
    let objtype = object_type_from(read_enum_field()?);
    let object = read_opt_node(mcx)?;
    let provider = read_str(mcx)?;
    let label = read_str(mcx)?;
    Ok(dn::SecLabelStmt {
        objtype: objtype,
        object: object,
        provider: provider,
        label: label,
    })
}

/// `_readDeclareCursorStmt` (readfuncs.funcs.c).
fn read_declare_cursor_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::DeclareCursorStmt<'mcx>> {
    let _ = mcx;
    let portalname = read_str(mcx)?;
    let options = read_int_field()?;
    let query = read_opt_node(mcx)?;
    Ok(dn::DeclareCursorStmt {
        portalname: portalname,
        options: options,
        query: query,
    })
}

/// `_readClosePortalStmt` (readfuncs.funcs.c).
fn read_close_portal_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::ClosePortalStmt<'mcx>> {
    let _ = mcx;
    let portalname = read_str(mcx)?;
    Ok(dn::ClosePortalStmt {
        portalname: portalname,
    })
}

/// `_readFetchStmt` (readfuncs.funcs.c).
fn read_fetch_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::FetchStmt<'mcx>> {
    let _ = mcx;
    let direction = fetch_direction_from(read_enum_field()?);
    let how_many = read_int64_field()?;
    let portalname = read_str(mcx)?;
    let ismove = read_bool_field()?;
    Ok(dn::FetchStmt {
        direction: direction,
        how_many: how_many,
        portalname: portalname,
        ismove: ismove,
    })
}

/// `_readIndexStmt` (readfuncs.funcs.c).
fn read_index_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::IndexStmt<'mcx>> {
    let _ = mcx;
    let idxname = read_str(mcx)?;
    let relation = read_opt_node(mcx)?;
    let accessMethod = read_str(mcx)?;
    let tableSpace = read_str(mcx)?;
    let indexParams = read_node_vec(mcx)?;
    let indexIncludingParams = read_node_vec(mcx)?;
    let options = read_node_vec(mcx)?;
    let whereClause = read_opt_node(mcx)?;
    let excludeOpNames = read_node_vec(mcx)?;
    let idxcomment = read_str(mcx)?;
    let indexOid = read_oid_field()?;
    let oldNumber = read_oid_field()?;
    let oldCreateSubid = read_uint_field()?;
    let oldFirstRelfilelocatorSubid = read_uint_field()?;
    let unique = read_bool_field()?;
    let nulls_not_distinct = read_bool_field()?;
    let primary = read_bool_field()?;
    let isconstraint = read_bool_field()?;
    let iswithoutoverlaps = read_bool_field()?;
    let deferrable = read_bool_field()?;
    let initdeferred = read_bool_field()?;
    let transformed = read_bool_field()?;
    let concurrent = read_bool_field()?;
    let if_not_exists = read_bool_field()?;
    let reset_default_tblspc = read_bool_field()?;
    Ok(dn::IndexStmt {
        idxname: idxname,
        relation: relation,
        accessMethod: accessMethod,
        tableSpace: tableSpace,
        indexParams: indexParams,
        indexIncludingParams: indexIncludingParams,
        options: options,
        whereClause: whereClause,
        excludeOpNames: excludeOpNames,
        idxcomment: idxcomment,
        indexOid: indexOid,
        oldNumber: oldNumber,
        oldCreateSubid: oldCreateSubid,
        oldFirstRelfilelocatorSubid: oldFirstRelfilelocatorSubid,
        unique: unique,
        nulls_not_distinct: nulls_not_distinct,
        primary: primary,
        isconstraint: isconstraint,
        iswithoutoverlaps: iswithoutoverlaps,
        deferrable: deferrable,
        initdeferred: initdeferred,
        transformed: transformed,
        concurrent: concurrent,
        if_not_exists: if_not_exists,
        reset_default_tblspc: reset_default_tblspc,
    })
}

/// `_readCreateStatsStmt` (readfuncs.funcs.c).
fn read_create_stats_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::CreateStatsStmt<'mcx>> {
    let _ = mcx;
    let defnames = read_node_vec(mcx)?;
    let stat_types = read_node_vec(mcx)?;
    let exprs = read_node_vec(mcx)?;
    let relations = read_node_vec(mcx)?;
    let stxcomment = read_str(mcx)?;
    let transformed = read_bool_field()?;
    let if_not_exists = read_bool_field()?;
    Ok(dn::CreateStatsStmt {
        defnames: defnames,
        stat_types: stat_types,
        exprs: exprs,
        relations: relations,
        stxcomment: stxcomment,
        transformed: transformed,
        if_not_exists: if_not_exists,
    })
}

/// `_readStatsElem` (readfuncs.funcs.c).
fn read_stats_elem<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::StatsElem<'mcx>> {
    let _ = mcx;
    let name = read_str(mcx)?;
    let expr = read_opt_node(mcx)?;
    Ok(dn::StatsElem {
        name: name,
        expr: expr,
    })
}

/// `_readAlterStatsStmt` (readfuncs.funcs.c).
fn read_alter_stats_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::AlterStatsStmt<'mcx>> {
    let _ = mcx;
    let defnames = read_node_vec(mcx)?;
    let stxstattarget = read_opt_node(mcx)?;
    let missing_ok = read_bool_field()?;
    Ok(dn::AlterStatsStmt {
        defnames: defnames,
        stxstattarget: stxstattarget,
        missing_ok: missing_ok,
    })
}

/// `_readCreateFunctionStmt` (readfuncs.funcs.c).
fn read_create_function_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::CreateFunctionStmt<'mcx>> {
    let _ = mcx;
    let is_procedure = read_bool_field()?;
    let replace = read_bool_field()?;
    let funcname = read_node_vec(mcx)?;
    let parameters = read_node_vec(mcx)?;
    let returnType = read_opt_node(mcx)?;
    let options = read_node_vec(mcx)?;
    let sql_body = read_opt_node(mcx)?;
    Ok(dn::CreateFunctionStmt {
        is_procedure: is_procedure,
        replace: replace,
        funcname: funcname,
        parameters: parameters,
        returnType: returnType,
        options: options,
        sql_body: sql_body,
    })
}

/// `_readFunctionParameter` (readfuncs.funcs.c).
fn read_function_parameter<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::FunctionParameter<'mcx>> {
    let _ = mcx;
    let name = read_str(mcx)?;
    let argType = read_opt_node(mcx)?;
    let mode = function_parameter_mode_from(read_enum_field()?);
    let defexpr = read_opt_node(mcx)?;
    let location = read_location_field()?;
    Ok(dn::FunctionParameter {
        name: name,
        argType: argType,
        mode: mode,
        defexpr: defexpr,
        location: location,
    })
}

/// `_readAlterFunctionStmt` (readfuncs.funcs.c).
fn read_alter_function_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::AlterFunctionStmt<'mcx>> {
    let _ = mcx;
    let objtype = object_type_from(read_enum_field()?);
    let func = read_opt_node(mcx)?;
    let actions = read_node_vec(mcx)?;
    Ok(dn::AlterFunctionStmt {
        objtype: objtype,
        func: func,
        actions: actions,
    })
}

/// `_readDoStmt` (readfuncs.funcs.c).
fn read_do_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::DoStmt<'mcx>> {
    let _ = mcx;
    let args = read_node_vec(mcx)?;
    Ok(dn::DoStmt {
        args: args,
    })
}

/// `_readCallStmt` (readfuncs.funcs.c).
fn read_call_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::CallStmt<'mcx>> {
    let _ = mcx;
    let funccall = read_opt_node(mcx)?;
    let funcexpr = read_opt_node(mcx)?;
    let outargs = read_node_vec(mcx)?;
    Ok(dn::CallStmt {
        funccall: funccall,
        funcexpr: funcexpr,
        outargs: outargs,
    })
}

/// `_readRenameStmt` (readfuncs.funcs.c).
fn read_rename_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::RenameStmt<'mcx>> {
    let _ = mcx;
    let renameType = object_type_from(read_enum_field()?);
    let relationType = object_type_from(read_enum_field()?);
    let relation = read_opt_node(mcx)?;
    let object = read_opt_node(mcx)?;
    let subname = read_str(mcx)?;
    let newname = read_str(mcx)?;
    let behavior = drop_behavior_from(read_enum_field()?);
    let missing_ok = read_bool_field()?;
    Ok(dn::RenameStmt {
        renameType: renameType,
        relationType: relationType,
        relation: relation,
        object: object,
        subname: subname,
        newname: newname,
        behavior: behavior,
        missing_ok: missing_ok,
    })
}

/// `_readAlterObjectDependsStmt` (readfuncs.funcs.c).
fn read_alter_object_depends_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::AlterObjectDependsStmt<'mcx>> {
    let _ = mcx;
    let objectType = object_type_from(read_enum_field()?);
    let relation = read_opt_node(mcx)?;
    let object = read_opt_node(mcx)?;
    let extname = read_opt_node(mcx)?;
    let remove = read_bool_field()?;
    Ok(dn::AlterObjectDependsStmt {
        objectType: objectType,
        relation: relation,
        object: object,
        extname: extname,
        remove: remove,
    })
}

/// `_readAlterObjectSchemaStmt` (readfuncs.funcs.c).
fn read_alter_object_schema_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::AlterObjectSchemaStmt<'mcx>> {
    let _ = mcx;
    let objectType = object_type_from(read_enum_field()?);
    let relation = read_opt_node(mcx)?;
    let object = read_opt_node(mcx)?;
    let newschema = read_str(mcx)?;
    let missing_ok = read_bool_field()?;
    Ok(dn::AlterObjectSchemaStmt {
        objectType: objectType,
        relation: relation,
        object: object,
        newschema: newschema,
        missing_ok: missing_ok,
    })
}

/// `_readAlterOwnerStmt` (readfuncs.funcs.c).
fn read_alter_owner_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::AlterOwnerStmt<'mcx>> {
    let _ = mcx;
    let objectType = object_type_from(read_enum_field()?);
    let relation = read_opt_node(mcx)?;
    let object = read_opt_node(mcx)?;
    let newowner = read_opt_node(mcx)?;
    Ok(dn::AlterOwnerStmt {
        objectType: objectType,
        relation: relation,
        object: object,
        newowner: newowner,
    })
}

/// `_readAlterOperatorStmt` (readfuncs.funcs.c).
fn read_alter_operator_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::AlterOperatorStmt<'mcx>> {
    let _ = mcx;
    let opername = read_opt_node(mcx)?;
    let options = read_node_vec(mcx)?;
    Ok(dn::AlterOperatorStmt {
        opername: opername,
        options: options,
    })
}

/// `_readAlterTypeStmt` (readfuncs.funcs.c).
fn read_alter_type_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::AlterTypeStmt<'mcx>> {
    let _ = mcx;
    let typeName = read_node_vec(mcx)?;
    let options = read_node_vec(mcx)?;
    Ok(dn::AlterTypeStmt {
        typeName: typeName,
        options: options,
    })
}

/// `_readRuleStmt` (readfuncs.funcs.c).
fn read_rule_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::RuleStmt<'mcx>> {
    let _ = mcx;
    let relation = read_opt_node(mcx)?;
    let rulename = read_str(mcx)?;
    let where_clause = read_opt_node(mcx)?;
    let event = cmd_type_from(read_enum_field()?);
    let instead = read_bool_field()?;
    let actions = read_node_vec(mcx)?;
    let replace = read_bool_field()?;
    Ok(dn::RuleStmt {
        relation: relation,
        rulename: rulename,
        where_clause: where_clause,
        event: event,
        instead: instead,
        actions: actions,
        replace: replace,
    })
}

/// `_readNotifyStmt` (readfuncs.funcs.c).
fn read_notify_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::NotifyStmt<'mcx>> {
    let _ = mcx;
    let conditionname = read_str(mcx)?;
    let payload = read_str(mcx)?;
    Ok(dn::NotifyStmt {
        conditionname: conditionname,
        payload: payload,
    })
}

/// `_readListenStmt` (readfuncs.funcs.c).
fn read_listen_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::ListenStmt<'mcx>> {
    let _ = mcx;
    let conditionname = read_str(mcx)?;
    Ok(dn::ListenStmt {
        conditionname: conditionname,
    })
}

/// `_readUnlistenStmt` (readfuncs.funcs.c).
fn read_unlisten_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::UnlistenStmt<'mcx>> {
    let _ = mcx;
    let conditionname = read_str(mcx)?;
    Ok(dn::UnlistenStmt {
        conditionname: conditionname,
    })
}

/// `_readTransactionStmt` (readfuncs.funcs.c).
fn read_transaction_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::TransactionStmt<'mcx>> {
    let _ = mcx;
    let kind = transaction_stmt_kind_from(read_enum_field()?);
    let options = read_node_vec(mcx)?;
    let savepoint_name = read_str(mcx)?;
    let gid = read_str(mcx)?;
    let chain = read_bool_field()?;
    let location = read_location_field()?;
    Ok(dn::TransactionStmt {
        kind: kind,
        options: options,
        savepoint_name: savepoint_name,
        gid: gid,
        chain: chain,
        location: location,
    })
}

/// `_readCompositeTypeStmt` (readfuncs.funcs.c).
fn read_composite_type_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::CompositeTypeStmt<'mcx>> {
    let _ = mcx;
    let typevar = read_opt_node(mcx)?;
    let coldeflist = read_node_vec(mcx)?;
    Ok(dn::CompositeTypeStmt {
        typevar: typevar,
        coldeflist: coldeflist,
    })
}

/// `_readCreateEnumStmt` (readfuncs.funcs.c).
fn read_create_enum_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::CreateEnumStmt<'mcx>> {
    let _ = mcx;
    let typeName = read_node_vec(mcx)?;
    let vals = read_node_vec(mcx)?;
    Ok(dn::CreateEnumStmt {
        typeName: typeName,
        vals: vals,
    })
}

/// `_readCreateRangeStmt` (readfuncs.funcs.c).
fn read_create_range_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::CreateRangeStmt<'mcx>> {
    let _ = mcx;
    let typeName = read_node_vec(mcx)?;
    let params = read_node_vec(mcx)?;
    Ok(dn::CreateRangeStmt {
        typeName: typeName,
        params: params,
    })
}

/// `_readAlterEnumStmt` (readfuncs.funcs.c).
fn read_alter_enum_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::AlterEnumStmt<'mcx>> {
    let _ = mcx;
    let typeName = read_node_vec(mcx)?;
    let oldVal = read_str(mcx)?;
    let newVal = read_str(mcx)?;
    let newValNeighbor = read_str(mcx)?;
    let newValIsAfter = read_bool_field()?;
    let skipIfNewValExists = read_bool_field()?;
    Ok(dn::AlterEnumStmt {
        typeName: typeName,
        oldVal: oldVal,
        newVal: newVal,
        newValNeighbor: newValNeighbor,
        newValIsAfter: newValIsAfter,
        skipIfNewValExists: skipIfNewValExists,
    })
}

/// `_readViewStmt` (readfuncs.funcs.c).
fn read_view_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::ViewStmt<'mcx>> {
    let _ = mcx;
    let view = read_opt_node(mcx)?;
    let aliases = read_node_vec(mcx)?;
    let query = read_opt_node(mcx)?;
    let replace = read_bool_field()?;
    let options = read_node_vec(mcx)?;
    let withCheckOption = view_check_option_from(read_enum_field()?);
    Ok(dn::ViewStmt {
        view: view,
        aliases: aliases,
        query: query,
        replace: replace,
        options: options,
        withCheckOption: withCheckOption,
    })
}

/// `_readLoadStmt` (readfuncs.funcs.c).
fn read_load_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::LoadStmt<'mcx>> {
    let _ = mcx;
    let filename = read_str(mcx)?;
    Ok(dn::LoadStmt {
        filename: filename,
    })
}

/// `_readCreatedbStmt` (readfuncs.funcs.c).
fn read_createdb_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::CreatedbStmt<'mcx>> {
    let _ = mcx;
    let dbname = read_str(mcx)?;
    let options = read_node_vec(mcx)?;
    Ok(dn::CreatedbStmt {
        dbname: dbname,
        options: options,
    })
}

/// `_readAlterDatabaseStmt` (readfuncs.funcs.c).
fn read_alter_database_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::AlterDatabaseStmt<'mcx>> {
    let _ = mcx;
    let dbname = read_str(mcx)?;
    let options = read_node_vec(mcx)?;
    Ok(dn::AlterDatabaseStmt {
        dbname: dbname,
        options: options,
    })
}

/// `_readAlterDatabaseRefreshCollStmt` (readfuncs.funcs.c).
fn read_alter_database_refresh_coll_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::AlterDatabaseRefreshCollStmt<'mcx>> {
    let _ = mcx;
    let dbname = read_str(mcx)?;
    Ok(dn::AlterDatabaseRefreshCollStmt {
        dbname: dbname,
    })
}

/// `_readAlterDatabaseSetStmt` (readfuncs.funcs.c).
fn read_alter_database_set_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::AlterDatabaseSetStmt<'mcx>> {
    let _ = mcx;
    let dbname = read_str(mcx)?;
    let setstmt = read_opt_node(mcx)?;
    Ok(dn::AlterDatabaseSetStmt {
        dbname: dbname,
        setstmt: setstmt,
    })
}

/// `_readDropdbStmt` (readfuncs.funcs.c).
fn read_dropdb_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::DropdbStmt<'mcx>> {
    let _ = mcx;
    let dbname = read_str(mcx)?;
    let missing_ok = read_bool_field()?;
    let options = read_node_vec(mcx)?;
    Ok(dn::DropdbStmt {
        dbname: dbname,
        missing_ok: missing_ok,
        options: options,
    })
}

/// `_readAlterSystemStmt` (readfuncs.funcs.c).
fn read_alter_system_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::AlterSystemStmt<'mcx>> {
    let _ = mcx;
    let setstmt = read_opt_node(mcx)?;
    Ok(dn::AlterSystemStmt {
        setstmt: setstmt,
    })
}

/// `_readClusterStmt` (readfuncs.funcs.c).
fn read_cluster_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::ClusterStmt<'mcx>> {
    let _ = mcx;
    let relation = read_opt_node(mcx)?;
    let indexname = read_str(mcx)?;
    let params = read_node_vec(mcx)?;
    Ok(dn::ClusterStmt {
        relation: relation,
        indexname: indexname,
        params: params,
    })
}

/// `_readVacuumStmt` (readfuncs.funcs.c).
fn read_vacuum_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::VacuumStmt<'mcx>> {
    let _ = mcx;
    let options = read_node_vec(mcx)?;
    let rels = read_node_vec(mcx)?;
    let is_vacuumcmd = read_bool_field()?;
    Ok(dn::VacuumStmt {
        options: options,
        rels: rels,
        is_vacuumcmd: is_vacuumcmd,
    })
}

/// `_readVacuumRelation` (readfuncs.funcs.c).
fn read_vacuum_relation<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::VacuumRelation<'mcx>> {
    let _ = mcx;
    let relation = read_opt_node(mcx)?;
    let oid = read_oid_field()?;
    let va_cols = read_node_vec(mcx)?;
    Ok(dn::VacuumRelation {
        relation: relation,
        oid: oid,
        va_cols: va_cols,
    })
}

/// `_readExplainStmt` (readfuncs.funcs.c).
fn read_explain_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::ExplainStmt<'mcx>> {
    let _ = mcx;
    let query = read_opt_node(mcx)?;
    let options = read_node_vec(mcx)?;
    Ok(dn::ExplainStmt {
        query: query,
        options: options,
    })
}

/// `_readCreateTableAsStmt` (readfuncs.funcs.c).
fn read_create_table_as_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::CreateTableAsStmt<'mcx>> {
    let _ = mcx;
    let query = read_opt_node(mcx)?;
    let into = read_opt_node(mcx)?;
    let objtype = object_type_from(read_enum_field()?);
    let is_select_into = read_bool_field()?;
    let if_not_exists = read_bool_field()?;
    Ok(dn::CreateTableAsStmt {
        query: query,
        into: into,
        objtype: objtype,
        is_select_into: is_select_into,
        if_not_exists: if_not_exists,
    })
}

/// `_readRefreshMatViewStmt` (readfuncs.funcs.c).
fn read_refresh_mat_view_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::RefreshMatViewStmt<'mcx>> {
    let _ = mcx;
    let concurrent = read_bool_field()?;
    let skip_data = read_bool_field()?;
    let relation = read_opt_node(mcx)?;
    Ok(dn::RefreshMatViewStmt {
        concurrent: concurrent,
        skip_data: skip_data,
        relation: relation,
    })
}

/// `_readCheckPointStmt` (readfuncs.funcs.c).
fn read_check_point_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::CheckPointStmt> {
    let _ = mcx;
    Ok(dn::CheckPointStmt {
    })
}

/// `_readDiscardStmt` (readfuncs.funcs.c).
fn read_discard_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::DiscardStmt> {
    let _ = mcx;
    let target = discard_mode_from(read_enum_field()?);
    Ok(dn::DiscardStmt {
        target: target,
    })
}

/// `_readLockStmt` (readfuncs.funcs.c).
fn read_lock_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::LockStmt<'mcx>> {
    let _ = mcx;
    let relations = read_node_vec(mcx)?;
    let mode = read_int_field()?;
    let nowait = read_bool_field()?;
    Ok(dn::LockStmt {
        relations: relations,
        mode: mode,
        nowait: nowait,
    })
}

/// `_readConstraintsSetStmt` (readfuncs.funcs.c).
fn read_constraints_set_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::ConstraintsSetStmt<'mcx>> {
    let _ = mcx;
    let constraints = read_node_vec(mcx)?;
    let deferred = read_bool_field()?;
    Ok(dn::ConstraintsSetStmt {
        constraints: constraints,
        deferred: deferred,
    })
}

/// `_readReindexStmt` (readfuncs.funcs.c).
fn read_reindex_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::ReindexStmt<'mcx>> {
    let _ = mcx;
    let kind = reindex_object_type_from(read_enum_field()?);
    let relation = read_opt_node(mcx)?;
    let name = read_str(mcx)?;
    let params = read_node_vec(mcx)?;
    Ok(dn::ReindexStmt {
        kind: kind,
        relation: relation,
        name: name,
        params: params,
    })
}

/// `_readCreateConversionStmt` (readfuncs.funcs.c).
fn read_create_conversion_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::CreateConversionStmt<'mcx>> {
    let _ = mcx;
    let conversion_name = read_node_vec(mcx)?;
    let for_encoding_name = read_str(mcx)?;
    let to_encoding_name = read_str(mcx)?;
    let func_name = read_node_vec(mcx)?;
    let def = read_bool_field()?;
    Ok(dn::CreateConversionStmt {
        conversion_name: conversion_name,
        for_encoding_name: for_encoding_name,
        to_encoding_name: to_encoding_name,
        func_name: func_name,
        def: def,
    })
}

/// `_readCreateCastStmt` (readfuncs.funcs.c).
fn read_create_cast_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::CreateCastStmt<'mcx>> {
    let _ = mcx;
    let sourcetype = read_opt_node(mcx)?;
    let targettype = read_opt_node(mcx)?;
    let func = read_opt_node(mcx)?;
    let context = coercion_context_from(read_enum_field()?);
    let inout = read_bool_field()?;
    Ok(dn::CreateCastStmt {
        sourcetype: sourcetype,
        targettype: targettype,
        func: func,
        context: context,
        inout: inout,
    })
}

/// `_readCreateTransformStmt` (readfuncs.funcs.c).
fn read_create_transform_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::CreateTransformStmt<'mcx>> {
    let _ = mcx;
    let replace = read_bool_field()?;
    let type_name = read_opt_node(mcx)?;
    let lang = read_str(mcx)?;
    let fromsql = read_opt_node(mcx)?;
    let tosql = read_opt_node(mcx)?;
    Ok(dn::CreateTransformStmt {
        replace: replace,
        type_name: type_name,
        lang: lang,
        fromsql: fromsql,
        tosql: tosql,
    })
}

/// `_readPrepareStmt` (readfuncs.funcs.c).
fn read_prepare_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::PrepareStmt<'mcx>> {
    let _ = mcx;
    let name = read_str(mcx)?;
    let argtypes = read_node_vec(mcx)?;
    let query = read_opt_node(mcx)?;
    Ok(dn::PrepareStmt {
        name: name,
        argtypes: argtypes,
        query: query,
    })
}

/// `_readExecuteStmt` (readfuncs.funcs.c).
fn read_execute_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::ExecuteStmt<'mcx>> {
    let _ = mcx;
    let name = read_str(mcx)?;
    let params = read_node_vec(mcx)?;
    Ok(dn::ExecuteStmt {
        name: name,
        params: params,
    })
}

/// `_readDeallocateStmt` (readfuncs.funcs.c).
fn read_deallocate_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::DeallocateStmt<'mcx>> {
    let _ = mcx;
    let name = read_str(mcx)?;
    let isall = read_bool_field()?;
    let location = read_location_field()?;
    Ok(dn::DeallocateStmt {
        name: name,
        isall: isall,
        location: location,
    })
}

/// `_readDropOwnedStmt` (readfuncs.funcs.c).
fn read_drop_owned_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::DropOwnedStmt<'mcx>> {
    let _ = mcx;
    let roles = read_node_vec(mcx)?;
    let behavior = drop_behavior_from(read_enum_field()?);
    Ok(dn::DropOwnedStmt {
        roles: roles,
        behavior: behavior,
    })
}

/// `_readReassignOwnedStmt` (readfuncs.funcs.c).
fn read_reassign_owned_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::ReassignOwnedStmt<'mcx>> {
    let _ = mcx;
    let roles = read_node_vec(mcx)?;
    let newrole = read_opt_node(mcx)?;
    Ok(dn::ReassignOwnedStmt {
        roles: roles,
        newrole: newrole,
    })
}

/// `_readAlterTSDictionaryStmt` (readfuncs.funcs.c).
fn read_alter_t_s_dictionary_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::AlterTSDictionaryStmt<'mcx>> {
    let _ = mcx;
    let dictname = read_node_vec(mcx)?;
    let options = read_node_vec(mcx)?;
    Ok(dn::AlterTSDictionaryStmt {
        dictname: dictname,
        options: options,
    })
}

/// `_readAlterTSConfigurationStmt` (readfuncs.funcs.c).
fn read_alter_t_s_configuration_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::AlterTSConfigurationStmt<'mcx>> {
    let _ = mcx;
    let kind = alter_t_s_config_type_from(read_enum_field()?);
    let cfgname = read_node_vec(mcx)?;
    let tokentype = read_node_vec(mcx)?;
    let dicts = read_node_vec(mcx)?;
    let override_ = read_bool_field()?;
    let replace = read_bool_field()?;
    let missing_ok = read_bool_field()?;
    Ok(dn::AlterTSConfigurationStmt {
        kind: kind,
        cfgname: cfgname,
        tokentype: tokentype,
        dicts: dicts,
        override_: override_,
        replace: replace,
        missing_ok: missing_ok,
    })
}

/// `_readPublicationTable` (readfuncs.funcs.c).
fn read_publication_table<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::PublicationTable<'mcx>> {
    let _ = mcx;
    let relation = read_opt_node(mcx)?;
    let where_clause = read_opt_node(mcx)?;
    let columns = read_node_vec(mcx)?;
    Ok(dn::PublicationTable {
        relation: relation,
        where_clause: where_clause,
        columns: columns,
    })
}

/// `_readPublicationObjSpec` (readfuncs.funcs.c).
fn read_publication_obj_spec<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::PublicationObjSpec<'mcx>> {
    let _ = mcx;
    let pubobjtype = publication_obj_spec_type_from(read_enum_field()?);
    let name = read_str(mcx)?;
    skip_label()?; // skip :pubtable
    let pubtable = match read::node_read(mcx, None)? {
        None => None,
        Some(n) => {
            let __n = PgBox::into_inner(n);
            let __tag = __n.node_tag();
            match __n.into_publicationtable() {
                Some(t) => Some(mcx::alloc_in(mcx, t)?),
                None => return Err(elog_error(alloc::format!("expected PublicationTable, got {:?}", __tag))),
            }
        },
    };
    let location = read_location_field()?;
    Ok(dn::PublicationObjSpec {
        pubobjtype: pubobjtype,
        name: name,
        pubtable: pubtable,
        location: location,
    })
}

/// `_readCreatePublicationStmt` (readfuncs.funcs.c).
fn read_create_publication_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::CreatePublicationStmt<'mcx>> {
    let _ = mcx;
    let pubname = read_str(mcx)?;
    let options = read_node_vec(mcx)?;
    let pubobjects = read_node_vec(mcx)?;
    let for_all_tables = read_bool_field()?;
    Ok(dn::CreatePublicationStmt {
        pubname: pubname,
        options: options,
        pubobjects: pubobjects,
        for_all_tables: for_all_tables,
    })
}

/// `_readAlterPublicationStmt` (readfuncs.funcs.c).
fn read_alter_publication_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::AlterPublicationStmt<'mcx>> {
    let _ = mcx;
    let pubname = read_str(mcx)?;
    let options = read_node_vec(mcx)?;
    let pubobjects = read_node_vec(mcx)?;
    let for_all_tables = read_bool_field()?;
    let action = alter_publication_action_from(read_enum_field()?);
    Ok(dn::AlterPublicationStmt {
        pubname: pubname,
        options: options,
        pubobjects: pubobjects,
        for_all_tables: for_all_tables,
        action: action,
    })
}

/// `_readCreateSubscriptionStmt` (readfuncs.funcs.c).
fn read_create_subscription_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::CreateSubscriptionStmt<'mcx>> {
    let _ = mcx;
    let subname = read_str(mcx)?;
    let conninfo = read_str(mcx)?;
    let publication = read_node_vec(mcx)?;
    let options = read_node_vec(mcx)?;
    Ok(dn::CreateSubscriptionStmt {
        subname: subname,
        conninfo: conninfo,
        publication: publication,
        options: options,
    })
}

/// `_readAlterSubscriptionStmt` (readfuncs.funcs.c).
fn read_alter_subscription_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::AlterSubscriptionStmt<'mcx>> {
    let _ = mcx;
    let kind = alter_subscription_type_from(read_enum_field()?);
    let subname = read_str(mcx)?;
    let conninfo = read_str(mcx)?;
    let publication = read_node_vec(mcx)?;
    let options = read_node_vec(mcx)?;
    Ok(dn::AlterSubscriptionStmt {
        kind: kind,
        subname: subname,
        conninfo: conninfo,
        publication: publication,
        options: options,
    })
}

/// `_readDropSubscriptionStmt` (readfuncs.funcs.c).
fn read_drop_subscription_stmt<'mcx>(mcx: Mcx<'mcx>) -> PgResult<dn::DropSubscriptionStmt<'mcx>> {
    let _ = mcx;
    let subname = read_str(mcx)?;
    let missing_ok = read_bool_field()?;
    let behavior = drop_behavior_from(read_enum_field()?);
    Ok(dn::DropSubscriptionStmt {
        subname: subname,
        missing_ok: missing_ok,
        behavior: behavior,
    })
}

// ---- enum decoders (READ_ENUM_FIELD) ----
fn alter_publication_action_from(code: i32) -> dn::AlterPublicationAction {
    match code {
        0 => dn::AlterPublicationAction::AP_AddObjects,
        1 => dn::AlterPublicationAction::AP_DropObjects,
        2 => dn::AlterPublicationAction::AP_SetObjects,
        _ => dn::AlterPublicationAction::AP_AddObjects,
    }
}
fn alter_subscription_type_from(code: i32) -> dn::AlterSubscriptionType {
    match code {
        0 => dn::AlterSubscriptionType::ALTER_SUBSCRIPTION_OPTIONS,
        1 => dn::AlterSubscriptionType::ALTER_SUBSCRIPTION_CONNECTION,
        2 => dn::AlterSubscriptionType::ALTER_SUBSCRIPTION_SET_PUBLICATION,
        3 => dn::AlterSubscriptionType::ALTER_SUBSCRIPTION_ADD_PUBLICATION,
        4 => dn::AlterSubscriptionType::ALTER_SUBSCRIPTION_DROP_PUBLICATION,
        5 => dn::AlterSubscriptionType::ALTER_SUBSCRIPTION_REFRESH,
        6 => dn::AlterSubscriptionType::ALTER_SUBSCRIPTION_ENABLED,
        7 => dn::AlterSubscriptionType::ALTER_SUBSCRIPTION_SKIP,
        _ => dn::AlterSubscriptionType::ALTER_SUBSCRIPTION_OPTIONS,
    }
}
fn alter_t_s_config_type_from(code: i32) -> dn::AlterTSConfigType {
    match code {
        0 => dn::AlterTSConfigType::ALTER_TSCONFIG_ADD_MAPPING,
        1 => dn::AlterTSConfigType::ALTER_TSCONFIG_ALTER_MAPPING_FOR_TOKEN,
        2 => dn::AlterTSConfigType::ALTER_TSCONFIG_REPLACE_DICT,
        3 => dn::AlterTSConfigType::ALTER_TSCONFIG_REPLACE_DICT_FOR_TOKEN,
        4 => dn::AlterTSConfigType::ALTER_TSCONFIG_DROP_MAPPING,
        _ => dn::AlterTSConfigType::ALTER_TSCONFIG_ADD_MAPPING,
    }
}
fn alter_table_type_from(code: i32) -> dn::AlterTableType {
    match code {
        0 => dn::AlterTableType::AT_AddColumn,
        1 => dn::AlterTableType::AT_AddColumnToView,
        2 => dn::AlterTableType::AT_ColumnDefault,
        3 => dn::AlterTableType::AT_CookedColumnDefault,
        4 => dn::AlterTableType::AT_DropNotNull,
        5 => dn::AlterTableType::AT_SetNotNull,
        6 => dn::AlterTableType::AT_SetExpression,
        7 => dn::AlterTableType::AT_DropExpression,
        8 => dn::AlterTableType::AT_SetStatistics,
        9 => dn::AlterTableType::AT_SetOptions,
        10 => dn::AlterTableType::AT_ResetOptions,
        11 => dn::AlterTableType::AT_SetStorage,
        12 => dn::AlterTableType::AT_SetCompression,
        13 => dn::AlterTableType::AT_DropColumn,
        14 => dn::AlterTableType::AT_AddIndex,
        15 => dn::AlterTableType::AT_ReAddIndex,
        16 => dn::AlterTableType::AT_AddConstraint,
        17 => dn::AlterTableType::AT_ReAddConstraint,
        18 => dn::AlterTableType::AT_ReAddDomainConstraint,
        19 => dn::AlterTableType::AT_AlterConstraint,
        20 => dn::AlterTableType::AT_ValidateConstraint,
        21 => dn::AlterTableType::AT_AddIndexConstraint,
        22 => dn::AlterTableType::AT_DropConstraint,
        23 => dn::AlterTableType::AT_ReAddComment,
        24 => dn::AlterTableType::AT_AlterColumnType,
        25 => dn::AlterTableType::AT_AlterColumnGenericOptions,
        26 => dn::AlterTableType::AT_ChangeOwner,
        27 => dn::AlterTableType::AT_ClusterOn,
        28 => dn::AlterTableType::AT_DropCluster,
        29 => dn::AlterTableType::AT_SetLogged,
        30 => dn::AlterTableType::AT_SetUnLogged,
        31 => dn::AlterTableType::AT_DropOids,
        32 => dn::AlterTableType::AT_SetAccessMethod,
        33 => dn::AlterTableType::AT_SetTableSpace,
        34 => dn::AlterTableType::AT_SetRelOptions,
        35 => dn::AlterTableType::AT_ResetRelOptions,
        36 => dn::AlterTableType::AT_ReplaceRelOptions,
        37 => dn::AlterTableType::AT_EnableTrig,
        38 => dn::AlterTableType::AT_EnableAlwaysTrig,
        39 => dn::AlterTableType::AT_EnableReplicaTrig,
        40 => dn::AlterTableType::AT_DisableTrig,
        41 => dn::AlterTableType::AT_EnableTrigAll,
        42 => dn::AlterTableType::AT_DisableTrigAll,
        43 => dn::AlterTableType::AT_EnableTrigUser,
        44 => dn::AlterTableType::AT_DisableTrigUser,
        45 => dn::AlterTableType::AT_EnableRule,
        46 => dn::AlterTableType::AT_EnableAlwaysRule,
        47 => dn::AlterTableType::AT_EnableReplicaRule,
        48 => dn::AlterTableType::AT_DisableRule,
        49 => dn::AlterTableType::AT_AddInherit,
        50 => dn::AlterTableType::AT_DropInherit,
        51 => dn::AlterTableType::AT_AddOf,
        52 => dn::AlterTableType::AT_DropOf,
        53 => dn::AlterTableType::AT_ReplicaIdentity,
        54 => dn::AlterTableType::AT_EnableRowSecurity,
        55 => dn::AlterTableType::AT_DisableRowSecurity,
        56 => dn::AlterTableType::AT_ForceRowSecurity,
        57 => dn::AlterTableType::AT_NoForceRowSecurity,
        58 => dn::AlterTableType::AT_GenericOptions,
        59 => dn::AlterTableType::AT_AttachPartition,
        60 => dn::AlterTableType::AT_DetachPartition,
        61 => dn::AlterTableType::AT_DetachPartitionFinalize,
        62 => dn::AlterTableType::AT_AddIdentity,
        63 => dn::AlterTableType::AT_SetIdentity,
        64 => dn::AlterTableType::AT_DropIdentity,
        65 => dn::AlterTableType::AT_ReAddStatistics,
        _ => dn::AlterTableType::AT_AddColumn,
    }
}
fn cmd_type_from(code: i32) -> types_nodes::nodes::CmdType {
    match code {
        0 => types_nodes::nodes::CmdType::CMD_UNKNOWN,
        1 => types_nodes::nodes::CmdType::CMD_SELECT,
        2 => types_nodes::nodes::CmdType::CMD_UPDATE,
        3 => types_nodes::nodes::CmdType::CMD_INSERT,
        4 => types_nodes::nodes::CmdType::CMD_DELETE,
        5 => types_nodes::nodes::CmdType::CMD_MERGE,
        6 => types_nodes::nodes::CmdType::CMD_UTILITY,
        7 => types_nodes::nodes::CmdType::CMD_NOTHING,
        _ => types_nodes::nodes::CmdType::CMD_UNKNOWN,
    }
}
fn coercion_context_from(code: i32) -> dn::CoercionContext {
    match code {
        0 => dn::CoercionContext::COERCION_IMPLICIT,
        1 => dn::CoercionContext::COERCION_ASSIGNMENT,
        2 => dn::CoercionContext::COERCION_PLPGSQL,
        3 => dn::CoercionContext::COERCION_EXPLICIT,
        _ => dn::CoercionContext::COERCION_IMPLICIT,
    }
}
fn constr_type_from(code: i32) -> dn::ConstrType {
    match code {
        0 => dn::ConstrType::CONSTR_NULL,
        1 => dn::ConstrType::CONSTR_NOTNULL,
        2 => dn::ConstrType::CONSTR_DEFAULT,
        3 => dn::ConstrType::CONSTR_IDENTITY,
        4 => dn::ConstrType::CONSTR_GENERATED,
        5 => dn::ConstrType::CONSTR_CHECK,
        6 => dn::ConstrType::CONSTR_PRIMARY,
        7 => dn::ConstrType::CONSTR_UNIQUE,
        8 => dn::ConstrType::CONSTR_EXCLUSION,
        9 => dn::ConstrType::CONSTR_FOREIGN,
        10 => dn::ConstrType::CONSTR_ATTR_DEFERRABLE,
        11 => dn::ConstrType::CONSTR_ATTR_NOT_DEFERRABLE,
        12 => dn::ConstrType::CONSTR_ATTR_DEFERRED,
        13 => dn::ConstrType::CONSTR_ATTR_IMMEDIATE,
        14 => dn::ConstrType::CONSTR_ATTR_ENFORCED,
        15 => dn::ConstrType::CONSTR_ATTR_NOT_ENFORCED,
        _ => dn::ConstrType::CONSTR_NULL,
    }
}
fn def_elem_action_from(code: i32) -> dn::DefElemAction {
    match code {
        0 => dn::DefElemAction::DEFELEM_UNSPEC,
        1 => dn::DefElemAction::DEFELEM_SET,
        2 => dn::DefElemAction::DEFELEM_ADD,
        3 => dn::DefElemAction::DEFELEM_DROP,
        _ => dn::DefElemAction::DEFELEM_UNSPEC,
    }
}
fn discard_mode_from(code: i32) -> dn::DiscardMode {
    match code {
        0 => dn::DiscardMode::DISCARD_ALL,
        1 => dn::DiscardMode::DISCARD_PLANS,
        2 => dn::DiscardMode::DISCARD_SEQUENCES,
        3 => dn::DiscardMode::DISCARD_TEMP,
        _ => dn::DiscardMode::DISCARD_ALL,
    }
}
fn drop_behavior_from(code: i32) -> types_nodes::parsenodes::DropBehavior {
    match code {
        0 => types_nodes::parsenodes::DropBehavior::Restrict,
        1 => types_nodes::parsenodes::DropBehavior::Cascade,
        _ => types_nodes::parsenodes::DropBehavior::Restrict,
    }
}
fn fetch_direction_from(code: i32) -> dn::FetchDirection {
    match code {
        0 => dn::FetchDirection::FETCH_FORWARD,
        1 => dn::FetchDirection::FETCH_BACKWARD,
        2 => dn::FetchDirection::FETCH_ABSOLUTE,
        3 => dn::FetchDirection::FETCH_RELATIVE,
        _ => dn::FetchDirection::FETCH_FORWARD,
    }
}
fn function_parameter_mode_from(code: i32) -> dn::FunctionParameterMode {
    match code {
        105 => dn::FunctionParameterMode::FUNC_PARAM_IN,
        111 => dn::FunctionParameterMode::FUNC_PARAM_OUT,
        98 => dn::FunctionParameterMode::FUNC_PARAM_INOUT,
        118 => dn::FunctionParameterMode::FUNC_PARAM_VARIADIC,
        116 => dn::FunctionParameterMode::FUNC_PARAM_TABLE,
        100 => dn::FunctionParameterMode::FUNC_PARAM_DEFAULT,
        _ => dn::FunctionParameterMode::FUNC_PARAM_IN,
    }
}
fn grant_target_type_from(code: i32) -> dn::GrantTargetType {
    match code {
        0 => dn::GrantTargetType::ACL_TARGET_OBJECT,
        1 => dn::GrantTargetType::ACL_TARGET_ALL_IN_SCHEMA,
        2 => dn::GrantTargetType::ACL_TARGET_DEFAULTS,
        _ => dn::GrantTargetType::ACL_TARGET_OBJECT,
    }
}
fn import_foreign_schema_type_from(code: i32) -> dn::ImportForeignSchemaType {
    match code {
        0 => dn::ImportForeignSchemaType::FDW_IMPORT_SCHEMA_ALL,
        1 => dn::ImportForeignSchemaType::FDW_IMPORT_SCHEMA_LIMIT_TO,
        2 => dn::ImportForeignSchemaType::FDW_IMPORT_SCHEMA_EXCEPT,
        _ => dn::ImportForeignSchemaType::FDW_IMPORT_SCHEMA_ALL,
    }
}
fn object_type_from(code: i32) -> types_nodes::parsenodes::ObjectType {
    match code {
        0 => types_nodes::parsenodes::ObjectType::AccessMethod,
        1 => types_nodes::parsenodes::ObjectType::Aggregate,
        2 => types_nodes::parsenodes::ObjectType::Amop,
        3 => types_nodes::parsenodes::ObjectType::Amproc,
        4 => types_nodes::parsenodes::ObjectType::Attribute,
        5 => types_nodes::parsenodes::ObjectType::Cast,
        6 => types_nodes::parsenodes::ObjectType::Column,
        7 => types_nodes::parsenodes::ObjectType::Collation,
        8 => types_nodes::parsenodes::ObjectType::Conversion,
        9 => types_nodes::parsenodes::ObjectType::Database,
        10 => types_nodes::parsenodes::ObjectType::Default,
        11 => types_nodes::parsenodes::ObjectType::Defacl,
        12 => types_nodes::parsenodes::ObjectType::Domain,
        13 => types_nodes::parsenodes::ObjectType::Domconstraint,
        14 => types_nodes::parsenodes::ObjectType::EventTrigger,
        15 => types_nodes::parsenodes::ObjectType::Extension,
        16 => types_nodes::parsenodes::ObjectType::Fdw,
        17 => types_nodes::parsenodes::ObjectType::ForeignServer,
        18 => types_nodes::parsenodes::ObjectType::ForeignTable,
        19 => types_nodes::parsenodes::ObjectType::Function,
        20 => types_nodes::parsenodes::ObjectType::Index,
        21 => types_nodes::parsenodes::ObjectType::Language,
        22 => types_nodes::parsenodes::ObjectType::Largeobject,
        23 => types_nodes::parsenodes::ObjectType::Matview,
        24 => types_nodes::parsenodes::ObjectType::Opclass,
        25 => types_nodes::parsenodes::ObjectType::Operator,
        26 => types_nodes::parsenodes::ObjectType::Opfamily,
        27 => types_nodes::parsenodes::ObjectType::ParameterAcl,
        28 => types_nodes::parsenodes::ObjectType::Policy,
        29 => types_nodes::parsenodes::ObjectType::Procedure,
        30 => types_nodes::parsenodes::ObjectType::Publication,
        31 => types_nodes::parsenodes::ObjectType::PublicationNamespace,
        32 => types_nodes::parsenodes::ObjectType::PublicationRel,
        33 => types_nodes::parsenodes::ObjectType::Role,
        34 => types_nodes::parsenodes::ObjectType::Routine,
        35 => types_nodes::parsenodes::ObjectType::Rule,
        36 => types_nodes::parsenodes::ObjectType::Schema,
        37 => types_nodes::parsenodes::ObjectType::Sequence,
        38 => types_nodes::parsenodes::ObjectType::Subscription,
        39 => types_nodes::parsenodes::ObjectType::StatisticExt,
        40 => types_nodes::parsenodes::ObjectType::Tabconstraint,
        41 => types_nodes::parsenodes::ObjectType::Table,
        42 => types_nodes::parsenodes::ObjectType::Tablespace,
        43 => types_nodes::parsenodes::ObjectType::Transform,
        44 => types_nodes::parsenodes::ObjectType::Trigger,
        45 => types_nodes::parsenodes::ObjectType::TsConfiguration,
        46 => types_nodes::parsenodes::ObjectType::TsDictionary,
        47 => types_nodes::parsenodes::ObjectType::TsParser,
        48 => types_nodes::parsenodes::ObjectType::TsTemplate,
        49 => types_nodes::parsenodes::ObjectType::Type,
        50 => types_nodes::parsenodes::ObjectType::UserMapping,
        51 => types_nodes::parsenodes::ObjectType::View,
        _ => types_nodes::parsenodes::ObjectType::AccessMethod,
    }
}
fn on_commit_action_from(code: i32) -> types_nodes::primnodes::OnCommitAction {
    match code {
        0 => types_nodes::primnodes::OnCommitAction::ONCOMMIT_NOOP,
        1 => types_nodes::primnodes::OnCommitAction::ONCOMMIT_PRESERVE_ROWS,
        2 => types_nodes::primnodes::OnCommitAction::ONCOMMIT_DELETE_ROWS,
        3 => types_nodes::primnodes::OnCommitAction::ONCOMMIT_DROP,
        _ => types_nodes::primnodes::OnCommitAction::ONCOMMIT_NOOP,
    }
}
fn partition_range_datum_kind_from(code: i32) -> types_nodes::partition::PartitionRangeDatumKind {
    match code {
        -1 => types_nodes::partition::PartitionRangeDatumKind::MinValue,
        0 => types_nodes::partition::PartitionRangeDatumKind::Value,
        1 => types_nodes::partition::PartitionRangeDatumKind::MaxValue,
        _ => types_nodes::partition::PartitionRangeDatumKind::MinValue,
    }
}
fn partition_strategy_from(code: i32) -> types_nodes::partition::PartitionStrategy {
    match code {
        108 => types_nodes::partition::PartitionStrategy::List,
        114 => types_nodes::partition::PartitionStrategy::Range,
        104 => types_nodes::partition::PartitionStrategy::Hash,
        _ => types_nodes::partition::PartitionStrategy::List,
    }
}
fn publication_obj_spec_type_from(code: i32) -> dn::PublicationObjSpecType {
    match code {
        0 => dn::PublicationObjSpecType::PUBLICATIONOBJ_TABLE,
        1 => dn::PublicationObjSpecType::PUBLICATIONOBJ_TABLES_IN_SCHEMA,
        2 => dn::PublicationObjSpecType::PUBLICATIONOBJ_TABLES_IN_CUR_SCHEMA,
        3 => dn::PublicationObjSpecType::PUBLICATIONOBJ_CONTINUATION,
        _ => dn::PublicationObjSpecType::PUBLICATIONOBJ_TABLE,
    }
}
fn reindex_object_type_from(code: i32) -> dn::ReindexObjectType {
    match code {
        0 => dn::ReindexObjectType::REINDEX_OBJECT_INDEX,
        1 => dn::ReindexObjectType::REINDEX_OBJECT_TABLE,
        2 => dn::ReindexObjectType::REINDEX_OBJECT_SCHEMA,
        3 => dn::ReindexObjectType::REINDEX_OBJECT_SYSTEM,
        4 => dn::ReindexObjectType::REINDEX_OBJECT_DATABASE,
        _ => dn::ReindexObjectType::REINDEX_OBJECT_INDEX,
    }
}
fn role_spec_type_from(code: i32) -> types_nodes::parsenodes::RoleSpecType {
    match code {
        0 => types_nodes::parsenodes::RoleSpecType::Cstring,
        1 => types_nodes::parsenodes::RoleSpecType::CurrentRole,
        2 => types_nodes::parsenodes::RoleSpecType::CurrentUser,
        3 => types_nodes::parsenodes::RoleSpecType::SessionUser,
        4 => types_nodes::parsenodes::RoleSpecType::Public,
        _ => types_nodes::parsenodes::RoleSpecType::Cstring,
    }
}
fn role_stmt_type_from(code: i32) -> dn::RoleStmtType {
    match code {
        0 => dn::RoleStmtType::ROLESTMT_ROLE,
        1 => dn::RoleStmtType::ROLESTMT_USER,
        2 => dn::RoleStmtType::ROLESTMT_GROUP,
        _ => dn::RoleStmtType::ROLESTMT_ROLE,
    }
}
fn sort_by_dir_from(code: i32) -> types_nodes::rawnodes::SortByDir {
    match code {
        0 => types_nodes::rawnodes::SortByDir::SORTBY_DEFAULT,
        1 => types_nodes::rawnodes::SortByDir::SORTBY_ASC,
        2 => types_nodes::rawnodes::SortByDir::SORTBY_DESC,
        3 => types_nodes::rawnodes::SortByDir::SORTBY_USING,
        _ => types_nodes::rawnodes::SortByDir::SORTBY_DEFAULT,
    }
}
fn sort_by_nulls_from(code: i32) -> types_nodes::rawnodes::SortByNulls {
    match code {
        0 => types_nodes::rawnodes::SortByNulls::SORTBY_NULLS_DEFAULT,
        1 => types_nodes::rawnodes::SortByNulls::SORTBY_NULLS_FIRST,
        2 => types_nodes::rawnodes::SortByNulls::SORTBY_NULLS_LAST,
        _ => types_nodes::rawnodes::SortByNulls::SORTBY_NULLS_DEFAULT,
    }
}
fn transaction_stmt_kind_from(code: i32) -> dn::TransactionStmtKind {
    match code {
        0 => dn::TransactionStmtKind::TRANS_STMT_BEGIN,
        1 => dn::TransactionStmtKind::TRANS_STMT_START,
        2 => dn::TransactionStmtKind::TRANS_STMT_COMMIT,
        3 => dn::TransactionStmtKind::TRANS_STMT_ROLLBACK,
        4 => dn::TransactionStmtKind::TRANS_STMT_SAVEPOINT,
        5 => dn::TransactionStmtKind::TRANS_STMT_RELEASE,
        6 => dn::TransactionStmtKind::TRANS_STMT_ROLLBACK_TO,
        7 => dn::TransactionStmtKind::TRANS_STMT_PREPARE,
        8 => dn::TransactionStmtKind::TRANS_STMT_COMMIT_PREPARED,
        9 => dn::TransactionStmtKind::TRANS_STMT_ROLLBACK_PREPARED,
        _ => dn::TransactionStmtKind::TRANS_STMT_BEGIN,
    }
}
fn variable_set_kind_from(code: i32) -> dn::VariableSetKind {
    match code {
        0 => dn::VariableSetKind::VAR_SET_VALUE,
        1 => dn::VariableSetKind::VAR_SET_DEFAULT,
        2 => dn::VariableSetKind::VAR_SET_CURRENT,
        3 => dn::VariableSetKind::VAR_SET_MULTI,
        4 => dn::VariableSetKind::VAR_RESET,
        5 => dn::VariableSetKind::VAR_RESET_ALL,
        _ => dn::VariableSetKind::VAR_SET_VALUE,
    }
}
fn view_check_option_from(code: i32) -> dn::ViewCheckOption {
    match code {
        0 => dn::ViewCheckOption::NO_CHECK_OPTION,
        1 => dn::ViewCheckOption::LOCAL_CHECK_OPTION,
        2 => dn::ViewCheckOption::CASCADED_CHECK_OPTION,
        _ => dn::ViewCheckOption::NO_CHECK_OPTION,
    }
}

pub(crate) fn try_read<'mcx>(mcx: Mcx<'mcx>, label: &[u8]) -> Option<PgResult<Node<'mcx>>> {
    let node = match label {
        b"INTOCLAUSE" => (|| Ok(Node::mk_into_clause(mcx, read_into_clause(mcx)?)))(),
        b"ROLESPEC" => (|| Ok(Node::mk_role_spec(mcx, read_role_spec(mcx)?)))(),
        b"TABLELIKECLAUSE" => (|| Ok(Node::mk_table_like_clause(mcx, read_table_like_clause(mcx)?)))(),
        b"INDEXELEM" => (|| Ok(Node::mk_index_elem(mcx, read_index_elem(mcx)?)))(),
        b"DEFELEM" => (|| Ok(Node::mk_def_elem(mcx, read_def_elem(mcx)?)))(),
        b"PARTITIONELEM" => (|| Ok(Node::mk_partition_elem(mcx, read_partition_elem(mcx)?)))(),
        b"PARTITIONSPEC" => (|| Ok(Node::mk_partition_spec(mcx, read_partition_spec(mcx)?)))(),
        b"PARTITIONBOUNDSPEC" => (|| Ok(Node::mk_partition_bound_spec(mcx, read_partition_bound_spec(mcx)?)))(),
        b"PARTITIONRANGEDATUM" => (|| Ok(Node::mk_partition_range_datum(mcx, read_partition_range_datum(mcx)?)))(),
        b"PARTITIONCMD" => (|| Ok(Node::mk_partition_cmd(mcx, read_partition_cmd(mcx)?)))(),
        b"RETURNSTMT" => (|| Ok(Node::mk_return_stmt(mcx, read_return_stmt(mcx)?)))(),
        b"PLASSIGNSTMT" => (|| Ok(Node::mk_pl_assign_stmt(mcx, read_p_l_assign_stmt(mcx)?)))(),
        b"CREATESCHEMASTMT" => (|| Ok(Node::mk_create_schema_stmt(mcx, read_create_schema_stmt(mcx)?)))(),
        b"ALTERTABLESTMT" => (|| Ok(Node::mk_alter_table_stmt(mcx, read_alter_table_stmt(mcx)?)))(),
        b"ALTERTABLECMD" => (|| Ok(Node::mk_alter_table_cmd(mcx, read_alter_table_cmd(mcx)?)))(),
        b"ATALTERCONSTRAINT" => (|| Ok(Node::mk_at_alter_constraint(mcx, read_a_t_alter_constraint(mcx)?)))(),
        b"REPLICAIDENTITYSTMT" => (|| Ok(Node::mk_replica_identity_stmt(mcx, read_replica_identity_stmt(mcx)?)))(),
        b"ALTERCOLLATIONSTMT" => (|| Ok(Node::mk_alter_collation_stmt(mcx, read_alter_collation_stmt(mcx)?)))(),
        b"ALTERDOMAINSTMT" => (|| Ok(Node::mk_alter_domain_stmt(mcx, read_alter_domain_stmt(mcx)?)))(),
        b"GRANTSTMT" => (|| Ok(Node::mk_grant_stmt(mcx, read_grant_stmt(mcx)?)))(),
        b"OBJECTWITHARGS" => (|| Ok(Node::mk_object_with_args(mcx, read_object_with_args(mcx)?)))(),
        b"ACCESSPRIV" => (|| Ok(Node::mk_access_priv(mcx, read_access_priv(mcx)?)))(),
        b"GRANTROLESTMT" => (|| Ok(Node::mk_grant_role_stmt(mcx, read_grant_role_stmt(mcx)?)))(),
        b"ALTERDEFAULTPRIVILEGESSTMT" => (|| Ok(Node::mk_alter_default_privileges_stmt(mcx, read_alter_default_privileges_stmt(mcx)?)))(),
        b"COPYSTMT" => (|| Ok(Node::mk_copy_stmt(mcx, read_copy_stmt(mcx)?)))(),
        b"VARIABLESETSTMT" => (|| Ok(Node::mk_variable_set_stmt(mcx, read_variable_set_stmt(mcx)?)))(),
        b"VARIABLESHOWSTMT" => (|| Ok(Node::mk_variable_show_stmt(mcx, read_variable_show_stmt(mcx)?)))(),
        b"CREATESTMT" => (|| Ok(Node::mk_create_stmt(mcx, read_create_stmt(mcx)?)))(),
        b"CONSTRAINT" => (|| Ok(Node::mk_constraint(mcx, read_constraint(mcx)?)))(),
        b"CREATETABLESPACESTMT" => (|| Ok(Node::mk_create_table_space_stmt(mcx, read_create_table_space_stmt(mcx)?)))(),
        b"DROPTABLESPACESTMT" => (|| Ok(Node::mk_drop_table_space_stmt(mcx, read_drop_table_space_stmt(mcx)?)))(),
        b"ALTERTABLESPACEOPTIONSSTMT" => (|| Ok(Node::mk_alter_table_space_options_stmt(mcx, read_alter_table_space_options_stmt(mcx)?)))(),
        b"ALTERTABLEMOVEALLSTMT" => (|| Ok(Node::mk_alter_table_move_all_stmt(mcx, read_alter_table_move_all_stmt(mcx)?)))(),
        b"CREATEEXTENSIONSTMT" => (|| Ok(Node::mk_create_extension_stmt(mcx, read_create_extension_stmt(mcx)?)))(),
        b"ALTEREXTENSIONSTMT" => (|| Ok(Node::mk_alter_extension_stmt(mcx, read_alter_extension_stmt(mcx)?)))(),
        b"ALTEREXTENSIONCONTENTSSTMT" => (|| Ok(Node::mk_alter_extension_contents_stmt(mcx, read_alter_extension_contents_stmt(mcx)?)))(),
        b"CREATEFDWSTMT" => (|| Ok(Node::mk_create_fdw_stmt(mcx, read_create_fdw_stmt(mcx)?)))(),
        b"ALTERFDWSTMT" => (|| Ok(Node::mk_alter_fdw_stmt(mcx, read_alter_fdw_stmt(mcx)?)))(),
        b"CREATEFOREIGNSERVERSTMT" => (|| Ok(Node::mk_create_foreign_server_stmt(mcx, read_create_foreign_server_stmt(mcx)?)))(),
        b"ALTERFOREIGNSERVERSTMT" => (|| Ok(Node::mk_alter_foreign_server_stmt(mcx, read_alter_foreign_server_stmt(mcx)?)))(),
        b"CREATEFOREIGNTABLESTMT" => (|| Ok(Node::mk_create_foreign_table_stmt(mcx, read_create_foreign_table_stmt(mcx)?)))(),
        b"CREATEUSERMAPPINGSTMT" => (|| Ok(Node::mk_create_user_mapping_stmt(mcx, read_create_user_mapping_stmt(mcx)?)))(),
        b"ALTERUSERMAPPINGSTMT" => (|| Ok(Node::mk_alter_user_mapping_stmt(mcx, read_alter_user_mapping_stmt(mcx)?)))(),
        b"DROPUSERMAPPINGSTMT" => (|| Ok(Node::mk_drop_user_mapping_stmt(mcx, read_drop_user_mapping_stmt(mcx)?)))(),
        b"IMPORTFOREIGNSCHEMASTMT" => (|| Ok(Node::mk_import_foreign_schema_stmt(mcx, read_import_foreign_schema_stmt(mcx)?)))(),
        b"CREATEPOLICYSTMT" => (|| Ok(Node::mk_create_policy_stmt(mcx, read_create_policy_stmt(mcx)?)))(),
        b"ALTERPOLICYSTMT" => (|| Ok(Node::mk_alter_policy_stmt(mcx, read_alter_policy_stmt(mcx)?)))(),
        b"CREATEAMSTMT" => (|| Ok(Node::mk_create_am_stmt(mcx, read_create_am_stmt(mcx)?)))(),
        b"CREATETRIGSTMT" => (|| Ok(Node::mk_create_trig_stmt(mcx, read_create_trig_stmt(mcx)?)))(),
        b"CREATEEVENTTRIGSTMT" => (|| Ok(Node::mk_create_event_trig_stmt(mcx, read_create_event_trig_stmt(mcx)?)))(),
        b"ALTEREVENTTRIGSTMT" => (|| Ok(Node::mk_alter_event_trig_stmt(mcx, read_alter_event_trig_stmt(mcx)?)))(),
        b"CREATEPLANGSTMT" => (|| Ok(Node::mk_create_p_lang_stmt(mcx, read_create_p_lang_stmt(mcx)?)))(),
        b"CREATEROLESTMT" => (|| Ok(Node::mk_create_role_stmt(mcx, read_create_role_stmt(mcx)?)))(),
        b"ALTERROLESTMT" => (|| Ok(Node::mk_alter_role_stmt(mcx, read_alter_role_stmt(mcx)?)))(),
        b"ALTERROLESETSTMT" => (|| Ok(Node::mk_alter_role_set_stmt(mcx, read_alter_role_set_stmt(mcx)?)))(),
        b"DROPROLESTMT" => (|| Ok(Node::mk_drop_role_stmt(mcx, read_drop_role_stmt(mcx)?)))(),
        b"CREATESEQSTMT" => (|| Ok(Node::mk_create_seq_stmt(mcx, read_create_seq_stmt(mcx)?)))(),
        b"ALTERSEQSTMT" => (|| Ok(Node::mk_alter_seq_stmt(mcx, read_alter_seq_stmt(mcx)?)))(),
        b"DEFINESTMT" => (|| Ok(Node::mk_define_stmt(mcx, read_define_stmt(mcx)?)))(),
        b"CREATEDOMAINSTMT" => (|| Ok(Node::mk_create_domain_stmt(mcx, read_create_domain_stmt(mcx)?)))(),
        b"CREATEOPCLASSSTMT" => (|| Ok(Node::mk_create_op_class_stmt(mcx, read_create_op_class_stmt(mcx)?)))(),
        b"CREATEOPCLASSITEM" => (|| Ok(Node::mk_create_op_class_item(mcx, read_create_op_class_item(mcx)?)))(),
        b"CREATEOPFAMILYSTMT" => (|| Ok(Node::mk_create_op_family_stmt(mcx, read_create_op_family_stmt(mcx)?)))(),
        b"ALTEROPFAMILYSTMT" => (|| Ok(Node::mk_alter_op_family_stmt(mcx, read_alter_op_family_stmt(mcx)?)))(),
        b"DROPSTMT" => (|| Ok(Node::mk_drop_stmt(mcx, read_drop_stmt(mcx)?)))(),
        b"TRUNCATESTMT" => (|| Ok(Node::mk_truncate_stmt(mcx, read_truncate_stmt(mcx)?)))(),
        b"COMMENTSTMT" => (|| Ok(Node::mk_comment_stmt(mcx, read_comment_stmt(mcx)?)))(),
        b"SECLABELSTMT" => (|| Ok(Node::mk_sec_label_stmt(mcx, read_sec_label_stmt(mcx)?)))(),
        b"DECLARECURSORSTMT" => (|| Ok(Node::mk_declare_cursor_stmt(mcx, read_declare_cursor_stmt(mcx)?)))(),
        b"CLOSEPORTALSTMT" => (|| Ok(Node::mk_close_portal_stmt(mcx, read_close_portal_stmt(mcx)?)))(),
        b"FETCHSTMT" => (|| Ok(Node::mk_fetch_stmt(mcx, read_fetch_stmt(mcx)?)))(),
        b"INDEXSTMT" => (|| Ok(Node::mk_index_stmt(mcx, read_index_stmt(mcx)?)))(),
        b"CREATESTATSSTMT" => (|| Ok(Node::mk_create_stats_stmt(mcx, read_create_stats_stmt(mcx)?)))(),
        b"STATSELEM" => (|| Ok(Node::mk_stats_elem(mcx, read_stats_elem(mcx)?)))(),
        b"ALTERSTATSSTMT" => (|| Ok(Node::mk_alter_stats_stmt(mcx, read_alter_stats_stmt(mcx)?)))(),
        b"CREATEFUNCTIONSTMT" => (|| Ok(Node::mk_create_function_stmt(mcx, read_create_function_stmt(mcx)?)))(),
        b"FUNCTIONPARAMETER" => (|| Ok(Node::mk_function_parameter(mcx, read_function_parameter(mcx)?)))(),
        b"ALTERFUNCTIONSTMT" => (|| Ok(Node::mk_alter_function_stmt(mcx, read_alter_function_stmt(mcx)?)))(),
        b"DOSTMT" => (|| Ok(Node::mk_do_stmt(mcx, read_do_stmt(mcx)?)))(),
        b"CALLSTMT" => (|| Ok(Node::mk_call_stmt(mcx, read_call_stmt(mcx)?)))(),
        b"RENAMESTMT" => (|| Ok(Node::mk_rename_stmt(mcx, read_rename_stmt(mcx)?)))(),
        b"ALTEROBJECTDEPENDSSTMT" => (|| Ok(Node::mk_alter_object_depends_stmt(mcx, read_alter_object_depends_stmt(mcx)?)))(),
        b"ALTEROBJECTSCHEMASTMT" => (|| Ok(Node::mk_alter_object_schema_stmt(mcx, read_alter_object_schema_stmt(mcx)?)))(),
        b"ALTEROWNERSTMT" => (|| Ok(Node::mk_alter_owner_stmt(mcx, read_alter_owner_stmt(mcx)?)))(),
        b"ALTEROPERATORSTMT" => (|| Ok(Node::mk_alter_operator_stmt(mcx, read_alter_operator_stmt(mcx)?)))(),
        b"ALTERTYPESTMT" => (|| Ok(Node::mk_alter_type_stmt(mcx, read_alter_type_stmt(mcx)?)))(),
        b"RULESTMT" => (|| Ok(Node::mk_rule_stmt(mcx, read_rule_stmt(mcx)?)))(),
        b"NOTIFYSTMT" => (|| Ok(Node::mk_notify_stmt(mcx, read_notify_stmt(mcx)?)))(),
        b"LISTENSTMT" => (|| Ok(Node::mk_listen_stmt(mcx, read_listen_stmt(mcx)?)))(),
        b"UNLISTENSTMT" => (|| Ok(Node::mk_unlisten_stmt(mcx, read_unlisten_stmt(mcx)?)))(),
        b"TRANSACTIONSTMT" => (|| Ok(Node::mk_transaction_stmt(mcx, read_transaction_stmt(mcx)?)))(),
        b"COMPOSITETYPESTMT" => (|| Ok(Node::mk_composite_type_stmt(mcx, read_composite_type_stmt(mcx)?)))(),
        b"CREATEENUMSTMT" => (|| Ok(Node::mk_create_enum_stmt(mcx, read_create_enum_stmt(mcx)?)))(),
        b"CREATERANGESTMT" => (|| Ok(Node::mk_create_range_stmt(mcx, read_create_range_stmt(mcx)?)))(),
        b"ALTERENUMSTMT" => (|| Ok(Node::mk_alter_enum_stmt(mcx, read_alter_enum_stmt(mcx)?)))(),
        b"VIEWSTMT" => (|| Ok(Node::mk_view_stmt(mcx, read_view_stmt(mcx)?)))(),
        b"LOADSTMT" => (|| Ok(Node::mk_load_stmt(mcx, read_load_stmt(mcx)?)))(),
        b"CREATEDBSTMT" => (|| Ok(Node::mk_createdb_stmt(mcx, read_createdb_stmt(mcx)?)))(),
        b"ALTERDATABASESTMT" => (|| Ok(Node::mk_alter_database_stmt(mcx, read_alter_database_stmt(mcx)?)))(),
        b"ALTERDATABASEREFRESHCOLLSTMT" => (|| Ok(Node::mk_alter_database_refresh_coll_stmt(mcx, read_alter_database_refresh_coll_stmt(mcx)?)))(),
        b"ALTERDATABASESETSTMT" => (|| Ok(Node::mk_alter_database_set_stmt(mcx, read_alter_database_set_stmt(mcx)?)))(),
        b"DROPDBSTMT" => (|| Ok(Node::mk_dropdb_stmt(mcx, read_dropdb_stmt(mcx)?)))(),
        b"ALTERSYSTEMSTMT" => (|| Ok(Node::mk_alter_system_stmt(mcx, read_alter_system_stmt(mcx)?)))(),
        b"CLUSTERSTMT" => (|| Ok(Node::mk_cluster_stmt(mcx, read_cluster_stmt(mcx)?)))(),
        b"VACUUMSTMT" => (|| Ok(Node::mk_vacuum_stmt(mcx, read_vacuum_stmt(mcx)?)))(),
        b"VACUUMRELATION" => (|| Ok(Node::mk_vacuum_relation(mcx, read_vacuum_relation(mcx)?)))(),
        b"EXPLAINSTMT" => (|| Ok(Node::mk_explain_stmt(mcx, read_explain_stmt(mcx)?)))(),
        b"CREATETABLEASSTMT" => (|| Ok(Node::mk_create_table_as_stmt(mcx, read_create_table_as_stmt(mcx)?)))(),
        b"REFRESHMATVIEWSTMT" => (|| Ok(Node::mk_refresh_mat_view_stmt(mcx, read_refresh_mat_view_stmt(mcx)?)))(),
        b"CHECKPOINTSTMT" => (|| Ok(Node::mk_check_point_stmt(mcx, read_check_point_stmt(mcx)?)))(),
        b"DISCARDSTMT" => (|| Ok(Node::mk_discard_stmt(mcx, read_discard_stmt(mcx)?)))(),
        b"LOCKSTMT" => (|| Ok(Node::mk_lock_stmt(mcx, read_lock_stmt(mcx)?)))(),
        b"CONSTRAINTSSETSTMT" => (|| Ok(Node::mk_constraints_set_stmt(mcx, read_constraints_set_stmt(mcx)?)))(),
        b"REINDEXSTMT" => (|| Ok(Node::mk_reindex_stmt(mcx, read_reindex_stmt(mcx)?)))(),
        b"CREATECONVERSIONSTMT" => (|| Ok(Node::mk_create_conversion_stmt(mcx, read_create_conversion_stmt(mcx)?)))(),
        b"CREATECASTSTMT" => (|| Ok(Node::mk_create_cast_stmt(mcx, read_create_cast_stmt(mcx)?)))(),
        b"CREATETRANSFORMSTMT" => (|| Ok(Node::mk_create_transform_stmt(mcx, read_create_transform_stmt(mcx)?)))(),
        b"PREPARESTMT" => (|| Ok(Node::mk_prepare_stmt(mcx, read_prepare_stmt(mcx)?)))(),
        b"EXECUTESTMT" => (|| Ok(Node::mk_execute_stmt(mcx, read_execute_stmt(mcx)?)))(),
        b"DEALLOCATESTMT" => (|| Ok(Node::mk_deallocate_stmt(mcx, read_deallocate_stmt(mcx)?)))(),
        b"DROPOWNEDSTMT" => (|| Ok(Node::mk_drop_owned_stmt(mcx, read_drop_owned_stmt(mcx)?)))(),
        b"REASSIGNOWNEDSTMT" => (|| Ok(Node::mk_reassign_owned_stmt(mcx, read_reassign_owned_stmt(mcx)?)))(),
        b"ALTERTSDICTIONARYSTMT" => (|| Ok(Node::mk_alter_ts_dictionary_stmt(mcx, read_alter_t_s_dictionary_stmt(mcx)?)))(),
        b"ALTERTSCONFIGURATIONSTMT" => (|| Ok(Node::mk_alter_ts_configuration_stmt(mcx, read_alter_t_s_configuration_stmt(mcx)?)))(),
        b"PUBLICATIONTABLE" => (|| Ok(Node::mk_publication_table(mcx, read_publication_table(mcx)?)))(),
        b"PUBLICATIONOBJSPEC" => (|| Ok(Node::mk_publication_obj_spec(mcx, read_publication_obj_spec(mcx)?)))(),
        b"CREATEPUBLICATIONSTMT" => (|| Ok(Node::mk_create_publication_stmt(mcx, read_create_publication_stmt(mcx)?)))(),
        b"ALTERPUBLICATIONSTMT" => (|| Ok(Node::mk_alter_publication_stmt(mcx, read_alter_publication_stmt(mcx)?)))(),
        b"CREATESUBSCRIPTIONSTMT" => (|| Ok(Node::mk_create_subscription_stmt(mcx, read_create_subscription_stmt(mcx)?)))(),
        b"ALTERSUBSCRIPTIONSTMT" => (|| Ok(Node::mk_alter_subscription_stmt(mcx, read_alter_subscription_stmt(mcx)?)))(),
        b"DROPSUBSCRIPTIONSTMT" => (|| Ok(Node::mk_drop_subscription_stmt(mcx, read_drop_subscription_stmt(mcx)?)))(),
        _ => return None,
    };
    Some(node)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::string::{String, ToString};
    use mcx::MemoryContext;
    use backend_nodes_core::read::string_to_node;
    use backend_nodes_outfuncs::nodeToString;
    use types_nodes::nodes::Node;
    use types_nodes::ddlnodes as dn;

    use crate::ensure_seams_for_tests as ensure_seams;

    /// OUT a framed DDL node, READ it back, assert byte-stable re-serialization
    /// (`nodeToString -> string_to_node -> nodeToString`).
    fn rt(node: &Node<'_>) -> String {
        ensure_seams();
        let ctx = MemoryContext::new("ddl-roundtrip");
        let mcx = ctx.mcx();
        let text = nodeToString(mcx, node).expect("nodeToString");
        let parsed = string_to_node(mcx, text.as_str()).expect("string_to_node");
        let text2 = nodeToString(mcx, &parsed).expect("re-serialize");
        assert_eq!(text.as_str(), text2.as_str(), "ddl re-serialize stable: {}", text.as_str());
        text.as_str().to_string()
    }

    #[test]
    fn defelem_round_trips() {
        let ctx = MemoryContext::new("de");
        let mcx = ctx.mcx();
        let de = dn::DefElem {
            defnamespace: None,
            defname: Some(PgString::from_str_in("oids", mcx).unwrap()),
            arg: None,
            defaction: dn::DefElemAction::DEFELEM_SET,
            location: -1,
        };
        let text = rt(&Node::mk_def_elem(mcx, de));
        assert!(text.starts_with("{DEFELEM :defnamespace <> :defname oids"), "{}", text);
        assert!(text.contains(":defaction 1"), "{}", text);
    }

    #[test]
    fn rolespec_round_trips() {
        let ctx = MemoryContext::new("rs");
        let mcx = ctx.mcx();
        let rs = dn::RoleSpec {
            roletype: types_nodes::parsenodes::RoleSpecType::Cstring,
            rolename: Some(PgString::from_str_in("alice", mcx).unwrap()),
            location: -1,
        };
        let text = rt(&Node::mk_role_spec(mcx, rs));
        assert!(text.starts_with("{ROLESPEC :roletype 0 :rolename alice"), "{}", text);
    }

    #[test]
    fn dropstmt_round_trips() {
        let ctx = MemoryContext::new("drop");
        let mcx = ctx.mcx();
        let ds = dn::DropStmt {
            objects: PgVec::new_in(mcx),
            removeType: types_nodes::parsenodes::ObjectType::Table,
            behavior: types_nodes::parsenodes::DropBehavior::Restrict,
            missing_ok: true,
            concurrent: false,
        };
        let text = rt(&Node::mk_drop_stmt(mcx, ds));
        assert!(text.starts_with("{DROPSTMT :objects <>"), "{}", text);
        assert!(text.contains(":behavior 0 :missing_ok true :concurrent false"), "{}", text);
    }

    #[test]
    fn notifystmt_round_trips() {
        let ctx = MemoryContext::new("notify");
        let mcx = ctx.mcx();
        let ns = dn::NotifyStmt {
            conditionname: Some(PgString::from_str_in("chan", mcx).unwrap()),
            payload: None,
        };
        let text = rt(&Node::mk_notify_stmt(mcx, ns));
        assert!(text.starts_with("{NOTIFYSTMT :conditionname chan :payload <>"), "{}", text);
    }

    #[test]
    fn partitionboundspec_round_trips() {
        // CHAR field (strategy), bools, ints, NIL node-lists.
        let ctx = MemoryContext::new("pbs");
        let mcx = ctx.mcx();
        let pbs = dn::PartitionBoundSpec {
            strategy: b'h' as i8,
            is_default: false,
            modulus: 4,
            remainder: 1,
            listdatums: PgVec::new_in(mcx),
            lowerdatums: PgVec::new_in(mcx),
            upperdatums: PgVec::new_in(mcx),
            location: -1,
        };
        let text = rt(&Node::mk_partition_bound_spec(mcx, pbs));
        assert!(text.starts_with("{PARTITIONBOUNDSPEC :strategy h :is_default false :modulus 4 :remainder 1"), "{}", text);
    }
}
