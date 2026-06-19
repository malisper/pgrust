//! Bridge from the canonical `'mcx`-arena parse nodes
//! (`types_nodes::ddlnodes`) that flow through utility dispatch to the owned
//! `types_parsenodes` role-statement model that `CreateRole`/`AlterRole`/
//! `AlterRoleSet`/`DropRole`/`ReassignOwnedObjects` consume.
//!
//! The two parse-node universes meet only at the utility-dispatch seam. The C
//! has one `CreateRoleStmt`; this repo carries an arena copy (built by the
//! grammar) and an owned copy (the command driver's vocabulary). We convert the
//! arena node into the owned form here, exactly as
//! `backend-catalog-pg-db-role-setting` does for `ALTER ROLE … SET`.

use types_error::PgResult;
use types_nodes::nodes::{ntag, Node as ANode};

use types_parsenodes as pn;

/// `elog(ERROR, "unrecognized node type: %d", nodeTag(node))` — a node shape the
/// owned role-statement model does not carry (an option arg or member node the
/// grammar would never actually produce here).
fn unrecognized(node: &ANode<'_>) -> types_error::PgError {
    backend_utils_error::ereport(types_error::ERROR)
        .errmsg_internal(format!("unrecognized node type: {}", node.node_tag().0))
        .into_error()
}

/// Convert one arena value/`RoleSpec`/`DefElem`/`AccessPriv`/`List` node to the
/// owned `types_parsenodes::Node`. Covers exactly the node kinds the role
/// statements' option lists, member lists, and `DefElem` args contain.
fn node_to_owned(node: &ANode<'_>) -> PgResult<pn::Node> {
    match node.node_tag() {
        ntag::T_Integer => Ok(pn::Node::Integer(pn::Integer {
            ival: node.expect_integer().ival,
        })),
        ntag::T_Float => Ok(pn::Node::Float(pn::Float {
            fval: Some(node.expect_float().fval.as_str().to_string()),
        })),
        ntag::T_Boolean => Ok(pn::Node::Boolean(pn::Boolean {
            boolval: node.expect_boolean().boolval,
        })),
        ntag::T_String => Ok(pn::Node::String(pn::StringNode {
            sval: Some(node.expect_string().sval.as_str().to_string()),
        })),
        ntag::T_BitString => Ok(pn::Node::BitString(pn::BitString {
            bsval: Some(node.expect_bitstring().bsval.as_str().to_string()),
        })),
        ntag::T_RoleSpec => Ok(pn::Node::RoleSpec(role_spec_to_owned(node.expect_rolespec()))),
        ntag::T_DefElem => Ok(pn::Node::DefElem(def_elem_to_owned(node.expect_defelem())?)),
        ntag::T_AccessPriv => Ok(pn::Node::AccessPriv(access_priv_to_owned(
            node.expect_accesspriv(),
        )?)),
        ntag::T_List => {
            let mut out = Vec::with_capacity(node.expect_list().len());
            for e in node.expect_list().as_slice() {
                out.push(node_to_owned(e)?);
            }
            Ok(pn::Node::List(out))
        }
        _ => Err(unrecognized(node)),
    }
}

fn role_spec_to_owned(rs: &types_nodes::ddlnodes::RoleSpec<'_>) -> pn::RoleSpec {
    use types_nodes::parsenodes::RoleSpecType as A;
    let roletype = match rs.roletype {
        A::Cstring => pn::RoleSpecType::ROLESPEC_CSTRING,
        A::CurrentRole => pn::RoleSpecType::ROLESPEC_CURRENT_ROLE,
        A::CurrentUser => pn::RoleSpecType::ROLESPEC_CURRENT_USER,
        A::SessionUser => pn::RoleSpecType::ROLESPEC_SESSION_USER,
        A::Public => pn::RoleSpecType::ROLESPEC_PUBLIC,
    };
    pn::RoleSpec {
        roletype,
        rolename: rs.rolename.as_ref().map(|s| s.as_str().to_string()),
        location: rs.location,
    }
}

fn def_elem_to_owned(de: &types_nodes::ddlnodes::DefElem<'_>) -> PgResult<pn::DefElem> {
    let arg = match de.arg.as_deref() {
        Some(a) => Some(Box::new(node_to_owned(a)?)),
        None => None,
    };
    Ok(pn::DefElem {
        defnamespace: de.defnamespace.as_ref().map(|s| s.as_str().to_string()),
        defname: de.defname.as_ref().map(|s| s.as_str().to_string()),
        arg,
        defaction: de.defaction,
        location: de.location,
    })
}

fn access_priv_to_owned(ap: &types_nodes::ddlnodes::AccessPriv<'_>) -> PgResult<pn::AccessPriv> {
    let mut cols = Vec::with_capacity(ap.cols.len());
    for c in ap.cols.as_slice() {
        cols.push(node_to_owned(c)?);
    }
    Ok(pn::AccessPriv {
        priv_name: ap.priv_name.as_ref().map(|s| s.as_str().to_string()),
        cols,
    })
}

fn options_to_owned(opts: &[types_nodes::nodes::NodePtr<'_>]) -> PgResult<Vec<pn::Node>> {
    let mut out = Vec::with_capacity(opts.len());
    for o in opts {
        out.push(node_to_owned(o)?);
    }
    Ok(out)
}

pub fn create_role_stmt_to_owned(
    s: &types_nodes::ddlnodes::CreateRoleStmt<'_>,
) -> PgResult<pn::CreateRoleStmt> {
    Ok(pn::CreateRoleStmt {
        stmt_type: s.stmt_type,
        role: s.role.as_ref().map(|r| r.as_str().to_string()),
        options: options_to_owned(s.options.as_slice())?,
    })
}

pub fn alter_role_stmt_to_owned(
    s: &types_nodes::ddlnodes::AlterRoleStmt<'_>,
) -> PgResult<pn::AlterRoleStmt> {
    let role = match s.role.as_deref() {
        Some(r) => Some(Box::new(node_to_owned(r)?)),
        None => None,
    };
    Ok(pn::AlterRoleStmt {
        role,
        options: options_to_owned(s.options.as_slice())?,
        action: s.action,
    })
}

pub fn alter_role_set_stmt_to_owned(
    s: &types_nodes::ddlnodes::AlterRoleSetStmt<'_>,
) -> PgResult<pn::AlterRoleSetStmt> {
    let role = match s.role.as_deref() {
        Some(r) => Some(Box::new(node_to_owned(r)?)),
        None => None,
    };
    let setstmt = match s.setstmt.as_deref() {
        Some(v) => Some(Box::new(variable_set_stmt_to_owned(v)?)),
        None => None,
    };
    Ok(pn::AlterRoleSetStmt {
        role,
        database: s.database.as_ref().map(|d| d.as_str().to_string()),
        setstmt,
    })
}

pub fn drop_role_stmt_to_owned(
    s: &types_nodes::ddlnodes::DropRoleStmt<'_>,
) -> PgResult<pn::DropRoleStmt> {
    let mut roles = Vec::with_capacity(s.roles.len());
    for r in s.roles.as_slice() {
        roles.push(node_to_owned(r)?);
    }
    Ok(pn::DropRoleStmt {
        roles,
        missing_ok: s.missing_ok,
    })
}

pub fn reassign_owned_stmt_to_owned(
    s: &types_nodes::ddlnodes::ReassignOwnedStmt<'_>,
) -> PgResult<pn::ReassignOwnedStmt> {
    let mut roles = Vec::with_capacity(s.roles.len());
    for r in s.roles.as_slice() {
        roles.push(node_to_owned(r)?);
    }
    let newrole = match s.newrole.as_deref() {
        Some(r) => Some(Box::new(node_to_owned(r)?)),
        None => None,
    };
    Ok(pn::ReassignOwnedStmt { roles, newrole })
}

pub fn grant_role_stmt_to_owned(
    s: &types_nodes::ddlnodes::GrantRoleStmt<'_>,
) -> PgResult<pn::GrantRoleStmt> {
    let mut granted_roles = Vec::with_capacity(s.granted_roles.len());
    for r in s.granted_roles.as_slice() {
        granted_roles.push(node_to_owned(r)?);
    }
    let mut grantee_roles = Vec::with_capacity(s.grantee_roles.len());
    for r in s.grantee_roles.as_slice() {
        grantee_roles.push(node_to_owned(r)?);
    }
    let mut opt = Vec::with_capacity(s.opt.len());
    for o in s.opt.as_slice() {
        opt.push(node_to_owned(o)?);
    }
    let grantor = match s.grantor.as_deref() {
        Some(r) => Some(Box::new(node_to_owned(r)?)),
        None => None,
    };
    Ok(pn::GrantRoleStmt {
        granted_roles,
        grantee_roles,
        is_grant: s.is_grant,
        opt,
        grantor,
        behavior: s.behavior,
    })
}

pub fn drop_owned_stmt_to_owned(
    s: &types_nodes::ddlnodes::DropOwnedStmt<'_>,
) -> PgResult<pn::DropOwnedStmt> {
    let mut roles = Vec::with_capacity(s.roles.len());
    for r in s.roles.as_slice() {
        roles.push(node_to_owned(r)?);
    }
    Ok(pn::DropOwnedStmt {
        roles,
        behavior: s.behavior,
    })
}

/// Convert an arena `VariableSetStmt` (the `setstmt` of `ALTER ROLE … SET`) to
/// the owned model. `AlterRoleSet` only forwards it to `AlterSetting`; the value
/// extraction over `args` (`A_Const` nodes) is the GUC owner's concern, so the
/// args are carried in owned-value form.
fn variable_set_stmt_to_owned(node: &ANode<'_>) -> PgResult<pn::Node> {
    use types_nodes::ddlnodes::VariableSetKind as A;
    if node.node_tag() != ntag::T_VariableSetStmt {
        return Err(unrecognized(node));
    }
    let v = node.expect_variablesetstmt();
    let kind = match v.kind {
        A::VAR_SET_VALUE => pn::VariableSetKind::SetValue,
        A::VAR_SET_DEFAULT => pn::VariableSetKind::SetDefault,
        A::VAR_SET_CURRENT => pn::VariableSetKind::SetCurrent,
        A::VAR_SET_MULTI => pn::VariableSetKind::SetMulti,
        A::VAR_RESET => pn::VariableSetKind::Reset,
        A::VAR_RESET_ALL => pn::VariableSetKind::ResetAll,
    };
    let mut args = Vec::with_capacity(v.args.len());
    for a in v.args.as_slice() {
        args.push(set_arg_to_owned(a)?);
    }
    Ok(pn::Node::VariableSetStmt(pn::VariableSetStmt {
        kind,
        name: v.name.as_ref().map(|s| s.as_str().to_string()),
        args,
        is_local: v.is_local,
        location: v.location,
    }))
}

/// One `VariableSetStmt.args` element — an `A_Const` (unwrapped to its inner
/// value node) or a bare value node, mirroring `db-role-setting`'s
/// `arena_arg_to_owned`.
fn set_arg_to_owned(arg: &ANode<'_>) -> PgResult<pn::Node> {
    let val = match arg.node_tag() {
        ntag::T_A_Const => match arg.expect_a_const().val.as_deref() {
            Some(v) => v,
            None => return Err(unrecognized(arg)),
        },
        _ => arg,
    };
    node_to_owned(val)
}
