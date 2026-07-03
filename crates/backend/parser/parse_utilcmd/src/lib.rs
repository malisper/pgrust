// CREATE TABLE plain-column lane + the parse_type.c slice it needs.
#![allow(non_snake_case)]

use mcx::Mcx;
use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult, ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_OBJECT, ERROR};
use types_nodes::rawnodes::{
    ColumnDef, Constraint, ConstrType, CreateStmt, IndexElem, IndexStmt, SortByDir, SortByNulls,
    TypeName,
};
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
// misc.c's pg_input_* callers pass NULL there too. pts_error_callback's
// CONTEXT line is not attached (divergence).
fn typeStringToTypeName<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx TypeName<'mcx>> {
    if s.bytes().all(|c| matches!(c, b' ' | b'\t' | b'\n' | b'\r' | 0x0c | 0x0b)) {
        return Err(invalid_type_name(s));
    }
    let list = gram_core::raw_parser(mcx, s, parser_seams::RawParseMode::RAW_PARSE_TYPE_NAME)?;
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

fn transformColumnDefinition<'mcx>(
    mcx: Mcx<'mcx>,
    column_node: Node<'mcx>,
    column: &ColumnDef<'mcx>,
    relname: &str,
    ckconstraints: &mut NodeList<'mcx>,
    nnconstraints: &mut NodeList<'mcx>,
    ixconstraints: &mut NodeList<'mcx>,
    fkconstraints: &mut NodeList<'mcx>,
) -> PgResult<()> {
    if column.raw_default.is_some() || column.cooked_default.is_some() {
        unported("pre-split column defaults");
    }
    let mut saw_default = false;
    let mut saw_nullable = false;
    let mut need_notnull = false;
    let mut col_not_null = column.is_not_null;
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
                ConstrType::CONSTR_IDENTITY | ConstrType::CONSTR_GENERATED => {
                    "identity/generated column constraints"
                }
                _ => "constraint attributes (transformConstraintAttrs)",
            }),
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
    debug_assert!(stmt.constraints.is_nil() && stmt.nnconstraints.is_nil());

    let relation = stmt.relation.expect("CreateStmt.relation");
    let relname = relation.relname.unwrap_or("");
    let mut columns = NodeList::nil();
    let mut ckconstraints = NodeList::nil();
    let mut nnconstraints = NodeList::nil();
    let mut ixconstraints = NodeList::nil();
    let mut fkconstraints = NodeList::nil();
    let mut alist = NodeList::nil();
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
                    &mut ixconstraints,
                    &mut fkconstraints,
                )?;
                columns.lappend(mcx, elt)?;
            }
            NodeTag::T_TableLikeClause => unported("LIKE clauses"),
            NodeTag::T_Constraint => {
                let c = elt.as_variant::<Constraint>().expect("Constraint");
                match c.contype {
                    ConstrType::CONSTR_PRIMARY | ConstrType::CONSTR_UNIQUE => {
                        ixconstraints.lappend(mcx, elt)?
                    }
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

    let mut result = NodeList::make1(mcx, stmt_node)?;
    for a in alist.iter() {
        result.lappend(mcx, a)?;
    }
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
) -> PgResult<()> {
    let mut indexlist = NodeList::nil();
    let mut pkey: Option<Node<'mcx>> = None;
    for cnode in ixconstraints.iter() {
        let constraint = cnode.as_variant::<Constraint>().expect("Constraint");
        debug_assert!(matches!(
            constraint.contype,
            ConstrType::CONSTR_PRIMARY | ConstrType::CONSTR_UNIQUE
        ));
        if constraint.indexname.is_some() {
            unported("transformIndexConstraint: USING INDEX (ExistingIndex)");
        }
        if constraint.deferrable || constraint.initdeferred {
            unported("transformIndexConstraint: DEFERRABLE constraint indexes");
        }

        let mut index = Node::build::<IndexStmt>(mcx)?;
        index.unique = true;
        index.primary = constraint.contype == ConstrType::CONSTR_PRIMARY;
        if index.primary {
            if pkey.is_some() {
                return Err(multiple_pkeys(relname, constraint.location));
            }
        }
        index.nulls_not_distinct = constraint.nulls_not_distinct;
        index.isconstraint = true;
        index.idxname = constraint.conname;
        index.relation = Some(relation);
        index.accessMethod = Some("btree");
        // SAFETY: parse tree is analyze-owned; the constraint node's options
        // list moves onto the IndexStmt (C shares the pointer).
        index.options =
            unsafe { cnode.with_mut::<Constraint, _>(|c| core::mem::take(&mut c.options)) }
                .expect("Constraint");
        index.tableSpace = constraint.indexspace;
        if !constraint.including.is_nil() {
            unported("transformIndexConstraint: INCLUDE columns");
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
            if index_params_equal(&index.indexParams, &prior.indexParams)
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

fn index_params_equal(a: &NodeList<'_>, b: &NodeList<'_>) -> bool {
    if a.len() != b.len() {
        return false;
    }
    for (x, y) in a.iter().zip(b.iter()) {
        let xe = x.as_variant::<IndexElem>().expect("IndexElem");
        let ye = y.as_variant::<IndexElem>().expect("IndexElem");
        if xe.name != ye.name
            || xe.ordering != ye.ordering
            || xe.nulls_ordering != ye.nulls_ordering
        {
            return false;
        }
    }
    true
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
            transformColumnDefinition(
                mcx,
                defnode,
                cd,
                relname,
                &mut ckconstraints,
                &mut nnconstraints,
                &mut ixconstraints,
                &mut fkconstraints,
            )?;
            if !ixconstraints.is_nil() || !fkconstraints.is_nil() {
                unported("ALTER TABLE ADD COLUMN with PRIMARY KEY/UNIQUE/REFERENCES");
            }
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
