// DDL "CREATE" family converters (included into convert.rs).
//
// c2rust DDL statement/helper structs live in `cd`
// (backend_nodes_types::parsenodes_ddl); the owned targets in `tdn`
// (types_nodes::ddlnodes). The uniform 5-rule mapping applies (see convert.rs).

use backend_nodes_types::parsenodes_ddl as cd;
use types_nodes::ddlnodes as tdn;

// ---------------------------------------------------------------------------
// Small enum converters (raw c2rust c_uint/c_int typedef -> owned #[repr] enum)
// ---------------------------------------------------------------------------

fn object_type(v: cd::ObjectType) -> tn_pn::ObjectType {
    use tn_pn::ObjectType::*;
    match v {
        cd::OBJECT_ACCESS_METHOD => AccessMethod,
        cd::OBJECT_AGGREGATE => Aggregate,
        cd::OBJECT_AMOP => Amop,
        cd::OBJECT_AMPROC => Amproc,
        cd::OBJECT_ATTRIBUTE => Attribute,
        cd::OBJECT_CAST => Cast,
        cd::OBJECT_COLUMN => Column,
        cd::OBJECT_COLLATION => Collation,
        cd::OBJECT_CONVERSION => Conversion,
        cd::OBJECT_DATABASE => Database,
        cd::OBJECT_DEFAULT => Default,
        cd::OBJECT_DEFACL => Defacl,
        cd::OBJECT_DOMAIN => Domain,
        cd::OBJECT_DOMCONSTRAINT => Domconstraint,
        cd::OBJECT_EVENT_TRIGGER => EventTrigger,
        cd::OBJECT_EXTENSION => Extension,
        cd::OBJECT_FDW => Fdw,
        cd::OBJECT_FOREIGN_SERVER => ForeignServer,
        cd::OBJECT_FOREIGN_TABLE => ForeignTable,
        cd::OBJECT_FUNCTION => Function,
        cd::OBJECT_INDEX => Index,
        cd::OBJECT_LANGUAGE => Language,
        cd::OBJECT_LARGEOBJECT => Largeobject,
        cd::OBJECT_MATVIEW => Matview,
        cd::OBJECT_OPCLASS => Opclass,
        cd::OBJECT_OPERATOR => Operator,
        cd::OBJECT_OPFAMILY => Opfamily,
        cd::OBJECT_PARAMETER_ACL => ParameterAcl,
        cd::OBJECT_POLICY => Policy,
        cd::OBJECT_PROCEDURE => Procedure,
        cd::OBJECT_PUBLICATION => Publication,
        cd::OBJECT_PUBLICATION_NAMESPACE => PublicationNamespace,
        cd::OBJECT_PUBLICATION_REL => PublicationRel,
        cd::OBJECT_ROLE => Role,
        cd::OBJECT_ROUTINE => Routine,
        cd::OBJECT_RULE => Rule,
        cd::OBJECT_SCHEMA => Schema,
        cd::OBJECT_SEQUENCE => Sequence,
        cd::OBJECT_SUBSCRIPTION => Subscription,
        cd::OBJECT_STATISTIC_EXT => StatisticExt,
        cd::OBJECT_TABCONSTRAINT => Tabconstraint,
        cd::OBJECT_TABLE => Table,
        cd::OBJECT_TABLESPACE => Tablespace,
        cd::OBJECT_TRANSFORM => Transform,
        cd::OBJECT_TRIGGER => Trigger,
        cd::OBJECT_TSCONFIGURATION => TsConfiguration,
        cd::OBJECT_TSDICTIONARY => TsDictionary,
        cd::OBJECT_TSPARSER => TsParser,
        cd::OBJECT_TSTEMPLATE => TsTemplate,
        cd::OBJECT_TYPE => Type,
        cd::OBJECT_USER_MAPPING => UserMapping,
        cd::OBJECT_VIEW => View,
        other => panic!("gram converter: invalid ObjectType {other}"),
    }
}

fn on_commit_action(v: cpr::OnCommitAction) -> tn_prim::OnCommitAction {
    use tn_prim::OnCommitAction::*;
    match v {
        cpr::ONCOMMIT_NOOP => ONCOMMIT_NOOP,
        cpr::ONCOMMIT_PRESERVE_ROWS => ONCOMMIT_PRESERVE_ROWS,
        cpr::ONCOMMIT_DELETE_ROWS => ONCOMMIT_DELETE_ROWS,
        cpr::ONCOMMIT_DROP => ONCOMMIT_DROP,
        other => panic!("gram converter: invalid OnCommitAction {other}"),
    }
}

fn role_spec_type(v: cd::RoleSpecType) -> tn_pn::RoleSpecType {
    use tn_pn::RoleSpecType::*;
    match v {
        cd::ROLESPEC_CSTRING => Cstring,
        cd::ROLESPEC_CURRENT_ROLE => CurrentRole,
        cd::ROLESPEC_CURRENT_USER => CurrentUser,
        cd::ROLESPEC_SESSION_USER => SessionUser,
        cd::ROLESPEC_PUBLIC => Public,
        other => panic!("gram converter: invalid RoleSpecType {other}"),
    }
}

fn constr_type(v: cd::ConstrType) -> tdn::ConstrType {
    use tdn::ConstrType::*;
    match v {
        cd::CONSTR_NULL => CONSTR_NULL,
        cd::CONSTR_NOTNULL => CONSTR_NOTNULL,
        cd::CONSTR_DEFAULT => CONSTR_DEFAULT,
        cd::CONSTR_IDENTITY => CONSTR_IDENTITY,
        cd::CONSTR_GENERATED => CONSTR_GENERATED,
        cd::CONSTR_CHECK => CONSTR_CHECK,
        cd::CONSTR_PRIMARY => CONSTR_PRIMARY,
        cd::CONSTR_UNIQUE => CONSTR_UNIQUE,
        cd::CONSTR_EXCLUSION => CONSTR_EXCLUSION,
        cd::CONSTR_FOREIGN => CONSTR_FOREIGN,
        cd::CONSTR_ATTR_DEFERRABLE => CONSTR_ATTR_DEFERRABLE,
        cd::CONSTR_ATTR_NOT_DEFERRABLE => CONSTR_ATTR_NOT_DEFERRABLE,
        cd::CONSTR_ATTR_DEFERRED => CONSTR_ATTR_DEFERRED,
        cd::CONSTR_ATTR_IMMEDIATE => CONSTR_ATTR_IMMEDIATE,
        cd::CONSTR_ATTR_ENFORCED => CONSTR_ATTR_ENFORCED,
        cd::CONSTR_ATTR_NOT_ENFORCED => CONSTR_ATTR_NOT_ENFORCED,
        other => panic!("gram converter: invalid ConstrType {other}"),
    }
}

fn def_elem_action(v: cd::DefElemAction) -> tdn::DefElemAction {
    use tdn::DefElemAction::*;
    match v {
        cd::DEFELEM_UNSPEC => DEFELEM_UNSPEC,
        cd::DEFELEM_SET => DEFELEM_SET,
        cd::DEFELEM_ADD => DEFELEM_ADD,
        cd::DEFELEM_DROP => DEFELEM_DROP,
        other => panic!("gram converter: invalid DefElemAction {other}"),
    }
}

fn function_parameter_mode(v: cd::FunctionParameterMode) -> tdn::FunctionParameterMode {
    use tdn::FunctionParameterMode::*;
    match v {
        cd::FUNC_PARAM_IN => FUNC_PARAM_IN,
        cd::FUNC_PARAM_OUT => FUNC_PARAM_OUT,
        cd::FUNC_PARAM_INOUT => FUNC_PARAM_INOUT,
        cd::FUNC_PARAM_VARIADIC => FUNC_PARAM_VARIADIC,
        cd::FUNC_PARAM_TABLE => FUNC_PARAM_TABLE,
        cd::FUNC_PARAM_DEFAULT => FUNC_PARAM_DEFAULT,
        other => panic!("gram converter: invalid FunctionParameterMode {other}"),
    }
}

fn role_stmt_type(v: cd::RoleStmtType) -> tdn::RoleStmtType {
    use tdn::RoleStmtType::*;
    match v {
        cd::ROLESTMT_ROLE => ROLESTMT_ROLE,
        cd::ROLESTMT_USER => ROLESTMT_USER,
        cd::ROLESTMT_GROUP => ROLESTMT_GROUP,
        other => panic!("gram converter: invalid RoleStmtType {other}"),
    }
}

fn coercion_context(v: cpr::CoercionContext) -> tdn::CoercionContext {
    use tdn::CoercionContext::*;
    match v {
        cpr::COERCION_IMPLICIT => COERCION_IMPLICIT,
        cpr::COERCION_ASSIGNMENT => COERCION_ASSIGNMENT,
        cpr::COERCION_PLPGSQL => COERCION_PLPGSQL,
        cpr::COERCION_EXPLICIT => COERCION_EXPLICIT,
        other => panic!("gram converter: invalid CoercionContext {other}"),
    }
}

fn view_check_option(v: cd::ViewCheckOption) -> tdn::ViewCheckOption {
    use tdn::ViewCheckOption::*;
    match v {
        cd::NO_CHECK_OPTION => NO_CHECK_OPTION,
        cd::LOCAL_CHECK_OPTION => LOCAL_CHECK_OPTION,
        cd::CASCADED_CHECK_OPTION => CASCADED_CHECK_OPTION,
        other => panic!("gram converter: invalid ViewCheckOption {other}"),
    }
}

fn partition_strategy(v: cd::PartitionStrategy) -> tn_part::PartitionStrategy {
    use tn_part::PartitionStrategy::*;
    match v {
        cd::PARTITION_STRATEGY_LIST => List,
        cd::PARTITION_STRATEGY_RANGE => Range,
        cd::PARTITION_STRATEGY_HASH => Hash,
        other => panic!("gram converter: invalid PartitionStrategy {other}"),
    }
}

fn partition_range_datum_kind(v: cd::PartitionRangeDatumKind) -> tn_part::PartitionRangeDatumKind {
    use tn_part::PartitionRangeDatumKind::*;
    match v {
        cd::PARTITION_RANGE_DATUM_MINVALUE => MinValue,
        cd::PARTITION_RANGE_DATUM_VALUE => Value,
        cd::PARTITION_RANGE_DATUM_MAXVALUE => MaxValue,
        other => panic!("gram converter: invalid PartitionRangeDatumKind {other}"),
    }
}

// ---------------------------------------------------------------------------
// Supporting / helper nodes
// ---------------------------------------------------------------------------

fn conv_rolespec<'mcx>(mcx: Mcx<'mcx>, p: *mut cd::RoleSpec) -> PgResult<tdn::RoleSpec<'mcx>> {
    let r = unsafe { &*p };
    Ok(tdn::RoleSpec {
        roletype: role_spec_type(r.roletype),
        rolename: cstr_opt(mcx, r.rolename)?,
        location: r.location,
    })
}

fn conv_defelem<'mcx>(mcx: Mcx<'mcx>, p: *mut cd::DefElem) -> PgResult<tdn::DefElem<'mcx>> {
    let d = unsafe { &*p };
    Ok(tdn::DefElem {
        defnamespace: cstr_opt(mcx, d.defnamespace)?,
        defname: cstr_opt(mcx, d.defname)?,
        arg: node_opt(mcx, d.arg)?,
        defaction: def_elem_action(d.defaction),
        location: d.location,
    })
}

fn conv_constraint<'mcx>(mcx: Mcx<'mcx>, p: *mut cd::Constraint) -> PgResult<tdn::Constraint<'mcx>> {
    let c = unsafe { &*p };
    Ok(tdn::Constraint {
        contype: constr_type(c.contype),
        conname: cstr_opt(mcx, c.conname)?,
        deferrable: c.deferrable,
        initdeferred: c.initdeferred,
        is_enforced: c.is_enforced,
        skip_validation: c.skip_validation,
        initially_valid: c.initially_valid,
        is_no_inherit: c.is_no_inherit,
        raw_expr: node_opt(mcx, c.raw_expr)?,
        cooked_expr: cstr_opt(mcx, c.cooked_expr)?,
        generated_when: c.generated_when as i8,
        generated_kind: c.generated_kind as i8,
        nulls_not_distinct: c.nulls_not_distinct,
        keys: node_list(mcx, c.keys)?,
        without_overlaps: c.without_overlaps,
        including: node_list(mcx, c.including)?,
        exclusions: node_list(mcx, c.exclusions)?,
        options: node_list(mcx, c.options)?,
        indexname: cstr_opt(mcx, c.indexname)?,
        indexspace: cstr_opt(mcx, c.indexspace)?,
        reset_default_tblspc: c.reset_default_tblspc,
        access_method: cstr_opt(mcx, c.access_method)?,
        where_clause: node_opt(mcx, c.where_clause)?,
        pktable: child_node_opt(mcx, c.pktable)?,
        fk_attrs: node_list(mcx, c.fk_attrs)?,
        pk_attrs: node_list(mcx, c.pk_attrs)?,
        fk_with_period: c.fk_with_period,
        pk_with_period: c.pk_with_period,
        fk_matchtype: c.fk_matchtype as i8,
        fk_upd_action: c.fk_upd_action as i8,
        fk_del_action: c.fk_del_action as i8,
        fk_del_set_cols: node_list(mcx, c.fk_del_set_cols)?,
        old_conpfeqop: node_list(mcx, c.old_conpfeqop)?,
        old_pktable_oid: c.old_pktable_oid,
        location: c.location,
    })
}

fn conv_tablelikeclause<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::TableLikeClause,
) -> PgResult<tdn::TableLikeClause<'mcx>> {
    let t = unsafe { &*p };
    Ok(tdn::TableLikeClause {
        relation: child_node_opt(mcx, t.relation)?,
        options: t.options,
        relationOid: t.relationOid,
    })
}

fn conv_indexelem<'mcx>(mcx: Mcx<'mcx>, p: *mut cd::IndexElem) -> PgResult<tdn::IndexElem<'mcx>> {
    let e = unsafe { &*p };
    Ok(tdn::IndexElem {
        name: cstr_opt(mcx, e.name)?,
        expr: node_opt(mcx, e.expr)?,
        indexcolname: cstr_opt(mcx, e.indexcolname)?,
        collation: node_list(mcx, e.collation)?,
        opclass: node_list(mcx, e.opclass)?,
        opclassopts: node_list(mcx, e.opclassopts)?,
        ordering: sort_by_dir(e.ordering),
        nulls_ordering: sort_by_nulls(e.nulls_ordering),
    })
}

fn conv_functionparameter<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::FunctionParameter,
) -> PgResult<tdn::FunctionParameter<'mcx>> {
    let f = unsafe { &*p };
    Ok(tdn::FunctionParameter {
        name: cstr_opt(mcx, f.name)?,
        argType: child_node_opt(mcx, f.argType)?,
        mode: function_parameter_mode(f.mode),
        defexpr: node_opt(mcx, f.defexpr)?,
        location: f.location,
    })
}

fn conv_objectwithargs<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::ObjectWithArgs,
) -> PgResult<tdn::ObjectWithArgs<'mcx>> {
    let o = unsafe { &*p };
    Ok(tdn::ObjectWithArgs {
        objname: node_list(mcx, o.objname)?,
        objargs: node_list(mcx, o.objargs)?,
        objfuncargs: node_list(mcx, o.objfuncargs)?,
        args_unspecified: o.args_unspecified,
    })
}

fn conv_accesspriv<'mcx>(mcx: Mcx<'mcx>, p: *mut cd::AccessPriv) -> PgResult<tdn::AccessPriv<'mcx>> {
    let a = unsafe { &*p };
    Ok(tdn::AccessPriv {
        priv_name: cstr_opt(mcx, a.priv_name)?,
        cols: node_list(mcx, a.cols)?,
    })
}

fn conv_createopclassitem<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::CreateOpClassItem,
) -> PgResult<tdn::CreateOpClassItem<'mcx>> {
    let i = unsafe { &*p };
    Ok(tdn::CreateOpClassItem {
        itemtype: i.itemtype,
        name: child_node_opt(mcx, i.name)?,
        number: i.number,
        order_family: node_list(mcx, i.order_family)?,
        class_args: node_list(mcx, i.class_args)?,
        storedtype: child_node_opt(mcx, i.storedtype)?,
    })
}

fn conv_statselem<'mcx>(mcx: Mcx<'mcx>, p: *mut cd::StatsElem) -> PgResult<tdn::StatsElem<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::StatsElem {
        name: cstr_opt(mcx, s.name)?,
        expr: node_opt(mcx, s.expr)?,
    })
}

fn conv_partitionelem<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::PartitionElem,
) -> PgResult<tdn::PartitionElem<'mcx>> {
    let e = unsafe { &*p };
    Ok(tdn::PartitionElem {
        name: cstr_opt(mcx, e.name)?,
        expr: node_opt(mcx, e.expr)?,
        collation: node_list(mcx, e.collation)?,
        opclass: node_list(mcx, e.opclass)?,
        location: e.location,
    })
}

fn conv_partitionspec<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::PartitionSpec,
) -> PgResult<tdn::PartitionSpec<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::PartitionSpec {
        strategy: partition_strategy(s.strategy),
        partParams: node_list(mcx, s.partParams)?,
        location: s.location,
    })
}

fn conv_partitionboundspec<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::PartitionBoundSpec,
) -> PgResult<tdn::PartitionBoundSpec<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::PartitionBoundSpec {
        strategy: s.strategy as i8,
        is_default: s.is_default,
        modulus: s.modulus,
        remainder: s.remainder,
        listdatums: node_list(mcx, s.listdatums)?,
        lowerdatums: node_list(mcx, s.lowerdatums)?,
        upperdatums: node_list(mcx, s.upperdatums)?,
        location: s.location,
    })
}

fn conv_partitionrangedatum<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::PartitionRangeDatum,
) -> PgResult<tdn::PartitionRangeDatum<'mcx>> {
    let d = unsafe { &*p };
    Ok(tdn::PartitionRangeDatum {
        kind: partition_range_datum_kind(d.kind),
        value: node_opt(mcx, d.value)?,
        location: d.location,
    })
}

fn conv_intoclause<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cpr::IntoClause,
) -> PgResult<tdn::IntoClause<'mcx>> {
    let i = unsafe { &*p };
    Ok(tdn::IntoClause {
        rel: child_node_opt(mcx, i.rel)?,
        colNames: node_list(mcx, i.col_names)?,
        accessMethod: cstr_opt(mcx, i.access_method)?,
        options: node_list(mcx, i.options)?,
        onCommit: on_commit_action(i.on_commit),
        tableSpaceName: cstr_opt(mcx, i.table_space_name)?,
        viewQuery: node_opt(mcx, i.view_query)?,
        skipData: i.skip_data,
    })
}

// ---------------------------------------------------------------------------
// CREATE-family statements
// ---------------------------------------------------------------------------

fn conv_createstmt<'mcx>(mcx: Mcx<'mcx>, p: *mut cd::CreateStmt) -> PgResult<tdn::CreateStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::CreateStmt {
        relation: child_node_opt(mcx, s.relation)?,
        tableElts: node_list(mcx, s.tableElts)?,
        inhRelations: node_list(mcx, s.inhRelations)?,
        partbound: child_node_opt(mcx, s.partbound)?,
        partspec: child_node_opt(mcx, s.partspec)?,
        ofTypename: child_node_opt(mcx, s.ofTypename)?,
        constraints: node_list(mcx, s.constraints)?,
        nnconstraints: node_list(mcx, s.nnconstraints)?,
        options: node_list(mcx, s.options)?,
        oncommit: on_commit_action(s.oncommit),
        tablespacename: cstr_opt(mcx, s.tablespacename)?,
        accessMethod: cstr_opt(mcx, s.accessMethod)?,
        if_not_exists: s.if_not_exists,
    })
}

fn conv_indexstmt<'mcx>(mcx: Mcx<'mcx>, p: *mut cd::IndexStmt) -> PgResult<tdn::IndexStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::IndexStmt {
        idxname: cstr_opt(mcx, s.idxname)?,
        relation: child_node_opt(mcx, s.relation)?,
        accessMethod: cstr_opt(mcx, s.accessMethod)?,
        tableSpace: cstr_opt(mcx, s.tableSpace)?,
        indexParams: node_list(mcx, s.indexParams)?,
        indexIncludingParams: node_list(mcx, s.indexIncludingParams)?,
        options: node_list(mcx, s.options)?,
        whereClause: node_opt(mcx, s.whereClause)?,
        excludeOpNames: node_list(mcx, s.excludeOpNames)?,
        idxcomment: cstr_opt(mcx, s.idxcomment)?,
        indexOid: s.indexOid,
        oldNumber: s.oldNumber,
        oldCreateSubid: s.oldCreateSubid,
        oldFirstRelfilelocatorSubid: s.oldFirstRelfilelocatorSubid,
        unique: s.unique,
        nulls_not_distinct: s.nulls_not_distinct,
        primary: s.primary,
        isconstraint: s.isconstraint,
        iswithoutoverlaps: s.iswithoutoverlaps,
        deferrable: s.deferrable,
        initdeferred: s.initdeferred,
        transformed: s.transformed,
        concurrent: s.concurrent,
        if_not_exists: s.if_not_exists,
        reset_default_tblspc: s.reset_default_tblspc,
    })
}

fn conv_createseqstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::CreateSeqStmt,
) -> PgResult<tdn::CreateSeqStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::CreateSeqStmt {
        sequence: child_node_opt(mcx, s.sequence)?,
        options: node_list(mcx, s.options)?,
        ownerId: s.ownerId,
        for_identity: s.for_identity,
        if_not_exists: s.if_not_exists,
    })
}

fn conv_createstatsstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::CreateStatsStmt,
) -> PgResult<tdn::CreateStatsStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::CreateStatsStmt {
        defnames: node_list(mcx, s.defnames)?,
        stat_types: node_list(mcx, s.stat_types)?,
        exprs: node_list(mcx, s.exprs)?,
        relations: node_list(mcx, s.relations)?,
        stxcomment: cstr_opt(mcx, s.stxcomment)?,
        transformed: s.transformed,
        if_not_exists: s.if_not_exists,
    })
}

fn conv_createfunctionstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::CreateFunctionStmt,
) -> PgResult<tdn::CreateFunctionStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::CreateFunctionStmt {
        is_procedure: s.is_procedure,
        replace: s.replace,
        funcname: node_list(mcx, s.funcname)?,
        parameters: node_list(mcx, s.parameters)?,
        returnType: child_node_opt(mcx, s.returnType)?,
        options: node_list(mcx, s.options)?,
        sql_body: node_opt(mcx, s.sql_body)?,
    })
}

fn conv_definestmt<'mcx>(mcx: Mcx<'mcx>, p: *mut cd::DefineStmt) -> PgResult<tdn::DefineStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::DefineStmt {
        kind: object_type(s.kind),
        oldstyle: s.oldstyle,
        defnames: node_list(mcx, s.defnames)?,
        args: node_list(mcx, s.args)?,
        definition: node_list(mcx, s.definition)?,
        if_not_exists: s.if_not_exists,
        replace: s.replace,
    })
}

fn conv_createdomainstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::CreateDomainStmt,
) -> PgResult<tdn::CreateDomainStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::CreateDomainStmt {
        domainname: node_list(mcx, s.domainname)?,
        typeName: child_node_opt(mcx, s.typeName)?,
        collClause: child_node_opt(mcx, s.collClause)?,
        constraints: node_list(mcx, s.constraints)?,
    })
}

fn conv_compositetypestmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::CompositeTypeStmt,
) -> PgResult<tdn::CompositeTypeStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::CompositeTypeStmt {
        typevar: child_node_opt(mcx, s.typevar)?,
        coldeflist: node_list(mcx, s.coldeflist)?,
    })
}

fn conv_createenumstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::CreateEnumStmt,
) -> PgResult<tdn::CreateEnumStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::CreateEnumStmt {
        typeName: node_list(mcx, s.typeName)?,
        vals: node_list(mcx, s.vals)?,
    })
}

fn conv_createrangestmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::CreateRangeStmt,
) -> PgResult<tdn::CreateRangeStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::CreateRangeStmt {
        typeName: node_list(mcx, s.typeName)?,
        params: node_list(mcx, s.params)?,
    })
}

fn conv_viewstmt<'mcx>(mcx: Mcx<'mcx>, p: *mut cd::ViewStmt) -> PgResult<tdn::ViewStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::ViewStmt {
        view: child_node_opt(mcx, s.view)?,
        aliases: node_list(mcx, s.aliases)?,
        query: node_opt(mcx, s.query)?,
        replace: s.replace,
        options: node_list(mcx, s.options)?,
        withCheckOption: view_check_option(s.withCheckOption),
    })
}

fn conv_createtableasstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::CreateTableAsStmt,
) -> PgResult<tdn::CreateTableAsStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::CreateTableAsStmt {
        query: node_opt(mcx, s.query)?,
        into: child_node_opt(mcx, s.into)?,
        objtype: object_type(s.objtype),
        is_select_into: s.is_select_into,
        if_not_exists: s.if_not_exists,
    })
}

fn conv_createschemastmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::CreateSchemaStmt,
) -> PgResult<tdn::CreateSchemaStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::CreateSchemaStmt {
        schemaname: cstr_opt(mcx, s.schemaname)?,
        authrole: child_node_opt(mcx, s.authrole)?,
        schemaElts: node_list(mcx, s.schemaElts)?,
        if_not_exists: s.if_not_exists,
    })
}

fn conv_createextensionstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::CreateExtensionStmt,
) -> PgResult<tdn::CreateExtensionStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::CreateExtensionStmt {
        extname: cstr_opt(mcx, s.extname)?,
        if_not_exists: s.if_not_exists,
        options: node_list(mcx, s.options)?,
    })
}

fn conv_createtrigstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::CreateTrigStmt,
) -> PgResult<tdn::CreateTrigStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::CreateTrigStmt {
        replace: s.replace,
        isconstraint: s.isconstraint,
        trigname: cstr_opt(mcx, s.trigname)?,
        relation: child_node_opt(mcx, s.relation)?,
        funcname: node_list(mcx, s.funcname)?,
        args: node_list(mcx, s.args)?,
        row: s.row,
        timing: s.timing,
        events: s.events,
        columns: node_list(mcx, s.columns)?,
        whenClause: node_opt(mcx, s.whenClause)?,
        transitionRels: node_list(mcx, s.transitionRels)?,
        deferrable: s.deferrable,
        initdeferred: s.initdeferred,
        constrrel: child_node_opt(mcx, s.constrrel)?,
    })
}

fn conv_createrolestmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::CreateRoleStmt,
) -> PgResult<tdn::CreateRoleStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::CreateRoleStmt {
        stmt_type: role_stmt_type(s.stmt_type),
        role: cstr_opt(mcx, s.role)?,
        options: node_list(mcx, s.options)?,
    })
}

fn conv_createdbstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::CreatedbStmt,
) -> PgResult<tdn::CreatedbStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::CreatedbStmt {
        dbname: cstr_opt(mcx, s.dbname)?,
        options: node_list(mcx, s.options)?,
    })
}

fn conv_createcaststmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::CreateCastStmt,
) -> PgResult<tdn::CreateCastStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::CreateCastStmt {
        sourcetype: child_node_opt(mcx, s.sourcetype)?,
        targettype: child_node_opt(mcx, s.targettype)?,
        func: child_node_opt(mcx, s.func)?,
        context: coercion_context(s.context),
        inout: s.inout,
    })
}

fn conv_createopclassstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::CreateOpClassStmt,
) -> PgResult<tdn::CreateOpClassStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::CreateOpClassStmt {
        opclassname: node_list(mcx, s.opclassname)?,
        opfamilyname: node_list(mcx, s.opfamilyname)?,
        amname: cstr_opt(mcx, s.amname)?,
        datatype: child_node_opt(mcx, s.datatype)?,
        items: node_list(mcx, s.items)?,
        isDefault: s.isDefault,
    })
}

fn conv_createopfamilystmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::CreateOpFamilyStmt,
) -> PgResult<tdn::CreateOpFamilyStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::CreateOpFamilyStmt {
        opfamilyname: node_list(mcx, s.opfamilyname)?,
        amname: cstr_opt(mcx, s.amname)?,
    })
}

fn conv_createplangstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::CreatePLangStmt,
) -> PgResult<tdn::CreatePLangStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::CreatePLangStmt {
        replace: s.replace,
        plname: cstr_opt(mcx, s.plname)?,
        plhandler: node_list(mcx, s.plhandler)?,
        plinline: node_list(mcx, s.plinline)?,
        plvalidator: node_list(mcx, s.plvalidator)?,
        pltrusted: s.pltrusted,
    })
}

fn conv_createtablespacestmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::CreateTableSpaceStmt,
) -> PgResult<tdn::CreateTableSpaceStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::CreateTableSpaceStmt {
        tablespacename: cstr_opt(mcx, s.tablespacename)?,
        owner: child_node_opt(mcx, s.owner)?,
        location: cstr_opt(mcx, s.location)?,
        options: node_list(mcx, s.options)?,
    })
}

fn conv_createconversionstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::CreateConversionStmt,
) -> PgResult<tdn::CreateConversionStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::CreateConversionStmt {
        conversion_name: node_list(mcx, s.conversion_name)?,
        for_encoding_name: cstr_opt(mcx, s.for_encoding_name)?,
        to_encoding_name: cstr_opt(mcx, s.to_encoding_name)?,
        func_name: node_list(mcx, s.func_name)?,
        def: s.def,
    })
}

fn conv_createamstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::CreateAmStmt,
) -> PgResult<tdn::CreateAmStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::CreateAmStmt {
        amname: cstr_opt(mcx, s.amname)?,
        handler_name: node_list(mcx, s.handler_name)?,
        amtype: s.amtype as i8,
    })
}

// ---------------------------------------------------------------------------
// ALTER / DROP family — small enum converters
// ---------------------------------------------------------------------------

fn drop_behavior(v: cd::DropBehavior) -> tn_pn::DropBehavior {
    use tn_pn::DropBehavior::*;
    match v {
        cd::DROP_RESTRICT => Restrict,
        cd::DROP_CASCADE => Cascade,
        other => panic!("gram converter: invalid DropBehavior {other}"),
    }
}

fn alter_table_type(v: cd::AlterTableType) -> tdn::AlterTableType {
    use tdn::AlterTableType::*;
    match v {
        cd::AT_AddColumn => AT_AddColumn,
        cd::AT_AddColumnToView => AT_AddColumnToView,
        cd::AT_ColumnDefault => AT_ColumnDefault,
        cd::AT_CookedColumnDefault => AT_CookedColumnDefault,
        cd::AT_DropNotNull => AT_DropNotNull,
        cd::AT_SetNotNull => AT_SetNotNull,
        cd::AT_SetExpression => AT_SetExpression,
        cd::AT_DropExpression => AT_DropExpression,
        cd::AT_SetStatistics => AT_SetStatistics,
        cd::AT_SetOptions => AT_SetOptions,
        cd::AT_ResetOptions => AT_ResetOptions,
        cd::AT_SetStorage => AT_SetStorage,
        cd::AT_SetCompression => AT_SetCompression,
        cd::AT_DropColumn => AT_DropColumn,
        cd::AT_AddIndex => AT_AddIndex,
        cd::AT_ReAddIndex => AT_ReAddIndex,
        cd::AT_AddConstraint => AT_AddConstraint,
        cd::AT_ReAddConstraint => AT_ReAddConstraint,
        cd::AT_ReAddDomainConstraint => AT_ReAddDomainConstraint,
        cd::AT_AlterConstraint => AT_AlterConstraint,
        cd::AT_ValidateConstraint => AT_ValidateConstraint,
        cd::AT_AddIndexConstraint => AT_AddIndexConstraint,
        cd::AT_DropConstraint => AT_DropConstraint,
        cd::AT_ReAddComment => AT_ReAddComment,
        cd::AT_AlterColumnType => AT_AlterColumnType,
        cd::AT_AlterColumnGenericOptions => AT_AlterColumnGenericOptions,
        cd::AT_ChangeOwner => AT_ChangeOwner,
        cd::AT_ClusterOn => AT_ClusterOn,
        cd::AT_DropCluster => AT_DropCluster,
        cd::AT_SetLogged => AT_SetLogged,
        cd::AT_SetUnLogged => AT_SetUnLogged,
        cd::AT_DropOids => AT_DropOids,
        cd::AT_SetAccessMethod => AT_SetAccessMethod,
        cd::AT_SetTableSpace => AT_SetTableSpace,
        cd::AT_SetRelOptions => AT_SetRelOptions,
        cd::AT_ResetRelOptions => AT_ResetRelOptions,
        cd::AT_ReplaceRelOptions => AT_ReplaceRelOptions,
        cd::AT_EnableTrig => AT_EnableTrig,
        cd::AT_EnableAlwaysTrig => AT_EnableAlwaysTrig,
        cd::AT_EnableReplicaTrig => AT_EnableReplicaTrig,
        cd::AT_DisableTrig => AT_DisableTrig,
        cd::AT_EnableTrigAll => AT_EnableTrigAll,
        cd::AT_DisableTrigAll => AT_DisableTrigAll,
        cd::AT_EnableTrigUser => AT_EnableTrigUser,
        cd::AT_DisableTrigUser => AT_DisableTrigUser,
        cd::AT_EnableRule => AT_EnableRule,
        cd::AT_EnableAlwaysRule => AT_EnableAlwaysRule,
        cd::AT_EnableReplicaRule => AT_EnableReplicaRule,
        cd::AT_DisableRule => AT_DisableRule,
        cd::AT_AddInherit => AT_AddInherit,
        cd::AT_DropInherit => AT_DropInherit,
        cd::AT_AddOf => AT_AddOf,
        cd::AT_DropOf => AT_DropOf,
        cd::AT_ReplicaIdentity => AT_ReplicaIdentity,
        cd::AT_EnableRowSecurity => AT_EnableRowSecurity,
        cd::AT_DisableRowSecurity => AT_DisableRowSecurity,
        cd::AT_ForceRowSecurity => AT_ForceRowSecurity,
        cd::AT_NoForceRowSecurity => AT_NoForceRowSecurity,
        cd::AT_GenericOptions => AT_GenericOptions,
        cd::AT_AttachPartition => AT_AttachPartition,
        cd::AT_DetachPartition => AT_DetachPartition,
        cd::AT_DetachPartitionFinalize => AT_DetachPartitionFinalize,
        cd::AT_AddIdentity => AT_AddIdentity,
        cd::AT_SetIdentity => AT_SetIdentity,
        cd::AT_DropIdentity => AT_DropIdentity,
        cd::AT_ReAddStatistics => AT_ReAddStatistics,
        other => panic!("gram converter: invalid AlterTableType {other}"),
    }
}

fn alter_tsconfig_type(v: cd::AlterTSConfigType) -> tdn::AlterTSConfigType {
    use tdn::AlterTSConfigType::*;
    match v {
        cd::ALTER_TSCONFIG_ADD_MAPPING => ALTER_TSCONFIG_ADD_MAPPING,
        cd::ALTER_TSCONFIG_ALTER_MAPPING_FOR_TOKEN => ALTER_TSCONFIG_ALTER_MAPPING_FOR_TOKEN,
        cd::ALTER_TSCONFIG_REPLACE_DICT => ALTER_TSCONFIG_REPLACE_DICT,
        cd::ALTER_TSCONFIG_REPLACE_DICT_FOR_TOKEN => ALTER_TSCONFIG_REPLACE_DICT_FOR_TOKEN,
        cd::ALTER_TSCONFIG_DROP_MAPPING => ALTER_TSCONFIG_DROP_MAPPING,
        other => panic!("gram converter: invalid AlterTSConfigType {other}"),
    }
}

fn alter_publication_action(v: cd::AlterPublicationAction) -> tdn::AlterPublicationAction {
    use tdn::AlterPublicationAction::*;
    match v {
        cd::AP_AddObjects => AP_AddObjects,
        cd::AP_DropObjects => AP_DropObjects,
        cd::AP_SetObjects => AP_SetObjects,
        other => panic!("gram converter: invalid AlterPublicationAction {other}"),
    }
}

fn alter_subscription_type(v: cd::AlterSubscriptionType) -> tdn::AlterSubscriptionType {
    use tdn::AlterSubscriptionType::*;
    match v {
        cd::ALTER_SUBSCRIPTION_OPTIONS => ALTER_SUBSCRIPTION_OPTIONS,
        cd::ALTER_SUBSCRIPTION_CONNECTION => ALTER_SUBSCRIPTION_CONNECTION,
        cd::ALTER_SUBSCRIPTION_SET_PUBLICATION => ALTER_SUBSCRIPTION_SET_PUBLICATION,
        cd::ALTER_SUBSCRIPTION_ADD_PUBLICATION => ALTER_SUBSCRIPTION_ADD_PUBLICATION,
        cd::ALTER_SUBSCRIPTION_DROP_PUBLICATION => ALTER_SUBSCRIPTION_DROP_PUBLICATION,
        cd::ALTER_SUBSCRIPTION_REFRESH => ALTER_SUBSCRIPTION_REFRESH,
        cd::ALTER_SUBSCRIPTION_ENABLED => ALTER_SUBSCRIPTION_ENABLED,
        cd::ALTER_SUBSCRIPTION_SKIP => ALTER_SUBSCRIPTION_SKIP,
        other => panic!("gram converter: invalid AlterSubscriptionType {other}"),
    }
}

// ---------------------------------------------------------------------------
// ALTER / DROP family — supporting / helper nodes
// ---------------------------------------------------------------------------

fn conv_partitioncmd<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::PartitionCmd,
) -> PgResult<tdn::PartitionCmd<'mcx>> {
    let c = unsafe { &*p };
    Ok(tdn::PartitionCmd {
        name: child_node_opt(mcx, c.name)?,
        bound: child_node_opt(mcx, c.bound)?,
        concurrent: c.concurrent,
    })
}

fn conv_replicaidentitystmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::ReplicaIdentityStmt,
) -> PgResult<tdn::ReplicaIdentityStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::ReplicaIdentityStmt {
        identity_type: s.identity_type as i8,
        name: cstr_opt(mcx, s.name)?,
    })
}

fn conv_ataltconstraint<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::ATAlterConstraint,
) -> PgResult<tdn::ATAlterConstraint<'mcx>> {
    let c = unsafe { &*p };
    Ok(tdn::ATAlterConstraint {
        conname: cstr_opt(mcx, c.conname)?,
        alterEnforceability: c.alterEnforceability,
        is_enforced: c.is_enforced,
        alterDeferrability: c.alterDeferrability,
        deferrable: c.deferrable,
        initdeferred: c.initdeferred,
        alterInheritability: c.alterInheritability,
        noinherit: c.noinherit,
    })
}

// ---------------------------------------------------------------------------
// ALTER / DROP family — statements
// ---------------------------------------------------------------------------

fn conv_altertablestmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::AlterTableStmt,
) -> PgResult<tdn::AlterTableStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::AlterTableStmt {
        relation: child_node_opt(mcx, s.relation)?,
        cmds: node_list(mcx, s.cmds)?,
        objtype: object_type(s.objtype),
        missing_ok: s.missing_ok,
    })
}

fn conv_altertablecmd<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::AlterTableCmd,
) -> PgResult<tdn::AlterTableCmd<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::AlterTableCmd {
        subtype: alter_table_type(s.subtype),
        name: cstr_opt(mcx, s.name)?,
        num: s.num,
        newowner: child_node_opt(mcx, s.newowner)?,
        def: node_opt(mcx, s.def)?,
        behavior: drop_behavior(s.behavior),
        missing_ok: s.missing_ok,
        recurse: s.recurse,
    })
}

fn conv_altercollationstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::AlterCollationStmt,
) -> PgResult<tdn::AlterCollationStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::AlterCollationStmt {
        collname: node_list(mcx, s.collname)?,
    })
}

fn conv_alterdomainstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::AlterDomainStmt,
) -> PgResult<tdn::AlterDomainStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::AlterDomainStmt {
        subtype: s.subtype as i8,
        typeName: node_list(mcx, s.typeName)?,
        name: cstr_opt(mcx, s.name)?,
        def: node_opt(mcx, s.def)?,
        behavior: drop_behavior(s.behavior),
        missing_ok: s.missing_ok,
    })
}

fn conv_alterenumstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::AlterEnumStmt,
) -> PgResult<tdn::AlterEnumStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::AlterEnumStmt {
        typeName: node_list(mcx, s.typeName)?,
        oldVal: cstr_opt(mcx, s.oldVal)?,
        newVal: cstr_opt(mcx, s.newVal)?,
        newValNeighbor: cstr_opt(mcx, s.newValNeighbor)?,
        newValIsAfter: s.newValIsAfter,
        skipIfNewValExists: s.skipIfNewValExists,
    })
}

fn conv_alterstatsstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::AlterStatsStmt,
) -> PgResult<tdn::AlterStatsStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::AlterStatsStmt {
        defnames: node_list(mcx, s.defnames)?,
        stxstattarget: node_opt(mcx, s.stxstattarget)?,
        missing_ok: s.missing_ok,
    })
}

fn conv_alterseqstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::AlterSeqStmt,
) -> PgResult<tdn::AlterSeqStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::AlterSeqStmt {
        sequence: child_node_opt(mcx, s.sequence)?,
        options: node_list(mcx, s.options)?,
        for_identity: s.for_identity,
        missing_ok: s.missing_ok,
    })
}

fn conv_alteropfamilystmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::AlterOpFamilyStmt,
) -> PgResult<tdn::AlterOpFamilyStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::AlterOpFamilyStmt {
        opfamilyname: node_list(mcx, s.opfamilyname)?,
        amname: cstr_opt(mcx, s.amname)?,
        isDrop: s.isDrop,
        items: node_list(mcx, s.items)?,
    })
}

fn conv_alterfunctionstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::AlterFunctionStmt,
) -> PgResult<tdn::AlterFunctionStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::AlterFunctionStmt {
        objtype: object_type(s.objtype),
        func: child_node_opt(mcx, s.func)?,
        actions: node_list(mcx, s.actions)?,
    })
}

fn conv_dropstmt<'mcx>(mcx: Mcx<'mcx>, p: *mut cd::DropStmt) -> PgResult<tdn::DropStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::DropStmt {
        objects: node_list(mcx, s.objects)?,
        removeType: object_type(s.removeType),
        behavior: drop_behavior(s.behavior),
        missing_ok: s.missing_ok,
        concurrent: s.concurrent,
    })
}

fn conv_renamestmt<'mcx>(mcx: Mcx<'mcx>, p: *mut cd::RenameStmt) -> PgResult<tdn::RenameStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::RenameStmt {
        renameType: object_type(s.renameType),
        relationType: object_type(s.relationType),
        relation: child_node_opt(mcx, s.relation)?,
        object: node_opt(mcx, s.object)?,
        subname: cstr_opt(mcx, s.subname)?,
        newname: cstr_opt(mcx, s.newname)?,
        behavior: drop_behavior(s.behavior),
        missing_ok: s.missing_ok,
    })
}

fn conv_alterobjectdependsstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::AlterObjectDependsStmt,
) -> PgResult<tdn::AlterObjectDependsStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::AlterObjectDependsStmt {
        objectType: object_type(s.objectType),
        relation: child_node_opt(mcx, s.relation)?,
        object: node_opt(mcx, s.object)?,
        extname: child_node_opt(mcx, s.extname)?,
        remove: s.remove,
    })
}

fn conv_alterobjectschemastmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::AlterObjectSchemaStmt,
) -> PgResult<tdn::AlterObjectSchemaStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::AlterObjectSchemaStmt {
        objectType: object_type(s.objectType),
        relation: child_node_opt(mcx, s.relation)?,
        object: node_opt(mcx, s.object)?,
        newschema: cstr_opt(mcx, s.newschema)?,
        missing_ok: s.missing_ok,
    })
}

fn conv_alterownerstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::AlterOwnerStmt,
) -> PgResult<tdn::AlterOwnerStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::AlterOwnerStmt {
        objectType: object_type(s.objectType),
        relation: child_node_opt(mcx, s.relation)?,
        object: node_opt(mcx, s.object)?,
        newowner: child_node_opt(mcx, s.newowner)?,
    })
}

fn conv_alteroperatorstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::AlterOperatorStmt,
) -> PgResult<tdn::AlterOperatorStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::AlterOperatorStmt {
        opername: child_node_opt(mcx, s.opername)?,
        options: node_list(mcx, s.options)?,
    })
}

fn conv_altertypestmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::AlterTypeStmt,
) -> PgResult<tdn::AlterTypeStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::AlterTypeStmt {
        typeName: node_list(mcx, s.typeName)?,
        options: node_list(mcx, s.options)?,
    })
}

fn conv_alterdefaultprivilegesstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::AlterDefaultPrivilegesStmt,
) -> PgResult<tdn::AlterDefaultPrivilegesStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::AlterDefaultPrivilegesStmt {
        options: node_list(mcx, s.options)?,
        action: child_node_opt(mcx, s.action)?,
    })
}

fn conv_alterrolestmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::AlterRoleStmt,
) -> PgResult<tdn::AlterRoleStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::AlterRoleStmt {
        role: child_node_opt(mcx, s.role)?,
        options: node_list(mcx, s.options)?,
        action: s.action,
    })
}

fn conv_alterrolesetstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::AlterRoleSetStmt,
) -> PgResult<tdn::AlterRoleSetStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::AlterRoleSetStmt {
        role: child_node_opt(mcx, s.role)?,
        database: cstr_opt(mcx, s.database)?,
        setstmt: child_node_opt(mcx, s.setstmt)?,
    })
}

fn conv_dropownedstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::DropOwnedStmt,
) -> PgResult<tdn::DropOwnedStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::DropOwnedStmt {
        roles: node_list(mcx, s.roles)?,
        behavior: drop_behavior(s.behavior),
    })
}

fn conv_reassignownedstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::ReassignOwnedStmt,
) -> PgResult<tdn::ReassignOwnedStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::ReassignOwnedStmt {
        roles: node_list(mcx, s.roles)?,
        newrole: child_node_opt(mcx, s.newrole)?,
    })
}

fn conv_altertablespaceoptionsstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::AlterTableSpaceOptionsStmt,
) -> PgResult<tdn::AlterTableSpaceOptionsStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::AlterTableSpaceOptionsStmt {
        tablespacename: cstr_opt(mcx, s.tablespacename)?,
        options: node_list(mcx, s.options)?,
        isReset: s.isReset,
    })
}

fn conv_altertablemoveallstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::AlterTableMoveAllStmt,
) -> PgResult<tdn::AlterTableMoveAllStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::AlterTableMoveAllStmt {
        orig_tablespacename: cstr_opt(mcx, s.orig_tablespacename)?,
        objtype: object_type(s.objtype),
        roles: node_list(mcx, s.roles)?,
        new_tablespacename: cstr_opt(mcx, s.new_tablespacename)?,
        nowait: s.nowait,
    })
}

fn conv_alterextensionstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::AlterExtensionStmt,
) -> PgResult<tdn::AlterExtensionStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::AlterExtensionStmt {
        extname: cstr_opt(mcx, s.extname)?,
        options: node_list(mcx, s.options)?,
    })
}

fn conv_alterextensioncontentsstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::AlterExtensionContentsStmt,
) -> PgResult<tdn::AlterExtensionContentsStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::AlterExtensionContentsStmt {
        extname: cstr_opt(mcx, s.extname)?,
        action: s.action,
        objtype: object_type(s.objtype),
        object: node_opt(mcx, s.object)?,
    })
}

fn conv_alterfdwstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::AlterFdwStmt,
) -> PgResult<tdn::AlterFdwStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::AlterFdwStmt {
        fdwname: cstr_opt(mcx, s.fdwname)?,
        func_options: node_list(mcx, s.func_options)?,
        options: node_list(mcx, s.options)?,
    })
}

fn conv_alterforeignserverstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::AlterForeignServerStmt,
) -> PgResult<tdn::AlterForeignServerStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::AlterForeignServerStmt {
        servername: cstr_opt(mcx, s.servername)?,
        version: cstr_opt(mcx, s.version)?,
        options: node_list(mcx, s.options)?,
        has_version: s.has_version,
    })
}

fn conv_alterusermappingstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::AlterUserMappingStmt,
) -> PgResult<tdn::AlterUserMappingStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::AlterUserMappingStmt {
        user: child_node_opt(mcx, s.user)?,
        servername: cstr_opt(mcx, s.servername)?,
        options: node_list(mcx, s.options)?,
    })
}

fn conv_alterpolicystmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::AlterPolicyStmt,
) -> PgResult<tdn::AlterPolicyStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::AlterPolicyStmt {
        policy_name: cstr_opt(mcx, s.policy_name)?,
        table: child_node_opt(mcx, s.table)?,
        roles: node_list(mcx, s.roles)?,
        qual: node_opt(mcx, s.qual)?,
        with_check: node_opt(mcx, s.with_check)?,
    })
}

fn conv_alterdatabasestmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::AlterDatabaseStmt,
) -> PgResult<tdn::AlterDatabaseStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::AlterDatabaseStmt {
        dbname: cstr_opt(mcx, s.dbname)?,
        options: node_list(mcx, s.options)?,
    })
}

fn conv_alterdatabaserefreshcollstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::AlterDatabaseRefreshCollStmt,
) -> PgResult<tdn::AlterDatabaseRefreshCollStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::AlterDatabaseRefreshCollStmt {
        dbname: cstr_opt(mcx, s.dbname)?,
    })
}

fn conv_alterdatabasesetstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::AlterDatabaseSetStmt,
) -> PgResult<tdn::AlterDatabaseSetStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::AlterDatabaseSetStmt {
        dbname: cstr_opt(mcx, s.dbname)?,
        setstmt: child_node_opt(mcx, s.setstmt)?,
    })
}

fn conv_altertsdictionarystmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::AlterTSDictionaryStmt,
) -> PgResult<tdn::AlterTSDictionaryStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::AlterTSDictionaryStmt {
        dictname: node_list(mcx, s.dictname)?,
        options: node_list(mcx, s.options)?,
    })
}

fn conv_altertsconfigurationstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::AlterTSConfigurationStmt,
) -> PgResult<tdn::AlterTSConfigurationStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::AlterTSConfigurationStmt {
        kind: alter_tsconfig_type(s.kind),
        cfgname: node_list(mcx, s.cfgname)?,
        tokentype: node_list(mcx, s.tokentype)?,
        dicts: node_list(mcx, s.dicts)?,
        override_: s.override_,
        replace: s.replace,
        missing_ok: s.missing_ok,
    })
}

fn conv_alterpublicationstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::AlterPublicationStmt,
) -> PgResult<tdn::AlterPublicationStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::AlterPublicationStmt {
        pubname: cstr_opt(mcx, s.pubname)?,
        options: node_list(mcx, s.options)?,
        pubobjects: node_list(mcx, s.pubobjects)?,
        for_all_tables: s.for_all_tables,
        action: alter_publication_action(s.action),
    })
}

fn conv_altersubscriptionstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::AlterSubscriptionStmt,
) -> PgResult<tdn::AlterSubscriptionStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::AlterSubscriptionStmt {
        kind: alter_subscription_type(s.kind),
        subname: cstr_opt(mcx, s.subname)?,
        conninfo: cstr_opt(mcx, s.conninfo)?,
        publication: node_list(mcx, s.publication)?,
        options: node_list(mcx, s.options)?,
    })
}

// ===========================================================================
// Utility / GRANT / transaction family (parser grammar F4)
// ===========================================================================

fn grant_target_type(v: cd::GrantTargetType) -> tdn::GrantTargetType {
    use tdn::GrantTargetType::*;
    match v {
        cd::ACL_TARGET_OBJECT => ACL_TARGET_OBJECT,
        cd::ACL_TARGET_ALL_IN_SCHEMA => ACL_TARGET_ALL_IN_SCHEMA,
        cd::ACL_TARGET_DEFAULTS => ACL_TARGET_DEFAULTS,
        other => panic!("gram converter: invalid GrantTargetType {other}"),
    }
}

fn variable_set_kind(v: cd::VariableSetKind) -> tdn::VariableSetKind {
    use tdn::VariableSetKind::*;
    match v {
        cd::VAR_SET_VALUE => VAR_SET_VALUE,
        cd::VAR_SET_DEFAULT => VAR_SET_DEFAULT,
        cd::VAR_SET_CURRENT => VAR_SET_CURRENT,
        cd::VAR_SET_MULTI => VAR_SET_MULTI,
        cd::VAR_RESET => VAR_RESET,
        cd::VAR_RESET_ALL => VAR_RESET_ALL,
        other => panic!("gram converter: invalid VariableSetKind {other}"),
    }
}

fn transaction_stmt_kind(v: cd::TransactionStmtKind) -> tdn::TransactionStmtKind {
    use tdn::TransactionStmtKind::*;
    match v {
        cd::TRANS_STMT_BEGIN => TRANS_STMT_BEGIN,
        cd::TRANS_STMT_START => TRANS_STMT_START,
        cd::TRANS_STMT_COMMIT => TRANS_STMT_COMMIT,
        cd::TRANS_STMT_ROLLBACK => TRANS_STMT_ROLLBACK,
        cd::TRANS_STMT_SAVEPOINT => TRANS_STMT_SAVEPOINT,
        cd::TRANS_STMT_RELEASE => TRANS_STMT_RELEASE,
        cd::TRANS_STMT_ROLLBACK_TO => TRANS_STMT_ROLLBACK_TO,
        cd::TRANS_STMT_PREPARE => TRANS_STMT_PREPARE,
        cd::TRANS_STMT_COMMIT_PREPARED => TRANS_STMT_COMMIT_PREPARED,
        cd::TRANS_STMT_ROLLBACK_PREPARED => TRANS_STMT_ROLLBACK_PREPARED,
        other => panic!("gram converter: invalid TransactionStmtKind {other}"),
    }
}

fn discard_mode(v: cd::DiscardMode) -> tdn::DiscardMode {
    use tdn::DiscardMode::*;
    match v {
        cd::DISCARD_ALL => DISCARD_ALL,
        cd::DISCARD_PLANS => DISCARD_PLANS,
        cd::DISCARD_SEQUENCES => DISCARD_SEQUENCES,
        cd::DISCARD_TEMP => DISCARD_TEMP,
        other => panic!("gram converter: invalid DiscardMode {other}"),
    }
}

fn reindex_object_type(v: cd::ReindexObjectType) -> tdn::ReindexObjectType {
    use tdn::ReindexObjectType::*;
    match v {
        cd::REINDEX_OBJECT_INDEX => REINDEX_OBJECT_INDEX,
        cd::REINDEX_OBJECT_TABLE => REINDEX_OBJECT_TABLE,
        cd::REINDEX_OBJECT_SCHEMA => REINDEX_OBJECT_SCHEMA,
        cd::REINDEX_OBJECT_SYSTEM => REINDEX_OBJECT_SYSTEM,
        cd::REINDEX_OBJECT_DATABASE => REINDEX_OBJECT_DATABASE,
        other => panic!("gram converter: invalid ReindexObjectType {other}"),
    }
}

fn import_foreign_schema_type(v: cd::ImportForeignSchemaType) -> tdn::ImportForeignSchemaType {
    use tdn::ImportForeignSchemaType::*;
    match v {
        cd::FDW_IMPORT_SCHEMA_ALL => FDW_IMPORT_SCHEMA_ALL,
        cd::FDW_IMPORT_SCHEMA_LIMIT_TO => FDW_IMPORT_SCHEMA_LIMIT_TO,
        cd::FDW_IMPORT_SCHEMA_EXCEPT => FDW_IMPORT_SCHEMA_EXCEPT,
        other => panic!("gram converter: invalid ImportForeignSchemaType {other}"),
    }
}

fn publication_obj_spec_type(v: cd::PublicationObjSpecType) -> tdn::PublicationObjSpecType {
    use tdn::PublicationObjSpecType::*;
    match v {
        cd::PUBLICATIONOBJ_TABLE => PUBLICATIONOBJ_TABLE,
        cd::PUBLICATIONOBJ_TABLES_IN_SCHEMA => PUBLICATIONOBJ_TABLES_IN_SCHEMA,
        cd::PUBLICATIONOBJ_TABLES_IN_CUR_SCHEMA => PUBLICATIONOBJ_TABLES_IN_CUR_SCHEMA,
        cd::PUBLICATIONOBJ_CONTINUATION => PUBLICATIONOBJ_CONTINUATION,
        other => panic!("gram converter: invalid PublicationObjSpecType {other}"),
    }
}

fn fetch_direction(v: cd::FetchDirection) -> tdn::FetchDirection {
    use tdn::FetchDirection::*;
    match v {
        cd::FETCH_FORWARD => FETCH_FORWARD,
        cd::FETCH_BACKWARD => FETCH_BACKWARD,
        cd::FETCH_ABSOLUTE => FETCH_ABSOLUTE,
        cd::FETCH_RELATIVE => FETCH_RELATIVE,
        other => panic!("gram converter: invalid FetchDirection {other}"),
    }
}

fn conv_grantstmt<'mcx>(mcx: Mcx<'mcx>, p: *mut cd::GrantStmt) -> PgResult<tdn::GrantStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::GrantStmt {
        is_grant: s.is_grant,
        targtype: grant_target_type(s.targtype),
        objtype: object_type(s.objtype),
        objects: node_list(mcx, s.objects)?,
        privileges: node_list(mcx, s.privileges)?,
        grantees: node_list(mcx, s.grantees)?,
        grant_option: s.grant_option,
        grantor: child_node_opt(mcx, s.grantor)?,
        behavior: drop_behavior(s.behavior),
    })
}

fn conv_grantrolestmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::GrantRoleStmt,
) -> PgResult<tdn::GrantRoleStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::GrantRoleStmt {
        granted_roles: node_list(mcx, s.granted_roles)?,
        grantee_roles: node_list(mcx, s.grantee_roles)?,
        is_grant: s.is_grant,
        opt: node_list(mcx, s.opt)?,
        grantor: child_node_opt(mcx, s.grantor)?,
        behavior: drop_behavior(s.behavior),
    })
}

fn conv_variablesetstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::VariableSetStmt,
) -> PgResult<tdn::VariableSetStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::VariableSetStmt {
        kind: variable_set_kind(s.kind),
        name: cstr_opt(mcx, s.name)?,
        args: node_list(mcx, s.args)?,
        jumble_args: s.jumble_args,
        is_local: s.is_local,
        location: s.location,
    })
}

fn conv_variableshowstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::VariableShowStmt,
) -> PgResult<tdn::VariableShowStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::VariableShowStmt { name: cstr_opt(mcx, s.name)? })
}

fn conv_transactionstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::TransactionStmt,
) -> PgResult<tdn::TransactionStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::TransactionStmt {
        kind: transaction_stmt_kind(s.kind),
        options: node_list(mcx, s.options)?,
        savepoint_name: cstr_opt(mcx, s.savepoint_name)?,
        gid: cstr_opt(mcx, s.gid)?,
        chain: s.chain,
        location: s.location,
    })
}

fn conv_copystmt<'mcx>(mcx: Mcx<'mcx>, p: *mut cd::CopyStmt) -> PgResult<tdn::CopyStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::CopyStmt {
        relation: child_node_opt(mcx, s.relation)?,
        query: node_opt(mcx, s.query)?,
        attlist: node_list(mcx, s.attlist)?,
        is_from: s.is_from,
        is_program: s.is_program,
        filename: cstr_opt(mcx, s.filename)?,
        options: node_list(mcx, s.options)?,
        where_clause: node_opt(mcx, s.whereClause)?,
    })
}

fn conv_explainstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::ExplainStmt,
) -> PgResult<tdn::ExplainStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::ExplainStmt {
        query: node_opt(mcx, s.query)?,
        options: node_list(mcx, s.options)?,
    })
}

fn conv_preparestmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::PrepareStmt,
) -> PgResult<tdn::PrepareStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::PrepareStmt {
        name: cstr_opt(mcx, s.name)?,
        argtypes: node_list(mcx, s.argtypes)?,
        query: node_opt(mcx, s.query)?,
    })
}

fn conv_executestmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::ExecuteStmt,
) -> PgResult<tdn::ExecuteStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::ExecuteStmt {
        name: cstr_opt(mcx, s.name)?,
        params: node_list(mcx, s.params)?,
    })
}

fn conv_deallocatestmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::DeallocateStmt,
) -> PgResult<tdn::DeallocateStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::DeallocateStmt {
        name: cstr_opt(mcx, s.name)?,
        isall: s.isall,
        location: s.location,
    })
}

fn conv_declarecursorstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::DeclareCursorStmt,
) -> PgResult<tdn::DeclareCursorStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::DeclareCursorStmt {
        portalname: cstr_opt(mcx, s.portalname)?,
        options: s.options,
        query: node_opt(mcx, s.query)?,
    })
}

fn conv_closeportalstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::ClosePortalStmt,
) -> PgResult<tdn::ClosePortalStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::ClosePortalStmt { portalname: cstr_opt(mcx, s.portalname)? })
}

fn conv_fetchstmt<'mcx>(mcx: Mcx<'mcx>, p: *mut cd::FetchStmt) -> PgResult<tdn::FetchStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::FetchStmt {
        direction: fetch_direction(s.direction),
        how_many: s.howMany as i64,
        portalname: cstr_opt(mcx, s.portalname)?,
        ismove: s.ismove,
    })
}

fn conv_vacuumstmt<'mcx>(mcx: Mcx<'mcx>, p: *mut cd::VacuumStmt) -> PgResult<tdn::VacuumStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::VacuumStmt {
        options: node_list(mcx, s.options)?,
        rels: node_list(mcx, s.rels)?,
        is_vacuumcmd: s.is_vacuumcmd,
    })
}

fn conv_vacuumrelation<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::VacuumRelation,
) -> PgResult<tdn::VacuumRelation<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::VacuumRelation {
        relation: child_node_opt(mcx, s.relation)?,
        oid: s.oid,
        va_cols: node_list(mcx, s.va_cols)?,
    })
}

fn conv_clusterstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::ClusterStmt,
) -> PgResult<tdn::ClusterStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::ClusterStmt {
        relation: child_node_opt(mcx, s.relation)?,
        indexname: cstr_opt(mcx, s.indexname)?,
        params: node_list(mcx, s.params)?,
    })
}

fn conv_reindexstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::ReindexStmt,
) -> PgResult<tdn::ReindexStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::ReindexStmt {
        kind: reindex_object_type(s.kind),
        relation: child_node_opt(mcx, s.relation)?,
        name: cstr_opt(mcx, s.name as *mut core::ffi::c_char)?,
        params: node_list(mcx, s.params)?,
    })
}

fn conv_discardstmt(p: *mut cd::DiscardStmt) -> tdn::DiscardStmt {
    let s = unsafe { &*p };
    tdn::DiscardStmt { target: discard_mode(s.target) }
}

fn conv_lockstmt<'mcx>(mcx: Mcx<'mcx>, p: *mut cd::LockStmt) -> PgResult<tdn::LockStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::LockStmt {
        relations: node_list(mcx, s.relations)?,
        mode: s.mode,
        nowait: s.nowait,
    })
}

fn conv_constraintssetstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::ConstraintsSetStmt,
) -> PgResult<tdn::ConstraintsSetStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::ConstraintsSetStmt {
        constraints: node_list(mcx, s.constraints)?,
        deferred: s.deferred,
    })
}

fn conv_loadstmt<'mcx>(mcx: Mcx<'mcx>, p: *mut cd::LoadStmt) -> PgResult<tdn::LoadStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::LoadStmt { filename: cstr_opt(mcx, s.filename)? })
}

fn conv_truncatestmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::TruncateStmt,
) -> PgResult<tdn::TruncateStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::TruncateStmt {
        relations: node_list(mcx, s.relations)?,
        restart_seqs: s.restart_seqs,
        behavior: drop_behavior(s.behavior),
    })
}

fn conv_commentstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::CommentStmt,
) -> PgResult<tdn::CommentStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::CommentStmt {
        objtype: object_type(s.objtype),
        object: node_opt(mcx, s.object)?,
        comment: cstr_opt(mcx, s.comment)?,
    })
}

fn conv_seclabelstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::SecLabelStmt,
) -> PgResult<tdn::SecLabelStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::SecLabelStmt {
        objtype: object_type(s.objtype),
        object: node_opt(mcx, s.object)?,
        provider: cstr_opt(mcx, s.provider)?,
        label: cstr_opt(mcx, s.label)?,
    })
}

fn conv_rulestmt<'mcx>(mcx: Mcx<'mcx>, p: *mut cd::RuleStmt) -> PgResult<tdn::RuleStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::RuleStmt {
        relation: child_node_opt(mcx, s.relation)?,
        rulename: cstr_opt(mcx, s.rulename)?,
        where_clause: node_opt(mcx, s.whereClause)?,
        event: cmd_type(s.event),
        instead: s.instead,
        actions: node_list(mcx, s.actions)?,
        replace: s.replace,
    })
}

fn conv_notifystmt<'mcx>(mcx: Mcx<'mcx>, p: *mut cd::NotifyStmt) -> PgResult<tdn::NotifyStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::NotifyStmt {
        conditionname: cstr_opt(mcx, s.conditionname)?,
        payload: cstr_opt(mcx, s.payload)?,
    })
}

fn conv_listenstmt<'mcx>(mcx: Mcx<'mcx>, p: *mut cd::ListenStmt) -> PgResult<tdn::ListenStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::ListenStmt { conditionname: cstr_opt(mcx, s.conditionname)? })
}

fn conv_unlistenstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::UnlistenStmt,
) -> PgResult<tdn::UnlistenStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::UnlistenStmt { conditionname: cstr_opt(mcx, s.conditionname)? })
}

fn conv_dostmt<'mcx>(mcx: Mcx<'mcx>, p: *mut cd::DoStmt) -> PgResult<tdn::DoStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::DoStmt { args: node_list(mcx, s.args)? })
}

fn conv_callstmt<'mcx>(mcx: Mcx<'mcx>, p: *mut cd::CallStmt) -> PgResult<tdn::CallStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::CallStmt {
        funccall: child_node_opt(mcx, s.funccall)?,
        funcexpr: child_node_opt(mcx, s.funcexpr)?,
        outargs: node_list(mcx, s.outargs)?,
    })
}

fn conv_refreshmatviewstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::RefreshMatViewStmt,
) -> PgResult<tdn::RefreshMatViewStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::RefreshMatViewStmt {
        concurrent: s.concurrent,
        skip_data: s.skipData,
        relation: child_node_opt(mcx, s.relation)?,
    })
}

fn conv_altersystemstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::AlterSystemStmt,
) -> PgResult<tdn::AlterSystemStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::AlterSystemStmt { setstmt: child_node_opt(mcx, s.setstmt)? })
}

fn conv_dropdbstmt<'mcx>(mcx: Mcx<'mcx>, p: *mut cd::DropdbStmt) -> PgResult<tdn::DropdbStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::DropdbStmt {
        dbname: cstr_opt(mcx, s.dbname)?,
        missing_ok: s.missing_ok,
        options: node_list(mcx, s.options)?,
    })
}

fn conv_droprolestmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::DropRoleStmt,
) -> PgResult<tdn::DropRoleStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::DropRoleStmt {
        roles: node_list(mcx, s.roles)?,
        missing_ok: s.missing_ok,
    })
}

fn conv_droptablespacestmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::DropTableSpaceStmt,
) -> PgResult<tdn::DropTableSpaceStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::DropTableSpaceStmt {
        tablespacename: cstr_opt(mcx, s.tablespacename)?,
        missing_ok: s.missing_ok,
    })
}

fn conv_createfdwstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::CreateFdwStmt,
) -> PgResult<tdn::CreateFdwStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::CreateFdwStmt {
        fdwname: cstr_opt(mcx, s.fdwname)?,
        func_options: node_list(mcx, s.func_options)?,
        options: node_list(mcx, s.options)?,
    })
}

fn conv_createforeignserverstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::CreateForeignServerStmt,
) -> PgResult<tdn::CreateForeignServerStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::CreateForeignServerStmt {
        servername: cstr_opt(mcx, s.servername)?,
        servertype: cstr_opt(mcx, s.servertype)?,
        version: cstr_opt(mcx, s.version)?,
        fdwname: cstr_opt(mcx, s.fdwname)?,
        if_not_exists: s.if_not_exists,
        options: node_list(mcx, s.options)?,
    })
}

fn conv_createforeigntablestmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::CreateForeignTableStmt,
) -> PgResult<tdn::CreateForeignTableStmt<'mcx>> {
    let s = unsafe { &*p };
    let base = conv_createstmt(mcx, &s.base as *const cd::CreateStmt as *mut cd::CreateStmt)?;
    Ok(tdn::CreateForeignTableStmt {
        base: mcx::alloc_in(mcx, base)?,
        servername: cstr_opt(mcx, s.servername)?,
        options: node_list(mcx, s.options)?,
    })
}

fn conv_createusermappingstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::CreateUserMappingStmt,
) -> PgResult<tdn::CreateUserMappingStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::CreateUserMappingStmt {
        user: child_node_opt(mcx, s.user)?,
        servername: cstr_opt(mcx, s.servername)?,
        if_not_exists: s.if_not_exists,
        options: node_list(mcx, s.options)?,
    })
}

fn conv_dropusermappingstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::DropUserMappingStmt,
) -> PgResult<tdn::DropUserMappingStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::DropUserMappingStmt {
        user: child_node_opt(mcx, s.user)?,
        servername: cstr_opt(mcx, s.servername)?,
        missing_ok: s.missing_ok,
    })
}

fn conv_importforeignschemastmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::ImportForeignSchemaStmt,
) -> PgResult<tdn::ImportForeignSchemaStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::ImportForeignSchemaStmt {
        server_name: cstr_opt(mcx, s.server_name)?,
        remote_schema: cstr_opt(mcx, s.remote_schema)?,
        local_schema: cstr_opt(mcx, s.local_schema)?,
        list_type: import_foreign_schema_type(s.list_type),
        table_list: node_list(mcx, s.table_list)?,
        options: node_list(mcx, s.options)?,
    })
}

fn conv_createpolicystmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::CreatePolicyStmt,
) -> PgResult<tdn::CreatePolicyStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::CreatePolicyStmt {
        policy_name: cstr_opt(mcx, s.policy_name)?,
        table: child_node_opt(mcx, s.table)?,
        cmd_name: cstr_opt(mcx, s.cmd_name)?,
        permissive: s.permissive,
        roles: node_list(mcx, s.roles)?,
        qual: node_opt(mcx, s.qual)?,
        with_check: node_opt(mcx, s.with_check)?,
    })
}

fn conv_publicationtable<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::PublicationTable,
) -> PgResult<tdn::PublicationTable<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::PublicationTable {
        relation: child_node_opt(mcx, s.relation)?,
        where_clause: node_opt(mcx, s.whereClause)?,
        columns: node_list(mcx, s.columns)?,
    })
}

fn conv_publicationobjspec<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::PublicationObjSpec,
) -> PgResult<tdn::PublicationObjSpec<'mcx>> {
    let s = unsafe { &*p };
    let pubtable = child_opt(mcx, s.pubtable, conv_publicationtable)?;
    Ok(tdn::PublicationObjSpec {
        pubobjtype: publication_obj_spec_type(s.pubobjtype),
        name: cstr_opt(mcx, s.name)?,
        pubtable,
        location: s.location,
    })
}

fn conv_createpublicationstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::CreatePublicationStmt,
) -> PgResult<tdn::CreatePublicationStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::CreatePublicationStmt {
        pubname: cstr_opt(mcx, s.pubname)?,
        options: node_list(mcx, s.options)?,
        pubobjects: node_list(mcx, s.pubobjects)?,
        for_all_tables: s.for_all_tables,
    })
}

fn conv_createsubscriptionstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::CreateSubscriptionStmt,
) -> PgResult<tdn::CreateSubscriptionStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::CreateSubscriptionStmt {
        subname: cstr_opt(mcx, s.subname)?,
        conninfo: cstr_opt(mcx, s.conninfo)?,
        publication: node_list(mcx, s.publication)?,
        options: node_list(mcx, s.options)?,
    })
}

fn conv_dropsubscriptionstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::DropSubscriptionStmt,
) -> PgResult<tdn::DropSubscriptionStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::DropSubscriptionStmt {
        subname: cstr_opt(mcx, s.subname)?,
        missing_ok: s.missing_ok,
        behavior: drop_behavior(s.behavior),
    })
}

fn conv_createeventtrigstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::CreateEventTrigStmt,
) -> PgResult<tdn::CreateEventTrigStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::CreateEventTrigStmt {
        trigname: cstr_opt(mcx, s.trigname)?,
        eventname: cstr_opt(mcx, s.eventname)?,
        whenclause: node_list(mcx, s.whenclause)?,
        funcname: node_list(mcx, s.funcname)?,
    })
}

fn conv_altereventtrigstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::AlterEventTrigStmt,
) -> PgResult<tdn::AlterEventTrigStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::AlterEventTrigStmt {
        trigname: cstr_opt(mcx, s.trigname)?,
        tgenabled: s.tgenabled as i8,
    })
}

fn conv_createtransformstmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: *mut cd::CreateTransformStmt,
) -> PgResult<tdn::CreateTransformStmt<'mcx>> {
    let s = unsafe { &*p };
    Ok(tdn::CreateTransformStmt {
        replace: s.replace,
        type_name: child_node_opt(mcx, s.type_name)?,
        lang: cstr_opt(mcx, s.lang)?,
        fromsql: child_node_opt(mcx, s.fromsql)?,
        tosql: child_node_opt(mcx, s.tosql)?,
    })
}
