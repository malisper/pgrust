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
