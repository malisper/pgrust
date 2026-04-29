use crate::backend::catalog::catalog::CatalogEntry;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlTypeKind, parse_expr};
use crate::backend::utils::cache::catcache::sql_type_oid;
use crate::include::catalog::{
    DEPENDENCY_AUTO, DEPENDENCY_INTERNAL, DEPENDENCY_NORMAL, PG_AM_RELATION_OID,
    PG_ATTRDEF_RELATION_OID, PG_CLASS_RELATION_OID, PG_COLLATION_RELATION_OID,
    PG_CONSTRAINT_RELATION_OID, PG_CONVERSION_RELATION_OID, PG_FOREIGN_DATA_WRAPPER_RELATION_OID,
    PG_FOREIGN_SERVER_RELATION_OID, PG_LANGUAGE_RELATION_OID, PG_NAMESPACE_RELATION_OID,
    PG_OPCLASS_RELATION_OID, PG_OPERATOR_RELATION_OID, PG_OPFAMILY_RELATION_OID,
    PG_PROC_RELATION_OID, PG_PUBLICATION_NAMESPACE_RELATION_OID, PG_PUBLICATION_REL_RELATION_OID,
    PG_PUBLICATION_RELATION_OID, PG_REWRITE_RELATION_OID, PG_STATISTIC_EXT_RELATION_OID,
    PG_TRIGGER_RELATION_OID, PG_TS_CONFIG_RELATION_OID, PG_TS_DICT_RELATION_OID,
    PG_TS_PARSER_RELATION_OID, PG_TS_TEMPLATE_RELATION_OID, PG_TYPE_RELATION_OID, PgAggregateRow,
    PgDependRow, PgForeignServerRow, PgLanguageRow, PgOpclassRow, PgOpfamilyRow, PgStatisticExtRow,
    PgTsConfigRow, PgTsDictRow, PgTsParserRow, PgTsTemplateRow,
};
use crate::include::nodes::parsenodes::{SqlExpr, function_arg_values};
use std::collections::BTreeSet;

pub fn sort_pg_depend_rows(rows: &mut [PgDependRow]) {
    rows.sort_by_key(|row| {
        (
            row.classid,
            row.objid,
            row.objsubid,
            row.refclassid,
            row.refobjid,
            row.refobjsubid,
            row.deptype as u32,
        )
    });
}

pub fn derived_pg_depend_rows(entry: &CatalogEntry) -> Vec<PgDependRow> {
    let mut rows = derived_relation_depend_rows(
        entry.relation_oid,
        entry.namespace_oid,
        entry.of_type_oid,
        entry.row_type_oid,
        &entry.desc,
    );
    if let Some(index_meta) = &entry.index_meta {
        rows.extend(index_meta.indkey.iter().map(|attnum| PgDependRow {
            classid: PG_CLASS_RELATION_OID,
            objid: entry.relation_oid,
            objsubid: 0,
            refclassid: PG_CLASS_RELATION_OID,
            refobjid: index_meta.indrelid,
            refobjsubid: i32::from(*attnum),
            deptype: DEPENDENCY_AUTO,
        }));
    }
    sort_pg_depend_rows(&mut rows);
    rows
}

pub fn index_backed_constraint_depend_rows(
    constraint_oid: u32,
    relation_oid: u32,
    index_oid: u32,
) -> Vec<PgDependRow> {
    let mut rows = vec![
        PgDependRow {
            classid: PG_CONSTRAINT_RELATION_OID,
            objid: constraint_oid,
            objsubid: 0,
            refclassid: PG_CLASS_RELATION_OID,
            refobjid: relation_oid,
            refobjsubid: 0,
            deptype: DEPENDENCY_AUTO,
        },
        PgDependRow {
            classid: PG_CONSTRAINT_RELATION_OID,
            objid: constraint_oid,
            objsubid: 0,
            refclassid: PG_CLASS_RELATION_OID,
            refobjid: index_oid,
            refobjsubid: 0,
            deptype: DEPENDENCY_INTERNAL,
        },
    ];
    sort_pg_depend_rows(&mut rows);
    rows
}

pub fn relation_constraint_depend_rows(constraint_oid: u32, relation_oid: u32) -> Vec<PgDependRow> {
    let mut rows = vec![PgDependRow {
        classid: PG_CONSTRAINT_RELATION_OID,
        objid: constraint_oid,
        objsubid: 0,
        refclassid: PG_CLASS_RELATION_OID,
        refobjid: relation_oid,
        refobjsubid: 0,
        deptype: DEPENDENCY_AUTO,
    }];
    sort_pg_depend_rows(&mut rows);
    rows
}

pub fn sequence_owned_by_depend_row(
    sequence_oid: u32,
    relation_oid: u32,
    attnum: i32,
) -> PgDependRow {
    PgDependRow {
        classid: PG_CLASS_RELATION_OID,
        objid: sequence_oid,
        objsubid: 0,
        refclassid: PG_CLASS_RELATION_OID,
        refobjid: relation_oid,
        refobjsubid: attnum,
        deptype: DEPENDENCY_AUTO,
    }
}

pub fn foreign_key_constraint_depend_rows(
    constraint_oid: u32,
    child_relation_oid: u32,
    parent_relation_oid: u32,
    parent_index_oid: u32,
) -> Vec<PgDependRow> {
    let mut rows = vec![
        PgDependRow {
            classid: PG_CONSTRAINT_RELATION_OID,
            objid: constraint_oid,
            objsubid: 0,
            refclassid: PG_CLASS_RELATION_OID,
            refobjid: child_relation_oid,
            refobjsubid: 0,
            deptype: DEPENDENCY_AUTO,
        },
        PgDependRow {
            classid: PG_CONSTRAINT_RELATION_OID,
            objid: constraint_oid,
            objsubid: 0,
            refclassid: PG_CLASS_RELATION_OID,
            refobjid: parent_relation_oid,
            refobjsubid: 0,
            deptype: DEPENDENCY_NORMAL,
        },
        PgDependRow {
            classid: PG_CONSTRAINT_RELATION_OID,
            objid: constraint_oid,
            objsubid: 0,
            refclassid: PG_CLASS_RELATION_OID,
            refobjid: parent_index_oid,
            refobjsubid: 0,
            deptype: DEPENDENCY_NORMAL,
        },
    ];
    sort_pg_depend_rows(&mut rows);
    rows
}

pub fn inheritance_depend_rows(relation_oid: u32, parent_oids: &[u32]) -> Vec<PgDependRow> {
    let mut rows = parent_oids
        .iter()
        .copied()
        .map(|parent_oid| PgDependRow {
            classid: PG_CLASS_RELATION_OID,
            objid: relation_oid,
            objsubid: 0,
            refclassid: PG_CLASS_RELATION_OID,
            refobjid: parent_oid,
            refobjsubid: 0,
            deptype: DEPENDENCY_NORMAL,
        })
        .collect::<Vec<_>>();
    sort_pg_depend_rows(&mut rows);
    rows
}

pub fn primary_key_owned_not_null_depend_rows(
    not_null_constraint_oid: u32,
    primary_constraint_oid: u32,
) -> Vec<PgDependRow> {
    let mut rows = vec![PgDependRow {
        classid: PG_CONSTRAINT_RELATION_OID,
        objid: not_null_constraint_oid,
        objsubid: 0,
        refclassid: PG_CONSTRAINT_RELATION_OID,
        refobjid: primary_constraint_oid,
        refobjsubid: 0,
        deptype: DEPENDENCY_INTERNAL,
    }];
    sort_pg_depend_rows(&mut rows);
    rows
}

pub fn derived_relation_depend_rows(
    relation_oid: u32,
    namespace_oid: u32,
    of_type_oid: u32,
    row_type_oid: u32,
    desc: &RelationDesc,
) -> Vec<PgDependRow> {
    let mut rows = vec![PgDependRow {
        classid: PG_CLASS_RELATION_OID,
        objid: relation_oid,
        objsubid: 0,
        refclassid: PG_NAMESPACE_RELATION_OID,
        refobjid: namespace_oid,
        refobjsubid: 0,
        deptype: DEPENDENCY_NORMAL,
    }];

    if row_type_oid != 0 {
        rows.push(PgDependRow {
            classid: PG_TYPE_RELATION_OID,
            objid: row_type_oid,
            objsubid: 0,
            refclassid: PG_CLASS_RELATION_OID,
            refobjid: relation_oid,
            refobjsubid: 0,
            deptype: DEPENDENCY_INTERNAL,
        });
    }
    if of_type_oid != 0 {
        rows.push(PgDependRow {
            classid: PG_CLASS_RELATION_OID,
            objid: relation_oid,
            objsubid: 0,
            refclassid: PG_TYPE_RELATION_OID,
            refobjid: of_type_oid,
            refobjsubid: 0,
            deptype: DEPENDENCY_NORMAL,
        });
    }

    rows.extend(desc.columns.iter().enumerate().filter_map(|(idx, column)| {
        Some(PgDependRow {
            classid: PG_ATTRDEF_RELATION_OID,
            objid: column.attrdef_oid?,
            objsubid: 0,
            refclassid: PG_CLASS_RELATION_OID,
            refobjid: relation_oid,
            refobjsubid: idx.saturating_add(1) as i32,
            deptype: DEPENDENCY_AUTO,
        })
    }));
    rows.extend(generated_column_attrdef_depend_rows(relation_oid, desc));
    rows.extend(desc.columns.iter().enumerate().filter_map(|(idx, column)| {
        Some(PgDependRow {
            classid: PG_CONSTRAINT_RELATION_OID,
            objid: column.not_null_constraint_oid?,
            objsubid: 0,
            refclassid: PG_CLASS_RELATION_OID,
            refobjid: relation_oid,
            refobjsubid: idx.saturating_add(1) as i32,
            deptype: DEPENDENCY_AUTO,
        })
    }));
    let mut referenced_type_oids = BTreeSet::new();
    for column in &desc.columns {
        let type_oid = sql_type_oid(column.sql_type);
        if type_oid != 0 {
            referenced_type_oids.insert(type_oid);
        }
        if column.sql_type.is_array
            && matches!(
                column.sql_type.kind,
                SqlTypeKind::Composite | SqlTypeKind::Record
            )
            && column.sql_type.type_oid != 0
        {
            referenced_type_oids.insert(column.sql_type.type_oid);
        }
    }
    rows.extend(
        referenced_type_oids
            .into_iter()
            .map(|type_oid| PgDependRow {
                classid: PG_CLASS_RELATION_OID,
                objid: relation_oid,
                objsubid: 0,
                refclassid: PG_TYPE_RELATION_OID,
                refobjid: type_oid,
                refobjsubid: 0,
                deptype: DEPENDENCY_NORMAL,
            }),
    );
    rows
}

fn generated_column_attrdef_depend_rows(
    relation_oid: u32,
    desc: &RelationDesc,
) -> Vec<PgDependRow> {
    let column_names = desc
        .columns
        .iter()
        .enumerate()
        .filter_map(|(idx, column)| {
            (!column.dropped).then(|| (column.name.to_ascii_lowercase(), (idx + 1) as i32))
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut rows = Vec::new();
    for column in &desc.columns {
        if column.dropped || column.generated.is_none() {
            continue;
        }
        let (Some(attrdef_oid), Some(expr_sql)) =
            (column.attrdef_oid, column.default_expr.as_ref())
        else {
            continue;
        };
        let Ok(expr) = parse_expr(expr_sql) else {
            continue;
        };
        let mut referenced = BTreeSet::new();
        collect_sql_expr_column_names(&expr, &mut referenced);
        rows.extend(referenced.into_iter().filter_map(|name| {
            let attnum = column_names.get(&name.to_ascii_lowercase()).copied()?;
            Some(PgDependRow {
                classid: PG_ATTRDEF_RELATION_OID,
                objid: attrdef_oid,
                objsubid: 0,
                refclassid: PG_CLASS_RELATION_OID,
                refobjid: relation_oid,
                refobjsubid: attnum,
                deptype: DEPENDENCY_NORMAL,
            })
        }));
    }
    rows
}

pub(crate) fn collect_sql_expr_column_names(expr: &SqlExpr, out: &mut BTreeSet<String>) {
    match expr {
        SqlExpr::Column(name) => {
            out.insert(name.clone());
        }
        SqlExpr::Parameter(_) => {}
        SqlExpr::Add(left, right)
        | SqlExpr::Sub(left, right)
        | SqlExpr::BitAnd(left, right)
        | SqlExpr::BitOr(left, right)
        | SqlExpr::BitXor(left, right)
        | SqlExpr::Shl(left, right)
        | SqlExpr::Shr(left, right)
        | SqlExpr::Mul(left, right)
        | SqlExpr::Div(left, right)
        | SqlExpr::Mod(left, right)
        | SqlExpr::Concat(left, right)
        | SqlExpr::Eq(left, right)
        | SqlExpr::NotEq(left, right)
        | SqlExpr::Lt(left, right)
        | SqlExpr::LtEq(left, right)
        | SqlExpr::Gt(left, right)
        | SqlExpr::GtEq(left, right)
        | SqlExpr::RegexMatch(left, right)
        | SqlExpr::And(left, right)
        | SqlExpr::Or(left, right)
        | SqlExpr::IsDistinctFrom(left, right)
        | SqlExpr::IsNotDistinctFrom(left, right)
        | SqlExpr::Overlaps(left, right)
        | SqlExpr::ArrayOverlap(left, right)
        | SqlExpr::ArrayContains(left, right)
        | SqlExpr::ArrayContained(left, right)
        | SqlExpr::JsonbContains(left, right)
        | SqlExpr::JsonbContained(left, right)
        | SqlExpr::JsonbExists(left, right)
        | SqlExpr::JsonbExistsAny(left, right)
        | SqlExpr::JsonbExistsAll(left, right)
        | SqlExpr::JsonbPathExists(left, right)
        | SqlExpr::JsonbPathMatch(left, right)
        | SqlExpr::JsonGet(left, right)
        | SqlExpr::JsonGetText(left, right)
        | SqlExpr::JsonPath(left, right)
        | SqlExpr::JsonPathText(left, right) => {
            collect_sql_expr_column_names(left, out);
            collect_sql_expr_column_names(right, out);
        }
        SqlExpr::BinaryOperator { left, right, .. }
        | SqlExpr::GeometryBinaryOp { left, right, .. } => {
            collect_sql_expr_column_names(left, out);
            collect_sql_expr_column_names(right, out);
        }
        SqlExpr::UnaryPlus(inner)
        | SqlExpr::Negate(inner)
        | SqlExpr::BitNot(inner)
        | SqlExpr::Cast(inner, _)
        | SqlExpr::Not(inner)
        | SqlExpr::IsNull(inner)
        | SqlExpr::IsNotNull(inner)
        | SqlExpr::FieldSelect { expr: inner, .. } => {
            collect_sql_expr_column_names(inner, out);
        }
        SqlExpr::Subscript { expr: inner, .. }
        | SqlExpr::GeometryUnaryOp { expr: inner, .. }
        | SqlExpr::PrefixOperator { expr: inner, .. }
        | SqlExpr::Collate { expr: inner, .. } => {
            collect_sql_expr_column_names(inner, out);
        }
        SqlExpr::AtTimeZone { expr, zone } => {
            collect_sql_expr_column_names(expr, out);
            collect_sql_expr_column_names(zone, out);
        }
        SqlExpr::Like {
            expr,
            pattern,
            escape,
            ..
        }
        | SqlExpr::Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            collect_sql_expr_column_names(expr, out);
            collect_sql_expr_column_names(pattern, out);
            if let Some(escape) = escape {
                collect_sql_expr_column_names(escape, out);
            }
        }
        SqlExpr::Case {
            arg,
            args,
            defresult,
        } => {
            if let Some(arg) = arg {
                collect_sql_expr_column_names(arg, out);
            }
            for when in args {
                collect_sql_expr_column_names(&when.expr, out);
                collect_sql_expr_column_names(&when.result, out);
            }
            if let Some(defresult) = defresult {
                collect_sql_expr_column_names(defresult, out);
            }
        }
        SqlExpr::ArrayLiteral(elements) | SqlExpr::Row(elements) => {
            for element in elements {
                collect_sql_expr_column_names(element, out);
            }
        }
        SqlExpr::ArraySubscript { array, subscripts } => {
            collect_sql_expr_column_names(array, out);
            for subscript in subscripts {
                if let Some(lower) = &subscript.lower {
                    collect_sql_expr_column_names(lower, out);
                }
                if let Some(upper) = &subscript.upper {
                    collect_sql_expr_column_names(upper, out);
                }
            }
        }
        SqlExpr::Xml(xml) => {
            for child in xml.child_exprs() {
                collect_sql_expr_column_names(child, out);
            }
        }
        SqlExpr::JsonQueryFunction(func) => {
            for child in func.child_exprs() {
                collect_sql_expr_column_names(child, out);
            }
        }
        SqlExpr::FuncCall {
            args,
            order_by,
            within_group,
            filter,
            ..
        } => {
            for arg in function_arg_values(args) {
                collect_sql_expr_column_names(arg, out);
            }
            for item in order_by {
                collect_sql_expr_column_names(&item.expr, out);
            }
            if let Some(within_group) = within_group {
                for item in within_group {
                    collect_sql_expr_column_names(&item.expr, out);
                }
            }
            if let Some(filter) = filter {
                collect_sql_expr_column_names(filter, out);
            }
        }
        SqlExpr::QuantifiedArray { left, array, .. } => {
            collect_sql_expr_column_names(left, out);
            collect_sql_expr_column_names(array, out);
        }
        SqlExpr::Default
        | SqlExpr::ParamRef(_)
        | SqlExpr::Const(_)
        | SqlExpr::IntegerLiteral(_)
        | SqlExpr::NumericLiteral(_)
        | SqlExpr::ScalarSubquery(_)
        | SqlExpr::ArraySubquery(_)
        | SqlExpr::Exists(_)
        | SqlExpr::InSubquery { .. }
        | SqlExpr::QuantifiedSubquery { .. }
        | SqlExpr::Random
        | SqlExpr::CurrentDate
        | SqlExpr::CurrentCatalog
        | SqlExpr::CurrentSchema
        | SqlExpr::CurrentUser
        | SqlExpr::SessionUser
        | SqlExpr::CurrentRole
        | SqlExpr::CurrentTime { .. }
        | SqlExpr::CurrentTimestamp { .. }
        | SqlExpr::LocalTime { .. }
        | SqlExpr::LocalTimestamp { .. } => {}
    }
}

pub fn proc_depend_rows(
    proc_oid: u32,
    namespace_oid: u32,
    return_type_oid: u32,
    arg_type_oids: &[u32],
) -> Vec<PgDependRow> {
    let mut rows = vec![PgDependRow {
        classid: PG_PROC_RELATION_OID,
        objid: proc_oid,
        objsubid: 0,
        refclassid: PG_NAMESPACE_RELATION_OID,
        refobjid: namespace_oid,
        refobjsubid: 0,
        deptype: DEPENDENCY_NORMAL,
    }];
    let mut referenced_type_oids = BTreeSet::new();
    if return_type_oid != 0 {
        referenced_type_oids.insert(return_type_oid);
    }
    referenced_type_oids.extend(arg_type_oids.iter().copied().filter(|oid| *oid != 0));
    rows.extend(
        referenced_type_oids
            .into_iter()
            .map(|type_oid| PgDependRow {
                classid: PG_PROC_RELATION_OID,
                objid: proc_oid,
                objsubid: 0,
                refclassid: PG_TYPE_RELATION_OID,
                refobjid: type_oid,
                refobjsubid: 0,
                deptype: DEPENDENCY_NORMAL,
            }),
    );
    sort_pg_depend_rows(&mut rows);
    rows
}

pub fn aggregate_depend_rows(
    proc_oid: u32,
    namespace_oid: u32,
    result_type_oid: u32,
    arg_type_oids: &[u32],
    aggregate_row: &PgAggregateRow,
) -> Vec<PgDependRow> {
    let mut rows = proc_depend_rows(proc_oid, namespace_oid, result_type_oid, arg_type_oids);
    for support_fn_oid in [
        aggregate_row.aggtransfn,
        aggregate_row.aggfinalfn,
        aggregate_row.aggcombinefn,
        aggregate_row.aggserialfn,
        aggregate_row.aggdeserialfn,
        aggregate_row.aggmtransfn,
        aggregate_row.aggminvtransfn,
        aggregate_row.aggmfinalfn,
    ] {
        if support_fn_oid != 0 {
            rows.push(PgDependRow {
                classid: PG_PROC_RELATION_OID,
                objid: proc_oid,
                objsubid: 0,
                refclassid: PG_PROC_RELATION_OID,
                refobjid: support_fn_oid,
                refobjsubid: 0,
                deptype: DEPENDENCY_NORMAL,
            });
        }
    }
    sort_pg_depend_rows(&mut rows);
    rows
}

pub fn operator_depend_rows(row: &crate::include::catalog::PgOperatorRow) -> Vec<PgDependRow> {
    let mut rows = vec![PgDependRow {
        classid: PG_OPERATOR_RELATION_OID,
        objid: row.oid,
        objsubid: 0,
        refclassid: PG_NAMESPACE_RELATION_OID,
        refobjid: row.oprnamespace,
        refobjsubid: 0,
        deptype: DEPENDENCY_NORMAL,
    }];

    for type_oid in [row.oprleft, row.oprright, row.oprresult]
        .into_iter()
        .filter(|oid| *oid != 0)
    {
        rows.push(PgDependRow {
            classid: PG_OPERATOR_RELATION_OID,
            objid: row.oid,
            objsubid: 0,
            refclassid: PG_TYPE_RELATION_OID,
            refobjid: type_oid,
            refobjsubid: 0,
            deptype: DEPENDENCY_NORMAL,
        });
    }

    for proc_oid in [row.oprcode, row.oprrest, row.oprjoin]
        .into_iter()
        .filter(|oid| *oid != 0)
    {
        rows.push(PgDependRow {
            classid: PG_OPERATOR_RELATION_OID,
            objid: row.oid,
            objsubid: 0,
            refclassid: PG_PROC_RELATION_OID,
            refobjid: proc_oid,
            refobjsubid: 0,
            deptype: DEPENDENCY_NORMAL,
        });
    }

    for operator_oid in [row.oprcom, row.oprnegate]
        .into_iter()
        .filter(|oid| *oid != 0)
    {
        rows.push(PgDependRow {
            classid: PG_OPERATOR_RELATION_OID,
            objid: row.oid,
            objsubid: 0,
            refclassid: PG_OPERATOR_RELATION_OID,
            refobjid: operator_oid,
            refobjsubid: 0,
            deptype: DEPENDENCY_NORMAL,
        });
    }

    sort_pg_depend_rows(&mut rows);
    rows
}

pub fn opfamily_depend_rows(row: &PgOpfamilyRow) -> Vec<PgDependRow> {
    let mut rows = vec![
        PgDependRow {
            classid: PG_OPFAMILY_RELATION_OID,
            objid: row.oid,
            objsubid: 0,
            refclassid: PG_NAMESPACE_RELATION_OID,
            refobjid: row.opfnamespace,
            refobjsubid: 0,
            deptype: DEPENDENCY_NORMAL,
        },
        PgDependRow {
            classid: PG_OPFAMILY_RELATION_OID,
            objid: row.oid,
            objsubid: 0,
            refclassid: PG_AM_RELATION_OID,
            refobjid: row.opfmethod,
            refobjsubid: 0,
            deptype: DEPENDENCY_NORMAL,
        },
    ];
    sort_pg_depend_rows(&mut rows);
    rows
}

pub fn opclass_depend_rows(row: &PgOpclassRow) -> Vec<PgDependRow> {
    let mut rows = vec![
        PgDependRow {
            classid: PG_OPCLASS_RELATION_OID,
            objid: row.oid,
            objsubid: 0,
            refclassid: PG_OPFAMILY_RELATION_OID,
            refobjid: row.opcfamily,
            refobjsubid: 0,
            deptype: DEPENDENCY_NORMAL,
        },
        PgDependRow {
            classid: PG_OPCLASS_RELATION_OID,
            objid: row.oid,
            objsubid: 0,
            refclassid: PG_NAMESPACE_RELATION_OID,
            refobjid: row.opcnamespace,
            refobjsubid: 0,
            deptype: DEPENDENCY_NORMAL,
        },
        PgDependRow {
            classid: PG_OPCLASS_RELATION_OID,
            objid: row.oid,
            objsubid: 0,
            refclassid: PG_TYPE_RELATION_OID,
            refobjid: row.opcintype,
            refobjsubid: 0,
            deptype: DEPENDENCY_NORMAL,
        },
        PgDependRow {
            classid: PG_OPCLASS_RELATION_OID,
            objid: row.oid,
            objsubid: 0,
            refclassid: PG_AM_RELATION_OID,
            refobjid: row.opcmethod,
            refobjsubid: 0,
            deptype: DEPENDENCY_NORMAL,
        },
    ];
    if row.opckeytype != 0 {
        rows.push(PgDependRow {
            classid: PG_OPCLASS_RELATION_OID,
            objid: row.oid,
            objsubid: 0,
            refclassid: PG_TYPE_RELATION_OID,
            refobjid: row.opckeytype,
            refobjsubid: 0,
            deptype: DEPENDENCY_NORMAL,
        });
    }
    sort_pg_depend_rows(&mut rows);
    rows
}

pub fn ts_dict_depend_rows(row: &PgTsDictRow) -> Vec<PgDependRow> {
    let mut rows = vec![
        PgDependRow {
            classid: PG_TS_DICT_RELATION_OID,
            objid: row.oid,
            objsubid: 0,
            refclassid: PG_NAMESPACE_RELATION_OID,
            refobjid: row.dictnamespace,
            refobjsubid: 0,
            deptype: DEPENDENCY_NORMAL,
        },
        PgDependRow {
            classid: PG_TS_DICT_RELATION_OID,
            objid: row.oid,
            objsubid: 0,
            refclassid: PG_TS_TEMPLATE_RELATION_OID,
            refobjid: row.dicttemplate,
            refobjsubid: 0,
            deptype: DEPENDENCY_NORMAL,
        },
    ];
    sort_pg_depend_rows(&mut rows);
    rows
}

pub fn ts_config_depend_rows(row: &PgTsConfigRow) -> Vec<PgDependRow> {
    let mut rows = vec![
        PgDependRow {
            classid: PG_TS_CONFIG_RELATION_OID,
            objid: row.oid,
            objsubid: 0,
            refclassid: PG_NAMESPACE_RELATION_OID,
            refobjid: row.cfgnamespace,
            refobjsubid: 0,
            deptype: DEPENDENCY_NORMAL,
        },
        PgDependRow {
            classid: PG_TS_CONFIG_RELATION_OID,
            objid: row.oid,
            objsubid: 0,
            refclassid: PG_TS_PARSER_RELATION_OID,
            refobjid: row.cfgparser,
            refobjsubid: 0,
            deptype: DEPENDENCY_NORMAL,
        },
    ];
    sort_pg_depend_rows(&mut rows);
    rows
}

pub fn ts_template_depend_rows(row: &PgTsTemplateRow) -> Vec<PgDependRow> {
    let mut rows = vec![PgDependRow {
        classid: PG_TS_TEMPLATE_RELATION_OID,
        objid: row.oid,
        objsubid: 0,
        refclassid: PG_NAMESPACE_RELATION_OID,
        refobjid: row.tmplnamespace,
        refobjsubid: 0,
        deptype: DEPENDENCY_NORMAL,
    }];
    for proc_oid in [row.tmplinit.unwrap_or(0), row.tmpllexize]
        .into_iter()
        .filter(|oid| *oid != 0)
    {
        rows.push(PgDependRow {
            classid: PG_TS_TEMPLATE_RELATION_OID,
            objid: row.oid,
            objsubid: 0,
            refclassid: PG_PROC_RELATION_OID,
            refobjid: proc_oid,
            refobjsubid: 0,
            deptype: DEPENDENCY_NORMAL,
        });
    }
    sort_pg_depend_rows(&mut rows);
    rows
}

pub fn ts_parser_depend_rows(row: &PgTsParserRow) -> Vec<PgDependRow> {
    let mut rows = vec![PgDependRow {
        classid: PG_TS_PARSER_RELATION_OID,
        objid: row.oid,
        objsubid: 0,
        refclassid: PG_NAMESPACE_RELATION_OID,
        refobjid: row.prsnamespace,
        refobjsubid: 0,
        deptype: DEPENDENCY_NORMAL,
    }];
    for proc_oid in [
        row.prsstart,
        row.prstoken,
        row.prsend,
        row.prsheadline.unwrap_or(0),
        row.prslextype,
    ]
    .into_iter()
    .filter(|oid| *oid != 0)
    {
        rows.push(PgDependRow {
            classid: PG_TS_PARSER_RELATION_OID,
            objid: row.oid,
            objsubid: 0,
            refclassid: PG_PROC_RELATION_OID,
            refobjid: proc_oid,
            refobjsubid: 0,
            deptype: DEPENDENCY_NORMAL,
        });
    }
    sort_pg_depend_rows(&mut rows);
    rows
}

pub fn conversion_depend_rows(row: &crate::include::catalog::PgConversionRow) -> Vec<PgDependRow> {
    let mut rows = vec![PgDependRow {
        classid: PG_CONVERSION_RELATION_OID,
        objid: row.oid,
        objsubid: 0,
        refclassid: PG_NAMESPACE_RELATION_OID,
        refobjid: row.connamespace,
        refobjsubid: 0,
        deptype: DEPENDENCY_NORMAL,
    }];
    if row.conproc != 0 {
        rows.push(PgDependRow {
            classid: PG_CONVERSION_RELATION_OID,
            objid: row.oid,
            objsubid: 0,
            refclassid: PG_PROC_RELATION_OID,
            refobjid: row.conproc,
            refobjsubid: 0,
            deptype: DEPENDENCY_NORMAL,
        });
    }
    sort_pg_depend_rows(&mut rows);
    rows
}

pub fn collation_depend_rows(row: &crate::include::catalog::PgCollationRow) -> Vec<PgDependRow> {
    let mut rows = vec![PgDependRow {
        classid: PG_COLLATION_RELATION_OID,
        objid: row.oid,
        objsubid: 0,
        refclassid: PG_NAMESPACE_RELATION_OID,
        refobjid: row.collnamespace,
        refobjsubid: 0,
        deptype: DEPENDENCY_NORMAL,
    }];
    sort_pg_depend_rows(&mut rows);
    rows
}

pub fn publication_rel_depend_rows(
    publication_rel_oid: u32,
    publication_oid: u32,
    relation_oid: u32,
) -> Vec<PgDependRow> {
    let mut rows = vec![
        PgDependRow {
            classid: PG_PUBLICATION_REL_RELATION_OID,
            objid: publication_rel_oid,
            objsubid: 0,
            refclassid: PG_PUBLICATION_RELATION_OID,
            refobjid: publication_oid,
            refobjsubid: 0,
            deptype: DEPENDENCY_AUTO,
        },
        PgDependRow {
            classid: PG_PUBLICATION_REL_RELATION_OID,
            objid: publication_rel_oid,
            objsubid: 0,
            refclassid: PG_CLASS_RELATION_OID,
            refobjid: relation_oid,
            refobjsubid: 0,
            deptype: DEPENDENCY_AUTO,
        },
    ];
    sort_pg_depend_rows(&mut rows);
    rows
}

pub fn publication_namespace_depend_rows(
    publication_namespace_oid: u32,
    publication_oid: u32,
    namespace_oid: u32,
) -> Vec<PgDependRow> {
    let mut rows = vec![
        PgDependRow {
            classid: PG_PUBLICATION_NAMESPACE_RELATION_OID,
            objid: publication_namespace_oid,
            objsubid: 0,
            refclassid: PG_PUBLICATION_RELATION_OID,
            refobjid: publication_oid,
            refobjsubid: 0,
            deptype: DEPENDENCY_AUTO,
        },
        PgDependRow {
            classid: PG_PUBLICATION_NAMESPACE_RELATION_OID,
            objid: publication_namespace_oid,
            objsubid: 0,
            refclassid: PG_NAMESPACE_RELATION_OID,
            refobjid: namespace_oid,
            refobjsubid: 0,
            deptype: DEPENDENCY_AUTO,
        },
    ];
    sort_pg_depend_rows(&mut rows);
    rows
}

pub fn statistic_ext_depend_rows(row: &PgStatisticExtRow) -> Vec<PgDependRow> {
    let mut rows = vec![PgDependRow {
        classid: PG_STATISTIC_EXT_RELATION_OID,
        objid: row.oid,
        objsubid: 0,
        refclassid: PG_NAMESPACE_RELATION_OID,
        refobjid: row.stxnamespace,
        refobjsubid: 0,
        deptype: DEPENDENCY_NORMAL,
    }];
    rows.push(PgDependRow {
        classid: PG_STATISTIC_EXT_RELATION_OID,
        objid: row.oid,
        objsubid: 0,
        refclassid: PG_CLASS_RELATION_OID,
        refobjid: row.stxrelid,
        refobjsubid: 0,
        deptype: DEPENDENCY_AUTO,
    });
    rows.extend(row.stxkeys.iter().copied().map(|attnum| PgDependRow {
        classid: PG_STATISTIC_EXT_RELATION_OID,
        objid: row.oid,
        objsubid: 0,
        refclassid: PG_CLASS_RELATION_OID,
        refobjid: row.stxrelid,
        refobjsubid: i32::from(attnum),
        deptype: DEPENDENCY_AUTO,
    }));
    sort_pg_depend_rows(&mut rows);
    rows
}

pub fn trigger_depend_rows(
    trigger_oid: u32,
    relation_oid: u32,
    proc_oid: u32,
    column_attnums: &[i16],
    constraint_oid: u32,
) -> Vec<PgDependRow> {
    let mut rows = vec![
        PgDependRow {
            classid: PG_TRIGGER_RELATION_OID,
            objid: trigger_oid,
            objsubid: 0,
            refclassid: PG_CLASS_RELATION_OID,
            refobjid: relation_oid,
            refobjsubid: 0,
            deptype: DEPENDENCY_AUTO,
        },
        PgDependRow {
            classid: PG_TRIGGER_RELATION_OID,
            objid: trigger_oid,
            objsubid: 0,
            refclassid: PG_PROC_RELATION_OID,
            refobjid: proc_oid,
            refobjsubid: 0,
            deptype: DEPENDENCY_NORMAL,
        },
    ];
    if constraint_oid != 0 {
        rows.push(PgDependRow {
            classid: PG_TRIGGER_RELATION_OID,
            objid: trigger_oid,
            objsubid: 0,
            refclassid: PG_CONSTRAINT_RELATION_OID,
            refobjid: constraint_oid,
            refobjsubid: 0,
            deptype: DEPENDENCY_INTERNAL,
        });
    }
    rows.extend(
        column_attnums
            .iter()
            .copied()
            .filter(|attnum| *attnum > 0)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(|attnum| PgDependRow {
                classid: PG_TRIGGER_RELATION_OID,
                objid: trigger_oid,
                objsubid: 0,
                refclassid: PG_CLASS_RELATION_OID,
                refobjid: relation_oid,
                refobjsubid: i32::from(attnum),
                deptype: DEPENDENCY_NORMAL,
            }),
    );
    sort_pg_depend_rows(&mut rows);
    rows
}

pub fn foreign_data_wrapper_depend_rows(
    fdw_oid: u32,
    handler_oid: u32,
    validator_oid: u32,
) -> Vec<PgDependRow> {
    let mut rows = Vec::new();
    if handler_oid != 0 {
        rows.push(PgDependRow {
            classid: PG_FOREIGN_DATA_WRAPPER_RELATION_OID,
            objid: fdw_oid,
            objsubid: 0,
            refclassid: PG_PROC_RELATION_OID,
            refobjid: handler_oid,
            refobjsubid: 0,
            deptype: DEPENDENCY_NORMAL,
        });
    }
    if validator_oid != 0 {
        rows.push(PgDependRow {
            classid: PG_FOREIGN_DATA_WRAPPER_RELATION_OID,
            objid: fdw_oid,
            objsubid: 0,
            refclassid: PG_PROC_RELATION_OID,
            refobjid: validator_oid,
            refobjsubid: 0,
            deptype: DEPENDENCY_NORMAL,
        });
    }
    sort_pg_depend_rows(&mut rows);
    rows
}

pub fn foreign_server_depend_rows(row: &PgForeignServerRow) -> Vec<PgDependRow> {
    let mut rows = vec![PgDependRow {
        classid: PG_FOREIGN_SERVER_RELATION_OID,
        objid: row.oid,
        objsubid: 0,
        refclassid: PG_FOREIGN_DATA_WRAPPER_RELATION_OID,
        refobjid: row.srvfdw,
        refobjsubid: 0,
        deptype: DEPENDENCY_NORMAL,
    }];
    sort_pg_depend_rows(&mut rows);
    rows
}

pub fn language_depend_rows(row: &PgLanguageRow) -> Vec<PgDependRow> {
    let mut rows = [row.lanplcallfoid, row.laninline, row.lanvalidator]
        .into_iter()
        .filter(|oid| *oid != 0)
        .map(|proc_oid| PgDependRow {
            classid: PG_LANGUAGE_RELATION_OID,
            objid: row.oid,
            objsubid: 0,
            refclassid: PG_PROC_RELATION_OID,
            refobjid: proc_oid,
            refobjsubid: 0,
            deptype: DEPENDENCY_NORMAL,
        })
        .collect::<Vec<_>>();
    sort_pg_depend_rows(&mut rows);
    rows
}

pub fn view_rewrite_depend_rows(
    rewrite_oid: u32,
    view_relation_oid: u32,
    referenced_relation_oids: &[u32],
) -> Vec<PgDependRow> {
    rewrite_depend_rows(
        rewrite_oid,
        view_relation_oid,
        referenced_relation_oids,
        DEPENDENCY_INTERNAL,
    )
}

pub fn relation_rule_depend_rows(
    rewrite_oid: u32,
    relation_oid: u32,
    referenced_relation_oids: &[u32],
) -> Vec<PgDependRow> {
    rewrite_depend_rows(
        rewrite_oid,
        relation_oid,
        referenced_relation_oids,
        DEPENDENCY_AUTO,
    )
}

fn rewrite_depend_rows(
    rewrite_oid: u32,
    relation_oid: u32,
    referenced_relation_oids: &[u32],
    owner_deptype: char,
) -> Vec<PgDependRow> {
    let mut rows = vec![PgDependRow {
        classid: PG_REWRITE_RELATION_OID,
        objid: rewrite_oid,
        objsubid: 0,
        refclassid: PG_CLASS_RELATION_OID,
        refobjid: relation_oid,
        refobjsubid: 0,
        deptype: owner_deptype,
    }];
    rows.extend(
        referenced_relation_oids
            .iter()
            .copied()
            .filter(|referenced_oid| *referenced_oid != relation_oid)
            .map(|relation_oid| PgDependRow {
                classid: PG_REWRITE_RELATION_OID,
                objid: rewrite_oid,
                objsubid: 0,
                refclassid: PG_CLASS_RELATION_OID,
                refobjid: relation_oid,
                refobjsubid: 0,
                deptype: DEPENDENCY_NORMAL,
            }),
    );
    sort_pg_depend_rows(&mut rows);
    rows
}
