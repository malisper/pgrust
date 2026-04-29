use super::ddl::map_catalog_error;
use super::{Database, relation_lock_tag};
use crate::backend::catalog::store::CatalogWriteContext;
use crate::backend::executor::{
    ExecError, ExecutorContext, StatsImportRuntime, TypedFunctionArg, Value,
    parse_text_array_literal_with_catalog_and_op,
};
use crate::backend::libpq::pqformat::format_exec_error;
use crate::backend::parser::{BoundRelation, CatalogLookup, SqlType, SqlTypeKind};
use crate::backend::storage::lmgr::TableLockMode;
use crate::backend::storage::smgr::RelFileLocator;
use crate::backend::utils::cache::catcache::sql_type_oid;
use crate::backend::utils::misc::notices::{push_backend_notice, push_warning};
use crate::include::catalog::{
    FLOAT4_TYPE_OID, FLOAT8_TYPE_OID, PgStatisticRow, TEXT_TYPE_OID, relkind_is_analyzable,
};
use crate::include::nodes::datum::ArrayValue;
use crate::pgrust::compact_string::CompactString;
use std::collections::HashMap;
use std::sync::Arc;

const STATISTIC_KIND_MCV: i16 = 1;
const STATISTIC_KIND_HISTOGRAM: i16 = 2;
const STATISTIC_KIND_CORRELATION: i16 = 3;
const STATISTIC_KIND_MCELEM: i16 = 4;
const STATISTIC_KIND_DECHIST: i16 = 5;
const STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM: i16 = 6;
const STATISTIC_KIND_BOUNDS_HISTOGRAM: i16 = 7;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExpectedArgType {
    Text,
    Int2,
    Int4,
    Float4,
    Bool,
    Float4Array,
}

#[derive(Debug, Clone, Copy)]
struct ArgSpec {
    name: &'static str,
    expected: ExpectedArgType,
}

#[derive(Debug, Default)]
struct ParsedStatsArgs {
    values: HashMap<String, TypedFunctionArg>,
    ok: bool,
}

impl ParsedStatsArgs {
    fn get(&self, name: &str) -> Option<&TypedFunctionArg> {
        self.values.get(name)
    }
}

const RELATION_ARG_SPECS: &[ArgSpec] = &[
    ArgSpec {
        name: "schemaname",
        expected: ExpectedArgType::Text,
    },
    ArgSpec {
        name: "relname",
        expected: ExpectedArgType::Text,
    },
    ArgSpec {
        name: "relpages",
        expected: ExpectedArgType::Int4,
    },
    ArgSpec {
        name: "reltuples",
        expected: ExpectedArgType::Float4,
    },
    ArgSpec {
        name: "relallvisible",
        expected: ExpectedArgType::Int4,
    },
    ArgSpec {
        name: "relallfrozen",
        expected: ExpectedArgType::Int4,
    },
];

const ATTRIBUTE_ARG_SPECS: &[ArgSpec] = &[
    ArgSpec {
        name: "schemaname",
        expected: ExpectedArgType::Text,
    },
    ArgSpec {
        name: "relname",
        expected: ExpectedArgType::Text,
    },
    ArgSpec {
        name: "attname",
        expected: ExpectedArgType::Text,
    },
    ArgSpec {
        name: "attnum",
        expected: ExpectedArgType::Int2,
    },
    ArgSpec {
        name: "inherited",
        expected: ExpectedArgType::Bool,
    },
    ArgSpec {
        name: "null_frac",
        expected: ExpectedArgType::Float4,
    },
    ArgSpec {
        name: "avg_width",
        expected: ExpectedArgType::Int4,
    },
    ArgSpec {
        name: "n_distinct",
        expected: ExpectedArgType::Float4,
    },
    ArgSpec {
        name: "most_common_vals",
        expected: ExpectedArgType::Text,
    },
    ArgSpec {
        name: "most_common_freqs",
        expected: ExpectedArgType::Float4Array,
    },
    ArgSpec {
        name: "histogram_bounds",
        expected: ExpectedArgType::Text,
    },
    ArgSpec {
        name: "correlation",
        expected: ExpectedArgType::Float4,
    },
    ArgSpec {
        name: "most_common_elems",
        expected: ExpectedArgType::Text,
    },
    ArgSpec {
        name: "most_common_elem_freqs",
        expected: ExpectedArgType::Float4Array,
    },
    ArgSpec {
        name: "elem_count_histogram",
        expected: ExpectedArgType::Float4Array,
    },
    ArgSpec {
        name: "range_length_histogram",
        expected: ExpectedArgType::Text,
    },
    ArgSpec {
        name: "range_empty_frac",
        expected: ExpectedArgType::Float4,
    },
    ArgSpec {
        name: "range_bounds_histogram",
        expected: ExpectedArgType::Text,
    },
];

impl StatsImportRuntime for Database {
    fn pg_restore_relation_stats(
        &self,
        ctx: &mut ExecutorContext,
        args: Vec<TypedFunctionArg>,
    ) -> Result<Value, ExecError> {
        let parsed = parse_variadic_pairs(args, RELATION_ARG_SPECS)?;
        let mut result = parsed.ok;
        let schemaname = required_text_arg(&parsed, "schemaname")?;
        let relname = required_text_arg(&parsed, "relname")?;
        let relation = self.resolve_stats_relation(ctx, &schemaname, &relname)?;

        let relpages = optional_int4_arg(&parsed, "relpages");
        let mut reltuples = optional_float4_arg(&parsed, "reltuples");
        if reltuples.is_some_and(|value| value < -1.0) {
            push_warning("argument \"reltuples\" must not be less than -1.0");
            reltuples = None;
            result = false;
        }
        let relallvisible = optional_int4_arg(&parsed, "relallvisible");
        let relallfrozen = optional_int4_arg(&parsed, "relallfrozen");

        let xid = ctx.ensure_write_xid()?;
        let write_ctx = self.catalog_write_context(ctx, xid);
        let effect = self
            .catalog
            .write()
            .set_relation_import_stats_mvcc(
                relation.relation_oid,
                relpages,
                reltuples,
                relallvisible,
                relallfrozen,
                &write_ctx,
            )
            .map_err(map_catalog_error)?;
        ctx.record_catalog_effect(effect);
        Ok(Value::Bool(result))
    }

    fn pg_clear_relation_stats(
        &self,
        ctx: &mut ExecutorContext,
        schemaname: Value,
        relname: Value,
    ) -> Result<Value, ExecError> {
        let schemaname = required_direct_text("schemaname", &schemaname)?;
        let relname = required_direct_text("relname", &relname)?;
        let relation = self.resolve_stats_relation(ctx, &schemaname, &relname)?;
        let xid = ctx.ensure_write_xid()?;
        let write_ctx = self.catalog_write_context(ctx, xid);
        let effect = self
            .catalog
            .write()
            .set_relation_import_stats_mvcc(
                relation.relation_oid,
                Some(0),
                Some(-1.0),
                Some(0),
                Some(0),
                &write_ctx,
            )
            .map_err(map_catalog_error)?;
        ctx.record_catalog_effect(effect);
        Ok(Value::Null)
    }

    fn pg_restore_attribute_stats(
        &self,
        ctx: &mut ExecutorContext,
        args: Vec<TypedFunctionArg>,
    ) -> Result<Value, ExecError> {
        let parsed = parse_variadic_pairs(args, ATTRIBUTE_ARG_SPECS)?;
        let mut result = parsed.ok;
        let schemaname = required_text_arg(&parsed, "schemaname")?;
        let relname = required_text_arg(&parsed, "relname")?;
        let inherited = required_bool_arg(&parsed, "inherited")?;
        let relation = self.resolve_stats_relation(ctx, &schemaname, &relname)?;
        let (attnum, column_index) = resolve_attribute_arg(&relation, &parsed, &relname)?;
        let column = relation.desc.columns[column_index].clone();

        let mut row = ctx
            .catalog
            .as_ref()
            .and_then(|catalog| {
                catalog
                    .statistic_rows_for_relation(relation.relation_oid)
                    .into_iter()
                    .find(|row| row.staattnum == attnum && row.stainherit == inherited)
            })
            .unwrap_or_else(|| empty_statistic_row(relation.relation_oid, attnum, inherited));

        if let Some(value) = optional_float4_arg(&parsed, "null_frac") {
            row.stanullfrac = value;
        }
        if let Some(value) = optional_int4_arg(&parsed, "avg_width") {
            row.stawidth = value;
        }
        if let Some(value) = optional_float4_arg(&parsed, "n_distinct") {
            row.stadistinct = value;
        }

        let catalog = ctx.catalog.as_deref().ok_or_else(missing_catalog_context)?;
        let attr_type = statistic_attribute_type(column.sql_type);
        let attr_type_oid = sql_type_oid(attr_type);
        let eq_op = catalog
            .operator_by_name_left_right("=", attr_type_oid, attr_type_oid)
            .map(|row| row.oid)
            .unwrap_or(0);
        let lt_op = catalog
            .operator_by_name_left_right("<", attr_type_oid, attr_type_oid)
            .map(|row| row.oid)
            .unwrap_or(0);
        let collation = column.collation_oid;

        let do_mcv = paired_args_present(
            &parsed,
            "most_common_vals",
            "most_common_freqs",
            &mut result,
        );
        let mut do_histogram = parsed.get("histogram_bounds").is_some();
        let mut do_correlation = parsed.get("correlation").is_some();
        let do_mcelem = paired_args_present(
            &parsed,
            "most_common_elems",
            "most_common_elem_freqs",
            &mut result,
        );
        let do_dechist = parsed.get("elem_count_histogram").is_some();
        let mut do_bounds_histogram = parsed.get("range_bounds_histogram").is_some();
        let mut do_range_length_histogram = paired_args_present(
            &parsed,
            "range_length_histogram",
            "range_empty_frac",
            &mut result,
        );

        if (do_histogram || do_correlation) && lt_op == 0 {
            push_warning(format!(
                "could not determine less-than operator for column \"{}\"",
                column.name
            ));
            do_histogram = false;
            do_correlation = false;
            result = false;
        }

        if (do_range_length_histogram || do_bounds_histogram)
            && !attr_type.is_range()
            && !attr_type.is_multirange()
        {
            push_warning_with_detail(
                format!("column \"{}\" is not a range type", column.name),
                "Cannot set STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM or STATISTIC_KIND_BOUNDS_HISTOGRAM.",
            );
            do_bounds_histogram = false;
            do_range_length_histogram = false;
            result = false;
        }

        let (elem_type, elem_eq_op) = if do_mcelem || do_dechist {
            match element_stat_type(catalog, attr_type) {
                Some((elem_type, elem_eq_op)) => (Some(elem_type), elem_eq_op),
                None => {
                    push_warning_with_detail(
                        format!(
                            "could not determine element type of column \"{}\"",
                            column.name
                        ),
                        "Cannot set STATISTIC_KIND_MCELEM or STATISTIC_KIND_DECHIST.",
                    );
                    result = false;
                    (None, 0)
                }
            }
        } else {
            (None, 0)
        };

        if do_mcv {
            if let (Some(vals), Some(freqs)) = (
                parse_text_stat_array(ctx, &parsed, "most_common_vals", attr_type, &mut result)?,
                float4_array_arg(&parsed, "most_common_freqs", &mut result),
            ) {
                set_stats_slot(
                    &mut row,
                    STATISTIC_KIND_MCV,
                    eq_op,
                    collation,
                    Some(freqs),
                    Some(vals.with_element_type_oid(attr_type_oid)),
                )?;
            }
        }

        if do_histogram
            && let Some(values) =
                parse_text_stat_array(ctx, &parsed, "histogram_bounds", attr_type, &mut result)?
        {
            set_stats_slot(
                &mut row,
                STATISTIC_KIND_HISTOGRAM,
                lt_op,
                collation,
                None,
                Some(values.with_element_type_oid(attr_type_oid)),
            )?;
        }

        if do_correlation && let Some(value) = optional_float4_arg(&parsed, "correlation") {
            set_stats_slot(
                &mut row,
                STATISTIC_KIND_CORRELATION,
                lt_op,
                collation,
                Some(float4_singleton_array(value)),
                None,
            )?;
        }

        if do_mcelem
            && let Some(elem_type) = elem_type
            && let (Some(vals), Some(freqs)) = (
                parse_text_stat_array(ctx, &parsed, "most_common_elems", elem_type, &mut result)?,
                float4_array_arg(&parsed, "most_common_elem_freqs", &mut result),
            )
        {
            set_stats_slot(
                &mut row,
                STATISTIC_KIND_MCELEM,
                elem_eq_op,
                collation,
                Some(freqs),
                Some(vals.with_element_type_oid(sql_type_oid(elem_type))),
            )?;
        }

        if do_dechist
            && elem_type.is_some()
            && let Some(histogram) = float4_array_arg(&parsed, "elem_count_histogram", &mut result)
        {
            set_stats_slot(
                &mut row,
                STATISTIC_KIND_DECHIST,
                elem_eq_op,
                collation,
                Some(histogram),
                None,
            )?;
        }

        if do_bounds_histogram
            && let Some(values) = parse_text_stat_array(
                ctx,
                &parsed,
                "range_bounds_histogram",
                attr_type,
                &mut result,
            )?
        {
            set_stats_slot(
                &mut row,
                STATISTIC_KIND_BOUNDS_HISTOGRAM,
                0,
                0,
                None,
                Some(values.with_element_type_oid(attr_type_oid)),
            )?;
        }

        if do_range_length_histogram
            && let Some(range_empty_frac) = optional_float4_arg(&parsed, "range_empty_frac")
            && let Some(values) = parse_text_stat_array(
                ctx,
                &parsed,
                "range_length_histogram",
                SqlType::new(SqlTypeKind::Float8).with_identity(FLOAT8_TYPE_OID, 0),
                &mut result,
            )?
        {
            let float8_lt = catalog
                .operator_by_name_left_right("<", FLOAT8_TYPE_OID, FLOAT8_TYPE_OID)
                .map(|row| row.oid)
                .unwrap_or(0);
            set_stats_slot(
                &mut row,
                STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM,
                float8_lt,
                0,
                Some(float4_singleton_array(range_empty_frac)),
                Some(values.with_element_type_oid(FLOAT8_TYPE_OID)),
            )?;
        }

        let xid = ctx.ensure_write_xid()?;
        let write_ctx = self.catalog_write_context(ctx, xid);
        let effect = self
            .catalog
            .write()
            .upsert_relation_statistic_mvcc(row, &write_ctx)
            .map_err(map_catalog_error)?;
        ctx.record_catalog_effect(effect);
        Ok(Value::Bool(result))
    }

    fn pg_clear_attribute_stats(
        &self,
        ctx: &mut ExecutorContext,
        schemaname: Value,
        relname: Value,
        attname: Value,
        inherited: Value,
    ) -> Result<Value, ExecError> {
        let schemaname = required_direct_text("schemaname", &schemaname)?;
        let relname = required_direct_text("relname", &relname)?;
        let attname = required_direct_text("attname", &attname)?;
        let inherited = required_direct_bool("inherited", &inherited)?;
        let relation = self.resolve_stats_relation(ctx, &schemaname, &relname)?;
        let attnum = resolve_attribute_name(&relation, &attname, &relname, "clear")?.0;
        let xid = ctx.ensure_write_xid()?;
        let write_ctx = self.catalog_write_context(ctx, xid);
        let effect = self
            .catalog
            .write()
            .delete_relation_statistic_mvcc(relation.relation_oid, attnum, inherited, &write_ctx)
            .map_err(map_catalog_error)?;
        ctx.record_catalog_effect(effect);
        Ok(Value::Null)
    }
}

impl Database {
    fn catalog_write_context(&self, ctx: &ExecutorContext, xid: u32) -> CatalogWriteContext {
        CatalogWriteContext {
            pool: Arc::clone(&ctx.pool),
            txns: Arc::clone(&ctx.txns),
            xid,
            cid: ctx.next_command_id,
            client_id: ctx.client_id,
            waiter: ctx.txn_waiter.clone(),
            interrupts: Arc::clone(&ctx.interrupts),
        }
    }

    fn resolve_stats_relation(
        &self,
        ctx: &mut ExecutorContext,
        schemaname: &str,
        relname: &str,
    ) -> Result<crate::backend::parser::BoundRelation, ExecError> {
        let catalog = ctx.catalog.as_deref().ok_or_else(missing_catalog_context)?;
        let qualified = format!("{schemaname}.{relname}");
        if !schemaname.eq_ignore_ascii_case("pg_temp")
            && !catalog
                .namespace_rows()
                .into_iter()
                .any(|row| row.nspname.eq_ignore_ascii_case(schemaname))
        {
            return Err(ExecError::DetailedError {
                message: format!("schema \"{schemaname}\" does not exist"),
                detail: None,
                hint: None,
                sqlstate: "3F000",
            });
        }
        let relation = catalog
            .lookup_any_relation(&qualified)
            .ok_or_else(|| relation_does_not_exist_error(&qualified))?;

        if matches!(relation.relkind, 'i' | 'I') {
            let index = catalog
                .index_row_by_oid(relation.relation_oid)
                .ok_or_else(|| internal_error(format!("missing pg_index row for \"{relname}\"")))?;
            let parent = catalog.relation_by_oid(index.indrelid).ok_or_else(|| {
                internal_error(format!("missing heap relation for \"{relname}\""))
            })?;
            validate_stats_relkind(&parent, relname)?;
            self.lock_stats_relation(ctx, &parent)?;
            self.lock_stats_relation(ctx, &relation)?;
            return Ok(relation);
        }

        validate_stats_relkind(&relation, relname)?;
        self.lock_stats_relation(ctx, &relation)?;
        Ok(relation)
    }

    fn lock_stats_relation(
        &self,
        ctx: &mut ExecutorContext,
        relation: &BoundRelation,
    ) -> Result<(), ExecError> {
        let rel = stats_relation_lock_tag(relation);
        self.table_locks.lock_table_interruptible(
            rel,
            TableLockMode::ShareUpdateExclusive,
            ctx.client_id,
            ctx.interrupts.as_ref(),
        )?;
        ctx.record_table_lock(rel);
        Ok(())
    }
}

fn stats_relation_lock_tag(relation: &BoundRelation) -> RelFileLocator {
    relation_lock_tag(relation)
}

fn validate_stats_relkind(
    relation: &crate::backend::parser::BoundRelation,
    relname: &str,
) -> Result<(), ExecError> {
    if relkind_is_analyzable(relation.relkind) {
        return Ok(());
    }
    let detail = relkind_stats_unsupported_detail(relation.relkind);
    Err(ExecError::DetailedError {
        message: format!("cannot modify statistics for relation \"{relname}\""),
        detail: Some(detail.into()),
        hint: None,
        sqlstate: "42809",
    })
}

fn relkind_stats_unsupported_detail(relkind: char) -> &'static str {
    match relkind {
        'S' => "This operation is not supported for sequences.",
        'v' => "This operation is not supported for views.",
        'c' => "This operation is not supported for composite types.",
        't' => "This operation is not supported for TOAST tables.",
        'f' => "This operation is not supported for foreign tables.",
        'I' => "This operation is not supported for partitioned indexes.",
        _ => "This operation is not supported for this relation kind.",
    }
}

fn parse_variadic_pairs(
    args: Vec<TypedFunctionArg>,
    specs: &[ArgSpec],
) -> Result<ParsedStatsArgs, ExecError> {
    if args.len() % 2 != 0 {
        return Err(ExecError::DetailedError {
            message: "variadic arguments must be name/value pairs".into(),
            detail: None,
            hint: Some(
                "Provide an even number of variadic arguments that can be divided into pairs."
                    .into(),
            ),
            sqlstate: "22023",
        });
    }

    let mut parsed = ParsedStatsArgs {
        values: HashMap::new(),
        ok: true,
    };
    for (pair_index, pair) in args.chunks_exact(2).enumerate() {
        let name_arg = &pair[0];
        let value_arg = &pair[1];
        let variadic_pos = pair_index * 2 + 1;
        if matches!(name_arg.value, Value::Null) {
            return Err(ExecError::DetailedError {
                message: format!("name at variadic position {variadic_pos} is null"),
                detail: None,
                hint: None,
                sqlstate: "22023",
            });
        }
        let Some(name) = text_arg(name_arg) else {
            return Err(ExecError::DetailedError {
                message: format!(
                    "name at variadic position {variadic_pos} has type {}, expected type text",
                    typed_arg_type_name(name_arg)
                ),
                detail: None,
                hint: None,
                sqlstate: "22023",
            });
        };
        let normalized = name.to_ascii_lowercase();
        if normalized == "version" {
            continue;
        }
        let Some(spec) = specs.iter().find(|spec| spec.name == normalized) else {
            push_warning(format!("unrecognized argument name: \"{name}\""));
            parsed.ok = false;
            continue;
        };
        if matches!(value_arg.value, Value::Null) {
            continue;
        }
        if !typed_arg_matches(value_arg, spec.expected) {
            push_warning(format!(
                "argument \"{}\" has type {}, expected type {}",
                spec.name,
                typed_arg_type_name(value_arg),
                expected_type_name(spec.expected)
            ));
            parsed.ok = false;
            continue;
        }
        parsed.values.insert(normalized, value_arg.clone());
    }
    Ok(parsed)
}

fn text_arg(arg: &TypedFunctionArg) -> Option<String> {
    if let Some(sql_type) = arg.sql_type
        && (sql_type.is_array || sql_type.kind != SqlTypeKind::Text)
    {
        return None;
    }
    arg.value.as_text().map(ToOwned::to_owned)
}

fn typed_arg_matches(arg: &TypedFunctionArg, expected: ExpectedArgType) -> bool {
    let type_matches = match expected {
        ExpectedArgType::Text => arg
            .sql_type
            .is_none_or(|ty| !ty.is_array && ty.kind == SqlTypeKind::Text),
        ExpectedArgType::Int2 => arg
            .sql_type
            .is_none_or(|ty| !ty.is_array && ty.kind == SqlTypeKind::Int2),
        ExpectedArgType::Int4 => arg
            .sql_type
            .is_none_or(|ty| !ty.is_array && ty.kind == SqlTypeKind::Int4),
        ExpectedArgType::Float4 => arg
            .sql_type
            .is_none_or(|ty| !ty.is_array && ty.kind == SqlTypeKind::Float4),
        ExpectedArgType::Bool => arg
            .sql_type
            .is_none_or(|ty| !ty.is_array && ty.kind == SqlTypeKind::Bool),
        ExpectedArgType::Float4Array => arg
            .sql_type
            .is_none_or(|ty| ty.is_array && ty.kind == SqlTypeKind::Float4),
    };
    if !type_matches {
        return false;
    }
    match expected {
        ExpectedArgType::Text => arg.value.as_text().is_some(),
        ExpectedArgType::Int2 => matches!(arg.value, Value::Int16(_)),
        ExpectedArgType::Int4 => matches!(arg.value, Value::Int32(_)),
        ExpectedArgType::Float4 => matches!(arg.value, Value::Float64(_)),
        ExpectedArgType::Bool => matches!(arg.value, Value::Bool(_)),
        ExpectedArgType::Float4Array => arg.value.as_array_value().is_some(),
    }
}

fn typed_arg_type_name(arg: &TypedFunctionArg) -> String {
    arg.sql_type.map(sql_type_name).unwrap_or_else(|| {
        arg.value
            .sql_type_hint()
            .map(sql_type_name)
            .unwrap_or_else(|| "unknown".into())
    })
}

fn sql_type_name(sql_type: SqlType) -> String {
    let base = match sql_type.kind {
        SqlTypeKind::Text => "text",
        SqlTypeKind::Int2 => "smallint",
        SqlTypeKind::Int4 => "integer",
        SqlTypeKind::Float4 => "real",
        SqlTypeKind::Float8 => "double precision",
        SqlTypeKind::Bool => "boolean",
        SqlTypeKind::Name => "name",
        SqlTypeKind::Oid => "oid",
        SqlTypeKind::RegClass => "regclass",
        SqlTypeKind::RegProc => "regproc",
        SqlTypeKind::RegProcedure => "regprocedure",
        SqlTypeKind::RegOper => "regoper",
        SqlTypeKind::RegOperator => "regoperator",
        SqlTypeKind::RegType => "regtype",
        SqlTypeKind::RegRole => "regrole",
        SqlTypeKind::RegNamespace => "regnamespace",
        SqlTypeKind::RegCollation => "regcollation",
        _ => "value",
    };
    if sql_type.is_array {
        format!("{base}[]")
    } else {
        base.into()
    }
}

fn expected_type_name(expected: ExpectedArgType) -> &'static str {
    match expected {
        ExpectedArgType::Text => "text",
        ExpectedArgType::Int2 => "smallint",
        ExpectedArgType::Int4 => "integer",
        ExpectedArgType::Float4 => "real",
        ExpectedArgType::Bool => "boolean",
        ExpectedArgType::Float4Array => "real[]",
    }
}

fn push_warning_with_detail(message: impl Into<String>, detail: impl Into<String>) {
    push_backend_notice("WARNING", "01000", message, Some(detail.into()), None);
}

fn relation_does_not_exist_error(qualified: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("relation \"{qualified}\" does not exist"),
        detail: None,
        hint: None,
        sqlstate: "42P01",
    }
}

fn required_text_arg(parsed: &ParsedStatsArgs, name: &str) -> Result<String, ExecError> {
    parsed
        .get(name)
        .and_then(text_arg)
        .ok_or_else(|| required_arg_error(name))
}

fn required_bool_arg(parsed: &ParsedStatsArgs, name: &str) -> Result<bool, ExecError> {
    parsed
        .get(name)
        .and_then(|arg| match arg.value {
            Value::Bool(value) => Some(value),
            _ => None,
        })
        .ok_or_else(|| required_arg_error(name))
}

fn optional_int4_arg(parsed: &ParsedStatsArgs, name: &str) -> Option<i32> {
    parsed.get(name).and_then(|arg| match arg.value {
        Value::Int32(value) => Some(value),
        _ => None,
    })
}

fn optional_float4_arg(parsed: &ParsedStatsArgs, name: &str) -> Option<f64> {
    parsed.get(name).and_then(|arg| match arg.value {
        Value::Float64(value) => Some(value),
        _ => None,
    })
}

fn required_direct_text(name: &str, value: &Value) -> Result<String, ExecError> {
    if matches!(value, Value::Null) {
        return Err(required_arg_error(name));
    }
    value
        .as_text()
        .map(ToOwned::to_owned)
        .ok_or_else(|| type_error(name, value, Value::Text(CompactString::new(""))))
}

fn required_direct_bool(name: &str, value: &Value) -> Result<bool, ExecError> {
    match value {
        Value::Bool(value) => Ok(*value),
        Value::Null => Err(required_arg_error(name)),
        other => Err(type_error(name, other, Value::Bool(false))),
    }
}

fn required_arg_error(name: &str) -> ExecError {
    ExecError::DetailedError {
        message: format!("argument \"{name}\" must not be null"),
        detail: None,
        hint: None,
        sqlstate: "22023",
    }
}

fn type_error(op: &str, left: &Value, right: Value) -> ExecError {
    ExecError::TypeMismatch {
        op: Box::leak(op.to_string().into_boxed_str()),
        left: left.clone(),
        right,
    }
}

fn paired_args_present(
    parsed: &ParsedStatsArgs,
    left: &str,
    right: &str,
    result: &mut bool,
) -> bool {
    let left_present = parsed.get(left).is_some();
    let right_present = parsed.get(right).is_some();
    if left_present == right_present {
        return left_present;
    }
    let missing = if left_present { right } else { left };
    let present = if left_present { left } else { right };
    push_warning(format!(
        "argument \"{missing}\" must be specified when argument \"{present}\" is specified"
    ));
    *result = false;
    false
}

fn float4_array_arg(parsed: &ParsedStatsArgs, name: &str, result: &mut bool) -> Option<ArrayValue> {
    let mut array = parsed.get(name)?.value.as_array_value()?;
    if array.dimensions.len() != 1 {
        push_warning(format!(
            "argument \"{name}\" must not be a multidimensional array"
        ));
        *result = false;
        return None;
    }
    if array
        .elements
        .iter()
        .any(|value| matches!(value, Value::Null))
    {
        push_warning(format!(
            "argument \"{name}\" array must not contain null values"
        ));
        *result = false;
        return None;
    }
    array.element_type_oid = Some(FLOAT4_TYPE_OID);
    Some(array)
}

fn parse_text_stat_array(
    ctx: &ExecutorContext,
    parsed: &ParsedStatsArgs,
    name: &str,
    element_type: SqlType,
    result: &mut bool,
) -> Result<Option<ArrayValue>, ExecError> {
    let Some(raw) = parsed.get(name).and_then(text_arg) else {
        return Ok(None);
    };
    let catalog = ctx.catalog.as_deref();
    let value = match parse_text_array_literal_with_catalog_and_op(
        &raw,
        element_type,
        "::array",
        catalog,
    ) {
        Ok(value) => value,
        Err(err) => {
            push_warning(format_exec_error(&err));
            *result = false;
            return Ok(None);
        }
    };
    let Some(array) = value.as_array_value() else {
        push_warning(format!("\"{name}\" could not be converted to an array"));
        *result = false;
        return Ok(None);
    };
    if array.dimensions.len() != 1 {
        push_warning(format!("\"{name}\" must not be a multidimensional array"));
        *result = false;
        return Ok(None);
    }
    if array
        .elements
        .iter()
        .any(|value| matches!(value, Value::Null))
    {
        push_warning(format!("\"{name}\" array must not contain null values"));
        *result = false;
        return Ok(None);
    }
    Ok(Some(array))
}

fn float4_singleton_array(value: f64) -> ArrayValue {
    ArrayValue::from_1d(vec![Value::Float64(value)]).with_element_type_oid(FLOAT4_TYPE_OID)
}

fn empty_statistic_row(starelid: u32, staattnum: i16, stainherit: bool) -> PgStatisticRow {
    PgStatisticRow {
        starelid,
        staattnum,
        stainherit,
        stanullfrac: 0.0,
        stawidth: 0,
        stadistinct: 0.0,
        stakind: [0; 5],
        staop: [0; 5],
        stacoll: [0; 5],
        stanumbers: Default::default(),
        stavalues: Default::default(),
    }
}

fn set_stats_slot(
    row: &mut PgStatisticRow,
    stakind: i16,
    staop: u32,
    stacoll: u32,
    stanumbers: Option<ArrayValue>,
    stavalues: Option<ArrayValue>,
) -> Result<(), ExecError> {
    let mut first_empty = None;
    let mut slot = None;
    for idx in 0..5 {
        if first_empty.is_none() && row.stakind[idx] == 0 {
            first_empty = Some(idx);
        }
        if row.stakind[idx] == stakind {
            slot = Some(idx);
            break;
        }
    }
    let idx = slot
        .or(first_empty)
        .ok_or_else(|| ExecError::DetailedError {
            message: "maximum number of statistics slots exceeded: 6".into(),
            detail: None,
            hint: None,
            sqlstate: "54000",
        })?;
    row.stakind[idx] = stakind;
    row.staop[idx] = staop;
    row.stacoll[idx] = stacoll;
    if let Some(stanumbers) = stanumbers {
        row.stanumbers[idx] = Some(stanumbers);
    }
    if let Some(stavalues) = stavalues {
        row.stavalues[idx] = Some(stavalues);
    }
    Ok(())
}

fn statistic_attribute_type(sql_type: SqlType) -> SqlType {
    if sql_type.is_multirange() && sql_type.multirange_range_oid != 0 {
        return SqlType::new(SqlTypeKind::Range).with_identity(sql_type.multirange_range_oid, 0);
    }
    sql_type
}

fn element_stat_type(catalog: &dyn CatalogLookup, attr_type: SqlType) -> Option<(SqlType, u32)> {
    let elem_type = if attr_type.kind == SqlTypeKind::TsVector && !attr_type.is_array {
        SqlType::new(SqlTypeKind::Text).with_identity(TEXT_TYPE_OID, 0)
    } else if attr_type.is_array {
        attr_type.element_type()
    } else {
        let type_oid = sql_type_oid(attr_type);
        let type_row = catalog.type_by_oid(type_oid)?;
        if type_row.typelem == 0 {
            return None;
        }
        catalog.type_by_oid(type_row.typelem)?.sql_type
    };
    let elem_type_oid = sql_type_oid(elem_type);
    let elem_eq_op = catalog
        .operator_by_name_left_right("=", elem_type_oid, elem_type_oid)?
        .oid;
    Some((elem_type, elem_eq_op))
}

fn resolve_attribute_arg(
    relation: &crate::backend::parser::BoundRelation,
    parsed: &ParsedStatsArgs,
    relname: &str,
) -> Result<(i16, usize), ExecError> {
    let has_attname = parsed.get("attname").is_some();
    let has_attnum = parsed.get("attnum").is_some();
    match (has_attname, has_attnum) {
        (true, true) => Err(ExecError::DetailedError {
            message: "cannot specify both \"attname\" and \"attnum\"".into(),
            detail: None,
            hint: None,
            sqlstate: "22023",
        }),
        (false, false) => Err(ExecError::DetailedError {
            message: "must specify either \"attname\" or \"attnum\"".into(),
            detail: None,
            hint: None,
            sqlstate: "22023",
        }),
        (true, false) => {
            let attname = required_text_arg(parsed, "attname")?;
            resolve_attribute_name(relation, &attname, relname, "modify")
        }
        (false, true) => {
            let attnum = parsed
                .get("attnum")
                .and_then(|arg| match arg.value {
                    Value::Int16(value) => Some(value),
                    _ => None,
                })
                .ok_or_else(|| required_arg_error("attnum"))?;
            resolve_attribute_number(relation, attnum, relname)
        }
    }
}

fn resolve_attribute_name(
    relation: &crate::backend::parser::BoundRelation,
    attname: &str,
    relname: &str,
    verb: &str,
) -> Result<(i16, usize), ExecError> {
    if system_column_attnum(attname).is_some() {
        return Err(ExecError::DetailedError {
            message: format!("cannot {verb} statistics on system column \"{attname}\""),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }
    relation
        .desc
        .columns
        .iter()
        .enumerate()
        .find(|(_, column)| !column.dropped && column.name.eq_ignore_ascii_case(attname))
        .map(|(idx, _)| ((idx + 1) as i16, idx))
        .ok_or_else(|| ExecError::DetailedError {
            message: format!("column \"{attname}\" of relation \"{relname}\" does not exist"),
            detail: None,
            hint: None,
            sqlstate: "42703",
        })
}

fn resolve_attribute_number(
    relation: &crate::backend::parser::BoundRelation,
    attnum: i16,
    relname: &str,
) -> Result<(i16, usize), ExecError> {
    if attnum < 0 {
        return Err(ExecError::DetailedError {
            message: format!("cannot modify statistics on system column {attnum}"),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }
    if attnum == 0 {
        return Err(ExecError::DetailedError {
            message: format!("column {attnum} of relation \"{relname}\" does not exist"),
            detail: None,
            hint: None,
            sqlstate: "42703",
        });
    }
    let idx = (attnum - 1) as usize;
    if relation
        .desc
        .columns
        .get(idx)
        .is_some_and(|column| !column.dropped)
    {
        Ok((attnum, idx))
    } else {
        Err(ExecError::DetailedError {
            message: format!("column {attnum} of relation \"{relname}\" does not exist"),
            detail: None,
            hint: None,
            sqlstate: "42703",
        })
    }
}

fn system_column_attnum(name: &str) -> Option<i16> {
    match name.to_ascii_lowercase().as_str() {
        "tableoid" => Some(-6),
        "cmax" => Some(-5),
        "xmax" => Some(-4),
        "cmin" => Some(-3),
        "xmin" => Some(-2),
        "ctid" => Some(-1),
        _ => None,
    }
}

fn missing_catalog_context() -> ExecError {
    internal_error("statistics import requires executor catalog context")
}

fn internal_error(message: impl Into<String>) -> ExecError {
    ExecError::DetailedError {
        message: message.into(),
        detail: None,
        hint: None,
        sqlstate: "XX000",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text_arg_value(value: &str) -> TypedFunctionArg {
        TypedFunctionArg {
            value: Value::Text(CompactString::new(value)),
            sql_type: Some(SqlType::new(SqlTypeKind::Text)),
        }
    }

    fn int4_arg_value(value: i32) -> TypedFunctionArg {
        TypedFunctionArg {
            value: Value::Int32(value),
            sql_type: Some(SqlType::new(SqlTypeKind::Int4)),
        }
    }

    #[test]
    fn variadic_pairs_reject_odd_count() {
        let err =
            parse_variadic_pairs(vec![text_arg_value("relname")], RELATION_ARG_SPECS).unwrap_err();
        assert!(matches!(
            err,
            ExecError::DetailedError {
                sqlstate: "22023",
                ..
            }
        ));
    }

    #[test]
    fn variadic_pairs_reject_null_name() {
        let err = parse_variadic_pairs(
            vec![
                TypedFunctionArg {
                    value: Value::Null,
                    sql_type: Some(SqlType::new(SqlTypeKind::Text)),
                },
                text_arg_value("x"),
            ],
            RELATION_ARG_SPECS,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            ExecError::DetailedError {
                sqlstate: "22023",
                ..
            }
        ));
    }

    #[test]
    fn variadic_pairs_reject_non_text_name() {
        let err = parse_variadic_pairs(
            vec![int4_arg_value(1), text_arg_value("x")],
            RELATION_ARG_SPECS,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            ExecError::DetailedError {
                sqlstate: "22023",
                ..
            }
        ));
    }

    #[test]
    fn variadic_pairs_warn_for_unknown_name() {
        let parsed = parse_variadic_pairs(
            vec![text_arg_value("unknown"), int4_arg_value(1)],
            RELATION_ARG_SPECS,
        )
        .unwrap();
        assert!(!parsed.ok);
        assert!(parsed.values.is_empty());
    }

    #[test]
    fn variadic_pairs_warn_for_wrong_value_type() {
        let parsed = parse_variadic_pairs(
            vec![text_arg_value("relpages"), text_arg_value("1")],
            RELATION_ARG_SPECS,
        )
        .unwrap();
        assert!(!parsed.ok);
        assert!(parsed.values.is_empty());
    }

    #[test]
    fn variadic_pairs_ignore_version() {
        let parsed = parse_variadic_pairs(
            vec![text_arg_value("version"), int4_arg_value(1)],
            RELATION_ARG_SPECS,
        )
        .unwrap();
        assert!(parsed.ok);
        assert!(parsed.values.is_empty());
    }
}
