use pest::Parser as _;
use pest::iterators::Pair;
use pest_derive::Parser;
use std::collections::BTreeSet;

use super::comments::{
    find_comment_blocked_string_continuation, normalize_position_syntax_preserving_layout,
    normalize_string_continuation_preserving_layout, strip_sql_comments_preserving_layout,
};
use super::parsenodes::*;
use crate::backend::executor::Value;
use crate::include::catalog::PolicyCommand;
use crate::include::nodes::datum::BitString;

#[derive(Parser)]
#[grammar = "backend/parser/gram.pest"]
struct SqlParser;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseOptions {
    pub standard_conforming_strings: bool,
}

impl Default for ParseOptions {
    fn default() -> Self {
        Self {
            standard_conforming_strings: true,
        }
    }
}

pub fn parse_statement(sql: &str) -> Result<Statement, ParseError> {
    parse_statement_with_options(sql, ParseOptions::default())
}

pub fn parse_statement_with_options(
    sql: &str,
    options: ParseOptions,
) -> Result<Statement, ParseError> {
    // :HACK: Some parser paths currently recurse deeply enough to overflow the
    // default Rust test-thread stack on modest statements (for example certain
    // `unnest(...)` forms). Run parsing on a dedicated larger stack until the
    // underlying recursion is flattened.
    run_with_parser_stack({
        let sql = sql.to_string();
        move || parse_statement_with_options_inner(sql, options)
    })
}

fn parse_statement_with_options_inner(
    sql: String,
    options: ParseOptions,
) -> Result<Statement, ParseError> {
    if let Some(token) = find_comment_blocked_string_continuation(&sql) {
        return Err(ParseError::UnexpectedToken {
            expected: "statement",
            actual: format!("syntax error at or near \"{token}\""),
        });
    }
    let sql = normalize_string_continuation_preserving_layout(&sql);
    let sql = normalize_psql_describe_syntax_preserving_layout(&sql);
    let sql = strip_sql_comments_preserving_layout(&sql);
    validate_unicode_string_literals(&sql, options)?;
    let sql = normalize_position_syntax_preserving_layout(&sql);
    if let Some(stmt) = try_parse_domain_statement(&sql)? {
        return Ok(stmt);
    }
    if let Some(stmt) = try_parse_conversion_statement(&sql)? {
        return Ok(stmt);
    }
    if let Some(stmt) = try_parse_foreign_data_wrapper_statement(&sql)? {
        return Ok(stmt);
    }
    if let Some(stmt) = try_parse_aggregate_statement(&sql)? {
        return Ok(stmt);
    }
    if let Some(stmt) = try_parse_create_function_statement(&sql)? {
        return Ok(stmt);
    }
    if let Some(stmt) = try_parse_drop_function_statement(&sql)? {
        return Ok(stmt);
    }
    if let Some(stmt) = try_parse_comment_on_function_statement(&sql)? {
        return Ok(stmt);
    }
    if let Some(stmt) = try_parse_create_operator_class_statement(&sql)? {
        return Ok(stmt);
    }
    if let Some(stmt) = try_parse_operator_statement(&sql)? {
        return Ok(stmt);
    }
    if let Some(stmt) = try_parse_trigger_statement(&sql)? {
        return Ok(stmt);
    }
    if let Some(stmt) = try_parse_set_transaction_statement(&sql)? {
        return Ok(stmt);
    }
    if let Some(stmt) = try_parse_publication_statement(&sql)? {
        return Ok(stmt);
    }
    if let Some(stmt) = try_parse_constraint_comment_statement(&sql)? {
        return Ok(stmt);
    }
    if let Some(stmt) = try_parse_create_type_statement(&sql)? {
        return Ok(stmt);
    }
    if let Some(stmt) = try_parse_grant_revoke_statement(&sql)? {
        return Ok(stmt);
    }
    if let Some(stmt) = try_parse_sequence_statement(&sql)? {
        return Ok(stmt);
    }
    if let Some(stmt) = try_parse_create_tablespace_statement(&sql)? {
        return Ok(stmt);
    }
    if let Some(stmt) = try_parse_policy_statement(&sql)? {
        return Ok(stmt);
    }
    if let Some(stmt) = try_parse_statistics_statement(&sql)? {
        return Ok(stmt);
    }
    if let Some(stmt) = try_parse_partition_statement(&sql, options)? {
        return Ok(stmt);
    }
    if let Some(stmt) = try_parse_alter_table_add_unnamed_constraint_statement(&sql, options)? {
        return Ok(stmt);
    }
    if let Some(stmt) = try_parse_alter_table_trigger_state_statement(&sql)? {
        return Ok(stmt);
    }
    if let Some(stmt) = try_parse_index_statement(&sql)? {
        return Ok(stmt);
    }
    if let Some(stmt) = try_parse_view_statement(&sql)? {
        return Ok(stmt);
    }
    if let Some(stmt) = try_parse_unsupported_statement(&sql) {
        if matches!(
            stmt,
            Statement::Unsupported(UnsupportedStatement {
                feature: "ROLE management",
                ..
            })
        ) {
            return Ok(stmt);
        }
    }
    match SqlParser::parse(Rule::statement, &sql) {
        Ok(mut pairs) => build_statement(pairs.next().ok_or(ParseError::UnexpectedEof)?),
        Err(err) => try_parse_unsupported_statement(&sql)
            .ok_or_else(|| map_pest_error("statement", &sql, err)),
    }
}

fn try_parse_alter_table_add_unnamed_constraint_statement(
    sql: &str,
    options: ParseOptions,
) -> Result<Option<Statement>, ParseError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();
    if !lowered.starts_with("alter table ") {
        return Ok(None);
    }
    let Some((split, constraint_offset)) = [
        (" add primary key", " add ".len()),
        (" add unique", " add ".len()),
        (" add check", " add ".len()),
        (" add not null", " add ".len()),
        (" add foreign key", " add ".len()),
    ]
    .into_iter()
    .find_map(|(needle, constraint_offset)| {
        let split = lowered.find(needle)?;
        let suffix = &lowered[split + needle.len()..];
        (!matches!(suffix.chars().next(), Some(ch) if !ch.is_whitespace() && ch != '('))
            .then_some((split, constraint_offset))
    }) else {
        return Ok(None);
    };
    let rewritten = format!(
        "{} add constraint __pgrust_internal_unnamed_constraint__ {}",
        &trimmed[..split],
        &trimmed[split + constraint_offset..]
    );
    let mut parsed = parse_statement_with_options_inner(rewritten, options)?;
    let Statement::AlterTableAddConstraint(ref mut stmt) = parsed else {
        return Ok(None);
    };
    match &mut stmt.constraint {
        TableConstraint::NotNull { attributes, .. }
        | TableConstraint::Check { attributes, .. }
        | TableConstraint::PrimaryKey { attributes, .. }
        | TableConstraint::Unique { attributes, .. }
        | TableConstraint::ForeignKey { attributes, .. }
            if attributes.name.as_deref() == Some("__pgrust_internal_unnamed_constraint__") =>
        {
            attributes.name = None;
        }
        _ => return Ok(None),
    }
    Ok(Some(parsed))
}

fn try_parse_create_tablespace_statement(sql: &str) -> Result<Option<Statement>, ParseError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();
    if !lowered.starts_with("create tablespace ") {
        return Ok(None);
    }
    Ok(Some(Statement::CreateTablespace(
        build_create_tablespace_statement(trimmed)?,
    )))
}

fn try_parse_policy_statement(sql: &str) -> Result<Option<Statement>, ParseError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();
    if lowered.starts_with("create policy ") {
        return build_create_policy_statement(trimmed)
            .map(|stmt| Some(Statement::CreatePolicy(stmt)));
    }
    if lowered.starts_with("alter policy ") {
        return build_alter_policy_statement(trimmed)
            .map(|stmt| Some(Statement::AlterPolicy(stmt)));
    }
    if lowered.starts_with("drop policy ") {
        return build_drop_policy_statement(trimmed).map(|stmt| Some(Statement::DropPolicy(stmt)));
    }
    Ok(None)
}

fn try_parse_aggregate_statement(sql: &str) -> Result<Option<Statement>, ParseError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();
    if lowered.starts_with("create aggregate ")
        || lowered.starts_with("create or replace aggregate ")
    {
        return build_create_aggregate_statement(trimmed)
            .map(|stmt| Some(Statement::CreateAggregate(stmt)));
    }
    if lowered.starts_with("drop aggregate ") {
        return build_drop_aggregate_statement(trimmed)
            .map(|stmt| Some(Statement::DropAggregate(stmt)));
    }
    if lowered.starts_with("comment on aggregate ") {
        return build_comment_on_aggregate_statement(trimmed)
            .map(|stmt| Some(Statement::CommentOnAggregate(stmt)));
    }
    Ok(None)
}

fn try_parse_statistics_statement(sql: &str) -> Result<Option<Statement>, ParseError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();
    if lowered.starts_with("create statistics ") {
        return build_create_statistics_statement(trimmed)
            .map(|stmt| Some(Statement::CreateStatistics(stmt)));
    }
    if lowered.starts_with("alter statistics ") {
        return build_alter_statistics_statement(trimmed)
            .map(|stmt| Some(Statement::AlterStatistics(stmt)));
    }
    Ok(None)
}

#[derive(Debug)]
enum PartitionStatementParseError {
    Parse(ParseError),
    Unsupported,
}

impl From<ParseError> for PartitionStatementParseError {
    fn from(value: ParseError) -> Self {
        Self::Parse(value)
    }
}

fn unsupported_partition_statement(sql: &str, feature: &'static str) -> Statement {
    Statement::Unsupported(UnsupportedStatement {
        sql: sql.trim().trim_end_matches(';').trim().into(),
        feature,
    })
}

fn try_parse_partition_statement(
    sql: &str,
    options: ParseOptions,
) -> Result<Option<Statement>, ParseError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();

    if (lowered.starts_with("create table ")
        || lowered.starts_with("create temp table ")
        || lowered.starts_with("create temporary table "))
        && find_next_top_level_keyword(trimmed, &["partition"]).is_some()
    {
        return match build_partition_create_table_statement(trimmed, options) {
            Ok(stmt) => Ok(Some(stmt)),
            Err(PartitionStatementParseError::Unsupported) => Ok(Some(
                unsupported_partition_statement(trimmed, "CREATE TABLE form"),
            )),
            Err(PartitionStatementParseError::Parse(err)) => Err(err),
        };
    }

    if lowered.starts_with("alter table ") && lowered.contains(" attach partition ") {
        return match build_alter_table_attach_partition_statement(trimmed) {
            Ok(stmt) => Ok(Some(Statement::AlterTableAttachPartition(stmt))),
            Err(PartitionStatementParseError::Unsupported) => Ok(Some(
                unsupported_partition_statement(trimmed, "ALTER TABLE form"),
            )),
            Err(PartitionStatementParseError::Parse(err)) => Err(err),
        };
    }

    Ok(None)
}

fn build_partition_create_table_statement(
    sql: &str,
    options: ParseOptions,
) -> Result<Statement, PartitionStatementParseError> {
    let partition_idx = find_next_top_level_keyword(sql, &["partition"])
        .ok_or(PartitionStatementParseError::Unsupported)?;
    let partition_clause = sql[partition_idx..].trim_start();
    if !keyword_at_start(partition_clause, "partition") {
        return Err(PartitionStatementParseError::Unsupported);
    }
    let after_partition = consume_keyword(partition_clause, "partition").trim_start();
    if keyword_at_start(after_partition, "by") {
        let base_stmt = parse_statement_with_options_inner(
            sql[..partition_idx].trim_end().to_string(),
            options,
        )
        .map_err(PartitionStatementParseError::Parse)?;
        let Statement::CreateTable(mut create_stmt) = base_stmt else {
            return Err(PartitionStatementParseError::Unsupported);
        };
        let (partition_spec, rest) = parse_partition_spec_clause(partition_clause)?;
        if !rest.trim().is_empty() {
            return Err(PartitionStatementParseError::Unsupported);
        }
        create_stmt.partition_spec = Some(partition_spec);
        return Ok(Statement::CreateTable(create_stmt));
    }

    if !keyword_at_start(after_partition, "of") {
        return Err(PartitionStatementParseError::Unsupported);
    }

    let synthetic_base = format!("{} ()", sql[..partition_idx].trim_end());
    let base_stmt = parse_statement_with_options_inner(synthetic_base, options)
        .map_err(PartitionStatementParseError::Parse)?;
    let Statement::CreateTable(mut create_stmt) = base_stmt else {
        return Err(PartitionStatementParseError::Unsupported);
    };
    create_stmt.inherits.clear();
    let (parent_name, elements, partition_bound, partition_spec, rest) =
        parse_partition_of_clause(partition_clause, options)?;
    if !rest.trim().is_empty() {
        return Err(PartitionStatementParseError::Unsupported);
    }
    create_stmt.elements = elements;
    create_stmt.partition_of = Some(parent_name);
    create_stmt.partition_bound = Some(partition_bound);
    create_stmt.partition_spec = partition_spec;
    Ok(Statement::CreateTable(create_stmt))
}

fn parse_partition_of_clause(
    input: &str,
    options: ParseOptions,
) -> Result<
    (
        String,
        Vec<CreateTableElement>,
        RawPartitionBoundSpec,
        Option<RawPartitionSpec>,
        &str,
    ),
    PartitionStatementParseError,
> {
    let mut rest = consume_keyword(input.trim_start(), "partition").trim_start();
    rest = consume_keyword(rest, "of").trim_start();
    let (parts, next) = parse_qualified_identifier_parts(rest)?;
    rest = next.trim_start();
    let elements = if rest.starts_with('(') {
        let (elements_sql, next) =
            take_parenthesized_segment(rest).map_err(PartitionStatementParseError::Parse)?;
        rest = next.trim_start();
        parse_partition_of_elements(&elements_sql, options)?
    } else {
        Vec::new()
    };
    let (bound, next) = parse_partition_bound_clause(rest)?;
    rest = next.trim_start();
    let partition_spec = if rest.is_empty() {
        None
    } else {
        let (partition_spec, next) = parse_partition_spec_clause(rest)?;
        rest = next.trim_start();
        Some(partition_spec)
    };
    Ok((parts.join("."), elements, bound, partition_spec, rest))
}

fn parse_partition_of_elements(
    elements_sql: &str,
    options: ParseOptions,
) -> Result<Vec<CreateTableElement>, PartitionStatementParseError> {
    let synthetic = format!("create table __pgrust_partition_of__ ({elements_sql})");
    let stmt = parse_statement_with_options_inner(synthetic, options)
        .map_err(PartitionStatementParseError::Parse)?;
    let Statement::CreateTable(create_stmt) = stmt else {
        return Err(PartitionStatementParseError::Unsupported);
    };
    Ok(create_stmt.elements)
}

fn parse_partition_spec_clause(
    input: &str,
) -> Result<(RawPartitionSpec, &str), PartitionStatementParseError> {
    let mut rest = input.trim_start();
    if !keyword_at_start(rest, "partition") {
        return Err(PartitionStatementParseError::Unsupported);
    }
    rest = consume_keyword(rest, "partition").trim_start();
    if !keyword_at_start(rest, "by") {
        return Err(PartitionStatementParseError::Unsupported);
    }
    rest = consume_keyword(rest, "by").trim_start();
    let strategy = if keyword_at_start(rest, "list") {
        rest = consume_keyword(rest, "list").trim_start();
        PartitionStrategy::List
    } else if keyword_at_start(rest, "range") {
        rest = consume_keyword(rest, "range").trim_start();
        PartitionStrategy::Range
    } else {
        return Err(PartitionStatementParseError::Unsupported);
    };
    let (keys_sql, rest) = take_parenthesized_segment(rest)?;
    let mut keys = Vec::new();
    for item in split_top_level_items(&keys_sql, ',')? {
        let trimmed = item.trim();
        if trimmed.is_empty() {
            return Err(ParseError::UnexpectedEof.into());
        }
        let (column, remainder) = parse_unqualified_identifier(trimmed, "partition column")?;
        if !remainder.trim().is_empty() {
            return Err(PartitionStatementParseError::Unsupported);
        }
        keys.push(RawPartitionKey::Column(column));
    }
    Ok((RawPartitionSpec { strategy, keys }, rest))
}

fn parse_partition_bound_clause(
    input: &str,
) -> Result<(RawPartitionBoundSpec, &str), PartitionStatementParseError> {
    let rest = input.trim_start();
    if keyword_at_start(rest, "default") {
        return Ok((
            RawPartitionBoundSpec::List {
                values: Vec::new(),
                is_default: true,
            },
            consume_keyword(rest, "default"),
        ));
    }

    if !keyword_at_start(rest, "for") {
        return Err(PartitionStatementParseError::Unsupported);
    }
    let mut rest = consume_keyword(rest, "for").trim_start();
    if !keyword_at_start(rest, "values") {
        return Err(PartitionStatementParseError::Unsupported);
    }
    rest = consume_keyword(rest, "values").trim_start();
    if keyword_at_start(rest, "in") {
        rest = consume_keyword(rest, "in").trim_start();
        let (values_sql, rest) = take_parenthesized_segment(rest)?;
        let values = split_top_level_items(&values_sql, ',')?
            .into_iter()
            .map(|item| parse_expr(&item))
            .collect::<Result<Vec<_>, _>>()?;
        return Ok((
            RawPartitionBoundSpec::List {
                values,
                is_default: false,
            },
            rest,
        ));
    }
    if keyword_at_start(rest, "from") {
        rest = consume_keyword(rest, "from").trim_start();
        let (from_sql, next) = take_parenthesized_segment(rest)?;
        let mut rest = next.trim_start();
        rest = consume_keyword(rest, "to").trim_start();
        let (to_sql, rest) = take_parenthesized_segment(rest)?;
        return Ok((
            RawPartitionBoundSpec::Range {
                from: parse_partition_range_datums(&from_sql)?,
                to: parse_partition_range_datums(&to_sql)?,
                is_default: false,
            },
            rest,
        ));
    }
    Err(PartitionStatementParseError::Unsupported)
}

fn parse_partition_range_datums(
    input: &str,
) -> Result<Vec<RawPartitionRangeDatum>, PartitionStatementParseError> {
    split_top_level_items(input, ',')?
        .into_iter()
        .map(|item| {
            let trimmed = item.trim();
            if trimmed.eq_ignore_ascii_case("minvalue") {
                Ok(RawPartitionRangeDatum::MinValue)
            } else if trimmed.eq_ignore_ascii_case("maxvalue") {
                Ok(RawPartitionRangeDatum::MaxValue)
            } else {
                Ok(RawPartitionRangeDatum::Value(parse_expr(trimmed)?))
            }
        })
        .collect()
}

fn build_alter_table_attach_partition_statement(
    sql: &str,
) -> Result<AlterTableAttachPartitionStatement, PartitionStatementParseError> {
    let mut rest = consume_keyword(sql.trim_start(), "alter").trim_start();
    rest = consume_keyword(rest, "table").trim_start();
    let mut if_exists = false;
    let mut only = false;
    if keyword_at_start(rest, "if") {
        rest = consume_keyword(rest, "if").trim_start();
        rest = consume_keyword(rest, "exists").trim_start();
        if_exists = true;
    }
    if keyword_at_start(rest, "only") {
        rest = consume_keyword(rest, "only").trim_start();
        only = true;
    }
    let (parent_parts, next) = parse_qualified_identifier_parts(rest)?;
    rest = next.trim_start();
    if !keyword_at_start(rest, "attach") {
        return Err(PartitionStatementParseError::Unsupported);
    }
    rest = consume_keyword(rest, "attach").trim_start();
    if !keyword_at_start(rest, "partition") {
        return Err(PartitionStatementParseError::Unsupported);
    }
    rest = consume_keyword(rest, "partition").trim_start();
    let (partition_parts, next) = parse_qualified_identifier_parts(rest)?;
    let (bound, rest) = parse_partition_bound_clause(next)?;
    if !rest.trim().is_empty() {
        return Err(PartitionStatementParseError::Unsupported);
    }
    Ok(AlterTableAttachPartitionStatement {
        if_exists,
        only,
        parent_table: parent_parts.join("."),
        partition_table: partition_parts.join("."),
        bound,
    })
}

fn try_parse_trigger_statement(sql: &str) -> Result<Option<Statement>, ParseError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();
    if lowered.starts_with("create trigger ")
        || lowered.starts_with("create or replace trigger ")
        || lowered.starts_with("create constraint trigger ")
        || lowered.starts_with("drop trigger ")
        || lowered.starts_with("alter trigger ")
    {
        if lowered.starts_with("drop trigger ") {
            return build_drop_trigger_statement(trimmed)
                .map(|stmt| Some(Statement::DropTrigger(stmt)));
        }
        if lowered.starts_with("alter trigger ") {
            return build_alter_trigger_rename_statement(trimmed)
                .map(|stmt| Some(Statement::AlterTriggerRename(stmt)));
        }
        return build_create_trigger_statement(trimmed)
            .map(|stmt| Some(Statement::CreateTrigger(stmt)));
    }
    Ok(None)
}

fn try_parse_alter_table_trigger_state_statement(
    sql: &str,
) -> Result<Option<Statement>, ParseError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();
    if !lowered.starts_with("alter table ") || !lowered.contains(" trigger ") {
        return Ok(None);
    }
    if !lowered.contains(" enable ") && !lowered.contains(" disable ") {
        return Ok(None);
    }
    build_alter_table_trigger_state_statement(trimmed)
        .map(|stmt| Some(Statement::AlterTableTriggerState(stmt)))
}

fn try_parse_set_transaction_statement(sql: &str) -> Result<Option<Statement>, ParseError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();
    let prefix = "set transaction isolation level ";
    if !lowered.starts_with(prefix) {
        return Ok(None);
    }
    let Some(level) = trimmed.get(prefix.len()..) else {
        return Err(ParseError::UnexpectedEof);
    };
    let level = level.trim();
    if level.is_empty() {
        return Err(ParseError::UnexpectedEof);
    }
    // :HACK: This only accepts the transaction-local spelling exercised by the
    // regression tests. pgrust still executes at a single effective isolation
    // level and stores the setting only as compatibility metadata.
    Ok(Some(Statement::Set(SetStatement {
        name: "transaction_isolation".into(),
        value: level.to_ascii_lowercase(),
        is_local: true,
    })))
}

fn try_parse_publication_statement(sql: &str) -> Result<Option<Statement>, ParseError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();
    if lowered.starts_with("create publication ") {
        return build_create_publication_statement(trimmed)
            .map(|stmt| Some(Statement::CreatePublication(stmt)));
    }
    if lowered.starts_with("alter publication ") {
        return build_alter_publication_statement(trimmed)
            .map(|stmt| Some(Statement::AlterPublication(stmt)));
    }
    if lowered.starts_with("drop publication ") {
        return build_drop_publication_statement(trimmed)
            .map(|stmt| Some(Statement::DropPublication(stmt)));
    }
    if lowered.starts_with("comment on publication ") {
        return build_comment_on_publication_statement(trimmed)
            .map(|stmt| Some(Statement::CommentOnPublication(stmt)));
    }
    Ok(None)
}

fn try_parse_constraint_comment_statement(sql: &str) -> Result<Option<Statement>, ParseError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();
    if !lowered.starts_with("comment on constraint ") {
        return Ok(None);
    }
    if lowered["comment on constraint ".len()..]
        .trim_start()
        .starts_with("on domain ")
    {
        return Ok(None);
    }
    build_comment_on_constraint_statement(trimmed)
        .map(|stmt| Some(Statement::CommentOnConstraint(stmt)))
}

fn build_create_publication_statement(sql: &str) -> Result<CreatePublicationStatement, ParseError> {
    let rest = sql
        .get("create publication".len()..)
        .ok_or(ParseError::UnexpectedEof)?
        .trim_start();
    let (publication_name, mut rest) = parse_unqualified_identifier(rest, "publication name")?;
    let mut target = PublicationTargetSpec::default();
    let mut options = PublicationOptions::default();

    if keyword_at_start(rest, "for") {
        rest = consume_keyword(rest.trim_start(), "for");
        let (parsed_target, next) = parse_create_publication_target(rest)?;
        target = parsed_target;
        rest = next;
    }
    if keyword_at_start(rest, "with") {
        rest = consume_keyword(rest.trim_start(), "with");
        let (parsed_options, next) = parse_publication_options_clause(rest)?;
        options = parsed_options;
        rest = next;
    }
    if !rest.trim().is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "end of CREATE PUBLICATION",
            actual: rest.trim().into(),
        });
    }
    reject_unsupported_publication_features(&target)?;
    Ok(CreatePublicationStatement {
        publication_name,
        target,
        options,
    })
}

fn build_alter_publication_statement(sql: &str) -> Result<AlterPublicationStatement, ParseError> {
    let rest = sql
        .get("alter publication".len()..)
        .ok_or(ParseError::UnexpectedEof)?
        .trim_start();
    let (publication_name, rest) = parse_unqualified_identifier(rest, "publication name")?;
    let rest = rest.trim_start();

    let (action, rest) = if keyword_at_start(rest, "rename") {
        let mut rest = consume_keyword(rest, "rename").trim_start();
        if !keyword_at_start(rest, "to") {
            return Err(ParseError::UnexpectedToken {
                expected: "RENAME TO new_name",
                actual: rest.into(),
            });
        }
        rest = consume_keyword(rest, "to").trim_start();
        let (new_name, rest) = parse_unqualified_identifier(rest, "publication name")?;
        (AlterPublicationAction::Rename { new_name }, rest)
    } else if keyword_at_start(rest, "owner") {
        let mut rest = consume_keyword(rest, "owner").trim_start();
        if !keyword_at_start(rest, "to") {
            return Err(ParseError::UnexpectedToken {
                expected: "OWNER TO new_owner",
                actual: rest.into(),
            });
        }
        rest = consume_keyword(rest, "to").trim_start();
        let (new_owner, rest) = parse_unqualified_identifier(rest, "role name")?;
        (AlterPublicationAction::OwnerTo { new_owner }, rest)
    } else if keyword_at_start(rest, "set") {
        let rest = consume_keyword(rest, "set").trim_start();
        if rest.starts_with('(') {
            let (options, rest) = parse_publication_options_clause(rest)?;
            (AlterPublicationAction::SetOptions(options), rest)
        } else {
            let (target, rest) = parse_publication_target_spec(rest)?;
            reject_unsupported_publication_features(&target)?;
            (AlterPublicationAction::SetObjects(target), rest)
        }
    } else if keyword_at_start(rest, "add") {
        let rest = consume_keyword(rest, "add").trim_start();
        let (target, rest) = parse_publication_target_spec(rest)?;
        reject_unsupported_publication_features(&target)?;
        (AlterPublicationAction::AddObjects(target), rest)
    } else if keyword_at_start(rest, "drop") {
        let rest = consume_keyword(rest, "drop").trim_start();
        let (target, rest) = parse_publication_target_spec(rest)?;
        reject_unsupported_publication_features(&target)?;
        (AlterPublicationAction::DropObjects(target), rest)
    } else {
        return Err(ParseError::UnexpectedToken {
            expected: "ALTER PUBLICATION action",
            actual: rest.into(),
        });
    };

    if !rest.trim().is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "end of ALTER PUBLICATION",
            actual: rest.trim().into(),
        });
    }

    Ok(AlterPublicationStatement {
        publication_name,
        action,
    })
}

fn build_drop_publication_statement(sql: &str) -> Result<DropPublicationStatement, ParseError> {
    let mut rest = sql
        .get("drop publication".len()..)
        .ok_or(ParseError::UnexpectedEof)?
        .trim_start();
    let mut if_exists = false;
    if let Some(next) = consume_keywords(rest, &["if", "exists"]) {
        if_exists = true;
        rest = next;
    }
    let mut cascade = false;
    if let Some(idx) = find_next_top_level_keyword(rest, &["cascade", "restrict"]) {
        let suffix = rest[idx..].trim_start();
        cascade = keyword_at_start(suffix, "cascade");
        rest = rest[..idx].trim_end();
    }
    let publication_names = split_top_level_items(rest, ',')?
        .into_iter()
        .map(|item| {
            let (name, trailing) = parse_unqualified_identifier(&item, "publication name")?;
            if !trailing.trim().is_empty() {
                return Err(ParseError::UnexpectedToken {
                    expected: "publication name",
                    actual: item,
                });
            }
            Ok(name)
        })
        .collect::<Result<Vec<_>, _>>()?;
    if publication_names.is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "publication name",
            actual: sql.into(),
        });
    }
    Ok(DropPublicationStatement {
        if_exists,
        publication_names,
        cascade,
    })
}

fn build_create_statistics_statement(sql: &str) -> Result<CreateStatisticsStatement, ParseError> {
    let mut rest = sql
        .get("create statistics".len()..)
        .ok_or(ParseError::UnexpectedEof)?
        .trim_start();
    let mut if_not_exists = false;
    if let Some(next) = consume_keywords(rest, &["if", "not", "exists"]) {
        if_not_exists = true;
        rest = next;
    }
    let (parts, next) = parse_qualified_identifier_parts(rest)?;
    let statistics_name = match parts.as_slice() {
        [name] => name.clone(),
        [schema, name] => format!("{schema}.{name}"),
        _ => return Err(ParseError::UnsupportedQualifiedName(parts.join("."))),
    };
    rest = next.trim_start();

    let mut kinds = Vec::new();
    if rest.starts_with('(') {
        let (body, next) = take_parenthesized_segment(rest)?;
        kinds = split_top_level_items(&body, ',')?
            .into_iter()
            .map(|item| normalize_simple_identifier(&item))
            .collect::<Result<Vec<_>, _>>()?;
        rest = next.trim_start();
    }

    if !keyword_at_start(rest, "on") {
        let token = if keyword_at_start(rest, "from") {
            "FROM"
        } else {
            ";"
        };
        return Err(ParseError::UnexpectedToken {
            expected: "CREATE STATISTICS name ON target_list FROM relation",
            actual: format!("syntax error at or near \"{token}\""),
        });
    }
    rest = consume_keyword(rest, "on").trim_start();

    let from_idx = find_next_top_level_keyword(rest, &["from"]).ok_or_else(|| {
        ParseError::UnexpectedToken {
            expected: "CREATE STATISTICS ... FROM relation",
            actual: "syntax error at or near \";\"".into(),
        }
    })?;
    let targets_sql = rest[..from_idx].trim();
    if targets_sql.is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "statistics target list",
            actual: "syntax error at or near \"FROM\"".into(),
        });
    }
    let targets = split_top_level_items(targets_sql, ',')?;
    rest = consume_keyword(rest[from_idx..].trim_start(), "from").trim_start();
    if rest.is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "relation name",
            actual: "syntax error at or near \";\"".into(),
        });
    }
    Ok(CreateStatisticsStatement {
        if_not_exists,
        statistics_name,
        kinds,
        targets,
        from_clause: rest.trim().to_string(),
    })
}

fn build_alter_statistics_statement(sql: &str) -> Result<AlterStatisticsStatement, ParseError> {
    let mut rest = sql
        .get("alter statistics".len()..)
        .ok_or(ParseError::UnexpectedEof)?
        .trim_start();
    let mut if_exists = false;
    if let Some(next) = consume_keywords(rest, &["if", "exists"]) {
        if_exists = true;
        rest = next;
    }
    let (parts, next) = parse_qualified_identifier_parts(rest)?;
    let statistics_name = match parts.as_slice() {
        [name] => name.clone(),
        [schema, name] => format!("{schema}.{name}"),
        _ => return Err(ParseError::UnsupportedQualifiedName(parts.join("."))),
    };
    rest = next.trim_start();
    let rest =
        consume_keywords(rest, &["set", "statistics"]).ok_or(ParseError::UnexpectedToken {
            expected: "SET STATISTICS signed_integer",
            actual: rest.into(),
        })?;
    let (statistics_target, rest) = parse_signed_i64_token(rest.trim_start())?;
    let statistics_target =
        i32::try_from(statistics_target).map_err(|_| ParseError::InvalidInteger(sql.into()))?;
    if !rest.trim().is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "end of ALTER STATISTICS",
            actual: rest.trim().into(),
        });
    }
    Ok(AlterStatisticsStatement {
        if_exists,
        statistics_name,
        statistics_target,
    })
}

fn build_comment_on_publication_statement(
    sql: &str,
) -> Result<CommentOnPublicationStatement, ParseError> {
    let rest = sql
        .get("comment on publication".len()..)
        .ok_or(ParseError::UnexpectedEof)?
        .trim_start();
    let (publication_name, mut rest) = parse_unqualified_identifier(rest, "publication name")?;
    rest = rest.trim_start();
    if !keyword_at_start(rest, "is") {
        return Err(ParseError::UnexpectedToken {
            expected: "IS string literal or NULL",
            actual: rest.into(),
        });
    }
    rest = consume_keyword(rest, "is").trim_start();
    let (comment, rest) = if keyword_at_start(rest, "null") {
        (None, consume_keyword(rest, "null"))
    } else {
        let token_len = scan_string_literal_token_len(rest).ok_or(ParseError::UnexpectedToken {
            expected: "comment string literal or NULL",
            actual: rest.into(),
        })?;
        (
            Some(decode_string_literal(&rest[..token_len])?),
            &rest[token_len..],
        )
    };
    if !rest.trim().is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "end of COMMENT ON PUBLICATION",
            actual: rest.trim().into(),
        });
    }
    Ok(CommentOnPublicationStatement {
        publication_name,
        comment,
    })
}

fn build_comment_on_constraint_statement(
    sql: &str,
) -> Result<CommentOnConstraintStatement, ParseError> {
    let rest = sql
        .get("comment on constraint".len()..)
        .ok_or(ParseError::UnexpectedEof)?
        .trim_start();
    let (constraint_name, mut rest) = parse_unqualified_identifier(rest, "constraint name")?;
    rest = rest.trim_start();
    if !keyword_at_start(rest, "on") {
        return Err(ParseError::UnexpectedToken {
            expected: "ON table_name",
            actual: rest.into(),
        });
    }
    rest = consume_keyword(rest, "on").trim_start();
    let (parts, mut rest) = parse_qualified_identifier_parts(rest)?;
    let table_name = match parts.as_slice() {
        [name] => name.clone(),
        [schema, name] => format!("{schema}.{name}"),
        _ => return Err(ParseError::UnsupportedQualifiedName(parts.join("."))),
    };
    rest = rest.trim_start();
    if !keyword_at_start(rest, "is") {
        return Err(ParseError::UnexpectedToken {
            expected: "IS string literal or NULL",
            actual: rest.into(),
        });
    }
    rest = consume_keyword(rest, "is").trim_start();
    let (comment, rest) = if keyword_at_start(rest, "null") {
        (None, consume_keyword(rest, "null"))
    } else {
        let token_len = scan_string_literal_token_len(rest).ok_or(ParseError::UnexpectedToken {
            expected: "comment string literal or NULL",
            actual: rest.into(),
        })?;
        (
            Some(decode_string_literal(&rest[..token_len])?),
            &rest[token_len..],
        )
    };
    if !rest.trim().is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "end of COMMENT ON CONSTRAINT",
            actual: rest.trim().into(),
        });
    }
    Ok(CommentOnConstraintStatement {
        constraint_name,
        table_name,
        comment,
    })
}

fn parse_create_publication_target(
    input: &str,
) -> Result<(PublicationTargetSpec, &str), ParseError> {
    if let Some(rest) = consume_keywords(input, &["all", "tables"]) {
        return Ok((
            PublicationTargetSpec {
                for_all_tables: true,
                objects: Vec::new(),
            },
            rest,
        ));
    }
    parse_publication_target_spec(input)
}

fn parse_publication_target_spec(input: &str) -> Result<(PublicationTargetSpec, &str), ParseError> {
    enum PublicationObjectMode {
        Table,
        Schema,
    }

    let mut rest = input.trim_start();
    let mut mode = if let Some(next) = consume_keywords(rest, &["table"]) {
        rest = next;
        PublicationObjectMode::Table
    } else if let Some(next) = consume_keywords(rest, &["tables", "in", "schema"]) {
        rest = next;
        PublicationObjectMode::Schema
    } else {
        return Err(ParseError::UnexpectedToken {
            expected: "TABLE ... or TABLES IN SCHEMA ...",
            actual: rest.into(),
        });
    };

    let mut objects = Vec::new();
    loop {
        let (object, next) = match mode {
            PublicationObjectMode::Table => {
                let (table, next) = parse_publication_table_object(rest)?;
                (PublicationObjectSpec::Table(table), next)
            }
            PublicationObjectMode::Schema => {
                let (schema, next) = parse_publication_schema_object(rest)?;
                (PublicationObjectSpec::Schema(schema), next)
            }
        };
        objects.push(object);
        rest = next.trim_start();
        let Some(after_comma) = rest.strip_prefix(',') else {
            break;
        };
        rest = after_comma.trim_start();
        if let Some(next) = consume_keywords(rest, &["table"]) {
            rest = next;
            mode = PublicationObjectMode::Table;
            continue;
        }
        if let Some(next) = consume_keywords(rest, &["tables", "in", "schema"]) {
            rest = next;
            mode = PublicationObjectMode::Schema;
            continue;
        }
    }

    if objects.len() > 1
        && objects.iter().any(|object| {
            matches!(
                object,
                PublicationObjectSpec::Table(PublicationTableSpec { relation_name, .. })
                    if relation_name == "current_schema"
            )
        })
    {
        return Err(ParseError::InvalidPublicationTableName(
            "current_schema".into(),
        ));
    }

    Ok((
        PublicationTargetSpec {
            for_all_tables: false,
            objects,
        },
        rest,
    ))
}

fn parse_publication_table_object(input: &str) -> Result<(PublicationTableSpec, &str), ParseError> {
    let mut rest = input.trim_start();
    let mut only = false;
    if keyword_at_start(rest, "only") {
        only = true;
        rest = consume_keyword(rest, "only").trim_start();
    }
    let (parts, mut rest) = parse_qualified_identifier_parts(rest)?;
    let relation_name = match parts.as_slice() {
        [name] => name.clone(),
        [schema, name] => format!("{schema}.{name}"),
        _ => return Err(ParseError::UnsupportedQualifiedName(parts.join("."))),
    };

    let mut column_names = Vec::new();
    let mut where_clause = None;
    if rest.trim_start().starts_with('(') {
        let (segment, next) = take_parenthesized_segment(rest)?;
        column_names = parse_publication_identifier_list(&segment)?;
        rest = next;
    }
    if keyword_at_start(rest, "where") {
        let after_where = consume_keyword(rest.trim_start(), "where").trim_start();
        let (segment, next) = take_parenthesized_segment(after_where)?;
        where_clause = Some(segment.trim().to_string());
        rest = next;
    }

    Ok((
        PublicationTableSpec {
            relation_name,
            only,
            column_names,
            where_clause,
        },
        rest,
    ))
}

fn parse_publication_schema_object(
    input: &str,
) -> Result<(PublicationSchemaSpec, &str), ParseError> {
    let rest = input.trim_start();
    let (schema_name, rest) = if keyword_at_start(rest, "current_schema") {
        (
            PublicationSchemaName::CurrentSchema,
            consume_keyword(rest, "current_schema"),
        )
    } else {
        let (parts, rest) = parse_qualified_identifier_parts(rest)?;
        match parts.as_slice() {
            [name] => (PublicationSchemaName::Name(name.clone()), rest),
            _ => {
                return Err(ParseError::InvalidPublicationSchemaName(parts.join(".")));
            }
        }
    };
    let trailing = rest.trim_start();
    if trailing.starts_with('(') {
        let _ = take_parenthesized_segment(trailing)?;
        return Err(ParseError::FeatureNotSupported(
            "publication column lists".into(),
        ));
    }
    if keyword_at_start(trailing, "where") {
        let after_where = consume_keyword(trailing, "where").trim_start();
        let _ = take_parenthesized_segment(after_where)?;
        return Err(ParseError::FeatureNotSupported(
            "publication row filters".into(),
        ));
    }
    Ok((PublicationSchemaSpec { schema_name }, rest))
}

fn parse_publication_options_clause(input: &str) -> Result<(PublicationOptions, &str), ParseError> {
    let (segment, rest) = take_parenthesized_segment(input)?;
    let mut options = Vec::new();
    let mut seen = BTreeSet::new();
    for item in split_top_level_items(&segment, ',')? {
        if item.trim().is_empty() {
            continue;
        }
        let (name, trailing) = parse_sql_identifier(&item)?;
        let normalized_name = name.to_ascii_lowercase();
        if !seen.insert(normalized_name.clone()) {
            return Err(ParseError::ConflictingOrRedundantOptions {
                option: normalized_name,
            });
        }
        let trailing = trailing.trim_start();
        let value = trailing
            .strip_prefix('=')
            .map(str::trim_start)
            .unwrap_or_default();
        let option = match normalized_name.as_str() {
            "publish" => {
                let (actions, trailing) = parse_publication_publish_actions(value)?;
                if !trailing.trim().is_empty() {
                    return Err(ParseError::UnexpectedToken {
                        expected: "publish action list",
                        actual: trailing.trim().into(),
                    });
                }
                PublicationOption::Publish(actions)
            }
            "publish_via_partition_root" => {
                let (value, trailing) = parse_publication_bool_value(value)?;
                if !trailing.trim().is_empty() {
                    return Err(ParseError::UnexpectedToken {
                        expected: "boolean publication option value",
                        actual: trailing.trim().into(),
                    });
                }
                PublicationOption::PublishViaPartitionRoot(value)
            }
            "publish_generated_columns" => {
                let (value, trailing) = parse_publication_generated_columns_value(value)?;
                if !trailing.trim().is_empty() {
                    return Err(ParseError::UnexpectedToken {
                        expected: "publish_generated_columns value",
                        actual: trailing.trim().into(),
                    });
                }
                PublicationOption::PublishGeneratedColumns(value)
            }
            _ => return Err(ParseError::UnrecognizedPublicationParameter(name)),
        };
        options.push(option);
    }
    Ok((PublicationOptions { options }, rest))
}

fn parse_publication_publish_actions(
    input: &str,
) -> Result<(PublicationPublishActions, &str), ParseError> {
    let (text, rest) = parse_publication_option_text_value(input)?;
    let mut actions = PublicationPublishActions {
        insert: false,
        update: false,
        delete: false,
        truncate: false,
    };
    for item in text
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
    {
        match item.to_ascii_lowercase().as_str() {
            "insert" => actions.insert = true,
            "update" => actions.update = true,
            "delete" => actions.delete = true,
            "truncate" => actions.truncate = true,
            other => {
                return Err(ParseError::UnrecognizedPublicationOptionValue {
                    option: "publish".into(),
                    value: other.into(),
                });
            }
        }
    }
    Ok((actions, rest))
}

fn parse_publication_generated_columns_value(
    input: &str,
) -> Result<(PublishGeneratedColumns, &str), ParseError> {
    let (value, rest) = parse_publication_option_text_value(input)?;
    let value = match value.to_ascii_lowercase().as_str() {
        "none" => PublishGeneratedColumns::None,
        "stored" => PublishGeneratedColumns::Stored,
        _ => {
            return Err(ParseError::InvalidPublicationParameterValue {
                parameter: "publish_generated_columns".into(),
                value,
            });
        }
    };
    Ok((value, rest))
}

fn parse_publication_option_text_value(input: &str) -> Result<(String, &str), ParseError> {
    let input = input.trim_start();
    if input.is_empty() {
        return Ok((String::new(), input));
    }
    if let Some(token_len) = scan_string_literal_token_len(input) {
        return Ok((
            decode_string_literal(&input[..token_len])?,
            &input[token_len..],
        ));
    }
    let (value, rest) = parse_sql_identifier(input)?;
    Ok((value, rest))
}

fn parse_publication_bool_value(input: &str) -> Result<(bool, &str), ParseError> {
    let input = input.trim_start();
    if let Some(token_len) = scan_string_literal_token_len(input) {
        let value = decode_string_literal(&input[..token_len])?;
        let parsed = match value.to_ascii_lowercase().as_str() {
            "true" | "on" | "yes" | "1" => true,
            "false" | "off" | "no" | "0" => false,
            _ => {
                return Err(ParseError::UnexpectedToken {
                    expected: "boolean option value",
                    actual: value,
                });
            }
        };
        return Ok((parsed, &input[token_len..]));
    }
    if let Some(rest) = input.strip_prefix('1') {
        if rest
            .chars()
            .next()
            .is_none_or(|ch| !(ch.is_ascii_alphanumeric() || ch == '_'))
        {
            return Ok((true, rest));
        }
    }
    if let Some(rest) = input.strip_prefix('0') {
        if rest
            .chars()
            .next()
            .is_none_or(|ch| !(ch.is_ascii_alphanumeric() || ch == '_'))
        {
            return Ok((false, rest));
        }
    }
    let (value, rest) = parse_sql_identifier(input)?;
    let parsed = match value.as_str() {
        "true" | "on" | "yes" => true,
        "false" | "off" | "no" => false,
        _ => {
            return Err(ParseError::UnexpectedToken {
                expected: "boolean option value",
                actual: value,
            });
        }
    };
    Ok((parsed, rest))
}

fn parse_publication_identifier_list(input: &str) -> Result<Vec<String>, ParseError> {
    split_top_level_items(input, ',')?
        .into_iter()
        .map(|item| {
            let (name, rest) = parse_unqualified_identifier(&item, "identifier")?;
            if !rest.trim().is_empty() {
                return Err(ParseError::UnexpectedToken {
                    expected: "identifier",
                    actual: item,
                });
            }
            Ok(name)
        })
        .collect()
}

fn reject_unsupported_publication_features(
    target: &PublicationTargetSpec,
) -> Result<(), ParseError> {
    for object in &target.objects {
        let PublicationObjectSpec::Table(table) = object else {
            continue;
        };
        if table.only {
            return Err(ParseError::FeatureNotSupported("publication ONLY".into()));
        }
        if !table.column_names.is_empty() {
            return Err(ParseError::FeatureNotSupported(
                "publication column lists".into(),
            ));
        }
        if table.where_clause.is_some() {
            return Err(ParseError::FeatureNotSupported(
                "publication row filters".into(),
            ));
        }
    }
    Ok(())
}

fn build_create_tablespace_statement(sql: &str) -> Result<CreateTablespaceStatement, ParseError> {
    let prefix = "create tablespace";
    let rest = sql
        .get(prefix.len()..)
        .ok_or(ParseError::UnexpectedEof)?
        .trim_start();
    let (tablespace_name, rest) = parse_sql_identifier(rest)?;
    let rest = rest.trim_start();
    if !keyword_at_start(rest, "location") {
        return Err(ParseError::UnexpectedToken {
            expected: "LOCATION string literal",
            actual: rest.into(),
        });
    }
    let rest = consume_keyword(rest, "location").trim_start();
    let token_len = scan_string_literal_token_len(rest).ok_or(ParseError::UnexpectedToken {
        expected: "tablespace location string literal",
        actual: rest.into(),
    })?;
    let location = decode_string_literal(&rest[..token_len])?;
    let trailing = rest[token_len..].trim();
    if !trailing.is_empty() {
        return Err(ParseError::FeatureNotSupported(format!(
            "unsupported CREATE TABLESPACE clause: {}",
            trailing.split_whitespace().next().unwrap_or(trailing)
        )));
    }
    Ok(CreateTablespaceStatement {
        tablespace_name,
        location,
    })
}

pub fn parse_expr(sql: &str) -> Result<SqlExpr, ParseError> {
    let sql = strip_sql_comments_preserving_layout(sql);
    SqlParser::parse(Rule::expr, &sql)
        .map_err(|e| map_pest_error("expression", &sql, e))
        .and_then(|mut pairs| {
            let pair = pairs.next().ok_or(ParseError::UnexpectedEof)?;
            if pairs.next().is_some() {
                return Err(ParseError::UnexpectedToken {
                    expected: "expression",
                    actual: sql.clone(),
                });
            }
            build_expr(pair)
        })
}

pub fn parse_type_name(sql: &str) -> Result<RawTypeName, ParseError> {
    let sql = strip_sql_comments_preserving_layout(sql);
    let lowered = sql.trim().to_ascii_lowercase();
    match lowered.as_str() {
        "int2vector" => return Ok(RawTypeName::Builtin(SqlType::new(SqlTypeKind::Int2Vector))),
        "oidvector" => return Ok(RawTypeName::Builtin(SqlType::new(SqlTypeKind::OidVector))),
        "name" => return Ok(RawTypeName::Builtin(SqlType::new(SqlTypeKind::Name))),
        "bpchar" | "pg_catalog.bpchar" => {
            return Ok(RawTypeName::Builtin(SqlType::new(SqlTypeKind::Char)));
        }
        "pg_node_tree" => return Ok(RawTypeName::Builtin(SqlType::new(SqlTypeKind::PgNodeTree))),
        "trigger" => return Ok(RawTypeName::Builtin(SqlType::new(SqlTypeKind::Trigger))),
        "void" => return Ok(RawTypeName::Builtin(SqlType::new(SqlTypeKind::Void))),
        "fdw_handler" => return Ok(RawTypeName::Builtin(SqlType::new(SqlTypeKind::FdwHandler))),
        "regrole" => return Ok(RawTypeName::Builtin(SqlType::new(SqlTypeKind::RegRole))),
        "regproc" => {
            return Ok(RawTypeName::Builtin(SqlType::new(
                SqlTypeKind::RegProcedure,
            )));
        }
        "regprocedure" => {
            return Ok(RawTypeName::Builtin(SqlType::new(
                SqlTypeKind::RegProcedure,
            )));
        }
        "regoperator" => {
            return Ok(RawTypeName::Builtin(SqlType::new(SqlTypeKind::RegOperator)));
        }
        _ => {}
    }
    SqlParser::parse(Rule::type_name, &sql)
        .map_err(|e| map_pest_error("type name", &sql, e))
        .and_then(|mut pairs| {
            let pair = pairs.next().ok_or(ParseError::UnexpectedEof)?;
            if pairs.next().is_some() {
                return Err(ParseError::UnexpectedToken {
                    expected: "type name",
                    actual: sql.clone(),
                });
            }
            Ok(build_type_name(pair))
        })
}

#[cfg(not(target_arch = "wasm32"))]
fn run_with_parser_stack<F, T>(f: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    std::thread::Builder::new()
        .name("pgrust-parser".into())
        .stack_size(32 * 1024 * 1024)
        .spawn(f)
        .expect("spawn parser thread")
        .join()
        .expect("parser thread panicked")
}

#[cfg(target_arch = "wasm32")]
fn run_with_parser_stack<F, T>(f: F) -> T
where
    F: FnOnce() -> T,
{
    // Browser wasm cannot spawn threads without a different runtime model.
    // Run inline there and keep the larger parser stack workaround only on
    // native targets.
    f()
}

fn try_parse_unsupported_statement(sql: &str) -> Option<Statement> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();

    let feature = if lowered.starts_with("alter table ") {
        Some("ALTER TABLE form")
    } else if lowered.starts_with("alter index ") {
        Some("ALTER INDEX")
    } else if lowered.starts_with("alter view ") {
        Some("ALTER VIEW")
    } else if lowered.starts_with("drop index ") {
        Some("DROP INDEX")
    } else if lowered.starts_with("drop table ") {
        Some("DROP TABLE form")
    } else if lowered.starts_with("drop domain ") {
        Some("DROP DOMAIN")
    } else if lowered.starts_with("comment on column ") {
        Some("COMMENT ON COLUMN")
    } else if lowered.starts_with("comment on constraint ") {
        Some("COMMENT ON CONSTRAINT")
    } else if lowered.starts_with("comment on index ") {
        Some("COMMENT ON INDEX")
    } else if lowered.starts_with("create domain ") {
        Some("CREATE DOMAIN")
    } else if lowered.starts_with("copy ") && lowered.contains(" to ") {
        Some("COPY TO")
    } else if lowered.starts_with("create unique index ") {
        Some("CREATE UNIQUE INDEX")
    } else if lowered.starts_with("create index ") {
        Some("CREATE INDEX form")
    } else if lowered.starts_with("create view ") {
        Some("CREATE VIEW form")
    } else if lowered.starts_with("create temp table ") {
        Some("CREATE TEMP TABLE form")
    } else if lowered.starts_with("select ") || lowered.starts_with("with ") {
        Some("SELECT form")
    } else if lowered.starts_with("delete from ") {
        Some("DELETE form")
    } else {
        None
    }?;

    Some(Statement::Unsupported(UnsupportedStatement {
        sql: trimmed.into(),
        feature,
    }))
}

fn try_parse_index_statement(sql: &str) -> Result<Option<Statement>, ParseError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();
    if !lowered.starts_with("alter index ") {
        return Ok(None);
    }
    if lowered.contains(" rename to ") {
        return build_alter_index_rename_statement(trimmed)
            .map(|stmt| Some(Statement::AlterIndexRename(stmt)));
    }
    if lowered.contains(" set statistics ") {
        return build_alter_index_alter_column_statistics_statement(trimmed)
            .map(|stmt| Some(Statement::AlterIndexAlterColumnStatistics(stmt)));
    }
    Ok(None)
}

fn try_parse_view_statement(sql: &str) -> Result<Option<Statement>, ParseError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();
    if !lowered.starts_with("alter view ") {
        return Ok(None);
    }
    if lowered.contains(" rename to ") {
        return build_alter_view_rename_statement(trimmed)
            .map(|stmt| Some(Statement::AlterViewRename(stmt)));
    }
    Ok(None)
}
fn try_parse_domain_statement(sql: &str) -> Result<Option<Statement>, ParseError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();
    if lowered.starts_with("create domain ") {
        return build_create_domain_statement(trimmed)
            .map(|stmt| Some(Statement::CreateDomain(stmt)));
    }
    if lowered.starts_with("drop domain ") {
        return build_drop_domain_statement(trimmed).map(|stmt| Some(Statement::DropDomain(stmt)));
    }
    if lowered.starts_with("comment on domain ") {
        return build_comment_on_domain_statement(trimmed)
            .map(|stmt| Some(Statement::CommentOnDomain(stmt)));
    }
    Ok(None)
}

fn try_parse_conversion_statement(sql: &str) -> Result<Option<Statement>, ParseError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();
    if lowered.starts_with("create default conversion ")
        || lowered.starts_with("create conversion ")
    {
        return build_create_conversion_statement(trimmed)
            .map(|stmt| Some(Statement::CreateConversion(stmt)));
    }
    if lowered.starts_with("drop conversion ") {
        return build_drop_conversion_statement(trimmed)
            .map(|stmt| Some(Statement::DropConversion(stmt)));
    }
    if lowered.starts_with("comment on conversion ") {
        return build_comment_on_conversion_statement(trimmed)
            .map(|stmt| Some(Statement::CommentOnConversion(stmt)));
    }
    Ok(None)
}

fn try_parse_foreign_data_wrapper_statement(sql: &str) -> Result<Option<Statement>, ParseError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();
    if lowered.starts_with("create foreign data wrapper ") {
        return build_create_foreign_data_wrapper_statement(trimmed)
            .map(|stmt| Some(Statement::CreateForeignDataWrapper(stmt)));
    }
    if lowered.starts_with("alter foreign data wrapper ") {
        return build_alter_foreign_data_wrapper_statement(trimmed);
    }
    if lowered.starts_with("drop foreign data wrapper ") {
        return build_drop_foreign_data_wrapper_statement(trimmed)
            .map(|stmt| Some(Statement::DropForeignDataWrapper(stmt)));
    }
    if lowered.starts_with("comment on foreign data wrapper ") {
        return build_comment_on_foreign_data_wrapper_statement(trimmed)
            .map(|stmt| Some(Statement::CommentOnForeignDataWrapper(stmt)));
    }
    Ok(None)
}

fn build_create_foreign_data_wrapper_statement(
    sql: &str,
) -> Result<CreateForeignDataWrapperStatement, ParseError> {
    let mut rest = sql["create foreign data wrapper ".len()..].trim_start();
    let (fdw_name, next) = parse_sql_identifier(rest)?;
    rest = next.trim_start();
    let mut handler_name = None;
    let mut validator_name = None;
    let mut saw_handler = false;
    let mut saw_validator = false;
    while !rest.is_empty() && !keyword_at_start(rest, "options") {
        if keyword_at_start(rest, "handler") {
            if saw_handler {
                return Err(ParseError::FeatureNotSupportedMessage(
                    "conflicting or redundant options".into(),
                ));
            }
            let next = consume_keyword(rest, "handler").trim_start();
            let (name, tail) = parse_sql_identifier(next)?;
            handler_name = Some(name);
            saw_handler = true;
            rest = tail.trim_start();
            continue;
        }
        if keyword_at_start(rest, "no handler") {
            if saw_handler {
                return Err(ParseError::FeatureNotSupportedMessage(
                    "conflicting or redundant options".into(),
                ));
            }
            handler_name = None;
            saw_handler = true;
            rest = consume_keyword(rest, "no handler").trim_start();
            continue;
        }
        if keyword_at_start(rest, "validator") {
            if saw_validator {
                return Err(ParseError::FeatureNotSupportedMessage(
                    "conflicting or redundant options".into(),
                ));
            }
            let next = consume_keyword(rest, "validator").trim_start();
            let (name, tail) = parse_sql_identifier(next)?;
            validator_name = Some(name);
            saw_validator = true;
            rest = tail.trim_start();
            continue;
        }
        if keyword_at_start(rest, "no validator") {
            if saw_validator {
                return Err(ParseError::FeatureNotSupportedMessage(
                    "conflicting or redundant options".into(),
                ));
            }
            validator_name = None;
            saw_validator = true;
            rest = consume_keyword(rest, "no validator").trim_start();
            continue;
        }
        return Err(ParseError::UnexpectedToken {
            expected: "HANDLER, NO HANDLER, VALIDATOR, NO VALIDATOR, OPTIONS, or end of statement",
            actual: rest.into(),
        });
    }
    let options = if rest.is_empty() {
        Vec::new()
    } else {
        parse_create_generic_options(rest)?
    };
    Ok(CreateForeignDataWrapperStatement {
        fdw_name,
        handler_name,
        validator_name,
        options,
    })
}

fn build_alter_foreign_data_wrapper_statement(sql: &str) -> Result<Option<Statement>, ParseError> {
    let mut rest = sql["alter foreign data wrapper ".len()..].trim_start();
    let (fdw_name, next) = parse_sql_identifier(rest)?;
    rest = next.trim_start();
    if keyword_at_start(rest, "owner to") {
        let next = consume_keyword(rest, "owner to").trim_start();
        let (new_owner, tail) = parse_sql_identifier(next)?;
        if !tail.trim().is_empty() {
            return Err(ParseError::UnexpectedToken {
                expected: "end of statement",
                actual: tail.trim().into(),
            });
        }
        return Ok(Some(Statement::AlterForeignDataWrapperOwner(
            AlterForeignDataWrapperOwnerStatement {
                fdw_name,
                new_owner,
            },
        )));
    }
    if keyword_at_start(rest, "rename to") {
        let next = consume_keyword(rest, "rename to").trim_start();
        let (new_name, tail) = parse_sql_identifier(next)?;
        if !tail.trim().is_empty() {
            return Err(ParseError::UnexpectedToken {
                expected: "end of statement",
                actual: tail.trim().into(),
            });
        }
        return Ok(Some(Statement::AlterForeignDataWrapperRename(
            AlterForeignDataWrapperRenameStatement { fdw_name, new_name },
        )));
    }

    let mut handler_name = None;
    let mut validator_name = None;
    let mut saw_handler = false;
    let mut saw_validator = false;
    while !rest.is_empty() && !keyword_at_start(rest, "options") {
        if keyword_at_start(rest, "handler") {
            if saw_handler {
                return Err(ParseError::FeatureNotSupportedMessage(
                    "conflicting or redundant options".into(),
                ));
            }
            let next = consume_keyword(rest, "handler").trim_start();
            let (name, tail) = parse_sql_identifier(next)?;
            handler_name = Some(Some(name));
            saw_handler = true;
            rest = tail.trim_start();
            continue;
        }
        if keyword_at_start(rest, "no handler") {
            if saw_handler {
                return Err(ParseError::FeatureNotSupportedMessage(
                    "conflicting or redundant options".into(),
                ));
            }
            handler_name = Some(None);
            saw_handler = true;
            rest = consume_keyword(rest, "no handler").trim_start();
            continue;
        }
        if keyword_at_start(rest, "validator") {
            if saw_validator {
                return Err(ParseError::FeatureNotSupportedMessage(
                    "conflicting or redundant options".into(),
                ));
            }
            let next = consume_keyword(rest, "validator").trim_start();
            let (name, tail) = parse_sql_identifier(next)?;
            validator_name = Some(Some(name));
            saw_validator = true;
            rest = tail.trim_start();
            continue;
        }
        if keyword_at_start(rest, "no validator") {
            if saw_validator {
                return Err(ParseError::FeatureNotSupportedMessage(
                    "conflicting or redundant options".into(),
                ));
            }
            validator_name = Some(None);
            saw_validator = true;
            rest = consume_keyword(rest, "no validator").trim_start();
            continue;
        }
        break;
    }
    let options = if rest.is_empty() {
        Vec::new()
    } else {
        parse_alter_generic_options(rest)?
    };
    Ok(Some(Statement::AlterForeignDataWrapper(
        AlterForeignDataWrapperStatement {
            fdw_name,
            handler_name,
            validator_name,
            options,
        },
    )))
}

fn build_drop_foreign_data_wrapper_statement(
    sql: &str,
) -> Result<DropForeignDataWrapperStatement, ParseError> {
    let tokens = sql.split_whitespace().collect::<Vec<_>>();
    let mut index = 4usize;
    let mut if_exists = false;
    if tokens
        .get(index)
        .is_some_and(|token| token.eq_ignore_ascii_case("if"))
    {
        if !tokens
            .get(index + 1)
            .is_some_and(|token| token.eq_ignore_ascii_case("exists"))
        {
            return Err(ParseError::UnexpectedToken {
                expected: "EXISTS",
                actual: tokens.get(index + 1).unwrap_or(&"").to_string(),
            });
        }
        if_exists = true;
        index += 2;
    }
    let Some(fdw_name) = tokens.get(index) else {
        return Err(ParseError::UnexpectedToken {
            expected: "foreign-data wrapper name",
            actual: sql.into(),
        });
    };
    let cascade = tokens
        .get(index + 1)
        .is_some_and(|token| token.eq_ignore_ascii_case("cascade"));
    if tokens.len() > index + 1 && !cascade && !tokens[index + 1].eq_ignore_ascii_case("restrict") {
        return Err(ParseError::UnexpectedToken {
            expected: "CASCADE, RESTRICT, or end of statement",
            actual: tokens[index + 1].into(),
        });
    }
    if tokens.len() > index + 2 {
        return Err(ParseError::UnexpectedToken {
            expected: "end of statement",
            actual: sql.into(),
        });
    }
    Ok(DropForeignDataWrapperStatement {
        if_exists,
        fdw_name: (*fdw_name).to_string(),
        cascade,
    })
}

fn build_comment_on_foreign_data_wrapper_statement(
    sql: &str,
) -> Result<CommentOnForeignDataWrapperStatement, ParseError> {
    let lower = sql.to_ascii_lowercase();
    let Some(is_offset) = lower.find(" is ") else {
        return Err(ParseError::UnexpectedToken {
            expected: "COMMENT ON FOREIGN DATA WRAPPER name IS ...",
            actual: sql.into(),
        });
    };
    let object = sql["comment on foreign data wrapper ".len()..is_offset].trim();
    let value = sql[is_offset + 4..].trim();
    let comment = if value.eq_ignore_ascii_case("null") {
        None
    } else if value.starts_with('\'') && value.ends_with('\'') && value.len() >= 2 {
        Some(value[1..value.len() - 1].replace("''", "'"))
    } else {
        return Err(ParseError::UnexpectedToken {
            expected: "quoted string or NULL",
            actual: value.into(),
        });
    };
    Ok(CommentOnForeignDataWrapperStatement {
        fdw_name: object.to_string(),
        comment,
    })
}

fn parse_create_generic_options(input: &str) -> Result<Vec<RelOption>, ParseError> {
    let rest = consume_keyword(input.trim_start(), "options").trim_start();
    let Some(inner) = rest
        .strip_prefix('(')
        .and_then(|value| value.strip_suffix(')'))
    else {
        return Err(ParseError::UnexpectedToken {
            expected: "OPTIONS (name 'value' [, ...])",
            actual: input.into(),
        });
    };
    parse_generic_option_list(inner)
}

fn parse_alter_generic_options(input: &str) -> Result<Vec<AlterGenericOption>, ParseError> {
    let rest = consume_keyword(input.trim_start(), "options").trim_start();
    let Some(inner) = rest
        .strip_prefix('(')
        .and_then(|value| value.strip_suffix(')'))
    else {
        return Err(ParseError::UnexpectedToken {
            expected: "OPTIONS (ADD|SET|DROP ...)",
            actual: input.into(),
        });
    };
    split_comma_separated_sql(inner)?
        .into_iter()
        .map(|part| {
            let part = part.trim();
            if keyword_at_start(part, "set") {
                let option = parse_rel_option(consume_keyword(part, "set").trim_start())?;
                Ok(AlterGenericOption {
                    action: AlterGenericOptionAction::Set,
                    name: option.name,
                    value: Some(option.value),
                })
            } else if keyword_at_start(part, "add") {
                let option = parse_rel_option(consume_keyword(part, "add").trim_start())?;
                Ok(AlterGenericOption {
                    action: AlterGenericOptionAction::Add,
                    name: option.name,
                    value: Some(option.value),
                })
            } else if keyword_at_start(part, "drop") {
                let (name, tail) =
                    parse_sql_identifier(consume_keyword(part, "drop").trim_start())?;
                if !tail.trim().is_empty() {
                    return Err(ParseError::UnexpectedToken {
                        expected: "end of option",
                        actual: tail.trim().into(),
                    });
                }
                Ok(AlterGenericOption {
                    action: AlterGenericOptionAction::Drop,
                    name,
                    value: None,
                })
            } else {
                let option = parse_rel_option(part)?;
                Ok(AlterGenericOption {
                    action: AlterGenericOptionAction::Add,
                    name: option.name,
                    value: Some(option.value),
                })
            }
        })
        .collect()
}

fn parse_generic_option_list(input: &str) -> Result<Vec<RelOption>, ParseError> {
    split_comma_separated_sql(input)?
        .into_iter()
        .map(|part| parse_rel_option(part.trim()))
        .collect()
}

fn parse_rel_option(input: &str) -> Result<RelOption, ParseError> {
    let (name, rest) = parse_sql_identifier(input)?;
    let rest = rest.trim_start();
    let literal_len = scan_string_literal_token_len(rest).ok_or(ParseError::UnexpectedToken {
        expected: "string literal",
        actual: rest.into(),
    })?;
    let value = decode_string_literal(&rest[..literal_len])?;
    if !rest[literal_len..].trim().is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "end of option",
            actual: rest[literal_len..].trim().into(),
        });
    }
    Ok(RelOption { name, value })
}

fn split_comma_separated_sql(input: &str) -> Result<Vec<&str>, ParseError> {
    let mut parts = Vec::new();
    let mut start = 0usize;
    let bytes = input.as_bytes();
    let mut index = 0usize;
    while index < bytes.len() {
        if bytes[index] == b'\'' {
            let literal_len =
                scan_string_literal_token_len(&input[index..]).ok_or(ParseError::UnexpectedEof)?;
            index += literal_len;
            continue;
        }
        if bytes[index] == b',' {
            parts.push(input[start..index].trim());
            start = index + 1;
        }
        index += 1;
    }
    let trailing = input[start..].trim();
    if !trailing.is_empty() {
        parts.push(trailing);
    }
    Ok(parts)
}

fn try_parse_create_function_statement(sql: &str) -> Result<Option<Statement>, ParseError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();
    if !lowered.starts_with("create function ")
        && !lowered.starts_with("create or replace function ")
    {
        return Ok(None);
    }
    Ok(Some(Statement::CreateFunction(
        build_create_function_statement(trimmed)?,
    )))
}

fn try_parse_drop_function_statement(sql: &str) -> Result<Option<Statement>, ParseError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();
    if !lowered.starts_with("drop function ") {
        return Ok(None);
    }
    Ok(Some(Statement::DropFunction(
        build_drop_function_statement(trimmed)?,
    )))
}

fn try_parse_comment_on_function_statement(sql: &str) -> Result<Option<Statement>, ParseError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();
    if !lowered.starts_with("comment on function ") {
        return Ok(None);
    }
    Ok(Some(Statement::CommentOnFunction(
        build_comment_on_function_statement(trimmed)?,
    )))
}

fn try_parse_create_operator_class_statement(sql: &str) -> Result<Option<Statement>, ParseError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();
    if !lowered.starts_with("create operator class ") {
        return Ok(None);
    }
    Ok(Some(Statement::CreateOperatorClass(
        build_create_operator_class_statement(trimmed)?,
    )))
}

fn try_parse_operator_statement(sql: &str) -> Result<Option<Statement>, ParseError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();
    if lowered.starts_with("create operator ")
        && !lowered.starts_with("create operator class ")
        && !lowered.starts_with("create operator family ")
    {
        return build_create_operator_statement(trimmed)
            .map(|stmt| Some(Statement::CreateOperator(stmt)));
    }
    if lowered.starts_with("alter operator ")
        && !lowered.starts_with("alter operator class ")
        && !lowered.starts_with("alter operator family ")
    {
        return build_alter_operator_statement(trimmed)
            .map(|stmt| Some(Statement::AlterOperator(stmt)));
    }
    if lowered.starts_with("drop operator ")
        && !lowered.starts_with("drop operator class ")
        && !lowered.starts_with("drop operator family ")
    {
        return build_drop_operator_statement(trimmed)
            .map(|stmt| Some(Statement::DropOperator(stmt)));
    }
    Ok(None)
}

fn try_parse_create_type_statement(sql: &str) -> Result<Option<Statement>, ParseError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();
    if lowered.starts_with("create type ") {
        return build_create_type_statement(trimmed).map(|stmt| Some(Statement::CreateType(stmt)));
    }
    if lowered.starts_with("drop type ") {
        return build_drop_type_statement(trimmed).map(|stmt| Some(Statement::DropType(stmt)));
    }
    Ok(None)
}

fn try_parse_grant_revoke_statement(sql: &str) -> Result<Option<Statement>, ParseError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();
    if lowered.starts_with("grant ") {
        return build_grant_statement(trimmed).map(Some);
    }
    if lowered.starts_with("revoke ") {
        return build_revoke_statement(trimmed).map(Some);
    }
    Ok(None)
}

fn build_grant_statement(sql: &str) -> Result<Statement, ParseError> {
    let lowered = sql.to_ascii_lowercase();
    if lowered.starts_with("grant create on database ") {
        return Ok(Statement::GrantObject(build_grant_database_create(sql)?));
    }
    if lowered.starts_with("grant all on schema ") {
        return Ok(Statement::GrantObject(build_grant_schema_all(sql)?));
    }
    if lowered.starts_with("grant execute on function ") {
        return Ok(Statement::GrantObject(build_grant_function_execute(sql)?));
    }
    if lowered.starts_with("grant select on ") {
        return Ok(Statement::GrantObject(build_grant_table_select(sql)?));
    }
    if lowered.starts_with("grant all on ") {
        return Ok(Statement::GrantObject(build_grant_table_all(sql)?));
    }
    if lowered.starts_with("grant all privileges on ") {
        return Ok(Statement::GrantObject(build_grant_table_all_privileges(
            sql,
        )?));
    }
    Ok(Statement::GrantRoleMembership(build_grant_role_membership(
        sql,
    )?))
}

fn build_revoke_statement(sql: &str) -> Result<Statement, ParseError> {
    let lowered = sql.to_ascii_lowercase();
    if lowered.starts_with("revoke create on database ") {
        return Ok(Statement::RevokeObject(build_revoke_database_create(sql)?));
    }
    if lowered.starts_with("revoke all privileges on ") {
        return Ok(Statement::RevokeObject(build_revoke_table_all_privileges(
            sql,
        )?));
    }
    Ok(Statement::RevokeRoleMembership(
        build_revoke_role_membership(sql)?,
    ))
}

fn build_grant_database_create(sql: &str) -> Result<GrantObjectStatement, ParseError> {
    let prefix = "grant create on database ";
    let rest = sql
        .get(prefix.len()..)
        .ok_or(ParseError::UnexpectedEof)?
        .trim_start();
    let (object_name, rest) = split_once_keyword(rest, "to")?;
    let (grantee_names, with_grant_option) = parse_grantees_with_optional_grant(rest)?;
    Ok(GrantObjectStatement {
        privilege: GrantObjectPrivilege::CreateOnDatabase,
        object_names: vec![normalize_simple_identifier(object_name)?],
        grantee_names,
        with_grant_option,
    })
}

fn build_grant_table_all_privileges(sql: &str) -> Result<GrantObjectStatement, ParseError> {
    let prefix = "grant all privileges on ";
    let rest = sql
        .get(prefix.len()..)
        .ok_or(ParseError::UnexpectedEof)?
        .trim_start();
    let (object_name, rest) = split_once_keyword(rest, "to")?;
    let (grantee_names, with_grant_option) = parse_grantees_with_optional_grant(rest)?;
    Ok(GrantObjectStatement {
        privilege: GrantObjectPrivilege::AllPrivilegesOnTable,
        object_names: vec![normalize_simple_identifier(object_name)?],
        grantee_names,
        with_grant_option,
    })
}

fn build_grant_table_all(sql: &str) -> Result<GrantObjectStatement, ParseError> {
    let prefix = "grant all on ";
    let rest = sql
        .get(prefix.len()..)
        .ok_or(ParseError::UnexpectedEof)?
        .trim_start();
    let (object_name, rest) = split_once_keyword(rest, "to")?;
    let (grantee_names, with_grant_option) = parse_grantees_with_optional_grant(rest)?;
    Ok(GrantObjectStatement {
        privilege: GrantObjectPrivilege::AllPrivilegesOnTable,
        object_names: vec![normalize_simple_identifier(object_name)?],
        grantee_names,
        with_grant_option,
    })
}

fn build_grant_table_select(sql: &str) -> Result<GrantObjectStatement, ParseError> {
    let prefix = "grant select on ";
    let rest = sql
        .get(prefix.len()..)
        .ok_or(ParseError::UnexpectedEof)?
        .trim_start();
    let (object_name, rest) = split_once_keyword(rest, "to")?;
    let (grantee_names, with_grant_option) = parse_grantees_with_optional_grant(rest)?;
    Ok(GrantObjectStatement {
        privilege: GrantObjectPrivilege::SelectOnTable,
        object_names: vec![normalize_simple_identifier(object_name)?],
        grantee_names,
        with_grant_option,
    })
}

fn build_grant_schema_all(sql: &str) -> Result<GrantObjectStatement, ParseError> {
    let prefix = "grant all on schema ";
    let rest = sql
        .get(prefix.len()..)
        .ok_or(ParseError::UnexpectedEof)?
        .trim_start();
    let (object_names, rest) = split_once_keyword(rest, "to")?;
    let (grantee_names, with_grant_option) = parse_grantees_with_optional_grant(rest)?;
    Ok(GrantObjectStatement {
        privilege: GrantObjectPrivilege::AllPrivilegesOnSchema,
        object_names: parse_identifier_list(object_names)?,
        grantee_names,
        with_grant_option,
    })
}

fn build_grant_function_execute(sql: &str) -> Result<GrantObjectStatement, ParseError> {
    let prefix = "grant execute on function ";
    let rest = sql
        .get(prefix.len()..)
        .ok_or(ParseError::UnexpectedEof)?
        .trim_start();
    let (object_name, rest) = split_once_keyword(rest, "to")?;
    let (grantee_names, with_grant_option) = parse_grantees_with_optional_grant(rest)?;
    Ok(GrantObjectStatement {
        privilege: GrantObjectPrivilege::ExecuteOnFunction,
        object_names: vec![object_name.trim().to_ascii_lowercase()],
        grantee_names,
        with_grant_option,
    })
}

fn build_revoke_database_create(sql: &str) -> Result<RevokeObjectStatement, ParseError> {
    let prefix = "revoke create on database ";
    let rest = sql
        .get(prefix.len()..)
        .ok_or(ParseError::UnexpectedEof)?
        .trim_start();
    let (object_name, rest) = split_once_keyword(rest, "from")?;
    let (grantee_names, cascade) = parse_revokee_list_with_optional_cascade(rest)?;
    Ok(RevokeObjectStatement {
        privilege: GrantObjectPrivilege::CreateOnDatabase,
        object_names: vec![normalize_simple_identifier(object_name)?],
        grantee_names,
        cascade,
    })
}

fn build_revoke_table_all_privileges(sql: &str) -> Result<RevokeObjectStatement, ParseError> {
    let prefix = "revoke all privileges on ";
    let rest = sql
        .get(prefix.len()..)
        .ok_or(ParseError::UnexpectedEof)?
        .trim_start();
    let (object_name, rest) = split_once_keyword(rest, "from")?;
    let (grantee_names, cascade) = parse_revokee_list_with_optional_cascade(rest)?;
    Ok(RevokeObjectStatement {
        privilege: GrantObjectPrivilege::AllPrivilegesOnTable,
        object_names: vec![normalize_simple_identifier(object_name)?],
        grantee_names,
        cascade,
    })
}

fn build_grant_role_membership(sql: &str) -> Result<GrantRoleMembershipStatement, ParseError> {
    let prefix = "grant ";
    let rest = sql
        .get(prefix.len()..)
        .ok_or(ParseError::UnexpectedEof)?
        .trim_start();
    let (role_names, rest) = split_once_keyword(rest, "to")?;
    let (grant_body, granted_by_clause) =
        split_optional_keyword(rest, "granted by").unwrap_or((rest.trim(), None));
    let (grantee_names_text, with_clause) =
        split_optional_keyword(grant_body, "with").unwrap_or((grant_body.trim(), None));
    let mut stmt = GrantRoleMembershipStatement {
        role_names: parse_identifier_list(role_names)?,
        grantee_names: parse_identifier_list(grantee_names_text)?,
        admin_option: false,
        inherit_option: None,
        set_option: None,
        granted_by: granted_by_clause.map(parse_role_grantor_spec).transpose()?,
        legacy_group_syntax: false,
    };
    if let Some(with_clause) = with_clause {
        let lowered = with_clause.to_ascii_lowercase();
        if lowered == "admin option" {
            stmt.admin_option = true;
        } else {
            for option in with_clause.split(',') {
                let option = option.trim();
                let mut parts = option.split_whitespace();
                let name = parts.next().ok_or(ParseError::UnexpectedEof)?;
                let value = parts.next().ok_or(ParseError::UnexpectedEof)?;
                if parts.next().is_some() {
                    return Err(ParseError::UnexpectedToken {
                        expected: "GRANT role option",
                        actual: option.into(),
                    });
                }
                match name.to_ascii_lowercase().as_str() {
                    "admin" => stmt.admin_option = parse_grant_bool(value)?,
                    "inherit" => stmt.inherit_option = Some(parse_grant_bool(value)?),
                    "set" => stmt.set_option = Some(parse_grant_bool(value)?),
                    _ => {
                        return Err(ParseError::UnexpectedToken {
                            expected: "GRANT role option",
                            actual: option.into(),
                        });
                    }
                }
            }
        }
    }
    Ok(stmt)
}

fn build_revoke_role_membership(sql: &str) -> Result<RevokeRoleMembershipStatement, ParseError> {
    let prefix = "revoke ";
    let rest = sql
        .get(prefix.len()..)
        .ok_or(ParseError::UnexpectedEof)?
        .trim_start();
    let (revoke_body, cascade) = split_optional_cascade_restrict_clause(rest)?;
    let (revoke_body, granted_by_clause) =
        split_optional_keyword(revoke_body, "granted by").unwrap_or((revoke_body.trim(), None));
    let (role_names, rest, revoke_membership, flags) =
        if let Some(stripped) = strip_keyword_prefix(revoke_body, "admin option for") {
            let (role_names, rest) = split_once_keyword(stripped, "from")?;
            (role_names, rest, false, (true, false, false))
        } else if let Some(stripped) = strip_keyword_prefix(revoke_body, "inherit option for") {
            let (role_names, rest) = split_once_keyword(stripped, "from")?;
            (role_names, rest, false, (false, true, false))
        } else if let Some(stripped) = strip_keyword_prefix(revoke_body, "set option for") {
            let (role_names, rest) = split_once_keyword(stripped, "from")?;
            (role_names, rest, false, (false, false, true))
        } else {
            let (role_names, rest) = split_once_keyword(revoke_body, "from")?;
            (role_names, rest, true, (false, false, false))
        };
    Ok(RevokeRoleMembershipStatement {
        role_names: parse_identifier_list(role_names)?,
        grantee_names: parse_identifier_list(rest)?,
        revoke_membership,
        admin_option: flags.0,
        inherit_option: flags.1,
        set_option: flags.2,
        cascade,
        granted_by: granted_by_clause.map(parse_role_grantor_spec).transpose()?,
        legacy_group_syntax: false,
    })
}

fn parse_role_grantor_spec(input: &str) -> Result<RoleGrantorSpec, ParseError> {
    let trimmed = input.trim();
    match trimmed.to_ascii_lowercase().as_str() {
        "current_user" => Ok(RoleGrantorSpec::CurrentUser),
        "current_role" => Ok(RoleGrantorSpec::CurrentRole),
        _ => Ok(RoleGrantorSpec::RoleName(normalize_simple_identifier(
            trimmed,
        )?)),
    }
}

fn split_optional_cascade_restrict_clause(input: &str) -> Result<(&str, bool), ParseError> {
    let boundary =
        find_next_top_level_keyword(input, &["cascade", "restrict"]).unwrap_or(input.len());
    let head = input[..boundary].trim_end();
    let tail = input[boundary..].trim();
    if tail.is_empty() {
        return Ok((head, false));
    }
    if tail.eq_ignore_ascii_case("cascade") {
        return Ok((head, true));
    }
    if tail.eq_ignore_ascii_case("restrict") {
        return Ok((head, false));
    }
    Err(ParseError::UnexpectedToken {
        expected: "CASCADE, RESTRICT, or end of statement",
        actual: tail.into(),
    })
}

fn split_once_keyword<'a>(input: &'a str, keyword: &str) -> Result<(&'a str, &'a str), ParseError> {
    let (left, right) =
        split_optional_keyword(input, keyword).ok_or_else(|| ParseError::UnexpectedToken {
            expected: "keyword-delimited clause",
            actual: input.into(),
        })?;
    right
        .map(|right| (left, right))
        .ok_or_else(|| ParseError::UnexpectedEof)
}

fn split_optional_keyword<'a>(input: &'a str, keyword: &str) -> Option<(&'a str, Option<&'a str>)> {
    let lowered = input.to_ascii_lowercase();
    let needle = format!(" {keyword} ");
    let index = lowered.find(&needle)?;
    let left = input[..index].trim();
    let right = input[index + needle.len()..].trim();
    Some((left, (!right.is_empty()).then_some(right)))
}

fn strip_keyword_prefix<'a>(input: &'a str, keyword: &str) -> Option<&'a str> {
    let lowered = input.to_ascii_lowercase();
    lowered
        .starts_with(keyword)
        .then(|| input[keyword.len()..].trim_start())
}

fn parse_grantees_with_optional_grant(input: &str) -> Result<(Vec<String>, bool), ParseError> {
    let (grantees, suffix) = split_optional_keyword(input, "with")
        .map(|(grantees, suffix)| (grantees, suffix.unwrap_or_default()))
        .unwrap_or((input.trim(), ""));
    let with_grant_option = if suffix.is_empty() {
        false
    } else if suffix.eq_ignore_ascii_case("grant option") {
        true
    } else {
        return Err(ParseError::UnexpectedToken {
            expected: "WITH GRANT OPTION",
            actual: suffix.into(),
        });
    };
    Ok((parse_identifier_list(grantees)?, with_grant_option))
}

fn parse_revokee_list_with_optional_cascade(
    input: &str,
) -> Result<(Vec<String>, bool), ParseError> {
    let lowered = input.to_ascii_lowercase();
    if let Some(stripped) = lowered.strip_suffix(" cascade") {
        let grantees_len = stripped.len();
        let grantees = input[..grantees_len].trim_end();
        return Ok((parse_identifier_list(grantees)?, true));
    }
    Ok((parse_identifier_list(input)?, false))
}

fn parse_identifier_list(input: &str) -> Result<Vec<String>, ParseError> {
    input
        .split(',')
        .map(normalize_simple_identifier)
        .collect::<Result<Vec<_>, _>>()
}

fn normalize_simple_identifier(input: &str) -> Result<String, ParseError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(ParseError::UnexpectedEof);
    }
    Ok(trimmed.trim_matches('"').to_ascii_lowercase())
}

fn parse_grant_bool(input: &str) -> Result<bool, ParseError> {
    match input.trim().to_ascii_lowercase().as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        other => Err(ParseError::UnexpectedToken {
            expected: "TRUE or FALSE",
            actual: other.into(),
        }),
    }
}
fn try_parse_sequence_statement(sql: &str) -> Result<Option<Statement>, ParseError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();
    if lowered.starts_with("create sequence ")
        || lowered.starts_with("create temp sequence ")
        || lowered.starts_with("create temporary sequence ")
    {
        return build_create_sequence_statement(trimmed)
            .map(|stmt| Some(Statement::CreateSequence(stmt)));
    }
    if !lowered.starts_with("alter sequence ") && !lowered.starts_with("drop sequence ") {
        return Ok(None);
    }
    if lowered.starts_with("drop sequence ") {
        return build_drop_sequence_statement(trimmed)
            .map(|stmt| Some(Statement::DropSequence(stmt)));
    }
    if lowered.contains(" owner to ") {
        return build_alter_sequence_owner_statement(trimmed)
            .map(|stmt| Some(Statement::AlterSequenceOwner(stmt)));
    }
    if lowered.contains(" rename to ") {
        return build_alter_sequence_rename_statement(trimmed)
            .map(|stmt| Some(Statement::AlterSequenceRename(stmt)));
    }
    build_alter_sequence_statement(trimmed).map(|stmt| Some(Statement::AlterSequence(stmt)))
}

fn parse_schema_qualified_name(
    input: &str,
) -> Result<((Option<String>, String), &str), ParseError> {
    let (parts, rest) = parse_qualified_identifier_parts(input)?;
    match parts.as_slice() {
        [name] => Ok(((None, name.clone()), rest)),
        [schema, name] => Ok(((Some(schema.clone()), name.clone()), rest)),
        _ => Err(ParseError::UnsupportedQualifiedName(parts.join("."))),
    }
}

fn parse_unqualified_identifier<'a>(
    input: &'a str,
    expected: &'static str,
) -> Result<(String, &'a str), ParseError> {
    let (parts, rest) = parse_qualified_identifier_parts(input)?;
    match parts.as_slice() {
        [name] => Ok((name.clone(), rest)),
        _ => Err(ParseError::UnexpectedToken {
            expected,
            actual: parts.join("."),
        }),
    }
}

fn parse_qualified_identifier_parts(input: &str) -> Result<(Vec<String>, &str), ParseError> {
    let (first, mut rest) = parse_sql_identifier(input)?;
    let mut parts = vec![first];
    loop {
        let trimmed = rest.trim_start();
        let Some(after_dot) = trimmed.strip_prefix('.') else {
            return Ok((parts, rest));
        };
        let (next, remaining) = parse_sql_identifier(after_dot.trim_start())?;
        parts.push(next);
        rest = remaining;
    }
}

fn parse_sequence_owned_by(input: &str) -> Result<(SequenceOwnedByClause, &str), ParseError> {
    let mut rest = consume_keyword(input.trim_start(), "owned").trim_start();
    rest = consume_keyword(rest, "by").trim_start();
    if keyword_at_start(rest, "none") {
        return Ok((SequenceOwnedByClause::None, consume_keyword(rest, "none")));
    }
    let (parts, rest) = parse_qualified_identifier_parts(rest)?;
    match parts.as_slice() {
        [table_name, column_name] => Ok((
            SequenceOwnedByClause::Column {
                table_name: table_name.clone(),
                column_name: column_name.clone(),
            },
            rest,
        )),
        [schema_name, table_name, column_name] => Ok((
            SequenceOwnedByClause::Column {
                table_name: format!("{schema_name}.{table_name}"),
                column_name: column_name.clone(),
            },
            rest,
        )),
        _ => Err(ParseError::UnexpectedToken {
            expected: "OWNED BY table.column or OWNED BY NONE",
            actual: input.into(),
        }),
    }
}

fn parse_signed_i64_token(input: &str) -> Result<(i64, &str), ParseError> {
    let input = input.trim_start();
    let mut end = 0usize;
    for (idx, ch) in input.char_indices() {
        if idx == 0 && matches!(ch, '+' | '-') {
            end = ch.len_utf8();
            continue;
        }
        if ch.is_ascii_digit() {
            end = idx + ch.len_utf8();
            continue;
        }
        break;
    }
    if end == 0 || (end == 1 && matches!(input.as_bytes()[0], b'+' | b'-')) {
        return Err(ParseError::UnexpectedToken {
            expected: "signed integer",
            actual: input.into(),
        });
    }
    let token = &input[..end];
    let value = token
        .parse::<i64>()
        .map_err(|_| ParseError::UnexpectedToken {
            expected: "signed integer in i64 range",
            actual: token.into(),
        })?;
    Ok((value, &input[end..]))
}

fn parse_positive_i64_token<'a>(
    input: &'a str,
    context: &'static str,
) -> Result<(i64, &'a str), ParseError> {
    let (value, rest) = parse_signed_i64_token(input)?;
    if value <= 0 {
        return Err(ParseError::UnexpectedToken {
            expected: context,
            actual: value.to_string(),
        });
    }
    Ok((value, rest))
}

fn parse_sequence_option_spec(input: &str) -> Result<(SequenceOptionsSpec, &str), ParseError> {
    let mut rest = input;
    let mut options = SequenceOptionsSpec::default();
    loop {
        let trimmed = rest.trim_start();
        if trimmed.is_empty() {
            return Ok((options, trimmed));
        }
        if keyword_at_start(trimmed, "increment") {
            let mut next = consume_keyword(trimmed, "increment").trim_start();
            if keyword_at_start(next, "by") {
                next = consume_keyword(next, "by").trim_start();
            }
            let (value, remainder) = parse_signed_i64_token(next)?;
            options.increment = Some(value);
            rest = remainder;
            continue;
        }
        if keyword_at_start(trimmed, "minvalue") {
            let (value, remainder) = parse_signed_i64_token(consume_keyword(trimmed, "minvalue"))?;
            options.minvalue = Some(Some(value));
            rest = remainder;
            continue;
        }
        if keyword_at_start(trimmed, "no minvalue") {
            options.minvalue = Some(None);
            rest = consume_keyword(consume_keyword(trimmed, "no").trim_start(), "minvalue");
            continue;
        }
        if keyword_at_start(trimmed, "maxvalue") {
            let (value, remainder) = parse_signed_i64_token(consume_keyword(trimmed, "maxvalue"))?;
            options.maxvalue = Some(Some(value));
            rest = remainder;
            continue;
        }
        if keyword_at_start(trimmed, "no maxvalue") {
            options.maxvalue = Some(None);
            rest = consume_keyword(consume_keyword(trimmed, "no").trim_start(), "maxvalue");
            continue;
        }
        if keyword_at_start(trimmed, "start") {
            let mut next = consume_keyword(trimmed, "start").trim_start();
            if keyword_at_start(next, "with") {
                next = consume_keyword(next, "with").trim_start();
            }
            let (value, remainder) = parse_signed_i64_token(next)?;
            options.start = Some(value);
            rest = remainder;
            continue;
        }
        if keyword_at_start(trimmed, "cache") {
            let (value, remainder) = parse_positive_i64_token(
                consume_keyword(trimmed, "cache"),
                "positive CACHE value",
            )?;
            options.cache = Some(value);
            rest = remainder;
            continue;
        }
        if keyword_at_start(trimmed, "cycle") {
            options.cycle = Some(true);
            rest = consume_keyword(trimmed, "cycle");
            continue;
        }
        if keyword_at_start(trimmed, "no cycle") {
            options.cycle = Some(false);
            rest = consume_keyword(consume_keyword(trimmed, "no").trim_start(), "cycle");
            continue;
        }
        if keyword_at_start(trimmed, "owned") {
            let (owned_by, remainder) = parse_sequence_owned_by(trimmed)?;
            options.owned_by = Some(owned_by);
            rest = remainder;
            continue;
        }
        return Ok((options, trimmed));
    }
}

fn parse_sequence_option_patch(
    input: &str,
) -> Result<(SequenceOptionsPatchSpec, &str), ParseError> {
    let mut rest = input;
    let mut options = SequenceOptionsPatchSpec::default();
    loop {
        let trimmed = rest.trim_start();
        if trimmed.is_empty() {
            return Ok((options, trimmed));
        }
        if keyword_at_start(trimmed, "restart") {
            let mut next = consume_keyword(trimmed, "restart").trim_start();
            if keyword_at_start(next, "with") {
                next = consume_keyword(next, "with").trim_start();
                let (value, remainder) = parse_signed_i64_token(next)?;
                options.restart = Some(Some(value));
                rest = remainder;
            } else {
                options.restart = Some(None);
                rest = next;
            }
            continue;
        }
        let (base, remainder) = parse_sequence_option_spec(trimmed)?;
        if remainder == trimmed {
            return Ok((options, trimmed));
        }
        if base.increment.is_some() {
            options.increment = base.increment;
        }
        if base.minvalue.is_some() {
            options.minvalue = base.minvalue;
        }
        if base.maxvalue.is_some() {
            options.maxvalue = base.maxvalue;
        }
        if base.start.is_some() {
            options.start = base.start;
        }
        if base.cache.is_some() {
            options.cache = base.cache;
        }
        if base.cycle.is_some() {
            options.cycle = base.cycle;
        }
        if base.owned_by.is_some() {
            options.owned_by = base.owned_by;
        }
        rest = remainder;
    }
}

fn build_create_sequence_statement(sql: &str) -> Result<CreateSequenceStatement, ParseError> {
    let mut rest = consume_keyword(sql.trim_start(), "create").trim_start();
    let persistence = if keyword_at_start(rest, "temporary") {
        rest = consume_keyword(rest, "temporary");
        TablePersistence::Temporary
    } else if keyword_at_start(rest, "temp") {
        rest = consume_keyword(rest, "temp");
        TablePersistence::Temporary
    } else {
        TablePersistence::Permanent
    };
    rest = rest.trim_start();
    rest = consume_keyword(rest, "sequence").trim_start();
    let mut if_not_exists = false;
    if keyword_at_start(rest, "if") {
        let after_if = consume_keyword(rest, "if").trim_start();
        if !keyword_at_start(after_if, "not") {
            return Err(ParseError::UnexpectedToken {
                expected: "IF NOT EXISTS",
                actual: sql.into(),
            });
        }
        let after_not = consume_keyword(after_if, "not").trim_start();
        if !keyword_at_start(after_not, "exists") {
            return Err(ParseError::UnexpectedToken {
                expected: "IF NOT EXISTS",
                actual: sql.into(),
            });
        }
        rest = consume_keyword(after_not, "exists").trim_start();
        if_not_exists = true;
    }
    let ((schema_name, sequence_name), rest) = parse_schema_qualified_name(rest)?;
    let (options, rest) = parse_sequence_option_spec(rest)?;
    if !rest.trim().is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "end of CREATE SEQUENCE statement",
            actual: rest.trim().into(),
        });
    }
    Ok(CreateSequenceStatement {
        schema_name,
        sequence_name,
        persistence,
        if_not_exists,
        options,
    })
}

fn build_alter_sequence_statement(sql: &str) -> Result<AlterSequenceStatement, ParseError> {
    let mut rest = consume_keyword(sql.trim_start(), "alter").trim_start();
    rest = consume_keyword(rest, "sequence").trim_start();
    let (parts, rest) = parse_qualified_identifier_parts(rest)?;
    let sequence_name = parts.join(".");
    let (options, rest) = parse_sequence_option_patch(rest)?;
    if !rest.trim().is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "end of ALTER SEQUENCE statement",
            actual: rest.trim().into(),
        });
    }
    Ok(AlterSequenceStatement {
        sequence_name,
        options,
    })
}

fn build_alter_sequence_owner_statement(
    sql: &str,
) -> Result<AlterRelationOwnerStatement, ParseError> {
    let mut rest = consume_keyword(sql.trim_start(), "alter").trim_start();
    rest = consume_keyword(rest, "sequence").trim_start();
    let (parts, rest) = parse_qualified_identifier_parts(rest)?;
    let relation_name = parts.join(".");
    let mut rest = rest.trim_start();
    rest = consume_keyword(rest, "owner").trim_start();
    rest = consume_keyword(rest, "to").trim_start();
    let (new_owner, rest) = parse_sql_identifier(rest)?;
    if !rest.trim().is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "end of ALTER SEQUENCE OWNER statement",
            actual: rest.trim().into(),
        });
    }
    Ok(AlterRelationOwnerStatement {
        if_exists: false,
        only: false,
        relation_name,
        new_owner,
    })
}

fn build_alter_sequence_rename_statement(
    sql: &str,
) -> Result<AlterTableRenameStatement, ParseError> {
    let mut rest = consume_keyword(sql.trim_start(), "alter").trim_start();
    rest = consume_keyword(rest, "sequence").trim_start();
    let (parts, rest) = parse_qualified_identifier_parts(rest)?;
    let table_name = parts.join(".");
    let mut rest = rest.trim_start();
    rest = consume_keyword(rest, "rename").trim_start();
    rest = consume_keyword(rest, "to").trim_start();
    let (new_table_name, rest) = parse_sql_identifier(rest)?;
    if !rest.trim().is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "end of ALTER SEQUENCE RENAME statement",
            actual: rest.trim().into(),
        });
    }
    Ok(AlterTableRenameStatement {
        if_exists: false,
        only: false,
        table_name,
        new_table_name,
    })
}

fn build_alter_index_rename_statement(sql: &str) -> Result<AlterTableRenameStatement, ParseError> {
    let mut rest = consume_keyword(sql.trim_start(), "alter").trim_start();
    rest = consume_keyword(rest, "index").trim_start();
    let mut if_exists = false;
    if keyword_at_start(rest, "if") {
        let after_if = consume_keyword(rest, "if").trim_start();
        if !keyword_at_start(after_if, "exists") {
            return Err(ParseError::UnexpectedToken {
                expected: "IF EXISTS",
                actual: sql.into(),
            });
        }
        if_exists = true;
        rest = consume_keyword(after_if, "exists").trim_start();
    }
    let (parts, rest) = parse_qualified_identifier_parts(rest)?;
    let table_name = parts.join(".");
    let mut rest = rest.trim_start();
    rest = consume_keyword(rest, "rename").trim_start();
    rest = consume_keyword(rest, "to").trim_start();
    let (new_table_name, rest) = parse_sql_identifier(rest)?;
    if !rest.trim().is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "end of ALTER INDEX RENAME statement",
            actual: rest.trim().into(),
        });
    }
    Ok(AlterTableRenameStatement {
        if_exists,
        only: false,
        table_name,
        new_table_name,
    })
}

fn build_alter_view_rename_statement(sql: &str) -> Result<AlterTableRenameStatement, ParseError> {
    let mut rest = consume_keyword(sql.trim_start(), "alter").trim_start();
    rest = consume_keyword(rest, "view").trim_start();
    let mut if_exists = false;
    if keyword_at_start(rest, "if") {
        let after_if = consume_keyword(rest, "if").trim_start();
        if !keyword_at_start(after_if, "exists") {
            return Err(ParseError::UnexpectedToken {
                expected: "IF EXISTS",
                actual: sql.into(),
            });
        }
        if_exists = true;
        rest = consume_keyword(after_if, "exists").trim_start();
    }
    let (parts, rest) = parse_qualified_identifier_parts(rest)?;
    let table_name = parts.join(".");
    let mut rest = rest.trim_start();
    rest = consume_keyword(rest, "rename").trim_start();
    rest = consume_keyword(rest, "to").trim_start();
    let (new_table_name, rest) = parse_sql_identifier(rest)?;
    if !rest.trim().is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "end of ALTER VIEW RENAME statement",
            actual: rest.trim().into(),
        });
    }
    Ok(AlterTableRenameStatement {
        if_exists,
        only: false,
        table_name,
        new_table_name,
    })
}

fn build_alter_index_alter_column_statistics_statement(
    sql: &str,
) -> Result<AlterIndexAlterColumnStatisticsStatement, ParseError> {
    let mut rest = consume_keyword(sql.trim_start(), "alter").trim_start();
    rest = consume_keyword(rest, "index").trim_start();
    let mut if_exists = false;
    if keyword_at_start(rest, "if") {
        let after_if = consume_keyword(rest, "if").trim_start();
        if !keyword_at_start(after_if, "exists") {
            return Err(ParseError::UnexpectedToken {
                expected: "IF EXISTS",
                actual: sql.into(),
            });
        }
        if_exists = true;
        rest = consume_keyword(after_if, "exists").trim_start();
    }
    let (parts, rest_after_name) = parse_qualified_identifier_parts(rest)?;
    let index_name = parts.join(".");
    let mut rest = rest_after_name.trim_start();
    rest = consume_keyword(rest, "alter").trim_start();
    if keyword_at_start(rest, "column") {
        rest = consume_keyword(rest, "column").trim_start();
    }
    let (column_number_sql, rest_after_column_number) = split_sql_identifier_token(rest)?;
    let column_number_i32 = column_number_sql
        .parse::<i32>()
        .map_err(|_| ParseError::InvalidInteger(column_number_sql.to_string()))?;
    if column_number_i32 <= 0 || column_number_i32 > i32::from(i16::MAX) {
        return Err(ParseError::DetailedError {
            message: format!("column number must be in range from 1 to {}", i16::MAX),
            detail: None,
            hint: None,
            sqlstate: "22023",
        });
    }
    let column_number = i16::try_from(column_number_i32)
        .map_err(|_| ParseError::InvalidInteger(column_number_i32.to_string()))?;
    let mut rest = rest_after_column_number.trim_start();
    rest = consume_keyword(rest, "set").trim_start();
    rest = consume_keyword(rest, "statistics").trim_start();
    let (statistics_target_sql, rest) = split_sql_identifier_token(rest)?;
    let statistics_target = statistics_target_sql
        .parse::<i32>()
        .map_err(|_| ParseError::InvalidInteger(statistics_target_sql.to_string()))?;
    if !rest.trim().is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "end of ALTER INDEX ALTER COLUMN SET STATISTICS statement",
            actual: rest.trim().into(),
        });
    }
    Ok(AlterIndexAlterColumnStatisticsStatement {
        if_exists,
        index_name,
        column_number,
        statistics_target,
    })
}
fn build_drop_sequence_statement(sql: &str) -> Result<DropSequenceStatement, ParseError> {
    let mut rest = consume_keyword(sql.trim_start(), "drop").trim_start();
    rest = consume_keyword(rest, "sequence").trim_start();
    let mut if_exists = false;
    if keyword_at_start(rest, "if") {
        let after_if = consume_keyword(rest, "if").trim_start();
        if !keyword_at_start(after_if, "exists") {
            return Err(ParseError::UnexpectedToken {
                expected: "IF EXISTS",
                actual: sql.into(),
            });
        }
        rest = consume_keyword(after_if, "exists").trim_start();
        if_exists = true;
    }
    let mut cascade = false;
    let mut sequence_names = Vec::new();
    for item in split_top_level_items(rest, ',')? {
        let trimmed = item.trim();
        if trimmed.is_empty() {
            continue;
        }
        let boundary =
            find_next_top_level_keyword(trimmed, &["cascade", "restrict"]).unwrap_or(trimmed.len());
        let name_sql = trimmed[..boundary].trim();
        if name_sql.is_empty() {
            return Err(ParseError::UnexpectedToken {
                expected: "sequence name",
                actual: sql.into(),
            });
        }
        let (parts, suffix) = parse_qualified_identifier_parts(name_sql)?;
        if !suffix.trim().is_empty() {
            return Err(ParseError::UnexpectedToken {
                expected: "sequence name",
                actual: name_sql.into(),
            });
        }
        sequence_names.push(parts.join("."));
        let tail = trimmed[boundary..].trim();
        if !tail.is_empty() {
            if tail.eq_ignore_ascii_case("cascade") {
                cascade = true;
            } else if !tail.eq_ignore_ascii_case("restrict") {
                return Err(ParseError::UnexpectedToken {
                    expected: "CASCADE or RESTRICT",
                    actual: tail.into(),
                });
            }
        }
    }
    Ok(DropSequenceStatement {
        if_exists,
        sequence_names,
        cascade,
    })
}

fn build_create_function_statement(sql: &str) -> Result<CreateFunctionStatement, ParseError> {
    let (prefix, replace_existing) = if sql
        .to_ascii_lowercase()
        .starts_with("create or replace function")
    {
        ("create or replace function", true)
    } else {
        ("create function", false)
    };
    let Some(rest) = sql.get(prefix.len()..) else {
        return Err(ParseError::UnexpectedToken {
            expected: "CREATE FUNCTION name(args) ...",
            actual: sql.into(),
        });
    };
    let rest = rest.trim_start();
    let ((schema_name, function_name), rest) = parse_qualified_sql_name(rest)?;
    let (arg_list, mut rest) = take_parenthesized_segment(rest)?;
    let args = parse_create_function_args(&arg_list)?;

    let mut return_spec = None;
    let mut language = None;
    let mut body = None;
    let mut link_symbol = None;
    let mut cost = None;
    let mut strict = false;
    let mut leakproof = false;
    let mut volatility = crate::backend::parser::FunctionVolatility::Volatile;
    let mut parallel = crate::backend::parser::FunctionParallel::Unsafe;

    while !rest.trim_start().is_empty() {
        rest = rest.trim_start();
        if keyword_at_start(rest, "returns") {
            if return_spec.is_some() {
                return Err(ParseError::UnexpectedToken {
                    expected: "single RETURNS clause",
                    actual: rest.into(),
                });
            }
            let (parsed, next_rest) = parse_create_function_returns(rest)?;
            return_spec = Some(parsed);
            rest = next_rest;
            continue;
        }
        if keyword_at_start(rest, "language") {
            if language.is_some() {
                return Err(ParseError::UnexpectedToken {
                    expected: "single LANGUAGE clause",
                    actual: rest.into(),
                });
            }
            let (parsed, next_rest) = parse_create_function_language(rest)?;
            language = Some(parsed);
            rest = next_rest;
            continue;
        }
        if keyword_at_start(rest, "cost") {
            if cost.is_some() {
                return Err(ParseError::UnexpectedToken {
                    expected: "single COST clause",
                    actual: rest.into(),
                });
            }
            let (parsed, next_rest) = parse_create_function_cost(rest)?;
            cost = Some(parsed);
            rest = next_rest;
            continue;
        }
        if keyword_at_start(rest, "as") {
            if body.is_some() {
                return Err(ParseError::UnexpectedToken {
                    expected: "single AS clause",
                    actual: rest.into(),
                });
            }
            let (parsed, parsed_link_symbol, next_rest) = parse_create_function_body(rest)?;
            body = Some(parsed);
            link_symbol = parsed_link_symbol;
            rest = next_rest;
            continue;
        }
        if keyword_at_start(rest, "return") {
            if body.is_some() {
                return Err(ParseError::UnexpectedToken {
                    expected: "single function body",
                    actual: rest.into(),
                });
            }
            let (parsed, next_rest) = parse_create_function_return_body(rest)?;
            body = Some(parsed);
            language.get_or_insert_with(|| "sql".into());
            rest = next_rest;
            continue;
        }
        if keyword_at_start(rest, "strict") {
            strict = true;
            rest = consume_keyword(rest, "strict");
            continue;
        }
        if keyword_at_start(rest, "leakproof") {
            leakproof = true;
            rest = consume_keyword(rest, "leakproof");
            continue;
        }
        if keyword_at_start(rest, "immutable") {
            volatility = crate::backend::parser::FunctionVolatility::Immutable;
            rest = consume_keyword(rest, "immutable");
            continue;
        }
        if keyword_at_start(rest, "stable") {
            volatility = crate::backend::parser::FunctionVolatility::Stable;
            rest = consume_keyword(rest, "stable");
            continue;
        }
        if keyword_at_start(rest, "volatile") {
            volatility = crate::backend::parser::FunctionVolatility::Volatile;
            rest = consume_keyword(rest, "volatile");
            continue;
        }
        if keyword_at_start(rest, "parallel") {
            let (parsed, next_rest) = parse_create_function_parallel(rest)?;
            parallel = parsed;
            rest = next_rest;
            continue;
        }
        return Err(ParseError::FeatureNotSupported(format!(
            "unsupported CREATE FUNCTION clause: {}",
            rest.split_whitespace().next().unwrap_or(rest)
        )));
    }

    let has_out_args = args
        .iter()
        .any(|arg| matches!(arg.mode, FunctionArgMode::Out | FunctionArgMode::InOut));
    let return_spec = match return_spec {
        Some(CreateFunctionReturnSpec::Type {
            ty: RawTypeName::Record,
            setof,
        }) if has_out_args => CreateFunctionReturnSpec::DerivedFromOutArgs {
            setof_record: setof,
        },
        Some(spec) => spec,
        None if has_out_args => CreateFunctionReturnSpec::DerivedFromOutArgs {
            setof_record: false,
        },
        None => {
            return Err(ParseError::UnexpectedToken {
                expected: "RETURNS clause or OUT/INOUT arguments",
                actual: sql.into(),
            });
        }
    };

    Ok(CreateFunctionStatement {
        schema_name,
        function_name,
        replace_existing,
        cost,
        args,
        return_spec,
        strict,
        leakproof,
        volatility,
        parallel,
        language: language.ok_or(ParseError::UnexpectedEof)?,
        body: body.ok_or(ParseError::UnexpectedEof)?,
        link_symbol,
    })
}

fn build_drop_function_statement(sql: &str) -> Result<DropFunctionStatement, ParseError> {
    let prefix = "drop function";
    let Some(rest) = sql.get(prefix.len()..) else {
        return Err(ParseError::UnexpectedToken {
            expected: "DROP FUNCTION name(args)",
            actual: sql.into(),
        });
    };
    let mut rest = rest.trim_start();
    let mut if_exists = false;
    if let Some(next) = consume_keywords(rest, &["if", "exists"]) {
        if_exists = true;
        rest = next.trim_start();
    }
    let ((schema_name, function_name), rest_after_name) = parse_qualified_sql_name(rest)?;
    let rest_after_name = rest_after_name.trim_start();
    let (arg_sql, suffix) = take_parenthesized_segment(rest_after_name)?;
    let suffix = suffix.trim();
    let cascade = if suffix.is_empty() || keyword_at_start(suffix, "restrict") {
        false
    } else if keyword_at_start(suffix, "cascade") {
        true
    } else {
        return Err(ParseError::UnexpectedToken {
            expected: "CASCADE, RESTRICT, or end of statement",
            actual: suffix.into(),
        });
    };
    let arg_types = split_comma_separated_sql(&arg_sql)?
        .into_iter()
        .filter(|item| !item.is_empty())
        .map(str::to_string)
        .collect();
    Ok(DropFunctionStatement {
        if_exists,
        schema_name,
        function_name,
        arg_types,
        cascade,
    })
}

#[derive(Debug, Default)]
struct ParsedCreateAggregateOptions {
    sfunc_name: Option<String>,
    stype: Option<RawTypeName>,
    finalfunc_name: Option<String>,
    initcond: Option<String>,
    parallel: Option<FunctionParallel>,
    basetype: Option<AggregateSignatureKind>,
}

fn build_create_aggregate_statement(sql: &str) -> Result<CreateAggregateStatement, ParseError> {
    let (prefix, replace_existing) = if sql
        .to_ascii_lowercase()
        .starts_with("create or replace aggregate")
    {
        ("create or replace aggregate", true)
    } else {
        ("create aggregate", false)
    };
    let Some(rest) = sql.get(prefix.len()..) else {
        return Err(ParseError::UnexpectedToken {
            expected: "CREATE AGGREGATE name(signature) (...)",
            actual: sql.into(),
        });
    };
    let rest = rest.trim_start();
    let ((schema_name, aggregate_name), rest) = parse_schema_qualified_name(rest)?;
    let (first_segment, rest) = take_parenthesized_segment(rest)?;
    let (signature, options_sql, rest) = if rest.trim_start().starts_with('(') {
        let signature = parse_aggregate_signature_kind(&first_segment)?;
        let (options_sql, rest) = take_parenthesized_segment(rest)?;
        (Some(signature), options_sql, rest)
    } else {
        (None, first_segment, rest)
    };
    if !rest.trim().is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "end of CREATE AGGREGATE",
            actual: rest.trim().into(),
        });
    }

    let parsed_options = parse_create_aggregate_options(&options_sql)?;
    let signature = match signature {
        Some(signature) => {
            if parsed_options.basetype.is_some() {
                return Err(ParseError::UnexpectedToken {
                    expected: "either an aggregate signature or BASETYPE, not both",
                    actual: aggregate_name.clone(),
                });
            }
            signature
        }
        None => parsed_options
            .basetype
            .clone()
            .ok_or_else(|| ParseError::UnexpectedToken {
                expected: "aggregate signature or BASETYPE option",
                actual: aggregate_name.clone(),
            })?,
    };

    let missing_actual = aggregate_name.clone();
    Ok(CreateAggregateStatement {
        schema_name,
        aggregate_name,
        replace_existing,
        signature,
        sfunc_name: parsed_options
            .sfunc_name
            .ok_or_else(|| ParseError::UnexpectedToken {
                expected: "SFUNC or SFUNC1 option",
                actual: missing_actual.clone(),
            })?,
        stype: parsed_options
            .stype
            .ok_or_else(|| ParseError::UnexpectedToken {
                expected: "STYPE or STYPE1 option",
                actual: missing_actual.clone(),
            })?,
        finalfunc_name: parsed_options.finalfunc_name,
        initcond: parsed_options.initcond,
        parallel: parsed_options.parallel,
    })
}

fn build_drop_aggregate_statement(sql: &str) -> Result<DropAggregateStatement, ParseError> {
    let prefix = "drop aggregate";
    let Some(rest) = sql.get(prefix.len()..) else {
        return Err(ParseError::UnexpectedToken {
            expected: "DROP AGGREGATE name(signature)",
            actual: sql.into(),
        });
    };
    let mut rest = rest.trim_start();
    let mut if_exists = false;
    if let Some(next) = consume_keywords(rest, &["if", "exists"]) {
        if_exists = true;
        rest = next.trim_start();
    }
    let ((schema_name, aggregate_name), rest_after_name) = parse_schema_qualified_name(rest)?;
    let (signature_sql, suffix) = take_parenthesized_segment(rest_after_name.trim_start())?;
    let suffix = suffix.trim();
    let cascade = if suffix.is_empty() || keyword_at_start(suffix, "restrict") {
        false
    } else if keyword_at_start(suffix, "cascade") {
        true
    } else {
        return Err(ParseError::UnexpectedToken {
            expected: "CASCADE, RESTRICT, or end of statement",
            actual: suffix.into(),
        });
    };
    Ok(DropAggregateStatement {
        if_exists,
        schema_name,
        aggregate_name,
        signature: parse_aggregate_signature_kind(&signature_sql)?,
        cascade,
    })
}

fn build_comment_on_aggregate_statement(
    sql: &str,
) -> Result<CommentOnAggregateStatement, ParseError> {
    let Some(rest) = sql.get("comment on aggregate".len()..) else {
        return Err(ParseError::UnexpectedToken {
            expected: "COMMENT ON AGGREGATE name(signature) IS ...",
            actual: sql.into(),
        });
    };
    let rest = rest.trim_start();
    let ((schema_name, aggregate_name), rest) = parse_schema_qualified_name(rest)?;
    let (signature_sql, rest) = take_parenthesized_segment(rest.trim_start())?;
    let rest = rest.trim_start();
    if !keyword_at_start(rest, "is") {
        return Err(ParseError::UnexpectedToken {
            expected: "IS",
            actual: rest.into(),
        });
    }
    let rest = consume_keyword(rest, "is").trim_start();
    let (comment, rest) = if keyword_at_start(rest, "null") {
        (None, consume_keyword(rest, "null"))
    } else {
        let len = scan_string_literal_token_len(rest).ok_or(ParseError::UnexpectedToken {
            expected: "quoted string or NULL",
            actual: rest.into(),
        })?;
        (Some(decode_string_literal(&rest[..len])?), &rest[len..])
    };
    if !rest.trim().is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "end of COMMENT ON AGGREGATE",
            actual: rest.trim().into(),
        });
    }
    Ok(CommentOnAggregateStatement {
        schema_name,
        aggregate_name,
        signature: parse_aggregate_signature_kind(&signature_sql)?,
        comment,
    })
}

fn build_comment_on_function_statement(
    sql: &str,
) -> Result<CommentOnFunctionStatement, ParseError> {
    let Some(rest) = sql.get("comment on function".len()..) else {
        return Err(ParseError::UnexpectedToken {
            expected: "COMMENT ON FUNCTION name(signature) IS ...",
            actual: sql.into(),
        });
    };
    let rest = rest.trim_start();
    let ((schema_name, function_name), rest) = parse_schema_qualified_name(rest)?;
    let (signature_sql, rest) = take_parenthesized_segment(rest.trim_start())?;
    let rest = rest.trim_start();
    if !keyword_at_start(rest, "is") {
        return Err(ParseError::UnexpectedToken {
            expected: "IS",
            actual: rest.into(),
        });
    }
    let rest = consume_keyword(rest, "is").trim_start();
    let (comment, rest) = if keyword_at_start(rest, "null") {
        (None, consume_keyword(rest, "null"))
    } else {
        let len = scan_string_literal_token_len(rest).ok_or(ParseError::UnexpectedToken {
            expected: "quoted string or NULL",
            actual: rest.into(),
        })?;
        (Some(decode_string_literal(&rest[..len])?), &rest[len..])
    };
    if !rest.trim().is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "end of COMMENT ON FUNCTION",
            actual: rest.trim().into(),
        });
    }
    let arg_types = if signature_sql.trim().is_empty() {
        Vec::new()
    } else {
        split_top_level_items(&signature_sql, ',')?
            .into_iter()
            .map(|arg| arg.trim().to_string())
            .collect::<Vec<_>>()
    };
    Ok(CommentOnFunctionStatement {
        schema_name,
        function_name,
        arg_types,
        comment,
    })
}

fn parse_create_aggregate_options(input: &str) -> Result<ParsedCreateAggregateOptions, ParseError> {
    let mut parsed = ParsedCreateAggregateOptions::default();
    for item in split_top_level_items(input, ',')? {
        let Some(eq_idx) = item.find('=') else {
            return Err(ParseError::UnexpectedToken {
                expected: "aggregate option assignment",
                actual: item,
            });
        };
        let key = item[..eq_idx].trim().to_ascii_lowercase();
        let value = item[eq_idx + 1..].trim();
        match key.as_str() {
            "sfunc" | "sfunc1" => parsed.sfunc_name = Some(parse_aggregate_proc_name(value)?),
            "stype" | "stype1" => parsed.stype = Some(parse_type_name(value)?),
            "finalfunc" => parsed.finalfunc_name = Some(parse_aggregate_proc_name(value)?),
            "initcond" | "initcond1" => parsed.initcond = Some(parse_aggregate_option_text(value)?),
            "parallel" => parsed.parallel = Some(parse_aggregate_parallel(value)?),
            "basetype" => parsed.basetype = Some(parse_legacy_aggregate_basetype(value)?),
            "sortop" | "hypothetical" | "finalfunc_extra" | "finalfunc_modify" | "sspace"
            | "combinefunc" | "serialfunc" | "deserialfunc" | "minitcond" => {
                return Err(ParseError::FeatureNotSupported(format!(
                    "{key} aggregate option is not supported"
                )));
            }
            _ if key.starts_with("ms") => {
                return Err(ParseError::FeatureNotSupported(format!(
                    "{key} aggregate option is not supported"
                )));
            }
            _ => {
                return Err(ParseError::FeatureNotSupported(format!(
                    "unsupported CREATE AGGREGATE option: {key}"
                )));
            }
        }
    }
    Ok(parsed)
}

fn parse_aggregate_signature_kind(input: &str) -> Result<AggregateSignatureKind, ParseError> {
    let trimmed = input.trim();
    if trimmed == "*" {
        return Ok(AggregateSignatureKind::Star);
    }
    if trimmed.is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "aggregate signature",
            actual: "()".into(),
        });
    }
    if keyword_boundary(trimmed, "order by").is_some() {
        return Err(ParseError::FeatureNotSupported(
            "ordered-set aggregate signatures are not supported".into(),
        ));
    }
    let args = split_top_level_items(trimmed, ',')?
        .into_iter()
        .map(|item| parse_aggregate_signature_arg(&item))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(AggregateSignatureKind::Args(args))
}

fn parse_aggregate_signature_arg(input: &str) -> Result<AggregateArgType, ParseError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "aggregate argument type",
            actual: input.into(),
        });
    }
    if keyword_at_start(trimmed, "variadic") {
        return Err(ParseError::FeatureNotSupported(
            "VARIADIC aggregate signatures are not supported".into(),
        ));
    }
    if let Ok((ident, rest)) = parse_sql_identifier(trimmed)
        && rest.trim().is_empty()
        && ident.eq_ignore_ascii_case("any")
    {
        return Ok(AggregateArgType::AnyPseudo);
    }
    parse_type_name(trimmed)
        .map(AggregateArgType::Type)
        .map_err(|_| {
            ParseError::FeatureNotSupported("named aggregate parameters are not supported".into())
        })
}

fn parse_legacy_aggregate_basetype(input: &str) -> Result<AggregateSignatureKind, ParseError> {
    let trimmed = input.trim();
    if let Some(len) = scan_string_literal_token_len(trimmed)
        && trimmed[len..].trim().is_empty()
    {
        let literal = decode_string_literal(&trimmed[..len])?;
        if literal.eq_ignore_ascii_case("any") {
            return Ok(AggregateSignatureKind::Star);
        }
    }
    if let Ok((ident, rest)) = parse_sql_identifier(trimmed)
        && rest.trim().is_empty()
        && ident.eq_ignore_ascii_case("any")
    {
        return Ok(AggregateSignatureKind::Star);
    }
    Ok(AggregateSignatureKind::Args(vec![
        parse_aggregate_signature_arg(trimmed)?,
    ]))
}

fn parse_aggregate_proc_name(input: &str) -> Result<String, ParseError> {
    let (parts, rest) = parse_qualified_identifier_parts(input)?;
    if !rest.trim().is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "function name",
            actual: input.into(),
        });
    }
    Ok(parts.join("."))
}

fn parse_aggregate_option_text(input: &str) -> Result<String, ParseError> {
    let trimmed = input.trim();
    if let Some(len) = scan_string_literal_token_len(trimmed)
        && trimmed[len..].trim().is_empty()
    {
        return decode_string_literal(&trimmed[..len]);
    }
    Ok(trimmed.to_string())
}

fn parse_aggregate_parallel(input: &str) -> Result<FunctionParallel, ParseError> {
    let (value, rest) = parse_sql_identifier(input)?;
    if !rest.trim().is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "PARALLEL SAFE, RESTRICTED, or UNSAFE",
            actual: input.into(),
        });
    }
    match value.as_str() {
        "safe" => Ok(FunctionParallel::Safe),
        "restricted" => Ok(FunctionParallel::Restricted),
        "unsafe" => Ok(FunctionParallel::Unsafe),
        _ => Err(ParseError::UnexpectedToken {
            expected: "SAFE, RESTRICTED, or UNSAFE",
            actual: value,
        }),
    }
}

fn build_create_trigger_statement(sql: &str) -> Result<CreateTriggerStatement, ParseError> {
    let lowered = sql.to_ascii_lowercase();
    if lowered.starts_with("create constraint trigger") {
        return Err(ParseError::FeatureNotSupported(
            "CONSTRAINT TRIGGER is not supported".into(),
        ));
    }
    let (prefix, replace_existing) = if lowered.starts_with("create or replace trigger") {
        ("create or replace trigger", true)
    } else {
        ("create trigger", false)
    };
    let Some(rest) = sql.get(prefix.len()..) else {
        return Err(ParseError::UnexpectedToken {
            expected: "CREATE TRIGGER name ...",
            actual: sql.into(),
        });
    };
    let (trigger_name, rest) = parse_sql_identifier(rest.trim_start())?;
    let rest = rest.trim_start();
    let (timing, rest) = if keyword_at_start(rest, "before") {
        (TriggerTiming::Before, consume_keyword(rest, "before"))
    } else if keyword_at_start(rest, "after") {
        (TriggerTiming::After, consume_keyword(rest, "after"))
    } else if keyword_at_start(rest, "instead") {
        let next = consume_keyword(rest, "instead").trim_start();
        if !keyword_at_start(next, "of") {
            return Err(ParseError::UnexpectedToken {
                expected: "OF",
                actual: next.into(),
            });
        }
        (TriggerTiming::Instead, consume_keyword(next, "of"))
    } else {
        return Err(ParseError::UnexpectedToken {
            expected: "BEFORE, AFTER, or INSTEAD OF",
            actual: rest.into(),
        });
    };
    let rest = rest.trim_start();
    let on_boundary =
        find_next_top_level_keyword(rest, &["on"]).ok_or_else(|| ParseError::UnexpectedToken {
            expected: "trigger event list followed by ON",
            actual: sql.into(),
        })?;
    let events = parse_trigger_events(rest[..on_boundary].trim())?;
    let mut rest = rest[on_boundary..].trim_start();
    rest = consume_keyword(rest, "on").trim_start();
    let ((schema_name, table_name), mut rest) = parse_schema_qualified_name(rest)?;

    let mut level = TriggerLevel::Statement;
    let mut referencing = Vec::new();
    let mut when_clause_sql = None;
    loop {
        rest = rest.trim_start();
        if rest.is_empty() {
            return Err(ParseError::UnexpectedToken {
                expected: "EXECUTE FUNCTION",
                actual: sql.into(),
            });
        }
        if keyword_at_start(rest, "execute") {
            break;
        }
        if keyword_at_start(rest, "for") {
            let mut next = consume_keyword(rest, "for").trim_start();
            next = consume_keyword(next, "each").trim_start();
            if keyword_at_start(next, "row") {
                level = TriggerLevel::Row;
                rest = consume_keyword(next, "row");
                continue;
            }
            if keyword_at_start(next, "statement") {
                level = TriggerLevel::Statement;
                rest = consume_keyword(next, "statement");
                continue;
            }
            return Err(ParseError::UnexpectedToken {
                expected: "ROW or STATEMENT",
                actual: next.into(),
            });
        }
        if keyword_at_start(rest, "when") {
            let rest_after_when = consume_keyword(rest, "when");
            let (when_sql, next_rest) = take_parenthesized_segment(rest_after_when)?;
            when_clause_sql = Some(when_sql.trim().to_string());
            rest = next_rest;
            continue;
        }
        if keyword_at_start(rest, "referencing") {
            let (parsed, next_rest) = parse_trigger_referencing_clause(rest)?;
            referencing = parsed;
            rest = next_rest;
            continue;
        }
        return Err(ParseError::FeatureNotSupported(format!(
            "unsupported CREATE TRIGGER clause: {}",
            rest.split_whitespace().next().unwrap_or(rest)
        )));
    }

    rest = consume_keyword(rest.trim_start(), "execute").trim_start();
    if keyword_at_start(rest, "function") {
        rest = consume_keyword(rest, "function").trim_start();
    } else if keyword_at_start(rest, "procedure") {
        rest = consume_keyword(rest, "procedure").trim_start();
    } else {
        return Err(ParseError::UnexpectedToken {
            expected: "FUNCTION or PROCEDURE",
            actual: rest.into(),
        });
    }
    let ((function_schema_name, function_name), rest) = parse_qualified_sql_name(rest)?;
    let (args_sql, rest) = take_parenthesized_segment(rest)?;
    if !rest.trim().is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "end of CREATE TRIGGER statement",
            actual: rest.trim().into(),
        });
    }

    Ok(CreateTriggerStatement {
        replace_existing,
        trigger_name,
        schema_name,
        table_name,
        timing,
        level,
        events,
        referencing,
        when_clause_sql,
        function_schema_name,
        function_name,
        func_args: parse_trigger_function_args(&args_sql)?,
    })
}

fn build_alter_table_trigger_state_statement(
    sql: &str,
) -> Result<AlterTableTriggerStateStatement, ParseError> {
    let mut rest = consume_keyword(sql.trim_start(), "alter").trim_start();
    rest = consume_keyword(rest, "table").trim_start();

    let mut if_exists = false;
    if keyword_at_start(rest, "if") {
        rest = consume_keyword(rest, "if").trim_start();
        if !keyword_at_start(rest, "exists") {
            return Err(ParseError::UnexpectedToken {
                expected: "EXISTS",
                actual: rest.into(),
            });
        }
        rest = consume_keyword(rest, "exists").trim_start();
        if_exists = true;
    }

    let mut only = false;
    if keyword_at_start(rest, "only") {
        rest = consume_keyword(rest, "only").trim_start();
        only = true;
    }

    let ((schema_name, table_name), mut rest) = parse_schema_qualified_name(rest)?;
    let table_name = match schema_name {
        Some(schema_name) => format!("{schema_name}.{table_name}"),
        None => table_name,
    };

    let mode = if keyword_at_start(rest.trim_start(), "enable") {
        rest = consume_keyword(rest.trim_start(), "enable").trim_start();
        if keyword_at_start(rest, "always") {
            rest = consume_keyword(rest, "always").trim_start();
            AlterTableTriggerMode::EnableAlways
        } else if keyword_at_start(rest, "replica") {
            rest = consume_keyword(rest, "replica").trim_start();
            AlterTableTriggerMode::EnableReplica
        } else {
            AlterTableTriggerMode::EnableOrigin
        }
    } else if keyword_at_start(rest.trim_start(), "disable") {
        rest = consume_keyword(rest.trim_start(), "disable").trim_start();
        AlterTableTriggerMode::Disable
    } else {
        return Err(ParseError::UnexpectedToken {
            expected: "ENABLE or DISABLE",
            actual: rest.trim_start().into(),
        });
    };

    if !keyword_at_start(rest, "trigger") {
        return Err(ParseError::UnexpectedToken {
            expected: "TRIGGER",
            actual: rest.into(),
        });
    }
    rest = consume_keyword(rest, "trigger").trim_start();

    let (target, rest) = if keyword_at_start(rest, "all") {
        if matches!(
            mode,
            AlterTableTriggerMode::EnableAlways | AlterTableTriggerMode::EnableReplica
        ) {
            return Err(ParseError::UnexpectedToken {
                expected: "trigger name",
                actual: "all".into(),
            });
        }
        (AlterTableTriggerTarget::All, consume_keyword(rest, "all"))
    } else if keyword_at_start(rest, "user") {
        if matches!(
            mode,
            AlterTableTriggerMode::EnableAlways | AlterTableTriggerMode::EnableReplica
        ) {
            return Err(ParseError::UnexpectedToken {
                expected: "trigger name",
                actual: "user".into(),
            });
        }
        (AlterTableTriggerTarget::User, consume_keyword(rest, "user"))
    } else {
        let (name, rest) = parse_sql_identifier(rest)?;
        (AlterTableTriggerTarget::Named(name), rest)
    };

    if !rest.trim().is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "end of ALTER TABLE trigger statement",
            actual: rest.trim().into(),
        });
    }

    Ok(AlterTableTriggerStateStatement {
        if_exists,
        only,
        table_name,
        target,
        mode,
    })
}

fn build_alter_trigger_rename_statement(
    sql: &str,
) -> Result<AlterTriggerRenameStatement, ParseError> {
    let mut rest = consume_keyword(sql.trim_start(), "alter").trim_start();
    rest = consume_keyword(rest, "trigger").trim_start();
    let (trigger_name, next) = parse_sql_identifier(rest)?;
    rest = next.trim_start();
    if !keyword_at_start(rest, "on") {
        return Err(ParseError::UnexpectedToken {
            expected: "ON",
            actual: rest.into(),
        });
    }
    rest = consume_keyword(rest, "on").trim_start();
    let ((schema_name, table_name), next) = parse_schema_qualified_name(rest)?;
    rest = next.trim_start();
    if !keyword_at_start(rest, "rename") {
        return Err(ParseError::UnexpectedToken {
            expected: "RENAME",
            actual: rest.into(),
        });
    }
    rest = consume_keyword(rest, "rename").trim_start();
    if !keyword_at_start(rest, "to") {
        return Err(ParseError::UnexpectedToken {
            expected: "TO",
            actual: rest.into(),
        });
    }
    rest = consume_keyword(rest, "to").trim_start();
    let (new_trigger_name, rest) = parse_sql_identifier(rest)?;
    if !rest.trim().is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "end of ALTER TRIGGER statement",
            actual: rest.trim().into(),
        });
    }
    Ok(AlterTriggerRenameStatement {
        trigger_name,
        schema_name,
        table_name,
        new_trigger_name,
    })
}

fn parse_trigger_events(input: &str) -> Result<Vec<TriggerEventSpec>, ParseError> {
    let mut rest = input.trim();
    let mut events = Vec::new();
    let mut seen = BTreeSet::new();
    while !rest.is_empty() {
        let (event, update_columns, next_rest) = if keyword_at_start(rest, "insert") {
            let next_rest = consume_keyword(rest, "insert");
            if keyword_at_start(next_rest.trim_start(), "of") {
                return Err(ParseError::UnexpectedToken {
                    expected: "trigger event list",
                    actual: "syntax error at or near \"OF\"".into(),
                });
            }
            (TriggerEvent::Insert, Vec::new(), next_rest)
        } else if keyword_at_start(rest, "update") {
            let mut next_rest = consume_keyword(rest, "update").trim_start();
            let update_columns = if keyword_at_start(next_rest, "of") {
                let rest_after_of = consume_keyword(next_rest, "of").trim_start();
                let boundary = find_next_top_level_keyword(rest_after_of, &["or"])
                    .unwrap_or(rest_after_of.len());
                let columns_sql = rest_after_of[..boundary].trim();
                if columns_sql.is_empty() {
                    return Err(ParseError::UnexpectedToken {
                        expected: "column list after UPDATE OF",
                        actual: input.into(),
                    });
                }
                next_rest = &rest_after_of[boundary..];
                parse_identifier_list(columns_sql)?
            } else {
                Vec::new()
            };
            (TriggerEvent::Update, update_columns, next_rest)
        } else if keyword_at_start(rest, "delete") {
            (
                TriggerEvent::Delete,
                Vec::new(),
                consume_keyword(rest, "delete"),
            )
        } else if keyword_at_start(rest, "truncate") {
            (
                TriggerEvent::Truncate,
                Vec::new(),
                consume_keyword(rest, "truncate"),
            )
        } else {
            return Err(ParseError::UnexpectedToken {
                expected: "INSERT, UPDATE, DELETE, or TRUNCATE",
                actual: rest.into(),
            });
        };

        if !seen.insert(event) {
            return Err(ParseError::DetailedError {
                message: "duplicate trigger events specified at or near \"ON\"".into(),
                detail: None,
                hint: None,
                sqlstate: "42601",
            });
        }

        events.push(TriggerEventSpec {
            event,
            update_columns,
        });
        rest = next_rest.trim_start();
        if keyword_at_start(rest, "or") {
            rest = consume_keyword(rest, "or").trim_start();
            continue;
        }
        if !rest.is_empty() {
            return Err(ParseError::UnexpectedToken {
                expected: "OR or end of trigger event list",
                actual: rest.into(),
            });
        }
    }
    if events.is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "trigger event",
            actual: input.into(),
        });
    }
    Ok(events)
}

fn parse_trigger_referencing_clause(
    input: &str,
) -> Result<(Vec<TriggerReferencingSpec>, &str), ParseError> {
    let mut rest = consume_keyword(input.trim_start(), "referencing");
    let mut specs = Vec::new();
    loop {
        rest = rest.trim_start();
        let (is_new, next) = if keyword_at_start(rest, "new") {
            (true, consume_keyword(rest, "new"))
        } else if keyword_at_start(rest, "old") {
            (false, consume_keyword(rest, "old"))
        } else {
            break;
        };
        let next = next.trim_start();
        let (is_table, next) = if keyword_at_start(next, "table") {
            (true, consume_keyword(next, "table"))
        } else if keyword_at_start(next, "row") {
            (false, consume_keyword(next, "row"))
        } else {
            return Err(ParseError::UnexpectedToken {
                expected: "TABLE or ROW",
                actual: next.into(),
            });
        };
        let next = next.trim_start();
        if !keyword_at_start(next, "as") {
            return Err(ParseError::UnexpectedToken {
                expected: "AS",
                actual: next.into(),
            });
        }
        let (name, next) = parse_sql_identifier(consume_keyword(next, "as").trim_start())?;
        specs.push(TriggerReferencingSpec {
            is_new,
            is_table,
            name,
        });
        rest = next;
    }
    if specs.is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "OLD/NEW TABLE or ROW",
            actual: rest.trim_start().into(),
        });
    }
    Ok((specs, rest))
}

fn parse_trigger_function_args(input: &str) -> Result<Vec<String>, ParseError> {
    let items = split_top_level_items(input, ',')?;
    if items.len() == 1 && items[0].trim().is_empty() {
        return Ok(Vec::new());
    }
    items
        .into_iter()
        .filter(|item| !item.trim().is_empty())
        .map(|item| {
            let trimmed = item.trim();
            if let Some(token_len) = scan_string_literal_token_len(trimmed) {
                if token_len == trimmed.len() {
                    return decode_string_literal(trimmed);
                }
            }
            if let Ok((ident, rest)) = parse_sql_identifier(trimmed)
                && rest.trim().is_empty()
            {
                return Ok(ident);
            }
            Ok(trimmed.to_string())
        })
        .collect()
}

fn build_drop_trigger_statement(sql: &str) -> Result<DropTriggerStatement, ParseError> {
    let prefix = "drop trigger";
    let Some(rest) = sql.get(prefix.len()..) else {
        return Err(ParseError::UnexpectedToken {
            expected: "DROP TRIGGER [IF EXISTS] name ON table",
            actual: sql.into(),
        });
    };
    let mut rest = rest.trim_start();
    let mut if_exists = false;
    if keyword_at_start(rest, "if") {
        rest = consume_keyword(rest, "if").trim_start();
        if !keyword_at_start(rest, "exists") {
            return Err(ParseError::UnexpectedToken {
                expected: "IF EXISTS",
                actual: sql.into(),
            });
        }
        rest = consume_keyword(rest, "exists").trim_start();
        if_exists = true;
    }
    let (trigger_name, rest_after_name) = parse_sql_identifier(rest)?;
    let mut rest = rest_after_name.trim_start();
    if !keyword_at_start(rest, "on") {
        return Err(ParseError::UnexpectedToken {
            expected: "ON",
            actual: rest.into(),
        });
    }
    rest = consume_keyword(rest, "on").trim_start();
    let ((schema_name, table_name), rest) = parse_schema_qualified_name(rest)?;
    let rest = rest.trim_start();
    let cascade = if rest.is_empty() {
        false
    } else if keyword_at_start(rest, "cascade") {
        if !consume_keyword(rest, "cascade").trim().is_empty() {
            return Err(ParseError::UnexpectedToken {
                expected: "end of statement",
                actual: rest.into(),
            });
        }
        true
    } else if keyword_at_start(rest, "restrict") {
        if !consume_keyword(rest, "restrict").trim().is_empty() {
            return Err(ParseError::UnexpectedToken {
                expected: "end of statement",
                actual: rest.into(),
            });
        }
        false
    } else {
        return Err(ParseError::UnexpectedToken {
            expected: "CASCADE, RESTRICT, or end of statement",
            actual: rest.into(),
        });
    };
    Ok(DropTriggerStatement {
        if_exists,
        trigger_name,
        schema_name,
        table_name,
        cascade,
    })
}

fn build_create_policy_statement(sql: &str) -> Result<CreatePolicyStatement, ParseError> {
    let prefix = "create policy";
    let Some(rest) = sql.get(prefix.len()..) else {
        return Err(ParseError::UnexpectedToken {
            expected: "CREATE POLICY name ON table",
            actual: sql.into(),
        });
    };
    let mut rest = rest.trim_start();
    let (policy_name, after_name) = parse_sql_identifier(rest)?;
    rest = after_name.trim_start();
    if !keyword_at_start(rest, "on") {
        return Err(ParseError::UnexpectedToken {
            expected: "ON",
            actual: rest.into(),
        });
    }
    rest = consume_keyword(rest, "on").trim_start();
    let ((schema_name, table_name), after_table) = parse_schema_qualified_name(rest)?;
    let _ = schema_name;
    rest = after_table.trim_start();

    let mut permissive = true;
    let mut command = PolicyCommand::All;
    let mut role_names = vec!["public".to_string()];
    let mut using_expr = None;
    let mut using_sql = None;
    let mut with_check_expr = None;
    let mut with_check_sql = None;

    while !rest.is_empty() {
        if keyword_at_start(rest, "as") {
            rest = consume_keyword(rest, "as").trim_start();
            if keyword_at_start(rest, "permissive") {
                permissive = true;
                rest = consume_keyword(rest, "permissive").trim_start();
                continue;
            }
            if keyword_at_start(rest, "restrictive") {
                permissive = false;
                rest = consume_keyword(rest, "restrictive").trim_start();
                continue;
            }
            return Err(ParseError::UnexpectedToken {
                expected: "PERMISSIVE or RESTRICTIVE",
                actual: rest.into(),
            });
        }
        if keyword_at_start(rest, "for") {
            rest = consume_keyword(rest, "for").trim_start();
            let (parsed_command, next_rest) = parse_policy_command(rest)?;
            command = parsed_command;
            rest = next_rest.trim_start();
            continue;
        }
        if keyword_at_start(rest, "to") {
            rest = consume_keyword(rest, "to").trim_start();
            let boundary =
                find_next_top_level_keyword(rest, &["using", "with"]).unwrap_or(rest.len());
            role_names = parse_policy_role_list(&rest[..boundary])?;
            rest = rest[boundary..].trim_start();
            continue;
        }
        if keyword_at_start(rest, "using") {
            rest = consume_keyword(rest, "using");
            let (sql, next_rest) = take_parenthesized_segment(rest)?;
            using_expr = Some(parse_expr(&sql)?);
            using_sql = Some(sql.trim().to_string());
            rest = next_rest.trim_start();
            continue;
        }
        if keyword_at_start(rest, "with") {
            rest = consume_keyword(rest, "with").trim_start();
            if !keyword_at_start(rest, "check") {
                return Err(ParseError::UnexpectedToken {
                    expected: "CHECK",
                    actual: rest.into(),
                });
            }
            rest = consume_keyword(rest, "check");
            let (sql, next_rest) = take_parenthesized_segment(rest)?;
            with_check_expr = Some(parse_expr(&sql)?);
            with_check_sql = Some(sql.trim().to_string());
            rest = next_rest.trim_start();
            continue;
        }
        return Err(ParseError::UnexpectedToken {
            expected: "AS, FOR, TO, USING, or WITH CHECK",
            actual: rest.into(),
        });
    }

    Ok(CreatePolicyStatement {
        policy_name,
        table_name,
        permissive,
        command,
        role_names,
        using_expr,
        using_sql,
        with_check_expr,
        with_check_sql,
    })
}

fn build_alter_policy_statement(sql: &str) -> Result<AlterPolicyStatement, ParseError> {
    let prefix = "alter policy";
    let Some(rest) = sql.get(prefix.len()..) else {
        return Err(ParseError::UnexpectedToken {
            expected: "ALTER POLICY name ON table",
            actual: sql.into(),
        });
    };
    let mut rest = rest.trim_start();
    let (policy_name, after_name) = parse_sql_identifier(rest)?;
    rest = after_name.trim_start();
    if !keyword_at_start(rest, "on") {
        return Err(ParseError::UnexpectedToken {
            expected: "ON",
            actual: rest.into(),
        });
    }
    rest = consume_keyword(rest, "on").trim_start();
    let ((schema_name, table_name), after_table) = parse_schema_qualified_name(rest)?;
    let _ = schema_name;
    rest = after_table.trim_start();

    if keyword_at_start(rest, "rename") {
        rest = consume_keyword(rest, "rename").trim_start();
        if !keyword_at_start(rest, "to") {
            return Err(ParseError::UnexpectedToken {
                expected: "TO",
                actual: rest.into(),
            });
        }
        rest = consume_keyword(rest, "to").trim_start();
        let (new_name, remainder) = parse_sql_identifier(rest)?;
        if !remainder.trim().is_empty() {
            return Err(ParseError::UnexpectedToken {
                expected: "end of statement",
                actual: remainder.trim().into(),
            });
        }
        return Ok(AlterPolicyStatement {
            policy_name,
            table_name,
            action: AlterPolicyAction::Rename { new_name },
        });
    }

    let mut role_names = None;
    let mut using_expr = None;
    let mut using_sql = None;
    let mut with_check_expr = None;
    let mut with_check_sql = None;
    let mut saw_clause = false;

    while !rest.is_empty() {
        if keyword_at_start(rest, "to") {
            rest = consume_keyword(rest, "to").trim_start();
            let boundary =
                find_next_top_level_keyword(rest, &["using", "with"]).unwrap_or(rest.len());
            role_names = Some(parse_policy_role_list(&rest[..boundary])?);
            rest = rest[boundary..].trim_start();
            saw_clause = true;
            continue;
        }
        if keyword_at_start(rest, "using") {
            rest = consume_keyword(rest, "using");
            let (sql, next_rest) = take_parenthesized_segment(rest)?;
            using_expr = Some(parse_expr(&sql)?);
            using_sql = Some(sql.trim().to_string());
            rest = next_rest.trim_start();
            saw_clause = true;
            continue;
        }
        if keyword_at_start(rest, "with") {
            rest = consume_keyword(rest, "with").trim_start();
            if !keyword_at_start(rest, "check") {
                return Err(ParseError::UnexpectedToken {
                    expected: "CHECK",
                    actual: rest.into(),
                });
            }
            rest = consume_keyword(rest, "check");
            let (sql, next_rest) = take_parenthesized_segment(rest)?;
            with_check_expr = Some(parse_expr(&sql)?);
            with_check_sql = Some(sql.trim().to_string());
            rest = next_rest.trim_start();
            saw_clause = true;
            continue;
        }
        return Err(ParseError::UnexpectedToken {
            expected: "RENAME, TO, USING, or WITH CHECK",
            actual: rest.into(),
        });
    }

    if !saw_clause {
        return Err(ParseError::UnexpectedToken {
            expected: "policy alteration clause",
            actual: sql.into(),
        });
    }

    Ok(AlterPolicyStatement {
        policy_name,
        table_name,
        action: AlterPolicyAction::Update {
            role_names,
            using_expr,
            using_sql,
            with_check_expr,
            with_check_sql,
        },
    })
}

fn build_drop_policy_statement(sql: &str) -> Result<DropPolicyStatement, ParseError> {
    let prefix = "drop policy";
    let Some(rest) = sql.get(prefix.len()..) else {
        return Err(ParseError::UnexpectedToken {
            expected: "DROP POLICY [IF EXISTS] name ON table",
            actual: sql.into(),
        });
    };
    let mut rest = rest.trim_start();
    let mut if_exists = false;
    if keyword_at_start(rest, "if") {
        rest = consume_keyword(rest, "if").trim_start();
        if !keyword_at_start(rest, "exists") {
            return Err(ParseError::UnexpectedToken {
                expected: "IF EXISTS",
                actual: sql.into(),
            });
        }
        rest = consume_keyword(rest, "exists").trim_start();
        if_exists = true;
    }
    let (policy_name, after_name) = parse_sql_identifier(rest)?;
    rest = after_name.trim_start();
    if !keyword_at_start(rest, "on") {
        return Err(ParseError::UnexpectedToken {
            expected: "ON",
            actual: rest.into(),
        });
    }
    rest = consume_keyword(rest, "on").trim_start();
    let ((schema_name, table_name), remainder) = parse_schema_qualified_name(rest)?;
    let _ = schema_name;
    if !remainder.trim().is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "end of statement",
            actual: remainder.trim().into(),
        });
    }
    Ok(DropPolicyStatement {
        if_exists,
        policy_name,
        table_name,
    })
}

fn parse_policy_command(input: &str) -> Result<(PolicyCommand, &str), ParseError> {
    if keyword_at_start(input, "all") {
        return Ok((PolicyCommand::All, consume_keyword(input, "all")));
    }
    if keyword_at_start(input, "select") {
        return Ok((PolicyCommand::Select, consume_keyword(input, "select")));
    }
    if keyword_at_start(input, "insert") {
        return Ok((PolicyCommand::Insert, consume_keyword(input, "insert")));
    }
    if keyword_at_start(input, "update") {
        return Ok((PolicyCommand::Update, consume_keyword(input, "update")));
    }
    if keyword_at_start(input, "delete") {
        return Ok((PolicyCommand::Delete, consume_keyword(input, "delete")));
    }
    Err(ParseError::UnexpectedToken {
        expected: "ALL, SELECT, INSERT, UPDATE, or DELETE",
        actual: input.into(),
    })
}

fn parse_policy_role_list(input: &str) -> Result<Vec<String>, ParseError> {
    let items = split_top_level_items(input.trim(), ',')?;
    let mut out = Vec::new();
    for item in items {
        let trimmed = item.trim();
        if trimmed.is_empty() {
            continue;
        }
        let (role_name, rest) = parse_sql_identifier(trimmed)?;
        if !rest.trim().is_empty() {
            return Err(ParseError::UnexpectedToken {
                expected: "role name",
                actual: trimmed.into(),
            });
        }
        out.push(role_name);
    }
    if out.is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "role name",
            actual: input.into(),
        });
    }
    Ok(out)
}

fn parse_qualified_sql_name(input: &str) -> Result<((Option<String>, String), &str), ParseError> {
    let (first, mut rest) = parse_sql_identifier(input)?;
    rest = rest.trim_start();
    if let Some(after_dot) = rest.strip_prefix('.') {
        let (second, rest) = parse_sql_identifier(after_dot.trim_start())?;
        return Ok(((Some(first), second), rest));
    }
    Ok(((None, first), rest))
}

fn build_create_type_statement(sql: &str) -> Result<CreateTypeStatement, ParseError> {
    let prefix = "create type";
    let Some(rest) = sql.get(prefix.len()..) else {
        return Err(ParseError::UnexpectedToken {
            expected: "CREATE TYPE name AS (...)",
            actual: sql.into(),
        });
    };
    let rest = rest.trim_start();
    let ((schema_name, type_name), rest) = parse_qualified_sql_name(rest)?;
    let rest = rest.trim_start();
    if rest.is_empty() {
        return Err(ParseError::FeatureNotSupported(
            "shell types are not supported in CREATE TYPE".into(),
        ));
    }
    if rest.starts_with('(') {
        return Err(ParseError::FeatureNotSupported(
            "base type definitions are not supported in CREATE TYPE".into(),
        ));
    }
    if !keyword_at_start(rest, "as") {
        return Err(ParseError::FeatureNotSupported(
            "unsupported CREATE TYPE form".into(),
        ));
    }
    let rest = consume_keyword(rest, "as").trim_start();
    if keyword_at_start(rest, "enum") {
        let rest = consume_keyword(rest, "enum").trim_start();
        let (label_list, rest) = take_parenthesized_segment(rest)?;
        if !rest.trim().is_empty() {
            return Err(ParseError::UnexpectedToken {
                expected: "end of statement",
                actual: rest.trim().into(),
            });
        }
        return Ok(CreateTypeStatement::Enum(CreateEnumTypeStatement {
            schema_name,
            type_name,
            labels: parse_create_enum_labels(&label_list)?,
        }));
    }
    if keyword_at_start(rest, "range") {
        let rest = consume_keyword(rest, "range").trim_start();
        let (option_list, rest) = take_parenthesized_segment(rest)?;
        if !rest.trim().is_empty() {
            return Err(ParseError::UnexpectedToken {
                expected: "end of statement",
                actual: rest.trim().into(),
            });
        }
        return Ok(CreateTypeStatement::Range(
            parse_create_range_type_statement(schema_name, type_name, &option_list)?,
        ));
    }
    let (attr_list, rest) = take_parenthesized_segment(rest)?;
    if !rest.trim().is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "end of statement",
            actual: rest.trim().into(),
        });
    }
    let attributes = parse_create_type_attributes(&attr_list)?;
    Ok(CreateTypeStatement::Composite(
        CreateCompositeTypeStatement {
            schema_name,
            type_name,
            attributes,
        },
    ))
}

fn build_create_operator_class_statement(
    sql: &str,
) -> Result<CreateOperatorClassStatement, ParseError> {
    let prefix = "create operator class";
    let Some(rest) = sql.get(prefix.len()..) else {
        return Err(ParseError::UnexpectedToken {
            expected: "CREATE OPERATOR CLASS name FOR TYPE ... USING ... AS ...",
            actual: sql.into(),
        });
    };
    let rest = rest.trim_start();
    let ((schema_name, opclass_name), rest) = parse_qualified_sql_name(rest)?;
    let rest = rest.trim_start();
    if !keyword_at_start(rest, "for") {
        return Err(ParseError::UnexpectedToken {
            expected: "FOR TYPE",
            actual: rest.into(),
        });
    }
    let rest = consume_keyword(rest, "for").trim_start();
    if !keyword_at_start(rest, "type") {
        return Err(ParseError::UnexpectedToken {
            expected: "TYPE",
            actual: rest.into(),
        });
    }
    let rest = consume_keyword(rest, "type").trim_start();
    let data_type_end = keyword_boundary(rest, "using").ok_or(ParseError::UnexpectedToken {
        expected: "USING access method",
        actual: rest.into(),
    })?;
    let data_type = parse_type_name(rest[..data_type_end].trim())?;
    let rest = rest[data_type_end..].trim_start();
    let rest = consume_keyword(rest, "using").trim_start();
    let (access_method, rest) = parse_sql_identifier(rest)?;
    let rest = rest.trim_start();
    let mut is_default = false;
    let rest = if keyword_at_start(rest, "default") {
        is_default = true;
        consume_keyword(rest, "default").trim_start()
    } else {
        rest
    };
    if !keyword_at_start(rest, "as") {
        return Err(ParseError::UnexpectedToken {
            expected: "AS",
            actual: rest.into(),
        });
    }
    let items = parse_create_operator_class_items(consume_keyword(rest, "as").trim_start())?;
    Ok(CreateOperatorClassStatement {
        schema_name,
        opclass_name,
        data_type,
        access_method,
        is_default,
        items,
    })
}

fn build_create_operator_statement(sql: &str) -> Result<CreateOperatorStatement, ParseError> {
    let prefix = "create operator";
    let rest = sql
        .get(prefix.len()..)
        .ok_or(ParseError::UnexpectedEof)?
        .trim_start();
    let ((schema_name, operator_name), rest) = parse_operator_name(rest)?;
    let (definition_sql, rest) = take_parenthesized_segment(rest.trim_start())?;
    if !rest.trim().is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "end of CREATE OPERATOR",
            actual: rest.trim().into(),
        });
    }

    let mut left_arg = None;
    let mut right_arg = None;
    let mut procedure = None;
    let mut commutator = None;
    let mut negator = None;
    let mut restrict = None;
    let mut join = None;
    let mut hashes = false;
    let mut merges = false;

    for item in split_top_level_items(&definition_sql, ',')? {
        let trimmed = item.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Some(eq_idx) = trimmed.find('=') else {
            let (option_name, tail) = parse_sql_identifier(trimmed)?;
            if !tail.trim().is_empty() {
                return Err(ParseError::UnexpectedToken {
                    expected: "operator definition option",
                    actual: trimmed.into(),
                });
            }
            match option_name.to_ascii_lowercase().as_str() {
                "hashes" => hashes = true,
                "merges" => merges = true,
                other => {
                    return Err(ParseError::UnexpectedToken {
                        expected: "recognized CREATE OPERATOR option",
                        actual: other.into(),
                    });
                }
            }
            continue;
        };

        let (option_name, tail) = parse_sql_identifier(trimmed[..eq_idx].trim())?;
        if !tail.trim().is_empty() {
            return Err(ParseError::UnexpectedToken {
                expected: "operator option name",
                actual: trimmed[..eq_idx].trim().into(),
            });
        }
        let value = trimmed[eq_idx + 1..].trim();
        match option_name.to_ascii_lowercase().as_str() {
            "procedure" | "function" => {
                let (target, rest) = parse_qualified_name_ref(value, "procedure name")?;
                if !rest.trim().is_empty() {
                    return Err(ParseError::UnexpectedToken {
                        expected: "procedure name",
                        actual: value.into(),
                    });
                }
                procedure = Some(target);
            }
            "leftarg" => left_arg = Some(parse_type_name(value)?),
            "rightarg" => right_arg = Some(parse_type_name(value)?),
            "commutator" => commutator = Some(parse_operator_name_value(value)?),
            "negator" => negator = Some(parse_operator_name_value(value)?),
            "restrict" => {
                if keyword_at_start(value, "none")
                    && consume_keyword(value.trim_start(), "none")
                        .trim()
                        .is_empty()
                {
                    restrict = None;
                } else {
                    let (target, rest) = parse_qualified_name_ref(value, "restriction estimator")?;
                    if !rest.trim().is_empty() {
                        return Err(ParseError::UnexpectedToken {
                            expected: "restriction estimator",
                            actual: value.into(),
                        });
                    }
                    restrict = Some(target);
                }
            }
            "join" => {
                if keyword_at_start(value, "none")
                    && consume_keyword(value.trim_start(), "none")
                        .trim()
                        .is_empty()
                {
                    join = None;
                } else {
                    let (target, rest) = parse_qualified_name_ref(value, "join estimator")?;
                    if !rest.trim().is_empty() {
                        return Err(ParseError::UnexpectedToken {
                            expected: "join estimator",
                            actual: value.into(),
                        });
                    }
                    join = Some(target);
                }
            }
            "hashes" => hashes = parse_operator_bool_value(value)?,
            "merges" => merges = parse_operator_bool_value(value)?,
            other => {
                return Err(ParseError::UnexpectedToken {
                    expected: "recognized CREATE OPERATOR option",
                    actual: other.into(),
                });
            }
        }
    }

    Ok(CreateOperatorStatement {
        schema_name,
        operator_name,
        left_arg,
        right_arg,
        procedure: procedure.ok_or_else(|| ParseError::UnexpectedToken {
            expected: "PROCEDURE option",
            actual: sql.into(),
        })?,
        commutator,
        negator,
        restrict,
        join,
        hashes,
        merges,
    })
}

fn build_alter_operator_statement(sql: &str) -> Result<AlterOperatorStatement, ParseError> {
    let prefix = "alter operator";
    let rest = sql
        .get(prefix.len()..)
        .ok_or(ParseError::UnexpectedEof)?
        .trim_start();
    let ((schema_name, operator_name), rest) = parse_operator_name(rest)?;
    let ((left_arg, right_arg), rest) = parse_operator_argtypes(rest.trim_start())?;
    let rest = rest.trim_start();
    if !keyword_at_start(rest, "set") {
        return Err(ParseError::UnexpectedToken {
            expected: "SET",
            actual: rest.into(),
        });
    }
    let (options_sql, rest) =
        take_parenthesized_segment(consume_keyword(rest, "set").trim_start())?;
    if !rest.trim().is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "end of ALTER OPERATOR",
            actual: rest.trim().into(),
        });
    }

    let mut options = Vec::new();
    for item in split_top_level_items(&options_sql, ',')? {
        let trimmed = item.trim();
        if trimmed.is_empty() {
            continue;
        }
        let (option_name_raw, value) = if let Some(eq_idx) = trimmed.find('=') {
            (trimmed[..eq_idx].trim(), Some(trimmed[eq_idx + 1..].trim()))
        } else {
            (trimmed, None)
        };
        let (option_name, tail) = parse_sql_identifier(option_name_raw)?;
        if !tail.trim().is_empty() {
            return Err(ParseError::UnexpectedToken {
                expected: "operator option name",
                actual: option_name_raw.into(),
            });
        }
        let option_name = option_name.to_ascii_lowercase();
        let option = match option_name.as_str() {
            "restrict" => {
                let function = match value {
                    None => None,
                    Some(value)
                        if keyword_at_start(value, "none")
                            && consume_keyword(value.trim_start(), "none")
                                .trim()
                                .is_empty() =>
                    {
                        None
                    }
                    Some(value) => {
                        let (target, rest) =
                            parse_qualified_name_ref(value, "restriction estimator")?;
                        if !rest.trim().is_empty() {
                            return Err(ParseError::UnexpectedToken {
                                expected: "restriction estimator",
                                actual: value.into(),
                            });
                        }
                        Some(target)
                    }
                };
                AlterOperatorOption::Restrict {
                    option_name,
                    function,
                }
            }
            "join" => {
                let function = match value {
                    None => None,
                    Some(value)
                        if keyword_at_start(value, "none")
                            && consume_keyword(value.trim_start(), "none")
                                .trim()
                                .is_empty() =>
                    {
                        None
                    }
                    Some(value) => {
                        let (target, rest) = parse_qualified_name_ref(value, "join estimator")?;
                        if !rest.trim().is_empty() {
                            return Err(ParseError::UnexpectedToken {
                                expected: "join estimator",
                                actual: value.into(),
                            });
                        }
                        Some(target)
                    }
                };
                AlterOperatorOption::Join {
                    option_name,
                    function,
                }
            }
            "commutator" => match value {
                Some(value) => AlterOperatorOption::Commutator {
                    option_name,
                    operator_name: parse_operator_name_value(value)?,
                },
                None => AlterOperatorOption::Unrecognized {
                    option_name,
                    raw_tokens: Vec::new(),
                },
            },
            "negator" => match value {
                Some(value) => AlterOperatorOption::Negator {
                    option_name,
                    operator_name: parse_operator_name_value(value)?,
                },
                None => AlterOperatorOption::Unrecognized {
                    option_name,
                    raw_tokens: Vec::new(),
                },
            },
            "merges" => AlterOperatorOption::Merges {
                option_name,
                enabled: value
                    .map(parse_operator_bool_value)
                    .transpose()?
                    .unwrap_or(true),
            },
            "hashes" => AlterOperatorOption::Hashes {
                option_name,
                enabled: value
                    .map(parse_operator_bool_value)
                    .transpose()?
                    .unwrap_or(true),
            },
            _ => AlterOperatorOption::Unrecognized {
                option_name,
                raw_tokens: value.into_iter().map(str::to_string).collect(),
            },
        };
        options.push(option);
    }

    Ok(AlterOperatorStatement {
        schema_name,
        operator_name,
        left_arg,
        right_arg,
        options,
    })
}

fn build_drop_operator_statement(sql: &str) -> Result<DropOperatorStatement, ParseError> {
    let prefix = "drop operator";
    let mut rest = sql
        .get(prefix.len()..)
        .ok_or(ParseError::UnexpectedEof)?
        .trim_start();
    let mut if_exists = false;
    if let Some(next) = consume_keywords(rest, &["if", "exists"]) {
        if_exists = true;
        rest = next.trim_start();
    }
    let ((schema_name, operator_name), rest_after_name) = parse_operator_name(rest)?;
    let ((left_arg, right_arg), suffix) = parse_operator_argtypes(rest_after_name.trim_start())?;
    let suffix = suffix.trim();
    if !(suffix.is_empty()
        || keyword_at_start(suffix, "restrict")
        || keyword_at_start(suffix, "cascade"))
    {
        return Err(ParseError::UnexpectedToken {
            expected: "CASCADE, RESTRICT, or end of statement",
            actual: suffix.into(),
        });
    }

    Ok(DropOperatorStatement {
        if_exists,
        schema_name,
        operator_name,
        left_arg,
        right_arg,
    })
}

fn parse_create_operator_class_items(
    input: &str,
) -> Result<Vec<CreateOperatorClassItem>, ParseError> {
    split_top_level_items(input, ',')?
        .into_iter()
        .filter(|item| !item.trim().is_empty())
        .map(|item| parse_create_operator_class_item(&item))
        .collect()
}

fn parse_create_operator_class_item(input: &str) -> Result<CreateOperatorClassItem, ParseError> {
    let trimmed = input.trim();
    if keyword_at_start(trimmed, "operator") {
        let rest = consume_keyword(trimmed, "operator").trim_start();
        let (strategy_number, rest) = parse_smallint(rest, "operator strategy number")?;
        let operator_name = rest.trim();
        if operator_name.is_empty() {
            return Err(ParseError::UnexpectedToken {
                expected: "operator name",
                actual: trimmed.into(),
            });
        }
        return Ok(CreateOperatorClassItem::Operator {
            strategy_number,
            operator_name: operator_name.to_string(),
        });
    }
    if keyword_at_start(trimmed, "function") {
        let rest = consume_keyword(trimmed, "function").trim_start();
        let (support_number, rest) = parse_smallint(rest, "support function number")?;
        let rest = rest.trim_start();
        let ((schema_name, function_name), rest) = parse_qualified_sql_name(rest)?;
        let (arg_types_sql, rest) = take_parenthesized_segment(rest.trim_start())?;
        if !rest.trim().is_empty() {
            return Err(ParseError::UnexpectedToken {
                expected: "end of function item",
                actual: rest.trim().into(),
            });
        }
        let arg_types = if arg_types_sql.trim().is_empty() {
            Vec::new()
        } else {
            split_top_level_items(&arg_types_sql, ',')?
                .into_iter()
                .map(|item| parse_type_name(item.trim()))
                .collect::<Result<Vec<_>, _>>()?
        };
        return Ok(CreateOperatorClassItem::Function {
            support_number,
            schema_name,
            function_name,
            arg_types,
        });
    }
    Err(ParseError::FeatureNotSupported(format!(
        "unsupported CREATE OPERATOR CLASS item: {}",
        trimmed.split_whitespace().next().unwrap_or(trimmed)
    )))
}

fn parse_operator_name(input: &str) -> Result<((Option<String>, String), &str), ParseError> {
    let input = input.trim_start();
    if let Ok((schema_name, rest)) = parse_sql_identifier(input) {
        let rest = rest.trim_start();
        if let Some(after_dot) = rest.strip_prefix('.') {
            let (operator_name, remaining) = parse_operator_token(after_dot)?;
            return Ok(((Some(schema_name), operator_name), remaining));
        }
    }
    let (operator_name, rest) = parse_operator_token(input)?;
    Ok(((None, operator_name), rest))
}

fn parse_operator_token(input: &str) -> Result<(String, &str), ParseError> {
    let input = input.trim_start();
    let token_len = input
        .char_indices()
        .take_while(|(_, ch)| {
            matches!(
                ch,
                '!' | '#'
                    | '%'
                    | '&'
                    | '*'
                    | '+'
                    | '-'
                    | '/'
                    | '<'
                    | '='
                    | '>'
                    | '?'
                    | '@'
                    | '^'
                    | '|'
                    | '~'
            )
        })
        .map(|(idx, ch)| idx + ch.len_utf8())
        .last()
        .unwrap_or(0);
    if token_len == 0 || !is_simple_operator_token(&input[..token_len]) {
        return Err(ParseError::UnexpectedToken {
            expected: "operator name",
            actual: input.into(),
        });
    }
    Ok((input[..token_len].to_string(), &input[token_len..]))
}

fn parse_operator_argtypes(
    input: &str,
) -> Result<((Option<RawTypeName>, Option<RawTypeName>), &str), ParseError> {
    let (args_sql, rest) = take_parenthesized_segment(input)?;
    let args = split_top_level_items(&args_sql, ',')?;
    if args.len() != 2 {
        return Err(ParseError::UnexpectedToken {
            expected: "(leftarg, rightarg)",
            actual: args_sql,
        });
    }
    let left_arg = parse_optional_operator_argtype(&args[0])?;
    let right_arg = parse_optional_operator_argtype(&args[1])?;
    Ok(((left_arg, right_arg), rest))
}

fn parse_optional_operator_argtype(input: &str) -> Result<Option<RawTypeName>, ParseError> {
    let input = input.trim();
    if input.is_empty() {
        return Err(ParseError::UnexpectedEof);
    }
    if keyword_at_start(input, "none") && consume_keyword(input, "none").trim().is_empty() {
        return Ok(None);
    }
    Ok(Some(parse_type_name(input)?))
}

fn parse_qualified_name_ref<'a>(
    input: &'a str,
    expected: &'static str,
) -> Result<(QualifiedNameRef, &'a str), ParseError> {
    let ((schema_name, name), rest) = parse_schema_qualified_name(input)?;
    if name.is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected,
            actual: input.into(),
        });
    }
    Ok((QualifiedNameRef { schema_name, name }, rest))
}

fn parse_operator_name_value(input: &str) -> Result<String, ParseError> {
    let ((schema_name, operator_name), rest) = parse_operator_name(input)?;
    if !rest.trim().is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "operator name",
            actual: input.into(),
        });
    }
    Ok(schema_name
        .map(|schema| format!("{schema}.{operator_name}"))
        .unwrap_or(operator_name))
}

fn parse_operator_bool_value(input: &str) -> Result<bool, ParseError> {
    let (value, rest) = parse_publication_bool_value(input)?;
    if !rest.trim().is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "boolean option value",
            actual: input.into(),
        });
    }
    Ok(value)
}

fn parse_create_type_attributes(input: &str) -> Result<Vec<CompositeTypeAttributeDef>, ParseError> {
    let items = split_top_level_items(input, ',')?;
    if items.len() == 1 && items[0].trim().is_empty() {
        return Ok(Vec::new());
    }
    items
        .into_iter()
        .filter(|item| !item.trim().is_empty())
        .map(|item| parse_create_type_attribute(&item))
        .collect()
}

fn parse_create_enum_labels(input: &str) -> Result<Vec<String>, ParseError> {
    split_top_level_items(input, ',')?
        .into_iter()
        .map(|item| {
            let trimmed = item.trim();
            let token_len =
                scan_string_literal_token_len(trimmed).ok_or(ParseError::UnexpectedToken {
                    expected: "enum label string literal",
                    actual: trimmed.to_string(),
                })?;
            if token_len != trimmed.len() {
                return Err(ParseError::UnexpectedToken {
                    expected: "enum label string literal",
                    actual: trimmed[token_len..].trim().to_string(),
                });
            }
            decode_string_literal(trimmed)
        })
        .collect()
}

fn parse_create_range_type_statement(
    schema_name: Option<String>,
    type_name: String,
    input: &str,
) -> Result<CreateRangeTypeStatement, ParseError> {
    let mut subtype = None;
    let mut subtype_diff = None;
    let mut collation = None;
    let mut multirange_type_name = None;
    for item in split_top_level_items(input, ',')? {
        let item = item.trim();
        if item.is_empty() {
            continue;
        }
        let Some((name, value)) = item.split_once('=') else {
            return Err(ParseError::UnexpectedToken {
                expected: "range option assignment",
                actual: item.into(),
            });
        };
        let option_name = name.trim().to_ascii_lowercase();
        let value = value.trim();
        match option_name.as_str() {
            "subtype" => subtype = Some(parse_type_name(value)?),
            "subtype_diff" => {
                let ((schema_name, function_name), rest) = parse_qualified_sql_name(value)?;
                if !rest.trim().is_empty() {
                    return Err(ParseError::UnexpectedToken {
                        expected: "range function name",
                        actual: value.into(),
                    });
                }
                subtype_diff = Some(match schema_name {
                    Some(schema_name) => format!("{schema_name}.{function_name}"),
                    None => function_name,
                });
            }
            "collation" => {
                let (name, rest) = parse_sql_identifier(value)?;
                if !rest.trim().is_empty() {
                    return Err(ParseError::UnexpectedToken {
                        expected: "collation name",
                        actual: value.into(),
                    });
                }
                collation = Some(name);
            }
            "multirange_type_name" => {
                let (name, rest) = parse_sql_identifier(value)?;
                if !rest.trim().is_empty() {
                    return Err(ParseError::UnexpectedToken {
                        expected: "multirange type name",
                        actual: value.into(),
                    });
                }
                multirange_type_name = Some(name);
            }
            _ => {
                return Err(ParseError::FeatureNotSupported(format!(
                    "CREATE TYPE AS RANGE option {option_name} is not supported yet"
                )));
            }
        }
    }
    Ok(CreateRangeTypeStatement {
        schema_name,
        type_name,
        subtype: subtype.ok_or_else(|| ParseError::UnexpectedToken {
            expected: "subtype option",
            actual: input.trim().into(),
        })?,
        subtype_diff,
        collation,
        multirange_type_name,
    })
}

fn parse_create_type_attribute(input: &str) -> Result<CompositeTypeAttributeDef, ParseError> {
    let trimmed = input.trim();
    let (name, rest) = parse_sql_identifier(trimmed)?;
    let rest = rest.trim_start();
    if rest.is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "composite type attribute type",
            actual: input.into(),
        });
    }
    let lowered = rest.to_ascii_lowercase();
    if lowered.split_whitespace().any(|tok| {
        matches!(
            tok,
            "collate"
                | "constraint"
                | "default"
                | "check"
                | "references"
                | "not"
                | "null"
                | "primary"
                | "unique"
                | "generated"
        )
    }) {
        return Err(ParseError::FeatureNotSupported(
            "CREATE TYPE attributes only support name and type".into(),
        ));
    }
    Ok(CompositeTypeAttributeDef {
        name,
        ty: parse_type_name(rest)?,
    })
}

fn build_drop_type_statement(sql: &str) -> Result<DropTypeStatement, ParseError> {
    let prefix = "drop type";
    let Some(rest) = sql.get(prefix.len()..) else {
        return Err(ParseError::UnexpectedToken {
            expected: "DROP TYPE [IF EXISTS] name",
            actual: sql.into(),
        });
    };
    let mut rest = rest.trim_start();
    let mut if_exists = false;
    if keyword_at_start(rest, "if") {
        rest = consume_keyword(rest, "if").trim_start();
        if !keyword_at_start(rest, "exists") {
            return Err(ParseError::UnexpectedToken {
                expected: "DROP TYPE IF EXISTS name",
                actual: sql.into(),
            });
        }
        rest = consume_keyword(rest, "exists").trim_start();
        if_exists = true;
    }
    let split_at =
        find_next_top_level_keyword(rest, &["cascade", "restrict"]).unwrap_or(rest.len());
    let names_sql = rest[..split_at].trim();
    let suffix = rest[split_at..].trim_start();
    if names_sql.is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "type name",
            actual: sql.into(),
        });
    }
    let type_names = split_top_level_items(names_sql, ',')?
        .into_iter()
        .filter(|item| !item.trim().is_empty())
        .map(|item| {
            let ((schema_name, type_name), trailing) = parse_qualified_sql_name(item.trim())?;
            if !trailing.trim().is_empty() {
                return Err(ParseError::UnexpectedToken {
                    expected: "type name",
                    actual: item,
                });
            }
            Ok(match schema_name {
                Some(schema_name) => format!("{schema_name}.{type_name}"),
                None => type_name,
            })
        })
        .collect::<Result<Vec<_>, ParseError>>()?;
    let cascade = if suffix.is_empty() {
        false
    } else if keyword_at_start(suffix, "cascade") {
        if !consume_keyword(suffix, "cascade").trim().is_empty() {
            return Err(ParseError::UnexpectedToken {
                expected: "end of statement",
                actual: suffix.into(),
            });
        }
        true
    } else if keyword_at_start(suffix, "restrict") {
        if !consume_keyword(suffix, "restrict").trim().is_empty() {
            return Err(ParseError::UnexpectedToken {
                expected: "end of statement",
                actual: suffix.into(),
            });
        }
        false
    } else {
        return Err(ParseError::UnexpectedToken {
            expected: "CASCADE, RESTRICT, or end of statement",
            actual: suffix.into(),
        });
    };
    Ok(DropTypeStatement {
        if_exists,
        type_names,
        cascade,
    })
}

fn parse_create_function_args(input: &str) -> Result<Vec<CreateFunctionArg>, ParseError> {
    let items = split_top_level_items(input, ',')?;
    if items.len() == 1 && items[0].trim().is_empty() {
        return Ok(Vec::new());
    }
    items
        .into_iter()
        .filter(|item| !item.trim().is_empty())
        .map(|item| parse_create_function_arg(&item))
        .collect()
}

fn parse_create_function_arg(input: &str) -> Result<CreateFunctionArg, ParseError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "function argument",
            actual: input.into(),
        });
    }
    let lowered = trimmed.to_ascii_lowercase();
    if lowered.contains(" default ")
        || lowered.contains(":=")
        || lowered.starts_with("variadic ")
        || lowered.contains(" variadic ")
    {
        return Err(ParseError::FeatureNotSupported(
            "default arguments and VARIADIC are not supported in CREATE FUNCTION".into(),
        ));
    }

    let (mode, rest) = if keyword_at_start(trimmed, "inout") {
        (FunctionArgMode::InOut, consume_keyword(trimmed, "inout"))
    } else if keyword_at_start(trimmed, "out") {
        (FunctionArgMode::Out, consume_keyword(trimmed, "out"))
    } else if keyword_at_start(trimmed, "in") {
        (FunctionArgMode::In, consume_keyword(trimmed, "in"))
    } else {
        (FunctionArgMode::In, trimmed)
    };
    let rest = rest.trim_start();
    let (name, type_sql) = match parse_sql_identifier(rest) {
        Ok((name, rest)) if !rest.trim().is_empty() => (Some(name), rest.trim()),
        _ => (None, rest),
    };
    if type_sql.trim().is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "argument type name",
            actual: input.into(),
        });
    }
    Ok(CreateFunctionArg {
        mode,
        name,
        ty: parse_type_name(type_sql.trim())?,
    })
}

fn parse_create_function_returns(
    input: &str,
) -> Result<(CreateFunctionReturnSpec, &str), ParseError> {
    let rest = consume_keyword(input.trim_start(), "returns").trim_start();
    if keyword_at_start(rest, "table") {
        let rest = consume_keyword(rest, "table").trim_start();
        let (columns_sql, rest) = take_parenthesized_segment(rest)?;
        return Ok((
            CreateFunctionReturnSpec::Table(parse_create_function_table_columns(&columns_sql)?),
            rest,
        ));
    }

    let setof = keyword_at_start(rest, "setof");
    let type_rest = if setof {
        consume_keyword(rest, "setof").trim_start()
    } else {
        rest
    };
    let boundary = find_next_top_level_keyword(
        type_rest,
        &[
            "cost",
            "language",
            "as",
            "strict",
            "immutable",
            "stable",
            "volatile",
            "leakproof",
            "parallel",
        ],
    )
    .unwrap_or(type_rest.len());
    let type_sql = type_rest[..boundary].trim();
    if type_sql.is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "return type name",
            actual: input.into(),
        });
    }
    Ok((
        CreateFunctionReturnSpec::Type {
            ty: parse_type_name(type_sql)?,
            setof,
        },
        &type_rest[boundary..],
    ))
}

fn parse_create_function_cost(input: &str) -> Result<(String, &str), ParseError> {
    let rest = consume_keyword(input.trim_start(), "cost").trim_start();
    let (cost_sql, rest) = split_sql_identifier_token(rest)?;
    let cost = cost_sql
        .parse::<f64>()
        .map_err(|_| ParseError::InvalidNumeric(cost_sql.to_string()))?;
    if !cost.is_finite() {
        return Err(ParseError::InvalidNumeric(cost_sql.to_string()));
    }
    Ok((cost_sql.to_string(), rest))
}

fn parse_create_function_table_columns(
    input: &str,
) -> Result<Vec<CreateFunctionTableColumn>, ParseError> {
    split_top_level_items(input, ',')?
        .into_iter()
        .filter(|item| !item.trim().is_empty())
        .map(|item| {
            let (name, rest) = parse_sql_identifier(item.trim())?;
            let type_sql = rest.trim();
            if type_sql.is_empty() {
                return Err(ParseError::UnexpectedToken {
                    expected: "TABLE column type",
                    actual: item.to_string(),
                });
            }
            Ok(CreateFunctionTableColumn {
                name,
                ty: parse_type_name(type_sql)?,
            })
        })
        .collect()
}

fn parse_create_function_language(input: &str) -> Result<(String, &str), ParseError> {
    let rest = consume_keyword(input.trim_start(), "language").trim_start();
    let (language, rest) = parse_sql_identifier(rest)?;
    Ok((language, rest))
}

fn parse_create_function_parallel(
    input: &str,
) -> Result<(crate::backend::parser::FunctionParallel, &str), ParseError> {
    let rest = consume_keyword(input.trim_start(), "parallel").trim_start();
    if keyword_at_start(rest, "safe") {
        return Ok((
            crate::backend::parser::FunctionParallel::Safe,
            consume_keyword(rest, "safe"),
        ));
    }
    if keyword_at_start(rest, "restricted") {
        return Ok((
            crate::backend::parser::FunctionParallel::Restricted,
            consume_keyword(rest, "restricted"),
        ));
    }
    if keyword_at_start(rest, "unsafe") {
        return Ok((
            crate::backend::parser::FunctionParallel::Unsafe,
            consume_keyword(rest, "unsafe"),
        ));
    }
    Err(ParseError::UnexpectedToken {
        expected: "PARALLEL SAFE, RESTRICTED, or UNSAFE",
        actual: input.into(),
    })
}

fn parse_create_function_body(input: &str) -> Result<(String, Option<String>, &str), ParseError> {
    let rest = consume_keyword(input.trim_start(), "as").trim_start();
    let token_len = scan_string_literal_token_len(rest).ok_or(ParseError::UnexpectedToken {
        expected: "function body string literal",
        actual: rest.into(),
    })?;
    let body = decode_string_literal(&rest[..token_len])?;
    let rest = &rest[token_len..];
    let rest = rest.trim_start();
    if let Some(rest) = rest.strip_prefix(',') {
        let rest = rest.trim_start();
        let token_len = scan_string_literal_token_len(rest).ok_or(ParseError::UnexpectedToken {
            expected: "function link symbol string literal",
            actual: rest.into(),
        })?;
        return Ok((
            body,
            Some(decode_string_literal(&rest[..token_len])?),
            &rest[token_len..],
        ));
    }
    Ok((body, None, rest))
}

fn parse_create_function_return_body(input: &str) -> Result<(String, &str), ParseError> {
    let rest = consume_keyword(input.trim_start(), "return").trim_start();
    if rest.is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "RETURN expression",
            actual: input.into(),
        });
    }

    // SQL-function shorthand uses a single expression body.
    let _ = parse_expr(rest)?;
    Ok((format!("select {rest}"), ""))
}

fn parse_sql_identifier(input: &str) -> Result<(String, &str), ParseError> {
    let input = input.trim_start();
    let Some(first) = input.chars().next() else {
        return Err(ParseError::UnexpectedEof);
    };
    if first == '"' {
        let mut i = 1usize;
        let bytes = input.as_bytes();
        while i < bytes.len() {
            if bytes[i] == b'"' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                    i += 2;
                } else {
                    return Ok((input[1..i].replace("\"\"", "\""), &input[i + 1..]));
                }
            } else {
                i += 1;
            }
        }
        return Err(ParseError::UnexpectedEof);
    }
    if !(first.is_ascii_alphabetic() || first == '_') {
        return Err(ParseError::UnexpectedToken {
            expected: "SQL identifier",
            actual: input.into(),
        });
    }
    let mut end = first.len_utf8();
    for ch in input[end..].chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            end += ch.len_utf8();
        } else {
            break;
        }
    }
    Ok((input[..end].to_ascii_lowercase(), &input[end..]))
}

fn split_sql_identifier_token(input: &str) -> Result<(&str, &str), ParseError> {
    let input = input.trim_start();
    if input.is_empty() {
        return Err(ParseError::UnexpectedEof);
    }
    let end = input
        .char_indices()
        .find_map(|(index, ch)| (index > 0 && ch.is_ascii_whitespace()).then_some(index))
        .unwrap_or(input.len());
    Ok((&input[..end], &input[end..]))
}

fn take_parenthesized_segment(input: &str) -> Result<(String, &str), ParseError> {
    let input = input.trim_start();
    if !input.starts_with('(') {
        return Err(ParseError::UnexpectedToken {
            expected: "(",
            actual: input.into(),
        });
    }
    let bytes = input.as_bytes();
    let mut depth = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                i = parse_delimited_token_end(bytes, i, b'\'');
                continue;
            }
            b'"' => {
                i = parse_delimited_token_end(bytes, i, b'"');
                continue;
            }
            b'$' => {
                if let Some(end) = scan_dollar_string_token_end(input, i) {
                    i = end;
                    continue;
                }
            }
            b'(' => depth += 1,
            b')' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Ok((input[1..i].to_string(), &input[i + 1..]));
                }
            }
            _ => {}
        }
        i += 1;
    }
    Err(ParseError::UnexpectedEof)
}

fn split_top_level_items(input: &str, separator: char) -> Result<Vec<String>, ParseError> {
    let mut items = Vec::new();
    let mut start = 0usize;
    let mut depth = 0usize;
    let bytes = input.as_bytes();
    let sep = separator as u8;
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                i = parse_delimited_token_end(bytes, i, b'\'');
                continue;
            }
            b'"' => {
                i = parse_delimited_token_end(bytes, i, b'"');
                continue;
            }
            b'$' => {
                if let Some(end) = scan_dollar_string_token_end(input, i) {
                    i = end;
                    continue;
                }
            }
            b'(' | b'[' => depth += 1,
            b')' | b']' => depth = depth.saturating_sub(1),
            byte if byte == sep && depth == 0 => {
                items.push(input[start..i].trim().to_string());
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    items.push(input[start..].trim().to_string());
    Ok(items)
}

fn find_next_top_level_keyword(input: &str, keywords: &[&str]) -> Option<usize> {
    let bytes = input.as_bytes();
    let mut depth = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                i = parse_delimited_token_end(bytes, i, b'\'');
                continue;
            }
            b'"' => {
                i = parse_delimited_token_end(bytes, i, b'"');
                continue;
            }
            b'$' => {
                if let Some(end) = scan_dollar_string_token_end(input, i) {
                    i = end;
                    continue;
                }
            }
            b'(' | b'[' => depth += 1,
            b')' | b']' => depth = depth.saturating_sub(1),
            _ if depth == 0 => {
                if keywords
                    .iter()
                    .any(|keyword| keyword_at_boundary(input, i, keyword))
                {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

fn keyword_boundary(input: &str, keyword: &str) -> Option<usize> {
    let lowered = input.to_ascii_lowercase();
    let keyword = keyword.to_ascii_lowercase();
    let bytes = lowered.as_bytes();
    let keyword_bytes = keyword.as_bytes();
    let mut depth = 0usize;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut i = 0usize;
    while i < bytes.len() {
        let ch = bytes[i] as char;
        if in_single_quote {
            if ch == '\'' {
                if i + 1 < bytes.len() && bytes[i + 1] as char == '\'' {
                    i += 2;
                    continue;
                }
                in_single_quote = false;
            }
            i += 1;
            continue;
        }
        if in_double_quote {
            if ch == '"' {
                if i + 1 < bytes.len() && bytes[i + 1] as char == '"' {
                    i += 2;
                    continue;
                }
                in_double_quote = false;
            }
            i += 1;
            continue;
        }
        match ch {
            '\'' => in_single_quote = true,
            '"' => in_double_quote = true,
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            _ => {}
        }
        if depth == 0
            && lowered[i..].starts_with(&keyword)
            && (i == 0 || !is_identifier_char(bytes[i.saturating_sub(1)] as char))
        {
            let end = i + keyword_bytes.len();
            if end == bytes.len() || !is_identifier_char(bytes[end] as char) {
                return Some(i);
            }
        }
        i += 1;
    }
    None
}

fn is_identifier_char(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphanumeric()
}

fn parse_smallint<'a>(
    input: &'a str,
    expected: &'static str,
) -> Result<(i16, &'a str), ParseError> {
    let input = input.trim_start();
    let digits = input
        .char_indices()
        .take_while(|(_, ch)| ch.is_ascii_digit())
        .map(|(idx, ch)| idx + ch.len_utf8())
        .last()
        .unwrap_or(0);
    if digits == 0 {
        return Err(ParseError::UnexpectedToken {
            expected,
            actual: input.into(),
        });
    }
    let value = input[..digits]
        .parse::<i16>()
        .map_err(|_| ParseError::UnexpectedToken {
            expected,
            actual: input[..digits].into(),
        })?;
    Ok((value, &input[digits..]))
}

fn scan_string_literal_token_len(input: &str) -> Option<usize> {
    let bytes = input.as_bytes();
    match bytes.first().copied()? {
        b'\'' => Some(parse_delimited_token_end(bytes, 0, b'\'')),
        b'e' | b'E' if bytes.get(1) == Some(&b'\'') => {
            Some(parse_delimited_token_end(bytes, 1, b'\''))
        }
        b'$' => scan_dollar_string_token_end(input, 0),
        _ => None,
    }
}

fn scan_dollar_string_token_end(input: &str, start: usize) -> Option<usize> {
    let suffix = &input[start..];
    if !suffix.starts_with('$') {
        return None;
    }
    let tag_end = suffix[1..].find('$')? + 1;
    let tag = &suffix[..=tag_end];
    let rest = &suffix[tag_end + 1..];
    let closing = rest.find(tag)?;
    Some(start + tag_end + 1 + closing + tag.len())
}

fn keyword_at_start(input: &str, keyword: &str) -> bool {
    keyword_at_boundary(input.trim_start(), 0, keyword)
}

fn consume_keywords<'a>(input: &'a str, keywords: &[&str]) -> Option<&'a str> {
    let mut rest = input.trim_start();
    for keyword in keywords {
        if !keyword_at_start(rest, keyword) {
            return None;
        }
        rest = consume_keyword(rest, keyword).trim_start();
    }
    Some(rest)
}

fn keyword_at_boundary(input: &str, start: usize, keyword: &str) -> bool {
    let end = start.saturating_add(keyword.len());
    input
        .get(start..end)
        .is_some_and(|slice| slice.eq_ignore_ascii_case(keyword))
        && input
            .get(end..)
            .and_then(|slice| slice.chars().next())
            .is_none_or(|ch| !(ch.is_ascii_alphanumeric() || ch == '_'))
}

fn consume_keyword<'a>(input: &'a str, keyword: &str) -> &'a str {
    &input[keyword.len()..]
}

fn build_create_domain_statement(sql: &str) -> Result<CreateDomainStatement, ParseError> {
    let prefix = "create domain ";
    let Some(rest) = sql.get(prefix.len()..) else {
        return Err(ParseError::UnexpectedToken {
            expected: "CREATE DOMAIN name [AS] type",
            actual: sql.into(),
        });
    };
    let rest = rest.trim_start();
    let domain_name_end =
        rest.find(char::is_whitespace)
            .ok_or_else(|| ParseError::UnexpectedToken {
                expected: "domain base type",
                actual: sql.into(),
            })?;
    let domain_name = &rest[..domain_name_end];
    let mut type_sql = rest[domain_name_end..].trim_start();
    if type_sql
        .get(..2)
        .is_some_and(|s| s.eq_ignore_ascii_case("as"))
        && type_sql
            .get(2..3)
            .is_none_or(|s| s.chars().all(char::is_whitespace))
    {
        type_sql = type_sql[2..].trim_start();
    }
    if type_sql.is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "domain base type",
            actual: sql.into(),
        });
    }
    let normalized_type_sql = normalize_domain_type_sql(type_sql);
    if normalized_type_sql.split_whitespace().any(|tok| {
        matches!(
            tok.to_ascii_lowercase().as_str(),
            "constraint"
                | "default"
                | "check"
                | "not"
                | "null"
                | "collate"
                | "references"
                | "unique"
                | "primary"
                | "generated"
                | "deferrable"
                | "no"
        )
    }) {
        return Err(ParseError::FeatureNotSupported(
            "CREATE DOMAIN constraints/defaults are not supported yet".into(),
        ));
    }
    Ok(CreateDomainStatement {
        domain_name: domain_name.to_string(),
        ty: parse_type_name(&normalized_type_sql)?,
    })
}

fn normalize_domain_type_sql(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'[' {
            out.push('[');
            i += 1;
            while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
            }
            while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            if i < bytes.len() && bytes[i] == b']' {
                out.push(']');
                i += 1;
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

fn build_drop_domain_statement(sql: &str) -> Result<DropDomainStatement, ParseError> {
    let tokens = sql.split_whitespace().collect::<Vec<_>>();
    let mut index = 2usize;
    let mut if_exists = false;
    if tokens
        .get(index)
        .is_some_and(|tok| tok.eq_ignore_ascii_case("if"))
    {
        if !tokens
            .get(index + 1)
            .zip(tokens.get(index + 2))
            .is_some_and(|(a, b)| a.eq_ignore_ascii_case("exists") && !b.is_empty())
        {
            return Err(ParseError::UnexpectedToken {
                expected: "DROP DOMAIN [IF EXISTS] name",
                actual: sql.into(),
            });
        }
        if_exists = true;
        index += 2;
    }
    let Some(name) = tokens.get(index) else {
        return Err(ParseError::UnexpectedToken {
            expected: "domain name",
            actual: sql.into(),
        });
    };
    let cascade = tokens
        .get(index + 1)
        .is_some_and(|tok| tok.eq_ignore_ascii_case("cascade"));
    if tokens.len() > index + 1 && !cascade && !tokens[index + 1].eq_ignore_ascii_case("restrict") {
        return Err(ParseError::UnexpectedToken {
            expected: "CASCADE, RESTRICT, or end of statement",
            actual: tokens[index + 1].into(),
        });
    }
    if tokens.len() > index + 2 {
        return Err(ParseError::UnexpectedToken {
            expected: "end of statement",
            actual: sql.into(),
        });
    }
    Ok(DropDomainStatement {
        if_exists,
        domain_name: (*name).to_string(),
        cascade,
    })
}

fn build_comment_on_domain_statement(sql: &str) -> Result<CommentOnDomainStatement, ParseError> {
    let lower = sql.to_ascii_lowercase();
    let Some(is_offset) = lower.find(" is ") else {
        return Err(ParseError::UnexpectedToken {
            expected: "COMMENT ON DOMAIN name IS ...",
            actual: sql.into(),
        });
    };
    let object = sql["comment on domain ".len()..is_offset].trim();
    let value = sql[is_offset + 4..].trim();
    let comment = if value.eq_ignore_ascii_case("null") {
        None
    } else if value.starts_with('\'') && value.ends_with('\'') && value.len() >= 2 {
        Some(value[1..value.len() - 1].replace("''", "'"))
    } else {
        return Err(ParseError::UnexpectedToken {
            expected: "quoted string or NULL",
            actual: value.into(),
        });
    };
    Ok(CommentOnDomainStatement {
        domain_name: object.to_string(),
        comment,
    })
}

fn build_create_conversion_statement(sql: &str) -> Result<CreateConversionStatement, ParseError> {
    let lowered = sql.to_ascii_lowercase();
    let (is_default, prefix) = if lowered.starts_with("create default conversion ") {
        (true, "create default conversion ")
    } else {
        (false, "create conversion ")
    };
    let rest = sql
        .get(prefix.len()..)
        .ok_or(ParseError::UnexpectedEof)?
        .trim_start();
    let ((schema_name, base_name), rest) = parse_schema_qualified_name(rest)?;
    let conversion_name = schema_name
        .map(|schema| format!("{schema}.{base_name}"))
        .unwrap_or(base_name);
    let rest = consume_keyword(rest.trim_start(), "for").trim_start();
    let for_len = scan_string_literal_token_len(rest).ok_or(ParseError::UnexpectedToken {
        expected: "source encoding string literal",
        actual: rest.into(),
    })?;
    let for_encoding = decode_string_literal(&rest[..for_len])?;
    let rest = rest[for_len..].trim_start();
    let rest = consume_keyword(rest, "to").trim_start();
    let to_len = scan_string_literal_token_len(rest).ok_or(ParseError::UnexpectedToken {
        expected: "destination encoding string literal",
        actual: rest.into(),
    })?;
    let to_encoding = decode_string_literal(&rest[..to_len])?;
    let rest = rest[to_len..].trim_start();
    let rest = consume_keyword(rest, "from").trim_start();
    let ((function_schema, function_base), trailing) = parse_schema_qualified_name(rest)?;
    let function_name = function_schema
        .map(|schema| format!("{schema}.{function_base}"))
        .unwrap_or(function_base);
    if !trailing.trim().is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "end of statement",
            actual: trailing.trim().into(),
        });
    }
    Ok(CreateConversionStatement {
        conversion_name,
        for_encoding,
        to_encoding,
        function_name,
        is_default,
    })
}

fn build_drop_conversion_statement(sql: &str) -> Result<DropConversionStatement, ParseError> {
    let tokens = sql.split_whitespace().collect::<Vec<_>>();
    if tokens.len() < 3 {
        return Err(ParseError::UnexpectedEof);
    }
    let mut index = 2usize;
    let mut if_exists = false;
    if tokens
        .get(index)
        .is_some_and(|tok| tok.eq_ignore_ascii_case("if"))
    {
        if !tokens
            .get(index + 1)
            .is_some_and(|tok| tok.eq_ignore_ascii_case("exists"))
        {
            return Err(ParseError::UnexpectedToken {
                expected: "EXISTS",
                actual: tokens.get(index + 1).unwrap_or(&"").to_string(),
            });
        }
        if_exists = true;
        index += 2;
    }
    let Some(name) = tokens.get(index) else {
        return Err(ParseError::UnexpectedToken {
            expected: "conversion name",
            actual: sql.into(),
        });
    };
    let mut cascade = false;
    if let Some(option) = tokens.get(index + 1) {
        if option.eq_ignore_ascii_case("cascade") {
            cascade = true;
        } else if !option.eq_ignore_ascii_case("restrict") {
            return Err(ParseError::UnexpectedToken {
                expected: "CASCADE, RESTRICT, or end of statement",
                actual: (*option).into(),
            });
        }
    }
    if tokens.len() > index + 2 {
        return Err(ParseError::UnexpectedToken {
            expected: "end of statement",
            actual: sql.into(),
        });
    }
    Ok(DropConversionStatement {
        if_exists,
        conversion_name: (*name).to_string(),
        cascade,
    })
}

fn build_comment_on_conversion_statement(
    sql: &str,
) -> Result<CommentOnConversionStatement, ParseError> {
    let lower = sql.to_ascii_lowercase();
    let Some(is_offset) = lower.find(" is ") else {
        return Err(ParseError::UnexpectedToken {
            expected: "COMMENT ON CONVERSION name IS ...",
            actual: sql.into(),
        });
    };
    let object = sql["comment on conversion ".len()..is_offset].trim();
    let value = sql[is_offset + 4..].trim();
    let comment = if value.eq_ignore_ascii_case("null") {
        None
    } else if value.starts_with('\'') && value.ends_with('\'') && value.len() >= 2 {
        Some(value[1..value.len() - 1].replace("''", "'"))
    } else {
        return Err(ParseError::UnexpectedToken {
            expected: "quoted string or NULL",
            actual: value.into(),
        });
    };
    Ok(CommentOnConversionStatement {
        conversion_name: object.to_string(),
        comment,
    })
}

#[cfg(test)]
pub(crate) fn pest_parse_keyword(rule: Rule, input: &str) -> Result<String, ParseError> {
    let mut pairs =
        SqlParser::parse(rule, input).map_err(|e| map_pest_error("keyword", input, e))?;
    Ok(pairs
        .next()
        .ok_or(ParseError::UnexpectedEof)?
        .as_str()
        .to_string())
}

fn map_pest_error(
    expected: &'static str,
    input: &str,
    err: pest::error::Error<Rule>,
) -> ParseError {
    use pest::error::{ErrorVariant, InputLocation};

    match err.variant {
        ErrorVariant::ParsingError { .. } => {
            let token = match err.location {
                InputLocation::Pos(pos) => syntax_error_token_at(input, pos),
                InputLocation::Span((start, _)) => syntax_error_token_at(input, start),
            };
            ParseError::UnexpectedToken {
                expected,
                actual: format!("syntax error at or near \"{token}\""),
            }
        }
        ErrorVariant::CustomError { message } => ParseError::UnexpectedToken {
            expected,
            actual: message,
        },
    }
}

fn syntax_error_token_at(input: &str, pos: usize) -> String {
    if pos >= input.len() {
        return "end of input".into();
    }

    let ch = input[pos..].chars().next().expect("valid utf-8");
    if ch.is_whitespace() {
        return syntax_error_token_at(input, pos + ch.len_utf8());
    }
    if ch == ',' {
        let next = pos + ch.len_utf8();
        if next < input.len() {
            let next_token = syntax_error_token_at(input, next);
            if next_token == ")" || next_token == "]" || next_token == "}" {
                return next_token;
            }
        }
    }
    if ch == '"' || ch == '\'' {
        let mut end = pos + ch.len_utf8();
        while end < input.len() {
            let next = input[end..].chars().next().expect("valid utf-8");
            end += next.len_utf8();
            if next == ch {
                if end < input.len() && input[end..].starts_with(ch) {
                    end += ch.len_utf8();
                    continue;
                }
                break;
            }
        }
        return input[pos..end].to_string();
    }
    if ch.is_ascii_alphanumeric() || ch == '_' {
        let mut end = pos + ch.len_utf8();
        while end < input.len() {
            let next = input[end..].chars().next().expect("valid utf-8");
            if next.is_ascii_alphanumeric() || next == '_' {
                end += next.len_utf8();
            } else {
                break;
            }
        }
        return input[pos..end].to_string();
    }
    ch.to_string()
}

fn validate_unicode_string_literals(sql: &str, options: ParseOptions) -> Result<(), ParseError> {
    let bytes = sql.as_bytes();
    let mut i = 0usize;

    while i < bytes.len() {
        if starts_unicode_string_token(bytes, i) {
            if !options.standard_conforming_strings {
                return Err(ParseError::UnexpectedToken {
                    expected: "string literal without Unicode escapes",
                    actual: "unsafe use of string constant with Unicode escapes".into(),
                });
            }

            let literal_end = parse_delimited_token_end(bytes, i + 2, b'\'');
            i = validate_unicode_uescape_clause(sql, literal_end)?;
            continue;
        }

        let ch = sql[i..].chars().next().expect("valid utf-8");
        i += ch.len_utf8();
    }

    Ok(())
}

fn validate_unicode_uescape_clause(sql: &str, mut i: usize) -> Result<usize, ParseError> {
    let bytes = sql.as_bytes();
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    if !starts_uescape_keyword(bytes, i) {
        return Ok(i);
    }

    i += "UESCAPE".len();
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }

    if i >= bytes.len() || bytes[i] != b'\'' {
        let token_end = scan_unicode_clause_token_end(bytes, i);
        let actual = if i < bytes.len() {
            &sql[i..token_end]
        } else {
            "end of input"
        };
        return Err(ParseError::UnexpectedToken {
            expected: "UESCAPE string literal",
            actual: format!(
                "UESCAPE must be followed by a simple string literal at or near \"{actual}\""
            ),
        });
    }

    let escape_end = parse_delimited_token_end(bytes, i, b'\'');
    let escape_raw = &sql[i..escape_end];
    let escape = decode_string_literal(escape_raw)?;
    let mut chars = escape.chars();
    let Some(ch) = chars.next() else {
        return Err(ParseError::UnexpectedToken {
            expected: "non-empty UESCAPE character",
            actual: "invalid Unicode escape character".into(),
        });
    };
    if chars.next().is_some() || matches!(ch, '+' | '"' | '\'' | ' ' | '\t' | '\n' | '\r') {
        return Err(ParseError::UnexpectedToken {
            expected: "valid UESCAPE character",
            actual: format!("invalid Unicode escape character at or near \"{escape_raw}\""),
        });
    }

    Ok(escape_end)
}

fn starts_unicode_string_token(bytes: &[u8], i: usize) -> bool {
    i + 2 < bytes.len()
        && matches!(bytes[i], b'u' | b'U')
        && bytes[i + 1] == b'&'
        && bytes[i + 2] == b'\''
}

fn starts_uescape_keyword(bytes: &[u8], i: usize) -> bool {
    let keyword = b"uescape";
    if i + keyword.len() > bytes.len() {
        return false;
    }
    if !bytes[i..i + keyword.len()].eq_ignore_ascii_case(keyword) {
        return false;
    }
    let before_ok = i == 0 || !is_identifier_continuation(bytes[i - 1] as char);
    let after_ok = i + keyword.len() == bytes.len()
        || !is_identifier_continuation(bytes[i + keyword.len()] as char);
    before_ok && after_ok
}

fn scan_unicode_clause_token_end(bytes: &[u8], mut i: usize) -> usize {
    while i < bytes.len()
        && !bytes[i].is_ascii_whitespace()
        && !matches!(bytes[i], b';' | b',' | b')')
    {
        i += 1;
    }
    i
}

fn is_identifier_continuation(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

fn parse_delimited_token_end(bytes: &[u8], start: usize, delimiter: u8) -> usize {
    let mut i = start + 1;
    while i < bytes.len() {
        if bytes[i] == delimiter {
            if i + 1 < bytes.len() && bytes[i + 1] == delimiter {
                i += 2;
            } else {
                return i + 1;
            }
        } else {
            i += 1;
        }
    }
    bytes.len()
}

fn build_statement(pair: Pair<'_, Rule>) -> Result<Statement, ParseError> {
    let inner = pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
    match inner.as_rule() {
        Rule::do_stmt => Ok(Statement::Do(build_do(inner)?)),
        Rule::explain_stmt => Ok(Statement::Explain(build_explain(inner)?)),
        Rule::table_stmt => Ok(Statement::Select(build_table_select(inner)?)),
        Rule::select_into_stmt => Ok(Statement::CreateTableAs(build_select_into(inner)?)),
        Rule::select_stmt => Ok(Statement::Select(build_select(inner)?)),
        Rule::values_stmt => Ok(Statement::Values(build_values_statement(inner)?)),
        Rule::copy_stmt => Ok(Statement::CopyFrom(build_copy_from(inner)?)),
        Rule::analyze_stmt => Ok(Statement::Analyze(build_analyze(inner)?)),
        Rule::checkpoint_stmt => Ok(Statement::Checkpoint(CheckpointStatement)),
        Rule::notify_stmt => Ok(Statement::Notify(build_notify(inner)?)),
        Rule::listen_stmt => Ok(Statement::Listen(build_listen(inner)?)),
        Rule::unlisten_stmt => Ok(Statement::Unlisten(build_unlisten(inner)?)),
        Rule::show_stmt => Ok(Statement::Show(build_show(inner)?)),
        Rule::set_session_authorization_stmt => Ok(Statement::SetSessionAuthorization(
            build_set_session_authorization(inner)?,
        )),
        Rule::reset_session_authorization_stmt => Ok(Statement::ResetSessionAuthorization(
            build_reset_session_authorization(inner)?,
        )),
        Rule::set_role_stmt => Ok(Statement::SetRole(build_set_role(inner)?)),
        Rule::reset_role_stmt => Ok(Statement::ResetRole(build_reset_role(inner)?)),
        Rule::set_stmt => Ok(Statement::Set(build_set(inner)?)),
        Rule::reset_stmt => Ok(Statement::Reset(build_reset(inner)?)),
        Rule::create_role_stmt => Ok(Statement::CreateRole(build_create_role(inner)?)),
        Rule::alter_role_stmt => Ok(Statement::AlterRole(build_alter_role(inner)?)),
        Rule::alter_group_stmt => build_alter_group(inner),
        Rule::create_database_stmt => Ok(Statement::CreateDatabase(build_create_database(inner)?)),
        Rule::create_index_stmt => Ok(Statement::CreateIndex(build_create_index(inner)?)),
        Rule::alter_table_add_column_stmt => Ok(Statement::AlterTableAddColumn(
            build_alter_table_add_column(inner)?,
        )),
        Rule::alter_table_add_constraint_stmt => Ok(Statement::AlterTableAddConstraint(
            build_alter_table_add_constraint(inner)?,
        )),
        Rule::alter_table_drop_constraint_stmt => Ok(Statement::AlterTableDropConstraint(
            build_alter_table_drop_constraint(inner)?,
        )),
        Rule::alter_table_alter_constraint_stmt => Ok(Statement::AlterTableAlterConstraint(
            build_alter_table_alter_constraint(inner)?,
        )),
        Rule::alter_table_rename_constraint_stmt => Ok(Statement::AlterTableRenameConstraint(
            build_alter_table_rename_constraint(inner)?,
        )),
        Rule::alter_table_drop_column_stmt => Ok(Statement::AlterTableDropColumn(
            build_alter_table_drop_column(inner)?,
        )),
        Rule::alter_table_alter_column_type_stmt => Ok(Statement::AlterTableAlterColumnType(
            build_alter_table_alter_column_type(inner)?,
        )),
        Rule::alter_table_alter_column_default_stmt => Ok(Statement::AlterTableAlterColumnDefault(
            build_alter_table_alter_column_default(inner)?,
        )),
        Rule::alter_table_alter_column_compression_stmt => {
            Ok(Statement::AlterTableAlterColumnCompression(
                build_alter_table_alter_column_compression(inner)?,
            ))
        }
        Rule::alter_table_alter_column_storage_stmt => Ok(Statement::AlterTableAlterColumnStorage(
            build_alter_table_alter_column_storage(inner)?,
        )),
        Rule::alter_table_alter_column_options_stmt => Ok(Statement::AlterTableAlterColumnOptions(
            build_alter_table_alter_column_options(inner)?,
        )),
        Rule::alter_table_alter_column_statistics_stmt => {
            Ok(Statement::AlterTableAlterColumnStatistics(
                build_alter_table_alter_column_statistics(inner)?,
            ))
        }
        Rule::alter_table_owner_stmt => Ok(Statement::AlterTableOwner(build_alter_relation_owner(
            inner,
        )?)),
        Rule::alter_table_rename_column_stmt => Ok(Statement::AlterTableRenameColumn(
            build_alter_table_rename_column(inner)?,
        )),
        Rule::alter_table_rename_stmt => Ok(Statement::AlterTableRename(build_alter_table_rename(
            inner,
        )?)),
        Rule::alter_view_owner_stmt => Ok(Statement::AlterViewOwner(build_alter_relation_owner(
            inner,
        )?)),
        Rule::alter_schema_owner_stmt => Ok(Statement::AlterSchemaOwner(build_alter_schema_owner(
            inner,
        )?)),
        Rule::alter_table_set_stmt => Ok(Statement::AlterTableSet(build_alter_table_set(inner)?)),
        Rule::alter_table_set_row_security_stmt => Ok(Statement::AlterTableSetRowSecurity(
            build_alter_table_set_row_security(inner)?,
        )),
        Rule::alter_policy_stmt => Ok(Statement::AlterPolicy(build_alter_policy_statement(
            inner.as_str(),
        )?)),
        Rule::alter_table_set_not_null_stmt => Ok(Statement::AlterTableSetNotNull(
            build_alter_table_set_not_null(inner)?,
        )),
        Rule::alter_table_drop_not_null_stmt => Ok(Statement::AlterTableDropNotNull(
            build_alter_table_drop_not_null(inner)?,
        )),
        Rule::alter_table_validate_constraint_stmt => Ok(Statement::AlterTableValidateConstraint(
            build_alter_table_validate_constraint(inner)?,
        )),
        Rule::alter_table_inherit_stmt => Ok(Statement::AlterTableInherit(
            build_alter_table_inherit(inner)?,
        )),
        Rule::alter_table_no_inherit_stmt => Ok(Statement::AlterTableNoInherit(
            build_alter_table_no_inherit(inner)?,
        )),
        Rule::comment_on_role_stmt => Ok(Statement::CommentOnRole(build_comment_on_role(inner)?)),
        Rule::comment_on_table_stmt => {
            Ok(Statement::CommentOnTable(build_comment_on_table(inner)?))
        }
        Rule::comment_on_index_stmt => {
            Ok(Statement::CommentOnIndex(build_comment_on_index(inner)?))
        }
        Rule::comment_on_rule_stmt => Ok(Statement::CommentOnRule(build_comment_on_rule(inner)?)),
        Rule::comment_on_trigger_stmt => Ok(Statement::CommentOnTrigger(build_comment_on_trigger(
            inner,
        )?)),
        Rule::create_schema_stmt => Ok(Statement::CreateSchema(build_create_schema(inner)?)),
        Rule::create_policy_stmt => Ok(Statement::CreatePolicy(build_create_policy_statement(
            inner.as_str(),
        )?)),
        Rule::create_table_stmt => build_create_table(inner),
        Rule::create_view_stmt => Ok(Statement::CreateView(build_create_view(inner)?)),
        Rule::create_rule_stmt => Ok(Statement::CreateRule(build_create_rule(inner)?)),
        Rule::drop_role_stmt => Ok(Statement::DropRole(build_drop_role(inner)?)),
        Rule::drop_database_stmt => Ok(Statement::DropDatabase(build_drop_database(inner)?)),
        Rule::drop_table_stmt => Ok(Statement::DropTable(build_drop_table(inner)?)),
        Rule::drop_index_stmt => Ok(Statement::DropIndex(build_drop_index(inner)?)),
        Rule::drop_view_stmt => Ok(Statement::DropView(build_drop_view(inner)?)),
        Rule::drop_rule_stmt => Ok(Statement::DropRule(build_drop_rule(inner)?)),
        Rule::drop_policy_stmt => Ok(Statement::DropPolicy(build_drop_policy_statement(
            inner.as_str(),
        )?)),
        Rule::drop_schema_stmt => Ok(Statement::DropSchema(build_drop_schema(inner)?)),
        Rule::drop_owned_stmt => Ok(Statement::DropOwned(build_drop_owned(inner)?)),
        Rule::reassign_owned_stmt => Ok(Statement::ReassignOwned(build_reassign_owned(inner)?)),
        Rule::truncate_table_stmt => Ok(Statement::TruncateTable(build_truncate_table(inner)?)),
        Rule::vacuum_stmt => Ok(Statement::Vacuum(build_vacuum(inner)?)),
        Rule::insert_stmt => Ok(Statement::Insert(build_insert(inner)?)),
        Rule::merge_stmt => Ok(Statement::Merge(build_merge(inner)?)),
        Rule::update_stmt => Ok(Statement::Update(build_update(inner)?)),
        Rule::delete_stmt => Ok(Statement::Delete(build_delete(inner)?)),
        Rule::begin_stmt => Ok(Statement::Begin),
        Rule::commit_stmt => Ok(Statement::Commit),
        Rule::rollback_stmt => Ok(Statement::Rollback),
        _ => Err(ParseError::UnexpectedToken {
            expected: "statement",
            actual: inner.as_str().into(),
        }),
    }
}

fn build_table_select(pair: Pair<'_, Rule>) -> Result<SelectStatement, ParseError> {
    let name = pair
        .into_inner()
        .find(|part| part.as_rule() == Rule::identifier)
        .map(build_identifier)
        .ok_or(ParseError::UnexpectedEof)?;
    Ok(SelectStatement {
        with: Vec::new(),
        with_recursive: false,
        distinct: false,
        from: Some(FromItem::Table { name, only: false }),
        targets: vec![SelectItem {
            expr: SqlExpr::Column("*".into()),
            output_name: "*".into(),
        }],
        where_clause: None,
        group_by: Vec::new(),
        having: None,
        window_clauses: Vec::new(),
        order_by: Vec::new(),
        limit: None,
        offset: None,
        locking_clause: None,
        set_operation: None,
    })
}

fn build_do(pair: Pair<'_, Rule>) -> Result<DoStatement, ParseError> {
    let mut language = None;
    let mut code = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::do_body => code = Some(decode_string_literal_pair(part)?),
            Rule::do_language_clause => {
                let ident = part
                    .into_inner()
                    .find(|inner| inner.as_rule() == Rule::identifier)
                    .ok_or(ParseError::UnexpectedEof)?;
                language = Some(build_identifier(ident));
            }
            _ => {}
        }
    }
    Ok(DoStatement {
        language,
        code: code.ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_analyze(pair: Pair<'_, Rule>) -> Result<AnalyzeStatement, ParseError> {
    let mut targets = Vec::new();
    let mut verbose = false;
    let mut skip_locked = false;
    let mut buffer_usage_limit = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::analyze_option_block => {
                let opts = build_analyze_options(part)?;
                verbose = opts.verbose;
                skip_locked = opts.skip_locked;
                buffer_usage_limit = opts.buffer_usage_limit;
            }
            Rule::maintenance_target_list => targets = build_maintenance_target_list(part)?,
            _ => {}
        }
    }
    Ok(AnalyzeStatement {
        targets,
        verbose,
        skip_locked,
        buffer_usage_limit,
    })
}

#[derive(Default)]
struct AnalyzeOptionsBuilder {
    verbose: bool,
    skip_locked: bool,
    buffer_usage_limit: Option<String>,
}

fn build_analyze_options(pair: Pair<'_, Rule>) -> Result<AnalyzeOptionsBuilder, ParseError> {
    let mut options = AnalyzeOptionsBuilder::default();
    for part in pair.into_inner() {
        let part = if part.as_rule() == Rule::analyze_option {
            part.into_inner().next().ok_or(ParseError::UnexpectedEof)?
        } else {
            part
        };
        match part.as_rule() {
            Rule::analyze_verbose_option => {
                options.verbose = parse_option_bool(part)?;
            }
            Rule::analyze_skip_locked_option => {
                options.skip_locked = parse_option_bool(part)?;
            }
            Rule::analyze_buffer_usage_limit_option => {
                options.buffer_usage_limit = Some(parse_option_scalar(part)?);
            }
            _ => {}
        }
    }
    Ok(options)
}

fn parse_option_bool(pair: Pair<'_, Rule>) -> Result<bool, ParseError> {
    let mut inner = pair.into_inner();
    match inner.next() {
        None => Ok(true),
        Some(part) if part.as_rule() == Rule::option_bool_value => {
            let value = part.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
            Ok(!matches!(value.as_rule(), Rule::kw_false | Rule::kw_off))
        }
        Some(_) => Ok(true),
    }
}

fn parse_option_scalar(pair: Pair<'_, Rule>) -> Result<String, ParseError> {
    let scalar = pair
        .into_inner()
        .find(|part| part.as_rule() == Rule::option_scalar_value)
        .ok_or(ParseError::UnexpectedEof)?;
    build_option_scalar_value(scalar)
}

fn build_option_scalar_value(pair: Pair<'_, Rule>) -> Result<String, ParseError> {
    let pair = pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
    Ok(match pair.as_rule() {
        Rule::quoted_string_literal
        | Rule::string_literal
        | Rule::unicode_string_literal
        | Rule::escape_string_literal
        | Rule::dollar_string_literal => decode_string_literal_pair(pair)?,
        Rule::option_bool_value => {
            let inner = pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
            inner.as_str().to_string()
        }
        _ => pair.as_str().to_string(),
    })
}

fn build_set(pair: Pair<'_, Rule>) -> Result<SetStatement, ParseError> {
    let mut is_local = false;
    let mut name = None;
    let mut value = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::set_scope => is_local = part.as_str().eq_ignore_ascii_case("local"),
            Rule::set_standard_clause
            | Rule::set_time_zone_clause
            | Rule::set_xml_option_clause => {
                for clause_part in part.into_inner() {
                    match clause_part.as_rule() {
                        Rule::identifier | Rule::time_zone_guc_name if name.is_none() => {
                            name = Some(build_set_guc_name(clause_part));
                        }
                        Rule::kw_xml if name.is_none() => name = Some("xmloption".to_string()),
                        Rule::kw_document if value.is_none() => {
                            value = Some("DOCUMENT".to_string())
                        }
                        Rule::kw_content if value.is_none() => value = Some("CONTENT".to_string()),
                        Rule::set_value_list => value = Some(build_set_value_list(clause_part)),
                        _ => {}
                    }
                }
            }
            Rule::set_value_list => value = Some(build_set_value_list(part)),
            _ => {}
        }
    }
    Ok(SetStatement {
        name: name.ok_or(ParseError::UnexpectedEof)?,
        value: value.ok_or(ParseError::UnexpectedEof)?,
        is_local,
    })
}

fn build_set_guc_name(pair: Pair<'_, Rule>) -> String {
    match pair.as_rule() {
        Rule::time_zone_guc_name => "timezone".to_string(),
        Rule::identifier => build_identifier(pair),
        _ => pair.as_str().to_ascii_lowercase(),
    }
}

fn build_show(pair: Pair<'_, Rule>) -> Result<ShowStatement, ParseError> {
    let mut name = None;
    for part in pair.into_inner() {
        if part.as_rule() == Rule::identifier {
            name = Some(build_identifier(part));
        }
    }
    let name = name.ok_or(ParseError::UnexpectedEof)?;
    if name.eq_ignore_ascii_case("tables") {
        return Err(ParseError::UnexpectedToken {
            expected: "configuration parameter",
            actual: name,
        });
    }
    Ok(ShowStatement { name })
}

fn build_notify(pair: Pair<'_, Rule>) -> Result<NotifyStatement, ParseError> {
    let mut channel = None;
    let mut payload = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier => channel = Some(build_identifier(part)),
            Rule::notify_payload => {
                let payload_value = part
                    .into_inner()
                    .find(|inner| {
                        matches!(
                            inner.as_rule(),
                            Rule::quoted_string_literal
                                | Rule::string_literal
                                | Rule::escape_string_literal
                                | Rule::unicode_string_literal
                                | Rule::dollar_string_literal
                        )
                    })
                    .ok_or(ParseError::UnexpectedEof)?;
                payload = Some(decode_string_literal_pair(payload_value)?);
            }
            _ => {}
        }
    }
    Ok(NotifyStatement {
        channel: channel.ok_or(ParseError::UnexpectedEof)?,
        payload,
    })
}

fn build_listen(pair: Pair<'_, Rule>) -> Result<ListenStatement, ParseError> {
    let channel = pair
        .into_inner()
        .find(|part| part.as_rule() == Rule::identifier)
        .map(build_identifier)
        .ok_or(ParseError::UnexpectedEof)?;
    Ok(ListenStatement { channel })
}

fn build_unlisten(pair: Pair<'_, Rule>) -> Result<UnlistenStatement, ParseError> {
    let text = pair.as_str().trim();
    let mut channel = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier => channel = Some(build_identifier(part)),
            _ => {}
        }
    }
    if channel.is_none() && !text.ends_with('*') {
        return Err(ParseError::UnexpectedEof);
    }
    Ok(UnlistenStatement { channel })
}

fn build_set_session_authorization(
    pair: Pair<'_, Rule>,
) -> Result<SetSessionAuthorizationStatement, ParseError> {
    let role_name = pair
        .into_inner()
        .find_map(|part| match part.as_rule() {
            Rule::identifier => Some(Ok(build_identifier(part))),
            Rule::quoted_string_literal => Some(decode_string_literal_pair(part)),
            Rule::session_authorization_target => {
                let target = part.into_inner().next()?;
                Some(match target.as_rule() {
                    Rule::identifier => Ok(build_identifier(target)),
                    Rule::quoted_string_literal
                    | Rule::string_literal
                    | Rule::escape_string_literal
                    | Rule::unicode_string_literal
                    | Rule::dollar_string_literal => decode_string_literal_pair(target),
                    _ => Err(ParseError::UnexpectedToken {
                        expected: "session authorization role name",
                        actual: format!("{:?}", target.as_rule()),
                    }),
                })
            }
            _ => None,
        })
        .transpose()?
        .ok_or(ParseError::UnexpectedEof)?;
    Ok(SetSessionAuthorizationStatement { role_name })
}

fn build_reset_session_authorization(
    _pair: Pair<'_, Rule>,
) -> Result<ResetSessionAuthorizationStatement, ParseError> {
    Ok(ResetSessionAuthorizationStatement)
}

fn build_set_role(pair: Pair<'_, Rule>) -> Result<SetRoleStatement, ParseError> {
    let role_name = pair
        .into_inner()
        .find(|part| part.as_rule() == Rule::identifier)
        .map(build_identifier);
    Ok(SetRoleStatement { role_name })
}

fn build_reset_role(_pair: Pair<'_, Rule>) -> Result<ResetRoleStatement, ParseError> {
    Ok(ResetRoleStatement)
}

fn build_reset(pair: Pair<'_, Rule>) -> Result<ResetStatement, ParseError> {
    let mut name = None;
    for part in pair.into_inner() {
        if part.as_rule() == Rule::identifier {
            name = Some(build_identifier(part));
        }
    }
    Ok(ResetStatement { name })
}

fn build_create_role(pair: Pair<'_, Rule>) -> Result<CreateRoleStatement, ParseError> {
    let is_user = pair
        .as_str()
        .to_ascii_lowercase()
        .starts_with("create user ");
    let mut role_name = None;
    let mut options = Vec::new();

    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier if role_name.is_none() => role_name = Some(build_identifier(part)),
            Rule::role_option => options.push(build_role_option(part)?),
            _ => {}
        }
    }

    Ok(CreateRoleStatement {
        role_name: role_name.ok_or(ParseError::UnexpectedEof)?,
        is_user,
        options,
    })
}

fn build_alter_role(pair: Pair<'_, Rule>) -> Result<AlterRoleStatement, ParseError> {
    let mut role_name = None;
    let mut rename_to = None;
    let mut options = Vec::new();

    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier if role_name.is_none() => role_name = Some(build_identifier(part)),
            Rule::alter_role_rename_clause => {
                rename_to = Some(build_alter_role_rename(part)?);
            }
            Rule::alter_role_option => options.push(build_role_option(part)?),
            _ => {}
        }
    }

    let action = if let Some(new_name) = rename_to {
        AlterRoleAction::Rename { new_name }
    } else {
        AlterRoleAction::Options(options)
    };

    Ok(AlterRoleStatement {
        role_name: role_name.ok_or(ParseError::UnexpectedEof)?,
        action,
    })
}

fn build_alter_group(pair: Pair<'_, Rule>) -> Result<Statement, ParseError> {
    let mut is_add = None;
    let mut role_name = None;
    let mut grantee_names = Vec::new();

    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier if role_name.is_none() => role_name = Some(build_identifier(part)),
            Rule::alter_group_action => {
                is_add = Some(
                    part.as_str()
                        .trim_start()
                        .to_ascii_lowercase()
                        .starts_with("add"),
                );
                for inner in part.into_inner() {
                    match inner.as_rule() {
                        Rule::ident_list => {
                            grantee_names.extend(inner.into_inner().map(build_identifier));
                        }
                        Rule::identifier => grantee_names.push(build_identifier(inner)),
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }

    let role_name = role_name.ok_or(ParseError::UnexpectedEof)?;
    let is_add = is_add.ok_or(ParseError::UnexpectedEof)?;
    if grantee_names.is_empty() {
        return Err(ParseError::UnexpectedEof);
    }
    if is_add {
        Ok(Statement::GrantRoleMembership(
            GrantRoleMembershipStatement {
                role_names: vec![role_name],
                grantee_names,
                admin_option: false,
                inherit_option: None,
                set_option: None,
                granted_by: None,
                legacy_group_syntax: true,
            },
        ))
    } else {
        Ok(Statement::RevokeRoleMembership(
            RevokeRoleMembershipStatement {
                role_names: vec![role_name],
                grantee_names,
                revoke_membership: true,
                admin_option: false,
                inherit_option: false,
                set_option: false,
                cascade: false,
                granted_by: None,
                legacy_group_syntax: true,
            },
        ))
    }
}

fn build_alter_role_rename(pair: Pair<'_, Rule>) -> Result<String, ParseError> {
    pair.into_inner()
        .find(|part| part.as_rule() == Rule::identifier)
        .map(build_identifier)
        .ok_or(ParseError::UnexpectedEof)
}

fn build_create_database(pair: Pair<'_, Rule>) -> Result<CreateDatabaseStatement, ParseError> {
    let mut database_name = None;
    let mut has_options = false;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier if database_name.is_none() => {
                database_name = Some(build_identifier(part))
            }
            Rule::create_database_options => has_options = true,
            _ => {}
        }
    }
    if has_options {
        return Err(ParseError::FeatureNotSupported(
            "CREATE DATABASE options".into(),
        ));
    }
    Ok(CreateDatabaseStatement {
        database_name: database_name.ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_role_option(pair: Pair<'_, Rule>) -> Result<RoleOption, ParseError> {
    let inner = pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
    match inner.as_rule() {
        Rule::alter_role_option => {
            let nested = inner.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
            build_role_option_from_rule(nested)
        }
        _ => build_role_option_from_rule(inner),
    }
}

fn build_role_option_from_rule(pair: Pair<'_, Rule>) -> Result<RoleOption, ParseError> {
    match pair.as_rule() {
        Rule::role_attr_option => build_role_attr_option(pair),
        Rule::role_connection_limit_option => {
            let limit = pair
                .into_inner()
                .find(|part| part.as_rule() == Rule::signed_integer)
                .ok_or(ParseError::UnexpectedEof)?;
            Ok(RoleOption::ConnectionLimit(parse_i32(limit)?))
        }
        Rule::role_password_option => {
            let mut value = None;
            for part in pair.into_inner() {
                match part.as_rule() {
                    Rule::quoted_string_literal
                    | Rule::string_literal
                    | Rule::unicode_string_literal
                    | Rule::escape_string_literal
                    | Rule::dollar_string_literal => {
                        value = Some(Some(decode_string_literal_pair(part)?))
                    }
                    Rule::kw_null => value = Some(None),
                    _ => {}
                }
            }
            Ok(RoleOption::Password(
                value.ok_or(ParseError::UnexpectedEof)?,
            ))
        }
        Rule::role_encrypted_password_option => {
            let value = pair
                .into_inner()
                .find(|part| {
                    matches!(
                        part.as_rule(),
                        Rule::quoted_string_literal
                            | Rule::string_literal
                            | Rule::unicode_string_literal
                            | Rule::escape_string_literal
                            | Rule::dollar_string_literal
                    )
                })
                .ok_or(ParseError::UnexpectedEof)?;
            Ok(RoleOption::EncryptedPassword(decode_string_literal_pair(
                value,
            )?))
        }
        Rule::role_membership_option => build_role_membership_option(pair),
        Rule::role_sysid_option => {
            let value = pair
                .into_inner()
                .find(|part| part.as_rule() == Rule::integer)
                .ok_or(ParseError::UnexpectedEof)?;
            Ok(RoleOption::Sysid(parse_i32(value)?))
        }
        _ => Err(ParseError::UnexpectedToken {
            expected: "role option",
            actual: pair.as_str().to_string(),
        }),
    }
}

fn build_role_attr_option(pair: Pair<'_, Rule>) -> Result<RoleOption, ParseError> {
    let attr = pair.as_str().to_ascii_lowercase();
    Ok(match attr.as_str() {
        "superuser" => RoleOption::Superuser(true),
        "nosuperuser" => RoleOption::Superuser(false),
        "createdb" => RoleOption::CreateDb(true),
        "nocreatedb" => RoleOption::CreateDb(false),
        "createrole" => RoleOption::CreateRole(true),
        "nocreaterole" => RoleOption::CreateRole(false),
        "inherit" => RoleOption::Inherit(true),
        "noinherit" => RoleOption::Inherit(false),
        "login" => RoleOption::Login(true),
        "nologin" => RoleOption::Login(false),
        "replication" => RoleOption::Replication(true),
        "noreplication" => RoleOption::Replication(false),
        "bypassrls" => RoleOption::BypassRls(true),
        "nobypassrls" => RoleOption::BypassRls(false),
        _ => {
            return Err(ParseError::UnexpectedToken {
                expected: "role attribute option",
                actual: attr,
            });
        }
    })
}

fn build_role_membership_option(pair: Pair<'_, Rule>) -> Result<RoleOption, ParseError> {
    let actual = pair.as_str().to_string();
    let trimmed = actual.trim_start();
    let option_name = if strip_keyword_prefix(trimmed, "in role").is_some() {
        Some("in role")
    } else if strip_keyword_prefix(trimmed, "role").is_some() {
        Some("role")
    } else if strip_keyword_prefix(trimmed, "admin").is_some() {
        Some("admin")
    } else if strip_keyword_prefix(trimmed, "user").is_some() {
        Some("user")
    } else {
        None
    };
    let mut roles = Vec::new();

    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::ident_list => roles.extend(part.into_inner().map(build_identifier)),
            Rule::identifier => roles.push(build_identifier(part)),
            _ => {}
        }
    }

    Ok(
        match option_name.ok_or_else(|| ParseError::UnexpectedToken {
            expected: "role membership option",
            actual: actual.clone(),
        })? {
            "role" | "user" => RoleOption::Role(roles),
            "admin" => RoleOption::Admin(roles),
            "in role" => RoleOption::InRole(roles),
            _ => unreachable!(),
        },
    )
}

fn build_set_value_list(pair: Pair<'_, Rule>) -> String {
    pair.into_inner()
        .filter(|part| part.as_rule() == Rule::set_value_atom)
        .map(build_simple_set_value_atom)
        .collect::<Vec<_>>()
        .join(", ")
}

fn build_simple_set_value_atom(pair: Pair<'_, Rule>) -> String {
    let pair = pair.clone().into_inner().next().unwrap_or(pair);
    match pair.as_rule() {
        Rule::signed_set_value => pair.as_str().to_string(),
        Rule::quoted_string_literal
        | Rule::string_literal
        | Rule::unicode_string_literal
        | Rule::escape_string_literal
        | Rule::dollar_string_literal => decode_string_literal_pair(pair).unwrap_or_default(),
        Rule::kw_true => "true".to_string(),
        Rule::kw_false => "false".to_string(),
        Rule::kw_on_value => "on".to_string(),
        Rule::kw_off => "off".to_string(),
        Rule::kw_default => "default".to_string(),
        Rule::identifier | Rule::numeric_literal | Rule::integer => pair.as_str().to_string(),
        _ => pair.as_str().to_string(),
    }
}

fn build_explain(pair: Pair<'_, Rule>) -> Result<ExplainStatement, ParseError> {
    let mut analyze = false;
    let mut buffers = false;
    let mut costs = true;
    let mut timing = true;
    let mut verbose = false;
    let mut statement = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::kw_analyze => analyze = true,
            Rule::explain_option => {
                let mut name_rule = None;
                let mut bool_val = true;
                for child in part.into_inner() {
                    match child.as_rule() {
                        Rule::explain_option_name => {
                            name_rule = child.into_inner().next().map(|r| r.as_rule());
                        }
                        Rule::explain_option_value => {
                            let val = child.into_inner().next();
                            if let Some(v) = val {
                                match v.as_rule() {
                                    Rule::kw_off | Rule::kw_false => bool_val = false,
                                    _ => bool_val = true,
                                }
                            }
                        }
                        _ => {}
                    }
                }
                match name_rule {
                    Some(Rule::kw_analyze) => analyze = bool_val,
                    Some(Rule::kw_buffers) => buffers = bool_val,
                    Some(Rule::kw_costs) => costs = bool_val,
                    Some(Rule::kw_timing) => timing = bool_val,
                    Some(Rule::kw_verbose) => verbose = bool_val,
                    _ => {} // SUMMARY, FORMAT: parsed but ignored
                }
            }
            Rule::select_stmt => statement = Some(Statement::Select(build_select(part)?)),
            Rule::insert_stmt => statement = Some(Statement::Insert(build_insert(part)?)),
            Rule::merge_stmt => statement = Some(Statement::Merge(build_merge(part)?)),
            Rule::update_stmt => statement = Some(Statement::Update(build_update(part)?)),
            Rule::delete_stmt => statement = Some(Statement::Delete(build_delete(part)?)),
            _ => {}
        }
    }
    Ok(ExplainStatement {
        analyze,
        buffers,
        costs,
        timing,
        verbose,
        statement: Box::new(statement.ok_or(ParseError::UnexpectedEof)?),
    })
}

pub(crate) fn build_select(pair: Pair<'_, Rule>) -> Result<SelectStatement, ParseError> {
    let raw = pair.as_str().to_string();
    let parts: Vec<Pair<'_, Rule>> = match pair.as_rule() {
        Rule::select_stmt => {
            let mut with_recursive = false;
            let mut with = Vec::new();
            let mut nested = None;
            for part in pair.into_inner() {
                match part.as_rule() {
                    Rule::cte_clause => {
                        let (recursive, ctes) = build_cte_clause(part)?;
                        with_recursive = recursive;
                        with = ctes;
                    }
                    Rule::set_operation_stmt
                    | Rule::simple_select_stmt
                    | Rule::simple_select_core => nested = Some(build_select(part)?),
                    _ => {}
                }
            }
            let mut stmt = nested.ok_or(ParseError::UnexpectedEof)?;
            if !with.is_empty() || with_recursive {
                stmt.with_recursive = with_recursive;
                stmt.with = with;
            }
            return Ok(stmt);
        }
        Rule::set_operation_stmt => return build_set_operation_select(pair),
        Rule::simple_select_stmt | Rule::simple_select_core => pair.into_inner().collect(),
        _ => {
            return Err(ParseError::UnexpectedToken {
                expected: "SELECT statement",
                actual: raw,
            });
        }
    };
    build_simple_select_statement(parts)
}

fn build_simple_select_statement(
    parts: Vec<Pair<'_, Rule>>,
) -> Result<SelectStatement, ParseError> {
    let mut with_recursive = false;
    let mut with = Vec::new();
    let mut distinct = false;
    let mut targets = None;
    let mut from = None;
    let mut where_clause = None;
    let mut group_by = Vec::new();
    let mut having = None;
    let mut window_clauses = Vec::new();
    let mut order_by = Vec::new();
    let mut limit = None;
    let mut offset = None;
    let mut locking_clause = None;
    for part in parts {
        match part.as_rule() {
            Rule::cte_clause => {
                let (recursive, ctes) = build_cte_clause(part)?;
                with_recursive = recursive;
                with = ctes;
            }
            Rule::select_distinct_clause => distinct = true,
            Rule::simple_select_core => {
                for inner in part.into_inner() {
                    match inner.as_rule() {
                        Rule::select_distinct_clause => distinct = true,
                        Rule::select_list => targets = Some(build_select_list(inner)?),
                        Rule::from_item => from = Some(build_from_item(inner)?),
                        Rule::expr => where_clause = Some(build_expr(inner)?),
                        Rule::group_by_clause => group_by = build_group_by_clause(inner)?,
                        Rule::having_clause => having = Some(build_having_clause(inner)?),
                        Rule::window_clause => window_clauses = build_window_clause(inner)?,
                        Rule::order_by_clause => order_by = build_order_by_clause(inner)?,
                        Rule::limit_clause => limit = Some(build_limit_clause(inner)?),
                        Rule::offset_clause => offset = Some(build_offset_clause(inner)?),
                        Rule::locking_clause => locking_clause = Some(build_locking_clause(inner)?),
                        _ => {}
                    }
                }
            }
            Rule::select_list => targets = Some(build_select_list(part)?),
            Rule::from_item => from = Some(build_from_item(part)?),
            Rule::expr => where_clause = Some(build_expr(part)?),
            Rule::group_by_clause => group_by = build_group_by_clause(part)?,
            Rule::having_clause => having = Some(build_having_clause(part)?),
            Rule::window_clause => window_clauses = build_window_clause(part)?,
            Rule::order_by_clause => order_by = build_order_by_clause(part)?,
            Rule::limit_clause => limit = Some(build_limit_clause(part)?),
            Rule::offset_clause => offset = Some(build_offset_clause(part)?),
            Rule::locking_clause => locking_clause = Some(build_locking_clause(part)?),
            _ => {}
        }
    }
    Ok(SelectStatement {
        with_recursive,
        with,
        distinct,
        from,
        targets: targets.unwrap_or_default(),
        where_clause,
        group_by,
        having,
        window_clauses,
        order_by,
        limit,
        offset,
        locking_clause,
        set_operation: None,
    })
}

fn build_select_into(pair: Pair<'_, Rule>) -> Result<CreateTableAsStatement, ParseError> {
    let mut query_parts = Vec::new();
    let mut relation = None;

    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::cte_clause
            | Rule::window_clause
            | Rule::order_by_clause
            | Rule::limit_clause
            | Rule::offset_clause
            | Rule::locking_clause => query_parts.push(part),
            Rule::select_into_core => {
                for inner in part.into_inner() {
                    match inner.as_rule() {
                        Rule::select_into_target => {
                            relation = Some(build_select_into_target(inner)?);
                        }
                        _ => query_parts.push(inner),
                    }
                }
            }
            _ => {}
        }
    }

    let (schema_name, table_name, persistence) = relation.ok_or(ParseError::UnexpectedEof)?;
    Ok(CreateTableAsStatement {
        schema_name,
        table_name,
        persistence,
        on_commit: OnCommitAction::PreserveRows,
        column_names: Vec::new(),
        query: build_simple_select_statement(query_parts)?,
        if_not_exists: false,
    })
}

fn build_select_into_target(
    pair: Pair<'_, Rule>,
) -> Result<(Option<String>, String, TablePersistence), ParseError> {
    let mut persistence = TablePersistence::Permanent;
    let mut relation_name = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::temp_clause => persistence = TablePersistence::Temporary,
            Rule::identifier => relation_name = Some(build_relation_name(inner)),
            _ => {}
        }
    }

    let (schema_name, table_name) = relation_name.ok_or(ParseError::UnexpectedEof)?;
    Ok((schema_name, table_name, persistence))
}

fn build_set_operation_term(pair: Pair<'_, Rule>) -> Result<SelectStatement, ParseError> {
    match pair.as_rule() {
        Rule::set_operation_term => {
            build_set_operation_term(pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?)
        }
        Rule::parenthesized_set_operation_term => build_select(
            pair.into_inner()
                .find(|part| matches!(part.as_rule(), Rule::select_stmt))
                .ok_or(ParseError::UnexpectedEof)?,
        ),
        Rule::simple_select_core
        | Rule::simple_select_stmt
        | Rule::set_operation_stmt
        | Rule::select_stmt => build_select(pair),
        _ => Err(ParseError::UnexpectedToken {
            expected: "set-operation term",
            actual: pair.as_str().into(),
        }),
    }
}

fn build_set_operation_select(pair: Pair<'_, Rule>) -> Result<SelectStatement, ParseError> {
    let raw = pair.as_str().to_string();
    let mut with_recursive = false;
    let mut with = Vec::new();
    let mut window_clauses = Vec::new();
    let mut order_by = Vec::new();
    let mut limit = None;
    let mut offset = None;
    let mut locking_clause = None;
    let mut operators = Vec::new();
    let mut inputs = Vec::new();

    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::cte_clause => {
                let (recursive, ctes) = build_cte_clause(part)?;
                with_recursive = recursive;
                with = ctes;
            }
            Rule::set_operation_term => inputs.push(build_set_operation_term(part)?),
            Rule::set_operation_clause => {
                let raw = part.as_str().trim_start().to_ascii_lowercase();
                let all = raw.split_ascii_whitespace().nth(1) == Some("all");
                let op = if raw.starts_with("union") {
                    SetOperator::Union { all }
                } else if raw.starts_with("intersect") {
                    SetOperator::Intersect { all }
                } else {
                    SetOperator::Except { all }
                };
                operators.push(op);
            }
            Rule::window_clause => window_clauses = build_window_clause(part)?,
            Rule::order_by_clause => order_by = build_order_by_clause(part)?,
            Rule::limit_clause => limit = Some(build_limit_clause(part)?),
            Rule::offset_clause => offset = Some(build_offset_clause(part)?),
            Rule::locking_clause => locking_clause = Some(build_locking_clause(part)?),
            _ => {}
        }
    }

    if inputs.len() < 2 {
        return Err(ParseError::UnexpectedToken {
            expected: "set operation with at least two inputs",
            actual: raw,
        });
    }

    let nested_set_operation = build_set_operation_tree(inputs, operators)?;

    Ok(SelectStatement {
        with_recursive,
        with,
        distinct: false,
        from: None,
        targets: Vec::new(),
        where_clause: None,
        group_by: Vec::new(),
        having: None,
        window_clauses,
        order_by,
        limit,
        offset,
        locking_clause,
        set_operation: Some(Box::new(nested_set_operation)),
    })
}

fn build_set_operation_tree(
    inputs: Vec<SelectStatement>,
    operators: Vec<SetOperator>,
) -> Result<SetOperationStatement, ParseError> {
    if inputs.len() != operators.len().saturating_add(1) {
        return Err(ParseError::UnexpectedToken {
            expected: "set-operation chain with one more input than operator",
            actual: format!("{} inputs and {} operators", inputs.len(), operators.len()),
        });
    }

    let mut pending_inputs = Vec::new();
    let mut pending_ops = Vec::new();
    let mut current = inputs[0].clone();

    for (op, next_input) in operators.into_iter().zip(inputs.into_iter().skip(1)) {
        if matches!(op, SetOperator::Intersect { .. }) {
            current = select_statement_for_set_operation(op, current, next_input);
        } else {
            pending_inputs.push(current);
            pending_ops.push(op);
            current = next_input;
        }
    }
    pending_inputs.push(current);

    let mut reduced_inputs = pending_inputs.into_iter();
    let mut nested = reduced_inputs.next().ok_or(ParseError::UnexpectedEof)?;
    for (op, next_input) in pending_ops.into_iter().zip(reduced_inputs) {
        nested = select_statement_for_set_operation(op, nested, next_input);
    }

    nested
        .set_operation
        .ok_or(ParseError::UnexpectedEof)
        .map(|op| *op)
}

fn select_statement_for_set_operation(
    op: SetOperator,
    left: SelectStatement,
    right: SelectStatement,
) -> SelectStatement {
    SelectStatement {
        with_recursive: false,
        with: Vec::new(),
        distinct: false,
        from: None,
        targets: Vec::new(),
        where_clause: None,
        group_by: Vec::new(),
        having: None,
        window_clauses: Vec::new(),
        order_by: Vec::new(),
        limit: None,
        offset: None,
        locking_clause: None,
        set_operation: Some(Box::new(SetOperationStatement {
            op,
            inputs: vec![left, right],
        })),
    }
}

fn build_values_statement(pair: Pair<'_, Rule>) -> Result<ValuesStatement, ParseError> {
    let mut with_recursive = false;
    let mut with = Vec::new();
    let mut rows = Vec::new();
    let mut order_by = Vec::new();
    let mut limit = None;
    let mut offset = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::cte_clause => {
                let (recursive, ctes) = build_cte_clause(part)?;
                with_recursive = recursive;
                with = ctes;
            }
            Rule::values_row => rows.push(build_values_row(part)?),
            Rule::order_by_clause => order_by = build_order_by_clause(part)?,
            Rule::limit_clause => limit = Some(build_limit_clause(part)?),
            Rule::offset_clause => offset = Some(build_offset_clause(part)?),
            _ => {}
        }
    }
    Ok(ValuesStatement {
        with_recursive,
        with,
        rows,
        order_by,
        limit,
        offset,
    })
}

fn wrap_values_as_select(stmt: ValuesStatement) -> SelectStatement {
    SelectStatement {
        with_recursive: stmt.with_recursive,
        with: stmt.with,
        distinct: false,
        from: Some(FromItem::Values { rows: stmt.rows }),
        targets: vec![SelectItem {
            output_name: "*".into(),
            expr: SqlExpr::Column("*".into()),
        }],
        where_clause: None,
        group_by: Vec::new(),
        having: None,
        window_clauses: Vec::new(),
        order_by: stmt.order_by,
        limit: stmt.limit,
        offset: stmt.offset,
        locking_clause: None,
        set_operation: None,
    }
}

fn build_cte_clause(pair: Pair<'_, Rule>) -> Result<(bool, Vec<CommonTableExpr>), ParseError> {
    let recursive = pair
        .as_str()
        .trim_start()
        .to_ascii_lowercase()
        .starts_with("with recursive");
    let mut ctes = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::common_table_expr => ctes.push(build_common_table_expr(part)?),
            _ => {}
        }
    }
    Ok((recursive, ctes))
}

fn build_common_table_expr(pair: Pair<'_, Rule>) -> Result<CommonTableExpr, ParseError> {
    let mut name = None;
    let mut column_names = Vec::new();
    let mut body = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier if name.is_none() => name = Some(build_identifier(part)),
            Rule::cte_column_list => {
                if let Some(list) = part
                    .into_inner()
                    .find(|inner| inner.as_rule() == Rule::ident_list)
                {
                    column_names = list.into_inner().map(build_identifier).collect();
                }
            }
            Rule::cte_body => {
                body = Some(build_cte_body(
                    part.into_inner().next().ok_or(ParseError::UnexpectedEof)?,
                )?)
            }
            _ => {}
        }
    }
    Ok(CommonTableExpr {
        name: name.ok_or(ParseError::UnexpectedEof)?,
        column_names,
        body: body.ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_cte_body(pair: Pair<'_, Rule>) -> Result<CteBody, ParseError> {
    match pair.as_rule() {
        Rule::select_stmt | Rule::simple_select_stmt | Rule::simple_select_core => {
            Ok(CteBody::Select(Box::new(build_select(pair)?)))
        }
        Rule::values_stmt => Ok(CteBody::Values(build_values_statement(pair)?)),
        Rule::recursive_union_cte_body => {
            let all = contains_union_all(pair.as_str());
            let mut inner = pair.into_inner();
            let anchor = build_cte_body(inner.next().ok_or(ParseError::UnexpectedEof)?)?;
            let mut recursive = None;
            for part in inner {
                match part.as_rule() {
                    Rule::select_stmt | Rule::simple_select_stmt | Rule::simple_select_core => {
                        recursive = Some(build_select(part)?)
                    }
                    Rule::parenthesized_set_operation_term => {
                        recursive = Some(build_set_operation_term(part)?)
                    }
                    _ => {}
                }
            }
            Ok(CteBody::RecursiveUnion {
                all,
                anchor: Box::new(anchor),
                recursive: Box::new(recursive.ok_or(ParseError::UnexpectedEof)?),
            })
        }
        _ => Err(ParseError::UnexpectedToken {
            expected: "SELECT or VALUES CTE body",
            actual: pair.as_str().into(),
        }),
    }
}

fn contains_union_all(sql: &str) -> bool {
    let mut prev_union = false;
    for token in sql.split_whitespace() {
        if prev_union && token.eq_ignore_ascii_case("all") {
            return true;
        }
        prev_union = token.eq_ignore_ascii_case("union");
    }
    false
}

fn build_group_by_clause(pair: Pair<'_, Rule>) -> Result<Vec<SqlExpr>, ParseError> {
    pair.into_inner()
        .filter(|part| part.as_rule() == Rule::expr)
        .map(build_expr)
        .collect()
}

fn build_having_clause(pair: Pair<'_, Rule>) -> Result<SqlExpr, ParseError> {
    let expr = pair
        .into_inner()
        .find(|part| part.as_rule() == Rule::expr)
        .ok_or(ParseError::UnexpectedEof)?;
    build_expr(expr)
}

fn build_order_by_clause(pair: Pair<'_, Rule>) -> Result<Vec<OrderByItem>, ParseError> {
    pair.into_inner()
        .filter(|part| part.as_rule() == Rule::order_by_item)
        .map(build_order_by_item)
        .collect()
}

fn build_locking_clause(pair: Pair<'_, Rule>) -> Result<SelectLockingClause, ParseError> {
    match pair.as_str().trim().to_ascii_lowercase().as_str() {
        "for no key update" => Ok(SelectLockingClause::ForNoKeyUpdate),
        "for update" => Ok(SelectLockingClause::ForUpdate),
        "for key share" => Ok(SelectLockingClause::ForKeyShare),
        "for share" => Ok(SelectLockingClause::ForShare),
        _ => Err(ParseError::UnexpectedToken {
            expected: "FOR UPDATE, FOR NO KEY UPDATE, FOR SHARE, or FOR KEY SHARE",
            actual: pair.as_str().into(),
        }),
    }
}

fn build_order_by_item(pair: Pair<'_, Rule>) -> Result<OrderByItem, ParseError> {
    let mut expr = None;
    let mut descending = false;
    let mut nulls_first = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::expr => expr = Some(build_expr(part)?),
            Rule::kw_desc => descending = true,
            Rule::kw_asc => descending = false,
            Rule::nulls_ordering => {
                for item in part.into_inner() {
                    match item.as_rule() {
                        Rule::kw_first => nulls_first = Some(true),
                        Rule::kw_last => nulls_first = Some(false),
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
    Ok(OrderByItem {
        expr: expr.ok_or(ParseError::UnexpectedEof)?,
        descending,
        nulls_first,
    })
}

fn build_limit_clause(pair: Pair<'_, Rule>) -> Result<usize, ParseError> {
    build_usize_clause(pair, "LIMIT")
}

fn build_offset_clause(pair: Pair<'_, Rule>) -> Result<usize, ParseError> {
    build_usize_clause(pair, "OFFSET")
}

fn build_usize_clause(pair: Pair<'_, Rule>, expected: &'static str) -> Result<usize, ParseError> {
    let integer = pair
        .into_inner()
        .find(|part| part.as_rule() == Rule::integer)
        .ok_or(ParseError::UnexpectedEof)?;
    integer
        .as_str()
        .parse::<usize>()
        .map_err(|_| ParseError::UnexpectedToken {
            expected,
            actual: integer.as_str().into(),
        })
}

fn build_from_item(pair: Pair<'_, Rule>) -> Result<FromItem, ParseError> {
    let raw = pair.as_str().to_string();
    match pair.as_rule() {
        Rule::from_item | Rule::from_primary | Rule::parenthesized_from_item => {
            build_from_item(pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?)
        }
        Rule::lateral_from_item => {
            let source = pair
                .into_inner()
                .find(|part| {
                    matches!(
                        part.as_rule(),
                        Rule::values_from_item
                            | Rule::srf_from_item
                            | Rule::derived_from_item
                            | Rule::parenthesized_from_item
                    )
                })
                .ok_or(ParseError::UnexpectedEof)?;
            Ok(FromItem::Lateral(Box::new(build_from_item(source)?)))
        }
        Rule::from_list_item => {
            let mut source = None;
            let mut alias = None;
            let mut column_aliases = AliasColumnSpec::None;
            for part in pair.into_inner() {
                match part.as_rule() {
                    Rule::joined_from_item => source = Some(build_from_item(part)?),
                    Rule::relation_alias => {
                        let (parsed_alias, parsed_column_aliases) = build_relation_alias(part)?;
                        alias = Some(parsed_alias);
                        column_aliases = parsed_column_aliases;
                    }
                    _ => {}
                }
            }
            let item = source.ok_or(ParseError::UnexpectedEof)?;
            if let Some(alias) = alias {
                Ok(FromItem::Alias {
                    source: Box::new(item),
                    alias,
                    column_aliases,
                    preserve_source_names: true,
                })
            } else {
                Ok(item)
            }
        }
        Rule::from_list => {
            let mut items = pair
                .into_inner()
                .filter(|part| part.as_rule() == Rule::from_list_item)
                .map(build_from_item);
            let mut item = items.next().ok_or(ParseError::UnexpectedEof)??;
            for next in items {
                item = FromItem::Join {
                    left: Box::new(item),
                    right: Box::new(next?),
                    kind: JoinKind::Cross,
                    constraint: JoinConstraint::None,
                };
            }
            Ok(item)
        }
        Rule::joined_from_item => {
            let mut parts = pair.into_inner();
            let mut item = build_from_item(parts.next().ok_or(ParseError::UnexpectedEof)?)?;
            for join_clause in parts {
                let (kind, right, constraint) = build_join_clause(join_clause)?;
                item = FromItem::Join {
                    left: Box::new(item),
                    right: Box::new(right),
                    kind,
                    constraint,
                };
            }
            Ok(item)
        }
        Rule::aliased_from_item => {
            let mut source = None;
            let mut alias = None;
            let mut column_aliases = AliasColumnSpec::None;
            for part in pair.into_inner() {
                match part.as_rule() {
                    Rule::only_table_from_item
                    | Rule::table_from_item
                    | Rule::lateral_from_item
                    | Rule::values_from_item
                    | Rule::parenthesized_table_from_item
                    | Rule::srf_from_item
                    | Rule::derived_from_item
                    | Rule::parenthesized_from_item
                    | Rule::from_primary => source = Some(build_from_item(part)?),
                    Rule::relation_alias => {
                        let (parsed_alias, parsed_column_aliases) = build_relation_alias(part)?;
                        alias = Some(parsed_alias);
                        column_aliases = parsed_column_aliases;
                    }
                    _ => {}
                }
            }
            let item = source.ok_or(ParseError::UnexpectedEof)?;
            if let Some(alias) = alias {
                Ok(FromItem::Alias {
                    source: Box::new(item),
                    alias,
                    column_aliases,
                    preserve_source_names: false,
                })
            } else {
                Ok(item)
            }
        }
        Rule::only_table_from_item => Ok(FromItem::Table {
            name: build_identifier(
                pair.into_inner()
                    .find(|part| part.as_rule() == Rule::identifier)
                    .ok_or(ParseError::UnexpectedEof)?,
            ),
            only: true,
        }),
        Rule::table_from_item | Rule::parenthesized_table_from_item => Ok(FromItem::Table {
            name: build_identifier(
                pair.into_inner()
                    .find(|part| part.as_rule() == Rule::identifier)
                    .ok_or(ParseError::UnexpectedEof)?,
            ),
            only: false,
        }),
        Rule::values_from_item => Ok(FromItem::Values {
            rows: pair
                .into_inner()
                .filter(|part| part.as_rule() == Rule::values_row)
                .map(build_values_row)
                .collect::<Result<Vec<_>, _>>()?,
        }),
        Rule::srf_from_item => {
            let mut name = None;
            let mut parsed_args = ParsedFunctionArgs::default();
            let mut with_ordinality = false;
            for part in pair.into_inner() {
                match part.as_rule() {
                    Rule::identifier if name.is_none() => name = Some(build_identifier(part)),
                    Rule::function_arg_list => {
                        parsed_args = build_function_arg_list(part)?;
                    }
                    Rule::srf_with_ordinality => with_ordinality = true,
                    _ => {}
                }
            }
            Ok(FromItem::FunctionCall {
                name: name.ok_or(ParseError::UnexpectedEof)?,
                args: parsed_args.args,
                func_variadic: parsed_args.func_variadic,
                with_ordinality,
            })
        }
        Rule::derived_from_item => {
            let select = pair
                .into_inner()
                .find(|part| part.as_rule() == Rule::select_stmt)
                .ok_or(ParseError::UnexpectedEof)?;
            Ok(FromItem::DerivedTable(Box::new(build_select(select)?)))
        }
        _ => Err(ParseError::UnexpectedToken {
            expected: "from clause",
            actual: raw,
        }),
    }
}

fn build_join_clause(
    pair: Pair<'_, Rule>,
) -> Result<(JoinKind, FromItem, JoinConstraint), ParseError> {
    let raw = pair.as_str().to_string();
    let mut kind = JoinKind::Inner;
    let mut right = None;
    let mut constraint = JoinConstraint::None;
    let mut natural = false;

    for part in pair.into_inner() {
        consume_join_part(part, &mut kind, &mut right, &mut constraint, &mut natural)?;
    }

    if natural {
        constraint = JoinConstraint::Natural;
        if matches!(kind, JoinKind::Cross) {
            return Err(ParseError::UnexpectedToken {
                expected: "NATURAL join without CROSS",
                actual: raw,
            });
        }
    }

    match kind {
        JoinKind::Cross => {
            if !matches!(constraint, JoinConstraint::None) {
                return Err(ParseError::UnexpectedToken {
                    expected: "CROSS JOIN without ON or USING",
                    actual: raw,
                });
            }
        }
        JoinKind::Inner | JoinKind::Left | JoinKind::Right | JoinKind::Full => {
            if matches!(constraint, JoinConstraint::None) {
                return Err(ParseError::UnexpectedToken {
                    expected: "join qualifier",
                    actual: raw,
                });
            }
        }
    }

    Ok((kind, right.ok_or(ParseError::UnexpectedEof)?, constraint))
}

fn consume_join_part(
    part: Pair<'_, Rule>,
    kind: &mut JoinKind,
    right: &mut Option<FromItem>,
    constraint: &mut JoinConstraint,
    natural: &mut bool,
) -> Result<(), ParseError> {
    match part.as_rule() {
        Rule::aliased_from_item => *right = Some(build_from_item(part)?),
        Rule::expr => *constraint = JoinConstraint::On(build_expr(part)?),
        Rule::join_using_clause => {
            let mut columns = Vec::new();
            collect_identifiers(part, &mut columns);
            *constraint = JoinConstraint::Using(columns);
        }
        Rule::cross_join_type => *kind = JoinKind::Cross,
        Rule::kw_left | Rule::left_join_type => *kind = JoinKind::Left,
        Rule::kw_right | Rule::right_join_type => *kind = JoinKind::Right,
        Rule::kw_full | Rule::full_join_type => *kind = JoinKind::Full,
        Rule::natural_marker => *natural = true,
        _ => {
            for inner in part.into_inner() {
                consume_join_part(inner, kind, right, constraint, natural)?;
            }
        }
    }
    Ok(())
}

fn collect_identifiers(pair: Pair<'_, Rule>, out: &mut Vec<String>) {
    match pair.as_rule() {
        Rule::identifier => out.push(build_identifier(pair)),
        _ => {
            for part in pair.into_inner() {
                collect_identifiers(part, out);
            }
        }
    }
}

fn build_relation_alias(pair: Pair<'_, Rule>) -> Result<(String, AliasColumnSpec), ParseError> {
    let mut alias = None;
    let mut column_aliases = AliasColumnSpec::None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier if alias.is_none() => alias = Some(build_identifier(part)),
            Rule::bare_relation_alias if alias.is_none() => {
                alias = Some(build_identifier(
                    part.into_inner().next().ok_or(ParseError::UnexpectedEof)?,
                ));
            }
            Rule::alias_column_spec => column_aliases = build_alias_column_spec(part)?,
            _ => {}
        }
    }
    Ok((alias.ok_or(ParseError::UnexpectedEof)?, column_aliases))
}

fn build_alias_column_spec(pair: Pair<'_, Rule>) -> Result<AliasColumnSpec, ParseError> {
    let mut defs = Vec::new();
    let mut names = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::alias_column_def => defs.push(build_alias_column_def(part)?),
            Rule::ident_list => collect_identifiers(part, &mut names),
            _ => {}
        }
    }
    if !defs.is_empty() {
        return Ok(AliasColumnSpec::Definitions(defs));
    }
    if !names.is_empty() {
        return Ok(AliasColumnSpec::Names(names));
    }
    Ok(AliasColumnSpec::None)
}

fn build_alias_column_def(pair: Pair<'_, Rule>) -> Result<AliasColumnDef, ParseError> {
    let mut inner = pair.into_inner();
    let name = build_identifier(inner.next().ok_or(ParseError::UnexpectedEof)?);
    let ty = build_type_name(inner.next().ok_or(ParseError::UnexpectedEof)?);
    Ok(AliasColumnDef { name, ty })
}

fn build_insert(pair: Pair<'_, Rule>) -> Result<InsertStatement, ParseError> {
    let mut with_recursive = false;
    let mut with = Vec::new();
    let mut table_name = None;
    let mut table_alias = None;
    let mut columns = None;
    let mut source = None;
    let mut on_conflict = None;
    let mut returning = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::cte_clause => {
                let (recursive, ctes) = build_cte_clause(part)?;
                with_recursive = recursive;
                with = ctes;
            }
            Rule::identifier if table_name.is_none() => table_name = Some(build_identifier(part)),
            Rule::insert_target_alias => {
                let alias = part
                    .into_inner()
                    .find(|inner| inner.as_rule() == Rule::insert_alias_identifier)
                    .ok_or(ParseError::UnexpectedEof)?;
                let alias = alias
                    .into_inner()
                    .find(|inner| inner.as_rule() == Rule::identifier)
                    .ok_or(ParseError::UnexpectedEof)?;
                table_alias = Some(build_identifier(alias));
            }
            Rule::assignment_target_list => columns = Some(build_assignment_target_list(part)?),
            Rule::insert_values_source => {
                source = Some(InsertSource::Values(
                    part.into_inner()
                        .filter(|inner| inner.as_rule() == Rule::values_row)
                        .map(build_values_row)
                        .collect::<Result<Vec<_>, _>>()?,
                ))
            }
            Rule::insert_default_values_source => source = Some(InsertSource::DefaultValues),
            Rule::select_stmt => source = Some(InsertSource::Select(Box::new(build_select(part)?))),
            Rule::on_conflict_clause => on_conflict = Some(build_on_conflict_clause(part)?),
            Rule::returning_clause => returning = build_returning_clause(part)?,
            _ => {}
        }
    }
    Ok(InsertStatement {
        with_recursive,
        with,
        table_name: table_name.ok_or(ParseError::UnexpectedEof)?,
        table_alias,
        columns,
        source: source.ok_or(ParseError::UnexpectedEof)?,
        on_conflict,
        returning,
    })
}

fn build_merge(pair: Pair<'_, Rule>) -> Result<MergeStatement, ParseError> {
    let mut with_recursive = false;
    let mut with = Vec::new();
    let mut target_table = None;
    let mut target_alias = None;
    let mut target_only = false;
    let mut source = None;
    let mut join_condition = None;
    let mut when_clauses = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::cte_clause => {
                let (recursive, ctes) = build_cte_clause(part)?;
                with_recursive = recursive;
                with = ctes;
            }
            Rule::merge_target => {
                let (name, alias, only) = build_merge_target(part)?;
                target_table = Some(name);
                target_alias = alias;
                target_only = only;
            }
            Rule::merge_source => {
                let inner = part.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
                source = Some(build_from_item(inner)?);
            }
            Rule::merge_join_condition => {
                let expr = part
                    .into_inner()
                    .find(|inner| inner.as_rule() == Rule::expr)
                    .ok_or(ParseError::UnexpectedEof)?;
                join_condition = Some(build_expr(expr)?);
            }
            Rule::merge_when_clause => when_clauses.push(build_merge_when_clause(part)?),
            _ => {}
        }
    }
    Ok(MergeStatement {
        with_recursive,
        with,
        target_table: target_table.ok_or(ParseError::UnexpectedEof)?,
        target_alias,
        target_only,
        source: source.ok_or(ParseError::UnexpectedEof)?,
        join_condition: join_condition.ok_or(ParseError::UnexpectedEof)?,
        when_clauses,
    })
}

fn build_merge_target(pair: Pair<'_, Rule>) -> Result<(String, Option<String>, bool), ParseError> {
    let mut table_name = None;
    let mut alias = None;
    let mut only = false;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::only_clause => only = true,
            Rule::identifier if table_name.is_none() => table_name = Some(build_identifier(part)),
            Rule::merge_target_alias => {
                let alias_pair = part.into_inner().find(|inner| {
                    matches!(
                        inner.as_rule(),
                        Rule::merge_alias_identifier | Rule::identifier
                    )
                });
                if let Some(alias_pair) = alias_pair {
                    alias = Some(build_identifier(alias_pair));
                }
            }
            _ => {}
        }
    }
    Ok((table_name.ok_or(ParseError::UnexpectedEof)?, alias, only))
}

fn build_merge_when_clause(pair: Pair<'_, Rule>) -> Result<MergeWhenClause, ParseError> {
    let inner = pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
    let match_kind = match inner.as_rule() {
        Rule::merge_when_matched_clause => MergeMatchKind::Matched,
        Rule::merge_when_not_matched_by_source_clause => MergeMatchKind::NotMatchedBySource,
        Rule::merge_when_not_matched_clause | Rule::merge_when_not_matched_by_target_clause => {
            MergeMatchKind::NotMatchedByTarget
        }
        _ => {
            return Err(ParseError::UnexpectedToken {
                expected: "merge when clause",
                actual: inner.as_str().to_string(),
            });
        }
    };
    let mut condition = None;
    let mut action = None;
    for part in inner.into_inner() {
        match part.as_rule() {
            Rule::merge_search_condition => {
                let expr = part
                    .into_inner()
                    .find(|inner| inner.as_rule() == Rule::expr)
                    .ok_or(ParseError::UnexpectedEof)?;
                condition = Some(build_expr(expr)?);
            }
            Rule::merge_matched_action
            | Rule::merge_not_matched_action
            | Rule::merge_update_action
            | Rule::merge_delete_action
            | Rule::merge_do_nothing_action
            | Rule::merge_insert_action => action = Some(build_merge_action(part)?),
            _ => {}
        }
    }
    Ok(MergeWhenClause {
        match_kind,
        condition,
        action: action.ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_merge_action(pair: Pair<'_, Rule>) -> Result<MergeAction, ParseError> {
    match pair.as_rule() {
        Rule::merge_matched_action | Rule::merge_not_matched_action => {
            build_merge_action(pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?)
        }
        Rule::merge_update_action => Ok(MergeAction::Update {
            assignments: pair
                .into_inner()
                .filter(|part| part.as_rule() == Rule::assignment)
                .map(build_assignment)
                .collect::<Result<Vec<_>, _>>()?,
        }),
        Rule::merge_delete_action => Ok(MergeAction::Delete),
        Rule::merge_do_nothing_action => Ok(MergeAction::DoNothing),
        Rule::merge_insert_action => {
            let mut columns = None;
            let mut source = None;
            for part in pair.into_inner() {
                match part.as_rule() {
                    Rule::merge_insert_columns => {
                        let mut names = Vec::new();
                        collect_identifiers(part, &mut names);
                        columns = Some(names);
                    }
                    Rule::merge_insert_values_source => {
                        let values = part
                            .into_inner()
                            .find(|inner| inner.as_rule() == Rule::values_row)
                            .ok_or(ParseError::UnexpectedEof)?;
                        source = Some(MergeInsertSource::Values(build_values_row(values)?));
                    }
                    Rule::merge_insert_default_values_source => {
                        source = Some(MergeInsertSource::DefaultValues);
                    }
                    _ => {}
                }
            }
            Ok(MergeAction::Insert {
                columns,
                source: source.ok_or(ParseError::UnexpectedEof)?,
            })
        }
        _ => Err(ParseError::UnexpectedToken {
            expected: "merge action",
            actual: pair.as_str().to_string(),
        }),
    }
}

fn build_on_conflict_clause(pair: Pair<'_, Rule>) -> Result<OnConflictClause, ParseError> {
    let mut target = None;
    let mut action = None;
    let mut assignments = Vec::new();
    let mut where_clause = None;
    fn apply_on_conflict_part(
        part: Pair<'_, Rule>,
        target: &mut Option<OnConflictTarget>,
        action: &mut Option<OnConflictAction>,
        assignments: &mut Vec<Assignment>,
        where_clause: &mut Option<SqlExpr>,
    ) -> Result<(), ParseError> {
        match part.as_rule() {
            Rule::on_conflict_do_nothing => {
                *action = Some(OnConflictAction::Nothing);
                for nested in part.into_inner() {
                    apply_on_conflict_part(nested, target, action, assignments, where_clause)?;
                }
            }
            Rule::on_conflict_do_update => {
                *action = Some(OnConflictAction::Update);
                for nested in part.into_inner() {
                    apply_on_conflict_part(nested, target, action, assignments, where_clause)?;
                }
            }
            Rule::on_conflict_target => *target = Some(build_on_conflict_target(part)?),
            Rule::assignment => assignments.push(build_assignment(part)?),
            Rule::expr => *where_clause = Some(build_expr(part)?),
            _ => {}
        }
        Ok(())
    }
    for part in pair.into_inner() {
        apply_on_conflict_part(
            part,
            &mut target,
            &mut action,
            &mut assignments,
            &mut where_clause,
        )?;
    }
    let action = action.ok_or(ParseError::UnexpectedEof)?;
    Ok(OnConflictClause {
        target,
        action,
        assignments: if matches!(action, OnConflictAction::Nothing) {
            Vec::new()
        } else {
            assignments
        },
        where_clause: if matches!(action, OnConflictAction::Nothing) {
            None
        } else {
            where_clause
        },
    })
}

fn build_on_conflict_target(pair: Pair<'_, Rule>) -> Result<OnConflictTarget, ParseError> {
    let inner = pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
    match inner.as_rule() {
        Rule::on_conflict_constraint_target => Ok(OnConflictTarget::Constraint(
            inner
                .into_inner()
                .find(|part| part.as_rule() == Rule::identifier)
                .map(build_identifier)
                .ok_or(ParseError::UnexpectedEof)?,
        )),
        Rule::on_conflict_inference_target => {
            let mut elements = Vec::new();
            let mut predicate = None;
            for part in inner.into_inner() {
                match part.as_rule() {
                    Rule::on_conflict_inference_elem => {
                        let mut expr = None;
                        let mut collation = None;
                        let mut opclass = None;
                        for child in part.into_inner() {
                            match child.as_rule() {
                                Rule::on_conflict_inference_expr => {
                                    expr = child
                                        .into_inner()
                                        .find(|part| part.as_rule() == Rule::expr)
                                        .map(build_expr)
                                        .transpose()?;
                                }
                                Rule::on_conflict_inference_collation => {
                                    collation = child
                                        .into_inner()
                                        .find(|part| part.as_rule() == Rule::collation_name)
                                        .map(build_collation_name)
                                        .transpose()?;
                                }
                                Rule::on_conflict_inference_opclass => {
                                    opclass = child.into_inner().next().map(build_identifier);
                                }
                                _ => {}
                            }
                        }
                        let mut expr = expr.ok_or(ParseError::UnexpectedEof)?;
                        if collation.is_none() {
                            if let SqlExpr::Collate {
                                expr: inner_expr,
                                collation: explicit_collation,
                            } = expr
                            {
                                expr = *inner_expr;
                                collation = Some(explicit_collation);
                            }
                        }
                        elements.push(OnConflictInferenceElem {
                            expr,
                            collation,
                            opclass,
                        });
                    }
                    Rule::on_conflict_inference_predicate => {
                        predicate = part
                            .into_inner()
                            .find(|part| part.as_rule() == Rule::expr)
                            .map(build_expr)
                            .transpose()?;
                    }
                    _ => {}
                }
            }
            Ok(OnConflictTarget::Inference(OnConflictInferenceSpec {
                elements,
                predicate,
            }))
        }
        _ => Err(ParseError::UnexpectedToken {
            expected: "ON CONFLICT target",
            actual: inner.as_str().to_string(),
        }),
    }
}

fn build_create_table(pair: Pair<'_, Rule>) -> Result<Statement, ParseError> {
    let mut relation_name = None;
    let mut persistence = TablePersistence::Permanent;
    let mut on_commit = OnCommitAction::PreserveRows;
    let mut elements = Vec::new();
    let mut inherits = Vec::new();
    let mut ctas_columns = Vec::new();
    let mut query = None;
    let mut is_ctas = false;
    let mut if_not_exists = false;
    for part in pair.into_inner() {
        let part = if part.as_rule() == Rule::create_table_tail {
            part.into_inner().next().ok_or(ParseError::UnexpectedEof)?
        } else {
            part
        };
        match part.as_rule() {
            Rule::temp_clause => persistence = TablePersistence::Temporary,
            Rule::if_not_exists_clause => if_not_exists = true,
            Rule::identifier if relation_name.is_none() => {
                relation_name = Some(build_relation_name(part))
            }
            Rule::create_table_column_form => {
                for inner in part.into_inner() {
                    match inner.as_rule() {
                        Rule::create_table_element => {
                            elements.push(build_create_table_element(inner)?)
                        }
                        Rule::on_commit_clause => on_commit = build_on_commit_action(inner)?,
                        _ => {}
                    }
                }
            }
            Rule::create_table_as_form => {
                is_ctas = true;
                for inner in part.into_inner() {
                    match inner.as_rule() {
                        Rule::ctas_column_list => {
                            ctas_columns = inner
                                .into_inner()
                                .find(|p| p.as_rule() == Rule::ident_list)
                                .map(|p| p.into_inner().map(build_identifier).collect())
                                .unwrap_or_default();
                        }
                        Rule::on_commit_clause => on_commit = build_on_commit_action(inner)?,
                        Rule::select_stmt => query = Some(build_select(inner)?),
                        _ => {}
                    }
                }
            }
            Rule::inherits_clause => {
                inherits = part
                    .into_inner()
                    .find(|inner| inner.as_rule() == Rule::ident_list)
                    .map(|inner| inner.into_inner().map(build_identifier).collect())
                    .unwrap_or_default();
            }
            Rule::table_storage_clause => validate_table_storage_clause(part)?,
            _ => {}
        }
    }
    let (schema_name, table_name) = relation_name.ok_or(ParseError::UnexpectedEof)?;
    if is_ctas && !inherits.is_empty() {
        return Err(ParseError::FeatureNotSupported(
            "CREATE TABLE AS ... INHERITS".into(),
        ));
    }
    if is_ctas {
        Ok(Statement::CreateTableAs(CreateTableAsStatement {
            schema_name,
            table_name,
            persistence,
            on_commit,
            column_names: ctas_columns,
            query: query.ok_or(ParseError::UnexpectedEof)?,
            if_not_exists,
        }))
    } else {
        Ok(Statement::CreateTable(CreateTableStatement {
            schema_name,
            table_name,
            persistence,
            on_commit,
            elements,
            inherits,
            partition_spec: None,
            partition_of: None,
            partition_bound: None,
            if_not_exists,
        }))
    }
}

fn build_create_schema(pair: Pair<'_, Rule>) -> Result<CreateSchemaStatement, ParseError> {
    let mut schema_name = None;
    let mut auth_role = None;
    let mut if_not_exists = false;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::if_not_exists_clause => if_not_exists = true,
            Rule::create_schema_authorization_only => {
                let role = part
                    .into_inner()
                    .find(|inner| inner.as_rule() == Rule::identifier)
                    .map(build_identifier)
                    .ok_or(ParseError::UnexpectedEof)?;
                auth_role = Some(role);
            }
            Rule::create_schema_authorization_clause => {
                let role = part
                    .into_inner()
                    .find(|inner| inner.as_rule() == Rule::identifier)
                    .map(build_identifier)
                    .ok_or(ParseError::UnexpectedEof)?;
                auth_role = Some(role);
            }
            Rule::identifier => {
                let ident = build_identifier(part);
                if schema_name.is_none() {
                    schema_name = Some(ident);
                }
            }
            _ => {}
        }
    }
    Ok(CreateSchemaStatement {
        schema_name,
        auth_role,
        if_not_exists,
    })
}

fn build_create_view(pair: Pair<'_, Rule>) -> Result<CreateViewStatement, ParseError> {
    let mut relation_name = None;
    let mut query = None;
    let mut query_sql = None;
    let mut or_replace = false;
    let mut check_option = ViewCheckOption::None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::create_or_replace_clause => or_replace = true,
            Rule::identifier if relation_name.is_none() => {
                relation_name = Some(build_relation_name(part))
            }
            Rule::select_stmt => {
                query_sql = Some(part.as_str().trim().to_string());
                query = Some(build_select(part)?);
            }
            Rule::view_check_option_clause => {
                let raw = part.as_str().trim();
                query_sql = Some(match query_sql.take() {
                    Some(sql) => format!("{sql} {raw}"),
                    None => raw.to_string(),
                });
                let lowered = raw.to_ascii_lowercase();
                check_option = if lowered.contains(" local ") || lowered.starts_with("with local ")
                {
                    ViewCheckOption::Local
                } else {
                    ViewCheckOption::Cascaded
                };
            }
            _ => {}
        }
    }
    let (schema_name, view_name) = relation_name.ok_or(ParseError::UnexpectedEof)?;
    Ok(CreateViewStatement {
        schema_name,
        view_name,
        query: query.ok_or(ParseError::UnexpectedEof)?,
        query_sql: query_sql.ok_or(ParseError::UnexpectedEof)?,
        or_replace,
        check_option,
    })
}

fn build_create_rule(pair: Pair<'_, Rule>) -> Result<CreateRuleStatement, ParseError> {
    let mut rule_name = None;
    let mut relation_name = None;
    let mut event = None;
    let mut do_kind = RuleDoKind::Also;
    let mut where_clause = None;
    let mut where_sql = None;
    let mut actions = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier if rule_name.is_none() => rule_name = Some(build_identifier(part)),
            Rule::identifier if relation_name.is_none() => {
                relation_name = Some(build_identifier(part));
            }
            Rule::rule_event => event = Some(build_rule_event(part)?),
            Rule::rule_do_kind => do_kind = build_rule_do_kind(part)?,
            Rule::rule_where_clause => {
                let expr = part
                    .clone()
                    .into_inner()
                    .find(|inner| inner.as_rule() == Rule::expr)
                    .ok_or(ParseError::UnexpectedEof)?;
                where_clause = Some(build_expr(expr)?);
                let raw = part.as_str().trim();
                where_sql = Some(raw["where".len()..].trim().to_string());
            }
            Rule::rule_action_body => actions = Some(build_rule_action_body(part)?),
            _ => {}
        }
    }
    Ok(CreateRuleStatement {
        rule_name: rule_name.ok_or(ParseError::UnexpectedEof)?,
        relation_name: relation_name.ok_or(ParseError::UnexpectedEof)?,
        event: event.ok_or(ParseError::UnexpectedEof)?,
        do_kind,
        where_clause,
        where_sql,
        actions: actions.ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_rule_event(pair: Pair<'_, Rule>) -> Result<RuleEvent, ParseError> {
    match pair.as_str().trim().to_ascii_lowercase().as_str() {
        "insert" => Ok(RuleEvent::Insert),
        "update" => Ok(RuleEvent::Update),
        "delete" => Ok(RuleEvent::Delete),
        "select" => Ok(RuleEvent::Select),
        _ => Err(ParseError::UnexpectedToken {
            expected: "rule event",
            actual: pair.as_str().into(),
        }),
    }
}

fn build_rule_do_kind(pair: Pair<'_, Rule>) -> Result<RuleDoKind, ParseError> {
    match pair.as_str().trim().to_ascii_lowercase().as_str() {
        "also" => Ok(RuleDoKind::Also),
        "instead" => Ok(RuleDoKind::Instead),
        _ => Err(ParseError::UnexpectedToken {
            expected: "rule kind",
            actual: pair.as_str().into(),
        }),
    }
}

fn build_rule_action_body(pair: Pair<'_, Rule>) -> Result<Vec<RuleActionStatement>, ParseError> {
    let mut actions = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::kw_nothing => return Ok(Vec::new()),
            Rule::rule_action_list => {
                for action_sql in split_rule_action_list(part.as_str())? {
                    actions.push(build_rule_action_statement_sql(&action_sql)?);
                }
            }
            Rule::rule_action_stmt => actions.push(build_rule_action_statement(part)?),
            _ => {}
        }
    }
    Ok(actions)
}

fn build_rule_action_statement(pair: Pair<'_, Rule>) -> Result<RuleActionStatement, ParseError> {
    let inner = if pair.as_rule() == Rule::rule_action_stmt {
        pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?
    } else {
        pair
    };
    build_rule_action_statement_sql(inner.as_str())
}

fn build_rule_action_statement_sql(sql: &str) -> Result<RuleActionStatement, ParseError> {
    let sql = sql.trim().trim_end_matches(';').trim().to_string();
    let statement = parse_statement(&sql)?;
    match &statement {
        Statement::Insert(stmt) if stmt.with_recursive || !stmt.with.is_empty() => {
            return Err(ParseError::FeatureNotSupported(
                "WITH in rule actions".into(),
            ));
        }
        Statement::Update(stmt) if stmt.with_recursive || !stmt.with.is_empty() => {
            return Err(ParseError::FeatureNotSupported(
                "WITH in rule actions".into(),
            ));
        }
        Statement::Delete(stmt) if stmt.with_recursive || !stmt.with.is_empty() => {
            return Err(ParseError::FeatureNotSupported(
                "WITH in rule actions".into(),
            ));
        }
        Statement::Unsupported(_) => {
            return Err(ParseError::FeatureNotSupported(
                "rule action statement".into(),
            ));
        }
        Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_) => {}
        _ => {
            return Err(ParseError::FeatureNotSupported(
                "rule action statement".into(),
            ));
        }
    }
    Ok(RuleActionStatement { statement, sql })
}

fn split_rule_action_list(list_sql: &str) -> Result<Vec<String>, ParseError> {
    let inner = list_sql
        .trim()
        .strip_prefix('(')
        .and_then(|sql| sql.strip_suffix(')'))
        .ok_or_else(|| ParseError::UnexpectedToken {
            expected: "parenthesized rule action list",
            actual: list_sql.into(),
        })?;

    let mut actions = Vec::new();
    let mut start = 0usize;
    let mut paren_depth = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    let chars = inner.char_indices().collect::<Vec<_>>();
    let mut index = 0usize;
    while index < chars.len() {
        let (offset, ch) = chars[index];
        if in_single {
            if ch == '\'' {
                let next_is_quote = chars
                    .get(index + 1)
                    .map(|(_, next)| *next == '\'')
                    .unwrap_or(false);
                if next_is_quote {
                    index += 1;
                } else {
                    in_single = false;
                }
            }
            index += 1;
            continue;
        }
        if in_double {
            if ch == '"' {
                let next_is_quote = chars
                    .get(index + 1)
                    .map(|(_, next)| *next == '"')
                    .unwrap_or(false);
                if next_is_quote {
                    index += 1;
                } else {
                    in_double = false;
                }
            }
            index += 1;
            continue;
        }
        match ch {
            '\'' => in_single = true,
            '"' => in_double = true,
            '(' => paren_depth += 1,
            ')' => paren_depth = paren_depth.saturating_sub(1),
            ';' if paren_depth == 0 => {
                let action = inner[start..offset].trim();
                if !action.is_empty() {
                    actions.push(action.to_string());
                }
                start = offset + ch.len_utf8();
            }
            _ => {}
        }
        index += 1;
    }

    let tail = inner[start..].trim();
    if !tail.is_empty() {
        actions.push(tail.to_string());
    }
    Ok(actions)
}

fn build_create_table_element(pair: Pair<'_, Rule>) -> Result<CreateTableElement, ParseError> {
    let inner = pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
    match inner.as_rule() {
        Rule::create_table_like_clause => Ok(CreateTableElement::Like(
            build_create_table_like_clause(inner)?,
        )),
        Rule::column_def => Ok(CreateTableElement::Column(build_column_def(inner)?)),
        Rule::table_constraint => Ok(CreateTableElement::Constraint(build_table_constraint(
            inner,
        )?)),
        _ => Err(ParseError::UnexpectedToken {
            expected: "column definition or table constraint",
            actual: inner.as_str().to_string(),
        }),
    }
}

fn build_create_table_like_clause(
    pair: Pair<'_, Rule>,
) -> Result<CreateTableLikeClause, ParseError> {
    let mut relation_name = None;
    let mut options = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier => relation_name = Some(build_identifier(part)),
            Rule::create_table_like_option => {
                let raw = part.as_str().trim().to_ascii_lowercase();
                let including = raw.starts_with("including");
                let option = if raw.ends_with("defaults") {
                    if including {
                        CreateTableLikeOption::IncludingDefaults
                    } else {
                        CreateTableLikeOption::ExcludingDefaults
                    }
                } else if raw.ends_with("constraints") {
                    if including {
                        CreateTableLikeOption::IncludingConstraints
                    } else {
                        CreateTableLikeOption::ExcludingConstraints
                    }
                } else if raw.ends_with("indexes") {
                    if including {
                        CreateTableLikeOption::IncludingIndexes
                    } else {
                        CreateTableLikeOption::ExcludingIndexes
                    }
                } else if raw.ends_with("all") {
                    if including {
                        CreateTableLikeOption::IncludingAll
                    } else {
                        CreateTableLikeOption::ExcludingAll
                    }
                } else {
                    return Err(ParseError::UnexpectedToken {
                        expected: "LIKE option",
                        actual: part.as_str().into(),
                    });
                };
                options.push(option);
            }
            _ => {}
        }
    }
    Ok(CreateTableLikeClause {
        relation_name: relation_name.ok_or(ParseError::UnexpectedEof)?,
        options,
    })
}

fn build_table_constraint(pair: Pair<'_, Rule>) -> Result<TableConstraint, ParseError> {
    let inner = pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
    build_table_constraint_inner(inner)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedReferencesClause {
    referenced_table: String,
    referenced_columns: Option<Vec<String>>,
    match_type: ForeignKeyMatchType,
    on_delete: ForeignKeyAction,
    on_delete_set_columns: Option<Vec<String>>,
    on_update: ForeignKeyAction,
}

fn build_table_constraint_inner(pair: Pair<'_, Rule>) -> Result<TableConstraint, ParseError> {
    let rule = pair.as_rule();
    if rule == Rule::named_table_constraint {
        let mut name = None;
        let mut constraint = None;
        for part in pair.into_inner() {
            match part.as_rule() {
                Rule::identifier if name.is_none() => name = Some(build_identifier(part)),
                Rule::primary_key_table_constraint
                | Rule::unique_table_constraint
                | Rule::check_table_constraint
                | Rule::not_null_table_constraint
                | Rule::foreign_key_table_constraint => {
                    constraint = Some(build_table_constraint_inner(part)?)
                }
                _ => {}
            }
        }
        let mut constraint = constraint.ok_or(ParseError::UnexpectedEof)?;
        set_table_constraint_name(&mut constraint, name.ok_or(ParseError::UnexpectedEof)?);
        return Ok(constraint);
    }

    let attributes = build_constraint_attributes(pair.clone())?;
    match rule {
        Rule::primary_key_table_constraint => {
            let body = pair
                .into_inner()
                .find(|part| part.as_rule() == Rule::primary_key_table_constraint_body)
                .ok_or(ParseError::UnexpectedEof)?;
            Ok(TableConstraint::PrimaryKey {
                attributes,
                columns: body
                    .into_inner()
                    .find(|part| part.as_rule() == Rule::ident_list)
                    .map(|part| part.into_inner().map(build_identifier).collect())
                    .unwrap_or_default(),
            })
        }
        Rule::unique_table_constraint => {
            let body = pair
                .into_inner()
                .find(|part| part.as_rule() == Rule::unique_table_constraint_body)
                .ok_or(ParseError::UnexpectedEof)?;
            let nulls_not_distinct = body
                .clone()
                .into_inner()
                .any(|part| part.as_rule() == Rule::unique_nulls_not_distinct_clause);
            let mut attributes = attributes;
            attributes.nulls_not_distinct = nulls_not_distinct;
            Ok(TableConstraint::Unique {
                attributes,
                columns: body
                    .into_inner()
                    .find(|part| part.as_rule() == Rule::ident_list)
                    .map(|part| part.into_inner().map(build_identifier).collect())
                    .unwrap_or_default(),
            })
        }
        Rule::check_table_constraint => {
            let body = pair
                .into_inner()
                .find(|part| part.as_rule() == Rule::check_table_constraint_body)
                .ok_or(ParseError::UnexpectedEof)?;
            let expr_sql = body
                .into_inner()
                .find(|part| part.as_rule() == Rule::expr)
                .map(|part| part.as_str().trim().to_string())
                .ok_or(ParseError::UnexpectedEof)?;
            Ok(TableConstraint::Check {
                attributes,
                expr_sql,
            })
        }
        Rule::not_null_table_constraint => {
            let body = pair
                .into_inner()
                .find(|part| part.as_rule() == Rule::not_null_table_constraint_body)
                .ok_or(ParseError::UnexpectedEof)?;
            let column = body
                .into_inner()
                .find(|part| part.as_rule() == Rule::identifier)
                .map(build_identifier)
                .ok_or(ParseError::UnexpectedEof)?;
            Ok(TableConstraint::NotNull { attributes, column })
        }
        Rule::foreign_key_table_constraint => {
            let body = pair
                .into_inner()
                .find(|part| part.as_rule() == Rule::foreign_key_table_constraint_body)
                .ok_or(ParseError::UnexpectedEof)?;
            let mut columns = None;
            let mut references = None;
            for part in body.into_inner() {
                match part.as_rule() {
                    Rule::ident_list if columns.is_none() => {
                        columns = Some(part.into_inner().map(build_identifier).collect())
                    }
                    Rule::references_clause => references = Some(build_references_clause(part)?),
                    _ => {}
                }
            }
            let references = references.ok_or(ParseError::UnexpectedEof)?;
            Ok(TableConstraint::ForeignKey {
                attributes,
                columns: columns.ok_or(ParseError::UnexpectedEof)?,
                referenced_table: references.referenced_table,
                referenced_columns: references.referenced_columns,
                match_type: references.match_type,
                on_delete: references.on_delete,
                on_delete_set_columns: references.on_delete_set_columns,
                on_update: references.on_update,
            })
        }
        _ => Err(ParseError::UnexpectedToken {
            expected: "table constraint",
            actual: pair.as_str().to_string(),
        }),
    }
}

fn set_enforced_attribute(enforced: &mut Option<bool>, value: bool) -> Result<(), ParseError> {
    if enforced.is_some() {
        return Err(ParseError::FeatureNotSupportedMessage(
            "multiple ENFORCED/NOT ENFORCED clauses not allowed".into(),
        ));
    }
    *enforced = Some(value);
    Ok(())
}

fn build_constraint_attributes(pair: Pair<'_, Rule>) -> Result<ConstraintAttributes, ParseError> {
    let mut attributes = ConstraintAttributes::default();
    for part in pair.into_inner() {
        if part.as_rule() != Rule::constraint_attribute {
            continue;
        }
        let attr = part
            .into_inner()
            .next()
            .expect("constraint attribute inner");
        match attr.as_rule() {
            Rule::not_valid_constraint_attribute => attributes.not_valid = true,
            Rule::no_inherit_constraint_attribute => attributes.no_inherit = true,
            Rule::deferrable_constraint_attribute => attributes.deferrable = Some(true),
            Rule::not_deferrable_constraint_attribute => attributes.deferrable = Some(false),
            Rule::initially_deferred_constraint_attribute => {
                attributes.initially_deferred = Some(true)
            }
            Rule::initially_immediate_constraint_attribute => {
                attributes.initially_deferred = Some(false)
            }
            Rule::enforced_constraint_attribute => {
                set_enforced_attribute(&mut attributes.enforced, true)?
            }
            Rule::not_enforced_constraint_attribute => {
                set_enforced_attribute(&mut attributes.enforced, false)?
            }
            _ => {}
        }
    }
    Ok(attributes)
}

fn set_table_constraint_name(constraint: &mut TableConstraint, name: String) {
    match constraint {
        TableConstraint::NotNull { attributes, .. }
        | TableConstraint::Check { attributes, .. }
        | TableConstraint::PrimaryKey { attributes, .. }
        | TableConstraint::Unique { attributes, .. }
        | TableConstraint::ForeignKey { attributes, .. } => attributes.name = Some(name),
    }
}

fn build_column_constraint(pair: Pair<'_, Rule>) -> Result<ColumnConstraint, ParseError> {
    let rule = pair.as_rule();
    if rule == Rule::named_column_constraint {
        let mut name = None;
        let mut constraint = None;
        for part in pair.into_inner() {
            match part.as_rule() {
                Rule::identifier if name.is_none() => name = Some(build_identifier(part)),
                Rule::not_null_column_constraint
                | Rule::check_column_constraint
                | Rule::primary_key_column_constraint
                | Rule::unique_column_constraint
                | Rule::references_column_constraint => {
                    constraint = Some(build_column_constraint(part)?)
                }
                _ => {}
            }
        }
        let mut constraint = constraint.ok_or(ParseError::UnexpectedEof)?;
        set_column_constraint_name(&mut constraint, name.ok_or(ParseError::UnexpectedEof)?);
        return Ok(constraint);
    }

    let attributes = build_constraint_attributes(pair.clone())?;
    match rule {
        Rule::not_null_column_constraint => Ok(ColumnConstraint::NotNull { attributes }),
        Rule::check_column_constraint => {
            let body = pair
                .into_inner()
                .find(|part| part.as_rule() == Rule::check_column_constraint_body)
                .ok_or(ParseError::UnexpectedEof)?;
            let expr_sql = body
                .into_inner()
                .find(|part| part.as_rule() == Rule::expr)
                .map(|part| part.as_str().trim().to_string())
                .ok_or(ParseError::UnexpectedEof)?;
            Ok(ColumnConstraint::Check {
                attributes,
                expr_sql,
            })
        }
        Rule::primary_key_column_constraint => Ok(ColumnConstraint::PrimaryKey { attributes }),
        Rule::unique_column_constraint => {
            let body = pair
                .into_inner()
                .find(|part| part.as_rule() == Rule::unique_column_constraint_body)
                .ok_or(ParseError::UnexpectedEof)?;
            let mut attributes = attributes;
            attributes.nulls_not_distinct = body
                .into_inner()
                .any(|part| part.as_rule() == Rule::unique_nulls_not_distinct_clause);
            Ok(ColumnConstraint::Unique { attributes })
        }
        Rule::references_column_constraint => {
            let body = pair
                .into_inner()
                .find(|part| part.as_rule() == Rule::references_column_constraint_body)
                .ok_or(ParseError::UnexpectedEof)?;
            let references = body
                .into_inner()
                .find(|part| part.as_rule() == Rule::references_clause)
                .map(build_references_clause)
                .transpose()?
                .ok_or(ParseError::UnexpectedEof)?;
            Ok(ColumnConstraint::References {
                attributes,
                referenced_table: references.referenced_table,
                referenced_columns: references.referenced_columns,
                match_type: references.match_type,
                on_delete: references.on_delete,
                on_delete_set_columns: references.on_delete_set_columns,
                on_update: references.on_update,
            })
        }
        _ => Err(ParseError::UnexpectedToken {
            expected: "column constraint",
            actual: pair.as_str().to_string(),
        }),
    }
}

fn build_references_clause(pair: Pair<'_, Rule>) -> Result<ParsedReferencesClause, ParseError> {
    let mut referenced_table = None;
    let mut referenced_columns = None;
    let mut match_type = ForeignKeyMatchType::Simple;
    let mut on_delete = ForeignKeyAction::NoAction;
    let mut on_delete_set_columns = None;
    let mut on_update = ForeignKeyAction::NoAction;

    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier if referenced_table.is_none() => {
                referenced_table = Some(build_identifier(part));
            }
            Rule::referenced_columns_clause => {
                referenced_columns = Some(
                    part.into_inner()
                        .find(|inner| inner.as_rule() == Rule::ident_list)
                        .map(|inner| inner.into_inner().map(build_identifier).collect())
                        .unwrap_or_default(),
                );
            }
            Rule::match_clause => {
                let text = part.as_str();
                let lower = text.to_ascii_lowercase();
                match_type = if lower.ends_with("full") {
                    ForeignKeyMatchType::Full
                } else if lower.ends_with("partial") {
                    ForeignKeyMatchType::Partial
                } else if lower.ends_with("simple") {
                    ForeignKeyMatchType::Simple
                } else {
                    return Err(ParseError::UnexpectedToken {
                        expected: "foreign-key match type",
                        actual: text.to_string(),
                    });
                };
            }
            Rule::reference_action_clause => {
                let (delete_action, action, set_columns) = build_reference_action_clause(part)?;
                if delete_action {
                    on_delete = action;
                    on_delete_set_columns = set_columns;
                } else {
                    on_update = action;
                }
            }
            _ => {}
        }
    }

    Ok(ParsedReferencesClause {
        referenced_table: referenced_table.ok_or(ParseError::UnexpectedEof)?,
        referenced_columns,
        match_type,
        on_delete,
        on_delete_set_columns,
        on_update,
    })
}

fn build_reference_action(pair: Pair<'_, Rule>) -> Result<ForeignKeyAction, ParseError> {
    build_reference_action_text(pair.as_str())
}

fn build_reference_action_clause(
    pair: Pair<'_, Rule>,
) -> Result<(bool, ForeignKeyAction, Option<Vec<String>>), ParseError> {
    let inner = pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
    let text = inner.as_str().trim();
    let lower = text.to_ascii_lowercase();
    match inner.as_rule() {
        Rule::delete_action_clause => {
            let action =
                lower
                    .strip_prefix("on delete ")
                    .ok_or_else(|| ParseError::UnexpectedToken {
                        expected: "ON DELETE action",
                        actual: text.to_string(),
                    })?;
            let set_columns = inner
                .into_inner()
                .find(|part| part.as_rule() == Rule::delete_reference_action)
                .and_then(|part| {
                    part.into_inner()
                        .find(|inner| inner.as_rule() == Rule::delete_set_columns_clause)
                })
                .map(build_ident_list_clause);
            Ok((
                true,
                build_reference_action_text(action.split('(').next().unwrap_or(action).trim())?,
                set_columns,
            ))
        }
        Rule::update_action_clause => {
            let action =
                lower
                    .strip_prefix("on update ")
                    .ok_or_else(|| ParseError::UnexpectedToken {
                        expected: "ON UPDATE action",
                        actual: text.to_string(),
                    })?;
            if action.contains('(') {
                return Err(ParseError::FeatureNotSupportedMessage(
                    "a column list with SET NULL is only supported for ON DELETE actions".into(),
                ));
            }
            Ok((false, build_reference_action_text(action.trim())?, None))
        }
        _ => Err(ParseError::UnexpectedToken {
            expected: "foreign-key action clause",
            actual: text.to_string(),
        }),
    }
}

fn build_ident_list_clause(pair: Pair<'_, Rule>) -> Vec<String> {
    pair.into_inner()
        .find(|inner| inner.as_rule() == Rule::ident_list)
        .map(|inner| inner.into_inner().map(build_identifier).collect())
        .unwrap_or_default()
}

fn build_reference_action_text(text: &str) -> Result<ForeignKeyAction, ParseError> {
    if text.eq_ignore_ascii_case("no action") {
        Ok(ForeignKeyAction::NoAction)
    } else if text.eq_ignore_ascii_case("restrict") {
        Ok(ForeignKeyAction::Restrict)
    } else if text.eq_ignore_ascii_case("cascade") {
        Ok(ForeignKeyAction::Cascade)
    } else if text.eq_ignore_ascii_case("set null") {
        Ok(ForeignKeyAction::SetNull)
    } else if text.eq_ignore_ascii_case("set default") {
        Ok(ForeignKeyAction::SetDefault)
    } else {
        Err(ParseError::UnexpectedToken {
            expected: "foreign-key action",
            actual: text.to_string(),
        })
    }
}

fn set_column_constraint_name(constraint: &mut ColumnConstraint, name: String) {
    match constraint {
        ColumnConstraint::NotNull { attributes }
        | ColumnConstraint::Check { attributes, .. }
        | ColumnConstraint::PrimaryKey { attributes }
        | ColumnConstraint::Unique { attributes }
        | ColumnConstraint::References { attributes, .. } => attributes.name = Some(name),
    }
}

fn build_create_index(pair: Pair<'_, Rule>) -> Result<CreateIndexStatement, ParseError> {
    let raw = pair.as_str().to_ascii_lowercase();
    let unique = raw.starts_with("create unique index");
    let mut if_not_exists = false;
    let mut index_name = None;
    let mut table_name = None;
    let mut using_method = None;
    let mut columns = Vec::new();
    let mut include_columns = Vec::new();
    let mut predicate = None;
    let mut predicate_sql = None;
    let mut options = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::if_not_exists_clause => if_not_exists = true,
            Rule::create_index_name if index_name.is_none() => {
                index_name = Some(build_identifier(
                    part.into_inner().next().ok_or(ParseError::UnexpectedEof)?,
                ))
            }
            Rule::identifier if table_name.is_none() => table_name = Some(build_identifier(part)),
            Rule::create_index_using_clause => {
                using_method = part
                    .into_inner()
                    .find(|inner| inner.as_rule() == Rule::identifier)
                    .map(build_identifier);
            }
            Rule::create_index_item => columns.push(build_create_index_item(part)?),
            Rule::create_index_include_clause => {
                include_columns.extend(
                    part.into_inner()
                        .filter(|inner| inner.as_rule() == Rule::ident_list)
                        .flat_map(|inner| inner.into_inner().map(build_identifier)),
                );
            }
            Rule::create_index_where_clause => {
                let expr = part
                    .into_inner()
                    .find(|inner| inner.as_rule() == Rule::expr)
                    .ok_or(ParseError::UnexpectedEof)?;
                predicate_sql = Some(expr.as_str().trim().to_string());
                predicate = Some(build_expr(expr)?);
            }
            Rule::create_index_with_clause => {
                for option in part
                    .into_inner()
                    .filter(|inner| inner.as_rule() == Rule::reloption)
                {
                    options.push(build_reloption(option)?);
                }
            }
            _ => {}
        }
    }
    if columns.is_empty() {
        return Err(ParseError::UnexpectedEof);
    }
    if if_not_exists && index_name.is_none() {
        return Err(ParseError::UnexpectedToken {
            expected: "index name after IF NOT EXISTS",
            actual: "syntax error at or near \"ON\"".into(),
        });
    }
    Ok(CreateIndexStatement {
        unique,
        if_not_exists,
        index_name: index_name.unwrap_or_default(),
        table_name: table_name.ok_or(ParseError::UnexpectedEof)?,
        using_method,
        columns,
        include_columns,
        predicate,
        predicate_sql,
        options,
    })
}

fn build_create_index_item(pair: Pair<'_, Rule>) -> Result<IndexColumnDef, ParseError> {
    let mut name = None;
    let mut expr_sql = None;
    let mut opclass = None;
    let mut descending = false;
    let mut nulls_first = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::create_index_target => {
                let inner = part.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
                match inner.as_rule() {
                    Rule::identifier if name.is_none() => name = Some(build_identifier(inner)),
                    Rule::create_index_expression if expr_sql.is_none() => {
                        let expr = inner
                            .into_inner()
                            .find(|inner| inner.as_rule() == Rule::expr)
                            .ok_or(ParseError::UnexpectedEof)?;
                        expr_sql = Some(expr.as_str().to_string());
                    }
                    _ => {}
                }
            }
            Rule::create_index_opclass if opclass.is_none() => {
                opclass = Some(build_identifier(
                    part.into_inner().next().ok_or(ParseError::UnexpectedEof)?,
                ))
            }
            Rule::kw_desc => descending = true,
            Rule::nulls_ordering => {
                let text = part.as_str().to_ascii_lowercase();
                nulls_first = Some(text.contains("first"));
            }
            _ => {}
        }
    }
    Ok(IndexColumnDef {
        name: name.unwrap_or_default(),
        expr_sql,
        expr_type: None,
        collation: None,
        opclass,
        descending,
        nulls_first,
    })
}

fn build_alter_table_set(pair: Pair<'_, Rule>) -> Result<AlterTableSetStatement, ParseError> {
    let mut if_exists = false;
    let mut only = false;
    let mut table_name = None;
    let mut options = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::alter_table_target => {
                let (parsed_if_exists, parsed_only, parsed_table_name) =
                    build_alter_table_target(part)?;
                if_exists = parsed_if_exists;
                only = parsed_only;
                table_name = Some(parsed_table_name);
            }
            Rule::identifier if table_name.is_none() => table_name = Some(build_identifier(part)),
            Rule::reloption => options.push(build_reloption(part)?),
            _ => {}
        }
    }
    Ok(AlterTableSetStatement {
        if_exists,
        only,
        table_name: table_name.ok_or(ParseError::UnexpectedEof)?,
        options,
    })
}

fn build_alter_table_set_row_security(
    pair: Pair<'_, Rule>,
) -> Result<AlterTableSetRowSecurityStatement, ParseError> {
    let mut if_exists = false;
    let mut only = false;
    let mut table_name = None;
    let mut action = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::alter_table_target => {
                let (parsed_if_exists, parsed_only, parsed_table_name) =
                    build_alter_table_target(part)?;
                if_exists = parsed_if_exists;
                only = parsed_only;
                table_name = Some(parsed_table_name);
            }
            Rule::alter_table_row_security_action => {
                action = Some(build_alter_table_row_security_action(part)?);
            }
            Rule::identifier if table_name.is_none() => table_name = Some(build_identifier(part)),
            _ => {}
        }
    }
    Ok(AlterTableSetRowSecurityStatement {
        if_exists,
        only,
        table_name: table_name.ok_or(ParseError::UnexpectedEof)?,
        action: action.ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_alter_table_row_security_action(
    pair: Pair<'_, Rule>,
) -> Result<AlterTableRowSecurityAction, ParseError> {
    let lowered = pair.as_str().to_ascii_lowercase();
    if lowered.starts_with("enable ") {
        return Ok(AlterTableRowSecurityAction::Enable);
    }
    if lowered.starts_with("disable ") {
        return Ok(AlterTableRowSecurityAction::Disable);
    }
    if lowered.starts_with("no force ") {
        return Ok(AlterTableRowSecurityAction::NoForce);
    }
    if lowered.starts_with("force ") {
        return Ok(AlterTableRowSecurityAction::Force);
    }
    Err(ParseError::UnexpectedEof)
}

fn build_comment_on_table(pair: Pair<'_, Rule>) -> Result<CommentOnTableStatement, ParseError> {
    let mut table_name = None;
    let mut comment = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier => table_name = Some(build_identifier(part)),
            Rule::quoted_string_literal
            | Rule::string_literal
            | Rule::unicode_string_literal
            | Rule::escape_string_literal
            | Rule::dollar_string_literal => {
                comment = Some(Some(decode_string_literal_pair(part)?))
            }
            Rule::kw_null => comment = Some(None),
            _ => {}
        }
    }
    Ok(CommentOnTableStatement {
        table_name: table_name.ok_or(ParseError::UnexpectedEof)?,
        comment: comment.ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_comment_on_index(pair: Pair<'_, Rule>) -> Result<CommentOnIndexStatement, ParseError> {
    let mut index_name = None;
    let mut comment = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier => index_name = Some(build_identifier(part)),
            Rule::quoted_string_literal
            | Rule::string_literal
            | Rule::unicode_string_literal
            | Rule::escape_string_literal
            | Rule::dollar_string_literal => {
                comment = Some(Some(decode_string_literal_pair(part)?))
            }
            Rule::kw_null => comment = Some(None),
            _ => {}
        }
    }
    Ok(CommentOnIndexStatement {
        index_name: index_name.ok_or(ParseError::UnexpectedEof)?,
        comment: comment.ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_comment_on_rule(pair: Pair<'_, Rule>) -> Result<CommentOnRuleStatement, ParseError> {
    let mut rule_name = None;
    let mut relation_name = None;
    let mut comment = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier if rule_name.is_none() => rule_name = Some(build_identifier(part)),
            Rule::identifier if relation_name.is_none() => {
                relation_name = Some(build_identifier(part));
            }
            Rule::quoted_string_literal
            | Rule::string_literal
            | Rule::unicode_string_literal
            | Rule::escape_string_literal
            | Rule::dollar_string_literal => {
                comment = Some(Some(decode_string_literal_pair(part)?))
            }
            Rule::kw_null => comment = Some(None),
            _ => {}
        }
    }
    Ok(CommentOnRuleStatement {
        rule_name: rule_name.ok_or(ParseError::UnexpectedEof)?,
        relation_name: relation_name.ok_or(ParseError::UnexpectedEof)?,
        comment: comment.ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_comment_on_trigger(pair: Pair<'_, Rule>) -> Result<CommentOnTriggerStatement, ParseError> {
    let mut trigger_name = None;
    let mut table_name = None;
    let mut comment = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier if trigger_name.is_none() => {
                trigger_name = Some(build_identifier(part))
            }
            Rule::identifier if table_name.is_none() => table_name = Some(build_identifier(part)),
            Rule::quoted_string_literal
            | Rule::string_literal
            | Rule::unicode_string_literal
            | Rule::escape_string_literal
            | Rule::dollar_string_literal => {
                comment = Some(Some(decode_string_literal_pair(part)?))
            }
            Rule::kw_null => comment = Some(None),
            _ => {}
        }
    }
    Ok(CommentOnTriggerStatement {
        trigger_name: trigger_name.ok_or(ParseError::UnexpectedEof)?,
        table_name: table_name.ok_or(ParseError::UnexpectedEof)?,
        comment: comment.ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_comment_on_role(pair: Pair<'_, Rule>) -> Result<CommentOnRoleStatement, ParseError> {
    let mut role_name = None;
    let mut comment = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier => role_name = Some(build_identifier(part)),
            Rule::quoted_string_literal
            | Rule::string_literal
            | Rule::unicode_string_literal
            | Rule::escape_string_literal
            | Rule::dollar_string_literal => {
                comment = Some(Some(decode_string_literal_pair(part)?))
            }
            Rule::kw_null => comment = Some(None),
            _ => {}
        }
    }
    Ok(CommentOnRoleStatement {
        role_name: role_name.ok_or(ParseError::UnexpectedEof)?,
        comment: comment.ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_reloption(pair: Pair<'_, Rule>) -> Result<RelOption, ParseError> {
    let mut name = None;
    let mut value = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier if name.is_none() => name = Some(build_identifier(part)),
            Rule::set_value_atom => value = Some(build_set_value_atom(part)?),
            _ => {}
        }
    }
    Ok(RelOption {
        name: name.ok_or(ParseError::UnexpectedEof)?,
        value: value.ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_set_value_atom(pair: Pair<'_, Rule>) -> Result<String, ParseError> {
    let mut inner = pair.into_inner();
    let part = inner.next().ok_or(ParseError::UnexpectedEof)?;
    match part.as_rule() {
        Rule::signed_set_value => Ok(part.as_str().to_string()),
        Rule::quoted_string_literal
        | Rule::string_literal
        | Rule::unicode_string_literal
        | Rule::escape_string_literal
        | Rule::dollar_string_literal => decode_string_literal_pair(part),
        Rule::identifier | Rule::numeric_literal | Rule::integer => Ok(part.as_str().to_string()),
        Rule::kw_default | Rule::kw_true | Rule::kw_false | Rule::kw_on_value | Rule::kw_off => {
            Ok(part.as_str().to_ascii_lowercase())
        }
        _ => Ok(part.as_str().to_string()),
    }
}

fn validate_table_storage_clause(pair: Pair<'_, Rule>) -> Result<(), ParseError> {
    let part = pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
    match part.as_rule() {
        Rule::without_oids_clause => Ok(()),
        Rule::table_with_clause => {
            for item in part
                .into_inner()
                .filter(|inner| inner.as_rule() == Rule::table_with_item)
            {
                let mut item_parts = item.into_inner();
                let name = build_identifier(item_parts.next().ok_or(ParseError::UnexpectedEof)?);
                let value = item_parts
                    .next()
                    .map(|value| value.as_str().to_ascii_lowercase());
                if name != name.to_ascii_lowercase() {
                    return Err(ParseError::UnrecognizedParameter(name));
                }
                if name.eq_ignore_ascii_case("oids")
                    && value
                        .as_deref()
                        .map_or(true, |value| matches!(value, "true" | "on" | "1"))
                {
                    return Err(ParseError::TablesDeclaredWithOidsNotSupported);
                }
            }
            Ok(())
        }
        _ => Err(ParseError::UnexpectedToken {
            expected: "table storage clause",
            actual: part.as_str().to_string(),
        }),
    }
}

fn build_relation_name(pair: Pair<'_, Rule>) -> (Option<String>, String) {
    let name = build_identifier(pair);
    if let Some((schema, rel)) = name.split_once('.') {
        (Some(schema.to_string()), rel.to_string())
    } else {
        (None, name)
    }
}

fn build_on_commit_action(pair: Pair<'_, Rule>) -> Result<OnCommitAction, ParseError> {
    let action = pair
        .into_inner()
        .find(|part| part.as_rule() == Rule::on_commit_action)
        .ok_or(ParseError::UnexpectedEof)?;
    let text = action.as_str();
    if text.eq_ignore_ascii_case("drop") {
        Ok(OnCommitAction::Drop)
    } else if text.eq_ignore_ascii_case("delete rows") {
        Ok(OnCommitAction::DeleteRows)
    } else {
        Ok(OnCommitAction::PreserveRows)
    }
}

fn build_drop_table(pair: Pair<'_, Rule>) -> Result<DropTableStatement, ParseError> {
    let mut if_exists = false;
    let mut table_names = Vec::new();
    let mut cascade = false;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::if_exists_clause => if_exists = true,
            Rule::ident_list => {
                table_names.extend(part.into_inner().map(build_identifier));
            }
            Rule::identifier => table_names.push(build_identifier(part)),
            Rule::drop_behavior => {
                cascade = part.as_str().eq_ignore_ascii_case("cascade");
            }
            _ => {}
        }
    }
    if table_names.is_empty() {
        return Err(ParseError::UnexpectedEof);
    }
    Ok(DropTableStatement {
        if_exists,
        table_names,
        cascade,
    })
}

fn build_drop_role(pair: Pair<'_, Rule>) -> Result<DropRoleStatement, ParseError> {
    let mut if_exists = false;
    let mut role_names = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::if_exists_clause => if_exists = true,
            Rule::ident_list => role_names.extend(part.into_inner().map(build_identifier)),
            Rule::identifier => role_names.push(build_identifier(part)),
            _ => {}
        }
    }
    if role_names.is_empty() {
        return Err(ParseError::UnexpectedEof);
    }
    Ok(DropRoleStatement {
        if_exists,
        role_names,
    })
}

fn build_drop_database(pair: Pair<'_, Rule>) -> Result<DropDatabaseStatement, ParseError> {
    let mut if_exists = false;
    let mut database_name = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::if_exists_clause => if_exists = true,
            Rule::identifier if database_name.is_none() => {
                database_name = Some(build_identifier(part));
            }
            _ => {}
        }
    }
    Ok(DropDatabaseStatement {
        if_exists,
        database_name: database_name.ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_drop_index(pair: Pair<'_, Rule>) -> Result<DropIndexStatement, ParseError> {
    let mut if_exists = false;
    let mut index_names = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::if_exists_clause => if_exists = true,
            Rule::ident_list => index_names.extend(part.into_inner().map(build_identifier)),
            Rule::identifier => index_names.push(build_identifier(part)),
            _ => {}
        }
    }
    if index_names.is_empty() {
        return Err(ParseError::UnexpectedEof);
    }
    Ok(DropIndexStatement {
        if_exists,
        index_names,
    })
}

fn build_drop_view(pair: Pair<'_, Rule>) -> Result<DropViewStatement, ParseError> {
    let mut if_exists = false;
    let mut view_names = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::if_exists_clause => if_exists = true,
            Rule::ident_list => {
                view_names.extend(part.into_inner().map(build_identifier));
            }
            Rule::identifier => view_names.push(build_identifier(part)),
            _ => {}
        }
    }
    if view_names.is_empty() {
        return Err(ParseError::UnexpectedEof);
    }
    Ok(DropViewStatement {
        if_exists,
        view_names,
    })
}

fn build_drop_rule(pair: Pair<'_, Rule>) -> Result<DropRuleStatement, ParseError> {
    let mut if_exists = false;
    let mut rule_name = None;
    let mut relation_name = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::if_exists_clause => if_exists = true,
            Rule::identifier if rule_name.is_none() => rule_name = Some(build_identifier(part)),
            Rule::identifier if relation_name.is_none() => {
                relation_name = Some(build_identifier(part));
            }
            _ => {}
        }
    }
    Ok(DropRuleStatement {
        if_exists,
        rule_name: rule_name.ok_or(ParseError::UnexpectedEof)?,
        relation_name: relation_name.ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_drop_schema(pair: Pair<'_, Rule>) -> Result<DropSchemaStatement, ParseError> {
    let mut if_exists = false;
    let mut schema_names = Vec::new();
    let mut cascade = false;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::if_exists_clause => if_exists = true,
            Rule::ident_list => schema_names.extend(part.into_inner().map(build_identifier)),
            Rule::identifier => schema_names.push(build_identifier(part)),
            Rule::drop_behavior => cascade = part.as_str().eq_ignore_ascii_case("cascade"),
            _ => {}
        }
    }
    if schema_names.is_empty() {
        return Err(ParseError::UnexpectedEof);
    }
    Ok(DropSchemaStatement {
        if_exists,
        schema_names,
        cascade,
    })
}

fn build_reassign_owned(pair: Pair<'_, Rule>) -> Result<ReassignOwnedStatement, ParseError> {
    let mut old_roles = Vec::new();
    let mut new_role = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::ident_list if old_roles.is_empty() => {
                old_roles.extend(part.into_inner().map(build_identifier));
            }
            Rule::identifier if new_role.is_none() && !old_roles.is_empty() => {
                new_role = Some(build_identifier(part));
            }
            Rule::identifier => old_roles.push(build_identifier(part)),
            _ => {}
        }
    }
    Ok(ReassignOwnedStatement {
        old_roles,
        new_role: new_role.ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_drop_owned(pair: Pair<'_, Rule>) -> Result<DropOwnedStatement, ParseError> {
    let mut role_names = Vec::new();
    let mut cascade = false;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::ident_list => role_names.extend(part.into_inner().map(build_identifier)),
            Rule::identifier => role_names.push(build_identifier(part)),
            Rule::drop_behavior => cascade = part.as_str().eq_ignore_ascii_case("cascade"),
            _ => {}
        }
    }
    if role_names.is_empty() {
        return Err(ParseError::UnexpectedEof);
    }
    Ok(DropOwnedStatement {
        role_names,
        cascade,
    })
}

fn build_truncate_table(pair: Pair<'_, Rule>) -> Result<TruncateTableStatement, ParseError> {
    let table_names = pair
        .into_inner()
        .find(|part| part.as_rule() == Rule::ident_list)
        .map(|part| part.into_inner().map(build_identifier).collect::<Vec<_>>())
        .ok_or(ParseError::UnexpectedEof)?;
    Ok(TruncateTableStatement { table_names })
}

fn build_vacuum(pair: Pair<'_, Rule>) -> Result<VacuumStatement, ParseError> {
    let mut targets = Vec::new();
    let mut analyze = false;
    let mut full = false;
    let mut verbose = false;
    let mut skip_locked = false;
    let mut buffer_usage_limit = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::kw_analyze => analyze = true,
            Rule::vacuum_option_block => {
                for opt in part.into_inner() {
                    let opt = if opt.as_rule() == Rule::vacuum_option {
                        opt.into_inner().next().ok_or(ParseError::UnexpectedEof)?
                    } else {
                        opt
                    };
                    match opt.as_rule() {
                        Rule::vacuum_analyze_option => analyze = parse_option_bool(opt)?,
                        Rule::vacuum_full_option => full = parse_option_bool(opt)?,
                        Rule::analyze_verbose_option => verbose = parse_option_bool(opt)?,
                        Rule::analyze_skip_locked_option => skip_locked = parse_option_bool(opt)?,
                        Rule::analyze_buffer_usage_limit_option => {
                            buffer_usage_limit = Some(parse_option_scalar(opt)?)
                        }
                        _ => {}
                    }
                }
            }
            Rule::maintenance_target_list => targets = build_maintenance_target_list(part)?,
            _ => {}
        }
    }
    Ok(VacuumStatement {
        targets,
        analyze,
        full,
        verbose,
        skip_locked,
        buffer_usage_limit,
    })
}

fn build_maintenance_target_list(
    pair: Pair<'_, Rule>,
) -> Result<Vec<MaintenanceTarget>, ParseError> {
    pair.into_inner()
        .filter(|part| part.as_rule() == Rule::maintenance_target)
        .map(build_maintenance_target)
        .collect()
}

fn build_maintenance_target(pair: Pair<'_, Rule>) -> Result<MaintenanceTarget, ParseError> {
    let mut only = false;
    let mut table_name = None;
    let mut columns = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::only_clause => only = true,
            Rule::identifier if table_name.is_none() => table_name = Some(build_identifier(part)),
            Rule::maintenance_column_list => {
                columns = part
                    .into_inner()
                    .find(|p| p.as_rule() == Rule::ident_list)
                    .map(|p| p.into_inner().map(build_identifier).collect())
                    .unwrap_or_default();
            }
            _ => {}
        }
    }
    Ok(MaintenanceTarget {
        table_name: table_name.ok_or(ParseError::UnexpectedEof)?,
        columns,
        only,
    })
}

fn build_update(pair: Pair<'_, Rule>) -> Result<UpdateStatement, ParseError> {
    let mut with_recursive = false;
    let mut with = Vec::new();
    let mut table_name = None;
    let mut target_alias = None;
    let mut only = false;
    let mut assignments = Vec::new();
    let mut from = None;
    let mut where_clause = None;
    let mut returning = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::cte_clause => {
                let (recursive, ctes) = build_cte_clause(part)?;
                with_recursive = recursive;
                with = ctes;
            }
            Rule::update_target => {
                let (parsed_table_name, parsed_alias, parsed_only) = build_update_target(part)?;
                table_name = Some(parsed_table_name);
                target_alias = parsed_alias;
                only = parsed_only;
            }
            Rule::from_item => from = Some(build_from_item(part)?),
            Rule::assignment => assignments.push(build_assignment(part)?),
            Rule::expr => where_clause = Some(build_expr(part)?),
            Rule::returning_clause => returning = build_returning_clause(part)?,
            _ => {}
        }
    }
    Ok(UpdateStatement {
        with_recursive,
        with,
        table_name: table_name.ok_or(ParseError::UnexpectedEof)?,
        target_alias,
        only,
        assignments,
        from,
        where_clause,
        returning,
    })
}

fn build_update_target(pair: Pair<'_, Rule>) -> Result<(String, Option<String>, bool), ParseError> {
    let mut table_name = None;
    let mut alias = None;
    let mut only = false;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::only_clause => only = true,
            Rule::identifier if table_name.is_none() => table_name = Some(build_identifier(part)),
            Rule::update_target_alias => {
                let alias_pair = part.into_inner().find(|inner| {
                    matches!(
                        inner.as_rule(),
                        Rule::update_alias_identifier | Rule::identifier
                    )
                });
                if let Some(alias_pair) = alias_pair {
                    alias = Some(build_identifier(alias_pair));
                }
            }
            _ => {}
        }
    }
    Ok((table_name.ok_or(ParseError::UnexpectedEof)?, alias, only))
}

fn build_delete(pair: Pair<'_, Rule>) -> Result<DeleteStatement, ParseError> {
    let mut with_recursive = false;
    let mut with = Vec::new();
    let mut table_name = None;
    let mut only = false;
    let mut where_clause = None;
    let mut returning = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::cte_clause => {
                let (recursive, ctes) = build_cte_clause(part)?;
                with_recursive = recursive;
                with = ctes;
            }
            Rule::only_clause => only = true,
            Rule::identifier if table_name.is_none() => table_name = Some(build_identifier(part)),
            Rule::expr => where_clause = Some(build_expr(part)?),
            Rule::returning_clause => returning = build_returning_clause(part)?,
            _ => {}
        }
    }
    Ok(DeleteStatement {
        with_recursive,
        with,
        table_name: table_name.ok_or(ParseError::UnexpectedEof)?,
        only,
        where_clause,
        returning,
    })
}

fn build_select_list(pair: Pair<'_, Rule>) -> Result<Vec<SelectItem>, ParseError> {
    let mut inner = pair.into_inner();
    let first = inner.next().ok_or(ParseError::EmptySelectList)?;
    if first.as_rule() == Rule::star {
        return Ok(vec![SelectItem {
            output_name: "*".into(),
            expr: SqlExpr::Column("*".into()),
        }]);
    }

    let mut items = Vec::new();
    for (index, item_pair) in std::iter::once(first).chain(inner).enumerate() {
        let mut preview_inner = item_pair.clone().into_inner();
        if let Some(first_part) = preview_inner.next() {
            if first_part.as_rule() == Rule::star {
                items.push(SelectItem {
                    output_name: "*".into(),
                    expr: SqlExpr::Column("*".into()),
                });
                continue;
            }
            if first_part.as_rule() == Rule::qualified_star {
                let relation = first_part
                    .as_str()
                    .strip_suffix(".*")
                    .ok_or(ParseError::UnexpectedEof)?
                    .to_string();
                items.push(SelectItem {
                    output_name: "*".into(),
                    expr: SqlExpr::Column(format!("{relation}.*")),
                });
                continue;
            }
        }

        let mut item_inner = item_pair.into_inner();
        let expr_pair = item_inner.next().ok_or(ParseError::UnexpectedEof)?;
        let expr_is_extract = top_level_extract_expr(expr_pair.clone());
        let expr = build_expr(expr_pair)?;
        let output_name = if let Some(alias_pair) = item_inner.next() {
            let alias = alias_pair
                .into_inner()
                .last()
                .ok_or(ParseError::UnexpectedEof)?;
            build_identifier(alias)
        } else if expr_is_extract {
            "extract".into()
        } else {
            select_item_name(&expr, index)
        };
        items.push(SelectItem { output_name, expr });
    }

    Ok(items)
}

fn build_returning_clause(pair: Pair<'_, Rule>) -> Result<Vec<SelectItem>, ParseError> {
    let select_list = pair
        .into_inner()
        .find(|part| part.as_rule() == Rule::select_list)
        .ok_or(ParseError::UnexpectedEof)?;
    build_select_list(select_list)
}

fn top_level_extract_expr(pair: Pair<'_, Rule>) -> bool {
    match pair.as_rule() {
        Rule::extract_expr => true,
        Rule::expr
        | Rule::or_expr
        | Rule::and_expr
        | Rule::not_expr
        | Rule::cmp_expr
        | Rule::json_access_expr
        | Rule::concat_expr
        | Rule::add_expr
        | Rule::bit_expr
        | Rule::shift_expr
        | Rule::pow_expr
        | Rule::mul_expr
        | Rule::unary_expr
        | Rule::positive_expr
        | Rule::negated_expr
        | Rule::postfix_expr
        | Rule::primary_expr => {
            let mut inner = pair.into_inner();
            let Some(first) = inner.next() else {
                return false;
            };
            if inner.next().is_some() {
                return false;
            }
            top_level_extract_expr(first)
        }
        _ => false,
    }
}

fn select_item_name(expr: &SqlExpr, index: usize) -> String {
    let _ = index;
    match expr {
        SqlExpr::Column(name) => name.rsplit('.').next().unwrap_or(name).to_string(),
        SqlExpr::ArrayLiteral(_) | SqlExpr::ArraySubquery(_) => "array".to_string(),
        SqlExpr::ArraySubscript { array, .. } => select_item_name(array, index),
        SqlExpr::FieldSelect { field, .. } => field.clone(),
        SqlExpr::Cast(inner, ty) => match inner.as_ref() {
            SqlExpr::Column(_) => select_item_name(inner, index),
            SqlExpr::Cast(grand_inner, _) if matches!(grand_inner.as_ref(), SqlExpr::Column(_)) => {
                select_item_name(inner, index)
            }
            _ => raw_type_output_name(ty).to_string(),
        },
        SqlExpr::Case { .. } => "case".to_string(),
        SqlExpr::Row(_) => "row".to_string(),
        SqlExpr::Random => "random".to_string(),
        SqlExpr::CurrentUser => "current_user".to_string(),
        SqlExpr::SessionUser => "session_user".to_string(),
        SqlExpr::CurrentRole => "current_role".to_string(),
        SqlExpr::FuncCall { name, .. } => name.clone(),
        _ => "?column?".to_string(),
    }
}

fn simple_func_call(name: impl Into<String>, args: Vec<SqlFunctionArg>) -> SqlExpr {
    SqlExpr::FuncCall {
        name: name.into(),
        args: SqlCallArgs::Args(args),
        order_by: Vec::new(),
        distinct: false,
        func_variadic: false,
        filter: None,
        over: None,
    }
}

fn sql_type_output_name(ty: SqlType) -> &'static str {
    match ty.kind {
        SqlTypeKind::AnyElement => "anyelement",
        SqlTypeKind::AnyRange => "anyrange",
        SqlTypeKind::AnyMultirange => "anymultirange",
        SqlTypeKind::AnyCompatible => "anycompatible",
        SqlTypeKind::AnyCompatibleArray => "anycompatiblearray",
        SqlTypeKind::AnyCompatibleRange => "anycompatiblerange",
        SqlTypeKind::AnyCompatibleMultirange => "anycompatiblemultirange",
        SqlTypeKind::Record => "record",
        SqlTypeKind::Composite => "record",
        SqlTypeKind::Trigger => "trigger",
        SqlTypeKind::Void => "void",
        SqlTypeKind::FdwHandler => "fdw_handler",
        SqlTypeKind::Int2 => "int2",
        SqlTypeKind::Int2Vector => "int2vector",
        SqlTypeKind::Int4 => "int4",
        SqlTypeKind::Range => "range",
        SqlTypeKind::Int4Range => "int4range",
        SqlTypeKind::Int8 => "int8",
        SqlTypeKind::Int8Range => "int8range",
        SqlTypeKind::Name => "name",
        SqlTypeKind::Oid => "oid",
        SqlTypeKind::RegClass => "regclass",
        SqlTypeKind::RegType => "regtype",
        SqlTypeKind::RegRole => "regrole",
        SqlTypeKind::RegOperator => "regoperator",
        SqlTypeKind::RegProcedure => "regprocedure",
        SqlTypeKind::Tid => "tid",
        SqlTypeKind::Xid => "xid",
        SqlTypeKind::OidVector => "oidvector",
        SqlTypeKind::Bit => "bit",
        SqlTypeKind::VarBit => "varbit",
        SqlTypeKind::Float4 => "float4",
        SqlTypeKind::Float8 => "float8",
        SqlTypeKind::Money => "money",
        SqlTypeKind::Numeric => "numeric",
        SqlTypeKind::NumericRange => "numrange",
        SqlTypeKind::Json => "json",
        SqlTypeKind::Jsonb => "jsonb",
        SqlTypeKind::JsonPath => "jsonpath",
        SqlTypeKind::Xml => "xml",
        SqlTypeKind::Date => "date",
        SqlTypeKind::DateRange => "daterange",
        SqlTypeKind::Time => "time without time zone",
        SqlTypeKind::TimeTz => "time with time zone",
        SqlTypeKind::Interval => "interval",
        SqlTypeKind::TsVector => "tsvector",
        SqlTypeKind::TsQuery => "tsquery",
        SqlTypeKind::RegConfig => "regconfig",
        SqlTypeKind::RegDictionary => "regdictionary",
        SqlTypeKind::AnyArray => "anyarray",
        SqlTypeKind::Multirange => "multirange",
        SqlTypeKind::Text => "text",
        SqlTypeKind::Bytea => "bytea",
        SqlTypeKind::Bool => "bool",
        SqlTypeKind::Point => "point",
        SqlTypeKind::Lseg => "lseg",
        SqlTypeKind::Path => "path",
        SqlTypeKind::Box => "box",
        SqlTypeKind::Polygon => "polygon",
        SqlTypeKind::Line => "line",
        SqlTypeKind::Circle => "circle",
        SqlTypeKind::Timestamp => "timestamp without time zone",
        SqlTypeKind::TimestampRange => "tsrange",
        SqlTypeKind::TimestampTz => "timestamp with time zone",
        SqlTypeKind::TimestampTzRange => "tstzrange",
        SqlTypeKind::PgNodeTree => "pg_node_tree",
        SqlTypeKind::Internal => "internal",
        SqlTypeKind::InternalChar => "char",
        SqlTypeKind::Char => "bpchar",
        SqlTypeKind::Varchar => "varchar",
    }
}

fn raw_type_output_name(ty: &RawTypeName) -> &str {
    match ty {
        RawTypeName::Builtin(sql_type) => sql_type_output_name(*sql_type),
        RawTypeName::Serial(SerialKind::Small) => "smallserial",
        RawTypeName::Serial(SerialKind::Regular) => "serial",
        RawTypeName::Serial(SerialKind::Big) => "bigserial",
        RawTypeName::Named { name, .. } => name.as_str(),
        RawTypeName::Record => "record",
    }
}

fn build_values_row(pair: Pair<'_, Rule>) -> Result<Vec<SqlExpr>, ParseError> {
    pair.into_inner()
        .next()
        .ok_or(ParseError::UnexpectedEof)?
        .into_inner()
        .map(build_expr)
        .collect()
}

fn build_assignment(pair: Pair<'_, Rule>) -> Result<Assignment, ParseError> {
    let mut inner = pair.into_inner();
    Ok(Assignment {
        target: build_assignment_target(inner.next().ok_or(ParseError::UnexpectedEof)?)?,
        expr: build_expr(inner.next().ok_or(ParseError::UnexpectedEof)?)?,
    })
}

fn build_assignment_target_list(pair: Pair<'_, Rule>) -> Result<Vec<AssignmentTarget>, ParseError> {
    pair.into_inner()
        .filter(|part| part.as_rule() == Rule::assignment_target)
        .map(build_assignment_target)
        .collect()
}

fn build_assignment_target(pair: Pair<'_, Rule>) -> Result<AssignmentTarget, ParseError> {
    let mut inner = pair.into_inner();
    let column = build_identifier(inner.next().ok_or(ParseError::UnexpectedEof)?);
    let mut subscripts = Vec::new();
    let mut field_path = Vec::new();
    for part in inner {
        match part.as_rule() {
            Rule::assignment_target_suffix => {
                let suffix = part.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
                match suffix.as_rule() {
                    Rule::subscript_suffix => {
                        if !field_path.is_empty() {
                            return Err(ParseError::UnexpectedToken {
                                expected: "record field selection at end of assignment target",
                                actual: suffix.as_str().into(),
                            });
                        }
                        subscripts.push(build_array_subscript(suffix)?);
                    }
                    Rule::field_select_suffix => {
                        let field = suffix
                            .into_inner()
                            .find(|part| part.as_rule() == Rule::identifier)
                            .map(build_identifier)
                            .ok_or(ParseError::UnexpectedEof)?;
                        field_path.push(field);
                    }
                    _ => {}
                }
            }
            Rule::subscript_suffix => {
                if !field_path.is_empty() {
                    return Err(ParseError::UnexpectedToken {
                        expected: "record field selection at end of assignment target",
                        actual: part.as_str().into(),
                    });
                }
                subscripts.push(build_array_subscript(part)?);
            }
            Rule::field_select_suffix => {
                let field = part
                    .into_inner()
                    .find(|inner| inner.as_rule() == Rule::identifier)
                    .map(build_identifier)
                    .ok_or(ParseError::UnexpectedEof)?;
                field_path.push(field);
            }
            _ => {}
        }
    }
    Ok(AssignmentTarget {
        column,
        subscripts,
        field_path,
    })
}

fn build_array_subscript(pair: Pair<'_, Rule>) -> Result<ArraySubscript, ParseError> {
    let raw = pair.as_str().to_string();
    let raw_inner = raw
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .unwrap_or(raw.as_str());
    let mut bounds = pair
        .into_inner()
        .filter(|part| part.as_rule() == Rule::subscript_bound)
        .map(|bound| {
            let expr = bound.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
            build_expr(expr).map(Box::new)
        });
    let has_slice = raw.contains(':');
    let lower = if has_slice && raw_inner.starts_with(':') {
        None
    } else {
        bounds.next().transpose()?
    };
    let upper = if has_slice {
        bounds.next().transpose()?
    } else {
        None
    };
    Ok(ArraySubscript {
        is_slice: has_slice,
        lower,
        upper,
    })
}

fn build_column_def(pair: Pair<'_, Rule>) -> Result<ColumnDef, ParseError> {
    let mut inner = pair.into_inner();
    let name = build_identifier(inner.next().ok_or(ParseError::UnexpectedEof)?);
    let ty = canonicalize_column_type_name(build_type_name(
        inner.next().ok_or(ParseError::UnexpectedEof)?,
    ))?;
    let mut default_expr = None;
    let mut constraints = Vec::new();
    for flag in inner {
        let Some(flag) = (match flag.as_rule() {
            Rule::column_modifier => flag.into_inner().next(),
            _ => Some(flag),
        }) else {
            continue;
        };
        match flag.as_rule() {
            Rule::column_default => {
                default_expr = flag
                    .into_inner()
                    .find(|part| matches!(part.as_rule(), Rule::expr | Rule::b_expr))
                    .map(|expr| expr.as_str().to_string());
            }
            Rule::nullable => {}
            Rule::named_column_constraint
            | Rule::not_null_column_constraint
            | Rule::check_column_constraint
            | Rule::primary_key_column_constraint
            | Rule::unique_column_constraint
            | Rule::references_column_constraint => {
                constraints.push(build_column_constraint(flag)?)
            }
            _ => {}
        }
    }
    Ok(ColumnDef {
        name,
        ty,
        default_expr,
        compression: None,
        constraints,
    })
}

fn canonicalize_column_type_name(ty: RawTypeName) -> Result<RawTypeName, ParseError> {
    match ty {
        RawTypeName::Named {
            name,
            array_bounds: 0,
        } => match name.to_ascii_lowercase().as_str() {
            "smallserial" | "serial2" => Ok(RawTypeName::Serial(SerialKind::Small)),
            "serial" | "serial4" => Ok(RawTypeName::Serial(SerialKind::Regular)),
            "bigserial" | "serial8" => Ok(RawTypeName::Serial(SerialKind::Big)),
            _ => Ok(RawTypeName::Named {
                name,
                array_bounds: 0,
            }),
        },
        RawTypeName::Named { name, array_bounds } => {
            if matches!(
                name.to_ascii_lowercase().as_str(),
                "smallserial" | "serial2" | "serial" | "serial4" | "bigserial" | "serial8"
            ) {
                return Err(ParseError::FeatureNotSupported(
                    "array of serial is not implemented".into(),
                ));
            }
            Ok(RawTypeName::Named { name, array_bounds })
        }
        other => Ok(other),
    }
}

fn build_alter_table_add_column(
    pair: Pair<'_, Rule>,
) -> Result<AlterTableAddColumnStatement, ParseError> {
    let mut if_exists = false;
    let mut only = false;
    let mut table_name = None;
    let mut column = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::alter_table_target => {
                let (parsed_if_exists, parsed_only, parsed_table_name) =
                    build_alter_table_target(part)?;
                if_exists = parsed_if_exists;
                only = parsed_only;
                table_name = Some(parsed_table_name);
            }
            Rule::identifier if table_name.is_none() => table_name = Some(build_identifier(part)),
            Rule::column_def => column = Some(build_column_def(part)?),
            _ => {}
        }
    }
    Ok(AlterTableAddColumnStatement {
        if_exists,
        only,
        table_name: table_name.ok_or(ParseError::UnexpectedEof)?,
        column: column.ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_alter_table_add_constraint(
    pair: Pair<'_, Rule>,
) -> Result<AlterTableAddConstraintStatement, ParseError> {
    let mut if_exists = false;
    let mut only = false;
    let mut table_name = None;
    let mut constraint = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::alter_table_target => {
                let (parsed_if_exists, parsed_only, parsed_table_name) =
                    build_alter_table_target(part)?;
                if_exists = parsed_if_exists;
                only = parsed_only;
                table_name = Some(parsed_table_name);
            }
            Rule::identifier if table_name.is_none() => table_name = Some(build_identifier(part)),
            Rule::table_constraint => constraint = Some(build_table_constraint(part)?),
            _ => {}
        }
    }
    Ok(AlterTableAddConstraintStatement {
        if_exists,
        only,
        table_name: table_name.ok_or(ParseError::UnexpectedEof)?,
        constraint: constraint.ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_alter_table_drop_column(
    pair: Pair<'_, Rule>,
) -> Result<AlterTableDropColumnStatement, ParseError> {
    let mut if_exists = false;
    let mut only = false;
    let mut parts = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::alter_table_target => {
                let (parsed_if_exists, parsed_only, parsed_table_name) =
                    build_alter_table_target(part)?;
                if_exists = parsed_if_exists;
                only = parsed_only;
                parts.push(parsed_table_name);
            }
            Rule::identifier => parts.push(build_identifier(part)),
            _ => {}
        }
    }
    let mut parts = parts.into_iter();
    Ok(AlterTableDropColumnStatement {
        if_exists,
        only,
        table_name: parts.next().ok_or(ParseError::UnexpectedEof)?,
        column_name: parts.next().ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_alter_table_drop_constraint(
    pair: Pair<'_, Rule>,
) -> Result<AlterTableDropConstraintStatement, ParseError> {
    let mut if_exists = false;
    let mut only = false;
    let mut parts = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::alter_table_target => {
                let (parsed_if_exists, parsed_only, parsed_table_name) =
                    build_alter_table_target(part)?;
                if_exists = parsed_if_exists;
                only = parsed_only;
                parts.push(parsed_table_name);
            }
            Rule::identifier => parts.push(build_identifier(part)),
            _ => {}
        }
    }
    let mut parts = parts.into_iter();
    Ok(AlterTableDropConstraintStatement {
        if_exists,
        only,
        table_name: parts.next().ok_or(ParseError::UnexpectedEof)?,
        constraint_name: parts.next().ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_alter_table_alter_constraint(
    pair: Pair<'_, Rule>,
) -> Result<AlterTableAlterConstraintStatement, ParseError> {
    let mut if_exists = false;
    let mut only = false;
    let mut table_name = None;
    let mut constraint_name = None;
    let mut deferrable = None;
    let mut initially_deferred = None;
    let mut enforced = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::alter_table_target => {
                let (parsed_if_exists, parsed_only, parsed_table_name) =
                    build_alter_table_target(part)?;
                if_exists = parsed_if_exists;
                only = parsed_only;
                table_name = Some(parsed_table_name);
            }
            Rule::identifier if table_name.is_none() => table_name = Some(build_identifier(part)),
            Rule::identifier if constraint_name.is_none() => {
                constraint_name = Some(build_identifier(part))
            }
            Rule::alter_table_constraint_action => {
                for inner in part.into_inner() {
                    match inner.as_rule() {
                        Rule::deferrable_constraint_attribute => deferrable = Some(true),
                        Rule::not_deferrable_constraint_attribute => deferrable = Some(false),
                        Rule::initially_deferred_constraint_attribute => {
                            initially_deferred = Some(true)
                        }
                        Rule::initially_immediate_constraint_attribute => {
                            initially_deferred = Some(false)
                        }
                        Rule::enforced_constraint_attribute => {
                            set_enforced_attribute(&mut enforced, true)?
                        }
                        Rule::not_enforced_constraint_attribute => {
                            set_enforced_attribute(&mut enforced, false)?
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
    Ok(AlterTableAlterConstraintStatement {
        if_exists,
        only,
        table_name: table_name.ok_or(ParseError::UnexpectedEof)?,
        constraint_name: constraint_name.ok_or(ParseError::UnexpectedEof)?,
        deferrable,
        initially_deferred,
        enforced,
    })
}

fn build_alter_table_rename_constraint(
    pair: Pair<'_, Rule>,
) -> Result<AlterTableRenameConstraintStatement, ParseError> {
    let mut if_exists = false;
    let mut only = false;
    let mut parts = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::alter_table_target => {
                let (parsed_if_exists, parsed_only, parsed_table_name) =
                    build_alter_table_target(part)?;
                if_exists = parsed_if_exists;
                only = parsed_only;
                parts.push(parsed_table_name);
            }
            Rule::identifier => parts.push(build_identifier(part)),
            _ => {}
        }
    }
    let mut parts = parts.into_iter();
    Ok(AlterTableRenameConstraintStatement {
        if_exists,
        only,
        table_name: parts.next().ok_or(ParseError::UnexpectedEof)?,
        constraint_name: parts.next().ok_or(ParseError::UnexpectedEof)?,
        new_constraint_name: parts.next().ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_alter_table_alter_column_type(
    pair: Pair<'_, Rule>,
) -> Result<AlterTableAlterColumnTypeStatement, ParseError> {
    let mut if_exists = false;
    let mut only = false;
    let mut table_name = None;
    let mut column_name = None;
    let mut ty = None;
    let mut using_expr = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::alter_table_target => {
                let (parsed_if_exists, parsed_only, parsed_table_name) =
                    build_alter_table_target(part)?;
                if_exists = parsed_if_exists;
                only = parsed_only;
                table_name = Some(parsed_table_name);
            }
            Rule::identifier if table_name.is_none() => table_name = Some(build_identifier(part)),
            Rule::identifier if column_name.is_none() => column_name = Some(build_identifier(part)),
            Rule::alter_table_column_type_action => {
                for inner in part.into_inner() {
                    match inner.as_rule() {
                        Rule::type_name => ty = Some(build_type_name(inner)),
                        Rule::alter_table_using_clause => {
                            let expr = inner
                                .into_inner()
                                .find(|item| item.as_rule() == Rule::expr)
                                .ok_or(ParseError::UnexpectedEof)?;
                            using_expr = Some(build_expr(expr)?);
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
    Ok(AlterTableAlterColumnTypeStatement {
        if_exists,
        only,
        table_name: table_name.ok_or(ParseError::UnexpectedEof)?,
        column_name: column_name.ok_or(ParseError::UnexpectedEof)?,
        ty: ty.ok_or(ParseError::UnexpectedEof)?,
        using_expr,
    })
}

fn build_alter_table_alter_column_default(
    pair: Pair<'_, Rule>,
) -> Result<AlterTableAlterColumnDefaultStatement, ParseError> {
    let mut if_exists = false;
    let mut only = false;
    let mut table_name = None;
    let mut column_name = None;
    let mut default_expr = None;
    let mut default_expr_sql = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::alter_table_target => {
                let (parsed_if_exists, parsed_only, parsed_table_name) =
                    build_alter_table_target(part)?;
                if_exists = parsed_if_exists;
                only = parsed_only;
                table_name = Some(parsed_table_name);
            }
            Rule::identifier if table_name.is_none() => table_name = Some(build_identifier(part)),
            Rule::identifier if column_name.is_none() => column_name = Some(build_identifier(part)),
            Rule::alter_table_column_default_action => {
                for inner in part.into_inner() {
                    match inner.as_rule() {
                        Rule::alter_table_set_default_action => {
                            let expr = inner
                                .into_inner()
                                .find(|item| item.as_rule() == Rule::expr)
                                .ok_or(ParseError::UnexpectedEof)?;
                            default_expr_sql = Some(expr.as_str().trim().to_string());
                            default_expr = Some(build_expr(expr)?);
                        }
                        Rule::alter_table_drop_default_action => {
                            default_expr = None;
                            default_expr_sql = None;
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
    Ok(AlterTableAlterColumnDefaultStatement {
        if_exists,
        only,
        table_name: table_name.ok_or(ParseError::UnexpectedEof)?,
        column_name: column_name.ok_or(ParseError::UnexpectedEof)?,
        default_expr,
        default_expr_sql,
    })
}

fn build_alter_table_alter_column_storage(
    pair: Pair<'_, Rule>,
) -> Result<AlterTableAlterColumnStorageStatement, ParseError> {
    let mut if_exists = false;
    let mut only = false;
    let mut table_name = None;
    let mut column_name = None;
    let mut storage = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::alter_table_target => {
                let (parsed_if_exists, parsed_only, parsed_table_name) =
                    build_alter_table_target(part)?;
                if_exists = parsed_if_exists;
                only = parsed_only;
                table_name = Some(parsed_table_name);
            }
            Rule::identifier if table_name.is_none() => table_name = Some(build_identifier(part)),
            Rule::identifier if column_name.is_none() => column_name = Some(build_identifier(part)),
            Rule::identifier => {
                storage = Some(match build_identifier(part).to_ascii_lowercase().as_str() {
                    "plain" => crate::include::access::htup::AttributeStorage::Plain,
                    "external" => crate::include::access::htup::AttributeStorage::External,
                    "extended" => crate::include::access::htup::AttributeStorage::Extended,
                    "main" => crate::include::access::htup::AttributeStorage::Main,
                    actual => {
                        return Err(ParseError::UnexpectedToken {
                            expected: "PLAIN, EXTERNAL, EXTENDED, or MAIN",
                            actual: actual.to_string(),
                        });
                    }
                });
            }
            _ => {}
        }
    }
    Ok(AlterTableAlterColumnStorageStatement {
        if_exists,
        only,
        table_name: table_name.ok_or(ParseError::UnexpectedEof)?,
        column_name: column_name.ok_or(ParseError::UnexpectedEof)?,
        storage: storage.ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_alter_table_alter_column_compression(
    pair: Pair<'_, Rule>,
) -> Result<AlterTableAlterColumnCompressionStatement, ParseError> {
    let mut if_exists = false;
    let mut only = false;
    let mut table_name = None;
    let mut column_name = None;
    let mut compression = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::alter_table_target => {
                let (parsed_if_exists, parsed_only, parsed_table_name) =
                    build_alter_table_target(part)?;
                if_exists = parsed_if_exists;
                only = parsed_only;
                table_name = Some(parsed_table_name);
            }
            Rule::identifier if table_name.is_none() => table_name = Some(build_identifier(part)),
            Rule::identifier if column_name.is_none() => column_name = Some(build_identifier(part)),
            Rule::identifier => {
                compression = Some(match build_identifier(part).to_ascii_lowercase().as_str() {
                    "default" => crate::include::access::htup::AttributeCompression::Default,
                    "pglz" => crate::include::access::htup::AttributeCompression::Pglz,
                    "lz4" => crate::include::access::htup::AttributeCompression::Lz4,
                    actual => {
                        return Err(ParseError::UnexpectedToken {
                            expected: "DEFAULT, PGLZ, or LZ4",
                            actual: actual.to_string(),
                        });
                    }
                });
            }
            Rule::kw_default => {
                compression = Some(crate::include::access::htup::AttributeCompression::Default);
            }
            _ => {}
        }
    }
    Ok(AlterTableAlterColumnCompressionStatement {
        if_exists,
        only,
        table_name: table_name.ok_or(ParseError::UnexpectedEof)?,
        column_name: column_name.ok_or(ParseError::UnexpectedEof)?,
        compression: compression.ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_alter_table_alter_column_options(
    pair: Pair<'_, Rule>,
) -> Result<AlterTableAlterColumnOptionsStatement, ParseError> {
    let mut if_exists = false;
    let mut only = false;
    let mut table_name = None;
    let mut column_name = None;
    let mut action = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::alter_table_target => {
                let (parsed_if_exists, parsed_only, parsed_table_name) =
                    build_alter_table_target(part)?;
                if_exists = parsed_if_exists;
                only = parsed_only;
                table_name = Some(parsed_table_name);
            }
            Rule::identifier if table_name.is_none() => table_name = Some(build_identifier(part)),
            Rule::identifier if column_name.is_none() => column_name = Some(build_identifier(part)),
            Rule::alter_table_column_options_action => {
                for inner in part.into_inner() {
                    match inner.as_rule() {
                        Rule::alter_table_set_options_action => {
                            let options = inner
                                .into_inner()
                                .filter(|item| item.as_rule() == Rule::reloption)
                                .map(build_reloption)
                                .collect::<Result<Vec<_>, _>>()?;
                            action = Some(AlterColumnOptionsAction::Set(options));
                        }
                        Rule::alter_table_reset_options_action => {
                            let options = inner
                                .into_inner()
                                .find(|item| item.as_rule() == Rule::ident_list)
                                .map(|list| list.into_inner().map(build_identifier).collect())
                                .ok_or(ParseError::UnexpectedEof)?;
                            action = Some(AlterColumnOptionsAction::Reset(options));
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
    Ok(AlterTableAlterColumnOptionsStatement {
        if_exists,
        only,
        table_name: table_name.ok_or(ParseError::UnexpectedEof)?,
        column_name: column_name.ok_or(ParseError::UnexpectedEof)?,
        action: action.ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_alter_table_alter_column_statistics(
    pair: Pair<'_, Rule>,
) -> Result<AlterTableAlterColumnStatisticsStatement, ParseError> {
    let mut if_exists = false;
    let mut only = false;
    let mut table_name = None;
    let mut column_name = None;
    let mut statistics_target = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::alter_table_target => {
                let (parsed_if_exists, parsed_only, parsed_table_name) =
                    build_alter_table_target(part)?;
                if_exists = parsed_if_exists;
                only = parsed_only;
                table_name = Some(parsed_table_name);
            }
            Rule::identifier if table_name.is_none() => table_name = Some(build_identifier(part)),
            Rule::identifier if column_name.is_none() => column_name = Some(build_identifier(part)),
            Rule::signed_integer => {
                statistics_target = Some(parse_i32(part)?);
            }
            _ => {}
        }
    }
    Ok(AlterTableAlterColumnStatisticsStatement {
        if_exists,
        only,
        table_name: table_name.ok_or(ParseError::UnexpectedEof)?,
        column_name: column_name.ok_or(ParseError::UnexpectedEof)?,
        statistics_target: statistics_target.ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_alter_table_set_not_null(
    pair: Pair<'_, Rule>,
) -> Result<AlterTableSetNotNullStatement, ParseError> {
    let mut if_exists = false;
    let mut only = false;
    let mut parts = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::alter_table_target => {
                let (parsed_if_exists, parsed_only, parsed_table_name) =
                    build_alter_table_target(part)?;
                if_exists = parsed_if_exists;
                only = parsed_only;
                parts.push(parsed_table_name);
            }
            Rule::identifier => parts.push(build_identifier(part)),
            _ => {}
        }
    }
    let mut parts = parts.into_iter();
    Ok(AlterTableSetNotNullStatement {
        if_exists,
        only,
        table_name: parts.next().ok_or(ParseError::UnexpectedEof)?,
        column_name: parts.next().ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_alter_table_drop_not_null(
    pair: Pair<'_, Rule>,
) -> Result<AlterTableDropNotNullStatement, ParseError> {
    let mut if_exists = false;
    let mut only = false;
    let mut parts = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::alter_table_target => {
                let (parsed_if_exists, parsed_only, parsed_table_name) =
                    build_alter_table_target(part)?;
                if_exists = parsed_if_exists;
                only = parsed_only;
                parts.push(parsed_table_name);
            }
            Rule::identifier => parts.push(build_identifier(part)),
            _ => {}
        }
    }
    let mut parts = parts.into_iter();
    Ok(AlterTableDropNotNullStatement {
        if_exists,
        only,
        table_name: parts.next().ok_or(ParseError::UnexpectedEof)?,
        column_name: parts.next().ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_alter_table_validate_constraint(
    pair: Pair<'_, Rule>,
) -> Result<AlterTableValidateConstraintStatement, ParseError> {
    let mut if_exists = false;
    let mut only = false;
    let mut parts = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::alter_table_target => {
                let (parsed_if_exists, parsed_only, parsed_table_name) =
                    build_alter_table_target(part)?;
                if_exists = parsed_if_exists;
                only = parsed_only;
                parts.push(parsed_table_name);
            }
            Rule::identifier => parts.push(build_identifier(part)),
            _ => {}
        }
    }
    let mut parts = parts.into_iter();
    Ok(AlterTableValidateConstraintStatement {
        if_exists,
        only,
        table_name: parts.next().ok_or(ParseError::UnexpectedEof)?,
        constraint_name: parts.next().ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_alter_table_no_inherit(
    pair: Pair<'_, Rule>,
) -> Result<AlterTableNoInheritStatement, ParseError> {
    let mut if_exists = false;
    let mut only = false;
    let mut parts = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::alter_table_target => {
                let (parsed_if_exists, parsed_only, parsed_table_name) =
                    build_alter_table_target(part)?;
                if_exists = parsed_if_exists;
                only = parsed_only;
                parts.push(parsed_table_name);
            }
            Rule::identifier => parts.push(build_identifier(part)),
            _ => {}
        }
    }
    let mut parts = parts.into_iter();
    Ok(AlterTableNoInheritStatement {
        if_exists,
        only,
        table_name: parts.next().ok_or(ParseError::UnexpectedEof)?,
        parent_name: parts.next().ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_alter_table_inherit(
    pair: Pair<'_, Rule>,
) -> Result<AlterTableInheritStatement, ParseError> {
    let mut if_exists = false;
    let mut only = false;
    let mut parts = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::alter_table_target => {
                let (parsed_if_exists, parsed_only, parsed_table_name) =
                    build_alter_table_target(part)?;
                if_exists = parsed_if_exists;
                only = parsed_only;
                parts.push(parsed_table_name);
            }
            Rule::identifier => parts.push(build_identifier(part)),
            _ => {}
        }
    }
    let mut parts = parts.into_iter();
    Ok(AlterTableInheritStatement {
        if_exists,
        only,
        table_name: parts.next().ok_or(ParseError::UnexpectedEof)?,
        parent_name: parts.next().ok_or(ParseError::UnexpectedEof)?,
    })
}
fn build_alter_relation_owner(
    pair: Pair<'_, Rule>,
) -> Result<AlterRelationOwnerStatement, ParseError> {
    let mut if_exists = false;
    let mut only = false;
    let mut parts = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::alter_table_target => {
                let (parsed_if_exists, parsed_only, parsed_table_name) =
                    build_alter_table_target(part)?;
                if_exists = parsed_if_exists;
                only = parsed_only;
                parts.push(parsed_table_name);
            }
            Rule::identifier => parts.push(build_identifier(part)),
            _ => {}
        }
    }
    let mut parts = parts.into_iter();
    Ok(AlterRelationOwnerStatement {
        if_exists,
        only,
        relation_name: parts.next().ok_or(ParseError::UnexpectedEof)?,
        new_owner: parts.next().ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_alter_schema_owner(pair: Pair<'_, Rule>) -> Result<AlterSchemaOwnerStatement, ParseError> {
    let mut parts = pair
        .into_inner()
        .filter(|part| part.as_rule() == Rule::identifier)
        .map(build_identifier);
    Ok(AlterSchemaOwnerStatement {
        schema_name: parts.next().ok_or(ParseError::UnexpectedEof)?,
        new_owner: parts.next().ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_alter_table_rename(pair: Pair<'_, Rule>) -> Result<AlterTableRenameStatement, ParseError> {
    let mut if_exists = false;
    let mut only = false;
    let mut parts = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::alter_table_target => {
                let (parsed_if_exists, parsed_only, parsed_table_name) =
                    build_alter_table_target(part)?;
                if_exists = parsed_if_exists;
                only = parsed_only;
                parts.push(parsed_table_name);
            }
            Rule::identifier => parts.push(build_identifier(part)),
            _ => {}
        }
    }
    let mut parts = parts.into_iter();
    Ok(AlterTableRenameStatement {
        if_exists,
        only,
        table_name: parts.next().ok_or(ParseError::UnexpectedEof)?,
        new_table_name: parts.next().ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_alter_table_rename_column(
    pair: Pair<'_, Rule>,
) -> Result<AlterTableRenameColumnStatement, ParseError> {
    let mut if_exists = false;
    let mut only = false;
    let mut parts = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::alter_table_target => {
                let (parsed_if_exists, parsed_only, parsed_table_name) =
                    build_alter_table_target(part)?;
                if_exists = parsed_if_exists;
                only = parsed_only;
                parts.push(parsed_table_name);
            }
            Rule::identifier => parts.push(build_identifier(part)),
            _ => {}
        }
    }
    let mut parts = parts.into_iter();
    Ok(AlterTableRenameColumnStatement {
        if_exists,
        only,
        table_name: parts.next().ok_or(ParseError::UnexpectedEof)?,
        column_name: parts.next().ok_or(ParseError::UnexpectedEof)?,
        new_column_name: parts.next().ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_alter_table_target(pair: Pair<'_, Rule>) -> Result<(bool, bool, String), ParseError> {
    let mut if_exists = false;
    let mut only = false;
    let mut table_name = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::if_exists_clause => if_exists = true,
            Rule::only_clause => only = true,
            Rule::identifier => table_name = Some(build_identifier(part)),
            _ => {}
        }
    }
    Ok((
        if_exists,
        only,
        table_name.ok_or(ParseError::UnexpectedEof)?,
    ))
}

fn build_type_name(pair: Pair<'_, Rule>) -> RawTypeName {
    fn add_array_bounds(ty: RawTypeName, bounds: usize) -> RawTypeName {
        let mut ty = ty;
        for _ in 0..bounds {
            ty = match ty {
                RawTypeName::Builtin(inner_ty) => RawTypeName::Builtin(SqlType::array_of(inner_ty)),
                RawTypeName::Named { name, array_bounds } => RawTypeName::Named {
                    name,
                    array_bounds: array_bounds.saturating_add(1),
                },
                other => other,
            };
        }
        ty
    }

    match pair.as_rule() {
        Rule::type_name | Rule::known_type_name => {
            let mut inner = pair.into_inner();
            let mut ty = build_type_name(inner.next().expect("type_name base"));
            for suffix in inner {
                let bounds = match suffix.as_rule() {
                    Rule::type_array_suffix => suffix
                        .into_inner()
                        .map(|part| match part.as_rule() {
                            Rule::array_suffix => 1usize,
                            Rule::array_decl_suffix => part
                                .into_inner()
                                .filter(|inner| inner.as_rule() == Rule::array_suffix)
                                .count(),
                            _ => 0,
                        })
                        .sum(),
                    Rule::array_suffix => 1,
                    Rule::array_decl_suffix => suffix
                        .into_inner()
                        .filter(|inner| inner.as_rule() == Rule::array_suffix)
                        .count(),
                    _ => 0,
                };
                ty = add_array_bounds(ty, bounds);
            }
            ty
        }
        Rule::known_base_type_name => {
            build_type_name(pair.into_inner().next().expect("base_type_name inner"))
        }
        Rule::qualified_known_base_type_name => {
            let mut inner = pair.into_inner();
            inner.next().expect("qualified_type_name schema");
            build_type_name(inner.next().expect("qualified_type_name base"))
        }
        Rule::array_type_alias => {
            let base = match pair
                .as_str()
                .trim_start_matches('_')
                .to_ascii_lowercase()
                .as_str()
            {
                "int2" | "smallint" => SqlType::new(SqlTypeKind::Int2),
                "int4" | "int" | "integer" => SqlType::new(SqlTypeKind::Int4),
                "int8" | "bigint" => SqlType::new(SqlTypeKind::Int8),
                "oid" => SqlType::new(SqlTypeKind::Oid),
                "name" => SqlType::new(SqlTypeKind::Name),
                "text" => SqlType::new(SqlTypeKind::Text),
                "bool" | "boolean" => SqlType::new(SqlTypeKind::Bool),
                "bytea" => SqlType::new(SqlTypeKind::Bytea),
                "money" => SqlType::new(SqlTypeKind::Money),
                "float4" | "real" => SqlType::new(SqlTypeKind::Float4),
                "float8" => SqlType::new(SqlTypeKind::Float8),
                "timestamp" => SqlType::new(SqlTypeKind::Timestamp),
                "json" => SqlType::new(SqlTypeKind::Json),
                "jsonb" => SqlType::new(SqlTypeKind::Jsonb),
                "jsonpath" => SqlType::new(SqlTypeKind::JsonPath),
                "xml" => SqlType::new(SqlTypeKind::Xml),
                other => panic!("unsupported array type alias: {other}"),
            };
            RawTypeName::Builtin(SqlType::array_of(base))
        }
        Rule::kw_record => RawTypeName::Record,
        Rule::kw_int2 | Rule::kw_smallint => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Int2)),
        Rule::kw_int4 | Rule::kw_int | Rule::kw_integer => {
            RawTypeName::Builtin(SqlType::new(SqlTypeKind::Int4))
        }
        Rule::kw_int8 | Rule::kw_bigint => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Int8)),
        Rule::kw_name => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Name)),
        Rule::kw_oid => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Oid)),
        Rule::bit_type => {
            let len = pair
                .into_inner()
                .find(|part| part.as_rule() == Rule::integer)
                .map(build_type_len)
                .transpose()
                .expect("bit length");
            match len {
                Some(len) => RawTypeName::Builtin(SqlType::with_bit_len(SqlTypeKind::Bit, len)),
                None => RawTypeName::Builtin(SqlType::with_bit_len(SqlTypeKind::Bit, 1)),
            }
        }
        Rule::varbit_type => {
            let len = pair
                .into_inner()
                .find(|part| part.as_rule() == Rule::integer)
                .map(build_type_len)
                .transpose()
                .expect("varbit length");
            match len {
                Some(len) => RawTypeName::Builtin(SqlType::with_bit_len(SqlTypeKind::VarBit, len)),
                None => RawTypeName::Builtin(SqlType::new(SqlTypeKind::VarBit)),
            }
        }
        Rule::kw_bytea => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Bytea)),
        Rule::kw_money => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Money)),
        Rule::kw_float4 | Rule::kw_real => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Float4)),
        Rule::kw_float8 | Rule::double_precision_type => {
            RawTypeName::Builtin(SqlType::new(SqlTypeKind::Float8))
        }
        Rule::numeric_type => {
            let dims = pair
                .into_inner()
                .filter(|part| matches!(part.as_rule(), Rule::integer | Rule::signed_integer))
                .map(build_numeric_typemod_component)
                .collect::<Result<Vec<_>, _>>()
                .expect("numeric precision/scale");
            match dims.as_slice() {
                [] => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Numeric)),
                [precision] => {
                    RawTypeName::Builtin(SqlType::with_numeric_precision_scale(*precision, 0))
                }
                [precision, scale] => {
                    RawTypeName::Builtin(SqlType::with_numeric_precision_scale(*precision, *scale))
                }
                _ => unreachable!("unexpected numeric typmod arity"),
            }
        }
        Rule::kw_text => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Text)),
        Rule::kw_json => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Json)),
        Rule::kw_jsonb => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Jsonb)),
        Rule::kw_jsonpath => RawTypeName::Builtin(SqlType::new(SqlTypeKind::JsonPath)),
        Rule::kw_xml => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Xml)),
        Rule::kw_tsvector => RawTypeName::Builtin(SqlType::new(SqlTypeKind::TsVector)),
        Rule::kw_tsquery => RawTypeName::Builtin(SqlType::new(SqlTypeKind::TsQuery)),
        Rule::kw_regclass => RawTypeName::Builtin(SqlType::new(SqlTypeKind::RegClass)),
        Rule::kw_regconfig => RawTypeName::Builtin(SqlType::new(SqlTypeKind::RegConfig)),
        Rule::kw_regdictionary => RawTypeName::Builtin(SqlType::new(SqlTypeKind::RegDictionary)),
        Rule::kw_regoperator => RawTypeName::Builtin(SqlType::new(SqlTypeKind::RegOperator)),
        Rule::kw_regprocedure => RawTypeName::Builtin(SqlType::new(SqlTypeKind::RegProcedure)),
        Rule::kw_bool | Rule::kw_boolean => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Bool)),
        Rule::date_type | Rule::kw_date => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Date)),
        Rule::time_type
        | Rule::kw_timetz
        | Rule::kw_timetz_atom
        | Rule::kw_time
        | Rule::kw_time_atom => {
            let normalized = pair.as_str().trim().to_ascii_lowercase();
            let precision = pair
                .clone()
                .into_inner()
                .find(|part| part.as_rule() == Rule::integer)
                .map(build_type_len)
                .transpose()
                .expect("time precision");
            let kind = if normalized == "timetz" || normalized.contains("with time zone") {
                SqlTypeKind::TimeTz
            } else {
                SqlTypeKind::Time
            };
            RawTypeName::Builtin(
                precision
                    .map(|precision| SqlType::with_time_precision(kind, precision))
                    .unwrap_or_else(|| SqlType::new(kind)),
            )
        }
        Rule::timestamp_type
        | Rule::kw_timestamptz
        | Rule::kw_timestamptz_atom
        | Rule::kw_timestamp
        | Rule::kw_timestamp_atom => {
            let normalized = pair.as_str().trim().to_ascii_lowercase();
            let precision = pair
                .clone()
                .into_inner()
                .find(|part| part.as_rule() == Rule::integer)
                .map(build_type_len)
                .transpose()
                .expect("timestamp precision");
            let kind = if normalized == "timestamptz" || normalized.contains("with time zone") {
                SqlTypeKind::TimestampTz
            } else {
                SqlTypeKind::Timestamp
            };
            RawTypeName::Builtin(
                precision
                    .map(|precision| SqlType::with_time_precision(kind, precision))
                    .unwrap_or_else(|| SqlType::new(kind)),
            )
        }
        Rule::kw_point => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Point)),
        Rule::kw_lseg => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Lseg)),
        Rule::kw_path => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Path)),
        Rule::kw_box => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Box)),
        Rule::kw_polygon => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Polygon)),
        Rule::kw_line => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Line)),
        Rule::kw_circle => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Circle)),
        Rule::internal_char_type => RawTypeName::Builtin(SqlType::new(SqlTypeKind::InternalChar)),
        Rule::char_type => {
            let len = pair
                .into_inner()
                .find(|part| part.as_rule() == Rule::integer)
                .map(build_type_len)
                .transpose()
                .expect("char length");
            match len {
                Some(len) => RawTypeName::Builtin(SqlType::with_char_len(SqlTypeKind::Char, len)),
                None => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Char)),
            }
        }
        Rule::varchar_type | Rule::character_varying_type => {
            let len = pair
                .into_inner()
                .find(|part| part.as_rule() == Rule::integer)
                .map(build_type_len)
                .transpose()
                .expect("varchar length");
            match len {
                Some(len) => {
                    RawTypeName::Builtin(SqlType::with_char_len(SqlTypeKind::Varchar, len))
                }
                None => RawTypeName::Builtin(SqlType::new(SqlTypeKind::Varchar)),
            }
        }
        Rule::named_type_name => RawTypeName::Named {
            name: build_identifier(pair.into_inner().next().expect("named_type_name inner")),
            array_bounds: 0,
        },
        _ => unreachable!("unexpected type rule {:?}", pair.as_rule()),
    }
}

fn build_type_len(pair: Pair<'_, Rule>) -> Result<i32, ParseError> {
    pair.as_str()
        .parse::<i32>()
        .map_err(|_| ParseError::InvalidInteger(pair.as_str().to_string()))
}

fn build_numeric_typemod_component(pair: Pair<'_, Rule>) -> Result<i32, ParseError> {
    pair.as_str()
        .parse::<i32>()
        .map_err(|_| ParseError::InvalidInteger(pair.as_str().to_string()))
}

fn parse_i32(pair: Pair<'_, Rule>) -> Result<i32, ParseError> {
    pair.as_str()
        .parse::<i32>()
        .map_err(|_| ParseError::InvalidInteger(pair.as_str().to_string()))
}

fn build_collation_name(pair: Pair<'_, Rule>) -> Result<String, ParseError> {
    let parts = pair
        .into_inner()
        .filter(|part| part.as_rule() == Rule::identifier)
        .map(build_identifier)
        .collect::<Vec<_>>();
    if parts.is_empty() {
        return Err(ParseError::UnexpectedEof);
    }
    Ok(parts.join("."))
}

fn build_identifier(pair: Pair<'_, Rule>) -> String {
    if pair.as_rule() == Rule::identifier {
        if let Some(inner) = pair.clone().into_inner().next() {
            return build_identifier(inner);
        }
    }
    let raw = pair.as_str();
    if pair.as_rule() == Rule::unicode_quoted_identifier {
        return decode_unicode_quoted_identifier(raw).unwrap_or_else(|_| raw.to_string());
    }
    if raw.starts_with('"') && raw.ends_with('"') {
        raw[1..raw.len() - 1].replace("\"\"", "\"")
    } else {
        raw.to_ascii_lowercase()
    }
}

pub(crate) fn build_expr(pair: Pair<'_, Rule>) -> Result<SqlExpr, ParseError> {
    match pair.as_rule() {
        Rule::expr
        | Rule::or_expr
        | Rule::and_expr
        | Rule::json_access_expr
        | Rule::concat_expr
        | Rule::add_expr
        | Rule::bit_expr
        | Rule::shift_expr
        | Rule::pow_expr
        | Rule::mul_expr => {
            let rule = pair.as_rule();
            let mut inner = pair.into_inner();
            let first = build_expr(inner.next().ok_or(ParseError::UnexpectedEof)?)?;
            if rule == Rule::json_access_expr {
                fold_json_access(first, inner)
            } else {
                fold_infix(first, inner)
            }
        }
        Rule::postfix_expr => {
            let mut inner = pair.into_inner();
            let mut expr = build_expr(inner.next().ok_or(ParseError::UnexpectedEof)?)?;
            for suffix in inner {
                match suffix.as_rule() {
                    Rule::cast_suffix => {
                        let ty = build_type_name(
                            suffix
                                .into_inner()
                                .find(|part| part.as_rule() == Rule::type_name)
                                .ok_or(ParseError::UnexpectedEof)?,
                        );
                        expr = SqlExpr::Cast(Box::new(expr), ty);
                    }
                    Rule::subscript_suffix => {
                        let subscript = build_array_subscript(suffix)?;
                        expr = match expr {
                            SqlExpr::ArraySubscript {
                                array,
                                mut subscripts,
                            } => {
                                subscripts.push(subscript);
                                SqlExpr::ArraySubscript { array, subscripts }
                            }
                            other => SqlExpr::ArraySubscript {
                                array: Box::new(other),
                                subscripts: vec![subscript],
                            },
                        };
                    }
                    Rule::field_select_suffix => {
                        let field = suffix
                            .into_inner()
                            .find(|part| part.as_rule() == Rule::identifier)
                            .map(build_identifier)
                            .ok_or(ParseError::UnexpectedEof)?;
                        expr = SqlExpr::FieldSelect {
                            expr: Box::new(expr),
                            field,
                        };
                    }
                    Rule::collate_suffix => {
                        let collation = build_collation_name(
                            suffix
                                .into_inner()
                                .find(|part| part.as_rule() == Rule::collation_name)
                                .ok_or(ParseError::UnexpectedEof)?,
                        )?;
                        expr = SqlExpr::Collate {
                            expr: Box::new(expr),
                            collation,
                        };
                    }
                    _ => {}
                }
            }
            Ok(expr)
        }
        Rule::unary_expr => build_expr(pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?),
        Rule::positive_expr => Ok(SqlExpr::UnaryPlus(Box::new(build_expr(
            pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?,
        )?))),
        Rule::negated_expr => {
            let raw = pair.as_str().trim_start();
            let expr = build_expr(pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?)?;
            if raw.starts_with("@-@") {
                Ok(SqlExpr::GeometryUnaryOp {
                    op: GeometryUnaryOp::Length,
                    expr: Box::new(expr),
                })
            } else if raw.starts_with('#') {
                Ok(SqlExpr::GeometryUnaryOp {
                    op: GeometryUnaryOp::Npoints,
                    expr: Box::new(expr),
                })
            } else if raw.starts_with("@@") {
                Ok(SqlExpr::GeometryUnaryOp {
                    op: GeometryUnaryOp::Center,
                    expr: Box::new(expr),
                })
            } else if raw.starts_with("?|") {
                Ok(SqlExpr::GeometryUnaryOp {
                    op: GeometryUnaryOp::IsVertical,
                    expr: Box::new(expr),
                })
            } else if raw.starts_with("?-") {
                Ok(SqlExpr::GeometryUnaryOp {
                    op: GeometryUnaryOp::IsHorizontal,
                    expr: Box::new(expr),
                })
            } else if raw.starts_with("||/") {
                Ok(simple_func_call(
                    "cbrt",
                    vec![SqlFunctionArg::positional(expr)],
                ))
            } else if raw.starts_with("!!") {
                Ok(SqlExpr::PrefixOperator {
                    op: "!!".into(),
                    expr: Box::new(expr),
                })
            } else if raw.starts_with("|/") {
                Ok(simple_func_call(
                    "sqrt",
                    vec![SqlFunctionArg::positional(expr)],
                ))
            } else if raw.starts_with('@') {
                Ok(simple_func_call(
                    "abs",
                    vec![SqlFunctionArg::positional(expr)],
                ))
            } else if raw.starts_with('~') {
                Ok(SqlExpr::BitNot(Box::new(expr)))
            } else {
                Ok(SqlExpr::Negate(Box::new(expr)))
            }
        }
        Rule::not_expr => {
            let mut inner = pair.into_inner();
            let first = inner.next().ok_or(ParseError::UnexpectedEof)?;
            if first.as_rule() == Rule::kw_not {
                Ok(SqlExpr::Not(Box::new(build_expr(
                    inner.next().ok_or(ParseError::UnexpectedEof)?,
                )?)))
            } else {
                build_expr(first)
            }
        }
        Rule::cmp_expr => {
            let mut inner = pair.into_inner();
            let left = build_expr(inner.next().ok_or(ParseError::UnexpectedEof)?)?;
            let Some(next) = inner.next() else {
                return Ok(left);
            };

            match next.as_rule() {
                Rule::null_predicate_suffix => build_null_predicate(left, next),
                Rule::is_document_suffix => {
                    let negated = next.into_inner().any(|part| part.as_rule() == Rule::kw_not);
                    let expr = SqlExpr::Xml(Box::new(RawXmlExpr {
                        op: RawXmlExprOp::IsDocument,
                        name: None,
                        named_args: Vec::new(),
                        arg_names: Vec::new(),
                        args: vec![left],
                        xml_option: None,
                        indent: None,
                        target_type: None,
                        standalone: None,
                    }));
                    Ok(if negated {
                        SqlExpr::Not(Box::new(expr))
                    } else {
                        expr
                    })
                }
                Rule::between_suffix => {
                    let mut negated = false;
                    let mut bounds = Vec::new();
                    for part in next.into_inner() {
                        match part.as_rule() {
                            Rule::kw_not => negated = true,
                            Rule::concat_expr => bounds.push(build_expr(part)?),
                            _ => {}
                        }
                    }
                    let low = bounds.first().cloned().ok_or(ParseError::UnexpectedEof)?;
                    let high = bounds.get(1).cloned().ok_or(ParseError::UnexpectedEof)?;
                    let between = SqlExpr::And(
                        Box::new(SqlExpr::GtEq(Box::new(left.clone()), Box::new(low))),
                        Box::new(SqlExpr::LtEq(Box::new(left), Box::new(high))),
                    );
                    Ok(if negated {
                        SqlExpr::Not(Box::new(between))
                    } else {
                        between
                    })
                }
                Rule::in_expr_list_suffix => {
                    let mut negated = false;
                    let mut values = Vec::new();
                    for part in next.into_inner() {
                        match part.as_rule() {
                            Rule::kw_not => negated = true,
                            Rule::expr_list => {
                                values = part
                                    .into_inner()
                                    .filter(|part| part.as_rule() == Rule::expr)
                                    .map(build_expr)
                                    .collect::<Result<Vec<_>, _>>()?;
                            }
                            _ => {}
                        }
                    }
                    Ok(SqlExpr::QuantifiedArray {
                        left: Box::new(left),
                        op: SubqueryComparisonOp::Eq,
                        is_all: negated,
                        array: Box::new(SqlExpr::ArrayLiteral(values)),
                    })
                }

                Rule::in_subquery_suffix => {
                    let mut negated = false;
                    let mut subquery = None;
                    for part in next.into_inner() {
                        match part.as_rule() {
                            Rule::kw_not => negated = true,
                            Rule::select_stmt => {
                                subquery = Some(build_select(part)?);
                            }
                            _ => {}
                        }
                    }
                    Ok(SqlExpr::InSubquery {
                        expr: Box::new(left),
                        subquery: Box::new(subquery.ok_or(ParseError::UnexpectedEof)?),
                        negated,
                    })
                }
                Rule::quantified_suffix => {
                    let mut parts = next.into_inner();
                    let op = match parts.next().ok_or(ParseError::UnexpectedEof)?.as_str() {
                        "@@" => SubqueryComparisonOp::Match,
                        "&&" => {
                            return Err(ParseError::UnexpectedToken {
                                expected: "comparison operator for ANY/ALL",
                                actual: "&&".into(),
                            });
                        }
                        "=" => SubqueryComparisonOp::Eq,
                        "<>" | "!=" => SubqueryComparisonOp::NotEq,
                        "<" => SubqueryComparisonOp::Lt,
                        "<=" => SubqueryComparisonOp::LtEq,
                        ">" => SubqueryComparisonOp::Gt,
                        ">=" => SubqueryComparisonOp::GtEq,
                        other => {
                            return Err(ParseError::UnexpectedToken {
                                expected: "subquery comparison operator",
                                actual: other.into(),
                            });
                        }
                    };
                    let quantifier = parts.next().ok_or(ParseError::UnexpectedEof)?;
                    let is_all = match quantifier.as_str().to_ascii_lowercase().as_str() {
                        "any" => false,
                        "all" => true,
                        _ => {
                            return Err(ParseError::UnexpectedToken {
                                expected: "ANY or ALL",
                                actual: quantifier.as_str().into(),
                            });
                        }
                    };
                    let rhs = parts.next().ok_or(ParseError::UnexpectedEof)?;
                    Ok(match rhs.as_rule() {
                        Rule::select_stmt => SqlExpr::QuantifiedSubquery {
                            left: Box::new(left),
                            op,
                            is_all,
                            subquery: Box::new(build_select(rhs)?),
                        },
                        Rule::expr => SqlExpr::QuantifiedArray {
                            left: Box::new(left),
                            op,
                            is_all,
                            array: Box::new(build_expr(rhs)?),
                        },
                        _ => {
                            return Err(ParseError::UnexpectedToken {
                                expected: "subquery or array expression",
                                actual: rhs.as_str().into(),
                            });
                        }
                    })
                }
                Rule::quantified_like_suffix => build_quantified_like_predicate(left, next),
                Rule::quantified_similar_suffix => build_quantified_similar_predicate(left, next),
                Rule::like_suffix => build_like_predicate(left, next),
                Rule::similar_suffix => build_similar_predicate(left, next),
                Rule::comp_op => {
                    let right = build_expr(inner.next().ok_or(ParseError::UnexpectedEof)?)?;
                    Ok(match next.as_str() {
                        "<->" => SqlExpr::GeometryBinaryOp {
                            op: GeometryBinaryOp::Distance,
                            left: Box::new(left),
                            right: Box::new(right),
                        },
                        "##" => SqlExpr::GeometryBinaryOp {
                            op: GeometryBinaryOp::ClosestPoint,
                            left: Box::new(left),
                            right: Box::new(right),
                        },
                        "?#" => SqlExpr::GeometryBinaryOp {
                            op: GeometryBinaryOp::Intersects,
                            left: Box::new(left),
                            right: Box::new(right),
                        },
                        "?||" => SqlExpr::GeometryBinaryOp {
                            op: GeometryBinaryOp::Parallel,
                            left: Box::new(left),
                            right: Box::new(right),
                        },
                        "?-|" => SqlExpr::GeometryBinaryOp {
                            op: GeometryBinaryOp::Perpendicular,
                            left: Box::new(left),
                            right: Box::new(right),
                        },
                        "~=" => SqlExpr::GeometryBinaryOp {
                            op: GeometryBinaryOp::Same,
                            left: Box::new(left),
                            right: Box::new(right),
                        },
                        "&<" => SqlExpr::GeometryBinaryOp {
                            op: GeometryBinaryOp::OverLeft,
                            left: Box::new(left),
                            right: Box::new(right),
                        },
                        "&>" => SqlExpr::GeometryBinaryOp {
                            op: GeometryBinaryOp::OverRight,
                            left: Box::new(left),
                            right: Box::new(right),
                        },
                        "<<|" | "<^" => SqlExpr::GeometryBinaryOp {
                            op: GeometryBinaryOp::Below,
                            left: Box::new(left),
                            right: Box::new(right),
                        },
                        "|>>" | ">^" => SqlExpr::GeometryBinaryOp {
                            op: GeometryBinaryOp::Above,
                            left: Box::new(left),
                            right: Box::new(right),
                        },
                        "&<|" => SqlExpr::GeometryBinaryOp {
                            op: GeometryBinaryOp::OverBelow,
                            left: Box::new(left),
                            right: Box::new(right),
                        },
                        "|&>" => SqlExpr::GeometryBinaryOp {
                            op: GeometryBinaryOp::OverAbove,
                            left: Box::new(left),
                            right: Box::new(right),
                        },
                        "?-" => SqlExpr::GeometryBinaryOp {
                            op: GeometryBinaryOp::IsHorizontal,
                            left: Box::new(left),
                            right: Box::new(right),
                        },
                        "@>" if expr_is_array_syntax(&left) && expr_is_array_syntax(&right) => {
                            SqlExpr::ArrayContains(Box::new(left), Box::new(right))
                        }
                        "@>" => SqlExpr::JsonbContains(Box::new(left), Box::new(right)),
                        "<@" if expr_is_array_syntax(&left) && expr_is_array_syntax(&right) => {
                            SqlExpr::ArrayContained(Box::new(left), Box::new(right))
                        }
                        "<@" => SqlExpr::JsonbContained(Box::new(left), Box::new(right)),
                        "@?" => SqlExpr::JsonbPathExists(Box::new(left), Box::new(right)),
                        "@@" if expr_is_jsonb_syntax(&left) && expr_is_jsonpath_syntax(&right) => {
                            SqlExpr::JsonbPathMatch(Box::new(left), Box::new(right))
                        }
                        "@@" => SqlExpr::BinaryOperator {
                            op: "@@".into(),
                            left: Box::new(left),
                            right: Box::new(right),
                        },
                        "?" => SqlExpr::JsonbExists(Box::new(left), Box::new(right)),
                        "?|" => SqlExpr::JsonbExistsAny(Box::new(left), Box::new(right)),
                        "?&" => SqlExpr::JsonbExistsAll(Box::new(left), Box::new(right)),
                        "&&" if expr_is_array_syntax(&left) && expr_is_array_syntax(&right) => {
                            SqlExpr::ArrayOverlap(Box::new(left), Box::new(right))
                        }
                        "&&" => SqlExpr::BinaryOperator {
                            op: "&&".into(),
                            left: Box::new(left),
                            right: Box::new(right),
                        },
                        "-|-" => SqlExpr::BinaryOperator {
                            op: "-|-".into(),
                            left: Box::new(left),
                            right: Box::new(right),
                        },
                        "=" => SqlExpr::Eq(Box::new(left), Box::new(right)),
                        "<>" | "!=" => SqlExpr::NotEq(Box::new(left), Box::new(right)),
                        "<" => SqlExpr::Lt(Box::new(left), Box::new(right)),
                        "<=" => SqlExpr::LtEq(Box::new(left), Box::new(right)),
                        ">" => SqlExpr::Gt(Box::new(left), Box::new(right)),
                        ">=" => SqlExpr::GtEq(Box::new(left), Box::new(right)),
                        "~" => SqlExpr::RegexMatch(Box::new(left), Box::new(right)),
                        _ => unreachable!(),
                    })
                }
                _ => Err(ParseError::UnexpectedToken {
                    expected: "comparison",
                    actual: next.as_str().into(),
                }),
            }
        }
        Rule::primary_expr => {
            build_expr(pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?)
        }
        Rule::scalar_subquery_expr => Ok(SqlExpr::ScalarSubquery(Box::new(
            build_select_like_subquery(pair)?,
        ))),
        Rule::array_subquery_expr => Ok(SqlExpr::ArraySubquery(Box::new(
            build_select_like_subquery(pair)?,
        ))),
        Rule::exists_expr => {
            let subquery = build_select(
                pair.into_inner()
                    .find(|part| part.as_rule() == Rule::select_stmt)
                    .ok_or(ParseError::UnexpectedEof)?,
            )?;
            Ok(SqlExpr::Exists(Box::new(subquery)))
        }
        Rule::case_expr => build_case_expr(pair),
        Rule::array_expr => build_array_literal(pair),
        Rule::cast_expr => {
            let mut expr = None;
            let mut ty = None;
            for part in pair.into_inner() {
                match part.as_rule() {
                    Rule::expr => expr = Some(build_expr(part)?),
                    Rule::type_name => ty = Some(build_type_name(part)),
                    _ => {}
                }
            }
            Ok(SqlExpr::Cast(
                Box::new(expr.ok_or(ParseError::UnexpectedEof)?),
                ty.ok_or(ParseError::UnexpectedEof)?,
            ))
        }
        Rule::row_expr | Rule::implicit_row_expr => Ok(SqlExpr::Row(
            pair.into_inner()
                .filter(|part| part.as_rule() == Rule::expr || part.as_rule() == Rule::expr_list)
                .flat_map(|part| match part.as_rule() {
                    Rule::expr => vec![part],
                    Rule::expr_list => part
                        .into_inner()
                        .filter(|inner| inner.as_rule() == Rule::expr)
                        .collect(),
                    _ => Vec::new(),
                })
                .map(build_expr)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        Rule::func_call => build_func_call(pair),
        Rule::qualified_star => Ok(SqlExpr::Column(pair.as_str().to_string())),
        Rule::position_expr => {
            let mut args = pair
                .into_inner()
                .filter(|part| part.as_rule() != Rule::kw_in);
            let needle = build_expr(args.next().ok_or(ParseError::UnexpectedEof)?)?;
            let haystack = build_expr(args.next().ok_or(ParseError::UnexpectedEof)?)?;
            Ok(simple_func_call(
                "position",
                vec![
                    SqlFunctionArg {
                        name: None,
                        value: needle,
                    },
                    SqlFunctionArg {
                        name: None,
                        value: haystack,
                    },
                ],
            ))
        }
        Rule::substring_expr => {
            let inner = pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
            match inner.as_rule() {
                Rule::substring_from_expr => {
                    let mut inner = inner.into_inner().filter(|part| {
                        !matches!(
                            part.as_rule(),
                            Rule::kw_from | Rule::kw_from_atom | Rule::kw_for | Rule::kw_for_atom
                        )
                    });
                    let value = parse_expr(
                        inner
                            .next()
                            .ok_or(ParseError::UnexpectedEof)?
                            .as_str()
                            .trim(),
                    )?;
                    let start = build_expr(inner.next().ok_or(ParseError::UnexpectedEof)?)?;
                    let mut args = vec![value, start];
                    if let Some(len) = inner.next() {
                        args.push(build_expr(len)?);
                    }
                    Ok(simple_func_call(
                        "substring",
                        args.into_iter().map(SqlFunctionArg::positional).collect(),
                    ))
                }
                Rule::substring_for_expr => {
                    let mut inner = inner
                        .into_inner()
                        .filter(|part| !matches!(part.as_rule(), Rule::kw_for | Rule::kw_for_atom));
                    let value = parse_expr(
                        inner
                            .next()
                            .ok_or(ParseError::UnexpectedEof)?
                            .as_str()
                            .trim(),
                    )?;
                    let len = build_expr(inner.next().ok_or(ParseError::UnexpectedEof)?)?;
                    Ok(simple_func_call(
                        "substring",
                        vec![value, SqlExpr::IntegerLiteral("1".into()), len]
                            .into_iter()
                            .map(SqlFunctionArg::positional)
                            .collect(),
                    ))
                }
                Rule::substring_similar_expr => {
                    let mut inner = inner.into_inner().filter(|part| {
                        !matches!(
                            part.as_rule(),
                            Rule::kw_similar
                                | Rule::kw_similar_atom
                                | Rule::kw_to
                                | Rule::kw_to_atom
                        )
                    });
                    let value = parse_expr(
                        inner
                            .next()
                            .ok_or(ParseError::UnexpectedEof)?
                            .as_str()
                            .trim(),
                    )?;
                    let pattern = build_expr(inner.next().ok_or(ParseError::UnexpectedEof)?)?;
                    let mut args = vec![
                        SqlFunctionArg::positional(value),
                        SqlFunctionArg::positional(pattern),
                    ];
                    if let Some(escape_clause) = inner.next() {
                        let expr = escape_clause
                            .into_inner()
                            .find(|inner| inner.as_rule() == Rule::concat_expr)
                            .ok_or(ParseError::UnexpectedEof)?;
                        args.push(SqlFunctionArg::positional(build_expr(expr)?));
                    }
                    Ok(simple_func_call("similar_substring", args))
                }
                _ => Err(ParseError::UnexpectedToken {
                    expected: "substring expression",
                    actual: inner.as_str().into(),
                }),
            }
        }
        Rule::extract_expr => {
            let mut field = None;
            let mut value = None;
            for part in pair.into_inner() {
                match part.as_rule() {
                    Rule::extract_field => {
                        let inner = part.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
                        field = Some(match inner.as_rule() {
                            Rule::identifier => {
                                SqlExpr::Const(Value::Text(build_identifier(inner).into()))
                            }
                            Rule::quoted_string_literal => SqlExpr::Const(Value::Text(
                                decode_string_literal_pair(inner)?.into(),
                            )),
                            _ => {
                                return Err(ParseError::UnexpectedToken {
                                    expected: "extract field",
                                    actual: inner.as_str().into(),
                                });
                            }
                        });
                    }
                    Rule::expr => value = Some(build_expr(part)?),
                    _ => {}
                }
            }
            Ok(simple_func_call(
                "date_part",
                vec![
                    SqlFunctionArg::positional(field.ok_or(ParseError::UnexpectedEof)?),
                    SqlFunctionArg::positional(value.ok_or(ParseError::UnexpectedEof)?),
                ],
            ))
        }
        Rule::overlay_expr => {
            let mut inner = pair.into_inner().filter(|part| {
                !matches!(
                    part.as_rule(),
                    Rule::kw_placing
                        | Rule::kw_placing_atom
                        | Rule::kw_from
                        | Rule::kw_from_atom
                        | Rule::kw_for
                        | Rule::kw_for_atom
                )
            });
            let value = parse_expr(
                inner
                    .next()
                    .ok_or(ParseError::UnexpectedEof)?
                    .as_str()
                    .trim(),
            )?;
            let placing = parse_expr(
                inner
                    .next()
                    .ok_or(ParseError::UnexpectedEof)?
                    .as_str()
                    .trim(),
            )?;
            let start = build_expr(inner.next().ok_or(ParseError::UnexpectedEof)?)?;
            let mut args = vec![value, placing, start];
            if let Some(len) = inner.next() {
                args.push(build_expr(len)?);
            }
            Ok(simple_func_call(
                "overlay",
                args.into_iter().map(SqlFunctionArg::positional).collect(),
            ))
        }
        Rule::trim_expr => build_trim_expr(pair),
        Rule::xml_element_expr => build_xml_element_expr(pair),
        Rule::xml_forest_expr => build_xml_forest_expr(pair),
        Rule::xml_parse_expr => build_xml_parse_expr(pair),
        Rule::xml_pi_expr => build_xml_pi_expr(pair),
        Rule::xml_root_expr => build_xml_root_expr(pair),
        Rule::xml_serialize_expr => build_xml_serialize_expr(pair),
        Rule::typed_string_literal => {
            let mut inner = pair.into_inner();
            let ty = build_type_name(inner.next().ok_or(ParseError::UnexpectedEof)?);
            let literal =
                decode_string_literal_pair(inner.next().ok_or(ParseError::UnexpectedEof)?)?;
            Ok(SqlExpr::Cast(
                Box::new(SqlExpr::Const(Value::Text(literal.into()))),
                ty,
            ))
        }
        Rule::bit_string_literal | Rule::binary_bit_literal | Rule::hex_bit_literal => Ok(
            SqlExpr::Const(Value::Bit(parse_bit_string_literal(pair.as_str())?)),
        ),
        Rule::identifier => Ok(SqlExpr::Column(build_identifier(pair))),
        Rule::kw_default => Ok(SqlExpr::Default),
        Rule::numeric_literal => Ok(SqlExpr::NumericLiteral(pair.as_str().to_string())),
        Rule::integer => Ok(SqlExpr::IntegerLiteral(pair.as_str().to_string())),
        Rule::quoted_string_literal
        | Rule::string_literal
        | Rule::unicode_string_literal
        | Rule::escape_string_literal
        | Rule::dollar_string_literal => Ok(SqlExpr::Const(Value::Text(
            decode_string_literal_pair(pair)?.into(),
        ))),
        Rule::kw_null => Ok(SqlExpr::Const(Value::Null)),
        Rule::kw_true => Ok(SqlExpr::Const(Value::Bool(true))),
        Rule::kw_false => Ok(SqlExpr::Const(Value::Bool(false))),
        Rule::kw_current_date => Ok(SqlExpr::CurrentDate),
        Rule::kw_current_user => Ok(SqlExpr::CurrentUser),
        Rule::kw_session_user => Ok(SqlExpr::SessionUser),
        Rule::kw_current_role => Ok(SqlExpr::CurrentRole),
        Rule::kw_current_time => Ok(SqlExpr::CurrentTime {
            precision: pair
                .into_inner()
                .find(|part| part.as_rule() == Rule::integer)
                .map(build_type_len)
                .transpose()?,
        }),
        Rule::kw_current_timestamp => Ok(SqlExpr::CurrentTimestamp {
            precision: pair
                .into_inner()
                .find(|part| part.as_rule() == Rule::integer)
                .map(build_type_len)
                .transpose()?,
        }),
        Rule::kw_localtime => Ok(SqlExpr::LocalTime {
            precision: pair
                .into_inner()
                .find(|part| part.as_rule() == Rule::integer)
                .map(build_type_len)
                .transpose()?,
        }),
        Rule::kw_localtimestamp => Ok(SqlExpr::LocalTimestamp {
            precision: pair
                .into_inner()
                .find(|part| part.as_rule() == Rule::integer)
                .map(build_type_len)
                .transpose()?,
        }),
        _ => Err(ParseError::UnexpectedToken {
            expected: "expression",
            actual: pair.as_str().into(),
        }),
    }
}

fn build_array_literal(pair: Pair<'_, Rule>) -> Result<SqlExpr, ParseError> {
    let elements = pair
        .into_inner()
        .find(|part| part.as_rule() == Rule::array_expr_elements)
        .map(build_array_literal_elements)
        .transpose()?
        .unwrap_or_default();
    Ok(SqlExpr::ArrayLiteral(elements))
}

fn build_select_like_subquery(pair: Pair<'_, Rule>) -> Result<SelectStatement, ParseError> {
    match pair
        .into_inner()
        .find(|part| matches!(part.as_rule(), Rule::select_stmt | Rule::values_stmt))
        .ok_or(ParseError::UnexpectedEof)?
    {
        part if part.as_rule() == Rule::select_stmt => build_select(part),
        part if part.as_rule() == Rule::values_stmt => {
            Ok(wrap_values_as_select(build_values_statement(part)?))
        }
        part => Err(ParseError::UnexpectedToken {
            expected: "SELECT or VALUES subquery",
            actual: part.as_str().into(),
        }),
    }
}

fn build_array_literal_elements(pair: Pair<'_, Rule>) -> Result<Vec<SqlExpr>, ParseError> {
    pair.into_inner()
        .filter(|part| part.as_rule() == Rule::array_expr_element)
        .map(|element| {
            let inner = element
                .into_inner()
                .next()
                .ok_or(ParseError::UnexpectedEof)?;
            match inner.as_rule() {
                Rule::nested_array_expr => build_array_literal(inner),
                Rule::expr => build_expr(inner),
                _ => Err(ParseError::UnexpectedToken {
                    expected: "array expression element",
                    actual: inner.as_str().into(),
                }),
            }
        })
        .collect()
}

fn build_func_call_name(pair: Pair<'_, Rule>) -> Result<String, ParseError> {
    match pair.as_rule() {
        Rule::func_call_name => {
            build_func_call_name(pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?)
        }
        Rule::identifier => Ok(build_identifier(pair)),
        Rule::agg_func => Ok(pair.as_str().to_ascii_lowercase()),
        _ => Err(ParseError::UnexpectedToken {
            expected: "function name",
            actual: pair.as_str().into(),
        }),
    }
}

fn build_func_call(pair: Pair<'_, Rule>) -> Result<SqlExpr, ParseError> {
    let mut name = None;
    let mut parsed_args = ParsedFunctionArgs::default();
    let mut order_by = Vec::new();
    let mut is_star = false;
    let mut distinct = false;
    let mut filter = None;
    let mut over = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::func_call_name => name = Some(build_func_call_name(part)?),
            Rule::agg_distinct => distinct = true,
            Rule::star => is_star = true,
            Rule::function_arg_list => {
                parsed_args = build_function_arg_list(part)?;
            }
            Rule::agg_order_by_clause => {
                order_by = part
                    .into_inner()
                    .filter(|inner| inner.as_rule() == Rule::order_by_item)
                    .map(build_order_by_item)
                    .collect::<Result<Vec<_>, _>>()?;
            }
            Rule::agg_filter_clause => {
                filter = Some(build_agg_filter_clause(part)?);
            }
            Rule::over_clause => {
                over = Some(build_over_clause(part)?);
            }
            _ => {}
        }
    }
    let name = name.ok_or(ParseError::UnexpectedEof)?;
    let args = if is_star {
        SqlCallArgs::Star
    } else {
        SqlCallArgs::Args(parsed_args.args)
    };
    if name.eq_ignore_ascii_case("random")
        && matches!(&args, SqlCallArgs::Args(args) if args.is_empty())
        && order_by.is_empty()
        && !distinct
        && filter.is_none()
        && over.is_none()
    {
        return Ok(SqlExpr::Random);
    }
    Ok(SqlExpr::FuncCall {
        name,
        args,
        order_by,
        distinct,
        func_variadic: !is_star && parsed_args.func_variadic,
        filter: filter.map(Box::new),
        over,
    })
}

fn build_agg_filter_clause(pair: Pair<'_, Rule>) -> Result<SqlExpr, ParseError> {
    let expr = pair
        .into_inner()
        .find(|part| part.as_rule() == Rule::expr)
        .ok_or(ParseError::UnexpectedEof)?;
    build_expr(expr)
}

fn build_window_frame_bound(pair: Pair<'_, Rule>) -> Result<RawWindowFrameBound, ParseError> {
    let pair_text = pair.as_str().to_string();
    let mut expr = None;
    let mut saw_unbounded = false;
    let mut saw_current = false;
    let mut saw_preceding = false;
    let mut saw_following = false;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::expr => expr = Some(build_expr(part)?),
            Rule::kw_unbounded => saw_unbounded = true,
            Rule::kw_current => saw_current = true,
            Rule::kw_preceding => saw_preceding = true,
            Rule::kw_following => saw_following = true,
            _ => {}
        }
    }
    match (
        saw_unbounded,
        saw_current,
        saw_preceding,
        saw_following,
        expr,
    ) {
        (true, false, true, false, None) => Ok(RawWindowFrameBound::UnboundedPreceding),
        (true, false, false, true, None) => Ok(RawWindowFrameBound::UnboundedFollowing),
        (false, true, false, false, None) => Ok(RawWindowFrameBound::CurrentRow),
        (false, false, true, false, Some(expr)) => {
            Ok(RawWindowFrameBound::OffsetPreceding(Box::new(expr)))
        }
        (false, false, false, true, Some(expr)) => {
            Ok(RawWindowFrameBound::OffsetFollowing(Box::new(expr)))
        }
        _ => Err(ParseError::UnexpectedToken {
            expected: "window frame bound",
            actual: pair_text,
        }),
    }
}

fn build_window_frame_clause(pair: Pair<'_, Rule>) -> Result<RawWindowFrame, ParseError> {
    let pair_text = pair.as_str().to_string();
    let mut mode = None;
    let mut bounds = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::kw_rows | Rule::kw_rows_atom => mode = Some(WindowFrameMode::Rows),
            Rule::kw_range | Rule::kw_range_atom => mode = Some(WindowFrameMode::Range),
            Rule::kw_groups | Rule::kw_groups_atom => mode = Some(WindowFrameMode::Groups),
            Rule::window_frame_bound => bounds.push(build_window_frame_bound(part)?),
            Rule::window_frame_between => {
                for inner in part.into_inner() {
                    if inner.as_rule() == Rule::window_frame_bound {
                        bounds.push(build_window_frame_bound(inner)?);
                    }
                }
            }
            Rule::window_frame_exclusion => {
                return Err(ParseError::FeatureNotSupported(
                    "window frame exclusion".into(),
                ));
            }
            _ => {}
        }
    }
    let (start_bound, end_bound) = match bounds.as_slice() {
        [start] => (start.clone(), RawWindowFrameBound::CurrentRow),
        [start, end] => (start.clone(), end.clone()),
        _ => {
            return Err(ParseError::UnexpectedToken {
                expected: "window frame clause",
                actual: pair_text,
            });
        }
    };
    Ok(RawWindowFrame {
        mode: mode.ok_or(ParseError::UnexpectedEof)?,
        start_bound,
        end_bound,
    })
}

fn build_over_clause(pair: Pair<'_, Rule>) -> Result<RawWindowSpec, ParseError> {
    let mut name = None;
    let mut spec = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier => name = Some(build_identifier(part)),
            Rule::raw_window_spec => spec = Some(build_raw_window_spec(part)?),
            _ => {}
        }
    }
    if let Some(name) = name {
        return Ok(RawWindowSpec {
            name: Some(name),
            partition_by: Vec::new(),
            order_by: Vec::new(),
            frame: None,
        });
    }
    Ok(spec.unwrap_or(RawWindowSpec {
        name: None,
        partition_by: Vec::new(),
        order_by: Vec::new(),
        frame: None,
    }))
}

fn build_window_clause(pair: Pair<'_, Rule>) -> Result<Vec<RawWindowClause>, ParseError> {
    pair.into_inner()
        .filter(|part| part.as_rule() == Rule::window_definition)
        .map(build_window_definition)
        .collect()
}

fn build_window_definition(pair: Pair<'_, Rule>) -> Result<RawWindowClause, ParseError> {
    let mut name = None;
    let mut spec = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier => name = Some(build_identifier(part)),
            Rule::raw_window_spec => spec = Some(build_raw_window_spec(part)?),
            _ => {}
        }
    }
    Ok(RawWindowClause {
        name: name.ok_or(ParseError::UnexpectedEof)?,
        spec: spec.unwrap_or(RawWindowSpec {
            name: None,
            partition_by: Vec::new(),
            order_by: Vec::new(),
            frame: None,
        }),
    })
}

fn build_raw_window_spec(pair: Pair<'_, Rule>) -> Result<RawWindowSpec, ParseError> {
    let mut name = None;
    let mut partition_by = Vec::new();
    let mut order_by = Vec::new();
    let mut frame = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::window_ref_name => {
                let ident = part
                    .into_inner()
                    .find(|inner| inner.as_rule() == Rule::identifier)
                    .ok_or(ParseError::UnexpectedEof)?;
                name = Some(build_identifier(ident));
            }
            Rule::identifier => name = Some(build_identifier(part)),
            Rule::window_partition_by_clause => {
                partition_by = part
                    .into_inner()
                    .filter(|inner| inner.as_rule() == Rule::expr)
                    .map(build_expr)
                    .collect::<Result<Vec<_>, _>>()?;
            }
            Rule::window_order_by_clause => {
                order_by = part
                    .into_inner()
                    .filter(|inner| inner.as_rule() == Rule::order_by_item)
                    .map(build_order_by_item)
                    .collect::<Result<Vec<_>, _>>()?;
            }
            Rule::window_frame_clause => frame = Some(Box::new(build_window_frame_clause(part)?)),
            _ => {}
        }
    }
    Ok(RawWindowSpec {
        name,
        partition_by,
        order_by,
        frame,
    })
}

#[derive(Default)]
struct ParsedFunctionArgs {
    args: Vec<SqlFunctionArg>,
    func_variadic: bool,
}

fn build_function_arg_list(pair: Pair<'_, Rule>) -> Result<ParsedFunctionArgs, ParseError> {
    let mut parsed = ParsedFunctionArgs::default();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::function_arg => parsed.args.push(build_function_arg(part)?),
            Rule::variadic_function_arg => {
                let expr = part
                    .into_inner()
                    .find(|inner| inner.as_rule() == Rule::expr)
                    .ok_or(ParseError::UnexpectedEof)?;
                parsed
                    .args
                    .push(SqlFunctionArg::positional(build_expr(expr)?));
                parsed.func_variadic = true;
            }
            _ => {}
        }
    }
    Ok(parsed)
}

fn build_function_arg(pair: Pair<'_, Rule>) -> Result<SqlFunctionArg, ParseError> {
    let inner = pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
    match inner.as_rule() {
        Rule::named_function_arg => {
            let mut inner = inner.into_inner();
            let name = build_identifier(inner.next().ok_or(ParseError::UnexpectedEof)?);
            let value = build_expr(inner.next().ok_or(ParseError::UnexpectedEof)?)?;
            Ok(SqlFunctionArg {
                name: Some(name),
                value,
            })
        }
        Rule::positional_function_arg => {
            let expr = inner.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
            Ok(SqlFunctionArg {
                name: None,
                value: build_expr(expr)?,
            })
        }
        Rule::expr => Ok(SqlFunctionArg {
            name: None,
            value: build_expr(inner)?,
        }),
        _ => Err(ParseError::UnexpectedToken {
            expected: "function argument",
            actual: inner.as_str().into(),
        }),
    }
}

fn build_copy_from(pair: Pair<'_, Rule>) -> Result<CopyFromStatement, ParseError> {
    let mut table_name = None;
    let mut columns = None;
    let mut source = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier if table_name.is_none() => {
                table_name = Some(build_identifier(part));
            }
            Rule::ident_list => {
                columns = Some(part.into_inner().map(build_identifier).collect());
            }
            Rule::quoted_string_literal
            | Rule::string_literal
            | Rule::unicode_string_literal
            | Rule::escape_string_literal
            | Rule::dollar_string_literal => {
                source = Some(CopySource::File(decode_string_literal_pair(part)?));
            }
            _ => {}
        }
    }
    Ok(CopyFromStatement {
        table_name: table_name.ok_or(ParseError::UnexpectedEof)?,
        columns,
        source: source.ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_null_predicate(left: SqlExpr, pair: Pair<'_, Rule>) -> Result<SqlExpr, ParseError> {
    let pair = if pair.as_rule() == Rule::null_predicate_suffix {
        pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?
    } else {
        pair
    };
    let raw = pair.as_str().to_ascii_lowercase();
    if raw == "is null" || raw == "isnull" {
        return Ok(SqlExpr::IsNull(Box::new(left)));
    }
    if raw == "is not null" || raw == "notnull" {
        return Ok(SqlExpr::IsNotNull(Box::new(left)));
    }
    if raw == "is true" {
        return Ok(SqlExpr::IsNotDistinctFrom(
            Box::new(left),
            Box::new(SqlExpr::Const(Value::Bool(true))),
        ));
    }
    if raw == "is not true" {
        return Ok(SqlExpr::IsDistinctFrom(
            Box::new(left),
            Box::new(SqlExpr::Const(Value::Bool(true))),
        ));
    }
    if raw == "is false" {
        return Ok(SqlExpr::IsNotDistinctFrom(
            Box::new(left),
            Box::new(SqlExpr::Const(Value::Bool(false))),
        ));
    }
    if raw == "is not false" {
        return Ok(SqlExpr::IsDistinctFrom(
            Box::new(left),
            Box::new(SqlExpr::Const(Value::Bool(false))),
        ));
    }
    if raw == "is unknown" {
        return Ok(SqlExpr::IsNull(Box::new(left)));
    }
    if raw == "is not unknown" {
        return Ok(SqlExpr::IsNotNull(Box::new(left)));
    }

    let mut right = None;
    let mut saw_not = false;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::expr | Rule::add_expr | Rule::primary_expr | Rule::cmp_expr => {
                right = Some(build_expr(part)?)
            }
            Rule::kw_not => saw_not = true,
            _ => {}
        }
    }

    let right = right.ok_or(ParseError::UnexpectedEof)?;
    Ok(if saw_not {
        SqlExpr::IsNotDistinctFrom(Box::new(left), Box::new(right))
    } else {
        SqlExpr::IsDistinctFrom(Box::new(left), Box::new(right))
    })
}

fn build_like_predicate(left: SqlExpr, pair: Pair<'_, Rule>) -> Result<SqlExpr, ParseError> {
    let mut negated = false;
    let mut case_insensitive = false;
    let mut pattern = None;
    let mut escape = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::kw_not => negated = true,
            Rule::kw_like => case_insensitive = false,
            Rule::kw_ilike => case_insensitive = true,
            Rule::concat_expr => {
                if pattern.is_none() {
                    pattern = Some(build_expr(part)?);
                }
            }
            Rule::escape_clause => {
                let expr = part
                    .into_inner()
                    .find(|inner| inner.as_rule() == Rule::concat_expr)
                    .ok_or(ParseError::UnexpectedEof)?;
                escape = Some(Box::new(build_expr(expr)?));
            }
            _ => {}
        }
    }
    Ok(SqlExpr::Like {
        expr: Box::new(left),
        pattern: Box::new(pattern.ok_or(ParseError::UnexpectedEof)?),
        escape,
        case_insensitive,
        negated,
    })
}

fn build_quantified_like_predicate(
    left: SqlExpr,
    pair: Pair<'_, Rule>,
) -> Result<SqlExpr, ParseError> {
    enum QuantifiedLikeRhs {
        Subquery(SelectStatement),
        Expr(SqlExpr),
    }

    let mut negated = false;
    let mut case_insensitive = false;
    let lowered = pair.as_str().to_ascii_lowercase();
    let is_all = if lowered.contains(" all ") {
        Some(true)
    } else if lowered.contains(" any ") {
        Some(false)
    } else {
        None
    };
    let mut rhs = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::kw_not => negated = true,
            Rule::kw_like => case_insensitive = false,
            Rule::kw_ilike => case_insensitive = true,
            Rule::select_stmt => rhs = Some(QuantifiedLikeRhs::Subquery(build_select(part)?)),
            Rule::expr => rhs = Some(QuantifiedLikeRhs::Expr(build_expr(part)?)),
            _ => {}
        }
    }

    let op = match (case_insensitive, negated) {
        (false, false) => SubqueryComparisonOp::Like,
        (false, true) => SubqueryComparisonOp::NotLike,
        (true, false) => SubqueryComparisonOp::ILike,
        (true, true) => SubqueryComparisonOp::NotILike,
    };
    let is_all = is_all.ok_or(ParseError::UnexpectedEof)?;
    match rhs.ok_or(ParseError::UnexpectedEof)? {
        QuantifiedLikeRhs::Subquery(subquery) => Ok(SqlExpr::QuantifiedSubquery {
            left: Box::new(left),
            op,
            is_all,
            subquery: Box::new(subquery),
        }),
        QuantifiedLikeRhs::Expr(array) => Ok(SqlExpr::QuantifiedArray {
            left: Box::new(left),
            op,
            is_all,
            array: Box::new(array),
        }),
    }
}

fn build_quantified_similar_predicate(
    left: SqlExpr,
    pair: Pair<'_, Rule>,
) -> Result<SqlExpr, ParseError> {
    enum QuantifiedSimilarRhs {
        Subquery(SelectStatement),
        Expr(SqlExpr),
    }

    let mut negated = false;
    let lowered = pair.as_str().to_ascii_lowercase();
    let is_all = if lowered.contains(" all ") {
        Some(true)
    } else if lowered.contains(" any ") {
        Some(false)
    } else {
        None
    };
    let mut rhs = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::kw_not => negated = true,
            Rule::select_stmt => rhs = Some(QuantifiedSimilarRhs::Subquery(build_select(part)?)),
            Rule::expr => rhs = Some(QuantifiedSimilarRhs::Expr(build_expr(part)?)),
            _ => {}
        }
    }

    let op = if negated {
        SubqueryComparisonOp::NotSimilar
    } else {
        SubqueryComparisonOp::Similar
    };
    let is_all = is_all.ok_or(ParseError::UnexpectedEof)?;
    match rhs.ok_or(ParseError::UnexpectedEof)? {
        QuantifiedSimilarRhs::Subquery(subquery) => Ok(SqlExpr::QuantifiedSubquery {
            left: Box::new(left),
            op,
            is_all,
            subquery: Box::new(subquery),
        }),
        QuantifiedSimilarRhs::Expr(array) => Ok(SqlExpr::QuantifiedArray {
            left: Box::new(left),
            op,
            is_all,
            array: Box::new(array),
        }),
    }
}

fn build_similar_predicate(left: SqlExpr, pair: Pair<'_, Rule>) -> Result<SqlExpr, ParseError> {
    let mut negated = false;
    let mut pattern = None;
    let mut escape = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::kw_not => negated = true,
            Rule::concat_expr => {
                if pattern.is_none() {
                    pattern = Some(build_expr(part)?);
                }
            }
            Rule::escape_clause => {
                let expr = part
                    .into_inner()
                    .find(|inner| inner.as_rule() == Rule::concat_expr)
                    .ok_or(ParseError::UnexpectedEof)?;
                escape = Some(Box::new(build_expr(expr)?));
            }
            _ => {}
        }
    }
    Ok(SqlExpr::Similar {
        expr: Box::new(left),
        pattern: Box::new(pattern.ok_or(ParseError::UnexpectedEof)?),
        escape,
        negated,
    })
}

fn build_trim_expr(pair: Pair<'_, Rule>) -> Result<SqlExpr, ParseError> {
    let trim_variant = pair
        .into_inner()
        .find(|inner| inner.as_rule() == Rule::trim_variant)
        .ok_or(ParseError::UnexpectedEof)?;
    let trim_variant = trim_variant
        .into_inner()
        .next()
        .ok_or(ParseError::UnexpectedEof)?;
    let trim_variant_rule = trim_variant.as_rule();
    let mut direction = "both";
    let mut trim_source = None;
    let mut trim_chars = None;
    for part in trim_variant.into_inner() {
        match part.as_rule() {
            Rule::trim_spec => direction = part.as_str(),
            Rule::expr => {
                let expr = build_expr(part)?;
                if trim_chars.is_none()
                    && matches!(
                        trim_variant_rule,
                        Rule::trim_spec_chars_from | Rule::trim_chars_from
                    )
                    && trim_source.is_none()
                {
                    trim_chars = Some(expr);
                } else {
                    trim_source = Some(expr);
                }
            }
            _ => {}
        }
    }
    let mut args = vec![SqlFunctionArg::positional(
        trim_source.ok_or(ParseError::UnexpectedEof)?,
    )];
    if let Some(chars) = trim_chars {
        args.push(SqlFunctionArg::positional(chars));
    }
    Ok(simple_func_call(
        match direction.to_ascii_lowercase().as_str() {
            "leading" => "ltrim",
            "trailing" => "rtrim",
            _ => "btrim",
        },
        args,
    ))
}

fn build_xml_attributes_expr(
    pair: Pair<'_, Rule>,
) -> Result<(Vec<SqlExpr>, Vec<String>), ParseError> {
    let mut named_args = Vec::new();
    let mut arg_names = Vec::new();
    for item in pair
        .into_inner()
        .filter(|part| part.as_rule() == Rule::xml_attribute_item)
    {
        let mut value = None;
        let mut name = None;
        for part in item.into_inner() {
            match part.as_rule() {
                Rule::expr => value = Some(build_expr(part)?),
                Rule::identifier => name = Some(build_identifier(part)),
                _ => {}
            }
        }
        named_args.push(value.ok_or(ParseError::UnexpectedEof)?);
        arg_names.push(name.unwrap_or_default());
    }
    Ok((named_args, arg_names))
}

fn build_xml_element_expr(pair: Pair<'_, Rule>) -> Result<SqlExpr, ParseError> {
    let mut name = None;
    let mut named_args = Vec::new();
    let mut arg_names = Vec::new();
    let mut args = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier if name.is_none() => name = Some(build_identifier(part)),
            Rule::xml_attributes_expr => {
                let (attr_values, attr_names) = build_xml_attributes_expr(part)?;
                named_args.extend(attr_values);
                arg_names.extend(attr_names);
            }
            Rule::expr => args.push(build_expr(part)?),
            _ => {}
        }
    }
    Ok(SqlExpr::Xml(Box::new(RawXmlExpr {
        op: RawXmlExprOp::Element,
        name,
        named_args,
        arg_names,
        args,
        xml_option: None,
        indent: None,
        target_type: None,
        standalone: None,
    })))
}

fn build_xml_forest_expr(pair: Pair<'_, Rule>) -> Result<SqlExpr, ParseError> {
    let mut args = Vec::new();
    let mut arg_names = Vec::new();
    for item in pair
        .into_inner()
        .filter(|part| part.as_rule() == Rule::xml_forest_item)
    {
        let mut value = None;
        let mut name = None;
        for part in item.into_inner() {
            match part.as_rule() {
                Rule::expr => value = Some(build_expr(part)?),
                Rule::identifier => name = Some(build_identifier(part)),
                _ => {}
            }
        }
        args.push(value.ok_or(ParseError::UnexpectedEof)?);
        arg_names.push(name.unwrap_or_default());
    }
    Ok(SqlExpr::Xml(Box::new(RawXmlExpr {
        op: RawXmlExprOp::Forest,
        name: None,
        named_args: Vec::new(),
        arg_names,
        args,
        xml_option: None,
        indent: None,
        target_type: None,
        standalone: None,
    })))
}

fn build_xml_parse_expr(pair: Pair<'_, Rule>) -> Result<SqlExpr, ParseError> {
    let mut xml_option = None;
    let mut args = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::kw_document => xml_option = Some(XmlOption::Document),
            Rule::kw_content => xml_option = Some(XmlOption::Content),
            Rule::expr => args.push(build_expr(part)?),
            _ => {}
        }
    }
    Ok(SqlExpr::Xml(Box::new(RawXmlExpr {
        op: RawXmlExprOp::Parse,
        name: None,
        named_args: Vec::new(),
        arg_names: Vec::new(),
        args,
        xml_option,
        indent: None,
        target_type: None,
        standalone: None,
    })))
}

fn build_xml_pi_expr(pair: Pair<'_, Rule>) -> Result<SqlExpr, ParseError> {
    let mut name = None;
    let mut args = Vec::new();
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::identifier if name.is_none() => name = Some(build_identifier(part)),
            Rule::expr => args.push(build_expr(part)?),
            _ => {}
        }
    }
    Ok(SqlExpr::Xml(Box::new(RawXmlExpr {
        op: RawXmlExprOp::Pi,
        name,
        named_args: Vec::new(),
        arg_names: Vec::new(),
        args,
        xml_option: None,
        indent: None,
        target_type: None,
        standalone: None,
    })))
}

fn build_xml_root_expr(pair: Pair<'_, Rule>) -> Result<SqlExpr, ParseError> {
    let mut args = Vec::new();
    let mut standalone = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::expr => args.push(build_expr(part)?),
            Rule::xml_root_standalone => {
                let has_yes = part.as_str().eq_ignore_ascii_case("yes");
                let has_no_value = part
                    .clone()
                    .into_inner()
                    .any(|inner| inner.as_rule() == Rule::kw_value);
                standalone = Some(if has_yes {
                    XmlStandalone::Yes
                } else if has_no_value {
                    XmlStandalone::NoValue
                } else {
                    XmlStandalone::No
                });
            }
            _ => {}
        }
    }
    Ok(SqlExpr::Xml(Box::new(RawXmlExpr {
        op: RawXmlExprOp::Root,
        name: None,
        named_args: Vec::new(),
        arg_names: Vec::new(),
        args,
        xml_option: None,
        indent: None,
        target_type: None,
        standalone,
    })))
}

fn build_xml_serialize_expr(pair: Pair<'_, Rule>) -> Result<SqlExpr, ParseError> {
    let mut xml_option = None;
    let mut args = Vec::new();
    let mut target_type = None;
    let mut saw_no = false;
    let mut indent = None;
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::kw_document => xml_option = Some(XmlOption::Document),
            Rule::kw_content => xml_option = Some(XmlOption::Content),
            Rule::expr => args.push(build_expr(part)?),
            Rule::type_name => target_type = Some(build_type_name(part)),
            Rule::kw_no => saw_no = true,
            Rule::kw_indent => indent = Some(!saw_no),
            _ => {}
        }
    }
    Ok(SqlExpr::Xml(Box::new(RawXmlExpr {
        op: RawXmlExprOp::Serialize,
        name: None,
        named_args: Vec::new(),
        arg_names: Vec::new(),
        args,
        xml_option,
        indent,
        target_type,
        standalone: None,
    })))
}

fn build_case_when(pair: Pair<'_, Rule>) -> Result<SqlCaseWhen, ParseError> {
    let mut expr = None;
    let mut result = None;
    for part in pair.into_inner() {
        if part.as_rule() == Rule::expr {
            if expr.is_none() {
                expr = Some(build_expr(part)?);
            } else {
                result = Some(build_expr(part)?);
            }
        }
    }
    Ok(SqlCaseWhen {
        expr: expr.ok_or(ParseError::UnexpectedEof)?,
        result: result.ok_or(ParseError::UnexpectedEof)?,
    })
}

fn build_case_expr(pair: Pair<'_, Rule>) -> Result<SqlExpr, ParseError> {
    let body = pair
        .into_inner()
        .find(|part| {
            matches!(
                part.as_rule(),
                Rule::searched_case_body | Rule::simple_case_body
            )
        })
        .ok_or(ParseError::UnexpectedEof)?;
    let mut arg = None;
    let mut args = Vec::new();
    let mut defresult = None;
    match body.as_rule() {
        Rule::searched_case_body => {
            for part in body.into_inner() {
                match part.as_rule() {
                    Rule::when_clause => args.push(build_case_when(part)?),
                    Rule::else_clause => {
                        let expr = part
                            .into_inner()
                            .find(|inner| inner.as_rule() == Rule::expr)
                            .ok_or(ParseError::UnexpectedEof)?;
                        defresult = Some(Box::new(build_expr(expr)?));
                    }
                    _ => {}
                }
            }
        }
        Rule::simple_case_body => {
            for part in body.into_inner() {
                match part.as_rule() {
                    Rule::expr if arg.is_none() => arg = Some(Box::new(build_expr(part)?)),
                    Rule::when_clause => args.push(build_case_when(part)?),
                    Rule::else_clause => {
                        let expr = part
                            .into_inner()
                            .find(|inner| inner.as_rule() == Rule::expr)
                            .ok_or(ParseError::UnexpectedEof)?;
                        defresult = Some(Box::new(build_expr(expr)?));
                    }
                    _ => {}
                }
            }
        }
        _ => {
            return Err(ParseError::UnexpectedToken {
                expected: "CASE body",
                actual: body.as_str().into(),
            });
        }
    }
    Ok(SqlExpr::Case {
        arg,
        args,
        defresult,
    })
}

fn fold_infix(
    first: SqlExpr,
    mut tail: pest::iterators::Pairs<'_, Rule>,
) -> Result<SqlExpr, ParseError> {
    let mut expr = first;
    while let Some(op) = tail.next() {
        let rhs = build_expr(tail.next().ok_or(ParseError::UnexpectedEof)?)?;
        expr = match op.as_rule() {
            Rule::kw_or => SqlExpr::Or(Box::new(expr), Box::new(rhs)),
            Rule::kw_and => SqlExpr::And(Box::new(expr), Box::new(rhs)),
            Rule::add_op => match op.as_str() {
                "+" => SqlExpr::Add(Box::new(expr), Box::new(rhs)),
                "-" => SqlExpr::Sub(Box::new(expr), Box::new(rhs)),
                _ => unreachable!(),
            },
            Rule::bit_op => match op.as_str().trim() {
                "&" => SqlExpr::BitAnd(Box::new(expr), Box::new(rhs)),
                "|" => SqlExpr::BitOr(Box::new(expr), Box::new(rhs)),
                "#" => SqlExpr::BitXor(Box::new(expr), Box::new(rhs)),
                _ => unreachable!(),
            },
            Rule::pow_op => simple_func_call(
                "power",
                vec![
                    SqlFunctionArg::positional(expr),
                    SqlFunctionArg::positional(rhs),
                ],
            ),
            Rule::shift_op => match op.as_str() {
                "<<" => SqlExpr::Shl(Box::new(expr), Box::new(rhs)),
                ">>" => SqlExpr::Shr(Box::new(expr), Box::new(rhs)),
                _ => unreachable!(),
            },
            Rule::concat_op => SqlExpr::Concat(Box::new(expr), Box::new(rhs)),
            Rule::mul_op => match op.as_str() {
                "*" => SqlExpr::Mul(Box::new(expr), Box::new(rhs)),
                "/" => SqlExpr::Div(Box::new(expr), Box::new(rhs)),
                "%" => SqlExpr::Mod(Box::new(expr), Box::new(rhs)),
                _ => unreachable!(),
            },
            _ => unreachable!(),
        };
    }
    Ok(expr)
}

fn fold_json_access(
    mut left: SqlExpr,
    mut inner: pest::iterators::Pairs<'_, Rule>,
) -> Result<SqlExpr, ParseError> {
    while let Some(op) = inner.next() {
        let right = build_expr(inner.next().ok_or(ParseError::UnexpectedEof)?)?;
        left = match op.as_str() {
            "->" => SqlExpr::JsonGet(Box::new(left), Box::new(right)),
            "->>" => SqlExpr::JsonGetText(Box::new(left), Box::new(right)),
            "#>" => SqlExpr::JsonPath(Box::new(left), Box::new(right)),
            "#>>" => SqlExpr::JsonPathText(Box::new(left), Box::new(right)),
            other => {
                return Err(ParseError::UnexpectedToken {
                    expected: "json access operator",
                    actual: other.into(),
                });
            }
        };
    }
    Ok(left)
}

fn decode_string_literal(raw: &str) -> Result<String, ParseError> {
    if raw.len() >= 2 && matches!(raw.as_bytes()[0], b'u' | b'U') && raw.as_bytes()[1] == b'&' {
        return decode_unicode_string_literal(raw);
    }

    if raw.starts_with('\'') {
        return Ok(raw[1..raw.len() - 1].replace("''", "'"));
    }

    if raw.len() >= 2 && matches!(raw.as_bytes()[0], b'e' | b'E') && raw.as_bytes()[1] == b'\'' {
        return decode_escape_string(&raw[1..]);
    }

    if raw.starts_with('$') {
        return decode_dollar_string(raw);
    }

    Err(ParseError::UnexpectedToken {
        expected: "string literal",
        actual: raw.into(),
    })
}

fn expr_is_array_syntax(expr: &SqlExpr) -> bool {
    match expr {
        SqlExpr::ArrayLiteral(_) => true,
        SqlExpr::Cast(_, RawTypeName::Builtin(ty)) => ty.is_array,
        SqlExpr::Const(Value::Array(_) | Value::PgArray(_)) => true,
        _ => false,
    }
}

fn expr_is_jsonb_syntax(expr: &SqlExpr) -> bool {
    match expr {
        SqlExpr::Cast(_, RawTypeName::Builtin(ty)) => {
            !ty.is_array && matches!(ty.kind, SqlTypeKind::Jsonb)
        }
        SqlExpr::Const(Value::Jsonb(_)) => true,
        _ => false,
    }
}

fn expr_is_jsonpath_syntax(expr: &SqlExpr) -> bool {
    match expr {
        SqlExpr::Cast(_, RawTypeName::Builtin(ty)) => {
            !ty.is_array && matches!(ty.kind, SqlTypeKind::JsonPath)
        }
        SqlExpr::Const(Value::JsonPath(_))
        | SqlExpr::Const(Value::Text(_))
        | SqlExpr::Const(Value::TextRef(_, _)) => true,
        _ => false,
    }
}

fn decode_string_literal_pair(pair: Pair<'_, Rule>) -> Result<String, ParseError> {
    match pair.as_rule() {
        Rule::unicode_string_literal => decode_unicode_string_literal(pair.as_str()),
        Rule::quoted_string_literal => {
            let inner = pair.into_inner().next().ok_or(ParseError::UnexpectedEof)?;
            decode_string_literal_pair(inner)
        }
        _ => decode_string_literal(pair.as_str()),
    }
}

fn decode_unicode_quoted_identifier(raw: &str) -> Result<String, ParseError> {
    let (literal, escape_char) = split_unicode_literal_parts(raw)?;
    let text = literal
        .strip_prefix('"')
        .and_then(|v| v.strip_suffix('"'))
        .ok_or(ParseError::UnexpectedToken {
            expected: "unicode quoted identifier",
            actual: raw.into(),
        })?
        .replace("\"\"", "\"");
    decode_unicode_escapes(&text, escape_char)
}

// :HACK: psql describe queries still rely on explicit OPERATOR(pg_catalog....)
// syntax that the grammar does not parse natively yet. Keep the shim narrow,
// but make it lexical-state-aware so it never rewrites inside strings,
// identifiers, or comments.
fn normalize_psql_describe_syntax_preserving_layout(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out = bytes.to_vec();
    let mut i = 0usize;
    let mut block_depth = 0usize;
    let mut dollar_tag: Option<Vec<u8>> = None;

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum State {
        Normal,
        LineComment,
        BlockComment,
        DollarString,
    }

    let mut state = State::Normal;

    while i < bytes.len() {
        match state {
            State::Normal => {
                if starts_line_comment(bytes, i) {
                    i += 2;
                    state = State::LineComment;
                } else if starts_block_comment(bytes, i) {
                    i += 2;
                    block_depth = 1;
                    state = State::BlockComment;
                } else if starts_escape_string_token(bytes, i) {
                    i = parse_delimited_token_end(bytes, i + 1, b'\'');
                } else if starts_unicode_string_token(bytes, i) {
                    i = parse_delimited_token_end(bytes, i + 2, b'\'');
                } else if starts_unicode_identifier_token(bytes, i) {
                    i = parse_delimited_token_end(bytes, i + 2, b'"');
                } else if bytes[i] == b'\'' {
                    i = parse_delimited_token_end(bytes, i, b'\'');
                } else if bytes[i] == b'"' {
                    i = parse_delimited_token_end(bytes, i, b'"');
                } else if let Some((tag, len)) = parse_dollar_tag(bytes, i) {
                    i += len;
                    dollar_tag = Some(tag);
                    state = State::DollarString;
                } else if let Some(end) =
                    rewrite_pg_operator_invocation_preserving_layout(bytes, &mut out, i)
                {
                    i = end;
                } else {
                    i += sql[i..].chars().next().expect("valid utf-8").len_utf8();
                }
            }
            State::LineComment => {
                if matches!(bytes[i], b'\n' | b'\r') {
                    i += 1;
                    state = State::Normal;
                } else {
                    i += 1;
                }
            }
            State::BlockComment => {
                if starts_block_comment(bytes, i) {
                    block_depth += 1;
                    i += 2;
                } else if ends_block_comment(bytes, i) {
                    block_depth -= 1;
                    i += 2;
                    if block_depth == 0 {
                        state = State::Normal;
                    }
                } else {
                    i += 1;
                }
            }
            State::DollarString => {
                if let Some(tag) = dollar_tag.as_ref() {
                    if matches_dollar_end(bytes, i, tag) {
                        i += tag.len() + 2;
                        dollar_tag = None;
                        state = State::Normal;
                    } else {
                        i += sql[i..].chars().next().expect("valid utf-8").len_utf8();
                    }
                } else {
                    state = State::Normal;
                }
            }
        }
    }

    String::from_utf8(out).expect("SQL normalization preserves UTF-8")
}

fn rewrite_pg_operator_invocation_preserving_layout(
    bytes: &[u8],
    out: &mut [u8],
    start: usize,
) -> Option<usize> {
    let prefix = b"operator(pg_catalog.";
    if !matches_ascii_insensitive(bytes, start, prefix)
        || !has_identifier_boundary_before(bytes, start)
    {
        return None;
    }
    let operator_start = start + prefix.len();
    let mut close = operator_start;
    while close < bytes.len() && bytes[close] != b')' {
        close += 1;
    }
    if close >= bytes.len() {
        return None;
    }
    let operator = std::str::from_utf8(&bytes[operator_start..close]).ok()?;
    if !is_simple_operator_token(operator) {
        return None;
    }
    out[start..=close].fill(b' ');
    out[start..start + operator.len()].copy_from_slice(operator.as_bytes());
    Some(close + 1)
}

fn matches_ascii_insensitive(bytes: &[u8], start: usize, pattern: &[u8]) -> bool {
    start + pattern.len() <= bytes.len()
        && bytes[start..start + pattern.len()].eq_ignore_ascii_case(pattern)
}

fn has_identifier_boundary_before(bytes: &[u8], start: usize) -> bool {
    start == 0 || !is_identifier_continuation(bytes[start - 1] as char)
}

fn has_identifier_boundary_after(bytes: &[u8], end: usize) -> bool {
    end >= bytes.len() || !is_identifier_continuation(bytes[end] as char)
}

fn starts_escape_string_token(bytes: &[u8], i: usize) -> bool {
    i + 1 < bytes.len() && matches!(bytes[i], b'e' | b'E') && bytes[i + 1] == b'\''
}

fn starts_unicode_identifier_token(bytes: &[u8], i: usize) -> bool {
    i + 2 < bytes.len()
        && matches!(bytes[i], b'u' | b'U')
        && bytes[i + 1] == b'&'
        && bytes[i + 2] == b'"'
}

fn starts_line_comment(bytes: &[u8], i: usize) -> bool {
    i + 1 < bytes.len() && bytes[i] == b'-' && bytes[i + 1] == b'-'
}

fn starts_block_comment(bytes: &[u8], i: usize) -> bool {
    i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'*'
}

fn ends_block_comment(bytes: &[u8], i: usize) -> bool {
    i + 1 < bytes.len() && bytes[i] == b'*' && bytes[i + 1] == b'/'
}

fn parse_dollar_tag(bytes: &[u8], start: usize) -> Option<(Vec<u8>, usize)> {
    if bytes.get(start) != Some(&b'$') {
        return None;
    }
    let mut end = start + 1;
    while end < bytes.len() && bytes[end] != b'$' {
        let byte = bytes[end];
        if !(byte.is_ascii_alphanumeric() || byte == b'_') {
            return None;
        }
        end += 1;
    }
    if end >= bytes.len() || bytes[end] != b'$' {
        return None;
    }
    Some((bytes[start + 1..end].to_vec(), end - start + 1))
}

fn matches_dollar_end(bytes: &[u8], start: usize, tag: &[u8]) -> bool {
    if bytes.get(start) != Some(&b'$') {
        return false;
    }
    let end = start + tag.len() + 1;
    end < bytes.len() && &bytes[start + 1..end] == tag && bytes[end] == b'$'
}

fn is_simple_operator_token(token: &str) -> bool {
    !token.is_empty()
        && token.bytes().all(|byte| {
            matches!(
                byte,
                b'!' | b'#'
                    | b'%'
                    | b'&'
                    | b'*'
                    | b'+'
                    | b'-'
                    | b'/'
                    | b'<'
                    | b'='
                    | b'>'
                    | b'?'
                    | b'@'
                    | b'^'
                    | b'|'
                    | b'~'
            )
        })
}

#[cfg(test)]
mod sql_normalization_tests {
    use super::normalize_psql_describe_syntax_preserving_layout;

    #[test]
    fn psql_describe_normalization_rewrites_exact_operator_without_touching_collation_clause() {
        let sql = "SELECT oid \
             FROM pg_catalog.pg_publication \
             WHERE pubname OPERATOR(pg_catalog.~) '^(pub)$' COLLATE pg_catalog.default \
             ORDER BY 1";
        let normalized = normalize_psql_describe_syntax_preserving_layout(sql);
        assert_eq!(normalized.len(), sql.len());
        assert!(
            !normalized
                .to_ascii_lowercase()
                .contains("operator(pg_catalog.~)")
        );
        assert!(normalized.contains("COLLATE pg_catalog.default"));
        assert!(normalized.contains("pubname ~"));
    }

    #[test]
    fn psql_describe_normalization_skips_single_and_escape_strings() {
        let sql = "SELECT 'OPERATOR(pg_catalog.~)', E'COLLATE pg_catalog.default'";
        assert_eq!(normalize_psql_describe_syntax_preserving_layout(sql), sql);
    }

    #[test]
    fn psql_describe_normalization_skips_unicode_and_dollar_strings() {
        let sql = "SELECT U&'OPERATOR(pg_catalog.~)' UESCAPE '!', $$COLLATE pg_catalog.default$$";
        assert_eq!(normalize_psql_describe_syntax_preserving_layout(sql), sql);
    }

    #[test]
    fn psql_describe_normalization_skips_quoted_identifiers() {
        let sql = "SELECT 1 AS \"OPERATOR(pg_catalog.~)\", 2 AS U&\"COLLATE pg_catalog.default\"";
        assert_eq!(normalize_psql_describe_syntax_preserving_layout(sql), sql);
    }

    #[test]
    fn psql_describe_normalization_skips_comments() {
        let sql = "SELECT 1 -- OPERATOR(pg_catalog.~) COLLATE pg_catalog.default\nFROM pg_catalog.pg_class";
        assert_eq!(normalize_psql_describe_syntax_preserving_layout(sql), sql);
    }
}

fn decode_unicode_string_literal(raw: &str) -> Result<String, ParseError> {
    let (literal, escape_char) = split_unicode_literal_parts(raw)?;
    let text = decode_string_literal(literal)?;
    decode_unicode_escapes(&text, escape_char)
}

fn split_unicode_literal_parts(raw: &str) -> Result<(&str, char), ParseError> {
    let lower = raw.to_ascii_lowercase();
    let Some(prefix_stripped) = lower.strip_prefix("u&") else {
        return Err(ParseError::UnexpectedToken {
            expected: "unicode string literal",
            actual: raw.into(),
        });
    };
    if let Some(idx) = prefix_stripped.find("uescape") {
        let literal_end = 2 + idx;
        let literal = raw[2..literal_end].trim_end();
        let clause = raw[literal_end..].trim_start();
        let clause_lower = clause.to_ascii_lowercase();
        if !clause_lower.starts_with("uescape") {
            return Err(ParseError::UnexpectedToken {
                expected: "UESCAPE clause",
                actual: clause.into(),
            });
        }
        let escape_raw = clause["UESCAPE".len()..].trim();
        let escape = decode_string_literal(escape_raw)?;
        let mut chars = escape.chars();
        let Some(ch) = chars.next() else {
            return Err(ParseError::UnexpectedToken {
                expected: "non-empty UESCAPE character",
                actual: raw.into(),
            });
        };
        if chars.next().is_some() || matches!(ch, '+' | '"' | '\'' | ' ' | '\t' | '\n' | '\r') {
            return Err(ParseError::UnexpectedToken {
                expected: "valid UESCAPE character",
                actual: raw.into(),
            });
        }
        Ok((literal.trim(), ch))
    } else {
        Ok((raw[2..].trim(), '\\'))
    }
}

fn decode_unicode_escapes(text: &str, escape_char: char) -> Result<String, ParseError> {
    let mut out = String::new();
    let chars: Vec<char> = text.chars().collect();
    let mut i = 0usize;
    while i < chars.len() {
        let ch = chars[i];
        if ch != escape_char {
            out.push(ch);
            i += 1;
            continue;
        }
        if i + 1 < chars.len() && chars[i + 1] == escape_char {
            out.push(escape_char);
            i += 2;
            continue;
        }
        if i + 1 >= chars.len() {
            return Err(ParseError::UnexpectedToken {
                expected: "valid Unicode escape sequence",
                actual: text.into(),
            });
        }
        let (digits, next) = if chars[i + 1] == '+' {
            if i + 8 > chars.len() {
                return Err(unicode_error("invalid Unicode escape"));
            }
            (chars[i + 2..i + 8].iter().collect::<String>(), i + 8)
        } else {
            if i + 5 > chars.len() {
                return Err(unicode_error("invalid Unicode escape"));
            }
            (chars[i + 1..i + 5].iter().collect::<String>(), i + 5)
        };
        let code = u32::from_str_radix(&digits, 16)
            .map_err(|_| unicode_error("invalid Unicode escape"))?;
        let (decoded, consumed_next) =
            decode_unicode_codepoint_with_surrogates(&chars, i, next, code, escape_char)?;
        out.push(decoded);
        i = consumed_next;
    }
    Ok(out)
}

fn parse_bit_string_literal(raw: &str) -> Result<BitString, ParseError> {
    let (prefix, literal) = raw.split_at(1);
    let decoded = decode_string_literal(literal)?;
    let bytes = decoded.as_bytes();
    match prefix.as_bytes()[0] {
        b'b' | b'B' => {
            let mut out = vec![0u8; BitString::byte_len(bytes.len() as i32)];
            for (idx, byte) in bytes.iter().enumerate() {
                match byte {
                    b'0' => {}
                    b'1' => out[idx / 8] |= 1 << (7 - (idx % 8)),
                    other => {
                        return Err(ParseError::UnexpectedToken {
                            expected: "valid binary digit",
                            actual: format!(
                                "\"{}\" is not a valid binary digit",
                                char::from(*other)
                            ),
                        });
                    }
                }
            }
            Ok(BitString::new(bytes.len() as i32, out))
        }
        b'x' | b'X' => {
            let mut out = Vec::with_capacity(bytes.len().div_ceil(2));
            let mut pending = None::<u8>;
            for byte in bytes {
                let nibble = match byte {
                    b'0'..=b'9' => *byte - b'0',
                    b'a'..=b'f' => *byte - b'a' + 10,
                    b'A'..=b'F' => *byte - b'A' + 10,
                    other => {
                        return Err(ParseError::UnexpectedToken {
                            expected: "valid hexadecimal digit",
                            actual: format!(
                                "\"{}\" is not a valid hexadecimal digit",
                                char::from(*other)
                            ),
                        });
                    }
                };
                if let Some(high) = pending.take() {
                    out.push((high << 4) | nibble);
                } else {
                    pending = Some(nibble);
                }
            }
            if let Some(high) = pending {
                out.push(high << 4);
            }
            Ok(BitString::new((bytes.len() * 4) as i32, out))
        }
        _ => Err(ParseError::UnexpectedToken {
            expected: "bit string literal",
            actual: raw.into(),
        }),
    }
}

fn decode_escape_string(raw: &str) -> Result<String, ParseError> {
    let text = raw[1..raw.len() - 1].replace("''", "'");
    let mut out = String::new();
    let mut chars = text.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        let escaped = chars.next().ok_or(ParseError::UnexpectedEof)?;
        match escaped {
            '\'' => out.push('\''),
            '"' => out.push('"'),
            '\\' => out.push('\\'),
            'b' => out.push('\u{0008}'),
            'f' => out.push('\u{000c}'),
            'n' => out.push('\n'),
            'r' => out.push('\r'),
            't' => out.push('\t'),
            'v' => out.push('\u{000b}'),
            'a' => out.push('\u{0007}'),
            'x' => {
                let hi = chars.next().ok_or(ParseError::UnexpectedEof)?;
                let lo = chars.next().ok_or(ParseError::UnexpectedEof)?;
                let value = u8::from_str_radix(&format!("{hi}{lo}"), 16).map_err(|_| {
                    ParseError::UnexpectedToken {
                        expected: "valid hex escape",
                        actual: raw.into(),
                    }
                })?;
                out.push(value as char);
            }
            'u' => {
                let code = collect_escape_digits(&mut chars, 4)?;
                let ch = decode_escape_codepoint(&mut chars, code)?;
                out.push(ch);
            }
            'U' => {
                let code = collect_escape_digits(&mut chars, 8)?;
                let ch = decode_escape_codepoint(&mut chars, code)?;
                out.push(ch);
            }
            '0'..='7' => {
                let mut digits = String::from(escaped);
                for _ in 0..2 {
                    if let Some(next) = chars.peek().copied() {
                        if ('0'..='7').contains(&next) {
                            digits.push(chars.next().unwrap());
                        } else {
                            break;
                        }
                    }
                }
                let value =
                    u8::from_str_radix(&digits, 8).map_err(|_| ParseError::UnexpectedToken {
                        expected: "valid octal escape",
                        actual: raw.into(),
                    })?;
                out.push(value as char);
            }
            other => out.push(other),
        }
    }
    Ok(out)
}

fn collect_escape_digits(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
    len: usize,
) -> Result<u32, ParseError> {
    let mut digits = String::with_capacity(len);
    for _ in 0..len {
        let Some(ch) = chars.next() else {
            return Err(unicode_error("invalid Unicode escape"));
        };
        digits.push(ch);
    }
    u32::from_str_radix(&digits, 16).map_err(|_| unicode_error("invalid Unicode escape"))
}

fn decode_dollar_string(raw: &str) -> Result<String, ParseError> {
    let end_tag_start = raw[1..]
        .find('$')
        .map(|idx| idx + 1)
        .ok_or(ParseError::UnexpectedEof)?;
    let tag = &raw[..=end_tag_start];
    let suffix = &raw[end_tag_start + 1..];
    let closing = suffix.rfind(tag).ok_or(ParseError::UnexpectedToken {
        expected: "matching dollar-quote terminator",
        actual: raw.into(),
    })?;
    Ok(suffix[..closing].to_string())
}

fn decode_escape_codepoint(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
    code: u32,
) -> Result<char, ParseError> {
    if let Some(high) = as_high_surrogate(code) {
        let low_prefix = chars
            .next()
            .ok_or_else(|| unicode_error("invalid Unicode surrogate pair"))?;
        let expected_len = match low_prefix {
            '\\' => match chars
                .next()
                .ok_or_else(|| unicode_error("invalid Unicode surrogate pair"))?
            {
                'u' => 4,
                'U' => 8,
                _ => return Err(unicode_error("invalid Unicode surrogate pair")),
            },
            _ => return Err(unicode_error("invalid Unicode surrogate pair")),
        };
        let low = collect_escape_digits(chars, expected_len)?;
        let Some(low) = as_low_surrogate(low) else {
            return Err(unicode_error("invalid Unicode surrogate pair"));
        };
        let codepoint = 0x10000 + (((high as u32) - 0xD800) << 10) + ((low as u32) - 0xDC00);
        char::from_u32(codepoint).ok_or_else(|| unicode_error("invalid Unicode escape value"))
    } else if as_low_surrogate(code).is_some() {
        Err(unicode_error("invalid Unicode surrogate pair"))
    } else {
        char::from_u32(code).ok_or_else(|| unicode_error("invalid Unicode escape value"))
    }
}

fn decode_unicode_codepoint_with_surrogates(
    chars: &[char],
    start: usize,
    next: usize,
    code: u32,
    escape_char: char,
) -> Result<(char, usize), ParseError> {
    if let Some(high) = as_high_surrogate(code) {
        let Some((low, consumed)) = parse_next_unicode_escape(chars, next, escape_char)? else {
            return Err(unicode_error("invalid Unicode surrogate pair"));
        };
        let Some(low) = as_low_surrogate(low) else {
            return Err(unicode_error("invalid Unicode surrogate pair"));
        };
        let codepoint = 0x10000 + (((high as u32) - 0xD800) << 10) + ((low as u32) - 0xDC00);
        let decoded = char::from_u32(codepoint)
            .ok_or_else(|| unicode_error("invalid Unicode escape value"))?;
        Ok((decoded, consumed))
    } else if as_low_surrogate(code).is_some() {
        let _ = start;
        Err(unicode_error("invalid Unicode surrogate pair"))
    } else {
        let decoded =
            char::from_u32(code).ok_or_else(|| unicode_error("invalid Unicode escape value"))?;
        Ok((decoded, next))
    }
}

fn parse_next_unicode_escape(
    chars: &[char],
    start: usize,
    escape_char: char,
) -> Result<Option<(u32, usize)>, ParseError> {
    if start >= chars.len() {
        return Ok(None);
    }
    if chars[start] != escape_char {
        return Ok(None);
    }
    if start + 1 >= chars.len() {
        return Err(unicode_error("invalid Unicode surrogate pair"));
    }
    let (digits, next) = if chars[start + 1] == '+' {
        if start + 8 > chars.len() {
            return Err(unicode_error("invalid Unicode surrogate pair"));
        }
        (
            chars[start + 2..start + 8].iter().collect::<String>(),
            start + 8,
        )
    } else {
        if start + 5 > chars.len() {
            return Err(unicode_error("invalid Unicode surrogate pair"));
        }
        (
            chars[start + 1..start + 5].iter().collect::<String>(),
            start + 5,
        )
    };
    let code = u32::from_str_radix(&digits, 16)
        .map_err(|_| unicode_error("invalid Unicode surrogate pair"))?;
    Ok(Some((code, next)))
}

fn unicode_error(message: &'static str) -> ParseError {
    ParseError::UnexpectedToken {
        expected: "valid Unicode string literal",
        actual: message.into(),
    }
}

fn as_high_surrogate(code: u32) -> Option<u16> {
    (0xD800..=0xDBFF).contains(&code).then_some(code as u16)
}

fn as_low_surrogate(code: u32) -> Option<u16> {
    (0xDC00..=0xDFFF).contains(&code).then_some(code as u16)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pest_parses_named_window_clause_statement() {
        SqlParser::parse(
            Rule::statement,
            "select row_number() over w from people window w as ()",
        )
        .unwrap();
    }

    #[test]
    fn pest_parses_window_frame_statement() {
        SqlParser::parse(
            Rule::statement,
            "select sum(id) over (w rows between 1 preceding and current row) from people window w as (partition by name order by id)",
        )
        .unwrap();
    }

    #[test]
    fn parse_notify_listen_and_unlisten_statements() {
        assert_eq!(
            parse_statement("notify alerts, 'hello'").unwrap(),
            Statement::Notify(NotifyStatement {
                channel: "alerts".to_string(),
                payload: Some("hello".to_string()),
            })
        );
        assert_eq!(
            parse_statement("listen alerts").unwrap(),
            Statement::Listen(ListenStatement {
                channel: "alerts".to_string(),
            })
        );
        assert_eq!(
            parse_statement("unlisten *").unwrap(),
            Statement::Unlisten(UnlistenStatement { channel: None })
        );
    }

    #[test]
    fn parse_create_operator_statement() {
        assert_eq!(
            parse_statement(
                "create operator === (procedure = regoperator_test_fn, leftarg = boolean, rightarg = boolean, restrict = customcontsel, join = contjoinsel, hashes, merges)"
            )
            .unwrap(),
            Statement::CreateOperator(CreateOperatorStatement {
                schema_name: None,
                operator_name: "===".to_string(),
                left_arg: Some(RawTypeName::Builtin(SqlType::new(SqlTypeKind::Bool))),
                right_arg: Some(RawTypeName::Builtin(SqlType::new(SqlTypeKind::Bool))),
                procedure: QualifiedNameRef {
                    schema_name: None,
                    name: "regoperator_test_fn".to_string(),
                },
                commutator: None,
                negator: None,
                restrict: Some(QualifiedNameRef {
                    schema_name: None,
                    name: "customcontsel".to_string(),
                }),
                join: Some(QualifiedNameRef {
                    schema_name: None,
                    name: "contjoinsel".to_string(),
                }),
                hashes: true,
                merges: true,
            })
        );
    }

    #[test]
    fn parse_alter_and_drop_operator_statements() {
        assert_eq!(
            parse_statement(
                "alter operator === (boolean, boolean) set (restrict = none, join = contjoinsel, hashes, merges = false)"
            )
            .unwrap(),
            Statement::AlterOperator(AlterOperatorStatement {
                schema_name: None,
                operator_name: "===".to_string(),
                left_arg: Some(RawTypeName::Builtin(SqlType::new(SqlTypeKind::Bool))),
                right_arg: Some(RawTypeName::Builtin(SqlType::new(SqlTypeKind::Bool))),
                options: vec![
                    AlterOperatorOption::Restrict {
                        option_name: "restrict".to_string(),
                        function: None,
                    },
                    AlterOperatorOption::Join {
                        option_name: "join".to_string(),
                        function: Some(QualifiedNameRef {
                            schema_name: None,
                            name: "contjoinsel".to_string(),
                        }),
                    },
                    AlterOperatorOption::Hashes {
                        option_name: "hashes".to_string(),
                        enabled: true,
                    },
                    AlterOperatorOption::Merges {
                        option_name: "merges".to_string(),
                        enabled: false,
                    },
                ],
            })
        );
        assert_eq!(
            parse_statement("drop operator if exists public.===(boolean, boolean) cascade")
                .unwrap(),
            Statement::DropOperator(DropOperatorStatement {
                if_exists: true,
                schema_name: Some("public".to_string()),
                operator_name: "===".to_string(),
                left_arg: Some(RawTypeName::Builtin(SqlType::new(SqlTypeKind::Bool))),
                right_arg: Some(RawTypeName::Builtin(SqlType::new(SqlTypeKind::Bool))),
            })
        );
    }

    #[test]
    fn parse_regoperator_type_name() {
        assert_eq!(
            parse_type_name("regoperator").unwrap(),
            RawTypeName::Builtin(SqlType::new(SqlTypeKind::RegOperator))
        );
    }
}
