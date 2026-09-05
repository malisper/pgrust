//! postgresImportForeignSchema (postgres_fdw.c:5466): query the remote
//! catalogs and build one CREATE FOREIGN TABLE statement per remote table.

use mcx::{Mcx, PgVec};
use pgclient::ExecStatus;
use types_core::Oid;
use types_error::{
    PgError, PgResult, ERRCODE_FDW_INVALID_OPTION_NAME, ERRCODE_FDW_SCHEMA_NOT_FOUND,
};
use types_nodes::rawnodes::{ImportForeignSchemaStmt, ImportForeignSchemaType};

use crate::connection;

const ATTRIBUTE_GENERATED_STORED: u8 = b's';

fn push_quoted_ident(out: &mut String, mcx: Mcx<'_>, s: &str) -> PgResult<()> {
    let q = adt_quote::quote_identifier(mcx, s.as_bytes())?;
    // SAFETY: quote_identifier preserves the (UTF-8) ident bytes, only adding
    // ASCII `"` quoting.
    out.push_str(unsafe { core::str::from_utf8_unchecked(q.as_bytes()) });
    Ok(())
}

fn string_literal(out: &mut String, val: &str) {
    // deparseStringLiteral (deparse.c).
    if val.contains('\\') {
        out.push_str(" E'");
    } else {
        out.push('\'');
    }
    for ch in val.chars() {
        if ch == '\'' || ch == '\\' {
            out.push(ch);
        }
        out.push(ch);
    }
    out.push('\'');
}

fn col<'a>(row: &'a [Option<Vec<u8>>], i: usize) -> PgResult<Option<&'a str>> {
    // Remote text-format values are supposed to arrive in the local (database)
    // encoding, but the bytes come straight off the wire from a foreign server
    // that may be hostile, compromised, or MITM'd (the transport has no TLS),
    // so nothing has validated them. Feeding un-verified bytes into a &str would
    // violate Rust's UTF-8 invariant (UB) and let a remote inject invalid
    // encoding into the generated CREATE FOREIGN TABLE DDL, quote_identifier,
    // and the local SQL parser. Verify against the database encoding first, as
    // C's pg_verifymbstr / pg_client_to_server would when client_encoding ==
    // database encoding, then use the checked conversion so no
    // from_utf8_unchecked sink is reachable from wire data.
    match row.get(i).and_then(|c| c.as_deref()) {
        None => Ok(None),
        Some(b) => {
            mbutils::pg_verifymbstr(b, false)?;
            let s = core::str::from_utf8(b).map_err(|_| {
                Box::new(PgError::error(
                    "invalid byte sequence in remote result".to_string(),
                ))
            })?;
            Ok(Some(s))
        }
    }
}

/// The IMPORT FOREIGN SCHEMA option set (postgres_fdw.c:5478-5500 defaults).
#[derive(Clone, Copy)]
pub(crate) struct ImportOptions {
    pub(crate) import_collate: bool,
    pub(crate) import_default: bool,
    pub(crate) import_generated: bool,
    pub(crate) import_not_null: bool,
}

impl Default for ImportOptions {
    fn default() -> Self {
        ImportOptions {
            import_collate: true,
            import_default: false,
            import_generated: true,
            import_not_null: true,
        }
    }
}

/// The remote column-metadata query (postgres_fdw.c:5552-5640): one row per
/// (relation, column) of the importable relations in `remote_schema`.
/// search_path is pinned to pg_catalog on the connection, so format_type()
/// and pg_get_expr() always schema-qualify, as C relies on.
pub(crate) fn import_columns_query(
    server_version: i32,
    import_collate: bool,
    list_type: ImportForeignSchemaType,
    remote_schema: &str,
    table_names: &[&str],
) -> String {
    let mut buf = String::from(
        "SELECT relname,   attname,   format_type(atttypid, atttypmod),   attnotnull,   \
         pg_get_expr(adbin, adrelid), ",
    );
    // Generated columns are supported since Postgres 12 (postgres_fdw.c:5559).
    if server_version >= 120000 {
        buf.push_str("  attgenerated, ");
    } else {
        buf.push_str("  NULL, ");
    }
    if import_collate {
        buf.push_str("  collname,   collnsp.nspname ");
    } else {
        buf.push_str("  NULL, NULL ");
    }
    buf.push_str(
        "FROM pg_class c   JOIN pg_namespace n ON     relnamespace = n.oid   \
         LEFT JOIN pg_attribute a ON     attrelid = c.oid AND attnum > 0       \
         AND NOT attisdropped   LEFT JOIN pg_attrdef ad ON     adrelid = c.oid AND adnum = attnum ",
    );
    if import_collate {
        buf.push_str(
            "  LEFT JOIN pg_collation coll ON     coll.oid = attcollation   \
             LEFT JOIN pg_namespace collnsp ON     collnsp.oid = collnamespace ",
        );
    }
    buf.push_str("WHERE c.relkind IN ('r','v','f','m','p')   AND n.nspname = ");
    string_literal(&mut buf, remote_schema);
    // Partitions are supported since Postgres 10 (postgres_fdw.c:5602).
    if server_version >= 100000 && list_type != ImportForeignSchemaType::FDW_IMPORT_SCHEMA_LIMIT_TO {
        buf.push_str(" AND NOT c.relispartition ");
    }
    if matches!(
        list_type,
        ImportForeignSchemaType::FDW_IMPORT_SCHEMA_LIMIT_TO
            | ImportForeignSchemaType::FDW_IMPORT_SCHEMA_EXCEPT
    ) {
        buf.push_str(" AND c.relname ");
        if list_type == ImportForeignSchemaType::FDW_IMPORT_SCHEMA_EXCEPT {
            buf.push_str("NOT ");
        }
        buf.push_str("IN (");
        let mut first = true;
        for name in table_names {
            if first {
                first = false;
            } else {
                buf.push_str(", ");
            }
            string_literal(&mut buf, name);
        }
        buf.push(')');
    }
    buf.push_str(" ORDER BY c.relname, a.attnum");
    buf
}

/// One column of the generated CREATE FOREIGN TABLE (postgres_fdw.c:5675-5725).
#[allow(clippy::too_many_arguments)]
pub(crate) fn append_column_def(
    sql: &mut String,
    mcx: Mcx<'_>,
    opts: &ImportOptions,
    attname: &str,
    typename: &str,
    attnotnull: &str,
    attdefault: Option<&str>,
    attgenerated: Option<&str>,
    collname: Option<&str>,
    collnamespace: Option<&str>,
) -> PgResult<()> {
    sql.push_str("  ");
    push_quoted_ident(sql, mcx, attname)?;
    sql.push(' ');
    sql.push_str(typename);

    // column_name keeps the link when the local column is renamed.
    sql.push_str(" OPTIONS (column_name ");
    string_literal(sql, attname);
    sql.push(')');

    if opts.import_collate {
        if let (Some(cn), Some(cns)) = (collname, collnamespace) {
            sql.push_str(" COLLATE ");
            push_quoted_ident(sql, mcx, cns)?;
            sql.push('.');
            push_quoted_ident(sql, mcx, cn)?;
        }
    }
    sql.push_str(&column_def_tail(opts, attnotnull, attdefault, attgenerated));
    Ok(())
}

/// The DEFAULT / GENERATED / NOT NULL tail of one imported column
/// (postgres_fdw.c:5700-5725).
pub(crate) fn column_def_tail(
    opts: &ImportOptions,
    attnotnull: &str,
    attdefault: Option<&str>,
    attgenerated: Option<&str>,
) -> String {
    let mut sql = String::new();
    if opts.import_default {
        if let Some(d) = attdefault {
            if attgenerated.is_none_or(|g| g.is_empty()) {
                sql.push_str(" DEFAULT ");
                sql.push_str(d);
            }
        }
    }
    if opts.import_generated {
        if let Some(g) = attgenerated {
            if g.as_bytes().first() == Some(&ATTRIBUTE_GENERATED_STORED) {
                // postgres_fdw.c:5712 only Asserts attdefault != NULL; a
                // production build prints "%s" of NULL as "(null)"
                // (src/port/snprintf.c:691) rather than aborting.
                sql.push_str(" GENERATED ALWAYS AS (");
                sql.push_str(attdefault.unwrap_or("(null)"));
                sql.push_str(") STORED");
            }
        }
    }
    if opts.import_not_null && attnotnull.as_bytes().first() == Some(&b't') {
        sql.push_str(" NOT NULL");
    }
    sql
}

pub fn postgresImportForeignSchema<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &ImportForeignSchemaStmt<'mcx>,
    server_oid: Oid,
) -> PgResult<PgVec<'mcx, &'mcx str>> {
    let mut opts = ImportOptions::default();

    for n in stmt.options.iter() {
        let def = n.as_def_elem().expect("IMPORT option list holds DefElems");
        match def.defname.unwrap_or("") {
            "import_collate" => opts.import_collate = commands_define::defGetBoolean(def)?,
            "import_default" => opts.import_default = commands_define::defGetBoolean(def)?,
            "import_generated" => opts.import_generated = commands_define::defGetBoolean(def)?,
            "import_not_null" => opts.import_not_null = commands_define::defGetBoolean(def)?,
            other => {
                return Err(Box::new(
                    PgError::error(format!("invalid option \"{other}\""))
                        .with_sqlstate(ERRCODE_FDW_INVALID_OPTION_NAME),
                ))
            }
        }
    }

    let server = foreigncmds::foreign::GetForeignServer(mcx, server_oid)?;
    let mapping =
        foreigncmds::foreign::GetUserMapping(mcx, miscinit::GetUserId(), server.serverid)?;
    let conn_key = connection::get_connection(mcx, &mapping, false)?;

    let remote_schema = stmt.remote_schema.expect("ImportForeignSchemaStmt.remote_schema");

    // The remote schema must exist.
    let mut buf = String::from("SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = ");
    string_literal(&mut buf, remote_schema);
    let res = connection::exec_query(conn_key, &buf)?;
    if res.status != ExecStatus::TuplesOk {
        return Err(connection::remote_error(&res, Some(&buf)));
    }
    if res.rows.len() != 1 {
        return Err(Box::new(
            PgError::error(format!(
                "schema \"{remote_schema}\" is not present on foreign server \"{}\"",
                server.servername
            ))
            .with_sqlstate(ERRCODE_FDW_SCHEMA_NOT_FOUND),
        ));
    }

    // Column metadata for every importable relation in the remote schema.
    let mut table_names: Vec<&str> = Vec::new();
    if matches!(
        stmt.list_type,
        ImportForeignSchemaType::FDW_IMPORT_SCHEMA_LIMIT_TO
            | ImportForeignSchemaType::FDW_IMPORT_SCHEMA_EXCEPT
    ) {
        for n in stmt.table_list.iter() {
            let rv = n.as_variant::<types_nodes::primnodes::RangeVar>().expect("RangeVar");
            table_names.push(rv.relname.expect("RangeVar.relname"));
        }
    }
    let buf = import_columns_query(
        connection::server_version(conn_key)?,
        opts.import_collate,
        stmt.list_type,
        remote_schema,
        &table_names,
    );

    let res = connection::exec_query(conn_key, &buf)?;
    if res.status != ExecStatus::TuplesOk {
        return Err(connection::remote_error(&res, Some(&buf)));
    }

    // DIVERGENCE from C: the local schema is baked into the emitted name.
    // C leaves it unqualified and utility.c overwrites relation->schemaname
    // after parsing; pgrust parse trees are immutable once built (see
    // utility's exec_import_foreign_schema_commands).
    let local_schema = stmt.local_schema.expect("ImportForeignSchemaStmt.local_schema");

    let mut commands: PgVec<'mcx, &'mcx str> = PgVec::new_in(mcx);
    let numrows = res.rows.len();
    let mut i = 0usize;
    while i < numrows {
        let tablename = col(&res.rows[i], 0)?.unwrap_or("");
        let mut sql = String::new();
        sql.push_str("CREATE FOREIGN TABLE ");
        push_quoted_ident(&mut sql, mcx, local_schema)?;
        sql.push('.');
        push_quoted_ident(&mut sql, mcx, tablename)?;
        sql.push_str(" (\n");
        let mut first_item = true;
        loop {
            let row = &res.rows[i];
            // A table with no columns shows up as a single all-NULL row.
            if let Some(attname) = col(row, 1)? {
                let typename = col(row, 2)?.unwrap_or("");
                let attnotnull = col(row, 3)?.unwrap_or("");
                let attdefault = col(row, 4)?;
                let attgenerated = col(row, 5)?;
                let collname = col(row, 6)?;
                let collnamespace = col(row, 7)?;

                if first_item {
                    first_item = false;
                } else {
                    sql.push_str(",\n");
                }
                append_column_def(
                    &mut sql,
                    mcx,
                    &opts,
                    attname,
                    typename,
                    attnotnull,
                    attdefault,
                    attgenerated,
                    collname,
                    collnamespace,
                )?;
            }
            i += 1;
            if i >= numrows || col(&res.rows[i], 0)?.unwrap_or("") != tablename {
                break;
            }
        }

        sql.push_str("\n) SERVER ");
        push_quoted_ident(&mut sql, mcx, server.servername)?;
        sql.push_str("\nOPTIONS (");
        sql.push_str("schema_name ");
        string_literal(&mut sql, remote_schema);
        sql.push_str(", table_name ");
        string_literal(&mut sql, tablename);
        sql.push_str(");");

        let bytes = mcx::slice_borrow_in(mcx, sql.as_bytes())?;
        // SAFETY: the source was a String (valid UTF-8), copied byte-for-byte.
        commands.push(unsafe { core::str::from_utf8_unchecked(bytes) });
    }

    connection::release_connection(conn_key);
    Ok(commands)
}

#[cfg(test)]
mod tests {
    use super::{col, column_def_tail, import_columns_query, ImportOptions};
    use types_nodes::rawnodes::ImportForeignSchemaType;

    // postgres_fdw.c:5559-5564 / 5602-5604: attgenerated is only selected from
    // a remote server >= 12 (NULL otherwise) and "AND NOT c.relispartition" is
    // only appended for a remote >= 10; both are gated on PQserverVersion.
    #[test]
    fn columns_query_is_gated_on_the_remote_server_version() {
        let q18 = import_columns_query(
            180006,
            true,
            ImportForeignSchemaType::FDW_IMPORT_SCHEMA_ALL,
            "s",
            &[],
        );
        assert!(q18.contains("  attgenerated, "), "{q18}");
        assert!(q18.contains(" AND NOT c.relispartition "), "{q18}");

        let q11 = import_columns_query(
            110022,
            true,
            ImportForeignSchemaType::FDW_IMPORT_SCHEMA_ALL,
            "s",
            &[],
        );
        assert!(!q11.contains("attgenerated"), "{q11}");
        assert!(q11.contains("pg_get_expr(adbin, adrelid),   NULL, "), "{q11}");
        assert!(q11.contains(" AND NOT c.relispartition "), "{q11}");

        let q96 = import_columns_query(
            90624,
            false,
            ImportForeignSchemaType::FDW_IMPORT_SCHEMA_EXCEPT,
            "s",
            &["t1", "t2"],
        );
        assert!(!q96.contains("attgenerated"), "{q96}");
        assert!(!q96.contains("relispartition"), "{q96}");
        assert!(q96.contains(" AND c.relname NOT IN ('t1', 't2')"), "{q96}");

        // LIMIT TO never filters partitions, whatever the version.
        let q18l = import_columns_query(
            180006,
            true,
            ImportForeignSchemaType::FDW_IMPORT_SCHEMA_LIMIT_TO,
            "s",
            &["t1"],
        );
        assert!(!q18l.contains("relispartition"), "{q18l}");
        assert!(q18l.contains(" AND c.relname IN ('t1')"), "{q18l}");
    }

    // postgres_fdw.c:5712: Assert(attdefault != NULL) is compiled out of a
    // production build, and appendStringInfo's %s of a NULL prints "(null)"
    // (src/port/snprintf.c:691). A remote catalog that reports a stored
    // generated column without an expression must not panic the backend.
    #[test]
    fn generated_column_without_expression_prints_null_like_c() {
        let opts = ImportOptions::default();
        assert_eq!(
            column_def_tail(&opts, "f", None, Some("s")),
            " GENERATED ALWAYS AS ((null)) STORED"
        );
        // The ordinary shapes are unchanged.
        assert_eq!(
            column_def_tail(&opts, "t", Some("(a * 2)"), Some("s")),
            " GENERATED ALWAYS AS ((a * 2)) STORED NOT NULL"
        );
        let with_default = ImportOptions { import_default: true, ..ImportOptions::default() };
        assert_eq!(column_def_tail(&with_default, "f", Some("7"), None), " DEFAULT 7");
        assert_eq!(column_def_tail(&with_default, "f", Some("7"), Some("")), " DEFAULT 7");
        assert_eq!(
            column_def_tail(&with_default, "f", Some("(a * 2)"), Some("s")),
            " GENERATED ALWAYS AS ((a * 2)) STORED"
        );
    }

    // A hostile/MITM'd foreign server can put arbitrary bytes into result cells
    // (relname/attname/format_type/pg_get_expr). col() must never build a &str
    // out of invalid-encoding bytes (that was UB via from_utf8_unchecked); it
    // must verify the database encoding and reject bad bytes with a clean error.
    #[test]
    fn col_rejects_invalid_encoding() {
        // The default database encoding is SQL_ASCII, under which pg_verifymbstr
        // accepts every byte; the checked from_utf8 in col() is what keeps a
        // &str from ever being built out of invalid bytes.

        // Valid values pass through unchanged.
        let ok_row = vec![Some(b"orders".to_vec()), Some("caf\u{00e9}".as_bytes().to_vec())];
        assert_eq!(col(&ok_row, 0).unwrap(), Some("orders"));
        assert_eq!(col(&ok_row, 1).unwrap(), Some("caf\u{00e9}"));

        // A NULL cell (or out-of-range index) is None, not an error.
        let null_row: Vec<Option<Vec<u8>>> = vec![None];
        assert_eq!(col(&null_row, 0).unwrap(), None);
        assert_eq!(col(&null_row, 7).unwrap(), None);

        // A truncated multi-byte UTF-8 sequence (the concrete UB trigger) is
        // rejected instead of reaching any unsafe str construction.
        let bad_trunc = vec![Some(b"col\xc3".to_vec())];
        col(&bad_trunc, 0).err().unwrap();

        // An outright invalid byte is likewise rejected.
        let bad_byte = vec![Some(b"bad\xff".to_vec())];
        col(&bad_byte, 0).err().unwrap();
    }
}
