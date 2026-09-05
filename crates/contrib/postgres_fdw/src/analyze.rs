//! postgres_fdw.c:4948-5453 — the ANALYZE half of the FdwRoutine:
//! postgresAnalyzeForeignTable, postgresGetAnalyzeInfoForForeignTable,
//! postgresAcquireSampleRowsFunc and analyze_row_processor. The remote
//! relation is sampled over a DECLARE/FETCH cursor (optionally with remote
//! TABLESAMPLE / random() sampling per the analyze_sampling option), the rows
//! are converted through the column input functions and reservoir-sampled
//! into the ANALYZE arena; the caller (commands_analyze::do_analyze_rel)
//! computes the statistics and writes reltuples/pg_statistic.

use commands_analyze::{sampling, AcquireSampleRowsFn, FdwAnalyzeRoutine};
use datum::Datum;
use foreigncmds::foreign::{GetForeignServer, GetForeignTable, GetUserMapping};
use mcx::{Mcx, MemoryContext, PgString, PgVec};
use pgclient::{ExecStatus, QueryResult};
use types_core::{BlockNumber, Oid};
use types_error::{ErrorLevel, PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED};
use types_rel::Relation;
use types_tuple::{HeapTupleData, TupleDescData};

use crate::connection;
use crate::deparse;
use crate::exec::{convert_result_row, AttInMeta};
use crate::loc;

/// PgFdwSamplingMethod (postgres_fdw.h).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum SamplingMethod {
    Off,
    Auto,
    Random,
    System,
    Bernoulli,
}

// The analyze_sampling option value -> method (postgres_fdw.c:5127-5165);
// the validator already restricted the value to these five spellings.
fn sampling_method(value: &str, current: SamplingMethod) -> SamplingMethod {
    match value {
        "off" => SamplingMethod::Off,
        "auto" => SamplingMethod::Auto,
        "random" => SamplingMethod::Random,
        "system" => SamplingMethod::System,
        "bernoulli" => SamplingMethod::Bernoulli,
        _ => current,
    }
}

// PQgetvalue: NULL cells read as the empty string.
fn cell(res: &QueryResult, row: usize, col: usize) -> String {
    res.rows
        .get(row)
        .and_then(|r| r.get(col))
        .and_then(|c| c.as_ref())
        .map(|b| String::from_utf8_lossy(b).into_owned())
        .unwrap_or_default()
}

// C strtoul(s, NULL, 10): optional leading whitespace and sign, then the
// longest run of digits; nothing parseable is 0.
fn c_strtoul(s: &str) -> u64 {
    let t = s.trim_start();
    let (neg, t) = match t.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, t.strip_prefix('+').unwrap_or(t)),
    };
    let digits: String = t.chars().take_while(|c| c.is_ascii_digit()).collect();
    let v: u64 = digits.parse().unwrap_or(0);
    if neg {
        v.wrapping_neg()
    } else {
        v
    }
}

// C strtod(s, NULL): the longest prefix that parses as a float; else 0.
fn c_strtod(s: &str) -> f64 {
    let t = s.trim_start();
    for end in (1..=t.len()).rev() {
        if let Some(prefix) = t.get(..end) {
            if let Ok(v) = prefix.parse::<f64>() {
                return v;
            }
        }
    }
    0.0
}

/// postgresAnalyzeForeignTable (postgres_fdw.c:4948): always supported;
/// fetch the remote page count now (the ANALYZE API wants it up front).
pub(crate) fn postgres_analyze_foreign_table<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
) -> PgResult<Option<(AcquireSampleRowsFn, BlockNumber)>> {
    // Remote access as the table's owner, even if ANALYZE was started by
    // some other user (postgres_fdw.c:4970-4972).
    let table = GetForeignTable(mcx, relation.rd_id)?;
    let user = GetUserMapping(mcx, relation.rd_rel.relowner, table.serverid)?;
    let key = connection::get_connection(mcx, &user, false)?;

    let mut sql = PgString::new_in(mcx);
    deparse::deparse_analyze_size_sql(&mut sql, mcx, relation)?;
    let res = connection::exec_query(key, sql.as_str())?;
    if res.status != ExecStatus::TuplesOk {
        return Err(connection::remote_error(&res, Some(sql.as_str())));
    }
    if res.rows.len() != 1 || res.nfields != 1 {
        return Err(Box::new(PgError::error(
            "unexpected result from deparseAnalyzeSizeSql query",
        )));
    }
    let totalpages = c_strtoul(&cell(&res, 0, 0)) as BlockNumber;
    connection::release_connection(key);
    Ok(Some((postgres_acquire_sample_rows, totalpages)))
}

/// postgresGetAnalyzeInfoForForeignTable (postgres_fdw.c:5012): the remote
/// pg_class.reltuples and whether the remote relation supports TABLESAMPLE.
fn get_analyze_info<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    key: Oid,
) -> PgResult<(f64, bool)> {
    let mut sql = PgString::new_in(mcx);
    deparse::deparse_analyze_info_sql(&mut sql, mcx, relation)?;
    let res = connection::exec_query(key, sql.as_str())?;
    if res.status != ExecStatus::TuplesOk {
        return Err(connection::remote_error(&res, Some(sql.as_str())));
    }
    if res.rows.len() != 1 || res.nfields != 2 {
        return Err(Box::new(PgError::error(
            "unexpected result from deparseAnalyzeInfoSql query",
        )));
    }
    let reltuples = c_strtod(&cell(&res, 0, 0));
    let relkind = cell(&res, 0, 1).bytes().next().unwrap_or(0);
    // TABLESAMPLE is supported only for regular tables, matviews and
    // partitioned tables.
    let can_tablesample = matches!(relkind, b'r' | b'm' | b'p');
    Ok((reltuples, can_tablesample))
}

// make_tuple_from_result_row's materialization half: the converted datums
// (living in the per-row temp context) are formed into a heap tuple in the
// ANALYZE arena (file_fdw's form_sample_tuple shape).
fn form_sample_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    tupdesc: &TupleDescData<'_>,
    values: &[Datum],
    nulls: &[bool],
) -> PgResult<HeapTupleData<'mcx>> {
    let owned = heaptuple::heap_form_tuple(mcx, tupdesc, values, nulls)?;
    let (ptr, len, tid, oid) = (
        owned.image().as_ptr(),
        owned.as_tuple().t_len,
        owned.as_tuple().t_self,
        owned.as_tuple().t_tableOid,
    );
    core::mem::forget(owned);
    // SAFETY: the image lives in `mcx` (the Analyze context) until context
    // teardown; nothing else writes it.
    Ok(unsafe { HeapTupleData::from_raw_parts(ptr, len, tid, oid) })
}

/// postgresAcquireSampleRowsFunc (postgres_fdw.c:5083): reservoir-sample the
/// remote relation over a cursor. `totaldeadrows` is always 0.
fn postgres_acquire_sample_rows<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    elevel: ErrorLevel,
    rows: &mut PgVec<'mcx, HeapTupleData<'mcx>>,
    targrows: i32,
    totalrows: &mut f64,
    totaldeadrows: &mut f64,
) -> PgResult<i32> {
    // PgFdwAnalyzeState.
    let base = rows.len();
    let mut attin = AttInMeta::build(relation.name(), &relation.rd_att)?;
    let mut numrows: i32 = 0;
    let mut samplerows = 0.0f64;
    let mut rowstoskip = -1.0f64;
    let mut rstate = sampling::reservoir_init_selection_state(
        pg_prng::global_prng(|p| p.next_u64()),
        targrows as u32,
    );
    // C's "postgres_fdw temporary data" per-tuple context.
    let mut temp_cxt = MemoryContext::new_bump("postgres_fdw temporary data");

    // Remote access as the table's owner (postgres_fdw.c:5122-5125).
    let table = GetForeignTable(mcx, relation.rd_id)?;
    let server = GetForeignServer(mcx, table.serverid)?;
    let user = GetUserMapping(mcx, relation.rd_rel.relowner, table.serverid)?;
    let key = connection::get_connection(mcx, &user, false)?;
    let server_version_num = connection::server_version(key)?;

    // What sampling method should we use? (server option, then table option)
    let mut method = SamplingMethod::Auto;
    let mut sample_frac = -1.0f64;
    let mut reltuples = 0.0f64;
    for opt in server.options.iter() {
        if opt.name == "analyze_sampling" {
            method = sampling_method(opt.require_value()?, method);
            break;
        }
    }
    for opt in table.options.iter() {
        if opt.name == "analyze_sampling" {
            method = sampling_method(opt.require_value()?, method);
            break;
        }
    }

    // Error-out if explicitly required one of the TABLESAMPLE methods, but
    // the server does not support it (postgres_fdw.c:5171-5177).
    if server_version_num < 95000
        && matches!(method, SamplingMethod::System | SamplingMethod::Bernoulli)
    {
        return Err(Box::new(
            PgError::error("remote server does not support TABLESAMPLE feature")
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }

    // Remote sampling rate from the remote reltuples (postgres_fdw.c:5184-5236).
    if method != SamplingMethod::Off {
        let (rt, can_tablesample) = get_analyze_info(mcx, relation, key)?;
        reltuples = rt;
        if !can_tablesample && method == SamplingMethod::Auto {
            method = SamplingMethod::Random;
        }
        if reltuples <= 0.0 || targrows as f64 >= reltuples {
            method = SamplingMethod::Off;
        } else {
            sample_frac = targrows as f64 / reltuples;
            debug_assert!((0.0..=1.0).contains(&sample_frac));
        }
    }

    // "auto": BERNOULLI where TABLESAMPLE exists, random() on old servers.
    if method == SamplingMethod::Auto {
        method = if server_version_num < 95000 {
            SamplingMethod::Random
        } else {
            SamplingMethod::Bernoulli
        };
    }

    // Construct cursor that retrieves whole rows from remote.
    let cursor_number = connection::get_cursor_number();
    let mut sql = PgString::new_in(mcx);
    sql.try_push_str(&format!("DECLARE c{cursor_number} CURSOR FOR "))?;
    let retrieved_attrs =
        deparse::deparse_analyze_sql(&mut sql, mcx, relation, method, sample_frac)?;

    let res = connection::exec_query(key, sql.as_str())?;
    if res.status != ExecStatus::CommandOk {
        return Err(connection::remote_error(&res, Some(sql.as_str())));
    }

    // Determine the fetch size (server option, then table option; default 100).
    let mut fetch_size: i32 = 100;
    for opt in server.options.iter() {
        if opt.name == "fetch_size" {
            if let guc::units::ParseNum::Ok(v) = guc::units::parse_int(opt.require_value()?, 0) {
                fetch_size = v;
            }
            break;
        }
    }
    for opt in table.options.iter() {
        if opt.name == "fetch_size" {
            if let guc::units::ParseNum::Ok(v) = guc::units::parse_int(opt.require_value()?, 0) {
                fetch_size = v;
            }
            break;
        }
    }
    let fetch_sql = format!("FETCH {fetch_size} FROM c{cursor_number}");

    // Retrieve and process rows a batch at a time.
    loop {
        // Allow users to cancel long query.
        postgres_seams::check_for_interrupts::call()?;

        let res = connection::exec_query(key, &fetch_sql)?;
        // On error, report the original query, not the FETCH.
        if res.status != ExecStatus::TuplesOk {
            return Err(connection::remote_error(&res, Some(sql.as_str())));
        }

        let batch_rows = res.rows.len();
        for row in &res.rows {
            // analyze_row_processor (postgres_fdw.c:5394): always count the
            // row; the first targrows rows fill the reservoir, later rows
            // replace a random element per Vitter's algorithm.
            samplerows += 1.0;
            let pos = if numrows < targrows {
                numrows += 1;
                Some(base + numrows as usize - 1)
            } else {
                if rowstoskip < 0.0 {
                    rowstoskip =
                        sampling::reservoir_get_next_s(&mut rstate, samplerows, targrows as u32);
                }
                let pos = if rowstoskip <= 0.0 {
                    let k = (targrows as f64 * sampling::sampler_random_fract(&mut rstate.randstate))
                        as usize;
                    debug_assert!(k < targrows as usize);
                    Some(base + k)
                } else {
                    None
                };
                rowstoskip -= 1.0;
                pos
            };
            if let Some(pos) = pos {
                // make_tuple_from_result_row in anl_cxt, conversions in temp_cxt.
                let (values, nulls, _ctid) =
                    convert_result_row(&mut attin, &retrieved_attrs, row, temp_cxt.mcx())?;
                let tup = form_sample_tuple(mcx, &relation.rd_att, &values, &nulls)?;
                if pos == rows.len() {
                    rows.push(tup);
                } else {
                    // C heap_freetuple's the replaced copy; here it stays in
                    // the Analyze arena until teardown (native acquire precedent).
                    rows[pos] = tup;
                }
                temp_cxt.reset();
            }
        }

        // Must be EOF if we didn't get all the rows requested.
        if (batch_rows as i32) < fetch_size {
            break;
        }
    }

    // Close the cursor, just to be tidy (close_cursor, postgres_fdw.c:3903).
    let close_sql = format!("CLOSE c{cursor_number}");
    let res = connection::exec_query(key, &close_sql)?;
    if res.status != ExecStatus::CommandOk {
        return Err(connection::remote_error(&res, Some(close_sql.as_str())));
    }
    connection::release_connection(key);

    // We assume that we have no dead tuple.
    *totaldeadrows = 0.0;
    // Without sampling we've retrieved all living tuples; otherwise use the
    // remote reltuples estimate.
    *totalrows = if method == SamplingMethod::Off { samplerows } else { reltuples };

    // Emit some interesting relation info.
    elog::ereport(elevel)
        .errmsg(format!(
            "\"{}\": table contains {:.0} rows, {} rows in sample",
            relation.name(),
            *totalrows,
            numrows
        ))
        .finish(loc("postgresAcquireSampleRowsFunc"))?;

    Ok(numrows)
}

pub(crate) static ANALYZE_ROUTINE: FdwAnalyzeRoutine =
    FdwAnalyzeRoutine { analyze_foreign_table: postgres_analyze_foreign_table };

#[cfg(test)]
mod tests {
    use super::{c_strtod, c_strtoul};

    #[test]
    fn strtoul_and_strtod_take_the_longest_numeric_prefix() {
        assert_eq!(c_strtoul("12"), 12);
        assert_eq!(c_strtoul(" 7 pages"), 7);
        assert_eq!(c_strtoul("x"), 0);
        assert_eq!(c_strtod("-1"), -1.0);
        assert_eq!(c_strtod("1000"), 1000.0);
        assert_eq!(c_strtod("1e+06"), 1_000_000.0);
        assert_eq!(c_strtod("junk"), 0.0);
    }
}
