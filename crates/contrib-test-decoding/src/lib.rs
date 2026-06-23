//! Port of PostgreSQL 18.3 `contrib/test_decoding/test_decoding.c` — the SQL
//! test/example logical-decoding output plugin.
//!
//! ## Loading model (no C ABI)
//!
//! Real PG `dlopen`s `$libdir/test_decoding.so` and resolves
//! `_PG_output_plugin_init`, which fills an `OutputPluginCallbacks` vtable. The
//! Rust backend exposes no C ABI (see the no-C-extension-loading decision), so
//! the plugin's C body is ported here and registered as a BUILTIN output plugin
//! ([`backend_utils_fmgr_dfmgr_seams::register_builtin_output_plugin`]) keyed by
//! the name `"test_decoding"`. `LoadOutputPlugin` consults that registry first
//! (Phase 0), and the per-change dispatch reaches [`invoke`] WITH the live
//! `LogicalDecodingContext` (so the callbacks write into `ctx->out` and
//! read/stow `ctx->output_plugin_private`).
//!
//! ## Faithfulness
//!
//! The textual wire format (`BEGIN`/`COMMIT`/`table ns.rel: INSERT: col[type]:val
//! ...` and friends) is transcribed byte-for-byte from `test_decoding.c`. The
//! per-attribute rendering (`tuple_to_stringinfo` / `print_literal`) runs
//! `heap_getattr` over the decoded tuple image against the relation's tupdesc,
//! `getTypeOutputInfo` + `OidOutputFunctionCall` for the value text, and
//! `format_type_be`/`quote_identifier`/`quote_qualified_identifier` for the
//! type/identifier names — all behind the same seams the C calls.

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

extern crate alloc;

use alloc::string::{String, ToString};

use types_core::primitive::Oid;
use types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};
use types_logical::{
    CallbackInvocation, LogicalDecodingContext, OutputPluginCallbackArgs, OutputPluginOptions,
    OUTPUT_PLUGIN_BINARY_OUTPUT, OUTPUT_PLUGIN_TEXTUAL_OUTPUT,
};

use backend_replication_logical_logical::{OutputPluginPrepareWrite, OutputPluginWrite};

use backend_access_common_detoast_seams as detoast;
use backend_access_common_relation_seams as relation;
use backend_replication_logical_reorderbuffer_seams as rb;
use backend_utils_adt_format_type_seams as fmttype;
use backend_utils_adt_ruleutils_seams as ruleutils;
use backend_utils_adt_timestamp_seams as timestamp;
use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_utils_cache_relcache_seams as relcache;
use backend_utils_fmgr_fmgr_seams as fmgr;
use backend_utils_mmgr_mcxt_seams as mcxt;

mod ondisk;

/// pg_type OIDs `print_literal` switches on but `types-core` does not yet name.
const INT2OID: Oid = 21;
const INT4OID: Oid = 23;
const INT8OID: Oid = 20;
const OIDOID: Oid = 26;
const FLOAT4OID: Oid = 700;
const FLOAT8OID: Oid = 701;
const NUMERICOID: Oid = 1700;
const BOOLOID: Oid = 16;
const BITOID: Oid = 1560;
const VARBITOID: Oid = 1562;

/// `AccessShareLock` is taken by ProcessTXN; the plugin re-opens with `NoLock`
/// (the relcache fetch `RelationIdGetRelation`).
const NO_LOCK: types_storage::lock::LOCKMODE = types_storage::lock::NoLock;

/// `TestDecodingData` (test_decoding.c:30) — the plugin's `output_plugin_private`.
struct TestDecodingData {
    include_xids: bool,
    include_timestamp: bool,
    skip_empty_xacts: bool,
    #[allow(dead_code)]
    only_local: bool,
    /// `bool` whether the plugin enabled streaming (`ctx->streaming &=
    /// enable_streaming`). The basic recovery-TAP path never streams.
    #[allow(dead_code)]
    enable_streaming: bool,
    /// Per-transaction private state (`txn->output_plugin_private` in C), keyed
    /// by xid. C stows a `TestDecodingTxnData` on the txn; the mcx world has no
    /// such field, so the plugin keeps its own xid-keyed map (observably
    /// identical — it is plugin-private scratch).
    txn_data: alloc::collections::BTreeMap<u32, TestDecodingTxnData>,
}

/// `TestDecodingTxnData` (test_decoding.c:48).
#[derive(Default)]
struct TestDecodingTxnData {
    xact_wrote_changes: bool,
    #[allow(dead_code)]
    stream_wrote_changes: bool,
}

/// Recover the plugin's private `&mut TestDecodingData` from
/// `ctx->output_plugin_private`.
fn plugin_data(ctx: &mut LogicalDecodingContext) -> &mut TestDecodingData {
    ctx.output_plugin_private
        .as_mut()
        .and_then(|b| b.downcast_mut::<TestDecodingData>())
        .expect("test_decoding: output_plugin_private not set (startup_cb must run first)")
}

/// `_PG_output_plugin_init` (test_decoding.c:131) — the callback-presence
/// bitmask the builtin registry returns. test_decoding registers every callback
/// (LSB = startup_cb, in `OutputPluginCallbacks::from_bits` order).
fn init() -> u32 {
    // startup, begin, change, truncate, commit, message, filter_by_origin,
    // shutdown, filter_prepare, begin_prepare, prepare, commit_prepared,
    // rollback_prepared, stream_start, stream_stop, stream_abort,
    // stream_prepare, stream_commit, stream_change, stream_message,
    // stream_truncate — all 21 bits set.
    (1u32 << 21) - 1
}

/// Dispatch one callback to the matching `pg_decode_*` function. Mirrors the C
/// function-pointer table: the runtime hands us the live `ctx` plus the resolved
/// callback args.
fn invoke(ctx: &mut LogicalDecodingContext, inv: &CallbackInvocation) -> PgResult<bool> {
    match &inv.args {
        OutputPluginCallbackArgs::Startup { is_init } => {
            pg_decode_startup(ctx, *is_init)?;
            Ok(false)
        }
        OutputPluginCallbackArgs::Shutdown => {
            pg_decode_shutdown(ctx);
            Ok(false)
        }
        OutputPluginCallbackArgs::Begin { txn } => {
            pg_decode_begin_txn(ctx, *txn)?;
            Ok(false)
        }
        OutputPluginCallbackArgs::Commit { txn, commit_lsn } => {
            pg_decode_commit_txn(ctx, *txn, *commit_lsn)?;
            Ok(false)
        }
        OutputPluginCallbackArgs::Change { txn, relation, change } => {
            pg_decode_change(ctx, *txn, *relation, *change)?;
            Ok(false)
        }
        OutputPluginCallbackArgs::Truncate { txn, nrelations, relations, change } => {
            pg_decode_truncate(ctx, *txn, *nrelations, *relations, *change)?;
            Ok(false)
        }
        OutputPluginCallbackArgs::Message {
            txn,
            message_lsn,
            transactional,
            prefix,
            message_size,
            message,
        } => {
            pg_decode_message(
                ctx,
                *txn,
                *message_lsn,
                *transactional,
                *prefix,
                *message_size,
                *message,
            )?;
            Ok(false)
        }
        OutputPluginCallbackArgs::FilterByOrigin { origin_id } => {
            Ok(pg_decode_filter(ctx, *origin_id))
        }
        OutputPluginCallbackArgs::FilterPrepare { xid: _, gid } => {
            Ok(pg_decode_filter_prepare(gid))
        }
        OutputPluginCallbackArgs::BeginPrepare { txn } => {
            pg_decode_begin_prepare_txn(ctx, *txn)?;
            Ok(false)
        }
        OutputPluginCallbackArgs::Prepare { txn, prepare_lsn } => {
            pg_decode_prepare_txn(ctx, *txn, *prepare_lsn)?;
            Ok(false)
        }
        OutputPluginCallbackArgs::CommitPrepared { txn, commit_lsn } => {
            pg_decode_commit_prepared_txn(ctx, *txn, *commit_lsn)?;
            Ok(false)
        }
        OutputPluginCallbackArgs::RollbackPrepared { txn, prepare_end_lsn, prepare_time } => {
            pg_decode_rollback_prepared_txn(ctx, *txn, *prepare_end_lsn, *prepare_time)?;
            Ok(false)
        }
        // Streaming callbacks: test_decoding registers them, but the basic
        // recovery-TAP path never streams (ctx->streaming is gated off unless
        // stream-changes is requested). They are faithfully ported below.
        OutputPluginCallbackArgs::StreamStart { txn } => {
            pg_decode_stream_start(ctx, *txn)?;
            Ok(false)
        }
        OutputPluginCallbackArgs::StreamStop { txn } => {
            pg_decode_stream_stop(ctx, *txn)?;
            Ok(false)
        }
        OutputPluginCallbackArgs::StreamAbort { txn, abort_lsn } => {
            pg_decode_stream_abort(ctx, *txn, *abort_lsn)?;
            Ok(false)
        }
        OutputPluginCallbackArgs::StreamPrepare { txn, prepare_lsn } => {
            pg_decode_stream_prepare(ctx, *txn, *prepare_lsn)?;
            Ok(false)
        }
        OutputPluginCallbackArgs::StreamCommit { txn, commit_lsn } => {
            pg_decode_stream_commit(ctx, *txn, *commit_lsn)?;
            Ok(false)
        }
        OutputPluginCallbackArgs::StreamChange { txn, relation, change } => {
            pg_decode_stream_change(ctx, *txn, *relation, *change)?;
            Ok(false)
        }
        OutputPluginCallbackArgs::StreamMessage {
            txn,
            message_lsn,
            transactional,
            prefix,
            message_size,
            message,
        } => {
            pg_decode_stream_message(
                ctx,
                *txn,
                *message_lsn,
                *transactional,
                *prefix,
                *message_size,
                *message,
            )?;
            Ok(false)
        }
        OutputPluginCallbackArgs::StreamTruncate { txn, nrelations, relations, change } => {
            pg_decode_stream_truncate(ctx, *txn, *nrelations, *relations, *change)?;
            Ok(false)
        }
    }
}

// ---------------------------------------------------------------------------
// `ctx->out` append helpers (appendStringInfo* against the backing store).
// ---------------------------------------------------------------------------

fn append_str(ctx: &LogicalDecodingContext, s: &str) {
    mcxt::store_append_string_info(ctx.out, s.as_bytes());
}
fn append_bytes(ctx: &LogicalDecodingContext, b: &[u8]) {
    mcxt::store_append_string_info(ctx.out, b);
}
fn append_char(ctx: &LogicalDecodingContext, c: char) {
    let mut buf = [0u8; 4];
    mcxt::store_append_string_info(ctx.out, c.encode_utf8(&mut buf).as_bytes());
}

// ---------------------------------------------------------------------------
// Callbacks.
// ---------------------------------------------------------------------------

/// `pg_decode_startup` (test_decoding.c:159).
fn pg_decode_startup(ctx: &mut LogicalDecodingContext, _is_init: bool) -> PgResult<()> {
    let mut data = TestDecodingData {
        include_xids: true,
        include_timestamp: false,
        skip_empty_xacts: false,
        only_local: false,
        enable_streaming: false,
        txn_data: alloc::collections::BTreeMap::new(),
    };

    // opt->output_type = OUTPUT_PLUGIN_TEXTUAL_OUTPUT; opt->receive_rewrites = false;
    let mut opt = OutputPluginOptions {
        output_type: OUTPUT_PLUGIN_TEXTUAL_OUTPUT,
        receive_rewrites: false,
    };

    // foreach(option, ctx->output_plugin_options) — parse the (key,value) DefElems.
    for (defname, arg) in
        backend_replication_logical_logical::output_plugin_options(ctx).into_iter()
    {
        match defname.as_str() {
            "include-xids" => data.include_xids = parse_opt_bool(&defname, arg, true)?,
            "include-timestamp" => {
                data.include_timestamp = parse_opt_bool(&defname, arg, true)?
            }
            "force-binary" => {
                // if elem->arg == NULL continue; else parse; if force_binary set BINARY.
                if let Some(v) = arg {
                    if parse_bool_value(&defname, &v)? {
                        opt.output_type = OUTPUT_PLUGIN_BINARY_OUTPUT;
                    }
                }
            }
            "skip-empty-xacts" => data.skip_empty_xacts = parse_opt_bool(&defname, arg, true)?,
            "only-local" => data.only_local = parse_opt_bool(&defname, arg, true)?,
            "include-rewrites" => {
                if let Some(v) = arg {
                    opt.receive_rewrites = parse_bool_value(&defname, &v)?;
                }
            }
            "stream-changes" => {
                if let Some(v) = arg {
                    data.enable_streaming = parse_bool_value(&defname, &v)?;
                }
            }
            _ => {
                return Err(PgError::error(format!(
                    "option \"{}\" = \"{}\" is unknown",
                    defname,
                    arg.as_deref().unwrap_or("(null)")
                ))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
            }
        }
    }

    // ctx->streaming &= enable_streaming;
    ctx.streaming &= data.enable_streaming;
    // opt copied back onto ctx->options (the wrapper reads ctx->options).
    ctx.options = opt;

    // ctx->output_plugin_private = data;
    ctx.output_plugin_private = Some(alloc::boxed::Box::new(data));
    Ok(())
}

/// `pg_decode_shutdown` (test_decoding.c:280) — MemoryContextDelete(data->context).
/// The plugin-private data (and its xid-keyed txn map) is owned by the Box and
/// dropped here.
fn pg_decode_shutdown(ctx: &mut LogicalDecodingContext) {
    ctx.output_plugin_private = None;
}

/// `pg_decode_begin_txn` (test_decoding.c:290).
fn pg_decode_begin_txn(ctx: &mut LogicalDecodingContext, txn: types_logical::TxnHandle) -> PgResult<()> {
    let xid = rb::txn_xid::call(txn);
    let skip_empty = {
        let data = plugin_data(ctx);
        data.txn_data.insert(xid, TestDecodingTxnData::default());
        data.skip_empty_xacts
    };
    if skip_empty {
        return Ok(());
    }
    pg_output_begin(ctx, xid, true)
}

/// `pg_output_begin` (test_decoding.c:310).
fn pg_output_begin(ctx: &mut LogicalDecodingContext, xid: u32, last_write: bool) -> PgResult<()> {
    OutputPluginPrepareWrite(ctx, last_write)?;
    if plugin_data(ctx).include_xids {
        append_str(ctx, &format!("BEGIN {xid}"));
    } else {
        append_str(ctx, "BEGIN");
    }
    OutputPluginWrite(ctx, last_write)
}

/// `pg_decode_commit_txn` (test_decoding.c:322).
fn pg_decode_commit_txn(
    ctx: &mut LogicalDecodingContext,
    txn: types_logical::TxnHandle,
    _commit_lsn: types_core::primitive::XLogRecPtr,
) -> PgResult<()> {
    let xid = rb::txn_xid::call(txn);
    let (skip_empty, include_xids, include_timestamp, xact_wrote_changes) = {
        let data = plugin_data(ctx);
        let xact_wrote_changes = data
            .txn_data
            .remove(&xid)
            .map(|t| t.xact_wrote_changes)
            .unwrap_or(false);
        (
            data.skip_empty_xacts,
            data.include_xids,
            data.include_timestamp,
            xact_wrote_changes,
        )
    };

    if skip_empty && !xact_wrote_changes {
        return Ok(());
    }

    OutputPluginPrepareWrite(ctx, true)?;
    if include_xids {
        append_str(ctx, &format!("COMMIT {xid}"));
    } else {
        append_str(ctx, "COMMIT");
    }
    if include_timestamp {
        let t = rb::txn_xact_time::call(txn);
        append_str(ctx, &format!(" (at {})", timestamptz_str(t)?));
    }
    OutputPluginWrite(ctx, true)
}

/// `pg_decode_change` (test_decoding.c:603).
fn pg_decode_change(
    ctx: &mut LogicalDecodingContext,
    txn: types_logical::TxnHandle,
    relation: types_logical::RelationHandle,
    change: types_logical::ChangeHandle,
) -> PgResult<()> {
    let xid = rb::txn_xid::call(txn);

    // output BEGIN if we haven't yet (skip-empty-xacts).
    let skip_empty = plugin_data(ctx).skip_empty_xacts;
    let already = plugin_data(ctx)
        .txn_data
        .get(&xid)
        .map(|t| t.xact_wrote_changes)
        .unwrap_or(false);
    if skip_empty && !already {
        pg_output_begin(ctx, xid, false)?;
    }
    if let Some(t) = plugin_data(ctx).txn_data.get_mut(&xid) {
        t.xact_wrote_changes = true;
    }

    let reloid = rb::resolve_relation_handle::call(relation);
    let resolved = rb::resolve_change_handle::call(change);

    OutputPluginPrepareWrite(ctx, true)?;

    // "table " quote_qualified_identifier(get_namespace_name(get_rel_namespace(relid)),
    //   class_form->relrewrite ? get_rel_name(class_form->relrewrite)
    //                          : NameStr(class_form->relname));
    let scratch = mcx::MemoryContext::new("test_decoding change");
    let m = scratch.mcx();

    let rel = relation::relation_open::call(m, reloid, NO_LOCK)?;
    let relnamespace = relcache::rd_rel_relnamespace::call(&rel)?;
    let relrewrite = relcache::rd_rel_relrewrite::call(reloid)?;
    let nsname = lsyscache::get_namespace_name::call(m, relnamespace)?;
    let relname = if relrewrite != types_core::primitive::InvalidOid {
        lsyscache::get_rel_name::call(m, relrewrite)?
    } else {
        lsyscache::get_rel_name::call(m, reloid)?
    };
    let nsname_s = nsname.as_ref().map(|s| s.as_str());
    let relname_s = relname.as_ref().map(|s| s.as_str()).unwrap_or("");
    let qualified = ruleutils::quote_qualified_identifier::call(m, nsname_s, relname_s)?;

    append_str(ctx, "table ");
    append_str(ctx, qualified.as_str());
    append_char(ctx, ':');

    // The relation tupdesc for the tuple rendering.
    let tupdesc = relcache::relation_get_descr::call(m, &rel)?;

    use rb::DecodedChangeKind as K;
    match resolved.kind {
        K::Insert => {
            append_str(ctx, " INSERT:");
            match &resolved.newtuple {
                None => append_str(ctx, " (no-tuple-data)"),
                Some(nt) => tuple_to_stringinfo(ctx, m, &tupdesc, nt, false)?,
            }
        }
        K::Update => {
            append_str(ctx, " UPDATE:");
            if let Some(ot) = &resolved.oldtuple {
                append_str(ctx, " old-key:");
                tuple_to_stringinfo(ctx, m, &tupdesc, ot, true)?;
                append_str(ctx, " new-tuple:");
            }
            match &resolved.newtuple {
                None => append_str(ctx, " (no-tuple-data)"),
                Some(nt) => tuple_to_stringinfo(ctx, m, &tupdesc, nt, false)?,
            }
        }
        K::Delete => {
            append_str(ctx, " DELETE:");
            match &resolved.oldtuple {
                None => append_str(ctx, " (no-tuple-data)"),
                Some(ot) => tuple_to_stringinfo(ctx, m, &tupdesc, ot, true)?,
            }
        }
        _ => {
            // REORDER_BUFFER_CHANGE_INTERNAL_* never reach the change callback.
            debug_assert!(false, "pg_decode_change on non-DML action");
        }
    }

    OutputPluginWrite(ctx, true)
}

/// `pg_decode_truncate` (test_decoding.c:690).
fn pg_decode_truncate(
    ctx: &mut LogicalDecodingContext,
    txn: types_logical::TxnHandle,
    nrelations: i32,
    relations: types_logical::RelationsHandle,
    change: types_logical::ChangeHandle,
) -> PgResult<()> {
    let xid = rb::txn_xid::call(txn);

    let skip_empty = plugin_data(ctx).skip_empty_xacts;
    let already = plugin_data(ctx)
        .txn_data
        .get(&xid)
        .map(|t| t.xact_wrote_changes)
        .unwrap_or(false);
    if skip_empty && !already {
        pg_output_begin(ctx, xid, false)?;
    }
    if let Some(t) = plugin_data(ctx).txn_data.get_mut(&xid) {
        t.xact_wrote_changes = true;
    }

    let resolved = rb::resolve_change_handle::call(change);

    OutputPluginPrepareWrite(ctx, true)?;
    append_str(ctx, "table ");

    let scratch = mcx::MemoryContext::new("test_decoding truncate");
    let m = scratch.mcx();
    for i in 0..nrelations {
        if i > 0 {
            append_str(ctx, ", ");
        }
        let reloid = rb::resolve_relations_handle::call(relations, i);
        let rel = relation::relation_open::call(m, reloid, NO_LOCK)?;
        let relnamespace = relcache::rd_rel_relnamespace::call(&rel)?;
        let nsname = lsyscache::get_namespace_name::call(m, relnamespace)?;
        let relname = lsyscache::get_rel_name::call(m, reloid)?;
        let qualified = ruleutils::quote_qualified_identifier::call(
            m,
            nsname.as_ref().map(|s| s.as_str()),
            relname.as_ref().map(|s| s.as_str()).unwrap_or(""),
        )?;
        append_str(ctx, qualified.as_str());
    }

    append_str(ctx, ": TRUNCATE:");
    // change->data.truncate.{restart_seqs,cascade}.
    let restart_seqs = resolved.truncate_restart_seqs;
    let cascade = resolved.truncate_cascade;
    if restart_seqs || cascade {
        if restart_seqs {
            append_str(ctx, " restart_seqs");
        }
        if cascade {
            append_str(ctx, " cascade");
        }
    } else {
        append_str(ctx, " (no-flags)");
    }

    OutputPluginWrite(ctx, true)
}

/// `pg_decode_message` (test_decoding.c:745).
#[allow(clippy::too_many_arguments)]
fn pg_decode_message(
    ctx: &mut LogicalDecodingContext,
    txn: types_logical::TxnHandle,
    _lsn: types_core::primitive::XLogRecPtr,
    transactional: bool,
    prefix: types_logical::PrefixHandle,
    sz: types_core::primitive::Size,
    message: types_logical::MessageHandle,
) -> PgResult<()> {
    if transactional {
        let xid = rb::txn_xid::call(txn);
        let skip_empty = plugin_data(ctx).skip_empty_xacts;
        let already = plugin_data(ctx)
            .txn_data
            .get(&xid)
            .map(|t| t.xact_wrote_changes)
            .unwrap_or(false);
        if skip_empty && !already {
            pg_output_begin(ctx, xid, false)?;
        }
        if let Some(t) = plugin_data(ctx).txn_data.get_mut(&xid) {
            t.xact_wrote_changes = true;
        }
    }

    let prefix_bytes = rb::resolve_prefix_handle::call(prefix);
    let message_bytes = rb::resolve_message_handle::call(message);

    OutputPluginPrepareWrite(ctx, true)?;
    append_str(
        ctx,
        &format!(
            "message: transactional: {} prefix: {}, sz: {} content:",
            transactional as i32,
            String::from_utf8_lossy(&prefix_bytes),
            sz
        ),
    );
    append_bytes(ctx, &message_bytes);
    OutputPluginWrite(ctx, true)
}

/// `pg_decode_filter` (test_decoding.c:463).
fn pg_decode_filter(ctx: &mut LogicalDecodingContext, origin_id: types_core::primitive::RepOriginId) -> bool {
    let only_local = plugin_data(ctx).only_local;
    only_local && origin_id != 0
}

/// `pg_decode_filter_prepare` (test_decoding.c:453).
fn pg_decode_filter_prepare(gid: &[u8]) -> bool {
    // strstr(gid, "_nodecode") != NULL
    gid.windows(b"_nodecode".len())
        .any(|w| w == b"_nodecode")
}

/// `pg_decode_begin_prepare_txn` (test_decoding.c:350).
fn pg_decode_begin_prepare_txn(
    ctx: &mut LogicalDecodingContext,
    txn: types_logical::TxnHandle,
) -> PgResult<()> {
    let xid = rb::txn_xid::call(txn);
    let skip_empty = {
        let data = plugin_data(ctx);
        data.txn_data.insert(xid, TestDecodingTxnData::default());
        data.skip_empty_xacts
    };
    if skip_empty {
        return Ok(());
    }
    pg_output_begin(ctx, xid, true)
}

/// `pg_decode_prepare_txn` (test_decoding.c:371).
fn pg_decode_prepare_txn(
    ctx: &mut LogicalDecodingContext,
    txn: types_logical::TxnHandle,
    _prepare_lsn: types_core::primitive::XLogRecPtr,
) -> PgResult<()> {
    let xid = rb::txn_xid::call(txn);
    let (skip_empty, include_xids, include_timestamp, xact_wrote_changes) = {
        let data = plugin_data(ctx);
        let w = data.txn_data.get(&xid).map(|t| t.xact_wrote_changes).unwrap_or(false);
        (data.skip_empty_xacts, data.include_xids, data.include_timestamp, w)
    };
    if skip_empty && !xact_wrote_changes {
        return Ok(());
    }
    OutputPluginPrepareWrite(ctx, true)?;
    let gid = rb::txn_gid::call(txn);
    append_str(ctx, &format!("PREPARE TRANSACTION {}", quote_literal_cstr(&gid)));
    if include_xids {
        append_str(ctx, &format!(", txid {xid}"));
    }
    if include_timestamp {
        let t = rb::txn_xact_time::call(txn);
        append_str(ctx, &format!(" (at {})", timestamptz_str(t)?));
    }
    OutputPluginWrite(ctx, true)
}

/// `pg_decode_commit_prepared_txn` (test_decoding.c:401).
fn pg_decode_commit_prepared_txn(
    ctx: &mut LogicalDecodingContext,
    txn: types_logical::TxnHandle,
    _commit_lsn: types_core::primitive::XLogRecPtr,
) -> PgResult<()> {
    let xid = rb::txn_xid::call(txn);
    let (include_xids, include_timestamp) = {
        let data = plugin_data(ctx);
        (data.include_xids, data.include_timestamp)
    };
    OutputPluginPrepareWrite(ctx, true)?;
    let gid = rb::txn_gid::call(txn);
    append_str(ctx, &format!("COMMIT PREPARED {}", quote_literal_cstr(&gid)));
    if include_xids {
        append_str(ctx, &format!(", txid {xid}"));
    }
    if include_timestamp {
        let t = rb::txn_xact_time::call(txn);
        append_str(ctx, &format!(" (at {})", timestamptz_str(t)?));
    }
    OutputPluginWrite(ctx, true)
}

/// `pg_decode_rollback_prepared_txn` (test_decoding.c:423).
fn pg_decode_rollback_prepared_txn(
    ctx: &mut LogicalDecodingContext,
    txn: types_logical::TxnHandle,
    _prepare_end_lsn: types_core::primitive::XLogRecPtr,
    _prepare_time: types_core::primitive::TimestampTz,
) -> PgResult<()> {
    let xid = rb::txn_xid::call(txn);
    let (include_xids, include_timestamp) = {
        let data = plugin_data(ctx);
        (data.include_xids, data.include_timestamp)
    };
    OutputPluginPrepareWrite(ctx, true)?;
    let gid = rb::txn_gid::call(txn);
    append_str(ctx, &format!("ROLLBACK PREPARED {}", quote_literal_cstr(&gid)));
    if include_xids {
        append_str(ctx, &format!(", txid {xid}"));
    }
    if include_timestamp {
        let t = rb::txn_xact_time::call(txn);
        append_str(ctx, &format!(" (at {})", timestamptz_str(t)?));
    }
    OutputPluginWrite(ctx, true)
}

// --- Streaming callbacks (test_decoding.c:769+). The basic TAP path never
// streams; ported for faithfulness. ---

fn pg_decode_stream_start(ctx: &mut LogicalDecodingContext, txn: types_logical::TxnHandle) -> PgResult<()> {
    let xid = rb::txn_xid::call(txn);
    let skip_empty = {
        let data = plugin_data(ctx);
        data.txn_data.entry(xid).or_default();
        data.txn_data.get_mut(&xid).unwrap().stream_wrote_changes = false;
        data.skip_empty_xacts
    };
    if skip_empty {
        return Ok(());
    }
    pg_output_stream_start(ctx, xid, true)
}

fn pg_output_stream_start(ctx: &mut LogicalDecodingContext, xid: u32, last_write: bool) -> PgResult<()> {
    OutputPluginPrepareWrite(ctx, last_write)?;
    if plugin_data(ctx).include_xids {
        append_str(ctx, &format!("opening a streamed block for transaction TXN {xid}"));
    } else {
        append_str(ctx, "opening a streamed block for transaction");
    }
    OutputPluginWrite(ctx, last_write)
}

fn pg_decode_stream_stop(ctx: &mut LogicalDecodingContext, txn: types_logical::TxnHandle) -> PgResult<()> {
    let xid = rb::txn_xid::call(txn);
    if plugin_data(ctx).skip_empty_xacts {
        return Ok(());
    }
    OutputPluginPrepareWrite(ctx, true)?;
    if plugin_data(ctx).include_xids {
        append_str(ctx, &format!("closing a streamed block for transaction TXN {xid}"));
    } else {
        append_str(ctx, "closing a streamed block for transaction");
    }
    OutputPluginWrite(ctx, true)
}

fn pg_decode_stream_abort(
    ctx: &mut LogicalDecodingContext,
    txn: types_logical::TxnHandle,
    _abort_lsn: types_core::primitive::XLogRecPtr,
) -> PgResult<()> {
    let xid = rb::txn_xid::call(txn);
    OutputPluginPrepareWrite(ctx, true)?;
    if plugin_data(ctx).include_xids {
        append_str(ctx, &format!("aborting streamed (sub)transaction TXN {xid}"));
    } else {
        append_str(ctx, "aborting streamed (sub)transaction");
    }
    OutputPluginWrite(ctx, true)
}

fn pg_decode_stream_prepare(
    ctx: &mut LogicalDecodingContext,
    txn: types_logical::TxnHandle,
    _prepare_lsn: types_core::primitive::XLogRecPtr,
) -> PgResult<()> {
    let xid = rb::txn_xid::call(txn);
    OutputPluginPrepareWrite(ctx, true)?;
    let gid = rb::txn_gid::call(txn);
    if plugin_data(ctx).include_xids {
        append_str(ctx, &format!("preparing streamed transaction TXN {} txid {}", quote_literal_cstr(&gid), xid));
    } else {
        append_str(ctx, &format!("preparing streamed transaction {}", quote_literal_cstr(&gid)));
    }
    OutputPluginWrite(ctx, true)
}

fn pg_decode_stream_commit(
    ctx: &mut LogicalDecodingContext,
    txn: types_logical::TxnHandle,
    _commit_lsn: types_core::primitive::XLogRecPtr,
) -> PgResult<()> {
    let xid = rb::txn_xid::call(txn);
    OutputPluginPrepareWrite(ctx, true)?;
    if plugin_data(ctx).include_xids {
        append_str(ctx, &format!("committing streamed transaction TXN {xid}"));
    } else {
        append_str(ctx, "committing streamed transaction");
    }
    OutputPluginWrite(ctx, true)
}

fn pg_decode_stream_change(
    ctx: &mut LogicalDecodingContext,
    txn: types_logical::TxnHandle,
    relation: types_logical::RelationHandle,
    change: types_logical::ChangeHandle,
) -> PgResult<()> {
    // The plugin just delegates to the same per-change rendering after marking
    // stream_wrote_changes (skip-empty-xacts off in the TAP path).
    let xid = rb::txn_xid::call(txn);
    if let Some(t) = plugin_data(ctx).txn_data.get_mut(&xid) {
        t.stream_wrote_changes = true;
    }
    pg_decode_change(ctx, txn, relation, change)
}

#[allow(clippy::too_many_arguments)]
fn pg_decode_stream_message(
    ctx: &mut LogicalDecodingContext,
    txn: types_logical::TxnHandle,
    lsn: types_core::primitive::XLogRecPtr,
    transactional: bool,
    prefix: types_logical::PrefixHandle,
    sz: types_core::primitive::Size,
    message: types_logical::MessageHandle,
) -> PgResult<()> {
    pg_decode_message(ctx, txn, lsn, transactional, prefix, sz, message)
}

fn pg_decode_stream_truncate(
    ctx: &mut LogicalDecodingContext,
    txn: types_logical::TxnHandle,
    nrelations: i32,
    relations: types_logical::RelationsHandle,
    change: types_logical::ChangeHandle,
) -> PgResult<()> {
    pg_decode_truncate(ctx, txn, nrelations, relations, change)
}

// ---------------------------------------------------------------------------
// tuple_to_stringinfo / print_literal (test_decoding.c:481 / 527).
// ---------------------------------------------------------------------------

/// `tuple_to_stringinfo` (test_decoding.c:527) — render each non-dropped,
/// non-system column as ` name[type]:value`.
fn tuple_to_stringinfo<'mcx>(
    ctx: &mut LogicalDecodingContext,
    mcx: mcx::Mcx<'mcx>,
    tupdesc: &types_tuple::heaptuple::TupleDescData<'mcx>,
    tuple: &rb::DecodedTuple,
    skip_nulls: bool,
) -> PgResult<()> {
    // Reconstruct a deformable on-disk heap tuple from the decoded image.
    let formed = ondisk::formed_tuple_from_decoded(mcx, tuple)?;

    let natts = tupdesc.natts;
    for natt in 0..natts {
        let attr = tupdesc.attr(natt as usize);

        // don't print dropped columns.
        if attr.attisdropped {
            continue;
        }
        // don't print system columns.
        if attr.attnum < 0 {
            continue;
        }

        let typid = attr.atttypid;

        // origval = heap_getattr(tuple, natt + 1, tupdesc, &isnull);
        let (origval, isnull) =
            backend_access_common_heaptuple::heap_getattr(mcx, &formed, natt + 1, tupdesc)?;

        if isnull && skip_nulls {
            continue;
        }

        // " " quote_identifier(NameStr(attr->attname))
        append_char(ctx, ' ');
        let attname = String::from_utf8_lossy(attr.attname.name_str()).into_owned();
        let q = ruleutils::quote_identifier::call(mcx, &attname)?;
        append_str(ctx, q.as_str());

        // "[" format_type_be(typid) "]"
        append_char(ctx, '[');
        let tname = fmttype::format_type_be::call(mcx, typid)?;
        append_str(ctx, tname.as_str());
        append_char(ctx, ']');

        // getTypeOutputInfo(typid, &typoutput, &typisvarlena);
        let (typoutput, typisvarlena) = lsyscache::get_type_output_info::call(typid)?;

        append_char(ctx, ':');

        if isnull {
            append_str(ctx, "null");
        } else if typisvarlena && datum_is_external_ondisk(&origval) {
            append_str(ctx, "unchanged-toast-datum");
        } else if !typisvarlena {
            let out = fmgr::oid_output_function_call::call(mcx, typoutput, &origval)?;
            print_literal(ctx, typid, &out);
        } else {
            // val = PG_DETOAST_DATUM(origval);
            let detoasted = detoast_datum(mcx, &origval)?;
            let out = fmgr::oid_output_function_call::call(mcx, typoutput, &detoasted)?;
            print_literal(ctx, typid, &out);
        }
    }
    Ok(())
}

/// `print_literal` (test_decoding.c:481).
fn print_literal(ctx: &mut LogicalDecodingContext, typid: Oid, outputstr: &[u8]) {
    match typid {
        INT2OID | INT4OID | INT8OID | OIDOID | FLOAT4OID | FLOAT8OID | NUMERICOID => {
            append_bytes(ctx, outputstr);
        }
        BITOID | VARBITOID => {
            append_str(ctx, "B'");
            append_bytes(ctx, outputstr);
            append_char(ctx, '\'');
        }
        BOOLOID => {
            if outputstr == b"t" {
                append_str(ctx, "true");
            } else {
                append_str(ctx, "false");
            }
        }
        _ => {
            append_char(ctx, '\'');
            // SQL_STR_DOUBLE(ch, false): double a `'`.
            for &ch in outputstr {
                if ch == b'\'' {
                    append_bytes(ctx, &[ch]);
                }
                append_bytes(ctx, &[ch]);
            }
            append_char(ctx, '\'');
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers.
// ---------------------------------------------------------------------------

/// Whether a by-reference varlena `Datum` is an on-disk-external (toasted)
/// pointer, mirroring `VARATT_IS_EXTERNAL_ONDISK(origval)`.
fn datum_is_external_ondisk(d: &backend_access_common_heaptuple::Datum<'_>) -> bool {
    match d {
        backend_access_common_heaptuple::Datum::ByRef(bytes) => {
            ondisk::varatt_is_external_ondisk(bytes)
        }
        _ => false,
    }
}

/// `PG_DETOAST_DATUM(origval)` — produce a fully in-line varlena image.
fn detoast_datum<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    d: &backend_access_common_heaptuple::Datum<'mcx>,
) -> PgResult<backend_access_common_heaptuple::Datum<'mcx>> {
    match d {
        backend_access_common_heaptuple::Datum::ByRef(bytes) => {
            let flat = detoast::detoast_attr::call(mcx, bytes)?;
            Ok(backend_access_common_heaptuple::Datum::ByRef(flat))
        }
        // Non-varlena values never reach the detoast arm.
        other => Ok(other.clone()),
    }
}

/// `timestamptz_to_str(t)` — ISO-style timestamp string.
fn timestamptz_str(t: types_core::primitive::TimestampTz) -> PgResult<String> {
    let scratch = mcx::MemoryContext::new("test_decoding ts");
    let s = timestamp::timestamptz_to_str::call(scratch.mcx(), t)?;
    Ok(s.as_str().to_string())
}

/// `quote_literal_cstr(s)` — SQL-quote a literal (the 2PC GID). Implemented
/// inline (matches `quote_literal` for a string with no embedded backslash
/// special-casing beyond escaping `'`): wrap in single quotes, doubling any
/// embedded `'`. (test_decoding GIDs are plain ASCII identifiers.)
fn quote_literal_cstr(s: &[u8]) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for &b in s {
        let c = b as char;
        if c == '\'' {
            out.push('\'');
        }
        out.push(c);
    }
    out.push('\'');
    out
}

fn parse_opt_bool(defname: &str, arg: Option<String>, default_when_null: bool) -> PgResult<bool> {
    match arg {
        None => Ok(default_when_null),
        Some(v) => parse_bool_value(defname, &v),
    }
}

/// `parse_bool(value, &out)` — the value boundary of test_decoding option
/// parsing. On parse failure raises the C
/// `could not parse value "%s" for parameter "%s"` error.
fn parse_bool_value(defname: &str, value: &str) -> PgResult<bool> {
    match value.to_ascii_lowercase().as_str() {
        "true" | "yes" | "on" | "1" | "t" | "y" => Ok(true),
        "false" | "no" | "off" | "0" | "f" | "n" => Ok(false),
        _ => Err(PgError::error(format!(
            "could not parse value \"{value}\" for parameter \"{defname}\""
        ))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)),
    }
}

/// Install this crate's seams: register the `test_decoding` builtin output
/// plugin into the Phase-0 registry.
pub fn init_seams() {
    backend_utils_fmgr_dfmgr_seams::register_builtin_output_plugin(
        backend_utils_fmgr_dfmgr_seams::BuiltinOutputPlugin {
            name: "test_decoding",
            init,
            invoke,
        },
    );
}
