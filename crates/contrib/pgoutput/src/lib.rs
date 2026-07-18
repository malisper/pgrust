// pgoutput.c: the logical replication output plugin (the publisher half of
// CREATE PUBLICATION/SUBSCRIPTION), registered as a builtin library like
// test_decoding.
//
// Ported for the non-streaming, non-two-phase protocol (the callbacks C
// registers for streaming and prepared transactions are deliberately not
// registered — the decode stack refuses those paths loudly; a subscriber
// requesting streaming gets C's own "not supported by output plugin" error).
// Row-filter publications and partition attribute-remapping refuse loudly;
// column lists, FOR ALL TABLES, schema publications and identical-descriptor
// partition routing are ported.
//
// C's TupleTableSlot staging is skipped: changes are written directly from
// the reorderbuffer's HeapTupleData (see logicalproto::logicalrep_write_tuple).
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;

use datum::Datum;
use elog::{elog, ereport};
use logical::{
    OutputPluginCallbacks, OutputPluginContext, OutputPluginOutputType, OutputPluginPrepareWrite,
    OutputPluginUpdateProgress, OutputPluginWrite,
};
use logicalproto::{
    logicalrep_should_publish_column, logicalrep_write_begin, logicalrep_write_commit,
    logicalrep_write_delete, logicalrep_write_insert, logicalrep_write_message,
    logicalrep_write_rel, logicalrep_write_truncate, logicalrep_write_typ,
    logicalrep_write_update, LOGICALREP_PROTO_MAX_VERSION_NUM, LOGICALREP_PROTO_MIN_VERSION_NUM,
    LOGICALREP_PROTO_STREAM_PARALLEL_VERSION_NUM, LOGICALREP_PROTO_STREAM_VERSION_NUM,
    LOGICALREP_PROTO_TWOPHASE_VERSION_NUM, PUBLISH_GENCOLS_NONE, PUBLISH_GENCOLS_STORED,
};
use mcx::MemoryContext;
use reorderbuffer::{ReorderBuffer, ReorderBufferChange, ReorderBufferChangeData, TxnId};
use types_core::catalog::FirstGenbkiObjectId;
use types_core::{
    InvalidOid, InvalidRepOriginId, InvalidTransactionId, Oid, RepOriginId, XLogRecPtr,
};
use types_error::{
    ErrorLocation, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_NAME,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_SYNTAX_ERROR, ERROR, WARNING,
};
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};
use types_nodes::bitmapset::Bitmapset;
use types_rel::pg_class::RELKIND_PARTITIONED_TABLE;
use types_rel::RelationData;

use cache_syscache::cacheinfo::{NAMESPACEOID, PUBLICATIONOID, PUBLICATIONRELMAP};
use cache_syscache::{ReleaseSysCache, SearchSysCache2, SysCacheGetAttr, SysCacheKey};
use pg_publication::{
    check_and_fetch_column_list, GetPublicationByName, GetRelationPublications,
    GetSchemaPublications, GetTopMostAncestorInPublication, PublicationActions,
};

const LIBRARY: &str = "pgoutput";

// pg_publication_rel.prqual attribute number (row filter).
const Anum_pg_publication_rel_prqual: i32 = 4;

fn loc(func: &'static str) -> ErrorLocation {
    ErrorLocation::new("pgoutput.c", 0, func)
}

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("unported callee reached from pgoutput.c: {what}")
}

// LOGICALREP_STREAM_* (pg_subscription.h).
const LOGICALREP_STREAM_OFF: u8 = b'f';
const LOGICALREP_STREAM_ON: u8 = b't';
const LOGICALREP_STREAM_PARALLEL: u8 = b'p';

// PGOutputData (pgoutput.h).
struct PGOutputData {
    protocol_version: u32,
    publication_names: Vec<String>,
    binary: bool,
    messages: bool,
    streaming: u8,
    two_phase: bool,
    publish_no_origin: bool,
    // Loaded Publication copies (C keeps List *publications in pubctx).
    publications: Vec<OwnedPublication>,
}

// An owned snapshot of pg_publication::Publication (whose name string borrows
// an arena; cache entries outlive any single arena here).
#[derive(Clone)]
struct OwnedPublication {
    oid: Oid,
    alltables: bool,
    pubviaroot: bool,
    pubgencols_type: u8,
    pubactions: PublicationActions,
}

struct PGOutputTxnData {
    sent_begin_txn: bool,
}

// RelationSyncEntry (pgoutput.c:126), minus the row-filter executor state
// (row filters refuse loudly) and the tuple slots/attrmap (tuples are written
// straight from HeapTupleData; descriptor-mismatched partition routing
// refuses loudly in pgoutput_change).
struct RelationSyncEntry {
    replicate_valid: bool,
    schema_sent: bool,
    include_gencols_type: u8,
    pubactions: PublicationActions,
    publish_as_relid: Oid,
    // Publication column list as raw attnums, ascending (C: Bitmapset).
    columns: Option<Vec<i16>>,
}

impl RelationSyncEntry {
    fn new() -> Self {
        RelationSyncEntry {
            replicate_valid: false,
            schema_sent: false,
            include_gencols_type: PUBLISH_GENCOLS_NONE,
            pubactions: PublicationActions {
                pubinsert: false,
                pubupdate: false,
                pubdelete: false,
                pubtruncate: false,
            },
            publish_as_relid: InvalidOid,
            columns: None,
        }
    }
}

thread_local! {
    // C static: HTAB *RelationSyncCache. None until init_rel_sync_cache.
    static REL_SYNC_CACHE: RefCell<Option<HashMap<Oid, Rc<RefCell<RelationSyncEntry>>>>> =
        const { RefCell::new(None) };
    // C static: publications_valid.
    static PUBLICATIONS_VALID: Cell<bool> = const { Cell::new(false) };
    static PUBLICATION_CALLBACK_REGISTERED: Cell<bool> = const { Cell::new(false) };
    static RELATION_CALLBACKS_REGISTERED: Cell<bool> = const { Cell::new(false) };
}

fn data_from(opc: &OutputPluginContext) -> &'static mut PGOutputData {
    debug_assert!(opc.output_plugin_private != 0);
    // SAFETY: set in pgoutput_startup; freed only in pgoutput_shutdown.
    unsafe { &mut *(opc.output_plugin_private as *mut PGOutputData) }
}

fn txndata_from(rb: &ReorderBuffer, txn: TxnId) -> Option<&'static mut PGOutputTxnData> {
    let p = rb.txn(txn).output_plugin_private;
    if p == 0 {
        return None;
    }
    // SAFETY: set in pgoutput_begin_txn; freed only in pgoutput_commit_txn.
    Some(unsafe { &mut *(p as *mut PGOutputTxnData) })
}

fn fc__pg_output_plugin_init(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: arg 0 carries the callbacks pointer from LoadOutputPlugin.
    let cb = unsafe { &mut *(fcinfo.arg(0).as_usize() as *mut OutputPluginCallbacks) };
    cb.startup_cb = Some(pgoutput_startup);
    cb.begin_cb = Some(pgoutput_begin_txn);
    cb.change_cb = Some(pgoutput_change);
    cb.truncate_cb = Some(pgoutput_truncate);
    cb.message_cb = Some(pgoutput_message);
    cb.commit_cb = Some(pgoutput_commit_txn);
    cb.filter_by_origin_cb = Some(pgoutput_origin_filter);
    cb.shutdown_cb = Some(pgoutput_shutdown);
    // C also registers the prepare family and stream_* callbacks; the decode
    // stack refuses streaming/two-phase loudly, and a subscriber that asks
    // for them is rejected in pgoutput_startup with C's own errors.
    Ok(Datum::from_usize(0))
}

fn conflicting_option() -> PgResult<()> {
    ereport(ERROR)
        .errcode(ERRCODE_SYNTAX_ERROR)
        .errmsg("conflicting or redundant options")
        .finish(loc("parse_output_parameters"))
}

fn parse_bool_value(name: &str, value: Option<&str>) -> PgResult<bool> {
    match value {
        None => Ok(true),
        Some(v) => match adt_bool::parse_bool(v) {
            Some(b) => Ok(b),
            None => {
                ereport(ERROR)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg(format!(
                        "could not parse value \"{v}\" for parameter \"{name}\""
                    ))
                    .finish(loc("parse_output_parameters"))?;
                unreachable!()
            }
        },
    }
}

// defGetStreamingMode (subscriptioncmds.c).
fn parse_streaming_mode(value: Option<&str>) -> PgResult<u8> {
    let Some(v) = value else {
        return Ok(LOGICALREP_STREAM_ON);
    };
    if v.eq_ignore_ascii_case("parallel") {
        return Ok(LOGICALREP_STREAM_PARALLEL);
    }
    match adt_bool::parse_bool(v) {
        Some(true) => Ok(LOGICALREP_STREAM_ON),
        Some(false) => Ok(LOGICALREP_STREAM_OFF),
        None => {
            ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!("{v} requires a Boolean value or \"parallel\""))
                .finish(loc("defGetStreamingMode"))?;
            unreachable!()
        }
    }
}

// parse_output_parameters (pgoutput.c:289).
fn parse_output_parameters(
    options: &[(String, Option<String>)],
    data: &mut PGOutputData,
) -> PgResult<()> {
    let mut protocol_version_given = false;
    let mut publication_names_given = false;
    let mut binary_option_given = false;
    let mut messages_option_given = false;
    let mut streaming_given = false;
    let mut two_phase_option_given = false;
    let mut origin_option_given = false;

    data.binary = false;
    data.streaming = LOGICALREP_STREAM_OFF;
    data.messages = false;
    data.two_phase = false;

    for (name, value) in options {
        let value_str = value.as_deref();
        match name.as_str() {
            "proto_version" => {
                if protocol_version_given {
                    conflicting_option()?;
                }
                protocol_version_given = true;
                let raw = value_str.unwrap_or("");
                let parsed: u64 = match raw.parse() {
                    Ok(v) => v,
                    Err(_) => {
                        ereport(ERROR)
                            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                            .errmsg("invalid proto_version")
                            .finish(loc("parse_output_parameters"))?;
                        unreachable!()
                    }
                };
                if parsed > u32::MAX as u64 {
                    ereport(ERROR)
                        .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                        .errmsg(format!("proto_version \"{raw}\" out of range"))
                        .finish(loc("parse_output_parameters"))?;
                }
                data.protocol_version = parsed as u32;
            }
            "publication_names" => {
                if publication_names_given {
                    conflicting_option()?;
                }
                publication_names_given = true;
                let raw = value_str.unwrap_or("");
                let ctx = MemoryContext::new("publication_names");
                match varlena::split_identifier_string(
                    ctx.mcx(),
                    raw,
                    b',',
                    mbutils::GetDatabaseEncoding(),
                )? {
                    Some(names) => data.publication_names = names,
                    None => {
                        ereport(ERROR)
                            .errcode(ERRCODE_INVALID_NAME)
                            .errmsg("invalid publication_names syntax")
                            .finish(loc("parse_output_parameters"))?;
                    }
                }
            }
            "binary" => {
                if binary_option_given {
                    conflicting_option()?;
                }
                binary_option_given = true;
                data.binary = parse_bool_value(name, value_str)?;
            }
            "messages" => {
                if messages_option_given {
                    conflicting_option()?;
                }
                messages_option_given = true;
                data.messages = parse_bool_value(name, value_str)?;
            }
            "streaming" => {
                if streaming_given {
                    conflicting_option()?;
                }
                streaming_given = true;
                data.streaming = parse_streaming_mode(value_str)?;
            }
            "two_phase" => {
                if two_phase_option_given {
                    conflicting_option()?;
                }
                two_phase_option_given = true;
                data.two_phase = parse_bool_value(name, value_str)?;
            }
            "origin" => {
                if origin_option_given {
                    conflicting_option()?;
                }
                origin_option_given = true;
                let origin = value_str.unwrap_or("");
                if origin.eq_ignore_ascii_case("none") {
                    data.publish_no_origin = true;
                } else if origin.eq_ignore_ascii_case("any") {
                    data.publish_no_origin = false;
                } else {
                    ereport(ERROR)
                        .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                        .errmsg(format!("unrecognized origin value: \"{origin}\""))
                        .finish(loc("parse_output_parameters"))?;
                }
            }
            other => {
                elog(ERROR, format!("unrecognized pgoutput option: {other}"))?;
            }
        }
    }

    if !protocol_version_given {
        ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("option \"proto_version\" missing")
            .finish(loc("parse_output_parameters"))?;
    }
    if !publication_names_given {
        ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("option \"publication_names\" missing")
            .finish(loc("parse_output_parameters"))?;
    }
    Ok(())
}

// pgoutput_startup (pgoutput.c:448).
fn pgoutput_startup(opc: &mut OutputPluginContext, is_init: bool) -> PgResult<()> {
    let mut data = Box::new(PGOutputData {
        protocol_version: 0,
        publication_names: Vec::new(),
        binary: false,
        messages: false,
        streaming: LOGICALREP_STREAM_OFF,
        two_phase: false,
        publish_no_origin: false,
        publications: Vec::new(),
    });

    // This plugin uses binary protocol.
    opc.options.output_type = OutputPluginOutputType::Binary;

    if !is_init {
        let options = std::mem::take(&mut opc.output_plugin_options);
        parse_output_parameters(&options, &mut data)?;
        opc.output_plugin_options = options;

        if data.protocol_version > LOGICALREP_PROTO_MAX_VERSION_NUM {
            ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(format!(
                    "client sent proto_version={} but server only supports protocol {} or lower",
                    data.protocol_version, LOGICALREP_PROTO_MAX_VERSION_NUM
                ))
                .finish(loc("pgoutput_startup"))?;
        }
        if data.protocol_version < LOGICALREP_PROTO_MIN_VERSION_NUM {
            ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(format!(
                    "client sent proto_version={} but server only supports protocol {} or higher",
                    data.protocol_version, LOGICALREP_PROTO_MIN_VERSION_NUM
                ))
                .finish(loc("pgoutput_startup"))?;
        }

        // Streaming: disabled by default; this plugin build doesn't stream
        // (the callbacks aren't registered), so requesting it gets C's
        // "not supported by output plugin" error.
        if data.streaming == LOGICALREP_STREAM_OFF {
            opc.streaming = false;
        } else if data.streaming == LOGICALREP_STREAM_ON
            && data.protocol_version < LOGICALREP_PROTO_STREAM_VERSION_NUM
        {
            ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!(
                    "requested proto_version={} does not support streaming, need {} or higher",
                    data.protocol_version, LOGICALREP_PROTO_STREAM_VERSION_NUM
                ))
                .finish(loc("pgoutput_startup"))?;
        } else if data.streaming == LOGICALREP_STREAM_PARALLEL
            && data.protocol_version < LOGICALREP_PROTO_STREAM_PARALLEL_VERSION_NUM
        {
            ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!(
                    "requested proto_version={} does not support parallel streaming, need {} or higher",
                    data.protocol_version, LOGICALREP_PROTO_STREAM_PARALLEL_VERSION_NUM
                ))
                .finish(loc("pgoutput_startup"))?;
        } else if !opc.streaming {
            ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg("streaming requested, but not supported by output plugin")
                .finish(loc("pgoutput_startup"))?;
        }

        // Two-phase: same shape; the decoding context doesn't support it.
        if !data.two_phase {
            opc.twophase_opt_given = false;
        } else if data.protocol_version < LOGICALREP_PROTO_TWOPHASE_VERSION_NUM {
            ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!(
                    "requested proto_version={} does not support two-phase commit, need {} or higher",
                    data.protocol_version, LOGICALREP_PROTO_TWOPHASE_VERSION_NUM
                ))
                .finish(loc("pgoutput_startup"))?;
        } else if !opc.twophase {
            ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg("two-phase commit requested, but not supported by output plugin")
                .finish(loc("pgoutput_startup"))?;
        } else {
            opc.twophase_opt_given = true;
        }

        // Init publication state.
        PUBLICATIONS_VALID.with(|c| c.set(false));

        if !PUBLICATION_CALLBACK_REGISTERED.with(|c| c.get()) {
            inval::invalidate::CacheRegisterSyscacheCallback(
                PUBLICATIONOID,
                publication_invalidation_cb,
                Datum::from_usize(0),
            )?;
            // C 18 also registers a RelSync callback here; the relcache
            // callback in init_rel_sync_cache covers those invalidations.
            PUBLICATION_CALLBACK_REGISTERED.with(|c| c.set(true));
        }

        init_rel_sync_cache()?;
    } else {
        // Slot initialization mode: no streaming / prepared transactions.
        opc.streaming = false;
        opc.twophase = false;
    }

    opc.output_plugin_private = Box::into_raw(data) as usize;
    Ok(())
}

// pgoutput_shutdown (pgoutput.c:1786).
fn pgoutput_shutdown(opc: &mut OutputPluginContext) -> PgResult<()> {
    REL_SYNC_CACHE.with(|c| *c.borrow_mut() = None);
    let p = opc.output_plugin_private;
    opc.output_plugin_private = 0;
    if p != 0 {
        // SAFETY: exclusive owner of the startup allocation.
        unsafe { drop(Box::from_raw(p as *mut PGOutputData)) };
    }
    Ok(())
}

// pgoutput_begin_txn (pgoutput.c:593): BEGIN is postponed until the first
// published change so empty transactions send nothing.
fn pgoutput_begin_txn(
    _opc: &mut OutputPluginContext,
    rb: &mut ReorderBuffer,
    txn: TxnId,
) -> PgResult<()> {
    let txndata = Box::new(PGOutputTxnData {
        sent_begin_txn: false,
    });
    rb.txn_mut(txn).output_plugin_private = Box::into_raw(txndata) as usize;
    Ok(())
}

// pgoutput_send_begin (pgoutput.c:607).
fn pgoutput_send_begin(
    opc: &mut OutputPluginContext,
    rb: &ReorderBuffer,
    txn: TxnId,
) -> PgResult<()> {
    let t = rb.txn(txn);
    let send_replication_origin = t.origin_id != InvalidRepOriginId;
    let (final_lsn, commit_time, xid) = (t.final_lsn, t.xact_time, t.xid);
    let (origin_id, origin_lsn) = (t.origin_id, t.origin_lsn);

    OutputPluginPrepareWrite(opc, !send_replication_origin)?;
    logicalrep_write_begin(opc.out.as_mut_vec(), final_lsn, commit_time, xid);
    txndata_from(rb, txn)
        .expect("begin callback allocated txndata")
        .sent_begin_txn = true;

    send_repl_origin(opc, origin_id, origin_lsn, send_replication_origin)?;

    OutputPluginWrite(opc, true)
}

// pgoutput_commit_txn (pgoutput.c:629).
fn pgoutput_commit_txn(
    opc: &mut OutputPluginContext,
    rb: &mut ReorderBuffer,
    txn: TxnId,
    commit_lsn: XLogRecPtr,
) -> PgResult<()> {
    let p = rb.txn(txn).output_plugin_private;
    rb.txn_mut(txn).output_plugin_private = 0;
    debug_assert!(p != 0);
    // SAFETY: exclusive owner of the begin-callback allocation.
    let txndata = unsafe { Box::from_raw(p as *mut PGOutputTxnData) };
    let sent_begin_txn = txndata.sent_begin_txn;
    drop(txndata);

    // No commit message unless some relevant change was sent downstream.
    OutputPluginUpdateProgress(opc, !sent_begin_txn)?;

    if !sent_begin_txn {
        return Ok(());
    }

    let t = rb.txn(txn);
    let (end_lsn, commit_time) = (t.end_lsn, t.xact_time);

    OutputPluginPrepareWrite(opc, true)?;
    logicalrep_write_commit(opc.out.as_mut_vec(), commit_lsn, end_lsn, commit_time);
    OutputPluginWrite(opc, true)
}

// maybe_send_schema (pgoutput.c:724), non-streaming arm.
fn maybe_send_schema(
    opc: &mut OutputPluginContext,
    relation: &RelationData<'static>,
    relentry: &Rc<RefCell<RelationSyncEntry>>,
) -> PgResult<()> {
    if relentry.borrow().schema_sent {
        return Ok(());
    }

    // If publishing via an ancestor's schema, send the ancestor's first.
    let publish_as_relid = relentry.borrow().publish_as_relid;
    if publish_as_relid != relation.rd_id {
        let ancestor = relcache::store::RelationIdGetRelation(publish_as_relid)?
            .unwrap_or_else(|| panic!("could not open relation {publish_as_relid}"));
        send_relation_and_attrs(opc, &ancestor, relentry)?;
    }

    send_relation_and_attrs(opc, relation, relentry)?;

    relentry.borrow_mut().schema_sent = true;
    Ok(())
}

// send_relation_and_attrs (pgoutput.c:795).
fn send_relation_and_attrs(
    opc: &mut OutputPluginContext,
    relation: &RelationData<'static>,
    relentry: &Rc<RefCell<RelationSyncEntry>>,
) -> PgResult<()> {
    let (columns, include_gencols_type) = {
        let e = relentry.borrow();
        (e.columns.clone(), e.include_gencols_type)
    };
    let columns = columns.as_deref();
    let xid = InvalidTransactionId; // not streaming

    // Send type info for user-created column types (hand-assigned OIDs are
    // "built in" and skipped).
    let desc = &relation.rd_att;
    for i in 0..desc.natts as usize {
        let att = desc.attr(i);
        if !logicalrep_should_publish_column(att, columns, include_gencols_type) {
            continue;
        }
        if att.atttypid < FirstGenbkiObjectId {
            continue;
        }
        OutputPluginPrepareWrite(opc, false)?;
        logicalrep_write_typ(opc.out.as_mut_vec(), xid, att.atttypid)?;
        OutputPluginWrite(opc, false)?;
    }

    OutputPluginPrepareWrite(opc, false)?;
    logicalrep_write_rel(
        opc.out.as_mut_vec(),
        xid,
        relation,
        columns,
        include_gencols_type,
    )?;
    OutputPluginWrite(opc, false)
}

// pgoutput_change (pgoutput.c:1481).
fn pgoutput_change(
    opc: &mut OutputPluginContext,
    rb: &mut ReorderBuffer,
    txn: TxnId,
    relation: &RelationData<'static>,
    change: &mut ReorderBufferChange,
) -> PgResult<()> {
    if !is_publishable_relation_data(relation) {
        return Ok(());
    }

    let data = data_from(opc);
    let relentry = get_rel_sync_entry(data, relation)?;

    let action = change.action;
    let (oldtuple, newtuple) = match &change.data {
        ReorderBufferChangeData::Tp {
            oldtuple, newtuple, ..
        } => (oldtuple.as_ref(), newtuple.as_ref()),
        _ => unreachable!("change callback with non-tuple payload"),
    };

    // Table-level action filter.
    {
        let e = relentry.borrow();
        match action {
            reorderbuffer::Insert => {
                if !e.pubactions.pubinsert {
                    return Ok(());
                }
            }
            reorderbuffer::Update => {
                if !e.pubactions.pubupdate {
                    return Ok(());
                }
            }
            reorderbuffer::Delete => {
                if !e.pubactions.pubdelete {
                    return Ok(());
                }
                // DELETE with no replica identity: nothing publishable.
                if oldtuple.is_none() {
                    return Ok(());
                }
            }
            _ => unreachable!("unexpected change action"),
        }
    }

    // Switch relation if publishing via root (partition routed to ancestor).
    let publish_as_relid = relentry.borrow().publish_as_relid;
    let ancestor;
    let targetrel: &RelationData<'static> = if publish_as_relid != relation.rd_id {
        debug_assert!(relation.rd_rel.relispartition);
        ancestor = relcache::store::RelationIdGetRelation(publish_as_relid)?
            .unwrap_or_else(|| panic!("could not open relation {publish_as_relid}"));
        // C converts tuples through an attrmap when the partition's
        // descriptor differs from the ancestor's; that conversion is
        // unported — refuse unless the descriptors line up.
        assert_descriptors_match(relation, &ancestor);
        &ancestor
    } else {
        relation
    };

    // Row filters were refused at entry-validation time; no transformation.

    // Send BEGIN if this is the first published change of the transaction.
    if let Some(txndata) = txndata_from(rb, txn) {
        if !txndata.sent_begin_txn {
            pgoutput_send_begin(opc, rb, txn)?;
        }
    }

    maybe_send_schema(opc, relation, &relentry)?;

    let (columns, include_gencols_type) = {
        let e = relentry.borrow();
        (e.columns.clone(), e.include_gencols_type)
    };
    let columns = columns.as_deref();
    let binary = data.binary;
    let xid = InvalidTransactionId; // not streaming

    OutputPluginPrepareWrite(opc, true)?;

    match action {
        reorderbuffer::Insert => {
            let new: &types_tuple::HeapTupleData = newtuple.expect("INSERT carries a new tuple");
            logicalrep_write_insert(
                opc.out.as_mut_vec(),
                xid,
                targetrel,
                new,
                binary,
                columns,
                include_gencols_type,
            )?;
        }
        reorderbuffer::Update => {
            let new: &types_tuple::HeapTupleData = newtuple.expect("UPDATE carries a new tuple");
            logicalrep_write_update(
                opc.out.as_mut_vec(),
                xid,
                targetrel,
                oldtuple.map(|t| &**t),
                new,
                binary,
                columns,
                include_gencols_type,
            )?;
        }
        reorderbuffer::Delete => {
            let old: &types_tuple::HeapTupleData = oldtuple.expect("checked above");
            logicalrep_write_delete(
                opc.out.as_mut_vec(),
                xid,
                targetrel,
                old,
                binary,
                columns,
                include_gencols_type,
            )?;
        }
        _ => unreachable!(),
    }

    OutputPluginWrite(opc, true)
}

// pgoutput_truncate (pgoutput.c:1654).
fn pgoutput_truncate(
    opc: &mut OutputPluginContext,
    rb: &mut ReorderBuffer,
    txn: TxnId,
    relations: &[Rc<RelationData<'static>>],
    change: &mut ReorderBufferChange,
) -> PgResult<()> {
    let data = data_from(opc);
    let mut relids: Vec<Oid> = Vec::with_capacity(relations.len());
    let mut queued: Vec<(Rc<RelationData<'static>>, Rc<RefCell<RelationSyncEntry>>)> = Vec::new();

    for relation in relations {
        if !is_publishable_relation_data(relation) {
            continue;
        }
        let relentry = get_rel_sync_entry(data, relation)?;
        {
            let e = relentry.borrow();
            if !e.pubactions.pubtruncate {
                continue;
            }
            // Don't send partitions if publishing only root tables.
            if relation.rd_rel.relispartition && e.publish_as_relid != relation.rd_id {
                continue;
            }
        }
        relids.push(relation.rd_id);
        queued.push((Rc::clone(relation), relentry));
    }

    if !relids.is_empty() {
        if let Some(txndata) = txndata_from(rb, txn) {
            if !txndata.sent_begin_txn {
                pgoutput_send_begin(opc, rb, txn)?;
            }
        }
        for (relation, relentry) in &queued {
            maybe_send_schema(opc, relation, relentry)?;
        }

        let (cascade, restart_seqs) = match &change.data {
            ReorderBufferChangeData::Truncate {
                cascade,
                restart_seqs,
                ..
            } => (*cascade, *restart_seqs),
            _ => unreachable!("truncate callback with non-truncate payload"),
        };

        OutputPluginPrepareWrite(opc, true)?;
        logicalrep_write_truncate(
            opc.out.as_mut_vec(),
            InvalidTransactionId,
            &relids,
            cascade,
            restart_seqs,
        );
        OutputPluginWrite(opc, true)?;
    }
    Ok(())
}

// pgoutput_message (pgoutput.c:1722).
fn pgoutput_message(
    opc: &mut OutputPluginContext,
    rb: &mut ReorderBuffer,
    txn: Option<TxnId>,
    message_lsn: XLogRecPtr,
    transactional: bool,
    prefix: &str,
    message: &[u8],
) -> PgResult<()> {
    let data = data_from(opc);
    if !data.messages {
        return Ok(());
    }

    if transactional {
        let txn = txn.expect("transactional message has a txn");
        if let Some(txndata) = txndata_from(rb, txn) {
            if !txndata.sent_begin_txn {
                pgoutput_send_begin(opc, rb, txn)?;
            }
        }
    }

    OutputPluginPrepareWrite(opc, true)?;
    logicalrep_write_message(
        opc.out.as_mut_vec(),
        InvalidTransactionId,
        message_lsn,
        transactional,
        prefix,
        message,
    );
    OutputPluginWrite(opc, true)
}

// pgoutput_origin_filter (pgoutput.c:1767).
fn pgoutput_origin_filter(opc: &mut OutputPluginContext, origin_id: RepOriginId) -> PgResult<bool> {
    let data = data_from(opc);
    Ok(data.publish_no_origin && origin_id != InvalidRepOriginId)
}

// send_repl_origin (pgoutput.c:2458). Reaching here with a real origin id
// needs the replication-origin engine (origin.c), owned by a parallel
// increment; until it lands this path is unreachable and refuses loudly.
fn send_repl_origin(
    _opc: &mut OutputPluginContext,
    origin_id: RepOriginId,
    _origin_lsn: XLogRecPtr,
    send_origin: bool,
) -> PgResult<()> {
    if send_origin && origin_id != InvalidRepOriginId {
        unported("send_repl_origin: replorigin_by_oid (origin.c)");
    }
    Ok(())
}

// LoadPublications (pgoutput.c:1799): missing publications are skipped with
// a WARNING so they can be created later in the WAL stream.
fn load_publications(pubnames: &[String]) -> PgResult<Vec<OwnedPublication>> {
    let mut result = Vec::with_capacity(pubnames.len());
    let ctx = MemoryContext::new("LoadPublications");
    for pubname in pubnames {
        match GetPublicationByName(ctx.mcx(), pubname, true)? {
            Some(pub_) => result.push(OwnedPublication {
                oid: pub_.oid,
                alltables: pub_.alltables,
                pubviaroot: pub_.pubviaroot,
                pubgencols_type: pub_.pubgencols_type,
                pubactions: PublicationActions {
                    pubinsert: pub_.pubactions.pubinsert,
                    pubupdate: pub_.pubactions.pubupdate,
                    pubdelete: pub_.pubactions.pubdelete,
                    pubtruncate: pub_.pubactions.pubtruncate,
                },
            }),
            None => {
                let _ = ereport(WARNING)
                    .errmsg(format!("skipped loading publication \"{pubname}\""))
                    .errdetail("The publication does not exist at this point in the WAL.")
                    .errhint("Create the publication if it does not exist.")
                    .finish(loc("LoadPublications"));
            }
        }
    }
    Ok(result)
}

// publication_invalidation_cb (pgoutput.c:1829).
fn publication_invalidation_cb(_arg: Datum, _cacheid: i32, _hashvalue: u32) {
    PUBLICATIONS_VALID.with(|c| c.set(false));
}

// init_rel_sync_cache (pgoutput.c:1971).
fn init_rel_sync_cache() -> PgResult<()> {
    let exists = REL_SYNC_CACHE.with(|c| c.borrow().is_some());
    if exists {
        return Ok(());
    }
    REL_SYNC_CACHE.with(|c| *c.borrow_mut() = Some(HashMap::new()));

    if RELATION_CALLBACKS_REGISTERED.with(|c| c.get()) {
        return Ok(());
    }
    inval::invalidate::CacheRegisterRelcacheCallback(
        rel_sync_cache_relation_cb,
        Datum::from_usize(0),
    )?;
    // Flush all entries after a pg_namespace change (possible schema rename).
    inval::invalidate::CacheRegisterSyscacheCallback(
        NAMESPACEOID,
        rel_sync_cache_publication_cb,
        Datum::from_usize(0),
    )?;
    RELATION_CALLBACKS_REGISTERED.with(|c| c.set(true));
    Ok(())
}

// rel_sync_cache_relation_cb (pgoutput.c:2382).
fn rel_sync_cache_relation_cb(_arg: Datum, relid: Oid) {
    REL_SYNC_CACHE.with(|c| {
        let mut borrow = c.borrow_mut();
        let Some(cache) = borrow.as_mut() else {
            return;
        };
        if relid != InvalidOid {
            if let Some(entry) = cache.get(&relid) {
                entry.borrow_mut().replicate_valid = false;
            }
        } else {
            for entry in cache.values() {
                entry.borrow_mut().replicate_valid = false;
            }
        }
    });
}

// rel_sync_cache_publication_cb (pgoutput.c:2432).
fn rel_sync_cache_publication_cb(_arg: Datum, _cacheid: i32, _hashvalue: u32) {
    REL_SYNC_CACHE.with(|c| {
        let mut borrow = c.borrow_mut();
        let Some(cache) = borrow.as_mut() else {
            return;
        };
        for entry in cache.values() {
            entry.borrow_mut().replicate_valid = false;
        }
    });
}

// is_publishable_relation over the plugin-visible RelationData.
fn is_publishable_relation_data(rel: &RelationData<'static>) -> bool {
    pg_publication::is_publishable_class(
        rel.rd_id,
        rel.rd_rel.relkind as u8,
        rel.rd_rel.relpersistence,
    )
}

// The partition-to-ancestor tuple conversion (build_attrmap_by_name_if_req /
// execute_attr_map_slot) is unported; identical descriptors need no map.
fn assert_descriptors_match(part: &RelationData<'static>, ancestor: &RelationData<'static>) {
    let pd = &part.rd_att;
    let ad = &ancestor.rd_att;
    let mismatch = pd.natts != ad.natts
        || (0..pd.natts as usize).any(|i| {
            let pa = pd.attr(i);
            let aa = ad.attr(i);
            pa.attname.name_str() != aa.attname.name_str()
                || pa.atttypid != aa.atttypid
                || pa.attisdropped != aa.attisdropped
        });
    if mismatch {
        unported("publish_via_partition_root with differing partition descriptors (attrmap)");
    }
}

// pgoutput_row_filter_init's detection arm (pgoutput.c:916): any row filter
// on a subscribed publication is refused loudly (ExprState eval unported).
fn refuse_row_filters(publications: &[&OwnedPublication], publish_as_relid: Oid) -> PgResult<()> {
    let schemaid = lsyscache::get_rel_namespace(publish_as_relid)?;
    let ctx = MemoryContext::new("refuse_row_filters");
    let schema_pubids = GetSchemaPublications(ctx.mcx(), schemaid)?;
    for pub_ in publications {
        if pub_.alltables {
            continue;
        }
        // FOR TABLES IN SCHEMA implies no row filter.
        if schema_pubids.contains(&pub_.oid) {
            continue;
        }
        if let Some(rftuple) = SearchSysCache2(
            PUBLICATIONRELMAP,
            SysCacheKey::Value(Datum::from_oid(publish_as_relid)),
            SysCacheKey::Value(Datum::from_oid(pub_.oid)),
        )? {
            let (_d, isnull) =
                SysCacheGetAttr(PUBLICATIONRELMAP, &rftuple, Anum_pg_publication_rel_prqual)?;
            ReleaseSysCache(rftuple);
            if !isnull {
                unported("row-filter publications (ExprState evaluation)");
            }
        }
    }
    Ok(())
}

// check_and_init_gencol (pgoutput.c:1062).
fn check_and_init_gencol(
    entry: &mut RelationSyncEntry,
    publications: &[&OwnedPublication],
    relation: &RelationData<'static>,
) -> PgResult<()> {
    let desc = &relation.rd_att;
    let gencolpresent = (0..desc.natts as usize).any(|i| desc.attr(i).attgenerated != 0);
    if !gencolpresent {
        entry.include_gencols_type = PUBLISH_GENCOLS_NONE;
        return Ok(());
    }

    let ctx = MemoryContext::new("check_and_init_gencol");
    let mut first = true;
    for pub_ in publications {
        // A column list takes precedence over publish_generated_columns.
        if has_column_list(ctx.mcx(), pub_, entry.publish_as_relid)? {
            continue;
        }
        if first {
            entry.include_gencols_type = pub_.pubgencols_type;
            first = false;
        } else if entry.include_gencols_type != pub_.pubgencols_type {
            ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(format!(
                    "cannot use different values of publish_generated_columns for table \"{}\" in different publications",
                    String::from_utf8_lossy(relation.rd_rel.relname.name_str())
                ))
                .finish(loc("check_and_init_gencol"))?;
        }
    }
    Ok(())
}

// check_and_fetch_column_list over an OwnedPublication, presence check only.
fn has_column_list(mcx: mcx::Mcx<'_>, pub_: &OwnedPublication, relid: Oid) -> PgResult<bool> {
    let p = owned_to_publication(mcx, pub_)?;
    check_and_fetch_column_list(mcx, &p, relid, None)
}

fn owned_to_publication<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    pub_: &OwnedPublication,
) -> PgResult<pg_publication::Publication<'mcx>> {
    Ok(pg_publication::Publication {
        oid: pub_.oid,
        name: mcx::PgString::from_str_in("", mcx)?,
        alltables: pub_.alltables,
        pubviaroot: pub_.pubviaroot,
        pubgencols_type: pub_.pubgencols_type,
        pubactions: PublicationActions {
            pubinsert: pub_.pubactions.pubinsert,
            pubupdate: pub_.pubactions.pubupdate,
            pubdelete: pub_.pubactions.pubdelete,
            pubtruncate: pub_.pubactions.pubtruncate,
        },
    })
}

// pgoutput_column_list_init (pgoutput.c:1122). entry.columns is the raw-attnum
// list (ascending); differing lists across publications are an error, per C.
fn pgoutput_column_list_init(
    entry: &mut RelationSyncEntry,
    publications: &[&OwnedPublication],
    relation: &RelationData<'static>,
) -> PgResult<()> {
    let ctx = MemoryContext::new("pgoutput_column_list_init");
    let mcx = ctx.mcx();
    let mut first = true;
    let mut found_pub_collist = false;
    let mut relcols: Option<Vec<i16>> = None;
    let mut acc: Option<Vec<i16>> = None;

    for pub_ in publications {
        let p = owned_to_publication(mcx, pub_)?;
        let mut bms = Bitmapset::empty();
        let found = check_and_fetch_column_list(mcx, &p, entry.publish_as_relid, Some(&mut bms))?;
        found_pub_collist |= found;

        let cols: Option<Vec<i16>> = if found {
            Some(bms.iter().map(|x| x as i16).collect())
        } else {
            // Non-column-list publication: all publishable columns
            // (pub_form_cols_map).
            if relcols.is_none() && publications.len() > 1 {
                let desc = &relation.rd_att;
                let mut all = Vec::new();
                for i in 0..desc.natts as usize {
                    let att = desc.attr(i);
                    if att.attisdropped {
                        continue;
                    }
                    if att.attgenerated != 0
                        && entry.include_gencols_type != PUBLISH_GENCOLS_STORED
                    {
                        continue;
                    }
                    all.push(att.attnum);
                }
                relcols = Some(all);
            }
            relcols.clone()
        };

        if first {
            acc = cols;
            first = false;
        } else if acc != cols {
            ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(format!(
                    "cannot use different column lists for table \"{}\" in different publications",
                    String::from_utf8_lossy(relation.rd_rel.relname.name_str())
                ))
                .finish(loc("pgoutput_column_list_init"))?;
        }
    }

    entry.columns = if found_pub_collist { acc } else { None };
    Ok(())
}

// get_rel_sync_entry (pgoutput.c:2051).
fn get_rel_sync_entry(
    data: &mut PGOutputData,
    relation: &RelationData<'static>,
) -> PgResult<Rc<RefCell<RelationSyncEntry>>> {
    let relid = relation.rd_id;

    let entry = REL_SYNC_CACHE.with(|c| {
        let mut borrow = c.borrow_mut();
        let cache = borrow.as_mut().expect("RelationSyncCache initialized");
        Rc::clone(
            cache
                .entry(relid)
                .or_insert_with(|| Rc::new(RefCell::new(RelationSyncEntry::new()))),
        )
    });

    if entry.borrow().replicate_valid {
        return Ok(entry);
    }

    // (Re)validate the entry.
    let ctx = MemoryContext::new("get_rel_sync_entry");
    let mcx = ctx.mcx();

    let schema_id = lsyscache::get_rel_namespace(relid)?;
    let pubids = GetRelationPublications(mcx, relid)?;
    let schema_pubids = GetSchemaPublications(mcx, schema_id)?;
    let am_partition = lsyscache::get_rel_relispartition(relid)?;
    let relkind = lsyscache::get_rel_relkind(relid)? as u8;

    // Reload publications if needed before use.
    if !PUBLICATIONS_VALID.with(|c| c.get()) {
        data.publications = load_publications(&data.publication_names)?;
        PUBLICATIONS_VALID.with(|c| c.set(true));
    }

    {
        let mut e = entry.borrow_mut();
        e.schema_sent = false;
        e.include_gencols_type = PUBLISH_GENCOLS_NONE;
        e.columns = None;
        e.pubactions = PublicationActions {
            pubinsert: false,
            pubupdate: false,
            pubdelete: false,
            pubtruncate: false,
        };

        let mut publish_as_relid = relid;
        let mut publish_ancestor_level = 0i32;
        let mut rel_publications: Vec<&OwnedPublication> = Vec::new();

        for pub_ in &data.publications {
            let mut publish = false;
            let mut pub_relid = relid;
            let mut ancestor_level = 0i32;

            if pub_.alltables {
                publish = true;
                if pub_.pubviaroot && am_partition {
                    let ancestors = pg_inherits::get_partition_ancestors(mcx, relid)?;
                    if let Some(&last) = ancestors.last() {
                        pub_relid = last;
                        ancestor_level = ancestors.len() as i32;
                    }
                }
            }

            if !publish {
                let mut ancestor_published = false;
                if am_partition {
                    let ancestors = pg_inherits::get_partition_ancestors(mcx, relid)?;
                    let mut level = 0i32;
                    let ancestor = GetTopMostAncestorInPublication(
                        mcx,
                        pub_.oid,
                        &ancestors,
                        Some(&mut level),
                    )?;
                    if ancestor != InvalidOid {
                        ancestor_published = true;
                        if pub_.pubviaroot {
                            pub_relid = ancestor;
                            ancestor_level = level;
                        }
                    }
                }
                if pubids.contains(&pub_.oid)
                    || schema_pubids.contains(&pub_.oid)
                    || ancestor_published
                {
                    publish = true;
                }
            }

            // Don't publish partitioned-table changes unless pubviaroot.
            if publish && (relkind != RELKIND_PARTITIONED_TABLE || pub_.pubviaroot) {
                e.pubactions.pubinsert |= pub_.pubactions.pubinsert;
                e.pubactions.pubupdate |= pub_.pubactions.pubupdate;
                e.pubactions.pubdelete |= pub_.pubactions.pubdelete;
                e.pubactions.pubtruncate |= pub_.pubactions.pubtruncate;

                if publish_ancestor_level > ancestor_level {
                    continue;
                }
                if publish_ancestor_level < ancestor_level {
                    publish_as_relid = pub_relid;
                    publish_ancestor_level = ancestor_level;
                    rel_publications.clear();
                } else {
                    debug_assert!(publish_as_relid == pub_relid);
                }
                rel_publications.push(pub_);
            }
        }

        e.publish_as_relid = publish_as_relid;

        if e.pubactions.pubinsert || e.pubactions.pubupdate || e.pubactions.pubdelete {
            // Row filters are unported: refuse loudly if any apply.
            refuse_row_filters(&rel_publications, publish_as_relid)?;
            check_and_init_gencol(&mut e, &rel_publications, relation)?;
            pgoutput_column_list_init(&mut e, &rel_publications, relation)?;
        }

        e.replicate_valid = true;
    }

    Ok(entry)
}

fn lookup(function: &str) -> Option<PGFunction> {
    match function {
        "_PG_output_plugin_init" => Some(fc__pg_output_plugin_init),
        _ => None,
    }
}

pub fn init_seams() {
    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        pg_init: None,
    });
}
