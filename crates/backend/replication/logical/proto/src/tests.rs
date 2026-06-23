//! Round-trip and byte-level tests over the reader/writer pairs that do not
//! reach into unported subsystems (relation/syscache/slot paths need live
//! seam owners and are exercised when those land).

use super::*;
use std::sync::Once;

fn install_fixtures() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        // No-op encoding conversion (matching client/server encodings).
        mbutils_seams::pg_server_to_client::set(|_, _| Ok(None));
        mbutils_seams::pg_client_to_server::set(|_, _| Ok(None));
        bms_seams::bms_is_member::set(fixture_bms_is_member);
        bms_seams::bms_add_member::set(fixture_bms_add_member);
    });
}

fn fixture_bms_is_member(x: i32, a: Option<&Bitmapset<'_>>) -> bool {
    if x < 0 {
        return false;
    }
    match a {
        Some(s) => {
            let w = (x / 64) as usize;
            w < s.words.len() && s.words[w] & (1u64 << (x % 64)) != 0
        }
        None => false,
    }
}

fn fixture_bms_add_member<'mcx>(
    mcx: Mcx<'mcx>,
    a: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    x: i32,
) -> PgResult<PgBox<'mcx, Bitmapset<'mcx>>> {
    let wordnum = (x / 64) as usize;
    let mut set = match a {
        Some(s) => s,
        None => mcx::alloc_in(
            mcx,
            Bitmapset {
                words: PgVec::new_in(mcx),
            },
        )?,
    };
    while set.words.len() <= wordnum {
        set.words.push(0);
    }
    set.words[wordnum] |= 1u64 << (x % 64);
    Ok(set)
}

struct Fixture {
    ctx: mcx::MemoryContext,
}

fn setup() -> Fixture {
    install_fixtures();
    Fixture {
        ctx: mcx::MemoryContext::new("proto-test"),
    }
}

fn reader<'mcx>(f: &'mcx Fixture, out: &StringInfo<'_>) -> StringInfo<'mcx> {
    StringInfo::from_vec(mcx::slice_in(f.ctx.mcx(), &out.data).unwrap())
}

fn txn_fixture<'mcx>(f: &'mcx Fixture, gid: Option<&[u8]>) -> ReorderBufferTXN<'mcx> {
    ReorderBufferTXN {
        txn_flags: wal::reorderbuffer::RBTXN_IS_PREPARED,
        xid: 654,
        gid: gid.map(|g| mcx::slice_in(f.ctx.mcx(), g).unwrap()),
        final_lsn: 0x0102030405060708,
        end_lsn: 0x1112131415161718,
        xact_time: 777_000_111,
    }
}

#[test]
fn begin_bytes_and_roundtrip() {
    let f = setup();
    let mut out = StringInfo::new_in(f.ctx.mcx());
    let txn = txn_fixture(&f, None);
    logicalrep_write_begin(&mut out, &txn).unwrap();

    // 'B' + final_lsn(8, network order) + commit_time(8) + xid(4)
    assert_eq!(out.data[0], b'B');
    assert_eq!(out.data.len(), 1 + 8 + 8 + 4);
    assert_eq!(&out.data[1..9], &0x0102030405060708u64.to_be_bytes());

    let mut in_ = reader(&f, &out);
    assert_eq!(pq_getmsgbyte(&mut in_).unwrap(), b'B' as i32);
    let mut begin_data = LogicalRepBeginData::default();
    logicalrep_read_begin(&mut in_, &mut begin_data).unwrap();
    assert_eq!(begin_data.final_lsn, txn.final_lsn);
    assert_eq!(begin_data.committime, txn.xact_time);
    assert_eq!(begin_data.xid, txn.xid);
}

#[test]
fn begin_rejects_invalid_lsn() {
    let f = setup();
    let mut out = StringInfo::new_in(f.ctx.mcx());
    let mut txn = txn_fixture(&f, None);
    txn.final_lsn = InvalidXLogRecPtr;
    logicalrep_write_begin(&mut out, &txn).unwrap();
    let mut in_ = reader(&f, &out);
    pq_getmsgbyte(&mut in_).unwrap();
    let err = logicalrep_read_begin(&mut in_, &mut LogicalRepBeginData::default()).unwrap_err();
    assert!(err.message().contains("final_lsn not set"));
}

#[test]
fn commit_roundtrip() {
    let f = setup();
    let mut out = StringInfo::new_in(f.ctx.mcx());
    let txn = txn_fixture(&f, None);
    logicalrep_write_commit(&mut out, &txn, 0xdeadbeef).unwrap();
    assert_eq!(out.data[0], b'C');

    let mut in_ = reader(&f, &out);
    pq_getmsgbyte(&mut in_).unwrap();
    let mut commit_data = LogicalRepCommitData::default();
    logicalrep_read_commit(&mut in_, &mut commit_data).unwrap();
    assert_eq!(commit_data.commit_lsn, 0xdeadbeef);
    assert_eq!(commit_data.end_lsn, txn.end_lsn);
    assert_eq!(commit_data.committime, txn.xact_time);
}

#[test]
fn prepare_family_roundtrip() {
    let f = setup();
    let txn = txn_fixture(&f, Some(b"gid-42"));

    // begin prepare
    let mut out = StringInfo::new_in(f.ctx.mcx());
    logicalrep_write_begin_prepare(&mut out, &txn).unwrap();
    assert_eq!(out.data[0], b'b');
    let mut in_ = reader(&f, &out);
    pq_getmsgbyte(&mut in_).unwrap();
    let mut bp = LogicalRepPreparedTxnData::default();
    logicalrep_read_begin_prepare(f.ctx.mcx(), &mut in_, &mut bp).unwrap();
    assert_eq!(bp.prepare_lsn, txn.final_lsn);
    assert_eq!(bp.end_lsn, txn.end_lsn);
    assert_eq!(bp.xid, txn.xid);
    assert_eq!(&bp.gid[..7], b"gid-42\0");

    // prepare
    let mut out = StringInfo::new_in(f.ctx.mcx());
    logicalrep_write_prepare(&mut out, &txn, 42).unwrap();
    assert_eq!(out.data[0], b'P');
    let mut in_ = reader(&f, &out);
    pq_getmsgbyte(&mut in_).unwrap();
    let mut pd = LogicalRepPreparedTxnData::default();
    logicalrep_read_prepare(f.ctx.mcx(), &mut in_, &mut pd).unwrap();
    assert_eq!(pd.prepare_lsn, 42);
    assert_eq!(pd.prepare_time, txn.xact_time);

    // stream prepare
    let mut out = StringInfo::new_in(f.ctx.mcx());
    logicalrep_write_stream_prepare(&mut out, &txn, 43).unwrap();
    assert_eq!(out.data[0], b'p');
    let mut in_ = reader(&f, &out);
    pq_getmsgbyte(&mut in_).unwrap();
    let mut sp = LogicalRepPreparedTxnData::default();
    logicalrep_read_stream_prepare(f.ctx.mcx(), &mut in_, &mut sp).unwrap();
    assert_eq!(sp.prepare_lsn, 43);

    // commit prepared
    let mut out = StringInfo::new_in(f.ctx.mcx());
    logicalrep_write_commit_prepared(&mut out, &txn, 44).unwrap();
    assert_eq!(out.data[0], b'K');
    let mut in_ = reader(&f, &out);
    pq_getmsgbyte(&mut in_).unwrap();
    let mut cp = LogicalRepCommitPreparedTxnData::default();
    logicalrep_read_commit_prepared(f.ctx.mcx(), &mut in_, &mut cp).unwrap();
    assert_eq!(cp.commit_lsn, 44);
    assert_eq!(&cp.gid[..7], b"gid-42\0");

    // rollback prepared
    let mut out = StringInfo::new_in(f.ctx.mcx());
    logicalrep_write_rollback_prepared(&mut out, &txn, 45, 123456).unwrap();
    assert_eq!(out.data[0], b'r');
    let mut in_ = reader(&f, &out);
    pq_getmsgbyte(&mut in_).unwrap();
    let mut rp = LogicalRepRollbackPreparedTxnData::default();
    logicalrep_read_rollback_prepared(f.ctx.mcx(), &mut in_, &mut rp).unwrap();
    assert_eq!(rp.prepare_end_lsn, 45);
    assert_eq!(rp.rollback_end_lsn, txn.end_lsn);
    assert_eq!(rp.prepare_time, 123456);
    assert_eq!(rp.rollback_time, txn.xact_time);
    assert_eq!(rp.xid, txn.xid);
}

#[test]
fn prepare_rejects_nonzero_flags() {
    let f = setup();
    // Hand-build a prepare body with flags = 1.
    let mut in_ = StringInfo::from_vec(mcx::slice_in(f.ctx.mcx(), &[1u8]).unwrap());
    let err = logicalrep_read_prepare(
        f.ctx.mcx(),
        &mut in_,
        &mut LogicalRepPreparedTxnData::default(),
    )
    .unwrap_err();
    assert!(err.message().contains("unrecognized flags 1 in prepare message"));
}

#[test]
fn origin_roundtrip() {
    let f = setup();
    let mut out = StringInfo::new_in(f.ctx.mcx());
    logicalrep_write_origin(&mut out, b"my_origin", 0x55).unwrap();
    assert_eq!(out.data[0], b'O');

    let mut in_ = reader(&f, &out);
    pq_getmsgbyte(&mut in_).unwrap();
    let mut origin_lsn = 0;
    let origin = logicalrep_read_origin(f.ctx.mcx(), &mut in_, &mut origin_lsn).unwrap();
    assert_eq!(origin_lsn, 0x55);
    assert_eq!(&origin[..], b"my_origin");
}

#[test]
fn truncate_roundtrip() {
    let f = setup();
    let mut out = StringInfo::new_in(f.ctx.mcx());
    let relids = [16384u32, 16385, 16386];
    logicalrep_write_truncate(&mut out, 9, 3, &relids, true, false).unwrap();
    assert_eq!(out.data[0], b'T');

    let mut in_ = reader(&f, &out);
    pq_getmsgbyte(&mut in_).unwrap();
    // streaming path wrote the xid
    assert_eq!(pq_getmsgint(&mut in_, 4).unwrap(), 9);
    let mut cascade = false;
    let mut restart_seqs = true;
    let got =
        logicalrep_read_truncate(f.ctx.mcx(), &mut in_, &mut cascade, &mut restart_seqs).unwrap();
    assert!(cascade);
    assert!(!restart_seqs);
    assert_eq!(&got[..], &relids);
}

#[test]
fn message_bytes() {
    let f = setup();
    let mut out = StringInfo::new_in(f.ctx.mcx());
    logicalrep_write_message(&mut out, 0, 0x10, true, b"pfx", 3, b"abcXX").unwrap();
    // 'M', flags=1 (no xid: InvalidTransactionId), lsn, "pfx\0", len=3, "abc"
    let expect: Vec<u8> = [
        &[b'M', 1u8][..],
        &0x10u64.to_be_bytes(),
        b"pfx\0",
        &3u32.to_be_bytes(),
        b"abc",
    ]
    .concat();
    assert_eq!(&out.data[..], &expect[..]);
}

#[test]
fn stream_start_stop_roundtrip() {
    let f = setup();
    let mut out = StringInfo::new_in(f.ctx.mcx());
    logicalrep_write_stream_start(&mut out, 77, true).unwrap();
    assert_eq!(out.data[0], b'S');

    let mut in_ = reader(&f, &out);
    pq_getmsgbyte(&mut in_).unwrap();
    let mut first_segment = false;
    let xid = logicalrep_read_stream_start(&mut in_, &mut first_segment).unwrap();
    assert_eq!(xid, 77);
    assert!(first_segment);

    let mut out = StringInfo::new_in(f.ctx.mcx());
    logicalrep_write_stream_stop(&mut out).unwrap();
    assert_eq!(&out.data[..], b"E");
}

#[test]
fn stream_commit_roundtrip() {
    let f = setup();
    let txn = txn_fixture(&f, None);
    let mut out = StringInfo::new_in(f.ctx.mcx());
    logicalrep_write_stream_commit(&mut out, &txn, 0x99).unwrap();
    assert_eq!(out.data[0], b'c');

    let mut in_ = reader(&f, &out);
    pq_getmsgbyte(&mut in_).unwrap();
    let mut commit_data = LogicalRepCommitData::default();
    let xid = logicalrep_read_stream_commit(&mut in_, &mut commit_data).unwrap();
    assert_eq!(xid, txn.xid);
    assert_eq!(commit_data.commit_lsn, 0x99);
}

#[test]
fn stream_abort_roundtrip_both_forms() {
    let f = setup();
    for abort_info in [true, false] {
        let mut out = StringInfo::new_in(f.ctx.mcx());
        logicalrep_write_stream_abort(&mut out, 5, 6, 0x77, 123, abort_info).unwrap();
        assert_eq!(out.data[0], b'A');

        let mut in_ = reader(&f, &out);
        pq_getmsgbyte(&mut in_).unwrap();
        let mut abort_data = LogicalRepStreamAbortData::default();
        logicalrep_read_stream_abort(&mut in_, &mut abort_data, abort_info).unwrap();
        assert_eq!(abort_data.xid, 5);
        assert_eq!(abort_data.subxid, 6);
        if abort_info {
            assert_eq!(abort_data.abort_lsn, 0x77);
            assert_eq!(abort_data.abort_time, 123);
        } else {
            assert_eq!(abort_data.abort_lsn, InvalidXLogRecPtr);
            assert_eq!(abort_data.abort_time, 0);
        }
    }
}

#[test]
fn read_rel_and_attrs() {
    let f = setup();
    let mcx = f.ctx.mcx();
    // Hand-build a RELATION body: remoteid, nspname ("" => pg_catalog),
    // relname, replident, natts=2, then per-attr (flags, name, typid, typmod).
    let mut body: Vec<u8> = Vec::new();
    body.extend_from_slice(&1234u32.to_be_bytes());
    body.extend_from_slice(b"\0"); // empty namespace -> pg_catalog
    body.extend_from_slice(b"tab\0");
    body.push(b'd');
    body.extend_from_slice(&2u16.to_be_bytes());
    // attr 0: replica-identity key
    body.push(1);
    body.extend_from_slice(b"id\0");
    body.extend_from_slice(&23u32.to_be_bytes());
    body.extend_from_slice(&(-1i32).to_be_bytes());
    // attr 1: not key
    body.push(0);
    body.extend_from_slice(b"payload\0");
    body.extend_from_slice(&25u32.to_be_bytes());
    body.extend_from_slice(&(-1i32).to_be_bytes());

    let mut in_ = StringInfo::from_vec(mcx::slice_in(mcx, &body).unwrap());
    let rel = logicalrep_read_rel(mcx, &mut in_).unwrap();
    assert_eq!(rel.remoteid, 1234);
    assert_eq!(&rel.nspname[..], b"pg_catalog");
    assert_eq!(&rel.relname[..], b"tab");
    assert_eq!(rel.replident, b'd');
    assert_eq!(rel.natts, 2);
    assert_eq!(&rel.attnames[0][..], b"id");
    assert_eq!(&rel.attnames[1][..], b"payload");
    assert_eq!(&rel.atttyps[..], &[23, 25]);
    assert!(fixture_bms_is_member(0, rel.attkeys.as_deref()));
    assert!(!fixture_bms_is_member(1, rel.attkeys.as_deref()));
}

#[test]
fn read_typ() {
    let f = setup();
    let mcx = f.ctx.mcx();
    let mut body: Vec<u8> = Vec::new();
    body.extend_from_slice(&5555u32.to_be_bytes());
    body.extend_from_slice(b"myschema\0");
    body.extend_from_slice(b"mytype\0");
    let mut in_ = StringInfo::from_vec(mcx::slice_in(mcx, &body).unwrap());
    let mut ltyp = LogicalRepTyp {
        remoteid: 0,
        nspname: PgVec::new_in(mcx),
        typname: PgVec::new_in(mcx),
    };
    logicalrep_read_typ(mcx, &mut in_, &mut ltyp).unwrap();
    assert_eq!(ltyp.remoteid, 5555);
    assert_eq!(&ltyp.nspname[..], b"myschema");
    assert_eq!(&ltyp.typname[..], b"mytype");
}

#[test]
fn read_insert_update_delete_tuples() {
    let f = setup();
    let mcx = f.ctx.mcx();

    // INSERT body: relid, 'N', natts=3, [text "x"], [null], [unchanged]
    let mut body: Vec<u8> = Vec::new();
    body.extend_from_slice(&77u32.to_be_bytes());
    body.push(b'N');
    body.extend_from_slice(&3u16.to_be_bytes());
    body.push(LOGICALREP_COLUMN_TEXT);
    body.extend_from_slice(&1u32.to_be_bytes());
    body.push(b'x');
    body.push(LOGICALREP_COLUMN_NULL);
    body.push(LOGICALREP_COLUMN_UNCHANGED);

    let mut in_ = StringInfo::from_vec(mcx::slice_in(mcx, &body).unwrap());
    let mut newtup = LogicalRepTupleData::new_in(mcx);
    let relid = logicalrep_read_insert(mcx, &mut in_, &mut newtup).unwrap();
    assert_eq!(relid, 77);
    assert_eq!(newtup.ncols, 3);
    assert_eq!(
        &newtup.colstatus[..],
        &[
            LOGICALREP_COLUMN_TEXT,
            LOGICALREP_COLUMN_NULL,
            LOGICALREP_COLUMN_UNCHANGED
        ]
    );
    assert_eq!(&newtup.colvalues[0].data[..], b"x");
    assert!(newtup.colvalues[1].data.is_empty());

    // UPDATE with old key tuple: relid, 'K', tuple, 'N', tuple
    let one_col_tuple: Vec<u8> = {
        let mut t = Vec::new();
        t.extend_from_slice(&1u16.to_be_bytes());
        t.push(LOGICALREP_COLUMN_BINARY);
        t.extend_from_slice(&2u32.to_be_bytes());
        t.extend_from_slice(b"ab");
        t
    };
    let mut body: Vec<u8> = Vec::new();
    body.extend_from_slice(&88u32.to_be_bytes());
    body.push(b'K');
    body.extend_from_slice(&one_col_tuple);
    body.push(b'N');
    body.extend_from_slice(&one_col_tuple);

    let mut in_ = StringInfo::from_vec(mcx::slice_in(mcx, &body).unwrap());
    let mut has_oldtuple = false;
    let mut oldtup = LogicalRepTupleData::new_in(mcx);
    let mut newtup = LogicalRepTupleData::new_in(mcx);
    let relid =
        logicalrep_read_update(mcx, &mut in_, &mut has_oldtuple, &mut oldtup, &mut newtup)
            .unwrap();
    assert_eq!(relid, 88);
    assert!(has_oldtuple);
    assert_eq!(&oldtup.colvalues[0].data[..], b"ab");
    assert_eq!(&newtup.colvalues[0].data[..], b"ab");

    // DELETE: relid, 'O', tuple
    let mut body: Vec<u8> = Vec::new();
    body.extend_from_slice(&99u32.to_be_bytes());
    body.push(b'O');
    body.extend_from_slice(&one_col_tuple);
    let mut in_ = StringInfo::from_vec(mcx::slice_in(mcx, &body).unwrap());
    let mut oldtup = LogicalRepTupleData::new_in(mcx);
    let relid = logicalrep_read_delete(mcx, &mut in_, &mut oldtup).unwrap();
    assert_eq!(relid, 99);
    assert_eq!(oldtup.colstatus[0], LOGICALREP_COLUMN_BINARY);

    // bad action byte
    let mut body: Vec<u8> = Vec::new();
    body.extend_from_slice(&99u32.to_be_bytes());
    body.push(b'N');
    let mut in_ = StringInfo::from_vec(mcx::slice_in(mcx, &body).unwrap());
    let mut oldtup = LogicalRepTupleData::new_in(mcx);
    let err = logicalrep_read_delete(mcx, &mut in_, &mut oldtup).unwrap_err();
    assert!(err.message().contains("expected action 'O' or 'K'"));
}

#[test]
fn read_tuple_rejects_unknown_kind() {
    let f = setup();
    let mcx = f.ctx.mcx();
    let mut body: Vec<u8> = Vec::new();
    body.extend_from_slice(&66u32.to_be_bytes());
    body.push(b'N');
    body.extend_from_slice(&1u16.to_be_bytes());
    body.push(b'?');
    let mut in_ = StringInfo::from_vec(mcx::slice_in(mcx, &body).unwrap());
    let mut newtup = LogicalRepTupleData::new_in(mcx);
    let err = logicalrep_read_insert(mcx, &mut in_, &mut newtup).unwrap_err();
    assert!(err
        .message()
        .contains("unrecognized data representation type '?'"));
}

#[test]
fn message_type_names() {
    assert_eq!(logicalrep_message_type(b'B' as i32), "BEGIN");
    assert_eq!(logicalrep_message_type(b'c' as i32), "STREAM COMMIT");
    assert_eq!(logicalrep_message_type(b'p' as i32), "STREAM PREPARE");
    assert_eq!(logicalrep_message_type(12345), "??? (12345)");
}

fn att_fixture(attnum: i16, dropped: bool, generated: i8) -> FormData_pg_attribute {
    FormData_pg_attribute {
        attrelid: 0,
        attname: types_tuple::heaptuple::NameData {
            data: [0; types_core::NAMEDATALEN as usize],
        },
        atttypid: 25,
        attlen: -1,
        attnum,
        atttypmod: -1,
        attndims: 0,
        attbyval: false,
        attalign: b'i' as i8,
        attstorage: b'x' as i8,
        attcompression: 0,
        attnotnull: false,
        atthasdef: false,
        atthasmissing: false,
        attidentity: 0,
        attgenerated: generated,
        attisdropped: dropped,
        attislocal: true,
        attinhcount: 0,
        attcollation: 0,
    }
}

#[test]
fn should_publish_column_rules() {
    let f = setup();
    let mcx = f.ctx.mcx();

    // dropped: never published
    assert!(!logicalrep_should_publish_column(
        &att_fixture(1, true, 0),
        None,
        PublishGencolsType::None
    ));

    // plain column, no list: published
    assert!(logicalrep_should_publish_column(
        &att_fixture(1, false, 0),
        None,
        PublishGencolsType::None
    ));

    // column list governs (even for generated columns)
    let columns = fixture_bms_add_member(mcx, None, 2).unwrap();
    assert!(!logicalrep_should_publish_column(
        &att_fixture(1, false, 0),
        Some(&columns),
        PublishGencolsType::None
    ));
    assert!(logicalrep_should_publish_column(
        &att_fixture(2, false, ATTRIBUTE_GENERATED_STORED),
        Some(&columns),
        PublishGencolsType::None
    ));

    // stored generated without list: only when Stored
    assert!(!logicalrep_should_publish_column(
        &att_fixture(1, false, ATTRIBUTE_GENERATED_STORED),
        None,
        PublishGencolsType::None
    ));
    assert!(logicalrep_should_publish_column(
        &att_fixture(1, false, ATTRIBUTE_GENERATED_STORED),
        None,
        PublishGencolsType::Stored
    ));

    // virtual generated: never published without a list
    assert!(!logicalrep_should_publish_column(
        &att_fixture(1, false, types_tuple::ATTRIBUTE_GENERATED_VIRTUAL),
        None,
        PublishGencolsType::Stored
    ));
}
