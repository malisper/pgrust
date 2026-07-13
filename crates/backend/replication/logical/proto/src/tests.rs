// Wire-format roundtrips for the catalog-free message types, plus read-side
// checks against hand-encoded byte streams for the tuple/rel payloads (the
// write side of those needs relcache/syscache and is exercised end-to-end by
// the pgoutput e2e).
use super::*;

#[test]
fn begin_roundtrip() {
    let mut out = Vec::new();
    logicalrep_write_begin(&mut out, 0x0102030405060708, 987654321, 42);
    assert_eq!(out[0], LOGICAL_REP_MSG_BEGIN);
    let mut r = Reader::new(&out[1..]);
    let b = logicalrep_read_begin(&mut r).unwrap();
    assert_eq!(b.final_lsn, 0x0102030405060708);
    assert_eq!(b.committime, 987654321);
    assert_eq!(b.xid, 42);
}

#[test]
fn begin_rejects_invalid_lsn() {
    let mut out = Vec::new();
    logicalrep_write_begin(&mut out, InvalidXLogRecPtr, 0, 1);
    let mut r = Reader::new(&out[1..]);
    assert!(logicalrep_read_begin(&mut r).is_err());
}

#[test]
fn commit_roundtrip() {
    let mut out = Vec::new();
    logicalrep_write_commit(&mut out, 0xDEAD, 0xBEEF, -5);
    assert_eq!(out[0], LOGICAL_REP_MSG_COMMIT);
    let mut r = Reader::new(&out[1..]);
    let c = logicalrep_read_commit(&mut r).unwrap();
    assert_eq!(c.commit_lsn, 0xDEAD);
    assert_eq!(c.end_lsn, 0xBEEF);
    assert_eq!(c.committime, -5);
}

#[test]
fn commit_rejects_bad_flags() {
    let mut out = Vec::new();
    logicalrep_write_commit(&mut out, 1, 2, 3);
    out[1] = 7; // corrupt flags
    let mut r = Reader::new(&out[1..]);
    assert!(logicalrep_read_commit(&mut r).is_err());
}

#[test]
fn origin_roundtrip() {
    let mut out = Vec::new();
    logicalrep_write_origin(&mut out, "some_origin", 0xAB);
    assert_eq!(out[0], LOGICAL_REP_MSG_ORIGIN);
    let mut r = Reader::new(&out[1..]);
    let (name, lsn) = logicalrep_read_origin(&mut r).unwrap();
    assert_eq!(name, "some_origin");
    assert_eq!(lsn, 0xAB);
}

#[test]
fn truncate_roundtrip() {
    let mut out = Vec::new();
    logicalrep_write_truncate(&mut out, InvalidTransactionId, &[16384, 16385], true, false);
    assert_eq!(out[0], LOGICAL_REP_MSG_TRUNCATE);
    let mut r = Reader::new(&out[1..]);
    let (relids, cascade, restart) = logicalrep_read_truncate(&mut r).unwrap();
    assert_eq!(relids, vec![16384, 16385]);
    assert!(cascade);
    assert!(!restart);
}

#[test]
fn message_wire_layout() {
    let mut out = Vec::new();
    logicalrep_write_message(&mut out, InvalidTransactionId, 0x10, true, "pfx", b"payload");
    assert_eq!(out[0], LOGICAL_REP_MSG_MESSAGE);
    let mut r = Reader::new(&out[1..]);
    assert_eq!(r.get_byte().unwrap(), MESSAGE_TRANSACTIONAL);
    assert_eq!(r.get_int64().unwrap(), 0x10);
    assert_eq!(r.get_string().unwrap(), "pfx");
    let len = r.get_int32().unwrap() as usize;
    assert_eq!(r.get_bytes(len).unwrap(), b"payload");
}

// Hand-encode a tuple payload exactly as logicalrep_write_tuple would and
// check the reader: 3 columns — text, null, binary.
#[test]
fn tuple_read_side() {
    let mut buf = Vec::new();
    buf.extend_from_slice(&3u16.to_be_bytes());
    buf.push(LOGICALREP_COLUMN_TEXT);
    buf.extend_from_slice(&2u32.to_be_bytes());
    buf.extend_from_slice(b"42");
    buf.push(LOGICALREP_COLUMN_NULL);
    buf.push(LOGICALREP_COLUMN_BINARY);
    buf.extend_from_slice(&4u32.to_be_bytes());
    buf.extend_from_slice(&[0, 0, 0, 7]);

    let mut r = Reader::new(&buf);
    let t = logicalrep_read_tuple(&mut r).unwrap();
    assert_eq!(t.ncols, 3);
    assert_eq!(t.colstatus, vec![b't', b'n', b'b']);
    assert_eq!(t.colvalues[0].as_deref(), Some(b"42".as_slice()));
    assert_eq!(t.colvalues[1], None);
    assert_eq!(t.colvalues[2].as_deref(), Some([0u8, 0, 0, 7].as_slice()));
}

// Hand-encode an INSERT body (relid + 'N' + tuple) and read it.
#[test]
fn insert_read_side() {
    let mut buf = Vec::new();
    buf.extend_from_slice(&16390u32.to_be_bytes());
    buf.push(b'N');
    buf.extend_from_slice(&1u16.to_be_bytes());
    buf.push(LOGICALREP_COLUMN_TEXT);
    buf.extend_from_slice(&1u32.to_be_bytes());
    buf.push(b'x');

    let mut r = Reader::new(&buf);
    let (relid, tup) = logicalrep_read_insert(&mut r).unwrap();
    assert_eq!(relid, 16390);
    assert_eq!(tup.ncols, 1);
    assert_eq!(tup.colvalues[0].as_deref(), Some(b"x".as_slice()));
}

// Hand-encode an UPDATE with an old key ('K') and a new tuple.
#[test]
fn update_read_side() {
    let mut buf = Vec::new();
    buf.extend_from_slice(&16391u32.to_be_bytes());
    buf.push(b'K');
    buf.extend_from_slice(&1u16.to_be_bytes());
    buf.push(LOGICALREP_COLUMN_TEXT);
    buf.extend_from_slice(&1u32.to_be_bytes());
    buf.push(b'1');
    buf.push(b'N');
    buf.extend_from_slice(&1u16.to_be_bytes());
    buf.push(LOGICALREP_COLUMN_TEXT);
    buf.extend_from_slice(&1u32.to_be_bytes());
    buf.push(b'2');

    let mut r = Reader::new(&buf);
    let u = logicalrep_read_update(&mut r).unwrap();
    assert_eq!(u.relid, 16391);
    assert!(u.has_oldtuple);
    assert_eq!(u.oldtup.unwrap().colvalues[0].as_deref(), Some(b"1".as_slice()));
    assert_eq!(u.newtup.colvalues[0].as_deref(), Some(b"2".as_slice()));
}

// Hand-encode a DELETE with an 'O' (full) old tuple.
#[test]
fn delete_read_side() {
    let mut buf = Vec::new();
    buf.extend_from_slice(&16392u32.to_be_bytes());
    buf.push(b'O');
    buf.extend_from_slice(&1u16.to_be_bytes());
    buf.push(LOGICALREP_COLUMN_NULL);

    let mut r = Reader::new(&buf);
    let (relid, tup) = logicalrep_read_delete(&mut r).unwrap();
    assert_eq!(relid, 16392);
    assert_eq!(tup.colstatus, vec![LOGICALREP_COLUMN_NULL]);
}

// Hand-encode a RELATION message body per logicalrep_write_rel's layout:
// relid, namespace (empty = pg_catalog), relname, replident, attrs.
#[test]
fn rel_read_side() {
    let mut buf = Vec::new();
    buf.extend_from_slice(&16393u32.to_be_bytes());
    buf.extend_from_slice(b"public\0");
    buf.extend_from_slice(b"tab\0");
    buf.push(b'd');
    buf.extend_from_slice(&2u16.to_be_bytes());
    // col 1: replica-identity member
    buf.push(LOGICALREP_IS_REPLICA_IDENTITY);
    buf.extend_from_slice(b"id\0");
    buf.extend_from_slice(&23u32.to_be_bytes()); // int4
    buf.extend_from_slice(&(-1i32 as u32).to_be_bytes());
    // col 2
    buf.push(0);
    buf.extend_from_slice(b"payload\0");
    buf.extend_from_slice(&25u32.to_be_bytes()); // text
    buf.extend_from_slice(&(-1i32 as u32).to_be_bytes());

    let mut r = Reader::new(&buf);
    let rel = logicalrep_read_rel(&mut r).unwrap();
    assert_eq!(rel.remoteid, 16393);
    assert_eq!(rel.nspname, "public");
    assert_eq!(rel.relname, "tab");
    assert_eq!(rel.replident, b'd');
    assert_eq!(rel.natts, 2);
    assert_eq!(rel.attnames, vec!["id".to_string(), "payload".to_string()]);
    assert_eq!(rel.atttyps, vec![23, 25]);
    assert_eq!(rel.attkeys, vec![true, false]);
}

// Namespace empty-string convention.
#[test]
fn rel_read_pg_catalog_namespace() {
    let mut buf = Vec::new();
    buf.extend_from_slice(&1259u32.to_be_bytes());
    buf.push(0); // empty namespace => pg_catalog
    buf.extend_from_slice(b"pg_class\0");
    buf.push(b'n');
    buf.extend_from_slice(&0u16.to_be_bytes());

    let mut r = Reader::new(&buf);
    let rel = logicalrep_read_rel(&mut r).unwrap();
    assert_eq!(rel.nspname, "pg_catalog");
    assert_eq!(rel.natts, 0);
}

// TYPE message read side.
#[test]
fn typ_read_side() {
    let mut buf = Vec::new();
    buf.extend_from_slice(&100001u32.to_be_bytes());
    buf.extend_from_slice(b"myschema\0");
    buf.extend_from_slice(b"mytype\0");

    let mut r = Reader::new(&buf);
    let t = logicalrep_read_typ(&mut r).unwrap();
    assert_eq!(t.remoteid, 100001);
    assert_eq!(t.nspname, "myschema");
    assert_eq!(t.typname, "mytype");
}

#[test]
fn message_type_names() {
    assert_eq!(logicalrep_message_type(LOGICAL_REP_MSG_INSERT), "INSERT");
    assert_eq!(logicalrep_message_type(LOGICAL_REP_MSG_STREAM_PREPARE), "STREAM PREPARE");
    assert_eq!(logicalrep_message_type(0xFF), "??? (255)");
}

#[test]
fn truncated_message_errors_not_panics() {
    let buf = [0u8; 3];
    let mut r = Reader::new(&buf);
    assert!(r.get_int64().is_err());
    let mut r2 = Reader::new(b"no-nul-terminator");
    assert!(r2.get_string().is_err());
}
