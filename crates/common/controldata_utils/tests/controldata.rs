//! KATs against a real pg_control produced by C PostgreSQL 18.3 initdb
//! (tests/data/pg_control; see the crate notes for regeneration).

use controldata_utils::*;

fn fixture_path() -> String {
    format!("{}/tests/data/pg_control", env!("CARGO_MANIFEST_DIR"))
}

fn fixture_bytes() -> Vec<u8> {
    std::fs::read(fixture_path()).unwrap()
}

#[test]
fn fixture_is_a_full_control_file() {
    assert_eq!(fixture_bytes().len(), PG_CONTROL_FILE_SIZE);
}

#[test]
fn reads_real_initdb_control_file() {
    let (cf, crc_ok) = get_controlfile_by_exact_path(&fixture_path()).unwrap();
    assert!(crc_ok);
    assert_eq!(cf.pg_control_version, PG_CONTROL_VERSION);
    assert_eq!(cf.catalog_version_no, CATALOG_VERSION_NO);
    assert_ne!(cf.system_identifier, 0);
    assert_eq!(cf.state, DB_SHUTDOWNED);
    assert!(cf.time > 0);
    assert_eq!(cf.maxAlign, 8);
    assert_eq!(cf.floatFormat, 1234567.0);
    assert_eq!(cf.blcksz, 8192);
    assert_eq!(cf.relseg_size, 131072);
    assert_eq!(cf.xlog_blcksz, 8192);
    assert_eq!(cf.xlog_seg_size, 16 * 1024 * 1024);
    assert_eq!(cf.nameDataLen, 64);
    assert_eq!(cf.indexMaxKeys, 32);
    assert_eq!(cf.toast_max_chunk_size, 1996);
    assert_eq!(cf.loblksize, 2048);
    assert!(cf.float8ByVal);
    assert_eq!(cf.MaxConnections, 100);
    assert_eq!(cf.checkPointCopy.ThisTimeLineID, 1);
    assert!(cf.checkPointCopy.fullPageWrites);
    assert_ne!(cf.checkPoint, 0);
    assert_eq!(cf.checkPointCopy.oldestXidDB, 1);
    assert!(cf.mock_authentication_nonce.iter().any(|&b| b != 0));
}

#[test]
fn get_controlfile_joins_datadir_path() {
    let dir = std::env::temp_dir().join(format!("cdu_get_{}", std::process::id()));
    std::fs::create_dir_all(dir.join("global")).unwrap();
    std::fs::write(dir.join("global/pg_control"), fixture_bytes()).unwrap();
    let (cf, crc_ok) = get_controlfile(dir.to_str().unwrap()).unwrap();
    assert!(crc_ok);
    assert_eq!(cf.pg_control_version, PG_CONTROL_VERSION);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn encode_is_byte_identical_to_c_image() {
    let bytes = fixture_bytes();
    let cf = ControlFileData::from_disk_bytes(&bytes);
    assert_eq!(cf.to_disk_bytes()[..], bytes[..SIZEOF_CONTROL_FILE_DATA]);
    assert_eq!(crc_of_image(&bytes), cf.crc);
}

#[test]
fn corrupt_byte_reports_crc_mismatch_without_raising() {
    let mut bytes = fixture_bytes();
    bytes[100] ^= 0xFF;
    let dir = std::env::temp_dir().join(format!("cdu_crc_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let p = dir.join("pg_control");
    std::fs::write(&p, &bytes).unwrap();
    let (_cf, crc_ok) = get_controlfile_by_exact_path(p.to_str().unwrap()).unwrap();
    assert!(!crc_ok);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn short_file_is_data_corrupted_error() {
    let dir = std::env::temp_dir().join(format!("cdu_short_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let p = dir.join("pg_control");
    std::fs::write(&p, &fixture_bytes()[..100]).unwrap();
    let err = get_controlfile_by_exact_path(p.to_str().unwrap()).unwrap_err();
    let msg = format!("{err:?}");
    assert!(msg.contains("read 100 of 296"), "{msg}");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn missing_file_is_open_error() {
    let err = get_controlfile_by_exact_path("/nonexistent/pg_control").unwrap_err();
    assert!(format!("{err:?}").contains("could not open file"));
}

#[test]
fn wrong_endian_version_is_byte_ordering_mismatch() {
    let mut bytes = fixture_bytes();
    let off = 8; // pg_control_version
    let v = u32::from_ne_bytes(bytes[off..off + 4].try_into().unwrap());
    bytes[off..off + 4].copy_from_slice(&v.swap_bytes().to_ne_bytes());
    let dir = std::env::temp_dir().join(format!("cdu_bo_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let p = dir.join("pg_control");
    std::fs::write(&p, &bytes).unwrap();
    let err = get_controlfile_by_exact_path(p.to_str().unwrap()).unwrap_err();
    assert!(format!("{err:?}").contains("byte ordering mismatch"));
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn update_controlfile_roundtrips_and_zero_pads() {
    let dir = std::env::temp_dir().join(format!("cdu_upd_{}", std::process::id()));
    std::fs::create_dir_all(dir.join("global")).unwrap();
    std::fs::write(dir.join("global/pg_control"), fixture_bytes()).unwrap();
    let datadir = dir.to_str().unwrap();

    let (mut cf, crc_ok) = get_controlfile(datadir).unwrap();
    assert!(crc_ok);
    let old_crc = cf.crc;

    cf.state = DB_IN_PRODUCTION;
    cf.checkPointCopy.nextOid = 99999;
    update_controlfile(datadir, &mut cf, true).unwrap();
    assert_ne!(cf.crc, old_crc);

    let written = std::fs::read(dir.join("global/pg_control")).unwrap();
    assert_eq!(written.len(), PG_CONTROL_FILE_SIZE);
    assert!(written[SIZEOF_CONTROL_FILE_DATA..].iter().all(|&b| b == 0));

    let (reread, crc_ok) = get_controlfile(datadir).unwrap();
    assert!(crc_ok);
    assert_eq!(reread, cf);
    assert_eq!(reread.state, DB_IN_PRODUCTION);
    assert_eq!(reread.checkPointCopy.nextOid, 99999);
    assert!(reread.time > 0);

    std::fs::remove_dir_all(&dir).unwrap();
}
