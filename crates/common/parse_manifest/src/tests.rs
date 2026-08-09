use std::sync::Once;

use manifest::{
    AddFileToBackupManifest, AddWALInfoToBackupManifest, BackupManifestInfo,
    InitializeBackupManifest, PgChecksumContext, PgChecksumType, SendBackupManifest,
    MANIFEST_OPTION_FORCE_ENCODE, MANIFEST_OPTION_YES, PG_CHECKSUM_MAX_LENGTH,
};
use mcx::MemoryContext;
use pg_sha2::PgSha256Ctx;

use crate::{
    c_strtoi64, c_strtou64, json_parse_manifest, parse_xlogrecptr, JsonManifestParseContext,
    ManifestWalRange, ParsedManifest,
};

const TEST_SYSID: u64 = 1234567890123456789;

fn install_seams() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        manifest::seams::get_system_identifier::set(|| TEST_SYSID);
        pgtz_seams::pg_open_tzfile::set(|_name, _canon, _buf| Ok(None));
    });
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Append the exact trailer the C server emits: the body (which must end
/// with a newline) is what the SHA-256 covers.
fn with_checksum(body: &str) -> Vec<u8> {
    let mut ctx = PgSha256Ctx::init_sha256();
    ctx.update(body.as_bytes());
    let digest = ctx.final_sha256();
    format!("{body}\"Manifest-Checksum\": \"{}\"}}\n", hex(&digest)).into_bytes()
}

fn parse(buffer: &[u8]) -> Result<ParsedManifest, String> {
    let ctx = MemoryContext::new("parse-manifest-test");
    ParsedManifest::parse(ctx.mcx(), buffer).map_err(|e| e.message().to_string())
}

fn expect_error(buffer: &[u8], expected: &str) {
    match parse(buffer) {
        Ok(_) => panic!("expected error {expected:?}, got successful parse"),
        Err(msg) => assert_eq!(msg, expected),
    }
}

// ---------------------------------------------------------------------------
// Real C-generated fixture (pg_basebackup from PostgreSQL 18.4, Homebrew;
// default --manifest-checksums=CRC32C; sysid captured from pg_controldata).
// ---------------------------------------------------------------------------

const C_FIXTURE: &[u8] = include_bytes!("../testdata/backup_manifest_pg18");
const C_FIXTURE_SYSID: u64 = 7671867332315642488;
const C_FIXTURE_NFILES: usize = 968; // grep -c '"Path"' over the fixture

#[test]
fn parses_real_c_manifest() {
    let m = parse(C_FIXTURE).unwrap();
    assert_eq!(m.version, 2);
    assert_eq!(m.system_identifier, Some(C_FIXTURE_SYSID));
    assert_eq!(m.files.len(), C_FIXTURE_NFILES);
    assert_eq!(
        m.wal_ranges,
        vec![ManifestWalRange { tli: 1, start_lsn: 0x2000028, end_lsn: 0x2000120 }]
    );

    // Every entry in a CRC32C-checksummed C manifest carries a 4-byte payload.
    for f in &m.files {
        assert_eq!(f.checksum_type, PgChecksumType::Crc32c, "{:?}", f.pathname);
        assert_eq!(f.checksum_payload.as_deref().map(<[u8]>::len), Some(4));
    }

    // Spot-check a known entry byte-for-byte:
    // { "Path": "PG_VERSION", "Size": 3, ..., "Checksum": "994e7be2" }
    let pv = m.files.iter().find(|f| f.pathname == b"PG_VERSION").unwrap();
    assert_eq!(pv.size, 3);
    assert_eq!(pv.checksum_payload.as_deref(), Some(&[0x99, 0x4e, 0x7b, 0xe2][..]));
}

#[test]
fn real_c_manifest_checksum_is_enforced() {
    // Flip one hex digit inside the trailing Manifest-Checksum.
    let mut corrupt = C_FIXTURE.to_vec();
    let pos = corrupt.len() - 4; // inside the checksum hex, before  "}\n
    corrupt[pos] = if corrupt[pos] == b'0' { b'1' } else { b'0' };
    expect_error(&corrupt, "manifest checksum mismatch");
}

// ---------------------------------------------------------------------------
// Round-trip against pgrust's own manifest emitter.
// ---------------------------------------------------------------------------

fn checksum_of(ty: PgChecksumType, data: &[u8]) -> Vec<u8> {
    let mut cc = PgChecksumContext::init(ty);
    cc.update(data);
    let mut buf = [0u8; PG_CHECKSUM_MAX_LENGTH];
    let n = cc.finalize(&mut buf);
    buf[..n].to_vec()
}

#[test]
fn roundtrips_emitter_manifest() {
    install_seams();
    let ctx = MemoryContext::new("emitter-roundtrip");
    let mcx = ctx.mcx();

    let mut m = BackupManifestInfo::zeroed();
    InitializeBackupManifest(mcx, &mut m, MANIFEST_OPTION_YES, PgChecksumType::Sha256).unwrap();

    let cases: &[(&[u8], &[u8], PgChecksumType)] = &[
        (b"backup_label", b"START WAL LOCATION\n", PgChecksumType::Crc32c),
        (b"base/1/1259", &[0u8; 128], PgChecksumType::Sha256),
        (b"PG_VERSION", b"18\n", PgChecksumType::None),
        (b"base/1/2619", b"abc", PgChecksumType::Sha224),
        (b"base/1/2620", b"abc", PgChecksumType::Sha384),
        (b"base/1/2621", b"abc", PgChecksumType::Sha512),
    ];
    for (path, data, ty) in cases {
        let mut cc = PgChecksumContext::init(*ty);
        cc.update(data);
        AddFileToBackupManifest(&mut m, 0, path, data.len() as i64, 1_700_000_000, &mut cc)
            .unwrap();
    }
    AddWALInfoToBackupManifest(mcx, &mut m, 0x016B3D50, 1, 0x016C0000, 1).unwrap();
    let bytes = SendBackupManifest(&mut m).unwrap().to_vec();

    let parsed = parse(&bytes).unwrap();
    assert_eq!(parsed.version, 2);
    assert_eq!(parsed.system_identifier, Some(TEST_SYSID));
    assert_eq!(parsed.files.len(), cases.len());
    for ((path, data, ty), f) in cases.iter().zip(&parsed.files) {
        assert_eq!(f.pathname, *path);
        assert_eq!(f.size, data.len() as u64);
        if *ty == PgChecksumType::None {
            // The emitter writes no Checksum-Algorithm/Checksum fields.
            assert_eq!(f.checksum_type, PgChecksumType::None);
            assert_eq!(f.checksum_payload, None);
        } else {
            assert_eq!(f.checksum_type, *ty);
            assert_eq!(f.checksum_payload.as_deref(), Some(&checksum_of(*ty, data)[..]));
        }
    }
    assert_eq!(
        parsed.wal_ranges,
        vec![ManifestWalRange { tli: 1, start_lsn: 0x016B3D50, end_lsn: 0x016C0000 }]
    );
}

#[test]
fn roundtrips_encoded_path() {
    install_seams();
    let ctx = MemoryContext::new("emitter-roundtrip-encoded");
    let mcx = ctx.mcx();

    let mut m = BackupManifestInfo::zeroed();
    InitializeBackupManifest(mcx, &mut m, MANIFEST_OPTION_FORCE_ENCODE, PgChecksumType::None)
        .unwrap();
    // Non-UTF8 path: always hex-encoded, and FORCE_ENCODE hex-encodes even
    // clean paths.
    let raw: &[u8] = b"base/1/bad\xff\x00name";
    let mut cc = PgChecksumContext::init(PgChecksumType::None);
    AddFileToBackupManifest(&mut m, 0, raw, 42, 1_700_000_000, &mut cc).unwrap();
    let mut cc = PgChecksumContext::init(PgChecksumType::None);
    AddFileToBackupManifest(&mut m, 0, b"PG_VERSION", 3, 1_700_000_000, &mut cc).unwrap();
    AddWALInfoToBackupManifest(mcx, &mut m, 0x0, 1, 0x100, 1).unwrap();
    let bytes = SendBackupManifest(&mut m).unwrap().to_vec();
    assert!(!bytes.windows(7).any(|w| w == b"\"Path\":"), "FORCE_ENCODE emits Encoded-Path only");

    let parsed = parse(&bytes).unwrap();
    assert_eq!(parsed.files[0].pathname, raw);
    assert_eq!(parsed.files[0].size, 42);
    assert_eq!(parsed.files[1].pathname, b"PG_VERSION");
}

// ---------------------------------------------------------------------------
// Version handling.
// ---------------------------------------------------------------------------

#[test]
fn accepts_version_1_manifest() {
    let body = concat!(
        "{ \"PostgreSQL-Backup-Manifest-Version\": 1,\n",
        "\"Files\": [\n",
        "{ \"Path\": \"PG_VERSION\", \"Size\": 3 }\n",
        "],\n",
    );
    let m = parse(&with_checksum(body)).unwrap();
    assert_eq!(m.version, 1);
    assert_eq!(m.system_identifier, None);
    assert_eq!(m.files.len(), 1);
    assert_eq!(m.files[0].checksum_type, PgChecksumType::None);
    assert_eq!(m.files[0].checksum_payload, None);
    assert!(m.wal_ranges.is_empty());
}

#[test]
fn version_int_truncation_matches_c() {
    // C parses the version with strtoi64 and assigns through `int`:
    // 4294967298 == 2 mod 2^32 is ACCEPTED as version 2. Pinned verbatim.
    let body = "{ \"PostgreSQL-Backup-Manifest-Version\": 4294967298,\n\"Files\": [],\n";
    let m = parse(&with_checksum(body)).unwrap();
    assert_eq!(m.version, 2);
}

#[test]
fn version_errors() {
    expect_error(
        br#"{"PostgreSQL-Backup-Manifest-Version": "bogus"}"#,
        "could not parse backup manifest: manifest version not an integer",
    );
    expect_error(
        br#"{"PostgreSQL-Backup-Manifest-Version": 3}"#,
        "could not parse backup manifest: unexpected manifest version",
    );
    // strtoi64 clamps on overflow (ERANGE unchecked in C) -> version check fails.
    expect_error(
        br#"{"PostgreSQL-Backup-Manifest-Version": 99999999999999999999999999}"#,
        "could not parse backup manifest: unexpected manifest version",
    );
    expect_error(
        br#"{"Files": []}"#,
        "could not parse backup manifest: expected version indicator",
    );
    // The version value must be a scalar.
    expect_error(
        br#"{"PostgreSQL-Backup-Manifest-Version": {}}"#,
        "could not parse backup manifest: unexpected object start",
    );
}

// ---------------------------------------------------------------------------
// Top-level structure errors.
// ---------------------------------------------------------------------------

#[test]
fn toplevel_errors() {
    expect_error(b"[]", "could not parse backup manifest: unexpected array start");
    expect_error(
        b"not json",
        "could not parse backup manifest: Token \"not\" is invalid.",
    );
    expect_error(
        b"",
        "could not parse backup manifest: The input string ended unexpectedly.",
    );
    expect_error(
        br#"{"PostgreSQL-Backup-Manifest-Version": 2, "Bogus": 1}"#,
        "could not parse backup manifest: unrecognized top-level field",
    );
    // Complete JSON object with no Manifest-Checksum field.
    expect_error(
        br#"{"PostgreSQL-Backup-Manifest-Version": 2}"#,
        "could not parse backup manifest: unexpected object end",
    );
    // Fields after Manifest-Checksum.
    expect_error(
        br#"{"PostgreSQL-Backup-Manifest-Version": 2, "Manifest-Checksum": "x", "More": 1}"#,
        "could not parse backup manifest: unexpected object field",
    );
    // "Files" value must be an array.
    expect_error(
        br#"{"PostgreSQL-Backup-Manifest-Version": 2, "Files": "nope"}"#,
        "could not parse backup manifest: unexpected scalar",
    );
    // Truncated manifest.
    expect_error(
        br#"{"PostgreSQL-Backup-Manifest-Version": 2, "Files": [{"Path": "x""#,
        "could not parse backup manifest: The input string ended unexpectedly.",
    );
    expect_error(
        br#"{"PostgreSQL-Backup-Manifest-Version": 2, "System-Identifier": "abc"}"#,
        "could not parse backup manifest: system identifier in manifest not an integer",
    );
}

// ---------------------------------------------------------------------------
// Per-file object errors.
// ---------------------------------------------------------------------------

fn files_manifest(file_obj: &str) -> Vec<u8> {
    format!(
        "{{\"PostgreSQL-Backup-Manifest-Version\": 2, \"Files\": [{file_obj}]}}"
    )
    .into_bytes()
}

#[test]
fn file_errors() {
    expect_error(
        &files_manifest(r#"{"Frob": 1}"#),
        "could not parse backup manifest: unexpected file field",
    );
    expect_error(
        &files_manifest(r#"{"Size": 2}"#),
        "could not parse backup manifest: missing path name",
    );
    expect_error(
        &files_manifest(r#"{"Path": "x", "Encoded-Path": "78", "Size": 2}"#),
        "could not parse backup manifest: both path name and encoded path name",
    );
    expect_error(
        &files_manifest(r#"{"Path": "x"}"#),
        "could not parse backup manifest: missing size",
    );
    expect_error(
        &files_manifest(r#"{"Path": "x", "Size": 2, "Checksum": "00"}"#),
        "could not parse backup manifest: checksum without algorithm",
    );
    // Odd-length hex.
    expect_error(
        &files_manifest(r#"{"Encoded-Path": "123", "Size": 2}"#),
        "could not parse backup manifest: could not decode file name",
    );
    // Non-hex characters.
    expect_error(
        &files_manifest(r#"{"Encoded-Path": "zz", "Size": 2}"#),
        "could not parse backup manifest: could not decode file name",
    );
    expect_error(
        &files_manifest(r#"{"Path": "x", "Size": "3x"}"#),
        "could not parse backup manifest: file size is not an integer",
    );
    expect_error(
        &files_manifest(r#"{"Path": "x", "Size": 2, "Checksum-Algorithm": "MD5", "Checksum": "00"}"#),
        "unrecognized checksum algorithm: \"MD5\"",
    );
    expect_error(
        &files_manifest(r#"{"Path": "x", "Size": 2, "Checksum-Algorithm": "CRC32C", "Checksum": "0g"}"#),
        "invalid checksum for file \"x\": \"0g\"",
    );
}

#[test]
fn checksum_algorithms_parse_case_insensitively() {
    let body = concat!(
        "{ \"PostgreSQL-Backup-Manifest-Version\": 2,\n",
        "\"Files\": [\n",
        "{ \"Path\": \"a\", \"Size\": 1, \"Checksum-Algorithm\": \"sha256\", \"Checksum\": \"00ff\" },\n",
        "{ \"Path\": \"b\", \"Size\": 1, \"Checksum-Algorithm\": \"NONE\" },\n",
        "{ \"Path\": \"c\", \"Size\": 1, \"Checksum-Algorithm\": \"CRC32C\", \"Checksum\": \"\" }\n",
        "],\n",
    );
    let m = parse(&with_checksum(body)).unwrap();
    assert_eq!(m.files[0].checksum_type, PgChecksumType::Sha256);
    // The parser does not length-check payload vs algorithm (C defers that
    // to consumers such as pg_verifybackup).
    assert_eq!(m.files[0].checksum_payload.as_deref(), Some(&[0x00, 0xff][..]));
    assert_eq!(m.files[1].checksum_type, PgChecksumType::None);
    assert_eq!(m.files[1].checksum_payload, None);
    // Empty Checksum string == no payload, like C's zero-length case.
    assert_eq!(m.files[2].checksum_type, PgChecksumType::Crc32c);
    assert_eq!(m.files[2].checksum_payload, None);
}

#[test]
fn file_size_strtou64_quirks_match_c() {
    // glibc strtou64 accepts a minus sign and wraps modulo 2^64.
    let body = "{ \"PostgreSQL-Backup-Manifest-Version\": 2,\n\"Files\": [\n{ \"Path\": \"x\", \"Size\": \"-1\" }\n],\n";
    let m = parse(&with_checksum(body)).unwrap();
    assert_eq!(m.files[0].size, u64::MAX);

    // Leading whitespace is skipped; trailing whitespace is *ep != 0.
    let body = "{ \"PostgreSQL-Backup-Manifest-Version\": 2,\n\"Files\": [\n{ \"Path\": \"x\", \"Size\": \" 12\" }\n],\n";
    let m = parse(&with_checksum(body)).unwrap();
    assert_eq!(m.files[0].size, 12);
    expect_error(
        &files_manifest(r#"{"Path": "x", "Size": "12 "}"#),
        "could not parse backup manifest: file size is not an integer",
    );
}

// ---------------------------------------------------------------------------
// WAL-range object errors.
// ---------------------------------------------------------------------------

fn wal_manifest(range_obj: &str) -> Vec<u8> {
    format!(
        "{{\"PostgreSQL-Backup-Manifest-Version\": 2, \"WAL-Ranges\": [{range_obj}]}}"
    )
    .into_bytes()
}

#[test]
fn wal_range_errors() {
    expect_error(
        &wal_manifest(r#"{"Bogus": 1}"#),
        "could not parse backup manifest: unexpected WAL range field",
    );
    expect_error(
        &wal_manifest(r#"{"Start-LSN": "0/0", "End-LSN": "0/0"}"#),
        "could not parse backup manifest: missing timeline",
    );
    expect_error(
        &wal_manifest(r#"{"Timeline": 1, "End-LSN": "0/0"}"#),
        "could not parse backup manifest: missing start LSN",
    );
    expect_error(
        &wal_manifest(r#"{"Timeline": 1, "Start-LSN": "0/0"}"#),
        "could not parse backup manifest: missing end LSN",
    );
    expect_error(
        &wal_manifest(r#"{"Timeline": "one", "Start-LSN": "0/0", "End-LSN": "0/0"}"#),
        "could not parse backup manifest: timeline is not an integer",
    );
    expect_error(
        &wal_manifest(r#"{"Timeline": 1, "Start-LSN": "garbage", "End-LSN": "0/0"}"#),
        "could not parse backup manifest: could not parse start LSN",
    );
    expect_error(
        &wal_manifest(r#"{"Timeline": 1, "Start-LSN": "0/0", "End-LSN": "16B3D50"}"#),
        "could not parse backup manifest: could not parse end LSN",
    );
}

// ---------------------------------------------------------------------------
// Manifest-Checksum trailer verification.
// ---------------------------------------------------------------------------

#[test]
fn manifest_checksum_errors() {
    // No newlines at all.
    expect_error(
        br#"{ "PostgreSQL-Backup-Manifest-Version": 2, "Files": [], "Manifest-Checksum": "00"}"#,
        "could not parse backup manifest: expected at least 2 lines",
    );
    // Two newlines, but the last line is not newline-terminated.
    expect_error(
        b"{ \"PostgreSQL-Backup-Manifest-Version\": 2,\n\"Files\": [],\n\"Manifest-Checksum\": \"00\"}",
        "could not parse backup manifest: last line not newline-terminated",
    );
    // Wrong length checksum string.
    expect_error(
        b"{ \"PostgreSQL-Backup-Manifest-Version\": 2,\n\"Files\": [],\n\"Manifest-Checksum\": \"00\"}\n",
        "invalid manifest checksum: \"00\"",
    );
    // Right length, non-hex contents.
    let bad = "z".repeat(64);
    expect_error(
        format!(
            "{{ \"PostgreSQL-Backup-Manifest-Version\": 2,\n\"Files\": [],\n\"Manifest-Checksum\": \"{bad}\"}}\n"
        )
        .as_bytes(),
        &format!("invalid manifest checksum: \"{bad}\""),
    );
    // Well-formed but wrong value.
    let wrong = "0".repeat(64);
    expect_error(
        format!(
            "{{ \"PostgreSQL-Backup-Manifest-Version\": 2,\n\"Files\": [],\n\"Manifest-Checksum\": \"{wrong}\"}}\n"
        )
        .as_bytes(),
        "manifest checksum mismatch",
    );
}

#[test]
fn callback_errors_propagate() {
    // A failing consumer callback aborts the parse with the consumer's error.
    struct Failing;
    impl JsonManifestParseContext for Failing {
        fn version_cb(&mut self, _v: i32) -> types_error::PgResult<()> {
            Err(types_error::PgError::error("consumer rejected version").into())
        }
        fn system_identifier_cb(&mut self, _s: u64) -> types_error::PgResult<()> {
            Ok(())
        }
        fn per_file_cb(
            &mut self,
            _p: &[u8],
            _s: u64,
            _t: PgChecksumType,
            _c: Option<&[u8]>,
        ) -> types_error::PgResult<()> {
            Ok(())
        }
        fn per_wal_range_cb(
            &mut self,
            _t: types_core::TimeLineID,
            _s: types_core::XLogRecPtr,
            _e: types_core::XLogRecPtr,
        ) -> types_error::PgResult<()> {
            Ok(())
        }
    }
    let ctx = MemoryContext::new("cb-error");
    let err = json_parse_manifest(ctx.mcx(), &mut Failing, C_FIXTURE).unwrap_err();
    assert_eq!(err.message(), "consumer rejected version");
}

// ---------------------------------------------------------------------------
// C-library helper parity (strtoi64 / strtou64 / sscanf %X/%X).
// ---------------------------------------------------------------------------

#[test]
fn strto_helpers_match_glibc() {
    assert_eq!(c_strtoi64(b"2"), (2, 1));
    assert_eq!(c_strtoi64(b""), (0, 0)); // no conversion; *ep == NUL passes in C
    assert_eq!(c_strtoi64(b" +42"), (42, 4));
    assert_eq!(c_strtoi64(b"-9223372036854775808"), (i64::MIN, 20));
    assert_eq!(c_strtoi64(b"9223372036854775808"), (i64::MAX, 19)); // ERANGE clamp
    assert_eq!(c_strtoi64(b"99999999999999999999999"), (i64::MAX, 23));
    assert_eq!(c_strtoi64(b"12x").1, 2); // endptr short of the end

    assert_eq!(c_strtou64(b"18446744073709551615"), (u64::MAX, 20));
    assert_eq!(c_strtou64(b"18446744073709551616"), (u64::MAX, 20)); // ERANGE clamp
    assert_eq!(c_strtou64(b"-1"), (u64::MAX, 2)); // unsigned negation
    assert_eq!(c_strtou64(b"abc"), (0, 0));
}

#[test]
fn parse_xlogrecptr_matches_sscanf() {
    assert_eq!(parse_xlogrecptr(b"0/16B3D50"), Some(0x016B_3D50));
    assert_eq!(parse_xlogrecptr(b"12/AB"), Some(0x12_0000_00AB));
    assert_eq!(parse_xlogrecptr(b"FFFFFFFF/FFFFFFFF"), Some(u64::MAX));
    // sscanf %X accepts 0x prefixes and leading whitespace...
    assert_eq!(parse_xlogrecptr(b"0x10/0"), Some(0x10_0000_0000));
    assert_eq!(parse_xlogrecptr(b" 1/0"), Some(1 << 32));
    // ...matches the literal '/' exactly (no whitespace skip)...
    assert_eq!(parse_xlogrecptr(b"1 /0"), None);
    // ...and ignores trailing garbage after the second conversion.
    assert_eq!(parse_xlogrecptr(b"1/0garbage"), Some(1 << 32));
    assert_eq!(parse_xlogrecptr(b"garbage"), None);
    assert_eq!(parse_xlogrecptr(b"1/"), None);
    assert_eq!(parse_xlogrecptr(b"/1"), None);
    assert_eq!(parse_xlogrecptr(b"16B3D50"), None); // no slash at all
}

#[test]
fn timeline_truncates_like_c_uint32_assignment() {
    // C: strtoul into a uint32 TLI truncates 2^32+5 to 5.
    let body = "{ \"PostgreSQL-Backup-Manifest-Version\": 2,\n\"WAL-Ranges\": [\n{ \"Timeline\": 4294967301, \"Start-LSN\": \"0/0\", \"End-LSN\": \"0/0\" }\n],\n";
    let m = parse(&with_checksum(body)).unwrap();
    assert_eq!(m.wal_ranges[0].tli, 5);
}
