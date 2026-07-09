use std::sync::Once;

use mcx::MemoryContext;

use crate::checksum::{PgChecksumContext, PgChecksumType};
use crate::*;

const TEST_SYSID: u64 = 1234567890123456789;

fn install_seams() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        seams::get_system_identifier::set(|| TEST_SYSID);
        pgtz_seams::pg_open_tzfile::set(|_name, _canon, _buf| Ok(None));
    });
}

// SHA-256 of BODY computed by an external tool (Python hashlib): the checksum oracle.
const BODY: &str = concat!(
    "{ \"PostgreSQL-Backup-Manifest-Version\": 2,\n",
    "\"System-Identifier\": 1234567890123456789,\n",
    "\"Files\": [\n",
    "{ \"Path\": \"base/1/1259\", \"Size\": 8192, ",
    "\"Last-Modified\": \"2023-11-14 22:13:20 GMT\" }\n",
    "],\n",
    "\"WAL-Ranges\": [\n",
    "{ \"Timeline\": 1, \"Start-LSN\": \"0/16B3D50\", \"End-LSN\": \"0/16C0000\" }\n",
    "],\n",
);
const BODY_SHA256: &str = "74a12417758631cb0a52648f9d24d5c6439b569874d2937d77a096549133a95f";

#[test]
fn full_manifest_bytes_and_checksum() {
    install_seams();
    let ctx = MemoryContext::new("manifest-test");
    let mcx = ctx.mcx();

    let mut m = BackupManifestInfo::zeroed();
    InitializeBackupManifest(mcx, &mut m, MANIFEST_OPTION_YES, PgChecksumType::None).unwrap();

    let mut cc = PgChecksumContext::init(PgChecksumType::None);
    AddFileToBackupManifest(&mut m, 0, b"base/1/1259", 8192, 1_700_000_000, &mut cc).unwrap();

    AddWALInfoToBackupManifest(mcx, &mut m, 0x016B3D50, 1, 0x016C0000, 1).unwrap();

    let bytes = SendBackupManifest(&mut m).unwrap().to_vec();

    let expected = format!("{BODY}\"Manifest-Checksum\": \"{BODY_SHA256}\"}}\n");
    assert_eq!(
        std::str::from_utf8(&bytes).unwrap(),
        expected,
        "manifest bytes drifted from C format"
    );
}

#[test]
fn crc32c_known_vector() {
    let mut cc = PgChecksumContext::init(PgChecksumType::Crc32c);
    cc.update(b"123456789");
    let mut out = [0u8; PG_CHECKSUM_MAX_LENGTH];
    let n = cc.finalize(&mut out);
    assert_eq!(n, 4);
    assert_eq!(out[..4], 0xE306_9283u32.to_le_bytes());
}

#[test]
fn crc32c_is_default_and_renders() {
    install_seams();
    let ctx = MemoryContext::new("manifest-test");
    let mcx = ctx.mcx();

    let mut m = BackupManifestInfo::zeroed();
    InitializeBackupManifest(mcx, &mut m, MANIFEST_OPTION_YES, PgChecksumType::Crc32c).unwrap();

    let mut cc = PgChecksumContext::init(PgChecksumType::Crc32c);
    cc.update(b"123456789");
    AddFileToBackupManifest(&mut m, 0, b"global/pg_control", 8192, 1_700_000_000, &mut cc).unwrap();
    AddWALInfoToBackupManifest(mcx, &mut m, 0x016B3D50, 1, 0x016C0000, 1).unwrap();
    let bytes = SendBackupManifest(&mut m).unwrap().to_vec();
    let text = std::str::from_utf8(&bytes).unwrap();

    assert!(
        text.contains("\"Checksum-Algorithm\": \"CRC32C\", \"Checksum\": \"839206e3\""),
        "checksum field wrong: {text}"
    );
}

#[test]
fn tablespace_path_prefix() {
    install_seams();
    let ctx = MemoryContext::new("manifest-test");
    let mcx = ctx.mcx();
    let mut m = BackupManifestInfo::zeroed();
    InitializeBackupManifest(mcx, &mut m, MANIFEST_OPTION_YES, PgChecksumType::None).unwrap();
    let mut cc = PgChecksumContext::init(PgChecksumType::None);
    AddFileToBackupManifest(&mut m, 16400, b"16401/12345", 100, 1_700_000_000, &mut cc).unwrap();
    let bytes = SendBackupManifest(&mut m).unwrap().to_vec();
    let text = String::from_utf8(bytes).unwrap();
    assert!(text.contains("\"Path\": \"pg_tblspc/16400/16401/12345\""), "{text}");
}

#[test]
fn encoded_path_for_invalid_utf8() {
    install_seams();
    let ctx = MemoryContext::new("manifest-test");
    let mcx = ctx.mcx();
    let mut m = BackupManifestInfo::zeroed();
    InitializeBackupManifest(mcx, &mut m, MANIFEST_OPTION_YES, PgChecksumType::None).unwrap();
    let mut cc = PgChecksumContext::init(PgChecksumType::None);
    AddFileToBackupManifest(&mut m, 0, b"ab\xFF", 1, 1_700_000_000, &mut cc).unwrap();
    let text = String::from_utf8(SendBackupManifest(&mut m).unwrap().to_vec()).unwrap();
    assert!(text.contains("\"Encoded-Path\": \"6162ff\""), "{text}");
}

#[test]
fn disabled_manifest_is_empty() {
    install_seams();
    let ctx = MemoryContext::new("manifest-test");
    let mcx = ctx.mcx();
    let mut m = BackupManifestInfo::zeroed();
    InitializeBackupManifest(mcx, &mut m, MANIFEST_OPTION_NO, PgChecksumType::None).unwrap();
    let mut cc = PgChecksumContext::init(PgChecksumType::None);
    AddFileToBackupManifest(&mut m, 0, b"base/1/1", 1, 1, &mut cc).unwrap();
    assert!(SendBackupManifest(&mut m).unwrap().is_empty());
}
