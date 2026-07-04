use crate::ondisk::*;
use crate::ReplicationSlotValidateNameInternal;
use types_error::{ERRCODE_INVALID_NAME, ERRCODE_NAME_TOO_LONG};

#[test]
fn validate_name_ok() {
    assert!(ReplicationSlotValidateNameInternal("my_slot_01").is_ok());
    assert!(ReplicationSlotValidateNameInternal(&"a".repeat(63)).is_ok());
}

#[test]
fn validate_name_too_short() {
    let (code, msg, hint) = ReplicationSlotValidateNameInternal("").unwrap_err();
    assert_eq!(code, ERRCODE_INVALID_NAME);
    assert_eq!(msg, "replication slot name \"\" is too short");
    assert!(hint.is_none());
}

#[test]
fn validate_name_too_long() {
    let name = "a".repeat(64);
    let (code, _, hint) = ReplicationSlotValidateNameInternal(&name).unwrap_err();
    assert_eq!(code, ERRCODE_NAME_TOO_LONG);
    assert!(hint.is_none());
}

#[test]
fn validate_name_bad_chars() {
    for bad in ["Slot", "slot-1", "slot 1", "slot.1", "sløt"] {
        let (code, msg, hint) = ReplicationSlotValidateNameInternal(bad).unwrap_err();
        assert_eq!(code, ERRCODE_INVALID_NAME);
        assert_eq!(
            msg,
            format!("replication slot name \"{bad}\" contains invalid character")
        );
        assert_eq!(
            hint.as_deref(),
            Some(
                "Replication slot names may only contain lower case letters, numbers, and the \
                 underscore character."
            )
        );
    }
}

fn sample_data() -> ReplicationSlotPersistentData {
    let mut d = ReplicationSlotPersistentData::default();
    d.name.namestrcpy("kat_slot");
    d.restart_lsn = 0x0102030405060708;
    d.confirmed_flush = 0x1122334455667788;
    d
}

// Known-answer image independently computed with a table-based CRC32C over
// the C struct layout (magic 0x1051CA1, version 5, length 184).
const KAT_HEX: &str = "a11c0501ba08c4f805000000b8000000\
6b61745f736c6f740000000000000000\
00000000000000000000000000000000\
00000000000000000000000000000000\
00000000000000000000000000000000\
00000000000000000000000000000000\
08070605040302010000000000000000\
88776655443322110000000000000000\
00000000000000000000000000000000\
00000000000000000000000000000000\
00000000000000000000000000000000\
00000000000000000000000000000000\
0000000000000000";

fn kat_bytes() -> Vec<u8> {
    (0..KAT_HEX.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&KAT_HEX[i..i + 2], 16).unwrap())
        .collect()
}

#[test]
fn on_disk_known_answer() {
    let image = serialize_state_file(&sample_data());
    assert_eq!(image.len(), 200);
    assert_eq!(header_magic(&image), SLOT_MAGIC);
    assert_eq!(header_version(&image), SLOT_VERSION);
    assert_eq!(header_length(&image), 184);
    assert_eq!(header_checksum(&image), 0xF8C408BA);
    assert_eq!(&image[..], &kat_bytes()[..]);
}

#[test]
fn on_disk_round_trip() {
    let mut d = sample_data();
    d.database = 16384;
    d.persistency = RS_EPHEMERAL;
    d.xmin = 731;
    d.catalog_xmin = 730;
    d.invalidated = RS_INVAL_IDLE_TIMEOUT;
    d.two_phase_at = 0xDEADBEEF;
    d.two_phase = true;
    d.plugin.namestrcpy("test_decoding");
    d.synced = 1;
    d.failover = true;

    let image = serialize_state_file(&d);
    assert_eq!(state_file_checksum(&image), header_checksum(&image));

    let back = deserialize_persistent_data(&image[ON_DISK_CONSTANT_SIZE..]);
    assert_eq!(back.name.data, d.name.data);
    assert_eq!(back.database, d.database);
    assert_eq!(back.persistency, d.persistency);
    assert_eq!(back.xmin, d.xmin);
    assert_eq!(back.catalog_xmin, d.catalog_xmin);
    assert_eq!(back.restart_lsn, d.restart_lsn);
    assert_eq!(back.invalidated, d.invalidated);
    assert_eq!(back.confirmed_flush, d.confirmed_flush);
    assert_eq!(back.two_phase_at, d.two_phase_at);
    assert_eq!(back.two_phase, d.two_phase);
    assert_eq!(back.plugin.data, d.plugin.data);
    assert_eq!(back.synced, d.synced);
    assert_eq!(back.failover, d.failover);
}

#[test]
fn checksum_covers_exact_range() {
    let image = serialize_state_file(&sample_data());
    // Bytes 0..8 (magic + checksum) are outside the checksummed range.
    let mut altered = image;
    altered[0] ^= 0xFF;
    assert_eq!(state_file_checksum(&altered), header_checksum(&image));
    let mut altered = image;
    altered[ON_DISK_SIZE - 1] ^= 0x01;
    assert_ne!(state_file_checksum(&altered), header_checksum(&image));
}

#[test]
fn invalidation_cause_names() {
    use crate::{GetSlotInvalidationCause, GetSlotInvalidationCauseName};
    assert_eq!(GetSlotInvalidationCauseName(RS_INVAL_NONE), "none");
    assert_eq!(GetSlotInvalidationCauseName(RS_INVAL_WAL_REMOVED), "wal_removed");
    assert_eq!(GetSlotInvalidationCauseName(RS_INVAL_HORIZON), "rows_removed");
    assert_eq!(
        GetSlotInvalidationCauseName(RS_INVAL_WAL_LEVEL),
        "wal_level_insufficient"
    );
    assert_eq!(GetSlotInvalidationCauseName(RS_INVAL_IDLE_TIMEOUT), "idle_timeout");
    assert_eq!(GetSlotInvalidationCause("rows_removed"), RS_INVAL_HORIZON);
}
