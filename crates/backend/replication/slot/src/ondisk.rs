use types_core::{Oid, TransactionId, XLogRecPtr};
use types_tuple::NameData;

pub const SLOT_MAGIC: u32 = 0x1051CA1;
pub const SLOT_VERSION: u32 = 5;

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ReplicationSlotPersistency(pub i32);
pub const RS_PERSISTENT: ReplicationSlotPersistency = ReplicationSlotPersistency(0);
pub const RS_EPHEMERAL: ReplicationSlotPersistency = ReplicationSlotPersistency(1);
pub const RS_TEMPORARY: ReplicationSlotPersistency = ReplicationSlotPersistency(2);

// Transparent i32 (not an enum): a state file byte pattern must never be UB.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ReplicationSlotInvalidationCause(pub i32);
pub const RS_INVAL_NONE: ReplicationSlotInvalidationCause = ReplicationSlotInvalidationCause(0);
pub const RS_INVAL_WAL_REMOVED: ReplicationSlotInvalidationCause =
    ReplicationSlotInvalidationCause(1 << 0);
pub const RS_INVAL_HORIZON: ReplicationSlotInvalidationCause =
    ReplicationSlotInvalidationCause(1 << 1);
pub const RS_INVAL_WAL_LEVEL: ReplicationSlotInvalidationCause =
    ReplicationSlotInvalidationCause(1 << 2);
pub const RS_INVAL_IDLE_TIMEOUT: ReplicationSlotInvalidationCause =
    ReplicationSlotInvalidationCause(1 << 3);
pub const RS_INVAL_MAX_CAUSES: usize = 4;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ReplicationSlotPersistentData {
    pub name: NameData,
    pub database: Oid,
    pub persistency: ReplicationSlotPersistency,
    pub xmin: TransactionId,
    pub catalog_xmin: TransactionId,
    pub restart_lsn: XLogRecPtr,
    pub invalidated: ReplicationSlotInvalidationCause,
    pub confirmed_flush: XLogRecPtr,
    pub two_phase_at: XLogRecPtr,
    pub two_phase: bool,
    pub plugin: NameData,
    pub synced: u8,
    pub failover: bool,
}

impl Default for ReplicationSlotPersistentData {
    fn default() -> Self {
        Self {
            name: NameData::default(),
            database: 0,
            persistency: RS_PERSISTENT,
            xmin: 0,
            catalog_xmin: 0,
            restart_lsn: 0,
            invalidated: RS_INVAL_NONE,
            confirmed_flush: 0,
            two_phase_at: 0,
            two_phase: false,
            plugin: NameData::default(),
            synced: 0,
            failover: false,
        }
    }
}

// C ReplicationSlotOnDisk: magic u32, checksum u32, version u32, length u32,
// slotdata. The derived sizes below are ground truth for the state file.
pub const ON_DISK_CONSTANT_SIZE: usize = 16;
pub const ON_DISK_NOT_CHECKSUMMED_SIZE: usize = 8;
pub const PERSISTENT_DATA_SIZE: usize = 184;
pub const ON_DISK_SIZE: usize = ON_DISK_CONSTANT_SIZE + PERSISTENT_DATA_SIZE;
pub const ON_DISK_CHECKSUMMED_SIZE: usize = ON_DISK_SIZE - ON_DISK_NOT_CHECKSUMMED_SIZE;

const OFF_NAME: usize = 0;
const OFF_DATABASE: usize = 64;
const OFF_PERSISTENCY: usize = 68;
const OFF_XMIN: usize = 72;
const OFF_CATALOG_XMIN: usize = 76;
const OFF_RESTART_LSN: usize = 80;
const OFF_INVALIDATED: usize = 88;
const OFF_CONFIRMED_FLUSH: usize = 96;
const OFF_TWO_PHASE_AT: usize = 104;
const OFF_TWO_PHASE: usize = 112;
const OFF_PLUGIN: usize = 113;
const OFF_SYNCED: usize = 177;
const OFF_FAILOVER: usize = 178;

const _: () = {
    use core::mem::{align_of, offset_of, size_of};
    assert!(size_of::<NameData>() == 64 && align_of::<NameData>() == 1);
    assert!(size_of::<ReplicationSlotPersistentData>() == PERSISTENT_DATA_SIZE);
    assert!(offset_of!(ReplicationSlotPersistentData, name) == OFF_NAME);
    assert!(offset_of!(ReplicationSlotPersistentData, database) == OFF_DATABASE);
    assert!(offset_of!(ReplicationSlotPersistentData, persistency) == OFF_PERSISTENCY);
    assert!(offset_of!(ReplicationSlotPersistentData, xmin) == OFF_XMIN);
    assert!(offset_of!(ReplicationSlotPersistentData, catalog_xmin) == OFF_CATALOG_XMIN);
    assert!(offset_of!(ReplicationSlotPersistentData, restart_lsn) == OFF_RESTART_LSN);
    assert!(offset_of!(ReplicationSlotPersistentData, invalidated) == OFF_INVALIDATED);
    assert!(offset_of!(ReplicationSlotPersistentData, confirmed_flush) == OFF_CONFIRMED_FLUSH);
    assert!(offset_of!(ReplicationSlotPersistentData, two_phase_at) == OFF_TWO_PHASE_AT);
    assert!(offset_of!(ReplicationSlotPersistentData, two_phase) == OFF_TWO_PHASE);
    assert!(offset_of!(ReplicationSlotPersistentData, plugin) == OFF_PLUGIN);
    assert!(offset_of!(ReplicationSlotPersistentData, synced) == OFF_SYNCED);
    assert!(offset_of!(ReplicationSlotPersistentData, failover) == OFF_FAILOVER);
};

fn put(buf: &mut [u8], off: usize, bytes: &[u8]) {
    buf[off..off + bytes.len()].copy_from_slice(bytes);
}

fn serialize_persistent_data(d: &ReplicationSlotPersistentData, buf: &mut [u8]) {
    put(buf, OFF_NAME, &d.name.data);
    put(buf, OFF_DATABASE, &d.database.to_ne_bytes());
    put(buf, OFF_PERSISTENCY, &d.persistency.0.to_ne_bytes());
    put(buf, OFF_XMIN, &d.xmin.to_ne_bytes());
    put(buf, OFF_CATALOG_XMIN, &d.catalog_xmin.to_ne_bytes());
    put(buf, OFF_RESTART_LSN, &d.restart_lsn.to_ne_bytes());
    put(buf, OFF_INVALIDATED, &d.invalidated.0.to_ne_bytes());
    put(buf, OFF_CONFIRMED_FLUSH, &d.confirmed_flush.to_ne_bytes());
    put(buf, OFF_TWO_PHASE_AT, &d.two_phase_at.to_ne_bytes());
    buf[OFF_TWO_PHASE] = d.two_phase as u8;
    put(buf, OFF_PLUGIN, &d.plugin.data);
    buf[OFF_SYNCED] = d.synced;
    buf[OFF_FAILOVER] = d.failover as u8;
}

fn get_u32(buf: &[u8], off: usize) -> u32 {
    u32::from_ne_bytes(buf[off..off + 4].try_into().unwrap())
}

fn get_i32(buf: &[u8], off: usize) -> i32 {
    i32::from_ne_bytes(buf[off..off + 4].try_into().unwrap())
}

fn get_u64(buf: &[u8], off: usize) -> u64 {
    u64::from_ne_bytes(buf[off..off + 8].try_into().unwrap())
}

pub fn deserialize_persistent_data(buf: &[u8]) -> ReplicationSlotPersistentData {
    let mut name = NameData::default();
    name.data.copy_from_slice(&buf[OFF_NAME..OFF_NAME + 64]);
    let mut plugin = NameData::default();
    plugin.data.copy_from_slice(&buf[OFF_PLUGIN..OFF_PLUGIN + 64]);
    ReplicationSlotPersistentData {
        name,
        database: get_u32(buf, OFF_DATABASE),
        persistency: ReplicationSlotPersistency(get_i32(buf, OFF_PERSISTENCY)),
        xmin: get_u32(buf, OFF_XMIN),
        catalog_xmin: get_u32(buf, OFF_CATALOG_XMIN),
        restart_lsn: get_u64(buf, OFF_RESTART_LSN),
        invalidated: ReplicationSlotInvalidationCause(get_i32(buf, OFF_INVALIDATED)),
        confirmed_flush: get_u64(buf, OFF_CONFIRMED_FLUSH),
        two_phase_at: get_u64(buf, OFF_TWO_PHASE_AT),
        two_phase: buf[OFF_TWO_PHASE] != 0,
        plugin,
        synced: buf[OFF_SYNCED],
        failover: buf[OFF_FAILOVER] != 0,
    }
}

pub fn state_file_checksum(image: &[u8]) -> u32 {
    let crc = crc32c::pg_comp_crc32c(
        crc32c::CRC32C_INIT,
        &image[ON_DISK_NOT_CHECKSUMMED_SIZE..ON_DISK_SIZE],
    );
    crc32c::fin_crc32c(crc)
}

pub fn serialize_state_file(d: &ReplicationSlotPersistentData) -> [u8; ON_DISK_SIZE] {
    let mut image = [0u8; ON_DISK_SIZE];
    put(&mut image, 0, &SLOT_MAGIC.to_ne_bytes());
    put(&mut image, 8, &SLOT_VERSION.to_ne_bytes());
    put(&mut image, 12, &(PERSISTENT_DATA_SIZE as u32).to_ne_bytes());
    serialize_persistent_data(d, &mut image[ON_DISK_CONSTANT_SIZE..]);
    let checksum = state_file_checksum(&image);
    put(&mut image, 4, &checksum.to_ne_bytes());
    image
}

pub fn header_magic(buf: &[u8]) -> u32 {
    get_u32(buf, 0)
}

pub fn header_checksum(buf: &[u8]) -> u32 {
    get_u32(buf, 4)
}

pub fn header_version(buf: &[u8]) -> u32 {
    get_u32(buf, 8)
}

pub fn header_length(buf: &[u8]) -> u32 {
    get_u32(buf, 12)
}
