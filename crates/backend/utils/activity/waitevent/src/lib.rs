//! wait_event.c + wait_event_funcs.c (my_wait_event_info indirection; C's
//! never-read local fallback is dropped).

use core::cell::Cell;
use core::sync::atomic::{AtomicU32, Ordering::Relaxed};

pub mod custom;
pub mod funcs;
#[cfg(test)]
mod tests;

thread_local! {
    static MY_WAIT_EVENT_INFO: Cell<Option<&'static AtomicU32>> = const { Cell::new(None) };
}

pub fn pgstat_set_wait_event_storage(slot: &'static AtomicU32) {
    MY_WAIT_EVENT_INFO.set(Some(slot));
}

pub fn pgstat_reset_wait_event_storage() {
    MY_WAIT_EVENT_INFO.set(None);
}

#[inline]
pub fn pgstat_report_wait_start(wait_event_info: u32) {
    if let Some(slot) = MY_WAIT_EVENT_INFO.get() {
        slot.store(wait_event_info, Relaxed);
    }
}

#[inline]
pub fn pgstat_report_wait_end() {
    pgstat_report_wait_start(0);
}

pub const PG_WAIT_LWLOCK: u32 = 0x0100_0000;
pub const PG_WAIT_LOCK: u32 = 0x0300_0000;
pub const PG_WAIT_BUFFERPIN: u32 = 0x0400_0000;
pub const PG_WAIT_ACTIVITY: u32 = 0x0500_0000;
pub const PG_WAIT_CLIENT: u32 = 0x0600_0000;
pub const PG_WAIT_EXTENSION: u32 = 0x0700_0000;
pub const PG_WAIT_IPC: u32 = 0x0800_0000;
pub const PG_WAIT_TIMEOUT: u32 = 0x0900_0000;
pub const PG_WAIT_IO: u32 = 0x0A00_0000;
pub const PG_WAIT_INJECTIONPOINT: u32 = 0x0B00_0000;

pub(crate) const WAIT_EVENT_CLASS_MASK: u32 = 0xFF00_0000;
const WAIT_EVENT_ID_MASK: u32 = 0x0000_FFFF;

// Ordering within each array is the eventId order fixed by
// wait_event_names.txt section order (generate-wait_event_types.pl).
static WAIT_EVENT_ACTIVITY_NAMES: [&str; 18] = [
    "ArchiverMain",
    "AutovacuumMain",
    "BgwriterHibernate",
    "BgwriterMain",
    "CheckpointerMain",
    "CheckpointerShutdown",
    "IoWorkerMain",
    "LogicalApplyMain",
    "LogicalLauncherMain",
    "LogicalParallelApplyMain",
    "RecoveryWalStream",
    "ReplicationSlotsyncMain",
    "ReplicationSlotsyncShutdown",
    "SysloggerMain",
    "WalReceiverMain",
    "WalSenderMain",
    "WalSummarizerWal",
    "WalWriterMain",
];

static WAIT_EVENT_CLIENT_NAMES: [&str; 9] = [
    "ClientRead",
    "ClientWrite",
    "GssOpenServer",
    "LibpqwalreceiverConnect",
    "LibpqwalreceiverReceive",
    "SslOpenServer",
    "WaitForStandbyConfirmation",
    "WalSenderWaitForWal",
    "WalSenderWriteData",
];

static WAIT_EVENT_IPC_NAMES: [&str; 57] = [
    "AppendReady",
    "ArchiveCleanupCommand",
    "ArchiveCommand",
    "BackendTermination",
    "BackupWaitWalArchive",
    "BgworkerShutdown",
    "BgworkerStartup",
    "BtreePage",
    "BufferIo",
    "CheckpointDelayComplete",
    "CheckpointDelayStart",
    "CheckpointDone",
    "CheckpointStart",
    "ExecuteGather",
    "HashBatchAllocate",
    "HashBatchElect",
    "HashBatchLoad",
    "HashBuildAllocate",
    "HashBuildElect",
    "HashBuildHashInner",
    "HashBuildHashOuter",
    "HashGrowBatchesDecide",
    "HashGrowBatchesElect",
    "HashGrowBatchesFinish",
    "HashGrowBatchesReallocate",
    "HashGrowBatchesRepartition",
    "HashGrowBucketsElect",
    "HashGrowBucketsReallocate",
    "HashGrowBucketsReinsert",
    "LogicalApplySendData",
    "LogicalParallelApplyStateChange",
    "LogicalSyncData",
    "LogicalSyncStateChange",
    "MessageQueueInternal",
    "MessageQueuePutMessage",
    "MessageQueueReceive",
    "MessageQueueSend",
    "MultixactCreation",
    "ParallelBitmapScan",
    "ParallelCreateIndexScan",
    "ParallelFinish",
    "ProcarrayGroupUpdate",
    "ProcSignalBarrier",
    "Promote",
    "RecoveryConflictSnapshot",
    "RecoveryConflictTablespace",
    "RecoveryEndCommand",
    "RecoveryPause",
    "ReplicationOriginDrop",
    "ReplicationSlotDrop",
    "RestoreCommand",
    "SafeSnapshot",
    "SyncRep",
    "WalReceiverExit",
    "WalReceiverWaitStart",
    "WalSummaryReady",
    "XactGroupUpdate",
];

static WAIT_EVENT_TIMEOUT_NAMES: [&str; 10] = [
    "BaseBackupThrottle",
    "CheckpointWriteDelay",
    "PgSleep",
    "RecoveryApplyDelay",
    "RecoveryRetrieveRetryInterval",
    "RegisterSyncRequest",
    "SpinDelay",
    "VacuumDelay",
    "VacuumTruncate",
    "WalSummarizerError",
];

static WAIT_EVENT_IO_NAMES: [&str; 81] = [
    "AioIoCompletion",
    "AioIoUringExecution",
    "AioIoUringSubmit",
    "BasebackupRead",
    "BasebackupSync",
    "BasebackupWrite",
    "BuffileRead",
    "BuffileTruncate",
    "BuffileWrite",
    "ControlFileRead",
    "ControlFileSync",
    "ControlFileSyncUpdate",
    "ControlFileWrite",
    "ControlFileWriteUpdate",
    "CopyFileCopy",
    "CopyFileRead",
    "CopyFileWrite",
    "DataFileExtend",
    "DataFileFlush",
    "DataFileImmediateSync",
    "DataFilePrefetch",
    "DataFileRead",
    "DataFileSync",
    "DataFileTruncate",
    "DataFileWrite",
    "DsmAllocate",
    "DsmFillZeroWrite",
    "LockFileAddtodatadirRead",
    "LockFileAddtodatadirSync",
    "LockFileAddtodatadirWrite",
    "LockFileCreateRead",
    "LockFileCreateSync",
    "LockFileCreateWrite",
    "LockFileRecheckdatadirRead",
    "LogicalRewriteCheckpointSync",
    "LogicalRewriteMappingSync",
    "LogicalRewriteMappingWrite",
    "LogicalRewriteSync",
    "LogicalRewriteTruncate",
    "LogicalRewriteWrite",
    "RelationMapRead",
    "RelationMapReplace",
    "RelationMapWrite",
    "ReorderBufferRead",
    "ReorderBufferWrite",
    "ReorderLogicalMappingRead",
    "ReplicationSlotRead",
    "ReplicationSlotRestoreSync",
    "ReplicationSlotSync",
    "ReplicationSlotWrite",
    "SlruFlushSync",
    "SlruRead",
    "SlruSync",
    "SlruWrite",
    "SnapbuildRead",
    "SnapbuildSync",
    "SnapbuildWrite",
    "TimelineHistoryFileSync",
    "TimelineHistoryFileWrite",
    "TimelineHistoryRead",
    "TimelineHistorySync",
    "TimelineHistoryWrite",
    "TwophaseFileRead",
    "TwophaseFileSync",
    "TwophaseFileWrite",
    "VersionFileSync",
    "VersionFileWrite",
    "WalsenderTimelineHistoryRead",
    "WalBootstrapSync",
    "WalBootstrapWrite",
    "WalCopyRead",
    "WalCopySync",
    "WalCopyWrite",
    "WalInitSync",
    "WalInitWrite",
    "WalRead",
    "WalSummaryRead",
    "WalSummaryWrite",
    "WalSync",
    "WalSyncMethodAssign",
    "WalWrite",
];

static WAIT_EVENT_BUFFERPIN_NAMES: [&str; 1] = ["BufferPin"];

// LockTagTypeNames (lockfuncs.c), indexed by LockTagType.
static LOCK_TAG_TYPE_NAMES: [&str; 12] = [
    "relation",
    "extend",
    "frozenid",
    "page",
    "tuple",
    "transactionid",
    "virtualxid",
    "spectoken",
    "object",
    "userlock",
    "advisory",
    "applytransaction",
];

pub fn pgstat_get_wait_event_type(wait_event_info: u32) -> Option<&'static str> {
    if wait_event_info == 0 {
        return None;
    }
    Some(match wait_event_info & WAIT_EVENT_CLASS_MASK {
        PG_WAIT_LWLOCK => "LWLock",
        PG_WAIT_LOCK => "Lock",
        PG_WAIT_BUFFERPIN => "BufferPin",
        PG_WAIT_ACTIVITY => "Activity",
        PG_WAIT_CLIENT => "Client",
        PG_WAIT_EXTENSION => "Extension",
        PG_WAIT_IPC => "IPC",
        PG_WAIT_TIMEOUT => "Timeout",
        PG_WAIT_IO => "IO",
        PG_WAIT_INJECTIONPOINT => "InjectionPoint",
        class => panic!("unknown wait event class {class:#010x}"),
    })
}

pub fn pgstat_get_wait_event(wait_event_info: u32) -> Option<&'static str> {
    if wait_event_info == 0 {
        return None;
    }
    let class_id = wait_event_info & WAIT_EVENT_CLASS_MASK;
    let event_id = (wait_event_info & WAIT_EVENT_ID_MASK) as usize;

    let named = |names: &'static [&'static str]| {
        *names.get(event_id).unwrap_or_else(|| {
            panic!("unknown wait event {event_id} in class {class_id:#010x}")
        })
    };
    Some(match class_id {
        PG_WAIT_LWLOCK => lwlock::GetLWLockIdentifier(class_id, event_id as u16),
        // GetLockNameFromTagType (lmgr.c) over LockTagTypeNames
        // (lockfuncs.c); the event id is the locktag type.
        PG_WAIT_LOCK => LOCK_TAG_TYPE_NAMES.get(event_id).copied().unwrap_or("???"),
        PG_WAIT_EXTENSION | PG_WAIT_INJECTIONPOINT => {
            custom::GetWaitEventCustomIdentifier(wait_event_info)
        }
        PG_WAIT_BUFFERPIN => named(&WAIT_EVENT_BUFFERPIN_NAMES),
        PG_WAIT_ACTIVITY => named(&WAIT_EVENT_ACTIVITY_NAMES),
        PG_WAIT_CLIENT => named(&WAIT_EVENT_CLIENT_NAMES),
        PG_WAIT_IPC => named(&WAIT_EVENT_IPC_NAMES),
        PG_WAIT_TIMEOUT => named(&WAIT_EVENT_TIMEOUT_NAMES),
        PG_WAIT_IO => named(&WAIT_EVENT_IO_NAMES),
        class => panic!("unknown wait event class {class:#010x}"),
    })
}

pub fn init_seams() {
    waitevent_seams::pgstat_report_wait_start::set(pgstat_report_wait_start);
    waitevent_seams::pgstat_report_wait_end::set(pgstat_report_wait_end);
    waitevent_seams::pgstat_set_wait_event_storage::set(pgstat_set_wait_event_storage);
    waitevent_seams::pgstat_reset_wait_event_storage::set(pgstat_reset_wait_event_storage);
}
