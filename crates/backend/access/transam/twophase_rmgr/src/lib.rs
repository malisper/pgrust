#![allow(non_upper_case_globals)]

use types_core::TransactionId;
use types_error::PgResult;

pub type TwoPhaseCallback = fn(TransactionId, u16, &[u8]) -> PgResult<()>;

pub const TWOPHASE_RM_END_ID: u8 = 0;
pub const TWOPHASE_RM_LOCK_ID: u8 = 1;
pub const TWOPHASE_RM_PGSTAT_ID: u8 = 2;
pub const TWOPHASE_RM_MULTIXACT_ID: u8 = 3;
pub const TWOPHASE_RM_PREDICATELOCK_ID: u8 = 4;
pub const TWOPHASE_RM_MAX_ID: u8 = TWOPHASE_RM_PREDICATELOCK_ID;

pub const NUM_TWOPHASE_RM: usize = TWOPHASE_RM_MAX_ID as usize + 1;

// PREDICATELOCK records are only written by a SERIALIZABLE prepare; the
// predicate crate's stub SSI never writes one, so a recovered record is a
// loud gap, not a silent drop.
fn predicatelock_twophase_recover(_xid: TransactionId, _info: u16, _recdata: &[u8]) -> PgResult<()> {
    panic!("predicatelock_twophase_recover: SSI 2PC recovery unported (predicate.c)");
}

pub static twophase_recover_callbacks: [Option<TwoPhaseCallback>; NUM_TWOPHASE_RM] = [
    None,
    Some(lock::lock_twophase_recover),
    None,
    Some(multixact::multixact_twophase_recover),
    Some(predicatelock_twophase_recover),
];

pub static twophase_postcommit_callbacks: [Option<TwoPhaseCallback>; NUM_TWOPHASE_RM] = [
    None,
    Some(lock::lock_twophase_postcommit),
    Some(pgstat::relation::pgstat_twophase_postcommit),
    Some(multixact::multixact_twophase_postcommit),
    None,
];

pub static twophase_postabort_callbacks: [Option<TwoPhaseCallback>; NUM_TWOPHASE_RM] = [
    None,
    Some(lock::lock_twophase_postabort),
    Some(pgstat::relation::pgstat_twophase_postabort),
    Some(multixact::multixact_twophase_postabort),
    None,
];

pub static twophase_standby_recover_callbacks: [Option<TwoPhaseCallback>; NUM_TWOPHASE_RM] = [
    None,
    Some(lock::lock_twophase_standby_recover),
    None,
    None,
    None,
];
