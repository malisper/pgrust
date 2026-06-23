//! Subscription catalog vocabulary (`catalog/pg_subscription.h`,
//! `catalog/pg_subscription_rel.h`).
//!
//! Relation / index OIDs, attribute numbers, the substate / two-phase / origin
//! / stream constants, and the decoded in-memory carriers
//! [`Subscription`] / [`SubscriptionRelState`] (the C `Subscription` /
//! `SubscriptionRelState` structs, field-for-field). The owning catalog crate
//! (`backend-catalog-pg-subscription`) decodes a `pg_subscription` /
//! `pg_subscription_rel` tuple into these owned forms and re-forms them for the
//! catalog mutators, so a consuming crate never touches the on-disk datum
//! layout.

#![allow(non_upper_case_globals)]

use mcx::{PgString, PgVec};
use ::types_core::primitive::{Oid, XLogRecPtr};

/* ==========================================================================
 * Relation / index OIDs (`pg_subscription.h` / `pg_subscription_rel.h`
 * CATALOG / DECLARE_*_INDEX lines).
 * ========================================================================== */

/// `SubscriptionRelationId` — `pg_subscription` (6100).
pub const SubscriptionRelationId: Oid = 6100;
/// `SubscriptionRelation_Rowtype_Id` — `pg_subscription` rowtype OID (6101).
pub const SubscriptionRelation_Rowtype_Id: Oid = 6101;
/// `SubscriptionRelRelationId` — `pg_subscription_rel` (6102).
pub const SubscriptionRelRelationId: Oid = 6102;

/// `SubscriptionObjectIndexId` — `pg_subscription_oid_index` (6114).
pub const SubscriptionObjectIndexId: Oid = 6114;
/// `SubscriptionNameIndexId` — `pg_subscription_subname_index` (6115).
pub const SubscriptionNameIndexId: Oid = 6115;
/// `SubscriptionRelSrrelidSrsubidIndexId` —
/// `pg_subscription_rel_srrelid_srsubid_index` (6117).
pub const SubscriptionRelSrrelidSrsubidIndexId: Oid = 6117;

/* ==========================================================================
 * `pg_subscription` attribute numbers (`FormData_pg_subscription`, in order).
 * ========================================================================== */

/// `Anum_pg_subscription_oid` = 1.
pub const Anum_pg_subscription_oid: i32 = 1;
/// `Anum_pg_subscription_subdbid` = 2.
pub const Anum_pg_subscription_subdbid: i32 = 2;
/// `Anum_pg_subscription_subskiplsn` = 3.
pub const Anum_pg_subscription_subskiplsn: i32 = 3;
/// `Anum_pg_subscription_subname` = 4.
pub const Anum_pg_subscription_subname: i32 = 4;
/// `Anum_pg_subscription_subowner` = 5.
pub const Anum_pg_subscription_subowner: i32 = 5;
/// `Anum_pg_subscription_subenabled` = 6.
pub const Anum_pg_subscription_subenabled: i32 = 6;
/// `Anum_pg_subscription_subbinary` = 7.
pub const Anum_pg_subscription_subbinary: i32 = 7;
/// `Anum_pg_subscription_substream` = 8.
pub const Anum_pg_subscription_substream: i32 = 8;
/// `Anum_pg_subscription_subtwophasestate` = 9.
pub const Anum_pg_subscription_subtwophasestate: i32 = 9;
/// `Anum_pg_subscription_subdisableonerr` = 10.
pub const Anum_pg_subscription_subdisableonerr: i32 = 10;
/// `Anum_pg_subscription_subpasswordrequired` = 11.
pub const Anum_pg_subscription_subpasswordrequired: i32 = 11;
/// `Anum_pg_subscription_subrunasowner` = 12.
pub const Anum_pg_subscription_subrunasowner: i32 = 12;
/// `Anum_pg_subscription_subfailover` = 13.
pub const Anum_pg_subscription_subfailover: i32 = 13;
/// `Anum_pg_subscription_subconninfo` = 14.
pub const Anum_pg_subscription_subconninfo: i32 = 14;
/// `Anum_pg_subscription_subslotname` = 15.
pub const Anum_pg_subscription_subslotname: i32 = 15;
/// `Anum_pg_subscription_subsynccommit` = 16.
pub const Anum_pg_subscription_subsynccommit: i32 = 16;
/// `Anum_pg_subscription_subpublications` = 17.
pub const Anum_pg_subscription_subpublications: i32 = 17;
/// `Anum_pg_subscription_suborigin` = 18.
pub const Anum_pg_subscription_suborigin: i32 = 18;

/// `Natts_pg_subscription` = 18.
pub const Natts_pg_subscription: usize = 18;

/* ==========================================================================
 * `pg_subscription_rel` attribute numbers (`FormData_pg_subscription_rel`).
 * ========================================================================== */

/// `Anum_pg_subscription_rel_srsubid` = 1.
pub const Anum_pg_subscription_rel_srsubid: i32 = 1;
/// `Anum_pg_subscription_rel_srrelid` = 2.
pub const Anum_pg_subscription_rel_srrelid: i32 = 2;
/// `Anum_pg_subscription_rel_srsubstate` = 3.
pub const Anum_pg_subscription_rel_srsubstate: i32 = 3;
/// `Anum_pg_subscription_rel_srsublsn` = 4.
pub const Anum_pg_subscription_rel_srsublsn: i32 = 4;

/// `Natts_pg_subscription_rel` = 4.
pub const Natts_pg_subscription_rel: usize = 4;

/* ==========================================================================
 * substate / two-phase / origin / stream constants
 * (`pg_subscription_rel.h` / `pg_subscription.h` EXPOSE_TO_CLIENT_CODE).
 * ========================================================================== */

/// `LOGICALREP_TWOPHASE_STATE_DISABLED` (`'d'`).
pub const LOGICALREP_TWOPHASE_STATE_DISABLED: i8 = b'd' as i8;
/// `LOGICALREP_TWOPHASE_STATE_PENDING` (`'p'`).
pub const LOGICALREP_TWOPHASE_STATE_PENDING: i8 = b'p' as i8;
/// `LOGICALREP_TWOPHASE_STATE_ENABLED` (`'e'`).
pub const LOGICALREP_TWOPHASE_STATE_ENABLED: i8 = b'e' as i8;

/// `LOGICALREP_ORIGIN_NONE` (`"none"`).
pub const LOGICALREP_ORIGIN_NONE: &str = "none";
/// `LOGICALREP_ORIGIN_ANY` (`"any"`).
pub const LOGICALREP_ORIGIN_ANY: &str = "any";

/// `LOGICALREP_STREAM_OFF` (`'f'`).
pub const LOGICALREP_STREAM_OFF: i8 = b'f' as i8;
/// `LOGICALREP_STREAM_ON` (`'t'`).
pub const LOGICALREP_STREAM_ON: i8 = b't' as i8;
/// `LOGICALREP_STREAM_PARALLEL` (`'p'`).
pub const LOGICALREP_STREAM_PARALLEL: i8 = b'p' as i8;

/// `SUBREL_STATE_INIT` — initializing (sublsn NULL).
pub const SUBREL_STATE_INIT: i8 = b'i' as i8;
/// `SUBREL_STATE_DATASYNC` — data is being synchronized (sublsn NULL).
pub const SUBREL_STATE_DATASYNC: i8 = b'd' as i8;
/// `SUBREL_STATE_FINISHEDCOPY` — tablesync copy phase completed (sublsn NULL).
pub const SUBREL_STATE_FINISHEDCOPY: i8 = b'f' as i8;
/// `SUBREL_STATE_SYNCDONE` — synchronization finished in front of apply.
pub const SUBREL_STATE_SYNCDONE: i8 = b's' as i8;
/// `SUBREL_STATE_READY` — ready (sublsn set).
pub const SUBREL_STATE_READY: i8 = b'r' as i8;

/// `SUBREL_STATE_UNKNOWN` — unknown state (`'\0'`); never stored in the catalog.
pub const SUBREL_STATE_UNKNOWN: i8 = b'\0' as i8;
/// `SUBREL_STATE_SYNCWAIT` — waiting for sync (`'w'`); IPC only.
pub const SUBREL_STATE_SYNCWAIT: i8 = b'w' as i8;
/// `SUBREL_STATE_CATCHUP` — catching up with apply (`'c'`); IPC only.
pub const SUBREL_STATE_CATCHUP: i8 = b'c' as i8;

/* ==========================================================================
 * Decoded in-memory carriers.
 * ========================================================================== */

/// In-memory form of a subscription, the decoded `pg_subscription` row as
/// returned by `GetSubscription` (`catalog/pg_subscription.h` `Subscription`).
///
/// The C `char *` fields become owned `PgString`s; the nullable `slotname`
/// becomes `Option<PgString>`; the `List *publications` of `String` value
/// nodes becomes `PgVec<PgString>`.
#[derive(Debug)]
pub struct Subscription<'mcx> {
    /// `oid` — Oid of the subscription.
    pub oid: Oid,
    /// `dbid` (`subdbid`) — Oid of the database which subscription is in.
    pub dbid: Oid,
    /// `skiplsn` (`subskiplsn`) — all changes finished at this LSN are skipped.
    pub skiplsn: XLogRecPtr,
    /// `name` (`subname`) — name of the subscription.
    pub name: PgString<'mcx>,
    /// `owner` (`subowner`) — Oid of the subscription owner.
    pub owner: Oid,
    /// `ownersuperuser` — is the subscription owner a superuser?
    pub ownersuperuser: bool,
    /// `enabled` (`subenabled`) — indicates if the subscription is enabled.
    pub enabled: bool,
    /// `binary` (`subbinary`) — wants data in binary format.
    pub binary: bool,
    /// `stream` (`substream`) — allow streaming in-progress transactions
    /// (`LOGICALREP_STREAM_xxx`).
    pub stream: i8,
    /// `twophasestate` (`subtwophasestate`) — allow streaming two-phase txns.
    pub twophasestate: i8,
    /// `disableonerr` (`subdisableonerr`) — auto-disable on worker error.
    pub disableonerr: bool,
    /// `passwordrequired` (`subpasswordrequired`) — must connection use a
    /// password?
    pub passwordrequired: bool,
    /// `runasowner` (`subrunasowner`) — run replication as subscription owner.
    pub runasowner: bool,
    /// `failover` (`subfailover`) — synchronize the associated replication
    /// slots to the standbys.
    pub failover: bool,
    /// `conninfo` (`subconninfo`) — connection string to the publisher.
    pub conninfo: PgString<'mcx>,
    /// `slotname` (`subslotname`) — name of the replication slot (`None` when
    /// SQL NULL).
    pub slotname: Option<PgString<'mcx>>,
    /// `synccommit` (`subsynccommit`) — synchronous commit setting for worker.
    pub synccommit: PgString<'mcx>,
    /// `publications` (`subpublications`) — list of publication names.
    pub publications: PgVec<'mcx, PgString<'mcx>>,
    /// `origin` (`suborigin`) — only publish data originating from this origin.
    pub origin: PgString<'mcx>,
}

/// In-memory per-relation state of a subscription, an element of the list
/// returned by `GetSubscriptionRelations` (`catalog/pg_subscription_rel.h`
/// `SubscriptionRelState`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SubscriptionRelState {
    /// `relid`.
    pub relid: Oid,
    /// `lsn`.
    pub lsn: XLogRecPtr,
    /// `state`.
    pub state: i8,
}
