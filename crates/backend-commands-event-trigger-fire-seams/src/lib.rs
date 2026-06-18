//! Seam declarations for the *firing* tail of `commands/event_trigger.c`.
//!
//! The fence entry points (`EventTriggerDDLCommandStart` / `…End` /
//! `EventTriggerSQLDrop` / `EventTriggerTableRewrite`) all begin with
//! `if (!IsUnderPostmaster || !event_triggers) return;` — so in standalone
//! single-user mode (the only mode this repo currently boots) the firing tail is
//! never reached. The tail (`EventTriggerCommonSetup` + `EventTriggerInvoke`)
//! pulls in the whole fmgr-dispatch / snapshot / `CreateCommandTag` /
//! `session_replication_role` / bitmapset firing machinery; porting it is a
//! distinct sub-campaign. Until then this seam stays uninstalled and panics
//! loudly if a build ever reaches it with `IsUnderPostmaster` true and an event
//! trigger present — never a silent stub.

#![allow(non_snake_case)]

use types_error::PgResult;
use types_evtcache::EventTriggerEvent;
use types_nodes::nodes::Node;

seam_core::seam!(
    /// `EventTriggerCommonSetup` + `EventTriggerInvoke` (event_trigger.c) — the
    /// post-gate firing tail for the `ddl_command_start` / `ddl_command_end` /
    /// `sql_drop` events. `parsetree` is the command's parse tree (for
    /// `CreateCommandTag`); `event`/`eventstr` identify the event being fired.
    /// `Err` carries the fmgr / snapshot / catalog error surface of the fired
    /// trigger functions.
    pub fn event_trigger_fire<'mcx>(
        parsetree: &Node<'mcx>,
        event: EventTriggerEvent,
        eventstr: &str,
    ) -> PgResult<()>
);
