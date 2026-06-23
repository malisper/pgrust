//! Seam declarations for the *firing* tail of `commands/event_trigger.c`.
//!
//! The fence entry points (`EventTriggerDDLCommandStart` / `…End` /
//! `EventTriggerSQLDrop` / `EventTriggerTableRewrite`) all begin with
//! `if (!IsUnderPostmaster || !event_triggers) return;`. Once
//! `IsUnderPostmaster` is true (the postmaster boot the regression suite uses)
//! and a `pg_event_trigger` row exists, the firing tail is reached: this seam
//! carries the post-gate `EventTriggerCommonSetup` run-list build (`CommandTag`
//! + `filter_event_trigger`) and `EventTriggerInvoke` (the fmgr dispatch of each
//! matching trigger function). The owner installs it from `init_seams`.

#![allow(non_snake_case)]

use ::types_error::PgResult;
use ::types_evtcache::EventTriggerEvent;
use ::nodes::nodes::Node;

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
