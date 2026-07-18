//! `pgsync::sim` — the permit scheduler's home (sim world only).
//!
//! Ownership (permit-s1 contract §5 fences): WS-SYNC created this module
//! once, in its FIRST pushed commit, to pin the [`hooks`] seam; everything
//! under `src/sim/**` belongs to WS-CORE from that commit on (scheduler
//! core, slot registry, seeded picker, virtual-time advance, watchdog,
//! SCHEDOP log). WS-SYNC does not touch this directory again.

pub mod hooks;
