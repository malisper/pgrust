//! Slots for the `extern const struct config_enum_entry ...[]` option
//! arrays guc_tables.c imports from other units; each owner installs its
//! array from its `init_seams()`.

#![allow(non_upper_case_globals)]

use crate::slots::{GucEnumOptionsSlot, GucSlot};

pub static archive_mode_options: GucEnumOptionsSlot = GucSlot::new("archive_mode_options");
pub static dynamic_shared_memory_options: GucEnumOptionsSlot = GucSlot::new("dynamic_shared_memory_options");
pub static io_method_options: GucEnumOptionsSlot = GucSlot::new("io_method_options");
pub static recovery_target_action_options: GucEnumOptionsSlot = GucSlot::new("recovery_target_action_options");
pub static wal_level_options: GucEnumOptionsSlot = GucSlot::new("wal_level_options");
pub static wal_sync_method_options: GucEnumOptionsSlot = GucSlot::new("wal_sync_method_options");
