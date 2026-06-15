use core::ffi::{c_char, c_double, c_int, c_void};

use crate::Oid;

pub type GucContext = u32;
pub const PGC_INTERNAL: GucContext = 0;
pub const PGC_POSTMASTER: GucContext = 1;
pub const PGC_SIGHUP: GucContext = 2;
pub const PGC_SU_BACKEND: GucContext = 3;
pub const PGC_BACKEND: GucContext = 4;
pub const PGC_SUSET: GucContext = 5;
pub const PGC_USERSET: GucContext = 6;

pub type GucSource = u32;
pub const PGC_S_DEFAULT: GucSource = 0;
pub const PGC_S_DYNAMIC_DEFAULT: GucSource = 1;
pub const PGC_S_ENV_VAR: GucSource = 2;
pub const PGC_S_FILE: GucSource = 3;
pub const PGC_S_ARGV: GucSource = 4;
pub const PGC_S_GLOBAL: GucSource = 5;
pub const PGC_S_DATABASE: GucSource = 6;
pub const PGC_S_USER: GucSource = 7;
pub const PGC_S_DATABASE_USER: GucSource = 8;
pub const PGC_S_CLIENT: GucSource = 9;
pub const PGC_S_OVERRIDE: GucSource = 10;
pub const PGC_S_INTERACTIVE: GucSource = 11;
pub const PGC_S_TEST: GucSource = 12;
pub const PGC_S_SESSION: GucSource = 13;

pub type config_type = u32;
pub const PGC_BOOL: config_type = 0;
pub const PGC_INT: config_type = 1;
pub const PGC_REAL: config_type = 2;
pub const PGC_STRING: config_type = 3;
pub const PGC_ENUM: config_type = 4;

pub type config_group = u32;
pub const UNGROUPED: config_group = 0;
pub const FILE_LOCATIONS: config_group = 1;
pub const CONN_AUTH_SETTINGS: config_group = 2;
pub const CONN_AUTH_TCP: config_group = 3;
pub const CONN_AUTH_AUTH: config_group = 4;
pub const CONN_AUTH_SSL: config_group = 5;
pub const RESOURCES_MEM: config_group = 6;
pub const RESOURCES_DISK: config_group = 7;
pub const RESOURCES_KERNEL: config_group = 8;
pub const RESOURCES_BGWRITER: config_group = 9;
pub const RESOURCES_IO: config_group = 10;
pub const RESOURCES_WORKER_PROCESSES: config_group = 11;
pub const WAL_SETTINGS: config_group = 12;
pub const WAL_CHECKPOINTS: config_group = 13;
pub const WAL_ARCHIVING: config_group = 14;
pub const WAL_RECOVERY: config_group = 15;
pub const WAL_ARCHIVE_RECOVERY: config_group = 16;
pub const WAL_RECOVERY_TARGET: config_group = 17;
pub const WAL_SUMMARIZATION: config_group = 18;
pub const REPLICATION_SENDING: config_group = 19;
pub const REPLICATION_PRIMARY: config_group = 20;
pub const REPLICATION_STANDBY: config_group = 21;
pub const REPLICATION_SUBSCRIBERS: config_group = 22;
pub const QUERY_TUNING_METHOD: config_group = 23;
pub const QUERY_TUNING_COST: config_group = 24;
pub const QUERY_TUNING_GEQO: config_group = 25;
pub const QUERY_TUNING_OTHER: config_group = 26;
pub const LOGGING_WHERE: config_group = 27;
pub const LOGGING_WHEN: config_group = 28;
pub const LOGGING_WHAT: config_group = 29;
pub const PROCESS_TITLE: config_group = 30;
pub const STATS_MONITORING: config_group = 31;
pub const STATS_CUMULATIVE: config_group = 32;
pub const VACUUM_AUTOVACUUM: config_group = 33;
pub const VACUUM_COST_DELAY: config_group = 34;
pub const VACUUM_DEFAULT: config_group = 35;
pub const VACUUM_FREEZING: config_group = 36;
pub const CLIENT_CONN_STATEMENT: config_group = 37;
pub const CLIENT_CONN_LOCALE: config_group = 38;
pub const CLIENT_CONN_PRELOAD: config_group = 39;
pub const CLIENT_CONN_OTHER: config_group = 40;
pub const LOCK_MANAGEMENT: config_group = 41;
pub const COMPAT_OPTIONS_PREVIOUS: config_group = 42;
pub const COMPAT_OPTIONS_OTHER: config_group = 43;
pub const ERROR_HANDLING_OPTIONS: config_group = 44;
pub const PRESET_OPTIONS: config_group = 45;
pub const CUSTOM_OPTIONS: config_group = 46;
pub const DEVELOPER_OPTIONS: config_group = 47;

pub type GucStackState = u32;
pub const GUC_SAVE: GucStackState = 0;
pub const GUC_SET: GucStackState = 1;
pub const GUC_LOCAL: GucStackState = 2;
pub const GUC_SET_LOCAL: GucStackState = 3;

pub const GUC_LIST_INPUT: c_int = 0x000001;
pub const GUC_LIST_QUOTE: c_int = 0x000002;
pub const GUC_NO_SHOW_ALL: c_int = 0x000004;
pub const GUC_NO_RESET: c_int = 0x000008;
pub const GUC_NO_RESET_ALL: c_int = 0x000010;
pub const GUC_EXPLAIN: c_int = 0x000020;
pub const GUC_REPORT: c_int = 0x000040;
pub const GUC_NOT_IN_SAMPLE: c_int = 0x000080;
pub const GUC_DISALLOW_IN_FILE: c_int = 0x000100;
pub const GUC_CUSTOM_PLACEHOLDER: c_int = 0x000200;
pub const GUC_SUPERUSER_ONLY: c_int = 0x000400;
pub const GUC_IS_NAME: c_int = 0x000800;
pub const GUC_NOT_WHILE_SEC_REST: c_int = 0x001000;
pub const GUC_DISALLOW_IN_AUTO_FILE: c_int = 0x002000;
pub const GUC_RUNTIME_COMPUTED: c_int = 0x004000;
pub const GUC_ALLOW_IN_PARALLEL: c_int = 0x008000;
pub const GUC_UNIT_KB: c_int = 0x01000000;
pub const GUC_UNIT_BLOCKS: c_int = 0x02000000;
pub const GUC_UNIT_XBLOCKS: c_int = 0x03000000;
pub const GUC_UNIT_MB: c_int = 0x04000000;
pub const GUC_UNIT_BYTE: c_int = 0x05000000;
pub const GUC_UNIT_MEMORY: c_int = 0x0F000000;
pub const GUC_UNIT_MS: c_int = 0x10000000;
pub const GUC_UNIT_S: c_int = 0x20000000;
pub const GUC_UNIT_MIN: c_int = 0x30000000;
pub const GUC_UNIT_TIME: c_int = 0x70000000;
pub const GUC_UNIT: c_int = GUC_UNIT_MEMORY | GUC_UNIT_TIME;

pub const GUC_IS_IN_FILE: c_int = 0x0001;
pub const GUC_PENDING_RESTART: c_int = 0x0002;
pub const GUC_NEEDS_REPORT: c_int = 0x0004;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ConfigVariable {
    pub name: *mut c_char,
    pub value: *mut c_char,
    pub errmsg: *mut c_char,
    pub filename: *mut c_char,
    pub sourceline: c_int,
    pub ignore: bool,
    pub applied: bool,
    pub next: *mut ConfigVariable,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct dlist_node {
    pub prev: *mut dlist_node,
    pub next: *mut dlist_node,
}

impl dlist_node {
    pub const fn new() -> Self {
        Self {
            prev: core::ptr::null_mut(),
            next: core::ptr::null_mut(),
        }
    }
}

impl Default for dlist_node {
    fn default() -> Self {
        Self::new()
    }
}

/// Head of a doubly linked list (`dlist_head` in `src/include/lib/ilist.h`).
///
/// Non-empty lists are internally circularly linked; an empty list may be
/// represented either as a pair of NULL pointers (zero-initialized) or as a
/// circular list whose `head.next`/`head.prev` both point back to `head`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct dlist_head {
    pub head: dlist_node,
}

impl dlist_head {
    pub const fn new() -> Self {
        Self {
            head: dlist_node::new(),
        }
    }
}

impl Default for dlist_head {
    fn default() -> Self {
        Self::new()
    }
}

/// Doubly linked list iterator for `dlist_head`/`dclist_head`
/// (`dlist_iter` in `src/include/lib/ilist.h`).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct dlist_iter {
    pub cur: *mut dlist_node,
    pub end: *mut dlist_node,
}

/// Doubly linked list mutable iterator (`dlist_mutable_iter`).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct dlist_mutable_iter {
    pub cur: *mut dlist_node,
    pub next: *mut dlist_node,
    pub end: *mut dlist_node,
}

/// Head of a doubly linked list with an item count (`dclist_head`).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct dclist_head {
    pub dlist: dlist_head,
    pub count: u32,
}

impl dclist_head {
    pub const fn new() -> Self {
        Self {
            dlist: dlist_head::new(),
            count: 0,
        }
    }
}

impl Default for dclist_head {
    fn default() -> Self {
        Self::new()
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct slist_node {
    pub next: *mut slist_node,
}

impl slist_node {
    pub const fn new() -> Self {
        Self {
            next: core::ptr::null_mut(),
        }
    }
}

impl Default for slist_node {
    fn default() -> Self {
        Self::new()
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct slist_head {
    pub head: slist_node,
}

impl slist_head {
    pub const fn new() -> Self {
        Self {
            head: slist_node::new(),
        }
    }
}

impl Default for slist_head {
    fn default() -> Self {
        Self::new()
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct slist_iter {
    pub cur: *mut slist_node,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct slist_mutable_iter {
    pub cur: *mut slist_node,
    pub next: *mut slist_node,
    pub prev: *mut slist_node,
}

// Compile-time layout assertions for the intrusive-list ABI structs, matching
// `src/include/lib/ilist.h`.  A pointer is one machine word; `uint32 count` in
// `dclist_head` sits after the two-word `dlist_head` and pads the struct out to
// three words.  Keyed to `size_of::<usize>()` so they hold on any target width.
const _ILIST_WORD: usize = core::mem::size_of::<usize>();
const _: () = assert!(core::mem::size_of::<dlist_node>() == 2 * _ILIST_WORD);
const _: () =
    assert!(core::mem::align_of::<dlist_node>() == core::mem::align_of::<*mut dlist_node>());
const _: () = assert!(core::mem::size_of::<dlist_head>() == 2 * _ILIST_WORD);
const _: () = assert!(core::mem::size_of::<dlist_iter>() == 2 * _ILIST_WORD);
const _: () = assert!(core::mem::size_of::<dlist_mutable_iter>() == 3 * _ILIST_WORD);
// dclist_head: dlist_head (2 words) + u32 count, padded out to 3 words.
const _: () = assert!(core::mem::size_of::<dclist_head>() == 3 * _ILIST_WORD);
const _: () = assert!(core::mem::offset_of!(dclist_head, count) == 2 * _ILIST_WORD);
const _: () = assert!(core::mem::size_of::<slist_node>() == _ILIST_WORD);
const _: () = assert!(core::mem::size_of::<slist_head>() == _ILIST_WORD);
const _: () = assert!(core::mem::size_of::<slist_iter>() == _ILIST_WORD);
const _: () = assert!(core::mem::size_of::<slist_mutable_iter>() == 3 * _ILIST_WORD);

#[repr(C)]
#[derive(Clone, Copy)]
pub union config_var_val {
    pub boolval: bool,
    pub intval: c_int,
    pub realval: c_double,
    pub stringval: *mut c_char,
    pub enumval: c_int,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct config_var_value {
    pub val: config_var_val,
    pub extra: *mut c_void,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct GucStack {
    pub prev: *mut GucStack,
    pub nest_level: c_int,
    pub state: GucStackState,
    pub source: GucSource,
    pub scontext: GucContext,
    pub masked_scontext: GucContext,
    pub srole: Oid,
    pub masked_srole: Oid,
    pub prior: config_var_value,
    pub masked: config_var_value,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct config_generic {
    pub name: *const c_char,
    pub context: GucContext,
    pub group: config_group,
    pub short_desc: *const c_char,
    pub long_desc: *const c_char,
    pub flags: c_int,
    pub vartype: config_type,
    pub status: c_int,
    pub source: GucSource,
    pub reset_source: GucSource,
    pub scontext: GucContext,
    pub reset_scontext: GucContext,
    pub srole: Oid,
    pub reset_srole: Oid,
    pub stack: *mut GucStack,
    pub extra: *mut c_void,
    pub nondef_link: dlist_node,
    pub stack_link: slist_node,
    pub report_link: slist_node,
    pub last_reported: *mut c_char,
    pub sourcefile: *mut c_char,
    pub sourceline: c_int,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct config_enum_entry {
    pub name: *const c_char,
    pub val: c_int,
    pub hidden: bool,
}

pub type GucBoolCheckHook =
    Option<unsafe extern "C" fn(*mut bool, *mut *mut c_void, GucSource) -> bool>;
pub type GucIntCheckHook =
    Option<unsafe extern "C" fn(*mut c_int, *mut *mut c_void, GucSource) -> bool>;
pub type GucRealCheckHook =
    Option<unsafe extern "C" fn(*mut c_double, *mut *mut c_void, GucSource) -> bool>;
pub type GucStringCheckHook =
    Option<unsafe extern "C" fn(*mut *mut c_char, *mut *mut c_void, GucSource) -> bool>;
pub type GucEnumCheckHook =
    Option<unsafe extern "C" fn(*mut c_int, *mut *mut c_void, GucSource) -> bool>;
pub type GucBoolAssignHook = Option<unsafe extern "C" fn(bool, *mut c_void)>;
pub type GucIntAssignHook = Option<unsafe extern "C" fn(c_int, *mut c_void)>;
pub type GucRealAssignHook = Option<unsafe extern "C" fn(c_double, *mut c_void)>;
pub type GucStringAssignHook = Option<unsafe extern "C" fn(*const c_char, *mut c_void)>;
pub type GucEnumAssignHook = Option<unsafe extern "C" fn(c_int, *mut c_void)>;
pub type GucShowHook = Option<unsafe extern "C" fn() -> *const c_char>;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct config_bool {
    pub gen: config_generic,
    pub variable: *mut bool,
    pub boot_val: bool,
    pub check_hook: GucBoolCheckHook,
    pub assign_hook: GucBoolAssignHook,
    pub show_hook: GucShowHook,
    pub reset_val: bool,
    pub reset_extra: *mut c_void,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct config_int {
    pub gen: config_generic,
    pub variable: *mut c_int,
    pub boot_val: c_int,
    pub min: c_int,
    pub max: c_int,
    pub check_hook: GucIntCheckHook,
    pub assign_hook: GucIntAssignHook,
    pub show_hook: GucShowHook,
    pub reset_val: c_int,
    pub reset_extra: *mut c_void,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct config_real {
    pub gen: config_generic,
    pub variable: *mut c_double,
    pub boot_val: c_double,
    pub min: c_double,
    pub max: c_double,
    pub check_hook: GucRealCheckHook,
    pub assign_hook: GucRealAssignHook,
    pub show_hook: GucShowHook,
    pub reset_val: c_double,
    pub reset_extra: *mut c_void,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct config_string {
    pub gen: config_generic,
    pub variable: *mut *mut c_char,
    pub boot_val: *const c_char,
    pub check_hook: GucStringCheckHook,
    pub assign_hook: GucStringAssignHook,
    pub show_hook: GucShowHook,
    pub reset_val: *mut c_char,
    pub reset_extra: *mut c_void,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct config_enum {
    pub gen: config_generic,
    pub variable: *mut c_int,
    pub boot_val: c_int,
    pub options: *const config_enum_entry,
    pub check_hook: GucEnumCheckHook,
    pub assign_hook: GucEnumAssignHook,
    pub show_hook: GucShowHook,
    pub reset_val: c_int,
    pub reset_extra: *mut c_void,
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{align_of, offset_of, size_of};

    #[test]
    fn guc_enum_values_match_postgres() {
        assert_eq!(PGC_INTERNAL, 0);
        assert_eq!(PGC_USERSET, 6);
        assert_eq!(PGC_S_DEFAULT, 0);
        assert_eq!(PGC_S_SESSION, 13);
        assert_eq!(PGC_ENUM, 4);
        assert_eq!(DEVELOPER_OPTIONS, 47);
        assert_eq!(GUC_DISALLOW_IN_FILE, 0x000100);
        assert_eq!(GUC_UNIT_MEMORY, 0x0F000000);
    }

    #[test]
    fn guc_config_layout_matches_generated_macos_shape() {
        assert_eq!(
            (
                size_of::<ConfigVariable>(),
                size_of::<dlist_node>(),
                size_of::<slist_node>(),
                size_of::<config_var_val>(),
                size_of::<config_var_value>(),
                size_of::<GucStack>(),
                size_of::<config_generic>()
            ),
            (48, 16, 8, 8, 16, 72, 144)
        );
        assert_eq!(
            (
                offset_of!(ConfigVariable, value),
                offset_of!(ConfigVariable, sourceline),
                offset_of!(ConfigVariable, next),
                offset_of!(config_generic, context),
                offset_of!(config_generic, group),
                offset_of!(config_generic, vartype),
                offset_of!(config_generic, stack),
                offset_of!(config_generic, nondef_link),
                offset_of!(config_generic, sourceline)
            ),
            (8, 32, 40, 8, 12, 36, 72, 88, 136)
        );
        assert_eq!(align_of::<ConfigVariable>(), 8);
        assert_eq!(size_of::<dlist_node>(), 16);
        assert_eq!(size_of::<slist_node>(), 8);
        assert_eq!(size_of::<config_var_val>(), 8);
        assert_eq!(size_of::<config_var_value>(), 16);
        assert_eq!(size_of::<GucStack>(), 72);
        assert_eq!(size_of::<config_generic>(), 144);
        assert_eq!(align_of::<config_generic>(), 8);
        assert_eq!(offset_of!(config_generic, name), 0);
        assert_eq!(offset_of!(config_generic, context), 8);
        assert_eq!(offset_of!(config_generic, group), 12);
        assert_eq!(offset_of!(config_generic, vartype), 36);
        assert_eq!(offset_of!(config_generic, stack), 72);
        assert_eq!(offset_of!(config_generic, nondef_link), 88);
        assert_eq!(offset_of!(config_generic, sourceline), 136);
    }

    #[test]
    fn guc_typed_config_layouts_match_generated_macos_shape() {
        assert_eq!(
            (
                size_of::<config_bool>(),
                size_of::<config_int>(),
                size_of::<config_real>(),
                size_of::<config_string>(),
                size_of::<config_enum>()
            ),
            (200, 208, 216, 200, 208)
        );
        assert_eq!(
            (
                offset_of!(config_bool, variable),
                offset_of!(config_int, min),
                offset_of!(config_real, max),
                offset_of!(config_string, reset_val),
                offset_of!(config_enum, options)
            ),
            (144, 156, 168, 184, 160)
        );
        assert_eq!(size_of::<config_bool>(), 200);
        assert_eq!(size_of::<config_int>(), 208);
        assert_eq!(size_of::<config_real>(), 216);
        assert_eq!(size_of::<config_string>(), 200);
        assert_eq!(size_of::<config_enum>(), 208);
        assert_eq!(offset_of!(config_bool, variable), 144);
        assert_eq!(offset_of!(config_int, min), 156);
        assert_eq!(offset_of!(config_real, max), 168);
        assert_eq!(offset_of!(config_string, reset_val), 184);
        assert_eq!(offset_of!(config_enum, options), 160);
    }
}
