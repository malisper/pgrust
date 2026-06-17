//! Port of `src/backend/utils/activity/wait_event.c` and
//! `wait_event_funcs.c` (PostgreSQL 18.3) — wait-event reporting
//! infrastructure.
//!
//! # What this crate owns
//!
//! * The wait-event *class* / *type* taxonomy (`pgstat_get_wait_event_type`)
//!   — pure bit-mask logic over `wait_event_info`.
//! * The built-in wait-event name/description table. PostgreSQL generates the
//!   per-class `pgstat_get_wait_<class>()` accessors and
//!   `wait_event_funcs_data.c` at build time from `wait_event_names.txt`. We
//!   embed the same canonical data file and parse it once into the same
//!   tables, the idiomatic equivalent of those generated switch statements.
//! * The wait-event *reporting storage* (`pgstat_report_wait_start` / `_end` /
//!   `pgstat_set_wait_event_storage` / `pgstat_reset_wait_event_storage`). In C
//!   `my_wait_event_info` starts as a process-local `uint32` and is later
//!   redirected at a shared-memory slot in `MyProc`. We model the same redirect
//!   with a thread-local default slot plus an installable shared slot.
//! * The custom (extension / injection-point) wait-event registry: the
//!   spinlock-guarded ID counter and the two shared-memory dynahash tables
//!   (`WaitEventCustomHashByName`, `WaitEventCustomHashByInfo`) protected by
//!   `WaitEventCustomLock`, plus the dedup / class-conflict / limit logic of
//!   `WaitEventCustomNew`.
//!
//! # Genuine externals (other crates' seams)
//!
//! `pgstat_get_wait_event` dispatches LWLock and heavyweight-lock names to two
//! functions owned by other subsystems:
//!
//! * `GetLWLockIdentifier` (`storage/lmgr/lwlock.c`) via
//!   `backend_storage_lmgr_lwlock_seams::get_lwlock_identifier`.
//! * `GetLockNameFromTagType` (`storage/lmgr/lmgr.c`) via
//!   `backend_storage_lmgr_lmgr_seams::get_lock_name_from_tag_type`.
//!
//! The custom store's shared-memory primitives (`ShmemInitStruct` /
//! `ShmemInitHash`, `hash_search` / `hash_seq_*`, `LWLockAcquire` of
//! `WaitEventCustomLock`) are direct dependencies on the ported owner crates.

#![allow(non_snake_case)]

use std::borrow::Cow;
use std::cell::{Cell, RefCell};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};

use backend_storage_ipc_shmem::{ShmemInitHash, ShmemInitStruct};
use backend_storage_lmgr_lmgr_seams::get_lock_name_from_tag_type;
use backend_storage_lmgr_lwlock_seams::{get_lwlock_identifier, lwlock_acquire_main};
use backend_storage_lmgr_s_lock::{s_init_lock, s_lock_macro, s_unlock, Spinlock};
use backend_utils_error::{elog, ereport};
use backend_utils_hash_dynahash_seams::{
    hash_estimate_size, hash_get_num_entries, hash_search, hash_seq_init, hash_seq_search,
};
use types_core::{uint16, uint32, Size};
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_DUPLICATE_OBJECT, ERRCODE_INTERNAL_ERROR,
    ERRCODE_OUT_OF_MEMORY, ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERROR,
};
use types_hash::hsearch::{
    HASHACTION, HASHCTL, HASH_BLOBS, HASH_ELEM, HASH_SEQ_STATUS, HASH_STRINGS, HTAB,
};
use types_pgstat::wait_event::{
    PG_WAIT_ACTIVITY, PG_WAIT_BUFFERPIN, PG_WAIT_CLIENT, PG_WAIT_EXTENSION, PG_WAIT_INJECTIONPOINT,
    PG_WAIT_IO, PG_WAIT_IPC, PG_WAIT_LOCK, PG_WAIT_LWLOCK, PG_WAIT_TIMEOUT,
    WAIT_EVENT_CLASS_MASK, WAIT_EVENT_CUSTOM_HASH_INIT_SIZE, WAIT_EVENT_CUSTOM_HASH_MAX_SIZE,
    WAIT_EVENT_CUSTOM_INITIAL_ID, WAIT_EVENT_ID_MASK,
};
use types_storage::{LW_EXCLUSIVE, LW_SHARED, WAIT_EVENT_CUSTOM_LOCK};

const SRCFILE: &str = "wait_event.c";

fn loc(funcname: &str) -> ErrorLocation {
    ErrorLocation::new(SRCFILE, 0, funcname)
}

/// `NAMEDATALEN` — the maximum length (including the terminating NUL in C) of a
/// custom wait-event name.
const NAMEDATALEN: usize = types_core::NAMEDATALEN as usize;

/// `MAXALIGN(LEN)` (`c.h`): round up to the maximum alignment (8 bytes on the
/// 64-bit migration profile).
const fn maxalign(len: usize) -> usize {
    const MAXIMUM_ALIGNOF: usize = 8;
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

/// The canonical wait-event catalog, identical to
/// `postgres-18.3/src/backend/utils/activity/wait_event_names.txt`.
const WAIT_EVENT_NAMES: &str = include_str!("wait_event_names.txt");

// ---------------------------------------------------------------------------
// Wait-event class / type taxonomy.
// ---------------------------------------------------------------------------

/// The wait-event class extracted from a `wait_event_info` value, as named by
/// `pgstat_get_wait_event_type`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WaitEventClass {
    LWLock,
    Lock,
    BufferPin,
    Activity,
    Client,
    Extension,
    Ipc,
    Timeout,
    Io,
    InjectionPoint,
    /// Unrecognized class (the C `default: "???"` arm).
    Unknown,
}

impl WaitEventClass {
    /// The type string reported by `pgstat_get_wait_event_type`.
    pub fn type_name(self) -> &'static str {
        match self {
            WaitEventClass::LWLock => "LWLock",
            WaitEventClass::Lock => "Lock",
            WaitEventClass::BufferPin => "BufferPin",
            WaitEventClass::Activity => "Activity",
            WaitEventClass::Client => "Client",
            WaitEventClass::Extension => "Extension",
            WaitEventClass::Ipc => "IPC",
            WaitEventClass::Timeout => "Timeout",
            WaitEventClass::Io => "IO",
            WaitEventClass::InjectionPoint => "InjectionPoint",
            WaitEventClass::Unknown => "???",
        }
    }
}

/// Decode the class of a `wait_event_info` value (the high byte under
/// `WAIT_EVENT_CLASS_MASK`).
pub fn wait_event_class(wait_event_info: uint32) -> WaitEventClass {
    match wait_event_info & WAIT_EVENT_CLASS_MASK {
        PG_WAIT_LWLOCK => WaitEventClass::LWLock,
        PG_WAIT_LOCK => WaitEventClass::Lock,
        PG_WAIT_BUFFERPIN => WaitEventClass::BufferPin,
        PG_WAIT_ACTIVITY => WaitEventClass::Activity,
        PG_WAIT_CLIENT => WaitEventClass::Client,
        PG_WAIT_EXTENSION => WaitEventClass::Extension,
        PG_WAIT_IPC => WaitEventClass::Ipc,
        PG_WAIT_TIMEOUT => WaitEventClass::Timeout,
        PG_WAIT_IO => WaitEventClass::Io,
        PG_WAIT_INJECTIONPOINT => WaitEventClass::InjectionPoint,
        _ => WaitEventClass::Unknown,
    }
}

/// `pgstat_get_wait_event_type()` — the type string for a wait event, or `None`
/// when the backend is not waiting (`wait_event_info == 0`).
pub fn pgstat_get_wait_event_type(wait_event_info: uint32) -> Option<&'static str> {
    if wait_event_info == 0 {
        return None;
    }
    Some(wait_event_class(wait_event_info).type_name())
}

// ---------------------------------------------------------------------------
// Built-in wait-event data, generated from wait_event_names.txt.
//
// PostgreSQL's build runs `generate-wait_event_types.pl` over this file to emit
// two artifacts this crate reproduces at runtime:
//
//   * `pgstat_wait_event.c` — the per-class `pgstat_get_wait_<class>()` name
//     lookups and the `WAIT_EVENT_*` enum whose values are
//     `PG_WAIT_<CLASS> | id`. The `id` is the position of the event in the
//     case-insensitively sorted list of that class, so we must sort before
//     assigning ids — file order is not enum order. Only the six classes that
//     get a generated lookup (Activity, BufferPin, Client, IPC, Timeout, IO)
//     participate; LWLock/Lock/Extension/InjectionPoint ids live elsewhere.
//
//   * `wait_event_funcs_data.c` — the `(type, name, description)` table fed to
//     `pg_get_wait_events()`. This enumerates every class, case-insensitively
//     sorted within each class, with each class sorted by typedef name. The
//     descriptions are post-processed (drop the surrounding quotes and trailing
//     period, expand `<quote>`, strip SGML tags, rewrite GUC `<xref>`s, drop
//     "; see ...").
// ---------------------------------------------------------------------------

/// One row of the wait-event name lookup used by `pgstat_get_wait_event`,
/// mirroring the generated `pgstat_get_wait_<class>()` switch arms. Only the six
/// classes that get a generated lookup appear here.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WaitEventData {
    pub type_: &'static str,
    pub name: &'static str,
    pub wait_event_info: uint32,
}

/// One parsed wait-event line: its class and the raw, unprocessed name +
/// description fields exactly as they appear in `wait_event_names.txt`.
struct RawWaitEvent {
    class: WaitEventGenClass,
    /// Column 1 (the typedef-enum symbol / verbatim name).
    symbol: String,
    /// Column 2, including its surrounding double quotes and trailing period.
    doc: String,
}

/// The wait-event classes as named by the generator's `Section:` headers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WaitEventGenClass {
    Activity,
    BufferPin,
    Client,
    Extension,
    Io,
    Ipc,
    Lock,
    LWLock,
    Timeout,
    InjectionPoint,
}

impl WaitEventGenClass {
    /// Parse a `Section: ClassName - <Name>` header's class token.
    fn from_section(section: &str) -> Option<Self> {
        Some(match section {
            "WaitEventActivity" => WaitEventGenClass::Activity,
            "WaitEventBufferPin" => WaitEventGenClass::BufferPin,
            "WaitEventClient" => WaitEventGenClass::Client,
            "WaitEventExtension" => WaitEventGenClass::Extension,
            "WaitEventIO" => WaitEventGenClass::Io,
            "WaitEventIPC" => WaitEventGenClass::Ipc,
            "WaitEventLock" => WaitEventGenClass::Lock,
            "WaitEventLWLock" => WaitEventGenClass::LWLock,
            "WaitEventTimeout" => WaitEventGenClass::Timeout,
            "WaitEventInjectionPoint" => WaitEventGenClass::InjectionPoint,
            _ => return None,
        })
    }

    /// The `$last` token (`$waitclass =~ s/^WaitEvent//`) the generator emits as
    /// the `type` column of `wait_event_funcs_data.c`.
    fn type_name(self) -> &'static str {
        match self {
            WaitEventGenClass::Activity => "Activity",
            WaitEventGenClass::BufferPin => "BufferPin",
            WaitEventGenClass::Client => "Client",
            WaitEventGenClass::Extension => "Extension",
            WaitEventGenClass::Io => "IO",
            WaitEventGenClass::Ipc => "IPC",
            WaitEventGenClass::Lock => "Lock",
            WaitEventGenClass::LWLock => "LWLock",
            WaitEventGenClass::Timeout => "Timeout",
            WaitEventGenClass::InjectionPoint => "InjectionPoint",
        }
    }

    /// The `PG_WAIT_<CLASS>` base id, for the six classes whose `WAIT_EVENT_*`
    /// enum is generated from this file. LWLock/Lock/Extension/InjectionPoint
    /// return `None`: their ids live in other subsystems.
    fn class_base(self) -> Option<uint32> {
        Some(match self {
            WaitEventGenClass::Activity => PG_WAIT_ACTIVITY,
            WaitEventGenClass::BufferPin => PG_WAIT_BUFFERPIN,
            WaitEventGenClass::Client => PG_WAIT_CLIENT,
            WaitEventGenClass::Io => PG_WAIT_IO,
            WaitEventGenClass::Ipc => PG_WAIT_IPC,
            WaitEventGenClass::Timeout => PG_WAIT_TIMEOUT,
            WaitEventGenClass::Extension
            | WaitEventGenClass::InjectionPoint
            | WaitEventGenClass::Lock
            | WaitEventGenClass::LWLock => return None,
        })
    }

    /// Whether this class keeps its name verbatim (`$waiteventdescription =
    /// $waiteventname`) instead of being CamelCased. True only for LWLock and
    /// Lock.
    fn name_is_verbatim(self) -> bool {
        matches!(self, WaitEventGenClass::LWLock | WaitEventGenClass::Lock)
    }

    /// The `WaitEvent<Class>` typedef name used as the sort key for class
    /// ordering.
    fn section_typedef(self) -> &'static str {
        match self {
            WaitEventGenClass::Activity => "WaitEventActivity",
            WaitEventGenClass::BufferPin => "WaitEventBufferPin",
            WaitEventGenClass::Client => "WaitEventClient",
            WaitEventGenClass::Extension => "WaitEventExtension",
            WaitEventGenClass::Io => "WaitEventIO",
            WaitEventGenClass::Ipc => "WaitEventIPC",
            WaitEventGenClass::Lock => "WaitEventLock",
            WaitEventGenClass::LWLock => "WaitEventLWLock",
            WaitEventGenClass::Timeout => "WaitEventTimeout",
            WaitEventGenClass::InjectionPoint => "WaitEventInjectionPoint",
        }
    }
}

/// Parse the embedded `wait_event_names.txt`, replicating the comment/blank/
/// `ABI_compatibility:`/`Section:` handling of `generate-wait_event_types.pl`'s
/// first pass. ABI-compatibility lines are kept in file order (the generator
/// appends them after sorting); all other lines are returned for sorting by the
/// callers.
fn parse_raw_wait_events() -> (Vec<RawWaitEvent>, Vec<RawWaitEvent>) {
    let mut lines: Vec<RawWaitEvent> = Vec::new();
    let mut abi_lines: Vec<RawWaitEvent> = Vec::new();
    let mut current_class: Option<WaitEventGenClass> = None;
    let mut abi_compatibility = false;

    for raw_line in WAIT_EVENT_NAMES.lines() {
        if raw_line.starts_with('#') || raw_line.trim().is_empty() {
            continue;
        }

        if let Some(rest) = raw_line.strip_prefix("Section: ClassName") {
            // `$section_name =~ s/^.*- //` — take the text after the last "- ".
            let name = rest.rsplit_once("- ").map(|(_, n)| n).unwrap_or(rest).trim();
            current_class = WaitEventGenClass::from_section(name);
            abi_compatibility = false;
            continue;
        }

        if raw_line.trim() == "ABI_compatibility:" {
            abi_compatibility = true;
            continue;
        }

        let Some(class) = current_class else {
            continue;
        };
        let Some((symbol, doc)) = raw_line.split_once('\t') else {
            continue;
        };
        let event = RawWaitEvent {
            class,
            symbol: symbol.trim().to_owned(),
            doc: doc.trim().to_owned(),
        };
        if abi_compatibility {
            abi_lines.push(event);
        } else {
            lines.push(event);
        }
    }

    (lines, abi_lines)
}

/// One class bucket of the generator's `%hashwe`: a class and its ordered list
/// of events.
struct ClassGroup {
    class: WaitEventGenClass,
    events: Vec<RawWaitEvent>,
}

/// The generator's `%hashwe`: events grouped by class, each class's list in the
/// order the generator stores them (sorted by name case-insensitively, then ABI
/// lines appended in file order).
fn grouped_wait_events() -> Vec<ClassGroup> {
    let (mut lines, abi_lines) = parse_raw_wait_events();

    // Sort by the second column (name) case-insensitively, matching
    // `sort { uc(col2) cmp uc(col2) }`.
    lines.sort_by(|a, b| {
        a.symbol
            .to_ascii_uppercase()
            .cmp(&b.symbol.to_ascii_uppercase())
    });

    // ABI lines are appended after sorting, in file order.
    lines.extend(abi_lines);

    // Bucket into per-class lists, preserving the just-computed order.
    let mut groups: Vec<ClassGroup> = Vec::new();
    for event in lines {
        match groups.iter_mut().find(|g| g.class == event.class) {
            Some(group) => group.events.push(event),
            None => {
                let class = event.class;
                groups.push(ClassGroup {
                    class,
                    events: vec![event],
                });
            }
        }
    }
    groups
}

/// Iterate the classes in the generator's emission order
/// (`sort { uc($a) cmp uc($b) }` over the class typedef names).
fn classes_sorted(groups: &[ClassGroup]) -> Vec<&ClassGroup> {
    let mut classes: Vec<&ClassGroup> = groups.iter().collect();
    classes.sort_by(|a, b| {
        a.class
            .section_typedef()
            .to_ascii_uppercase()
            .cmp(&b.class.section_typedef().to_ascii_uppercase())
    });
    classes
}

/// The wait-event *name* (`$wev->[1]`) the generator computes: verbatim for
/// LWLock/Lock, CamelCase of the UPPER_SNAKE symbol otherwise.
fn wait_event_name(event: &RawWaitEvent) -> String {
    if event.class.name_is_verbatim() {
        event.symbol.clone()
    } else {
        wait_event_name_from_symbol(&event.symbol)
    }
}

/// The name lookup used by `pgstat_get_wait_event` (`pgstat_get_wait_<class>`),
/// computed once. Maps `wait_event_info` -> `(type, name)` for the six classes
/// whose enum is generated from this file. Ids are assigned in the sorted order
/// of each class (the enum order), the first member taking `PG_WAIT_<CLASS>`.
pub fn wait_event_data() -> &'static [WaitEventData] {
    static DATA: OnceLock<Vec<WaitEventData>> = OnceLock::new();
    DATA.get_or_init(build_name_lookup)
}

fn build_name_lookup() -> Vec<WaitEventData> {
    let groups = grouped_wait_events();
    let mut rows: Vec<WaitEventData> = Vec::new();
    for group in &groups {
        let Some(base) = group.class.class_base() else {
            // LWLock/Lock/Extension/InjectionPoint ids are not generated here.
            continue;
        };
        for (id, event) in group.events.iter().enumerate() {
            rows.push(WaitEventData {
                type_: group.class.type_name(),
                name: leak_string(wait_event_name(event)),
                wait_event_info: base | id as uint32,
            });
        }
    }
    rows
}

/// Find the built-in name-lookup row for an exact `wait_event_info`.
pub fn wait_event_data_by_info(wait_event_info: uint32) -> Option<&'static WaitEventData> {
    wait_event_data()
        .iter()
        .find(|row| row.wait_event_info == wait_event_info)
}

/// Convert an UPPER_SNAKE symbol into PostgreSQL's CamelCase wait-event name,
/// matching the generator (`substr($part,0,1) . lc(substr($part,1))` per
/// `_`-split part).
fn wait_event_name_from_symbol(symbol: &str) -> String {
    let mut out = String::new();
    for part in symbol.split('_') {
        if part.is_empty() {
            continue;
        }
        let mut chars = part.chars();
        if let Some(first) = chars.next() {
            out.push(first.to_ascii_uppercase());
            for ch in chars {
                out.push(ch.to_ascii_lowercase());
            }
        }
    }
    out
}

/// Process one wait-event doc string into the description column of
/// `wait_event_funcs_data.c`, replicating the generator's transforms in order:
///
/// 1. `substr $desc, 1, -2` — drop the leading `"` and the trailing `."`.
/// 2. (C only) escape `'`; not a content change, skipped here.
/// 3. `<quote>X</quote>` -> `"X"`.
/// 4. `<tag>X</tag>` -> `X` (strip a paired SGML markup).
/// 5. rewrite `<xref linkend="guc-foo-bar"/>` -> `foo_bar`.
/// 6. drop a trailing `; see ...`.
fn process_description(doc: &str) -> String {
    // Step 1: substr($desc, 1, -2). `doc` includes the surrounding quotes and
    // trailing period; drop the first char and the last two on char boundaries.
    let mut step1 = String::new();
    let n = doc.chars().count();
    if n >= 3 {
        for ch in doc.chars().skip(1).take(n - 3) {
            step1.push(ch);
        }
    }

    // Step 3: <quote>X</quote> -> "X".
    let step3 = replace_quote_markup(&step1);
    // Step 4: <tag>X</tag> -> X (non-greedy paired markup strip).
    let step4 = strip_sgml_markup(&step3);
    // Step 5: rewrite GUC xrefs.
    let desc = rewrite_guc_xrefs(&step4);
    // Step 6: drop "; see ..." to end of string.
    let cut = desc.find("; see").unwrap_or(desc.len());
    desc[..cut].to_owned()
}

/// `$new_desc =~ s/<quote>(.*?)<\/quote>/\\"$1\\"/g` — wrap quoted spans in
/// literal double quotes.
fn replace_quote_markup(input: &str) -> String {
    let mut out = String::new();
    let mut rest = input;
    while let Some(start) = rest.find("<quote>") {
        let after_open = &rest[start + "<quote>".len()..];
        if let Some(end) = after_open.find("</quote>") {
            out.push_str(&rest[..start]);
            out.push('"');
            out.push_str(&after_open[..end]);
            out.push('"');
            rest = &after_open[end + "</quote>".len()..];
        } else {
            break;
        }
    }
    out.push_str(rest);
    out
}

/// `$new_desc =~ s/<.*?>(.*?)<.*?>/$1/g` — non-greedy strip of a paired SGML
/// markup `<open>middle<close>`, keeping `middle`.
fn strip_sgml_markup(input: &str) -> String {
    let mut out = String::new();
    let mut rest = input;
    loop {
        let Some(open_start) = rest.find('<') else {
            break;
        };
        let after_open_lt = &rest[open_start + 1..];
        let Some(open_gt_rel) = after_open_lt.find('>') else {
            break;
        };
        let middle_start = open_start + 1 + open_gt_rel + 1;
        let middle_and_rest = &rest[middle_start..];
        let Some(close_lt_rel) = middle_and_rest.find('<') else {
            break;
        };
        let middle = &middle_and_rest[..close_lt_rel];
        let after_close_lt = &middle_and_rest[close_lt_rel + 1..];
        let Some(close_gt_rel) = after_close_lt.find('>') else {
            break;
        };
        out.push_str(&rest[..open_start]);
        out.push_str(middle);
        rest = &after_close_lt[close_gt_rel + 1..];
    }
    out.push_str(rest);
    out
}

/// Replicate the GUC-xref rewriting:
/// `<xref linkend="guc-foo-bar"/>` -> `foo_bar`.
fn rewrite_guc_xrefs(input: &str) -> String {
    const PREFIX: &str = "<xref linkend=\"guc-";
    const SUFFIX: &str = "\"/>";
    let mut out = String::new();
    let mut rest = input;
    while let Some(start) = rest.find(PREFIX) {
        let after_prefix = &rest[start + PREFIX.len()..];
        if let Some(end) = after_prefix.find(SUFFIX) {
            out.push_str(&rest[..start]);
            for ch in after_prefix[..end].chars() {
                out.push(if ch == '-' { '_' } else { ch });
            }
            rest = &after_prefix[end + SUFFIX.len()..];
        } else {
            break;
        }
    }
    out.push_str(rest);
    out
}

/// Promote a parsed name/description to a process-lifetime `&'static str`. The
/// built-in tables are computed exactly once (`OnceLock`) and live for the life
/// of the process, mirroring the C string literals they replace.
fn leak_string(value: String) -> &'static str {
    Box::leak(value.into_boxed_str())
}

// ---------------------------------------------------------------------------
// pgstat_get_wait_event().
// ---------------------------------------------------------------------------

/// `pgstat_get_wait_event()` — the human-readable name for a wait event, or
/// `None` when the backend is not waiting.
pub fn pgstat_get_wait_event(wait_event_info: uint32) -> PgResult<Option<Cow<'static, str>>> {
    if wait_event_info == 0 {
        return Ok(None);
    }

    let class_id = wait_event_info & WAIT_EVENT_CLASS_MASK;
    let event_id = (wait_event_info & WAIT_EVENT_ID_MASK) as uint16;

    let name = match class_id {
        PG_WAIT_LWLOCK => Cow::Borrowed(get_lwlock_identifier::call(class_id, event_id)),
        PG_WAIT_LOCK => Cow::Borrowed(get_lock_name_from_tag_type::call(event_id)),
        PG_WAIT_EXTENSION | PG_WAIT_INJECTIONPOINT => GetWaitEventCustomIdentifier(wait_event_info)?,
        PG_WAIT_BUFFERPIN | PG_WAIT_ACTIVITY | PG_WAIT_CLIENT | PG_WAIT_IPC | PG_WAIT_TIMEOUT
        | PG_WAIT_IO => wait_event_data_by_info(wait_event_info)
            .map(|row| Cow::Borrowed(row.name))
            .unwrap_or(Cow::Borrowed("unknown wait event")),
        _ => Cow::Borrowed("unknown wait event"),
    };

    Ok(Some(name))
}

// ---------------------------------------------------------------------------
// Wait-event reporting storage (my_wait_event_info redirect).
// ---------------------------------------------------------------------------

/// A redirectable storage slot for the current wait event, standing in for the
/// shared-memory `uint32` in `MyProc` that `pgstat_set_wait_event_storage`
/// points `my_wait_event_info` at. The four-byte field is read and written
/// atomically, matching the `volatile uint32` access in C.
#[derive(Clone, Default)]
pub struct WaitEventStorage {
    value: Arc<AtomicU32>,
}

impl WaitEventStorage {
    pub fn new() -> Self {
        Self::default()
    }

    /// The currently stored `wait_event_info`.
    pub fn get(&self) -> uint32 {
        self.value.load(Ordering::Relaxed)
    }

    /// Store a `wait_event_info`.
    pub fn set(&self, wait_event_info: uint32) {
        self.value.store(wait_event_info, Ordering::Relaxed);
    }
}

thread_local! {
    /// `local_my_wait_event_info` — the per-backend default slot used before
    /// (and after) shared storage is installed.
    static LOCAL_WAIT_EVENT_INFO: AtomicU32 = const { AtomicU32::new(0) };
    /// The currently installed shared slot, if any (`my_wait_event_info`
    /// pointing into shared memory).
    static CURRENT_WAIT_EVENT_STORAGE: RefCell<Option<WaitEventStorage>> =
        const { RefCell::new(None) };
}

/// A scope guard that restores the previously installed storage when dropped,
/// making the C "install during startup / reset during shutdown" lifetime safe
/// in Rust: the redirect cannot outlive the guard.
pub struct WaitEventStorageGuard {
    previous: Option<WaitEventStorage>,
}

impl Drop for WaitEventStorageGuard {
    fn drop(&mut self) {
        CURRENT_WAIT_EVENT_STORAGE.with(|current| {
            *current.borrow_mut() = self.previous.take();
        });
    }
}

/// `pgstat_set_wait_event_storage()` — redirect wait-event reporting to
/// `storage` until the returned guard is dropped (or
/// [`pgstat_reset_wait_event_storage`] is called).
pub fn pgstat_set_wait_event_storage(storage: WaitEventStorage) -> WaitEventStorageGuard {
    let previous = CURRENT_WAIT_EVENT_STORAGE.with(|current| {
        let mut current = current.borrow_mut();
        std::mem::replace(&mut *current, Some(storage))
    });
    WaitEventStorageGuard { previous }
}

/// `pgstat_reset_wait_event_storage()` — point reporting back at the
/// process-local default slot.
pub fn pgstat_reset_wait_event_storage() {
    CURRENT_WAIT_EVENT_STORAGE.with(|current| {
        *current.borrow_mut() = None;
    });
}

// ---------------------------------------------------------------------------
// Per-PGPROC wait-event storage registry (the `&MyProc->wait_event_info`
// redirect that `InitProcess` / `InitAuxiliaryProcess` install).
//
// C points `my_wait_event_info` directly at the `uint32` field inside the
// backend's own PGPROC in shared memory, so other backends scanning the proc
// array observe the live wait event. PGPROC is modelled (types-storage) with a
// plain `uint32 wait_event_info`, which can't alias an `Arc<AtomicU32>`; we
// instead keep a process-global registry of one shared [`WaitEventStorage`]
// per ProcNumber. `pgstat_set_wait_event_storage_for_proc(procno)` installs
// that proc's slot as the current redirect (any backend can later read the
// same slot by procno), and reset points back at the process-local default.
// ---------------------------------------------------------------------------

static PER_PROC_WAIT_EVENT_STORAGE: std::sync::OnceLock<
    std::sync::Mutex<std::collections::HashMap<i32, WaitEventStorage>>,
> = std::sync::OnceLock::new();

fn per_proc_storage(procno: types_core::ProcNumber) -> WaitEventStorage {
    let map = PER_PROC_WAIT_EVENT_STORAGE.get_or_init(|| std::sync::Mutex::new(Default::default()));
    let mut map = map.lock().expect("wait-event storage registry poisoned");
    map.entry(procno)
        .or_insert_with(WaitEventStorage::new)
        .clone()
}

/// `pgstat_set_wait_event_storage(&GetPGProcByNumber(procno)->wait_event_info)`
/// — the seam-shaped install used by `InitProcess` / `InitAuxiliaryProcess`.
/// Installs the named proc's shared slot as the current redirect, valid until
/// `pgstat_reset_wait_event_storage` (the C contract: no scope guard, the
/// redirect persists for the backend's working life).
pub fn pgstat_set_wait_event_storage_for_proc(procno: types_core::ProcNumber) {
    let storage = per_proc_storage(procno);
    CURRENT_WAIT_EVENT_STORAGE.with(|current| {
        *current.borrow_mut() = Some(storage);
    });
}

/// `pgstat_report_wait_start()` — record that the backend is now waiting on
/// `wait_event_info`.
pub fn pgstat_report_wait_start(wait_event_info: uint32) {
    with_current_storage(|slot| slot.store(wait_event_info, Ordering::Relaxed));
}

/// `pgstat_report_wait_end()` — record that the backend is no longer waiting.
pub fn pgstat_report_wait_end() {
    with_current_storage(|slot| slot.store(0, Ordering::Relaxed));
}

/// The `wait_event_info` currently stored for this backend. (Not a C export;
/// exposed for callers/tests inspecting the slot `my_wait_event_info` targets.)
pub fn pgstat_current_wait_event_info() -> uint32 {
    with_current_storage(|slot| slot.load(Ordering::Relaxed))
}

fn with_current_storage<T>(f: impl FnOnce(&AtomicU32) -> T) -> T {
    CURRENT_WAIT_EVENT_STORAGE.with(|current| {
        if let Some(storage) = current.borrow().as_ref() {
            f(&storage.value)
        } else {
            LOCAL_WAIT_EVENT_INFO.with(f)
        }
    })
}

// ---------------------------------------------------------------------------
// Custom (Extension / InjectionPoint) wait events.
//
// The custom store lives in shared memory: a spinlock-guarded id counter and
// two dynahash tables, all protected by WaitEventCustomLock. We hold the same
// per-backend handle pointers C keeps as file-scope statics.
// ---------------------------------------------------------------------------

/// `WaitEventCustomCounterData` — the shared id counter and its spinlock.
#[repr(C)]
struct WaitEventCustomCounterData {
    /// `int nextId` — next ID to assign.
    next_id: i32,
    /// `slock_t mutex` — protects the counter.
    mutex: Spinlock,
}

/// `WaitEventCustomEntryByInfo` — find names from infos (`HASH_BLOBS`).
#[repr(C)]
struct WaitEventCustomEntryByInfo {
    /// `uint32 wait_event_info` — hash key.
    wait_event_info: uint32,
    /// `char wait_event_name[NAMEDATALEN]` — custom wait event name.
    wait_event_name: [u8; NAMEDATALEN],
}

/// `WaitEventCustomEntryByName` — find infos from names (`HASH_STRINGS`).
#[repr(C)]
struct WaitEventCustomEntryByName {
    /// `char wait_event_name[NAMEDATALEN]` — hash key.
    wait_event_name: [u8; NAMEDATALEN],
    wait_event_info: uint32,
}

thread_local! {
    /// `static WaitEventCustomCounterData *WaitEventCustomCounter`.
    static WAIT_EVENT_CUSTOM_COUNTER: Cell<*mut WaitEventCustomCounterData> =
        const { Cell::new(std::ptr::null_mut()) };
    /// `static HTAB *WaitEventCustomHashByInfo`.
    static WAIT_EVENT_CUSTOM_HASH_BY_INFO: Cell<*mut HTAB> =
        const { Cell::new(std::ptr::null_mut()) };
    /// `static HTAB *WaitEventCustomHashByName`.
    static WAIT_EVENT_CUSTOM_HASH_BY_NAME: Cell<*mut HTAB> =
        const { Cell::new(std::ptr::null_mut()) };
}

/// A fixed-size NUL-terminated name key buffer, the `char[NAMEDATALEN]` C passes
/// to `hash_search` for the by-name (`HASH_STRINGS`) table. The caller has
/// already checked `name.len() < NAMEDATALEN`.
fn name_key(name: &str) -> [u8; NAMEDATALEN] {
    let mut key = [0u8; NAMEDATALEN];
    let bytes = name.as_bytes();
    key[..bytes.len()].copy_from_slice(bytes);
    key
}

/// Copy a `&str` into a fixed `char[NAMEDATALEN]` field, NUL-padded
/// (`strlcpy(dst, name, NAMEDATALEN)`; the caller guarantees it fits).
fn strlcpy_name(dst: &mut [u8; NAMEDATALEN], name: &str) {
    let bytes = name.as_bytes();
    dst[..bytes.len()].copy_from_slice(bytes);
}

/// Read a NUL-terminated `char[NAMEDATALEN]` field back to an owned `String`.
fn name_from_field(field: &[u8; NAMEDATALEN]) -> String {
    let end = field.iter().position(|&b| b == 0).unwrap_or(NAMEDATALEN);
    String::from_utf8_lossy(&field[..end]).into_owned()
}

/// `WaitEventCustomShmemSize()` — bytes of shared memory the custom store needs.
pub fn WaitEventCustomShmemSize() -> PgResult<Size> {
    let mut sz = maxalign(std::mem::size_of::<WaitEventCustomCounterData>());
    sz = backend_storage_ipc_shmem::add_size(
        sz,
        hash_estimate_size::call(
            WAIT_EVENT_CUSTOM_HASH_MAX_SIZE,
            std::mem::size_of::<WaitEventCustomEntryByInfo>(),
        ),
    )?;
    sz = backend_storage_ipc_shmem::add_size(
        sz,
        hash_estimate_size::call(
            WAIT_EVENT_CUSTOM_HASH_MAX_SIZE,
            std::mem::size_of::<WaitEventCustomEntryByName>(),
        ),
    )?;
    Ok(sz)
}

/// `WaitEventCustomShmemInit()` — attach/initialize the custom store in shmem.
pub fn WaitEventCustomShmemInit() -> PgResult<()> {
    let (counter_ptr, found) = ShmemInitStruct(
        "WaitEventCustomCounterData",
        std::mem::size_of::<WaitEventCustomCounterData>(),
    )?;
    let counter = counter_ptr.as_ptr().cast::<WaitEventCustomCounterData>();
    WAIT_EVENT_CUSTOM_COUNTER.with(|c| c.set(counter));

    if !found {
        // Initialize the allocation counter and its spinlock.
        // SAFETY: `counter` points at freshly-allocated shared memory we own.
        unsafe {
            (*counter).next_id = WAIT_EVENT_CUSTOM_INITIAL_ID as i32;
            s_init_lock(&(*counter).mutex);
        }
    }

    // Initialize or attach the hash tables to store custom wait events.
    let mut info = HASHCTL::new();
    info.keysize = std::mem::size_of::<uint32>();
    info.entrysize = std::mem::size_of::<WaitEventCustomEntryByInfo>();
    let by_info = ShmemInitHash(
        "WaitEventCustom hash by wait event information",
        WAIT_EVENT_CUSTOM_HASH_INIT_SIZE,
        WAIT_EVENT_CUSTOM_HASH_MAX_SIZE,
        &mut info,
        HASH_ELEM | HASH_BLOBS,
    )?;
    WAIT_EVENT_CUSTOM_HASH_BY_INFO.with(|c| c.set(by_info));

    // key is a NULL-terminated string
    info.keysize = NAMEDATALEN;
    info.entrysize = std::mem::size_of::<WaitEventCustomEntryByName>();
    let by_name = ShmemInitHash(
        "WaitEventCustom hash by name",
        WAIT_EVENT_CUSTOM_HASH_INIT_SIZE,
        WAIT_EVENT_CUSTOM_HASH_MAX_SIZE,
        &mut info,
        HASH_ELEM | HASH_STRINGS,
    )?;
    WAIT_EVENT_CUSTOM_HASH_BY_NAME.with(|c| c.set(by_name));

    Ok(())
}

/// `WaitEventExtensionNew()` — allocate (or return the existing) wait-event id
/// for an extension-defined event named `wait_event_name`.
pub fn WaitEventExtensionNew(wait_event_name: &str) -> PgResult<uint32> {
    WaitEventCustomNew(PG_WAIT_EXTENSION, wait_event_name)
}

/// `WaitEventInjectionPointNew()` — allocate (or return the existing) wait-event
/// id for an injection-point event named `wait_event_name`.
pub fn WaitEventInjectionPointNew(wait_event_name: &str) -> PgResult<uint32> {
    WaitEventCustomNew(PG_WAIT_INJECTIONPOINT, wait_event_name)
}

/// `WaitEventCustomNew()` — allocate a new event ID and return the wait event
/// info, or the existing info if the name is already defined.
fn WaitEventCustomNew(classId: uint32, wait_event_name: &str) -> PgResult<uint32> {
    // Check the limit of the length of the event name.
    if wait_event_name.len() >= NAMEDATALEN {
        elog(
            ERROR,
            format!(
                "cannot use custom wait event string longer than {} characters",
                NAMEDATALEN - 1
            ),
        )?;
        unreachable!("elog(ERROR) returned");
    }

    let by_name = WAIT_EVENT_CUSTOM_HASH_BY_NAME.with(|c| c.get());
    let by_info = WAIT_EVENT_CUSTOM_HASH_BY_INFO.with(|c| c.get());
    let counter = WAIT_EVENT_CUSTOM_COUNTER.with(|c| c.get());
    let key = name_key(wait_event_name);

    // Check if the wait event info associated to the name is already defined,
    // and return it if so.
    let guard = lwlock_acquire_main::call(WAIT_EVENT_CUSTOM_LOCK, LW_SHARED)?;
    let (entry, found) = hash_search::call(by_name, key.as_ptr(), HASHACTION::HASH_FIND)?;
    guard.release()?;
    if found {
        // SAFETY: `entry` is a live entry in the shared by-name table.
        let info = unsafe { (*entry.cast::<WaitEventCustomEntryByName>()).wait_event_info };
        let oldClassId = info & WAIT_EVENT_CLASS_MASK;
        if oldClassId != classId {
            return Err(class_conflict_error(wait_event_name, info));
        }
        return Ok(info);
    }

    // Allocate and register a new wait event. Recheck under the exclusive lock,
    // as a concurrent process could have inserted the same name since the
    // shared lock was released.
    let guard = lwlock_acquire_main::call(WAIT_EVENT_CUSTOM_LOCK, LW_EXCLUSIVE)?;
    let (entry, found) = hash_search::call(by_name, key.as_ptr(), HASHACTION::HASH_FIND)?;
    if found {
        guard.release()?;
        // SAFETY: as above.
        let info = unsafe { (*entry.cast::<WaitEventCustomEntryByName>()).wait_event_info };
        let oldClassId = info & WAIT_EVENT_CLASS_MASK;
        if oldClassId != classId {
            return Err(class_conflict_error(wait_event_name, info));
        }
        return Ok(info);
    }

    // Allocate a new event Id.
    // SAFETY: `counter` points at the live shared counter struct.
    let event_id = unsafe {
        s_lock_macro(&(*counter).mutex, Some(SRCFILE), 0, Some("WaitEventCustomNew"));
        if (*counter).next_id >= WAIT_EVENT_CUSTOM_HASH_MAX_SIZE as i32 {
            s_unlock(&(*counter).mutex);
            guard.release()?;
            ereport(ERROR)
                .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                .errmsg("too many custom wait events")
                .finish(loc("WaitEventCustomNew"))?;
            unreachable!("ereport(ERROR) returned");
        }
        let id = (*counter).next_id as uint16;
        (*counter).next_id += 1;
        s_unlock(&(*counter).mutex);
        id
    };

    // Register the new wait event.
    let wait_event_info = classId | event_id as uint32;
    let (entry_by_info, found) =
        hash_search::call(by_info, (&wait_event_info as *const uint32).cast(), HASHACTION::HASH_ENTER)?;
    debug_assert!(!found);
    // SAFETY: `entry_by_info` is a fresh live entry in the by-info table.
    unsafe {
        let e = entry_by_info.cast::<WaitEventCustomEntryByInfo>();
        (*e).wait_event_info = wait_event_info;
        strlcpy_name(&mut (*e).wait_event_name, wait_event_name);
    }

    let (entry_by_name, found) = hash_search::call(by_name, key.as_ptr(), HASHACTION::HASH_ENTER)?;
    debug_assert!(!found);
    // SAFETY: `entry_by_name` is a fresh live entry in the by-name table.
    unsafe {
        (*entry_by_name.cast::<WaitEventCustomEntryByName>()).wait_event_info = wait_event_info;
    }

    guard.release()?;
    Ok(wait_event_info)
}

/// `GetWaitEventCustomIdentifier()` — the name registered for a custom wait
/// event, or the built-in `"Extension"` literal.
fn GetWaitEventCustomIdentifier(wait_event_info: uint32) -> PgResult<Cow<'static, str>> {
    // Built-in event?
    if wait_event_info == PG_WAIT_EXTENSION {
        return Ok(Cow::Borrowed("Extension"));
    }

    // It is a user-defined wait event, so look up the hash table.
    let by_info = WAIT_EVENT_CUSTOM_HASH_BY_INFO.with(|c| c.get());
    let guard = lwlock_acquire_main::call(WAIT_EVENT_CUSTOM_LOCK, LW_SHARED)?;
    let (entry, _found) = hash_search::call(
        by_info,
        (&wait_event_info as *const uint32).cast(),
        HASHACTION::HASH_FIND,
    )?;
    guard.release()?;

    if entry.is_null() {
        return Err(custom_wait_event_missing(wait_event_info));
    }
    // SAFETY: `entry` is a live entry in the by-info table.
    let name = unsafe { name_from_field(&(*entry.cast::<WaitEventCustomEntryByInfo>()).wait_event_name) };
    Ok(Cow::Owned(name))
}

/// `GetWaitEventCustomNames()` — the names of all registered custom wait events
/// in class `classId`. (In C this returns a palloc'd `char **` plus a count; the
/// idiomatic shape is an owned `Vec<String>`.)
pub fn GetWaitEventCustomNames(classId: uint32) -> PgResult<Vec<String>> {
    let by_name = WAIT_EVENT_CUSTOM_HASH_BY_NAME.with(|c| c.get());

    let guard = lwlock_acquire_main::call(WAIT_EVENT_CUSTOM_LOCK, LW_SHARED)?;

    // Now we can safely count the number of entries.
    let els = hash_get_num_entries::call(by_name);
    let mut names: Vec<String> = Vec::with_capacity(els.max(0) as usize);

    // Now scan the hash table to copy the data.
    let mut hash_seq = HASH_SEQ_STATUS::default();
    hash_seq_init::call(&mut hash_seq, by_name);
    loop {
        let hentry = match hash_seq_search::call(&mut hash_seq) {
            Ok(p) => p,
            Err(e) => {
                guard.release()?;
                return Err(e);
            }
        };
        if hentry.is_null() {
            break;
        }
        // SAFETY: `hentry` is a live entry in the by-name table.
        let entry = unsafe { &*hentry.cast::<WaitEventCustomEntryByName>() };
        if (entry.wait_event_info & WAIT_EVENT_CLASS_MASK) != classId {
            continue;
        }
        names.push(name_from_field(&entry.wait_event_name));
    }

    guard.release()?;
    Ok(names)
}

// ---------------------------------------------------------------------------
// pg_get_wait_events() (wait_event_funcs.c).
// ---------------------------------------------------------------------------

/// An owned wait-event row, the idiomatic form of one tuple produced by
/// `pg_get_wait_events`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WaitEventRow {
    pub type_: String,
    pub name: String,
    pub description: String,
}

/// The static `waitEventData[]` table of `wait_event_funcs.c`, i.e. the
/// contents of the generated `wait_event_funcs_data.c`. Unlike
/// [`wait_event_data`] (the `pgstat_get_wait_*` name lookup) this enumerates
/// every class, case-insensitively sorted within each class, with the classes
/// emitted in case-insensitive typedef-name order, and carries the fully
/// post-processed description. Computed once.
fn wait_event_funcs_data() -> &'static [WaitEventRow] {
    static DATA: OnceLock<Vec<WaitEventRow>> = OnceLock::new();
    DATA.get_or_init(build_funcs_data)
}

fn build_funcs_data() -> Vec<WaitEventRow> {
    let groups = grouped_wait_events();
    let ordered = classes_sorted(&groups);
    let mut rows: Vec<WaitEventRow> = Vec::new();
    for group in &ordered {
        for event in &group.events {
            rows.push(WaitEventRow {
                type_: group.class.type_name().to_owned(),
                name: wait_event_name(event),
                description: process_description(&event.doc),
            });
        }
    }
    rows
}

/// `pg_get_wait_events()` — every built-in wait event followed by the
/// registered Extension and InjectionPoint custom events, as
/// (type, name, description) rows.
pub fn pg_get_wait_events() -> PgResult<Vec<WaitEventRow>> {
    let builtin = wait_event_funcs_data();
    let extension_names = GetWaitEventCustomNames(PG_WAIT_EXTENSION)?;
    let injection_names = GetWaitEventCustomNames(PG_WAIT_INJECTIONPOINT)?;

    let mut rows: Vec<WaitEventRow> = Vec::new();
    let total = builtin
        .len()
        .saturating_add(extension_names.len())
        .saturating_add(injection_names.len());
    rows.try_reserve(total).map_err(|_| out_of_memory())?;

    for row in builtin {
        rows.push(row.clone());
    }

    for name in extension_names {
        let description =
            format!("Waiting for custom wait event \"{name}\" defined by extension module");
        rows.push(WaitEventRow {
            type_: "Extension".to_owned(),
            name,
            description,
        });
    }

    for name in injection_names {
        let description = format!("Waiting for injection point \"{name}\"");
        rows.push(WaitEventRow {
            type_: "InjectionPoint".to_owned(),
            name,
            description,
        });
    }

    Ok(rows)
}

// ---------------------------------------------------------------------------
// Errors.
// ---------------------------------------------------------------------------

fn class_conflict_error(wait_event_name: &str, wait_event_info: uint32) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_DUPLICATE_OBJECT)
        .errmsg(format!(
            "wait event \"{}\" already exists in type \"{}\"",
            wait_event_name,
            pgstat_get_wait_event_type(wait_event_info).unwrap_or("???")
        ))
        .into_error()
}

fn custom_wait_event_missing(wait_event_info: uint32) -> PgError {
    PgError::error(format!(
        "could not find custom name for wait event information {wait_event_info}"
    ))
    .with_sqlstate(ERRCODE_INTERNAL_ERROR)
}

fn out_of_memory() -> PgError {
    PgError::error("out of memory").with_sqlstate(ERRCODE_OUT_OF_MEMORY)
}

/// Install this crate's inward seams (`waitevent-seams`).
pub fn init_seams() {
    backend_utils_activity_waitevent_seams::pgstat_report_wait_start::set(pgstat_report_wait_start);
    backend_utils_activity_waitevent_seams::pgstat_report_wait_end::set(pgstat_report_wait_end);
    backend_utils_activity_waitevent_seams::wait_event_custom_shmem_size::set(
        WaitEventCustomShmemSize,
    );
    backend_utils_activity_waitevent_seams::wait_event_custom_shmem_init::set(
        WaitEventCustomShmemInit,
    );
    // wait_event.c owns pgstat_set_wait_event_storage / _reset; the seam
    // declarations live in the consolidated pgstat-seams crate.
    backend_utils_activity_pgstat_seams::pgstat_set_wait_event_storage_for_proc::set(
        pgstat_set_wait_event_storage_for_proc,
    );
    backend_utils_activity_pgstat_seams::pgstat_reset_wait_event_storage::set(
        pgstat_reset_wait_event_storage,
    );
}

#[cfg(test)]
mod tests;
