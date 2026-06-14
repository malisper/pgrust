//! `cmdtag.c` — data and routines for command-tag names and enumeration
//! (`src/backend/tcop/cmdtag.c`), PostgreSQL 18.3.
//!
//! Self-contained lookup over the static `tag_behavior[]` table (generated from
//! `tcop/cmdtaglist.h` via the `PG_CMDTAG` macro). The two leaf primitives the
//! C calls — `pg_strcasecmp` and `pg_ulltoa_n` — already live in their own
//! crates (`port-pgstrcasecmp`, `backend-utils-adt-numutils`) and are called
//! directly; both are acyclic leaves.
//!
//! [`CommandTag`], [`QueryCompletion`], the `CMDTAG_*` constants and
//! [`COMPLETION_TAG_BUFSIZE`] come from `types_portal`.

#![no_std]

extern crate alloc;

use backend_utils_adt_numutils::{pg_ulltoa_n, MAX_UINT64_DIGITS};
use mcx::{Mcx, PgString};
use port_pgstrcasecmp::pg_strcasecmp;
use types_error::PgResult;
use types_portal::{
    CommandTag, QueryCompletion, CMDTAG_INSERT, CMDTAG_UNKNOWN, COMPLETION_TAG_BUFSIZE,
};

/// `MAXINT8LEN` (`src/include/utils/builtins.h`) — "-9223372036854775808" plus
/// NUL. Used only by the [`build_query_completion_string`] buffer-bound assert.
const MAXINT8LEN: usize = 20;

/// `CommandTagBehavior` (cmdtag.c:20-28) — one row of the static `tag_behavior`
/// table built from `tcop/cmdtaglist.h` via the `PG_CMDTAG` macro.
///
/// The C `namelen` (`uint8`) field is omitted: the tag names are stored as
/// `&'static str`, so the length is `name.len()` (every name is < 256 bytes and
/// pure ASCII, so this is byte-identical to the C `strlen`).
#[derive(Clone, Copy, Debug)]
pub struct CommandTagBehavior {
    /// tag name, e.g. `"SELECT"` (the C `name` field).
    pub name: &'static str,
    /// `event_trigger_ok`.
    pub event_trigger_ok: bool,
    /// `table_rewrite_ok`.
    pub table_rewrite_ok: bool,
    /// `display_rowcount` — whether the affected-row count is shown in the
    /// command-completion string.
    pub display_rowcount: bool,
}

/// Expansion of the C `PG_CMDTAG(tag, name, evtrgok, rwrok, rowcnt)` macro
/// (cmdtag.c:30-31): `{ name, (uint8) (sizeof(name) - 1), evtrgok, rwrok, rowcnt }`.
macro_rules! pg_cmdtag {
    ($name:literal, $evtrgok:literal, $rwrok:literal, $rowcnt:literal) => {
        CommandTagBehavior {
            name: $name,
            event_trigger_ok: $evtrgok,
            table_rewrite_ok: $rwrok,
            display_rowcount: $rowcnt,
        }
    };
}

/// `static const CommandTagBehavior tag_behavior[]` (cmdtag.c:33-35),
/// `#include "tcop/cmdtaglist.h"`. Entries are in the exact order of
/// `cmdtaglist.h` (alphabetically sorted by textual name, per the header's
/// contract for `bsearch`); the index of each row equals its [`CommandTag`]
/// integer value.
pub static TAG_BEHAVIOR: [CommandTagBehavior; 193] = [
    pg_cmdtag!("???", false, false, false),
    pg_cmdtag!("ALTER ACCESS METHOD", true, false, false),
    pg_cmdtag!("ALTER AGGREGATE", true, false, false),
    pg_cmdtag!("ALTER CAST", true, false, false),
    pg_cmdtag!("ALTER COLLATION", true, false, false),
    pg_cmdtag!("ALTER CONSTRAINT", true, false, false),
    pg_cmdtag!("ALTER CONVERSION", true, false, false),
    pg_cmdtag!("ALTER DATABASE", false, false, false),
    pg_cmdtag!("ALTER DEFAULT PRIVILEGES", true, false, false),
    pg_cmdtag!("ALTER DOMAIN", true, false, false),
    pg_cmdtag!("ALTER EVENT TRIGGER", false, false, false),
    pg_cmdtag!("ALTER EXTENSION", true, false, false),
    pg_cmdtag!("ALTER FOREIGN DATA WRAPPER", true, false, false),
    pg_cmdtag!("ALTER FOREIGN TABLE", true, false, false),
    pg_cmdtag!("ALTER FUNCTION", true, false, false),
    pg_cmdtag!("ALTER INDEX", true, false, false),
    pg_cmdtag!("ALTER LANGUAGE", true, false, false),
    pg_cmdtag!("ALTER LARGE OBJECT", true, false, false),
    pg_cmdtag!("ALTER MATERIALIZED VIEW", true, true, false),
    pg_cmdtag!("ALTER OPERATOR", true, false, false),
    pg_cmdtag!("ALTER OPERATOR CLASS", true, false, false),
    pg_cmdtag!("ALTER OPERATOR FAMILY", true, false, false),
    pg_cmdtag!("ALTER POLICY", true, false, false),
    pg_cmdtag!("ALTER PROCEDURE", true, false, false),
    pg_cmdtag!("ALTER PUBLICATION", true, false, false),
    pg_cmdtag!("ALTER ROLE", false, false, false),
    pg_cmdtag!("ALTER ROUTINE", true, false, false),
    pg_cmdtag!("ALTER RULE", true, false, false),
    pg_cmdtag!("ALTER SCHEMA", true, false, false),
    pg_cmdtag!("ALTER SEQUENCE", true, false, false),
    pg_cmdtag!("ALTER SERVER", true, false, false),
    pg_cmdtag!("ALTER STATISTICS", true, false, false),
    pg_cmdtag!("ALTER SUBSCRIPTION", true, false, false),
    pg_cmdtag!("ALTER SYSTEM", false, false, false),
    pg_cmdtag!("ALTER TABLE", true, true, false),
    pg_cmdtag!("ALTER TABLESPACE", false, false, false),
    pg_cmdtag!("ALTER TEXT SEARCH CONFIGURATION", true, false, false),
    pg_cmdtag!("ALTER TEXT SEARCH DICTIONARY", true, false, false),
    pg_cmdtag!("ALTER TEXT SEARCH PARSER", true, false, false),
    pg_cmdtag!("ALTER TEXT SEARCH TEMPLATE", true, false, false),
    pg_cmdtag!("ALTER TRANSFORM", true, false, false),
    pg_cmdtag!("ALTER TRIGGER", true, false, false),
    pg_cmdtag!("ALTER TYPE", true, true, false),
    pg_cmdtag!("ALTER USER MAPPING", true, false, false),
    pg_cmdtag!("ALTER VIEW", true, false, false),
    pg_cmdtag!("ANALYZE", false, false, false),
    pg_cmdtag!("BEGIN", false, false, false),
    pg_cmdtag!("CALL", false, false, false),
    pg_cmdtag!("CHECKPOINT", false, false, false),
    pg_cmdtag!("CLOSE", false, false, false),
    pg_cmdtag!("CLOSE CURSOR", false, false, false),
    pg_cmdtag!("CLOSE CURSOR ALL", false, false, false),
    pg_cmdtag!("CLUSTER", false, false, false),
    pg_cmdtag!("COMMENT", true, false, false),
    pg_cmdtag!("COMMIT", false, false, false),
    pg_cmdtag!("COMMIT PREPARED", false, false, false),
    pg_cmdtag!("COPY", false, false, true),
    pg_cmdtag!("COPY FROM", false, false, false),
    pg_cmdtag!("CREATE ACCESS METHOD", true, false, false),
    pg_cmdtag!("CREATE AGGREGATE", true, false, false),
    pg_cmdtag!("CREATE CAST", true, false, false),
    pg_cmdtag!("CREATE COLLATION", true, false, false),
    pg_cmdtag!("CREATE CONSTRAINT", true, false, false),
    pg_cmdtag!("CREATE CONVERSION", true, false, false),
    pg_cmdtag!("CREATE DATABASE", false, false, false),
    pg_cmdtag!("CREATE DOMAIN", true, false, false),
    pg_cmdtag!("CREATE EVENT TRIGGER", false, false, false),
    pg_cmdtag!("CREATE EXTENSION", true, false, false),
    pg_cmdtag!("CREATE FOREIGN DATA WRAPPER", true, false, false),
    pg_cmdtag!("CREATE FOREIGN TABLE", true, false, false),
    pg_cmdtag!("CREATE FUNCTION", true, false, false),
    pg_cmdtag!("CREATE INDEX", true, false, false),
    pg_cmdtag!("CREATE LANGUAGE", true, false, false),
    pg_cmdtag!("CREATE MATERIALIZED VIEW", true, false, false),
    pg_cmdtag!("CREATE OPERATOR", true, false, false),
    pg_cmdtag!("CREATE OPERATOR CLASS", true, false, false),
    pg_cmdtag!("CREATE OPERATOR FAMILY", true, false, false),
    pg_cmdtag!("CREATE POLICY", true, false, false),
    pg_cmdtag!("CREATE PROCEDURE", true, false, false),
    pg_cmdtag!("CREATE PUBLICATION", true, false, false),
    pg_cmdtag!("CREATE ROLE", false, false, false),
    pg_cmdtag!("CREATE ROUTINE", true, false, false),
    pg_cmdtag!("CREATE RULE", true, false, false),
    pg_cmdtag!("CREATE SCHEMA", true, false, false),
    pg_cmdtag!("CREATE SEQUENCE", true, false, false),
    pg_cmdtag!("CREATE SERVER", true, false, false),
    pg_cmdtag!("CREATE STATISTICS", true, false, false),
    pg_cmdtag!("CREATE SUBSCRIPTION", true, false, false),
    pg_cmdtag!("CREATE TABLE", true, false, false),
    pg_cmdtag!("CREATE TABLE AS", true, false, false),
    pg_cmdtag!("CREATE TABLESPACE", false, false, false),
    pg_cmdtag!("CREATE TEXT SEARCH CONFIGURATION", true, false, false),
    pg_cmdtag!("CREATE TEXT SEARCH DICTIONARY", true, false, false),
    pg_cmdtag!("CREATE TEXT SEARCH PARSER", true, false, false),
    pg_cmdtag!("CREATE TEXT SEARCH TEMPLATE", true, false, false),
    pg_cmdtag!("CREATE TRANSFORM", true, false, false),
    pg_cmdtag!("CREATE TRIGGER", true, false, false),
    pg_cmdtag!("CREATE TYPE", true, false, false),
    pg_cmdtag!("CREATE USER MAPPING", true, false, false),
    pg_cmdtag!("CREATE VIEW", true, false, false),
    pg_cmdtag!("DEALLOCATE", false, false, false),
    pg_cmdtag!("DEALLOCATE ALL", false, false, false),
    pg_cmdtag!("DECLARE CURSOR", false, false, false),
    pg_cmdtag!("DELETE", false, false, true),
    pg_cmdtag!("DISCARD", false, false, false),
    pg_cmdtag!("DISCARD ALL", false, false, false),
    pg_cmdtag!("DISCARD PLANS", false, false, false),
    pg_cmdtag!("DISCARD SEQUENCES", false, false, false),
    pg_cmdtag!("DISCARD TEMP", false, false, false),
    pg_cmdtag!("DO", false, false, false),
    pg_cmdtag!("DROP ACCESS METHOD", true, false, false),
    pg_cmdtag!("DROP AGGREGATE", true, false, false),
    pg_cmdtag!("DROP CAST", true, false, false),
    pg_cmdtag!("DROP COLLATION", true, false, false),
    pg_cmdtag!("DROP CONSTRAINT", true, false, false),
    pg_cmdtag!("DROP CONVERSION", true, false, false),
    pg_cmdtag!("DROP DATABASE", false, false, false),
    pg_cmdtag!("DROP DOMAIN", true, false, false),
    pg_cmdtag!("DROP EVENT TRIGGER", false, false, false),
    pg_cmdtag!("DROP EXTENSION", true, false, false),
    pg_cmdtag!("DROP FOREIGN DATA WRAPPER", true, false, false),
    pg_cmdtag!("DROP FOREIGN TABLE", true, false, false),
    pg_cmdtag!("DROP FUNCTION", true, false, false),
    pg_cmdtag!("DROP INDEX", true, false, false),
    pg_cmdtag!("DROP LANGUAGE", true, false, false),
    pg_cmdtag!("DROP MATERIALIZED VIEW", true, false, false),
    pg_cmdtag!("DROP OPERATOR", true, false, false),
    pg_cmdtag!("DROP OPERATOR CLASS", true, false, false),
    pg_cmdtag!("DROP OPERATOR FAMILY", true, false, false),
    pg_cmdtag!("DROP OWNED", true, false, false),
    pg_cmdtag!("DROP POLICY", true, false, false),
    pg_cmdtag!("DROP PROCEDURE", true, false, false),
    pg_cmdtag!("DROP PUBLICATION", true, false, false),
    pg_cmdtag!("DROP ROLE", false, false, false),
    pg_cmdtag!("DROP ROUTINE", true, false, false),
    pg_cmdtag!("DROP RULE", true, false, false),
    pg_cmdtag!("DROP SCHEMA", true, false, false),
    pg_cmdtag!("DROP SEQUENCE", true, false, false),
    pg_cmdtag!("DROP SERVER", true, false, false),
    pg_cmdtag!("DROP STATISTICS", true, false, false),
    pg_cmdtag!("DROP SUBSCRIPTION", true, false, false),
    pg_cmdtag!("DROP TABLE", true, false, false),
    pg_cmdtag!("DROP TABLESPACE", false, false, false),
    pg_cmdtag!("DROP TEXT SEARCH CONFIGURATION", true, false, false),
    pg_cmdtag!("DROP TEXT SEARCH DICTIONARY", true, false, false),
    pg_cmdtag!("DROP TEXT SEARCH PARSER", true, false, false),
    pg_cmdtag!("DROP TEXT SEARCH TEMPLATE", true, false, false),
    pg_cmdtag!("DROP TRANSFORM", true, false, false),
    pg_cmdtag!("DROP TRIGGER", true, false, false),
    pg_cmdtag!("DROP TYPE", true, false, false),
    pg_cmdtag!("DROP USER MAPPING", true, false, false),
    pg_cmdtag!("DROP VIEW", true, false, false),
    pg_cmdtag!("EXECUTE", false, false, false),
    pg_cmdtag!("EXPLAIN", false, false, false),
    pg_cmdtag!("FETCH", false, false, true),
    pg_cmdtag!("GRANT", true, false, false),
    pg_cmdtag!("GRANT ROLE", false, false, false),
    pg_cmdtag!("IMPORT FOREIGN SCHEMA", true, false, false),
    pg_cmdtag!("INSERT", false, false, true),
    pg_cmdtag!("LISTEN", false, false, false),
    pg_cmdtag!("LOAD", false, false, false),
    pg_cmdtag!("LOCK TABLE", false, false, false),
    pg_cmdtag!("LOGIN", true, false, false),
    pg_cmdtag!("MERGE", false, false, true),
    pg_cmdtag!("MOVE", false, false, true),
    pg_cmdtag!("NOTIFY", false, false, false),
    pg_cmdtag!("PREPARE", false, false, false),
    pg_cmdtag!("PREPARE TRANSACTION", false, false, false),
    pg_cmdtag!("REASSIGN OWNED", false, false, false),
    pg_cmdtag!("REFRESH MATERIALIZED VIEW", true, false, false),
    pg_cmdtag!("REINDEX", true, false, false),
    pg_cmdtag!("RELEASE", false, false, false),
    pg_cmdtag!("RESET", false, false, false),
    pg_cmdtag!("REVOKE", true, false, false),
    pg_cmdtag!("REVOKE ROLE", false, false, false),
    pg_cmdtag!("ROLLBACK", false, false, false),
    pg_cmdtag!("ROLLBACK PREPARED", false, false, false),
    pg_cmdtag!("SAVEPOINT", false, false, false),
    pg_cmdtag!("SECURITY LABEL", true, false, false),
    pg_cmdtag!("SELECT", false, false, true),
    pg_cmdtag!("SELECT FOR KEY SHARE", false, false, false),
    pg_cmdtag!("SELECT FOR NO KEY UPDATE", false, false, false),
    pg_cmdtag!("SELECT FOR SHARE", false, false, false),
    pg_cmdtag!("SELECT FOR UPDATE", false, false, false),
    pg_cmdtag!("SELECT INTO", true, false, false),
    pg_cmdtag!("SET", false, false, false),
    pg_cmdtag!("SET CONSTRAINTS", false, false, false),
    pg_cmdtag!("SHOW", false, false, false),
    pg_cmdtag!("START TRANSACTION", false, false, false),
    pg_cmdtag!("TRUNCATE TABLE", false, false, false),
    pg_cmdtag!("UNLISTEN", false, false, false),
    pg_cmdtag!("UPDATE", false, false, true),
    pg_cmdtag!("VACUUM", false, false, false),
];

/// Index into [`TAG_BEHAVIOR`] for a [`CommandTag`].
///
/// In C, `tag_behavior[commandTag]` indexes with a raw enum; here we validate
/// the tag against the table bounds and panic on an out-of-range tag (which
/// would be a programming error: a `CommandTag` is always a valid enumerator in
/// PostgreSQL).
#[inline]
fn row(command_tag: CommandTag) -> &'static CommandTagBehavior {
    let idx = usize::try_from(command_tag)
        .ok()
        .filter(|&i| i < TAG_BEHAVIOR.len())
        .unwrap_or_else(|| panic!("CommandTag out of range: {command_tag}"));
    &TAG_BEHAVIOR[idx]
}

/// `InitializeQueryCompletion` (cmdtag.c:39-44) — reset a [`QueryCompletion`] to
/// the unknown/empty state. Idiomatic: takes `&mut`.
pub fn initialize_query_completion(qc: &mut QueryCompletion) {
    qc.commandTag = CMDTAG_UNKNOWN;
    qc.nprocessed = 0;
}

/// `GetCommandTagName` (cmdtag.c:46-50) — the tag name, e.g. `"SELECT"`.
pub fn get_command_tag_name(command_tag: CommandTag) -> &'static str {
    row(command_tag).name
}

/// `GetCommandTagNameAndLen` (cmdtag.c:52-57) — the tag name and its byte length.
/// In C the length is the cached `namelen`; here it is `name.len()`
/// (byte-identical, since every name is ASCII and < 256 bytes).
pub fn get_command_tag_name_and_len(command_tag: CommandTag) -> (&'static str, usize) {
    let name = row(command_tag).name;
    (name, name.len())
}

/// `command_tag_display_rowcount` (cmdtag.c:59-63).
pub fn command_tag_display_rowcount(command_tag: CommandTag) -> bool {
    row(command_tag).display_rowcount
}

/// `command_tag_event_trigger_ok` (cmdtag.c:65-69).
pub fn command_tag_event_trigger_ok(command_tag: CommandTag) -> bool {
    row(command_tag).event_trigger_ok
}

/// `command_tag_table_rewrite_ok` (cmdtag.c:71-75).
pub fn command_tag_table_rewrite_ok(command_tag: CommandTag) -> bool {
    row(command_tag).table_rewrite_ok
}

/// `GetCommandTagEnum` (cmdtag.c:82-107) — binary search of the
/// alphabetically-sorted [`TAG_BEHAVIOR`] table; returns [`CMDTAG_UNKNOWN`] if
/// not recognized.
///
/// Idiomatic: takes the command name as a byte slice. As in C, the string is
/// treated as NUL-terminated, so the comparison stops at the first NUL; an
/// empty (or NUL-leading) name maps to [`CMDTAG_UNKNOWN`], mirroring the C
/// `commandname == NULL || *commandname == '\0'` guard.
pub fn get_command_tag_enum(commandname: &[u8]) -> CommandTag {
    // A NUL-terminated C string treats the first NUL as the end; mirror that.
    let name = match commandname.iter().position(|&b| b == 0) {
        Some(end) => &commandname[..end],
        None => commandname,
    };
    if name.is_empty() {
        return CMDTAG_UNKNOWN;
    }

    // base = tag_behavior; last = tag_behavior + lengthof(tag_behavior) - 1;
    let mut base: isize = 0;
    let mut last: isize = (TAG_BEHAVIOR.len() as isize) - 1;

    while last >= base {
        // position = base + ((last - base) >> 1);
        let position: isize = base + ((last - base) >> 1);
        // result = pg_strcasecmp(commandname, position->name);
        let result = pg_strcasecmp(name, TAG_BEHAVIOR[position as usize].name.as_bytes());
        if result == 0 {
            // return (CommandTag) (position - tag_behavior);
            return position as CommandTag;
        } else if result < 0 {
            last = position - 1;
        } else {
            base = position + 1;
        }
    }
    CMDTAG_UNKNOWN
}

/// `BuildQueryCompletionString` (cmdtag.c:120-163) — build the command-completion
/// string (with `nprocessed` appended for display-rowcount tags).
///
/// Idiomatic: instead of writing into a caller-supplied
/// `char buff[COMPLETION_TAG_BUFSIZE]` and returning its `strlen`, this allocates
/// the result in `mcx` (a context-charged [`PgString`], the `buff[]` analog) and
/// returns it; an allocation refusal surfaces as a recoverable `PgError` rather
/// than aborting. The C buffer bound is preserved as a debug-asserted invariant
/// on the tag length. If `nameonly` is true, only the tag name is returned.
pub fn build_query_completion_string<'mcx>(
    mcx: Mcx<'mcx>,
    qc: &QueryCompletion,
    nameonly: bool,
) -> PgResult<PgString<'mcx>> {
    let tag: CommandTag = qc.commandTag;
    // const char *tagname = GetCommandTagNameAndLen(tag, &taglen);
    let (tagname, taglen) = get_command_tag_name_and_len(tag);

    // ensure that the tagname isn't long enough to overrun the buffer
    // Assert(taglen <= COMPLETION_TAG_BUFSIZE - MAXINT8LEN - 4);
    debug_assert!(taglen <= COMPLETION_TAG_BUFSIZE - MAXINT8LEN - 4);

    // We assume the tagname is plain ASCII and therefore requires no encoding
    // conversion. memcpy(buff, tagname, taglen);
    let mut buff = PgString::from_str_in(tagname, mcx)?;

    // In PostgreSQL versions 11 and earlier, it was possible to create a table
    // WITH OIDS.  When inserting into such a table, INSERT used to include the
    // Oid of the inserted record in the completion tag.  To maintain
    // compatibility in the wire protocol, we now write a "0" (for InvalidOid)
    // in the location where we once wrote the new record's Oid.
    if command_tag_display_rowcount(tag) && !nameonly {
        if tag == CMDTAG_INSERT {
            // *bufp++ = ' '; *bufp++ = '0';
            buff.try_push_str(" 0")?;
        }
        // *bufp++ = ' ';
        buff.try_push_str(" ")?;
        // bufp += pg_ulltoa_n(qc->nprocessed, bufp);
        let mut digits = [0u8; MAX_UINT64_DIGITS];
        let n = pg_ulltoa_n(qc.nprocessed, &mut digits);
        // pg_ulltoa_n writes only ASCII decimal digits, so the checked decode
        // never fails.
        let s = core::str::from_utf8(&digits[..n]).expect("pg_ulltoa_n writes ASCII digits only");
        buff.try_push_str(s)?;
    }

    Ok(buff)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mcx::MemoryContext;
    use types_portal::{CMDTAG_DELETE, CMDTAG_MERGE, CMDTAG_SELECT, CMDTAG_UPDATE};

    /// The table must have exactly 193 entries (`grep -c '^PG_CMDTAG'
    /// cmdtaglist.h` == 193).
    #[test]
    fn table_has_193_entries() {
        assert_eq!(TAG_BEHAVIOR.len(), 193);
    }

    /// Names are all pure ASCII (so `name.len()` equals C's `strlen`/`namelen`).
    #[test]
    fn names_are_ascii() {
        for row in TAG_BEHAVIOR.iter() {
            assert!(row.name.is_ascii(), "non-ASCII name: {:?}", row.name);
            assert!(!row.name.is_empty());
        }
    }

    /// Well-known fixed `CMDTAG_*` integer values match the table positions
    /// (and thus cmdtaglist.h order).
    #[test]
    fn known_tag_values_match_positions() {
        assert_eq!(get_command_tag_name(CMDTAG_UNKNOWN), "???");
        assert_eq!(get_command_tag_name(CMDTAG_DELETE), "DELETE");
        assert_eq!(get_command_tag_name(CMDTAG_INSERT), "INSERT");
        assert_eq!(get_command_tag_name(CMDTAG_MERGE), "MERGE");
        assert_eq!(get_command_tag_name(CMDTAG_SELECT), "SELECT");
        assert_eq!(get_command_tag_name(CMDTAG_UPDATE), "UPDATE");
    }

    /// The table must be sorted ascending by name under `pg_strcasecmp` (the
    /// bsearch contract).
    #[test]
    fn table_is_sorted_for_bsearch() {
        for w in TAG_BEHAVIOR.windows(2) {
            let cmp = pg_strcasecmp(w[0].name.as_bytes(), w[1].name.as_bytes());
            assert!(cmp < 0, "not sorted: {:?} vs {:?}", w[0].name, w[1].name);
        }
    }

    /// Every name must round-trip through `get_command_tag_enum` (bsearch hit),
    /// case-insensitively.
    #[test]
    fn enum_roundtrips_every_tag() {
        for (i, row) in TAG_BEHAVIOR.iter().enumerate() {
            let got = get_command_tag_enum(row.name.as_bytes());
            assert_eq!(got as usize, i, "lookup {:?} => {}", row.name, got);

            let lower = row.name.to_ascii_lowercase();
            let got_lower = get_command_tag_enum(lower.as_bytes());
            assert_eq!(got_lower as usize, i);
        }
    }

    #[test]
    fn enum_null_and_empty_are_unknown() {
        assert_eq!(get_command_tag_enum(b""), CMDTAG_UNKNOWN);
        // A leading NUL terminates the string at length 0 (C `*commandname == '\0'`).
        assert_eq!(get_command_tag_enum(b"\0"), CMDTAG_UNKNOWN);
    }

    #[test]
    fn enum_unrecognized_is_unknown() {
        assert_eq!(get_command_tag_enum(b"NOT A COMMAND"), CMDTAG_UNKNOWN);
    }

    #[test]
    fn enum_stops_at_embedded_nul() {
        // "SELECT\0junk" must match SELECT (C treats it as NUL-terminated).
        assert_eq!(get_command_tag_enum(b"SELECT\0junk"), CMDTAG_SELECT);
    }

    #[test]
    fn initialize_query_completion_zeros() {
        let mut qc = QueryCompletion {
            commandTag: CMDTAG_SELECT,
            nprocessed: 42,
        };
        initialize_query_completion(&mut qc);
        assert_eq!(qc.commandTag, CMDTAG_UNKNOWN);
        assert_eq!(qc.nprocessed, 0);
    }

    fn build(tag: CommandTag, nprocessed: u64, nameonly: bool) -> alloc::string::String {
        let ctx = MemoryContext::new("build_query_completion_string-test");
        let qc = QueryCompletion {
            commandTag: tag,
            nprocessed,
        };
        let s = build_query_completion_string(ctx.mcx(), &qc, nameonly).unwrap();
        alloc::string::String::from(s.as_str())
    }

    #[test]
    fn build_select_includes_rowcount() {
        assert_eq!(build(CMDTAG_SELECT, 5, false), "SELECT 5");
    }

    #[test]
    fn build_insert_includes_oid_zero_and_rowcount() {
        assert_eq!(build(CMDTAG_INSERT, 7, false), "INSERT 0 7");
    }

    #[test]
    fn build_update_delete_merge_rowcount() {
        assert_eq!(build(CMDTAG_UPDATE, 3, false), "UPDATE 3");
        assert_eq!(build(CMDTAG_DELETE, 0, false), "DELETE 0");
        assert_eq!(build(CMDTAG_MERGE, 12, false), "MERGE 12");
    }

    #[test]
    fn build_nameonly_omits_rowcount() {
        assert_eq!(build(CMDTAG_SELECT, 5, true), "SELECT");
        assert_eq!(build(CMDTAG_INSERT, 7, true), "INSERT");
    }

    #[test]
    fn build_non_rowcount_tag_is_name_only() {
        // BEGIN has display_rowcount=false.
        let begin = get_command_tag_enum(b"BEGIN");
        assert_eq!(build(begin, 99, false), "BEGIN");
    }

    #[test]
    fn build_large_rowcount() {
        use alloc::format;
        assert_eq!(
            build(CMDTAG_SELECT, u64::MAX, false),
            format!("SELECT {}", u64::MAX)
        );
        assert_eq!(
            build(CMDTAG_SELECT, 1234567890123456789, false),
            "SELECT 1234567890123456789"
        );
    }

    #[test]
    fn display_flags_byte_exact_spot_checks() {
        // From cmdtaglist.h: rowcount=true only for COPY, DELETE, FETCH, INSERT,
        // MERGE, MOVE, SELECT, UPDATE.
        let rowcount_true: &[&[u8]] = &[
            b"COPY", b"DELETE", b"FETCH", b"INSERT", b"MERGE", b"MOVE", b"SELECT", b"UPDATE",
        ];
        let expected_true = TAG_BEHAVIOR.iter().filter(|r| r.display_rowcount).count();
        assert_eq!(expected_true, rowcount_true.len());
        for name in rowcount_true {
            let tag = get_command_tag_enum(name);
            assert!(command_tag_display_rowcount(tag), "{name:?}");
        }

        // table_rewrite_ok=true only for ALTER MATERIALIZED VIEW, ALTER TABLE,
        // ALTER TYPE.
        let rewrite_true: &[&[u8]] = &[b"ALTER MATERIALIZED VIEW", b"ALTER TABLE", b"ALTER TYPE"];
        let expected_rw = TAG_BEHAVIOR.iter().filter(|r| r.table_rewrite_ok).count();
        assert_eq!(expected_rw, rewrite_true.len());
        for name in rewrite_true {
            let tag = get_command_tag_enum(name);
            assert!(command_tag_table_rewrite_ok(tag), "{name:?}");
        }
    }

    #[test]
    fn event_trigger_ok_spot_checks() {
        let yes = get_command_tag_enum(b"ALTER AGGREGATE");
        assert!(command_tag_event_trigger_ok(yes));
        let no = get_command_tag_enum(b"ALTER DATABASE");
        assert!(!command_tag_event_trigger_ok(no));
        let login = get_command_tag_enum(b"LOGIN");
        assert!(command_tag_event_trigger_ok(login));
    }

    #[test]
    fn name_and_len_matches() {
        for row in TAG_BEHAVIOR.iter() {
            let tag = get_command_tag_enum(row.name.as_bytes());
            let (n, len) = get_command_tag_name_and_len(tag);
            assert_eq!(n, row.name);
            assert_eq!(len, row.name.len());
        }
    }
}
