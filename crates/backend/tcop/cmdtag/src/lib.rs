// cmdtag.c — command-tag names, flags, and enumeration (PG 18.3).
#![allow(non_snake_case)]

use ::numutils::pg_ulltoa_n;
use ::pgstrcasecmp::pg_strcasecmp;
use ::types_core::CommandTag;
use ::types_portal::{QueryCompletion, CMDTAG_INSERT, CMDTAG_UNKNOWN, COMPLETION_TAG_BUFSIZE};

#[cfg(test)]
mod tests;

const MAXINT8LEN: usize = 20;

pub struct CommandTagBehavior {
    pub name: &'static str,
    pub event_trigger_ok: bool,
    pub table_rewrite_ok: bool,
    pub display_rowcount: bool,
}

macro_rules! t {
    ($name:literal, $evtrgok:literal, $rwrok:literal, $rowcnt:literal) => {
        CommandTagBehavior {
            name: $name,
            event_trigger_ok: $evtrgok,
            table_rewrite_ok: $rwrok,
            display_rowcount: $rowcnt,
        }
    };
}

// Row order is cmdtaglist.h order (sorted by name for the bsearch); the row
// index IS the CommandTag value. Generated from the header, diff-verified.
pub static TAG_BEHAVIOR: [CommandTagBehavior; 193] = [
    t!("???", false, false, false),
    t!("ALTER ACCESS METHOD", true, false, false),
    t!("ALTER AGGREGATE", true, false, false),
    t!("ALTER CAST", true, false, false),
    t!("ALTER COLLATION", true, false, false),
    t!("ALTER CONSTRAINT", true, false, false),
    t!("ALTER CONVERSION", true, false, false),
    t!("ALTER DATABASE", false, false, false),
    t!("ALTER DEFAULT PRIVILEGES", true, false, false),
    t!("ALTER DOMAIN", true, false, false),
    t!("ALTER EVENT TRIGGER", false, false, false),
    t!("ALTER EXTENSION", true, false, false),
    t!("ALTER FOREIGN DATA WRAPPER", true, false, false),
    t!("ALTER FOREIGN TABLE", true, false, false),
    t!("ALTER FUNCTION", true, false, false),
    t!("ALTER INDEX", true, false, false),
    t!("ALTER LANGUAGE", true, false, false),
    t!("ALTER LARGE OBJECT", true, false, false),
    t!("ALTER MATERIALIZED VIEW", true, true, false),
    t!("ALTER OPERATOR", true, false, false),
    t!("ALTER OPERATOR CLASS", true, false, false),
    t!("ALTER OPERATOR FAMILY", true, false, false),
    t!("ALTER POLICY", true, false, false),
    t!("ALTER PROCEDURE", true, false, false),
    t!("ALTER PUBLICATION", true, false, false),
    t!("ALTER ROLE", false, false, false),
    t!("ALTER ROUTINE", true, false, false),
    t!("ALTER RULE", true, false, false),
    t!("ALTER SCHEMA", true, false, false),
    t!("ALTER SEQUENCE", true, false, false),
    t!("ALTER SERVER", true, false, false),
    t!("ALTER STATISTICS", true, false, false),
    t!("ALTER SUBSCRIPTION", true, false, false),
    t!("ALTER SYSTEM", false, false, false),
    t!("ALTER TABLE", true, true, false),
    t!("ALTER TABLESPACE", false, false, false),
    t!("ALTER TEXT SEARCH CONFIGURATION", true, false, false),
    t!("ALTER TEXT SEARCH DICTIONARY", true, false, false),
    t!("ALTER TEXT SEARCH PARSER", true, false, false),
    t!("ALTER TEXT SEARCH TEMPLATE", true, false, false),
    t!("ALTER TRANSFORM", true, false, false),
    t!("ALTER TRIGGER", true, false, false),
    t!("ALTER TYPE", true, true, false),
    t!("ALTER USER MAPPING", true, false, false),
    t!("ALTER VIEW", true, false, false),
    t!("ANALYZE", false, false, false),
    t!("BEGIN", false, false, false),
    t!("CALL", false, false, false),
    t!("CHECKPOINT", false, false, false),
    t!("CLOSE", false, false, false),
    t!("CLOSE CURSOR", false, false, false),
    t!("CLOSE CURSOR ALL", false, false, false),
    t!("CLUSTER", false, false, false),
    t!("COMMENT", true, false, false),
    t!("COMMIT", false, false, false),
    t!("COMMIT PREPARED", false, false, false),
    t!("COPY", false, false, true),
    t!("COPY FROM", false, false, false),
    t!("CREATE ACCESS METHOD", true, false, false),
    t!("CREATE AGGREGATE", true, false, false),
    t!("CREATE CAST", true, false, false),
    t!("CREATE COLLATION", true, false, false),
    t!("CREATE CONSTRAINT", true, false, false),
    t!("CREATE CONVERSION", true, false, false),
    t!("CREATE DATABASE", false, false, false),
    t!("CREATE DOMAIN", true, false, false),
    t!("CREATE EVENT TRIGGER", false, false, false),
    t!("CREATE EXTENSION", true, false, false),
    t!("CREATE FOREIGN DATA WRAPPER", true, false, false),
    t!("CREATE FOREIGN TABLE", true, false, false),
    t!("CREATE FUNCTION", true, false, false),
    t!("CREATE INDEX", true, false, false),
    t!("CREATE LANGUAGE", true, false, false),
    t!("CREATE MATERIALIZED VIEW", true, false, false),
    t!("CREATE OPERATOR", true, false, false),
    t!("CREATE OPERATOR CLASS", true, false, false),
    t!("CREATE OPERATOR FAMILY", true, false, false),
    t!("CREATE POLICY", true, false, false),
    t!("CREATE PROCEDURE", true, false, false),
    t!("CREATE PUBLICATION", true, false, false),
    t!("CREATE ROLE", false, false, false),
    t!("CREATE ROUTINE", true, false, false),
    t!("CREATE RULE", true, false, false),
    t!("CREATE SCHEMA", true, false, false),
    t!("CREATE SEQUENCE", true, false, false),
    t!("CREATE SERVER", true, false, false),
    t!("CREATE STATISTICS", true, false, false),
    t!("CREATE SUBSCRIPTION", true, false, false),
    t!("CREATE TABLE", true, false, false),
    t!("CREATE TABLE AS", true, false, false),
    t!("CREATE TABLESPACE", false, false, false),
    t!("CREATE TEXT SEARCH CONFIGURATION", true, false, false),
    t!("CREATE TEXT SEARCH DICTIONARY", true, false, false),
    t!("CREATE TEXT SEARCH PARSER", true, false, false),
    t!("CREATE TEXT SEARCH TEMPLATE", true, false, false),
    t!("CREATE TRANSFORM", true, false, false),
    t!("CREATE TRIGGER", true, false, false),
    t!("CREATE TYPE", true, false, false),
    t!("CREATE USER MAPPING", true, false, false),
    t!("CREATE VIEW", true, false, false),
    t!("DEALLOCATE", false, false, false),
    t!("DEALLOCATE ALL", false, false, false),
    t!("DECLARE CURSOR", false, false, false),
    t!("DELETE", false, false, true),
    t!("DISCARD", false, false, false),
    t!("DISCARD ALL", false, false, false),
    t!("DISCARD PLANS", false, false, false),
    t!("DISCARD SEQUENCES", false, false, false),
    t!("DISCARD TEMP", false, false, false),
    t!("DO", false, false, false),
    t!("DROP ACCESS METHOD", true, false, false),
    t!("DROP AGGREGATE", true, false, false),
    t!("DROP CAST", true, false, false),
    t!("DROP COLLATION", true, false, false),
    t!("DROP CONSTRAINT", true, false, false),
    t!("DROP CONVERSION", true, false, false),
    t!("DROP DATABASE", false, false, false),
    t!("DROP DOMAIN", true, false, false),
    t!("DROP EVENT TRIGGER", false, false, false),
    t!("DROP EXTENSION", true, false, false),
    t!("DROP FOREIGN DATA WRAPPER", true, false, false),
    t!("DROP FOREIGN TABLE", true, false, false),
    t!("DROP FUNCTION", true, false, false),
    t!("DROP INDEX", true, false, false),
    t!("DROP LANGUAGE", true, false, false),
    t!("DROP MATERIALIZED VIEW", true, false, false),
    t!("DROP OPERATOR", true, false, false),
    t!("DROP OPERATOR CLASS", true, false, false),
    t!("DROP OPERATOR FAMILY", true, false, false),
    t!("DROP OWNED", true, false, false),
    t!("DROP POLICY", true, false, false),
    t!("DROP PROCEDURE", true, false, false),
    t!("DROP PUBLICATION", true, false, false),
    t!("DROP ROLE", false, false, false),
    t!("DROP ROUTINE", true, false, false),
    t!("DROP RULE", true, false, false),
    t!("DROP SCHEMA", true, false, false),
    t!("DROP SEQUENCE", true, false, false),
    t!("DROP SERVER", true, false, false),
    t!("DROP STATISTICS", true, false, false),
    t!("DROP SUBSCRIPTION", true, false, false),
    t!("DROP TABLE", true, false, false),
    t!("DROP TABLESPACE", false, false, false),
    t!("DROP TEXT SEARCH CONFIGURATION", true, false, false),
    t!("DROP TEXT SEARCH DICTIONARY", true, false, false),
    t!("DROP TEXT SEARCH PARSER", true, false, false),
    t!("DROP TEXT SEARCH TEMPLATE", true, false, false),
    t!("DROP TRANSFORM", true, false, false),
    t!("DROP TRIGGER", true, false, false),
    t!("DROP TYPE", true, false, false),
    t!("DROP USER MAPPING", true, false, false),
    t!("DROP VIEW", true, false, false),
    t!("EXECUTE", false, false, false),
    t!("EXPLAIN", false, false, false),
    t!("FETCH", false, false, true),
    t!("GRANT", true, false, false),
    t!("GRANT ROLE", false, false, false),
    t!("IMPORT FOREIGN SCHEMA", true, false, false),
    t!("INSERT", false, false, true),
    t!("LISTEN", false, false, false),
    t!("LOAD", false, false, false),
    t!("LOCK TABLE", false, false, false),
    t!("LOGIN", true, false, false),
    t!("MERGE", false, false, true),
    t!("MOVE", false, false, true),
    t!("NOTIFY", false, false, false),
    t!("PREPARE", false, false, false),
    t!("PREPARE TRANSACTION", false, false, false),
    t!("REASSIGN OWNED", false, false, false),
    t!("REFRESH MATERIALIZED VIEW", true, false, false),
    t!("REINDEX", true, false, false),
    t!("RELEASE", false, false, false),
    t!("RESET", false, false, false),
    t!("REVOKE", true, false, false),
    t!("REVOKE ROLE", false, false, false),
    t!("ROLLBACK", false, false, false),
    t!("ROLLBACK PREPARED", false, false, false),
    t!("SAVEPOINT", false, false, false),
    t!("SECURITY LABEL", true, false, false),
    t!("SELECT", false, false, true),
    t!("SELECT FOR KEY SHARE", false, false, false),
    t!("SELECT FOR NO KEY UPDATE", false, false, false),
    t!("SELECT FOR SHARE", false, false, false),
    t!("SELECT FOR UPDATE", false, false, false),
    t!("SELECT INTO", true, false, false),
    t!("SET", false, false, false),
    t!("SET CONSTRAINTS", false, false, false),
    t!("SHOW", false, false, false),
    t!("START TRANSACTION", false, false, false),
    t!("TRUNCATE TABLE", false, false, false),
    t!("UNLISTEN", false, false, false),
    t!("UPDATE", false, false, true),
    t!("VACUUM", false, false, false),
];

#[inline]
fn row(tag: CommandTag) -> &'static CommandTagBehavior {
    &TAG_BEHAVIOR[tag.0 as usize]
}

pub fn InitializeQueryCompletion(qc: &mut QueryCompletion) {
    qc.commandTag = CMDTAG_UNKNOWN;
    qc.nprocessed = 0;
}

pub fn GetCommandTagName(commandTag: CommandTag) -> &'static str {
    row(commandTag).name
}

// C caches namelen as a u8 alongside the name; &'static str already carries
// the byte length (all names are ASCII), so this is the same load.
pub fn GetCommandTagNameAndLen(commandTag: CommandTag) -> (&'static str, usize) {
    let name = row(commandTag).name;
    (name, name.len())
}

pub fn command_tag_display_rowcount(commandTag: CommandTag) -> bool {
    row(commandTag).display_rowcount
}

pub fn command_tag_event_trigger_ok(commandTag: CommandTag) -> bool {
    row(commandTag).event_trigger_ok
}

pub fn command_tag_table_rewrite_ok(commandTag: CommandTag) -> bool {
    row(commandTag).table_rewrite_ok
}

pub fn GetCommandTagEnum(commandname: &[u8]) -> CommandTag {
    // C strings end at the first NUL; mirror the *commandname == '\0' guard.
    let name = match commandname.iter().position(|&b| b == 0) {
        Some(end) => &commandname[..end],
        None => commandname,
    };
    if name.is_empty() {
        return CMDTAG_UNKNOWN;
    }

    let mut base: isize = 0;
    let mut last: isize = TAG_BEHAVIOR.len() as isize - 1;
    while last >= base {
        let position = base + ((last - base) >> 1);
        let result = pg_strcasecmp(name, TAG_BEHAVIOR[position as usize].name.as_bytes());
        if result == 0 {
            return CommandTag(position as i32);
        } else if result < 0 {
            last = position - 1;
        } else {
            base = position + 1;
        }
    }
    CMDTAG_UNKNOWN
}

// Writes the completion tag (plus " 0"/rowcount decoration and the trailing
// NUL the wire message carries) into the caller's stack buffer and returns
// its strlen — the C shape; per-statement, zero allocation.
pub fn BuildQueryCompletionString(
    buff: &mut [u8; COMPLETION_TAG_BUFSIZE],
    qc: &QueryCompletion,
    nameonly: bool,
) -> usize {
    let tag = qc.commandTag;
    let (tagname, taglen) = GetCommandTagNameAndLen(tag);

    buff[..taglen].copy_from_slice(tagname.as_bytes());
    let mut bufp = taglen;

    debug_assert!(taglen <= COMPLETION_TAG_BUFSIZE - MAXINT8LEN - 4);

    if command_tag_display_rowcount(tag) && !nameonly {
        // WITH OIDS compatibility: INSERT keeps a "0" where the new row's Oid
        // once went (see cmdtag.c).
        if tag == CMDTAG_INSERT {
            buff[bufp] = b' ';
            buff[bufp + 1] = b'0';
            bufp += 2;
        }
        buff[bufp] = b' ';
        bufp += 1;
        bufp += pg_ulltoa_n(qc.nprocessed, &mut buff[bufp..]);
    }

    buff[bufp] = 0;
    bufp
}
