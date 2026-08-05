use super::*;
use ::types_portal::{
    CMDTAG_DELETE, CMDTAG_FETCH, CMDTAG_MERGE, CMDTAG_MOVE, CMDTAG_SELECT, CMDTAG_UPDATE,
};

#[test]
fn table_shape() {
    assert_eq!(TAG_BEHAVIOR.len(), 193);
    for r in TAG_BEHAVIOR.iter() {
        assert!(r.name.is_ascii());
        assert!(!r.name.is_empty());
        assert!(r.name.len() <= COMPLETION_TAG_BUFSIZE - MAXINT8LEN - 4);
    }
    for w in TAG_BEHAVIOR.windows(2) {
        assert!(
            pg_strcasecmp(w[0].name.as_bytes(), w[1].name.as_bytes()) < 0,
            "not sorted: {:?} vs {:?}",
            w[0].name,
            w[1].name
        );
    }
}

#[test]
fn known_positions_match_cmdtaglist_h() {
    assert_eq!(GetCommandTagName(CMDTAG_UNKNOWN), "???");
    assert_eq!(GetCommandTagName(CMDTAG_DELETE), "DELETE");
    assert_eq!(GetCommandTagName(CMDTAG_FETCH), "FETCH");
    assert_eq!(GetCommandTagName(CMDTAG_INSERT), "INSERT");
    assert_eq!(GetCommandTagName(CMDTAG_MERGE), "MERGE");
    assert_eq!(GetCommandTagName(CMDTAG_MOVE), "MOVE");
    assert_eq!(GetCommandTagName(CommandTag::REFRESH_MATERIALIZED_VIEW), "REFRESH MATERIALIZED VIEW");
    assert_eq!(GetCommandTagName(CMDTAG_SELECT), "SELECT");
    assert_eq!(GetCommandTagName(CMDTAG_UPDATE), "UPDATE");
}

#[test]
fn flag_sets_match_cmdtaglist_h() {
    let rowcount: Vec<&str> = TAG_BEHAVIOR
        .iter()
        .filter(|r| r.display_rowcount)
        .map(|r| r.name)
        .collect();
    assert_eq!(
        rowcount,
        ["COPY", "DELETE", "FETCH", "INSERT", "MERGE", "MOVE", "SELECT", "UPDATE"]
    );
    let rewrite: Vec<&str> = TAG_BEHAVIOR
        .iter()
        .filter(|r| r.table_rewrite_ok)
        .map(|r| r.name)
        .collect();
    assert_eq!(rewrite, ["ALTER MATERIALIZED VIEW", "ALTER TABLE", "ALTER TYPE"]);
    assert_eq!(TAG_BEHAVIOR.iter().filter(|r| r.event_trigger_ok).count(), 124);
    assert!(command_tag_event_trigger_ok(GetCommandTagEnum(b"LOGIN")));
    assert!(!command_tag_event_trigger_ok(GetCommandTagEnum(b"ALTER DATABASE")));
}

#[test]
fn enum_roundtrips_every_name() {
    for (i, r) in TAG_BEHAVIOR.iter().enumerate() {
        assert_eq!(GetCommandTagEnum(r.name.as_bytes()), CommandTag(i as i32));
        let lower = r.name.to_ascii_lowercase();
        assert_eq!(GetCommandTagEnum(lower.as_bytes()), CommandTag(i as i32));
    }
}

#[test]
fn enum_edge_cases() {
    assert_eq!(GetCommandTagEnum(b""), CMDTAG_UNKNOWN);
    assert_eq!(GetCommandTagEnum(b"\0"), CMDTAG_UNKNOWN);
    assert_eq!(GetCommandTagEnum(b"NOT A COMMAND"), CMDTAG_UNKNOWN);
    assert_eq!(GetCommandTagEnum(b"SELECT\0junk"), CMDTAG_SELECT);
    assert_eq!(GetCommandTagEnum(b"SELEC"), CMDTAG_UNKNOWN);
    assert_eq!(GetCommandTagEnum(b"SELECTX"), CMDTAG_UNKNOWN);
}

#[test]
fn initialize_query_completion_resets() {
    let mut qc = QueryCompletion {
        commandTag: CMDTAG_SELECT,
        nprocessed: 42,
    };
    InitializeQueryCompletion(&mut qc);
    assert_eq!(qc.commandTag, CMDTAG_UNKNOWN);
    assert_eq!(qc.nprocessed, 0);
}

fn build(tag: CommandTag, nprocessed: u64, nameonly: bool) -> String {
    let mut buff = [0xAAu8; COMPLETION_TAG_BUFSIZE];
    let qc = QueryCompletion {
        commandTag: tag,
        nprocessed,
    };
    let len = BuildQueryCompletionString(&mut buff, &qc, nameonly);
    assert_eq!(buff[len], 0, "NUL-terminated at the returned strlen");
    String::from_utf8(buff[..len].to_vec()).unwrap()
}

#[test]
fn completion_strings_match_c() {
    assert_eq!(build(CMDTAG_SELECT, 5, false), "SELECT 5");
    assert_eq!(build(CMDTAG_INSERT, 7, false), "INSERT 0 7");
    assert_eq!(build(CMDTAG_UPDATE, 3, false), "UPDATE 3");
    assert_eq!(build(CMDTAG_DELETE, 0, false), "DELETE 0");
    assert_eq!(build(CMDTAG_MERGE, 12, false), "MERGE 12");
    assert_eq!(build(CMDTAG_SELECT, 5, true), "SELECT");
    assert_eq!(build(CMDTAG_INSERT, 7, true), "INSERT");
    assert_eq!(build(GetCommandTagEnum(b"BEGIN"), 99, false), "BEGIN");
    assert_eq!(
        build(CMDTAG_SELECT, u64::MAX, false),
        format!("SELECT {}", u64::MAX)
    );
}

#[test]
fn name_and_len_agree() {
    for (i, r) in TAG_BEHAVIOR.iter().enumerate() {
        let (n, len) = GetCommandTagNameAndLen(CommandTag(i as i32));
        assert_eq!(n, r.name);
        assert_eq!(len, r.name.len());
    }
}
