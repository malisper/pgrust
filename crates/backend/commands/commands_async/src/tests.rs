use super::*;

fn n(channel: &str, payload: &str) -> Notification {
    let mut data = Vec::new();
    data.extend_from_slice(channel.as_bytes());
    data.push(0);
    data.extend_from_slice(payload.as_bytes());
    data.push(0);
    Notification {
        channel_len: channel.len() as u16,
        payload_len: payload.len() as u16,
        data: data.into_boxed_slice(),
    }
}

#[test]
fn dedup_linear_and_hashed() {
    let mut list =
        NotificationList { nesting_level: 1, events: vec![n("a", "x")], hashtab: None, upper: None };
    assert!(exists_pending_notify(&list, &n("a", "x")));
    assert!(!exists_pending_notify(&list, &n("a", "y")));
    assert!(!exists_pending_notify(&list, &n("b", "x")));

    for i in 0..MIN_HASHABLE_NOTIFIES + 4 {
        let e = n(&format!("c{i}"), "p");
        if !exists_pending_notify(&list, &e) {
            add_event_to_pending_notifies(&mut list, e);
        }
    }
    assert!(list.hashtab.is_some());
    assert!(exists_pending_notify(&list, &n("c3", "p")));
    assert!(!exists_pending_notify(&list, &n("c3", "q")));
    let len = list.events.len();
    let dup = n("c3", "p");
    if !exists_pending_notify(&list, &dup) {
        add_event_to_pending_notifies(&mut list, dup);
    }
    assert_eq!(list.events.len(), len);
}

fn push_notify(level: i32, channel: &str, payload: &str) {
    let e = n(channel, payload);
    LOCAL.with(|s| {
        let mut pending = s.pending_notifies.borrow_mut();
        match pending.as_mut() {
            Some(list) if level <= list.nesting_level => {
                if !exists_pending_notify(list, &e) {
                    add_event_to_pending_notifies(list, e);
                }
            }
            _ => {
                *pending = Some(Box::new(NotificationList {
                    nesting_level: level,
                    events: vec![e],
                    hashtab: None,
                    upper: pending.take(),
                }));
            }
        }
    });
}

fn pending_payloads() -> Vec<String> {
    LOCAL.with(|s| {
        s.pending_notifies.borrow().as_ref().map_or(Vec::new(), |l| {
            l.events
                .iter()
                .map(|e| {
                    String::from_utf8_lossy(
                        &e.data[e.channel_len as usize + 1
                            ..e.channel_len as usize + 1 + e.payload_len as usize],
                    )
                    .into_owned()
                })
                .collect()
        })
    })
}

#[test]
fn subxact_commit_merges_without_dups_and_abort_pops() {
    clear_pending_actions_and_notifies();

    push_notify(1, "ch", "parent");
    push_notify(2, "ch", "parent"); // dup vs parent, dropped at merge
    push_notify(2, "ch", "child");
    assert_eq!(pending_payloads(), vec!["parent", "child"]);
    at_subcommit_merge(2);
    assert_eq!(pending_payloads(), vec!["parent", "child"]);
    assert_eq!(LOCAL.with(|s| s.pending_notifies.borrow().as_ref().unwrap().nesting_level), 1);

    // Abort of a deeper subxact discards only its list.
    push_notify(3, "ch", "doomed");
    at_subabort_pop(3);
    assert_eq!(pending_payloads(), vec!["parent", "child"]);

    // Level gap: child at level 3 under parent at 1 -> reparent by decrement.
    push_notify(3, "ch", "gap");
    at_subcommit_merge(3);
    assert_eq!(LOCAL.with(|s| s.pending_notifies.borrow().as_ref().unwrap().nesting_level), 2);
    at_subcommit_merge(2);
    assert_eq!(pending_payloads(), vec!["parent", "child", "gap"]);

    clear_pending_actions_and_notifies();
}

#[test]
fn at_prepare_rejects_pending_notify() {
    clear_pending_actions_and_notifies();
    assert!(AtPrepare_Notify().is_ok());
    push_notify(1, "ch", "p");
    let err = AtPrepare_Notify().unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_FEATURE_NOT_SUPPORTED);
    clear_pending_actions_and_notifies();
}
