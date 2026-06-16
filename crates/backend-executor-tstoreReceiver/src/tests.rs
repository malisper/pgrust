//! Tests for the tuplestore `DestReceiver`.

extern crate std;

use super::*;

/// `CreateTuplestoreDestReceiver` registers a live receiver in the tcop-dest
/// router and a per-receiver state slot; `dest_destroy` releases it.
#[test]
fn create_and_destroy_roundtrip() {
    let dr = CreateTuplestoreDestReceiver();
    assert_ne!(dr, DestReceiverHandle::NULL);

    // The state slot exists and the router handle is recorded.
    let token = token_for_handle(dr).expect("receiver state registered");
    with_receiver(token, |st| {
        assert_eq!(st.dr_handle, dr);
        assert_eq!(st.mode, ReceiveMode::Notoast);
        assert!(st.portal.is_none());
        assert!(!st.detoast);
    });

    // dest_destroy releases the slot.
    tstore_destroy_receiver(dr).unwrap();
    assert!(token_for_handle(dr).is_none());
}

/// `varatt_is_external` matches the 1-byte-header external tag (`0x01`).
#[test]
fn varatt_external_tag() {
    assert!(varatt_is_external(&[0x01, 0x00]));
    assert!(!varatt_is_external(&[0x02, 0x00]));
    assert!(!varatt_is_external(&[]));
}
