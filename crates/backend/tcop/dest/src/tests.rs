use super::*;
use ::types_portal::{CMDTAG_INSERT, CMDTAG_SELECT};
use std::cell::{Cell, RefCell};
use std::sync::Once;

thread_local! {
    static SENT: RefCell<Vec<(u8, Vec<u8>)>> = const { RefCell::new(Vec::new()) };
    static FLUSHES: Cell<u32> = const { Cell::new(0) };
}

fn setup() -> mcx::MemoryContext {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        pqcomm_seams::pq_putmessage::set(|msgtype, body| {
            SENT.with(|s| s.borrow_mut().push((msgtype, body.to_vec())));
            Ok(0)
        });
        pqcomm_seams::pq_flush::set(|| {
            FLUSHES.with(|c| c.set(c.get() + 1));
            Ok(0)
        });
        xact_seams::transaction_block_status_code::set(|| b'I');
    });
    SENT.with(|s| s.borrow_mut().clear());
    FLUSHES.with(|c| c.set(0));
    mcx::MemoryContext::new("tcop_dest-test")
}

fn sent() -> Vec<(u8, Vec<u8>)> {
    SENT.with(|s| s.borrow().clone())
}

const ALL_DESTS: [CommandDest; 13] = [
    CommandDest::None,
    CommandDest::Debug,
    CommandDest::Remote,
    CommandDest::RemoteExecute,
    CommandDest::RemoteSimple,
    CommandDest::Spi,
    CommandDest::Tuplestore,
    CommandDest::IntoRel,
    CommandDest::CopyOut,
    CommandDest::SqlFunction,
    CommandDest::TransientRel,
    CommandDest::TupleQueue,
    CommandDest::ExplainSerialize,
];

#[test]
fn create_dest_receiver_mydest_roundtrip() {
    for dest in [
        CommandDest::None,
        CommandDest::Debug,
        CommandDest::Remote,
        CommandDest::RemoteExecute,
        CommandDest::RemoteSimple,
        CommandDest::Spi,
        CommandDest::Tuplestore,
    ] {
        assert_eq!(CreateDestReceiver(dest).mydest(), dest);
    }
    assert_eq!(NONE_RECEIVER.mydest(), CommandDest::None);
}

#[test]
fn create_dest_receiver_unported_owners_panic() {
    for dest in [
        CommandDest::IntoRel,
        CommandDest::CopyOut,
        CommandDest::SqlFunction,
        CommandDest::TransientRel,
        CommandDest::TupleQueue,
        CommandDest::ExplainSerialize,
    ] {
        assert!(std::panic::catch_unwind(move || CreateDestReceiver(dest)).is_err());
    }
}

fn empty_desc(mcx: mcx::Mcx<'_>) -> TupleDescData<'_> {
    TupleDescData {
        natts: 0,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: mcx::PgVec::new_in(mcx),
        attrs: mcx::PgVec::new_in(mcx),
    }
}

fn virtual_slot(mcx: mcx::Mcx<'_>) -> SlotData<'_> {
    SlotData::Virtual(types_slot::VirtualTupleTableSlot {
        base: types_slot::TupleTableSlot::new_in(mcx, types_slot::TupleSlotKind::Virtual),
        data: mcx::PgVec::new_in(mcx),
    })
}

#[test]
fn donothing_receiver_is_functional() {
    let ctx = setup();
    let mut r = CreateDestReceiver(CommandDest::None);
    let desc = empty_desc(ctx.mcx());
    let mut slot = virtual_slot(ctx.mcx());
    r.startup(1, &desc).unwrap();
    assert!(r.receive_slot(&mut slot).unwrap());
    r.shutdown().unwrap();
    r.destroy();
    BeginCommand(CMDTAG_SELECT, CommandDest::None);
}

#[test]
fn shell_receivers_panic_on_dispatch() {
    let ctx = setup();
    // Remote panics too: printtup's receive_slot on a descriptor-less slot.
    for dest in [CommandDest::Debug, CommandDest::Remote, CommandDest::RemoteSimple] {
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut r = CreateDestReceiver(dest);
            let mut slot = virtual_slot(ctx.mcx());
            let _ = r.receive_slot(&mut slot);
        }))
        .is_err());
    }
    // dest.c statics use donothingCleanup for rShutdown: real no-op.
    CreateDestReceiver(CommandDest::Debug).shutdown().unwrap();
    CreateDestReceiver(CommandDest::RemoteSimple).shutdown().unwrap();
    CreateDestReceiver(CommandDest::Spi).shutdown().unwrap();
}

#[test]
fn end_command_sends_tag_with_nul_for_remote_only() {
    setup();
    let qc = QueryCompletion {
        commandTag: CMDTAG_SELECT,
        nprocessed: 5,
    };
    for dest in ALL_DESTS {
        if !matches!(
            dest,
            CommandDest::Remote | CommandDest::RemoteExecute | CommandDest::RemoteSimple
        ) {
            EndCommand(&qc, dest, false).unwrap();
        }
    }
    assert!(sent().is_empty());

    EndCommand(&qc, CommandDest::Remote, false).unwrap();
    assert_eq!(sent(), vec![(b'C', b"SELECT 5\0".to_vec())]);

    let qc = QueryCompletion {
        commandTag: CMDTAG_INSERT,
        nprocessed: 7,
    };
    EndCommand(&qc, CommandDest::RemoteExecute, true).unwrap();
    assert_eq!(sent()[1], (b'C', b"INSERT\0".to_vec()));
}

#[test]
fn end_replication_command_sends_tag_with_nul() {
    setup();
    EndReplicationCommand(b"COPY 0").unwrap();
    assert_eq!(sent(), vec![(b'C', b"COPY 0\0".to_vec())]);
}

#[test]
fn null_command_sends_empty_query_response_for_remote_only() {
    setup();
    for dest in ALL_DESTS {
        NullCommand(dest).unwrap();
    }
    assert_eq!(
        sent(),
        vec![(b'I', vec![]), (b'I', vec![]), (b'I', vec![])]
    );
}

#[test]
fn ready_for_query_sends_status_and_flushes_for_remote_only() {
    let ctx = setup();
    for dest in ALL_DESTS {
        ReadyForQuery(ctx.mcx(), dest).unwrap();
    }
    assert_eq!(
        sent(),
        vec![(b'Z', b"I".to_vec()), (b'Z', b"I".to_vec()), (b'Z', b"I".to_vec())]
    );
    assert_eq!(FLUSHES.with(|c| c.get()), 3);
}
