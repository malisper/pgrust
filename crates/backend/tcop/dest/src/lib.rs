// dest.c — communication-destination management (PG 18.3).
#![allow(non_snake_case)]

use ::mcx::Mcx;
use ::types_core::CommandTag;
use ::types_dest::CommandDest;
use ::types_error::PgResult;
use ::types_portal::{Portal, QueryCompletion, COMPLETION_TAG_BUFSIZE};
use ::types_slot::SlotData;
use ::types_tuple::TupleDescData;

#[cfg(test)]
mod tests;

const PQMSG_COMMAND_COMPLETE: u8 = b'C';
const PQMSG_READY_FOR_QUERY: u8 = b'Z';
const PQMSG_EMPTY_QUERY_RESPONSE: u8 = b'I';

// C's DestReceiver is a vtable struct downcast by its owner
// (`(DR_printtup *) self`); the receiver set is closed (one constructor per
// CommandDest in CreateDestReceiver's switch), so dispatch is an enum match
// (rule 4): receive_slot is per-row hot at M1 and each arm is a direct call.
pub enum DestReceiver<'mcx> {
    DoNothing,                             // donothingDR (DestNone); fully functional
    DebugTup,                              // debugtupDR shell; callbacks in printtup.c
    PrintTup(printtup::DrPrinttup<'mcx>),  // printtup_create_DR(Remote|RemoteExecute)
    PrintSimple,                           // printsimpleDR shell; callbacks in printsimple.c
    SpiPrintTup,                           // spi_printtupDR shell; callbacks in spi.c
    Tuplestore(tstore_receiver::DrTstore), // CreateTuplestoreDestReceiver (tstoreReceiver.c)
    IntoRel(createas_seams::IntoRelState<'mcx>), // CreateIntoRelDestReceiver (createas.c)
    TransientRel(matview_seams::TransientRelState<'mcx>), // CreateTransientRelDestReceiver (matview.c)
}

impl<'mcx> DestReceiver<'mcx> {
    // false means "stop early, as if the scan ended".
    #[inline]
    pub fn receive_slot(&mut self, slot: &mut SlotData<'mcx>) -> PgResult<bool> {
        match self {
            DestReceiver::DoNothing => Ok(true),
            DestReceiver::PrintTup(dr) => dr.receive_slot(slot),
            DestReceiver::SpiPrintTup => spi_seams::spi_printtup::call(slot),
            DestReceiver::Tuplestore(dr) => dr.receive_slot(slot),
            DestReceiver::IntoRel(state) => createas_seams::intorel_receive::call(state, slot),
            DestReceiver::TransientRel(state) => {
                matview_seams::transientrel_receive::call(state, slot)
            }
            other => other.unported("receiveSlot"),
        }
    }

    pub fn startup(&mut self, operation: i32, typeinfo: &TupleDescData<'_>) -> PgResult<()> {
        match self {
            DestReceiver::DoNothing => Ok(()),
            DestReceiver::PrintTup(dr) => dr.startup(operation, typeinfo),
            DestReceiver::SpiPrintTup => spi_seams::spi_dest_startup::call(operation, typeinfo),
            DestReceiver::Tuplestore(dr) => dr.startup(operation, typeinfo),
            DestReceiver::IntoRel(state) => {
                createas_seams::intorel_startup::call(state, operation, typeinfo)
            }
            DestReceiver::TransientRel(state) => {
                matview_seams::transientrel_startup::call(state, operation, typeinfo)
            }
            other => other.unported("rStartup"),
        }
    }

    pub fn shutdown(&mut self) -> PgResult<()> {
        match self {
            DestReceiver::DoNothing
            | DestReceiver::DebugTup
            | DestReceiver::PrintSimple
            | DestReceiver::SpiPrintTup => Ok(()),
            DestReceiver::PrintTup(dr) => {
                dr.shutdown();
                Ok(())
            }
            DestReceiver::Tuplestore(dr) => {
                dr.shutdown();
                Ok(())
            }
            DestReceiver::IntoRel(state) => createas_seams::intorel_shutdown::call(state),
            DestReceiver::TransientRel(state) => matview_seams::transientrel_shutdown::call(state),
        }
    }

    // rDestroy: statics use donothingCleanup, printtup pfrees; here it drops.
    pub fn destroy(self) {}

    pub fn mydest(&self) -> CommandDest {
        match self {
            DestReceiver::DoNothing => CommandDest::None,
            DestReceiver::DebugTup => CommandDest::Debug,
            DestReceiver::PrintTup(dr) => dr.mydest,
            DestReceiver::PrintSimple => CommandDest::RemoteSimple,
            DestReceiver::SpiPrintTup => CommandDest::Spi,
            DestReceiver::Tuplestore(_) => CommandDest::Tuplestore,
            DestReceiver::IntoRel(_) => CommandDest::IntoRel,
            DestReceiver::TransientRel(_) => CommandDest::TransientRel,
        }
    }

    #[cold]
    fn unported(&self, method: &str) -> ! {
        panic!(
            "DestReceiver {:?}.{method}: owner callbacks not ported yet \
             (printtup.c/printsimple.c/spi.c)",
            self.mydest()
        )
    }
}

// SetRemoteDestReceiverParams (printtup.c) at the enum boundary: C downcasts
// the DestReceiver*, the match is the same demux.
pub fn SetRemoteDestReceiverParams<'mcx>(receiver: &mut DestReceiver<'mcx>, portal: Portal<'mcx>) {
    match receiver {
        DestReceiver::PrintTup(dr) => printtup::SetRemoteDestReceiverParams(dr, portal),
        _ => panic!("SetRemoteDestReceiverParams: not a printtup receiver"),
    }
}

// SetTuplestoreDestReceiverParams (tstoreReceiver.c) at the enum boundary.
pub fn SetTuplestoreDestReceiverParams(
    receiver: &mut DestReceiver<'_>,
    tstore: types_portal::TuplestoreHandle,
    detoast: bool,
) {
    match receiver {
        DestReceiver::Tuplestore(dr) => tstore_receiver::set_params(dr, tstore, detoast),
        _ => panic!("SetTuplestoreDestReceiverParams: not a tuplestore receiver"),
    }
}

// DestReceiver *None_Receiver: C's shared static donothingDR.
pub const NONE_RECEIVER: DestReceiver<'static> = DestReceiver::DoNothing;

pub fn BeginCommand(_commandTag: CommandTag, _dest: CommandDest) {
    // Nothing to do at present
}

pub fn CreateDestReceiver<'mcx>(dest: CommandDest) -> DestReceiver<'mcx> {
    match dest {
        CommandDest::Remote | CommandDest::RemoteExecute => {
            DestReceiver::PrintTup(printtup::printtup_create_DR(dest))
        }
        CommandDest::RemoteSimple => DestReceiver::PrintSimple,
        CommandDest::None => DestReceiver::DoNothing,
        CommandDest::Debug => DestReceiver::DebugTup,
        CommandDest::Spi => DestReceiver::SpiPrintTup,
        CommandDest::Tuplestore => DestReceiver::Tuplestore(tstore_receiver::tstore_create_DR()),
        // Constructors owned by unported units.
        CommandDest::IntoRel
        | CommandDest::CopyOut
        | CommandDest::SqlFunction
        | CommandDest::TransientRel
        | CommandDest::TupleQueue
        | CommandDest::ExplainSerialize => {
            panic!("CreateDestReceiver({dest:?}): owning unit not ported yet")
        }
    }
}

pub fn EndCommand(
    qc: &QueryCompletion,
    dest: CommandDest,
    force_undecorated_output: bool,
) -> PgResult<()> {
    match dest {
        CommandDest::Remote | CommandDest::RemoteExecute | CommandDest::RemoteSimple => {
            let mut completionTag = [0u8; COMPLETION_TAG_BUFSIZE];
            let len =
                cmdtag::BuildQueryCompletionString(&mut completionTag, qc, force_undecorated_output);
            // len + 1 ships the trailing NUL, as C does.
            pqcomm_seams::pq_putmessage::call(PQMSG_COMMAND_COMPLETE, &completionTag[..len + 1])?;
        }

        CommandDest::None
        | CommandDest::Debug
        | CommandDest::Spi
        | CommandDest::Tuplestore
        | CommandDest::IntoRel
        | CommandDest::CopyOut
        | CommandDest::SqlFunction
        | CommandDest::TransientRel
        | CommandDest::TupleQueue
        | CommandDest::ExplainSerialize => {}
    }
    Ok(())
}

pub fn EndReplicationCommand(commandTag: &[u8]) -> PgResult<()> {
    // Replication tags are short constants; stack-stage tag + NUL, no palloc.
    let mut buf = [0u8; COMPLETION_TAG_BUFSIZE];
    assert!(commandTag.len() < COMPLETION_TAG_BUFSIZE);
    buf[..commandTag.len()].copy_from_slice(commandTag);
    pqcomm_seams::pq_putmessage::call(PQMSG_COMMAND_COMPLETE, &buf[..commandTag.len() + 1])?;
    Ok(())
}

pub fn NullCommand(dest: CommandDest) -> PgResult<()> {
    match dest {
        CommandDest::Remote | CommandDest::RemoteExecute | CommandDest::RemoteSimple => {
            pqformat::pq_putemptymessage(PQMSG_EMPTY_QUERY_RESPONSE)?;
        }

        CommandDest::None
        | CommandDest::Debug
        | CommandDest::Spi
        | CommandDest::Tuplestore
        | CommandDest::IntoRel
        | CommandDest::CopyOut
        | CommandDest::SqlFunction
        | CommandDest::TransientRel
        | CommandDest::TupleQueue
        | CommandDest::ExplainSerialize => {}
    }
    Ok(())
}

pub fn ReadyForQuery(mcx: Mcx<'_>, dest: CommandDest) -> PgResult<()> {
    match dest {
        CommandDest::Remote | CommandDest::RemoteExecute | CommandDest::RemoteSimple => {
            let mut buf = pqformat::pq_beginmessage(mcx, PQMSG_READY_FOR_QUERY)?;
            pqformat::pq_sendbyte(&mut buf, xact_seams::transaction_block_status_code::call())?;
            pqformat::pq_endmessage(buf)?;
            pqcomm_seams::pq_flush::call()?;
        }

        CommandDest::None
        | CommandDest::Debug
        | CommandDest::Spi
        | CommandDest::Tuplestore
        | CommandDest::IntoRel
        | CommandDest::CopyOut
        | CommandDest::SqlFunction
        | CommandDest::TransientRel
        | CommandDest::TupleQueue
        | CommandDest::ExplainSerialize => {}
    }
    Ok(())
}

pub fn init_seams() {}
