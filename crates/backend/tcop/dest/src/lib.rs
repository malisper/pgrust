// dest.c — communication-destination management (PG 18.3).
#![allow(non_snake_case)]

use ::mcx::Mcx;
use ::types_core::CommandTag;
use ::types_dest::CommandDest;
use ::types_error::PgResult;
use ::types_portal::{QueryCompletion, COMPLETION_TAG_BUFSIZE};
use ::types_slot::TupleTableSlot;
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
// When printtup lands, PrintTup grows its DR_printtup state —
// `PrintTup(printtup::DrPrinttup<'mcx>)`, lifting the enum to
// `DestReceiver<'mcx>` — and these arms call into it; other owners likewise.
pub enum DestReceiver {
    DoNothing,             // donothingDR (DestNone); fully functional
    DebugTup,              // debugtupDR shell; callbacks in printtup.c
    PrintTup(CommandDest), // printtup_create_DR(Remote|RemoteExecute) shell
    PrintSimple,           // printsimpleDR shell; callbacks in printsimple.c
    SpiPrintTup,           // spi_printtupDR shell; callbacks in spi.c
}

impl DestReceiver {
    // false means "stop early, as if the scan ended".
    #[inline]
    pub fn receive_slot(&mut self, _slot: &mut TupleTableSlot<'_>) -> PgResult<bool> {
        match self {
            DestReceiver::DoNothing => Ok(true),
            other => other.unported("receiveSlot"),
        }
    }

    pub fn startup(&mut self, _operation: i32, _typeinfo: &TupleDescData<'_>) -> PgResult<()> {
        match self {
            DestReceiver::DoNothing => Ok(()),
            other => other.unported("rStartup"),
        }
    }

    pub fn shutdown(&mut self) -> PgResult<()> {
        match self {
            DestReceiver::DoNothing
            | DestReceiver::DebugTup
            | DestReceiver::PrintSimple
            | DestReceiver::SpiPrintTup => Ok(()),
            other @ DestReceiver::PrintTup(_) => other.unported("rShutdown"),
        }
    }

    // rDestroy: statics use donothingCleanup, printtup pfrees; here it drops.
    pub fn destroy(self) {}

    pub fn mydest(&self) -> CommandDest {
        match self {
            DestReceiver::DoNothing => CommandDest::None,
            DestReceiver::DebugTup => CommandDest::Debug,
            DestReceiver::PrintTup(dest) => *dest,
            DestReceiver::PrintSimple => CommandDest::RemoteSimple,
            DestReceiver::SpiPrintTup => CommandDest::Spi,
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

// DestReceiver *None_Receiver: C's shared static donothingDR.
pub const NONE_RECEIVER: DestReceiver = DestReceiver::DoNothing;

pub fn BeginCommand(_commandTag: CommandTag, _dest: CommandDest) {
    // Nothing to do at present
}

pub fn CreateDestReceiver(dest: CommandDest) -> DestReceiver {
    match dest {
        CommandDest::Remote | CommandDest::RemoteExecute => DestReceiver::PrintTup(dest),
        CommandDest::RemoteSimple => DestReceiver::PrintSimple,
        CommandDest::None => DestReceiver::DoNothing,
        CommandDest::Debug => DestReceiver::DebugTup,
        CommandDest::Spi => DestReceiver::SpiPrintTup,
        // Constructors owned by unported units.
        CommandDest::Tuplestore
        | CommandDest::IntoRel
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
