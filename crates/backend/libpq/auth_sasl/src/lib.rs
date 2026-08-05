//! auth-sasl.c: CheckSASLAuth; the mechanism vtable is a monomorphized trait,
//! sendAuthRequest a parameter (direct dep would cycle auth -> auth_sasl -> auth).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use elog::{elog, ereport};
use mcx::MemoryContext;
use stringinfo::StringInfo;
use types_error::{ErrorLocation, PgResult, DEBUG4, ERRCODE_PROTOCOL_VIOLATION, ERROR};
use types_startup::Port;

pub const PG_SASL_EXCHANGE_CONTINUE: i32 = 0;
pub const PG_SASL_EXCHANGE_SUCCESS: i32 = 1;
pub const PG_SASL_EXCHANGE_FAILURE: i32 = 2;

pub const PG_MAX_SASL_MESSAGE_LENGTH: i32 = 1024;

pub type AuthRequest = u32;
pub const AUTH_REQ_SASL: AuthRequest = 10;
pub const AUTH_REQ_SASL_CONT: AuthRequest = 11;
pub const AUTH_REQ_SASL_FIN: AuthRequest = 12;

pub const PqMsg_SASLResponse: u8 = b'p';

pub const STATUS_OK: i32 = 0;
pub const STATUS_ERROR: i32 = -1;
pub const STATUS_EOF: i32 = -2;

pub trait SaslMech {
    type State;

    fn max_message_length(&self) -> i32;
    fn get_mechanisms(&self, port: &Port, buf: &mut StringInfo<'_>) -> PgResult<()>;
    fn init(
        &self,
        port: &Port,
        selected_mech: &[u8],
        shadow_pass: Option<&str>,
    ) -> PgResult<Self::State>;
    // (PG_SASL_EXCHANGE_* result, output); Some(empty) is a real empty
    // challenge, None sends nothing (SASL distinguishes these).
    fn exchange(
        &self,
        state: &mut Self::State,
        port: &mut Port,
        input: Option<&[u8]>,
        logdetail: &mut Option<String>,
    ) -> PgResult<(i32, Option<Vec<u8>>)>;
}

fn loc(line: i32, func: &'static str) -> ErrorLocation {
    ErrorLocation::new("src/backend/libpq/auth-sasl.c", line, func)
}

pub fn CheckSASLAuth<M: SaslMech>(
    mech: &M,
    port: &mut Port,
    shadow_pass: Option<&str>,
    logdetail: &mut Option<String>,
    send_auth_request: impl Fn(&Port, AuthRequest, &[u8]) -> PgResult<()>,
) -> PgResult<i32> {
    let scratch = MemoryContext::new("CheckSASLAuth");
    let mut sasl_mechs = StringInfo::new_in(scratch.mcx())?;
    mech.get_mechanisms(port, &mut sasl_mechs)?;
    sasl_mechs.append_byte(0)?;
    send_auth_request(port, AUTH_REQ_SASL, sasl_mechs.as_bytes())?;
    drop(sasl_mechs);

    let mut opaq: Option<M::State> = None;
    let mut initial = true;
    let mut result;

    loop {
        pqcomm::pq_startmsgread()?;
        let mtype = pqcomm::pq_getbyte()?;
        if mtype != PqMsg_SASLResponse as i32 {
            if mtype != -1 {
                ereport(ERROR)
                    .errcode(ERRCODE_PROTOCOL_VIOLATION)
                    .errmsg(format!("expected SASL response, got message type {mtype}"))
                    .finish(loc(89, "CheckSASLAuth"))?;
            }
            return Ok(STATUS_EOF);
        }

        let msg_cx = MemoryContext::new("CheckSASLAuth message");
        let mut buf = StringInfo::new_in(msg_cx.mcx())?;
        if pqcomm::pq_getmessage(&mut buf, mech.max_message_length())? != 0 {
            return Ok(STATUS_ERROR); // EOF; pq_getmessage already logged
        }

        elog(
            DEBUG4,
            &format!("processing received SASL response of length {}", buf.len()),
        )?;

        let input: Option<Vec<u8>> = if initial {
            let selected_mech = pqformat::pq_getmsgrawstring(&mut buf)?.to_vec();
            // Missing/invalid secret still inits: doomed mock auth, so failure
            // is indistinguishable from a wrong password.
            opaq = Some(mech.init(port, &selected_mech, shadow_pass)?);

            let inputlen = pqformat::pq_getmsgint(&mut buf, 4)? as i32;
            initial = false;
            if inputlen == -1 {
                None
            } else {
                Some(pqformat::pq_getmsgbytes(&mut buf, inputlen as usize)?.to_vec())
            }
        } else {
            let len = buf.len();
            Some(pqformat::pq_getmsgbytes(&mut buf, len)?.to_vec())
        };
        pqformat::pq_getmsgend(&buf)?;
        drop(buf);

        let state = opaq.as_mut().expect("SASL state initialized on first message");
        let (r, output) = mech.exchange(state, port, input.as_deref(), logdetail)?;
        result = r;

        if let Some(output) = output {
            if result == PG_SASL_EXCHANGE_FAILURE {
                ereport(ERROR)
                    .errmsg_internal("output message found after SASL exchange failure")
                    .finish(loc(171, "CheckSASLAuth"))?;
            }

            elog(
                DEBUG4,
                &format!("sending SASL challenge of length {}", output.len()),
            )?;

            if result == PG_SASL_EXCHANGE_SUCCESS {
                send_auth_request(port, AUTH_REQ_SASL_FIN, &output)?;
            } else {
                send_auth_request(port, AUTH_REQ_SASL_CONT, &output)?;
            }
        }

        if result != PG_SASL_EXCHANGE_CONTINUE {
            break;
        }
    }

    if result != PG_SASL_EXCHANGE_SUCCESS {
        return Ok(STATUS_ERROR);
    }
    Ok(STATUS_OK)
}
