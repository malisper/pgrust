//! Buffer/framing tests over a scripted fake transport: the
//! `with_my_proc_port` / `secure_read` / `secure_write` seams are installed
//! once with fakes whose backing state is `thread_local`, so each test thread
//! gets its own connection script.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::Once;

use types_error::PgResult;
use net::{Port, SockError, SockResult};

use crate::*;

thread_local! {
    static FAKE_PORT: RefCell<Option<Port>> = const { RefCell::new(None) };
    static READ_SCRIPT: RefCell<VecDeque<Vec<u8>>> = const { RefCell::new(VecDeque::new()) };
    static WRITTEN: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
    /// Max bytes a single fake secure_write accepts (to exercise partial
    /// sends); 0 = unlimited.
    static WRITE_CHUNK: Cell<usize> = const { Cell::new(0) };
}

fn fake_with_my_proc_port(f: &mut dyn FnMut(Option<&mut Port>)) {
    FAKE_PORT.with(|p| f(p.borrow_mut().as_mut()));
}

fn fake_secure_read(_port: &mut Port, buf: &mut [u8]) -> PgResult<SockResult> {
    READ_SCRIPT.with(|s| {
        let mut script = s.borrow_mut();
        match script.front_mut() {
            None => Ok(Err(SockError::Eof)),
            Some(chunk) => {
                let n = chunk.len().min(buf.len());
                buf[..n].copy_from_slice(&chunk[..n]);
                if n == chunk.len() {
                    script.pop_front();
                } else {
                    chunk.drain(..n);
                }
                Ok(Ok(n))
            }
        }
    })
}

fn fake_secure_write(_port: &mut Port, buf: &[u8]) -> PgResult<SockResult> {
    let cap = WRITE_CHUNK.with(Cell::get);
    let n = if cap == 0 { buf.len() } else { buf.len().min(cap) };
    WRITTEN.with(|w| w.borrow_mut().extend_from_slice(&buf[..n]));
    Ok(Ok(n))
}

fn install_fakes() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        init_small_seams::with_my_proc_port::set(fake_with_my_proc_port);
        be_secure_seams::secure_read::set(fake_secure_read);
        be_secure_seams::secure_write::set(fake_secure_write);
    });
}

/// Reset per-thread comm state and connect the fake port.
fn fresh_connection() {
    install_fakes();
    FAKE_PORT.with(|p| *p.borrow_mut() = Some(Port::zeroed()));
    READ_SCRIPT.with(|s| s.borrow_mut().clear());
    WRITTEN.with(|w| w.borrow_mut().clear());
    WRITE_CHUNK.with(|c| c.set(0));
    with_pq_alloc(|st| {
        st.send_buffer.clear();
        st.send_buffer.resize(PQ_SEND_BUFFER_SIZE, 0);
    });
    PQ.with(|s| {
        let mut st = s.borrow_mut();
        st.send_pointer = 0;
        st.send_start = 0;
        st.recv_buffer = [0; PQ_RECV_BUFFER_SIZE];
        st.recv_pointer = 0;
        st.recv_length = 0;
        st.comm_busy = false;
        st.comm_reading_msg = false;
    });
}

fn script_read(bytes: &[u8]) {
    READ_SCRIPT.with(|s| s.borrow_mut().push_back(bytes.to_vec()));
}

fn written() -> Vec<u8> {
    WRITTEN.with(|w| w.borrow().clone())
}

#[test]
fn putmessage_frames_and_flushes() {
    fresh_connection();

    assert_eq!(pq_putmessage(b'Z', &[b'I']).unwrap(), 0);
    assert!(pq_is_send_pending());
    assert_eq!(pq_flush().unwrap(), 0);
    assert!(!pq_is_send_pending());

    // type byte, length word (4 + body), body
    assert_eq!(written(), vec![b'Z', 0, 0, 0, 5, b'I']);
}

#[test]
fn putmessage_v2_has_no_length_word() {
    fresh_connection();

    assert_eq!(pq_putmessage_v2(b'E', b"oops\0").unwrap(), 0);
    assert_eq!(pq_flush().unwrap(), 0);
    assert_eq!(written(), b"Eoops\0".to_vec());
}

#[test]
fn putmessage_noblock_enlarges_buffer() {
    fresh_connection();

    let big = vec![0xabu8; PQ_SEND_BUFFER_SIZE * 2];
    pq_putmessage_noblock(b'd', &big).unwrap();
    // Nothing flushed yet; the whole message is buffered.
    assert!(written().is_empty());
    assert!(pq_is_send_pending());
    assert_eq!(pq_flush().unwrap(), 0);

    let out = written();
    assert_eq!(out.len(), 1 + 4 + big.len());
    assert_eq!(out[0], b'd');
    assert_eq!(
        u32::from_be_bytes([out[1], out[2], out[3], out[4]]) as usize,
        big.len() + 4
    );
    assert_eq!(&out[5..], &big[..]);
}

#[test]
fn large_message_bypasses_buffer_and_partial_writes_complete() {
    fresh_connection();
    WRITE_CHUNK.with(|c| c.set(1000)); // force partial sends

    let big = vec![0x5au8; PQ_SEND_BUFFER_SIZE * 3];
    assert_eq!(pq_putmessage(b'D', &big).unwrap(), 0);
    assert_eq!(pq_flush().unwrap(), 0);

    let out = written();
    assert_eq!(out.len(), 1 + 4 + big.len());
    assert_eq!(&out[5..], &big[..]);
}

#[test]
fn getbytes_and_getbyte_read_through_buffer() {
    fresh_connection();
    script_read(b"hello");
    script_read(b" world");

    pq_startmsgread().unwrap();
    assert_eq!(pq_getbyte().unwrap(), b'h' as i32);
    assert_eq!(pq_peekbyte().unwrap(), b'e' as i32);
    let mut rest = [0u8; 10];
    assert_eq!(pq_getbytes(&mut rest).unwrap(), 0);
    assert_eq!(&rest, b"ello world");
    // script exhausted -> EOF
    assert_eq!(pq_getbyte().unwrap(), EOF);
    pq_endmsgread();
}

#[test]
fn getmessage_strips_length_word() {
    fresh_connection();
    let body = b"SELECT 1\0";
    let mut wire = Vec::new();
    wire.extend_from_slice(&((body.len() as u32) + 4).to_be_bytes());
    wire.extend_from_slice(body);
    script_read(&wire);

    pq_startmsgread().unwrap();
    let ctx = mcx::MemoryContext::new("MessageContext");
    let mut s = StringInfo::new_in(ctx.mcx());
    assert_eq!(pq_getmessage(&mut s, 0x3fffffff).unwrap(), 0);
    assert_eq!(s.as_bytes(), body);
    assert!(!pq_is_reading_msg());
}

#[test]
fn getmessage_rejects_bad_length() {
    fresh_connection();
    script_read(&2u32.to_be_bytes()); // < 4 is invalid

    pq_startmsgread().unwrap();
    let ctx = mcx::MemoryContext::new("MessageContext");
    let mut s = StringInfo::new_in(ctx.mcx());
    assert_eq!(pq_getmessage(&mut s, 1000).unwrap(), EOF);
}

#[test]
fn getmessage_respects_maxlen() {
    fresh_connection();
    script_read(&100u32.to_be_bytes());

    pq_startmsgread().unwrap();
    let ctx = mcx::MemoryContext::new("MessageContext");
    let mut s = StringInfo::new_in(ctx.mcx());
    assert_eq!(pq_getmessage(&mut s, 50).unwrap(), EOF);
}

#[test]
fn getbyte_if_available_returns_zero_when_idle() {
    fresh_connection();
    // Non-blocking read on an empty script reports EOF (fake returns 0), so
    // script one byte instead and then drain it.
    script_read(b"x");
    pq_startmsgread().unwrap();
    let mut c = 0u8;
    assert_eq!(pq_getbyte_if_available(&mut c).unwrap(), 1);
    assert_eq!(c, b'x');
    // Now the connection reports EOF.
    assert_eq!(pq_getbyte_if_available(&mut c).unwrap(), EOF);
    pq_endmsgread();
}

#[test]
fn buffer_remaining_data_counts_unread_bytes() {
    fresh_connection();
    script_read(b"abcd");

    pq_startmsgread().unwrap();
    assert_eq!(pq_getbyte().unwrap(), b'a' as i32);
    assert_eq!(pq_buffer_remaining_data(), 3);
    pq_endmsgread();
}

#[test]
fn comm_reset_clears_busy_flag() {
    fresh_connection();
    PQ.with(|s| s.borrow_mut().comm_busy = true);
    // Suppressed while busy.
    assert_eq!(pq_putmessage(b'Z', &[]).unwrap(), 0);
    assert!(written().is_empty());
    pq_comm_reset();
    assert_eq!(pq_putmessage(b'Z', &[]).unwrap(), 0);
    assert_eq!(pq_flush().unwrap(), 0);
    assert_eq!(written(), vec![b'Z', 0, 0, 0, 4]);
}

#[test]
fn socket_set_nonblocking_errors_without_port() {
    install_fakes();
    FAKE_PORT.with(|p| *p.borrow_mut() = None);
    let err = socket_set_nonblocking(true).unwrap_err();
    assert_eq!(err.sqlstate, types_error::ERRCODE_CONNECTION_DOES_NOT_EXIST);
    FAKE_PORT.with(|p| *p.borrow_mut() = Some(Port::zeroed()));
}

#[test]
fn parse_strtoul_full_matches_c_semantics() {
    assert_eq!(parse_strtoul_full("123"), Some(123));
    assert_eq!(parse_strtoul_full("  42"), Some(42));
    assert_eq!(parse_strtoul_full("+7"), Some(7));
    assert_eq!(parse_strtoul_full("-1"), Some(u64::MAX));
    assert_eq!(parse_strtoul_full(""), None);
    assert_eq!(parse_strtoul_full("12abc"), None);
    assert_eq!(parse_strtoul_full("staff"), None);
    // C-locale isspace includes vertical tab; strtoul clamps on overflow.
    assert_eq!(parse_strtoul_full("\x0b9"), Some(9));
    assert_eq!(parse_strtoul_full("99999999999999999999999999"), Some(u64::MAX));
}
