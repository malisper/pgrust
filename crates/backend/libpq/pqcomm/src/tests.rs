use super::*;
use std::cell::RefCell as StdRefCell;
use std::collections::VecDeque;
use std::sync::Once;

thread_local! {
    static WIRE: StdRefCell<Vec<u8>> = const { StdRefCell::new(Vec::new()) };
    static INPUT: StdRefCell<VecDeque<Vec<u8>>> = const { StdRefCell::new(VecDeque::new()) };
    static WRITE_CHUNK: Cell<usize> = const { Cell::new(usize::MAX) };
    // Sticky errno for every write; one-shot overrides pop first.
    static WRITE_ERRNO: Cell<i32> = const { Cell::new(0) };
    static WRITE_ERRNOS: StdRefCell<Vec<i32>> = const { StdRefCell::new(Vec::new()) };
    static READ_ERRNOS: StdRefCell<Vec<i32>> = const { StdRefCell::new(Vec::new()) };
    static HAVE_PORT: Cell<bool> = const { Cell::new(true) };
    static NOBLOCK: Cell<bool> = const { Cell::new(false) };
}

fn install() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        be_secure_seams::secure_write::set(|buf| {
            if let Some(e) = WRITE_ERRNOS.with(|v| v.borrow_mut().pop()) {
                return Ok(Err(e));
            }
            let e = WRITE_ERRNO.with(Cell::get);
            if e != 0 {
                return Ok(Err(e));
            }
            let n = buf.len().min(WRITE_CHUNK.with(Cell::get));
            WIRE.with(|w| w.borrow_mut().extend_from_slice(&buf[..n]));
            Ok(Ok(n))
        });
        be_secure_seams::secure_read::set(|buf| {
            if let Some(e) = READ_ERRNOS.with(|v| v.borrow_mut().pop()) {
                return Ok(Err(e));
            }
            INPUT.with(|q| {
                let mut q = q.borrow_mut();
                match q.front_mut() {
                    None => Ok(Ok(0)),
                    Some(chunk) => {
                        let n = chunk.len().min(buf.len());
                        buf[..n].copy_from_slice(&chunk[..n]);
                        chunk.drain(..n);
                        if chunk.is_empty() {
                            q.pop_front();
                        }
                        Ok(Ok(n))
                    }
                }
            })
        });
        init_small::init_seams();
        pgstat_seams::pgstat_set_session_end_cause_fatal::set(|| {});
        ipc_seams::proc_exit::set(|code, _pid| panic!("proc_exit({code})"));
        be_secure_seams::set_port_noblock::set(|noblock| {
            if !HAVE_PORT.with(Cell::get) {
                return false;
            }
            NOBLOCK.with(|c| c.set(noblock));
            true
        });
    });
}

fn setup() {
    install();
    pq_init_buffers().unwrap();
    WIRE.with(|w| w.borrow_mut().clear());
    INPUT.with(|q| q.borrow_mut().clear());
    WRITE_CHUNK.with(|c| c.set(usize::MAX));
    WRITE_ERRNO.with(|c| c.set(0));
    WRITE_ERRNOS.with(|v| v.borrow_mut().clear());
    READ_ERRNOS.with(|v| v.borrow_mut().clear());
    HAVE_PORT.with(|c| c.set(true));
    init_small::globals::SetClientConnectionLost(false);
    init_small::globals::SetInterruptPending(false);
}

fn wire() -> Vec<u8> {
    WIRE.with(|w| w.borrow().clone())
}

fn feed(bytes: &[u8]) {
    INPUT.with(|q| q.borrow_mut().push_back(bytes.to_vec()));
}

fn ctx() -> MemoryContext {
    MemoryContext::new("pqcomm-test")
}

#[test]
fn putmessage_buffers_until_flush() {
    setup();
    assert_eq!(pq_putmessage(b'Z', &[b'I']).unwrap(), 0);
    assert!(wire().is_empty());
    assert!(pq_is_send_pending());
    assert_eq!(pq_flush().unwrap(), 0);
    assert!(!pq_is_send_pending());
    assert_eq!(wire(), vec![b'Z', 0, 0, 0, 5, b'I']);
}

#[test]
fn putmessage_v2_has_no_length_word() {
    setup();
    assert_eq!(pq_putmessage_v2(b'E', b"bad\0").unwrap(), 0);
    assert_eq!(pq_flush().unwrap(), 0);
    assert_eq!(wire(), b"Ebad\0");
}

#[test]
fn oversize_message_takes_direct_send_path() {
    setup();
    let body = vec![0xabu8; PQ_SEND_BUFFER_SIZE * 2 + 17];
    assert_eq!(pq_putmessage(b'D', &body).unwrap(), 0);
    // header buffered, body already on the wire before any pq_flush
    let w = wire();
    assert_eq!(w.len(), 5 + body.len());
    assert_eq!(w[0], b'D');
    assert_eq!(&w[1..5], ((body.len() + 4) as u32).to_be_bytes());
    assert_eq!(&w[5..], &body[..]);
    assert!(!pq_is_send_pending());
}

#[test]
fn buffer_full_flushes_mid_message() {
    setup();
    let body = vec![7u8; PQ_SEND_BUFFER_SIZE - 3];
    assert_eq!(pq_putmessage(b'D', &body).unwrap(), 0);
    assert_eq!(pq_putmessage(b'C', b"SELECT 1\0").unwrap(), 0);
    assert_eq!(pq_flush().unwrap(), 0);
    let mut expect = vec![b'D'];
    expect.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
    expect.extend_from_slice(&body);
    expect.push(b'C');
    expect.extend_from_slice(&13u32.to_be_bytes());
    expect.extend_from_slice(b"SELECT 1\0");
    assert_eq!(wire(), expect);
}

#[test]
fn partial_writes_complete_on_flush() {
    setup();
    WRITE_CHUNK.with(|c| c.set(3));
    assert_eq!(pq_putmessage(b'N', b"abcdef").unwrap(), 0);
    assert_eq!(pq_flush().unwrap(), 0);
    assert_eq!(wire(), vec![b'N', 0, 0, 0, 10, b'a', b'b', b'c', b'd', b'e', b'f']);
}

#[test]
fn putmessage_noblock_grows_buffer_without_flush() {
    setup();
    let body = vec![9u8; PQ_SEND_BUFFER_SIZE + 100];
    pq_putmessage_noblock(b'D', &body).unwrap();
    assert!(wire().is_empty());
    assert!(pq_is_send_pending());
    assert_eq!(pq_flush().unwrap(), 0);
    assert_eq!(wire().len(), 5 + body.len());
}

#[test]
fn send_failure_drops_buffer_and_flags_interrupt() {
    setup();
    assert_eq!(pq_putmessage(b'Z', &[b'I']).unwrap(), 0);
    WRITE_ERRNO.with(|c| c.set(libc::EPIPE));
    assert_eq!(pq_flush().unwrap(), EOF);
    assert!(!pq_is_send_pending());
    assert!(init_small::globals::ClientConnectionLost());
    assert!(init_small::globals::InterruptPending());
}

#[test]
fn eintr_on_write_retries() {
    setup();
    WRITE_ERRNOS.with(|v| v.borrow_mut().push(libc::EINTR));
    assert_eq!(pq_putmessage(b'Z', &[b'I']).unwrap(), 0);
    assert_eq!(pq_flush().unwrap(), 0);
    assert_eq!(wire(), vec![b'Z', 0, 0, 0, 5, b'I']);
}

#[test]
fn eintr_on_read_retries() {
    setup();
    READ_ERRNOS.with(|v| v.borrow_mut().push(libc::EINTR));
    feed(b"q");
    pq_startmsgread().unwrap();
    assert_eq!(pq_getbyte().unwrap(), b'q' as i32);
    pq_endmsgread();
}

#[test]
fn flush_if_writable_wouldblock_keeps_data() {
    setup();
    assert_eq!(pq_putmessage(b'Z', &[b'I']).unwrap(), 0);
    WRITE_ERRNO.with(|c| c.set(libc::EAGAIN));
    assert_eq!(pq_flush_if_writable().unwrap(), 0);
    assert!(pq_is_send_pending());
    assert!(NOBLOCK.with(Cell::get));
    WRITE_ERRNO.with(|c| c.set(0));
    assert_eq!(pq_flush().unwrap(), 0);
    assert!(!NOBLOCK.with(Cell::get));
    assert_eq!(wire(), vec![b'Z', 0, 0, 0, 5, b'I']);
}

#[test]
fn no_client_connection_errors() {
    setup();
    assert_eq!(pq_putmessage(b'Z', &[b'I']).unwrap(), 0);
    HAVE_PORT.with(|c| c.set(false));
    assert!(pq_flush().is_err());
    // busy stays set (C longjmp semantics) until pq_comm_reset
    HAVE_PORT.with(|c| c.set(true));
    assert_eq!(pq_flush().unwrap(), 0);
    assert!(wire().is_empty());
    pq_comm_reset();
    assert_eq!(pq_flush().unwrap(), 0);
    assert_eq!(wire(), vec![b'Z', 0, 0, 0, 5, b'I']);
}

#[test]
fn getbyte_and_peekbyte() {
    setup();
    feed(b"ab");
    pq_startmsgread().unwrap();
    assert_eq!(pq_peekbyte().unwrap(), b'a' as i32);
    assert_eq!(pq_getbyte().unwrap(), b'a' as i32);
    assert_eq!(pq_getbyte().unwrap(), b'b' as i32);
    assert_eq!(pq_getbyte().unwrap(), EOF);
    pq_endmsgread();
}

#[test]
fn getbytes_spans_recv_refills() {
    setup();
    feed(b"hel");
    feed(b"lo wo");
    feed(b"rld");
    pq_startmsgread().unwrap();
    let mut out = [0u8; 11];
    assert_eq!(pq_getbytes(&mut out).unwrap(), 0);
    assert_eq!(&out, b"hello world");
    pq_endmsgread();
}

#[test]
fn getbyte_if_available() {
    setup();
    pq_startmsgread().unwrap();
    let mut c = 0u8;
    // empty input: mock returns Ok(0) = EOF
    assert_eq!(pq_getbyte_if_available(&mut c).unwrap(), EOF);
    READ_ERRNOS.with(|v| v.borrow_mut().push(libc::EAGAIN));
    assert_eq!(pq_getbyte_if_available(&mut c).unwrap(), 0);
    assert!(NOBLOCK.with(Cell::get));
    feed(b"x");
    assert_eq!(pq_getbyte_if_available(&mut c).unwrap(), 1);
    assert_eq!(c, b'x');
    pq_endmsgread();
}

#[test]
fn getmessage_roundtrip() {
    setup();
    let mut msg = 9u32.to_be_bytes().to_vec();
    msg.extend_from_slice(b"query");
    feed(&msg);
    let c = ctx();
    let mut s = StringInfo::new_in(c.mcx()).unwrap();
    pq_startmsgread().unwrap();
    assert_eq!(pq_getmessage(&mut s, 30000).unwrap(), 0);
    assert_eq!(s.as_bytes(), b"query");
    assert!(!pq_is_reading_msg());
}

#[test]
fn getmessage_empty_body() {
    setup();
    feed(&4u32.to_be_bytes());
    let c = ctx();
    let mut s = StringInfo::new_in(c.mcx()).unwrap();
    pq_startmsgread().unwrap();
    assert_eq!(pq_getmessage(&mut s, 30000).unwrap(), 0);
    assert!(s.as_bytes().is_empty());
}

#[test]
fn getmessage_invalid_length() {
    setup();
    let c = ctx();
    for bad in [3i32, 30001] {
        feed(&bad.to_be_bytes());
        let mut s = StringInfo::new_in(c.mcx()).unwrap();
        pq_startmsgread().unwrap();
        assert_eq!(pq_getmessage(&mut s, 30000).unwrap(), EOF);
        pq_endmsgread();
        INPUT.with(|q| q.borrow_mut().clear());
    }
}

#[test]
fn getmessage_eof_in_length_word() {
    setup();
    feed(&[0, 0]);
    let c = ctx();
    let mut s = StringInfo::new_in(c.mcx()).unwrap();
    pq_startmsgread().unwrap();
    assert_eq!(pq_getmessage(&mut s, 30000).unwrap(), EOF);
    pq_endmsgread();
}

#[test]
fn getmessage_incomplete_body() {
    setup();
    let mut msg = 10u32.to_be_bytes().to_vec();
    msg.extend_from_slice(b"abc");
    feed(&msg);
    let c = ctx();
    let mut s = StringInfo::new_in(c.mcx()).unwrap();
    pq_startmsgread().unwrap();
    assert_eq!(pq_getmessage(&mut s, 30000).unwrap(), EOF);
    assert!(s.as_bytes().is_empty());
    pq_endmsgread();
}

#[test]
fn getmessage_body_larger_than_recv_buffer() {
    setup();
    let body: Vec<u8> = (0..PQ_RECV_BUFFER_SIZE * 2 + 33).map(|i| (i % 251) as u8).collect();
    let mut msg = ((body.len() + 4) as u32).to_be_bytes().to_vec();
    msg.extend_from_slice(&body);
    feed(&msg);
    let c = ctx();
    let mut s = StringInfo::new_in(c.mcx()).unwrap();
    pq_startmsgread().unwrap();
    assert_eq!(pq_getmessage(&mut s, i32::MAX).unwrap(), 0);
    assert_eq!(s.as_bytes(), &body[..]);
}

#[test]
fn buffer_remaining_data_counts_unread() {
    setup();
    feed(b"abcd");
    pq_startmsgread().unwrap();
    assert_eq!(pq_getbyte().unwrap(), b'a' as i32);
    assert_eq!(pq_buffer_remaining_data(), 3);
    pq_endmsgread();
}

// FATAL is process exit in C; the mock proc_exit panics instead.
#[test]
#[should_panic(expected = "proc_exit(1)")]
fn startmsgread_twice_is_protocol_loss() {
    setup();
    pq_startmsgread().unwrap();
    let _ = pq_startmsgread();
}
