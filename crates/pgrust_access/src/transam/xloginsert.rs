use std::cell::RefCell;
use std::collections::BTreeMap;

use crate::transam::xlog::{Lsn, REGBUF_FORCE_IMAGE};
use pgrust_storage::buffer::{BufferTag, PAGE_SIZE};

#[derive(Debug, Clone)]
pub struct RegisteredBuffer {
    pub block_id: u8,
    pub tag: BufferTag,
    pub flags: u8,
    pub page_image: Option<Box<[u8; PAGE_SIZE]>>,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Default)]
pub struct RegisteredXLogRecord {
    pub blocks: BTreeMap<u8, RegisteredBuffer>,
    pub main_data: Vec<u8>,
    pub origin: Option<u32>,
    pub top_level_xid: Option<u32>,
}

thread_local! {
    static INSERT_STATE: RefCell<RegisteredXLogRecord> = RefCell::new(RegisteredXLogRecord::default());
}

pub fn xlog_begin_insert() {
    INSERT_STATE.with(|state| {
        *state.borrow_mut() = RegisteredXLogRecord::default();
    });
}

pub fn xlog_register_buffer(block_id: u8, tag: BufferTag, flags: u8) {
    INSERT_STATE.with(|state| {
        let mut state = state.borrow_mut();
        state.blocks.insert(
            block_id,
            RegisteredBuffer {
                block_id,
                tag,
                flags,
                page_image: None,
                data: Vec::new(),
            },
        );
    });
}

pub fn xlog_register_buffer_image(block_id: u8, page: &[u8; PAGE_SIZE]) {
    INSERT_STATE.with(|state| {
        let mut state = state.borrow_mut();
        let buffer = state
            .blocks
            .get_mut(&block_id)
            .expect("xlog_register_buffer_image requires prior xlog_register_buffer");
        buffer.page_image = Some(Box::new(*page));
        buffer.flags |= REGBUF_FORCE_IMAGE;
    });
}

pub fn xlog_register_data(data: &[u8]) {
    INSERT_STATE.with(|state| {
        state.borrow_mut().main_data.extend_from_slice(data);
    });
}

pub fn xlog_register_buf_data(block_id: u8, data: &[u8]) {
    INSERT_STATE.with(|state| {
        let mut state = state.borrow_mut();
        let buffer = state
            .blocks
            .get_mut(&block_id)
            .expect("xlog_register_buf_data requires prior xlog_register_buffer");
        buffer.data.extend_from_slice(data);
    });
}

pub fn xlog_register_origin(origin: u32) {
    INSERT_STATE.with(|state| {
        state.borrow_mut().origin = Some(origin);
    });
}

pub fn xlog_register_top_level_xid(xid: u32) {
    INSERT_STATE.with(|state| {
        state.borrow_mut().top_level_xid = Some(xid);
    });
}

pub(crate) fn take_registered_record() -> RegisteredXLogRecord {
    INSERT_STATE.with(|state| {
        let mut state = state.borrow_mut();
        std::mem::take(&mut *state)
    })
}

pub fn xlog_insert(
    wal: &crate::transam::xlog::WalWriter,
    xid: u32,
    rmid: u8,
    info: u8,
) -> Result<Lsn, crate::transam::xlog::WalError> {
    wal.insert_registered_record(xid, rmid, info, take_registered_record())
}
