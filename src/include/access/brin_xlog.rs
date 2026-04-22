pub const XLOG_BRIN_CREATE_INDEX: u8 = 0x00;
pub const XLOG_BRIN_INSERT: u8 = 0x10;
pub const XLOG_BRIN_UPDATE: u8 = 0x20;
pub const XLOG_BRIN_SAMEPAGE_UPDATE: u8 = 0x30;
pub const XLOG_BRIN_REVMAP_EXTEND: u8 = 0x40;
pub const XLOG_BRIN_DESUMMARIZE: u8 = 0x50;

pub const XLOG_BRIN_OPMASK: u8 = 0x70;
pub const XLOG_BRIN_INIT_PAGE: u8 = 0x80;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct XlBrinCreateIndex {
    pub pages_per_range: u32,
    pub version: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct XlBrinInsert {
    pub heap_blk: u32,
    pub pages_per_range: u32,
    pub offnum: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct XlBrinUpdate {
    pub old_offnum: u16,
    pub insert: XlBrinInsert,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct XlBrinSamepageUpdate {
    pub offnum: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct XlBrinRevmapExtend {
    pub target_blk: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct XlBrinDesummarize {
    pub pages_per_range: u32,
    pub heap_blk: u32,
    pub reg_offset: u16,
}
