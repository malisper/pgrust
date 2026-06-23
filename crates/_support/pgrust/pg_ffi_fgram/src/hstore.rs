use core::mem::size_of;

pub const HENTRY_ISFIRST: u32 = 0x8000_0000;
pub const HENTRY_ISNULL: u32 = 0x4000_0000;
pub const HENTRY_POSMASK: u32 = 0x3fff_ffff;
pub const HS_FLAG_NEWVERSION: u32 = 0x8000_0000;
pub const HSTORE_COUNT_MASK: u32 = 0x0fff_ffff;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
pub struct HEntry {
    pub entry: u32,
}

impl HEntry {
    pub const fn new(entry: u32) -> Self {
        Self { entry }
    }

    pub const fn is_first(self) -> bool {
        self.entry & HENTRY_ISFIRST != 0
    }

    pub const fn is_null(self) -> bool {
        self.entry & HENTRY_ISNULL != 0
    }

    pub const fn end_pos(self) -> u32 {
        self.entry & HENTRY_POSMASK
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
pub struct HStore {
    pub vl_len_: i32,
    pub size_: u32,
}

impl HStore {
    pub const fn count(self) -> u32 {
        self.size_ & HSTORE_COUNT_MASK
    }

    pub const fn has_new_version_flag(self) -> bool {
        self.size_ & HS_FLAG_NEWVERSION != 0
    }

    pub fn set_count(&mut self, count: u32) {
        self.size_ = count | HS_FLAG_NEWVERSION;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
pub struct HOldEntry {
    pub keylen: u16,
    pub vallen: u16,
    pub valisnull_pos: u32,
}

impl HOldEntry {
    pub const fn new(keylen: u16, vallen: u16, valisnull: bool, pos: u32) -> Self {
        Self {
            keylen,
            vallen,
            valisnull_pos: (pos << 1) | valisnull as u32,
        }
    }

    pub const fn valisnull(self) -> bool {
        self.valisnull_pos & 1 != 0
    }

    pub const fn pos(self) -> u32 {
        self.valisnull_pos >> 1
    }
}

pub const HSHRDSIZE: usize = size_of::<HStore>();
