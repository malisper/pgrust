use core::ffi::{c_char, c_int, c_void};

use crate::types::Size;

pub type ssize_t = isize;
pub type locale_t = *mut c_void;

pub const COLLPROVIDER_DEFAULT: c_char = b'd' as c_char;
pub const COLLPROVIDER_BUILTIN: c_char = b'b' as c_char;
pub const COLLPROVIDER_ICU: c_char = b'i' as c_char;
pub const COLLPROVIDER_LIBC: c_char = b'c' as c_char;

pub type pg_locale_t = *mut pg_locale_struct;

pub type StrncollFn = Option<
    unsafe extern "C" fn(*const c_char, ssize_t, *const c_char, ssize_t, pg_locale_t) -> c_int,
>;
pub type StrnxfrmFn =
    Option<unsafe extern "C" fn(*mut c_char, Size, *const c_char, ssize_t, pg_locale_t) -> Size>;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct collate_methods {
    pub strncoll: StrncollFn,
    pub strnxfrm: StrnxfrmFn,
    pub strnxfrm_prefix: StrnxfrmFn,
    pub strxfrm_is_safe: bool,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct pg_locale_struct {
    pub provider: c_char,
    pub deterministic: bool,
    pub collate_is_c: bool,
    pub ctype_is_c: bool,
    pub is_default: bool,
    pub collate: *const collate_methods,
    pub info: PgLocaleInfo,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub union PgLocaleInfo {
    pub builtin: PgLocaleBuiltinInfo,
    pub lt: locale_t,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct PgLocaleBuiltinInfo {
    pub locale: *const c_char,
    pub casemap_full: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_locale_struct_matches_non_icu_generated_layout() {
        assert_eq!(core::mem::size_of::<collate_methods>(), 32);
        assert_eq!(core::mem::align_of::<collate_methods>(), 8);
        assert_eq!(core::mem::size_of::<pg_locale_struct>(), 32);
        assert_eq!(core::mem::align_of::<pg_locale_struct>(), 8);
        assert_eq!(core::mem::offset_of!(pg_locale_struct, provider), 0);
        assert_eq!(core::mem::offset_of!(pg_locale_struct, collate), 8);
        assert_eq!(core::mem::offset_of!(pg_locale_struct, info), 16);
    }
}
