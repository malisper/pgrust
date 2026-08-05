
use crate::regex_consts;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct RegError(pub i32);

pub type RegResult<T> = Result<T, RegError>;

impl RegError {
    #[inline]
    pub const fn new(code: i32) -> Self {
        RegError(code)
    }

    #[inline]
    pub const fn code(self) -> i32 {
        self.0
    }
}

impl From<i32> for RegError {
    #[inline]
    fn from(code: i32) -> Self {
        RegError(code)
    }
}


#[inline]
pub const fn err_espace() -> RegError {
    RegError(regex_consts::REG_ESPACE)
}

#[inline]
pub const fn err_assert() -> RegError {
    RegError(regex_consts::REG_ASSERT)
}

#[inline]
pub const fn err_invarg() -> RegError {
    RegError(regex_consts::REG_INVARG)
}

#[inline]
pub const fn err_etoobig() -> RegError {
    RegError(regex_consts::REG_ETOOBIG)
}

#[inline]
pub const fn err_ecolors() -> RegError {
    RegError(regex_consts::REG_ECOLORS)
}

impl From<alloc::collections::TryReserveError> for RegError {
    #[inline]
    fn from(_: alloc::collections::TryReserveError) -> Self {
        err_espace()
    }
}
