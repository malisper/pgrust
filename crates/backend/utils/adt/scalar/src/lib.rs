//! oid.c comparison slice + xid.c out/eq slice; the rest of the adt-scalar
//! batch stays todo.

pub mod builtins;
#[cfg(test)]
mod tests;

use ::types_core::Oid;

macro_rules! oid_cmp_ops {
    ($($name:ident: $op:tt;)*) => {$(
        #[inline]
        pub fn $name(arg1: Oid, arg2: Oid) -> bool {
            arg1 $op arg2
        }
    )*};
}

oid_cmp_ops! {
    oideq: ==; oidne: !=;
    oidlt: <;  oidle: <=;
    oidgt: >;  oidge: >=;
}

/// `xidout` (xid.c) into a caller buffer; returns the byte length.
#[inline]
pub fn xidout(xid: u32, buf: &mut [u8]) -> usize {
    let mut tmp = [0u8; 10];
    let mut n = 0;
    let mut v = xid;
    loop {
        tmp[n] = b'0' + (v % 10) as u8;
        v /= 10;
        n += 1;
        if v == 0 {
            break;
        }
    }
    for i in 0..n {
        buf[i] = tmp[n - 1 - i];
    }
    n
}

#[inline]
pub fn xideq(x1: u32, x2: u32) -> bool {
    x1 == x2
}

#[inline]
pub fn xidneq(x1: u32, x2: u32) -> bool {
    x1 != x2
}
