
use core::ffi::c_int;
use core::mem::size_of;

use crate::mem::{palloc0, pfree};
use crate::types::{symbol, SN_env};
use crate::utilities::{create_s, lose_s, replace_s};

#[allow(non_snake_case)]
pub unsafe fn SN_create_env(S_size: c_int, I_size: c_int) -> *mut SN_env {
    let z: *mut SN_env = palloc0(size_of::<SN_env>()) as *mut SN_env;
    if z.is_null() {
        return core::ptr::null_mut();
    }

    let ok = unsafe {
        (*z).p = create_s();
        if (*z).p.is_null() {
            false
        } else if {
            let mut good = true;
            if S_size != 0 {
                (*z).S = palloc0((S_size as usize).wrapping_mul(size_of::<*mut symbol>()))
                    as *mut *mut symbol;
                if (*z).S.is_null() {
                    good = false;
                } else {
                    let mut i: c_int = 0;
                    while i < S_size {
                        *(*z).S.offset(i as isize) = create_s();
                        if (*(*z).S.offset(i as isize)).is_null() {
                            good = false;
                            break;
                        }
                        i += 1;
                    }
                }
            }
            good
        } {
            if I_size != 0 {
                (*z).I = palloc0((I_size as usize).wrapping_mul(size_of::<c_int>())) as *mut c_int;
                !(*z).I.is_null()
            } else {
                true
            }
        } else {
            false
        }
    };

    if ok {
        return z;
    }

    unsafe { SN_close_env(z, S_size) };
    core::ptr::null_mut()
}

#[allow(non_snake_case)]
pub unsafe fn SN_close_env(z: *mut SN_env, S_size: c_int) {
    if z.is_null() {
        return;
    }
    unsafe {
        if !(*z).S.is_null() {
            let mut i: c_int = 0;
            while i < S_size {
                lose_s(*(*z).S.offset(i as isize));
                i += 1;
            }
            pfree((*z).S as *mut core::ffi::c_void);
        }
        pfree((*z).I as *mut core::ffi::c_void);
        if !(*z).p.is_null() {
            lose_s((*z).p);
        }
        pfree(z as *mut core::ffi::c_void);
    }
}

#[allow(non_snake_case)]
pub unsafe fn SN_set_current(z: *mut SN_env, size: c_int, s: *const symbol) -> c_int {
    let err = unsafe { replace_s(z, 0, (*z).l, size, s, core::ptr::null_mut()) };
    unsafe { (*z).c = 0 };
    err
}
