//! oid.c comparison slice; the rest of the adt-scalar batch stays todo.

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
