#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

// `alloc` exists only for `FmgrInfo.fn_expr`'s `Rc<dyn Any>` carrier; the
// crate is otherwise core-only.
extern crate alloc;

pub mod catalog;
pub mod cmdtag;
pub mod fmgr;
pub mod geo;
pub mod init;
pub mod instrument;
pub mod keywords;
pub mod primitive;
pub mod xact;

pub use catalog::*;
pub use cmdtag::*;
pub use fmgr::*;
pub use geo::*;
pub use init::*;
pub use instrument::*;
pub use keywords::*;
pub use primitive::*;
pub use xact::*;
