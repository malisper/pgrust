#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

pub mod dedup;
pub mod genam;
pub mod page;
pub mod state;
pub mod vacuum;
pub mod xlog;

pub use genam::*;
pub use page::*;
pub use state::*;
pub use vacuum::*;
pub use xlog::*;

#[cfg(test)]
mod tests;
