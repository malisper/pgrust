// :HACK: root compatibility shim while storage lives in `pgrust_storage`.
pub mod bufpage {
    pub use pgrust_storage::page::bufpage::*;
}

pub use bufpage::*;
