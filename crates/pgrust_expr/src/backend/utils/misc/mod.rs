pub mod guc_datetime;
pub mod guc_xml;
pub mod notices {
    pub fn push_warning(message: String) {
        crate::services::current_services().push_warning(message);
    }
}
pub mod stack_depth {
    pub use pgrust_core::stack_depth::*;
}
