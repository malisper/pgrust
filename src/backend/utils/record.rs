// :HACK: Keep the old root utility path while anonymous record descriptor
// metadata lives with portable node types.
pub use pgrust_nodes::record::{
    assign_anonymous_record_descriptor, lookup_anonymous_record_descriptor,
    register_anonymous_record_descriptor,
};
