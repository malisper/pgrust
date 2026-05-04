// :HACK: Preserve the historical root executor path while compiled tuple
// decoding lives in `pgrust_executor`.
pub(crate) use pgrust_executor::CompiledTupleDecoder;
