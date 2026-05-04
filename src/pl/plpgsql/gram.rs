// :HACK: root compatibility shim while PL/pgSQL parser lives in `pgrust_plpgsql`.
pub use pgrust_plpgsql::parse_block;
