// :HACK: root compatibility shim while PL/pgSQL cache metadata lives in `pgrust_plpgsql`.
pub use pgrust_plpgsql::{PlpgsqlFunctionCacheKey, RelationShape, TransitionTableShape};

use pgrust_plpgsql::CompiledFunction;

pub type PlpgsqlFunctionCache = pgrust_plpgsql::PlpgsqlFunctionCache<CompiledFunction>;
