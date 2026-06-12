//! Expression-evaluation vocabulary (executor/execExpr.h), trimmed.

/// `ProjectionInfo` (execnodes.h) — node for caching needed info for
/// projection. Trimmed: ports so far only set/test a `ProjectionInfo *` for
/// NULL-ness (`ps_ProjInfo`); the expression machinery stays with its owning
/// unit when it lands.
#[derive(Clone, Debug, Default)]
pub struct ProjectionInfo;
