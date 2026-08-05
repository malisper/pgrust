use queryenvironment::{get_visible_ENR_metadata, EphemeralNamedRelationMetadataData};

use crate::parse_node::ParseState;

pub fn name_matches_visible_ENR(pstate: &ParseState<'_, '_>, refname: &str) -> bool {
    get_visible_ENR(pstate, refname).is_some()
}

pub fn get_visible_ENR<'p, 'mcx>(
    pstate: &ParseState<'p, 'mcx>,
    refname: &str,
) -> Option<&'p EphemeralNamedRelationMetadataData<'mcx>> {
    get_visible_ENR_metadata(pstate.p_queryEnv, refname)
}
