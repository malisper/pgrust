use std::rc::Rc;

use mcx::{Mcx, PgString};
use types_core::Oid;
use types_error::PgResult;

use crate::{cache_mcx, with_state};

// C divergence: qual texts, not trees; the consumer parses (and recomputes
// hassublinks) per use.
pub struct RowSecurityPolicyMeta {
    pub policy_name: PgString<'static>,
    pub polcmd: u8,
    pub permissive: bool,
    pub roles: Vec<Oid>,
    pub qual_src: Option<PgString<'static>>,
    pub with_check_src: Option<PgString<'static>>,
}

pub struct RdRowSecurity {
    // C lcons-builds rd_rsdesc->policies over the name-order scan, so the
    // walked list is reverse name order — preserved by the .rev() below.
    pub policies: Vec<RowSecurityPolicyMeta>,
}

pub fn RelationGetRowSecurityDesc<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<Rc<RdRowSecurity>> {
    if let Some(hit) = with_state(|st| st.policies_cache.get(&relid).cloned()) {
        return Ok(hit);
    }
    let rows = relcache_build_seams::scan_pg_policy::call(mcx, relid)?;
    let cmcx = cache_mcx();
    let mut policies: Vec<RowSecurityPolicyMeta> = Vec::with_capacity(rows.len());
    for row in rows.iter().rev() {
        policies.push(RowSecurityPolicyMeta {
            policy_name: PgString::from_str_in(row.polname, cmcx)?,
            polcmd: row.polcmd,
            permissive: row.polpermissive,
            roles: row.polroles.iter().copied().collect(),
            qual_src: match row.polqual {
                Some(s) => Some(PgString::from_str_in(s, cmcx)?),
                None => None,
            },
            with_check_src: match row.polwithcheck {
                Some(s) => Some(PgString::from_str_in(s, cmcx)?),
                None => None,
            },
        });
    }
    let built = Rc::new(RdRowSecurity { policies });
    with_state(|st| st.policies_cache.insert(relid, Rc::clone(&built)));
    Ok(built)
}

pub(crate) fn forget(relid: Oid) {
    with_state(|st| st.policies_cache.remove(&relid));
}
