// RelationBuildDesc's catalog-scan half; installed by the future build unit.
use std::rc::Rc;

use mcx::{Mcx, PgVec};
use types_core::Oid;
use types_error::PgResult;
use types_rel::{FormData_pg_class, FormData_pg_index, RdOptions};
use types_tuple::TupleDescData;

// options: RelationParseRelOptions folds into the installer (parsed form only).
pub struct ScannedPgClass {
    pub form: FormData_pg_class,
    // Threaded beside the trimmed form (relchecks/relhastriggers were
    // dropped from it).
    pub relchecks: i16,
    pub relhastriggers: bool,
    pub relhasrules: bool,
    pub options: Option<RdOptions>,
}

pub struct IndexAccessInfo {
    pub index: FormData_pg_index<'static>,
    pub opcintype: PgVec<'static, Oid>,
    pub opfamily: PgVec<'static, Oid>,
    pub indoption: PgVec<'static, i16>,
    pub indcollation: PgVec<'static, Oid>,
    // C's IndexSupportInitialize preload of rd_support/rd_supportinfo, one
    // BTORDER_PROC slot per key column (std Vec: rd_supportinfo's shape);
    // without it the first scan of pg_amproc's own index recurses.
    pub supportinfo: Vec<Option<types_fmgr::FmgrInfo>>,
    // C rd_support: nkey x amsupport proc OIDs, row-major.
    pub support: PgVec<'static, Oid>,
}

seam_core::seam!(
    pub fn scan_pg_relation(
        target_rel_id: Oid,
        index_ok: bool,
        force_non_historic: bool,
    ) -> PgResult<Option<ScannedPgClass>>
);

seam_core::seam!(
    pub fn relation_build_tuple_desc(
        mcx: Mcx<'static>,
        relid: Oid,
        form: &FormData_pg_class,
        relchecks: i16,
    ) -> PgResult<Rc<TupleDescData<'static>>>
);

seam_core::seam!(
    pub fn relation_init_index_access_info(
        mcx: Mcx<'static>,
        relid: Oid,
        form: &FormData_pg_class,
    ) -> PgResult<IndexAccessInfo>
);

// One pg_rewrite row (RelationBuildRuleLock's scan); ev_qual/ev_action are
// the detoasted nodeToString texts ("<>" = NULL tree).
pub struct PgRewriteRuleShape<'mcx> {
    pub rule_id: Oid,
    pub ev_type: u8,
    pub ev_enabled: u8,
    pub is_instead: bool,
    pub ev_qual: &'mcx str,
    pub ev_action: &'mcx str,
}

seam_core::seam!(
    // pg_rewrite scan over RewriteRelRulenameIndexId (name order, as C).
    pub fn scan_pg_rewrite<'mcx>(
        mcx: Mcx<'mcx>,
        ev_class: Oid,
    ) -> PgResult<PgVec<'mcx, PgRewriteRuleShape<'mcx>>>
);

// One pg_policy row (RelationBuildRowSecurity's scan, policy.c); the quals are
// detoasted nodeToString texts, roles the decoded polroles oid[] elements.
pub struct PgPolicyShape<'mcx> {
    pub polname: &'mcx str,
    pub polcmd: u8,
    pub polpermissive: bool,
    pub polroles: &'mcx [Oid],
    pub polqual: Option<&'mcx str>,
    pub polwithcheck: Option<&'mcx str>,
}

seam_core::seam!(
    // pg_policy scan over PolicyPolrelidPolnameIndexId (polname order, as C).
    pub fn scan_pg_policy<'mcx>(
        mcx: Mcx<'mcx>,
        polrelid: Oid,
    ) -> PgResult<PgVec<'mcx, PgPolicyShape<'mcx>>>
);

pub struct PgIndexListShape {
    pub indexrelid: Oid,
    pub indislive: bool,
    pub indisunique: bool,
    pub indisprimary: bool,
    pub indimmediate: bool,
    pub indisvalid: bool,
    pub indisreplident: bool,
    pub has_indpred: bool,
}

seam_core::seam!(
    pub fn scan_pg_index_shapes<'mcx>(
        mcx: Mcx<'mcx>,
        indrelid: Oid,
    ) -> PgResult<PgVec<'mcx, PgIndexListShape>>
);

seam_core::seam!(
    // RelationGetExclusionInfo's pg_constraint half (relcache.c): conexclop
    // of the exclusion (or conperiod pk/unique) constraint owning index_relid.
    pub fn scan_exclusion_ops<'mcx>(
        mcx: Mcx<'mcx>,
        conrelid: Oid,
        index_relid: Oid,
    ) -> PgResult<PgVec<'mcx, Oid>>
);

seam_core::seam!(
    // RelationBuildTriggers (trigger.c); None when the rel has no pg_trigger
    // rows (relhastriggers can lag drops).
    pub fn build_trigger_desc(
        mcx: Mcx<'static>,
        relid: Oid,
    ) -> PgResult<Option<types_trigger::TriggerDesc<'static>>>
);

seam_core::seam!(
    // RelationGetFKeyList's pg_constraint scan (relcache.c): contype='f' rows
    // on conrelid, DeconstructFkConstraintRow-decoded, scan (index) order.
    pub fn scan_pg_constraint_fkeys<'mcx>(
        mcx: Mcx<'mcx>,
        conrelid: Oid,
    ) -> PgResult<PgVec<'mcx, types_rel::ForeignKeyCacheInfo>>
);

seam_core::seam!(
    pub fn scan_pg_statistic_ext_oids<'mcx>(
        mcx: Mcx<'mcx>,
        stxrelid: Oid,
    ) -> PgResult<PgVec<'mcx, Oid>>
);
