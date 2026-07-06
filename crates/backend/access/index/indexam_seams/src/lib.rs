use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use types_rel::{Relation, LOCKMODE};

seam_core::seam!(
    pub fn index_open<'mcx>(
        mcx: Mcx<'mcx>,
        relation_id: Oid,
        lockmode: LOCKMODE,
    ) -> PgResult<Relation<'mcx>>
);

seam_core::seam!(
    // index_can_return (indexam.c): amcanreturn dispatch; opens the index
    // with AccessShareLock as C's amutils fallback does.
    pub fn index_can_return(mcx: Mcx<'_>, index_oid: Oid, attno: i32) -> PgResult<bool>
);

seam_core::seam!(
    pub fn try_index_open<'mcx>(
        mcx: Mcx<'mcx>,
        relation_id: Oid,
        lockmode: LOCKMODE,
    ) -> PgResult<Option<Relation<'mcx>>>
);

seam_core::seam!(
    // RelationGetIndexAttOptions (relcache.c): parsed per-key-column opclass
    // options struct images (C rd_opcoptions), Rc'd so AM states outlive
    // relcache invalidation. Implemented by catalog_index (needs syscache +
    // fmgr, both above the AM layer).
    pub fn relation_get_index_att_options(
        rel: &Relation<'_>,
    ) -> PgResult<std::rc::Rc<[Option<std::boxed::Box<[u8]>>]>>
);

seam_core::seam!(
    // GetIndexInputType's expression-column arm (spgutils.c):
    // getBaseType(exprType(<indexcol's entry in rd_indexprs>)). Implemented
    // by catalog_index (RelationGetIndexExpressions + exprType live above
    // the AM layer); the expression tree is deserialized into a scratch
    // context there — only the type Oid crosses back.
    pub fn index_expression_input_type<'a, 'mcx>(
        rel: &'a Relation<'mcx>,
        indexcol_0based: usize,
    ) -> PgResult<Oid>
);
