use mcx::Mcx;
use pg_depend::ObjectAddress;
use types_error::PgResult;

pub fn getObjectDescription<'mcx>(
    mcx: Mcx<'mcx>,
    object: &ObjectAddress,
) -> PgResult<Option<String>> {
    catalog_objectaddress::getObjectDescription(mcx, object, false)
}
