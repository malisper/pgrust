use crate::deflist::{deserialize_deflist, serialize_deflist, DefItem, DefValue};

fn ser(items: &[DefItem<'_>]) -> String {
    let ctx = mcx::MemoryContext::new("tsearchcmds-test");
    let out = serialize_deflist(ctx.mcx(), items).unwrap();
    String::from_utf8(out.to_vec()).unwrap()
}

#[test]
fn serialize_matches_c_tsdicts_outputs() {
    let ctx = mcx::MemoryContext::new("tsearchcmds-test");
    let mcx = ctx.mcx();
    let one = |name, value| DefItem { name, value: Some(value) };
    assert_eq!(
        ser(&[one("synonyms", DefValue::Str("synonym_sample"))]),
        "synonyms = 'synonym_sample'"
    );
    assert_eq!(
        ser(&[one("synonyms", DefValue::Str("synonym_sample")), one("casesensitive", DefValue::Int(1))]),
        "synonyms = 'synonym_sample', casesensitive = 1"
    );
    assert_eq!(
        ser(&[one("synonyms", DefValue::Str("synonym_sample")), one("casesensitive", DefValue::Str("off"))]),
        "synonyms = 'synonym_sample', casesensitive = 'off'"
    );
    assert_eq!(
        ser(&[one("dictfile", DefValue::Str("ispell_sample")), one("afffile", DefValue::Str("ispell_sample"))]),
        "dictfile = 'ispell_sample', afffile = 'ispell_sample'"
    );
    // quote_identifier: mixed case forces double quotes.
    assert_eq!(ser(&[one("DictFile", DefValue::Str("x"))]), "\"DictFile\" = 'x'");
    // quote doubling and E'' for backslashes.
    assert_eq!(ser(&[one("a", DefValue::Str("it's"))]), "a = 'it''s'");
    assert_eq!(ser(&[one("a", DefValue::Str("a\\b"))]), "a = E'a\\\\b'");
    assert_eq!(ser(&[one("a", DefValue::Float("1.5"))]), "a = 1.5");
    assert_eq!(ser(&[one("a", DefValue::Bool(true))]), "a = 'true'");
    let _ = mcx;
}

#[test]
fn deserialize_round_trips() {
    let ctx = mcx::MemoryContext::new("tsearchcmds-test");
    let mcx = ctx.mcx();
    let items = deserialize_deflist(mcx, b"synonyms = 'synonym_sample', casesensitive = 1").unwrap();
    assert_eq!(items.len(), 2);
    assert_eq!(items[0].name, "synonyms");
    assert_eq!(items[0].value, Some(DefValue::Str("synonym_sample")));
    assert_eq!(items[1].name, "casesensitive");
    assert_eq!(items[1].value, Some(DefValue::Int(1)));
    assert_eq!(
        String::from_utf8(serialize_deflist(mcx, &items).unwrap().to_vec()).unwrap(),
        "synonyms = 'synonym_sample', casesensitive = 1"
    );

    let items = deserialize_deflist(mcx, b"casesensitive = 'off'").unwrap();
    assert_eq!(items[0].value, Some(DefValue::Str("off")));

    // Escaped forms serialize_deflist emits.
    let items = deserialize_deflist(mcx, b"a = 'it''s', b = E'a\\\\b'").unwrap();
    assert_eq!(items[0].value, Some(DefValue::Str("it's")));
    assert_eq!(items[1].value, Some(DefValue::Str("a\\b")));

    // Backward-compat forms C accepts but never emits.
    let items = deserialize_deflist(mcx, b"  k1 = v1 , \"K 2\" = \"v\"\"2\"  k3=true k4=1.25").unwrap();
    assert_eq!(items[0].name, "k1");
    assert_eq!(items[0].value, Some(DefValue::Str("v1")));
    assert_eq!(items[1].name, "K 2");
    assert_eq!(items[1].value, Some(DefValue::Str("v\"2")));
    assert_eq!(items[2].value, Some(DefValue::Bool(true)));
    assert_eq!(items[3].value, Some(DefValue::Float("1.25")));

    assert!(deserialize_deflist(mcx, b"k =").is_err());
    assert!(deserialize_deflist(mcx, b"k ! v").is_err());
    assert!(deserialize_deflist(mcx, b"k = 'unterminated").is_err());
}

// ALTER TSDICTIONARY/TSCONFIGURATION "must be owner" gate: superuser bypass
// (object_ownercheck's superuser_arg fast path) and the C error shape for
// the non-owner arm (aclcheck_error ACLCHECK_NOT_OWNER, aclchk.c).
#[test]
fn ts_ownercheck_superuser_bypass_and_owner_error_shape() {
    use types_nodes::parsenodes::ObjectType;
    // Bootstrap superuser + !IsUnderPostmaster: superuser.c's escape hatch
    // answers without catalog access.
    miscinit::SetUserIdAndSecContext(types_core::BOOTSTRAP_SUPERUSERID, 0);
    crate::ownercheck(
        crate::TSDictionaryRelationId,
        3765,
        ObjectType::OBJECT_TSDICTIONARY,
        "english_stem",
    )
    .unwrap();
    crate::ownercheck(
        crate::TSConfigRelationId,
        3748,
        ObjectType::OBJECT_TSCONFIGURATION,
        "english",
    )
    .unwrap();

    // Non-owner arm raises C's message/sqlstate.
    let e = aclchk::aclcheck_error(
        aclchk::ACLCHECK_NOT_OWNER,
        ObjectType::OBJECT_TSDICTIONARY,
        "english_stem",
    )
    .unwrap_err();
    assert_eq!(e.message(), "must be owner of text search dictionary english_stem");
    assert_eq!(e.sqlstate(), types_error::ERRCODE_INSUFFICIENT_PRIVILEGE);
    let e = aclchk::aclcheck_error(
        aclchk::ACLCHECK_NOT_OWNER,
        ObjectType::OBJECT_TSCONFIGURATION,
        "english",
    )
    .unwrap_err();
    assert_eq!(e.message(), "must be owner of text search configuration english");
}
