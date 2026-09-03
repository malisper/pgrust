// InvalidateConstraintCacheCallBack (ri_triggers.c) selection rules. The
// regression these pin: without the callback, ALTER TABLE ... RENAME CONSTRAINT
// left the pre-rename conname in the constraint cache and every later violation
// in the session named a constraint that no longer existed.
use super::*;

fn stub_info(constraint_oid: Oid, root: Oid, oid_hv: u32, root_hv: u32) -> RiConstraintInfo {
    RiConstraintInfo {
        constraint_id: constraint_oid,
        constraint_root_id: root,
        oidHashValue: oid_hv,
        rootHashValue: root_hv,
        conname: NameData::default(),
        pk_relid: InvalidOid,
        fk_relid: InvalidOid,
        confmatchtype: FKCONSTR_MATCH_SIMPLE,
        nkeys: 0,
        ndelsetcols: 0,
        confdelsetcols: [0; RI_MAX_NUMKEYS],
        fk_attnums: [0; RI_MAX_NUMKEYS],
        pk_attnums: [0; RI_MAX_NUMKEYS],
        pf_eq_oprs: [InvalidOid; RI_MAX_NUMKEYS],
        pp_eq_oprs: [InvalidOid; RI_MAX_NUMKEYS],
        ff_eq_oprs: [InvalidOid; RI_MAX_NUMKEYS],
        hasperiod: false,
        period_contained_by_oper: InvalidOid,
        agged_period_contained_by_oper: InvalidOid,
        period_intersect_oper: InvalidOid,
    }
}

fn test_mcx() -> Mcx<'static> {
    thread_local! {
        static CTX: &'static MemoryContext =
            Box::leak(Box::new(MemoryContext::new("ri-inval-test")));
    }
    CTX.with(|c| c.mcx())
}

fn seed(entries: &[RiConstraintInfo]) {
    let mcx = test_mcx();
    RI_CONSTRAINT_CACHE.with(|c| {
        let mut b = c.borrow_mut();
        let m = b.get_or_insert_with(|| PgHashMap::new_in(mcx));
        m.clear();
        for e in entries {
            m.insert(e.constraint_id, e.clone());
        }
    });
}

fn cached() -> Vec<Oid> {
    let mut v = RI_CONSTRAINT_CACHE
        .with(|c| c.borrow().as_ref().map(|m| m.keys().copied().collect::<Vec<_>>()))
        .unwrap_or_default();
    v.sort_unstable();
    v
}

// A pg_constraint inval for one constraint drops exactly that entry.
#[test]
fn inval_drops_only_the_matching_entry() {
    seed(&[
        stub_info(100, 100, 0xAAAA, 0xAAAA),
        stub_info(200, 200, 0xBBBB, 0xBBBB),
        stub_info(300, 300, 0xCCCC, 0xCCCC),
    ]);
    InvalidateConstraintCacheCallBack(Datum::null(), cache_syscache::CONSTROID, 0xBBBB);
    assert_eq!(cached(), vec![100, 300]);
}

// An inval that matches nothing leaves the cache alone: pg_constraint update
// traffic must not behave like a blanket flush.
#[test]
fn inval_of_unrelated_constraint_keeps_entries() {
    seed(&[stub_info(100, 100, 0xAAAA, 0xAAAA), stub_info(200, 200, 0xBBBB, 0xBBBB)]);
    InvalidateConstraintCacheCallBack(Datum::null(), cache_syscache::CONSTROID, 0xDEAD);
    assert_eq!(cached(), vec![100, 200]);
}

// Inherited (partition) children carry the root's hash value; invalidating the
// root constraint must take them down too, even though their own hash differs.
#[test]
fn inval_of_root_drops_inherited_children() {
    seed(&[
        stub_info(10, 10, 0x1111, 0x1111),      // the root itself
        stub_info(11, 10, 0x2222, 0x1111),      // child of the root
        stub_info(12, 10, 0x3333, 0x1111),      // child of the root
        stub_info(20, 20, 0x4444, 0x4444),      // unrelated
    ]);
    InvalidateConstraintCacheCallBack(Datum::null(), cache_syscache::CONSTROID, 0x1111);
    assert_eq!(cached(), vec![20]);
}

// A child's own inval does not touch the root or its siblings.
#[test]
fn inval_of_child_leaves_root_and_siblings() {
    seed(&[
        stub_info(10, 10, 0x1111, 0x1111),
        stub_info(11, 10, 0x2222, 0x1111),
        stub_info(12, 10, 0x3333, 0x1111),
    ]);
    InvalidateConstraintCacheCallBack(Datum::null(), cache_syscache::CONSTROID, 0x2222);
    assert_eq!(cached(), vec![10, 12]);
}

// hashvalue == 0 is a reset message: everything goes.
#[test]
fn reset_message_flushes_everything() {
    seed(&[stub_info(100, 100, 0xAAAA, 0xAAAA), stub_info(200, 200, 0xBBBB, 0xBBBB)]);
    InvalidateConstraintCacheCallBack(Datum::null(), cache_syscache::CONSTROID, 0);
    assert!(cached().is_empty());
}

// Past 1000 live entries C stops matching and pretends it got a reset (the
// pg_dump-restore O(N^2) escape hatch), so even a non-matching hash value
// empties the cache. At exactly 1000 the selective path still applies.
#[test]
fn over_a_thousand_entries_degrades_to_a_reset() {
    let many: Vec<_> =
        (1..=1000u32).map(|i| stub_info(i, i, 0x10000 + i, 0x10000 + i)).collect();
    seed(&many);
    InvalidateConstraintCacheCallBack(Datum::null(), cache_syscache::CONSTROID, 0xDEAD);
    assert_eq!(cached().len(), 1000, "1000 entries: still selective");

    let mut many = many;
    many.push(stub_info(1001, 1001, 0x20001, 0x20001));
    seed(&many);
    InvalidateConstraintCacheCallBack(Datum::null(), cache_syscache::CONSTROID, 0xDEAD);
    assert!(cached().is_empty(), "1001 entries: reset");
}

// An inval arriving before the cache has ever been built is a no-op, not a panic.
#[test]
fn inval_on_unbuilt_cache_is_a_noop() {
    RI_CONSTRAINT_CACHE.with(|c| drop(c.borrow_mut().take()));
    InvalidateConstraintCacheCallBack(Datum::null(), cache_syscache::CONSTROID, 0xAAAA);
    assert!(cached().is_empty());
}

#[test]
fn no_pg_constraint_entry_is_ereport_42p17() {
    let e = no_pg_constraint_entry("ri_trig", "child");
    assert_eq!(e.sqlstate(), types_error::ERRCODE_INVALID_OBJECT_DEFINITION);
    assert_eq!(
        e.message(),
        "no pg_constraint entry for trigger \"ri_trig\" on table \"child\""
    );
    assert_eq!(
        e.hint(),
        Some(
            "Remove this referential integrity trigger and its mates, then do ALTER TABLE ADD CONSTRAINT."
        )
    );
}

#[test]
fn ri_check_trigger_wrong_timing_is_ereport_39p01() {
    let e = ri_CheckTrigger("RI_FKey_check_ins", RI_TRIGTYPE_INSERT, 0).err().unwrap();
    assert_eq!(e.sqlstate(), types_error::ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED);
    assert_eq!(
        e.message(),
        "function \"RI_FKey_check_ins\" must be fired AFTER ROW"
    );
}

#[test]
fn ri_check_trigger_wrong_event_is_ereport_39p01() {
    let e = ri_CheckTrigger("RI_FKey_check_ins", RI_TRIGTYPE_INSERT, 0x5).err().unwrap();
    assert_eq!(e.sqlstate(), types_error::ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED);
    assert_eq!(
        e.message(),
        "function \"RI_FKey_check_ins\" must be fired for INSERT"
    );
}

#[test]
fn unexpected_ri_query_result_is_ereport_xx000() {
    let e = unexpected_ri_query_result("pk", "fk_con", "fk");
    assert_eq!(e.sqlstate(), types_error::ERRCODE_INTERNAL_ERROR);
    assert_eq!(
        e.message(),
        "referential integrity query on \"pk\" from constraint \"fk_con\" on \"fk\" gave unexpected result"
    );
    assert_eq!(
        e.hint(),
        Some("This is most likely due to a rule having rewritten the query.")
    );
}

#[test]
fn rls_owner_bypass_matches_c() {
    assert!(!ri_rls_owner_blocks(true, true, false, true, false));
    assert!(!ri_rls_owner_blocks(false, true, true, true, true));
    assert!(ri_rls_owner_blocks(false, true, false, false, true));
    assert!(ri_rls_owner_blocks(false, false, true, true, false));
    assert!(!ri_rls_owner_blocks(false, false, false, false, false));
}

#[test]
fn ri_builtin_rows_match_canonical() {
    fmgr_core::assert_rows_match_canonical(crate::RI_TRIGGERS_BUILTINS);
}

// datum_image_eq bounds every by-ref key comparison by the extent of the tuple
// image the datum was fetched from, so a forged varlena header on a crafted PK
// heap page can no longer drive an out-of-bounds read (idx 8 / CWE-125).

// Build a 4-byte-header varlena whose declared size matches its buffer.
fn mk_varlena(content: &[u8]) -> Vec<u8> {
    let total = content.len() + 4;
    let mut buf = vec![0u8; total];
    let word = types_tuple::varatt::set_varsize_4b_word(total as u32);
    buf[..4].copy_from_slice(&word.to_ne_bytes());
    buf[4..].copy_from_slice(content);
    buf
}

#[test]
fn datum_image_eq_varlena_equal_and_unequal() {
    let a = mk_varlena(b"hello");
    let b = mk_varlena(b"hello");
    let c = mk_varlena(b"world");
    let d = mk_varlena(b"hell"); // different length

    let da = Datum::from_usize(a.as_ptr() as usize);
    let db = Datum::from_usize(b.as_ptr() as usize);
    let dc = Datum::from_usize(c.as_ptr() as usize);
    let dd = Datum::from_usize(d.as_ptr() as usize);

    assert!(datum_image_eq(da, db, false, -1, a.len(), b.len()));
    assert!(!datum_image_eq(da, dc, false, -1, a.len(), c.len()));
    // Different lengths short-circuit before any slice is formed.
    assert!(!datum_image_eq(da, dd, false, -1, a.len(), d.len()));
}

#[test]
fn datum_image_eq_byval_widths() {
    let a = Datum::from_i32(0x0102_0304);
    let b = Datum::from_i32(0x0102_0304);
    let c = Datum::from_i32(0x0506_0708);
    assert!(datum_image_eq(a, b, true, 4, 0, 0));
    assert!(!datum_image_eq(a, c, true, 4, 0, 0));
}

#[test]
#[should_panic(expected = "corrupt")]
fn datum_image_eq_forged_varlena_length_is_rejected() {
    // A crafted PK datum: the 4-byte header declares a huge length while the
    // real image (and the `avail` bound) is tiny. Without the bound this drove
    // slice::from_raw_parts past the tuple/page; now it is a clean corruption
    // error before any byte is compared.
    let mut buf = vec![0u8; 8];
    let forged = types_tuple::varatt::set_varsize_4b_word(4096);
    buf[..4].copy_from_slice(&forged.to_ne_bytes());
    let good = mk_varlena(&[0u8; 4]); // total 8, honest header
    let da = Datum::from_usize(buf.as_ptr() as usize);
    let db = Datum::from_usize(good.as_ptr() as usize);
    // avail bounds both datums to their real 8-byte buffers.
    let _ = datum_image_eq(da, db, false, -1, buf.len(), good.len());
}

// Every syscache miss in C's ri_triggers.c is reported with
// elog(ERROR, "cache lookup failed for <what> %u") -- catchable, SQLSTATE
// XX000 (elog's default at ERROR), transaction-scoped.  pgrust used to
// panic! at all seven of these probes, which aborts the whole backend.
#[test]
fn ri_cache_lookup_failures_are_catchable_xx000() {
    for (what, oid, expected) in [
        ("constraint", 16384u32, "cache lookup failed for constraint 16384"),
        ("type", 23, "cache lookup failed for type 23"),
        ("namespace", 2200, "cache lookup failed for namespace 2200"),
        ("operator", 96, "cache lookup failed for operator 96"),
        ("collation", 100, "cache lookup failed for collation 100"),
    ] {
        let e = cache_lookup_failed(what, oid);
        assert_eq!(e.message(), expected);
        assert_eq!(e.sqlstate(), ERRCODE_INTERNAL_ERROR);
        assert_eq!(e.level(), ERROR);
    }
}

// audit-18.6 ri_triggers-1: every RI_FKey_* fmgr row used to be
// fc_internal_dispatch_only, which panics; C's entry points start with
// ri_CheckTrigger, so a call without a TriggerData context is 39P01
// "was not called by trigger manager" (the direct SELECT / LANGUAGE internal
// wrapper route). Red before the fix: this call panicked.
#[test]
fn fmgr_row_without_trigger_context_is_39p01() {
    let mut fcinfo = types_fmgr::LocalFcinfo::<0>::fresh(InvalidOid);
    // SAFETY: the scratch context outlives the call.
    unsafe { fcinfo.set_result_mcx(test_mcx()) };
    for row in RI_TRIGGERS_BUILTINS {
        let err = (row.func)(None, &mut fcinfo).err().expect("no trigger context");
        assert_eq!(err.sqlstate(), ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED);
        assert_eq!(
            err.message(),
            format!("function \"{}\" was not called by trigger manager", row.name)
        );
    }
}

// The fmgr row's identity is the const parameter, not flinfo (a LANGUAGE
// internal wrapper calls in under its own pg_proc OID).
#[test]
fn fmgr_rows_name_their_own_builtin() {
    for row in RI_TRIGGERS_BUILTINS {
        let (name, _) = ri_trig_kind(row.foid).expect("every row is an RI builtin");
        assert_eq!(name, row.name);
    }
}

// ri_CheckTrigger's event checks in C's order: AFTER ROW first, then the
// event kind (the AFTER STATEMENT / BEFORE ROW firings of finding 1).
#[test]
fn check_trigger_reports_timing_before_event_kind() {
    use types_trigger::{TRIGGER_EVENT_AFTER, TRIGGER_EVENT_BEFORE, TRIGGER_EVENT_INSERT, TRIGGER_EVENT_ROW};
    let stmt = TRIGGER_EVENT_INSERT | TRIGGER_EVENT_AFTER;
    let before_row = TRIGGER_EVENT_INSERT | TRIGGER_EVENT_ROW | TRIGGER_EVENT_BEFORE;
    let after_row = TRIGGER_EVENT_INSERT | TRIGGER_EVENT_ROW | TRIGGER_EVENT_AFTER;
    for ev in [stmt, before_row] {
        let err = ri_CheckTrigger("RI_FKey_noaction_del", RI_TRIGTYPE_DELETE, ev).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED);
        assert_eq!(err.message(), "function \"RI_FKey_noaction_del\" must be fired AFTER ROW");
    }
    let err = ri_CheckTrigger("RI_FKey_noaction_del", RI_TRIGTYPE_DELETE, after_row).unwrap_err();
    assert_eq!(err.message(), "function \"RI_FKey_noaction_del\" must be fired for DELETE");
    assert!(ri_CheckTrigger("RI_FKey_check_ins", RI_TRIGTYPE_INSERT, after_row).is_ok());
}

// audit-18.6 ri_triggers-2: ri_LoadConstraintInfo / ri_FetchConstraintInfo
// cross-check failures are elog(ERROR) in C (catchable XX000, no
// backend abort). They were assert!/panic! here; "not a foreign key" is
// reachable from SQL through CREATE CONSTRAINT TRIGGER on an RI builtin.
#[test]
fn constraint_cross_checks_are_xx000_errors() {
    let e = not_a_foreign_key(16397);
    assert_eq!(e.sqlstate(), ERRCODE_INTERNAL_ERROR);
    assert_eq!(e.message(), "constraint 16397 is not a foreign key constraint");
    let e = wrong_pg_constraint_entry("rt_t", "rt");
    assert_eq!(e.sqlstate(), ERRCODE_INTERNAL_ERROR);
    assert_eq!(e.message(), "wrong pg_constraint entry for trigger \"rt_t\" on table \"rt\"");
    let e = unrecognized_confmatchtype(b'x' as i8);
    assert_eq!(e.sqlstate(), ERRCODE_INTERNAL_ERROR);
    assert_eq!(e.message(), format!("unrecognized confmatchtype: {}", b'x'));
}
