//! Kani C≡Rust equivalence — ACL privilege-inquiry layer (WAVE 9 re-opens).
//!
//! Re-opens the `excluded(state)` has_*_privilege rows by generalizing the
//! state-seam-probe membership-oracle pattern to the FULL SQL-visible
//! composition: shipped fc_* wrapper (text/name/oid arg unpack, privilege
//! string -> AclMode parse, verdict/NULL mapping) + the shipped aclchk
//! object-aclcheck layer (system-catalog write strip, superuser bypass,
//! null-ACL acldefault, stored-ACL decode, aclmask, pg_read_all_data/
//! pg_write_all_data/pg_maintain fixups) — everything IN-theorem except the
//! seams below, each replaced identically on both sides by universally
//! quantified symbolic state.
//!
//! SEAMS (mirror c/pg_aclcheck.c's header; skew controls prove each is
//! load-bearing):
//!   1. membership oracle   — adt_acl::has_privs_of_role -> 8-slot
//!      first-match table (keys = exactly the roles a call can query:
//!      owner, every grantee, the three predefined-role oids, one spare).
//!   2. superuser oracle    — superuser::superuser_arg -> 2-slot table.
//!   3. catalog-tuple seam  — cache_syscache::SearchSysCache1 /
//!      SysCacheGetAttr[NotNull] / ReleaseSysCache stubbed to one symbolic
//!      object row (found, owner, relkind, relnamespace, acl-or-null).  The
//!      stored-ACL arm feeds a REAL aclitem[] varlena image, so the shipped
//!      decode (check_acl_payload + read_acl_item) stays in-theorem;
//!      aclchk::with_acl_datum is re-hosted onto a local buffer (its
//!      thread_local Vec scratch is Kani-unsupported; the decode calls
//!      inside are the shipped fns).
//!   4. role-name oracle    — adt_acl::get_role_oid stubbed to a
//!      call-indexed (2-slot) found/oid table; the shipped
//!      get_role_oid_or_public "public" short-circuit stays in-theorem.
//!   5. object-name oracle  — convert_table_name_str / get_database_oid /
//!      get_language_oid / get_namespace_oid stubbed TOTAL (always-found
//!      symbolic oid).  Name->oid failure precedence is out of proof.
//!   6. current user        — miscinit_seams::get_user_id.
//!   7. role-name-by-oid    — aclitemout's AUTHOID lookups: 2-slot
//!      oid->(found, name) table with a one-deep latch (lookup and rolname
//!      read are strictly sequential on both sides).
//!   8. namespace session state — isTempNamespace / isTempToastNamespace
//!      PINNED false (fence: non-temp namespaces).  The Rust port of
//!      IsSystemClass has NO temp-toast arm; probe_system_class_temp_toast
//!      is the expected-FAIL divergence witness for that gap.
//!
//! Also in the family: seam-wiring stubs replace the aclchk_seams dispatch
//! slots with direct calls to the same aclchk fns that init_seams installs
//! (wiring-identical; keeps AtomicPtr machinery out of the formula), and
//! elog::message_level_is_interesting is stubbed to `level >= ERROR`
//! (suppresses the aclparse grantor-defaulting WARNING emission path;
//! WARNING text/emission is out of proof on both sides — error VALUES are
//! not affected).
//!
//! FENCES (documented per-harness): privilege/name text is NUL-free ASCII
//! (SQL-reachable text; C model buffers are NUL-terminated), symbolic
//! privilege text capped at 8 bytes (len<=8) with LITERAL spot harnesses
//! for the longer map entries ("REFERENCES", "* WITH GRANT OPTION",
//! comma-lists); stored ACLs capped at 3 entries; error-message TEXT never
//! crosses the theorem — Err parity is (verdict, sqlstate-class) via the
//! PGQ_ERR_* code map.
//!
//! Controls (DEFAULT solver; kissat never terminates on failing
//! harnesses): control_membership_skew, control_catalog_owner_skew,
//! control_priv_map_mismatch — and probe_system_class_temp_toast (expected
//! FAIL, known-divergence witness, adjudicate before any recording).
//!
//! Run recipe: see runqueue.txt (family root).
#![recursion_limit = "512"] // ~25 stacked #[kani::stub] attrs per harness

#[cfg(kani)]
#[allow(static_mut_refs)]
mod proofs {
    use adt_acl::AclItem;
    use datum::{Datum, NullableDatum};
    use proof_support::{mcx_stubs, stubs};
    use types_core::Oid;
    use types_error::{PgError, PgResult};
    use types_fmgr::{LocalFcinfo, PGFunction};

    use std::os::raw::{c_char, c_int};

    // =====================================================================
    // C externs — vendored cores + seam globals (c/pg_aclcheck.c)
    // =====================================================================
    extern "C" {
        // table family
        fn pg_has_table_privilege_name_name(rolename: *const c_char, priv_: *mut c_char, isnull: *mut c_int, err: *mut c_int) -> c_int;
        fn pg_has_table_privilege_name(priv_: *mut c_char, isnull: *mut c_int, err: *mut c_int) -> c_int;
        fn pg_has_table_privilege_name_id(rolename: *const c_char, tableoid: Oid, priv_: *mut c_char, isnull: *mut c_int, err: *mut c_int) -> c_int;
        fn pg_has_table_privilege_id(tableoid: Oid, priv_: *mut c_char, isnull: *mut c_int, err: *mut c_int) -> c_int;
        fn pg_has_table_privilege_id_name(roleid: Oid, priv_: *mut c_char, isnull: *mut c_int, err: *mut c_int) -> c_int;
        fn pg_has_table_privilege_id_id(roleid: Oid, tableoid: Oid, priv_: *mut c_char, isnull: *mut c_int, err: *mut c_int) -> c_int;

        // sequence family
        fn pg_has_sequence_privilege_name_name(rolename: *const c_char, priv_: *mut c_char, isnull: *mut c_int, err: *mut c_int) -> c_int;
        fn pg_has_sequence_privilege_name(priv_: *mut c_char, isnull: *mut c_int, err: *mut c_int) -> c_int;
        fn pg_has_sequence_privilege_name_id(rolename: *const c_char, seqoid: Oid, priv_: *mut c_char, isnull: *mut c_int, err: *mut c_int) -> c_int;
        fn pg_has_sequence_privilege_id(seqoid: Oid, priv_: *mut c_char, isnull: *mut c_int, err: *mut c_int) -> c_int;
        fn pg_has_sequence_privilege_id_name(roleid: Oid, priv_: *mut c_char, isnull: *mut c_int, err: *mut c_int) -> c_int;
        fn pg_has_sequence_privilege_id_id(roleid: Oid, seqoid: Oid, priv_: *mut c_char, isnull: *mut c_int, err: *mut c_int) -> c_int;

        // generic object families (database/function/language/schema)
        fn pg_has_database_privilege_name_name(rolename: *const c_char, priv_: *mut c_char, isnull: *mut c_int, err: *mut c_int) -> c_int;
        fn pg_has_database_privilege_name(priv_: *mut c_char, isnull: *mut c_int, err: *mut c_int) -> c_int;
        fn pg_has_database_privilege_name_id(rolename: *const c_char, objoid: Oid, priv_: *mut c_char, isnull: *mut c_int, err: *mut c_int) -> c_int;
        fn pg_has_database_privilege_id(objoid: Oid, priv_: *mut c_char, isnull: *mut c_int, err: *mut c_int) -> c_int;
        fn pg_has_database_privilege_id_name(roleid: Oid, priv_: *mut c_char, isnull: *mut c_int, err: *mut c_int) -> c_int;
        fn pg_has_database_privilege_id_id(roleid: Oid, objoid: Oid, priv_: *mut c_char, isnull: *mut c_int, err: *mut c_int) -> c_int;

        fn pg_has_function_privilege_name_id(rolename: *const c_char, objoid: Oid, priv_: *mut c_char, isnull: *mut c_int, err: *mut c_int) -> c_int;
        fn pg_has_function_privilege_id(objoid: Oid, priv_: *mut c_char, isnull: *mut c_int, err: *mut c_int) -> c_int;
        fn pg_has_function_privilege_id_id(roleid: Oid, objoid: Oid, priv_: *mut c_char, isnull: *mut c_int, err: *mut c_int) -> c_int;

        fn pg_has_language_privilege_name_name(rolename: *const c_char, priv_: *mut c_char, isnull: *mut c_int, err: *mut c_int) -> c_int;
        fn pg_has_language_privilege_name(priv_: *mut c_char, isnull: *mut c_int, err: *mut c_int) -> c_int;
        fn pg_has_language_privilege_name_id(rolename: *const c_char, objoid: Oid, priv_: *mut c_char, isnull: *mut c_int, err: *mut c_int) -> c_int;
        fn pg_has_language_privilege_id(objoid: Oid, priv_: *mut c_char, isnull: *mut c_int, err: *mut c_int) -> c_int;
        fn pg_has_language_privilege_id_name(roleid: Oid, priv_: *mut c_char, isnull: *mut c_int, err: *mut c_int) -> c_int;
        fn pg_has_language_privilege_id_id(roleid: Oid, objoid: Oid, priv_: *mut c_char, isnull: *mut c_int, err: *mut c_int) -> c_int;

        fn pg_has_schema_privilege_name_name(rolename: *const c_char, priv_: *mut c_char, isnull: *mut c_int, err: *mut c_int) -> c_int;
        fn pg_has_schema_privilege_name(priv_: *mut c_char, isnull: *mut c_int, err: *mut c_int) -> c_int;
        fn pg_has_schema_privilege_name_id(rolename: *const c_char, objoid: Oid, priv_: *mut c_char, isnull: *mut c_int, err: *mut c_int) -> c_int;
        fn pg_has_schema_privilege_id(objoid: Oid, priv_: *mut c_char, isnull: *mut c_int, err: *mut c_int) -> c_int;
        fn pg_has_schema_privilege_id_name(roleid: Oid, priv_: *mut c_char, isnull: *mut c_int, err: *mut c_int) -> c_int;
        fn pg_has_schema_privilege_id_id(roleid: Oid, objoid: Oid, priv_: *mut c_char, isnull: *mut c_int, err: *mut c_int) -> c_int;

        // io family + stubs rows
        fn pg_aclitemin(s: *const c_char, out: *mut AclItem, err: *mut c_int) -> c_int;
        fn pg_aclitemout(aip: *const AclItem, out: *mut c_char, err: *mut c_int) -> c_int;
        fn pg_aclinsert(err: *mut c_int) -> c_int;
        fn pg_aclremove(err: *mut c_int) -> c_int;
        fn pg_acldefault_sql(objtypec: c_char, owner: Oid, items_out: *mut AclItem, nout: *mut c_int, err: *mut c_int) -> c_int;

        // seam globals (see c/pg_aclcheck.c header)
        static mut pgq_memb_role: [Oid; 8];
        static mut pgq_memb_ans: [c_int; 8];
        static mut pgq_memb_default: c_int;
        static mut pgq_super_role: [Oid; 2];
        static mut pgq_super_ans: [c_int; 2];
        static mut pgq_super_default: c_int;
        static mut pgq_cat_found: c_int;
        static mut pgq_cat_owner: Oid;
        static mut pgq_cat_relkind: c_int;
        static mut pgq_cat_relnamespace: Oid;
        static mut pgq_cat_acl_isnull: c_int;
        static mut pgq_cat_nacl: c_int;
        // NOTE: pgq_cat_acl (AclItem[8]) is deliberately NOT declared here —
        // struct-typed globals shared across the goto-link boundary abort
        // goto-cc (casting_replace_symbol invariant). Writes go through the
        // scalar-args C setter below.
        fn pgq_set_cat_acl(i: c_int, grantee: Oid, grantor: Oid, privs: u64) -> c_int;
        static mut pgq_role_calls: c_int;
        static mut pgq_role_found: [c_int; 2];
        static mut pgq_role_oid: [Oid; 2];
        static mut pgq_objname_oid: Oid;
        static mut pgq_current_user: Oid;
        static mut pgq_rname_found: [c_int; 2];
        static mut pgq_rname_oid: [Oid; 2];
        static mut pgq_rname_name: [[c_char; 64]; 2];
        static mut pgq_is_temp_namespace: c_int;
        static mut pgq_temp_toast: c_int;
        static mut pgq_my_database_id: Oid;
    }

    // PGQ_ERR_* codes (c/pg_aclcheck.c) — sqlstate-class parity map.
    fn errcode_of(e: &PgError) -> c_int {
        use types_error as te;
        if e.sqlstate == te::ERRCODE_INVALID_PARAMETER_VALUE {
            1
        } else if e.sqlstate == te::ERRCODE_UNDEFINED_OBJECT {
            2
        } else if e.sqlstate == te::ERRCODE_UNDEFINED_TABLE {
            3
        } else if e.sqlstate == te::ERRCODE_WRONG_OBJECT_TYPE {
            4
        } else if e.sqlstate == te::ERRCODE_NAME_TOO_LONG {
            5
        } else if e.sqlstate == te::ERRCODE_INVALID_TEXT_REPRESENTATION {
            6
        } else if e.sqlstate == te::ERRCODE_FEATURE_NOT_SUPPORTED {
            7
        } else if e.sqlstate == te::ERRCODE_UNDEFINED_SCHEMA {
            9
        } else {
            8 // internal / elog class
        }
    }

    // =====================================================================
    // Rust-side seam state (mirrors the pgq_* C globals; arm_* helpers
    // write BOTH sides with the same symbolic values)
    // =====================================================================
    const ZERO_ITEM: AclItem = AclItem { ai_grantee: 0, ai_grantor: 0, ai_privs: 0 };

    static mut R_MEMB_ROLE: [Oid; 8] = [0; 8];
    static mut R_MEMB_ANS: [bool; 8] = [false; 8];
    static mut R_MEMB_DEFAULT: bool = false;
    static mut R_SUPER_ROLE: [Oid; 2] = [0; 2];
    static mut R_SUPER_ANS: [bool; 2] = [false; 2];
    static mut R_SUPER_DEFAULT: bool = false;
    static mut R_CAT_FOUND: bool = false;
    static mut R_CAT_OWNER: Oid = 0;
    static mut R_CAT_RELKIND: u8 = 0;
    static mut R_CAT_RELNS: Oid = 0;
    static mut R_CAT_ACL_ISNULL: bool = false;
    static mut R_CAT_NACL: usize = 0;
    static mut R_CAT_ACL: [AclItem; 4] = [ZERO_ITEM; 4];
    static mut R_ROLE_CALLS: usize = 0;
    static mut R_ROLE_FOUND: [bool; 2] = [false; 2];
    static mut R_ROLE_OID: [Oid; 2] = [0; 2];
    static mut R_OBJNAME_OID: Oid = 0;
    static mut R_CURRENT_USER: Oid = 0;
    static mut R_RNAME_FOUND: [bool; 2] = [false; 2];
    static mut R_RNAME_OID: [Oid; 2] = [0; 2];
    static mut R_RNAME_NAME: [[u8; 64]; 2] = [[0; 64]; 2];
    static mut R_RNAME_LATCH: usize = 0;

    /// The stored-ACL varlena image the catalog seam hands to the shipped
    /// decoder: 4B header + 20B array header + up to 4 aclitems.
    static mut ACL_IMG: [u8; 4 + 20 + 16 * 4] = [0; 88];

    const ROLE_PG_READ_ALL_DATA: Oid = 6181;
    const ROLE_PG_WRITE_ALL_DATA: Oid = 6182;
    const ROLE_PG_MAINTAIN: Oid = 6337;

    // ---- seam models (kani::stub targets) ----

    /// Seam 1: membership oracle (member-fixed restriction; see module doc).
    fn model_has_privs(_member: Oid, role: Oid) -> PgResult<bool> {
        unsafe {
            for i in 0..8 {
                if R_MEMB_ROLE[i] == role {
                    return Ok(R_MEMB_ANS[i]);
                }
            }
            Ok(R_MEMB_DEFAULT)
        }
    }

    /// Seam 2: superuser oracle.
    fn model_superuser(roleid: Oid) -> PgResult<bool> {
        unsafe {
            for i in 0..2 {
                if R_SUPER_ROLE[i] == roleid {
                    return Ok(R_SUPER_ANS[i]);
                }
            }
            Ok(R_SUPER_DEFAULT)
        }
    }

    /// Seam 3: catalog-tuple seam. The returned CatCTuple is an inert token
    /// (all consumers of the pin are stubbed below; the image pointer is
    /// never dereferenced) — constructed by transmute because the type's
    /// fields are crate-private. All fields nonzero (NonNull validity).
    fn fake_tuple() -> catcache::CatCTuple {
        #[repr(C)]
        struct FakeTup {
            a: i32,
            b: u32,
            p: usize,
        }
        // SAFETY: same size; every byte nonzero-compatible with NonNull.
        unsafe { core::mem::transmute(FakeTup { a: 1, b: 1, p: 0x100 }) }
    }

    fn model_search_syscache1(
        cache_id: i32,
        key1: cache_syscache::SysCacheKey<'_>,
    ) -> PgResult<Option<catcache::CatCTuple>> {
        if cache_id == cache_syscache::cacheinfo::AUTHOID {
            // seam 7: role-name-by-oid (aclitemout); one-deep latch
            let roleid = match key1 {
                cache_syscache::SysCacheKey::Value(d) => d.as_oid(),
                _ => 0,
            };
            unsafe {
                for i in 0..2 {
                    if R_RNAME_OID[i] == roleid && R_RNAME_FOUND[i] {
                        R_RNAME_LATCH = i;
                        return Ok(Some(fake_tuple()));
                    }
                }
            }
            return Ok(None);
        }
        // seam 3: the single inspected object row
        unsafe {
            if R_CAT_FOUND {
                Ok(Some(fake_tuple()))
            } else {
                Ok(None)
            }
        }
    }

    fn model_syscache_get_attr(
        _cache_id: i32,
        _tup: &catcache::CatCTuple,
        _attnum: i32,
    ) -> PgResult<(Datum, bool)> {
        // Nullable attr reads in the vendored paths are exactly the ACL
        // columns (relacl/datacl/proacl/lanacl/nspacl).
        unsafe {
            if R_CAT_ACL_ISNULL {
                Ok((Datum::from_usize(0), true))
            } else {
                Ok((Datum::from_usize(ACL_IMG.as_ptr() as usize), false))
            }
        }
    }

    fn model_syscache_get_attr_not_null(
        cache_id: i32,
        _tup: &catcache::CatCTuple,
        attnum: i32,
    ) -> PgResult<Datum> {
        unsafe {
            if cache_id == cache_syscache::cacheinfo::AUTHOID {
                // rolname read for the latched role (aclitemout)
                return Ok(Datum::from_usize(
                    R_RNAME_NAME[R_RNAME_LATCH].as_ptr() as usize
                ));
            }
            if cache_id == cache_syscache::cacheinfo::RELOID {
                return Ok(match attnum {
                    18 => Datum::from_u8(R_CAT_RELKIND), // relkind
                    3 => Datum::from_oid(R_CAT_RELNS),   // relnamespace
                    _ => Datum::from_oid(R_CAT_OWNER),   // relowner (6)
                });
            }
            // generic object classes: only the owner column is read NotNull
            Ok(Datum::from_oid(R_CAT_OWNER))
        }
    }

    fn model_release_syscache(tuple: catcache::CatCTuple) {
        core::mem::forget(tuple); // inert token; no pin to drop
    }

    /// aclchk::with_acl_datum re-hosted onto a local buffer: the TLS Vec
    /// scratch is Kani-unsupported; the DECODE inside (check_acl_payload +
    /// read_acl_item) is the SHIPPED code and stays in-theorem.
    fn model_with_acl_datum<R>(
        d: Datum,
        f: impl FnOnce(&[AclItem]) -> PgResult<R>,
    ) -> PgResult<R> {
        let p = d.as_usize() as *const u8;
        // SAFETY: seam images are 4B-header varlenas built by arm_catalog.
        let payload: &[u8] = unsafe {
            let size = (core::ptr::read_unaligned(p as *const u32) >> 2) as usize;
            core::slice::from_raw_parts(p.add(4), size - 4)
        };
        let n = adt_acl::varlena::check_acl_payload(payload)?;
        let mut items = [ZERO_ITEM; 4];
        let n = n.min(4);
        for i in 0..n {
            items[i] = adt_acl::varlena::read_acl_item(payload, i);
        }
        f(&items[..n])
    }

    /// Seam 4: role-name oracle, call-indexed (mirrors the C model exactly;
    /// the shipped missing_ok error contract stays in the stub body).
    fn model_get_role_oid(_rolname: &str, missing_ok: bool) -> PgResult<Oid> {
        unsafe {
            let i = if R_ROLE_CALLS < 2 { R_ROLE_CALLS } else { 1 };
            R_ROLE_CALLS += 1;
            let oid = if R_ROLE_FOUND[i] { R_ROLE_OID[i] } else { 0 };
            if oid == 0 && !missing_ok {
                return Err(Box::new(
                    PgError::error("role does not exist")
                        .with_sqlstate(types_error::ERRCODE_UNDEFINED_OBJECT),
                ));
            }
            Ok(oid)
        }
    }

    // Seam 5: object-name oracles, TOTAL (always found).
    fn model_convert_table_name_str(_mcx: mcx::Mcx<'_>, _rawname: &str) -> PgResult<Oid> {
        unsafe { Ok(R_OBJNAME_OID) }
    }
    fn model_get_database_oid(_mcx: mcx::Mcx<'_>, _dbname: &str, _missing_ok: bool) -> PgResult<Oid> {
        unsafe { Ok(R_OBJNAME_OID) }
    }
    fn model_get_namespace_oid(_nspname: &str, _missing_ok: bool) -> PgResult<Oid> {
        unsafe { Ok(R_OBJNAME_OID) }
    }
    fn model_get_language_oid(_langname: &str, _missing_ok: bool) -> PgResult<Oid> {
        unsafe { Ok(R_OBJNAME_OID) }
    }

    // Seam 6: current user.
    fn model_get_user_id() -> Oid {
        unsafe { R_CURRENT_USER }
    }

    // Seam 3 companions on the lsyscache/syscache_seams routes.
    fn model_get_rel_relkind(_relid: Oid) -> PgResult<i8> {
        unsafe {
            if R_CAT_FOUND {
                Ok(R_CAT_RELKIND as i8)
            } else {
                Ok(0)
            }
        }
    }
    fn model_pg_class_relname(_relid: Oid) -> PgResult<Option<types_tuple::NameData>> {
        Ok(None) // error-message text only; out of proof
    }

    // Seam 8: namespace session state, pinned (fence: non-temp namespaces).
    fn model_is_temp_namespace(_nsp: Oid) -> bool {
        false
    }

    // Seam wiring: replace the aclchk_seams dispatch slots with direct calls
    // to the fns aclchk::init_seams installs (wiring-identical).
    fn wire_pg_class_aclcheck_ext(t: Oid, r: Oid, m: u64) -> PgResult<(i32, bool)> {
        aclchk::pg_class_aclcheck_ext(t, r, m)
    }
    fn wire_object_aclcheck(c: Oid, o: Oid, r: Oid, m: u64) -> PgResult<i32> {
        aclchk::object_aclcheck(c, o, r, m)
    }
    fn wire_object_aclcheck_ext(c: Oid, o: Oid, r: Oid, m: u64) -> PgResult<(i32, bool)> {
        aclchk::object_aclcheck_ext(c, o, r, m)
    }

    // WARNING emission out of proof (aclparse grantor defaulting); ERROR
    // behavior preserved.
    fn model_level_interesting(elevel: types_error::ErrorLevel) -> bool {
        elevel >= types_error::ERROR
    }

    /// Unreachable at runtime here (the only builder-level report is the
    /// aclparse WARNING, suppressed by model_level_interesting); present so
    /// reachability codegen never enters the elog report subtree
    /// (ipc_seams::proc_exit::call ICEs the Kani 0.67 goto codegen).
    fn model_throw_error_data(edata: PgError) -> PgResult<()> {
        if edata.level >= types_error::ERROR {
            Err(Box::new(edata))
        } else {
            Ok(())
        }
    }

    // =====================================================================
    // Seam arming (writes BOTH sides with the same symbolic values)
    // =====================================================================

    /// Arm the catalog-tuple seam with a fully symbolic object row (found,
    /// owner, relkind, relnamespace, acl-null-or-(nacl<=3 symbolic items))
    /// and build the varlena image the Rust decoder reads.
    fn arm_catalog() {
        let found: bool = kani::any();
        let owner: Oid = kani::any();
        let relkind: u8 = kani::any();
        let relns: Oid = kani::any();
        let isnull: bool = kani::any();
        let nacl: usize = kani::any();
        kani::assume(nacl <= 3);
        unsafe {
            R_CAT_FOUND = found;
            R_CAT_OWNER = owner;
            R_CAT_RELKIND = relkind;
            R_CAT_RELNS = relns;
            R_CAT_ACL_ISNULL = isnull;
            R_CAT_NACL = nacl;
            pgq_cat_found = found as c_int;
            pgq_cat_owner = owner;
            pgq_cat_relkind = relkind as c_int;
            pgq_cat_relnamespace = relns;
            pgq_cat_acl_isnull = isnull as c_int;
            pgq_cat_nacl = nacl as c_int;
            for i in 0..4 {
                let it = AclItem {
                    ai_grantee: kani::any(),
                    ai_grantor: kani::any(),
                    ai_privs: kani::any(),
                };
                R_CAT_ACL[i] = it;
                pgq_set_cat_acl(i as c_int, it.ai_grantee, it.ai_grantor, it.ai_privs);
            }
            for i in 4..8 {
                pgq_set_cat_acl(i as c_int, 0, 0, 0);
            }
            // 4B-header aclitem[] varlena image (allocacl layout)
            let size: u32 = (4 + 20 + 16 * nacl) as u32;
            ACL_IMG[0..4].copy_from_slice(&(size << 2).to_le_bytes());
            ACL_IMG[4..8].copy_from_slice(&1i32.to_le_bytes()); // ndim
            ACL_IMG[8..12].copy_from_slice(&0i32.to_le_bytes()); // dataoffset
            ACL_IMG[12..16].copy_from_slice(&adt_acl::ACLITEMOID.to_le_bytes()); // elemtype
            ACL_IMG[16..20].copy_from_slice(&(nacl as i32).to_le_bytes()); // dims
            ACL_IMG[20..24].copy_from_slice(&1i32.to_le_bytes()); // lbound
            for i in 0..4 {
                let off = 24 + 16 * i;
                ACL_IMG[off..off + 4].copy_from_slice(&R_CAT_ACL[i].ai_grantee.to_le_bytes());
                ACL_IMG[off + 4..off + 8].copy_from_slice(&R_CAT_ACL[i].ai_grantor.to_le_bytes());
                ACL_IMG[off + 8..off + 16].copy_from_slice(&R_CAT_ACL[i].ai_privs.to_le_bytes());
            }
        }
    }

    /// Arm the membership oracle over the reachable query set (owner, every
    /// grantee, the predefined-role oids, one spare) with fully symbolic
    /// answers, the superuser oracle over {roleid, spare}, and the scalar
    /// seams. Call AFTER arm_catalog.
    fn arm_role_seams(roleid: Oid) {
        unsafe {
            let keys: [Oid; 8] = [
                R_CAT_OWNER,
                R_CAT_ACL[0].ai_grantee,
                R_CAT_ACL[1].ai_grantee,
                R_CAT_ACL[2].ai_grantee,
                ROLE_PG_READ_ALL_DATA,
                ROLE_PG_WRITE_ALL_DATA,
                ROLE_PG_MAINTAIN,
                kani::any(),
            ];
            for i in 0..8 {
                let ans: bool = kani::any();
                R_MEMB_ROLE[i] = keys[i];
                R_MEMB_ANS[i] = ans;
                pgq_memb_role[i] = keys[i];
                pgq_memb_ans[i] = ans as c_int;
            }
            let d: bool = kani::any();
            R_MEMB_DEFAULT = d;
            pgq_memb_default = d as c_int;

            let skeys: [Oid; 2] = [roleid, kani::any()];
            for i in 0..2 {
                let ans: bool = kani::any();
                R_SUPER_ROLE[i] = skeys[i];
                R_SUPER_ANS[i] = ans;
                pgq_super_role[i] = skeys[i];
                pgq_super_ans[i] = ans as c_int;
            }
            let sd: bool = kani::any();
            R_SUPER_DEFAULT = sd;
            pgq_super_default = sd as c_int;

            let cu: Oid = kani::any();
            R_CURRENT_USER = cu;
            pgq_current_user = cu;

            let obj: Oid = kani::any();
            R_OBJNAME_OID = obj;
            pgq_objname_oid = obj;

            R_ROLE_CALLS = 0;
            pgq_role_calls = 0;
            for i in 0..2 {
                let f: bool = kani::any();
                let o: Oid = kani::any();
                R_ROLE_FOUND[i] = f;
                R_ROLE_OID[i] = o;
                pgq_role_found[i] = f as c_int;
                pgq_role_oid[i] = o;
            }

            // seam 8 pinned (fence: non-temp namespaces; see module doc)
            pgq_is_temp_namespace = 0;
            pgq_temp_toast = 0;
            pgq_my_database_id = kani::any();
        }
    }

    // =====================================================================
    // fcinfo plumbing
    // =====================================================================
    const fn builtin(foid: u32) -> PGFunction {
        let t = adt_acl::builtins::ACL_BUILTINS;
        let mut i = 0;
        loop {
            if t[i].foid == foid {
                return t[i].func;
            }
            i += 1;
        }
    }

    /// Invoke a shipped fc_* wrapper on a real LocalFcinfo frame; returns
    /// (result, returned-NULL).
    fn run_fc<const N: usize>(f: PGFunction, args: [Datum; N]) -> (PgResult<Datum>, bool) {
        let mut fci = LocalFcinfo::<N>::new(0);
        let mut i = 0;
        while i < N {
            fci.args[i] = NullableDatum::value(args[i]);
            i += 1;
        }
        let r = f(None, &mut fci);
        let isnull = fci.isnull;
        (r, isnull)
    }

    /// As run_fc but with an armed result mcx (name-object forms and
    /// varlena-returning wrappers reach fcinfo.result_mcx()).
    fn run_fc_mcx<const N: usize>(
        f: PGFunction,
        args: [Datum; N],
        mcx: mcx::Mcx<'_>,
    ) -> (PgResult<Datum>, bool) {
        let mut fci = LocalFcinfo::<N>::new(0);
        let mut i = 0;
        while i < N {
            fci.args[i] = NullableDatum::value(args[i]);
            i += 1;
        }
        // SAFETY: ctx outlives the call (forgotten at harness end).
        unsafe { fci.set_result_mcx(mcx) };
        let r = f(None, &mut fci);
        let isnull = fci.isnull;
        (r, isnull)
    }

    const PRIV_CAP: usize = 8;

    /// Symbolic NUL-free ASCII text (fence: SQL-reachable text values).
    fn any_text_bytes() -> ([u8; PRIV_CAP], usize) {
        let mut b = [0u8; PRIV_CAP];
        let len: usize = kani::any();
        kani::assume(len <= PRIV_CAP);
        for i in 0..PRIV_CAP {
            let c: u8 = kani::any();
            kani::assume(c >= 1 && c <= 127);
            b[i] = c;
        }
        (b, len)
    }

    /// 4B-header text varlena image for arg i of a strict builtin.
    fn text_image(bytes: &[u8], len: usize) -> [u8; 4 + PRIV_CAP] {
        let mut img = [0u8; 4 + PRIV_CAP];
        let size = ((4 + len) as u32) << 2;
        img[0..4].copy_from_slice(&size.to_le_bytes());
        let mut i = 0;
        while i < len {
            img[4 + i] = bytes[i];
            i += 1;
        }
        img
    }

    /// NUL-terminated copy for the C side (modifiable, as text_to_cstring
    /// hands convert_any_priv_string).
    fn cbuf(bytes: &[u8], len: usize) -> [u8; PRIV_CAP + 1] {
        let mut b = [0u8; PRIV_CAP + 1];
        let mut i = 0;
        while i < len {
            b[i] = bytes[i];
            i += 1;
        }
        b
    }

    /// Symbolic role Name (64-byte, NUL-terminated, ASCII, len<=6 so that
    /// "public" stays reachable).
    fn any_name() -> [u8; 64] {
        let mut n = [0u8; 64];
        let len: usize = kani::any();
        kani::assume(len <= 6);
        for i in 0..6 {
            let c: u8 = kani::any();
            kani::assume(c >= 1 && c <= 127);
            if i < len {
                n[i] = c;
            }
        }
        n
    }

    /// Compare a shipped-wrapper outcome against the C core's outcome.
    fn assert_same(
        r: PgResult<Datum>,
        isnull: bool,
        c_ret: c_int,
        c_isnull: c_int,
        c_err: c_int,
    ) {
        match r {
            Ok(d) => {
                assert!(c_err == 0);
                assert!(isnull as c_int == c_isnull);
                if !isnull {
                    assert!(d.as_bool() as c_int == c_ret);
                }
            }
            Err(e) => {
                let code = errcode_of(&e);
                core::mem::forget(e); // Box<PgError> drop glue out of formula
                assert!(c_err == code);
            }
        }
    }

    // =====================================================================
    // Harness generator: the full common stub roster
    // =====================================================================
    macro_rules! acl_harness {
        ($(#[$doc:meta])* $name:ident, $unwind:literal, $body:block) => {
            $(#[$doc])*
            #[kani::proof]
            #[kani::unwind($unwind)]
            #[kani::stub(adt_acl::has_privs_of_role, model_has_privs)]
            #[kani::stub(superuser::superuser_arg, model_superuser)]
            #[kani::stub(cache_syscache::SearchSysCache1, model_search_syscache1)]
            #[kani::stub(cache_syscache::SysCacheGetAttr, model_syscache_get_attr)]
            #[kani::stub(cache_syscache::SysCacheGetAttrNotNull, model_syscache_get_attr_not_null)]
            #[kani::stub(cache_syscache::ReleaseSysCache, model_release_syscache)]
            #[kani::stub(aclchk::with_acl_datum, model_with_acl_datum)]
            #[kani::stub(aclchk_seams::pg_class_aclcheck_ext::call, wire_pg_class_aclcheck_ext)]
            #[kani::stub(aclchk_seams::object_aclcheck::call, wire_object_aclcheck)]
            #[kani::stub(aclchk_seams::object_aclcheck_ext::call, wire_object_aclcheck_ext)]
            #[kani::stub(miscinit_seams::get_user_id::call, model_get_user_id)]
            #[kani::stub(adt_acl::get_role_oid, model_get_role_oid)]
            #[kani::stub(adt_acl::builtins::convert_table_name_str, model_convert_table_name_str)]
            #[kani::stub(dbcommands_seams::get_database_oid::call, model_get_database_oid)]
            #[kani::stub(catalog_namespace::get_namespace_oid, model_get_namespace_oid)]
            #[kani::stub(adt_acl::get_language_oid, model_get_language_oid)]
            #[kani::stub(catalog_namespace::isTempNamespace, model_is_temp_namespace)]
            #[kani::stub(lsyscache::get_rel_relkind, model_get_rel_relkind)]
            #[kani::stub(syscache_seams::pg_class_relname::call, model_pg_class_relname)]
            #[kani::stub(elog::message_level_is_interesting, model_level_interesting)]
            #[kani::stub(elog::ThrowErrorData, model_throw_error_data)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            #[kani::stub(std::string::String::from_utf8_lossy, stubs::stub_from_utf8_lossy)]
            fn $name() $body
        };
    }

    /// mcx-armed variant (name-object forms; acldefault_sql; aclitemout):
    /// adds the proof_support mcx-stubs recipe.
    macro_rules! acl_harness_mcx {
        ($(#[$doc:meta])* $name:ident, $unwind:literal, $body:block) => {
            $(#[$doc])*
            #[kani::proof]
            #[kani::unwind($unwind)]
            #[kani::stub(adt_acl::has_privs_of_role, model_has_privs)]
            #[kani::stub(superuser::superuser_arg, model_superuser)]
            #[kani::stub(cache_syscache::SearchSysCache1, model_search_syscache1)]
            #[kani::stub(cache_syscache::SysCacheGetAttr, model_syscache_get_attr)]
            #[kani::stub(cache_syscache::SysCacheGetAttrNotNull, model_syscache_get_attr_not_null)]
            #[kani::stub(cache_syscache::ReleaseSysCache, model_release_syscache)]
            #[kani::stub(aclchk::with_acl_datum, model_with_acl_datum)]
            #[kani::stub(aclchk_seams::pg_class_aclcheck_ext::call, wire_pg_class_aclcheck_ext)]
            #[kani::stub(aclchk_seams::object_aclcheck::call, wire_object_aclcheck)]
            #[kani::stub(aclchk_seams::object_aclcheck_ext::call, wire_object_aclcheck_ext)]
            #[kani::stub(miscinit_seams::get_user_id::call, model_get_user_id)]
            #[kani::stub(adt_acl::get_role_oid, model_get_role_oid)]
            #[kani::stub(adt_acl::builtins::convert_table_name_str, model_convert_table_name_str)]
            #[kani::stub(dbcommands_seams::get_database_oid::call, model_get_database_oid)]
            #[kani::stub(catalog_namespace::get_namespace_oid, model_get_namespace_oid)]
            #[kani::stub(adt_acl::get_language_oid, model_get_language_oid)]
            #[kani::stub(catalog_namespace::isTempNamespace, model_is_temp_namespace)]
            #[kani::stub(lsyscache::get_rel_relkind, model_get_rel_relkind)]
            #[kani::stub(syscache_seams::pg_class_relname::call, model_pg_class_relname)]
            #[kani::stub(elog::message_level_is_interesting, model_level_interesting)]
            #[kani::stub(elog::ThrowErrorData, model_throw_error_data)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            #[kani::stub(std::string::String::from_utf8_lossy, stubs::stub_from_utf8_lossy)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            fn $name() $body
        };
    }

    // =====================================================================
    // has_*_privilege — id_id / name_id / id forms (no mcx needed)
    // =====================================================================
    macro_rules! eq_id_id {
        ($name:ident, $foid:literal, $cfn:ident, $unwind:literal) => {
            acl_harness! {
                /// (roleid, objoid, priv) form: symbolic everything, priv text
                /// len<=8 NUL-free ASCII; seams per module doc.
                $name, $unwind, {
                    let roleid: Oid = kani::any();
                    let objoid: Oid = kani::any();
                    let (pb, plen) = any_text_bytes();
                    arm_catalog();
                    arm_role_seams(roleid);

                    const F: PGFunction = builtin($foid);
                    let img = text_image(&pb, plen);
                    let (r, isnull) = run_fc::<3>(
                        F,
                        [
                            Datum::from_oid(roleid),
                            Datum::from_oid(objoid),
                            Datum::from_usize(img.as_ptr() as usize),
                        ],
                    );

                    let mut cp = cbuf(&pb, plen);
                    let (mut cisnull, mut cerr) = (0 as c_int, 0 as c_int);
                    let c = unsafe {
                        $cfn(roleid, objoid, cp.as_mut_ptr() as *mut c_char, &mut cisnull, &mut cerr)
                    };
                    assert_same(r, isnull, c, cisnull, cerr);
                }
            }
        };
    }

    macro_rules! eq_name_id {
        ($name:ident, $foid:literal, $cfn:ident, $unwind:literal) => {
            acl_harness! {
                /// (rolename, objoid, priv) form: symbolic Name (len<=6,
                /// "public" reachable) through the shipped
                /// get_role_oid_or_public + the call-indexed role oracle.
                $name, $unwind, {
                    let objoid: Oid = kani::any();
                    let (pb, plen) = any_text_bytes();
                    let name = any_name();
                    arm_catalog();
                    // The resolved roleid is not a dedicated superuser-oracle
                    // key here; it answers via the shared symbolic DEFAULT.
                    // Only ONE superuser query (the resolved role) is
                    // reachable per call, so a single symbolic bool still
                    // covers the seam's full output range — identically on
                    // both sides.
                    arm_role_seams(kani::any());

                    const F: PGFunction = builtin($foid);
                    let img = text_image(&pb, plen);
                    let (r, isnull) = run_fc::<3>(
                        F,
                        [
                            Datum::from_usize(name.as_ptr() as usize),
                            Datum::from_oid(objoid),
                            Datum::from_usize(img.as_ptr() as usize),
                        ],
                    );

                    let mut cp = cbuf(&pb, plen);
                    let (mut cisnull, mut cerr) = (0 as c_int, 0 as c_int);
                    let c = unsafe {
                        $cfn(name.as_ptr() as *const c_char, objoid, cp.as_mut_ptr() as *mut c_char, &mut cisnull, &mut cerr)
                    };
                    assert_same(r, isnull, c, cisnull, cerr);
                }
            }
        };
    }

    macro_rules! eq_id_form {
        ($name:ident, $foid:literal, $cfn:ident, $unwind:literal) => {
            acl_harness! {
                /// (objoid, priv) current-user form: GetUserId seam.
                $name, $unwind, {
                    let objoid: Oid = kani::any();
                    let (pb, plen) = any_text_bytes();
                    arm_catalog();
                    arm_role_seams(kani::any());
                    // superuser key must cover the CURRENT user this form uses
                    unsafe {
                        R_SUPER_ROLE[0] = R_CURRENT_USER;
                        pgq_super_role[0] = R_CURRENT_USER;
                    }

                    const F: PGFunction = builtin($foid);
                    let img = text_image(&pb, plen);
                    let (r, isnull) = run_fc::<2>(
                        F,
                        [Datum::from_oid(objoid), Datum::from_usize(img.as_ptr() as usize)],
                    );

                    let mut cp = cbuf(&pb, plen);
                    let (mut cisnull, mut cerr) = (0 as c_int, 0 as c_int);
                    let c = unsafe {
                        $cfn(objoid, cp.as_mut_ptr() as *mut c_char, &mut cisnull, &mut cerr)
                    };
                    assert_same(r, isnull, c, cisnull, cerr);
                }
            }
        };
    }

    // name-object forms (object referenced by name -> oracle; needs mcx)
    macro_rules! eq_name_name {
        ($name:ident, $foid:literal, $cfn:ident, $unwind:literal) => {
            acl_harness_mcx! {
                /// (rolename, objname, priv) form: object name -> total
                /// oracle (name bytes opaque, LITERAL "t"); role name
                /// symbolic; result mcx armed via the mcx-stubs recipe.
                $name, $unwind, {
                    let (pb, plen) = any_text_bytes();
                    let name = any_name();
                    arm_catalog();
                    // resolved-role superuser answer rides the shared
                    // symbolic default (single reachable query; see
                    // eq_name_id note).
                    arm_role_seams(kani::any());

                    let ctx = mcx::MemoryContext::new_bump("kani-aclcheck");
                    const F: PGFunction = builtin($foid);
                    let img = text_image(&pb, plen);
                    let obj = text_image(b"t", 1);
                    let (r, isnull) = run_fc_mcx::<3>(
                        F,
                        [
                            Datum::from_usize(name.as_ptr() as usize),
                            Datum::from_usize(obj.as_ptr() as usize),
                            Datum::from_usize(img.as_ptr() as usize),
                        ],
                        ctx.mcx(),
                    );

                    let mut cp = cbuf(&pb, plen);
                    let (mut cisnull, mut cerr) = (0 as c_int, 0 as c_int);
                    let c = unsafe {
                        $cfn(name.as_ptr() as *const c_char, cp.as_mut_ptr() as *mut c_char, &mut cisnull, &mut cerr)
                    };
                    assert_same(r, isnull, c, cisnull, cerr);
                    core::mem::forget(ctx);
                }
            }
        };
    }

    macro_rules! eq_id_name {
        ($name:ident, $foid:literal, $cfn:ident, $unwind:literal) => {
            acl_harness_mcx! {
                /// (roleid, objname, priv) form: object-name oracle + mcx.
                $name, $unwind, {
                    let roleid: Oid = kani::any();
                    let (pb, plen) = any_text_bytes();
                    arm_catalog();
                    arm_role_seams(roleid);

                    let ctx = mcx::MemoryContext::new_bump("kani-aclcheck");
                    const F: PGFunction = builtin($foid);
                    let img = text_image(&pb, plen);
                    let obj = text_image(b"t", 1);
                    let (r, isnull) = run_fc_mcx::<3>(
                        F,
                        [
                            Datum::from_oid(roleid),
                            Datum::from_usize(obj.as_ptr() as usize),
                            Datum::from_usize(img.as_ptr() as usize),
                        ],
                        ctx.mcx(),
                    );

                    let mut cp = cbuf(&pb, plen);
                    let (mut cisnull, mut cerr) = (0 as c_int, 0 as c_int);
                    let c = unsafe {
                        $cfn(roleid, cp.as_mut_ptr() as *mut c_char, &mut cisnull, &mut cerr)
                    };
                    assert_same(r, isnull, c, cisnull, cerr);
                    core::mem::forget(ctx);
                }
            }
        };
    }

    macro_rules! eq_name_form {
        ($name:ident, $foid:literal, $cfn:ident, $unwind:literal) => {
            acl_harness_mcx! {
                /// (objname, priv) current-user form: object-name oracle +
                /// GetUserId seam + mcx.
                $name, $unwind, {
                    let (pb, plen) = any_text_bytes();
                    arm_catalog();
                    arm_role_seams(kani::any());
                    unsafe {
                        R_SUPER_ROLE[0] = R_CURRENT_USER;
                        pgq_super_role[0] = R_CURRENT_USER;
                    }

                    let ctx = mcx::MemoryContext::new_bump("kani-aclcheck");
                    const F: PGFunction = builtin($foid);
                    let img = text_image(&pb, plen);
                    let obj = text_image(b"t", 1);
                    let (r, isnull) = run_fc_mcx::<2>(
                        F,
                        [
                            Datum::from_usize(obj.as_ptr() as usize),
                            Datum::from_usize(img.as_ptr() as usize),
                        ],
                        ctx.mcx(),
                    );

                    let mut cp = cbuf(&pb, plen);
                    let (mut cisnull, mut cerr) = (0 as c_int, 0 as c_int);
                    let c = unsafe {
                        $cfn(cp.as_mut_ptr() as *mut c_char, &mut cisnull, &mut cerr)
                    };
                    assert_same(r, isnull, c, cisnull, cerr);
                    core::mem::forget(ctx);
                }
            }
        };
    }

    // ---- table (oids 1922-1927; pg_class route incl. system-catalog strip
    // and read/write/maintain fixups) ----
    eq_id_id!(eq_has_table_privilege_id_id, 1925, pg_has_table_privilege_id_id, 20);
    eq_name_id!(eq_has_table_privilege_name_id, 1923, pg_has_table_privilege_name_id, 20);
    eq_id_form!(eq_has_table_privilege_id, 1927, pg_has_table_privilege_id, 20);
    eq_name_name!(eq_has_table_privilege_name_name, 1922, pg_has_table_privilege_name_name, 20);
    eq_id_name!(eq_has_table_privilege_id_name, 1924, pg_has_table_privilege_id_name, 20);
    eq_name_form!(eq_has_table_privilege_name, 1926, pg_has_table_privilege_name, 20);

    // ---- sequence (oids 2181-2186; adds the relkind gate) ----
    eq_id_id!(eq_has_sequence_privilege_id_id, 2184, pg_has_sequence_privilege_id_id, 20);
    eq_name_id!(eq_has_sequence_privilege_name_id, 2182, pg_has_sequence_privilege_name_id, 20);
    eq_id_form!(eq_has_sequence_privilege_id, 2186, pg_has_sequence_privilege_id, 20);
    eq_name_name!(eq_has_sequence_privilege_name_name, 2181, pg_has_sequence_privilege_name_name, 20);
    eq_id_name!(eq_has_sequence_privilege_id_name, 2183, pg_has_sequence_privilege_id_name, 20);
    eq_name_form!(eq_has_sequence_privilege_name, 2185, pg_has_sequence_privilege_name, 20);

    // ---- database (oids 2250-2255; generic object route) ----
    eq_id_id!(eq_has_database_privilege_id_id, 2253, pg_has_database_privilege_id_id, 14);
    eq_name_id!(eq_has_database_privilege_name_id, 2251, pg_has_database_privilege_name_id, 14);
    eq_id_form!(eq_has_database_privilege_id, 2255, pg_has_database_privilege_id, 14);
    eq_name_name!(eq_has_database_privilege_name_name, 2250, pg_has_database_privilege_name_name, 14);
    eq_id_name!(eq_has_database_privilege_id_name, 2252, pg_has_database_privilege_id_name, 14);
    eq_name_form!(eq_has_database_privilege_name, 2254, pg_has_database_privilege_name, 14);

    // ---- function (oids 2257/2259/2261; the name-OBJECT forms 2256/2258/
    // 2260 stay excluded — regprocedurein name resolution is parser+catalog
    // dominated, see runqueue/report) ----
    eq_id_id!(eq_has_function_privilege_id_id, 2259, pg_has_function_privilege_id_id, 14);
    eq_name_id!(eq_has_function_privilege_name_id, 2257, pg_has_function_privilege_name_id, 14);
    eq_id_form!(eq_has_function_privilege_id, 2261, pg_has_function_privilege_id, 14);

    // ---- language (oids 2262-2267) ----
    eq_id_id!(eq_has_language_privilege_id_id, 2265, pg_has_language_privilege_id_id, 14);
    eq_name_id!(eq_has_language_privilege_name_id, 2263, pg_has_language_privilege_name_id, 14);
    eq_id_form!(eq_has_language_privilege_id, 2267, pg_has_language_privilege_id, 14);
    eq_name_name!(eq_has_language_privilege_name_name, 2262, pg_has_language_privilege_name_name, 14);
    eq_id_name!(eq_has_language_privilege_id_name, 2264, pg_has_language_privilege_id_name, 14);
    eq_name_form!(eq_has_language_privilege_name, 2266, pg_has_language_privilege_name, 14);

    // ---- schema (oids 2268-2273; pg_namespace route incl. usage fixup;
    // temp-namespace arm fenced OFF, see module doc) ----
    eq_id_id!(eq_has_schema_privilege_id_id, 2271, pg_has_schema_privilege_id_id, 14);
    eq_name_id!(eq_has_schema_privilege_name_id, 2269, pg_has_schema_privilege_name_id, 14);
    eq_id_form!(eq_has_schema_privilege_id, 2273, pg_has_schema_privilege_id, 14);
    eq_name_name!(eq_has_schema_privilege_name_name, 2268, pg_has_schema_privilege_name_name, 14);
    eq_id_name!(eq_has_schema_privilege_id_name, 2270, pg_has_schema_privilege_id_name, 14);
    eq_name_form!(eq_has_schema_privilege_name, 2272, pg_has_schema_privilege_name, 14);

    // =====================================================================
    // LITERAL spot harnesses: the priv-map entries len<=8 cannot spell
    // (WITH GRANT OPTION variants, REFERENCES, comma-lists).
    // =====================================================================
    macro_rules! spot_priv {
        ($name:ident, $foid:literal, $cfn:ident, $lit:literal, $unwind:literal) => {
            acl_harness! {
                /// LITERAL privilege-string spot: full seam quantification,
                /// concrete priv text (covers a map entry out of the len<=8
                /// symbolic band).
                $name, $unwind, {
                    let roleid: Oid = kani::any();
                    let objoid: Oid = kani::any();
                    arm_catalog();
                    arm_role_seams(roleid);

                    const LIT: &[u8] = $lit;
                    const F: PGFunction = builtin($foid);
                    let mut img = [0u8; 4 + LIT.len()];
                    let size = ((4 + LIT.len()) as u32) << 2;
                    img[0..4].copy_from_slice(&size.to_le_bytes());
                    img[4..].copy_from_slice(LIT);
                    let (r, isnull) = run_fc::<3>(
                        F,
                        [
                            Datum::from_oid(roleid),
                            Datum::from_oid(objoid),
                            Datum::from_usize(img.as_ptr() as usize),
                        ],
                    );

                    let mut cp = [0u8; LIT.len() + 1];
                    cp[..LIT.len()].copy_from_slice(LIT);
                    let (mut cisnull, mut cerr) = (0 as c_int, 0 as c_int);
                    let c = unsafe {
                        $cfn(roleid, objoid, cp.as_mut_ptr() as *mut c_char, &mut cisnull, &mut cerr)
                    };
                    assert_same(r, isnull, c, cisnull, cerr);
                }
            }
        };
    }

    spot_priv!(spot_table_references, 1925, pg_has_table_privilege_id_id, b"REFERENCES", 20);
    spot_priv!(spot_table_select_wgo, 1925, pg_has_table_privilege_id_id, b"SELECT WITH GRANT OPTION", 30);
    spot_priv!(spot_table_list, 1925, pg_has_table_privilege_id_id, b"SELECT, INSERT", 20);
    spot_priv!(spot_database_temporary, 2253, pg_has_database_privilege_id_id, b"TEMPORARY", 14);
    spot_priv!(spot_schema_usage_wgo, 2271, pg_has_schema_privilege_id_id, b"USAGE WITH GRANT OPTION", 30);

    // =====================================================================
    // Covers (vacuity insurance) — DEFAULT solver (covers are SAT calls)
    // =====================================================================
    acl_harness! {
        /// Regime covers for the table id_id route inside the fenced domain:
        /// NULL return, both verdicts, priv-parse error, superuser bypass,
        /// null-ACL default path and stored-ACL path.
        cover_table_id_id_regimes, 20, {
            let roleid: Oid = kani::any();
            let objoid: Oid = kani::any();
            let (pb, plen) = any_text_bytes();
            arm_catalog();
            arm_role_seams(roleid);

            const F: PGFunction = builtin(1925);
            let img = text_image(&pb, plen);
            let (r, isnull) = run_fc::<3>(
                F,
                [
                    Datum::from_oid(roleid),
                    Datum::from_oid(objoid),
                    Datum::from_usize(img.as_ptr() as usize),
                ],
            );
            match r {
                Ok(d) => {
                    kani::cover!(isnull, "missing-object NULL reachable");
                    kani::cover!(!isnull && d.as_bool(), "granted verdict reachable");
                    kani::cover!(!isnull && !d.as_bool(), "denied verdict reachable");
                    kani::cover!(
                        unsafe { !isnull && R_CAT_ACL_ISNULL },
                        "acldefault path reachable"
                    );
                    kani::cover!(
                        unsafe { !isnull && !R_CAT_ACL_ISNULL && R_CAT_NACL > 0 },
                        "stored-ACL decode path reachable"
                    );
                    core::mem::forget(d);
                }
                Err(e) => {
                    kani::cover!(errcode_of(&e) == 1, "priv-parse error reachable");
                    core::mem::forget(e);
                }
            }
        }
    }

    // =====================================================================
    // Controls (must FAIL; DEFAULT solver) + known-divergence probe
    // =====================================================================
    acl_harness! {
        /// Membership-oracle skew: identical inputs, C side answers all-true
        /// while Rust answers all-false — MUST FAIL (seam load-bearing).
        control_membership_skew, 20, {
            let roleid: Oid = kani::any();
            let objoid: Oid = kani::any();
            arm_catalog();
            arm_role_seams(roleid);
            unsafe {
                // no superuser/system shortcuts; force the aclmask walk
                kani::assume(!R_CAT_ACL_ISNULL && R_CAT_NACL > 0 && R_CAT_FOUND);
                for i in 0..2 {
                    R_SUPER_ANS[i] = false;
                    pgq_super_ans[i] = 0;
                }
                R_SUPER_DEFAULT = false;
                pgq_super_default = 0;
                for i in 0..8 {
                    R_MEMB_ANS[i] = false;
                    pgq_memb_ans[i] = 1; // SKEW
                }
                R_MEMB_DEFAULT = false;
                pgq_memb_default = 1; // SKEW
            }

            const LIT: &[u8] = b"SELECT";
            const F: PGFunction = builtin(1925);
            let mut img = [0u8; 4 + 6];
            img[0..4].copy_from_slice(&(((4 + 6) as u32) << 2).to_le_bytes());
            img[4..].copy_from_slice(LIT);
            let (r, isnull) = run_fc::<3>(
                F,
                [
                    Datum::from_oid(roleid),
                    Datum::from_oid(objoid),
                    Datum::from_usize(img.as_ptr() as usize),
                ],
            );
            let mut cp = [0u8; 7];
            cp[..6].copy_from_slice(LIT);
            let (mut cisnull, mut cerr) = (0 as c_int, 0 as c_int);
            let c = unsafe {
                pg_has_table_privilege_id_id(roleid, objoid, cp.as_mut_ptr() as *mut c_char, &mut cisnull, &mut cerr)
            };
            assert_same(r, isnull, c, cisnull, cerr);
        }
    }

    acl_harness! {
        /// Catalog-seam skew: the C side reads a DIFFERENT owner than the
        /// Rust side (concrete witness setup) — MUST FAIL.
        control_catalog_owner_skew, 20, {
            arm_catalog();
            arm_role_seams(100);
            unsafe {
                // concrete, verdict-splitting configuration
                R_CAT_FOUND = true;
                pgq_cat_found = 1;
                R_CAT_ACL_ISNULL = true;
                pgq_cat_acl_isnull = 1;
                R_CAT_RELKIND = b'r';
                pgq_cat_relkind = b'r' as c_int;
                R_CAT_RELNS = 2200;
                pgq_cat_relnamespace = 2200;
                R_CAT_OWNER = 100;
                pgq_cat_owner = 101; // SKEW
                for i in 0..2 {
                    R_SUPER_ANS[i] = false;
                    pgq_super_ans[i] = 0;
                }
                R_SUPER_DEFAULT = false;
                pgq_super_default = 0;
                // membership true exactly on the (roleid==100, role==100) key
                for i in 0..8 {
                    let hit = R_MEMB_ROLE[i] == 100;
                    R_MEMB_ANS[i] = hit;
                    pgq_memb_ans[i] = hit as c_int;
                }
                R_MEMB_DEFAULT = false;
                pgq_memb_default = 0;
            }

            const LIT: &[u8] = b"SELECT";
            const F: PGFunction = builtin(1925);
            let mut img = [0u8; 4 + 6];
            img[0..4].copy_from_slice(&(((4 + 6) as u32) << 2).to_le_bytes());
            img[4..].copy_from_slice(LIT);
            let (r, isnull) = run_fc::<3>(
                F,
                [
                    Datum::from_oid(100),
                    Datum::from_oid(50000),
                    Datum::from_usize(img.as_ptr() as usize),
                ],
            );
            let mut cp = [0u8; 7];
            cp[..6].copy_from_slice(LIT);
            let (mut cisnull, mut cerr) = (0 as c_int, 0 as c_int);
            let c = unsafe {
                pg_has_table_privilege_id_id(100, 50000, cp.as_mut_ptr() as *mut c_char, &mut cisnull, &mut cerr)
            };
            assert_same(r, isnull, c, cisnull, cerr);
        }
    }

    acl_harness! {
        /// Plain-logic negative control: Rust TABLE map vs C SEQUENCE map on
        /// "TRIGGER" (present only in the table map) — MUST FAIL.
        control_priv_map_mismatch, 20, {
            let roleid: Oid = kani::any();
            let objoid: Oid = kani::any();
            arm_catalog();
            arm_role_seams(roleid);
            unsafe {
                kani::assume(R_CAT_FOUND);
                kani::assume(R_CAT_RELKIND == b'S'); // keep C's relkind gate quiet
            }

            const LIT: &[u8] = b"TRIGGER";
            const F: PGFunction = builtin(1925); // has_table_privilege_id_id
            let mut img = [0u8; 4 + 7];
            img[0..4].copy_from_slice(&(((4 + 7) as u32) << 2).to_le_bytes());
            img[4..].copy_from_slice(LIT);
            let (r, isnull) = run_fc::<3>(
                F,
                [
                    Datum::from_oid(roleid),
                    Datum::from_oid(objoid),
                    Datum::from_usize(img.as_ptr() as usize),
                ],
            );
            let mut cp = [0u8; 8];
            cp[..7].copy_from_slice(LIT);
            let (mut cisnull, mut cerr) = (0 as c_int, 0 as c_int);
            let c = unsafe {
                pg_has_sequence_privilege_id_id(roleid, objoid, cp.as_mut_ptr() as *mut c_char, &mut cisnull, &mut cerr)
            };
            assert_same(r, isnull, c, cisnull, cerr);
        }
    }

    acl_harness! {
        /// KNOWN-DIVERGENCE PROBE (expected FAIL — adjudicate, do not
        /// record as a rig defect): C's IsSystemClass includes the
        /// isTempToastNamespace arm; the Rust port has no such arm.  With
        /// the temp-toast session flag raised on the C side only, the
        /// system-catalog write strip fires in C but not in Rust.
        probe_system_class_temp_toast, 20, {
            arm_catalog();
            arm_role_seams(100);
            unsafe {
                pgq_temp_toast = 1; // C-side session state; Rust has no arm
                R_CAT_FOUND = true;
                pgq_cat_found = 1;
                R_CAT_RELKIND = b'r';
                pgq_cat_relkind = b'r' as c_int;
                R_CAT_RELNS = 16384; // NOT pg_toast; oid >= FirstUnpinned
                pgq_cat_relnamespace = 16384;
                R_CAT_ACL_ISNULL = false;
                pgq_cat_acl_isnull = 0;
                R_CAT_NACL = 1;
                pgq_cat_nacl = 1;
                let it = AclItem { ai_grantee: 100, ai_grantor: 100, ai_privs: 1 }; // INSERT
                R_CAT_ACL[0] = it;
                pgq_set_cat_acl(0, it.ai_grantee, it.ai_grantor, it.ai_privs);
                // rebuild image for the Rust decoder
                let size: u32 = (4 + 20 + 16) as u32;
                ACL_IMG[0..4].copy_from_slice(&(size << 2).to_le_bytes());
                ACL_IMG[16..20].copy_from_slice(&1i32.to_le_bytes());
                ACL_IMG[24..28].copy_from_slice(&100u32.to_le_bytes());
                ACL_IMG[28..32].copy_from_slice(&100u32.to_le_bytes());
                ACL_IMG[32..40].copy_from_slice(&1u64.to_le_bytes());
                for i in 0..2 {
                    R_SUPER_ANS[i] = false;
                    pgq_super_ans[i] = 0;
                }
                R_SUPER_DEFAULT = false;
                pgq_super_default = 0;
            }

            const LIT: &[u8] = b"INSERT";
            const F: PGFunction = builtin(1925);
            let mut img = [0u8; 4 + 6];
            img[0..4].copy_from_slice(&(((4 + 6) as u32) << 2).to_le_bytes());
            img[4..].copy_from_slice(LIT);
            let (r, isnull) = run_fc::<3>(
                F,
                [
                    Datum::from_oid(100),
                    Datum::from_oid(50000),
                    Datum::from_usize(img.as_ptr() as usize),
                ],
            );
            let mut cp = [0u8; 7];
            cp[..6].copy_from_slice(LIT);
            let (mut cisnull, mut cerr) = (0 as c_int, 0 as c_int);
            let c = unsafe {
                pg_has_table_privilege_id_id(100, 50000, cp.as_mut_ptr() as *mut c_char, &mut cisnull, &mut cerr)
            };
            assert_same(r, isnull, c, cisnull, cerr);
        }
    }

    // =====================================================================
    // aclinsert / aclremove (oids 1035/1036) — pure error stubs
    // =====================================================================
    acl_harness! {
        /// aclinsert: unconditional feature-not-supported error, verdict +
        /// sqlstate-class parity.
        eq_aclinsert, 4, {
            const F: PGFunction = builtin(1035);
            let (r, _isnull) = run_fc::<2>(F, [Datum::from_i32(0), Datum::from_i32(0)]);
            let mut cerr: c_int = 0;
            unsafe { pg_aclinsert(&mut cerr) };
            match r {
                Ok(_) => assert!(false),
                Err(e) => {
                    let code = errcode_of(&e);
                    core::mem::forget(e);
                    assert!(code == cerr && cerr == 7);
                }
            }
        }
    }

    acl_harness! {
        /// aclremove: as aclinsert.
        eq_aclremove, 4, {
            const F: PGFunction = builtin(1036);
            let (r, _isnull) = run_fc::<2>(F, [Datum::from_i32(0), Datum::from_i32(0)]);
            let mut cerr: c_int = 0;
            unsafe { pg_aclremove(&mut cerr) };
            match r {
                Ok(_) => assert!(false),
                Err(e) => {
                    let code = errcode_of(&e);
                    core::mem::forget(e);
                    assert!(code == cerr && cerr == 7);
                }
            }
        }
    }

    // =====================================================================
    // acldefault_sql (oid 3943) — pure objtype+owner -> default ACL
    // =====================================================================
    acl_harness_mcx! {
        /// acldefault_sql over the full valid objtype-char set (fence:
        /// valid chars; the error arm has its own spot below) and fully
        /// symbolic owner.  The Rust wrapper's varlena image is decoded by
        /// the SHIPPED check_acl_payload/read_acl_item and compared
        /// item-by-item against C's acldefault output.
        eq_acldefault_sql, 12, {
            let owner: Oid = kani::any();
            let c: u8 = kani::any();
            kani::assume(matches!(
                c,
                b'c' | b'r' | b's' | b'd' | b'f' | b'l' | b'L' | b'n' | b'p' | b't' | b'F' | b'S' | b'T'
            ));
            arm_catalog();
            arm_role_seams(owner);

            let ctx = mcx::MemoryContext::new_bump("kani-aclcheck");
            const F: PGFunction = builtin(3943);
            let (r, isnull) = run_fc_mcx::<2>(
                F,
                [Datum::from_i8(c as i8), Datum::from_oid(owner)],
                ctx.mcx(),
            );

            let mut items = [ZERO_ITEM; 8];
            let mut nout: c_int = 0;
            let mut cerr: c_int = 0;
            let ok = unsafe {
                pg_acldefault_sql(c as c_char, owner, items.as_mut_ptr(), &mut nout, &mut cerr)
            };
            assert!(ok == 1 && cerr == 0);
            assert!(!isnull);
            let d = r.unwrap_or_else(|e| {
                core::mem::forget(e);
                panic!("acldefault_sql errored on a valid objtype");
            });
            // decode the returned aclitem[] varlena with the shipped decoder
            let p = d.as_usize() as *const u8;
            let payload: &[u8] = unsafe {
                let size = (core::ptr::read_unaligned(p as *const u32) >> 2) as usize;
                core::slice::from_raw_parts(p.add(4), size - 4)
            };
            let n = adt_acl::varlena::check_acl_payload(payload).unwrap_or_else(|e| {
                core::mem::forget(e);
                panic!("shipped image failed shipped decode")
            });
            assert!(n as c_int == nout);
            let mut i = 0;
            while i < n {
                let it = adt_acl::varlena::read_acl_item(payload, i);
                assert!(it == items[i]);
                i += 1;
            }
            core::mem::forget(ctx);
        }
    }

    acl_harness_mcx! {
        /// acldefault_sql error arm: invalid objtype char (LITERAL 'x') —
        /// both sides error (internal/elog class).
        // unwind 9 (was 6): arm_role_seams loops 8 iterations; 6 fired the
        // unwinding assertion (artifact FAILED, run lane 2026-07-29)
        spot_acldefault_sql_bad_char, 9, {
            arm_catalog();
            arm_role_seams(1);
            let ctx = mcx::MemoryContext::new_bump("kani-aclcheck");
            const F: PGFunction = builtin(3943);
            let (r, _isnull) = run_fc_mcx::<2>(
                F,
                [Datum::from_i8(b'x' as i8), Datum::from_oid(10)],
                ctx.mcx(),
            );
            let mut items = [ZERO_ITEM; 8];
            let mut nout: c_int = 0;
            let mut cerr: c_int = 0;
            unsafe { pg_acldefault_sql(b'x' as c_char, 10, items.as_mut_ptr(), &mut nout, &mut cerr) };
            match r {
                Ok(_) => assert!(false),
                Err(e) => {
                    let code = errcode_of(&e);
                    core::mem::forget(e);
                    assert!(code == cerr && cerr == 8);
                }
            }
            core::mem::forget(ctx);
        }
    }

    // =====================================================================
    // aclitemin (oid 1031) — parse core (hard-error context; the fc
    // wrapper's cstring unwrap/16-byte result pack is the proved
    // arg_aclitem shape from state-seam-probe)
    // =====================================================================
    const IN_CAP: usize = 8;

    acl_harness! {
        /// aclitemin over ALL NUL-free byte strings len<=8: Ok-arm item
        /// parity + Err-arm sqlstate-class parity, role lookups through the
        /// call-indexed name oracle.  (getid/aclparse/priv-char loop, the
        /// "public"/keyword logic and the grantor default all in-theorem;
        /// grantor-default WARNING emission out of proof.)
        eq_aclitemin_len8, 12, {
            let mut s = [0u8; IN_CAP + 1];
            let len: usize = kani::any();
            kani::assume(len <= IN_CAP);
            for i in 0..IN_CAP {
                let c: u8 = kani::any();
                kani::assume(c >= 1);
                if i < len {
                    s[i] = c;
                }
            }
            arm_catalog();
            arm_role_seams(kani::any());

            let r = adt_acl::aclitemin(&s[..len], None);

            let mut out = ZERO_ITEM;
            let mut cerr: c_int = 0;
            let ok = unsafe { pg_aclitemin(s.as_ptr() as *const c_char, &mut out, &mut cerr) };
            match r {
                Ok(Some(item)) => {
                    assert!(cerr == 0 && ok == 1);
                    assert!(item == out);
                }
                Ok(None) => assert!(false), // soft path unreachable (no escontext)
                Err(e) => {
                    let code = errcode_of(&e);
                    core::mem::forget(e);
                    assert!(cerr == code);
                }
            }
        }
    }

    acl_harness! {
        /// aclitemin regime covers within len<=8: Ok arm, syntax-error arm,
        /// undefined-role arm all reachable (vacuity insurance for the
        /// fences above).
        cover_aclitemin_regimes, 12, {
            let mut s = [0u8; IN_CAP + 1];
            let len: usize = kani::any();
            kani::assume(len <= IN_CAP);
            for i in 0..IN_CAP {
                let c: u8 = kani::any();
                kani::assume(c >= 1);
                if i < len {
                    s[i] = c;
                }
            }
            arm_catalog();
            arm_role_seams(kani::any());
            let r = adt_acl::aclitemin(&s[..len], None);
            match r {
                Ok(Some(item)) => {
                    kani::cover!(true, "accept arm reachable");
                    kani::cover!(item.ai_grantee == 0, "public grantee reachable");
                    kani::cover!(item.ai_grantor == 10, "grantor default reachable");
                    core::mem::forget(item);
                }
                Ok(None) => {}
                Err(e) => {
                    kani::cover!(errcode_of(&e) == 6, "syntax error reachable");
                    kani::cover!(errcode_of(&e) == 2, "undefined role reachable");
                    core::mem::forget(e);
                }
            }
        }
    }

    acl_harness! {
        /// LITERAL long-input spots for aclitemin (quoted identifier,
        /// keyword form, explicit grantor) — out of the len<=8 band.
        spot_aclitemin_literals, 40, {
            arm_catalog();
            arm_role_seams(kani::any());
            unsafe {
                // both lookups found with symbolic oids
                kani::assume(R_ROLE_FOUND[0] && R_ROLE_FOUND[1]);
            }
            const LIT: &[u8] = b"user \"a\"\"b\"=rw*dD/grantor";
            let mut s = [0u8; 26];
            s[..25].copy_from_slice(LIT);
            let r = adt_acl::aclitemin(&s[..25], None);
            let mut out = ZERO_ITEM;
            let mut cerr: c_int = 0;
            let ok = unsafe { pg_aclitemin(s.as_ptr() as *const c_char, &mut out, &mut cerr) };
            match r {
                Ok(Some(item)) => {
                    assert!(cerr == 0 && ok == 1);
                    assert!(item == out);
                }
                Ok(None) => assert!(false),
                Err(e) => {
                    let code = errcode_of(&e);
                    core::mem::forget(e);
                    assert!(cerr == code);
                }
            }
        }
    }

    // =====================================================================
    // aclitemout (oid 1032) — format core adt_acl::aclitemout (the fc
    // wrapper's thread-local scratch is out of proof); role names through
    // the oid->name oracle (seam 7).
    // =====================================================================

    /// Arm seam 7 with two concrete-name entries over symbolic oids.
    fn arm_rname(names: [&[u8]; 2]) {
        unsafe {
            for i in 0..2 {
                let f: bool = kani::any();
                let o: Oid = kani::any();
                R_RNAME_FOUND[i] = f;
                R_RNAME_OID[i] = o;
                pgq_rname_found[i] = f as c_int;
                pgq_rname_oid[i] = o;
                R_RNAME_NAME[i] = [0; 64];
                for (j, &b) in names[i].iter().enumerate() {
                    R_RNAME_NAME[i][j] = b;
                    pgq_rname_name[i][j] = b as c_char;
                }
                for j in names[i].len()..64 {
                    pgq_rname_name[i][j] = 0;
                }
            }
            R_RNAME_LATCH = 0;
        }
    }

    acl_harness_mcx! {
        /// aclitemout with fully symbolic privs/goptions and symbolic
        /// grantee/grantor routed through a 2-slot oid->name oracle with
        /// CONCRETE names (one quote-needing) — full output-image parity.
        /// Numeric-fallback arms are fenced to found roles here (the
        /// sprintf model is exercised in the spot below).  Data-dependent
        /// output length: expect the CNF-width cost law; see runqueue.
        eq_aclitemout_named, 20, {
            arm_catalog();
            arm_role_seams(kani::any());
            arm_rname([b"r1", b"a\"b"]);
            let item = AclItem {
                ai_grantee: kani::any(),
                ai_grantor: kani::any(),
                ai_privs: kani::any(),
            };
            unsafe {
                // fence: both queried roles resolve (numeric arm spotted below)
                kani::assume(
                    (item.ai_grantee == 0
                        || (item.ai_grantee == R_RNAME_OID[0] && R_RNAME_FOUND[0])
                        || (item.ai_grantee == R_RNAME_OID[1] && R_RNAME_FOUND[1]))
                        && ((item.ai_grantor == R_RNAME_OID[0] && R_RNAME_FOUND[0])
                            || (item.ai_grantor == R_RNAME_OID[1] && R_RNAME_FOUND[1])),
                );
            }

            let ctx = mcx::MemoryContext::new_bump("kani-aclcheck");
            let r = adt_acl::aclitemout(ctx.mcx(), &item);

            let mut cout = [0u8; 200];
            let mut cerr: c_int = 0;
            let clen =
                unsafe { pg_aclitemout(&item, cout.as_mut_ptr() as *mut c_char, &mut cerr) };
            let out = r.unwrap_or_else(|e| {
                core::mem::forget(e);
                panic!("aclitemout errored")
            });
            // shipped core appends a trailing NUL
            assert!(out.len() == clen as usize + 1);
            let mut i = 0;
            while i < clen as usize {
                assert!(out[i] == cout[i]);
                i += 1;
            }
            assert!(out[clen as usize] == 0);
            core::mem::forget(out);
            core::mem::forget(ctx);
        }
    }

    acl_harness_mcx! {
        /// aclitemout numeric-fallback spot: roles NOT found -> both sides
        /// render the oid in decimal (C side via the documented sprintf %u
        /// model), LITERAL privs.
        // unwind 65 (was 20): arm_rname's pgq-side zero-fill loop runs
        // 64-len iterations; 20 fired the unwinding assertion (artifact
        // FAILED, run lane 2026-07-29)
        spot_aclitemout_numeric, 65, {
            arm_catalog();
            arm_role_seams(kani::any());
            arm_rname([b"r1", b"a\"b"]);
            unsafe {
                for i in 0..2 {
                    R_RNAME_FOUND[i] = false;
                    pgq_rname_found[i] = 0;
                }
            }
            let item = AclItem { ai_grantee: 4294967295, ai_grantor: 305419896, ai_privs: (1u64 << 1) | (1u64 << 33) };

            let ctx = mcx::MemoryContext::new_bump("kani-aclcheck");
            let r = adt_acl::aclitemout(ctx.mcx(), &item);
            let mut cout = [0u8; 200];
            let mut cerr: c_int = 0;
            let clen =
                unsafe { pg_aclitemout(&item, cout.as_mut_ptr() as *mut c_char, &mut cerr) };
            let out = r.unwrap_or_else(|e| {
                core::mem::forget(e);
                panic!("aclitemout errored")
            });
            assert!(out.len() == clen as usize + 1);
            let mut i = 0;
            while i < clen as usize {
                assert!(out[i] == cout[i]);
                i += 1;
            }
            core::mem::forget(out);
            core::mem::forget(ctx);
        }
    }
}
