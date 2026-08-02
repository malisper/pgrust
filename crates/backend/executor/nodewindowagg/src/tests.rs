use ::types_nodes::rawnodes::{
    FRAMEOPTION_END_CURRENT_ROW, FRAMEOPTION_RANGE, FRAMEOPTION_START_UNBOUNDED_PRECEDING,
};

use crate::*;

// parsenodes.h: RANGE | START_UNBOUNDED_PRECEDING | END_CURRENT_ROW == 0x422.
#[test]
fn frameoption_defaults_value_matches_c() {
    assert_eq!(
        FRAMEOPTION_DEFAULTS,
        FRAMEOPTION_RANGE | FRAMEOPTION_START_UNBOUNDED_PRECEDING | FRAMEOPTION_END_CURRENT_ROW
    );
    assert_eq!(FRAMEOPTION_DEFAULTS, 0x422);
}

// initialize_peragg fixtures: three fake window aggregates over builtin
// component functions, syscache faked at the seam boundary.
mod peragg {
    use super::*;
    use std::sync::Once;

    const TEXTOID: Oid = 25;
    const VARCHAROID: Oid = 1043;
    const INT4OID: Oid = 23;
    const F_TEXT_LARGER: Oid = 459; // strict, byref
    const F_TEXT_SMALLER: Oid = 458;
    const PG_CATALOG_NSP: Oid = 11;
    // mymax: finalmodify 'r', strict transfn (text_larger), NULL initval.
    const MYMAX: Oid = 90210;
    // mybad: zero-arg, finalmodify 'w' (the user-facing window-fn error arm).
    const MYBAD: Oid = 90211;
    // mydenied: transfn text_smaller, EXECUTE denied to the aggregate owner.
    const MYDENIED: Oid = 90212;
    const AGG_OWNER: Oid = 555;

    static SEAMS: Once = Once::new();

    fn agg_shape(fnoid: Oid) -> syscache_seams::PgAggregateShape {
        syscache_seams::PgAggregateShape {
            aggkind: b'n' as i8,
            aggnumdirectargs: 0,
            aggtransfn: if fnoid == MYDENIED { F_TEXT_SMALLER } else { F_TEXT_LARGER },
            aggfinalfn: 0,
            aggcombinefn: 0,
            aggserialfn: 0,
            aggdeserialfn: 0,
            aggmtransfn: 0,
            aggminvtransfn: 0,
            aggmfinalfn: 0,
            aggfinalextra: false,
            aggmfinalextra: false,
            aggfinalmodify: if fnoid == MYBAD { b'w' as i8 } else { b'r' as i8 },
            aggmfinalmodify: b'r' as i8,
            aggsortop: 0,
            aggtranstype: TEXTOID,
            aggtransspace: 0,
            aggmtranstype: 0,
        }
    }

    fn install_seams() {
        SEAMS.call_once(|| {
            fmgr_core::init_seams();
            syscache_seams::lookup_pg_aggregate_shape::set(|fnoid| {
                Ok(matches!(fnoid, MYMAX | MYBAD | MYDENIED).then(|| agg_shape(fnoid)))
            });
            syscache_seams::pg_aggregate_agginitval::set(|_mcx, fnoid| {
                Ok(matches!(fnoid, MYMAX | MYBAD | MYDENIED).then_some(None))
            });
            syscache_seams::lookup_pg_proc_secdef::set(|fnoid| {
                Ok(
                    matches!(fnoid, MYMAX | MYBAD | MYDENIED).then(|| {
                        syscache_seams::PgProcSecdefShape {
                            proowner: AGG_OWNER,
                            prosecdef: false,
                            proconfig: None,
                        }
                    }),
                )
            });
            syscache_seams::lookup_pg_type_shape::set(|typid| {
                Ok(matches!(typid, TEXTOID | VARCHAROID).then(|| {
                    ::types_tuple::PgTypeShape {
                        typlen: -1,
                        typbyval: false,
                        typalign: b'i' as i8,
                        typstorage: b'x' as i8,
                        typcollation: 100,
                    }
                }))
            });
            // IsBinaryCoercible: varchar -> text is a binary implicit cast.
            syscache_seams::pg_type_base_shape::set(|typid| {
                Ok(matches!(typid, TEXTOID | VARCHAROID | INT4OID).then(|| {
                    syscache_seams::PgTypeBaseShape {
                        typtype: b'b' as i8,
                        typbasetype: 0,
                        typtypmod: -1,
                        typelem: 0,
                        typsubscript: 0,
                    }
                }))
            });
            syscache_seams::lookup_pg_cast_shape::set(|src, tgt| {
                Ok((src == VARCHAROID && tgt == TEXTOID).then_some(
                    syscache_seams::PgCastShape {
                        oid: 10001,
                        castfunc: 0,
                        castcontext: b'i' as i8,
                        castmethod: b'b' as i8,
                    },
                ))
            });
            aclchk_seams::object_aclcheck::set(|_classid, objid, roleid, _mode| {
                Ok(i32::from(roleid == AGG_OWNER && objid == F_TEXT_SMALLER))
            });
            aclchk_seams::aclcheck_error::set(|_aclresult, _objtype, name| {
                Err(Box::new(
                    PgError::error(format!("permission denied for function {name}"))
                        .with_sqlstate(::types_error::ERRCODE_INSUFFICIENT_PRIVILEGE),
                ))
            });
            syscache_seams::pg_proc_proname::set(|fnoid| {
                let name = match fnoid {
                    F_TEXT_LARGER => "text_larger",
                    F_TEXT_SMALLER => "text_smaller",
                    MYBAD => "mybad",
                    _ => return Ok(None),
                };
                let mut nd = ::types_tuple::NameData::default();
                nd.namestrcpy(name);
                Ok(Some(nd))
            });
            // format_procedure support (zero-arg MYBAD only).
            syscache_seams::lookup_pg_proc_signature::set(|mcx, fnoid| {
                Ok((fnoid == MYBAD).then(|| (TEXTOID, PgVec::new_in(mcx))))
            });
            syscache_seams::lookup_pg_proc_name_candidates::set(|mcx, proname| {
                let mut v = PgVec::new_in(mcx);
                if proname == "mybad" {
                    v.push(syscache_seams::PgProcCandidate {
                        oid: MYBAD,
                        pronamespace: PG_CATALOG_NSP,
                        pronargs: 0,
                        pronargdefaults: 0,
                        provariadic: 0,
                        proargtypes: PgVec::new_in(mcx),
                    });
                }
                Ok(v)
            });
            namespace_seams::fetch_search_path::set(|mcx, _implicit| {
                let mut v = PgVec::new_in(mcx);
                v.push(PG_CATALOG_NSP);
                Ok(v)
            });
        });
    }

    fn mk_wfunc<'m>(mcx: ::mcx::Mcx<'m>, fnoid: Oid, argtype: Option<Oid>) -> Node<'m> {
        let mut args = NodeList::nil();
        if let Some(t) = argtype {
            let arg = Node::mk_const(mcx, t, -1, 100, -1, Datum::null(), true, false).unwrap();
            args.lappend(mcx, arg).unwrap();
        }
        Node::mk(
            mcx,
            WindowFunc {
                winfnoid: fnoid,
                wintype: TEXTOID,
                wincollid: 100,
                inputcollid: 100,
                args,
                aggfilter: None,
                runCondition: NodeList::nil(),
                winref: 1,
                winstar: false,
                winagg: true,
                location: -1,
            },
        )
        .unwrap()
    }

    #[allow(clippy::type_complexity)]
    fn run_default<'m>(
        mcx: ::mcx::Mcx<'m>,
        fnoid: Oid,
        argtype: Option<Oid>,
    ) -> PgResult<(PgVec<'m, Oid>, PgVec<'m, NullableDatum>)> {
        let wfunc = mk_wfunc(mcx, fnoid, argtype).as_window_func().unwrap();
        let mut agg_specs_args = PgVec::new_in(mcx);
        let mut trans_init = PgVec::new_in(mcx);
        let mut trans_fnoid = PgVec::new_in(mcx);
        let mut trans_collid = PgVec::new_in(mcx);
        let mut trans_typlen = PgVec::new_in(mcx);
        let mut trans_byval = PgVec::new_in(mcx);
        let mut trans_argtypes = PgVec::new_in(mcx);
        let mut default_final = PgVec::new_in(mcx);
        initialize_peragg_default(
            mcx,
            wfunc,
            &mut agg_specs_args,
            &mut trans_init,
            &mut trans_fnoid,
            &mut trans_collid,
            &mut trans_typlen,
            &mut trans_byval,
            &mut trans_argtypes,
            &mut default_final,
        )?;
        Ok((trans_fnoid, trans_init))
    }

    // nodeWindowAgg.c:3038: strict transfn + NULL initval accepts a
    // binary-coercible (varchar -> text) first input, like C.
    #[test]
    fn strict_null_initval_accepts_binary_coercible_input() {
        install_seams();
        let cx = MemoryContext::new("peragg test");
        let (trans_fnoid, trans_init) = run_default(cx.mcx(), MYMAX, Some(VARCHAROID)).unwrap();
        assert_eq!(trans_fnoid[0], F_TEXT_LARGER);
        assert!(trans_init[0].isnull);
    }

    // ... and rejects an incompatible first input with C's catchable
    // ERRCODE_INVALID_FUNCTION_DEFINITION error.
    #[test]
    fn strict_null_initval_rejects_incompatible_input() {
        install_seams();
        let cx = MemoryContext::new("peragg test");
        let e = run_default(cx.mcx(), MYMAX, Some(INT4OID)).unwrap_err();
        assert_eq!(e.sqlstate(), ::types_error::ERRCODE_INVALID_FUNCTION_DEFINITION);
        assert_eq!(
            e.message(),
            format!("aggregate {MYMAX} needs to have compatible input type and transition type")
        );
    }

    // nodeWindowAgg.c:2957: non-read-only finalfn raises the clean
    // format_procedure error instead of aborting.
    #[test]
    fn non_read_only_finalmodify_is_clean_error() {
        install_seams();
        let cx = MemoryContext::new("peragg test");
        let e = run_default(cx.mcx(), MYBAD, None).unwrap_err();
        assert_eq!(e.sqlstate(), ::types_error::ERRCODE_FEATURE_NOT_SUPPORTED);
        assert_eq!(
            e.message(),
            "aggregate function mybad() does not support use as a window function"
        );
    }

    // nodeWindowAgg.c:2911-2944: component-fn EXECUTE is checked as the
    // aggregate owner; denial routes through aclcheck_error with the
    // component function's name.
    #[test]
    fn component_fn_acl_checked_as_aggregate_owner() {
        install_seams();
        let cx = MemoryContext::new("peragg test");
        let e = run_default(cx.mcx(), MYDENIED, Some(VARCHAROID)).unwrap_err();
        assert_eq!(e.sqlstate(), ::types_error::ERRCODE_INSUFFICIENT_PRIVILEGE);
        assert_eq!(e.message(), "permission denied for function text_smaller");
    }

    // The framed lane shares the same three ported checks; drive the happy
    // path through it (non-moving generic kernel over text_larger).
    #[test]
    fn framed_lane_accepts_binary_coercible_input() {
        install_seams();
        let cx = MemoryContext::new("peragg test");
        let mcx = cx.mcx();
        let wnode = mk_wfunc(mcx, MYMAX, Some(VARCHAROID));
        let wfunc = wnode.as_window_func().unwrap();
        let shared =
            make_agg_state_node(mcx, mcx.context().new_child_bump("test aggs")).unwrap();
        let pa = initialize_peragg_framed(
            mcx,
            wnode,
            wfunc,
            FRAMEOPTION_START_UNBOUNDED_PRECEDING,
            0,
            ::execexpr::ParamBind::NONE,
            shared,
            None,
        )
        .unwrap();
        assert!(pa.fn_strict);
        assert!(!pa.has_inverse);
        assert!(pa.init_value.isnull);
    }
}
