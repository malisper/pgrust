use super::*;

fn reset() {
    SPI_STACK.with(|s| {
        let drained: Vec<_> = s.borrow_mut().drain(..).collect();
        for conn in drained {
            crate::teardown_connection(conn);
        }
    });
    crate::sync_connected();
    set_spi_processed(0);
    set_spi_result(0);
    set_spi_tuptable(None);
}

#[test]
fn connect_finish_roundtrip() {
    reset();
    assert_eq!(SPI_connect().unwrap(), SPI_OK_CONNECT);
    assert_eq!(debug_stack_depth(), 1);
    assert_eq!(SPI_finish().unwrap(), SPI_OK_FINISH);
    assert_eq!(debug_stack_depth(), 0);
    assert_eq!(SPI_finish().unwrap(), SPI_ERROR_UNCONNECTED);
}

#[test]
fn nesting_preserves_outer_globals() {
    reset();
    SPI_connect().unwrap();
    set_spi_processed(7);
    set_spi_result(SPI_OK_SELECT);
    SPI_connect().unwrap();
    assert_eq!(SPI_processed(), 0);
    assert_eq!(SPI_result(), 0);
    SPI_finish().unwrap();
    assert_eq!(SPI_processed(), 7);
    assert_eq!(SPI_result(), SPI_OK_SELECT);
    SPI_finish().unwrap();
}

#[test]
fn at_eoxact_pops_leaked_levels() {
    reset();
    SPI_connect().unwrap();
    SPI_connect().unwrap();
    AtEOXact_SPI(false).unwrap();
    assert_eq!(debug_stack_depth(), 0);
    assert_eq!(debug_live_counts(), (0, 0));
}

#[test]
fn at_eosubxact_pops_only_matching_subid() {
    reset();
    SPI_connect().unwrap();
    let cur = xact::GetCurrentSubTransactionId();
    AtEOSubXact_SPI(false, cur + 1).unwrap();
    assert_eq!(debug_stack_depth(), 1);
    AtEOSubXact_SPI(false, cur).unwrap();
    assert_eq!(debug_stack_depth(), 0);
}

// AtEOSubXact_SPI (spi.c:569): a tuple table dropped with its aborted
// subtransaction also clears SPI_tuptable when it is the current result
// (`if (tuptable == SPI_tuptable) SPI_tuptable = NULL;`), not only
// _SPI_current->tuptable — after _SPI_execute_plan's hand-off the table is
// the caller's (SPI_tuptable) and the connection's pointer is already NULL.
#[test]
fn at_eosubxact_abort_clears_handed_off_spi_tuptable() {
    reset();
    SPI_connect().unwrap();
    let cur = xact::GetCurrentSubTransactionId();
    let table = mcx::McxOwned::<tuptable::TuptabTy>::try_new(
        MemoryContext::new("SPI TupTable"),
        |mcx| {
            Ok(TuptabData {
                tupdesc: tupdesc::CreateTemplateTupleDesc(mcx, 0)?,
                vals: mcx::vec_with_capacity_in(mcx, 1)?,
            })
        },
    )
    .unwrap();
    with_current(|c| {
        c.tuptables.push(tuptable::TuptabEntry { id: 77, subid: cur + 1, table });
    });
    // _SPI_execute_plan's tail: SPI_tuptable = my_tuptable;
    // _SPI_current->tuptable = NULL.
    set_spi_tuptable(Some(TuptabHandle(77)));
    with_current(|c| c.tuptable = None);

    AtEOSubXact_SPI(false, cur + 1).unwrap();
    assert_eq!(debug_stack_depth(), 1, "the connection belongs to the outer subxact");
    assert_eq!(debug_live_counts().0, 0, "the subxact's tuple table is gone");
    assert!(
        SPI_tuptable().is_none(),
        "SPI_tuptable still points at the dropped table (C resets it)"
    );
    SPI_finish().unwrap();
}

// C's SPITupleTable is a stable heap pointer: a domain input function or
// PL/pgSQL call reached while a caller walks a result table may connect to
// SPI again (tablefunc crosstab under a plpgsql-checked output domain).
#[test]
fn tuptable_with_allows_nested_spi_connect() {
    reset();
    SPI_connect().unwrap();
    let cur = xact::GetCurrentSubTransactionId();
    let table = mcx::McxOwned::<tuptable::TuptabTy>::try_new(
        MemoryContext::new("SPI TupTable"),
        |mcx| {
            Ok(TuptabData {
                tupdesc: tupdesc::CreateTemplateTupleDesc(mcx, 0)?,
                vals: mcx::vec_with_capacity_in(mcx, 1)?,
            })
        },
    )
    .unwrap();
    with_current(|c| {
        c.tuptables.push(tuptable::TuptabEntry { id: 78, subid: cur, table });
    });
    let n = tuptable_with(TuptabHandle(78), |t| {
        SPI_connect().unwrap();
        let depth = debug_stack_depth();
        SPI_finish().unwrap();
        (t.vals.len(), depth)
    });
    assert_eq!(n, (0, 2));
    SPI_freetuptable(TuptabHandle(78)).unwrap();
    SPI_finish().unwrap();
}

#[test]
fn empty_stack_seam_arms() {
    reset();
    init_seams();
    assert!(!spi_seams::spi_inside_nonatomic_context::call());
    spi_seams::at_eoxact_spi::call(true).unwrap();
    spi_seams::at_eoxact_spi::call(false).unwrap();
    spi_seams::at_eosubxact_spi::call(false, 2).unwrap();
}

#[test]
fn begin_end_call_exec_discipline() {
    reset();
    assert_eq!(_SPI_begin_call(true), SPI_ERROR_UNCONNECTED);
    SPI_connect().unwrap();
    assert_eq!(_SPI_begin_call(true), 0);
    let subid = with_current(|c| c.exec_subid).unwrap();
    assert_eq!(subid, xact::GetCurrentSubTransactionId());
    _SPI_end_call(true);
    let subid = with_current(|c| c.exec_subid).unwrap();
    assert_eq!(subid, types_core::InvalidSubTransactionId);
    SPI_finish().unwrap();
}

// SPI_register_relation / SPI_unregister_relation (spi.c:3297/3331): the
// public ENR surface — unconnected, duplicate, and not-found return codes.
#[test]
fn register_relation_roundtrip() {
    reset();
    let enr = SpiNamedRelation {
        name: "enr_one",
        reliddesc: 12345,
        tupdesc: None,
        enrtype: queryenvironment::ENR_NAMED_TUPLESTORE,
        enrtuples: 3.0,
        reldata: types_portal::TuplestoreHandle::NULL,
    };
    assert_eq!(SPI_register_relation(&enr).unwrap(), SPI_ERROR_UNCONNECTED);
    assert_eq!(SPI_unregister_relation("enr_one").unwrap(), SPI_ERROR_UNCONNECTED);

    SPI_connect().unwrap();
    assert!(current_query_env().is_null(), "no environment before the first registration");
    assert_eq!(SPI_register_relation(&enr).unwrap(), SPI_OK_REL_REGISTER);
    assert!(!current_query_env().is_null());
    assert_eq!(SPI_register_relation(&enr).unwrap(), SPI_ERROR_REL_DUPLICATE);
    let seen = queryenvironment::hold::with_env(current_query_env(), |env| {
        queryenvironment::get_ENR(env, "enr_one").map(|e| (e.md.reliddesc, e.md.enrtuples))
    });
    assert_eq!(seen, Some((12345, 3.0)));
    assert_eq!(SPI_unregister_relation("enr_two").unwrap(), SPI_ERROR_REL_NOT_FOUND);
    assert_eq!(SPI_unregister_relation("enr_one").unwrap(), SPI_OK_REL_UNREGISTER);
    assert_eq!(SPI_unregister_relation("enr_one").unwrap(), SPI_ERROR_REL_NOT_FOUND);
    assert_eq!(SPI_register_relation(&enr).unwrap(), SPI_OK_REL_REGISTER);
    SPI_finish().unwrap();
}

// spi.c:1972 SPI_result_code_string: every documented code by name, and the
// "Unrecognized SPI code %d" fallback (SPI_ERROR_COPY's neighbour -5 is
// unassigned in spi.h).
#[test]
fn result_code_string_matches_spi_c() {
    let named = [
        (SPI_ERROR_CONNECT, "SPI_ERROR_CONNECT"),
        (SPI_ERROR_COPY, "SPI_ERROR_COPY"),
        (SPI_ERROR_OPUNKNOWN, "SPI_ERROR_OPUNKNOWN"),
        (SPI_ERROR_UNCONNECTED, "SPI_ERROR_UNCONNECTED"),
        (SPI_ERROR_ARGUMENT, "SPI_ERROR_ARGUMENT"),
        (SPI_ERROR_PARAM, "SPI_ERROR_PARAM"),
        (SPI_ERROR_TRANSACTION, "SPI_ERROR_TRANSACTION"),
        (SPI_ERROR_NOATTRIBUTE, "SPI_ERROR_NOATTRIBUTE"),
        (SPI_ERROR_NOOUTFUNC, "SPI_ERROR_NOOUTFUNC"),
        (SPI_ERROR_TYPUNKNOWN, "SPI_ERROR_TYPUNKNOWN"),
        (SPI_ERROR_REL_DUPLICATE, "SPI_ERROR_REL_DUPLICATE"),
        (SPI_ERROR_REL_NOT_FOUND, "SPI_ERROR_REL_NOT_FOUND"),
        (SPI_OK_CONNECT, "SPI_OK_CONNECT"),
        (SPI_OK_FINISH, "SPI_OK_FINISH"),
        (SPI_OK_FETCH, "SPI_OK_FETCH"),
        (SPI_OK_UTILITY, "SPI_OK_UTILITY"),
        (SPI_OK_SELECT, "SPI_OK_SELECT"),
        (SPI_OK_SELINTO, "SPI_OK_SELINTO"),
        (SPI_OK_INSERT, "SPI_OK_INSERT"),
        (SPI_OK_DELETE, "SPI_OK_DELETE"),
        (SPI_OK_UPDATE, "SPI_OK_UPDATE"),
        (SPI_OK_CURSOR, "SPI_OK_CURSOR"),
        (SPI_OK_INSERT_RETURNING, "SPI_OK_INSERT_RETURNING"),
        (SPI_OK_DELETE_RETURNING, "SPI_OK_DELETE_RETURNING"),
        (SPI_OK_UPDATE_RETURNING, "SPI_OK_UPDATE_RETURNING"),
        (SPI_OK_REWRITTEN, "SPI_OK_REWRITTEN"),
        (SPI_OK_REL_REGISTER, "SPI_OK_REL_REGISTER"),
        (SPI_OK_REL_UNREGISTER, "SPI_OK_REL_UNREGISTER"),
        (SPI_OK_TD_REGISTER, "SPI_OK_TD_REGISTER"),
        (SPI_OK_MERGE, "SPI_OK_MERGE"),
        (SPI_OK_MERGE_RETURNING, "SPI_OK_MERGE_RETURNING"),
    ];
    for (code, name) in named {
        assert_eq!(SPI_result_code_string(code), name, "code {code}");
    }
    assert_eq!(SPI_result_code_string(0), "Unrecognized SPI code 0");
    assert_eq!(SPI_result_code_string(-5), "Unrecognized SPI code -5");
    assert_eq!(SPI_result_code_string(20), "Unrecognized SPI code 20");
}
