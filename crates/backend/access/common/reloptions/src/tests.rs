use super::*;
use ::mcx::MemoryContext;
use ::types_nodes::{Node, NodeList};

fn def<'mcx>(
    mcx: Mcx<'mcx>,
    ns: Option<&'mcx str>,
    name: &'mcx str,
    arg: Option<&'mcx str>,
) -> Node<'mcx> {
    let arg = arg.map(|v| Node::mk(mcx, ::types_nodes::String { sval: v }).unwrap());
    Node::mk(
        mcx,
        DefElem {
            defnamespace: ns,
            defname: Some(name),
            arg,
            defaction: ::types_nodes::parsenodes::DefElemAction::DEFELEM_UNSPEC,
            location: -1,
        },
    )
    .unwrap()
}

fn texts_of(image: &[u8]) -> Vec<String> {
    let cx = MemoryContext::new("t");
    let out: Vec<String> =
        option_text_strs(cx.mcx(), image).unwrap().iter().map(|s| s.to_string()).collect();
    out
}

#[test]
fn transform_builds_name_value_texts() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let list = NodeList::make3(
        mcx,
        def(mcx, None, "fillfactor", Some("30")),
        def(mcx, None, "autovacuum_enabled", Some("false")),
        def(mcx, Some("toast"), "vacuum_truncate", Some("false")),
    )
    .unwrap();
    let img = transformRelOptions(mcx, None, &list, None, HEAP_RELOPT_NAMESPACES, true, false)
        .unwrap()
        .unwrap();
    assert_eq!(texts_of(&img), ["fillfactor=30", "autovacuum_enabled=false"]);
    let img =
        transformRelOptions(mcx, None, &list, Some("toast"), HEAP_RELOPT_NAMESPACES, true, false)
            .unwrap()
            .unwrap();
    assert_eq!(texts_of(&img), ["vacuum_truncate=false"]);
}

#[test]
fn transform_replaces_and_resets() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let list = NodeList::make1(mcx, def(mcx, None, "fillfactor", Some("30"))).unwrap();
    let old = transformRelOptions(mcx, None, &list, None, HEAP_RELOPT_NAMESPACES, true, false)
        .unwrap()
        .unwrap();
    let list2 = NodeList::make2(
        mcx,
        def(mcx, None, "fillfactor", Some("31")),
        def(mcx, None, "autovacuum_enabled", None),
    )
    .unwrap();
    let img =
        transformRelOptions(mcx, Some(&old), &list2, None, HEAP_RELOPT_NAMESPACES, true, false)
            .unwrap()
            .unwrap();
    assert_eq!(texts_of(&img), ["fillfactor=31", "autovacuum_enabled=true"]);

    let reset = NodeList::make1(mcx, def(mcx, None, "fillfactor", None)).unwrap();
    let img2 =
        transformRelOptions(mcx, Some(&img), &reset, None, HEAP_RELOPT_NAMESPACES, true, true)
            .unwrap()
            .unwrap();
    assert_eq!(texts_of(&img2), ["autovacuum_enabled=true"]);

    let reset_all = NodeList::make1(mcx, def(mcx, None, "autovacuum_enabled", None)).unwrap();
    let none =
        transformRelOptions(mcx, Some(&img2), &reset_all, None, HEAP_RELOPT_NAMESPACES, true, true)
            .unwrap();
    assert!(none.is_none());

    let bad_reset = NodeList::make1(mcx, def(mcx, None, "fillfactor", Some("12"))).unwrap();
    let err = transformRelOptions(mcx, None, &bad_reset, None, HEAP_RELOPT_NAMESPACES, true, true)
        .unwrap_err();
    assert_eq!(err.message(), "RESET must not include values for parameters");
}

#[test]
fn transform_expands_short_header_old_image() {
    // heap_form_tuple stores small arrays with a 1-byte header; the merge
    // paths must see them as C's DatumGetArrayTypeP would.
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let list = NodeList::make2(
        mcx,
        def(mcx, None, "n_distinct", Some("100")),
        def(mcx, None, "n_distinct_inherited", Some("5")),
    )
    .unwrap();
    let old = transformRelOptions(mcx, None, &list, None, &[], false, false).unwrap().unwrap();
    assert!(old.len() - 4 + 1 <= 0x7F, "test image must be short-able");
    let mut short: Vec<u8> = Vec::new();
    short.push((((old.len() - 4 + 1) as u8) << 1) | 0x01);
    short.extend_from_slice(&old[4..]);

    let reset = NodeList::make1(mcx, def(mcx, None, "n_distinct", None)).unwrap();
    let img = transformRelOptions(mcx, Some(&short), &reset, None, &[], false, true)
        .unwrap()
        .unwrap();
    assert_eq!(texts_of(&img), ["n_distinct_inherited=5"]);

    let opts = attribute_reloptions(mcx, Some(&short), true).unwrap().unwrap();
    assert_eq!(opts.n_distinct, 100.0);
    assert_eq!(opts.n_distinct_inherited, 5.0);
}

fn parse_heap_err(mcx: Mcx<'_>, defs: &NodeList<'_>) -> (String, Option<String>) {
    let res = transformRelOptions(mcx, None, defs, None, HEAP_RELOPT_NAMESPACES, true, false)
        .and_then(|img| heap_reloptions(mcx, RELKIND_RELATION, img.as_deref(), true));
    let e = res.unwrap_err();
    let out = (e.message().to_string(), e.detail().map(|d| d.to_string()));
    out
}

#[test]
fn validation_error_texts_match_c() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();

    let l = NodeList::make1(mcx, def(mcx, None, "fillfactor", Some("2"))).unwrap();
    let (m, d) = parse_heap_err(mcx, &l);
    assert_eq!(m, "value 2 out of bounds for option \"fillfactor\"");
    assert_eq!(d.as_deref(), Some("Valid values are between \"10\" and \"100\"."));

    let l = NodeList::make1(
        mcx,
        def(mcx, None, "autovacuum_analyze_scale_factor", Some("110.0")),
    )
    .unwrap();
    let (m, d) = parse_heap_err(mcx, &l);
    assert_eq!(m, "value 110.0 out of bounds for option \"autovacuum_analyze_scale_factor\"");
    assert_eq!(d.as_deref(), Some("Valid values are between \"0.000000\" and \"100.000000\"."));

    let l = NodeList::make1(mcx, def(mcx, None, "not_existing_option", Some("2"))).unwrap();
    let (m, _) = parse_heap_err(mcx, &l);
    assert_eq!(m, "unrecognized parameter \"not_existing_option\"");

    let l = NodeList::make1(
        mcx,
        def(mcx, Some("not_existing_namespace"), "fillfactor", Some("2")),
    )
    .unwrap();
    let (m, _) = parse_heap_err(mcx, &l);
    assert_eq!(m, "unrecognized parameter namespace \"not_existing_namespace\"");

    let l = NodeList::make1(mcx, def(mcx, None, "fillfactor", Some("string"))).unwrap();
    let (m, _) = parse_heap_err(mcx, &l);
    assert_eq!(m, "invalid value for integer option \"fillfactor\": string");

    let l = NodeList::make1(mcx, def(mcx, None, "autovacuum_enabled", Some("12"))).unwrap();
    let (m, _) = parse_heap_err(mcx, &l);
    assert_eq!(m, "invalid value for boolean option \"autovacuum_enabled\": 12");

    let l = NodeList::make1(
        mcx,
        def(mcx, None, "autovacuum_analyze_scale_factor", Some("string")),
    )
    .unwrap();
    let (m, _) = parse_heap_err(mcx, &l);
    assert_eq!(
        m,
        "invalid value for floating point option \"autovacuum_analyze_scale_factor\": string"
    );

    let l = NodeList::make2(
        mcx,
        def(mcx, None, "fillfactor", Some("30")),
        def(mcx, None, "fillfactor", Some("40")),
    )
    .unwrap();
    let (m, _) = parse_heap_err(mcx, &l);
    assert_eq!(m, "parameter \"fillfactor\" specified more than once");

    // Name-only non-boolean: "fillfactor" flattens to fillfactor=true.
    let l = NodeList::make1(mcx, def(mcx, None, "fillfactor", None)).unwrap();
    let (m, _) = parse_heap_err(mcx, &l);
    assert_eq!(m, "invalid value for integer option \"fillfactor\": true");

    // -30.1 parses via the strtod leg, rint()s to -30, then bounds-fails
    // with the original string in the message.
    let l = NodeList::make1(mcx, def(mcx, None, "fillfactor", Some("-30.1"))).unwrap();
    let (m, _) = parse_heap_err(mcx, &l);
    assert_eq!(m, "value -30.1 out of bounds for option \"fillfactor\"");
}

#[test]
fn heap_parse_fills_struct_and_toast_adjusts() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let l = NodeList::make3(
        mcx,
        def(mcx, None, "fillfactor", Some("30")),
        def(mcx, None, "autovacuum_enabled", Some("false")),
        def(mcx, None, "autovacuum_analyze_scale_factor", Some("0.2")),
    )
    .unwrap();
    let img = transformRelOptions(mcx, None, &l, None, HEAP_RELOPT_NAMESPACES, true, false)
        .unwrap()
        .unwrap();
    let o = heap_reloptions(mcx, RELKIND_RELATION, Some(&img), true).unwrap().unwrap();
    assert_eq!(o.fillfactor, 30);
    assert!(!o.autovacuum.enabled);
    assert_eq!(o.autovacuum.analyze_scale_factor, 0.2);
    assert_eq!(o.toast_tuple_target, 2032);
    assert!(o.vacuum_truncate && !o.vacuum_truncate_set);

    let l = NodeList::make1(mcx, def(mcx, None, "autovacuum_vacuum_cost_delay", Some("23")))
        .unwrap();
    let img = transformRelOptions(mcx, None, &l, None, HEAP_RELOPT_NAMESPACES, true, false)
        .unwrap()
        .unwrap();
    let o = heap_reloptions(mcx, RELKIND_TOASTVALUE, Some(&img), true).unwrap().unwrap();
    assert_eq!(o.fillfactor, 100);
    assert_eq!(o.autovacuum.vacuum_cost_delay, 23.0);
    assert_eq!(o.autovacuum.analyze_threshold, -1);
    assert_eq!(o.autovacuum.analyze_scale_factor, -1.0);

    let l = NodeList::make1(mcx, def(mcx, None, "vacuum_truncate", Some("false"))).unwrap();
    let img = transformRelOptions(mcx, None, &l, None, HEAP_RELOPT_NAMESPACES, true, false)
        .unwrap()
        .unwrap();
    let o = heap_reloptions(mcx, RELKIND_RELATION, Some(&img), true).unwrap().unwrap();
    assert!(!o.vacuum_truncate && o.vacuum_truncate_set);
}

#[test]
fn index_am_parses() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let l = NodeList::make2(
        mcx,
        def(mcx, None, "fillfactor", Some("30")),
        def(mcx, None, "deduplicate_items", Some("off")),
    )
    .unwrap();
    let img = transformRelOptions(mcx, None, &l, None, &[], false, false).unwrap().unwrap();
    let o = index_reloptions(mcx, BTREE_AM_OID, Some(&img), true).unwrap().unwrap();
    let bt = o.btree().unwrap();
    assert_eq!(bt.fillfactor, 30);
    assert!(!bt.deduplicate_items);

    let e = index_reloptions(mcx, HASH_AM_OID, Some(&img), true).unwrap_err();
    assert_eq!(e.message(), "unrecognized parameter \"deduplicate_items\"");

    let l = NodeList::make1(mcx, def(mcx, None, "buffering", Some("off"))).unwrap();
    let img = transformRelOptions(mcx, None, &l, None, &[], false, false).unwrap().unwrap();
    let o = index_reloptions(mcx, GIST_AM_OID, Some(&img), true).unwrap().unwrap();
    assert_eq!(
        o.gist().unwrap().buffering_mode,
        GistOptBufferingMode::GIST_OPTION_BUFFERING_OFF
    );
}

#[test]
fn pgrcolumnar_parallel_workers_reloption() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();

    // Accepted and parsed (the CREATE/ALTER SET path).
    let l = NodeList::make1(mcx, def(mcx, None, "parallel_workers", Some("16"))).unwrap();
    let img = transformRelOptions(mcx, None, &l, None, HEAP_RELOPT_NAMESPACES, true, false)
        .unwrap()
        .unwrap();
    let o = pgrcolumnar_reloptions(mcx, Some(&img), true).unwrap().unwrap();
    assert_eq!(o.parallel_workers, 16);

    // Default is unset (-1), matching StdRdOptions.
    let l = NodeList::make1(mcx, def(mcx, None, "codec", Some("lz4"))).unwrap();
    let img = transformRelOptions(mcx, None, &l, None, HEAP_RELOPT_NAMESPACES, true, false)
        .unwrap()
        .unwrap();
    let o = pgrcolumnar_reloptions(mcx, Some(&img), true).unwrap().unwrap();
    assert_eq!(o.parallel_workers, -1);

    // Range-validated like the heap reloption (0..1024).
    let l = NodeList::make1(mcx, def(mcx, None, "parallel_workers", Some("2000"))).unwrap();
    let img = transformRelOptions(mcx, None, &l, None, HEAP_RELOPT_NAMESPACES, true, false)
        .unwrap()
        .unwrap();
    let e = pgrcolumnar_reloptions(mcx, Some(&img), true).unwrap_err();
    assert_eq!(
        e.message(),
        "invalid value for integer option \"parallel_workers\": 2000 (valid: 0..1024)"
    );

    // ALTER TABLE SET lock level comes from the shared name table: SUEL.
    let l = NodeList::make1(mcx, def(mcx, None, "parallel_workers", None)).unwrap();
    assert_eq!(AlterTableGetRelOptionsLockLevel(&l), ShareUpdateExclusiveLock);
}

#[test]
fn lock_levels() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let l = NodeList::make1(mcx, def(mcx, None, "fillfactor", None)).unwrap();
    assert_eq!(AlterTableGetRelOptionsLockLevel(&l), ShareUpdateExclusiveLock);
    let l = NodeList::make2(
        mcx,
        def(mcx, None, "fillfactor", None),
        def(mcx, None, "buffering", None),
    )
    .unwrap();
    assert_eq!(AlterTableGetRelOptionsLockLevel(&l), AccessExclusiveLock);
    assert_eq!(AlterTableGetRelOptionsLockLevel(&NodeList::nil()), AccessExclusiveLock);
}
