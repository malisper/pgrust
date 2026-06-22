// ===========================================================================
// ParseFuncOrColumn (parse_func.c:90) and func_get_detail (parse_func.c:1395)
//
// Included from lib.rs (shares its imports). The big dispatcher and the catalog
// lookup live here to keep lib.rs's leaf helpers readable.
// ===========================================================================

/// `func_get_detail`'s result bundle (the C out-parameters).
struct FuncDetail<'mcx> {
    fdresult: FuncDetailCode,
    funcid: Oid,
    rettype: Oid,
    retset: bool,
    nvargs: i32,
    vatype: Oid,
    true_typeids: Vec<Oid>,
    argdefaults: PgVec<'mcx, types_nodes::nodes::NodePtr<'mcx>>,
    /// The chosen candidate's `argnumbers` (named/mixed notation only). C writes
    /// these into the call's `NamedArgExpr` nodes inside `func_get_detail`; here
    /// the caller applies them to its owned `fargs` via
    /// [`apply_named_arg_positions`].
    argnumbers: Option<Vec<i32>>,
}

/// Port target: `ParseFuncOrColumn` (parse_func.c:90).
///
/// Parse a function call (or, when `fn_` is `None`, column-projection syntax).
/// `funcname` is the qualified name (`String` components); `fargs` is the
/// already-transformed argument list (consumed and rebuilt). Returns the
/// produced node, or `None` on failure when `fn_` is `None` (column syntax).
pub fn ParseFuncOrColumn<'mcx>(
    pstate: &mut ParseState<'mcx>,
    funcname: &[PgString<'_>],
    mut fargs: Vec<Expr<'static>>,
    last_srf: Option<&Expr<'_>>,
    fn_: Option<&FuncCall<'mcx>>,
    proc_call: bool,
    location: i32,
) -> PgResult<Option<Expr<'static>>> {
    let mcx = pstate_mcx(pstate);

    let is_column = fn_.is_none();
    let agg_order_len = fn_.map(|f| f.agg_order.len()).unwrap_or(0);
    let mut agg_filter: Option<Expr<'static>> = None;
    let over_present = fn_.map(|f| f.over.is_some()).unwrap_or(false);
    let agg_within_group = fn_.map(|f| f.agg_within_group).unwrap_or(false);
    let agg_star = fn_.map(|f| f.agg_star).unwrap_or(false);
    let agg_distinct = fn_.map(|f| f.agg_distinct).unwrap_or(false);
    let mut func_variadic = fn_.map(|f| f.func_variadic).unwrap_or(false);
    let funcformat: CoercionForm = fn_.map(|f| f.funcformat).unwrap_or(COERCE_EXPLICIT_CALL);

    let mut aggkind: i8 = 0;

    // If there's an aggregate filter, transform it using transformWhereClause.
    if let Some(f) = fn_ {
        if let Some(af) = f.agg_filter.as_deref() {
            agg_filter = transform_where_clause_filter(pstate, af)?;
        }
    }

    // Most of the rest of the parser assumes functions don't have more than
    // FUNC_MAX_ARGS parameters.
    if fargs.len() as i32 > FUNC_MAX_ARGS as i32 {
        return Err(too_many_arguments_error(Some(pstate), location)?);
    }

    // Extract arg type info in preparation for function lookup.
    //
    // If any arguments are Param markers of type VOID, discard them (the JDBC
    // OUT-param hack), unless column syntax or WITHIN GROUP.
    let mut actual_arg_types: Vec<Oid> = Vec::new();
    {
        let mut i: usize = 0;
        while i < fargs.len() {
            let argtype = exprType(Some(&fargs[i]))?;

            if argtype == VOIDOID
                && fargs[i].is_param()
                && !is_column
                && !agg_within_group
            {
                // foreach_delete_current: drop the i-th cell, keep iterating.
                fargs.remove(i);
                continue;
            }

            actual_arg_types.push(argtype);
            i += 1;
        }
    }
    let nargs = actual_arg_types.len() as i32;

    // Check for named arguments; if any, build a list of names. Mixed notation
    // is allowed but only with all named parameters after all the unnamed ones.
    let mut argnames: Vec<PgString<'mcx>> = Vec::new();
    for arg in &fargs {
        if let Some(na) = arg.as_namedargexpr() {
            let name = na.name.clone().unwrap_or_default();
            // Reject duplicate arg names.
            if argnames.iter().any(|n| n.as_str() == name) {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg(format!("argument name \"{name}\" used more than once"))
                    .errposition(parser_errposition(Some(pstate), na.location))
                    .into_error());
            }
            argnames.push(PgString::from_str_in(&name, mcx)?);
        } else if !argnames.is_empty() {
            let loc = exprLocation(Some(arg))?;
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg("positional argument cannot follow named argument")
                .errposition(parser_errposition(Some(pstate), loc))
                .into_error());
        }
    }

    // Decide whether it's legitimate to consider the construct a column
    // projection.
    let could_be_projection = nargs == 1
        && !proc_call
        && agg_order_len == 0
        && agg_filter.is_none()
        && !agg_star
        && !agg_distinct
        && !over_present
        && !func_variadic
        && argnames.is_empty()
        && funcname.len() == 1
        && (actual_arg_types[0] == RECORDOID || ISCOMPLEX(actual_arg_types[0])?);

    // C: `first_arg = linitial(fargs)` — a bare pointer alias, consumed only on
    // the column-projection path, which requires a composite/RECORD argument
    // (`could_be_projection`). We materialize an owned copy only in that case,
    // via the sanctioned deep-copy `Expr::clone_in`: a derived `.clone()` would
    // panic on a `SubLink`/`SubPlan` argument whose embedded sub-tree is
    // context-allocated. For a scalar argument no copy is taken at all, so the
    // panicking `Clone` is never reached. Materialized here (before `fargs` is
    // re-ordered/mutated below) so the borrow ends immediately.
    let first_arg: Option<Expr<'static>> = if could_be_projection {
        match fargs.first() {
            // Deep-copy into the parser arena (`mcx`); the produced function/agg
            // node tree carries the parser arena's `'static` notional lifetime
            // (the convention `make_const`/coerce produce), so erase here.
            Some(e) => Some(e.clone_in(mcx)?.erase_lifetime()),
            None => None,
        }
    } else {
        None
    };

    // If it's column syntax, check for column projection case first.
    if could_be_projection && is_column {
        // Deep-copy again (`clone_in`) rather than move: the disjoint NotFound
        // path below also consumes `first_arg`, and the borrow checker can't see
        // the two are mutually exclusive.
        let fa = match &first_arg {
            Some(e) => Some(e.clone_in(mcx)?.erase_lifetime()),
            None => None,
        };
        let retval = ParseComplexProjection(pstate, funcname[0].as_str(), fa, location)?;
        if retval.is_some() {
            return Ok(retval);
        }
        // If not recognized as a projection, just press on.
    }

    // func_get_detail looks up the function in the catalogs, etc. C brackets it
    // with setup_parser_errposition_callback(&pcbstate, pstate, location): the
    // "function does not exist"/ambiguity ereports below are raised by THIS
    // caller and already carry parser_errposition(location), but func_get_detail's
    // own callees can throw without a position of their own — notably
    // FuncnameGetCandidates' namespace resolution, which raises "schema does not
    // exist" for a bad schema-qualified function name. The ambient
    // error_context_stack is retired (docs/query-lifecycle-raii.md), so we attach
    // the cursor position where func_get_detail returns Err, only when the error
    // has none of its own (C: pcb_error_callback runs errposition() and errstart
    // honors it only if edata->cursorpos == 0).
    let detail = func_get_detail(
        mcx,
        funcname,
        &fargs,
        &argnames,
        nargs,
        &actual_arg_types,
        !func_variadic,
        true,
        proc_call,
        true,
    )
    .map_err(|e| {
        if e.cursor_position().is_some() {
            return e;
        }
        let pos = parser_errposition(Some(pstate), location);
        if pos > 0 {
            e.with_cursor_position(pos)
        } else {
            e
        }
    })?;
    let fdresult = detail.fdresult;
    let funcid = detail.funcid;
    let mut rettype = detail.rettype;
    let retset = detail.retset;
    let nvargs = detail.nvargs;
    let vatype = detail.vatype;
    let declared_arg_types = detail.true_typeids.clone();
    let argdefaults = detail.argdefaults;

    // Apply the chosen candidate's argnumbers to our owned fargs (the C side
    // writes them into the call's NamedArgExpr nodes inside func_get_detail).
    apply_named_arg_positions(&mut fargs, &detail.argnumbers);

    // Check for various wrong-kind-of-routine cases.

    // If this is a CALL, reject things that aren't procedures.
    if proc_call
        && matches!(
            fdresult,
            FuncDetailCode::Normal
                | FuncDetailCode::Aggregate
                | FuncDetailCode::WindowFunc
                | FuncDetailCode::Coercion
        )
    {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!(
                "{} is not a procedure",
                func_signature_string(funcname, nargs, &argnames, &actual_arg_types)?
            ))
            .errhint("To call a function, use SELECT.")
            .errposition(parser_errposition(Some(pstate), location))
            .into_error());
    }
    // Conversely, if not a CALL, reject procedures.
    if fdresult == FuncDetailCode::Procedure && !proc_call {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!(
                "{} is a procedure",
                func_signature_string(funcname, nargs, &argnames, &actual_arg_types)?
            ))
            .errhint("To call a procedure, use CALL.")
            .errposition(parser_errposition(Some(pstate), location))
            .into_error());
    }

    if matches!(
        fdresult,
        FuncDetailCode::Normal | FuncDetailCode::Procedure | FuncDetailCode::Coercion
    ) {
        // Complain if there was anything indicating it must be an aggregate or
        // window function.
        if agg_star {
            return Err(wrong_object(
                pstate,
                format!(
                    "{}(*) specified, but {} is not an aggregate function",
                    name_list_to_string_str(funcname)?,
                    name_list_to_string_str(funcname)?
                ),
                location,
            )?);
        }
        if agg_distinct {
            return Err(wrong_object(
                pstate,
                format!(
                    "DISTINCT specified, but {} is not an aggregate function",
                    name_list_to_string_str(funcname)?
                ),
                location,
            )?);
        }
        if agg_within_group {
            return Err(wrong_object(
                pstate,
                format!(
                    "WITHIN GROUP specified, but {} is not an aggregate function",
                    name_list_to_string_str(funcname)?
                ),
                location,
            )?);
        }
        if agg_order_len != 0 {
            return Err(wrong_object(
                pstate,
                format!(
                    "ORDER BY specified, but {} is not an aggregate function",
                    name_list_to_string_str(funcname)?
                ),
                location,
            )?);
        }
        if agg_filter.is_some() {
            return Err(wrong_object(
                pstate,
                format!(
                    "FILTER specified, but {} is not an aggregate function",
                    name_list_to_string_str(funcname)?
                ),
                location,
            )?);
        }
        if over_present {
            return Err(wrong_object(
                pstate,
                format!(
                    "OVER specified, but {} is not a window function nor an aggregate function",
                    name_list_to_string_str(funcname)?
                ),
                location,
            )?);
        }
    }

    // fdresult-type-specific processing.
    match fdresult {
        FuncDetailCode::Normal | FuncDetailCode::Procedure => {
            // Nothing special to do for these cases.
        }
        FuncDetailCode::Aggregate => {
            // It's an aggregate; fetch needed info from the pg_aggregate entry.
            let agg = agg_row_by_oid::call(mcx, funcid)?
                .ok_or_else(|| internal_error("cache lookup failed for aggregate"))?;
            aggkind = agg.aggkind;
            let cat_direct_args = agg.aggnumdirectargs;

            // Now check various disallowed cases.
            if AGGKIND_IS_ORDERED_SET(aggkind) {
                if !agg_within_group {
                    return Err(wrong_object(
                        pstate,
                        format!(
                            "WITHIN GROUP is required for ordered-set aggregate {}",
                            name_list_to_string_str(funcname)?
                        ),
                        location,
                    )?);
                }
                if over_present {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                        .errmsg(format!(
                            "OVER is not supported for ordered-set aggregate {}",
                            name_list_to_string_str(funcname)?
                        ))
                        .errposition(parser_errposition(Some(pstate), location))
                        .into_error());
                }
                // gram.y rejects DISTINCT + WITHIN GROUP / VARIADIC + WITHIN GROUP.
                debug_assert!(!agg_distinct);
                debug_assert!(!func_variadic);

                let num_aggregated_args = agg_order_len as i32;
                let num_direct_args = nargs - num_aggregated_args;
                debug_assert!(num_direct_args >= 0);

                if !OidIsValid(vatype) {
                    // Test is simple if aggregate isn't variadic.
                    if num_direct_args != cat_direct_args {
                        return Err(ordered_set_direct_args_error(
                            pstate,
                            funcname,
                            nargs,
                            &argnames,
                            &actual_arg_types,
                            cat_direct_args,
                            num_direct_args,
                            location,
                        )?);
                    }
                } else {
                    // Reverse-engineer pronargs from func_get_detail info.
                    let mut pronargs = nargs;
                    if nvargs > 1 {
                        pronargs -= nvargs - 1;
                    }
                    if cat_direct_args < pronargs {
                        // VARIADIC isn't part of direct args, so still easy.
                        if num_direct_args != cat_direct_args {
                            return Err(ordered_set_direct_args_error(
                                pstate,
                                funcname,
                                nargs,
                                &argnames,
                                &actual_arg_types,
                                cat_direct_args,
                                num_direct_args,
                                location,
                            )?);
                        }
                    } else if aggkind == AGGKIND_HYPOTHETICAL {
                        if nvargs != 2 * num_aggregated_args {
                            return Err(ereport(ERROR)
                                .errcode(ERRCODE_UNDEFINED_FUNCTION)
                                .errmsg(format!(
                                    "function {} does not exist",
                                    func_signature_string(
                                        funcname,
                                        nargs,
                                        &argnames,
                                        &actual_arg_types
                                    )?
                                ))
                                .errhint(format!(
                                    "To use the hypothetical-set aggregate {}, the number of \
                                     hypothetical direct arguments (here {}) must match the number \
                                     of ordering columns (here {}).",
                                    name_list_to_string_str(funcname)?,
                                    nvargs - num_aggregated_args,
                                    num_aggregated_args
                                ))
                                .errposition(parser_errposition(Some(pstate), location))
                                .into_error());
                        }
                    } else if nvargs <= num_aggregated_args {
                        return Err(ereport(ERROR)
                            .errcode(ERRCODE_UNDEFINED_FUNCTION)
                            .errmsg(format!(
                                "function {} does not exist",
                                func_signature_string(
                                    funcname,
                                    nargs,
                                    &argnames,
                                    &actual_arg_types
                                )?
                            ))
                            .errhint_plural(
                                format!(
                                    "There is an ordered-set aggregate {}, but it requires at \
                                     least {} direct argument.",
                                    name_list_to_string_str(funcname)?,
                                    cat_direct_args
                                ),
                                format!(
                                    "There is an ordered-set aggregate {}, but it requires at \
                                     least {} direct arguments.",
                                    name_list_to_string_str(funcname)?,
                                    cat_direct_args
                                ),
                                cat_direct_args as u64,
                            )
                            .errposition(parser_errposition(Some(pstate), location))
                            .into_error());
                    }
                }

                // Check type matching of hypothetical arguments.
                if aggkind == AGGKIND_HYPOTHETICAL {
                    unify_hypothetical_args(
                        pstate,
                        &mut fargs,
                        num_aggregated_args,
                        &mut actual_arg_types,
                        &declared_arg_types,
                    )?;
                }
            } else {
                // Normal aggregate, so it can't have WITHIN GROUP.
                if agg_within_group {
                    return Err(wrong_object(
                        pstate,
                        format!(
                            "{} is not an ordered-set aggregate, so it cannot have WITHIN GROUP",
                            name_list_to_string_str(funcname)?
                        ),
                        location,
                    )?);
                }
            }
        }
        FuncDetailCode::WindowFunc => {
            // True window functions must be called with a window definition.
            if !over_present {
                return Err(wrong_object(
                    pstate,
                    format!(
                        "window function {} requires an OVER clause",
                        name_list_to_string_str(funcname)?
                    ),
                    location,
                )?);
            }
            // And, per spec, WITHIN GROUP isn't allowed.
            if agg_within_group {
                return Err(wrong_object(
                    pstate,
                    format!(
                        "window function {} cannot have WITHIN GROUP",
                        name_list_to_string_str(funcname)?
                    ),
                    location,
                )?);
            }
        }
        FuncDetailCode::Coercion => {
            // We interpreted it as a type coercion.
            let arg1 = fargs
                .into_iter()
                .next()
                .ok_or_else(|| internal_error("FUNCDETAIL_COERCION requires linitial(fargs)"))?;
            let coerced = coerce_type::call(
                Some(pstate),
                arg1,
                actual_arg_types[0],
                rettype,
                -1,
                CoercionContext::COERCION_EXPLICIT,
                COERCE_EXPLICIT_CALL,
                location,
            )?;
            return Ok(Some(coerced));
        }
        FuncDetailCode::Multiple => {
            // Multiple possible functional matches.
            if is_column {
                return Ok(None);
            }
            if proc_call {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_AMBIGUOUS_FUNCTION)
                    .errmsg(format!(
                        "procedure {} is not unique",
                        func_signature_string(funcname, nargs, &argnames, &actual_arg_types)?
                    ))
                    .errhint(
                        "Could not choose a best candidate procedure. \
                         You might need to add explicit type casts.",
                    )
                    .errposition(parser_errposition(Some(pstate), location))
                    .into_error());
            } else {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_AMBIGUOUS_FUNCTION)
                    .errmsg(format!(
                        "function {} is not unique",
                        func_signature_string(funcname, nargs, &argnames, &actual_arg_types)?
                    ))
                    .errhint(
                        "Could not choose a best candidate function. \
                         You might need to add explicit type casts.",
                    )
                    .errposition(parser_errposition(Some(pstate), location))
                    .into_error());
            }
        }
        FuncDetailCode::NotFound => {
            // Not found as a function.
            if is_column {
                return Ok(None);
            }

            // Check for column projection interpretation, since we didn't before.
            if could_be_projection {
                let retval =
                    ParseComplexProjection(pstate, funcname[0].as_str(), first_arg, location)?;
                if retval.is_some() {
                    return Ok(retval);
                }
            }

            // No function, and no column either.
            if agg_order_len as i32 > 1 && !agg_within_group {
                // agg(x, ORDER BY y,z) ... perhaps misplaced ORDER BY.
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_FUNCTION)
                    .errmsg(format!(
                        "function {} does not exist",
                        func_signature_string(funcname, nargs, &argnames, &actual_arg_types)?
                    ))
                    .errhint(
                        "No aggregate function matches the given name and argument types. \
                         Perhaps you misplaced ORDER BY; ORDER BY must appear after all regular \
                         arguments of the aggregate.",
                    )
                    .errposition(parser_errposition(Some(pstate), location))
                    .into_error());
            } else if proc_call {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_FUNCTION)
                    .errmsg(format!(
                        "procedure {} does not exist",
                        func_signature_string(funcname, nargs, &argnames, &actual_arg_types)?
                    ))
                    .errhint(
                        "No procedure matches the given name and argument types. \
                         You might need to add explicit type casts.",
                    )
                    .errposition(parser_errposition(Some(pstate), location))
                    .into_error());
            } else {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_FUNCTION)
                    .errmsg(format!(
                        "function {} does not exist",
                        func_signature_string(funcname, nargs, &argnames, &actual_arg_types)?
                    ))
                    .errhint(
                        "No function matches the given name and argument types. \
                         You might need to add explicit type casts.",
                    )
                    .errposition(parser_errposition(Some(pstate), location))
                    .into_error());
            }
        }
    }

    // Include default-argument types in actual_arg_types for generic type
    // consistency (but not in the parse node).
    let mut nargsplusdefs = nargs;
    for expr in argdefaults.iter() {
        // probably shouldn't happen ...
        if nargsplusdefs >= FUNC_MAX_ARGS as i32 {
            return Err(too_many_arguments_error(Some(pstate), location)?);
        }
        let e = expr
            .as_expr()
            .ok_or_else(|| internal_error("argdefault is not an expression"))?;
        actual_arg_types.push(exprType(Some(e))?);
        nargsplusdefs += 1;
    }

    // Enforce consistency with polymorphic argument and return types, possibly
    // adjusting return type or declared_arg_types (the cast destination for
    // make_fn_arguments).
    let mut declared_arg_types = declared_arg_types;
    rettype = enforce_generic_type_consistency::call(
        &actual_arg_types,
        &mut declared_arg_types,
        nargsplusdefs,
        rettype,
        false,
    )?;

    // Perform the necessary typecasting of arguments.
    make_fn_arguments(Some(pstate), &mut fargs, &actual_arg_types, &declared_arg_types)?;

    // If the function isn't actually variadic, forget VARIADIC decoration.
    if !OidIsValid(vatype) {
        debug_assert!(nvargs == 0);
        func_variadic = false;
    }

    // If it's a variadic function call, transform the last nvargs arguments into
    // an array --- unless it's an "any" variadic.
    if nvargs > 0 && vatype != ANYOID {
        let non_var_args = nargs - nvargs;
        debug_assert!(non_var_args >= 0);

        // list_copy_tail(fargs, non_var_args) + list_truncate(fargs, non_var_args).
        let vargs: Vec<Expr> = fargs.split_off(non_var_args as usize);

        let element_typeid = exprType(vargs.first())?;
        let array_typeid = get_array_type::call(element_typeid)?.unwrap_or(InvalidOid);
        if !OidIsValid(array_typeid) {
            let loc = expr_location_list(&vargs)?;
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!(
                    "could not find array type for data type {}",
                    format_type_be_owned::call(element_typeid)?
                ))
                .errposition(parser_errposition(Some(pstate), loc))
                .into_error());
        }
        let list_loc = expr_location_list(&vargs)?;

        let newa = ArrayExpr {
            array_typeid,
            // array_collid will be set by parse_collate.c.
            array_collid: InvalidOid,
            element_typeid,
            elements: vargs,
            multidims: false,
            location: list_loc,
        };

        fargs.push(Expr::ArrayExpr(newa));

        // We could not have had VARIADIC marking before ...
        debug_assert!(!func_variadic);
        // ... but now, it's a VARIADIC call.
        func_variadic = true;
    }

    // If an "any" variadic is called with explicit VARIADIC marking, insist the
    // variadic parameter be of some array type.
    if nargs > 0 && vatype == ANYOID && func_variadic {
        let va_arr_typid = actual_arg_types[(nargs - 1) as usize];

        if !OidIsValid(get_base_element_type::call(va_arr_typid)?) {
            let loc = exprLocation(fargs.last())?;
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DATATYPE_MISMATCH)
                .errmsg("VARIADIC argument must be an array")
                .errposition(parser_errposition(Some(pstate), loc))
                .into_error());
        }
    }

    // if it returns a set, check that's OK.
    if retset {
        check_srf_call_placement(pstate, last_srf, location)?;
    }

    // build the appropriate output structure.
    let retval: Expr<'static>;
    if matches!(fdresult, FuncDetailCode::Normal | FuncDetailCode::Procedure) {
        let funcexpr = FuncExpr {
            funcid,
            funcresulttype: rettype,
            funcretset: retset,
            funcvariadic: func_variadic,
            funcformat,
            // funccollid and inputcollid will be set by parse_collate.c.
            funccollid: InvalidOid,
            inputcollid: InvalidOid,
            args: fargs,
            location,
        };
        retval = Expr::FuncExpr(funcexpr);
    } else if fdresult == FuncDetailCode::Aggregate && !over_present {
        // aggregate function.
        //
        // Reject parameterless aggregate without (*) syntax.
        if fargs.is_empty() && !agg_star && !agg_within_group {
            return Err(wrong_object(
                pstate,
                format!(
                    "{}(*) must be used to call a parameterless aggregate function",
                    name_list_to_string_str(funcname)?
                ),
                location,
            )?);
        }

        if retset {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                .errmsg("aggregates cannot return sets")
                .errposition(parser_errposition(Some(pstate), location))
                .into_error());
        }

        // Named arguments are disallowed for aggregates for now.
        if !argnames.is_empty() {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("aggregates cannot use named arguments")
                .errposition(parser_errposition(Some(pstate), location))
                .into_error());
        }

        let aggref = Aggref {
            aggfnoid: funcid,
            aggtype: rettype,
            // aggcollid and inputcollid will be set by parse_collate.c.
            aggcollid: InvalidOid,
            inputcollid: InvalidOid,
            aggtranstype: InvalidOid, // will be set by planner
            // aggargtypes set by transformAggregateCall.
            aggargtypes: Vec::new(),
            // aggdirectargs and args set by transformAggregateCall.
            aggdirectargs: Vec::new(),
            args: Vec::new(),
            // aggorder and aggdistinct set by transformAggregateCall.
            aggorder: Vec::new(),
            aggdistinct: Vec::new(),
            aggfilter: agg_filter.map(Box::new),
            aggstar: agg_star,
            aggvariadic: func_variadic,
            aggkind,
            aggpresorted: false,
            // agglevelsup set by transformAggregateCall.
            agglevelsup: 0,
            aggsplit: types_nodes::nodeagg::AGGSPLIT_SIMPLE, // planner might change this
            aggno: -1, // planner will set aggno/aggtransno
            aggtransno: -1,
            location,
        };

        // parse_agg.c does additional aggregate-specific processing. Move the
        // raw ORDER BY items out of the FuncCall (already owned by `fn_`'s
        // caller; we clone into the call's mcx).
        let aggorder = clone_node_ptr_vec(mcx, fn_.map(|f| &f.agg_order))?;
        let finished =
            transform_aggregate_call::call(pstate, aggref, fargs, aggorder, agg_distinct)?;
        retval = Expr::Aggref(finished);
    } else {
        // window function.
        debug_assert!(over_present);
        debug_assert!(!agg_within_group); // also checked above

        let winagg = fdresult == FuncDetailCode::Aggregate;

        // agg_star is allowed for aggregate functions but distinct isn't.
        if agg_distinct {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("DISTINCT is not implemented for window functions")
                .errposition(parser_errposition(Some(pstate), location))
                .into_error());
        }

        // Reject parameterless aggregate without (*) syntax.
        if winagg && fargs.is_empty() && !agg_star {
            return Err(wrong_object(
                pstate,
                format!(
                    "{}(*) must be used to call a parameterless aggregate function",
                    name_list_to_string_str(funcname)?
                ),
                location,
            )?);
        }

        // ordered aggs not allowed in windows yet.
        if agg_order_len != 0 {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("aggregate ORDER BY is not implemented for window functions")
                .errposition(parser_errposition(Some(pstate), location))
                .into_error());
        }

        // FILTER is not yet supported with true window functions.
        if !winagg && agg_filter.is_some() {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("FILTER is not implemented for non-aggregate window functions")
                .errposition(parser_errposition(Some(pstate), location))
                .into_error());
        }

        // Window functions can't either take or return sets.
        if !p_last_srf_eq(pstate, last_srf)? {
            let loc = node_expr_location(pstate.p_last_srf.as_deref())?;
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("window function calls cannot contain set-returning function calls")
                .errhint(
                    "You might be able to move the set-returning function into a LATERAL FROM item.",
                )
                .errposition(parser_errposition(Some(pstate), loc))
                .into_error());
        }

        if retset {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                .errmsg("window functions cannot return sets")
                .errposition(parser_errposition(Some(pstate), location))
                .into_error());
        }

        let wfunc = WindowFunc {
            winfnoid: funcid,
            wintype: rettype,
            // wincollid and inputcollid will be set by parse_collate.c.
            wincollid: InvalidOid,
            inputcollid: InvalidOid,
            args: fargs,
            aggfilter: agg_filter.map(Box::new),
            runCondition: Vec::new(),
            // winref will be set by transformWindowFuncCall.
            winref: 0,
            winstar: agg_star,
            winagg,
            location,
        };

        // parse_agg.c does additional window-func-specific processing.
        let over = fn_
            .and_then(|f| f.over.as_deref())
            .ok_or_else(|| internal_error("over checked above"))?
            .clone_in(mcx)?;
        let finished = transform_window_func_call::call(pstate, wfunc, over)?;
        retval = Expr::WindowFunc(finished);
    }

    // if it returns a set, remember it for error checks at higher levels.
    if retset {
        set_p_last_srf(pstate, &retval)?;
    }

    Ok(Some(retval))
}

/// Helper: the standard `ERRCODE_WRONG_OBJECT_TYPE` error with a positioned msg.
fn wrong_object(pstate: &ParseState<'_>, msg: String, location: i32) -> PgResult<PgError> {
    Ok(ereport(ERROR)
        .errcode(ERRCODE_WRONG_OBJECT_TYPE)
        .errmsg(msg)
        .errposition(parser_errposition(Some(pstate), location))
        .into_error())
}

/// Helper: the ordered-set "requires N direct argument(s)" undefined-function
/// error shared by the two non-variadic / variadic-easy branches.
fn ordered_set_direct_args_error(
    pstate: &ParseState<'_>,
    funcname: &[PgString<'_>],
    nargs: i32,
    argnames: &[PgString<'_>],
    actual_arg_types: &[Oid],
    cat_direct_args: i32,
    num_direct_args: i32,
    location: i32,
) -> PgResult<PgError> {
    Ok(ereport(ERROR)
        .errcode(ERRCODE_UNDEFINED_FUNCTION)
        .errmsg(format!(
            "function {} does not exist",
            func_signature_string(funcname, nargs, argnames, actual_arg_types)?
        ))
        .errhint_plural(
            format!(
                "There is an ordered-set aggregate {}, but it requires {} direct argument, not {}.",
                name_list_to_string_str(funcname)?,
                cat_direct_args,
                num_direct_args
            ),
            format!(
                "There is an ordered-set aggregate {}, but it requires {} direct arguments, not {}.",
                name_list_to_string_str(funcname)?,
                cat_direct_args,
                num_direct_args
            ),
            cat_direct_args as u64,
        )
        .errposition(parser_errposition(Some(pstate), location))
        .into_error())
}

/// Re-stamp the named-notation argument positions returned by `func_get_detail`
/// onto our owned `fargs`.
fn apply_named_arg_positions(fargs: &mut [Expr<'_>], argnumbers: &Option<Vec<i32>>) {
    let Some(numbers) = argnumbers else {
        return;
    };
    for (i, arg) in fargs.iter_mut().enumerate() {
        if let Some(na) = arg.as_namedargexpr_mut() {
            if let Some(&n) = numbers.get(i) {
                na.argnumber = n;
            }
        }
    }
}

/// `exprLocation((Node *) list)` over a `List *` of exprs (the variadic-array
/// list location). Mirrors `exprLocation`'s list handling: the earliest member
/// location.
fn expr_location_list(list: &[Expr<'_>]) -> PgResult<i32> {
    let mut loc = -1i32;
    for e in list {
        let l = exprLocation(Some(e))?;
        if l < 0 {
            continue;
        }
        if loc < 0 || l < loc {
            loc = l;
        }
    }
    Ok(loc)
}

/// Deep-copy a `List *` of `Node *` into `mcx` (the ORDER BY raw items).
fn clone_node_ptr_vec<'mcx>(
    mcx: Mcx<'mcx>,
    src: Option<&PgVec<'mcx, types_nodes::nodes::NodePtr<'mcx>>>,
) -> PgResult<PgVec<'mcx, types_nodes::nodes::NodePtr<'mcx>>> {
    let mut out = PgVec::new_in(mcx);
    if let Some(src) = src {
        for n in src.iter() {
            out.push(mcx::alloc_in(mcx, n.clone_in(mcx)?)?);
        }
    }
    Ok(out)
}

/// `transformWhereClause(pstate, agg_filter, EXPR_KIND_FILTER, "FILTER")`
/// (parse_clause.c) — transform the aggregate FILTER expression. Reached
/// through the parse_clause seam.
fn transform_where_clause_filter<'mcx>(
    pstate: &mut ParseState<'mcx>,
    af: &Node<'mcx>,
) -> PgResult<Option<Expr<'static>>> {
    let mcx = pstate_mcx(pstate);
    let clause = af.clone_in(mcx)?;
    // The FILTER clause is built in the parser arena (`mcx`); erase to the parser
    // arena's `'static` notional lifetime to match the produced agg/window node.
    Ok(backend_parser_clause_seams::transform_where_clause::call(
        mcx,
        pstate,
        Some(clause),
        ParseExprKind::EXPR_KIND_FILTER,
        "FILTER",
    )?
    .map(|e| e.erase_lifetime()))
}

// ===========================================================================
// func_get_detail (parse_func.c:1395)
// ===========================================================================

/// Port target: `func_get_detail` (parse_func.c:1395). Returns the
/// [`FuncDetail`] bundle of out-parameters.
///
/// `with_argdefaults` corresponds to a non-NULL `argdefaults` out-pointer in C.
fn func_get_detail<'mcx>(
    mcx: Mcx<'mcx>,
    funcname: &[PgString<'_>],
    fargs: &[Expr<'_>],
    fargnames: &[PgString<'_>],
    nargs: i32,
    argtypes: &[Oid],
    expand_variadic: bool,
    expand_defaults: bool,
    include_out_arguments: bool,
    with_argdefaults: bool,
) -> PgResult<FuncDetail<'mcx>> {
    let mut detail = FuncDetail {
        fdresult: FuncDetailCode::NotFound,
        funcid: InvalidOid,
        rettype: InvalidOid,
        retset: false,
        nvargs: 0,
        vatype: InvalidOid,
        true_typeids: Vec::new(),
        argdefaults: PgVec::new_in(mcx),
        argnumbers: None,
    };

    // Get list of possible candidates from namespace search.
    let names: Vec<&str> = funcname.iter().map(|s| s.as_str()).collect();
    let argnames_refs: Vec<&str> = fargnames.iter().map(|s| s.as_str()).collect();
    let raw_candidates = funcname_get_candidates::call(
        mcx,
        &names,
        nargs,
        &argnames_refs,
        expand_variadic,
        expand_defaults,
        include_out_arguments,
        false,
    )?;

    // Quickly check for an exact match to the input datatypes.
    let mut best_candidate: Option<FuncCandidate<'mcx>> = None;
    for cand in raw_candidates.iter() {
        // if nargs==0, argtypes can be null; don't pass that to memcmp.
        if nargs == 0 || oid_slices_eq(argtypes, &cand.args, nargs as usize) {
            best_candidate = Some(clone_candidate(mcx, cand)?);
            break;
        }
    }

    if best_candidate.is_none() {
        // Consider whether this is really a type-coercion request.
        if nargs == 1 && !fargs.is_empty() && fargnames.is_empty() {
            let target_type = func_name_as_type::call(funcname)?;

            if OidIsValid(target_type) {
                let source_type = argtypes[0];
                let arg1 = &fargs[0];
                let iscoercion: bool;

                if source_type == UNKNOWNOID && arg1.is_const() {
                    // always treat typename('literal') as coercion
                    iscoercion = true;
                } else {
                    let (cpathtype, _cfuncid) =
                        find_coercion_pathway_explicit::call(target_type, source_type)?;
                    iscoercion = match cpathtype {
                        CoercionPathType::Relabeltype => true,
                        CoercionPathType::Coerceviaio => {
                            !((source_type == RECORDOID || ISCOMPLEX(source_type)?)
                                && type_category(target_type)? == TYPCATEGORY_STRING)
                        }
                        _ => false,
                    };
                }

                if iscoercion {
                    // Treat it as a type coercion.
                    detail.funcid = InvalidOid;
                    detail.rettype = target_type;
                    detail.retset = false;
                    detail.nvargs = 0;
                    detail.vatype = InvalidOid;
                    detail.true_typeids = argtypes.to_vec();
                    detail.fdresult = FuncDetailCode::Coercion;
                    return Ok(detail);
                }
            }
        }

        // didn't find an exact match, so now try to match up candidates...
        if !raw_candidates.is_empty() {
            let current_candidates =
                func_match_argtypes(mcx, nargs, argtypes, &raw_candidates)?;
            let ncandidates = current_candidates.len();

            // one match only? then run with it...
            if ncandidates == 1 {
                best_candidate = current_candidates.into_iter().next();
            } else if ncandidates > 1 {
                let chosen = func_select_candidate(mcx, nargs, argtypes, &current_candidates)?;
                match chosen {
                    Some(oid) => {
                        best_candidate = current_candidates
                            .into_iter()
                            .find(|c| c.oid == oid);
                    }
                    None => {
                        // ambiguous function call.
                        detail.fdresult = FuncDetailCode::Multiple;
                        return Ok(detail);
                    }
                }
            }
        }
    }

    if let Some(best_candidate) = best_candidate {
        // If the "best candidate" represents multiple equivalently good
        // functions, treat as ambiguous.
        if !OidIsValid(best_candidate.oid) {
            detail.fdresult = FuncDetailCode::Multiple;
            return Ok(detail);
        }

        // Disallow VARIADIC with named args unless the last arg matched.
        if !fargnames.is_empty() && !expand_variadic && nargs > 0 {
            let last = best_candidate
                .argnumbers
                .get((nargs - 1) as usize)
                .copied()
                .unwrap_or(0);
            if last != nargs - 1 {
                detail.fdresult = FuncDetailCode::NotFound;
                return Ok(detail);
            }
        }

        detail.funcid = best_candidate.oid;
        detail.nvargs = best_candidate.nvargs;
        detail.true_typeids = best_candidate.args.to_vec();

        // If processing named args, return the chosen candidate's argnumbers
        // (the caller writes them into the call's NamedArgExpr nodes).
        if !best_candidate.argnumbers.is_empty() {
            detail.argnumbers = Some(best_candidate.argnumbers.to_vec());
        }

        let pform = proc_row_by_oid::call(mcx, best_candidate.oid)?
            .ok_or_else(|| internal_error("cache lookup failed for function"))?;
        detail.rettype = pform.prorettype;
        detail.retset = pform.proretset;
        detail.vatype = pform.provariadic;

        // fetch default args if caller wants 'em.
        if with_argdefaults && best_candidate.ndargs > 0 {
            // shouldn't happen, FuncnameGetCandidates messed up.
            if best_candidate.ndargs > pform.pronargdefaults {
                return Err(internal_error("not enough default arguments"));
            }

            let defaults = proc_argdefaults::call(mcx, best_candidate.oid)?;

            // Delete any unused defaults from the returned list.
            if !best_candidate.argnumbers.is_empty() {
                // Named notation: select the needed default items by argnumber.
                let first = (best_candidate.nargs - best_candidate.ndargs) as usize;
                let mut defargnumbers = BitSet::new();
                for k in 0..best_candidate.ndargs as usize {
                    if let Some(&n) = best_candidate.argnumbers.get(first + k) {
                        defargnumbers.add(n);
                    }
                }
                let mut newdefaults: PgVec<'mcx, types_nodes::nodes::NodePtr<'mcx>> =
                    PgVec::new_in(mcx);
                let mut i = best_candidate.nominalnargs - pform.pronargdefaults;
                for d in defaults {
                    if defargnumbers.contains(i) {
                        newdefaults.push(d);
                    }
                    i += 1;
                }
                debug_assert!(newdefaults.len() as i32 == best_candidate.ndargs);
                detail.argdefaults = newdefaults;
            } else {
                // Positional notation: remove unwanted ones from the front.
                let ndelete = defaults.len() as i32 - best_candidate.ndargs;
                let mut defaults = defaults;
                if ndelete > 0 {
                    defaults.drain(0..ndelete as usize);
                }
                detail.argdefaults = defaults;
            }
        }

        detail.fdresult = match pform.prokind as u8 {
            PROKIND_AGGREGATE => FuncDetailCode::Aggregate,
            PROKIND_FUNCTION => FuncDetailCode::Normal,
            PROKIND_PROCEDURE => FuncDetailCode::Procedure,
            PROKIND_WINDOW => FuncDetailCode::WindowFunc,
            other => {
                return Err(internal_error_owned(format!(
                    "unrecognized prokind: {}",
                    other as char
                )));
            }
        };

        return Ok(detail);
    }

    Ok(detail) // FUNCDETAIL_NOTFOUND
}

/// `elog(ERROR, "...")` with an owned (formatted) message.
fn internal_error_owned(msg: String) -> PgError {
    ereport(ERROR).errmsg_internal(msg).into_error()
}

// ===========================================================================
// unify_hypothetical_args (parse_func.c:1740)
// ===========================================================================

/// Port target: `unify_hypothetical_args` (parse_func.c:1740).
fn unify_hypothetical_args<'mcx>(
    pstate: &mut ParseState<'mcx>,
    fargs: &mut [Expr<'static>],
    num_aggregated_args: i32,
    actual_arg_types: &mut [Oid],
    declared_arg_types: &[Oid],
) -> PgResult<()> {
    let mcx = pstate_mcx(pstate);
    let num_direct_args = fargs.len() as i32 - num_aggregated_args;
    let num_non_hypothetical_args = num_direct_args - num_aggregated_args;
    // safety check (should only trigger with a misdeclared agg).
    if num_non_hypothetical_args < 0 {
        return Err(internal_error(
            "incorrect number of arguments to hypothetical-set aggregate",
        ));
    }

    let mut hargpos = num_non_hypothetical_args;
    while hargpos < num_direct_args {
        let aargpos = num_direct_args + (hargpos - num_non_hypothetical_args);
        let hp = hargpos as usize;
        let ap = aargpos as usize;

        // A mismatch means AggregateCreate didn't check properly ...
        if declared_arg_types[hp] != declared_arg_types[ap] {
            return Err(internal_error(
                "hypothetical-set aggregate has inconsistent declared argument types",
            ));
        }

        // No need to unify if make_fn_arguments will coerce.
        if declared_arg_types[hp] != ANYOID {
            hargpos += 1;
            continue;
        }

        // Select common type, preferring the aggregated argument's type.
        // Deep-copy via `clone_in`, not a shallow `.clone()`: the aggregated
        // arg may be an `Aggref`/`SubLink` whose context-allocated TargetEntry
        // children panic a derived `.clone()` (e.g. `rank(sum(x)) WITHIN GROUP`).
        let pair = vec![
            fargs[ap].clone_in(mcx)?.erase_lifetime(),
            fargs[hp].clone_in(mcx)?.erase_lifetime(),
        ];
        let commontype = select_common_type::call(pstate, &pair, Some("WITHIN GROUP"))?;
        let commontypmod =
            backend_parser_coerce_seams::select_common_typmod::call(&pair, commontype)?;

        // Perform the coercions.
        let coerced_h = coerce_type::call(
            Some(pstate),
            fargs[hp].clone_in(mcx)?.erase_lifetime(),
            actual_arg_types[hp],
            commontype,
            commontypmod,
            CoercionContext::COERCION_IMPLICIT,
            COERCE_IMPLICIT_CAST,
            -1,
        )?;
        fargs[hp] = coerced_h;
        actual_arg_types[hp] = commontype;

        let coerced_a = coerce_type::call(
            Some(pstate),
            fargs[ap].clone_in(mcx)?.erase_lifetime(),
            actual_arg_types[ap],
            commontype,
            commontypmod,
            CoercionContext::COERCION_IMPLICIT,
            COERCE_IMPLICIT_CAST,
            -1,
        )?;
        fargs[ap] = coerced_a;
        actual_arg_types[ap] = commontype;

        hargpos += 1;
    }

    Ok(())
}

// ===========================================================================
// ParseComplexProjection (parse_func.c:1911)
// ===========================================================================

/// Port target: `ParseComplexProjection` (parse_func.c:1911).
///
/// Handle a single-complex-arg function call that may be a column projection;
/// returns a transformed expression, or `None`.
fn ParseComplexProjection<'mcx>(
    pstate: &mut ParseState<'mcx>,
    funcname: &str,
    first_arg: Option<Expr<'static>>,
    location: i32,
) -> PgResult<Option<Expr<'static>>> {
    let mcx = pstate_mcx(pstate);

    let first_arg = match first_arg {
        Some(n) => n,
        None => return Ok(None),
    };

    // Special case for whole-row Vars: resolve (foo.*).bar directly.
    if let Some(var) = first_arg.expect_var() {
        if var.varattno == InvalidAttrNumber {
            // Return a Var if funcname matches a column, else NULL.
            return scan_ns_item_for_column_by_posn::call(
                pstate,
                var.varno,
                var.varlevelsup as i32,
                funcname,
                location,
            );
        }
    }

    // Else use get_expr_result_tupdesc(); a RECORD Var needs expandRecordVariable.
    // get_expr_result_tupdesc takes Option<&Node> (C: (Node *) first_arg).
    let first_arg_node = Node::mk_expr(mcx, first_arg.clone_in(mcx)?)?;
    let tupdesc = if let Some(var) = first_arg.expect_var() {
        if var.vartype == RECORDOID {
            let var = var.clone();
            expand_record_variable::call(mcx, pstate, &var, 0)?
        } else {
            get_expr_result_tupdesc::call(mcx, Some(&first_arg_node), true)?
        }
    } else {
        get_expr_result_tupdesc::call(mcx, Some(&first_arg_node), true)?
    };
    let tupdesc = match tupdesc {
        Some(td) => td,
        None => return Ok(None), // unresolvable RECORD type
    };

    for i in 0..tupdesc.natts as usize {
        let att = tupdesc.attr(i);
        if att_name_eq(&att.attname, funcname) && !att.attisdropped {
            // Success, so generate a FieldSelect expression.
            let fselect = FieldSelect {
                arg: Some(Box::new(first_arg)),
                fieldnum: (i + 1) as AttrNumber,
                resulttype: att.atttypid,
                resulttypmod: att.atttypmod,
                // save attribute's collation for parse_collate.c.
                resultcollid: att.attcollation,
            };
            return Ok(Some(Expr::FieldSelect(fselect)));
        }
    }

    Ok(None) // funcname does not match any column
}

/// `strcmp(funcname, NameStr(att->attname)) == 0`.
fn att_name_eq(attname: &types_tuple::heaptuple::NameData, funcname: &str) -> bool {
    let raw = attname.name_str();
    // NameStr is a C string within a fixed-size NameData; compare up to the NUL.
    let end = raw.iter().position(|&b| b == 0).unwrap_or(raw.len());
    raw[..end] == *funcname.as_bytes()
}

// --- tiny Bitmapset stand-in for func_get_detail's defargnumbers ------------
//
// func_get_detail builds a transient bitmapset of argnumbers (small, bounded by
// FUNC_MAX_ARGS) only to test membership while scanning the defaults list. A
// fixed-capacity bitset over [0, FUNC_MAX_ARGS) is an exact, allocation-light
// stand-in for bms_add_member / bms_is_member here.
struct BitSet {
    bits: [bool; FUNC_MAX_ARGS],
}

impl BitSet {
    fn new() -> Self {
        Self {
            bits: [false; FUNC_MAX_ARGS],
        }
    }
    fn add(&mut self, n: i32) {
        if n >= 0 && (n as usize) < FUNC_MAX_ARGS {
            self.bits[n as usize] = true;
        }
    }
    fn contains(&self, n: i32) -> bool {
        n >= 0 && (n as usize) < FUNC_MAX_ARGS && self.bits[n as usize]
    }
}
