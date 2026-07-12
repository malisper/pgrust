//! `generate_normalized_query` + `fill_in_constant_lengths`: replace each
//! recorded constant location with $n by re-lexing the query with the core
//! scanner. C sorts and scribbles lengths into jstate->clocations in place;
//! the tap hands the JumbleState out by shared ref, so this works on a copy.

struct Loc {
    location: i32,
    length: i32,
    squashed: bool,
    extern_param: bool,
}

/// `generate_normalized_query` (returns the normalized text; query is the
/// CleanQuerytext'd slice, query_loc its offset in the original string).
pub(crate) fn generate_normalized_query(
    jstate: &queryjumble::JumbleState<'_>,
    query: &str,
    query_loc: i32,
) -> String {
    let mut locs: Vec<Loc> = jstate
        .clocations
        .iter()
        .map(|l| Loc {
            location: l.location,
            length: l.length,
            squashed: l.squashed,
            extern_param: l.extern_param,
        })
        .collect();

    fill_in_constant_lengths(&mut locs, query, query_loc);

    let qbytes = query.as_bytes();
    let mut norm = String::with_capacity(query.len() + locs.len() * 10);
    let mut quer_loc = 0usize; // source byte position
    let mut last_off = 0usize;
    let mut last_tok_len = 0usize;
    let mut num_constants_replaced = 0i32;

    for l in &locs {
        // An external param with no squashed lists keeps its original text.
        if l.extern_param && !jstate.has_squashed_lists {
            continue;
        }
        debug_assert!(l.location >= query_loc); // C Assert(loc >= 0)
        if l.length < 0 || l.location < query_loc {
            continue; // duplicate or bogus location
        }
        let off = (l.location - query_loc) as usize;
        let len_to_wrt = off - last_off - last_tok_len;
        norm.push_str(core::str::from_utf8(&qbytes[quer_loc..quer_loc + len_to_wrt]).unwrap());
        norm.push('$');
        norm.push_str(
            &(num_constants_replaced + 1 + jstate.highest_extern_param_id).to_string(),
        );
        if l.squashed {
            norm.push_str(" /*, ... */");
        }
        num_constants_replaced += 1;

        quer_loc = off + l.length as usize;
        last_off = off;
        last_tok_len = l.length as usize;
    }

    norm.push_str(core::str::from_utf8(&qbytes[quer_loc..]).unwrap());
    norm
}

/// `fill_in_constant_lengths`.
fn fill_in_constant_lengths(locs: &mut [Loc], query: &str, query_loc: i32) {
    locs.sort_by_key(|l| l.location);

    let ctx = mcx::MemoryContext::new("pgss normalize");
    let mcx = ctx.mcx();
    let settings = scan_fgram::ScannerSettings {
        // C: we don't want to re-emit any escape string warnings.
        escape_string_warning: false,
        encoding: mbutils::GetDatabaseEncoding(),
        client_encoding: mbutils::GetDatabaseEncoding(),
        ..scan_fgram::ScannerSettings::default()
    };
    let mut scanner = scan_fgram::Scanner::new(query.as_bytes(), mcx, settings);
    let mut lval = scan_fgram::CoreYYSTYPE::None;
    let mut lloc: i32 = 0;
    let mut eof = false;

    for i in 0..locs.len() {
        if i > 0 && locs[i].location == locs[i - 1].location {
            locs[i].length = -1; // ignore duplicates past the first
            continue;
        }
        if locs[i].squashed {
            continue; // squashable list: the jumbler recorded its extent
        }
        let loc = locs[i].location - query_loc;
        debug_assert!(loc >= 0);
        if eof {
            break;
        }
        loop {
            let tok = match scanner.core_yylex(&mut lval, &mut lloc) {
                Ok(t) => t,
                Err(_) => 0,
            };
            if tok == 0 {
                eof = true;
                break;
            }
            if lloc >= loc {
                if query.as_bytes().get(loc as usize) == Some(&b'-') {
                    // Negative constant: the '-' and the value are replaced
                    // as one token pair (C keeps the minus in the $n span).
                    let tok = match scanner.core_yylex(&mut lval, &mut lloc) {
                        Ok(t) => t,
                        Err(_) => 0,
                    };
                    if tok == 0 {
                        eof = true;
                        break;
                    }
                }
                // C: strlen(scanbuf + loc) over flex's NUL after the token.
                locs[i].length = scanner.tok_end() as i32 - loc;
                break;
            }
        }
    }
}
