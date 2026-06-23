//! Port of `src/backend/tsearch/dict_thesaurus.c` — the `thesaurus` dictionary:
//! phrase-to-phrase substitution through a sub-dictionary.
//!
//! C builds intrusive `LexemeInfo` chains with `palloc`'d
//! `nextentry`/`nextvariant` pointers and stashes a raw `LexemeInfo *` in
//! `DictSubState.private_state`. This port replaces every pointer with an
//! `Option<usize>` index into the carrying
//! [`DictThesaurus`](::tsearch::DictThesaurus)'s `arena`, reproducing
//! `findVariant` / `matchIdSubst` / `checkMatch` 1:1.

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

use ::define_seams::{def_get_string, DefElemArg};
use ::dict_seams::{get_ts_dict_oid_from_name, subdict_lexize};
use ::ts_locale_seams::readfile;
use ::ts_utils_seams::get_tsearch_config_filename;
use ::mbutils_seams::pg_mblen_range;

use ::mcx::{Mcx, PgString, PgVec};
use ::types_core::{uint16, uint32};
use ::types_error::PgResult;
use ::tsearch::{
    DictThesaurus, LexemeInfo, OwnedTSLexeme, TSLexeme, TheLexeme, TheSubstitute, ThesaurusSubState,
    DT_USEASIS, TSL_ADDPOS,
};

use crate::{config_file_error, config_file_error_hint, elog_error, invalid_param, is_space};

/* ---- arena helpers (replacing the C palloc'd LexemeInfo chains) ---- */

fn arena_alloc(arena: &mut PgVec<'_, LexemeInfo>, node: LexemeInfo) -> PgResult<usize> {
    let idx = arena.len();
    arena
        .try_reserve(1)
        .map_err(|_| arena.allocator().oom(core::mem::size_of::<LexemeInfo>()))?;
    arena.push(node);
    Ok(idx)
}

/* ---- construction helpers ---- */

/// `newLexeme(d, b, e, idsubst, posinsubst)`: append a lexeme `word` for the
/// given substitution position.
fn new_lexeme<'mcx>(
    mcx: Mcx<'mcx>,
    d: &mut DictThesaurus<'mcx>,
    word: &[u8],
    idsubst: uint32,
    posinsubst: uint16,
) -> PgResult<()> {
    // ptr->entries = palloc(sizeof(LexemeInfo)); nextentry = NULL.
    let entries = arena_alloc(
        &mut d.arena,
        LexemeInfo {
            idsubst,
            posinsubst,
            tnvariant: 0,
            nextentry: None,
            nextvariant: None,
        },
    )?;
    let lexeme = bytes_to_pgstring(mcx, word)?;
    d.wrds
        .try_reserve(1)
        .map_err(|_| mcx.oom(core::mem::size_of::<TheLexeme>()))?;
    d.wrds.push(TheLexeme {
        lexeme: Some(lexeme),
        entries: Some(entries),
    });
    Ok(())
}

/// `addWrd(d, b, e, idsubst, nwrd, posinsubst, useasis)`: append the `nwrd`-th
/// word of substitution `idsubst`'s result.
#[allow(clippy::too_many_arguments)]
fn add_wrd<'mcx>(
    mcx: Mcx<'mcx>,
    d: &mut DictThesaurus<'mcx>,
    word: &[u8],
    idsubst: uint32,
    nwrd: uint16,
    posinsubst: uint16,
    useasis: bool,
) -> PgResult<()> {
    // C grows d->subst lazily; the owned Vec just needs to reach idsubst+1.
    if nwrd == 0 && idsubst as i32 >= d.nsubst {
        d.nsubst = if d.nsubst == 0 { 16 } else { d.nsubst * 2 };
    }
    while d.subst.len() <= idsubst as usize {
        d.subst
            .try_reserve(1)
            .map_err(|_| mcx.oom(core::mem::size_of::<TheSubstitute>()))?;
        d.subst.push(TheSubstitute {
            lastlexeme: 0,
            reslen: 0,
            res: PgVec::new_in(mcx),
        });
    }

    let lexeme = bytes_to_pgstring(mcx, word)?;
    let ptr = &mut d.subst[idsubst as usize];

    // ptr->lastlexeme = posinsubst - 1;
    ptr.lastlexeme = posinsubst.wrapping_sub(1);

    // C appends ptr->res[nres] then NUL-terminates; nwrd==0 starts fresh.
    if nwrd == 0 {
        ptr.res.clear();
    }
    ptr.res
        .try_reserve(1)
        .map_err(|_| mcx.oom(core::mem::size_of::<TSLexeme>()))?;
    ptr.res.push(TSLexeme {
        nvariant: nwrd,
        flags: if useasis { DT_USEASIS } else { 0 },
        lexeme,
    });
    Ok(())
}

const TR_WAITLEX: i32 = 1;
const TR_INLEX: i32 = 2;
const TR_WAITSUBS: i32 = 3;
const TR_INSUBS: i32 = 4;

/// `thesaurusRead(filename, d)`: parse the `.ths` file into `d->wrds` /
/// `d->subst`.
fn thesaurus_read<'mcx>(
    mcx: Mcx<'mcx>,
    filename: &[u8],
    d: &mut DictThesaurus<'mcx>,
) -> PgResult<()> {
    // filename = get_tsearch_config_filename(filename, "ths");
    let path = get_tsearch_config_filename::call(mcx, filename, b"ths")?;
    let content = readfile::call(&path).map_err(|_| {
        config_file_error(format!(
            "could not open thesaurus file \"{}\": %m",
            String::from_utf8_lossy(&path)
        ))
    })?;

    let mut idsubst: uint32 = 0;
    let mut useasis = false;

    for line in content.split(|&b| b == b'\n') {
        let mut state = TR_WAITLEX;
        let mut beginwrd: Option<usize> = None;
        let mut posinsubst: uint32 = 0;
        let mut nwrd: uint32 = 0;

        let mut i = 0usize;

        // is it a comment? skip leading spaces.
        while i < line.len() && line[i] != 0 && is_space(line[i]) {
            i += pg_mblen_range::call(&line[i..])? as usize;
        }
        let first = byte_at(line, i);
        if first == b'#' || first == 0 || first == b'\n' || first == b'\r' {
            continue;
        }

        while i < line.len() && line[i] != 0 {
            let c = line[i];
            if state == TR_WAITLEX {
                if c == b':' {
                    if posinsubst == 0 {
                        return Err(config_file_error("unexpected delimiter"));
                    }
                    state = TR_WAITSUBS;
                } else if !is_space(c) {
                    beginwrd = Some(i);
                    state = TR_INLEX;
                }
            } else if state == TR_INLEX {
                if c == b':' {
                    let b = beginwrd
                        .ok_or_else(|| elog_error("thesaurusRead: beginwrd unset in TR_INLEX"))?;
                    new_lexeme(mcx, d, &line[b..i], idsubst, posinsubst as uint16)?;
                    posinsubst += 1;
                    state = TR_WAITSUBS;
                } else if is_space(c) {
                    let b = beginwrd
                        .ok_or_else(|| elog_error("thesaurusRead: beginwrd unset in TR_INLEX"))?;
                    new_lexeme(mcx, d, &line[b..i], idsubst, posinsubst as uint16)?;
                    posinsubst += 1;
                    state = TR_WAITLEX;
                }
            } else if state == TR_WAITSUBS {
                if c == b'*' {
                    useasis = true;
                    state = TR_INSUBS;
                    beginwrd = Some(i + pg_mblen_range::call(&line[i..])? as usize);
                } else if c == b'\\' {
                    useasis = false;
                    state = TR_INSUBS;
                    beginwrd = Some(i + pg_mblen_range::call(&line[i..])? as usize);
                } else if !is_space(c) {
                    useasis = false;
                    beginwrd = Some(i);
                    state = TR_INSUBS;
                }
            } else if state == TR_INSUBS {
                if is_space(c) {
                    let b = beginwrd
                        .ok_or_else(|| elog_error("thesaurusRead: beginwrd unset in TR_INSUBS"))?;
                    if i == b {
                        return Err(config_file_error("unexpected end of line or lexeme"));
                    }
                    add_wrd(mcx, d, &line[b..i], idsubst, nwrd as uint16, posinsubst as uint16, useasis)?;
                    nwrd += 1;
                    state = TR_WAITSUBS;
                }
            } else {
                return Err(elog_error(format!("unrecognized thesaurus state: {state}")));
            }

            i += pg_mblen_range::call(&line[i..])? as usize;
        }

        if state == TR_INSUBS {
            let b =
                beginwrd.ok_or_else(|| elog_error("thesaurusRead: beginwrd unset in TR_INSUBS"))?;
            if i == b {
                return Err(config_file_error("unexpected end of line or lexeme"));
            }
            add_wrd(mcx, d, &line[b..i], idsubst, nwrd as uint16, posinsubst as uint16, useasis)?;
            nwrd += 1;
        }

        idsubst += 1;

        if !(nwrd != 0 && posinsubst != 0) {
            return Err(config_file_error("unexpected end of line"));
        }

        if nwrd != (nwrd as uint16) as uint32 || posinsubst != (posinsubst as uint16) as uint32 {
            return Err(config_file_error("too many lexemes in thesaurus entry"));
        }
    }

    d.nsubst = idsubst as i32;
    // C never uses subst past idsubst.
    d.subst.truncate(d.nsubst as usize);
    Ok(())
}

/// `addCompiledLexeme(newwrds, nnw, tnm, lexeme, src, tnvariant)`: append a
/// (possibly null) compiled lexeme into `newwrds`.
fn add_compiled_lexeme<'mcx>(
    mcx: Mcx<'mcx>,
    arena: &mut PgVec<'mcx, LexemeInfo>,
    newwrds: &mut PgVec<'mcx, TheLexeme<'mcx>>,
    lexeme: Option<&OwnedTSLexeme>,
    src: usize,
    tnvariant: uint16,
) -> PgResult<()> {
    let (lex, tnv): (Option<PgString<'mcx>>, uint16) = match lexeme {
        Some(l) => (Some(bytes_to_pgstring(mcx, l.lexeme.as_bytes())?), tnvariant),
        None => (None, 1),
    };

    let src_node = arena[src];
    let entries = arena_alloc(
        arena,
        LexemeInfo {
            idsubst: src_node.idsubst,
            posinsubst: src_node.posinsubst,
            tnvariant: tnv,
            nextentry: None,
            nextvariant: None,
        },
    )?;

    newwrds
        .try_reserve(1)
        .map_err(|_| mcx.oom(core::mem::size_of::<TheLexeme>()))?;
    newwrds.push(TheLexeme {
        lexeme: lex,
        entries: Some(entries),
    });
    Ok(())
}

/// `cmpLexemeInfo(a, b)`.
fn cmp_lexeme_info(arena: &[LexemeInfo], a: Option<usize>, b: Option<usize>) -> i32 {
    let (Some(a), Some(b)) = (a, b) else {
        return 0;
    };
    let a = &arena[a];
    let b = &arena[b];
    if a.idsubst == b.idsubst {
        if a.posinsubst == b.posinsubst {
            if a.tnvariant == b.tnvariant {
                return 0;
            }
            return if a.tnvariant > b.tnvariant { 1 } else { -1 };
        }
        return if a.posinsubst > b.posinsubst { 1 } else { -1 };
    }
    if a.idsubst > b.idsubst {
        1
    } else {
        -1
    }
}

/// `cmpLexeme(a, b)`: order by lexeme (`NULL` sorts last).
fn cmp_lexeme(a: &TheLexeme<'_>, b: &TheLexeme<'_>) -> i32 {
    match (&a.lexeme, &b.lexeme) {
        (None, None) => 0,
        (None, Some(_)) => 1,
        (Some(_), None) => -1,
        (Some(sa), Some(sb)) => match sa.as_bytes().cmp(sb.as_bytes()) {
            core::cmp::Ordering::Less => -1,
            core::cmp::Ordering::Equal => 0,
            core::cmp::Ordering::Greater => 1,
        },
    }
}

/// `cmpTheLexeme(a, b)`: by lexeme, then by `-cmpLexemeInfo(entries)`.
fn cmp_the_lexeme(arena: &[LexemeInfo], a: &TheLexeme<'_>, b: &TheLexeme<'_>) -> i32 {
    let res = cmp_lexeme(a, b);
    if res != 0 {
        return res;
    }
    -cmp_lexeme_info(arena, a.entries, b.entries)
}

/// `compileTheLexeme(d)`: normalize each rule lexeme via the sub-dictionary,
/// expand variants, sort+uniq.
fn compile_the_lexeme<'mcx>(mcx: Mcx<'mcx>, d: &mut DictThesaurus<'mcx>) -> PgResult<()> {
    let mut newwrds: PgVec<'mcx, TheLexeme<'mcx>> = PgVec::new_in(mcx);

    // C frees each d->wrds[i] as it goes; take ownership to iterate.
    let wrds = core::mem::replace(&mut d.wrds, PgVec::new_in(mcx));

    for wrd in wrds.iter() {
        let entries = wrd
            .entries
            .ok_or_else(|| elog_error("compileTheLexeme: rule lexeme has no entries"))?;
        let lexeme_str = wrd.lexeme.as_ref().map(|s| s.as_str()).unwrap_or("");

        if lexeme_str == "?" {
            // Is stop word marker?
            add_compiled_lexeme(mcx, &mut d.arena, &mut newwrds, None, entries, 0)?;
        } else {
            // ptr = FunctionCall4(&subdict->lexize, dictData, lexeme, strlen, NULL);
            let ptr0 = subdict_lexize::call(d.subdict_oid, lexeme_str.as_bytes().to_vec())?;

            match ptr0 {
                None => {
                    let idsubst = d.arena[entries].idsubst;
                    return Err(config_file_error(format!(
                        "thesaurus sample word \"{lexeme_str}\" isn't recognized by subdictionary (rule {})",
                        idsubst + 1
                    )));
                }
                Some(ref arr) if arr.is_empty() => {
                    // !(ptr->lexeme): C's res->lexeme == NULL (empty array).
                    let idsubst = d.arena[entries].idsubst;
                    return Err(config_file_error_hint(
                        format!(
                            "thesaurus sample word \"{lexeme_str}\" is a stop word (rule {})",
                            idsubst + 1
                        ),
                        "Use \"?\" to represent a stop word within a sample phrase.",
                    ));
                }
                Some(arr) => {
                    // Walk the lexeme array variant by variant.
                    let mut p = 0usize;
                    while p < arr.len() {
                        let curvar = arr[p].nvariant;

                        // compute n words in one variant (tnvar)
                        let mut remp = p + 1;
                        let mut tnvar: i32 = 1;
                        while remp < arr.len() {
                            if arr[remp].nvariant != arr[remp - 1].nvariant {
                                break;
                            }
                            tnvar += 1;
                            remp += 1;
                        }

                        // emit all words of the current variant
                        let mut remp2 = p;
                        while remp2 < arr.len() && arr[remp2].nvariant == curvar {
                            add_compiled_lexeme(
                                mcx,
                                &mut d.arena,
                                &mut newwrds,
                                Some(&arr[remp2]),
                                entries,
                                tnvar as uint16,
                            )?;
                            remp2 += 1;
                        }

                        p = remp2;
                    }
                }
            }
        }
        // C: pfree(d->wrds[i].lexeme); pfree(d->wrds[i].entries);  (owned drop)
    }

    d.wrds = newwrds;

    if d.wrds.len() > 1 {
        // qsort(d->wrds, d->nwrds, sizeof(TheLexeme), cmpTheLexeme);
        // Snapshot the arena keys to satisfy the borrow checker (sort reads the
        // immutable arena while reordering wrds).
        let arena_snapshot: Vec<LexemeInfo> = d.arena.iter().copied().collect();
        d.wrds.sort_by(|a, b| {
            match cmp_the_lexeme(&arena_snapshot, a, b) {
                x if x < 0 => core::cmp::Ordering::Less,
                x if x > 0 => core::cmp::Ordering::Greater,
                _ => core::cmp::Ordering::Equal,
            }
        });

        // uniq: merge equal-lexeme runs by chaining entries via nextentry.
        let mut out: PgVec<'mcx, TheLexeme<'mcx>> = PgVec::new_in(mcx);
        let src = core::mem::replace(&mut d.wrds, PgVec::new_in(mcx));
        let mut iter = src.into_iter();
        if let Some(first) = iter.next() {
            out.try_reserve(1)
                .map_err(|_| mcx.oom(core::mem::size_of::<TheLexeme>()))?;
            out.push(first);
            for ptrw in iter {
                let neww = out
                    .last_mut()
                    .ok_or_else(|| elog_error("compileTheLexeme: uniq output empty"))?;
                if cmp_lexeme(&ptrw, neww) == 0 {
                    if cmp_lexeme_info(&arena_snapshot, ptrw.entries, neww.entries) != 0 {
                        // ptrwrds->entries->nextentry = newwrds->entries;
                        // newwrds->entries = ptrwrds->entries;
                        let p_entries = ptrw.entries;
                        if let Some(pe) = p_entries {
                            d.arena[pe].nextentry = neww.entries;
                        }
                        neww.entries = p_entries;
                    }
                    // else: pfree(ptrwrds->entries) — arena node left orphaned
                    // (reclaimed with the dictionary), matching C's pfree.
                } else {
                    out.try_reserve(1)
                        .map_err(|_| mcx.oom(core::mem::size_of::<TheLexeme>()))?;
                    out.push(ptrw);
                }
            }
        }
        d.wrds = out;
    }

    Ok(())
}

/// `compileTheSubstitute(d)`: lexize each substitute word through the
/// sub-dictionary, producing the final stored `res` arrays.
fn compile_the_substitute<'mcx>(mcx: Mcx<'mcx>, d: &mut DictThesaurus<'mcx>) -> PgResult<()> {
    for i in 0..d.subst.len() {
        // rem = d->subst[i].res;  res = fresh (outptr starts empty).
        let rem = core::mem::replace(&mut d.subst[i].res, PgVec::new_in(mcx));
        let mut out: PgVec<'mcx, TSLexeme<'mcx>> = PgVec::new_in(mcx);

        let mut inptr = 0usize;
        while inptr < rem.len() {
            // lexized = (flags & DT_USEASIS) ? {*inptr, flags cleared}
            //                                : FunctionCall4(subdict, inptr->lexeme, ...);
            let in_lex = rem[inptr].lexeme.as_str();
            let lexized: Option<Vec<OwnedTSLexeme>> = if rem[inptr].flags & DT_USEASIS != 0 {
                // do not lexize
                Some(alloc::vec![OwnedTSLexeme {
                    nvariant: rem[inptr].nvariant,
                    flags: 0,
                    lexeme: String::from(in_lex),
                }])
            } else {
                subdict_lexize::call(d.subdict_oid, in_lex.as_bytes().to_vec())?
            };

            match lexized {
                Some(ref lx) if !lx.is_empty() => {
                    // toset = (lexized->lexeme && outptr != res) ? (outptr - res) : -1
                    let toset: isize = if !out.is_empty() {
                        out.len() as isize
                    } else {
                        -1
                    };

                    // while (lexized->lexeme) { *outptr = *lexized; pstrdup; outptr++; }
                    for lex in lx.iter() {
                        let lexeme = bytes_to_pgstring(mcx, lex.lexeme.as_bytes())?;
                        out.try_reserve(1)
                            .map_err(|_| mcx.oom(core::mem::size_of::<TSLexeme>()))?;
                        out.push(TSLexeme {
                            nvariant: lex.nvariant,
                            flags: lex.flags,
                            lexeme,
                        });
                    }

                    // if (toset > 0) res[toset].flags |= TSL_ADDPOS;
                    if toset > 0 {
                        out[toset as usize].flags |= TSL_ADDPOS;
                    }
                }
                Some(_) => {
                    // non-null but lexized->lexeme == NULL (stop word).
                    return Err(config_file_error(format!(
                        "thesaurus substitute word \"{in_lex}\" is a stop word (rule {})",
                        i + 1
                    )));
                }
                None => {
                    return Err(config_file_error(format!(
                        "thesaurus substitute word \"{in_lex}\" isn't recognized by subdictionary (rule {})",
                        i + 1
                    )));
                }
            }

            inptr += 1;
        }

        // if (outptr == d->subst[i].res) ereport(...phrase is empty...);
        if out.is_empty() {
            return Err(config_file_error(format!(
                "thesaurus substitute phrase is empty (rule {})",
                i + 1
            )));
        }

        d.subst[i].reslen = out.len() as uint16;
        d.subst[i].res = out;
    }

    Ok(())
}

/* ---- SQL-facing entry points ---- */

/// `thesaurus_init(PG_FUNCTION_ARGS)`: read the rule file and compile lexemes /
/// substitutions through the sub-dictionary.
pub fn thesaurus_init<'mcx>(
    mcx: Mcx<'mcx>,
    dictoptions: &[(String, Option<DefElemArg>)],
) -> PgResult<DictThesaurus<'mcx>> {
    let mut d = DictThesaurus {
        subdict_oid: 0,
        wrds: PgVec::new_in(mcx),
        subst: PgVec::new_in(mcx),
        nsubst: 0,
        arena: PgVec::new_in(mcx),
    };

    let mut subdictname: Option<PgString<'mcx>> = None;
    let mut fileloaded = false;

    for (defname, arg) in dictoptions {
        if defname == "dictfile" {
            if fileloaded {
                return Err(invalid_param("multiple DictFile parameters"));
            }
            let base = def_get_string::call(mcx, defname.clone(), arg.clone())?;
            thesaurus_read(mcx, base.as_bytes(), &mut d)?;
            fileloaded = true;
        } else if defname == "dictionary" {
            if subdictname.is_some() {
                return Err(invalid_param("multiple Dictionary parameters"));
            }
            // subdictname = pstrdup(defGetString(defel));
            subdictname = Some(def_get_string::call(mcx, defname.clone(), arg.clone())?);
        } else {
            return Err(invalid_param(format!(
                "unrecognized Thesaurus parameter: \"{defname}\""
            )));
        }
    }

    if !fileloaded {
        return Err(invalid_param("missing DictFile parameter"));
    }
    let Some(subdictname) = subdictname else {
        return Err(invalid_param("missing Dictionary parameter"));
    };

    // namelist = stringToQualifiedNameList(subdictname, NULL);
    // d->subdictOid = get_ts_dict_oid(namelist, false);
    d.subdict_oid = get_ts_dict_oid_from_name::call(String::from(subdictname.as_str()))?;

    compile_the_lexeme(mcx, &mut d)?;
    compile_the_substitute(mcx, &mut d)?;

    Ok(d)
}

/// `findTheLexeme(d, lexeme)`: bsearch `d->wrds` for `lexeme` (`None` = stop-word
/// marker). Returns the matching `TheLexeme`'s `entries` arena index.
fn find_the_lexeme(d: &DictThesaurus<'_>, lexeme: Option<&[u8]>) -> Option<usize> {
    if d.wrds.is_empty() {
        return None;
    }
    bsearch_thelexeme(&d.wrds, lexeme).and_then(|idx| d.wrds[idx].entries)
}

/// `matchIdSubst(stored, idsubst)`.
fn match_id_subst(arena: &[LexemeInfo], stored: Option<usize>, idsubst: uint32) -> bool {
    let Some(stored) = stored else {
        return true;
    };
    let mut s = Some(stored);
    while let Some(idx) = s {
        let node = &arena[idx];
        if node.idsubst == idsubst {
            return true;
        }
        s = node.nextvariant;
    }
    false
}

/// `findVariant(in, stored, curpos, newin, newn)`.
fn find_variant(
    arena: &mut [LexemeInfo],
    mut r#in: Option<usize>,
    stored: Option<usize>,
    curpos: uint16,
    newin: &mut [Option<usize>],
    newn: i32,
) -> Option<usize> {
    loop {
        let mut i: i32 = 0;
        let mut ptr = newin[0];

        while i < newn {
            let iu = i as usize;

            // while (newin[i] && newin[i]->idsubst < ptr->idsubst) newin[i] = nextentry;
            while let Some(ni) = newin[iu] {
                let pidsubst = ptr.map(|p| arena[p].idsubst).unwrap_or(0);
                if arena[ni].idsubst < pidsubst {
                    newin[iu] = arena[ni].nextentry;
                } else {
                    break;
                }
            }

            if newin[iu].is_none() {
                return r#in;
            }

            let ni = newin[iu].unwrap();
            let pidsubst = ptr.map(|p| arena[p].idsubst).unwrap_or(0);
            if arena[ni].idsubst > pidsubst {
                ptr = newin[iu];
                i = 0; // C: i = -1; then for-loop i++
                continue;
            }

            // while (newin[i]->idsubst == ptr->idsubst)
            loop {
                let ni = match newin[iu] {
                    Some(ni) => ni,
                    None => return r#in,
                };
                let pidsubst = ptr.map(|p| arena[p].idsubst).unwrap_or(0);
                if arena[ni].idsubst != pidsubst {
                    break;
                }
                if arena[ni].posinsubst == curpos && arena[ni].tnvariant as i32 == newn {
                    ptr = newin[iu];
                    break;
                }
                newin[iu] = arena[ni].nextentry;
                if newin[iu].is_none() {
                    return r#in;
                }
            }

            let ni = newin[iu].unwrap();
            let pidsubst = ptr.map(|p| arena[p].idsubst).unwrap_or(0);
            if arena[ni].idsubst != pidsubst {
                ptr = newin[iu];
                i = 0; // C: i = -1; then for-loop i++
                continue;
            }

            i += 1;
        }

        if i == newn {
            let pidsubst = ptr.map(|p| arena[p].idsubst).unwrap_or(0);
            if match_id_subst(arena, stored, pidsubst)
                && (r#in.is_none() || !match_id_subst(arena, r#in, pidsubst))
            {
                // found
                if let Some(p) = ptr {
                    arena[p].nextvariant = r#in;
                }
                r#in = ptr;
            }
        }

        // step forward
        for slot in newin.iter_mut().take(newn as usize) {
            if let Some(s) = *slot {
                *slot = arena[s].nextentry;
            }
        }
    }
}

/// `copyTSLexeme(ts)`: deep-copy a substitution's stored result.
fn copy_ts_lexeme<'mcx>(
    mcx: Mcx<'mcx>,
    ts: &TheSubstitute<'mcx>,
) -> PgResult<PgVec<'mcx, TSLexeme<'mcx>>> {
    let mut res: PgVec<'mcx, TSLexeme<'mcx>> = PgVec::new_in(mcx);
    for lex in ts.res[..ts.reslen as usize].iter() {
        let lexeme = bytes_to_pgstring(mcx, lex.lexeme.as_bytes())?;
        res.try_reserve(1)
            .map_err(|_| mcx.oom(core::mem::size_of::<TSLexeme>()))?;
        res.push(TSLexeme {
            nvariant: lex.nvariant,
            flags: lex.flags,
            lexeme,
        });
    }
    Ok(res)
}

/// `checkMatch(d, info, curpos, moreres)`: emit a substitution if a complete
/// phrase matched at `curpos`.
fn check_match<'mcx>(
    mcx: Mcx<'mcx>,
    d: &DictThesaurus<'mcx>,
    info: Option<usize>,
    curpos: uint16,
    moreres: &mut bool,
) -> PgResult<Option<PgVec<'mcx, TSLexeme<'mcx>>>> {
    *moreres = false;
    let mut info = info;
    while let Some(idx) = info {
        let node = d.arena[idx];
        debug_assert!(node.idsubst < d.nsubst as uint32);
        if node.nextvariant.is_some() {
            *moreres = true;
        }
        if d.subst[node.idsubst as usize].lastlexeme == curpos {
            return Ok(Some(copy_ts_lexeme(mcx, &d.subst[node.idsubst as usize])?));
        }
        info = node.nextvariant;
    }
    Ok(None)
}

/// `thesaurus_lexize(PG_FUNCTION_ARGS)`: match phrases and emit substitutions.
pub fn thesaurus_lexize<'mcx>(
    mcx: Mcx<'mcx>,
    d: &mut DictThesaurus<'mcx>,
    input: &[u8],
    len: i32,
    state: &mut ThesaurusSubState,
) -> PgResult<Option<PgVec<'mcx, TSLexeme<'mcx>>>> {
    // if (dstate->isend) PG_RETURN_POINTER(NULL);
    if state.isend {
        return Ok(None);
    }
    let stored = state.stored;

    // if (stored) curpos = stored->posinsubst + 1;
    let mut curpos: uint16 = 0;
    if let Some(s) = stored {
        curpos = d.arena[s].posinsubst + 1;
    }

    // The cache-validity refresh (`if (!d->subdict->isvalid) d->subdict =
    // lookup_ts_dictionary_cache(d->subdictOid)`) is performed inside the
    // sub-dictionary lexize fmgr dispatch (keyed on `subdictOid`).
    let in_bytes = crate::dict_simple::slice_from_in(input, len);
    let res = subdict_lexize::call(d.subdict_oid, in_bytes.to_vec())?;

    let mut info: Option<usize> = None;

    match res {
        Some(ref arr) if !arr.is_empty() => {
            let mut p = 0usize;
            while p < arr.len() {
                let nv = arr[p].nvariant;
                let basevar = p;
                let mut nlex: uint16 = 0;
                while p < arr.len() && nv == arr[p].nvariant {
                    nlex += 1;
                    p += 1;
                }

                // infos[i] = findTheLexeme(d, basevar[i].lexeme); break on NULL.
                let mut infos: Vec<Option<usize>> = alloc::vec![None; nlex as usize];
                let mut i: uint16 = 0;
                while i < nlex {
                    let lx = arr[basevar + i as usize].lexeme.as_bytes();
                    infos[i as usize] = find_the_lexeme(d, Some(lx));
                    if infos[i as usize].is_none() {
                        break;
                    }
                    i += 1;
                }

                if i < nlex {
                    // no chance to find
                    continue;
                }

                info = find_variant(&mut d.arena, info, stored, curpos, &mut infos, nlex as i32);
            }
        }
        Some(_) => {
            // stop-word: res non-null but res->lexeme == NULL
            let mut infos = [find_the_lexeme(d, None)];
            info = find_variant(&mut d.arena, None, stored, curpos, &mut infos, 1);
        }
        None => {
            info = None; // word isn't recognized
        }
    }

    state.stored = info;

    if info.is_none() {
        state.getnext = false;
        return Ok(None);
    }

    let mut moreres = false;
    let matched = check_match(mcx, d, info, curpos, &mut moreres)?;
    if let Some(matched) = matched {
        state.getnext = moreres;
        return Ok(Some(matched));
    }

    state.getnext = true;
    Ok(None)
}

/* ---- comparator-backed search ---- */

/// `bsearch(&key, d->wrds, ..., cmpLexemeQ)` over the `cmpLexeme`-sorted array.
/// `key` is `None` for the stop-word marker (`NULL` lexeme, sorts last).
fn bsearch_thelexeme(slice: &[TheLexeme<'_>], key: Option<&[u8]>) -> Option<usize> {
    let mut lo = 0isize;
    let mut hi = slice.len() as isize - 1;
    while lo <= hi {
        let mid = (lo + hi) / 2;
        let c = cmp_lexeme_key(key, &slice[mid as usize]);
        match c.cmp(&0) {
            core::cmp::Ordering::Equal => return Some(mid as usize),
            core::cmp::Ordering::Less => hi = mid - 1,
            core::cmp::Ordering::Greater => lo = mid + 1,
        }
    }
    None
}

/// `cmpLexeme(key, b)` for the bsearch key (`None` = `NULL` lexeme).
fn cmp_lexeme_key(key: Option<&[u8]>, b: &TheLexeme<'_>) -> i32 {
    match (key, &b.lexeme) {
        (None, None) => 0,
        (None, Some(_)) => 1,
        (Some(_), None) => -1,
        (Some(ka), Some(sb)) => match ka.cmp(sb.as_bytes()) {
            core::cmp::Ordering::Less => -1,
            core::cmp::Ordering::Equal => 0,
            core::cmp::Ordering::Greater => 1,
        },
    }
}

/* ---- small helpers ---- */

fn byte_at(line: &[u8], i: usize) -> u8 {
    if i < line.len() {
        line[i]
    } else {
        0
    }
}

fn bytes_to_pgstring<'mcx>(mcx: Mcx<'mcx>, b: &[u8]) -> PgResult<PgString<'mcx>> {
    let s = core::str::from_utf8(b).map_err(|_| elog_error("dict_thesaurus: non-UTF-8 lexeme"))?;
    PgString::from_str_in(s, mcx)
}
