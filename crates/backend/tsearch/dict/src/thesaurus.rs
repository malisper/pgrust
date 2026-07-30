use std::rc::Rc;

use ::mcx::{vec_with_capacity_in, Mcx, MemoryContext, PgVec};
use ::ts_cache::{lookup_ts_dictionary_cache, TSDictionaryCacheEntry};
use ::ts_locale::dict_api::{lexize_result_ref, DictInitData, LexizeResult};
use ::ts_locale::{get_tsearch_config_filename, tsearch_readlines, DictSubState, TsLexeme};
use ::types_core::Oid;
use ::types_error::{PgError, PgResult, ERRCODE_CONFIG_FILE_ERROR};

use crate::simple::invalid_param;

const DT_USEASIS: u16 = 0x1000;
const TSL_ADDPOS: u16 = ::ts_locale::TSL_ADDPOS;

#[derive(Clone, Copy)]
struct LexemeInfo {
    idsubst: u32,
    posinsubst: u16,
    tnvariant: u16,
    nextentry: Option<usize>,
    nextvariant: Option<usize>,
}

struct TheLexeme {
    lexeme: Option<PgVec<'static, u8>>,
    entries: Option<usize>,
}

struct TheSubstitute {
    lastlexeme: u16,
    reslen: u16,
    res: PgVec<'static, TsLexeme<'static>>,
}

// C's palloc'd LexemeInfo chains become indices into `arena`; findVariant's
// runtime nextvariant writes scribble arena nodes exactly like C.
pub struct DictThesaurus {
    mcx: Mcx<'static>,
    subdict_oid: Oid,
    subdict: Rc<TSDictionaryCacheEntry>,
    wrds: PgVec<'static, TheLexeme>,
    subst: PgVec<'static, TheSubstitute>,
    nsubst: i32,
    arena: PgVec<'static, LexemeInfo>,
}

#[track_caller]
#[cold]
fn config_file_error(msg: String) -> Box<PgError> {
    PgError::error(msg).with_sqlstate(ERRCODE_CONFIG_FILE_ERROR).into()
}

fn copy_bytes(mcx: Mcx<'static>, b: &[u8]) -> PgResult<PgVec<'static, u8>> {
    let mut v = vec_with_capacity_in(mcx, b.len())?;
    v.extend_from_slice(b);
    Ok(v)
}

fn arena_alloc(arena: &mut PgVec<'static, LexemeInfo>, node: LexemeInfo) -> usize {
    arena.push(node);
    arena.len() - 1
}

fn new_lexeme(d: &mut DictThesaurus, word: &[u8], idsubst: u32, posinsubst: u16) -> PgResult<()> {
    let entries = arena_alloc(
        &mut d.arena,
        LexemeInfo { idsubst, posinsubst, tnvariant: 0, nextentry: None, nextvariant: None },
    );
    let lexeme = copy_bytes(d.mcx, word)?;
    d.wrds.push(TheLexeme { lexeme: Some(lexeme), entries: Some(entries) });
    Ok(())
}

fn add_wrd(
    d: &mut DictThesaurus,
    word: &[u8],
    idsubst: u32,
    nwrd: u16,
    posinsubst: u16,
    useasis: bool,
) -> PgResult<()> {
    while d.subst.len() <= idsubst as usize {
        d.subst.push(TheSubstitute { lastlexeme: 0, reslen: 0, res: PgVec::new_in(d.mcx) });
    }
    let lexeme = copy_bytes(d.mcx, word)?;
    let ptr = &mut d.subst[idsubst as usize];
    ptr.lastlexeme = posinsubst.wrapping_sub(1);
    if nwrd == 0 {
        ptr.res.clear();
    }
    ptr.res.push(TsLexeme {
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

fn mblen(s: &[u8]) -> usize {
    ::mbutils::pg_mblen(s) as usize
}

fn thesaurus_read(d: &mut DictThesaurus, filename: &[u8]) -> PgResult<()> {
    let mcx = d.mcx;
    let path = get_tsearch_config_filename(mcx, filename, "ths")?;
    let Some(lines) = tsearch_readlines(mcx, &path)? else {
        return Err(config_file_error(format!(
            "could not open thesaurus file \"{}\": No such file or directory",
            String::from_utf8_lossy(&path)
        )));
    };

    let mut idsubst: u32 = 0;
    for line in lines.iter() {
        let mut state = TR_WAITLEX;
        let mut beginwrd = 0usize;
        let mut posinsubst: u32 = 0;
        let mut nwrd: u32 = 0;
        let mut useasis = false;

        let mut i = 0usize;
        while i < line.len() && line[i].is_ascii_whitespace() && line[i] != b'\n' && line[i] != b'\r'
        {
            i += mblen(&line[i..]);
        }
        match line.get(i) {
            None | Some(b'#') | Some(b'\n') | Some(b'\r') => continue,
            _ => {}
        }

        while i < line.len() {
            let c = line[i];
            if state == TR_WAITLEX {
                if c == b':' {
                    if posinsubst == 0 {
                        return Err(config_file_error("unexpected delimiter".into()));
                    }
                    state = TR_WAITSUBS;
                } else if !c.is_ascii_whitespace() {
                    beginwrd = i;
                    state = TR_INLEX;
                }
            } else if state == TR_INLEX {
                if c == b':' {
                    new_lexeme(d, &line[beginwrd..i], idsubst, posinsubst as u16)?;
                    posinsubst += 1;
                    state = TR_WAITSUBS;
                } else if c.is_ascii_whitespace() {
                    new_lexeme(d, &line[beginwrd..i], idsubst, posinsubst as u16)?;
                    posinsubst += 1;
                    state = TR_WAITLEX;
                }
            } else if state == TR_WAITSUBS {
                if c == b'*' {
                    useasis = true;
                    state = TR_INSUBS;
                    beginwrd = i + mblen(&line[i..]);
                } else if c == b'\\' {
                    useasis = false;
                    state = TR_INSUBS;
                    beginwrd = i + mblen(&line[i..]);
                } else if !c.is_ascii_whitespace() {
                    useasis = false;
                    beginwrd = i;
                    state = TR_INSUBS;
                }
            } else if state == TR_INSUBS && c.is_ascii_whitespace() {
                if i == beginwrd {
                    return Err(config_file_error("unexpected end of line or lexeme".into()));
                }
                add_wrd(d, &line[beginwrd..i], idsubst, nwrd as u16, posinsubst as u16, useasis)?;
                nwrd += 1;
                state = TR_WAITSUBS;
            }
            i += mblen(&line[i..]);
        }

        if state == TR_INSUBS {
            if i == beginwrd {
                return Err(config_file_error("unexpected end of line or lexeme".into()));
            }
            add_wrd(d, &line[beginwrd..i], idsubst, nwrd as u16, posinsubst as u16, useasis)?;
            nwrd += 1;
        }

        idsubst += 1;

        if nwrd == 0 || posinsubst == 0 {
            return Err(config_file_error("unexpected end of line".into()));
        }
        if nwrd > u16::MAX as u32 || posinsubst > u16::MAX as u32 {
            return Err(config_file_error("too many lexemes in thesaurus entry".into()));
        }
    }

    d.nsubst = idsubst as i32;
    d.subst.truncate(d.nsubst as usize);
    Ok(())
}

fn subdict_lexize_static<'m>(
    subdict: &TSDictionaryCacheEntry,
    mcx: Mcx<'m>,
    word: &[u8],
) -> PgResult<Option<&'m LexizeResult<'m>>> {
    let w = subdict.call_lexize(mcx, word, None)?;
    // SAFETY: result allocated in `mcx`, live for 'm.
    Ok(unsafe { lexize_result_ref(w) })
}

fn add_compiled_lexeme(
    mcx: Mcx<'static>,
    arena: &mut PgVec<'static, LexemeInfo>,
    newwrds: &mut PgVec<'static, TheLexeme>,
    lexeme: Option<&TsLexeme<'_>>,
    src: usize,
    tnvariant: u16,
) -> PgResult<()> {
    let (lex, tnv) = match lexeme {
        Some(l) => (Some(copy_bytes(mcx, &l.lexeme)?), tnvariant),
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
    );
    newwrds.push(TheLexeme { lexeme: lex, entries: Some(entries) });
    Ok(())
}

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

fn cmp_lexeme(a: &TheLexeme, b: &TheLexeme) -> i32 {
    match (&a.lexeme, &b.lexeme) {
        (None, None) => 0,
        (None, Some(_)) => 1,
        (Some(_), None) => -1,
        (Some(sa), Some(sb)) => match sa.as_slice().cmp(sb.as_slice()) {
            core::cmp::Ordering::Less => -1,
            core::cmp::Ordering::Equal => 0,
            core::cmp::Ordering::Greater => 1,
        },
    }
}

fn compile_the_lexeme(d: &mut DictThesaurus) -> PgResult<()> {
    let mcx = d.mcx;
    let mut newwrds: PgVec<'static, TheLexeme> = PgVec::new_in(mcx);
    let wrds = core::mem::replace(&mut d.wrds, PgVec::new_in(mcx));

    let scratch = MemoryContext::new("thesaurus compile");
    for wrd in wrds.iter() {
        let entries = wrd.entries.expect("rule lexeme has entries");
        let lexeme = wrd.lexeme.as_ref().map(|v| v.as_slice()).unwrap_or(b"");

        if lexeme == b"?" {
            add_compiled_lexeme(mcx, &mut d.arena, &mut newwrds, None, entries, 0)?;
            continue;
        }
        let res = subdict_lexize_static(&d.subdict, scratch.mcx(), lexeme)?;
        match res {
            None => {
                return Err(config_file_error(format!(
                    "thesaurus sample word \"{}\" isn't recognized by subdictionary (rule {})",
                    String::from_utf8_lossy(lexeme),
                    d.arena[entries].idsubst + 1
                )));
            }
            Some(LexizeResult(arr)) if arr.is_empty() => {
                return Err(Box::new(
                    PgError::error(format!(
                        "thesaurus sample word \"{}\" is a stop word (rule {})",
                        String::from_utf8_lossy(lexeme),
                        d.arena[entries].idsubst + 1
                    ))
                    .with_sqlstate(ERRCODE_CONFIG_FILE_ERROR)
                    .with_hint("Use \"?\" to represent a stop word within a sample phrase."),
                ));
            }
            Some(LexizeResult(arr)) => {
                let mut p = 0usize;
                while p < arr.len() {
                    let curvar = arr[p].nvariant;
                    let mut remp = p + 1;
                    let mut tnvar: u16 = 1;
                    while remp < arr.len() && arr[remp].nvariant == arr[remp - 1].nvariant {
                        tnvar += 1;
                        remp += 1;
                    }
                    let mut q = p;
                    while q < arr.len() && arr[q].nvariant == curvar {
                        add_compiled_lexeme(
                            mcx,
                            &mut d.arena,
                            &mut newwrds,
                            Some(&arr[q]),
                            entries,
                            tnvar,
                        )?;
                        q += 1;
                    }
                    p = q;
                }
            }
        }
    }

    d.wrds = newwrds;

    if d.wrds.len() > 1 {
        // Sort reads the arena while reordering wrds: snapshot for the borrow.
        let arena_snapshot: Vec<LexemeInfo> = d.arena.iter().copied().collect();
        d.wrds.sort_by(|a, b| {
            let r = match cmp_lexeme(a, b) {
                0 => -cmp_lexeme_info(&arena_snapshot, a.entries, b.entries),
                r => r,
            };
            r.cmp(&0)
        });

        let mut out: PgVec<'static, TheLexeme> = PgVec::new_in(mcx);
        let src = core::mem::replace(&mut d.wrds, PgVec::new_in(mcx));
        for ptrw in src {
            match out.last_mut() {
                Some(neww) if cmp_lexeme(&ptrw, neww) == 0 => {
                    if cmp_lexeme_info(&arena_snapshot, ptrw.entries, neww.entries) != 0 {
                        if let Some(pe) = ptrw.entries {
                            d.arena[pe].nextentry = neww.entries;
                        }
                        neww.entries = ptrw.entries;
                    }
                }
                _ => out.push(ptrw),
            }
        }
        d.wrds = out;
    }
    Ok(())
}

fn compile_the_substitute(d: &mut DictThesaurus) -> PgResult<()> {
    let mcx = d.mcx;
    let scratch = MemoryContext::new("thesaurus compile");
    for i in 0..d.subst.len() {
        let rem = core::mem::replace(&mut d.subst[i].res, PgVec::new_in(mcx));
        let mut out: PgVec<'static, TsLexeme<'static>> = PgVec::new_in(mcx);

        for inlex in rem.iter() {
            if inlex.flags & DT_USEASIS != 0 {
                let toset: isize = if out.is_empty() { -1 } else { out.len() as isize };
                out.push(TsLexeme {
                    nvariant: inlex.nvariant,
                    flags: 0,
                    lexeme: copy_bytes(mcx, &inlex.lexeme)?,
                });
                if toset > 0 {
                    out[toset as usize].flags |= TSL_ADDPOS;
                }
                continue;
            }
            let lexized = subdict_lexize_static(&d.subdict, scratch.mcx(), &inlex.lexeme)?
                .map(|r| r.0.as_slice());

            match lexized {
                Some(lx) if !lx.is_empty() => {
                    let toset: isize = if out.is_empty() { -1 } else { out.len() as isize };
                    for lex in lx {
                        out.push(TsLexeme {
                            nvariant: lex.nvariant,
                            flags: lex.flags,
                            lexeme: copy_bytes(mcx, &lex.lexeme)?,
                        });
                    }
                    if toset > 0 {
                        out[toset as usize].flags |= TSL_ADDPOS;
                    }
                }
                Some(_) => {
                    return Err(config_file_error(format!(
                        "thesaurus substitute word \"{}\" is a stop word (rule {})",
                        String::from_utf8_lossy(&inlex.lexeme),
                        i + 1
                    )));
                }
                None => {
                    return Err(config_file_error(format!(
                        "thesaurus substitute word \"{}\" isn't recognized by subdictionary (rule {})",
                        String::from_utf8_lossy(&inlex.lexeme),
                        i + 1
                    )));
                }
            }
        }

        if out.is_empty() {
            return Err(config_file_error(format!(
                "thesaurus substitute phrase is empty (rule {})",
                i + 1
            )));
        }
        d.subst[i].reslen = out.len() as u16;
        d.subst[i].res = out;
    }
    Ok(())
}

pub fn thesaurus_init(init: &DictInitData<'static>) -> PgResult<DictThesaurus> {
    let mcx = init.mcx;
    let mut fileloaded = false;
    let mut filename: Option<&[u8]> = None;
    let mut subdictname: Option<&[u8]> = None;
    for (name, value) in init.dict_options.iter() {
        if name.as_slice() == b"dictfile" {
            if fileloaded {
                return Err(invalid_param("multiple DictFile parameters".into()));
            }
            filename = Some(value.as_slice());
            fileloaded = true;
        } else if name.as_slice() == b"dictionary" {
            if subdictname.is_some() {
                return Err(invalid_param("multiple Dictionary parameters".into()));
            }
            subdictname = Some(value.as_slice());
        } else {
            return Err(invalid_param(format!(
                "unrecognized Thesaurus parameter: \"{}\"",
                String::from_utf8_lossy(name)
            )));
        }
    }
    if !fileloaded {
        return Err(invalid_param("missing DictFile parameter".into()));
    }
    let Some(subdictname) = subdictname else {
        return Err(invalid_param("missing Dictionary parameter".into()));
    };

    let scratch = MemoryContext::new("thesaurus init");
    let sub = String::from_utf8_lossy(subdictname).into_owned();
    let names = adt_regproc::string_to_qualified_name_list(scratch.mcx(), &sub, None)?
        .expect("hard-error name list");
    let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
    let subdict_oid = ::ts_cache::get_ts_dict_oid(&refs, false)?;
    let subdict = lookup_ts_dictionary_cache(subdict_oid)?;

    let mut d = DictThesaurus {
        mcx,
        subdict_oid,
        subdict,
        wrds: PgVec::new_in(mcx),
        subst: PgVec::new_in(mcx),
        nsubst: 0,
        arena: PgVec::new_in(mcx),
    };
    thesaurus_read(&mut d, filename.expect("checked above"))?;
    compile_the_lexeme(&mut d)?;
    compile_the_substitute(&mut d)?;
    Ok(d)
}

fn find_the_lexeme(d: &DictThesaurus, lexeme: Option<&[u8]>) -> Option<usize> {
    if d.wrds.is_empty() {
        return None;
    }
    let mut lo = 0isize;
    let mut hi = d.wrds.len() as isize - 1;
    while lo <= hi {
        let mid = ((lo + hi) / 2) as usize;
        let b = &d.wrds[mid];
        let c = match (lexeme, &b.lexeme) {
            (None, None) => 0,
            (None, Some(_)) => 1,
            (Some(_), None) => -1,
            (Some(k), Some(sb)) => match k.cmp(sb.as_slice()) {
                core::cmp::Ordering::Less => -1,
                core::cmp::Ordering::Equal => 0,
                core::cmp::Ordering::Greater => 1,
            },
        };
        match c {
            0 => return d.wrds[mid].entries,
            c if c < 0 => hi = mid as isize - 1,
            _ => lo = mid as isize + 1,
        }
    }
    None
}

fn match_id_subst(arena: &[LexemeInfo], stored: Option<usize>, idsubst: u32) -> bool {
    let Some(stored) = stored else {
        return true;
    };
    let mut s = Some(stored);
    while let Some(idx) = s {
        if arena[idx].idsubst == idsubst {
            return true;
        }
        s = arena[idx].nextvariant;
    }
    false
}

fn find_variant(
    arena: &mut [LexemeInfo],
    mut acc: Option<usize>,
    stored: Option<usize>,
    curpos: u16,
    newin: &mut [Option<usize>],
    newn: i32,
) -> Option<usize> {
    loop {
        let mut i: i32 = 0;
        let mut ptr = newin[0];

        while i < newn {
            let iu = i as usize;
            while let Some(ni) = newin[iu] {
                let pid = ptr.map(|p| arena[p].idsubst).unwrap_or(0);
                if arena[ni].idsubst < pid {
                    newin[iu] = arena[ni].nextentry;
                } else {
                    break;
                }
            }
            let Some(ni) = newin[iu] else {
                return acc;
            };
            let pid = ptr.map(|p| arena[p].idsubst).unwrap_or(0);
            if arena[ni].idsubst > pid {
                ptr = newin[iu];
                i = 0;
                continue;
            }

            loop {
                let Some(ni) = newin[iu] else {
                    return acc;
                };
                let pid = ptr.map(|p| arena[p].idsubst).unwrap_or(0);
                if arena[ni].idsubst != pid {
                    break;
                }
                if arena[ni].posinsubst == curpos && arena[ni].tnvariant as i32 == newn {
                    ptr = newin[iu];
                    break;
                }
                newin[iu] = arena[ni].nextentry;
                if newin[iu].is_none() {
                    return acc;
                }
            }

            let ni = newin[iu].expect("checked non-none above");
            let pid = ptr.map(|p| arena[p].idsubst).unwrap_or(0);
            if arena[ni].idsubst != pid {
                ptr = newin[iu];
                i = 0;
                continue;
            }
            i += 1;
        }

        if i == newn {
            let pid = ptr.map(|p| arena[p].idsubst).unwrap_or(0);
            if match_id_subst(arena, stored, pid)
                && (acc.is_none() || !match_id_subst(arena, acc, pid))
            {
                if let Some(p) = ptr {
                    arena[p].nextvariant = acc;
                }
                acc = ptr;
            }
        }

        for slot in newin.iter_mut().take(newn as usize) {
            if let Some(s) = *slot {
                *slot = arena[s].nextentry;
            }
        }
    }
}

fn copy_ts_lexeme<'m>(
    mcx: Mcx<'m>,
    ts: &TheSubstitute,
) -> PgResult<PgVec<'m, TsLexeme<'m>>> {
    let mut res = PgVec::new_in(mcx);
    res.try_reserve_exact(ts.reslen as usize)
        .map_err(|_| mcx.oom(ts.reslen as usize))?;
    for lex in ts.res[..ts.reslen as usize].iter() {
        let mut lexeme = vec_with_capacity_in(mcx, lex.lexeme.len())?;
        lexeme.extend_from_slice(&lex.lexeme);
        res.push(TsLexeme { nvariant: lex.nvariant, flags: lex.flags, lexeme });
    }
    Ok(res)
}

fn check_match<'m>(
    mcx: Mcx<'m>,
    d: &DictThesaurus,
    info: Option<usize>,
    curpos: u16,
    moreres: &mut bool,
) -> PgResult<Option<PgVec<'m, TsLexeme<'m>>>> {
    *moreres = false;
    let mut info = info;
    while let Some(idx) = info {
        let node = d.arena[idx];
        debug_assert!(node.idsubst < d.nsubst as u32);
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

// DictSubState.private_state carries the stored LexemeInfo chain head as
// arena index + 1 (0 = NULL).
fn stored_from_state(state: &DictSubState) -> Option<usize> {
    let w = state.private_state as usize;
    if w == 0 {
        None
    } else {
        Some(w - 1)
    }
}

pub fn thesaurus_lexize<'m>(
    mcx: Mcx<'m>,
    d: &mut DictThesaurus,
    token: &[u8],
    state: &mut DictSubState,
) -> PgResult<Option<PgVec<'m, TsLexeme<'m>>>> {
    if state.isend {
        return Ok(None);
    }
    let stored = stored_from_state(state);
    let mut curpos: u16 = 0;
    if let Some(s) = stored {
        curpos = d.arena[s].posinsubst + 1;
    }

    if !d.subdict.isvalid.get() {
        d.subdict = lookup_ts_dictionary_cache(d.subdict_oid)?;
    }
    let res = subdict_lexize_static(&d.subdict, mcx, token)?;

    let mut info: Option<usize> = None;
    match res {
        Some(LexizeResult(arr)) if !arr.is_empty() => {
            let mut p = 0usize;
            while p < arr.len() {
                let nv = arr[p].nvariant;
                let basevar = p;
                let mut nlex = 0usize;
                while p < arr.len() && arr[p].nvariant == nv {
                    nlex += 1;
                    p += 1;
                }
                let mut infos: Vec<Option<usize>> = vec![None; nlex];
                let mut i = 0usize;
                while i < nlex {
                    infos[i] = find_the_lexeme(d, Some(&arr[basevar + i].lexeme));
                    if infos[i].is_none() {
                        break;
                    }
                    i += 1;
                }
                if i < nlex {
                    continue;
                }
                info = find_variant(&mut d.arena, info, stored, curpos, &mut infos, nlex as i32);
            }
        }
        Some(_) => {
            let mut infos = [find_the_lexeme(d, None)];
            info = find_variant(&mut d.arena, None, stored, curpos, &mut infos, 1);
        }
        None => {
            info = None;
        }
    }

    state.private_state = info.map_or(core::ptr::null_mut(), |i| (i + 1) as *mut core::ffi::c_void);

    if info.is_none() {
        state.getnext = false;
        return Ok(None);
    }

    let mut moreres = false;
    if let Some(matched) = check_match(mcx, d, info, curpos, &mut moreres)? {
        state.getnext = moreres;
        return Ok(Some(matched));
    }
    state.getnext = true;
    Ok(None)
}
