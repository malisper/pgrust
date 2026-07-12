//! `contrib/unaccent/unaccent.c` — the unaccent text search dictionary
//! template plus the `unaccent()` SQL wrapper. C's pointer trie (256
//! `TrieChar` per node) is an index-linked arena here; same byte-at-a-time
//! longest-match search, `next`/`replace` 1-based (0 = C's NULL).

use ::mcx::{alloc_in, vec_with_capacity_in, Mcx, PgVec};
use ::ts_locale::dict_api::{lexize_result_ref, DictInitData, LexizeResult};
use ::ts_locale::{byte_isspace, get_tsearch_config_filename, TsLexeme, TSL_FILTER};
use ::types_core::OidIsValid;
use ::types_error::{
    PgError, PgResult, ERRCODE_CONFIG_FILE_ERROR, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_UNDEFINED_OBJECT, ERRCODE_UNTRANSLATABLE_CHARACTER,
};
use ::types_fmgr::{varlena_result, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};
use datum::Datum;

const LIBRARY: &str = "unaccent";

#[derive(Clone, Copy)]
struct TrieCell {
    next: u32,
    replace: u32,
}

pub struct UnaccentTrie {
    nodes: PgVec<'static, PgVec<'static, TrieCell>>,
    replacements: PgVec<'static, PgVec<'static, u8>>,
}

impl UnaccentTrie {
    fn new_node(&mut self, mcx: Mcx<'static>) -> PgResult<usize> {
        let mut node = vec_with_capacity_in(mcx, 256)?;
        node.resize(256, TrieCell { next: 0, replace: 0 });
        self.nodes.push(node);
        Ok(self.nodes.len() - 1)
    }

    fn place(&mut self, mcx: Mcx<'static>, src: &[u8], replace_to: &[u8]) -> PgResult<()> {
        debug_assert!(!src.is_empty());
        if self.nodes.is_empty() {
            self.new_node(mcx)?;
        }
        let mut node = 0usize;
        for (i, &b) in src.iter().enumerate() {
            let b = b as usize;
            if i == src.len() - 1 {
                if self.nodes[node][b].replace != 0 {
                    let _ = ::elog::ThrowErrorData(
                        PgError::warning("duplicate source strings, first one will be used")
                            .with_sqlstate(ERRCODE_CONFIG_FILE_ERROR),
                    );
                } else {
                    let mut r = vec_with_capacity_in(mcx, replace_to.len())?;
                    r.extend_from_slice(replace_to);
                    self.replacements.push(r);
                    self.nodes[node][b].replace = self.replacements.len() as u32;
                }
            } else {
                if self.nodes[node][b].next == 0 {
                    let new = self.new_node(mcx)?;
                    self.nodes[node][b].next = new as u32 + 1;
                }
                node = (self.nodes[node][b].next - 1) as usize;
            }
        }
        Ok(())
    }

    fn find_replace_to(&self, src: &[u8]) -> Option<(usize, usize)> {
        if self.nodes.is_empty() {
            return None;
        }
        let mut result = None;
        let mut node = 0usize;
        let mut matchlen = 0usize;
        loop {
            if matchlen >= src.len() {
                break;
            }
            let cell = self.nodes[node][src[matchlen] as usize];
            matchlen += 1;
            if cell.replace != 0 {
                result = Some((cell.replace as usize - 1, matchlen));
            }
            if cell.next == 0 {
                break;
            }
            node = cell.next as usize - 1;
        }
        result
    }
}

fn config_warning(msg: &str) {
    let _ = ::elog::ThrowErrorData(
        PgError::warning(msg).with_sqlstate(ERRCODE_CONFIG_FILE_ERROR),
    );
}

fn read_rules_lines<'mcx>(
    mcx: Mcx<'mcx>,
    filename: &[u8],
) -> PgResult<Result<PgVec<'mcx, PgVec<'mcx, u8>>, std::io::Error>> {
    let path = String::from_utf8_lossy(filename).into_owned();
    let raw = match std::fs::read(&path) {
        Ok(b) => b,
        Err(e) => return Ok(Err(e)),
    };
    let mut lines: PgVec<'mcx, PgVec<'mcx, u8>> = PgVec::new_in(mcx);
    for chunk in raw.split_inclusive(|&b| b == b'\n') {
        match ::mbutils::pg_any_to_server(mcx, chunk, ::wchar::PG_UTF8) {
            Ok(Some(v)) => lines.push(v),
            Ok(None) => {
                let mut v = vec_with_capacity_in(mcx, chunk.len())?;
                v.extend_from_slice(chunk);
                lines.push(v);
            }
            Err(e) if e.sqlstate() == ERRCODE_UNTRANSLATABLE_CHARACTER => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(Ok(lines))
}

// C initTrie's line parser, states as in C: 0 initial, 1 in src, 2 after
// src, 3 in trg, 4 in quoted trg, 5 after trg, -1/-2 syntax errors.
fn parse_rule_line(line: &[u8]) -> Result<Option<(&[u8], Vec<u8>)>, i32> {
    let mut state: i32 = 0;
    let mut src: (usize, usize) = (0, 0);
    let mut trg: (usize, usize) = (0, 0);
    let mut trgquoted = false;

    let mut i = 0usize;
    while i < line.len() {
        let ptrlen = ::mbutils::pg_mblen(&line[i..]) as usize;
        if byte_isspace(line[i]) {
            if state == 1 {
                state = 2;
            } else if state == 3 {
                state = 5;
            }
            if state != 4 {
                i += ptrlen;
                continue;
            }
        }
        match state {
            0 => {
                src = (i, i + ptrlen);
                state = 1;
            }
            1 => {
                src.1 = i + ptrlen;
            }
            2 => {
                if line[i] == b'"' {
                    trgquoted = true;
                    state = 4;
                } else {
                    state = 3;
                }
                trg = (i, i + ptrlen);
            }
            3 => {
                trg.1 = i + ptrlen;
            }
            4 => {
                trg.1 = i + ptrlen;
                if line[i] == b'"' {
                    if line.get(i + 1) == Some(&b'"') {
                        i += ptrlen;
                        trg.1 += 1;
                    } else {
                        state = 5;
                    }
                }
            }
            _ => {
                state = -1;
            }
        }
        i += ptrlen;
    }

    if state == 1 || state == 2 {
        trg = (0, 0);
    }
    if state == 4 {
        state = -2;
    }
    if state <= 0 && state != 0 {
        return Err(state);
    }
    if state == 0 {
        return Ok(None);
    }

    let trg_bytes = &line[trg.0..trg.1];
    let trgstore = if trgquoted {
        let inner = &trg_bytes[1..trg_bytes.len() - 1];
        let mut out = Vec::with_capacity(inner.len());
        let mut j = 0usize;
        while j < inner.len() {
            out.push(inner[j]);
            if inner[j] == b'"' && inner.get(j + 1) == Some(&b'"') {
                j += 1;
            }
            j += 1;
        }
        out
    } else {
        trg_bytes.to_vec()
    };

    Ok(Some((&line[src.0..src.1], trgstore)))
}

fn init_trie(mcx: Mcx<'static>, filename: &[u8]) -> PgResult<UnaccentTrie> {
    let path = get_tsearch_config_filename(mcx, filename, "rules")?;
    let lines = match read_rules_lines(mcx, &path)? {
        Ok(lines) => lines,
        Err(e) => {
            // C: could not open unaccent file "%s": %m — strerror(errno).
            let errno_text = e
                .raw_os_error()
                .map(strerror_text)
                .unwrap_or_else(|| e.to_string());
            return Err(PgError::error(format!(
                "could not open unaccent file \"{}\": {errno_text}",
                String::from_utf8_lossy(&path)
            ))
            .with_sqlstate(ERRCODE_CONFIG_FILE_ERROR)
            .into());
        }
    };
    let mut trie = UnaccentTrie {
        nodes: PgVec::new_in(mcx),
        replacements: PgVec::new_in(mcx),
    };
    for line in lines.iter() {
        match parse_rule_line(line) {
            Ok(Some((src, trg))) => trie.place(mcx, src, &trg)?,
            Ok(None) => {}
            Err(-1) => {
                config_warning("invalid syntax: more than two strings in unaccent rule")
            }
            Err(_) => {
                config_warning("invalid syntax: unfinished quoted string in unaccent rule")
            }
        }
    }
    Ok(trie)
}

fn strerror_text(errno: i32) -> String {
    // SAFETY: strerror returns a static NUL-terminated string for any errno.
    unsafe { core::ffi::CStr::from_ptr(libc::strerror(errno)) }
        .to_string_lossy()
        .into_owned()
}

fn invalid_param(msg: impl Into<String>) -> Box<PgError> {
    Box::new(PgError::error(msg.into()).with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE))
}

pub fn unaccent_init(init: &DictInitData<'static>) -> PgResult<UnaccentTrie> {
    let mut root: Option<UnaccentTrie> = None;
    for (name, value) in init.dict_options.iter() {
        if name.as_slice() == b"rules" {
            if root.is_some() {
                return Err(invalid_param("multiple Rules parameters"));
            }
            root = Some(init_trie(init.mcx, value.as_slice())?);
        } else {
            return Err(invalid_param(format!(
                "unrecognized Unaccent parameter: \"{}\"",
                String::from_utf8_lossy(name)
            )));
        }
    }
    let Some(trie) = root else {
        return Err(invalid_param("missing Rules parameter"));
    };
    Ok(trie)
}

pub fn unaccent_lexize<'mcx>(
    mcx: Mcx<'mcx>,
    trie: &UnaccentTrie,
    token: &[u8],
) -> PgResult<Option<LexizeResult<'mcx>>> {
    let mut buf: Option<PgVec<'mcx, u8>> = None;
    let mut i = 0usize;
    while i < token.len() {
        if let Some((ridx, matchlen)) = trie.find_replace_to(&token[i..]) {
            if buf.is_none() {
                let mut b = vec_with_capacity_in(mcx, token.len())?;
                b.extend_from_slice(&token[..i]);
                buf = Some(b);
            }
            buf.as_mut()
                .expect("just initialized")
                .extend_from_slice(&trie.replacements[ridx]);
            i += matchlen;
        } else {
            let matchlen = ::mbutils::pg_mblen_range(&token[i..])?.max(1) as usize;
            if let Some(b) = buf.as_mut() {
                b.extend_from_slice(&token[i..i + matchlen]);
            }
            i += matchlen;
        }
    }

    match buf {
        Some(lexeme) => {
            let mut out = PgVec::new_in(mcx);
            out.push(TsLexeme { nvariant: 0, flags: TSL_FILTER, lexeme });
            Ok(Some(LexizeResult(out)))
        }
        None => Ok(None),
    }
}


fn arg_dict_ptr(fcinfo: &Fcinfo) -> usize {
    fcinfo.arg(0).as_usize()
}

fn arg_token<'a>(fcinfo: &'a Fcinfo) -> &'a [u8] {
    let len = fcinfo.arg(2).as_i32().max(0) as usize;
    // SAFETY: dict_api lexize convention — arg1 points at `len` live bytes.
    unsafe { core::slice::from_raw_parts(fcinfo.arg(1).as_usize() as *const u8, len) }
}

fn fc_unaccent_init(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: arg0 is the DictInitData built by the ts_cache dictionary loader.
    let init = unsafe { &*(arg_dict_ptr(fcinfo) as *const DictInitData<'static>) };
    let trie = unaccent_init(init)?;
    let (ptr, _) = ::mcx::PgBox::into_raw_with_allocator(alloc_in(init.mcx, trie)?);
    Ok(Datum::from_usize(ptr as usize))
}

fn fc_unaccent_lexize(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: arg0 came from fc_unaccent_init and outlives the cache entry.
    let trie = unsafe { &*(arg_dict_ptr(fcinfo) as *const UnaccentTrie) };
    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    match unaccent_lexize(mcx, trie, arg_token(fcinfo))? {
        None => Ok(Datum::from_usize(0)),
        Some(r) => {
            let (ptr, _) = ::mcx::PgBox::into_raw_with_allocator(alloc_in(mcx, r)?);
            Ok(Datum::from_usize(ptr as usize))
        }
    }
}

fn fc_unaccent_dict(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (dict_oid, str_arg) = if fcinfo.nargs == 1 {
        let flinfo = flinfo.as_ref().expect("unaccent(text): resolved FmgrInfo required");
        let procnspid = lsyscache::get_func_namespace(flinfo.fn_oid)?;
        let dict_oid =
            syscache_seams::lookup_pg_ts_dict_oid_by_name_nsp::call("unaccent", procnspid)?;
        if !OidIsValid(dict_oid) {
            let nspname = lsyscache::get_namespace_name(fcinfo.result_mcx(), procnspid)?
                .map(|s| s.as_str().to_string())
                .unwrap_or_default();
            return Err(Box::new(
                PgError::error(format!(
                    "text search dictionary \"{nspname}.unaccent\" does not exist"
                ))
                .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
            ));
        }
        (dict_oid, 0)
    } else {
        (fcinfo.arg(0).as_oid(), 1)
    };

    // SAFETY: both SQL signatures are strict; the text arg is non-null.
    let token = unsafe { fcinfo.arg_varlena_packed(str_arg)? }.data().to_vec();
    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };

    let dict = ts_cache::lookup_ts_dictionary_cache(dict_oid)?;
    let res_word = dict.call_lexize(mcx, &token, None)?;

    // SAFETY: the result word lives in `mcx`.
    let out: &[u8] = match unsafe { lexize_result_ref(res_word) } {
        Some(LexizeResult(v)) if !v.is_empty() => &v[0].lexeme,
        _ => &token,
    };
    Ok(varlena_result(varlena::cstring_to_text(mcx, out)?))
}

fn lookup(function: &str) -> Option<PGFunction> {
    Some(match function {
        "unaccent_init" => fc_unaccent_init,
        "unaccent_lexize" => fc_unaccent_lexize,
        "unaccent_dict" => fc_unaccent_dict,
        _ => return None,
    })
}

pub fn init_seams() {
    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        // unaccent.c's PG_MODULE_MAGIC_EXT has no _PG_init.
        pg_init: None,
    });
}

#[cfg(test)]
mod tests;
