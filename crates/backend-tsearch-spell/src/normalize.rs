//! Word normalization: affix stripping (`NINormalizeWord` and helpers) and
//! Hunspell compound splitting (`SplitToVariants` and helpers).
//!
//! Port of the spell.c runtime routines: `FindAffixes`, `CheckAffix`,
//! `addToResult`, `NormalizeSubWord`, `CheckCompoundAffixes`, `CopyVar`,
//! `AddStem`, `SplitToVariants`, `addNorm`, `NINormalizeWord`.

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use mcx::{Mcx, PgString, PgVec};
use types_error::PgResult;
use types_regex::RegexecResult;
use types_tsearch::TSLexeme;

use crate::{
    check_stack_depth, Affix, AffixReg, IspellDict, FF_COMPOUNDBEGIN, FF_COMPOUNDFORBIDFLAG,
    FF_COMPOUNDLAST, FF_COMPOUNDMIDDLE, FF_COMPOUNDONLY, FF_CROSSPRODUCT, FF_PREFIX, FF_SUFFIX,
    MAXNORMLEN, MAX_NORM,
};

/// `GETWCHAR(W,L,N,T)`: the byte at position `n`, front for prefix, back for
/// suffix. `w` is the word, `l` its byte length.
#[inline]
fn getwchar(w: &[u8], l: i32, n: i32, t: i32) -> u8 {
    let idx = if t == FF_PREFIX { n } else { l - 1 - n };
    w[idx as usize]
}

/// `SplitVar` — a compound-split variant (the C linked-list node, here an
/// element of a `Vec`). `stem` holds owned stem strings.
struct SplitVar {
    stem: Vec<Vec<u8>>,
}

impl SplitVar {
    fn new() -> Self {
        SplitVar { stem: Vec::new() }
    }
    /// `CopyVar(s, makedup)` with `makedup` always cloning owned vecs (the owned
    /// model never aliases, so the `makedup==0` shallow copy is also a clone).
    fn copy_from(other: &SplitVar) -> Self {
        SplitVar {
            stem: other.stem.clone(),
        }
    }
    /// `AddStem(v, word)`.
    fn add_stem(&mut self, word: Vec<u8>) {
        self.stem.push(word);
    }
    fn nstem(&self) -> usize {
        self.stem.len()
    }
}

impl<'mcx> IspellDict<'mcx> {
    /* ---- FindAffixes ---- */

    /// `FindAffixes`: walk the affix trie from `node` matching `word`'s edge,
    /// returning `(arena_node_idx, slot_index)` of the matched [`AffixNodeData`]
    /// that carries affixes, plus the new `level`. `None` if no match.
    fn find_affixes(
        &self,
        mut node: Option<usize>,
        word: &[u8],
        wrdlen: i32,
        level: &mut i32,
        type_: i32,
    ) -> Option<(usize, usize)> {
        // Void-node handling: the synthetic prepended node.
        if let Some(ni) = node {
            if self.af_arena[ni].isvoid {
                let slot = &self.af_arena[ni].data[0];
                if slot.naff() != 0 {
                    return Some((ni, 0));
                }
                node = slot.node;
            }
        }

        while let Some(ni) = node {
            if *level >= wrdlen {
                break;
            }
            let data = &self.af_arena[ni].data;
            let symbol = getwchar(word, wrdlen, *level, type_);
            let mut lo = 0usize;
            let mut hi = data.len();
            let mut matched = false;
            while lo < hi {
                let mid = lo + (hi - lo) / 2;
                let d = &data[mid];
                match d.val.cmp(&symbol) {
                    core::cmp::Ordering::Equal => {
                        *level += 1;
                        if d.naff() != 0 {
                            return Some((ni, mid));
                        }
                        node = d.node;
                        matched = true;
                        break;
                    }
                    core::cmp::Ordering::Less => lo = mid + 1,
                    core::cmp::Ordering::Greater => hi = mid,
                }
            }
            if !matched {
                break;
            }
        }
        None
    }

    /* ---- CheckAffix ---- */

    /// `CheckAffix`: apply affix `affix` to `word` (whose byte length is `len`),
    /// producing the candidate base form in `out` if the compound flags permit
    /// and the affix's matcher accepts. Returns `(matched, new_baselen)`; when
    /// `matched`, `out` holds the result.
    fn check_affix(
        &self,
        word: &[u8],
        len: usize,
        affix: &Affix,
        flagflags: i32,
        out: &mut Vec<u8>,
        baselen: Option<i32>,
    ) -> PgResult<(bool, Option<i32>)> {
        let mut new_baselen = baselen;

        /* Check compound allow flags */
        if flagflags == 0 {
            if affix.flagflags & FF_COMPOUNDONLY != 0 {
                return Ok((false, new_baselen));
            }
        } else if flagflags & FF_COMPOUNDBEGIN != 0 {
            if affix.flagflags & FF_COMPOUNDFORBIDFLAG != 0 {
                return Ok((false, new_baselen));
            }
            if (affix.flagflags & FF_COMPOUNDBEGIN) == 0 && affix.type_ == FF_SUFFIX {
                return Ok((false, new_baselen));
            }
        } else if flagflags & FF_COMPOUNDMIDDLE != 0 {
            if (affix.flagflags & FF_COMPOUNDMIDDLE) == 0
                || (affix.flagflags & FF_COMPOUNDFORBIDFLAG) != 0
            {
                return Ok((false, new_baselen));
            }
        } else if flagflags & FF_COMPOUNDLAST != 0 {
            if affix.flagflags & FF_COMPOUNDFORBIDFLAG != 0 {
                return Ok((false, new_baselen));
            }
            if (affix.flagflags & FF_COMPOUNDLAST) == 0 && affix.type_ == FF_PREFIX {
                return Ok((false, new_baselen));
            }
        }

        let replen = affix.repl.len();

        /* make the replace pattern of the affix */
        out.clear();
        if affix.type_ == FF_SUFFIX {
            // strcpy(newword, word); strcpy(newword + len - replen, find);
            out.extend_from_slice(&word[..len]);
            out.truncate(len - replen);
            out.extend_from_slice(&affix.find);
            if baselen.is_some() {
                new_baselen = Some((len - replen) as i32);
            }
        } else {
            if let Some(bl) = baselen {
                if (bl as usize + affix.find.len()) <= replen {
                    return Ok((false, new_baselen));
                }
            }
            // strcpy(newword, find); strcat(newword, word + replen);
            out.extend_from_slice(&affix.find);
            out.extend_from_slice(&word[replen..]);
        }

        /* check the resulting word */
        match &affix.reg {
            AffixReg::Simple => Ok((true, new_baselen)),
            AffixReg::Regis(regis) => {
                if crate::rs_execute(regis, out)? {
                    Ok((true, new_baselen))
                } else {
                    Ok((false, new_baselen))
                }
            }
            AffixReg::Regex(handle) => {
                // Convert the candidate to wide characters, then execute.
                let data =
                    backend_utils_mb_mbutils_seams::pg_mb2wchar_with_len::call(self.mcx, out)?;
                let res = backend_regex_core_seams::pg_regexec::call(*handle, &data, 0, &mut [])?;
                match res {
                    RegexecResult::Matched => Ok((true, new_baselen)),
                    RegexecResult::NoMatch => Ok((false, new_baselen)),
                    RegexecResult::Failed(f) => Err(backend_utils_error::ereport(types_error::ERROR)
                        .errcode(types_error::ERRCODE_INVALID_REGULAR_EXPRESSION)
                        .errmsg(alloc::format!("regular expression failed: {}", f.message))
                        .into_error()),
                }
            }
        }
    }

    /* ---- addToResult ---- */

    /// `addToResult`: append `word` to `forms` if not a duplicate of the last
    /// entry and the cap is not reached. Returns whether it was added.
    fn add_to_result(forms: &mut Vec<Vec<u8>>, word: &[u8]) -> bool {
        if forms.len() >= MAX_NORM - 1 {
            return false;
        }
        if forms.is_empty() || forms.last().map(|w| w.as_slice()) != Some(word) {
            forms.push(word.to_vec());
            return true;
        }
        false
    }

    /* ---- NormalizeSubWord ---- */

    /// `NormalizeSubWord`: produce every normal form of `word` reachable by
    /// stripping prefixes/suffixes, subject to the compound `flag`. Returns the
    /// owned list of forms (empty when none — the C `NULL`).
    fn normalize_sub_word(&self, word: &[u8], flag: i32) -> PgResult<Vec<Vec<u8>>> {
        let wrdlen = word.len() as i32;
        if wrdlen as usize > MAXNORMLEN {
            return Ok(Vec::new());
        }
        let mut forms: Vec<Vec<u8>> = Vec::new();
        let mut newword: Vec<u8> = Vec::new();
        let mut pnewword: Vec<u8> = Vec::new();

        /* Check that the word itself is a normal form */
        if self.find_word(word, &[], flag)? != 0 {
            forms.push(word.to_vec());
        }

        /* Find all other NORMAL forms of `word` (check only prefix) */
        let mut pnode = self.prefix;
        let mut plevel = 0;
        while pnode.is_some() {
            let found = self.find_affixes(pnode, word, wrdlen, &mut plevel, FF_PREFIX);
            let (ni, slot) = match found {
                Some(x) => x,
                None => break,
            };
            let naff = self.af_arena[ni].data[slot].naff();
            for j in 0..naff {
                let aff_idx = self.af_arena[ni].data[slot].aff[j];
                let (ok, _) = self.check_affix(
                    word,
                    wrdlen as usize,
                    &self.affixes[aff_idx],
                    flag,
                    &mut newword,
                    None,
                )?;
                if ok {
                    let affflag = self.affixes[aff_idx].flag.as_slice().to_vec();
                    if self.find_word(&newword, &affflag, flag)? != 0 {
                        Self::add_to_result(&mut forms, &newword);
                    }
                }
            }
            pnode = self.af_arena[ni].data[slot].node;
        }

        /* Find all other NORMAL forms (check suffix and then prefix) */
        let mut snode = self.suffix;
        let mut slevel = 0;
        while snode.is_some() {
            let found = self.find_affixes(snode, word, wrdlen, &mut slevel, FF_SUFFIX);
            let (sni, sslot) = match found {
                Some(x) => x,
                None => break,
            };
            let snaff = self.af_arena[sni].data[sslot].naff();
            for i in 0..snaff {
                let aff_i = self.af_arena[sni].data[sslot].aff[i];
                let (ok, baselen) = self.check_affix(
                    word,
                    wrdlen as usize,
                    &self.affixes[aff_i],
                    flag,
                    &mut newword,
                    Some(0),
                )?;
                if ok {
                    let aff_i_flag = self.affixes[aff_i].flag.as_slice().to_vec();
                    let aff_i_flagflags = self.affixes[aff_i].flagflags;
                    if self.find_word(&newword, &aff_i_flag, flag)? != 0 {
                        Self::add_to_result(&mut forms, &newword);
                    }

                    /* now look at the changed word with prefixes */
                    let swrdlen = newword.len() as i32;
                    let newword_snapshot = newword.clone();
                    let mut ppnode = self.prefix;
                    let mut pplevel = 0;
                    let mut baselen = baselen;
                    while ppnode.is_some() {
                        let pfound = self.find_affixes(
                            ppnode,
                            &newword_snapshot,
                            swrdlen,
                            &mut pplevel,
                            FF_PREFIX,
                        );
                        let (pni, pslot) = match pfound {
                            Some(x) => x,
                            None => break,
                        };
                        let pnaff = self.af_arena[pni].data[pslot].naff();
                        for j in 0..pnaff {
                            let aff_j = self.af_arena[pni].data[pslot].aff[j];
                            let (pok, new_bl) = self.check_affix(
                                &newword_snapshot,
                                swrdlen as usize,
                                &self.affixes[aff_j],
                                flag,
                                &mut pnewword,
                                baselen,
                            )?;
                            baselen = new_bl;
                            if pok {
                                let aff_j_flagflags = self.affixes[aff_j].flagflags;
                                let ff: Vec<u8> = if (aff_j_flagflags
                                    & aff_i_flagflags
                                    & FF_CROSSPRODUCT)
                                    != 0
                                {
                                    Vec::new()
                                } else {
                                    self.affixes[aff_j].flag.as_slice().to_vec()
                                };
                                if self.find_word(&pnewword, &ff, flag)? != 0 {
                                    Self::add_to_result(&mut forms, &pnewword);
                                }
                            }
                        }
                        ppnode = self.af_arena[pni].data[pslot].node;
                    }
                }
            }
            snode = self.af_arena[sni].data[sslot].node;
        }

        Ok(forms)
    }

    /* ---- CheckCompoundAffixes ---- */

    /// `CheckCompoundAffixes`: find a matching compound affix in
    /// `compound_affix[*ptr..]`, advancing `*ptr`. Returns the C `len`/`0`
    /// result, or `-1` when none matches.
    fn check_compound_affixes(
        &self,
        ptr: &mut usize,
        word: &[u8],
        mut len: i32,
        check_in_place: bool,
    ) -> i32 {
        if self.compound_affix.is_empty() {
            return -1;
        }
        if check_in_place {
            while *ptr < self.compound_affix.len() {
                let ca = &self.compound_affix[*ptr];
                if len > ca.len && bncmp_eq(&ca.affix, word, ca.len as usize) {
                    len = ca.len;
                    let issuffix = ca.issuffix;
                    *ptr += 1;
                    return if issuffix { len } else { 0 };
                }
                *ptr += 1;
            }
        } else {
            while *ptr < self.compound_affix.len() {
                let ca = &self.compound_affix[*ptr];
                if let Some(affbegin) = crate::bstrstr(word, &ca.affix) {
                    if len > ca.len {
                        len = ca.len + affbegin as i32;
                        let issuffix = ca.issuffix;
                        *ptr += 1;
                        return if issuffix { len } else { 0 };
                    }
                }
                *ptr += 1;
            }
        }
        -1
    }

    /* ---- SplitToVariants ---- */

    /// `SplitToVariants`: enumerate the compound-split variants of
    /// `word[startpos..]`. Returns the list of variants (the C linked list).
    fn split_to_variants(
        &self,
        snode: Option<usize>,
        orig: Option<&SplitVar>,
        word: &[u8],
        wordlen: i32,
        mut startpos: i32,
        minpos: i32,
    ) -> PgResult<Vec<SplitVar>> {
        check_stack_depth()?;

        let mut node = if snode.is_some() {
            snode
        } else {
            self.dictionary
        };
        let mut level = if snode.is_some() { minpos } else { startpos };

        let mut notprobed = alloc::vec![1u8; wordlen as usize];
        let mut var = match orig {
            Some(o) => SplitVar::copy_from(o),
            None => SplitVar::new(),
        };
        let mut result: Vec<SplitVar> = Vec::new();

        while level < wordlen {
            /* find word with epenthetic and/or compound affix */
            let mut caff = 0usize;
            loop {
                if level <= startpos {
                    break;
                }
                let lenaff0 = self.check_compound_affixes(
                    &mut caff,
                    &word[level as usize..],
                    wordlen - level,
                    node.is_some(),
                );
                if lenaff0 < 0 {
                    break;
                }

                let lenaff = level - startpos + lenaff0;

                if notprobed[(startpos + lenaff - 1) as usize] == 0 {
                    continue;
                }
                if level + lenaff - 1 <= minpos {
                    continue;
                }
                if lenaff as usize >= MAXNORMLEN {
                    continue; // skip too-big value
                }

                let buf: Vec<u8> = if lenaff > 0 {
                    word[startpos as usize..(startpos + lenaff) as usize].to_vec()
                } else {
                    Vec::new()
                };

                let compoundflag = if level == 0 {
                    FF_COMPOUNDBEGIN
                } else if level == wordlen - 1 {
                    FF_COMPOUNDLAST
                } else {
                    FF_COMPOUNDMIDDLE
                };
                let subres = self.normalize_sub_word(&buf, compoundflag)?;
                if !subres.is_empty() {
                    /* Yes, it was a word from the dictionary */
                    let mut new = SplitVar::copy_from(&var);
                    notprobed[(startpos + lenaff - 1) as usize] = 0;
                    for s in &subres {
                        new.add_stem(s.clone());
                    }
                    // (*p).next = SplitToVariants(...); append all the produced
                    // variants to the result list (the C tail-append walk).
                    let mut more = self.split_to_variants(
                        None,
                        Some(&new),
                        word,
                        wordlen,
                        startpos + lenaff,
                        startpos + lenaff,
                    )?;
                    result.append(&mut more);
                }
            }

            let ni = match node {
                Some(ni) => ni,
                None => break,
            };

            // binary search the word-trie node for word[level]
            let data = &self.sp_arena[ni].data;
            let wc = word[level as usize];
            let mut lo = 0usize;
            let mut hi = data.len();
            let mut found: Option<usize> = None;
            while lo < hi {
                let mid = lo + (hi - lo) / 2;
                match data[mid].val.cmp(&wc) {
                    core::cmp::Ordering::Equal => {
                        found = Some(mid);
                        break;
                    }
                    core::cmp::Ordering::Less => lo = mid + 1,
                    core::cmp::Ordering::Greater => hi = mid,
                }
            }

            if let Some(mid) = found {
                let compoundflag = if startpos == 0 {
                    FF_COMPOUNDBEGIN
                } else if level == wordlen - 1 {
                    FF_COMPOUNDLAST
                } else {
                    FF_COMPOUNDMIDDLE
                };

                let d = &self.sp_arena[ni].data[mid];
                let d_isword = d.isword;
                let d_compoundflag = d.compoundflag;
                let d_node = d.node;

                /* find the infinitive */
                if d_isword
                    && (d_compoundflag & compoundflag as u32) != 0
                    && notprobed[level as usize] != 0
                {
                    /* we found a full compound-allowed word */
                    if level > minpos {
                        /* and its length is more than minimal */
                        if wordlen == level + 1 {
                            /* it was the last word */
                            var.add_stem(word[startpos as usize..wordlen as usize].to_vec());
                            result.insert(0, var);
                            return Ok(result);
                        } else {
                            /* search for a bigger word at the same point */
                            let mut more = self.split_to_variants(
                                node, Some(&var), word, wordlen, startpos, level,
                            )?;
                            // We will find the next word: append the bigger-word
                            // variants, then continue stepping `var` forward.
                            level += 1;
                            var.add_stem(word[startpos as usize..level as usize].to_vec());
                            node = self.dictionary;
                            startpos = level;
                            result.append(&mut more);
                            continue;
                        }
                    }
                }
                node = d_node;
            } else {
                node = None;
            }
            level += 1;
        }

        var.add_stem(word[startpos as usize..wordlen as usize].to_vec());
        // The C returns `var` (with `var->next` already chained); here `var` is
        // the head, so it leads the result list.
        result.insert(0, var);
        Ok(result)
    }

    /* ---- NINormalizeWord ---- */

    /// `NINormalizeWord`: normalize `word`, returning the produced lexemes.
    /// The lexeme array is allocated in `out` (C: the caller's current context),
    /// distinct from the dictionary's own context.
    pub fn ni_normalize_word<'out>(
        &self,
        out: Mcx<'out>,
        word: &[u8],
    ) -> PgResult<PgVec<'out, TSLexeme<'out>>> {
        let mut lres: PgVec<'out, TSLexeme<'out>> = PgVec::new_in(out);
        let mut nvariant: u16 = 1;

        let res = self.normalize_sub_word(word, 0)?;
        for form in res {
            if lres.len() >= MAX_NORM {
                break;
            }
            add_norm(out, &mut lres, &form, 0, nvariant)?;
            nvariant += 1;
        }

        if self.usecompound {
            let wordlen = word.len() as i32;
            let variants = self.split_to_variants(None, None, word, wordlen, 0, -1)?;

            for var in variants {
                if var.nstem() > 1 {
                    let last = var.stem[var.nstem() - 1].clone();
                    let subres = self.normalize_sub_word(&last, FF_COMPOUNDLAST)?;

                    if !subres.is_empty() {
                        for sub in &subres {
                            for i in 0..var.nstem() - 1 {
                                add_norm(out, &mut lres, &var.stem[i], 0, nvariant)?;
                            }
                            add_norm(out, &mut lres, sub, 0, nvariant)?;
                            nvariant += 1;
                        }
                    }
                }
            }
        }

        Ok(lres)
    }
}

/* ---- addNorm ---- */

/// `addNorm`: append a lexeme `word` (with `flags`/`nvariant`) to `lres`, up to
/// the `MAX_NORM` cap. The lexeme string is allocated in `out`.
fn add_norm<'out>(
    out: Mcx<'out>,
    lres: &mut PgVec<'out, TSLexeme<'out>>,
    word: &[u8],
    flags: i32,
    nvariant: u16,
) -> PgResult<()> {
    if lres.len() < MAX_NORM - 1 {
        let lexeme = PgString::from_str_in(&String::from_utf8_lossy(word), out)?;
        lres.try_reserve(1)
            .map_err(|_| out.oom(core::mem::size_of::<TSLexeme>()))?;
        lres.push(TSLexeme {
            nvariant,
            flags: flags as u16,
            lexeme,
        });
    }
    Ok(())
}

/// `strncmp(a, b, n) == 0` over NUL-free byte slices.
#[inline]
fn bncmp_eq(a: &[u8], b: &[u8], n: usize) -> bool {
    crate::bncmp(a, b, n) == core::cmp::Ordering::Equal
}
