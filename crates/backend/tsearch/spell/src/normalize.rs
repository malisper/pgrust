use ::mcx::{Mcx, PgVec};
use ::regex::RegexecResult;
use ::ts_locale::TsLexeme;
use ::types_error::{PgError, PgResult, ERRCODE_INVALID_REGULAR_EXPRESSION};

use crate::{
    check_stack_depth, new_bytes, Affix, AffixReg, IspellDict, FF_COMPOUNDBEGIN,
    FF_COMPOUNDFORBIDFLAG, FF_COMPOUNDLAST, FF_COMPOUNDMIDDLE, FF_COMPOUNDONLY, FF_CROSSPRODUCT,
    FF_PREFIX, FF_SUFFIX, MAXNORMLEN, MAX_NORM,
};

#[inline]
fn getwchar(w: &[u8], l: i32, n: i32, t: i32) -> u8 {
    let idx = if t == FF_PREFIX { n } else { l - 1 - n };
    w[idx as usize]
}

struct SplitVar {
    stem: Vec<Vec<u8>>,
}

impl SplitVar {
    fn new() -> Self {
        SplitVar { stem: Vec::new() }
    }
    fn copy_from(other: &SplitVar) -> Self {
        SplitVar { stem: other.stem.clone() }
    }
    fn add_stem(&mut self, word: Vec<u8>) {
        self.stem.push(word);
    }
    fn nstem(&self) -> usize {
        self.stem.len()
    }
}

impl<'mcx> IspellDict<'mcx> {
    fn find_affixes(
        &self,
        mut node: Option<usize>,
        word: &[u8],
        wrdlen: i32,
        level: &mut i32,
        type_: i32,
    ) -> Option<(usize, usize)> {
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

        out.clear();
        if affix.type_ == FF_SUFFIX {
            if replen > len {
                return Ok((false, new_baselen));
            }
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
            if replen > len {
                return Ok((false, new_baselen));
            }
            out.extend_from_slice(&affix.find);
            out.extend_from_slice(&word[replen..len]);
        }

        match &affix.reg {
            AffixReg::Simple => Ok((true, new_baselen)),
            AffixReg::Regis(regis) => {
                if crate::rs_execute(regis, out)? {
                    Ok((true, new_baselen))
                } else {
                    Ok((false, new_baselen))
                }
            }
            AffixReg::Regex(re) => {
                let data = ::mbutils::pg_mb2wchar_with_len(self.mcx, out)?;
                let res =
                    ::regex_core::regex_export_free_error::seam_pg_regexec(re, &data, 0, &mut [])?;
                match res {
                    RegexecResult::Matched => Ok((true, new_baselen)),
                    RegexecResult::NoMatch => Ok((false, new_baselen)),
                    RegexecResult::Failed(f) => Err(PgError::error(format!(
                        "regular expression failed: {}",
                        f.message
                    ))
                    .with_sqlstate(ERRCODE_INVALID_REGULAR_EXPRESSION)
                    .into()),
                }
            }
        }
    }

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

    fn normalize_sub_word(&self, word: &[u8], flag: i32) -> PgResult<Vec<Vec<u8>>> {
        let wrdlen = word.len() as i32;
        if wrdlen as usize > MAXNORMLEN {
            return Ok(Vec::new());
        }
        let mut forms: Vec<Vec<u8>> = Vec::new();
        let mut newword: Vec<u8> = Vec::new();
        let mut pnewword: Vec<u8> = Vec::new();

        if self.find_word(word, &[], flag)? != 0 {
            forms.push(word.to_vec());
        }

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

        let mut node = if snode.is_some() { snode } else { self.dictionary };
        let mut level = if snode.is_some() { minpos } else { startpos };

        let mut notprobed = vec![1u8; wordlen as usize];
        let mut var = match orig {
            Some(o) => SplitVar::copy_from(o),
            None => SplitVar::new(),
        };
        let mut result: Vec<SplitVar> = Vec::new();

        while level < wordlen {
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
                    continue;
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
                    let mut new = SplitVar::copy_from(&var);
                    notprobed[(startpos + lenaff - 1) as usize] = 0;
                    for s in &subres {
                        new.add_stem(s.clone());
                    }
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

                if d_isword
                    && (d_compoundflag & compoundflag as u32) != 0
                    && notprobed[level as usize] != 0
                {
                    if level > minpos {
                        if wordlen == level + 1 {
                            var.add_stem(word[startpos as usize..wordlen as usize].to_vec());
                            result.insert(0, var);
                            return Ok(result);
                        } else {
                            let mut more = self.split_to_variants(
                                node, Some(&var), word, wordlen, startpos, level,
                            )?;
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
        result.insert(0, var);
        Ok(result)
    }

    pub fn ni_normalize_word<'out>(
        &self,
        out: Mcx<'out>,
        word: &[u8],
    ) -> PgResult<PgVec<'out, TsLexeme<'out>>> {
        let mut lres: PgVec<'out, TsLexeme<'out>> = PgVec::new_in(out);
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

fn add_norm<'out>(
    out: Mcx<'out>,
    lres: &mut PgVec<'out, TsLexeme<'out>>,
    word: &[u8],
    flags: i32,
    nvariant: u16,
) -> PgResult<()> {
    if lres.len() < MAX_NORM - 1 {
        let lexeme = new_bytes(out, word)?;
        lres.try_reserve(1)
            .map_err(|_| out.oom(core::mem::size_of::<TsLexeme>()))?;
        lres.push(TsLexeme { nvariant, flags: flags as u16, lexeme });
    }
    Ok(())
}

#[inline]
fn bncmp_eq(a: &[u8], b: &[u8], n: usize) -> bool {
    crate::bncmp(a, b, n) == core::cmp::Ordering::Equal
}
