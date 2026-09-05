use ::mcx::{Mcx, PgVec};
use ::regex::{RegcompResult, REG_ADVANCED, REG_NOSUB};
use ::types_core::catalog::DEFAULT_COLLATION_OID;
use ::ts_locale::TsearchReadline;
use ::types_error::{PgError, PgResult, ERRCODE_INVALID_REGULAR_EXPRESSION};

use crate::{
    atoi, bcmp, bstrchr, bytes_lossy, check_stack_depth, config_file_error, elog_internal,
    findchar, findchar2,
    isdigit, isprint, isspace, new_bytes, pg_mblen_clamped, reserve_one, rs_compile, rs_is_regis,
    str_tolower, strbcmp, strbncmp, strtol, t_isalpha, t_iseq, Affix, AffixNode, AffixNodeData,
    AffixReg, CmpdAffix, CompoundAffixFlag, FlagKey, FlagMode, IspellDict, SpNode, SpNodeData,
    Spell, FF_COMPOUNDBEGIN, FF_COMPOUNDFLAG, FF_COMPOUNDFLAGMASK, FF_COMPOUNDFORBIDFLAG,
    FF_COMPOUNDLAST, FF_COMPOUNDMIDDLE, FF_COMPOUNDONLY, FF_COMPOUNDPERMITFLAG, FF_CROSSPRODUCT,
    FF_PREFIX, FF_SUFFIX, FLAGNUM_MAXSIZE,
};

#[inline]
fn getwchar(w: &[u8], l: i32, n: i32, t: i32) -> u8 {
    let idx = if t == FF_PREFIX { n } else { l - 1 - n };
    w[idx as usize]
}

#[inline]
fn getchar(a: &Affix, n: i32, t: i32) -> u8 {
    getwchar(&a.repl, a.repl.len() as i32, n, t)
}

// NIImportOOAffixes' second-pass locals, carried across lines.
struct OoAffixState {
    type_: Vec<u8>,
    sflag: Vec<u8>,
    mask: Vec<u8>,
    find: Vec<u8>,
    repl: Vec<u8>,
    is_suffix: bool,
    naffix: i32,
    curaffix: i32,
    flagflags: i32,
}

// NIImportAffixes' locals, carried across lines.
struct AffixState {
    flag: Vec<u8>,
    mask: Vec<u8>,
    find: Vec<u8>,
    repl: Vec<u8>,
    suffixes: bool,
    prefixes: bool,
    flagflags: i32,
    oldformat: bool,
}

impl<'mcx> IspellDict<'mcx> {
    fn get_next_flag_from_string(
        &self,
        sflagset: &[u8],
        pos: &mut usize,
        out: &mut Vec<u8>,
    ) -> PgResult<()> {
        let sbuf_start = *pos;
        let mut maxstep: i32 = if self.flag_mode == FlagMode::Long { 2 } else { 1 };
        let mut met_comma = false;

        while *pos < sflagset.len() {
            let stop;
            match self.flag_mode {
                FlagMode::Long | FlagMode::Char => {
                    let c = &sflagset[*pos..];
                    let clen = pg_mblen_clamped(c);
                    out.extend_from_slice(&c[..clen]);
                    *pos += clen;
                    maxstep -= 1;
                    stop = maxstep == 0;
                }
                FlagMode::Num => {
                    let rest = &sflagset[*pos..];
                    let (raw, consumed, valid) = strtol(rest);
                    let s_val = raw as i32;
                    if consumed == 0 || !valid {
                        return Err(config_file_error(format!(
                            "invalid affix flag \"{}\"",
                            bytes_lossy(rest)
                        ))
                        .into());
                    }
                    if !(0..=FLAGNUM_MAXSIZE).contains(&s_val) {
                        return Err(config_file_error(format!(
                            "affix flag \"{}\" is out of range",
                            bytes_lossy(rest)
                        ))
                        .into());
                    }
                    out.extend_from_slice(format!("{s_val}").as_bytes());
                    *pos += consumed;
                    while *pos < sflagset.len() {
                        let cc = &sflagset[*pos..];
                        if isdigit(cc[0]) {
                            if !met_comma {
                                return Err(config_file_error(format!(
                                    "invalid affix flag \"{}\"",
                                    bytes_lossy(cc)
                                ))
                                .into());
                            }
                            break;
                        } else if t_iseq(cc, b',') {
                            if met_comma {
                                return Err(config_file_error(format!(
                                    "invalid affix flag \"{}\"",
                                    bytes_lossy(cc)
                                ))
                                .into());
                            }
                            met_comma = true;
                        } else if !isspace(cc[0]) {
                            return Err(config_file_error(format!(
                                "invalid character in affix flag \"{}\"",
                                bytes_lossy(cc)
                            ))
                            .into());
                        }
                        *pos += pg_mblen_clamped(cc);
                    }
                    stop = true;
                }
            }
            if stop {
                break;
            }
        }

        if self.flag_mode == FlagMode::Long && maxstep > 0 {
            return Err(config_file_error(format!(
                "invalid affix flag \"{}\" with \"long\" flag value",
                bytes_lossy(&sflagset[sbuf_start..])
            ))
            .into());
        }
        Ok(())
    }

    fn is_affix_flag_in_use(&self, affix: i32, affixflag: &[u8]) -> PgResult<bool> {
        if affixflag.is_empty() {
            return Ok(true);
        }
        debug_assert!((affix as usize) < self.affix_data.len());

        let data = &self.affix_data[affix as usize];
        let mut pos = 0usize;
        let mut flag: Vec<u8> = Vec::new();
        while pos < data.len() {
            flag.clear();
            self.get_next_flag_from_string(data, &mut pos, &mut flag)?;
            if flag == *affixflag {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn ni_add_spell(&mut self, word: &[u8], flag: &[u8]) -> PgResult<()> {
        let mcx = self.mcx;
        let sp = Spell {
            word: new_bytes(mcx, word)?,
            flag: if !flag.is_empty() { new_bytes(mcx, flag)? } else { PgVec::new_in(mcx) },
            affix: 0,
            len: 0,
        };
        reserve_one(mcx, &mut self.spell)?;
        self.spell.push(sp);
        Ok(())
    }

    pub(crate) fn find_word(&self, word: &[u8], affixflag: &[u8], mut flag: i32) -> PgResult<i32> {
        let mut node = self.dictionary;
        let mut ptr = 0usize;

        flag &= FF_COMPOUNDFLAGMASK;

        while let Some(ni) = node {
            if ptr >= word.len() {
                break;
            }
            let data = &self.sp_arena[ni].data;
            let target = word[ptr];
            let mut lo = 0usize;
            let mut hi = data.len();
            let mut matched = false;
            while lo < hi {
                let mid = lo + (hi - lo) / 2;
                let d = &data[mid];
                match d.val.cmp(&target) {
                    core::cmp::Ordering::Equal => {
                        if ptr + 1 == word.len() && d.isword {
                            if flag == 0 {
                                if d.compoundflag & FF_COMPOUNDONLY as u32 != 0 {
                                    return Ok(0);
                                }
                            } else if (flag as u32 & d.compoundflag) == 0 {
                                return Ok(0);
                            }
                            if self.is_affix_flag_in_use(d.affix as i32, affixflag)? {
                                return Ok(1);
                            }
                        }
                        node = d.node;
                        ptr += 1;
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
        Ok(0)
    }

    fn ni_add_affix(
        &mut self,
        flag: &[u8],
        flagflags: i32,
        mask: &[u8],
        find: &[u8],
        repl: &[u8],
        type_: i32,
    ) -> PgResult<()> {
        let mcx = self.mcx;

        let reg = if mask == b"." || mask.is_empty() {
            AffixReg::Simple
        } else if rs_is_regis(mask)? {
            AffixReg::Regis(rs_compile(mcx, type_ == FF_SUFFIX, mask)?)
        } else {
            let mut tmask: Vec<u8> = Vec::with_capacity(mask.len() + 2);
            if type_ == FF_SUFFIX {
                tmask.extend_from_slice(mask);
                tmask.push(b'$');
            } else {
                tmask.push(b'^');
                tmask.extend_from_slice(mask);
            }
            let wmask = ::mbutils::pg_mb2wchar_with_len(mcx, &tmask)?;
            match ::regex_core::regex_export_free_error::seam_pg_regcomp(
                &wmask,
                REG_ADVANCED | REG_NOSUB,
                DEFAULT_COLLATION_OID,
            )? {
                RegcompResult::Compiled(c) => AffixReg::Regex(c),
                RegcompResult::Failed(f) => {
                    return Err(PgError::error(format!(
                        "invalid regular expression: {}",
                        f.message
                    ))
                    .with_sqlstate(ERRCODE_INVALID_REGULAR_EXPRESSION)
                    .into());
                }
            }
        };

        let mut flagflags = flagflags;
        if (flagflags & FF_COMPOUNDONLY) != 0 || (flagflags & FF_COMPOUNDPERMITFLAG) != 0 {
            if (flagflags & FF_COMPOUNDFLAG) == 0 {
                flagflags |= FF_COMPOUNDFLAG;
            }
        }

        let affix = Affix {
            flag: new_bytes(mcx, flag)?,
            type_,
            flagflags,
            find: if !find.is_empty() { new_bytes(mcx, find)? } else { PgVec::new_in(mcx) },
            repl: if !repl.is_empty() { new_bytes(mcx, repl)? } else { PgVec::new_in(mcx) },
            reg,
        };
        reserve_one(mcx, &mut self.affixes)?;
        self.affixes.push(affix);
        Ok(())
    }

    pub fn ni_import_dictionary(&mut self, filename: &[u8]) -> PgResult<()> {
        let mcx = self.mcx;
        let mut rd = open_file(mcx, filename, "dictionary")?;

        loop {
            let next = rd.readline();
            let Some(raw_line) = rd.with_context(next)? else {
                break;
            };
            let r = self.import_dictionary_line(mcx, &raw_line);
            rd.with_context(r)?;
        }
        Ok(())
    }

    // NIImportDictionary's loop body for one line (spell.c:534-563).
    fn import_dictionary_line(&mut self, mcx: Mcx<'mcx>, raw_line: &[u8]) -> PgResult<()> {
        let mut line: Vec<u8> = raw_line.to_vec();

        let flag: Vec<u8>;
        if let Some(slash) = findchar(&line, b'/') {
            let mut s = slash + 1;
            let flag_start = s;
            while s < line.len() {
                let c = &line[s..];
                if pg_mblen_clamped(c) == 1 && isprint(c[0]) && !isspace(c[0]) {
                    s += 1;
                } else {
                    break;
                }
            }
            flag = line[flag_start..s].to_vec();
            line.truncate(slash);
        } else {
            flag = Vec::new();
        }

        let mut s = 0usize;
        while s < line.len() {
            if isspace(line[s]) {
                line.truncate(s);
                break;
            }
            s += pg_mblen_clamped(&line[s..]);
        }

        let pstr = str_tolower(mcx, &line)?;
        self.ni_add_spell(&pstr, &flag)
    }

    fn get_nextfield(s: &[u8], pos: &mut usize, out: &mut Vec<u8>) -> bool {
        const PAE_WAIT_MASK: i32 = 0;
        const PAE_INMASK: i32 = 1;
        let mut state = PAE_WAIT_MASK;

        while *pos < s.len() {
            let c = &s[*pos..];
            let clen = pg_mblen_clamped(c);
            if state == PAE_WAIT_MASK {
                if t_iseq(c, b'#') {
                    return false;
                } else if !isspace(c[0]) {
                    out.extend_from_slice(&c[..clen]);
                    state = PAE_INMASK;
                }
            } else {
                if isspace(c[0]) {
                    return true;
                } else {
                    out.extend_from_slice(&c[..clen]);
                }
            }
            *pos += clen;
        }
        state == PAE_INMASK
    }

    fn parse_ooaffentry(
        s: &[u8],
        type_: &mut Vec<u8>,
        flag: &mut Vec<u8>,
        find: &mut Vec<u8>,
        repl: &mut Vec<u8>,
        mask: &mut Vec<u8>,
    ) -> PgResult<i32> {
        const PAE_WAIT_TYPE: i32 = 6;
        const PAE_WAIT_FLAG: i32 = 7;
        const PAE_WAIT_FIND: i32 = 2;
        const PAE_WAIT_REPL: i32 = 4;
        const PAE_WAIT_MASK: i32 = 0;

        type_.clear();
        flag.clear();
        find.clear();
        repl.clear();
        mask.clear();

        let mut state = PAE_WAIT_TYPE;
        let mut fields_read = 0;
        let mut pos = 0usize;

        while pos < s.len() {
            let valid = match state {
                PAE_WAIT_TYPE => {
                    let v = Self::get_nextfield(s, &mut pos, type_);
                    state = PAE_WAIT_FLAG;
                    v
                }
                PAE_WAIT_FLAG => {
                    let v = Self::get_nextfield(s, &mut pos, flag);
                    state = PAE_WAIT_FIND;
                    v
                }
                PAE_WAIT_FIND => {
                    let v = Self::get_nextfield(s, &mut pos, find);
                    state = PAE_WAIT_REPL;
                    v
                }
                PAE_WAIT_REPL => {
                    let v = Self::get_nextfield(s, &mut pos, repl);
                    state = PAE_WAIT_MASK;
                    v
                }
                PAE_WAIT_MASK => {
                    let v = Self::get_nextfield(s, &mut pos, mask);
                    state = -1;
                    v
                }
                other => {
                    return Err(elog_internal(format!(
                        "unrecognized state in parse_ooaffentry: {other}"
                    ))
                    .into());
                }
            };
            if valid {
                fields_read += 1;
            } else {
                break;
            }
            if state < 0 {
                break;
            }
        }
        Ok(fields_read)
    }

    fn parse_affentry(
        s: &[u8],
        mask: &mut Vec<u8>,
        find: &mut Vec<u8>,
        repl: &mut Vec<u8>,
    ) -> PgResult<bool> {
        const PAE_WAIT_MASK: i32 = 0;
        const PAE_INMASK: i32 = 1;
        const PAE_WAIT_FIND: i32 = 2;
        const PAE_INFIND: i32 = 3;
        const PAE_WAIT_REPL: i32 = 4;
        const PAE_INREPL: i32 = 5;

        mask.clear();
        find.clear();
        repl.clear();
        let mut state = PAE_WAIT_MASK;
        let mut pos = 0usize;

        while pos < s.len() {
            let c = &s[pos..];
            let clen = pg_mblen_clamped(c);
            if state == PAE_WAIT_MASK {
                if t_iseq(c, b'#') {
                    return Ok(false);
                } else if !isspace(c[0]) {
                    mask.extend_from_slice(&c[..clen]);
                    state = PAE_INMASK;
                }
            } else if state == PAE_INMASK {
                if t_iseq(c, b'>') {
                    state = PAE_WAIT_FIND;
                } else if !isspace(c[0]) {
                    mask.extend_from_slice(&c[..clen]);
                }
            } else if state == PAE_WAIT_FIND {
                if t_iseq(c, b'-') {
                    state = PAE_INFIND;
                } else if t_isalpha(c) || t_iseq(c, b'\'') {
                    repl.extend_from_slice(&c[..clen]);
                    state = PAE_INREPL;
                } else if !isspace(c[0]) {
                    return Err(config_file_error("syntax error".into()).into());
                }
            } else if state == PAE_INFIND {
                if t_iseq(c, b',') {
                    state = PAE_WAIT_REPL;
                } else if t_isalpha(c) {
                    find.extend_from_slice(&c[..clen]);
                } else if !isspace(c[0]) {
                    return Err(config_file_error("syntax error".into()).into());
                }
            } else if state == PAE_WAIT_REPL {
                if t_iseq(c, b'-') {
                    break;
                } else if t_isalpha(c) {
                    repl.extend_from_slice(&c[..clen]);
                    state = PAE_INREPL;
                } else if !isspace(c[0]) {
                    return Err(config_file_error("syntax error".into()).into());
                }
            } else if state == PAE_INREPL {
                if t_iseq(c, b'#') {
                    break;
                } else if t_isalpha(c) {
                    repl.extend_from_slice(&c[..clen]);
                } else if !isspace(c[0]) {
                    return Err(config_file_error("syntax error".into()).into());
                }
            } else {
                return Err(elog_internal(format!(
                    "unrecognized state in parse_affentry: {state}"
                ))
                .into());
            }
            pos += clen;
        }

        Ok(!mask.is_empty() && (!find.is_empty() || !repl.is_empty()))
    }

    fn set_compound_affix_flag_value(
        &self,
        entry: &mut CompoundAffixFlag,
        s: &[u8],
        val: u32,
    ) -> PgResult<()> {
        if self.flag_mode == FlagMode::Num {
            let (raw, consumed, valid) = strtol(s);
            let i = raw as i32;
            if consumed == 0 || !valid {
                return Err(config_file_error(format!(
                    "invalid affix flag \"{}\"",
                    bytes_lossy(s)
                ))
                .into());
            }
            if !(0..=FLAGNUM_MAXSIZE).contains(&i) {
                return Err(config_file_error(format!(
                    "affix flag \"{}\" is out of range",
                    bytes_lossy(s)
                ))
                .into());
            }
            entry.flag = FlagKey::Num(i as u32);
        } else {
            entry.flag = FlagKey::Str(s.to_vec());
        }
        entry.flag_mode = self.flag_mode;
        entry.value = val;
        Ok(())
    }

    fn add_compound_affix_flag_value(&mut self, s: &[u8], val: u32) -> PgResult<()> {
        let mut start = 0usize;
        while start < s.len() && isspace(s[start]) {
            start += pg_mblen_clamped(&s[start..]);
        }
        if start >= s.len() {
            return Err(config_file_error("syntax error".into()).into());
        }

        let mut sflag: Vec<u8> = Vec::new();
        let mut p = start;
        while p < s.len() && !isspace(s[p]) && s[p] != b'\n' {
            let clen = pg_mblen_clamped(&s[p..]);
            sflag.extend_from_slice(&s[p..p + clen]);
            p += clen;
        }

        let mut entry = CompoundAffixFlag {
            flag: FlagKey::Num(0),
            flag_mode: self.flag_mode,
            value: 0,
        };
        self.set_compound_affix_flag_value(&mut entry, &sflag, val)?;

        let mcx = self.mcx;
        reserve_one(mcx, &mut self.compound_affix_flags)?;
        self.compound_affix_flags.push(entry);
        self.usecompound = true;
        Ok(())
    }

    fn cmpcmdflag(f1: &CompoundAffixFlag, f2: &CompoundAffixFlag) -> core::cmp::Ordering {
        debug_assert_eq!(f1.flag_mode, f2.flag_mode);
        match (&f1.flag, &f2.flag) {
            (FlagKey::Num(a), FlagKey::Num(b)) => a.cmp(b),
            (FlagKey::Str(a), FlagKey::Str(b)) => bcmp(a, b),
            (FlagKey::Num(_), FlagKey::Str(_)) => core::cmp::Ordering::Less,
            (FlagKey::Str(_), FlagKey::Num(_)) => core::cmp::Ordering::Greater,
        }
    }

    fn get_compound_affix_flag_value(&self, s: &[u8]) -> PgResult<i32> {
        if self.compound_affix_flags.is_empty() {
            return Ok(0);
        }
        let mut flag: u32 = 0;
        let mut pos = 0usize;
        let mut sflag: Vec<u8> = Vec::new();
        while pos < s.len() {
            sflag.clear();
            self.get_next_flag_from_string(s, &mut pos, &mut sflag)?;
            let mut key = CompoundAffixFlag {
                flag: FlagKey::Num(0),
                flag_mode: self.flag_mode,
                value: 0,
            };
            self.set_compound_affix_flag_value(&mut key, &sflag, 0)?;

            if let Ok(idx) = self
                .compound_affix_flags
                .binary_search_by(|probe| Self::cmpcmdflag(probe, &key))
            {
                flag |= self.compound_affix_flags[idx].value;
            }
        }
        Ok(flag as i32)
    }

    // `filled` counts the affix_data slots NIImportOOAffixes has set so far
    // (they fill in order from 0), so C's NULL-slot test is `alias >= filled`.
    fn get_affix_flag_set(&self, s: &[u8], filled: i32) -> PgResult<Vec<u8>> {
        if self.use_flag_aliases && !s.is_empty() {
            let (raw, consumed, valid) = strtol(s);
            if consumed == 0 || !valid {
                return Err(config_file_error(format!(
                    "invalid affix alias \"{}\"",
                    bytes_lossy(s)
                ))
                .into());
            }
            let curaffix = raw as i32;
            if curaffix > 0 && curaffix < self.affix_data.len() as i32 {
                // upstream 4689ea9ceee3 (18.6): Fix memory-safety bugs in the ispell/hunspell dictionary loader.
                if curaffix >= filled {
                    return Err(config_file_error(format!(
                        "invalid affix alias \"{}\"",
                        bytes_lossy(s)
                    ))
                    .into());
                }
                // No -1: the empty string was prepended in NIImportOOAffixes.
                return Ok(self.affix_data[curaffix as usize].as_slice().to_vec());
            } else if curaffix > self.affix_data.len() as i32 {
                return Err(config_file_error(format!(
                    "invalid affix alias \"{}\"",
                    bytes_lossy(s)
                ))
                .into());
            }
            Ok(Vec::new())
        } else {
            Ok(s.to_vec())
        }
    }

    fn ni_import_oo_affixes(&mut self, filename: &[u8]) -> PgResult<()> {
        let mcx = self.mcx;
        self.usecompound = false;
        self.use_flag_aliases = false;
        self.flag_mode = FlagMode::Char;

        // First pass (spell.c:1238): the FLAG mode and compound flags.
        let mut rd = open_file(mcx, filename, "affix")?;
        loop {
            let next = rd.readline();
            let Some(line) = rd.with_context(next)? else {
                break;
            };
            let r = self.import_oo_affix_flags_line(&line);
            rd.with_context(r)?;
        }
        drop(rd);

        if self.compound_affix_flags.len() > 1 {
            let mut tmp: Vec<CompoundAffixFlag> =
                self.compound_affix_flags.iter().cloned().collect();
            tmp.sort_by(Self::cmpcmdflag);
            let mut rebuilt: PgVec<CompoundAffixFlag> = PgVec::new_in(mcx);
            rebuilt.try_reserve(tmp.len()).map_err(|_| mcx.oom(tmp.len()))?;
            for e in tmp {
                rebuilt.push(e);
            }
            self.compound_affix_flags = rebuilt;
        }

        // Second pass (spell.c:1305): the AF alias table and the entries.
        let mut st = OoAffixState {
            type_: Vec::new(),
            sflag: Vec::new(),
            mask: Vec::new(),
            find: Vec::new(),
            repl: Vec::new(),
            is_suffix: false,
            naffix: 0,
            curaffix: 0,
            flagflags: 0,
        };
        let mut rd = open_file(mcx, filename, "affix")?;
        loop {
            let next = rd.readline();
            let Some(line) = rd.with_context(next)? else {
                break;
            };
            let r = self.import_oo_affix_line(mcx, &mut st, &line);
            rd.with_context(r)?;
        }
        drop(rd);
        // upstream 4689ea9ceee3 (18.6): Fix memory-safety bugs in the ispell/hunspell dictionary loader.
        if self.use_flag_aliases && st.curaffix != st.naffix {
            return Err(config_file_error(format!(
                "number of aliases is less than specified number {}",
                st.naffix - 1
            ))
            .into());
        }
        Ok(())
    }

    // NIImportOOAffixes first-pass loop body for one line (spell.c:1243-1303).
    fn import_oo_affix_flags_line(&mut self, line: &[u8]) -> PgResult<()> {
        if line.is_empty() || isspace(line[0]) || t_iseq(line, b'#') {
            return Ok(());
        }
        if let Some(rest) = strip_prefix(line, b"COMPOUNDFLAG") {
            self.add_compound_affix_flag_value(rest, FF_COMPOUNDFLAG as u32)?;
        } else if let Some(rest) = strip_prefix(line, b"COMPOUNDBEGIN") {
            self.add_compound_affix_flag_value(rest, FF_COMPOUNDBEGIN as u32)?;
        } else if let Some(rest) = strip_prefix(line, b"COMPOUNDLAST") {
            self.add_compound_affix_flag_value(rest, FF_COMPOUNDLAST as u32)?;
        } else if let Some(rest) = strip_prefix(line, b"COMPOUNDEND") {
            self.add_compound_affix_flag_value(rest, FF_COMPOUNDLAST as u32)?;
        } else if let Some(rest) = strip_prefix(line, b"COMPOUNDMIDDLE") {
            self.add_compound_affix_flag_value(rest, FF_COMPOUNDMIDDLE as u32)?;
        } else if let Some(rest) = strip_prefix(line, b"ONLYINCOMPOUND") {
            self.add_compound_affix_flag_value(rest, FF_COMPOUNDONLY as u32)?;
        } else if let Some(rest) = strip_prefix(line, b"COMPOUNDPERMITFLAG") {
            self.add_compound_affix_flag_value(rest, FF_COMPOUNDPERMITFLAG as u32)?;
        } else if let Some(rest) = strip_prefix(line, b"COMPOUNDFORBIDFLAG") {
            self.add_compound_affix_flag_value(rest, FF_COMPOUNDFORBIDFLAG as u32)?;
        } else if let Some(rest) = strip_prefix(line, b"FLAG") {
            let mut p = 0usize;
            while p < rest.len() && isspace(rest[p]) {
                p += pg_mblen_clamped(&rest[p..]);
            }
            let tail = &rest[p..];
            if !tail.is_empty() {
                if has_prefix(tail, b"long") {
                    self.flag_mode = FlagMode::Long;
                } else if has_prefix(tail, b"num") {
                    self.flag_mode = FlagMode::Num;
                } else if !has_prefix(tail, b"default") {
                    return Err(config_file_error(
                        "Ispell dictionary supports only \"default\", \"long\", and \"num\" flag values".into(),
                    )
                    .into());
                }
            }
        }
        Ok(())
    }

    // NIImportOOAffixes second-pass loop body for one line (spell.c:1310-1445).
    fn import_oo_affix_line(
        &mut self,
        mcx: Mcx<'mcx>,
        st: &mut OoAffixState,
        line: &[u8],
    ) -> PgResult<()> {
        if line.is_empty() || isspace(line[0]) || t_iseq(line, b'#') {
            return Ok(());
        }

        let fields_read = Self::parse_ooaffentry(
            line, &mut st.type_, &mut st.sflag, &mut st.find, &mut st.repl, &mut st.mask,
        )?;

        let ptype = str_tolower(mcx, &st.type_)?;

        if has_prefix(&ptype, b"af") {
            if !self.use_flag_aliases {
                self.use_flag_aliases = true;
                st.naffix = atoi(&st.sflag);
                if st.naffix <= 0 {
                    return Err(config_file_error(
                        "invalid number of flag vector aliases".into(),
                    )
                    .into());
                }
                st.naffix += 1;
                // C: Conf->AffixData = palloc0(naffix * sizeof(char *)).
                // palloc0 enforces AllocSizeIsValid, so an oversized alias
                // count raises "invalid memory alloc request size" BEFORE
                // any allocation is attempted. Reproduce C's exact
                // condition (naffix * sizeof(char *), i.e. 8 bytes per
                // slot on LP64 — NOT this port's element size) so the
                // threshold and the error match C. Without this the port
                // handed a ~39 GB request straight to try_reserve, which C
                // refuses outright (found by spellfam_diff: an `AF` line
                // whose count atoi-truncates to 1215752191).
                ::mcx::check_alloc_size(
                    (st.naffix as usize).saturating_mul(core::mem::size_of::<*const u8>()),
                )?;
                self.affix_data
                    .try_reserve(st.naffix as usize)
                    .map_err(|_| mcx.oom(st.naffix as usize))?;
                for _ in 0..st.naffix {
                    self.affix_data.push(PgVec::new_in(mcx));
                }
                st.curaffix += 1;
            } else if st.curaffix < st.naffix {
                let dup = new_bytes(mcx, &st.sflag)?;
                self.affix_data[st.curaffix as usize] = dup;
                st.curaffix += 1;
            } else {
                return Err(config_file_error(format!(
                    "number of aliases exceeds specified number {}",
                    st.naffix - 1
                ))
                .into());
            }
            return Ok(());
        }
        if fields_read < 4 || (!has_prefix(&ptype, b"sfx") && !has_prefix(&ptype, b"pfx")) {
            return Ok(());
        }

        let sflaglen = st.sflag.len();
        if sflaglen == 0
            || (sflaglen > 1 && self.flag_mode == FlagMode::Char)
            || (sflaglen > 2 && self.flag_mode == FlagMode::Long)
        {
            return Ok(());
        }

        if fields_read == 4 {
            st.is_suffix = has_prefix(&ptype, b"sfx");
            if t_iseq(&st.find, b'y') || t_iseq(&st.find, b'Y') {
                st.flagflags = FF_CROSSPRODUCT;
            } else {
                st.flagflags = 0;
            }
        } else {
            let mut aflg: i32 = 0;
            if let Some(slash) = bstrchr(&st.repl, b'/') {
                let fs = self.get_affix_flag_set(&st.repl[slash + 1..], st.curaffix)?;
                aflg |= self.get_compound_affix_flag_value(&fs)?;
            }
            let mut prepl = str_tolower(mcx, &st.repl)?;
            if let Some(slash) = bstrchr(&prepl, b'/') {
                prepl.truncate(slash);
            }
            let mut pfind = str_tolower(mcx, &st.find)?;
            let pmask = str_tolower(mcx, &st.mask)?;
            if t_iseq(&st.find, b'0') {
                pfind.clear();
            }
            if t_iseq(&st.repl, b'0') {
                prepl.clear();
            }

            self.ni_add_affix(
                &st.sflag,
                st.flagflags | aflg,
                &pmask,
                &pfind,
                &prepl,
                if st.is_suffix { FF_SUFFIX } else { FF_PREFIX },
            )?;
        }
        Ok(())
    }

    pub fn ni_import_affixes(&mut self, filename: &[u8]) -> PgResult<()> {
        let mcx = self.mcx;
        self.usecompound = false;
        self.use_flag_aliases = false;
        self.flag_mode = FlagMode::Char;

        let mut st = AffixState {
            flag: Vec::new(),
            mask: Vec::new(),
            find: Vec::new(),
            repl: Vec::new(),
            suffixes: false,
            prefixes: false,
            flagflags: 0,
            oldformat: false,
        };
        let mut rd = open_file(mcx, filename, "affix")?;
        let mut isnewformat = false;
        loop {
            let next = rd.readline();
            let Some(line) = rd.with_context(next)? else {
                break;
            };
            let r = self.import_affix_line(mcx, &mut st, &line);
            if rd.with_context(r)? {
                isnewformat = true;
                break;
            }
        }
        if !isnewformat {
            return Ok(());
        }

        // spell.c:1581 isnewformat: raised with the new-format line still
        // current, so it carries that line's config-file context.
        if st.oldformat {
            return rd.with_context(Err(config_file_error(
                "affix file contains both old-style and new-style commands".into(),
            )
            .into()));
        }
        drop(rd);

        self.ni_import_oo_affixes(filename)
    }

    // NIImportAffixes loop body for one line (spell.c:1476-1578); Ok(true)
    // is C's `goto isnewformat`.
    fn import_affix_line(
        &mut self,
        mcx: Mcx<'mcx>,
        st: &mut AffixState,
        line: &[u8],
    ) -> PgResult<bool> {
        let pstr = str_tolower(mcx, line)?;

        if pstr.first() == Some(&b'#') || pstr.first() == Some(&b'\n') {
            return Ok(false);
        }

        if has_prefix(&pstr, b"compoundwords") {
            if let Some(idx) = findchar2(line, b'l', b'L') {
                let mut s = idx;
                while s < line.len() && !isspace(line[s]) {
                    s += pg_mblen_clamped(&line[s..]);
                }
                while s < line.len() && isspace(line[s]) {
                    s += pg_mblen_clamped(&line[s..]);
                }
                if s < line.len() && pg_mblen_clamped(&line[s..]) == 1 {
                    self.add_compound_affix_flag_value(&line[s..], FF_COMPOUNDFLAG as u32)?;
                    self.usecompound = true;
                }
                st.oldformat = true;
                return Ok(false);
            }
        }
        if has_prefix(&pstr, b"suffixes") {
            st.suffixes = true;
            st.prefixes = false;
            st.oldformat = true;
            return Ok(false);
        }
        if has_prefix(&pstr, b"prefixes") {
            st.suffixes = false;
            st.prefixes = true;
            st.oldformat = true;
            return Ok(false);
        }
        if has_prefix(&pstr, b"flag") {
            let mut s = 4usize.min(line.len());
            st.flagflags = 0;
            while s < line.len() && isspace(line[s]) {
                s += pg_mblen_clamped(&line[s..]);
            }
            if line.get(s) == Some(&b'*') {
                st.flagflags |= FF_CROSSPRODUCT;
                s += 1;
            } else if line.get(s) == Some(&b'~') {
                st.flagflags |= FF_COMPOUNDONLY;
                s += 1;
            }
            if line.get(s) == Some(&b'\\') {
                s += 1;
            }
            if s < line.len() && pg_mblen_clamped(&line[s..]) == 1 {
                st.flag.clear();
                st.flag.push(line[s]);
                s += 1;
                let c = line.get(s).copied().unwrap_or(0);
                if c == 0 || c == b'#' || c == b'\n' || c == b':' || isspace(c) {
                    st.oldformat = true;
                    return Ok(false);
                }
            }
            return Ok(true);
        }
        if has_prefix(line, b"COMPOUNDFLAG")
            || has_prefix(line, b"COMPOUNDMIN")
            || has_prefix(line, b"PFX")
            || has_prefix(line, b"SFX")
        {
            return Ok(true);
        }

        if !st.suffixes && !st.prefixes {
            return Ok(false);
        }

        if !Self::parse_affentry(&pstr, &mut st.mask, &mut st.find, &mut st.repl)? {
            return Ok(false);
        }

        self.ni_add_affix(
            &st.flag,
            st.flagflags,
            &st.mask,
            &st.find,
            &st.repl,
            if st.suffixes { FF_SUFFIX } else { FF_PREFIX },
        )?;
        Ok(false)
    }

    fn merge_affix(&mut self, a1: i32, a2: i32) -> PgResult<i32> {
        debug_assert!(a1 < self.affix_data.len() as i32 && a2 < self.affix_data.len() as i32);

        if self.affix_data[a1 as usize].is_empty() {
            return Ok(a2);
        } else if self.affix_data[a2 as usize].is_empty() {
            return Ok(a1);
        }

        let mcx = self.mcx;
        let mut merged: Vec<u8> = Vec::new();
        merged.extend_from_slice(&self.affix_data[a1 as usize]);
        if self.flag_mode == FlagMode::Num {
            merged.push(b',');
        }
        merged.extend_from_slice(&self.affix_data[a2 as usize]);

        let pv = new_bytes(mcx, &merged)?;
        reserve_one(mcx, &mut self.affix_data)?;
        self.affix_data.push(pv);
        Ok(self.affix_data.len() as i32 - 1)
    }

    fn make_compound_flags(&self, affix: i32) -> PgResult<u32> {
        debug_assert!(affix < self.affix_data.len() as i32);
        let data = &self.affix_data[affix as usize];
        Ok((self.get_compound_affix_flag_value(data)? & FF_COMPOUNDFLAGMASK) as u32)
    }

    pub fn ni_sort_dictionary(&mut self) -> PgResult<()> {
        let mcx = self.mcx;

        if self.use_flag_aliases {
            for i in 0..self.spell.len() {
                let curaffix;
                if !self.spell[i].flag.is_empty() {
                    let flagbytes = self.spell[i].flag.as_slice().to_vec();
                    let (ca, consumed, valid) = strtol(&flagbytes);
                    if consumed == 0 || !valid {
                        return Err(config_file_error(format!(
                            "invalid affix alias \"{}\"",
                            bytes_lossy(&flagbytes)
                        ))
                        .into());
                    }
                    let ca = ca as i32;
                    if ca < 0 || ca >= self.affix_data.len() as i32 {
                        return Err(config_file_error(format!(
                            "invalid affix alias \"{}\"",
                            bytes_lossy(&flagbytes)
                        ))
                        .into());
                    }
                    let endc = flagbytes.get(consumed).copied().unwrap_or(0);
                    if endc != 0 && !isdigit(endc) && !isspace(endc) {
                        return Err(config_file_error(format!(
                            "invalid affix alias \"{}\"",
                            bytes_lossy(&flagbytes)
                        ))
                        .into());
                    }
                    curaffix = ca;
                } else {
                    curaffix = 0;
                }
                let wordlen = self.spell[i].word.len() as i32;
                self.spell[i].affix = curaffix;
                self.spell[i].len = wordlen;
            }
        } else {
            sort_spell_by(&mut self.spell, |a, b| bcmp(&a.flag, &b.flag));

            let mut naffix = 0;
            for i in 0..self.spell.len() {
                if i == 0
                    || bcmp(&self.spell[i].flag, &self.spell[i - 1].flag)
                        != core::cmp::Ordering::Equal
                {
                    naffix += 1;
                }
            }

            let mut affix_data: PgVec<PgVec<u8>> = PgVec::new_in(mcx);
            affix_data
                .try_reserve(naffix as usize)
                .map_err(|_| mcx.oom(naffix as usize))?;
            let mut curaffix: i32 = -1;
            for i in 0..self.spell.len() {
                let is_new = i == 0
                    || bcmp(&self.spell[i].flag, &affix_data[curaffix as usize])
                        != core::cmp::Ordering::Equal;
                if is_new {
                    curaffix += 1;
                    debug_assert!(curaffix < naffix);
                    let dup = new_bytes(mcx, &self.spell[i].flag.as_slice().to_vec())?;
                    affix_data.push(dup);
                }
                let wordlen = self.spell[i].word.len() as i32;
                self.spell[i].affix = curaffix;
                self.spell[i].len = wordlen;
            }
            self.affix_data = affix_data;
        }

        sort_spell_by(&mut self.spell, |a, b| bcmp(&a.word, &b.word));
        let n = self.spell.len() as i32;
        self.dictionary = self.mk_sp_node(0, n, 0)?;
        Ok(())
    }

    fn mk_sp_node(&mut self, low: i32, high: i32, level: i32) -> PgResult<Option<usize>> {
        // RECURSION GUARD. C's mkSPNode (spell.c:1637) recurses once per
        // character of the longest word and carries NO check_stack_depth();
        // upstream relies on caller-side word lengths staying small. This
        // port's frame is far larger than the C frame (owning PgVec/Vec
        // locals), so it reaches the OS guard page at a shallower `level`
        // than C does -- i.e. pgrust OS-crashes on input C survives. Admit
        // against the BYTE bound, exactly as the sibling recursion in
        // normalize.rs (SplitToVariants) already does.
        //
        // WHERE THIS SITS RELATIVE TO THE DIFFERENTIAL: raising 54001 here is
        // NOT self-evidently divergence-free. C reaches this input without
        // erroring, so on any input deep enough to trip our byte bound the two
        // sides genuinely disagree. That debt is DISCHARGED by the TWO-SIDED
        // 54001 RULE in the spellfam_diff driver (adjudicate_54001): a 54001
        // is only carved when the input's computed nesting ceiling clears a
        // byte-derived depth floor AND the C side's witnessed outcome on the
        // same input is success-or-54001; a shallow 54001 or one opposite a
        // different C error fails the harness, and every admitted carve is
        // counted and logged. The residual asymmetry (C succeeding where this
        // guard fires on a genuinely deep input) is the ratified threshold
        // non-surface — C's firing point is configuration-dependent even
        // between two C builds via the max_stack_depth GUC.
        check_stack_depth()?;
        let mcx = self.mcx;
        let mut nchar = 0;
        let mut lastchar: u8 = 0;
        let mut lownew = low;

        let mut i = low;
        while i < high {
            let sp = &self.spell[i as usize];
            if sp.len > level && lastchar != sp.word[level as usize] {
                nchar += 1;
                lastchar = sp.word[level as usize];
            }
            i += 1;
        }

        if nchar == 0 {
            return Ok(None);
        }

        let node_idx = self.alloc_sp_node(mcx)?;
        let mut data: Vec<SpNodeData> = Vec::with_capacity(nchar as usize);
        let mut cur = SpNodeData::empty();
        let mut have_cur = false;

        lastchar = 0;
        i = low;
        while i < high {
            let (splen, ch, sp_affix) = {
                let sp = &self.spell[i as usize];
                (
                    sp.len,
                    if sp.len > level { sp.word[level as usize] } else { 0 },
                    sp.affix,
                )
            };
            if splen > level {
                if lastchar != ch {
                    if lastchar != 0 {
                        cur.node = self.mk_sp_node(lownew, i, level + 1)?;
                        lownew = i;
                        data.push(cur);
                        cur = SpNodeData::empty();
                    }
                    lastchar = ch;
                }
                have_cur = true;
                cur.val = ch;
                if splen == level + 1 {
                    let mut clear_compound_only = false;
                    if cur.isword && cur.affix != sp_affix as u32 {
                        let cf_existing = cur.compoundflag;
                        let cf_new = self.make_compound_flags(sp_affix)?;
                        clear_compound_only = (FF_COMPOUNDONLY as u32 & cf_existing & cf_new) == 0;
                        let merged = self.merge_affix(cur.affix as i32, sp_affix)?;
                        cur.affix = merged as u32;
                    } else {
                        cur.affix = sp_affix as u32;
                    }
                    cur.isword = true;

                    let cf = self.make_compound_flags(cur.affix as i32)?;
                    cur.compoundflag = cf;

                    if (cur.compoundflag & FF_COMPOUNDONLY as u32) != 0
                        && (cur.compoundflag & FF_COMPOUNDFLAG as u32) == 0
                    {
                        cur.compoundflag |= FF_COMPOUNDFLAG as u32;
                    }
                    if clear_compound_only {
                        cur.compoundflag &= !(FF_COMPOUNDONLY as u32);
                    }
                }
            }
            i += 1;
        }

        if have_cur {
            cur.node = self.mk_sp_node(lownew, high, level + 1)?;
            data.push(cur);
        }

        let mut pv: PgVec<SpNodeData> = PgVec::new_in(mcx);
        pv.try_reserve(data.len()).map_err(|_| mcx.oom(data.len()))?;
        for d in data {
            pv.push(d);
        }
        self.sp_arena[node_idx].data = pv;
        Ok(Some(node_idx))
    }

    fn alloc_sp_node(&mut self, mcx: Mcx<'mcx>) -> PgResult<usize> {
        reserve_one(mcx, &mut self.sp_arena)?;
        self.sp_arena.push(SpNode { data: PgVec::new_in(mcx) });
        Ok(self.sp_arena.len() - 1)
    }

    fn mk_a_node(
        &mut self,
        low: i32,
        high: i32,
        level: i32,
        type_: i32,
    ) -> PgResult<Option<usize>> {
        // RECURSION GUARD -- see mk_sp_node. C's mkANode (spell.c:1828) has no
        // check_stack_depth() either, and here `level` advances once per
        // character of the longest affix repl string, which comes straight out
        // of the .aff file: the depth is input-controlled and unbounded. This
        // is the frame that killed the spell 10M differential floor four
        // times (ASan stack-overflow, 246 identical mk_a_node frames).
        check_stack_depth()?;
        let mcx = self.mcx;
        let mut nchar = 0;
        let mut lastchar: u8 = 0;
        let mut lownew = low;

        let mut i = low;
        while i < high {
            let a = &self.affixes[i as usize];
            if a.replen() > level && lastchar != getchar(a, level, type_) {
                nchar += 1;
                lastchar = getchar(a, level, type_);
            }
            i += 1;
        }
        if nchar == 0 {
            return Ok(None);
        }

        let node_idx = self.alloc_a_node(mcx, false)?;
        let mut data: Vec<AffixNodeData> = Vec::with_capacity(nchar as usize);
        let mut cur = AffixNodeData::empty(mcx);
        let mut have_cur = false;
        let mut naff: Vec<usize> = Vec::new();

        lastchar = 0;
        i = low;
        while i < high {
            let (replen, ch) = {
                let a = &self.affixes[i as usize];
                (
                    a.replen(),
                    if a.replen() > level { getchar(a, level, type_) } else { 0 },
                )
            };
            if replen > level {
                if lastchar != ch {
                    if lastchar != 0 {
                        cur.node = self.mk_a_node(lownew, i, level + 1, type_)?;
                        if !naff.is_empty() {
                            let mut aff: PgVec<usize> = PgVec::new_in(mcx);
                            aff.try_reserve(naff.len()).map_err(|_| mcx.oom(naff.len()))?;
                            for &x in &naff {
                                aff.push(x);
                            }
                            cur.aff = aff;
                            naff.clear();
                        }
                        data.push(cur);
                        cur = AffixNodeData::empty(mcx);
                        lownew = i;
                    }
                    lastchar = ch;
                }
                have_cur = true;
                cur.val = ch;
                if replen == level + 1 {
                    naff.push(i as usize);
                }
            }
            i += 1;
        }

        if have_cur {
            cur.node = self.mk_a_node(lownew, high, level + 1, type_)?;
            if !naff.is_empty() {
                let mut aff: PgVec<usize> = PgVec::new_in(mcx);
                aff.try_reserve(naff.len()).map_err(|_| mcx.oom(naff.len()))?;
                for &x in &naff {
                    aff.push(x);
                }
                cur.aff = aff;
            }
            data.push(cur);
        }

        let mut pv: PgVec<AffixNodeData> = PgVec::new_in(mcx);
        pv.try_reserve(data.len()).map_err(|_| mcx.oom(data.len()))?;
        for d in data {
            pv.push(d);
        }
        self.af_arena[node_idx].data = pv;
        Ok(Some(node_idx))
    }

    fn alloc_a_node(&mut self, mcx: Mcx<'mcx>, isvoid: bool) -> PgResult<usize> {
        reserve_one(mcx, &mut self.af_arena)?;
        self.af_arena.push(AffixNode { isvoid, data: PgVec::new_in(mcx) });
        Ok(self.af_arena.len() - 1)
    }

    fn mk_void_affix(&mut self, issuffix: bool, startsuffix: i32) -> PgResult<()> {
        let mcx = self.mcx;
        let start = if issuffix { startsuffix } else { 0 };
        let end = if issuffix { self.affixes.len() as i32 } else { startsuffix };

        let void_idx = self.alloc_a_node(mcx, true)?;
        let mut slot = AffixNodeData::empty(mcx);
        slot.node = if issuffix { self.suffix } else { self.prefix };

        let mut cnt = 0usize;
        let mut i = start;
        while i < end {
            if self.affixes[i as usize].replen() == 0 {
                cnt += 1;
            }
            i += 1;
        }

        if cnt > 0 {
            let mut aff: PgVec<usize> = PgVec::new_in(mcx);
            aff.try_reserve(cnt).map_err(|_| mcx.oom(cnt))?;
            let mut i = start;
            while i < end {
                if self.affixes[i as usize].replen() == 0 {
                    aff.push(i as usize);
                }
                i += 1;
            }
            slot.aff = aff;
        }

        let mut data: PgVec<AffixNodeData> = PgVec::new_in(mcx);
        reserve_one(mcx, &mut data)?;
        data.push(slot);
        self.af_arena[void_idx].data = data;

        if issuffix {
            self.suffix = Some(void_idx);
        } else {
            self.prefix = Some(void_idx);
        }
        Ok(())
    }

    fn is_affix_in_use(&self, affixflag: &[u8]) -> PgResult<bool> {
        for i in 0..self.affix_data.len() as i32 {
            if self.is_affix_flag_in_use(i, affixflag)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn ni_sort_affixes(&mut self) -> PgResult<()> {
        if self.affixes.is_empty() {
            return Ok(());
        }
        let mcx = self.mcx;

        sort_affix(&mut self.affixes);

        let mut firstsuffix = self.affixes.len() as i32;
        let mut cmpd: Vec<CmpdAffix> = Vec::new();

        let mut i: usize = 0;
        while i < self.affixes.len() {
            let (atype, aflagflags, areplen, aflag, arepl) = {
                let a = &self.affixes[i];
                (
                    a.type_,
                    a.flagflags,
                    a.replen(),
                    a.flag.as_slice().to_vec(),
                    a.repl.as_slice().to_vec(),
                )
            };
            if atype == FF_SUFFIX && (i as i32) < firstsuffix {
                firstsuffix = i as i32;
            }

            if (aflagflags & FF_COMPOUNDFLAG) != 0 && areplen > 0 && self.is_affix_in_use(&aflag)? {
                let issuffix = atype == FF_SUFFIX;
                let unique = match cmpd.last() {
                    None => true,
                    Some(prev) => {
                        issuffix != prev.issuffix
                            || strbncmp(&prev.affix, &arepl, prev.len as usize)
                                != core::cmp::Ordering::Equal
                    }
                };
                if unique {
                    cmpd.push(CmpdAffix {
                        affix: new_bytes(mcx, &arepl)?,
                        len: areplen,
                        issuffix,
                    });
                }
            }
            i += 1;
        }
        // C's { affix=NULL } terminator is the Vec length.
        let mut compound: PgVec<CmpdAffix> = PgVec::new_in(mcx);
        compound.try_reserve(cmpd.len()).map_err(|_| mcx.oom(cmpd.len()))?;
        for c in cmpd {
            compound.push(c);
        }
        self.compound_affix = compound;

        let naffixes = self.affixes.len() as i32;
        self.prefix = self.mk_a_node(0, firstsuffix, 0, FF_PREFIX)?;
        self.suffix = self.mk_a_node(firstsuffix, naffixes, 0, FF_SUFFIX)?;
        self.mk_void_affix(true, firstsuffix)?;
        self.mk_void_affix(false, firstsuffix)?;
        Ok(())
    }
}

// tsearch_readline_begin at spell.c:525 (NIImportDictionary), :1238/:1305
// (NIImportOOAffixes) and :1467 (NIImportAffixes): an open failure is
// `could not open <kind> file "%s": %m` with fopen's errno; while the file
// is open, every error carries the readline callback's per-line context
// (`TsearchReadline::with_context`).
pub(crate) fn open_file<'mcx>(
    mcx: Mcx<'mcx>,
    filename: &[u8],
    kind: &str,
) -> PgResult<TsearchReadline<'mcx>> {
    match ::ts_locale::tsearch_readline_begin(mcx, filename) {
        Ok(rd) => Ok(rd),
        Err(errno) => Err(::ts_locale::could_not_open_error(kind, filename, errno).into()),
    }
}

#[inline]
fn has_prefix(s: &[u8], lit: &[u8]) -> bool {
    s.len() >= lit.len() && &s[..lit.len()] == lit
}

#[inline]
fn strip_prefix<'a>(s: &'a [u8], lit: &[u8]) -> Option<&'a [u8]> {
    if has_prefix(s, lit) {
        Some(&s[lit.len()..])
    } else {
        None
    }
}

fn sort_spell_by<'mcx>(
    spell: &mut PgVec<'mcx, Spell<'mcx>>,
    mut cmp: impl FnMut(&Spell, &Spell) -> core::cmp::Ordering,
) {
    spell.as_mut_slice().sort_by(|a, b| cmp(a, b));
}

// cmpaffix: type first, then strcmp over repl for prefixes / strbcmp for suffixes.
fn sort_affix(affixes: &mut PgVec<Affix>) {
    affixes.as_mut_slice().sort_by(|a1, a2| {
        match a1.type_.cmp(&a2.type_) {
            core::cmp::Ordering::Equal => {}
            ord => return ord,
        }
        if a1.type_ == FF_PREFIX {
            bcmp(&a1.repl, &a2.repl)
        } else {
            strbcmp(&a1.repl, &a2.repl)
        }
    });
}
