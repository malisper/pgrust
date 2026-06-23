//! `.dict` / `.affix` parsing and prefix-tree (trie) construction.
//!
//! Port of the spell.c construction routines: `NIImportDictionary`,
//! `NIImportAffixes` (ispell + Hunspell `OOAFFIX`), `NISortDictionary`,
//! `NISortAffixes`, and their static helpers (`getNextFlagFromString`,
//! `IsAffixFlagInUse`, `NIAddSpell`, `FindWord`, `NIAddAffix`, the field
//! parsers, the compound-flag table, `MergeAffix`, `mkSPNode`, `mkANode`,
//! `mkVoidAffix`, `isAffixInUse`).

extern crate alloc;

use alloc::format;
use alloc::vec::Vec;

use ::mcx::{Mcx, PgVec};
use ::types_error::PgResult;
use ::regex::{RegcompResult, REG_ADVANCED, REG_NOSUB};

use crate::{
    atoi, bcmp, bstrchr, bytes_lossy, config_file_error, elog_internal, findchar, findchar2,
    isdigit, isprint, isspace, new_bytes, pg_mblen_clamped, reserve_one, rs_compile, rs_is_regis,
    str_tolower, strbcmp, strbncmp, strtol, t_isalpha, t_iseq, Affix, AffixNode, AffixNodeData,
    AffixReg, CmpdAffix, CompoundAffixFlag, FlagKey, FlagMode, IspellDict, SpNode, SpNodeData,
    Spell, DEFAULT_COLLATION_OID, ERRCODE_INVALID_REGULAR_EXPRESSION, FF_COMPOUNDBEGIN,
    FF_COMPOUNDFLAG, FF_COMPOUNDFLAGMASK, FF_COMPOUNDFORBIDFLAG, FF_COMPOUNDLAST, FF_COMPOUNDMIDDLE,
    FF_COMPOUNDONLY, FF_COMPOUNDPERMITFLAG, FF_CROSSPRODUCT, FF_PREFIX, FF_SUFFIX, FLAGNUM_MAXSIZE,
};

/// `GETWCHAR(W,L,N,T)`: the byte at affix position `n` of word `w` (length `l`),
/// counting from the front for a prefix and the back for a suffix.
#[inline]
fn getwchar(w: &[u8], l: i32, n: i32, t: i32) -> u8 {
    let idx = if t == FF_PREFIX { n } else { l - 1 - n };
    w[idx as usize]
}

/// `GETCHAR(A,N,T)`: [`getwchar`] over an affix's `repl`.
#[inline]
fn getchar(a: &Affix, n: i32, t: i32) -> u8 {
    getwchar(&a.repl, a.repl.len() as i32, n, t)
}

impl<'mcx> IspellDict<'mcx> {
    /* ---- getNextFlagFromString ---- */

    /// `getNextFlagFromString`: consume the next flag from `sflagset` (advancing
    /// `*pos`), appending its canonical bytes to `out`. Mirrors the C state
    /// machine across `Char`/`Long`/`Num` flag modes.
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
                        )));
                    }
                    if !(0..=FLAGNUM_MAXSIZE).contains(&s_val) {
                        return Err(config_file_error(format!(
                            "affix flag \"{}\" is out of range",
                            bytes_lossy(rest)
                        )));
                    }
                    // C: `sflag += sprintf(sflag, "%0d", s);`
                    out.extend_from_slice(format!("{s_val}").as_bytes());
                    *pos += consumed;
                    while *pos < sflagset.len() {
                        let cc = &sflagset[*pos..];
                        if isdigit(cc[0]) {
                            if !met_comma {
                                return Err(config_file_error(format!(
                                    "invalid affix flag \"{}\"",
                                    bytes_lossy(cc)
                                )));
                            }
                            break;
                        } else if t_iseq(cc, b',') {
                            if met_comma {
                                return Err(config_file_error(format!(
                                    "invalid affix flag \"{}\"",
                                    bytes_lossy(cc)
                                )));
                            }
                            met_comma = true;
                        } else if !isspace(cc[0]) {
                            return Err(config_file_error(format!(
                                "invalid character in affix flag \"{}\"",
                                bytes_lossy(cc)
                            )));
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
            )));
        }
        Ok(())
    }

    /* ---- IsAffixFlagInUse ---- */

    /// `IsAffixFlagInUse`: does `AffixData[affix]` contain the flag `affixflag`?
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

    /* ---- NIAddSpell ---- */

    /// `NIAddSpell`: add a `word`/`flag` pair to `Conf->Spell`.
    fn ni_add_spell(&mut self, word: &[u8], flag: &[u8]) -> PgResult<()> {
        let mcx = self.mcx;
        let sp = Spell {
            word: new_bytes(mcx, word)?,
            flag: if !flag.is_empty() {
                new_bytes(mcx, flag)?
            } else {
                PgVec::new_in(mcx)
            },
            affix: 0,
            len: 0,
        };
        reserve_one(mcx, &mut self.spell)?;
        self.spell.push(sp);
        Ok(())
    }

    /* ---- FindWord ---- */

    /// `FindWord`: is `word` (with affix flag `affixflag`, compound mask `flag`)
    /// present in the dictionary trie? Returns 1 (found) or 0.
    pub(crate) fn find_word(&self, word: &[u8], affixflag: &[u8], mut flag: i32) -> PgResult<i32> {
        let mut node = self.dictionary;
        let mut ptr = 0usize; // byte index into `word`

        flag &= FF_COMPOUNDFLAGMASK;

        while let Some(ni) = node {
            if ptr >= word.len() {
                break;
            }
            let data = &self.sp_arena[ni].data;
            // binary search by `val` (the trie is sorted on the character byte)
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

    /* ---- NIAddAffix ---- */

    /// `NIAddAffix`: add an affix rule (with its compiled word-ending matcher).
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

        // Determine the matcher: simple ("."/empty), regis, or full regex.
        let reg = if mask == b"." || mask.is_empty() {
            AffixReg::Simple
        } else if rs_is_regis(mask)? {
            AffixReg::Regis(rs_compile(mcx, type_ == FF_SUFFIX, mask)?)
        } else {
            // Wrap the mask with the anchor (C `sprintf(tmask, "%s$"/"^%s")`),
            // widen to pg_wchar, and compile with REG_ADVANCED | REG_NOSUB.
            let mut tmask: Vec<u8> = Vec::with_capacity(mask.len() + 2);
            if type_ == FF_SUFFIX {
                tmask.extend_from_slice(mask);
                tmask.push(b'$');
            } else {
                tmask.push(b'^');
                tmask.extend_from_slice(mask);
            }
            let wmask = mbutils_seams::pg_mb2wchar_with_len::call(mcx, &tmask)?;
            match regex_core_seams::pg_regcomp::call(
                &wmask,
                REG_ADVANCED | REG_NOSUB,
                DEFAULT_COLLATION_OID,
            )? {
                RegcompResult::Compiled(c) => AffixReg::Regex(c),
                RegcompResult::Failed(f) => {
                    return Err(utils_error::ereport(::types_error::ERROR)
                        .errcode(ERRCODE_INVALID_REGULAR_EXPRESSION)
                        .errmsg(format!("invalid regular expression: {}", f.message))
                        .into_error());
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
            find: if !find.is_empty() {
                new_bytes(mcx, find)?
            } else {
                PgVec::new_in(mcx)
            },
            repl: if !repl.is_empty() {
                new_bytes(mcx, repl)?
            } else {
                PgVec::new_in(mcx)
            },
            reg,
        };
        reserve_one(mcx, &mut self.affixes)?;
        self.affixes.push(affix);
        Ok(())
    }

    /* ---- NIImportDictionary ---- */

    /// `NIImportDictionary`: load words from a `.dict` file into `Conf->Spell`.
    pub fn ni_import_dictionary(&mut self, filename: &[u8]) -> PgResult<()> {
        let mcx = self.mcx;
        let content = open_file(filename, "dictionary")?;

        for raw_line in lines(&content) {
            // mutable working copy of the line (NUL-free)
            let mut line: Vec<u8> = raw_line.to_vec();

            /* Extract flag from the line */
            let flag: Vec<u8>;
            if let Some(slash) = findchar(&line, b'/') {
                // terminate word at '/', then scan the flag run after it
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

            /* Remove trailing spaces (truncate at the first whitespace) */
            let mut s = 0usize;
            while s < line.len() {
                if isspace(line[s]) {
                    line.truncate(s);
                    break;
                }
                s += pg_mblen_clamped(&line[s..]);
            }

            let pstr = str_tolower(mcx, &line)?;
            self.ni_add_spell(&pstr, &flag)?;
        }
        Ok(())
    }

    /* ---- get_nextfield (Hunspell field tokenizer) ---- */

    /// `get_nextfield`: tokenize the next whitespace-delimited field of `str`
    /// (advancing `*pos`), copying it into `out`. Returns whether a field was
    /// found (the C `state == PAE_INMASK`). A `#` at field start ends parsing.
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
                // PAE_INMASK
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

    /* ---- parse_ooaffentry (Hunspell affix line) ---- */

    /// `parse_ooaffentry`: split a Hunspell affix line into
    /// `(type, flag, find, repl, mask)`, returning the number of fields read.
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
                    )));
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

    /* ---- parse_affentry (ispell format) ---- */

    /// `parse_affentry`: parse an ispell-format affix line into
    /// `(mask, find, repl)`; returns whether the entry is usable.
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
                    return Err(config_file_error("syntax error".into()));
                }
            } else if state == PAE_INFIND {
                if t_iseq(c, b',') {
                    state = PAE_WAIT_REPL;
                } else if t_isalpha(c) {
                    find.extend_from_slice(&c[..clen]);
                } else if !isspace(c[0]) {
                    return Err(config_file_error("syntax error".into()));
                }
            } else if state == PAE_WAIT_REPL {
                if t_iseq(c, b'-') {
                    break; // void repl
                } else if t_isalpha(c) {
                    repl.extend_from_slice(&c[..clen]);
                    state = PAE_INREPL;
                } else if !isspace(c[0]) {
                    return Err(config_file_error("syntax error".into()));
                }
            } else if state == PAE_INREPL {
                if t_iseq(c, b'#') {
                    break;
                } else if t_isalpha(c) {
                    repl.extend_from_slice(&c[..clen]);
                } else if !isspace(c[0]) {
                    return Err(config_file_error("syntax error".into()));
                }
            } else {
                return Err(elog_internal(format!(
                    "unrecognized state in parse_affentry: {state}"
                )));
            }
            pos += clen;
        }

        Ok(!mask.is_empty() && (!find.is_empty() || !repl.is_empty()))
    }

    /* ---- compound-affix flag table ---- */

    /// `setCompoundAffixFlagValue`: fill `entry` from the flag text `s`.
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
                )));
            }
            if !(0..=FLAGNUM_MAXSIZE).contains(&i) {
                return Err(config_file_error(format!(
                    "affix flag \"{}\" is out of range",
                    bytes_lossy(s)
                )));
            }
            entry.flag = FlagKey::Num(i as u32);
        } else {
            entry.flag = FlagKey::Str(s.to_vec());
        }
        entry.flag_mode = self.flag_mode;
        entry.value = val;
        Ok(())
    }

    /// `addCompoundAffixFlagValue`: parse the first flag from `s` and append a
    /// compound-flag table entry with `val`.
    fn add_compound_affix_flag_value(&mut self, s: &[u8], val: u32) -> PgResult<()> {
        // skip leading whitespace
        let mut start = 0usize;
        while start < s.len() && isspace(s[start]) {
            start += pg_mblen_clamped(&s[start..]);
        }
        if start >= s.len() {
            return Err(config_file_error("syntax error".into()));
        }

        // Get flag without \n / whitespace.
        let mut sflag: Vec<u8> = Vec::new();
        let mut p = start;
        while p < s.len() && !isspace(s[p]) && s[p] != b'\n' {
            let clen = copy_char_into_vec(&s[p..], &mut sflag);
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

    /// `cmpcmdflag`: ordering for the compound-flag table.
    fn cmpcmdflag(f1: &CompoundAffixFlag, f2: &CompoundAffixFlag) -> core::cmp::Ordering {
        debug_assert_eq!(f1.flag_mode, f2.flag_mode);
        match (&f1.flag, &f2.flag) {
            (FlagKey::Num(a), FlagKey::Num(b)) => a.cmp(b),
            (FlagKey::Str(a), FlagKey::Str(b)) => bcmp(a, b),
            // Mixed modes never occur (debug_assert above); order Num before Str.
            (FlagKey::Num(_), FlagKey::Str(_)) => core::cmp::Ordering::Less,
            (FlagKey::Str(_), FlagKey::Num(_)) => core::cmp::Ordering::Greater,
        }
    }

    /// `getCompoundAffixFlagValue`: OR together the values of every compound
    /// flag named in `s`.
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

    /// `getAffixFlagSet`: resolve an `AF`-alias index (or return `s` unchanged).
    /// Returns the resolved flagset bytes.
    fn get_affix_flag_set(&self, s: &[u8]) -> PgResult<Vec<u8>> {
        if self.use_flag_aliases && !s.is_empty() {
            let (raw, consumed, valid) = strtol(s);
            if consumed == 0 || !valid {
                return Err(config_file_error(format!(
                    "invalid affix alias \"{}\"",
                    bytes_lossy(s)
                )));
            }
            let curaffix = raw as i32;
            if curaffix > 0 && curaffix < self.affix_data.len() as i32 {
                // Do not subtract 1 (the empty string was added in NIImportOOAffixes).
                return Ok(self.affix_data[curaffix as usize].as_slice().to_vec());
            } else if curaffix > self.affix_data.len() as i32 {
                return Err(config_file_error(format!(
                    "invalid affix alias \"{}\"",
                    bytes_lossy(s)
                )));
            }
            Ok(Vec::new())
        } else {
            Ok(s.to_vec())
        }
    }

    /* ---- NIImportOOAffixes (Hunspell) ---- */

    fn ni_import_oo_affixes(&mut self, filename: &[u8]) -> PgResult<()> {
        let mcx = self.mcx;
        self.usecompound = false;
        self.use_flag_aliases = false;
        self.flag_mode = FlagMode::Char;

        let content = open_file(filename, "affix")?;

        // First pass: find any flag / compound-flag declarations.
        for line in lines(&content) {
            if line.is_empty() || isspace(line[0]) || t_iseq(line, b'#') {
                continue;
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
                        ));
                    }
                }
            }
        }

        if self.compound_affix_flags.len() > 1 {
            // qsort the compound-flag table.
            let mut tmp: Vec<CompoundAffixFlag> = self.compound_affix_flags.iter().cloned().collect();
            tmp.sort_by(Self::cmpcmdflag);
            let mut rebuilt: PgVec<CompoundAffixFlag> = PgVec::new_in(mcx);
            rebuilt.try_reserve(tmp.len()).map_err(|_| mcx.oom(tmp.len()))?;
            for e in tmp {
                rebuilt.push(e);
            }
            self.compound_affix_flags = rebuilt;
        }

        // Second pass: parse AF aliases + PFX/SFX rules.
        let mut type_ = Vec::new();
        let mut sflag = Vec::new();
        let mut mask = Vec::new();
        let mut find = Vec::new();
        let mut repl = Vec::new();
        let mut is_suffix = false;
        let mut naffix: i32 = 0;
        let mut curaffix: i32 = 0;
        let mut flagflags: i32 = 0;

        for line in lines(&content) {
            if line.is_empty() || isspace(line[0]) || t_iseq(line, b'#') {
                continue;
            }

            let fields_read =
                Self::parse_ooaffentry(line, &mut type_, &mut sflag, &mut find, &mut repl, &mut mask)?;

            let ptype = str_tolower(mcx, &type_)?;

            /* First try to parse AF parameter (alias compression) */
            if has_prefix(&ptype, b"af") {
                if !self.use_flag_aliases {
                    self.use_flag_aliases = true;
                    naffix = atoi(&sflag);
                    if naffix <= 0 {
                        return Err(config_file_error(
                            "invalid number of flag vector aliases".into(),
                        ));
                    }
                    naffix += 1;
                    // AffixData[curaffix=0] = VoidString
                    self.affix_data
                        .try_reserve(naffix as usize)
                        .map_err(|_| mcx.oom(naffix as usize))?;
                    for _ in 0..naffix {
                        self.affix_data.push(PgVec::new_in(mcx));
                    }
                    curaffix += 1;
                } else if curaffix < naffix {
                    let dup = new_bytes(mcx, &sflag)?;
                    self.affix_data[curaffix as usize] = dup;
                    curaffix += 1;
                } else {
                    return Err(config_file_error(format!(
                        "number of aliases exceeds specified number {}",
                        naffix - 1
                    )));
                }
                continue;
            }
            /* Else try to parse prefixes and suffixes */
            if fields_read < 4 || (!has_prefix(&ptype, b"sfx") && !has_prefix(&ptype, b"pfx")) {
                continue;
            }

            let sflaglen = sflag.len();
            if sflaglen == 0
                || (sflaglen > 1 && self.flag_mode == FlagMode::Char)
                || (sflaglen > 2 && self.flag_mode == FlagMode::Long)
            {
                continue;
            }

            if fields_read == 4 {
                is_suffix = has_prefix(&ptype, b"sfx");
                if t_iseq(&find, b'y') || t_iseq(&find, b'Y') {
                    flagflags = FF_CROSSPRODUCT;
                } else {
                    flagflags = 0;
                }
            } else {
                let mut aflg: i32 = 0;
                if let Some(slash) = bstrchr(&repl, b'/') {
                    let fs = self.get_affix_flag_set(&repl[slash + 1..])?;
                    aflg |= self.get_compound_affix_flag_value(&fs)?;
                }
                let mut prepl = str_tolower(mcx, &repl)?;
                if let Some(slash) = bstrchr(&prepl, b'/') {
                    prepl.truncate(slash);
                }
                let mut pfind = str_tolower(mcx, &find)?;
                let pmask = str_tolower(mcx, &mask)?;
                if t_iseq(&find, b'0') {
                    pfind.clear();
                }
                if t_iseq(&repl, b'0') {
                    prepl.clear();
                }

                self.ni_add_affix(
                    &sflag,
                    flagflags | aflg,
                    &pmask,
                    &pfind,
                    &prepl,
                    if is_suffix { FF_SUFFIX } else { FF_PREFIX },
                )?;
            }
        }
        Ok(())
    }

    /* ---- NIImportAffixes ---- */

    /// `NIImportAffixes`: load an affix file (ispell or Hunspell `OOAFFIX`).
    pub fn ni_import_affixes(&mut self, filename: &[u8]) -> PgResult<()> {
        let mcx = self.mcx;
        self.usecompound = false;
        self.use_flag_aliases = false;
        self.flag_mode = FlagMode::Char;

        let content = open_file(filename, "affix")?;

        let mut flag: Vec<u8> = Vec::new();
        let mut mask = Vec::new();
        let mut find = Vec::new();
        let mut repl = Vec::new();
        let mut suffixes = false;
        let mut prefixes = false;
        let mut flagflags: i32 = 0;
        let mut oldformat = false;
        let mut goto_newformat = false;

        for line in lines(&content) {
            let pstr = str_tolower(mcx, line)?;

            /* Skip comments and empty lines (C: pstr[0]=='#' || pstr[0]=='\n') */
            if pstr.first() == Some(&b'#') || pstr.first() == Some(&b'\n') {
                continue;
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
                    oldformat = true;
                    continue;
                }
            }
            if has_prefix(&pstr, b"suffixes") {
                suffixes = true;
                prefixes = false;
                oldformat = true;
                continue;
            }
            if has_prefix(&pstr, b"prefixes") {
                suffixes = false;
                prefixes = true;
                oldformat = true;
                continue;
            }
            if has_prefix(&pstr, b"flag") {
                // operate on the non-lowercased string after "flag"
                let mut s = 4usize.min(line.len());
                flagflags = 0;
                while s < line.len() && isspace(line[s]) {
                    s += pg_mblen_clamped(&line[s..]);
                }
                if line.get(s) == Some(&b'*') {
                    flagflags |= FF_CROSSPRODUCT;
                    s += 1;
                } else if line.get(s) == Some(&b'~') {
                    flagflags |= FF_COMPOUNDONLY;
                    s += 1;
                }
                if line.get(s) == Some(&b'\\') {
                    s += 1;
                }
                if s < line.len() && pg_mblen_clamped(&line[s..]) == 1 {
                    flag.clear();
                    flag.push(line[s]);
                    s += 1;
                    let c = line.get(s).copied().unwrap_or(0);
                    if c == 0 || c == b'#' || c == b'\n' || c == b':' || isspace(c) {
                        oldformat = true;
                        continue;
                    }
                }
                goto_newformat = true;
                break;
            }
            if has_prefix(line, b"COMPOUNDFLAG")
                || has_prefix(line, b"COMPOUNDMIN")
                || has_prefix(line, b"PFX")
                || has_prefix(line, b"SFX")
            {
                goto_newformat = true;
                break;
            }

            if !suffixes && !prefixes {
                continue;
            }

            if !Self::parse_affentry(&pstr, &mut mask, &mut find, &mut repl)? {
                continue;
            }

            self.ni_add_affix(
                &flag,
                flagflags,
                &mask,
                &find,
                &repl,
                if suffixes { FF_SUFFIX } else { FF_PREFIX },
            )?;
        }

        if !goto_newformat {
            return Ok(());
        }

        /* isnewformat: */
        if oldformat {
            return Err(config_file_error(
                "affix file contains both old-style and new-style commands".into(),
            ));
        }

        self.ni_import_oo_affixes(filename)
    }

    /* ---- MergeAffix ---- */

    /// `MergeAffix`: merge the flag sets `AffixData[a1]` and `AffixData[a2]`,
    /// appending the merged set and returning its index (or `a1`/`a2` if one is
    /// empty).
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

    /* ---- makeCompoundFlags ---- */

    fn make_compound_flags(&self, affix: i32) -> PgResult<u32> {
        debug_assert!(affix < self.affix_data.len() as i32);
        let data = &self.affix_data[affix as usize];
        Ok((self.get_compound_affix_flag_value(data)? & FF_COMPOUNDFLAGMASK) as u32)
    }

    /* ---- NISortDictionary ---- */

    /// `NISortDictionary`: sort the word list and build the dictionary trie.
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
                        )));
                    }
                    let ca = ca as i32;
                    if ca < 0 || ca >= self.affix_data.len() as i32 {
                        return Err(config_file_error(format!(
                            "invalid affix alias \"{}\"",
                            bytes_lossy(&flagbytes)
                        )));
                    }
                    let endc = flagbytes.get(consumed).copied().unwrap_or(0);
                    if endc != 0 && !isdigit(endc) && !isspace(endc) {
                        return Err(config_file_error(format!(
                            "invalid affix alias \"{}\"",
                            bytes_lossy(&flagbytes)
                        )));
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
            /* Count the number of different flags used in the dictionary */
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

            // AffixData = palloc0(naffix); then fill while sorting.
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

        /* Start building the prefix tree */
        sort_spell_by(&mut self.spell, |a, b| bcmp(&a.word, &b.word));
        let n = self.spell.len() as i32;
        self.dictionary = self.mk_sp_node(0, n, 0)?;
        Ok(())
    }

    /* ---- mkSPNode ---- */

    /// `mkSPNode`: build the word trie for `Spell[low..high]` at `level`,
    /// returning the new node's arena index (or `None` for an empty subtree).
    fn mk_sp_node(&mut self, low: i32, high: i32, level: i32) -> PgResult<Option<usize>> {
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

        // Allocate the node up front.
        let node_idx = self.alloc_sp_node(mcx)?;
        // Build the data slots into a local vector, then move it into the arena
        // (avoids interleaving &mut self.sp_arena with recursive mk_sp_node).
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
                    if sp.len > level {
                        sp.word[level as usize]
                    } else {
                        0
                    },
                    sp.affix,
                )
            };
            if splen > level {
                if lastchar != ch {
                    if lastchar != 0 {
                        // Next level of the prefix tree.
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

        // Final slot: next level of the prefix tree.
        if have_cur {
            cur.node = self.mk_sp_node(lownew, high, level + 1)?;
            data.push(cur);
        }

        // Move the data vector into the (already-allocated) arena node, charging it.
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
        self.sp_arena.push(SpNode {
            data: PgVec::new_in(mcx),
        });
        Ok(self.sp_arena.len() - 1)
    }

    /* ---- mkANode ---- */

    /// `mkANode`: build the affix trie for `Affix[low..high]` at `level`,
    /// returning the new node's arena index.
    fn mk_a_node(
        &mut self,
        low: i32,
        high: i32,
        level: i32,
        type_: i32,
    ) -> PgResult<Option<usize>> {
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
                    if a.replen() > level {
                        getchar(a, level, type_)
                    } else {
                        0
                    },
                )
            };
            if replen > level {
                if lastchar != ch {
                    if lastchar != 0 {
                        // Next level of the prefix tree.
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
        self.af_arena.push(AffixNode {
            isvoid,
            data: PgVec::new_in(mcx),
        });
        Ok(self.af_arena.len() - 1)
    }

    /* ---- mkVoidAffix ---- */

    /// `mkVoidAffix`: prepend a synthetic "void" node (holding the empty-`repl`
    /// affixes) to the prefix or suffix trie.
    fn mk_void_affix(&mut self, issuffix: bool, startsuffix: i32) -> PgResult<()> {
        let mcx = self.mcx;
        let start = if issuffix { startsuffix } else { 0 };
        let end = if issuffix {
            self.affixes.len() as i32
        } else {
            startsuffix
        };

        let void_idx = self.alloc_a_node(mcx, true)?;
        let mut slot = AffixNodeData::empty(mcx);
        slot.node = if issuffix { self.suffix } else { self.prefix };

        // Count affixes with empty replace string.
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

    /* ---- isAffixInUse ---- */

    fn is_affix_in_use(&self, affixflag: &[u8]) -> PgResult<bool> {
        for i in 0..self.affix_data.len() as i32 {
            if self.is_affix_flag_in_use(i, affixflag)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /* ---- NISortAffixes ---- */

    /// `NISortAffixes`: sort the affix list, build the compound-affix list, and
    /// build the prefix/suffix tries.
    pub fn ni_sort_affixes(&mut self) -> PgResult<()> {
        if self.affixes.is_empty() {
            return Ok(());
        }
        let mcx = self.mcx;

        // qsort the affix array by cmpaffix.
        sort_affix(&mut self.affixes);

        // Build the compound-affix list.
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
        // C appends a terminating { affix=NULL } entry; with an owned Vec the
        // length itself is the terminator, so no sentinel is stored.
        let mut compound: PgVec<CmpdAffix> = PgVec::new_in(mcx);
        compound.try_reserve(cmpd.len()).map_err(|_| mcx.oom(cmpd.len()))?;
        for c in cmpd {
            compound.push(c);
        }
        self.compound_affix = compound;

        /* Start building the prefix tree */
        let naffixes = self.affixes.len() as i32;
        self.prefix = self.mk_a_node(0, firstsuffix, 0, FF_PREFIX)?;
        self.suffix = self.mk_a_node(firstsuffix, naffixes, 0, FF_SUFFIX)?;
        self.mk_void_affix(true, firstsuffix)?;
        self.mk_void_affix(false, firstsuffix)?;
        Ok(())
    }
}

/* ====================== free-function helpers ====================== */

/// Append the whole leading multibyte character of `src` to `out`; returns its
/// byte length (a `Vec` analog of [`copy_char`], used where no context exists).
fn copy_char_into_vec(src: &[u8], out: &mut Vec<u8>) -> usize {
    let clen = pg_mblen_clamped(src);
    out.extend_from_slice(&src[..clen]);
    clen
}

/// Open a config file via the seam; map a failure to the C
/// `could not open <kind> file "%s": %m` error.
fn open_file(filename: &[u8], kind: &str) -> PgResult<Vec<u8>> {
    ts_locale_seams::readfile::call(filename).map_err(|errno| {
        config_file_error(format!(
            "could not open {kind} file \"{}\": {errno}",
            bytes_lossy(filename)
        ))
    })
}

/// Iterate over the lines of `content`, **keeping** the trailing `\n` (the C
/// `tsearch_readline` returns the line with its newline; the parsers handle the
/// `\n` explicitly). A final line without a newline is still yielded.
fn lines(content: &[u8]) -> impl Iterator<Item = &[u8]> {
    LineIter {
        buf: content,
        pos: 0,
    }
}

struct LineIter<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Iterator for LineIter<'a> {
    type Item = &'a [u8];
    fn next(&mut self) -> Option<&'a [u8]> {
        if self.pos >= self.buf.len() {
            return None;
        }
        let start = self.pos;
        let mut end = start;
        while end < self.buf.len() && self.buf[end] != b'\n' {
            end += 1;
        }
        let line_end = if end < self.buf.len() { end + 1 } else { end };
        self.pos = line_end;
        Some(&self.buf[start..line_end])
    }
}

/// `STRNCMP(s, lit) == 0`: does `s` begin with the literal `lit`?
#[inline]
fn has_prefix(s: &[u8], lit: &[u8]) -> bool {
    s.len() >= lit.len() && &s[..lit.len()] == lit
}

/// If `s` begins with `lit`, the bytes after it; else `None`.
#[inline]
fn strip_prefix<'a>(s: &'a [u8], lit: &[u8]) -> Option<&'a [u8]> {
    if has_prefix(s, lit) {
        Some(&s[lit.len()..])
    } else {
        None
    }
}

/// Stable sort the `Spell` array by `cmp` (the C `qsort`; ties are broken
/// arbitrarily there, so any consistent order is acceptable).
fn sort_spell_by<'mcx>(
    spell: &mut PgVec<'mcx, Spell<'mcx>>,
    mut cmp: impl FnMut(&Spell, &Spell) -> core::cmp::Ordering,
) {
    spell.as_mut_slice().sort_by(|a, b| cmp(a, b));
}

/// `cmpaffix`: type first, then forward (`strcmp`) for prefixes / backward
/// (`strbcmp`) for suffixes, over `repl`.
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
