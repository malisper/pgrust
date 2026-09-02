//! pg_locale_builtin.c case workers: unicode_case over UTF-8, with the
//! isalnum-transition word-boundary iterator for strtitle.

use crate::PgLocale;

pub(crate) fn strlower_builtin(dest: &mut [u8], src: &[u8], locale: &PgLocale) -> usize {
    unicode_case::unicode_strlower(dest, src, locale.builtin_casemap_full)
}

pub(crate) fn strupper_builtin(dest: &mut [u8], src: &[u8], locale: &PgLocale) -> usize {
    unicode_case::unicode_strupper(dest, src, locale.builtin_casemap_full)
}

pub(crate) fn strfold_builtin(dest: &mut [u8], src: &[u8], locale: &PgLocale) -> usize {
    unicode_case::unicode_strfold(dest, src, locale.builtin_casemap_full)
}

pub(crate) fn strtitle_builtin(dest: &mut [u8], src: &[u8], locale: &PgLocale) -> usize {
    let full = locale.builtin_casemap_full;
    let posix = !full;
    let mut offset = 0usize;
    let mut init = false;
    let mut prev_alnum = false;
    let mut wbnext = move || {
        while offset < src.len() && src[offset] != 0 {
            // upstream 9021c8f3cabc (18.6): unicode_case.c: defend against truncated UTF8.
            let ulen = wchar::pg_utf_mblen(&src[offset..]) as usize;
            let prev_offset = offset;
            if offset + ulen > src.len() {
                init = true;
                offset = src.len();
                return prev_offset;
            }
            let u = wchar::utf8_to_unicode(&src[offset..]);
            let curr_alnum = unicode_category::pg_u_isalnum(u, posix);
            if !init || curr_alnum != prev_alnum {
                init = true;
                offset += ulen;
                prev_alnum = curr_alnum;
                return prev_offset;
            }
            offset += ulen;
        }
        src.len()
    };
    unicode_case::unicode_strtitle(dest, src, full, &mut wbnext)
}
