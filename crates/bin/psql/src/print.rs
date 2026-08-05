//! Result rendering in psql's formats (print.c): aligned (default, border 1,
//! linestyle ascii), unaligned, and the expanded (\x) variants. Widths are
//! display widths (PQdsplen semantics) computed via the ported wchar crate.

use std::io::Write;

pub const FORMAT_ALIGNED: u8 = 0;
pub const FORMAT_UNALIGNED: u8 = 1;

#[derive(Clone)]
pub struct PrintOptions {
    pub format: u8,
    pub expanded: bool,
    pub tuples_only: bool,
    pub null_string: String,
    pub fieldsep: String,
    pub recordsep: String,
}

impl Default for PrintOptions {
    fn default() -> Self {
        PrintOptions {
            format: FORMAT_ALIGNED,
            expanded: false,
            tuples_only: false,
            null_string: String::new(),
            fieldsep: "|".into(),
            recordsep: "\n".into(),
        }
    }
}

pub struct Table {
    pub title: Option<String>,
    pub headers: Vec<String>,
    /// 'l' or 'r' per column.
    pub aligns: Vec<char>,
    /// None = NULL.
    pub cells: Vec<Vec<Option<String>>>,
    /// Extra footer lines (e.g. \d index lists). When None and not
    /// tuples-only, the default "(N rows)" footer is printed.
    pub footers: Option<Vec<String>>,
}

/// Display width of one string (single line, no newlines), PQdsplen-style.
pub fn dsplen(s: &str) -> usize {
    let b = s.as_bytes();
    let mut i = 0;
    let mut w = 0usize;
    while i < b.len() {
        let ml = wchar::pg_encoding_mblen(wchar::PG_UTF8, &b[i..]) as usize;
        let ml = ml.min(b.len() - i).max(1);
        let d = wchar::pg_encoding_dsplen(wchar::PG_UTF8, &b[i..]);
        w += if d < 0 { 1 } else { d as usize };
        i += ml;
    }
    w
}

/// Split a cell into display lines (psql treats \n as line break; \r alone
/// is kept as a (zero-width-ish) char — corpus avoids bare \r).
fn cell_lines(s: &str) -> Vec<&str> {
    s.split('\n').collect()
}

pub fn print_table(t: &Table, opt: &PrintOptions, out: &mut dyn Write) -> std::io::Result<()> {
    if opt.format == FORMAT_UNALIGNED {
        if opt.expanded {
            print_unaligned_vertical(t, opt, out)
        } else {
            print_unaligned_text(t, opt, out)
        }
    } else if opt.expanded {
        print_aligned_vertical(t, opt, out)
    } else {
        print_aligned_text(t, opt, out)
    }
}

fn default_footer(nrows: usize) -> String {
    if nrows == 1 {
        "(1 row)".to_string()
    } else {
        format!("({nrows} rows)")
    }
}

fn print_aligned_text(t: &Table, opt: &PrintOptions, out: &mut dyn Write) -> std::io::Result<()> {
    let ncols = t.headers.len();
    let nrows = t.cells.len();

    // Column widths: max over header and all cell lines.
    let mut widths: Vec<usize> = t.headers.iter().map(|h| dsplen(h)).collect();
    if opt.tuples_only {
        for w in widths.iter_mut() {
            *w = 0;
        }
    }
    // Resolve cell text (NULL replacement) once.
    let resolved: Vec<Vec<String>> = t
        .cells
        .iter()
        .map(|row| {
            row.iter()
                .map(|c| c.clone().unwrap_or_else(|| opt.null_string.clone()))
                .collect()
        })
        .collect();
    for row in &resolved {
        for (j, cell) in row.iter().enumerate() {
            for line in cell_lines(cell) {
                let w = dsplen(line);
                if w > widths[j] {
                    widths[j] = w;
                }
            }
        }
    }

    let total_width: usize = if ncols > 0 {
        widths.iter().map(|w| w + 2).sum::<usize>() + ncols.saturating_sub(1)
    } else {
        0
    };
    if let Some(title) = &t.title {
        // Centered over the total table width.
        let tw = dsplen(title);
        let lpad = if total_width > tw { (total_width - tw) / 2 } else { 0 };
        writeln!(out, "{}{}", " ".repeat(lpad), title)?;
    }
    if !opt.tuples_only {
        if ncols > 0 {
            // Header: centered, extra space to the right; fully padded
            // including the trailing space of the last column.
            let mut line = String::new();
            for (j, h) in t.headers.iter().enumerate() {
                if j > 0 {
                    line.push('|');
                }
                let hw = dsplen(h);
                let nb = widths[j].saturating_sub(hw);
                let l = nb / 2;
                let r = nb - l;
                line.push(' ');
                line.push_str(&" ".repeat(l));
                line.push_str(h);
                line.push_str(&" ".repeat(r));
                line.push(' ');
            }
            writeln!(out, "{line}")?;
        }
        // Separator ("--" for a zero-column result, as psql prints).
        let mut sep = String::new();
        for (j, w) in widths.iter().enumerate() {
            if j > 0 {
                sep.push('+');
            }
            sep.push_str(&"-".repeat(w + 2));
        }
        if ncols == 0 {
            sep.push_str("--");
        }
        writeln!(out, "{sep}")?;
    }

    for row in &resolved {
        if ncols == 0 {
            continue;
        }
        // Split every cell into lines; emit max-line-count physical lines.
        let split: Vec<Vec<&str>> = row.iter().map(|c| cell_lines(c)).collect();
        let nlines = split.iter().map(|l| l.len()).max().unwrap_or(1);
        for ln in 0..nlines {
            let mut line = String::new();
            for j in 0..ncols {
                if j > 0 {
                    line.push('|');
                }
                let cell_line = split[j].get(ln).copied().unwrap_or("");
                // In-cell continuation marker: '+' when more lines follow in
                // THIS cell (it takes the trailing-space slot).
                let more = ln + 1 < split[j].len();
                let w = dsplen(cell_line);
                let pad = widths[j].saturating_sub(w);
                let last = j + 1 == ncols;
                line.push(' ');
                if last && cell_line.is_empty() && !more {
                    // Trailing empty cell (NULL or exhausted continuation):
                    // psql emits nothing past the leading space.
                } else if t.aligns.get(j) == Some(&'r') {
                    line.push_str(&" ".repeat(pad));
                    line.push_str(cell_line);
                    if more {
                        line.push('+');
                    } else if !last {
                        line.push(' ');
                    }
                } else {
                    line.push_str(cell_line);
                    if more {
                        line.push_str(&" ".repeat(pad));
                        line.push('+');
                    } else if !last {
                        line.push_str(&" ".repeat(pad));
                        line.push(' ');
                    }
                }
            }
            writeln!(out, "{line}")?;
        }
    }

    match &t.footers {
        Some(fs) => {
            for f in fs {
                writeln!(out, "{f}")?;
            }
        }
        None => {
            if !opt.tuples_only {
                writeln!(out, "{}", default_footer(nrows))?;
            }
        }
    }
    writeln!(out)?;
    Ok(())
}

fn print_aligned_vertical(
    t: &Table,
    opt: &PrintOptions,
    out: &mut dyn Write,
) -> std::io::Result<()> {
    let ncols = t.headers.len();
    if let Some(title) = &t.title {
        writeln!(out, "{title}")?;
    }
    if t.cells.is_empty() {
        writeln!(out, "(0 rows)")?;
        writeln!(out)?;
        return Ok(());
    }
    let hwidth = t.headers.iter().map(|h| dsplen(h)).max().unwrap_or(0);
    let resolved: Vec<Vec<String>> = t
        .cells
        .iter()
        .map(|row| {
            row.iter()
                .map(|c| c.clone().unwrap_or_else(|| opt.null_string.clone()))
                .collect()
        })
        .collect();
    let mut dwidth = 0usize;
    for row in &resolved {
        for cell in row {
            for line in cell_lines(cell) {
                dwidth = dwidth.max(dsplen(line));
            }
        }
    }

    for (i, row) in resolved.iter().enumerate() {
        if !opt.tuples_only {
            // "-[ RECORD N ]----" padded with '-' out to the full line width
            // (hwidth + 3 + dwidth), never truncated below its own length.
            let hdr = format!("-[ RECORD {} ]", i + 1);
            let total = hwidth + 3 + dwidth;
            let mut line = hdr;
            while dsplen(&line) < total {
                line.push('-');
            }
            writeln!(out, "{line}")?;
        } else if i > 0 {
            writeln!(out)?;
        }
        for j in 0..ncols {
            let name = &t.headers[j];
            let lines = cell_lines(&row[j]);
            for (ln, l) in lines.iter().enumerate() {
                let more = ln + 1 < lines.len();
                let label = if ln == 0 { name.as_str() } else { "" };
                let pad = hwidth - dsplen(label);
                let mut outl = String::new();
                outl.push_str(label);
                outl.push_str(&" ".repeat(pad));
                outl.push_str(" | ");
                outl.push_str(l);
                if more {
                    // Value padded to the data width, wrap marker after.
                    outl.push_str(&" ".repeat(dwidth.saturating_sub(dsplen(l))));
                    outl.push('+');
                }
                writeln!(out, "{outl}")?;
            }
        }
    }
    if let Some(fs) = &t.footers {
        for f in fs {
            writeln!(out, "{f}")?;
        }
    }
    writeln!(out)?;
    Ok(())
}

fn print_unaligned_text(t: &Table, opt: &PrintOptions, out: &mut dyn Write) -> std::io::Result<()> {
    if let Some(title) = &t.title {
        writeln!(out, "{title}")?;
    }
    if !opt.tuples_only && !t.headers.is_empty() {
        let line = t.headers.join(&opt.fieldsep);
        write!(out, "{line}{}", opt.recordsep)?;
    }
    for row in &t.cells {
        let line = row
            .iter()
            .map(|c| c.clone().unwrap_or_else(|| opt.null_string.clone()))
            .collect::<Vec<_>>()
            .join(&opt.fieldsep);
        write!(out, "{line}{}", opt.recordsep)?;
    }
    match &t.footers {
        Some(fs) => {
            for f in fs {
                write!(out, "{f}{}", opt.recordsep)?;
            }
        }
        None => {
            if !opt.tuples_only {
                write!(out, "{}{}", default_footer(t.cells.len()), opt.recordsep)?;
            }
        }
    }
    Ok(())
}

fn print_unaligned_vertical(
    t: &Table,
    opt: &PrintOptions,
    out: &mut dyn Write,
) -> std::io::Result<()> {
    if let Some(title) = &t.title {
        writeln!(out, "{title}")?;
    }
    for (i, row) in t.cells.iter().enumerate() {
        if i > 0 {
            write!(out, "{}", opt.recordsep)?;
        }
        for (j, c) in row.iter().enumerate() {
            let v = c.clone().unwrap_or_else(|| opt.null_string.clone());
            write!(out, "{}{}{}{}", t.headers[j], opt.fieldsep, v, opt.recordsep)?;
        }
    }
    if let Some(fs) = &t.footers {
        for f in fs {
            write!(out, "{f}{}", opt.recordsep)?;
        }
    }
    Ok(())
}
