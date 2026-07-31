//! Input sources: stdin, script files (-f, \i), and a small interactive
//! line editor with history (arrows/backspace/Ctrl-C/Ctrl-D) — deliberately
//! dependency-free per repo convention. Tab completion is out of scope
//! (increment 1); tabs insert literally.

use std::io::{BufRead, BufReader, Read, Write};

enum Source {
    Stdin(BufReader<std::io::Stdin>),
    File(BufReader<std::fs::File>),
}

impl Source {
    fn read_line(&mut self) -> Option<String> {
        let mut s = String::new();
        let n = match self {
            Source::Stdin(r) => r.read_line(&mut s),
            Source::File(r) => r.read_line(&mut s),
        };
        match n {
            Ok(0) | Err(_) => None,
            Ok(_) => {
                while s.ends_with('\n') || s.ends_with('\r') {
                    s.pop();
                }
                Some(s)
            }
        }
    }
}

pub struct InputStack {
    sources: Vec<Source>,
    history: Vec<String>,
}

impl InputStack {
    pub fn empty() -> Self {
        InputStack { sources: Vec::new(), history: Vec::new() }
    }

    pub fn stdin() -> Self {
        InputStack {
            sources: vec![Source::Stdin(BufReader::new(std::io::stdin()))],
            history: Vec::new(),
        }
    }

    pub fn files(paths: &[String]) -> Result<Self, String> {
        let mut sources = Vec::new();
        // Stack pops from the end: push in reverse order.
        for p in paths.iter().rev() {
            if p == "-" {
                sources.push(Source::Stdin(BufReader::new(std::io::stdin())));
            } else {
                let f = std::fs::File::open(p)
                    .map_err(|e| format!("{p}: {}", io_errmsg(&e)))?;
                sources.push(Source::File(BufReader::new(f)));
            }
        }
        Ok(InputStack { sources, history: Vec::new() })
    }

    /// Push an include file (\i) on top of the stack.
    pub fn push_file(&mut self, path: &str) -> Result<(), String> {
        let f = std::fs::File::open(path).map_err(|e| format!("{path}: {}", io_errmsg(&e)))?;
        self.sources.push(Source::File(BufReader::new(f)));
        Ok(())
    }

    /// Pop the exhausted top source. Returns true if sources remain.
    pub fn pop(&mut self) -> bool {
        self.sources.pop();
        !self.sources.is_empty()
    }

    /// Plain line read from the current (top) source; None at its EOF.
    pub fn read_line_raw(&mut self) -> Option<String> {
        loop {
            let src = self.sources.last_mut()?;
            match src.read_line() {
                Some(l) => return Some(l),
                None => {
                    // Only auto-pop nested sources; the bottom source's EOF
                    // is the caller's EOF.
                    if self.sources.len() > 1 {
                        self.sources.pop();
                        continue;
                    }
                    return None;
                }
            }
        }
    }

    /// Interactive read with prompt + line editing + history. Falls back to
    /// a plain read when stdin is not a terminal-editable stream.
    pub fn read_line_interactive(&mut self, prompt: &str) -> Option<String> {
        // If the top of stack is a file (\i from interactive), read plainly.
        if self.sources.len() > 1 {
            return self.read_line_raw();
        }
        match read_line_edited(prompt, &mut self.history) {
            Some(EditResult::Line(l)) => {
                if !l.trim().is_empty() {
                    self.history.push(l.clone());
                }
                Some(l)
            }
            Some(EditResult::Interrupt) => Some(String::new()),
            None => None,
        }
    }
}

fn io_errmsg(e: &std::io::Error) -> String {
    // strerror-style text, matching what psql shows ("No such file or
    // directory").
    #[cfg(unix)]
    {
        if let Some(code) = e.raw_os_error() {
            unsafe {
                let p = libc::strerror(code);
                if !p.is_null() {
                    return std::ffi::CStr::from_ptr(p).to_string_lossy().into_owned();
                }
            }
        }
    }
    e.to_string()
}

enum EditResult {
    Line(String),
    Interrupt,
}

#[cfg(unix)]
fn read_line_edited(prompt: &str, history: &mut Vec<String>) -> Option<EditResult> {
    use std::os::fd::AsRawFd;
    let stdin_fd = std::io::stdin().as_raw_fd();
    // Raw mode.
    let mut orig: libc::termios = unsafe { std::mem::zeroed() };
    if unsafe { libc::tcgetattr(stdin_fd, &mut orig) } != 0 {
        // Not a tty after all: plain read.
        print!("{prompt}");
        let _ = std::io::stdout().flush();
        let mut s = String::new();
        return match std::io::stdin().read_line(&mut s) {
            Ok(0) | Err(_) => None,
            Ok(_) => {
                while s.ends_with('\n') || s.ends_with('\r') {
                    s.pop();
                }
                Some(EditResult::Line(s))
            }
        };
    }
    let mut raw = orig;
    raw.c_lflag &= !(libc::ECHO | libc::ICANON | libc::ISIG);
    raw.c_iflag &= !(libc::IXON | libc::ICRNL);
    unsafe { libc::tcsetattr(stdin_fd, libc::TCSANOW, &raw) };

    let restore = |orig: &libc::termios| unsafe {
        libc::tcsetattr(stdin_fd, libc::TCSANOW, orig);
    };

    let mut buf: Vec<char> = Vec::new();
    let mut pos = 0usize;
    let mut hist_idx = history.len();
    let mut saved_line: Option<Vec<char>> = None;
    let mut out = std::io::stdout();
    let redraw = |out: &mut std::io::Stdout, buf: &[char], pos: usize| {
        let line: String = buf.iter().collect();
        let _ = write!(out, "\r\x1b[K{prompt}{line}");
        let back = buf.len() - pos;
        if back > 0 {
            let _ = write!(out, "\x1b[{back}D");
        }
        let _ = out.flush();
    };
    let _ = write!(out, "{prompt}");
    let _ = out.flush();

    let mut byte = [0u8; 1];
    let mut pending: Vec<u8> = Vec::new();
    loop {
        let n = unsafe { libc::read(stdin_fd, byte.as_mut_ptr().cast(), 1) };
        if n <= 0 {
            restore(&orig);
            let _ = writeln!(out);
            return if buf.is_empty() { None } else { Some(EditResult::Line(buf.iter().collect())) };
        }
        let b = byte[0];
        match b {
            b'\r' | b'\n' => {
                restore(&orig);
                let _ = writeln!(out);
                let _ = out.flush();
                return Some(EditResult::Line(buf.iter().collect()));
            }
            0x03 => {
                // Ctrl-C: abandon the line.
                restore(&orig);
                let _ = writeln!(out, "^C");
                let _ = out.flush();
                return Some(EditResult::Interrupt);
            }
            0x04 => {
                // Ctrl-D on empty line = EOF.
                if buf.is_empty() {
                    restore(&orig);
                    let _ = writeln!(out);
                    return None;
                }
            }
            0x7f | 0x08 => {
                if pos > 0 {
                    buf.remove(pos - 1);
                    pos -= 1;
                    redraw(&mut out, &buf, pos);
                }
            }
            0x01 => {
                pos = 0;
                redraw(&mut out, &buf, pos);
            }
            0x05 => {
                pos = buf.len();
                redraw(&mut out, &buf, pos);
            }
            0x0b => {
                buf.truncate(pos);
                redraw(&mut out, &buf, pos);
            }
            0x15 => {
                buf.drain(..pos);
                pos = 0;
                redraw(&mut out, &buf, pos);
            }
            0x1b => {
                // Escape sequence: read [ + final byte.
                let mut b2 = [0u8; 1];
                if unsafe { libc::read(stdin_fd, b2.as_mut_ptr().cast(), 1) } <= 0 {
                    continue;
                }
                if b2[0] != b'[' {
                    continue;
                }
                let mut b3 = [0u8; 1];
                if unsafe { libc::read(stdin_fd, b3.as_mut_ptr().cast(), 1) } <= 0 {
                    continue;
                }
                match b3[0] {
                    b'A' => {
                        // Up: previous history entry.
                        if hist_idx > 0 {
                            if hist_idx == history.len() {
                                saved_line = Some(buf.clone());
                            }
                            hist_idx -= 1;
                            buf = history[hist_idx].chars().collect();
                            pos = buf.len();
                            redraw(&mut out, &buf, pos);
                        }
                    }
                    b'B' => {
                        if hist_idx < history.len() {
                            hist_idx += 1;
                            buf = if hist_idx == history.len() {
                                saved_line.take().unwrap_or_default()
                            } else {
                                history[hist_idx].chars().collect()
                            };
                            pos = buf.len();
                            redraw(&mut out, &buf, pos);
                        }
                    }
                    b'C' => {
                        if pos < buf.len() {
                            pos += 1;
                            redraw(&mut out, &buf, pos);
                        }
                    }
                    b'D' => {
                        if pos > 0 {
                            pos -= 1;
                            redraw(&mut out, &buf, pos);
                        }
                    }
                    b'3' => {
                        // Delete key: consume '~'.
                        let mut b4 = [0u8; 1];
                        let _ = unsafe { libc::read(stdin_fd, b4.as_mut_ptr().cast(), 1) };
                        if pos < buf.len() {
                            buf.remove(pos);
                            redraw(&mut out, &buf, pos);
                        }
                    }
                    b'H' => {
                        pos = 0;
                        redraw(&mut out, &buf, pos);
                    }
                    b'F' => {
                        pos = buf.len();
                        redraw(&mut out, &buf, pos);
                    }
                    _ => {}
                }
            }
            _ => {
                // Assemble UTF-8 sequences.
                pending.push(b);
                if let Ok(s) = std::str::from_utf8(&pending) {
                    if let Some(c) = s.chars().next() {
                        buf.insert(pos, c);
                        pos += 1;
                        pending.clear();
                        redraw(&mut out, &buf, pos);
                    }
                } else if pending.len() >= 4 {
                    pending.clear();
                }
            }
        }
    }
}

#[cfg(not(unix))]
fn read_line_edited(prompt: &str, _history: &mut Vec<String>) -> Option<EditResult> {
    print!("{prompt}");
    let _ = std::io::stdout().flush();
    let mut s = String::new();
    match std::io::stdin().read_line(&mut s) {
        Ok(0) | Err(_) => None,
        Ok(_) => {
            while s.ends_with('\n') || s.ends_with('\r') {
                s.pop();
            }
            Some(EditResult::Line(s))
        }
    }
}

// Keep Read imported on non-unix builds.
#[allow(unused)]
fn _read_marker<R: Read>(_r: R) {}
