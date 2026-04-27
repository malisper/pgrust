use std::fs;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(target_os = "macos")]
use std::ffi::CString;

use crate::backend::access::transam::{ControlFileState, ControlFileStore};
use crate::pgrust::cluster::Cluster;
use crate::pgrust::database::{Database, DatabaseOpenOptions};

static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);
static GOLDEN_SEED_DIR: OnceLock<PathBuf> = OnceLock::new();

pub(crate) fn scratch_temp_dir(suite: &str, label: &str) -> PathBuf {
    let path = unique_temp_dir(suite, label);
    let _ = fs::remove_dir_all(&path);
    fs::create_dir_all(&path).unwrap();
    path
}

pub(crate) fn seeded_temp_dir(suite: &str, label: &str) -> PathBuf {
    let seed = golden_seed_dir();
    let path = unique_temp_dir(suite, label);
    let _ = fs::remove_dir_all(&path);
    clone_tree(seed, &path).unwrap();
    path
}

fn golden_seed_dir() -> &'static PathBuf {
    GOLDEN_SEED_DIR.get_or_init(|| {
        let seed = scratch_temp_dir("golden_seed", "cluster");
        {
            let cluster = Cluster::open_with_options(seed.clone(), DatabaseOpenOptions::new(16))
                .expect("bootstrap golden seed cluster");
            drop(cluster);
        }

        let control = ControlFileStore::load(&seed)
            .expect("load golden seed control file")
            .snapshot();
        assert_eq!(
            control.state,
            ControlFileState::ShutDown,
            "golden seed must be cleanly shut down",
        );
        assert!(
            ControlFileStore::path(&seed).exists(),
            "golden seed must include a control file",
        );

        seed
    })
}

fn unique_temp_dir(suite: &str, label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "pgrust_{suite}_{label}_{}_{}",
        std::process::id(),
        NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed)
    ))
}

fn clone_tree(src: &Path, dst: &Path) -> Result<(), String> {
    let metadata = fs::metadata(src).map_err(io_error)?;
    if metadata.is_dir() {
        fs::create_dir_all(dst).map_err(io_error)?;
        fs::set_permissions(dst, metadata.permissions()).map_err(io_error)?;
        for entry in fs::read_dir(src).map_err(io_error)? {
            let entry = entry.map_err(io_error)?;
            let child_src = entry.path();
            let child_dst = dst.join(entry.file_name());
            clone_tree(&child_src, &child_dst)?;
        }
        return Ok(());
    }

    clone_file(src, dst)?;
    fs::set_permissions(dst, metadata.permissions()).map_err(io_error)?;
    Ok(())
}

fn clone_file(src: &Path, dst: &Path) -> Result<(), String> {
    #[cfg(target_os = "macos")]
    {
        if try_clonefile(src, dst).is_ok() {
            return Ok(());
        }
    }

    fs::copy(src, dst).map_err(io_error)?;
    Ok(())
}

#[cfg(target_os = "macos")]
fn try_clonefile(src: &Path, dst: &Path) -> Result<(), String> {
    use std::os::unix::ffi::OsStrExt;

    let src = CString::new(src.as_os_str().as_bytes()).map_err(|e| e.to_string())?;
    let dst = CString::new(dst.as_os_str().as_bytes()).map_err(|e| e.to_string())?;
    let rc = unsafe { libc::clonefile(src.as_ptr(), dst.as_ptr(), 0) };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error().to_string())
    }
}

fn io_error(err: std::io::Error) -> String {
    err.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seeded_temp_dir_opens_immediately() {
        let dir = seeded_temp_dir("test_support", "opens_immediately");
        Database::open(&dir, 16).unwrap();
    }

    #[test]
    fn seeded_temp_dirs_are_independent_copies() {
        let left = seeded_temp_dir("test_support", "left");
        let right = seeded_temp_dir("test_support", "right");

        let marker = left.join("postgresql.conf");
        fs::write(&marker, "checkpoint_timeout = '7min'\n").unwrap();

        assert!(marker.exists());
        assert!(!right.join("postgresql.conf").exists());
    }

    #[test]
    fn scratch_temp_dir_starts_empty() {
        let dir = scratch_temp_dir("test_support", "empty");
        assert!(!ControlFileStore::path(&dir).exists());
        assert!(fs::read_dir(&dir).unwrap().next().is_none());
    }
}
