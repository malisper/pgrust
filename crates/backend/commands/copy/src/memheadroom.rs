//! Container-aware memory headroom (GL-COPYFAST-1 §3 defect row, the
//! memruns auto-sizing fix).
//!
//! The defect: on the fleet's 27Gi pods the container-leaf
//! `/sys/fs/cgroup/memory.max` reads `max` (the limit lives on an ancestor
//! slice), so the old probe fell through to `/proc/meminfo` MemAvailable of
//! the NODE (~31 GB) and the auto budget OOM-killed the merge. The fix:
//!
//!  1. Walk the cgroup hierarchy FROM OUR OWN LEAF (`/proc/self/cgroup`)
//!     upward to the mount root and take the tightest bounded level (v2
//!     `memory.max`; v1 `memory.limit_in_bytes` when the memory controller
//!     lives on v1) — a limit on a pod/user slice above the leaf now counts.
//!  2. `meminfo` is trusted ONLY when the hierarchy is provably visible and
//!     unbounded: our leaf path is non-root AND its directory exists under
//!     the mount. A namespaced root (`0::/`, the private-cgroupns container
//!     shape) or a remapped path can hide an ancestor limit, so those
//!     postures return None — auto REFUSES and the explicit-MB cap (which
//!     stays the trump, see `memrun_budget`) is the recipe.
//!
//! Pure logic is factored over an injected mount root + file contents so the
//! fixture-tree unit tests below cover every posture without a container.

#[cfg(target_os = "linux")]
pub(crate) fn memory_headroom_bytes() -> Option<u64> {
    let self_cgroup = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    let meminfo = std::fs::read_to_string("/proc/meminfo").unwrap_or_default();
    headroom_with(std::path::Path::new("/sys/fs/cgroup"), &self_cgroup, &meminfo)
}

/// Non-Linux (macOS dev): no reliable signal; auto stays off.
#[cfg(not(target_os = "linux"))]
pub(crate) fn memory_headroom_bytes() -> Option<u64> {
    None
}

#[cfg(any(target_os = "linux", test))]
use std::path::{Path, PathBuf};

// The pure logic below is compiled for Linux (the production entry) and for
// tests everywhere (the fixture trees are plain directories).
#[cfg(any(target_os = "linux", test))]
fn read_num(p: &Path) -> Option<u64> {
    std::fs::read_to_string(p).ok()?.trim().parse().ok()
}

#[cfg(any(target_os = "linux", test))]
fn read_stat_field(p: &Path, key: &str) -> Option<u64> {
    let s = std::fs::read_to_string(p).ok()?;
    s.lines().find_map(|l| l.strip_prefix(key)?.trim().parse().ok())
}

#[cfg(any(target_os = "linux", test))]
fn meminfo_available(meminfo: &str) -> Option<u64> {
    let kb = meminfo.lines().find_map(|l| {
        l.strip_prefix("MemAvailable:")?.trim().strip_suffix("kB")?.trim().parse::<u64>().ok()
    })?;
    Some(kb * 1024)
}

/// Walk `base/rel` up to `base` (inclusive). At every level with a readable
/// numeric limit, headroom = limit − usage (+ per-level reclaimable file
/// cache on v2); the result is the MIN over bounded levels — every bounded
/// ancestor is a real constraint, and the set includes the first bounded
/// ancestor the defect row asks for. Returns (bound, leaf_dir_exists).
#[cfg(any(target_os = "linux", test))]
fn walk(base: &Path, rel: &str, v2: bool) -> (Option<u64>, bool) {
    let leaf: PathBuf = if rel.is_empty() { base.to_path_buf() } else { base.join(rel) };
    let leaf_visible = leaf.is_dir();
    let mut best: Option<u64> = None;
    let mut p = leaf;
    loop {
        let lim = if v2 {
            // "max" fails the parse -> unbounded at this level.
            read_num(&p.join("memory.max"))
        } else {
            // v1 spells unlimited as a huge number.
            read_num(&p.join("memory.limit_in_bytes")).filter(|&l| l < (1u64 << 60))
        };
        if let Some(lim) = lim {
            let usage = if v2 {
                read_num(&p.join("memory.current")).unwrap_or(0)
            } else {
                read_num(&p.join("memory.usage_in_bytes")).unwrap_or(0)
            };
            let reclaimable = if v2 {
                read_stat_field(&p.join("memory.stat"), "inactive_file ").unwrap_or(0)
            } else {
                0
            };
            let h = lim.saturating_sub(usage).saturating_add(reclaimable);
            best = Some(best.map_or(h, |b| b.min(h)));
        }
        if p == *base {
            break;
        }
        match p.parent() {
            Some(par) if par.starts_with(base) => p = par.to_path_buf(),
            _ => break,
        }
    }
    (best, leaf_visible)
}

/// The testable core: `root` plays /sys/fs/cgroup, `self_cgroup` the
/// /proc/self/cgroup contents, `meminfo` the /proc/meminfo contents.
#[cfg(any(target_os = "linux", test))]
fn headroom_with(root: &Path, self_cgroup: &str, meminfo: &str) -> Option<u64> {
    // Which hierarchy carries the memory controller? A named v1 line wins
    // (hybrid hosts put memory on v1; the v2 subtree then has no memory.*
    // files); otherwise the unified v2 line; otherwise no cgroups at all.
    let v1_rel = self_cgroup.lines().find_map(|l| {
        let mut it = l.splitn(3, ':');
        let _id = it.next()?;
        let ctls = it.next()?;
        let path = it.next()?;
        if ctls.split(',').any(|c| c == "memory") {
            Some(path.trim().trim_start_matches('/').to_string())
        } else {
            None
        }
    });
    let v2_rel = self_cgroup
        .lines()
        .find_map(|l| l.strip_prefix("0::"))
        .map(|p| p.trim().trim_start_matches('/').to_string());

    let (bound, unbounded_proven) = if let Some(rel) = &v1_rel {
        let base = root.join("memory");
        if base.is_dir() {
            let (b, leaf_visible) = walk(&base, rel, false);
            (b, !rel.is_empty() && leaf_visible)
        } else {
            // Memory controller claimed but not mounted where we can see it:
            // never trust meminfo.
            (None, false)
        }
    } else if let Some(rel) = &v2_rel {
        let (b, leaf_visible) = walk(root, rel, true);
        (b, !rel.is_empty() && leaf_visible)
    } else {
        // No cgroup membership lines at all: bare environment.
        (None, true)
    };
    if bound.is_some() {
        return bound;
    }
    if unbounded_proven {
        return meminfo_available(meminfo);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    const GIB: u64 = 1 << 30;
    const MEMINFO: &str = "MemTotal:       32000000 kB\nMemFree:         1000000 kB\nMemAvailable:   30000000 kB\n";

    struct Fixture {
        root: PathBuf,
    }

    impl Fixture {
        fn new(name: &str) -> Fixture {
            let root = std::env::temp_dir().join(format!(
                "pgrust-memheadroom-{}-{}",
                std::process::id(),
                name
            ));
            let _ = fs::remove_dir_all(&root);
            fs::create_dir_all(&root).unwrap();
            Fixture { root }
        }

        fn dir(&self, rel: &str) -> PathBuf {
            let d = self.root.join(rel);
            fs::create_dir_all(&d).unwrap();
            d
        }

        fn write(&self, rel: &str, name: &str, content: &str) {
            fs::write(self.dir(rel).join(name), content).unwrap();
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    /// The C1 OOM defect, fixed: leaf reads "max", the limit lives on the
    /// pod slice two levels up — the walk finds it (with that level's usage
    /// and reclaimable file cache), never meminfo.
    #[test]
    fn v2_bounded_ancestor_beats_meminfo() {
        let f = Fixture::new("v2-ancestor");
        let leaf = "kubepods.slice/kubepods-pod1.slice/cri-abc.scope";
        f.write(leaf, "memory.max", "max\n");
        f.write(leaf, "memory.current", "1073741824\n");
        f.write("kubepods.slice/kubepods-pod1.slice", "memory.max", &(27 * GIB).to_string());
        f.write("kubepods.slice/kubepods-pod1.slice", "memory.current", &(2 * GIB).to_string());
        f.write(
            "kubepods.slice/kubepods-pod1.slice",
            "memory.stat",
            &format!("anon 100\ninactive_file {}\nslab 5\n", GIB / 2),
        );
        let got = headroom_with(&f.root, &format!("0::/{leaf}\n"), MEMINFO);
        assert_eq!(got, Some(27 * GIB - 2 * GIB + GIB / 2));
    }

    /// A bounded leaf is found directly (the pre-fix happy case, kept).
    #[test]
    fn v2_bounded_leaf() {
        let f = Fixture::new("v2-leaf");
        f.write("user.slice/app.scope", "memory.max", &(8 * GIB).to_string());
        f.write("user.slice/app.scope", "memory.current", &GIB.to_string());
        f.write("user.slice/app.scope", "memory.stat", "inactive_file 0\n");
        let got = headroom_with(&f.root, "0::/user.slice/app.scope\n", MEMINFO);
        assert_eq!(got, Some(7 * GIB));
    }

    /// Leaf AND ancestor bounded: the tightest constraint wins (a loose leaf
    /// cap must not hide a nearly-full pod slice).
    #[test]
    fn v2_nested_bounds_take_min() {
        let f = Fixture::new("v2-nested");
        let leaf = "kubepods.slice/pod.slice/c.scope";
        f.write(leaf, "memory.max", &(100 * GIB).to_string());
        f.write(leaf, "memory.current", &GIB.to_string());
        f.write("kubepods.slice/pod.slice", "memory.max", &(27 * GIB).to_string());
        f.write("kubepods.slice/pod.slice", "memory.current", &(26 * GIB).to_string());
        let got = headroom_with(&f.root, &format!("0::/{leaf}\n"), MEMINFO);
        assert_eq!(got, Some(GIB));
    }

    /// The private-cgroupns container posture ("0::/", every visible level
    /// unbounded): an invisible ancestor limit may exist, so meminfo must
    /// NOT be trusted — None, auto refuses, the explicit cap rules.
    #[test]
    fn v2_namespaced_root_refuses_meminfo() {
        let f = Fixture::new("v2-nsroot");
        f.write("", "memory.max", "max\n");
        f.write("", "memory.current", "1024\n");
        assert_eq!(headroom_with(&f.root, "0::/\n", MEMINFO), None);
    }

    /// A non-root leaf whose directory is missing under the mount is a
    /// remapped (namespaced) view: same refusal.
    #[test]
    fn v2_invisible_leaf_refuses_meminfo() {
        let f = Fixture::new("v2-remap");
        f.write("", "memory.max", "max\n");
        assert_eq!(
            headroom_with(&f.root, "0::/kubepods.slice/gone.scope\n", MEMINFO),
            None
        );
    }

    /// Fully visible, genuinely unbounded host hierarchy: meminfo is the
    /// kernel's own availability estimate and stays usable.
    #[test]
    fn v2_visible_unbounded_uses_meminfo() {
        let f = Fixture::new("v2-host");
        let leaf = "system.slice/pg.service";
        f.write(leaf, "memory.max", "max\n");
        f.write(leaf, "memory.current", "1024\n");
        f.write("system.slice", "memory.max", "max\n");
        let got = headroom_with(&f.root, &format!("0::/{leaf}\n"), MEMINFO);
        assert_eq!(got, Some(30000000 * 1024));
    }

    /// v1 (memory controller on a named hierarchy): the same ancestor walk
    /// over limit_in_bytes/usage_in_bytes, unlimited spelled as a huge
    /// number.
    #[test]
    fn v1_bounded_ancestor() {
        let f = Fixture::new("v1-ancestor");
        f.write("memory/docker/abc", "memory.limit_in_bytes", "9223372036854771712\n");
        f.write("memory/docker/abc", "memory.usage_in_bytes", "1024\n");
        f.write("memory/docker", "memory.limit_in_bytes", &(27 * GIB).to_string());
        f.write("memory/docker", "memory.usage_in_bytes", &(2 * GIB).to_string());
        let cg = "12:cpu,cpuacct:/docker/abc\n4:memory:/docker/abc\n0::/docker/abc\n";
        assert_eq!(headroom_with(&f.root, cg, MEMINFO), Some(25 * GIB));
    }

    /// v1 visible and unlimited to the root: meminfo allowed.
    #[test]
    fn v1_visible_unbounded_uses_meminfo() {
        let f = Fixture::new("v1-host");
        f.write("memory/user.slice", "memory.limit_in_bytes", "9223372036854771712\n");
        f.write("memory/user.slice", "memory.usage_in_bytes", "1024\n");
        let cg = "4:memory:/user.slice\n";
        assert_eq!(headroom_with(&f.root, cg, MEMINFO), Some(30000000 * 1024));
    }

    /// v1 line present but the controller mount is missing: refuse.
    #[test]
    fn v1_unmounted_refuses_meminfo() {
        let f = Fixture::new("v1-unmounted");
        let cg = "4:memory:/docker/abc\n";
        assert_eq!(headroom_with(&f.root, cg, MEMINFO), None);
    }

    /// No cgroup membership at all: bare environment, meminfo stands.
    #[test]
    fn no_cgroups_uses_meminfo() {
        let f = Fixture::new("bare");
        assert_eq!(headroom_with(&f.root, "", MEMINFO), Some(30000000 * 1024));
    }
}
