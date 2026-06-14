//! Planner path vocabulary (nodes/pathnodes.h), trimmed.
//!
//! C's `Path *` is a tagged pointer to any concrete path struct; the owned
//! model is the [`PathNode`] enum (the concrete node type — C's `nodeTag` /
//! `IsA` — is the variant; the `pathtype` field stays data, as in C). Child
//! links (`subpath`, `subpaths`) are owned nodes, so capability recursion
//! (e.g. `ExecSupportsMarkRestore`) needs no resolution seam. Variants and
//! fields are added as their consuming units are ported.

use mcx::{Mcx, PgBox, PgVec};
use types_error::PgResult;

use crate::nodes::NodeTag;

/// `Path` (pathnodes.h) — the abstract base every path node embeds first.
/// Trimmed to the fields ports consume.
#[derive(Debug)]
pub struct PathData {
    /// `NodeTag pathtype` — tag identifying the scan/join plan-node type this
    /// path would produce (`T_SeqScan`, `T_IndexScan`, ...). Distinct from the
    /// node's own tag (the [`PathNode`] variant).
    pub pathtype: NodeTag,
}

impl PathData {
    /// Deep copy of the embedded `Path` base into `mcx` (C: `copyObject`
    /// shape). Fallible to mirror the family-wide `clone_in` convention; the
    /// only field is the `Copy` `pathtype` tag.
    pub fn clone_in<'b>(&self, _mcx: Mcx<'b>) -> PgResult<PathData> {
        Ok(PathData {
            pathtype: self.pathtype,
        })
    }
}

/// `IndexOptInfo` (pathnodes.h) — per-index planning information, trimmed.
#[derive(Debug)]
pub struct IndexOptInfo {
    /// `bool amcanmarkpos` — does the index AM support mark/restore?
    pub amcanmarkpos: bool,
}

impl IndexOptInfo {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible to mirror the
    /// family-wide `clone_in` convention; the only field is a `Copy` `bool`.
    pub fn clone_in<'b>(&self, _mcx: Mcx<'b>) -> PgResult<IndexOptInfo> {
        Ok(IndexOptInfo {
            amcanmarkpos: self.amcanmarkpos,
        })
    }
}

/// `IndexPath` (pathnodes.h), trimmed.
#[derive(Debug)]
pub struct IndexPath<'mcx> {
    /// `Path path` — the embedded base (pathtype `T_IndexScan` or
    /// `T_IndexOnlyScan`).
    pub path: PathData,
    /// `IndexOptInfo *indexinfo` — the index to be scanned (never NULL in C).
    pub indexinfo: PgBox<'mcx, IndexOptInfo>,
}

impl IndexPath<'_> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying
    /// allocates the embedded `IndexOptInfo`.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<IndexPath<'b>> {
        Ok(IndexPath {
            path: self.path.clone_in(mcx)?,
            indexinfo: mcx::alloc_in(mcx, self.indexinfo.clone_in(mcx)?)?,
        })
    }
}

/// `CustomPath` (pathnodes.h), trimmed.
#[derive(Debug)]
pub struct CustomPath {
    /// `Path path` — the embedded base (pathtype `T_CustomScan`).
    pub path: PathData,
    /// `uint32 flags` — mask of `CUSTOMPATH_*` flags.
    pub flags: u32,
}

impl CustomPath {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible to mirror the
    /// family-wide `clone_in` convention.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CustomPath> {
        Ok(CustomPath {
            path: self.path.clone_in(mcx)?,
            flags: self.flags,
        })
    }
}

/// `ProjectionPath` (pathnodes.h) — a projection over a subpath, trimmed.
/// One of the Path types whose `pathtype` is `T_Result`.
#[derive(Debug)]
pub struct ProjectionPath<'mcx> {
    /// `Path path` — the embedded base (pathtype `T_Result`).
    pub path: PathData,
    /// `Path *subpath` — path representing the input to the projection
    /// (never NULL in C).
    pub subpath: PgBox<'mcx, PathNode<'mcx>>,
}

impl ProjectionPath<'_> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying
    /// recurses into the owned `subpath`.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<ProjectionPath<'b>> {
        Ok(ProjectionPath {
            path: self.path.clone_in(mcx)?,
            subpath: mcx::alloc_in(mcx, self.subpath.clone_in(mcx)?)?,
        })
    }
}

/// `MinMaxAggPath` (pathnodes.h), trimmed — a childless-Result producer.
#[derive(Debug)]
pub struct MinMaxAggPath {
    /// `Path path` — the embedded base (pathtype `T_Result`).
    pub path: PathData,
}

impl MinMaxAggPath {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible to mirror the
    /// family-wide `clone_in` convention.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<MinMaxAggPath> {
        Ok(MinMaxAggPath {
            path: self.path.clone_in(mcx)?,
        })
    }
}

/// `GroupResultPath` (pathnodes.h), trimmed — a childless-Result producer.
#[derive(Debug)]
pub struct GroupResultPath {
    /// `Path path` — the embedded base (pathtype `T_Result`).
    pub path: PathData,
}

impl GroupResultPath {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible to mirror the
    /// family-wide `clone_in` convention.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<GroupResultPath> {
        Ok(GroupResultPath {
            path: self.path.clone_in(mcx)?,
        })
    }
}

/// `AppendPath` (pathnodes.h), trimmed.
#[derive(Debug)]
pub struct AppendPath<'mcx> {
    /// `Path path` — the embedded base (pathtype `T_Append`).
    pub path: PathData,
    /// `List *subpaths` — the component Paths.
    pub subpaths: PgVec<'mcx, PathNode<'mcx>>,
}

impl AppendPath<'_> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying recurses
    /// into the owned `subpaths` list.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<AppendPath<'b>> {
        let mut subpaths = mcx::vec_with_capacity_in(mcx, self.subpaths.len())?;
        for child in self.subpaths.iter() {
            subpaths.push(child.clone_in(mcx)?);
        }
        Ok(AppendPath {
            path: self.path.clone_in(mcx)?,
            subpaths,
        })
    }
}

/// `MergeAppendPath` (pathnodes.h), trimmed.
#[derive(Debug)]
pub struct MergeAppendPath<'mcx> {
    /// `Path path` — the embedded base (pathtype `T_MergeAppend`).
    pub path: PathData,
    /// `List *subpaths` — the component Paths.
    pub subpaths: PgVec<'mcx, PathNode<'mcx>>,
}

impl MergeAppendPath<'_> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying recurses
    /// into the owned `subpaths` list.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<MergeAppendPath<'b>> {
        let mut subpaths = mcx::vec_with_capacity_in(mcx, self.subpaths.len())?;
        for child in self.subpaths.iter() {
            subpaths.push(child.clone_in(mcx)?);
        }
        Ok(MergeAppendPath {
            path: self.path.clone_in(mcx)?,
            subpaths,
        })
    }
}

/// A path-tree node (`Path *` in C). The concrete node type (`IsA`) is the
/// variant; `Path(PathData)` is a plain base `Path` node (e.g. a simple
/// `RTE_RESULT` base relation).
#[derive(Debug)]
#[non_exhaustive]
pub enum PathNode<'mcx> {
    /// `T_Path` — a plain base path.
    Path(PathData),
    /// `T_IndexPath`.
    IndexPath(IndexPath<'mcx>),
    /// `T_CustomPath`.
    CustomPath(CustomPath),
    /// `T_ProjectionPath`.
    ProjectionPath(ProjectionPath<'mcx>),
    /// `T_MinMaxAggPath`.
    MinMaxAggPath(MinMaxAggPath),
    /// `T_GroupResultPath`.
    GroupResultPath(GroupResultPath),
    /// `T_AppendPath`.
    AppendPath(AppendPath<'mcx>),
    /// `T_MergeAppendPath`.
    MergeAppendPath(MergeAppendPath<'mcx>),
}

impl PathNode<'_> {
    /// `&((Path *) node)->...` — the embedded `Path` base.
    pub fn path_head(&self) -> &PathData {
        match self {
            PathNode::Path(p) => p,
            PathNode::IndexPath(p) => &p.path,
            PathNode::CustomPath(p) => &p.path,
            PathNode::ProjectionPath(p) => &p.path,
            PathNode::MinMaxAggPath(p) => &p.path,
            PathNode::GroupResultPath(p) => &p.path,
            PathNode::AppendPath(p) => &p.path,
            PathNode::MergeAppendPath(p) => &p.path,
        }
    }

    /// Deep copy of the path node (and its sub-path tree) into `mcx`
    /// (C: `copyObject` shape). Fallible: copying allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<PathNode<'b>> {
        match self {
            PathNode::Path(p) => Ok(PathNode::Path(p.clone_in(mcx)?)),
            PathNode::IndexPath(p) => Ok(PathNode::IndexPath(p.clone_in(mcx)?)),
            PathNode::CustomPath(p) => Ok(PathNode::CustomPath(p.clone_in(mcx)?)),
            PathNode::ProjectionPath(p) => Ok(PathNode::ProjectionPath(p.clone_in(mcx)?)),
            PathNode::MinMaxAggPath(p) => Ok(PathNode::MinMaxAggPath(p.clone_in(mcx)?)),
            PathNode::GroupResultPath(p) => Ok(PathNode::GroupResultPath(p.clone_in(mcx)?)),
            PathNode::AppendPath(p) => Ok(PathNode::AppendPath(p.clone_in(mcx)?)),
            PathNode::MergeAppendPath(p) => Ok(PathNode::MergeAppendPath(p.clone_in(mcx)?)),
        }
    }
}
