"""Where things live, and nothing else.

The API service sits between two repositories that must not know about each
other. It is the only component that imports both sides, so it is also the only
component that needs to know where they are on disk.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

#: archipelago-api/
HERE = Path(__file__).resolve().parent.parent
#: FYP_Karthik/ -- the directory holding both repositories.
PROJECT_ROOT = HERE.parent


def _path(env: str, default: Path) -> Path:
    raw = os.environ.get(env)
    return Path(raw).expanduser().resolve() if raw else default


#: The clustering repository. Added to sys.path rather than pip-installed so the
#: service works against a checkout with no build step. Nothing in it is modified.
CLUSTERING_ROOT = _path("ARCHIPELAGO_CLUSTERING_ROOT", PROJECT_ROOT / "dEA-clustering")

#: The frontend repository, for its data/ library of runs.
FRONTEND_ROOT = _path("ARCHIPELAGO_FRONTEND_ROOT", PROJECT_ROOT / "archipelago-frontend")

#: Scratch space: uploaded runs and written artifacts.
WORK_ROOT = _path("ARCHIPELAGO_API_WORK", HERE / "work")
UPLOAD_ROOT = WORK_ROOT / "uploads"
ARTIFACT_ROOT = WORK_ROOT / "artifacts"

#: Directories a client may name a run inside. A request that resolves outside
#: every root is refused: `run_path` is client-supplied and would otherwise read
#: any directory the server process can reach.
RUN_ROOTS: tuple[Path, ...] = tuple(
    p
    for p in (
        FRONTEND_ROOT / "data",
        CLUSTERING_ROOT / "dEA-run-logs",
        UPLOAD_ROOT,
        *(
            Path(extra).expanduser().resolve()
            for extra in os.environ.get("ARCHIPELAGO_EXTRA_RUN_ROOTS", "").split(os.pathsep)
            if extra.strip()
        ),
    )
)

#: How many jobs may cluster at once. The pipeline is CPU-bound and holds the
#: whole feature matrix (458 MB on the largest corpus run), so this is a memory
#: bound as much as a scheduling one.
MAX_CONCURRENT_JOBS = int(os.environ.get("ARCHIPELAGO_API_CONCURRENCY", "2"))

#: Completed jobs kept in memory before the oldest is evicted.
JOB_HISTORY = int(os.environ.get("ARCHIPELAGO_API_JOB_HISTORY", "32"))


def ensure_directories() -> None:
    for directory in (WORK_ROOT, UPLOAD_ROOT, ARTIFACT_ROOT):
        directory.mkdir(parents=True, exist_ok=True)


def install_import_path() -> None:
    """Make ``archipelago_clustering`` importable from its checkout."""
    root = str(CLUSTERING_ROOT)
    if root not in sys.path:
        sys.path.insert(0, root)
