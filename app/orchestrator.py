"""Calling the clustering pipeline, in its own fixed order, for a chosen subset.

This is the only module that imports ``archipelago_clustering``, and it imports
it the way any other consumer would: through the public entry points, changing
nothing. Specifically it uses

* ``config.Config``                     -- every stage already defaults to OFF
* ``reader.RunDirectory``               -- the schema 2.0 reader
* ``features.registry.build_features``  -- genome vectorisation
* ``stages.pipeline.run_stages``        -- the cascade itself
* ``runner.run``                        -- the full artifact, when asked for one

The toggle requirement needs no new logic in the pipeline because the pipeline
was already built for it: ``run_stages`` appends only the enabled stages, in a
hardcoded order, and each refines the partition the previous one produced. All
this layer does is translate an HTTP request into a ``Config`` and translate the
resulting labels into the frontend's node keys.
"""

from __future__ import annotations

import shutil
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import numpy as np

from . import settings
from .contracts import Counts, StageParameters, StageToggles

settings.install_import_path()

from archipelago_clustering.config import Config  # noqa: E402
from archipelago_clustering.features.registry import build_features  # noqa: E402
from archipelago_clustering.reader import RunDirectory  # noqa: E402
from archipelago_clustering.stages.pipeline import run_stages  # noqa: E402

#: The cascade order, taken from the pipeline's own configuration rather than
#: restated here, so it cannot drift out of step with ``run_stages``. A client
#: chooses which of these run; it never chooses the sequence.
STAGE_ORDER: tuple[str, ...] = tuple(Config().stage_flags().keys())

STAGE_DESCRIPTIONS = {
    "lsh": "Locality-sensitive hashing. Blocks near-identical locations together "
           "and bounds the work the later stages see.",
    "birch": "A CF-tree summarises each region in one pass with bounded memory.",
    "denstream": "Density micro-clusters with time decay, so a region the search "
                 "has abandoned fades instead of sitting there forever.",
}


class RunNotFound(Exception):
    pass


class RunNotAllowed(Exception):
    """A path that resolves outside every configured run root."""


# --------------------------------------------------------------------------
# resolving which run to read
# --------------------------------------------------------------------------


def _is_run_directory(path: Path) -> bool:
    # evaluations.csv is the only genuinely required file: it is the dataset.
    # run.jsonl carries the metadata the pipeline needs to place islands on one
    # clock, so both are checked before a directory is offered as a run.
    return (path / "evaluations.csv").is_file() and (path / "run.jsonl").is_file()


def resolve_run(*, run_path: str | None = None, run_id: str | None = None) -> Path:
    """Turn a client-supplied identifier into a directory, or refuse it.

    ``run_path`` is confined to :data:`settings.RUN_ROOTS`. Without that check a
    caller could name any directory the service can read.
    """
    if run_path:
        candidate = Path(run_path).expanduser()
        try:
            candidate = candidate.resolve(strict=True)
        except OSError as error:
            raise RunNotFound(f"no such directory: {run_path}") from error
        if not any(_within(candidate, root) for root in settings.RUN_ROOTS):
            allowed = ", ".join(str(r) for r in settings.RUN_ROOTS)
            raise RunNotAllowed(
                f"{candidate} is outside the configured run roots ({allowed}). "
                "Upload the run instead, or add its parent to "
                "ARCHIPELAGO_EXTRA_RUN_ROOTS."
            )
        if not _is_run_directory(candidate):
            raise RunNotFound(
                f"{candidate} has no evaluations.csv and run.jsonl -- not a schema 2.0 run"
            )
        return candidate

    for root in settings.RUN_ROOTS:
        candidate = root / (run_id or "")
        if candidate.is_dir() and _is_run_directory(candidate):
            return candidate.resolve()
    raise RunNotFound(f"no run named {run_id!r} under any configured run root")


def _within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def discover_runs() -> list[tuple[str, Path, str]]:
    """Every run the service can see, as ``(run_id, path, source)``."""
    found: list[tuple[str, Path, str]] = []
    seen: set[Path] = set()
    for root in settings.RUN_ROOTS:
        if not root.is_dir():
            continue
        source = root.name if root.name != "data" else root.parent.name
        for child in sorted(root.iterdir(), reverse=True):
            resolved = child.resolve()
            if child.is_dir() and _is_run_directory(child) and resolved not in seen:
                seen.add(resolved)
                found.append((child.name, resolved, source))
    return found


# --------------------------------------------------------------------------
# request -> Config
# --------------------------------------------------------------------------


def build_config(stages: StageToggles, parameters: StageParameters) -> Config:
    """A ``Config`` with exactly the requested stages on and nothing else changed.

    Built from a default ``Config()``, in which every stage is already off, so an
    unselected stage is off because the pipeline says so rather than because this
    layer remembered to turn it off.
    """
    config = Config()
    config.lsh.enabled = stages.lsh
    config.birch.enabled = stages.birch
    config.denstream.enabled = stages.denstream

    overrides = parameters.model_dump(exclude_none=True)
    for key, value in overrides.items():
        if key == "seed":
            config.seed = value
            continue
        stage, _, field_name = key.partition("_")
        setattr(getattr(config, stage), field_name, value)
    # Re-validate: a field assigned after construction bypasses pydantic's
    # constraints, and a bad threshold should be a 422 here rather than a
    # traceback inside someone else's stage.
    return Config.model_validate(config.model_dump())


def executed_stages(stages: StageToggles) -> list[str]:
    """The selected stages, in cascade order. This is the whole toggle contract."""
    selected = set(stages.selected())
    return [name for name in STAGE_ORDER if name in selected]


# --------------------------------------------------------------------------
# the run itself
# --------------------------------------------------------------------------


@dataclass
class Outcome:
    assignments: dict[str, int]
    node_weights: dict[str, float] | None
    counts: Counts
    diagnostics: dict[str, Any]
    resolved_parameters: dict[str, Any]
    elapsed_seconds: float
    warnings: list[str] = field(default_factory=list)
    artifact_dir: Path | None = None
    artifact_files: list[str] | None = None


Progress = Callable[[str], None]


def cluster(
    run_dir: Path,
    stages: StageToggles,
    parameters: StageParameters,
    *,
    include_artifact: bool = False,
    artifact_dir: Path | None = None,
    progress: Progress | None = None,
) -> Outcome:
    """Run the selected stages over ``run_dir`` and return frontend-shaped labels."""
    started = time.perf_counter()
    say = progress or (lambda _: None)
    warnings: list[str] = []

    config = build_config(stages, parameters)
    order = executed_stages(stages)

    say("reading run")
    directory = RunDirectory(run_dir)
    metadata = directory.read_metadata()

    say("vectorising genomes")
    features, _row_ids = build_features(directory, metadata, config)

    n_rows = len(features)
    usable = int(features.mask.sum())
    if usable < n_rows:
        warnings.append(
            f"{n_rows - usable} of {n_rows} evaluations have no genome body and "
            "cannot be clustered; each keeps its own exact location."
        )

    if order:
        say(f"clustering: {' -> '.join(order)}")
        assignment = run_stages(features, config)
        labels = assignment.labels
        diagnostics = assignment.diagnostics
        row_weights = assignment.row_weights
    else:
        # Nothing selected is a legitimate request: it is the Level 0 baseline,
        # which is exactly what the pipeline itself does with an empty config.
        labels = None
        diagnostics = {}
        row_weights = None

    say("mapping labels onto node keys")
    assignments, node_weights, mapping_warnings, cluster_count = _node_keys(
        features, labels, row_weights
    )
    warnings.extend(mapping_warnings)

    resolved: dict[str, Any] = {
        "stage_order": order,
        "metric": features.metric.value,
        "feature_dimension": int(features.matrix.shape[1]) if features.matrix.size else 0,
        "rows_with_genome_body": usable,
        "config": config.model_dump(),
    }

    counts = Counts(
        evaluations=n_rows,
        rows_clustered=usable if order else 0,
        rows_without_genome=n_rows - usable,
        level0_nodes=len({f"{i}:{h}" for i, h in zip(features.islands, features.hashes)}),
        clusters=cluster_count,
        nodes=_node_count(assignments),
    )

    outcome = Outcome(
        assignments=assignments,
        node_weights=node_weights,
        counts=counts,
        diagnostics=diagnostics,
        resolved_parameters=resolved,
        elapsed_seconds=time.perf_counter() - started,
        warnings=warnings,
    )

    if include_artifact:
        say("writing schema 1.0 artifact")
        outcome.artifact_dir, outcome.artifact_files = _write_artifact(
            run_dir, config, artifact_dir
        )
        outcome.elapsed_seconds = time.perf_counter() - started

    return outcome


def _node_count(assignments: dict[str, int]) -> int:
    """Distinct (island, label) pairs -- what the frontend will draw.

    A macro node never spans two islands (``levels.collapse`` keys groups by
    island), so the drawn node count is not the raw label count.
    """
    return len({(key.split(":", 1)[0], label) for key, label in assignments.items()})


def _node_keys(
    features,
    labels: np.ndarray | None,
    row_weights: np.ndarray | None,
) -> tuple[dict[str, int], dict[str, float] | None, list[str], int]:
    """Collapse per-row labels onto the frontend's ``"<island>:<genome_hash>"`` keys.

    Two things need care.

    *Rows with no genome body* were never fed to a stage. They keep a group of
    their own, numbered above every real cluster, so they land where the pipeline
    itself would put them: at their own exact location.

    *One key, two labels.* Rows sharing a key share a genome and therefore a
    feature vector, so LSH and BIRCH always agree on them. DenStream does not
    have to: it ages points, and a location revisited long after it was first
    seen can fall in a different micro-cluster. Rather than pick silently, take
    the majority label and report how often it happened.
    """
    per_key: dict[str, list[int]] = defaultdict(list)
    weight_per_key: dict[str, float] = {}

    next_singleton = (int(labels.max()) + 1) if labels is not None and labels.size else 0
    singletons: dict[str, int] = {}

    for i, (island, genome_hash) in enumerate(zip(features.islands, features.hashes)):
        key = f"{int(island)}:{genome_hash}"
        if labels is not None and features.mask[i]:
            per_key[key].append(int(labels[i]))
        else:
            # Unclusterable, or nothing enabled: one group per exact location.
            label = singletons.get(key)
            if label is None:
                label = singletons[key] = next_singleton
                next_singleton += 1
            per_key[key].append(label)
        if row_weights is not None:
            # A node's weight is the strongest of its members': they are one
            # region, and the freshest visit is what says the region is alive.
            weight = float(row_weights[i])
            if weight > weight_per_key.get(key, float("-inf")):
                weight_per_key[key] = weight

    assignments: dict[str, int] = {}
    conflicts = 0
    for key, values in per_key.items():
        if len(set(values)) > 1:
            conflicts += 1
            assignments[key] = Counter(values).most_common(1)[0][0]
        else:
            assignments[key] = values[0]

    warnings: list[str] = []
    if conflicts:
        warnings.append(
            f"{conflicts} location(s) received more than one cluster label -- expected "
            "when DenStream runs, since it ages points and a revisited location can "
            "fall in a newer micro-cluster. The majority label was used."
        )

    cluster_count = len(set(assignments.values()))
    weights = weight_per_key if row_weights is not None else None
    return assignments, weights, warnings, cluster_count


def _write_artifact(
    run_dir: Path, config: Config, artifact_dir: Path | None
) -> tuple[Path, list[str]]:
    """Produce the clustering repository's own schema-1.0 output directory.

    A second full pass over the run, which is why it is opt-in. Worth having: it
    is the documented contract other consumers read, and it is what makes an API
    response auditable against the CLI.
    """
    from archipelago_clustering.runner import run as run_pipeline

    target = artifact_dir or (settings.ARTIFACT_ROOT / run_dir.name)
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True, exist_ok=True)
    run_pipeline(run_dir, target, config)
    return target, sorted(p.name for p in target.iterdir() if p.is_file())
