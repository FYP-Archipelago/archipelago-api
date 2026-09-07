"""The request and response shapes. Versioned, like everything else here.

The response deliberately carries *labels*, not the clustering repository's STN
artifact. The frontend already builds its own Level 0 network from the log and
already knows how to collapse it given one label per node
(``archipelago_ui.levels.collapse``). Handing it labels lets the existing 3D view
render a clustered network unchanged; handing it ``nodes.json`` would mean
rewriting that view to consume a second, differently-shaped graph.

The artifact is still available -- see ``include_artifact`` -- because it is the
clustering repository's documented contract and other consumers will want it.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

#: This API's own contract version, independent of the clustering artifact's
#: schema 1.0 and the log format's schema 2.0. MAJOR bumps break clients.
API_SCHEMA_VERSION = "1.0"

JobStatus = Literal["queued", "running", "done", "error"]


class StageToggles(BaseModel):
    """Which of the three stages to run.

    Only inclusion is a choice. Order is not: the cascade is LSH -> BIRCH ->
    DenStream and each stage refines the partition the previous one produced, so
    running them in another order would not mean the same thing. The server
    ignores the order these fields arrive in.
    """

    lsh: bool = False
    birch: bool = False
    denstream: bool = False

    def selected(self) -> list[str]:
        return [name for name, on in self.model_dump().items() if on]

    @property
    def any_enabled(self) -> bool:
        return self.lsh or self.birch or self.denstream


class StageParameters(BaseModel):
    """Optional overrides. Anything left unset keeps the pipeline's own default.

    These are passed straight through to ``archipelago_clustering.config``; the
    names match its models field for field, so a value that is invalid there is
    invalid here for the same reason.
    """

    # LSH
    lsh_hashes: int | None = Field(default=None, ge=1)
    lsh_radius: float | Literal["auto"] | None = None
    # BIRCH
    birch_threshold: float | Literal["auto"] | None = None
    birch_branching_factor: int | None = Field(default=None, ge=2)
    # DenStream
    denstream_epsilon: float | Literal["auto"] | None = None
    denstream_mu: float | None = Field(default=None, gt=0)
    denstream_beta: float | None = Field(default=None, gt=0, le=1)
    denstream_half_life: float | Literal["auto"] | None = None
    denstream_clock: Literal["arrival", "t_wall", "generation"] | None = None
    # global
    seed: int | None = None


class ClusterRequest(BaseModel):
    """Body of ``POST /cluster/run`` when the run already exists server-side.

    Exactly one of ``run_path`` and ``run_id`` identifies the run. Uploads use the
    multipart form of the same endpoint instead.
    """

    run_path: str | None = None
    run_id: str | None = None
    stages: StageToggles = StageToggles()
    parameters: StageParameters = StageParameters()
    #: Also write the clustering repository's schema-1.0 artifact directory and
    #: serve it from /cluster/artifact/{job_id}/{file}. Off by default: it is a
    #: second full pass over the run and the frontend does not read it.
    include_artifact: bool = False

    @model_validator(mode="after")
    def _one_identifier(self) -> ClusterRequest:
        if bool(self.run_path) == bool(self.run_id):
            raise ValueError("provide exactly one of run_path or run_id")
        return self


class JobAccepted(BaseModel):
    """202 from ``POST /cluster/run``."""

    schema_version: str = API_SCHEMA_VERSION
    job_id: str
    run_id: str
    status: JobStatus
    #: The selected stages in the order they will actually execute.
    stages_executed: list[str]
    result_url: str
    status_url: str


class JobState(BaseModel):
    """``GET /cluster/status/{job_id}``."""

    schema_version: str = API_SCHEMA_VERSION
    job_id: str
    run_id: str
    status: JobStatus
    stages_executed: list[str]
    submitted_at: str
    started_at: str | None = None
    finished_at: str | None = None
    elapsed_seconds: float | None = None
    #: Coarse progress, so a polling client can say something useful.
    phase: str | None = None
    error: str | None = None


class Counts(BaseModel):
    evaluations: int
    rows_clustered: int
    rows_without_genome: int
    #: Distinct locations before clustering, scoped by island -- the Level 0 node
    #: count the frontend would draw on its own. The compression figure the UI
    #: shows is this over ``nodes``.
    level0_nodes: int
    #: Distinct labels the cascade produced, across all islands.
    clusters: int
    #: Distinct (island, label) pairs -- what the frontend will actually draw,
    #: because a macro node never spans two islands.
    nodes: int


class ClusterResult(BaseModel):
    """``GET /cluster/result/{job_id}`` once the job is done."""

    schema_version: str = API_SCHEMA_VERSION
    job_id: str
    run_id: str
    status: JobStatus
    #: Keyed ``"<island_id>:<genome_hash>"`` -- the frontend's own node key. A key
    #: the frontend does not have is ignored; a node the map does not cover keeps
    #: its own singleton group rather than being merged into an arbitrary one.
    assignments: dict[str, int]
    #: DenStream's decayed weight per node key, absent unless DenStream ran. A
    #: weight of 0 is a region the search has left: fade it, do not drop it.
    node_weights: dict[str, float] | None = None
    stages_requested: list[str]
    stages_executed: list[str]
    counts: Counts
    #: Per-stage diagnostics straight from the pipeline. Advisory, not contract:
    #: do not build required UI on individual keys.
    diagnostics: dict[str, Any]
    resolved_parameters: dict[str, Any]
    elapsed_seconds: float
    #: Non-fatal things the caller should see -- label conflicts, undecodable
    #: rows, a run whose logs dropped records.
    warnings: list[str] = Field(default_factory=list)
    #: Present only when ``include_artifact`` was set.
    artifact_files: list[str] | None = None


class RunSummary(BaseModel):
    run_id: str
    path: str
    algorithm: str | None = None
    benchmark: str | None = None
    genome_encoding: str | None = None
    num_islands: int | None = None
    source: str


class RunList(BaseModel):
    schema_version: str = API_SCHEMA_VERSION
    runs: list[RunSummary]


class StageInfo(BaseModel):
    schema_version: str = API_SCHEMA_VERSION
    #: The fixed cascade order. Clients may include or exclude; never reorder.
    order: list[str]
    descriptions: dict[str, str]
