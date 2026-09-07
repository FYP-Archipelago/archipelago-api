"""The REST surface: the seam between the frontend and the clustering pipeline.

Neither repository knows about the other, and neither changes. This service is
the only thing that imports the pipeline, and the only thing the frontend talks
to over HTTP.

    POST /cluster/run                     start a job (JSON body, or a zip upload)
    GET  /cluster/status/{job_id}         poll it
    GET  /cluster/result/{job_id}         the labels, once it is done
    GET  /cluster/artifact/{job_id}/...   the schema 1.0 artifact, if requested
    GET  /cluster/runs                    what runs the service can see
    GET  /stages                          the fixed cascade order
    GET  /health
"""

from __future__ import annotations

import json
from pathlib import Path

from fastapi import FastAPI, File, Form, HTTPException, Response, UploadFile
from fastapi.middleware.cors import CORSMiddleware

from . import orchestrator, settings, uploads
from .contracts import (
    API_SCHEMA_VERSION,
    ClusterRequest,
    ClusterResult,
    JobAccepted,
    JobState,
    RunList,
    RunSummary,
    StageInfo,
    StageParameters,
    StageToggles,
)
from .jobs import STORE, Job
from .version import __version__ as SERVICE_VERSION

settings.ensure_directories()

app = FastAPI(
    title="Archipelago clustering API",
    version=API_SCHEMA_VERSION,
    summary="Runs the dEA clustering cascade over a run and returns node labels.",
)

# The frontend is a Streamlit server calling this from Python, so CORS is not
# strictly needed today. It is here because a browser client is the obvious next
# consumer and the clustering repository's own notes call adding it a half-hour
# job -- this is that half hour.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


# --------------------------------------------------------------------------
# discovery
# --------------------------------------------------------------------------


@app.get("/health")
def health() -> dict:
    return {
        "status": "ok",
        # Two versions, deliberately: what the service is, and what it promises.
        "service_version": SERVICE_VERSION,
        "schema_version": API_SCHEMA_VERSION,
        "clustering_root": str(settings.CLUSTERING_ROOT),
        "run_roots": [str(r) for r in settings.RUN_ROOTS],
        "jobs": len(STORE.all()),
    }


@app.get("/stages", response_model=StageInfo)
def stages() -> StageInfo:
    """The cascade order. A client chooses membership; it never chooses sequence."""
    return StageInfo(
        order=list(orchestrator.STAGE_ORDER),
        descriptions=orchestrator.STAGE_DESCRIPTIONS,
    )


@app.get("/cluster/runs", response_model=RunList)
def list_runs() -> RunList:
    """Runs the service can reach, so a client can offer a path it will accept."""
    summaries = []
    for run_id, path, source in orchestrator.discover_runs():
        summaries.append(
            RunSummary(run_id=run_id, path=str(path), source=source, **_peek(path))
        )
    return RunList(runs=summaries)


def _peek(path: Path) -> dict:
    """Cheap provenance from the first line of run.jsonl -- the run_start event.

    Reading the whole log to label a picker entry would cost more than the page
    it feeds; a failure here is not worth reporting, the entry just shows bare.
    """
    try:
        with (path / "run.jsonl").open() as handle:
            for line in handle:
                event = json.loads(line)
                if event.get("event") == "run_start":
                    return {
                        "algorithm": event.get("algorithm"),
                        "benchmark": event.get("benchmark"),
                        "genome_encoding": event.get("genome_encoding"),
                        "num_islands": event.get("num_islands"),
                    }
    except (OSError, json.JSONDecodeError):
        pass
    return {}


# --------------------------------------------------------------------------
# submitting work
# --------------------------------------------------------------------------


def _accept(job: Job) -> JobAccepted:
    return JobAccepted(
        job_id=job.job_id,
        run_id=job.run_id,
        status=job.status,
        stages_executed=job.stages_executed,
        result_url=f"/cluster/result/{job.job_id}",
        status_url=f"/cluster/status/{job.job_id}",
    )


def _resolve_or_400(**kwargs) -> Path:
    try:
        return orchestrator.resolve_run(**kwargs)
    except orchestrator.RunNotAllowed as error:
        raise HTTPException(status_code=403, detail=str(error)) from error
    except orchestrator.RunNotFound as error:
        raise HTTPException(status_code=404, detail=str(error)) from error


@app.post("/cluster/run", response_model=JobAccepted, status_code=202)
def cluster_run(request: ClusterRequest) -> JobAccepted:
    """Cluster a run the service can already see.

    ``stages`` says which of LSH, BIRCH and DenStream to include. It does not say
    in what order: the cascade order is fixed, each stage refines the partition
    the one before it produced, and the response reports the order actually used.
    An empty selection is allowed and means the Level 0 baseline.
    """
    run_dir = _resolve_or_400(run_path=request.run_path, run_id=request.run_id)
    try:
        orchestrator.build_config(request.stages, request.parameters)
    except Exception as error:  # pydantic validation of the merged config
        raise HTTPException(status_code=422, detail=f"bad parameters: {error}") from error
    job = STORE.submit(
        run_dir,
        request.stages,
        request.parameters,
        include_artifact=request.include_artifact,
    )
    return _accept(job)


@app.post("/cluster/run/upload", response_model=JobAccepted, status_code=202)
async def cluster_upload(
    archive: UploadFile = File(..., description="zip of one run directory"),
    lsh: bool = Form(False),
    birch: bool = Form(False),
    denstream: bool = Form(False),
    name: str | None = Form(None),
    parameters: str | None = Form(None, description="StageParameters as a JSON object"),
    include_artifact: bool = Form(False),
) -> JobAccepted:
    """Same thing, for a run the service cannot see on disk.

    Multipart rather than JSON so the run travels as a file instead of base64 in
    a body; a 100 000-evaluation run is 200 MB of CSV.
    """
    try:
        run_dir = uploads.install_zip(await archive.read(), name)
    except uploads.BadUpload as error:
        raise HTTPException(status_code=400, detail=str(error)) from error

    try:
        params = StageParameters.model_validate_json(parameters) if parameters else StageParameters()
    except ValueError as error:
        raise HTTPException(status_code=422, detail=f"bad parameters: {error}") from error

    toggles = StageToggles(lsh=lsh, birch=birch, denstream=denstream)
    job = STORE.submit(run_dir, toggles, params, include_artifact=include_artifact)
    return _accept(job)


# --------------------------------------------------------------------------
# collecting it
# --------------------------------------------------------------------------


def _job_or_404(job_id: str) -> Job:
    job = STORE.get(job_id)
    if job is None:
        raise HTTPException(
            status_code=404,
            detail=f"no job {job_id}. Jobs live in memory and the oldest finished "
                   "ones are evicted; re-submit the run.",
        )
    return job


@app.get("/cluster/status/{job_id}", response_model=JobState)
def cluster_status(job_id: str) -> JobState:
    return _job_or_404(job_id).state()


@app.get("/cluster/result/{job_id}", response_model=ClusterResult)
def cluster_result(job_id: str) -> ClusterResult:
    """The labels. 202 while the job is still running, 500 if it failed.

    A still-running job is not an error, so it does not get a 4xx: the client
    asked a valid question about a real job and the answer is "not yet".
    """
    job = _job_or_404(job_id)
    if job.status == "error":
        raise HTTPException(status_code=500, detail=job.error or "clustering failed")
    if job.status != "done":
        return Response(
            status_code=202,
            media_type="application/json",
            content=job.state().model_dump_json(),
        )
    return job.result()


@app.get("/cluster/artifact/{job_id}/{filename}")
def cluster_artifact(job_id: str, filename: str) -> Response:
    """One file of the clustering repository's schema 1.0 artifact directory."""
    job = _job_or_404(job_id)
    if job.outcome is None or job.outcome.artifact_dir is None:
        raise HTTPException(
            status_code=404,
            detail="this job was not run with include_artifact=true",
        )
    # Resolve and re-check: filename is client-supplied, and "../../etc/passwd"
    # is a path traversal that a naive join would happily serve.
    target = (job.outcome.artifact_dir / filename).resolve()
    if not target.is_file() or job.outcome.artifact_dir.resolve() not in target.parents:
        raise HTTPException(status_code=404, detail=f"no {filename} in this artifact")
    return Response(content=target.read_bytes(), media_type="application/json")
