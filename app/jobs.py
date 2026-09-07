"""Background execution, because clustering is not a request-length operation.

The largest run in the corpus is 99 755 evaluations and takes about 33 seconds
with the full cascade. That is well past the point where a client should be
holding a socket open, so ``POST /cluster/run`` accepts the work and returns an
id, and the client polls.

The store is in memory and single-process on purpose. There is no durable state
worth a database here: a job is a pure function of a run directory and a config,
so losing one costs a re-request, not data. If this ever needs to survive a
restart or span processes, the thing to add is a queue, not a table.
"""

from __future__ import annotations

import threading
import traceback
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from . import settings
from .contracts import (
    ClusterResult,
    JobState,
    JobStatus,
    StageParameters,
    StageToggles,
)
from .orchestrator import Outcome, cluster, executed_stages


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


@dataclass
class Job:
    job_id: str
    run_id: str
    run_dir: Path
    stages: StageToggles
    parameters: StageParameters
    include_artifact: bool
    stages_executed: list[str]
    submitted_at: str = field(default_factory=_now)
    status: JobStatus = "queued"
    phase: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
    outcome: Outcome | None = None
    error: str | None = None

    def state(self) -> JobState:
        return JobState(
            job_id=self.job_id,
            run_id=self.run_id,
            status=self.status,
            stages_executed=self.stages_executed,
            submitted_at=self.submitted_at,
            started_at=self.started_at,
            finished_at=self.finished_at,
            elapsed_seconds=self.outcome.elapsed_seconds if self.outcome else None,
            phase=self.phase,
            error=self.error,
        )

    def result(self) -> ClusterResult:
        assert self.outcome is not None
        return ClusterResult(
            job_id=self.job_id,
            run_id=self.run_id,
            status=self.status,
            assignments=self.outcome.assignments,
            node_weights=self.outcome.node_weights,
            stages_requested=self.stages.selected(),
            stages_executed=self.stages_executed,
            counts=self.outcome.counts,
            diagnostics=self.outcome.diagnostics,
            resolved_parameters=self.outcome.resolved_parameters,
            elapsed_seconds=self.outcome.elapsed_seconds,
            warnings=self.outcome.warnings,
            artifact_files=self.outcome.artifact_files,
        )


class JobStore:
    def __init__(self, history: int = settings.JOB_HISTORY):
        self._jobs: OrderedDict[str, Job] = OrderedDict()
        self._lock = threading.Lock()
        self._slots = threading.Semaphore(settings.MAX_CONCURRENT_JOBS)
        self._history = history

    # -- lookup ---------------------------------------------------------

    def get(self, job_id: str) -> Job | None:
        with self._lock:
            return self._jobs.get(job_id)

    def all(self) -> list[Job]:
        with self._lock:
            return list(self._jobs.values())

    # -- submission -----------------------------------------------------

    def submit(
        self,
        run_dir: Path,
        stages: StageToggles,
        parameters: StageParameters,
        *,
        include_artifact: bool = False,
    ) -> Job:
        job = Job(
            job_id=uuid.uuid4().hex[:16],
            run_id=run_dir.name,
            run_dir=run_dir,
            stages=stages,
            parameters=parameters,
            include_artifact=include_artifact,
            stages_executed=executed_stages(stages),
        )
        with self._lock:
            self._jobs[job.job_id] = job
            self._evict_locked()
        threading.Thread(target=self._run, args=(job,), daemon=True).start()
        return job

    def _evict_locked(self) -> None:
        """Drop the oldest finished jobs once the history bound is exceeded.

        Only finished ones: evicting a job that is still running would leave a
        thread writing into an object nobody can reach.
        """
        while len(self._jobs) > self._history:
            for job_id, job in self._jobs.items():
                if job.status in ("done", "error"):
                    del self._jobs[job_id]
                    break
            else:
                return

    def _run(self, job: Job) -> None:
        with self._slots:
            job.status = "running"
            job.started_at = _now()
            try:
                job.outcome = cluster(
                    job.run_dir,
                    job.stages,
                    job.parameters,
                    include_artifact=job.include_artifact,
                    artifact_dir=settings.ARTIFACT_ROOT / job.job_id,
                    progress=lambda phase: setattr(job, "phase", phase),
                )
                job.status = "done"
                job.phase = "done"
            except Exception as error:  # noqa: BLE001 -- reported, not swallowed
                job.status = "error"
                job.phase = "failed"
                job.error = f"{type(error).__name__}: {error}"
                traceback.print_exc()
            finally:
                job.finished_at = _now()


#: One store per process. Created here rather than in main so tests can import it.
STORE = JobStore()
