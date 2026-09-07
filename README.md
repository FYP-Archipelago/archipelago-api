# archipelago-api

The connecting layer between **archipelago-frontend** and **dEA-clustering**.

Team 21, Dept. of CSE, Amrita Vishwa Vidyapeetham. Guide: Dr. Ritwik M.

**v0.1** · response schema **1.0**. See [CHANGELOG.md](CHANGELOG.md); both are
reported by `GET /health` and they move independently — the service version is
what this is, the schema version is what it promises clients.

Neither of the two repositories knows the other exists, and neither changes. This
service is the only component that imports the clustering pipeline, and the only
one the frontend talks to over HTTP.

```
archipelago-frontend  ──HTTP──▶  archipelago-api  ──import──▶  dEA-clustering
   (Streamlit)                    (FastAPI)                     (unmodified)
```

---

## What it returns, and why it is not the artifact

`dEA-clustering` already emits a documented schema-1.0 artifact — `nodes.json`,
`edges.json` and the rest. This API does not hand that to the frontend by
default, and the reason is worth stating.

The frontend builds its **own** Level 0 network from the same log, and its 3D
view draws that object. It already knows how to collapse it: `levels.collapse()`
takes one label per node and does the whole mechanical reduction — summing
visits, rewriting edges, keeping migrations pointed at the right macro nodes.

So the smallest thing that connects the two systems is **labels**, keyed by the
frontend's own node key:

```json
{ "assignments": { "0:eebbaba91a67…": 17, "0:3f1c9d4a…": 17, "1:88ae0c…": 4 } }
```

Handing over `nodes.json` instead would mean rewriting the visualization to
consume a second, differently-shaped graph. Labels mean the 3D view, the metrics,
the migration overlay and the projection all render a clustered network with no
change to any of them.

The artifact is still available — `include_artifact: true`, then
`GET /cluster/artifact/{job_id}/manifest.json`. It is the clustering repository's
published contract and other consumers will want it; it is opt-in because it is a
second full pass over the run and the frontend does not read it.

## The one thing this layer must get right

The cascade is **LSH → BIRCH → DenStream**, and each stage refines the partition
the one before it produced. A client chooses *which* stages run. It never chooses
the sequence:

```jsonc
{ "stages": { "denstream": true, "lsh": true } }   // order in the request is irrelevant
→ { "stages_executed": ["lsh", "denstream"] }      // always cascade order
```

`orchestrator.STAGE_ORDER` is read from the pipeline's own `Config.stage_flags()`
rather than restated here, so it cannot drift out of step with `run_stages`. A
test asserts the value; another asserts all eight combinations keep the order.

An empty selection is a valid request, not an error: it is the Level 0 baseline,
which is exactly what the pipeline does with an unconfigured `Config`.

## Running it

```bash
cd archipelago-api
python3 -m venv --system-site-packages .venv
./.venv/bin/pip install -r requirements.txt
./.venv/bin/python -m uvicorn app.main:app --port 8000
```

Interactive docs at <http://127.0.0.1:8000/docs>.

`--system-site-packages` because the service needs the clustering pipeline's
dependencies (numpy, scikit-learn, scipy, pandas) and those are usually already
installed; drop it and install `dEA-clustering`'s own requirements into the venv
instead. The pipeline itself is imported from its checkout, not pip-installed, so
there is no build step and nothing in it is modified.

### Configuration

| variable | default | what it does |
|---|---|---|
| `ARCHIPELAGO_CLUSTERING_ROOT` | `../dEA-clustering` | the pipeline checkout to import |
| `ARCHIPELAGO_FRONTEND_ROOT` | `../archipelago-frontend` | for its `data/` run library |
| `ARCHIPELAGO_EXTRA_RUN_ROOTS` | — | extra directories runs may be read from, `:`-separated |
| `ARCHIPELAGO_API_WORK` | `./work` | uploads and written artifacts |
| `ARCHIPELAGO_API_CONCURRENCY` | `2` | jobs clustering at once |

`run_path` is client-supplied, so it is confined to the configured roots. A path
outside every root is a 403 with the roots named, not a silent read — see
`orchestrator.resolve_run`.

## Endpoints

| | |
|---|---|
| `POST /cluster/run` | start a job on a run the service can see. `202` + a `job_id` |
| `POST /cluster/run/upload` | same, with the run as a zip, for when it cannot |
| `GET /cluster/status/{job_id}` | queued / running / done / error, with a phase |
| `GET /cluster/result/{job_id}` | the labels. `202` while still running |
| `GET /cluster/artifact/{job_id}/{file}` | the schema-1.0 artifact, if requested |
| `GET /cluster/runs` | every run the service can reach |
| `GET /stages` | the fixed cascade order |
| `GET /health` | that, plus the configured roots |

Asynchronous because clustering is not a request-length operation: the largest
corpus run is 99 755 evaluations and takes about 33 seconds with the full
cascade. A still-running job answers `202`, not a `4xx` — the client asked a
valid question about a real job and the answer is "not yet".

```bash
curl -s localhost:8000/cluster/run -H 'content-type: application/json' -d '{
  "run_id": "run-20260902T133050Z-886cb804",
  "stages": {"lsh": true, "birch": true}
}'
```

## Response

```jsonc
{
  "schema_version": "1.0",
  "run_id": "run-20260902T133050Z-886cb804",
  "assignments": { "<island>:<genome_hash>": 17, … },
  "node_weights": { "<island>:<genome_hash>": 0.83, … },  // DenStream only
  "stages_requested": ["lsh", "birch"],
  "stages_executed":  ["lsh", "birch"],                   // cascade order
  "counts": {
    "evaluations": 1710, "rows_clustered": 1710, "rows_without_genome": 0,
    "level0_nodes": 1082,   // what the frontend would draw unaided
    "clusters": 412,        // distinct labels the cascade produced
    "nodes": 430            // distinct (island, label) — what gets drawn
  },
  "diagnostics": { "lsh": {…}, "birch": {…} },
  "resolved_parameters": { "stage_order": […], "metric": "l2", "config": {…} },
  "elapsed_seconds": 0.22,
  "warnings": []
}
```

`clusters` and `nodes` differ because a macro node never spans two islands —
`levels.collapse` keys groups by `(island, label)`, since merging across islands
would invent a location no island visited.

### Two things that show up in `warnings`

**A location with more than one label.** Rows sharing a `genome_hash` share a
feature vector, so LSH and BIRCH always agree on them. DenStream need not: it
ages points, and a location revisited long after it was first seen can fall in a
newer micro-cluster. The majority label is used and the count is reported, rather
than the choice being made silently.

**Rows with no genome body.** A `hashed` row, or one `genome_sample_every`
skipped, cannot be clustered. Each keeps a group of its own at its exact
location — the same thing the pipeline does internally.

## Layout

```
app/
  main.py          the HTTP surface
  contracts.py     request and response models
  orchestrator.py  the only module that imports archipelago_clustering
  jobs.py          background execution and the in-memory job store
  uploads.py       accepting a run as a zip
  settings.py      where the two repositories are
  version.py       the service version, distinct from the schema version
tests/test_api.py  28 tests
```

The job store is in memory and single-process on purpose. A job is a pure
function of a run directory and a config, so losing one costs a re-request, not
data. If this ever has to survive a restart, the thing to add is a queue.

## Tests

```bash
./.venv/bin/python -m pytest tests/ -q      # 28 passed in ~5s
```

The frontend has 12 more that exercise this service end to end — start it, then
`cd ../archipelago-frontend && pytest tests/`. They check the collapse invariants
from that repo's `docs/EXTENDING.md` against labels this service actually
returned, for every one of the seven non-empty stage combinations.

They cover all eight toggle combinations, that request field order cannot reorder
the cascade, that only selected stages reach the `Config`, path-traversal refusal
on both `run_path` and the artifact route, and an end-to-end check that an
uploaded run produces byte-identical assignments to the same run read by path.
