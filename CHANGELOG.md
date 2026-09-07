# Changelog

One entry per tagged version of the clustering API service, following the same
convention as `archipelago-frontend`.

Two version numbers live here and they move independently:

* **`service_version`** (`app/version.py`) — this file. What the service *is*.
* **`schema_version`** (`contracts.API_SCHEMA_VERSION`) — what the service
  *promises* clients about the shape of a response. A release that changes only
  internals bumps the first and not the second.

Both are reported by `GET /health`.

## v0.1 — schema 1.0

**Shipped.** The connecting layer between `archipelago-frontend` and
`dEA-clustering`. Nine endpoints; the ones that matter are `POST /cluster/run`
and `GET /cluster/result/{job_id}`.

**What it demonstrates.** That the two repositories can be joined without either
knowing the other exists. This service is the only component that imports the
clustering pipeline, and it imports it the way any consumer would — through
`Config`, `RunDirectory`, `build_features`, `run_stages` and `runner.run`. Not a
line of `dEA-clustering` was modified.

**The toggle contract.** A request chooses *which* of LSH, BIRCH and DenStream
run. It never chooses the sequence: they cascade in that fixed order and each
refines the partition the previous one produced, so any other order would not
mean the same thing. `orchestrator.STAGE_ORDER` is read from the pipeline's own
`Config.stage_flags()` rather than restated, so it cannot drift from
`run_stages`. All eight combinations are covered by tests, including one that a
request naming the stages in reverse still executes them in cascade order.

**What building it revealed.**

- **Labels are the right payload, not the artifact.** `dEA-clustering` already
  emits a schema-1.0 STN artifact, but the frontend builds its own Level 0
  network and collapses it given one label per node. Returning the artifact would
  have forced a rewrite of the visualization to consume a second, differently
  shaped graph. Returning labels keyed `"<island>:<genome_hash>"` — the
  frontend's own node key — meant no view changed at all.
- **A `genome_hash` is not guaranteed one label.** Identical genomes give
  identical feature vectors, so LSH and BIRCH always agree. DenStream ages
  points, so a revisited location can fall in a newer micro-cluster. The majority
  label is taken and the count is reported in `warnings`, rather than the choice
  being made silently.
- **`clusters` and `nodes` are different numbers.** A macro node never spans two
  islands, so what gets drawn is the count of distinct `(island, label)` pairs.
  Both are reported.
- **Clustering is not a request-length operation.** The largest corpus run takes
  about 33 seconds, so jobs run in the background and a still-running job answers
  `202` — the client asked a valid question about a real job and the answer is
  "not yet", which is not a client error.
- **`run_path` is client-supplied and had to be confined.** Without a root check
  a caller could name any directory the service can read. A path outside every
  configured root is a `403` that names the roots.

**Tests.** 28, in `tests/test_api.py`. Plus 12 integration tests on the frontend
side that check the collapse invariants against labels from a running service.
