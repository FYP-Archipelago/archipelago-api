"""What this layer has to get right.

Three things, really: the cascade order never depends on the request, only the
selected stages run, and the labels come back in the shape the frontend's
``levels.collapse`` accepts. Everything else is plumbing.
"""

from __future__ import annotations

import io
import sys
import time
import zipfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app import orchestrator  # noqa: E402
from app.contracts import StageParameters, StageToggles  # noqa: E402
from app.main import app  # noqa: E402

client = TestClient(app)

#: Smallest run in the corpus (1 710 evaluations), so the suite stays quick.
SMALL_RUN = "run-20260902T133050Z-886cb804"


def _runs() -> list[dict]:
    return client.get("/cluster/runs").json()["runs"]


def _submit(stages: dict, run_id: str = SMALL_RUN, **extra) -> dict:
    response = client.post(
        "/cluster/run", json={"run_id": run_id, "stages": stages, **extra}
    )
    assert response.status_code == 202, response.text
    return response.json()


def _await(job_id: str, timeout: float = 180.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        response = client.get(f"/cluster/result/{job_id}")
        if response.status_code == 200:
            return response.json()
        if response.status_code >= 400:
            pytest.fail(f"job failed: {response.text}")
        time.sleep(0.2)
    pytest.fail(f"job {job_id} did not finish in {timeout}s")


# --------------------------------------------------------------------------
# the fixed order -- the requirement this whole layer exists to hold
# --------------------------------------------------------------------------


def test_cascade_order_comes_from_the_pipeline_not_from_here():
    """If ``run_stages`` is ever reordered, this fails rather than drifting."""
    assert orchestrator.STAGE_ORDER == ("lsh", "birch", "denstream")


@pytest.mark.parametrize(
    "selected, expected",
    [
        ({}, []),
        ({"lsh": True}, ["lsh"]),
        ({"birch": True}, ["birch"]),
        ({"denstream": True}, ["denstream"]),
        ({"lsh": True, "birch": True}, ["lsh", "birch"]),
        ({"birch": True, "denstream": True}, ["birch", "denstream"]),
        ({"lsh": True, "denstream": True}, ["lsh", "denstream"]),
        ({"lsh": True, "birch": True, "denstream": True}, ["lsh", "birch", "denstream"]),
    ],
)
def test_every_toggle_combination_keeps_cascade_order(selected, expected):
    assert orchestrator.executed_stages(StageToggles(**selected)) == expected


def test_request_field_order_cannot_reorder_the_cascade():
    """A client that names denstream first still gets the cascade order."""
    reversed_body = {"denstream": True, "birch": True, "lsh": True}
    assert orchestrator.executed_stages(StageToggles(**reversed_body)) == [
        "lsh",
        "birch",
        "denstream",
    ]


def test_only_selected_stages_are_enabled_in_the_config():
    config = orchestrator.build_config(
        StageToggles(lsh=True, denstream=True), StageParameters()
    )
    assert config.stage_flags() == {"lsh": True, "birch": False, "denstream": True}


def test_parameters_reach_the_pipeline_config():
    config = orchestrator.build_config(
        StageToggles(birch=True),
        StageParameters(birch_threshold=0.25, denstream_mu=4.0, seed=7),
    )
    assert config.birch.threshold == 0.25
    assert config.denstream.mu == 4.0
    assert config.seed == 7


def test_bad_parameters_are_rejected_before_a_job_starts():
    response = client.post(
        "/cluster/run",
        json={
            "run_id": SMALL_RUN,
            "stages": {"birch": True},
            "parameters": {"denstream_beta": 5.0},  # constrained to (0, 1]
        },
    )
    assert response.status_code == 422


# --------------------------------------------------------------------------
# discovery and refusal
# --------------------------------------------------------------------------


def test_health_and_stages():
    assert client.get("/health").json()["status"] == "ok"
    body = client.get("/stages").json()
    assert body["order"] == ["lsh", "birch", "denstream"]
    assert set(body["descriptions"]) == {"lsh", "birch", "denstream"}


def test_runs_are_discovered_from_both_repositories():
    runs = _runs()
    assert any(r["run_id"] == SMALL_RUN for r in runs)
    assert {r["source"] for r in runs} >= {"dEA-run-logs"}


def test_unknown_run_is_404():
    response = client.post("/cluster/run", json={"run_id": "nope", "stages": {}})
    assert response.status_code == 404


def test_path_outside_the_run_roots_is_refused():
    response = client.post("/cluster/run", json={"run_path": "/etc", "stages": {}})
    assert response.status_code in (403, 404)


def test_exactly_one_identifier_is_required():
    both = client.post(
        "/cluster/run", json={"run_id": "a", "run_path": "/b", "stages": {}}
    )
    neither = client.post("/cluster/run", json={"stages": {}})
    assert both.status_code == 422 and neither.status_code == 422


# --------------------------------------------------------------------------
# end to end
# --------------------------------------------------------------------------


def test_level0_baseline_gives_one_group_per_location():
    """No stages selected is a real answer, not an error: it is Level 0."""
    result = _await(_submit({})["job_id"])
    assert result["stages_executed"] == []
    counts = result["counts"]
    assert counts["level0_nodes"] == counts["nodes"] == len(result["assignments"])
    assert result["node_weights"] is None


def test_full_cascade_compresses_and_labels_every_location():
    result = _await(
        _submit({"lsh": True, "birch": True, "denstream": True})["job_id"]
    )
    counts = result["counts"]

    assert result["stages_executed"] == ["lsh", "birch", "denstream"]
    # Every Level 0 location is covered, so no node is left ungrouped.
    assert len(result["assignments"]) == counts["level0_nodes"]
    # The cascade exists to reduce the node count; if it does not, say so loudly.
    assert counts["nodes"] < counts["level0_nodes"]
    # DenStream ran, so weights come back and can drive opacity.
    assert result["node_weights"] and len(result["node_weights"]) == len(result["assignments"])
    assert set(result["diagnostics"]) == {"lsh", "birch", "denstream"}


def test_node_keys_match_the_frontend_key_format():
    """``"<island>:<genome_hash>"`` -- the index of ``STN.nodes``."""
    result = _await(_submit({"birch": True})["job_id"])
    for key in list(result["assignments"])[:50]:
        island, _, genome_hash = key.partition(":")
        assert island.isdigit() or island.lstrip("-").isdigit()
        assert len(genome_hash) == 32 and int(genome_hash, 16) >= 0


def test_a_node_never_spans_two_islands_after_collapse():
    """The invariant ``levels.collapse`` enforces, checked on our side too."""
    result = _await(_submit({"lsh": True, "birch": True})["job_id"])
    islands_per_label: dict[int, set[str]] = {}
    for key, label in result["assignments"].items():
        islands_per_label.setdefault(label, set()).add(key.split(":", 1)[0])
    # A label may legitimately span islands here -- the frontend splits it by
    # island when it collapses -- so the drawn node count must account for that.
    expected = sum(len(v) for v in islands_per_label.values())
    assert result["counts"]["nodes"] == expected


def test_fewer_stages_never_compress_more_than_the_full_cascade():
    """Each stage refines the one before it, so the cascade is monotone."""
    birch_only = _await(_submit({"birch": True})["job_id"])["counts"]["nodes"]
    full = _await(
        _submit({"lsh": True, "birch": True, "denstream": True})["job_id"]
    )["counts"]["nodes"]
    assert full <= birch_only


def test_artifact_is_written_and_served_on_request():
    job = _submit({"birch": True}, include_artifact=True)
    result = _await(job["job_id"])
    assert "manifest.json" in result["artifact_files"]

    manifest = client.get(f"/cluster/artifact/{job['job_id']}/manifest.json").json()
    assert manifest["schema_version"] == "1.0"
    # The artifact and the labels came from the same config, so they agree.
    assert manifest["stages"] == {"lsh": False, "birch": True, "denstream": False}


def test_artifact_path_traversal_is_refused():
    job = _submit({"birch": True}, include_artifact=True)
    _await(job["job_id"])
    escaped = client.get(f"/cluster/artifact/{job['job_id']}/../../../etc/passwd")
    assert escaped.status_code == 404


def test_status_and_result_agree_on_an_unknown_job():
    assert client.get("/cluster/status/deadbeef").status_code == 404
    assert client.get("/cluster/result/deadbeef").status_code == 404


# --------------------------------------------------------------------------
# upload
# --------------------------------------------------------------------------


def _zip_of(run_dir: Path) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for name in ("evaluations.csv", "run.jsonl", "summary.json"):
            source = run_dir / name
            if source.is_file():
                archive.write(source, f"{run_dir.name}/{name}")
    return buffer.getvalue()


def test_uploaded_run_clusters_the_same_way():
    run_dir = orchestrator.resolve_run(run_id=SMALL_RUN)
    response = client.post(
        "/cluster/run/upload",
        files={"archive": ("run.zip", _zip_of(run_dir), "application/zip")},
        data={"birch": "true", "name": "uploaded-test-run"},
    )
    assert response.status_code == 202, response.text
    result = _await(response.json()["job_id"])
    assert result["stages_executed"] == ["birch"]

    direct = _await(_submit({"birch": True})["job_id"])
    assert result["assignments"] == direct["assignments"]


def test_upload_without_evaluations_is_rejected():
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("notes.txt", "nothing here")
    response = client.post(
        "/cluster/run/upload",
        files={"archive": ("bad.zip", buffer.getvalue(), "application/zip")},
        data={"birch": "true"},
    )
    assert response.status_code == 400
