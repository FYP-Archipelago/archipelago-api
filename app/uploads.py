"""Accepting a run as a zip, for when the service cannot see the client's disk.

Deliberately small. The frontend has its own richer ingest for the same job
(``archipelago_ui/ingest.py``); duplicating a trimmed version here is the cost of
the two repositories not importing each other, and it is a low price for a
function this short.
"""

from __future__ import annotations

import io
import re
import shutil
import zipfile
from pathlib import Path

from . import settings

#: The schema 2.0 contract files. Only evaluations.csv and run.jsonl are needed
#: to cluster; the rest are copied when present because other pages read them.
CONTRACT_FILES = (
    "evaluations.csv",
    "evaluations.schema.json",
    "run.jsonl",
    "resolved_config.yaml",
    "summary.json",
)

_SAFE_NAME = re.compile(r"[^A-Za-z0-9._-]+")


class BadUpload(ValueError):
    pass


def safe_name(raw: str) -> str:
    name = _SAFE_NAME.sub("-", raw.strip()).strip("-.")
    if not name:
        raise BadUpload("the run needs a name")
    return name[:120]


def install_zip(payload: bytes, name: str | None = None) -> Path:
    """Extract one run directory out of ``payload`` into the uploads root.

    Handles a zip of the run directory, of its contents, or of either nested a
    few levels down -- whichever a user happens to produce.
    """
    settings.ensure_directories()
    try:
        archive = zipfile.ZipFile(io.BytesIO(payload))
    except zipfile.BadZipFile as error:
        raise BadUpload("not a readable zip archive") from error

    members = {
        Path(info.filename).as_posix(): info
        for info in archive.infolist()
        if not info.is_dir()
    }
    anchors = [p for p in members if Path(p).name == "evaluations.csv"]
    if not anchors:
        raise BadUpload("no evaluations.csv anywhere in the archive")
    if len(anchors) > 1:
        raise BadUpload(
            f"{len(anchors)} evaluations.csv files -- zip one run, not a runs/ directory"
        )
    prefix = Path(anchors[0]).parent.as_posix()

    suggested = Path(anchors[0]).parent.name or "uploaded-run"
    target = settings.UPLOAD_ROOT / safe_name(name or suggested)
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True)

    written = []
    for filename in CONTRACT_FILES:
        member = f"{filename}" if prefix in ("", ".") else f"{prefix}/{filename}"
        info = members.get(member)
        if info is None:
            continue
        # Write by name into a directory we chose, never by the archive's own
        # path: an entry called ../../x would otherwise escape the upload root.
        (target / filename).write_bytes(archive.read(info))
        written.append(filename)

    if "run.jsonl" not in written:
        shutil.rmtree(target)
        raise BadUpload(
            "run.jsonl is missing. Clustering needs it: the island clock offsets it "
            "carries are what put every island on one timeline."
        )
    return target
