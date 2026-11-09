"""Paths and process helpers for the native (Docker-free) local stack.

Docker Desktop needs macOS 13; this box runs macOS 12. The stack therefore
runs from downloaded release tarballs plus the Postgres binaries bundled by the
`pgserver` wheel. Everything lives under ``~/.local/ledgerline-stream`` so the
repo stays small and nothing is downloaded twice.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import tarfile
import time
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
TOOLS = Path.home() / ".local" / "ledgerline-stream"
RUN = REPO / ".run"
ARTIFACTS = REPO / "artifacts"
CHECKPOINTS = REPO / ".checkpoints"
PGDATA = REPO / ".pgdata"

KAFKA_VERSION = "3.9.1"
KAFKA_SCALA = "2.13"
KAFKA_HOME = TOOLS / f"kafka_{KAFKA_SCALA}-{KAFKA_VERSION}"
KAFKA_URL = (
    f"https://archive.apache.org/dist/kafka/{KAFKA_VERSION}/kafka_{KAFKA_SCALA}-{KAFKA_VERSION}.tgz"
)

FLINK_VERSION = "1.20.3"
FLINK_HOME = TOOLS / f"flink-{FLINK_VERSION}"
FLINK_URL = (
    f"https://archive.apache.org/dist/flink/flink-{FLINK_VERSION}/"
    f"flink-{FLINK_VERSION}-bin-scala_2.12.tgz"
)

PG_PORT = 5433
PG_USER = "postgres"
PG_DATABASE = "ledgerline"
PG_JDBC = f"jdbc:postgresql://127.0.0.1:{PG_PORT}/{PG_DATABASE}"


def ensure_dirs() -> None:
    for path in (TOOLS, RUN, ARTIFACTS, CHECKPOINTS):
        path.mkdir(parents=True, exist_ok=True)


def download(url: str, target: Path) -> Path:
    """Download and extract a .tgz once. Idempotent."""
    if target.exists():
        return target
    ensure_dirs()
    archive = TOOLS / Path(url).name
    if not archive.exists():
        print(f"downloading {url}")
        with urllib.request.urlopen(url) as response, archive.open("wb") as out:
            shutil.copyfileobj(response, out)
    print(f"extracting {archive.name}")
    with tarfile.open(archive, "r:gz") as tar:
        tar.extractall(TOOLS)
    archive.unlink(missing_ok=True)
    return target


def kafka_bin(name: str) -> Path:
    return KAFKA_HOME / "bin" / name


def flink_bin(name: str) -> Path:
    return FLINK_HOME / "bin" / name


def postgres_bin() -> Path:
    from pgserver._commands import POSTGRES_BIN_PATH

    return Path(POSTGRES_BIN_PATH)


def wait_for(description: str, predicate, timeout: float = 90.0, interval: float = 1.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            print(f"[ok] {description}")
            return
        time.sleep(interval)
    raise SystemExit(f"[timeout] {description} did not become ready in {timeout:.0f}s")


def run(cmd: list[str], *, check: bool = True, **kwargs) -> subprocess.CompletedProcess:
    printable = " ".join(str(part) for part in cmd)
    print(f"+ {printable}")
    return subprocess.run(cmd, check=check, text=True, **kwargs)


def pid_file(name: str) -> Path:
    RUN.mkdir(parents=True, exist_ok=True)
    return RUN / f"{name}.pid"


def write_pid(name: str, pid: int) -> None:
    pid_file(name).write_text(str(pid))


def read_pid(name: str) -> int | None:
    path = pid_file(name)
    if not path.exists():
        return None
    try:
        return int(path.read_text().strip())
    except ValueError:
        return None


def process_alive(pid: int | None) -> bool:
    if pid is None:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def stop_pid(name: str, *, sig: int = 15) -> bool:
    pid = read_pid(name)
    if not process_alive(pid):
        pid_file(name).unlink(missing_ok=True)
        return False
    os.kill(pid, sig)
    for _ in range(30):
        if not process_alive(pid):
            break
        time.sleep(0.5)
    pid_file(name).unlink(missing_ok=True)
    return True
