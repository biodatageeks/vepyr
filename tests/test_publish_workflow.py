"""Static guarantees of publish_to_pypi.yml that review must not have to re-derive."""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WF = (ROOT / ".github/workflows/publish_to_pypi.yml").read_text()
LOCK = (ROOT / "uv.lock").read_text()


def job_block(name: str) -> str:
    m = re.search(
        rf"^  {name}:\n(.*?)(?=^  [a-z][a-z0-9-]*:\n|^\S|\Z)", WF, re.S | re.M
    )
    assert m, name
    return m.group(1)


def locked_versions(package: str, python: str) -> list[str]:
    """Versions uv.lock resolves `package` to for one CPython minor.

    uv forks the resolution by interpreter, and each fork's package entry
    lists the markers it applies to. Only the `>=`/`<`/`==` minor-version
    markers uv.lock uses here are understood.
    """
    minor = tuple(int(p) for p in python.split("."))
    found = []
    for entry in LOCK.split("\n[[package]]\n"):
        if not re.search(rf'^name = "{re.escape(package)}"$', entry, re.M):
            continue
        version = re.search(r'^version = "([^"]+)"$', entry, re.M).group(1)
        markers = re.search(r"^resolution-markers = \[\n(.*?)^\]", entry, re.S | re.M)
        if markers is None:
            found.append(version)
            continue
        for marker in re.findall(r'^\s*"(.*)",?$', markers.group(1), re.M):
            if all(_holds(clause, minor) for clause in marker.split(" and ")):
                found.append(version)
                break
    return found


def _holds(clause: str, minor: tuple[int, int]) -> bool:
    m = re.fullmatch(r"python_full_version (>=|<|==) '(\d+)\.(\d+)(?:\.\*)?'", clause)
    assert m, f"unhandled marker clause: {clause}"
    op, bound = m.group(1), (int(m.group(2)), int(m.group(3)))
    return {">=": minor >= bound, "<": minor < bound, "==": minor == bound}[op]


def test_smoke_datafusion_pin_matches_uv_lock():
    smoke = job_block("linux-aarch64-smoke")
    python = re.search(r'python-version: "([\d.]+)"', smoke).group(1)
    pinned = re.findall(r'"datafusion==([^"]+)"', smoke)
    assert len(pinned) == 1, pinned
    assert locked_versions("datafusion", python) == pinned, (
        f"linux-aarch64-smoke pins datafusion=={pinned[0]} for Python {python}; "
        "bump it to the version uv.lock resolves"
    )


def test_publish_waits_for_the_aarch64_smoke_job():
    needs = re.search(r"^    needs: \[(.*)\]$", job_block("publish"), re.M).group(1)
    assert "linux-aarch64-smoke" in [n.strip() for n in needs.split(",")]


def test_self_hosted_build_deletes_its_root_owned_tree():
    linux = job_block("linux")
    step = linux.split("- name: Delete the root-owned build tree", 1)
    assert len(step) == 2, "cleanup step missing"
    assert "if: always() && runner.environment == 'self-hosted'" in step[1]
    assert "--entrypoint find" in step[1]
    # It must come after the upload, or it would delete dist/ first.
    assert linux.index("actions/upload-artifact") < linux.index(
        "- name: Delete the root-owned build tree"
    )
