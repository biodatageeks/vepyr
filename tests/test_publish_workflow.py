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


def test_smoke_pins_match_uv_lock():
    smoke = job_block("linux-aarch64-smoke")
    python = re.search(r'python-version: "([\d.]+)"', smoke).group(1)
    pins = dict(re.findall(r'"([a-z0-9_-]+)==([^"]+)"', smoke))
    assert set(pins) == {"pytest", "datafusion"}, pins
    for package, version in pins.items():
        assert locked_versions(package, python) == [version], (
            f"linux-aarch64-smoke pins {package}=={version} for Python {python}; "
            "bump it to the version uv.lock resolves"
        )


def test_linux_build_images_are_pinned_by_digest():
    # maturin-action reuses any local image carrying the requested tag without
    # pulling, and the self-hosted hosts are shared, so a tag is not enough.
    images = re.findall(r"^\s+container: (\S+)$", job_block("linux"), re.M)
    images = [i for i in images if not i.startswith("${{")]
    assert len(images) == 2, images
    allowed = (
        r"quay\.io/pypa/manylinux2014_x86_64@sha256:[0-9a-f]{64}",
        r"ghcr\.io/rust-cross/manylinux_2_28-cross@sha256:[0-9a-f]{64}",
    )
    for image, pattern in zip(images, allowed):
        assert re.fullmatch(pattern, image), image


def test_both_linux_wheels_build_on_the_self_hosted_hosts():
    # The fat-LTO link does not fit a hosted runner's 16 GB without swapping.
    linux = job_block("linux")
    runners = re.findall(r"^\s+runner: (.+)$", linux, re.M)
    assert runners == ["[self-hosted, linux, x64, vepyr-wgs]"] * 2, runners
    assert "swapon" not in linux


def test_aarch64_wheel_is_cross_built_for_manylinux_2_28():
    # The cross image fixes the glibc floor; the flag makes the tag state it,
    # and a cross build needs the abi3 floor named instead of found.
    linux = job_block("linux")
    entry = linux.split("- target: aarch64", 1)[1].split("steps:", 1)[0]
    assert re.search(r'^\s+manylinux: "2_28"$', entry, re.M), entry
    assert re.search(r"^\s+interpreter: --interpreter python3\.10$", entry, re.M), entry
    assert "manylinux: ${{ matrix.manylinux }}" in linux
    assert "args: --profile dist --out dist ${{ matrix.interpreter }}" in linux
    assert 'features = ["abi3-py310"]' in (ROOT / "Cargo.toml").read_text()


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
