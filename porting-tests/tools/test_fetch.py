"""Tests for :mod:`run_tests.fetch`, against a local directory standing in for the Hub.

No test here touches the network. Everything runs against a synthetic remote built by
the ``remote`` fixture — three flavours, seven entities, three contigs, with a lister
and a downloader that read that directory instead of huggingface.co — except the two
that point ``HF_ENDPOINT`` at a closed local port on purpose, to prove that an
unreachable Hub is a clean exit 4 and not a traceback.
"""

from __future__ import annotations

import fcntl
import gzip
import hashlib
import json
import os
import shutil
import subprocess
import sys
import threading
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Final

import pytest

import run_tests.fetch as fetch_cache
from run_tests.fetch import (
    CACHE_METADATA,
    ENTITIES,
    MANIFEST,
    PROVENANCE,
    FastaPin,
    Flavour,
    RemoteFile,
    Selection,
    _merge_contigs,
    allow_patterns,
    bsd_sum,
    fetch,
    load_dataset_pins,
    read_provenance,
    select_remote,
    trim_manifest,
    verify_flavour,
    write_fai,
)
from run_tests.verdict import Exit, RunTestsError

CONTIGS: Final = ("chr1", "chr21", "chr22")
REVISIONS: Final[dict[Flavour, str]] = {
    Flavour.ENSEMBL: "15a048f0585a7a40d2d40cb1c2f7806638ee500b",
    Flavour.REFSEQ: "d1b97a0a72d8ed94418b7a65562e89d8a0a5b3ca",
    Flavour.MERGED: "5b83dd8d249106c6cc3f1c04c522b4bec716cc97",
}
PINS_TOML: Final = (
    "\n".join(
        ["schema_version = 1", ""]
        + [
            f'[hf_cache_{f.value}]\nrepo = "https://huggingface.co/datasets/biodatageeks/vepyr_116_GRCh38_{f.value}"\n'
            f'ref = "main"\nsha = "{REVISIONS[f]}"\nrole = "dataset"\nnote = "n"\n'
            for f in Flavour
        ]
    )
    + "\n"
)


@dataclass
class FakeHub:
    """A directory of flavours playing the Hub; records every download call."""

    root: Path
    calls: list[tuple[str, str, list[str], Path]] = field(default_factory=list)

    def repo_dir(self, repo_id: str) -> Path:
        return self.root / repo_id.rsplit("_", 1)[1]

    def lister(self, repo_id: str, revision: str) -> list[RemoteFile]:
        flavour = Flavour(repo_id.rsplit("_", 1)[1])
        if revision != REVISIONS[flavour]:
            raise RunTestsError(Exit.REVISION, f"{repo_id}: no revision {revision}")
        files = []
        for path in sorted(self.repo_dir(repo_id).rglob("*")):
            if path.is_file():
                rel = path.relative_to(self.repo_dir(repo_id)).as_posix()
                data = path.read_bytes()
                sha = (
                    hashlib.sha256(data).hexdigest()
                    if rel.endswith(".parquet")
                    else None
                )
                files.append(
                    RemoteFile(
                        path=rel,
                        size=len(data),
                        sha256=sha,
                        git_blob_sha=None
                        if sha
                        else hashlib.sha1(
                            f"blob {len(data)}\0".encode() + data
                        ).hexdigest(),
                    )
                )
        return files

    def downloader(
        self, repo_id: str, revision: str, patterns: list[str], local_dir: Path
    ) -> None:
        self.calls.append((repo_id, revision, patterns, local_dir))
        for remote in select_remote(self.lister(repo_id, revision), patterns):
            target = local_dir / remote.path
            if target.exists() and patterns != [f"*/{MANIFEST}"]:
                continue  # snapshot_download's metadata short-circuit: present, skipped
            # only the manifests-only call forces a present file (hub_downloader,
            # force_download): a trimmed manifest survives every other call (#206)
            target.parent.mkdir(parents=True, exist_ok=True)
            # like snapshot_download: materialise into a temp name, then rename
            tmp = target.with_name(f".{target.name}.{os.getpid()}.incomplete")
            shutil.copyfile(self.repo_dir(repo_id) / remote.path, tmp)
            os.replace(tmp, target)


def build_hub(hub: Path, contigs: Sequence[str] = CONTIGS) -> FakeHub:
    """Three flavours x seven entities x ``contigs``, plus a README each."""
    for flavour in Flavour:
        for entity in ENTITIES:
            entity_dir = hub / flavour.value / entity
            entity_dir.mkdir(parents=True)
            entries = []
            for contig in contigs:
                (entity_dir / f"{contig}.parquet").write_bytes(
                    f"{flavour}:{entity}:{contig}".encode()
                )
                entries.append(
                    {"chrom": contig, "dataset": f"{contig}.parquet", "rows": 1}
                )
            (entity_dir / MANIFEST).write_text(json.dumps(entries, indent=2) + "\n")
        (hub / flavour.value / "README.md").write_text(f"# {flavour}\n")
        (hub / flavour.value / "chr_synonyms.txt").write_text("21\tNC_000021.9\n")
        (hub / flavour.value / "reference_policy.json").write_text("{}\n")
    return FakeHub(root=hub)


@pytest.fixture
def remote(tmp_path: Path) -> FakeHub:
    """Three flavours x seven entities x three contigs, plus a README each."""
    return build_hub(tmp_path / "hub")


def top_up_worker(
    hub: Path, pins: Path, root: Path, contig: str, deadline: float
) -> None:
    """One OS process of the arm-C harness: wait for the deadline, fetch one contig."""
    remote = FakeHub(root=hub)
    datasets, _ = load_dataset_pins(pins)
    while time.time() < deadline:
        time.sleep(0.005)
    code = fetch(
        _selection(root, contigs=(contig,), flavours=(Flavour.ENSEMBL,)),
        datasets,
        None,
        lister=remote.lister,
        downloader=remote.downloader,
        out=lambda _: None,
    )
    raise SystemExit(int(code))


@pytest.fixture
def pins(tmp_path: Path) -> Path:
    path = tmp_path / "PINS.toml"
    path.write_text(PINS_TOML)
    return path


def _selection(root: Path, **overrides: object) -> Selection:
    base = dict(
        root=root,
        flavours=tuple(Flavour),
        contigs=None,
        fasta=False,
        trim_manifests=True,
        verify=False,
        dry_run=False,
    )
    base.update(overrides)
    return Selection(**base)  # type: ignore[arg-type]


def _run(
    remote: FakeHub, pins: Path, root: Path, **overrides: object
) -> tuple[Exit, list[str]]:
    dataset_pins, fasta_pin = load_dataset_pins(pins)
    out: list[str] = []
    code = fetch(
        _selection(root, **overrides),
        dataset_pins,
        fasta_pin,
        lister=remote.lister,
        downloader=remote.downloader,
        argv=["--cache-dir", str(root)],
        pins_toml=pins,
        out=out.append,
    )
    return code, out


# --- patterns and selection ----------------------------------------------------------


def test_whole_genome_pattern_is_star() -> None:
    assert allow_patterns(None) == ["*"]


def test_per_contig_patterns_name_shards_manifests_and_readme() -> None:
    assert allow_patterns(["chr21", "chr22"]) == [
        "*/chr21.parquet",
        "*/chr22.parquet",
        "*/chrom_manifest.json",
        "README.md",
        *CACHE_METADATA,
    ]


def test_select_remote_matches_like_fnmatch(remote: FakeHub) -> None:
    files = remote.lister(
        "biodatageeks/vepyr_116_GRCh38_ensembl", REVISIONS[Flavour.ENSEMBL]
    )
    chosen = select_remote(files, allow_patterns(["chr21"]))
    assert len(chosen) == 7 + 7 + 3
    assert all(
        f.path.endswith(("chr21.parquet", MANIFEST, "README.md", *CACHE_METADATA))
        for f in chosen
    )


@pytest.mark.parametrize("name", CACHE_METADATA)
def test_partial_download_fetches_and_repairs_metadata(
    remote: FakeHub, pins: Path, tmp_path: Path, name: str
) -> None:
    root = tmp_path / "empty-cache"
    for attempt in range(2):
        code, _ = _run(
            remote,
            pins,
            root,
            contigs=("chr21",),
            flavours=(Flavour.MERGED,),
            verify=True,
        )
        assert code == Exit.OK
        local = root / "116_GRCh38_merged" / name
        assert local.read_bytes() == (remote.root / "merged" / name).read_bytes()
        if attempt == 0:
            local.unlink()
    assert read_provenance(root).runs[-1].added_files == 1


@pytest.mark.parametrize("name", CACHE_METADATA)
def test_missing_remote_metadata_is_incomplete(
    remote: FakeHub, pins: Path, tmp_path: Path, name: str
) -> None:
    (remote.root / "merged" / name).unlink()
    with pytest.raises(RunTestsError) as caught:
        _run(
            remote,
            pins,
            tmp_path / "cache",
            contigs=("chr21",),
            flavours=(Flavour.MERGED,),
        )
    assert caught.value.code == Exit.INCOMPLETE
    assert name in str(caught.value)
    assert not remote.calls


@pytest.mark.parametrize("name", CACHE_METADATA)
def test_verify_rejects_corrupted_metadata(
    remote: FakeHub, pins: Path, tmp_path: Path, name: str
) -> None:
    root = tmp_path / "cache"
    _run(remote, pins, root, contigs=("chr21",), flavours=(Flavour.MERGED,))
    local = root / "116_GRCh38_merged" / name
    local.write_bytes(b"x" * local.stat().st_size)
    with pytest.raises(RunTestsError) as caught:
        _run(
            remote,
            pins,
            root,
            contigs=("chr21",),
            flavours=(Flavour.MERGED,),
            verify=True,
        )
    assert caught.value.code == Exit.VERIFY
    assert name in str(caught.value)


# --- pins -----------------------------------------------------------------------------


def test_pins_are_read_per_flavour(pins: Path) -> None:
    datasets, fasta = load_dataset_pins(pins)
    assert datasets[Flavour.REFSEQ].repo_id == "biodatageeks/vepyr_116_GRCh38_refseq"
    assert datasets[Flavour.REFSEQ].revision == REVISIONS[Flavour.REFSEQ]
    assert fasta is None


def test_a_missing_flavour_pin_is_usage_error(tmp_path: Path) -> None:
    bad = tmp_path / "PINS.toml"
    bad.write_text(PINS_TOML.replace("[hf_cache_merged]", "[hf_cache_mergd]"))
    with pytest.raises(RunTestsError) as caught:
        load_dataset_pins(bad)
    assert caught.value.code == Exit.USAGE
    assert "hf_cache_merged" in str(caught.value)


def test_a_missing_pins_file_is_usage_error(tmp_path: Path) -> None:
    with pytest.raises(RunTestsError) as caught:
        load_dataset_pins(tmp_path / "absent.toml")
    assert caught.value.code == Exit.USAGE


FASTA_TABLE: Final = (
    "[grch38_fasta]\n"
    'repo = "https://ftp.ensembl.org/pub/release-116/fasta/homo_sapiens/dna"\n'
    'ref = "Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz"\n'
    f'sha = "{"1" * 64}"\n'
    'role = "dataset"\n'
    'ensembl_sum = "22450 861294"\n'
)


def test_fasta_pin_reads_ensembl_sum_and_refuses_a_pin_without_it(
    tmp_path: Path,
) -> None:
    """An absent (or misspelt) ``ensembl_sum`` is a usage error, never a silent skip.

    The field is read, not ``.get``-ed with a fallback to the empty string.
    """
    good = tmp_path / "PINS.toml"
    good.write_text(PINS_TOML + FASTA_TABLE)
    _, fasta = load_dataset_pins(good)
    assert fasta is not None and fasta.ensembl_sum == "22450 861294"
    bad = tmp_path / "bad.toml"
    bad.write_text(PINS_TOML + FASTA_TABLE.replace("ensembl_sum =", "ensembl_sun ="))
    with pytest.raises(RunTestsError) as caught:
        load_dataset_pins(bad)
    assert caught.value.code == Exit.USAGE
    assert "ensembl_sum" in str(caught.value)


def test_fasta_pin_sha_must_be_a_64_hex_sha256(tmp_path: Path) -> None:
    """A git sha or a truncated paste is exit 2, not a silently skipped .fa check."""
    good = tmp_path / "PINS.toml"
    good.write_text(PINS_TOML + FASTA_TABLE)
    _, fasta = load_dataset_pins(good)
    assert fasta is not None and fasta.sha256_fa == "1" * 64
    for bad_sha in ("0e02f9c6a1", "g" * 64):
        bad = tmp_path / "bad.toml"
        bad.write_text(
            PINS_TOML + FASTA_TABLE.replace('"' + "1" * 64 + '"', f'"{bad_sha}"')
        )
        with pytest.raises(RunTestsError) as caught:
            load_dataset_pins(bad)
        assert caught.value.code == Exit.USAGE
        assert "sha" in str(caught.value)


def _tiny_fasta_pin(tmp_path: Path, sha256_fa: str) -> tuple[FastaPin, bytes]:
    gz_bytes = gzip.compress(TINY_FA)
    gz_path = tmp_path / "served.fa.gz"
    gz_path.write_bytes(gz_bytes)
    checksum, blocks = bsd_sum(gz_path)
    pin = FastaPin(
        url="https://ftp.ensembl.org/x/y.fa.gz",
        sha256_fa=sha256_fa,
        ensembl_sum=f"{checksum} {blocks}",
    )
    return pin, gz_bytes


def _fetch_fasta_root(
    remote: FakeHub, pins: Path, root: Path, pin: FastaPin, fetcher: object
) -> Exit:
    datasets, _ = load_dataset_pins(pins)
    return fetch(
        _selection(root, contigs=("chr21",), flavours=(Flavour.ENSEMBL,), fasta=True),
        datasets,
        pin,
        lister=remote.lister,
        downloader=remote.downloader,
        fasta_fetcher=fetcher,  # type: ignore[arg-type]
        out=lambda _: None,
    )


def test_fetch_fasta_with_a_wrong_sha256_exits_5(
    remote: FakeHub, pins: Path, tmp_path: Path
) -> None:
    """The .fa digest is compared against the pin; the mutant dropping it dies here."""
    pin, gz_bytes = _tiny_fasta_pin(tmp_path, "0" * 64)
    with pytest.raises(RunTestsError) as caught:
        _fetch_fasta_root(
            remote,
            pins,
            tmp_path / "root",
            pin,
            lambda url, dst: dst.write_bytes(gz_bytes),
        )
    assert caught.value.code == Exit.VERIFY
    assert "sha256" in str(caught.value)


def test_an_interrupted_fasta_download_leaves_no_file_and_is_retried(
    remote: FakeHub, pins: Path, tmp_path: Path
) -> None:
    """F4: a dropped transfer must not poison the root; the next run fetches again."""
    pin, gz_bytes = _tiny_fasta_pin(tmp_path, hashlib.sha256(TINY_FA).hexdigest())
    calls: list[str] = []

    def dropped(url: str, destination: Path) -> None:
        calls.append("dropped")
        destination.write_bytes(gz_bytes[: len(gz_bytes) // 2])
        raise OSError("connection reset")

    def healthy(url: str, destination: Path) -> None:
        calls.append("healthy")
        destination.write_bytes(gz_bytes)

    root = tmp_path / "root"
    with pytest.raises(RunTestsError) as caught:
        _fetch_fasta_root(remote, pins, root, pin, dropped)
    assert caught.value.code == Exit.INCOMPLETE, "F6: a dropped transfer is exit 4"
    assert [p.name for p in (root / "fasta").iterdir()] == [], "no .gz and no .part"
    assert _fetch_fasta_root(remote, pins, root, pin, healthy) == Exit.OK
    assert calls == ["dropped", "healthy"]


def test_a_sum_mismatch_names_the_gz_path_to_remove(
    remote: FakeHub, pins: Path, tmp_path: Path
) -> None:
    pin = FastaPin(
        url="https://ftp.ensembl.org/x/y.fa.gz", sha256_fa="a" * 64, ensembl_sum="1 1"
    )
    gz_bytes = gzip.compress(TINY_FA)
    root = tmp_path / "root"
    with pytest.raises(RunTestsError) as caught:
        _fetch_fasta_root(
            remote, pins, root, pin, lambda url, dst: dst.write_bytes(gz_bytes)
        )
    assert caught.value.code == Exit.VERIFY
    assert str(root / "fasta" / "y.fa.gz") in str(caught.value)


# --- dry run --------------------------------------------------------------------------


def test_dry_run_lists_files_and_bytes_and_writes_nothing(
    remote: FakeHub, pins: Path, tmp_path: Path
) -> None:
    root = tmp_path / "root"
    code, out = _run(
        remote,
        pins,
        root,
        contigs=("chr21",),
        flavours=(Flavour.ENSEMBL,),
        dry_run=True,
    )
    assert code == Exit.OK
    assert not root.exists(), "dry-run must not create the root"
    assert remote.calls == []
    assert sum(line.startswith("116_GRCh38_ensembl/") for line in out) == 17
    assert out[-1].startswith("# total: 17 files, ") and out[-1].endswith(
        "; nothing written"
    )


def test_dry_run_with_an_unknown_contig_exits_incomplete(
    remote: FakeHub, pins: Path, tmp_path: Path
) -> None:
    with pytest.raises(RunTestsError) as caught:
        _run(remote, pins, tmp_path / "root", contigs=("chrZZ",), dry_run=True)
    assert caught.value.code == Exit.INCOMPLETE
    assert "chrZZ" in str(caught.value) or "names no shard" in str(caught.value)
    assert "chrMT" not in str(caught.value), "chrMT is refused by the entity guard"


# --- per-contig fetch, trimming, provenance -------------------------------------------


def test_per_contig_fetch_trims_manifests_and_records_contigs(
    remote: FakeHub, pins: Path, tmp_path: Path
) -> None:
    root = tmp_path / "root"
    code, _ = _run(
        remote, pins, root, contigs=("chr21", "chr22"), flavours=(Flavour.ENSEMBL,)
    )
    assert code == Exit.OK
    manifest = json.loads(
        (root / "116_GRCh38_ensembl/variation" / MANIFEST).read_text()
    )
    assert [e["chrom"] for e in manifest] == ["chr21", "chr22"]
    assert not (root / "116_GRCh38_ensembl/variation/chr1.parquet").exists()
    provenance = read_provenance(root)
    assert provenance is not None
    record = provenance.datasets["ensembl"]
    assert record.revision == REVISIONS[Flavour.ENSEMBL]
    assert record.contigs == ["chr21", "chr22"]
    assert record.manifests_trimmed is True
    assert record.files == 2 * 7 + 7 + 3
    assert provenance.runs[-1].added_files == 24
    assert provenance.runs[-1].refreshed_manifests == 7
    assert provenance.pins_toml_sha256 == hashlib.sha256(pins.read_bytes()).hexdigest()


def test_no_trim_leaves_manifests_verbatim(
    remote: FakeHub, pins: Path, tmp_path: Path
) -> None:
    root = tmp_path / "root"
    _run(
        remote,
        pins,
        root,
        contigs=("chr21",),
        flavours=(Flavour.ENSEMBL,),
        trim_manifests=False,
    )
    manifest = json.loads((root / "116_GRCh38_ensembl/exon" / MANIFEST).read_text())
    assert [e["chrom"] for e in manifest] == list(CONTIGS)
    assert read_provenance(root).datasets["ensembl"].manifests_trimmed is False  # type: ignore[union-attr]


def test_full_genome_fetch_records_all_and_touches_no_manifest(
    remote: FakeHub, pins: Path, tmp_path: Path
) -> None:
    root = tmp_path / "root"
    code, _ = _run(remote, pins, root)
    assert code == Exit.OK
    for flavour in Flavour:
        assert read_provenance(root).datasets[flavour.value].contigs == "ALL"  # type: ignore[union-attr]
        for entity in ENTITIES:
            assert (root / flavour.dir_name / entity / MANIFEST).read_text() == (
                remote.root / flavour.value / entity / MANIFEST
            ).read_text()
    assert [c[2] for c in remote.calls] == [["*"]] * 3


def test_second_run_adds_nothing_and_appends_a_run(
    remote: FakeHub, pins: Path, tmp_path: Path
) -> None:
    root = tmp_path / "root"
    _run(remote, pins, root, contigs=("chr21",), flavours=(Flavour.ENSEMBL,))
    _run(remote, pins, root, contigs=("chr21",), flavours=(Flavour.ENSEMBL,))
    runs = read_provenance(root).runs  # type: ignore[union-attr]
    assert len(runs) == 2
    assert runs[1].added_files == 0
    assert runs[1].skipped_files == 17


def test_contig_runs_accumulate_into_a_list_never_all(
    remote: FakeHub, pins: Path, tmp_path: Path
) -> None:
    root = tmp_path / "root"
    _run(remote, pins, root, contigs=("chr21",), flavours=(Flavour.ENSEMBL,))
    _run(remote, pins, root, contigs=("chr22",), flavours=(Flavour.ENSEMBL,))
    assert read_provenance(root).datasets["ensembl"].contigs == ["chr21", "chr22"]  # type: ignore[union-attr]


def _chroms(root: Path, entity: str) -> list[str]:
    """The ``chrom`` column of one ensembl entity's manifest under ``root``."""
    manifest = root / "116_GRCh38_ensembl" / entity / MANIFEST
    return [e["chrom"] for e in json.loads(manifest.read_text())]


def test_a_top_up_with_a_new_contig_refreshes_every_trimmed_manifest(
    remote: FakeHub, pins: Path, tmp_path: Path
) -> None:
    """#206: the chr22 run must not leave chr21-only manifests beside chr22 shards."""
    root = tmp_path / "root"
    _run(remote, pins, root, contigs=("chr21",), flavours=(Flavour.ENSEMBL,))
    _, out = _run(remote, pins, root, contigs=("chr22",), flavours=(Flavour.ENSEMBL,))
    assert "7 manifest(s) refreshed" in out[-1]
    assert read_provenance(root).runs[-1].refreshed_manifests == 7  # type: ignore[union-attr]
    for entity in ENTITIES:
        assert _chroms(root, entity) == ["chr21", "chr22"], entity


def test_rerunning_the_declared_contigs_repairs_a_stale_root(
    remote: FakeHub, pins: Path, tmp_path: Path
) -> None:
    """#206: fetching declared contigs repairs a root with unlisted shards.

    The stale root is the one master left behind: chr21 and chr22 shards, manifests
    trimmed to chr21. Every shard is present, so nothing is downloaded; only the gate
    can notice the unlisted chr22 shard.
    """
    root = tmp_path / "root"
    _run(remote, pins, root, contigs=("chr21",), flavours=(Flavour.ENSEMBL,))
    for entity in ENTITIES:
        shutil.copyfile(
            remote.root / "ensembl" / entity / "chr22.parquet",
            root / "116_GRCh38_ensembl" / entity / "chr22.parquet",
        )
    _run(remote, pins, root, contigs=("chr21", "chr22"), flavours=(Flavour.ENSEMBL,))
    run = read_provenance(root).runs[-1]  # type: ignore[union-attr]
    assert (run.added_files, run.refreshed_manifests) == (0, 7)
    for entity in ENTITIES:
        assert _chroms(root, entity) == ["chr21", "chr22"], entity


def test_a_run_on_a_consistent_root_rewrites_no_manifest(
    remote: FakeHub, pins: Path, tmp_path: Path
) -> None:
    """#206 regression guard: the fix is a sharper gate, not "always re-trim"."""
    root = tmp_path / "root"
    for _ in range(2):
        _run(
            remote, pins, root, contigs=("chr21", "chr22"), flavours=(Flavour.ENSEMBL,)
        )
    assert read_provenance(root).runs[-1].refreshed_manifests == 0  # type: ignore[union-attr]
    assert [c[2] for c in remote.calls].count([f"*/{MANIFEST}"]) == 1


def test_a_shard_the_hub_manifest_omits_never_forces_a_manifest_refetch(
    remote: FakeHub, pins: Path, tmp_path: Path
) -> None:
    """#216 F1: only requested shards open the gate, so the root settles.

    A whole-flavour root holds shards the Hub's own manifest omits (the shared cache
    has GL*/HG*_PATCH exon shards). Re-fetching cannot list them, so a gate over every
    ``*.parquet`` stayed open and each per-contig run made a forced manifests-only call.
    """
    exon = remote.root / "ensembl" / "exon"
    (exon / "GL000009.2.parquet").write_bytes(b"ensembl:exon:GL000009.2")
    root = tmp_path / "root"
    _run(remote, pins, root, flavours=(Flavour.ENSEMBL,))
    flavour_dir = root / "116_GRCh38_ensembl"
    assert (flavour_dir / "exon" / "GL000009.2.parquet").is_file()
    manifests = {e: (flavour_dir / e / MANIFEST).read_bytes() for e in ENTITIES}
    remote.calls.clear()
    for _ in range(3):
        code, _ = _run(
            remote, pins, root, contigs=("chr21",), flavours=(Flavour.ENSEMBL,)
        )
        assert code is Exit.OK
    assert [c[2] for c in remote.calls].count([f"*/{MANIFEST}"]) == 0
    assert read_provenance(root).runs[-1].refreshed_manifests == 0  # type: ignore[union-attr]
    assert {e: (flavour_dir / e / MANIFEST).read_bytes() for e in ENTITIES} == manifests


def test_flavour_subset_fetches_only_those(
    remote: FakeHub, pins: Path, tmp_path: Path
) -> None:
    root = tmp_path / "root"
    _run(
        remote,
        pins,
        root,
        flavours=(Flavour.REFSEQ, Flavour.MERGED),
        contigs=("chr21",),
    )
    assert not (root / "116_GRCh38_ensembl").exists()
    assert set(read_provenance(root).datasets) == {"refseq", "merged"}  # type: ignore[union-attr]


def test_gitattributes_from_the_hub_listing_counts_as_present(
    remote: FakeHub, pins: Path, tmp_path: Path
) -> None:
    """I-20: every real dataset lists ``.gitattributes``; a whole-genome selection wants
    it, so it must count as on disk once it is — only this tool's own temp names are
    transient. Before the fix every idempotent whole-genome/--fasta run exited 4."""
    for flavour in Flavour:
        (remote.root / flavour.value / ".gitattributes").write_text(
            "*.parquet filter=lfs\n"
        )
    root = tmp_path / "root"
    for _ in range(2):
        code, _ = _run(remote, pins, root, flavours=(Flavour.ENSEMBL,))
        assert code == Exit.OK
    assert (root / "116_GRCh38_ensembl" / ".gitattributes").is_file()
    record = read_provenance(root).datasets["ensembl"]  # type: ignore[union-attr]
    assert record.files == 3 * 7 + 7 + 4, (
        "metadata, README.md and .gitattributes counted"
    )
    assert read_provenance(root).runs[-1].skipped_files == 32  # type: ignore[union-attr]


def test_files_and_bytes_describe_the_root_like_contigs_do(
    remote: FakeHub, pins: Path, tmp_path: Path
) -> None:
    """W3: after chr21 then chr22 the record counts every file on disk, not one run."""
    root = tmp_path / "root"
    _run(remote, pins, root, contigs=("chr21",), flavours=(Flavour.ENSEMBL,))
    _run(remote, pins, root, contigs=("chr22",), flavours=(Flavour.ENSEMBL,))
    record = read_provenance(root).datasets["ensembl"]  # type: ignore[union-attr]
    on_disk = [
        p
        for p in (root / "116_GRCh38_ensembl").rglob("*")
        if p.is_file() and ".cache" not in p.parts
    ]
    assert record.contigs == ["chr21", "chr22"]
    assert record.files == len(on_disk) == 2 * 7 + 7 + 3
    assert record.bytes == sum(p.stat().st_size for p in on_disk)


def _forced_calls(remote: FakeHub) -> int:
    """How many manifests-only (forced) calls ``remote`` has served."""
    return [c[2] for c in remote.calls].count([f"*/{MANIFEST}"])


def test_a_whole_genome_run_untrims_a_per_contig_root_once(
    remote: FakeHub, pins: Path, tmp_path: Path
) -> None:
    """#223: ``contigs: ALL`` must not sit beside manifests trimmed to one contig.

    The whole-genome run makes one forced manifests-only call (keyed on the record's
    ``manifests_trimmed``), does not trim, and records ``manifests_trimmed: false``;
    a second whole-genome run on the repaired root makes no forced call.
    """
    root = tmp_path / "root"
    _run(remote, pins, root, contigs=("chr21",), flavours=(Flavour.ENSEMBL,))
    before = _forced_calls(remote)
    code, out = _run(remote, pins, root, flavours=(Flavour.ENSEMBL,))
    assert code is Exit.OK
    assert _forced_calls(remote) - before == 1
    assert "7 manifest(s) refreshed" in out[-1]
    assert "trimmed" not in out[-1]  # an untrim is not a trim (PR #228 review)
    record = read_provenance(root).datasets["ensembl"]  # type: ignore[union-attr]
    assert (record.contigs, record.manifests_trimmed) == ("ALL", False)
    assert read_provenance(root).runs[-1].refreshed_manifests == 7  # type: ignore[union-attr]
    for entity in ENTITIES:
        assert _chroms(root, entity) == list(CONTIGS), entity
    before = _forced_calls(remote)
    _run(remote, pins, root, flavours=(Flavour.ENSEMBL,))
    assert _forced_calls(remote) - before == 0
    assert read_provenance(root).runs[-1].refreshed_manifests == 0  # type: ignore[union-attr]


def test_a_whole_genome_run_with_a_hub_omitted_shard_settles_after_untrim(
    remote: FakeHub, pins: Path, tmp_path: Path
) -> None:
    """#223: a shard the Hub manifest omits never keeps the untrim call firing.

    The trigger is the record, not "a shard on disk the manifest does not list", so
    after the one transition call later whole-genome runs make none.
    """
    (remote.root / "ensembl" / "exon" / "GL000009.2.parquet").write_bytes(b"omitted")
    root = tmp_path / "root"
    _run(remote, pins, root, contigs=("chr21",), flavours=(Flavour.ENSEMBL,))
    before = _forced_calls(remote)
    for _ in range(3):
        code, _ = _run(remote, pins, root, flavours=(Flavour.ENSEMBL,))
        assert code is Exit.OK
    assert (root / "116_GRCh38_ensembl" / "exon" / "GL000009.2.parquet").is_file()
    assert _forced_calls(remote) - before == 1


def test_the_untrim_trigger_is_per_flavour(
    remote: FakeHub, pins: Path, tmp_path: Path
) -> None:
    """#223: only the flavour whose record says ``manifests_trimmed`` is untrimmed.

    A root holding whole-genome refseq and per-contig ensembl: one whole-genome run
    over both makes exactly one forced call, for ensembl, and none for refseq.
    """
    root = tmp_path / "root"
    _run(remote, pins, root, flavours=(Flavour.REFSEQ,))
    _run(remote, pins, root, contigs=("chr21",), flavours=(Flavour.ENSEMBL,))
    start = len(remote.calls)
    code, _ = _run(remote, pins, root, flavours=(Flavour.ENSEMBL, Flavour.REFSEQ))
    assert code is Exit.OK
    forced = [
        repo_id.rsplit("_", 1)[1]
        for repo_id, _, patterns, _ in remote.calls[start:]
        if patterns == [f"*/{MANIFEST}"]
    ]
    assert forced == ["ensembl"]
    datasets = read_provenance(root).datasets  # type: ignore[union-attr]
    for name in ("ensembl", "refseq"):
        assert (datasets[name].contigs, datasets[name].manifests_trimmed) == (
            "ALL",
            False,
        ), name


def test_the_untrim_call_runs_under_the_provenance_lock(
    remote: FakeHub, pins: Path, tmp_path: Path
) -> None:
    """#223: the forced manifests-only call of an untrim holds ``PROVENANCE.lock``.

    The downloader probes the lock with a non-blocking ``flock`` from a second open
    file description: during the forced call the probe must find it taken.
    """
    root = tmp_path / "root"
    _run(remote, pins, root, contigs=("chr21",), flavours=(Flavour.ENSEMBL,))
    probes: list[bool] = []

    def probing_downloader(
        repo_id: str, revision: str, patterns: list[str], local_dir: Path
    ) -> None:
        if patterns == [f"*/{MANIFEST}"]:
            with (root / fetch_cache.PROVENANCE_LOCK).open("a+") as handle:
                try:
                    fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    probes.append(True)
                else:
                    fcntl.flock(handle, fcntl.LOCK_UN)
                    probes.append(False)
        remote.downloader(repo_id, revision, patterns, local_dir)

    dataset_pins, fasta_pin = load_dataset_pins(pins)
    code = fetch(
        _selection(root, flavours=(Flavour.ENSEMBL,)),
        dataset_pins,
        fasta_pin,
        lister=remote.lister,
        downloader=probing_downloader,
        out=lambda _: None,
    )
    assert code is Exit.OK
    assert probes == [True]


def test_a_whole_genome_root_topped_up_with_one_contig_stays_all(
    remote: FakeHub, pins: Path, tmp_path: Path
) -> None:
    """W5: ``ALL`` is sticky, in the helper and through :func:`fetch`."""
    assert _merge_contigs("ALL", ["chr21"]) == "ALL"
    assert _merge_contigs(["chr21"], None) == "ALL"
    root = tmp_path / "root"
    _run(remote, pins, root, flavours=(Flavour.ENSEMBL,))
    _run(remote, pins, root, contigs=("chr21",), flavours=(Flavour.ENSEMBL,))
    assert read_provenance(root).datasets["ensembl"].contigs == "ALL"  # type: ignore[union-attr]


def test_two_concurrent_runs_on_one_root_keep_both_records(
    remote: FakeHub, pins: Path, tmp_path: Path
) -> None:
    """W1: PROVENANCE.json read-modify-write is locked; neither run erases the other."""
    root = tmp_path / "root"
    datasets, _ = load_dataset_pins(pins)
    gate = threading.Barrier(2)
    errors: list[BaseException] = []

    def slow_downloader(
        repo_id: str, revision: str, patterns: list[str], local_dir: Path
    ) -> None:
        remote.downloader(repo_id, revision, patterns, local_dir)
        if patterns != [f"*/{MANIFEST}"]:  # the manifest re-fetch runs under the lock
            gate.wait(timeout=10)  # both runs have read the (absent) provenance by now

    def worker(flavour: Flavour) -> None:
        try:
            fetch(
                _selection(root, contigs=("chr21",), flavours=(flavour,)),
                datasets,
                None,
                lister=remote.lister,
                downloader=slow_downloader,
                out=lambda _: None,
            )
        except BaseException as exc:
            errors.append(exc)

    threads = [
        threading.Thread(target=worker, args=(f,))
        for f in (Flavour.ENSEMBL, Flavour.REFSEQ)
    ]
    for th in threads:
        th.start()
    for th in threads:
        th.join(timeout=30)
    assert errors == []
    provenance = read_provenance(root)
    assert provenance is not None
    assert set(provenance.datasets) == {"ensembl", "refseq"}
    assert len(provenance.runs) == 2


def test_twelve_processes_topping_up_one_root_keep_every_record(
    pins: Path, tmp_path: Path
) -> None:
    """N1 (R2 arm C): 12 OS processes, one root, one contig each, trim ON, released on a
    shared deadline so their trim/commit windows overlap — every record present, zero
    tracebacks: manifests are rewritten atomically, the trim loop runs under the lock.
    """
    contigs = [f"chr{i}" for i in range(1, 13)]
    hub = build_hub(tmp_path / "hub", contigs).root
    root = tmp_path / "root"
    deadline = time.time() + 1.5
    driver = (
        "import sys, test_fetch as t; from pathlib import Path; "
        "t.top_up_worker(Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3]), "
        "sys.argv[4], float(sys.argv[5]))"
    )
    env = {
        **os.environ,
        "PYTHONPATH": str(Path(__file__).parent),
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    procs = [
        subprocess.Popen(
            [
                sys.executable,
                "-c",
                driver,
                str(hub),
                str(pins),
                str(root),
                c,
                str(deadline),
            ],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for c in contigs
    ]
    results = [
        (c, *pr.communicate(timeout=120), pr.returncode)
        for c, pr in zip(contigs, procs, strict=True)
    ]
    crashes = [
        (c, err.strip().splitlines()[-1]) for c, _, err, rc in results if rc != 0
    ]
    assert crashes == [], crashes
    assert all("Traceback" not in err for _, _, err, _ in results)
    provenance = read_provenance(root)
    assert provenance is not None
    assert sorted(provenance.datasets["ensembl"].contigs) == sorted(contigs)
    assert len(provenance.runs) == 12
    for entity in ENTITIES:
        manifest = json.loads(
            (root / "116_GRCh38_ensembl" / entity / MANIFEST).read_text()
        )
        assert sorted(e["chrom"] for e in manifest) == sorted(contigs), entity
    assert not list(root.glob("**/*.tmp")), "no temp file left behind"


def test_write_provenance_is_atomic_via_a_process_unique_tmp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The file is never written in place: a tmp beside it, then ``os.replace``."""
    replaced: list[tuple[str, str]] = []
    real_replace = os.replace

    def spy(src: str | Path, dst: str | Path) -> None:
        replaced.append((Path(src).name, Path(dst).name))
        real_replace(src, dst)

    monkeypatch.setattr(fetch_cache.os, "replace", spy)
    fetch_cache.write_provenance(tmp_path, fetch_cache.Provenance())
    assert replaced and replaced[-1][1] == PROVENANCE
    tmp_name = replaced[-1][0]
    assert tmp_name != PROVENANCE and str(os.getpid()) in tmp_name, tmp_name
    assert not (tmp_path / tmp_name).exists()
    assert read_provenance(tmp_path) is not None


def test_trim_manifest_writes_atomically(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    entity = tmp_path / "exon"
    entity.mkdir()
    (entity / "chr21.parquet").write_bytes(b"x")
    manifest = entity / MANIFEST
    manifest.write_text(
        json.dumps(
            [
                {"chrom": "chr1", "dataset": "chr1.parquet", "rows": 1},
                {"chrom": "chr21", "dataset": "chr21.parquet", "rows": 1},
            ]
        )
    )
    replaced: list[tuple[str, str]] = []
    real_replace = os.replace

    def spy(src: str | Path, dst: str | Path) -> None:
        replaced.append((Path(src).name, Path(dst).name))
        real_replace(src, dst)

    monkeypatch.setattr(fetch_cache.os, "replace", spy)
    assert trim_manifest(manifest) is True
    assert (
        replaced and replaced[-1][1] == MANIFEST and str(os.getpid()) in replaced[-1][0]
    )
    assert [e["chrom"] for e in json.loads(manifest.read_text())] == ["chr21"]
    assert list(entity.glob("*.tmp")) == []


# --- failure paths --------------------------------------------------------------------


def test_a_damaged_provenance_is_a_usage_error_not_a_traceback(
    remote: FakeHub, pins: Path, tmp_path: Path
) -> None:
    """F5: corrupt JSON, an unknown key, and a foreign schema_version all exit 2."""
    root = tmp_path / "root"
    _run(remote, pins, root, contigs=("chr21",), flavours=(Flavour.ENSEMBL,))
    good = (root / PROVENANCE).read_text()
    variants = {
        "corrupt": good[: len(good) // 2],
        "unknown key": good.replace('"repo_id"', '"repo_idd"', 1),
        "schema": good.replace('"schema_version": 1', '"schema_version": 99', 1),
    }
    for label, text in variants.items():
        (root / PROVENANCE).write_text(text)
        with pytest.raises(RunTestsError) as caught:
            _run(remote, pins, root, contigs=("chr21",), flavours=(Flavour.ENSEMBL,))
        assert caught.value.code == Exit.USAGE, label
        assert PROVENANCE in str(caught.value), label


def test_revision_mismatch_on_disk_exits_3(
    remote: FakeHub, pins: Path, tmp_path: Path
) -> None:
    root = tmp_path / "root"
    _run(remote, pins, root, contigs=("chr21",), flavours=(Flavour.ENSEMBL,))
    text = (root / PROVENANCE).read_text().replace(REVISIONS[Flavour.ENSEMBL], "0" * 40)
    (root / PROVENANCE).write_text(text)
    with pytest.raises(RunTestsError) as caught:
        _run(remote, pins, root, contigs=("chr21",), flavours=(Flavour.ENSEMBL,))
    assert caught.value.code == Exit.REVISION
    assert "revision on disk" in str(caught.value) and "hf_cache_ensembl" in str(
        caught.value
    )
    assert "removing PROVENANCE.json" not in str(caught.value)
    assert "NEW root (set VEPYR_CACHE_ROOT)" in str(caught.value), (
        "the remedy is a NEW root, not deleting the record"
    )


def test_unreachable_revision_exits_3(remote: FakeHub, tmp_path: Path) -> None:
    bad = tmp_path / "PINS.toml"
    bad.write_text(PINS_TOML.replace(REVISIONS[Flavour.ENSEMBL], "f" * 40))
    with pytest.raises(RunTestsError) as caught:
        _run(remote, bad, tmp_path / "root", flavours=(Flavour.ENSEMBL,), dry_run=True)
    assert caught.value.code == Exit.REVISION


def test_a_downloader_that_drops_a_file_exits_4(
    remote: FakeHub, pins: Path, tmp_path: Path
) -> None:
    def lossy(
        repo_id: str, revision: str, patterns: list[str], local_dir: Path
    ) -> None:
        remote.downloader(repo_id, revision, patterns, local_dir)
        (local_dir / "exon/chr21.parquet").unlink()

    datasets, _ = load_dataset_pins(pins)
    with pytest.raises(RunTestsError) as caught:
        fetch(
            _selection(
                tmp_path / "root", contigs=("chr21",), flavours=(Flavour.ENSEMBL,)
            ),
            datasets,
            None,
            lister=remote.lister,
            downloader=lossy,
            out=lambda _: None,
        )
    assert caught.value.code == Exit.INCOMPLETE
    assert "exon/chr21.parquet" in str(caught.value)


def test_verify_passes_on_a_clean_fetch_and_fails_on_a_corrupted_shard(
    remote: FakeHub, pins: Path, tmp_path: Path
) -> None:
    root = tmp_path / "root"
    code, _ = _run(
        remote, pins, root, contigs=("chr21",), flavours=(Flavour.ENSEMBL,), verify=True
    )
    assert code == Exit.OK
    (root / "116_GRCh38_ensembl/variation/chr21.parquet").write_bytes(b"corrupt")
    with pytest.raises(RunTestsError) as caught:
        _run(
            remote,
            pins,
            root,
            contigs=("chr21",),
            flavours=(Flavour.ENSEMBL,),
            verify=True,
        )
    assert caught.value.code == Exit.VERIFY
    assert "variation/chr21.parquet" in str(caught.value)


def test_verify_catches_a_manifest_naming_an_absent_shard(tmp_path: Path) -> None:
    entity = tmp_path / "variation"
    entity.mkdir()
    (entity / MANIFEST).write_text(
        json.dumps([{"chrom": "chr9", "dataset": "chr9.parquet", "rows": 1}])
    )
    wanted = [RemoteFile(path=f"variation/{MANIFEST}", size=1, sha256=None)]
    assert verify_flavour(tmp_path, wanted) == [
        "manifest variation/chrom_manifest.json names absent shard chr9.parquet"
    ]


def test_trim_manifest_refuses_to_empty_a_manifest(tmp_path: Path) -> None:
    """An entity with no shard on disk is exit 4, never a manifest left naming chr1."""
    entity = tmp_path / "motif"
    entity.mkdir()
    manifest = entity / MANIFEST
    text = json.dumps([{"chrom": "chr1", "dataset": "chr1.parquet", "rows": 1}])
    manifest.write_text(text)
    with pytest.raises(RunTestsError) as caught:
        trim_manifest(manifest)
    assert caught.value.code == Exit.INCOMPLETE
    assert "motif" in str(caught.value)
    assert manifest.read_text() == text, "the manifest is not rewritten either"


def test_a_contig_missing_from_one_entity_exits_4_naming_it(
    remote: FakeHub, pins: Path, tmp_path: Path
) -> None:
    """Like chrMT on the real dataset (no motif/regulatory shard): refused up front."""
    motif = remote.repo_dir("biodatageeks/vepyr_116_GRCh38_ensembl") / "motif"
    (motif / "chr22.parquet").unlink()
    entries = json.loads((motif / MANIFEST).read_text())
    (motif / MANIFEST).write_text(
        json.dumps([e for e in entries if e["chrom"] != "chr22"], indent=2) + "\n"
    )
    root = tmp_path / "root"
    with pytest.raises(RunTestsError) as caught:
        _run(remote, pins, root, contigs=("chr22",), flavours=(Flavour.ENSEMBL,))
    assert caught.value.code == Exit.INCOMPLETE
    assert "motif" in str(caught.value) and "chr22" in str(caught.value)
    assert remote.calls == [], "refused before anything is downloaded"
    assert not root.exists()


def test_a_root_the_tool_just_produced_passes_its_own_verify(
    remote: FakeHub, pins: Path, tmp_path: Path
) -> None:
    """The internal AC of F2: exit 0 without --verify implies exit 0 with it."""
    root = tmp_path / "root"
    code, _ = _run(remote, pins, root, contigs=("chr21",), flavours=(Flavour.ENSEMBL,))
    assert code == Exit.OK
    code, _ = _run(
        remote, pins, root, contigs=("chr21",), flavours=(Flavour.ENSEMBL,), verify=True
    )
    assert code == Exit.OK


# --- FASTA ----------------------------------------------------------------------------

TINY_FA: Final = b">1 dna:chromosome\nACGTACGT\nACG\n>MT dna:mito\nTTTT\n"


def test_write_fai_matches_the_htslib_layout(tmp_path: Path) -> None:
    fa = tmp_path / "t.fa"
    fa.write_bytes(TINY_FA)
    fai = write_fai(fa)
    assert fai.read_text() == "1\t11\t18\t8\t9\nMT\t4\t44\t4\t5\n"


@pytest.mark.skipif(shutil.which("samtools") is None, reason="samtools not on PATH")
def test_write_fai_agrees_with_samtools_faidx(tmp_path: Path) -> None:
    ours = tmp_path / "ours.fa"
    ours.write_bytes(TINY_FA)
    theirs = tmp_path / "theirs.fa"
    theirs.write_bytes(TINY_FA)
    subprocess.run(["samtools", "faidx", str(theirs)], check=True)
    assert write_fai(ours).read_text() == (tmp_path / "theirs.fa.fai").read_text()


@pytest.mark.skipif(shutil.which("sum") is None, reason="sum not on PATH")
def test_bsd_sum_agrees_with_the_sum_binary(tmp_path: Path) -> None:
    blob = tmp_path / "blob"
    blob.write_bytes(bytes(range(256)) * 41 + b"tail")
    checksum, blocks = bsd_sum(blob)
    expected = subprocess.run(
        ["sum", str(blob)], capture_output=True, text=True, check=True
    ).stdout.split()
    assert [str(checksum), str(blocks)] == expected[:2]


def test_fetch_fasta_downloads_once_checks_sum_and_indexes(
    remote: FakeHub, pins: Path, tmp_path: Path
) -> None:
    gz_bytes = gzip.compress(TINY_FA)
    gz_path = tmp_path / "served.fa.gz"
    gz_path.write_bytes(gz_bytes)
    checksum, blocks = bsd_sum(gz_path)
    fetches: list[str] = []

    def fetcher(url: str, destination: Path) -> None:
        fetches.append(url)
        destination.write_bytes(gz_bytes)

    fasta_pin = FastaPin(
        url="https://ftp.ensembl.org/pub/release-116/fasta/homo_sapiens/dna/Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz",
        sha256_fa=hashlib.sha256(TINY_FA).hexdigest(),
        ensembl_sum=f"{checksum} {blocks}",
    )
    datasets, _ = load_dataset_pins(pins)
    root = tmp_path / "root"
    for _ in range(2):
        code = fetch(
            _selection(
                root, contigs=("chr21",), flavours=(Flavour.ENSEMBL,), fasta=True
            ),
            datasets,
            fasta_pin,
            lister=remote.lister,
            downloader=remote.downloader,
            fasta_fetcher=fetcher,
            out=lambda _: None,
        )
        assert code == Exit.OK
    assert fetches == [fasta_pin.url], (
        "the .fa.gz is fetched exactly once across two runs"
    )
    fa = root / "fasta/Homo_sapiens.GRCh38.dna.primary_assembly.fa"
    assert fa.read_bytes() == TINY_FA
    assert (root / "fasta/Homo_sapiens.GRCh38.dna.primary_assembly.fa.fai").is_file()
    assert read_provenance(root).fasta.sha256_fa == fasta_pin.sha256_fa  # type: ignore[union-attr]


def test_fetch_fasta_with_a_wrong_sum_exits_5(
    remote: FakeHub, pins: Path, tmp_path: Path
) -> None:
    gz_bytes = gzip.compress(TINY_FA)
    fasta_pin = FastaPin(
        url="https://ftp.ensembl.org/x/y.fa.gz", sha256_fa="a" * 64, ensembl_sum="1 1"
    )
    datasets, _ = load_dataset_pins(pins)
    with pytest.raises(RunTestsError) as caught:
        fetch(
            _selection(
                tmp_path / "root",
                contigs=("chr21",),
                flavours=(Flavour.ENSEMBL,),
                fasta=True,
            ),
            datasets,
            fasta_pin,
            lister=remote.lister,
            downloader=remote.downloader,
            fasta_fetcher=lambda url, dst: dst.write_bytes(gz_bytes),
            out=lambda _: None,
        )
    assert caught.value.code == Exit.VERIFY


# --- the real Hub client, without a reachable Hub ---------------------------------


def test_an_unreachable_hub_endpoint_is_exit_4_not_a_traceback(tmp_path: Path) -> None:
    """F6: httpx.ConnectError is not an OSError; the CLI must still exit 4, not 1."""
    env = {
        **os.environ,
        "VEPYR_CACHE_ROOT": str(tmp_path / "root"),
        "HF_ENDPOINT": "http://127.0.0.1:9",  # discard port: connection refused
        "HF_HUB_OFFLINE": "0",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONPATH": str(Path(__file__).parent),
    }
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "run_tests",
            "0.9.0",
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert proc.returncode == Exit.INCOMPLETE, proc.stderr[-800:]
    assert "Traceback" not in proc.stderr
    assert "Hub unreachable" in proc.stderr
    assert not (tmp_path / "root").exists()


def test_a_forced_download_timeout_wrapped_in_value_error_is_exit_4(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """huggingface_hub re-raises a transport failure of a forced download as
    ``ValueError("Force download failed due to the above error.")`` chained from
    ``httpx.ReadTimeout``; upstream it escaped as a raw traceback (exit 1)."""
    import httpx
    import huggingface_hub

    def failing(*_a: object, **_kw: object) -> None:
        try:
            raise httpx.ReadTimeout("The read operation timed out")
        except httpx.ReadTimeout as exc:
            raise ValueError("Force download failed due to the above error.") from exc

    monkeypatch.setattr(huggingface_hub, "snapshot_download", failing)
    with pytest.raises(RunTestsError) as caught:
        fetch_cache.hub_downloader(
            "org/repo", "a" * 40, ["*/chrom_manifest.json"], tmp_path
        )
    assert caught.value.code is Exit.INCOMPLETE
    assert "Hub unreachable" in str(caught.value) and "timed out" in str(caught.value)

    def other(*_a: object, **_kw: object) -> None:
        raise ValueError("not a transport failure")

    monkeypatch.setattr(huggingface_hub, "snapshot_download", other)
    with pytest.raises(
        ValueError
    ):  # positive control: unrelated ValueErrors still surface
        fetch_cache.hub_downloader("org/repo", "a" * 40, ["*"], tmp_path)
