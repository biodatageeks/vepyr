"""``bless`` — produce and re-check the VEP 116.2 oracle file of a data-test directory.

Entry point: ``./bless`` at the repository root (``python -m bless``). The
package is split by concern:

* :mod:`bless.testdir` — reading ``test.toml``, body md5, writing metadata back;
* :mod:`bless.ensembl` — the release-116 cache/FASTA: completeness checks and the
  checksum-verified download from Ensembl's public FTP;
* :mod:`bless.vep` — the pinned docker image and the one fixed VEP command;
* :mod:`bless.cli` — argument handling and the three modes (bless, check,
  check + reproduce).
"""


class BlessError(RuntimeError):
    """A failure the user can act on; ``str(exc)`` is the one-line stderr message."""


__all__ = ["BlessError"]
