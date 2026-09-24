"""verify.py is a script, not a package module, so it is loaded with importlib
(the same way nf-core-module/stage_testdata.py loads its fixture's prepare.py)
rather than imported as `scripts.verify`.
"""

import importlib.util
import json
import sys
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "verify.py"


def _load_verify():
    spec = importlib.util.spec_from_file_location("pibench_verify_script", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


verify = _load_verify()


def test_update_result_creates_a_new_file(tmp_path):
    path = tmp_path / "verify.json"
    verify.update_result(path, "Q1", {"reference_rows": 3})
    assert json.loads(path.read_text()) == {"Q1": {"reference_rows": 3}}


def test_update_result_keeps_earlier_queries_on_a_later_call(tmp_path):
    path = tmp_path / "verify.json"
    verify.update_result(path, "Q1", {"reference_rows": 3})
    verify.update_result(path, "Q2", {"reference_rows": 5})
    data = json.loads(path.read_text())
    assert data == {
        "Q1": {"reference_rows": 3},
        "Q2": {"reference_rows": 5},
    }


def test_update_result_overwrites_only_the_named_query(tmp_path):
    path = tmp_path / "verify.json"
    verify.update_result(path, "Q1", {"reference_rows": 3})
    verify.update_result(path, "Q2", {"reference_rows": 5})
    verify.update_result(path, "Q1", {"reference_rows": 99})
    data = json.loads(path.read_text())
    assert data["Q1"]["reference_rows"] == 99
    assert data["Q2"]["reference_rows"] == 5


def test_update_result_writes_atomically_and_leaves_no_tmp_file(tmp_path):
    path = tmp_path / "verify.json"
    verify.update_result(path, "Q1", {"reference_rows": 1})
    assert path.exists()
    assert not (tmp_path / "verify.json.tmp").exists()


def test_update_result_returns_the_merged_dict(tmp_path):
    path = tmp_path / "verify.json"
    verify.update_result(path, "Q1", {"reference_rows": 3})
    merged = verify.update_result(path, "Q2", {"reference_rows": 5})
    assert set(merged) == {"Q1", "Q2"}


def test_sh_raises_runtime_error_naming_the_command_and_log(tmp_path):
    import pytest

    log = tmp_path / "logs" / "Q1.fail.stderr.txt"
    cmd = [sys.executable, "-c", "import sys; sys.stderr.write('boom'); sys.exit(3)"]
    with pytest.raises(RuntimeError) as excinfo:
        verify.sh(cmd, log)
    assert "exit 3" in str(excinfo.value)
    assert str(log) in str(excinfo.value)
    assert log.exists()
    assert log.read_text() == "boom"


def test_sh_discards_stdout_and_captures_stderr(tmp_path):
    log = tmp_path / "logs" / "Q1.ok.stderr.txt"
    cmd = [
        sys.executable,
        "-c",
        "import sys; print('to stdout'); sys.stderr.write('to stderr')",
    ]
    verify.sh(cmd, log)
    assert log.read_text() == "to stderr"
