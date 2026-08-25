Continuous Integration (CI)

This repository includes a GitHub Actions workflow to run automated tests on every push and pull request.

What is tested
- The whole suite under `tests/`, minus anything marked `real_io`.
- Tests are selected by marker, not by file list: `pytest -q -m "not real_io"`.
  Do not go back to naming files in the workflow — the previous version did, and
  it kept invoking paths at the repo root long after the tests moved into
  `tests/`, so CI was red (and unread) from 2331cc6 (2025-10-31) onwards.

What is NOT tested
- Tests marked `@pytest.mark.real_io` (currently the two S3 fallback integration
  tests in `tests/test_api.py`). They hit the real NOAA bucket and real disk.

How to run locally
- Create a virtualenv and install dependencies:
  - python3 -m venv .venv
  - source .venv/bin/activate
  - pip install -r requirements.txt   # pytest-env included; without it the
                                      # `env =` block in pytest.ini is ignored

- Run exactly what CI runs:
  - pytest -q -m "not real_io"        # ~47 tests, ~3 min

- Run everything, real IO included:
  - pytest -q

- Run one module or one test:
  - pytest tests/test_api.py
  - pytest tests/test_circuit_breaker.py::test_health_includes_circuit_breaker_field

CI workflow location
- .github/workflows/ci.yml

Notes
- The CI uses Python 3.11 on ubuntu-latest.
- PROCESSOR_MODE=simulador is set in the workflow as well as in pytest.ini. The
  redundancy is deliberate: if pytest-env were ever missing, pytest drops the
  pytest.ini `env =` block silently and the suite would run against real
  Lustre/S3 instead of the simulator.
- The suite needs no real storage: it passes with SOURCE_PATH and DOWNLOAD_PATH
  pointing at paths that do not exist (verified 2026-08-24), because the
  fixtures monkeypatch them onto temporary directories.
