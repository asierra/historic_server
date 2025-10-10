Continuous Integration (CI)

This repository includes a GitHub Actions workflow to run automated tests on every push and pull request.

What is tested
- API and simulator behavior (test_api.py, test_simulator_sources_behavior.py)
- Real Recover behavior without external IO (test_recover_behavior.py with mocked S3)

How to run locally
- Create a virtualenv and install dependencies:
  - python -m venv .venv
  - source .venv/bin/activate
  - pip install -r requirements.txt

- Run all tests:
  - pytest -q

- Run only simulator behavior tests:
  - pytest -q test_simulator_sources_behavior.py

- Run only recover behavior tests:
  - pytest -q test_recover_behavior.py

CI workflow location
- .github/workflows/ci.yml

Notes
- The CI uses Python 3.11 on ubuntu-latest.
- PROCESSOR_MODE=simulador is set for simulator tests.
- Recover tests do not perform network IO; S3 calls are mocked.
