# ParallelX

`set -euo pipefail`

---

## 1) Go to project folder (adjust if you're already there)

`cd "ParallelX"`

---

## 2) Create + activate a fresh venv

`rm -rf .venv; python3 -m venv .venv 2>/dev/null || python -m venv .venv; source .venv/bin/activate 2>/dev/null || source .venv/Scripts/activate`

---

## 3) Install deps (incl dev deps for tests)

`python -m pip install -U pip; python -m pip install -e ".[dev]"`

---

## 4) Run tests

`python -m unittest discover -s tests -p 'test*.py' -v`

---

## 5) Generate sample data

`python examples/scripts/generate_sample_text.py`

---

## 6) Run workflow demos

`mkdir -p .cache; parallelx run examples/workflows/montecarlo_pi.json --max-workers 4 --cache-dir .cache; parallelx run examples/workflows/wordcount.json --max-workers 4 --tag-limits io=2,cpu=4 --cache-dir .cache`

---

## 7) View output

`cat examples/data/wordcount_out.json`

---
