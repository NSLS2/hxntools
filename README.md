hxntools
========
Python library of tools used at NSLS-II's Hard X-ray Nanoprobe (HXN, 03id) beamline

Development
-----------

Install the repository hooks:

```console
uv run --locked --only-group dev pre-commit install
```

Run every hook without making a commit:

```console
uv run --locked --only-group dev pre-commit run --all-files
```

Ruff formatting runs on every commit. The legacy `src/` tree is temporarily excluded from both Ruff checks and formatting; narrow or remove that exclusion as files are adopted.
