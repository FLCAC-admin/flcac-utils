# FLCAC-utils

This package supports access of data on the [Federal LCA Commons (FLCAC)](https://www.lcacommons.gov/) and contains functions for writing JSON-ld objects for data submission to the FLCAC.
Format specifications for tabular data used to create unit processes are available [here](/format_specs/exchanges.md).
An examples of using this package for developing on-road transportat unit processes from MOVES can be found in the [uslci-moves](https://github.com/FLCAC-admin/uslci-moves) repository.

## Install `flcac_utils`

Recommend installing editably by cloning the repository, navigating to the directory and using
`pip install -e .`

For tests and linting: `pip install -e ".[dev]"`, then `ruff format flcac_utils tests`, `ruff check flcac_utils tests`, and `pytest`. Style rules and **88-character** line length are in `pyproject.toml`.

