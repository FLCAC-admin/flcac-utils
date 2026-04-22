"""
Integration checks for prepare_tech_flow_mappings using fixture CSVs.

CSV layout matches technosphere mapping files; see format_specs/tech_mapping.md.
"""

from pathlib import Path

import pandas as pd
import pytest

from flcac_utils.mapping import prepare_tech_flow_mappings

_FIXTURE_DIR = Path(__file__).resolve().parent / "data"


@pytest.mark.network
def test_prepare_tech_flow_mappings_valid_provider_csv():
    """USLCI fuel rows plus US EB 120 V row with at-user mix."""
    df = pd.read_csv(_FIXTURE_DIR / "tech_mapping_providers_valid.csv")
    prepare_tech_flow_mappings(df, auth=False)


@pytest.mark.network
def test_prepare_tech_flow_mappings_invalid_provider_raises():
    """120 V electricity flow with a grid mix provider that is not that product."""
    df = pd.read_csv(_FIXTURE_DIR / "tech_mapping_providers_invalid_electricity.csv")
    with pytest.raises(ValueError, match="does not supply"):
        prepare_tech_flow_mappings(df, auth=False)
