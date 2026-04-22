# -*- coding: utf-8 -*-
"""
Integration test: extract a flow from FLCAC (public API), merge into a small
exchange table, write olca zip. Requires network. No code runs at import time.
"""

from pathlib import Path

import pandas as pd
import pytest

parent_path = Path(__file__).parent


@pytest.fixture
def df_olca() -> pd.DataFrame:
    return pd.read_csv(parent_path / "flow_unit_test.csv")


@pytest.mark.network
def test_flow_extract_write_zip(df_olca, tmp_path):
    """Build processes from flow_unit_test.csv and write zip under tmp_path."""
    from flcac_utils.generate_processes import (
        build_flow_dict,
        build_process_dict,
        validate_exchange_data,
        write_objects,
    )
    from flcac_utils.util import extract_flows

    flow_dict = extract_flows(
        {"USLCI": ["Diesel, dispensed at pump"]},
        add_tags=False,
        auth=False,
    )

    validate_exchange_data(df_olca)
    flows, new_flows = build_flow_dict(df_olca)

    api_flows = {flow.id: flow for k, flow in flow_dict.items()}
    if not (flows.keys() | api_flows.keys()) == flows.keys():
        print("Warning, some flows not consistent")
    else:
        flows.update(api_flows)

    processes = {}
    for s in df_olca["source_type"].unique():
        _df_olca = df_olca.query("source_type == @s")
        p_dict = build_process_dict(_df_olca, flows, meta={})
        processes.update(p_dict)

    write_objects("test", flows, new_flows, processes, out_path=tmp_path)

    zips = list(tmp_path.glob("test_olca2.0_*.zip"))
    assert len(zips) == 1
    assert zips[0].is_file()
