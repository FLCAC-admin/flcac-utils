"""
Unit tests for flcac_utils.generate_processes (no network, no golden zips).

Full JSON-LD smoke tests stay in test_olca.py; keep fast tests here for CI.
"""

import olca_schema as olca
import pandas as pd
import pytest

from flcac_utils.generate_processes import make_exchanges


def _minimal_flow(flow_uuid: str) -> olca.Flow:
    f = olca.Flow()
    f.id = flow_uuid
    f.name = "Test flow"
    return f


@pytest.fixture
def process_name() -> str:
    return "Unit test process"


@pytest.fixture
def flow_uuid() -> str:
    return "11111111-1111-1111-1111-111111111111"


def test_make_exchanges_description_nan_becomes_empty_string(process_name, flow_uuid):
    """Missing / NaN exchange descriptions must serialize as '', not null."""
    flows = {flow_uuid: _minimal_flow(flow_uuid)}
    p = olca.Process()
    p.name = process_name

    df = pd.DataFrame(
        [
            {
                "ProcessName": process_name,
                "FlowUUID": flow_uuid,
                "reference": False,
                "IsInput": True,
                "amount": 1.0,
                "unit": "kg",
                "description": float("nan"),
                "FlowType": "ELEMENTARY_FLOW",
            }
        ]
    )
    make_exchanges(p, df, flows)
    assert p.exchanges[0].description == ""


def test_make_exchanges_description_none_becomes_empty_string(process_name, flow_uuid):
    flows = {flow_uuid: _minimal_flow(flow_uuid)}
    p = olca.Process()
    p.name = process_name

    df = pd.DataFrame(
        [
            {
                "ProcessName": process_name,
                "FlowUUID": flow_uuid,
                "reference": False,
                "IsInput": True,
                "amount": 1.0,
                "unit": "kg",
                "description": None,
                "FlowType": "ELEMENTARY_FLOW",
            }
        ]
    )
    make_exchanges(p, df, flows)
    assert p.exchanges[0].description == ""


def test_make_exchanges_description_non_null_preserved(process_name, flow_uuid):
    flows = {flow_uuid: _minimal_flow(flow_uuid)}
    p = olca.Process()
    p.name = process_name

    df = pd.DataFrame(
        [
            {
                "ProcessName": process_name,
                "FlowUUID": flow_uuid,
                "reference": False,
                "IsInput": True,
                "amount": 1.0,
                "unit": "kg",
                "description": "emission note",
                "FlowType": "ELEMENTARY_FLOW",
            }
        ]
    )
    make_exchanges(p, df, flows)
    assert p.exchanges[0].description == "emission note"


def test_make_exchanges_avoided_product_nan_is_false(process_name, flow_uuid):
    """Blank cells become NaN; must not set is_avoided_product True (gh #20)."""
    flows = {flow_uuid: _minimal_flow(flow_uuid)}
    p = olca.Process()
    p.name = process_name
    df = pd.DataFrame(
        [
            {
                "ProcessName": process_name,
                "FlowUUID": flow_uuid,
                "reference": False,
                "IsInput": True,
                "amount": 1.0,
                "unit": "kg",
                "description": "",
                "FlowType": "ELEMENTARY_FLOW",
                "avoided_product": float("nan"),
            }
        ]
    )
    make_exchanges(p, df, flows)
    assert p.exchanges[0].is_avoided_product is False
