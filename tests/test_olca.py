"""
Integration test: build olca objects from test_electricity.csv and write a zip.

Run with pytest (tmp output) or manually: python tests/test_olca.py
"""

import tempfile
import zipfile
from pathlib import Path

import pandas as pd
import yaml

parent_path = Path(__file__).parent
data_path = parent_path / "data"


def run_electricity_export(out_path: Path) -> Path:
    """
    Load fixtures, validate, build flows/processes, write zip under out_path.

    Returns path_to_written_zip.
    """
    df_olca = pd.read_csv(data_path / "test_electricity.csv")

    from esupy.location import extract_coordinates

    geo_json = extract_coordinates(group="countries")
    locations = {k: geo_json[k] for k in df_olca["location"] if not pd.isnull(k)}

    from flcac_utils.generate_processes import (
        build_flow_dict,
        build_location_dict,
        build_process_dict,
        validate_exchange_data,
        write_objects,
    )
    from flcac_utils.util import (
        extract_actors_from_process_meta,
        extract_sources_from_process_meta,
    )

    with open(data_path / "process_metadata.yaml") as f:
        process_meta = yaml.safe_load(f)

    process_meta, source_objs = extract_sources_from_process_meta(
        process_meta, bib_path=data_path / "test.bib"
    )
    process_meta, actor_objs = extract_actors_from_process_meta(process_meta)

    location_objs = build_location_dict(df_olca, locations)

    validate_exchange_data(df_olca)
    flows, new_flows = build_flow_dict(df_olca)
    processes = build_process_dict(
        df_olca,
        flows,
        meta=process_meta,
        strict_metadata=False,  # Commons actor lookup is incomplete in this fixture
        loc_objs=location_objs,
        source_objs=source_objs,
        actor_objs=actor_objs,
    )

    write_objects(
        "test_electricity",
        flows,
        new_flows,
        processes,
        location_objs,
        source_objs,
        actor_objs,
        out_path=out_path,
    )

    zips = sorted(out_path.glob("test_electricity_olca2.0_*.zip"))
    if not zips:
        raise AssertionError(f"No zip written under {out_path}")
    return zips[-1]


def test_object_build(tmp_path):
    """Two grid processes (AR, GB), one shared product flow; zip in temp dir."""
    zip_path = run_electricity_export(tmp_path)

    assert zip_path.is_file()
    assert zip_path.suffix == ".zip"
    with zipfile.ZipFile(zip_path, "r") as zf:
        names = zf.namelist()
    flow_count = len(
        [n for n in names if n.startswith("flows") and n.endswith(".json")]
    )
    process_count = len(
        [n for n in names if n.startswith("process") and n.endswith(".json")]
    )
    source_count = len(
        [n for n in names if n.startswith("source") and n.endswith(".json")]
    )
    actor_count = len(
        [n for n in names if n.startswith("actor") and n.endswith(".json")]
    )
    assert flow_count == 1, "Expected 1 flow object in output zip"
    assert process_count == 2, "Expected 2 process objects in output zip"
    assert source_count == 3, "Expected 3 source objects in output zip"
    assert actor_count == 2, "Expected 2 actor objects in output zip"


if __name__ == "__main__":
    # Leave files on disk so you can inspect the zip (remove folder when done).
    out = Path(tempfile.mkdtemp(prefix="flcac_test_olca_"))
    zip_path = run_electricity_export(out)
    print(f"wrote: {zip_path}")
    print(f"output directory: {out}")
