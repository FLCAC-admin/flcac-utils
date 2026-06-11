"""
Supporting functions
"""

import math
import zipfile
from datetime import datetime, time, timezone
from pathlib import Path

import esupy.bibtex
import olca_schema as o
import pandas as pd
from esupy.location import extract_coordinates
from esupy.util import make_uuid

from flcac_utils.commons_api import get_single_object, read_commons_data
from flcac_utils.meta_coerce import as_lookup_str, coerce_calendar_year


def norm_uuid(value: object) -> str:
    """Return a lowercase, stripped string id, or empty string if missing."""
    if value is None:
        return ""
    return str(value).strip().lower()


def assign_year_to_meta(meta, year1, year2=None):
    y1 = coerce_calendar_year(year1)
    y2 = coerce_calendar_year(year2 if year2 is not None else year1)
    meta["valid_from"] = datetime(y1, 1, 1).isoformat(timespec="seconds")
    meta["valid_until"] = datetime(y2, 12, 31).isoformat(timespec="seconds")
    return meta


def _set_base_attributes(entity, name: str):
    """Sets base attributes for new flows."""
    if (entity.id is None) or (entity.id == ""):
        entity.id = make_uuid(name)
    if entity.name is None:
        entity.name = name
    # entity.version = '00.00.001'
    # set to noon local time
    entity.last_change = (
        datetime.combine(datetime.now(timezone.utc).date(), time(12)).isoformat() + "Z"
    )
    return entity


def format_dqi_score(dqi_dict):
    """generates a string in the form of "(1;2;3;2;2)"
    from a passed dictionary:
        Flow reliability:
          score: 2
        Temporal correlation:
          score: 1
    """
    dqi = ";".join([str(v.get("score", "")) for k, v in dqi_dict.items()])
    return f"({dqi})"


def generate_locations_from_exchange_df(df):
    """generates a ditionary of location objects in the form of
    {'US': <geojson>} using 2-digit ISO codes
    df must contain the 'location' column
    """
    if "location" not in df.columns:
        raise KeyError(
            '"location" must be present in the dataframe to generate location objects'
        )
    geo_json = extract_coordinates(group="countries")
    locations = dict(
        (k, geo_json[k]) for k in df["location"].unique() if not pd.isnull(k)
    )
    return locations


def increment_dqi_value(s: str, pos: int) -> str:
    """Increments the dqi string at pos by 1.
    pos is 1-based index"""
    # Split the dqi string into a list of integers
    s = s.strip("()")
    numbers = list(map(int, s.split(";")))

    # Increment the value at position N
    if 1 <= pos <= len(numbers):
        numbers[pos - 1] += 1
    else:
        raise IndexError("Position is out of range")

    # Join the list back into a comma-separated string
    dqi = ";".join(map(str, numbers))
    return f"({dqi})"


def extract_actors_from_process_meta(process_meta: dict, **kwargs) -> (dict, dict):
    """
    Based on a metadata file, for all potential metadata fields which are actors,
    extract the actors by name via API calls.
    Metadata fields must be in the format of {'Repo label': 'Actor.name'}
    To generate a new actor object use the key "_NEW"
    Process metadata file is modified and returned to leave only the actor name

    Returns a dictionary of {'Actor.name': olca.Actor}
    """
    print("Identifying actors from metadata")
    actor_list = []
    new_actors = []
    for field in ("data_set_owner", "data_generator", "data_documentor"):
        actor_dict = process_meta.get(field, "")
        if actor_dict in (None, ""):
            continue
        elif (isinstance(actor_dict, dict)) and (list(actor_dict.keys())[0] == "_NEW"):
            d = list(actor_dict.values())[0]
            if d not in new_actors:
                new_actors.append(d)
            process_meta[field] = as_lookup_str(d.get("name"))
        elif isinstance(actor_dict, dict):
            d = actor_dict.copy()
            if d not in actor_list:
                actor_list.append(d)
            process_meta[field] = as_lookup_str(list(actor_dict.values())[0])
        else:
            raise ValueError(
                " ".join(
                    [
                        f"{field} must be a dictionary. For new actors, assign the ",
                        "key as '_NEW'",
                    ]
                )
            )
    actor_dict = {}
    for a in actor_list:
        # Reformat all identified actors by repository into a list
        repo = list(a.keys())[0]
        actor = list(a.values())[0]
        if repo in actor_dict:
            actor_dict[repo]["ACTORS"].append(actor)
        else:
            actor_dict[repo] = {"ACTORS": [actor]}
    actor_objs = {}
    if actor_dict:
        # Extract actors from API, recreate dictionary in correct format
        actors = read_commons_data(actor_dict, auth=kwargs.get("auth", False))
        for repo, a_list in actors.items():
            actor_objs.update({a.name: a for a in a_list})
    requested_actors = [list(a.values())[0] for a in actor_list]
    missing_actors = sorted(set(requested_actors) - set(actor_objs.keys()))
    if missing_actors:
        print("WARNING: not all actors found. Missing: " + ", ".join(missing_actors))
    # Generate and append new actor objs
    for d in new_actors:
        a = o.Actor.from_dict(d)
        a = _set_base_attributes(a, name=a.name)
        actor_objs[d.get("name")] = a
    return process_meta, actor_objs


def extract_sources_from_process_meta(
    process_meta: dict, bib_path: Path
) -> (dict, dict):
    """
    Based on a metadata file, for all potential metadata fields which are sources,
    generate the source objects from .bibtex file from bib_path.
    Metadata fields must be in the format of {'bib_id': 'Source.name'}
    Process metadata file is modified and returned to leave only the source name

    Returns a dictionary of {'Source.name': olca.Source}
    """
    print("Identifying sources from metadata")
    all_source_dict = {}
    for field in ("sources", "publication"):
        source_dict = process_meta.get(field, "")
        if source_dict in (None, ""):
            continue
        if isinstance(source_dict, dict):
            all_source_dict.update(source_dict)
            process_meta[field] = as_lookup_str(list(source_dict.values())[0])
        elif isinstance(source_dict, list):
            names = []
            for s_dict in source_dict:
                all_source_dict.update(s_dict)
                names.append(as_lookup_str(list(s_dict.values())[0]))
            process_meta[field] = names
    source_list = esupy.bibtex.generate_sources(
        bib_path=bib_path, bibids=all_source_dict
    )
    # rearrange the structure of the dictionary to {name: olca.Source}
    source_objs = {}
    for k in source_list:
        if k.year == "":
            # esupy assigns blank when it needs to be None due to int type
            # see #3, consider direct fix in esupy
            k.year = None
        source_objs[k.name] = k

    return process_meta, source_objs


def extract_dqsystems(dq_dict: dict, **kwargs) -> dict["str", o.DQSystem]:
    """
    :param: dq_dict dictionary that takes the form of
        {'Process' : {<repo>: <dq.Name>},
         'Flow' : {<repo>: <dq.Name>}}
    Returns a dictionary of {'Process': o.DQSystem,
                             'Flow': o.DQSystem}
    """
    print("Extracting dqSystems")
    api_dict = {}
    for t, repo_dict in dq_dict.items():
        repo = list(repo_dict.keys())[0]
        if "DQ_SYSTEM" in api_dict.get(repo, {}):
            api_dict[repo]["DQ_SYSTEM"].append(list(repo_dict.values())[0])
        else:
            api_dict[repo] = {"DQ_SYSTEM": [list(repo_dict.values())[0]]}

    # api_dict = {'Federal LCA Commons Core Database': {
    #     'DQ_SYSTEM': ['US EPA - Process Pedigree Matrix',
    #                   'US EPA - Flow Pedigree Matrix']}
    #     }
    # Extract dq_systems from API, recreate dictionary in correct format
    dqsystems = read_commons_data(api_dict, auth=kwargs.get("auth", False))
    dq_objs = {}
    for repo, dq_list in dqsystems.items():
        for d in dq_list:
            if d.name == dq_dict.get("Process", {}).get(repo):
                dq_objs["Process"] = d
            if d.name == dq_dict.get("Flow", {}).get(repo):
                dq_objs["Flow"] = d
    # if len(dq_list) != len(dq_objs):
    #     print('WARNING: not all actors found')
    return dq_objs


def extract_flows(flow_dict: dict, add_tags=False, **kwargs) -> dict["str", o.Flow]:
    """
    :param: flow_dict dictionary that takes the form of
        {<repo>: [flow.Name, flow.Name, ...]}
    Returns a dictionary of {'flow.Name': o.Flow}
    """
    print("Extracting flows")
    flow_dict = {k: {"FLOWS": v} for k, v in flow_dict.items()}
    api_flows = read_commons_data(flow_dict, auth=kwargs.get("auth", False))

    # rearrange the structure of the dictionary to {name: olca.Flow}
    flow_objs = {}
    for repo, f_list in api_flows.items():
        for f in f_list:
            if add_tags:
                f.tags = [repo]
            flow_objs[f.name] = f
    return flow_objs


def extract_processes(
    process_dict: dict, to_ref=False, **kwargs
) -> dict["str", o.Process]:
    """
    :param: process_dict dictionary that takes the form of
        {<repo>: [process.Name, process.Name, ...]}
    Returns a dictionary of {'process.Name': o.Process}
    """
    print("Extracting processes")
    process_dict = {k: {"PROCESS": v} for k, v in process_dict.items()}
    api_processes = read_commons_data(process_dict, auth=kwargs.get("auth", False))

    # rearrange the structure of the dictionary to {name: olca.Process}
    process_objs = {}
    for repo, p_list in api_processes.items():
        for p in p_list:
            if to_ref:
                p = p.to_ref()
            process_objs[p.name] = p
    return process_objs


def extract_bridge_process(tgt_name, repo):
    """
    Special handling of using exisiting bridge process in another repo.
    Extracts the bridge process object and the input flow object

    :param: tgt_name str name of the bridge process
    :param: repo str name of the source repo
    Returns a tuple of the o.Process, o.Flow
    """
    b = extract_processes({repo: tgt_name}, to_ref=False, auth=False)
    for e in b[tgt_name].exchanges:
        if e.is_input:
            input_flow = e.flow.id
            break
    f = get_single_object(repo, "FLOW", input_flow)
    return (b, f)


def zero_pad_version(
    s: str, widths: tuple[int, ...] = (2, 2, 3), sep: str = "."
) -> str:
    """
    Convert a dotted numeric string like "1.0", "1.1", or "1.2.1" into a fixed-width
    dotted format with leading zeros (default "XX.XX.XXX" -> widths (2,2,3)).

    Rules:
      - Split on `sep`.
      - Missing segments are treated as 0.
      - Extra segments beyond `widths` raise ValueError.
      - Each segment must be an integer-like token (e.g., "01", "2"); empty
        tokens invalid.

    Parameters
    ----------
    s:
        Input dotted string.
    widths:
        Target width for each segment (default (2, 2, 3)).
    sep:
        Segment separator (default ".").

    Returns
    -------
    str
        Zero-padded version string with exactly len(widths) segments.

    Examples
    --------
    >>> zero_pad_version("1.0")
    '01.00.000'
    >>> zero_pad_version("1.1")
    '01.01.000'
    >>> zero_pad_version("1.2.1")
    '01.02.001'
    """
    if s is None:
        raise ValueError("Input cannot be None")

    parts = s.strip().split(sep) if s.strip() != "" else []
    if any(p == "" for p in parts):
        raise ValueError(f"Invalid version string (empty segment): {s!r}")

    if len(parts) > len(widths):
        raise ValueError(f"Too many segments in {s!r}; expected at most {len(widths)}")

    # Parse existing parts as integers
    nums: list[int] = []
    for p in parts:
        try:
            n = int(p)
        except ValueError as e:
            raise ValueError(f"Non-integer segment {p!r} in {s!r}") from e
        if n < 0:
            raise ValueError(f"Negative segment {p!r} in {s!r}")
        nums.append(n)

    # Fill missing segments with zeros
    nums.extend([0] * (len(widths) - len(nums)))

    # Format with zero padding; enforce that segment fits in width
    out = []
    for n, w in zip(nums, widths):
        if n >= 10**w:
            raise ValueError(f"Segment {n} does not fit in width {w} for {s!r}")
        out.append(f"{n:0{w}d}")

    return sep.join(out)


def round_to_sig_figs(number, sig_figs):
    """
    Rounds a number to a specified number of significant figures.

    Args:
        number (float or int): The number to round.
        sig_figs (int): The desired number of significant figures.

    Returns:
        float: The number rounded to the specified significant figures.
    """
    if number == 0:
        return 0.0  # Handle zero separately

    # Calculate the order of magnitude
    magnitude = int(math.floor(math.log10(abs(number))))

    # Determine the rounding position relative to the decimal point
    # For example, if sig_figs is 3 and magnitude is 2 (e.g., for 1234),
    # then we want to round to -1 decimal places (tens place)
    # If magnitude is -2 (e.g., for 0.00123), and sig_figs is 3,
    # then we want to round to 1 decimal place (thousands place)
    decimal_places = sig_figs - 1 - magnitude

    return round(number, decimal_places)


if __name__ == "__main__":
    dq_dict = {
        "Process": {
            "Federal LCA Commons Core Database": "US EPA - Process Pedigree Matrix"
        },
        "Flow": {"Federal LCA Commons Core Database": "US EPA - Flow Pedigree Matrix"},
    }
    dq_objs = extract_dqsystems(dq_dict)
    flow_dict = {
        "US Electricity Baseline": {"FLOWS": ["Electricity, AC, 120 V"]},
        "Heavy equipment operation": {
            "FLOWS": [
                "Diesel; dispensed at pump",
                "Compressed natural gas; dispensed at pump",
                "Gasoline; dispensed at pump",
            ]
        },
    }
    flow_dict = extract_flows(flow_dict)


def extract_latest_zip(
    fpath_zip: Path,
    working_dir: Path,
    output_folder_name: str | None = None,
    overwrite: bool = True,
    delete_zip: bool = False,
) -> Path:
    """
    Extract the most recently created ZIP from a directory (or a single ZIP file).

    Optionally delete the ZIP after extraction. Place output inside
    ``working_dir``.

    Args:
        fpath_zip (Path): ZIP file or directory containing ZIP files.
        working_dir (Path): Where extracted files will be placed.
        output_folder_name (str | None): Output folder name; default is ZIP stem.
        overwrite (bool): Whether to overwrite existing files. Defaults to True.

    Returns:
        Path to the directory where files were extracted.
    """
    if not fpath_zip.exists():
        raise FileNotFoundError(f"Path not found: {fpath_zip}")

    if not working_dir.exists():
        working_dir.mkdir(parents=True)

    # Determine the ZIP file to extract
    if fpath_zip.is_dir():
        zip_files = list(fpath_zip.glob("*.zip"))
        if not zip_files:
            raise FileNotFoundError(f"No ZIP files found in directory: {fpath_zip}")
        latest_zip = max(zip_files, key=lambda z: z.stat().st_ctime)
    else:
        latest_zip = fpath_zip

    # Decide output folder name
    if output_folder_name:
        output_folder = working_dir / output_folder_name
    else:
        output_folder = working_dir / latest_zip.stem

    output_folder.mkdir(parents=True, exist_ok=True)

    try:
        with zipfile.ZipFile(latest_zip, "r") as archive:
            if not overwrite:
                existing_files = [
                    output_folder / name
                    for name in archive.namelist()
                    if (output_folder / name).exists()
                ]
                if existing_files:
                    print(f"Skipping extraction; files already exist: {existing_files}")
                    return output_folder
            archive.extractall(output_folder)
    except zipfile.BadZipFile:
        raise ValueError(f"Invalid ZIP file: {latest_zip}")

    if delete_zip:
        latest_zip.unlink()

    print(f"Extracted files from {latest_zip.name} to {output_folder}")
    return output_folder
