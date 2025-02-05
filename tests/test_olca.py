"""
Test generation of objects using simple electricity dataframe
"""

import pandas as pd
from pathlib import Path
import yaml

parent_path = Path(__file__).parent

df_olca = pd.read_csv(parent_path / 'test_electricity.csv')

#%% Assign locations to processes
# pull location objects from esupy
from esupy.location import extract_coordinates

geo_json = extract_coordinates(group='countries')

locations = dict((k, geo_json[k]) for k in df_olca['location'] if not pd.isnull(k))

#%% Build supporting objects
from flcac_utils.util import extract_actors_from_process_meta, \
    extract_sources_from_process_meta
from flcac_utils.generate_processes import build_location_dict

with open(parent_path / 'process_metadata.yaml') as f:
    process_meta = yaml.safe_load(f)

(process_meta, source_objs) = extract_sources_from_process_meta(
    process_meta, bib_path = parent_path / 'test.bib')
print(f'Source objects: {len(source_objs)}')
(process_meta, actor_objs) = extract_actors_from_process_meta(process_meta)
print(f'Actor objects: {len(actor_objs)}')

# generate dictionary of location objects
location_objs = build_location_dict(df_olca, locations)
print(f'Location objects: {len(location_objs)}')

#%% Create json file
from flcac_utils.generate_processes import build_flow_dict, \
    build_process_dict, write_objects, validate_exchange_data

validate_exchange_data(df_olca)
flows, new_flows = build_flow_dict(df_olca)
processes = build_process_dict(df_olca,
                               flows,
                               meta=process_meta,
                               loc_objs=location_objs,
                               source_objs=source_objs,
                               actor_objs=actor_objs,
                               )

write_objects('test_electricity', flows, new_flows, processes,
              location_objs, source_objs, actor_objs,
              )
