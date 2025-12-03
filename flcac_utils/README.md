# Generating JSON-ld objects from tabular data

The general sequence to building objects is:

```{python}
# Read in the exchange table
df_olca = pd.read_csv('my_exchange_table.csv')

# Extract the process metadata input file
with open('metadata.yaml') as f:
    process_meta = yaml.safe_load(f)

# Generating source, actor, or location objects is not required
(process_meta, source_objs) = extract_sources_from_process_meta(
    process_meta, bib_path = foo)
(process_meta, actor_objs) = extract_actors_from_process_meta(process_meta)

# Pull location objects from esupy
from esupy.location import extract_coordinates
geo_json = extract_coordinates(group='countries')
locations = dict((k, geo_json[k]) for k in df_olca['location'] if not pd.isnull(k))
location_objs = build_location_dict(df_olca, locations)

# Build the flow and process objects, including exchanges
validate_exchange_data(df_olca)
(flows, new_flows) = build_flow_dict(df_olca)
processes = build_process_dict(df_olca, flows, meta=process_meta,
                               loc_objs=location_objs,
                               source_objs=source_objs,
                               actor_objs=actor_objs)

# Write to json
write_objects('my_name', flows, new_flows, processes,
              location_objs, source_objs, actor_objs)
```

The input dataframe `df_olca` should conform to the exchange and process table [format specs](/format_specs/exchanges.md).
Process metadata is written in yaml, an example can be found for [electricity](/tests/process_metadata.yaml).

Enabled features include:

- If needed, apply [elementary flow mapping using esupy](https://github.com/USEPA/esupy/blob/main/esupy/mapping.py))

```{python}
kwargs = {}
kwargs['material_crosswalk'] = (parent_path / 'elementary_mapping.csv')
## ^^ hack to pass a local mapping file

mapped_df = apply_flow_mapping(
    df=df, source=None, flow_type='ELEMENTARY_FLOW',
    keep_unmapped_rows=True, ignore_source_name=True,
    field_dict = {
        'SourceName': '',
        'FlowableName': 'FlowName',
        'FlowableUnit': 'unit',
        'FlowableContext': 'Context',
        'FlowableQuantity': 'amount',
        'UUID': 'FlowUUID'},
    **kwargs
    )
```
- If needed, apply technosphere flow mapping using a mapping file per the
[format specs](/format_specs/tech_mapping.md).
Bridge processes can be generated using `build_process_dict()` and written alongside
other objects in `write_objects()`

```{python}
from flcac_utils.mapping import prepare_tech_flow_mappings

## Identify mappings for technosphere flows
flow_dict, flow_objs, provider_dict = prepare_tech_flow_mappings(
    pd.read_csv(parent_path / 'tech_mapping.csv'), auth=False)

#%% Apply tech flow mapping
from flcac_utils.mapping import apply_tech_flow_mapping, create_bridge_processes
df_olca = apply_tech_flow_mapping(df_olca, flow_dict, flow_objs, provider_dict)
df_bridge = create_bridge_processes(df_olca, flow_dict, flow_objs)
```

- Actors can be referenced and assigned directly from the commons via the API using
the following syntax in the metadata file, where the key is the repo, and the values is the object name.
To create a new actor object, use the key `_NEW`

```{yaml}
data_documentor:
    USLCI: 'ERG'
    _NEW:
        name: 'Actor Name'
        email: ''
        description: ''
```

- Sources can be generated if made available as [bibtex file](/tests/test.bib),
using the following syntax, where the bibid is the key and the openLCA name is the value:

```{yaml}
sources:
 - ember_climate_yearly_2024: Ember Climate 1
 - ember_climate_data_2022: Ember Climate 2
```

- Locations can be assigned to processes using two-digit `location` code.
Lcation objects generated match those used by default in openLCA
