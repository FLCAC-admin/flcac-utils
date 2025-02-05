# Submission Tool back-end code

The general sequence to building objects is:

```{python}
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

The input dataframe `df_olca` should conform to the exchange and process table [format specs](format_specs/exchanges.md).
Process metadata is written in yaml, an example can be found for [electricity](../electricity/electricity_process_metadata.yaml).

Enabled features include:

- Actors can be referenced and assigned directly from the commons via the API using
the following syntax in the metadata file, where the key is the repo, and the values is the object name:

```{yaml}
data_documentor:
    USLCI: 'Franklin Associates, A Division of ERG'
```

- Sources can be generated if made available as [bibtex file](../electricity/electricity.bib),
using the following syntax, where the bibid is the key and the openLCA name is the value:

```{yaml}
sources:
 - ember_climate_yearly_2024: Ember Climate 1
 - ember_climate_data_2022: Ember Climate 2
```

- Locations can be assigned to processes using two-digit `location` code.
Lcation objects generated match those used by default in openLCA
