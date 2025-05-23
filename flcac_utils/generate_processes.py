# requires >= v0.11
# !pip install olca-schema


import olca_schema as olca
import olca_schema.zipio as zipio #for writing to json
import olca_schema.units as units
import datetime
import pandas as pd
from esupy.util import make_uuid
from esupy.location import olca_location_meta
from pathlib import Path


outPath = Path(__file__).parents[1] / 'output'

'''
Exchange schema lists fields that are required for progression of the script
'''
exchange_schema = {
    "ProcessID": {'dtype': 'str', 'required': False},
    "ProcessCategory": {'dtype': 'str', 'required': True},
    "ProcessName": {'dtype': 'str', 'required': True},
    "FlowUUID": {'dtype': 'str', 'required': True},
    "FlowName": {'dtype': 'str', 'required': True},
    "Context":  {'dtype': 'str', 'required': True},
    "IsInput": {'dtype': 'bool', 'required': True},
    "FlowType": {'dtype': 'str', 'required': True},
    "reference":  {'dtype': 'bool', 'required': True},
    "default_provider": {'dtype': 'bool', 'required': False},
    "description": {'dtype': 'str', 'required': False},
    "amount":  {'dtype': 'float', 'required': True},
    "unit":  {'dtype': 'str', 'required': True},
    "avoided_product": {'dtype': 'bool', 'required': False},
    "exchange_dqi": {'dtype': 'str', 'required': False},
    # "tag": {'dtype': 'str', 'required': False}
}

# convert units to units in olca_schema.units
unit_dict = {
    "metric ton": "ton"
}


def _set_base_attributes(
        entity,
        name: str
        ):
    """Sets base attributes for new flows."""
    if entity.id == '':
        entity.id = make_uuid(name)
    if entity.name is None:
        entity.name = name
    #entity.version = '00.00.001'
    entity.last_change = datetime.datetime.utcnow().isoformat() + 'Z'
    return entity


def validate_exchange_data(df):
    """Checks exchange dataframe for validity"""
    reqd = set([k for k, v in exchange_schema.items()
                if v['required'] == True])
    if not reqd.issubset(set(df.columns)):
        print(reqd - set(df.columns))

    for c in reqd:
        if df[c].isna().any():
            raise ValueError(f'ERROR: Missing data in {c}')

    ## validate units align with olca
    x = {u: units.unit_ref(u) for u in set(df['unit'])}
    keys = [k for k,v in x.items() if v is None]
    if keys:
        raise ValueError('Incorrect units present in exchange data: ',
                         f'{", ".join(keys)}')


def get_process_metadata(p: olca.Process,
                         metadata: dict,
                         **kwargs
                         ) -> olca.Process:
    """Generates and attaches process metadata to olca.Process p.
    kwargs may contain "source_objs", "actor_objs" which are dictionaries
    of olca objects with names as keys
    """
    pdoc = olca.ProcessDocumentation()
    for k, v in metadata.items():
        if k in dir(p):
            # some metadata items attach directly to the process
            setattr(p, k, v)
        elif k not in dir(pdoc):
            print(f'WARNING: {k} not a process doc key')
            continue
        elif (v is None) or (len(v) == 0):
            continue  # no metadata to add, skip
        elif k in ('sources', 'publication'):
            if 'source_objs' not in kwargs:
                print('No Sources passed!!')
                continue
            else:
                if k == 'sources':
                    # list of source objects
                    v = [kwargs.get('source_objs').get(s).to_ref() for s in v]
                elif k == 'publication':
                    # single source object
                    v = kwargs.get('source_objs').get(v).to_ref()
        elif k in ('data_set_owner', 'data_generator', 'data_documentor'):
            if 'actor_objs' not in kwargs:
                print('No Actors passed!!')
                continue
            else:
                a = kwargs.get('actor_objs').get(v)
                if a:
                    v = a.to_ref()
                else:
                    print(f'Actor: `{v}` not found!')
                    continue
        elif k in ('reviews'):
            rev_list = []
            for i, r in v.items():
                rev = olca.Review(review_type = r.get('reviewType'),
                                  details = r.get('details'),
                                  )
                if 'report' in r:
                    report = list(r['report'].values())[0]
                    s = kwargs.get('source_objs')
                    s = s.get(report).to_ref() if s else None
                    rev.report = s
            rev_list.append(rev)
            v = rev_list
        setattr(pdoc, k, v)
    if 'creation_date' not in metadata.keys():
        pdoc.creation_date = datetime.datetime.now().isoformat(timespec='seconds')
    p.process_documentation = pdoc
    return p


def make_exchanges(
        p: olca.Process,
        df: pd.DataFrame,
        flows: dict,
        process_db: pd.DataFrame = None
        ) -> olca.Process:
    """
    Creates and attaches exchanges for olca.Process p. Requires flow_dict and
    process_db as reference to other objects available within the database.
    """
    if not process_db:
        process_db = pd.DataFrame()
    exch_lst = []
    for index, row in df.query('ProcessName==@p.name').iterrows():
        e = olca.Exchange()
        e.flow = flows[row['FlowUUID']].to_ref()
        e.is_quantitative_reference = bool(row['reference'])
        e.is_input = bool(row['IsInput'])
        e.amount = row['amount']
        e.description = row.get('description')
        e.is_avoided_product = bool(row.get('avoided_product', False))
        e.unit = units.unit_ref(row['unit'])
        # ^^ needs to be a Ref not a str
        e.flow_property = units.property_ref(row['unit'])
        # ^^ required when it is not the reference flow property of the flow
        if 'exchange_dqi' in row and p.exchange_dq_system is not None:
            e.dq_entry = row['exchange_dqi']
        if 'default_provider' in row and (pd.notna(row['default_provider']) and
                                          row['default_provider'] != ''):
            # Requires identifying the UUID of the default provider, but
            # TODO then how do you assign a provider from wihtin the new data?
            dp = olca.Process()
            dp.id = row['default_provider']
            # dp_row = process_db.loc[process_db['ID'] == row['default_provider']]
            # if len(dp_row) == 0:  # Checks for populated default provider field
            #     if row['default_provider'] in df['ProcessID'].values:
            #         dp.id = make_uuid(row['default_provider'])
            #     else:
            #         print('WARNING: ambiguous default provider')
            # elif len(dp_row) == 1:
            #     dp.id = row['default_provider']
            # else:
            #     print('WARNING: ambiguous default provider')
            e.default_provider = dp.to_ref()
        exch_lst.append(e)
    p.exchanges = exch_lst

    return p


def build_flow_dict(df: pd.DataFrame,
                    tech_flows_db: pd.DataFrame=None
                    ) -> tuple[dict, list]:
    """
    Creates a dictionary of olca.Flow objects with UUID as dictionary key and a
        list of UUIDs indicating which new flows need to be written to JSON.

    :param df: DataFrame of process and exchange data; see exchange_schema
    :param tech_flows_df: DataFrame (optional), provide in order to link to existing
        technosphere flows. DataFrame must contain "FlowUUID" and "FlowName"
    :return: 1) dict of olca.Flow objects with UUID as dictionary key
             2) list of FlowUUIDs (keys) which align with those Flow objects
                that need to be written to JSON.
    """
    # Create dictionary of all flows
    # Flows must exist before exchanges can be created
    # https://greendelta.github.io/olca-ipc.py/olca/index.html#olca.flow_of
    flows = {}
    print('Creating Dictionary of flows')

    ## Attempt to retrieve FEDEFL so that UUIDs of exchange flows can be assessed for
    ## whether they exist in the FEDEFL.
    try:
        import fedelemflowlist
        fl = fedelemflowlist.get_flows()
    except (ImportError, AttributeError):
        print("FEDEFL not available, UUIDs will not be checked")
        fl = None

    new_flows_to_write = []
    for index, row in df.drop_duplicates('FlowUUID').iterrows():
    
        # If flow UUID is neither in FEDEFL or database of technospheric flows
        # then it needs to be created based on user supplied data
        if (fl is not None and (row['FlowUUID'] not in fl['Flow UUID'].values) and
                (tech_flows_db is None or
                    row['FlowUUID'] not in tech_flows_db['UUID'].values)):
    
            print(f'Creating new flow: {row["FlowName"]}')
            flow = olca.Flow()
            if not pd.isna(row['FlowUUID']):
                flow.id = row['FlowUUID']
            else:
                flow.id = ''
            flow = _set_base_attributes(flow, row['FlowName'])
            flow.flow_properties = [olca.FlowPropertyFactor(
                is_ref_flow_property=True,
                conversion_factor=1.0,
                flow_property=units.property_ref(row["unit"]))
            ]
            flow.category = row['Context']
            if row['FlowType'] == 'PRODUCT_FLOW':
                flow.flow_type = olca.FlowType.PRODUCT_FLOW
            elif row['FlowType'] == 'WASTE_FLOW':
                flow.flow_type = olca.FlowType.WASTE_FLOW
            elif row['FlowType'] == 'ELEMENTARY_FLOW':
                flow.flow_type = olca.FlowType.ELEMENTARY_FLOW
            if 'Tag' in row:
                tag = row['Tag']
                if isinstance(tag, str):
                    tag = [tag]
                if isinstance(tag, list):
                    flow.tags = tag
            # To-do: Add check on valid flow type
            flows[flow.id] = flow
            new_flows_to_write.append(flow.id)
    
        # If flow UUID is in the FEDEFL
        elif (fl is not None and (row['FlowUUID'] in fl['Flow UUID'].values)):
            ## don't need full flow metadata will be pulled directly from
            ## fedelemflowlist
            flow = olca.Flow()
            flow.name = fl.query('`Flow UUID` == @row.FlowUUID')['Flowable'].item()
            flow.id = row['FlowUUID']
            flow.flow_type = olca.FlowType.ELEMENTARY_FLOW
            flows[flow.id] = flow
    
        # If flow UUID is in database technospheric flow list
        elif row['FlowUUID'] in tech_flows_db['UUID'].values:
            ## existing technosphere flows are not written to json so full flow
            ## metadata is not needed
            flow = olca.Flow()
            flow.name = tech_flows_db.query('UUID == @row.FlowUUID')['FlowName'].item()
            flow.id = row['FlowUUID']
            if row['FlowType'] == 'PRODUCT_FLOW':
                flow.flow_type = olca.FlowType.PRODUCT_FLOW
            elif row['FlowType'] == 'WASTE_FLOW':
                flow.flow_type = olca.FlowType.WASTE_FLOW
            flows[flow.id] = flow
        else:
            raise ValueError
    return(flows, new_flows_to_write)


def build_process_dict(df: pd.DataFrame,
                       flows: dict[str, olca.Flow],
                       meta: dict[str, str],
                       **kwargs
                       ) -> dict:
    """
    Creates a dictionary of olca.Process objects with UUID as dictionary key.

    :param df: DataFrame of process and exchange data; see exchange_schema
    :param flows: dict of olca.Flow objects with UUID as dictionary key
    :param meta:
    :kwargs:
        loc_objs: dict[str, olca.Location]
        source_objs: dict[str, olca.Source]
        actor_objs: dict[str, olca.Actor]
        dq_objs: dict[str, olca.DQSystem]
    :return: dict of olca.Process objects with UUID as dictionary key
    """
    ## This code block is useful when considering allocation (see AISI work)
    # r_flows = {}  # reference flows list, by process
    # nr_flows = {}  # non-reference flows list, by process
    # for index, row in df.iterrows():
    #     if bool(row['reference']) == True:
    #         if row['ProcessID'] in r_flows.keys():
    #             r_flows[row['ProcessID']].append(flows[row['FlowUUID']])
    #         elif row['ProcessID'] not in r_flows.keys():
    #             r_flows[row['ProcessID']] = []
    #             r_flows[row['ProcessID']].append(flows[row['FlowUUID']])
    #     elif bool(row['reference']) == False:
    #         if row['ProcessID'] in nr_flows.keys():
    #             nr_flows[row['ProcessID']].append(flows[row['FlowUUID']])
    #         elif row['ProcessID'] not in nr_flows.keys():
    #             nr_flows[row['ProcessID']] = []
    #             nr_flows[row['ProcessID']].append(flows[row['FlowUUID']])

    # Create Dictionary of all processes
    # https://greendelta.github.io/olca-ipc.py/olca/schema.html#olca.schema.Process
    processes = {}
    print('Creating Dictionary of processes\n')
    cols = [c for c in ['ProcessID', 'ProcessCategory', 'ProcessName', 'location']
            if c in df.columns]
    for i, row in df[cols].drop_duplicates().iterrows():
        name = row['ProcessName']
        print(name)
        p0 = olca.Process()
        p0 = _set_base_attributes(p0, name)
        # Make sure UUID is always set based on process name so it never changes
        p0.id = make_uuid(name) if 'ProcessID' not in cols else row['ProcessID']
        p0.process_type = olca.ProcessType.UNIT_PROCESS
        p0.category = row['ProcessCategory']
        p0.default_allocation_method = olca.AllocationType.PHYSICAL_ALLOCATION

        if kwargs.get('loc_objs'):
            loc = kwargs['loc_objs'].get(row['location'])
            if loc:
                p0.location = loc.to_ref()

        if kwargs.get('dq_objs'):
            p0.dq_system = kwargs['dq_objs'].get('Process').to_ref()
            p0.exchange_dq_system = kwargs['dq_objs'].get('Flow').to_ref()

        # print('Creating Metadata for Process', p)
        p0 = get_process_metadata(p = p0, metadata = meta, **kwargs)
        print('Creating Exchanges for Process', name)
        p0 = make_exchanges(p = p0, df = df,
                            flows = flows,
                            # process_db = process_db)
                            process_db = None)
        print('\n')
        processes[p0.id] = p0
    return processes


def build_location_dict(df: pd.DataFrame,
                        locations: dict[str, dict]
                        ) -> dict:
    """
    Creates a dictionary of olca.Location objects with ISO-code as dictionary key.

    :param df: DataFrame of process and exchange data; see exchange_schema
    :param locations: dictionary of geoJsons with loc code as key
    :return: dict of olca.Location objects with ISO-code as dictionary key
    """
    loc_objs = {}
    print('Creating dictionary of Locations...')

    loc_meta = olca_location_meta().drop(columns='Category')
    loc_meta.columns = loc_meta.columns.str.lower()
    loc_meta = (loc_meta.rename(columns={'id': '@id'})
                        .set_index('code')
                        .to_dict(orient='index'))
    for loc_code in df['location'].drop_duplicates().dropna():
        if loc_code == '': continue
        meta = loc_meta.get(loc_code)
        properties = locations.get(loc_code, {}).get('properties')
        # consistent with olca v2.0 ref data, update lat/long from ecoinvent geoJSONs
        meta.update({k: properties[k] for k in ['latitude', 'longitude']
                     if (properties and k in properties)})
        loc = olca.Location().from_dict(meta)
        loc.code = loc_code
        loc.geometry = locations.get(loc_code, {}).get('geometry')
        loc_objs[loc_code] = loc
    return loc_objs


def _write_obj(
        file: str,
        obj: dict,
        path: Path = outPath
        ):
    """Creates a zip json from dictionary of olca obj e.g. file = 'json.zip'"""
    with zipio.ZipWriter(path / file) as W:
        for x in obj.values():
            if x.last_change is None:
                x.last_change = datetime.datetime.utcnow().isoformat() + 'Z'
            if x.version is None:
                x.version = '00.00.001'
            W.write(x)


def write_objects(name: str,
                  flows: dict[str, olca.Flow],
                  new_flows_to_write: list,
                  processes: dict[str, olca.Process],
                  *args,
                  out_path=outPath
                  ):
    """
    Writes a collection of objects to json-ld to the out_path

    :param name: str, stub for json-ld filename
    :param flows: dict[UUID, olca.Flow]
    :param new_flows_to_write: list of UUIDs found within flows
    :param processes: dict[UUID, olc.Process]
    :args:
        additional dictionaries of olca objects where values are objects
        for writing to json-ld e.g., Sources, Actors, etc.
    """
    ## Attempt to retrieve FEDEFL so that UUIDs of exchange flows can be assessed for
    ## whether they exist in the FEDEFL.
    try:
        import fedelemflowlist
        fl = fedelemflowlist.get_flows()
    except (ImportError, AttributeError):
        print("FEDEFL not available, UUIDs will not be checked")
        fl = None

    # generate flow lists to write
    flowlist = fl.query('`Flow UUID` in @flows.keys()')
    t_flowlist = {k: v for k, v in flows.items() if k in new_flows_to_write}
    
    # Write JSON -- IMPORT into database with **Units and Flow Properties**
    # Select "Update data sets with newer versions" to replace any created flows
    # and processes, but not any exisiting technosphere flows
    
    timestr = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    json_file = f'{name}_olca2.0_{timestr}.zip'
    
    # Remove existing json (otherwise it gets extended)
    (out_path / json_file).unlink(missing_ok=True)
    # Create output folder if it doesn't exist
    out_path.mkdir(parents=False, exist_ok=True)
    print(f"Writing json to {out_path/json_file}")
    # write flows directly from flow list based on those found in processes
    fedelemflowlist.write_jsonld(flowlist, path=out_path / json_file)
    # write tech flows
    _write_obj(file=json_file, obj=t_flowlist, path=out_path)
    # write processes
    _write_obj(file=json_file, obj=processes, path=out_path)
    # write additional objects as needed
    for a in args:
        _write_obj(file=json_file, obj=a, path=out_path)
