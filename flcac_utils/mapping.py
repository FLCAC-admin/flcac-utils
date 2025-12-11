"""
Mapping functions
"""

import pandas as pd
import numpy as np

from esupy.util import make_uuid
from flcac_utils.util import extract_flows, extract_processes

def prepare_tech_flow_mappings(df, auth=False):
    """
    Prepares data objects from a technosphere flow mapping file

    :param df: technosphere flow mapping file see format_specs/tech_mapping.md
    :param auth: bool, if authorized access to FLCAC is required set to True

    Returns:
        flow_dict: dict where the key is the source flow name and the value is a
            dictionary of data on the target flow with the following keys:
                BRIDGE
                name
                provider
                repo
                conversion
                unit
                bridge_flow_name
                id
        flow_objs: dict where the key is the soure flow name and the value is the
            olca Flow object extracted from the FLCAC
        provider_dict: dict where the key is the...
    """
    ## Identify mappings for technosphere flows
    df = df.replace(np.nan, None)
    flow_dict = {row['SourceFlowName']:
        {'BRIDGE': False if pd.isna(row.get('Bridge')) else row.get('Bridge'),
         'bridge_flow_name': row['BridgeFlowName'] if row.get('BridgeFlowName') else np.nan,
         'name': row['TargetFlowName'],
         'provider': row.get('Provider') if not row.get('Bridge') else np.nan,
         'repo': np.nan if pd.isna(row['TargetRepoName']) else {row['TargetRepoName']: row['TargetFlowName']},
         'conversion': row['ConversionFactor'],
         'unit': row['TargetUnit']} for _, row in df.iterrows()}
        ## swap the flow names for bridge processes?
    
    ## extract flow objects in flow_dict from commons via API
    f_dict = {}
    p_dict = {}
    for k, v in flow_dict.items():
        if ('repo' in v) and (not pd.isna(v.get('repo'))):
            repo = list(v.get('repo').keys())[0]
            flow = list(v.get('repo').values())[0]
            if repo in f_dict:
                f_dict[repo].extend([flow])
            else:
                f_dict[repo] = [flow]
            if not pd.isna(v['provider']):
                if repo in p_dict:
                    p_dict[repo].extend([v['provider']])
                else:
                    p_dict[repo] = [v['provider']]
    
    flow_objs = extract_flows(f_dict, add_tags=False, auth=auth) # don't add tags, all flows are internal
    provider_dict = extract_processes(p_dict, to_ref=True, auth=auth)
    
    for k, v in flow_dict.items():
        if not flow_dict[k].get('BRIDGE'):
            o = flow_objs.get(v['name'])
            flow_dict[k]['id'] = o.id if o else None
            if not o:
                if pd.isna(flow_dict[k]['repo']):
                    print(f'New flow needed: {v["name"]}.')
                else:
                    print(f'Flow: {v["name"]} not found.')
    
    return (flow_dict, flow_objs, provider_dict)

def apply_tech_flow_mapping(df, flow_dict, flow_objs, provider_dict, cond=None) -> pd.DataFrame:
    """
    Updates the dataframe to implement the tech flow mapping.
    Input data frame must have:
        'name' --> SourceFlowName
        'amount' --> Source amount
    pass condition if desired, e.g., cond = df['FlowName'] != "Not this flow"
    """
    if 'FlowUUID' not in df:
        df['FlowUUID'] = np.nan
    if 'name' not in df:
        raise KeyError("'name' must be in passed dataframe")
    if 'amount' not in df:
        raise KeyError("'amount' must be in passed dataframe")
    if 'unit' not in df:
        raise KeyError("'unit' must be in passed dataframe")

    if not cond:
        cond = df['FlowType'] != "ELEMENTARY_FLOW"

    def get_context(n):
        try:
            return flow_objs.get(n).category
        except AttributeError:
            return ''

    df = (df
        ## Handle bridge processes
           .assign(bridge = lambda x: np.where(cond,
               x['name'].map(
                   {k: True for k, v in flow_dict.items()
                    if v.get('BRIDGE', False)}),
               False))
           .assign(repo = lambda x: np.where(cond,
               x['name'].map(
                   {k: list(v['repo'].keys())[0] for k, v in flow_dict.items()
                    if v.get('BRIDGE', False)}),
               ''))
           .assign(bridge_flow_name = lambda x: np.where(cond,
               x['name'].map(
                   {k: v['bridge_flow_name'] for k, v in flow_dict.items()
                    if v.get('BRIDGE', False)}),
               ''))
           )

    df = (df
          ## Flow mapping and conversions
          .assign(FlowUUID = lambda x: np.where(cond,
                x['name'].map(
                    {k: v['id'] for k, v in flow_dict.items() if 'id' in v})
                    .fillna(x['FlowUUID']),
                x['FlowUUID']))
          .assign(FlowName = lambda x: np.where(cond,
                x['name'].map(
                    {k: v['name'] for k, v in flow_dict.items()}).fillna(x['name']),
                x['name']))
          .assign(Context = lambda x: np.where(cond,
                x['name'].map(
                    {k: get_context(v['name']) for k, v in flow_dict.items()})
                    .fillna(x['Context']),
                x['Context']))
          )
    cond2 = cond * (df['bridge'] != True)
    df = (df
          ## Some modifications don't apply to flows that are bridged
           .assign(unit = lambda x: np.where(cond2, 
                x['name'].map(
                    {k: v.get('unit') for k, v in flow_dict.items()}).fillna(x['unit']),
                x['unit']))
           .assign(conversion = lambda x: np.where(cond2, 
                x['name'].map(
                    {k: v.get('conversion', 1) for k, v in flow_dict.items()})
                    .fillna(1),
                1))
           .assign(amount = lambda x: x['amount'] * x['conversion'])

        ## Handle default providers
           .assign(default_provider_process = lambda x: np.where(cond,
                x['name'].map(
                    {k: v['provider'] for k, v in flow_dict.items() 
                     if not pd.isna(v['provider'])}),
                ''))
           .assign(default_provider = lambda x: np.where(cond,
                x['default_provider_process'].map(
                    {k: v.id for k, v in provider_dict.items()}),
                ''))
           )

    ## when the provider is a newly created process, assign that here
    cond3 = (cond * (df['default_provider'].isna()) * 
             (df['default_provider_process'].isin(df['ProcessName'])))
    df = (df
           .assign(default_provider = lambda x: np.where(cond3,
                x['default_provider_process'].map(
                    dict(zip(df['ProcessName'], df['ProcessID']))),
                x['default_provider']))
           )

    df = (df
          ## Make some special adjustments to providers for bridge flows
           .assign(default_provider_process = lambda x: np.where(x['bridge'] == True,
                x.apply(
                    lambda z: create_bridge_name(z['repo'], z['bridge_flow_name']), axis=1),
                x['default_provider_process']))
           .assign(default_provider = lambda x: np.where(x['bridge'] == True,
                x['default_provider_process'].apply(make_uuid),
                x['default_provider']))
           .assign(FlowName = lambda x: np.where(x['bridge'] == True,
                x['bridge_flow_name'], x['FlowName']))
           .assign(FlowUUID = lambda x: np.where(x['bridge'] == True,
                x['FlowName'].apply(make_uuid),
                x['FlowUUID']))
           )

    return df.drop(columns=['conversion'])

def create_bridge_name(repo, flowname):
    if repo == 'USLCI':
        return f'{flowname} - PROXY'
    else:
        return f'{flowname} BRIDGE, USLCI to {repo}'

def create_bridge_category(repo, flowname):
    if repo == 'USLCI':
        return 'Proxy Processes'
    else:
        return f'Bridge Processes / USLCI to {repo}'


def create_bridge_processes(df, flow_dict, flow_objs):
    """ Builds bridge processes to facilitate technosphere flow mapping"""
    flow_dict1 = {k: v for k, v in flow_dict.items() if v.get('BRIDGE', False)}
    if 'bridge' not in df:
        return pd.DataFrame()
    if len(df.query('bridge == True')) == 0:
        return pd.DataFrame()
    df_bridge = (df
                 .query('bridge == True')
                 .drop_duplicates(subset = 'FlowName')
                 .drop(columns=['default_provider', 'default_provider_process'], errors='ignore')
                 .reset_index(drop=True)
                 .assign(amount = 1)
                 .assign(ProcessCategory = lambda x: x.apply(
                     lambda z: create_bridge_category(z['repo'], z['bridge_flow_name']), axis=1))
                 .assign(ProcessName = lambda x: x.apply(
                     lambda z: create_bridge_name(z['repo'], z['bridge_flow_name']), axis=1))
                 .assign(FlowName = lambda x: x['bridge_flow_name'])
                 .assign(ProcessID = lambda x: x['ProcessName'].apply(make_uuid))
                 # ^ need more args passed to UUID to avoid duplicates?
                 )
    df_bridge = (pd.concat([
            df_bridge
                .assign(reference = lambda x: ~x['reference'])
                .assign(IsInput = lambda x: ~x['IsInput'])
                .assign(FlowUUID = lambda x: x['FlowName'].apply(make_uuid)),
            # ^ first chunk is for new flows
            df_bridge
               .assign(FlowName = lambda x: x['name'].map(
                   {k: v.get('name', 'ERROR') for k, v in flow_dict1.items()}))
               .assign(FlowUUID = lambda x: x['name'].map(
                   {k: flow_objs.get(v['name']).id for k, v in flow_dict1.items()}))
               .assign(unit = lambda x: x['name'].map(
                   {k: v.get('unit') for k, v in flow_dict1.items()}))
               .assign(conversion = lambda x: x['name'].map(
                   {k: v.get('conversion', 1) for k, v in flow_dict1.items()}))
               .assign(amount = lambda x: x['amount'] * x['conversion'])
               # ^ apply unit conversion
               .drop(columns=['conversion'])
               .assign(Tag = lambda x: x['name'].map(
                   {k: list(v.get('repo').keys())[0] for k, v in flow_dict1.items()}))
             # ^ second chunk is for bridged flows

             ## TODO Need to add default providers for these when they are bridges WITHIN
             # a database? Would be nice, but not required
            ], ignore_index=True)
            .drop(columns=['bridge'])
        )
    return df_bridge
