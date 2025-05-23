"""
Mapping functions
"""

import pandas as pd
import numpy as np

from esupy.util import make_uuid
from flcac_utils.util import extract_flows, extract_processes

def prepare_tech_flow_mappings(df, auth=False):
    ## Identify mappings for technosphere flows (fuel inputs)
    fuel_dict = {row['SourceFlowName']:
                      {'BRIDGE': row['Bridge'],
                       'name': row['BridgeFlowName'] if row['BridgeFlowName'] else row['TargetFlowName'],
                       'provider': row['Provider'] if not row['Bridge'] else np.nan,
                       'repo': {row['TargetRepoName']: row['TargetFlowName']},
                       'conversion': row['ConversionFactor'],
                       'unit': row['TargetUnit']} for _, row in df.iterrows()}
                ## swap the flow names for bridge processes?
    
    ## extract fuel objects in fuel_dict from commons via API
    f_dict = {}
    p_dict = {}
    for k, v in fuel_dict.items():
        if 'repo' in v:
            repo = list(v.get('repo').keys())[0]
            flow = list(v.get('repo').values())[0]
            fuel_dict[k]['target_name'] = flow
            if not fuel_dict[k].get('BRIDGE'):
                fuel_dict[k]['name'] = flow
            if repo in f_dict:
                f_dict[repo].extend([flow])
            else:
                f_dict[repo] = [flow]
            if not pd.isna(v['provider']):
                if repo in p_dict:
                    p_dict[repo].extend([v['provider']])
                else:
                    p_dict[repo] = [v['provider']]
    
    flow_dict = extract_flows(f_dict, add_tags=False, auth=auth) # don't add tags, all flows are internal
    provider_dict = extract_processes(p_dict, to_ref=True, auth=auth)
    
    for k, v in fuel_dict.items():
        if not fuel_dict[k].get('BRIDGE'):
            fuel_dict[k]['id'] = flow_dict.get(v['name']).id
        else:
            fuel_dict[k]['id'] = make_uuid(fuel_dict[k].get('name'))
    
    return (fuel_dict, flow_dict, provider_dict)
