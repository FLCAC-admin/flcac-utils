"""
Mapping functions
"""

import pandas as pd
import numpy as np

from esupy.util import make_uuid
from flcac_utils.util import extract_flows, extract_processes

def prepare_tech_flow_mappings(df, auth=False):
    ## Identify mappings for technosphere flows (fuel inputs)
    flow_dict = {row['SourceFlowName']:
                      {'BRIDGE': row.get('Bridge'),
                       'name': row['BridgeFlowName'] if row.get('BridgeFlowName') else row['TargetFlowName'],
                       'provider': row.get('Provider') if not row.get('Bridge') else np.nan,
                       'repo': {row['TargetRepoName']: row['TargetFlowName']},
                       'conversion': row['ConversionFactor'],
                       'unit': row['TargetUnit']} for _, row in df.iterrows()}
                ## swap the flow names for bridge processes?
    
    ## extract fuel objects in flow_dict from commons via API
    f_dict = {}
    p_dict = {}
    for k, v in flow_dict.items():
        if 'repo' in v:
            repo = list(v.get('repo').keys())[0]
            flow = list(v.get('repo').values())[0]
            flow_dict[k]['target_name'] = flow
            if not flow_dict[k].get('BRIDGE'):
                flow_dict[k]['name'] = flow
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
                print(f'Flow: {v["name"]} not found.')
        else:
            flow_dict[k]['id'] = make_uuid(flow_dict[k].get('name'))
    
    return (flow_dict, flow_objs, provider_dict)
