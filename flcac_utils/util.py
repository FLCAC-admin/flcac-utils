"""
Supporting functions
"""

import datetime
import pandas as pd
from pathlib import Path
import olca_schema as o
import esupy.bibtex
from esupy.location import extract_coordinates
from flcac_utils.commons_api import read_commons_data



def assign_year_to_meta(meta, year):
    meta['valid_from'] = datetime.datetime(year, 1, 1).isoformat(timespec='seconds')
    meta['valid_until'] = datetime.datetime(year, 12, 31).isoformat(timespec='seconds')
    return meta

def format_dqi_score(dqi_dict):
    """generates a string in the form of "(1;2;3;2;2)"
    from a passed dictionary:
        Flow reliability:
          score: 2
        Temporal correlation:
          score: 1
    """
    dqi = ";".join([str(v.get('score','')) for k,v in dqi_dict.items()])
    return f'({dqi})'

def generate_locations_from_exchange_df(df):
    """generates a ditionary of location objects in the form of
    {'US': <geojson>} using 2-digit ISO codes
    df must contain the 'location' column
    """
    if 'location' not in df.columns:
        raise KeyError('"location" must be present in the dataframe to '
                       'generate location objects')
    geo_json = extract_coordinates(group='countries')
    locations = dict((k, geo_json[k]) for k in df['location'].unique()
                     if not pd.isnull(k))
    return locations

def increment_dqi_value(s: str, pos: int) -> str:
    """Increments the dqi string at pos by 1.
    pos is 1-based index"""
    # Split the dqi string into a list of integers
    s = s.strip('()')
    numbers = list(map(int, s.split(';')))

    # Increment the value at position N
    if 1 <= pos <= len(numbers):
        numbers[pos-1] += 1
    else:
        raise IndexError("Position is out of range")

    # Join the list back into a comma-separated string
    dqi = ';'.join(map(str, numbers))
    return f'({dqi})'

def extract_actors_from_process_meta(process_meta: dict,
                                     **kwargs
                                     ) -> (dict, dict):
    """
    Based on a metadata file, for all potential metadata fields which are actors,
    extract the actors by name via API calls.
    Metadata fields must be in the format of {'Repo label': 'Actor.name'}
    Process metadata file is modified and returned to leave only the actor name

    Returns a dictionary of {'Actor.name': olca.Actor}
    """
    print('Identifying actors from metadata')
    actor_list = []
    for field in ('data_set_owner', 'data_generator', 'data_documentor'):
        actor_dict = process_meta.get(field, '')
        if type(actor_dict) == dict:
            actor_list.append(actor_dict.copy())
            process_meta[field] = list(actor_dict.values())[0]
    actor_dict = {}
    for a in actor_list:
        # Reformat all identified actors by repository into a list
        repo = list(a.keys())[0]
        actor = list(a.values())[0]
        if repo in actor_dict:
            actor_dict[repo]['ACTORS'].append(actor)
        else:
            actor_dict[repo] = {'ACTORS': [actor]}
    actor_objs = {}
    if actor_dict:
        # Extract actors from API, recreate dictionary in correct format
        actors = read_commons_data(actor_dict, auth=kwargs.get('auth', False))
        for repo, a_list in actors.items():
            actor_objs = {a.name: a for a in a_list}
    if len(actor_list) != len(actor_objs):
        print('WARNING: not all actors found')
    return process_meta, actor_objs


def extract_sources_from_process_meta(process_meta: dict,
                                      bib_path: Path
                                      ) -> (dict, dict):
    """
    Based on a metadata file, for all potential metadata fields which are sources,
    generate the source objects from .bibtex file from bib_path.
    Metadata fields must be in the format of {'bib_id': 'Source.name'}
    Process metadata file is modified and returned to leave only the source name

    Returns a dictionary of {'Source.name': olca.Source}
    """
    print('Identifying sources from metadata')
    all_source_dict = {}
    for field in ('sources', 'publication'):
        source_dict = process_meta.get(field, '')
        if type(source_dict) == dict:
            all_source_dict.update(source_dict)
            process_meta[field] = list(source_dict.values())[0]
        elif type(source_dict) == list:
            l = []
            for s_dict in source_dict:
                all_source_dict.update(s_dict)
                l.append(list(s_dict.values())[0])
            process_meta[field] = l
    source_list = esupy.bibtex.generate_sources(
        bib_path = bib_path,
        bibids = all_source_dict)
    # rearrange the structure of the dictionary to {name: olca.Source}
    source_objs = {k.name: k for k in source_list}

    return process_meta, source_objs


def extract_dqsystems(dq_dict: dict, **kwargs) -> dict['str', o.DQSystem]:
    """
    :param: dq_dict dictionary that takes the form of
        {'Process' : {<repo>: <dq.Name>},
         'Flow' : {<repo>: <dq.Name>}}
    Returns a dictionary of {'Process': o.DQSystem,
                             'Flow': o.DQSystem}
    """
    print('Extracting dqSystems')
    api_dict = {}
    for t, repo_dict in dq_dict.items():
        repo = list(repo_dict.keys())[0]
        if 'DQ_SYSTEM' in api_dict.get(repo, {}):
            api_dict[repo]['DQ_SYSTEM'].append(list(repo_dict.values())[0])
        else:
            api_dict[repo] = {'DQ_SYSTEM': [list(repo_dict.values())[0]]}

    # api_dict = {'Federal LCA Commons Core Database': {
    #     'DQ_SYSTEM': ['US EPA - Process Pedigree Matrix',
    #                   'US EPA - Flow Pedigree Matrix']}
    #     }
    # Extract dq_systems from API, recreate dictionary in correct format
    dqsystems = read_commons_data(api_dict, auth=kwargs.get('auth', False))
    dq_objs = {}
    for repo, dq_list in dqsystems.items():
        for d in dq_list:
            if d.name == dq_dict.get('Process').get(repo):
                dq_objs['Process'] = d
            if d.name == dq_dict.get('Flow').get(repo):
                dq_objs['Flow'] = d
    # if len(dq_list) != len(dq_objs):
    #     print('WARNING: not all actors found')
    return dq_objs


def extract_flows(flow_dict: dict, add_tags=False, **kwargs) -> dict['str', o.Flow]:
    """
    :param: flow_dict dictionary that takes the form of
        {<repo>: [flow.Name, flow.Name, ...]}
    Returns a dictionary of {'flow.Name': o.Flow}
    """
    print('Extracting flows')
    flow_dict = {k: {'FLOWS': v} for k,v in flow_dict.items()}
    api_flows = read_commons_data(flow_dict, auth=kwargs.get('auth', False))

    # rearrange the structure of the dictionary to {name: olca.Flow}
    flow_objs = {}
    for repo, f_list in api_flows.items():
        for f in f_list:
            if add_tags:
                f.tags = [repo]
            flow_objs[f.name] = f
    return flow_objs


def extract_processes(process_dict: dict, to_ref = False, **kwargs
                      ) -> dict['str', o.Process]:
    """
    :param: process_dict dictionary that takes the form of
        {<repo>: [process.Name, process.Name, ...]}
    Returns a dictionary of {'process.Name': o.Process}
    """
    print('Extracting processes')
    process_dict = {k: {'PROCESS': v} for k,v in process_dict.items()}
    api_processes = read_commons_data(process_dict, auth=kwargs.get('auth', False))

    # rearrange the structure of the dictionary to {name: olca.Process}
    process_objs = {}
    for repo, p_list in api_processes.items():
        for p in p_list:
            if to_ref:
                p = p.to_ref()
            process_objs[p.name] = p
    return process_objs


if __name__ == "__main__":
    dq_dict = {'Process': {'Federal LCA Commons Core Database':
                           'US EPA - Process Pedigree Matrix'},
               'Flow': {'Federal LCA Commons Core Database':
                        'US EPA - Flow Pedigree Matrix'}}
    dq_objs = extract_dqsystems(dq_dict)
    flow_dict = {'US Electricity Baseline':
                    {'FLOWS': ['Electricity, AC, 120 V']},
                'Heavy equipment operation': 
                    {'FLOWS': ['Diesel; dispensed at pump',
                               'Compressed natural gas; dispensed at pump',
                               'Gasoline; dispensed at pump']}
                    }
    flow_dict = extract_flows(flow_dict)
