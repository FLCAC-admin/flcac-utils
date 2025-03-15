"""
Functions to support accessing the FLCAC via API
"""

import urllib.request
import requests
import io
import json
from pathlib import Path
import pandas as pd
import zipfile
import yaml
import olca_schema as olca

parent_path = Path(__file__).parent
data_path = parent_path / 'data'

commons_base = 'https://www.lcacommons.gov/lca-collaboration'

def get_config():
    with open(data_path / "repos.yml", "r") as file:
        config = yaml.safe_load(file)
    return config

def login():
    """Logs in to the API and returns the auth token."""
    username = input("Enter your username: ")
    password = input("Enter your password: ")
    
    url = f"{commons_base}/ws/public/login"
    payload = {
        "username": username,
        "password": password
    }

    try:
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            print("Login successful.")
            return response.cookies.get("JSESSIONID")
        else:
            print(f"Login failed with status code: {response.status_code}")
            print(f"Response content: {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Login request failed: {str(e)}")
        return None

def get_repository_info(token, group, repo):
    """Gets repository metadata to check supported types."""
    url = f"{commons_base}/ws/repository/{group}/{repo}"
    
    if token:
        cookies = {"JSESSIONID": token}
    else:
        cookies = None
        # get public status
        url = f"{commons_base}/ws/public/repository/{group}/{repo}"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.get(url, cookies=cookies, headers=headers)
        if response.status_code == 200:
            repo_info = response.json()
            # print("\nRepository Information:")
            # print(json.dumps(repo_info, indent=2))
            return repo_info
        else:
            print(f"Failed to get repository info: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error getting repository info: {e}")
        return None

def get_recent_commits(token, group, repo):
    """Fetches recent commits to get the hash of our latest commit."""
    if not token:
        endpoints = [
            f'{commons_base}/ws/public/repository/{group}/{repo}',
            ]
        cookies = None
    
    else:     
    # Try multiple endpoints to find commits
        endpoints = [
            # Direct history endpoint
            # f"{commons_base}/ws/history/{group}/{repo}",
            # Repository activities
            # f"{commons_base}/ws/repository/{group}/{repo}/activity",
            # Search endpoint with commits
            # f"{commons_base}/ws/history/search/{group}/{repo}",
            f"{commons_base}/ws/public/browse/{group}/{repo}"
        ]
        cookies = {"JSESSIONID": token}
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    
    for url in endpoints:
        try:
            # print(f"\nTrying commits endpoint: {url}")
            response = requests.get(url, cookies=cookies, headers=headers)
            # print(f"Response status: {response.status_code}")
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    # print("\nResponse data:")
                    # print(json.dumps(data, indent=2))

                    if not token:
                        commit_hash = data['settings']['id']
                        return commit_hash
                    
                    # Check for commits in various response formats
                    commits = None
                    if isinstance(data, dict):
                        commits = data.get('data', []) or data.get('commits', []) or data.get('activities', [])
                    elif isinstance(data, list):
                        commits = data
                    timestamp0 = 0
                    if commits and len(commits) > 0:
                        # Look for our commit based on message and timestamp
                        for commit in commits:
                            timestamp = commit.get('commitTimestamp', 0)
                            if timestamp > timestamp0:
                                timestamp0 = timestamp
                                commit_hash = commit.get('id') or commit.get('commitId') or commit.get('hash')
                                message = commit.get('commitMessage', '')
                        if commit_hash:
                            # print(f"Found matching commit hash: {commit_hash}: {message}")
                            return commit_hash
                            
                except json.JSONDecodeError as e:
                    print(f"Failed to parse response from {url}: {e}")
            else:
                print(f"Response content: {response.text}")
                
        except requests.exceptions.RequestException as e:
            print(f"Error with endpoint {url}: {e}")
    
    print("Could not find commit hash in any endpoint")
    return None

def return_request(owner, repo, object_type = 'PROCESS', **kwargs):
    token = kwargs.get('token', None)

    url = (f'{commons_base}/'
           f'ws/public/download/json/prepare/'
           f'{owner}/{repo}?path={object_type}')

    cookies = {"JSESSIONID": token}
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    resp = requests.get(url, cookies=cookies, headers=headers)
    json_token = resp.content.decode()
    # URL used to download json once token is identified
    download_url = 'https://www.lcacommons.gov/lca-collaboration/ws/public/download/json'
    resp = requests.get(url = f'{download_url}/{json_token}', cookies=cookies)
    return resp

def read_json(f, path):
    data = f.read(path)
    if len(data) == 0:
        return
    d = json.loads(data.decode("utf-8"))
    # print(d.get('name'))
    return d

def check_obj_append(d, search_objs, obj_type):
    if not d:
        return False
    elif not search_objs:
        return True
    elif search_objs and d.get('name') in search_objs[obj_type]:
        return True
    else:
        return False

def process_response(resp, object_types, search_objs=None):
    object_list = []
    with zipfile.ZipFile(io.BytesIO(resp.content), "r") as f:
        for name in f.namelist():
            ## extract only the objects within the relevant subfolder
            if "PROCESS" in object_types:
                if name.startswith('process'):
                    d = read_json(f, name)
                    if check_obj_append(d, search_objs, obj_type='PROCESS'):
                        l = olca.Process.from_dict(d)
                        object_list.append(l)
                    else: continue
            if 'IMPACT_METHOD' in object_types:
                if name.startswith('lcia_categories'):
                    d = read_json(f, name)
                    if check_obj_append(d, search_objs, obj_type='IMPACT_METHOD'):
                        l = olca.ImpactCategory.from_dict(d)
                        object_list.append(l)
                    else: continue
                elif name.startswith('lcia_methods'):
                    d = read_json(f, name)
                    if check_obj_append(d, search_objs, obj_type='IMPACT_METHOD'):
                        l = olca.ImpactMethod.from_dict(d)
                        object_list.append(l)
                    else: continue
            if 'ACTORS' in object_types:
                if name.startswith('actor'):
                    d = read_json(f, name)
                    if check_obj_append(d, search_objs, obj_type='ACTORS'):
                        l = olca.Actor.from_dict(d)
                        object_list.append(l)
                    else: continue
            if 'SOURCES' in object_types:
                if name.startswith('source'):
                    d = read_json(f, name)
                    if check_obj_append(d, search_objs, obj_type='SOURCES'):
                        l = olca.Source.from_dict(d)
                        object_list.append(l)
                    else: continue
            if 'DQ_SYSTEM' in object_types:
                if name.startswith('dq_system'):
                    d = read_json(f, name)
                    if check_obj_append(d, search_objs, obj_type='DQ_SYSTEM'):
                        l = olca.DQSystem.from_dict(d)
                        object_list.append(l)
                    else: continue
            if 'FLOWS' in object_types:
                if name.startswith('flows'):
                    d = read_json(f, name)
                    if check_obj_append(d, search_objs, obj_type='FLOWS'):
                        l = olca.Flow.from_dict(d)
                        object_list.append(l)
                    else: continue
    return object_list

def read_commons_data(object_dict, auth=False):
    token = login() if auth else None
    
    config = get_config()
    data_dict = {}
    for repo, object_types in object_dict.items():
        search_objs = {}
        repo_data = config.get(repo)
        if not repo_data:
            raise ValueError(f'{repo} not found in config!')
        print(f'Accessing API for {repo}')
        if type(object_types) == dict:
            search_objs = object_types.copy()
            object_types = list(object_types.keys())
        elif type(object_types) == str:
            object_types = [object_types]
        api_objects = [i for i in object_types if i in 
                       ('PROCESS', 'DQ_SYSTEM', 'IMPACT_METHOD')]
        api_objects = ['PROCESS'] if not api_objects else api_objects
        resp = return_request(owner = repo_data.get('owner'),
                              repo = repo_data.get('repo'),
                              object_type = api_objects[0],
                              token = token
                              )
        data_dict[repo] = process_response(resp,
                                           object_types=object_types,
                                           search_objs=search_objs)
    return data_dict

if __name__ == '__main__':
    object_dict = {
        # 'USLCI': 'PROCESS',
        # 'US Electricity Baseline':
        #     {'ACTORS': ['NETL', 'NREL'],
        #      'PROCESS': ['coal extraction and processing - Northern Appalachia, BIT, Underground']},
        # 'Construction and Demolition Debris Management': 'SOURCES',
        'Federal LCA Commons Core Database': 'DQ_SYSTEM',
        'CED Method': 'IMPACT_METHOD',
        }
    data_dict = read_commons_data(object_dict, auth=False)
