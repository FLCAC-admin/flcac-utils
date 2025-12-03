"""
Metadata
"""

import pandas as pd
from pathlib import Path

parent_path = Path(__file__).parent

metadata_keys = [
    "description",
    "valid_until",
    "valid_from",
    "time_description",
    "geography_description",
    "technology_description",
    "inventory_method_description",
    "modeling_constants_description",
    "completeness_description",
    "data_selection_description",
    "data_treatment_description",
    "sampling_description",
    "data_collection_description",
    "use_advice",
    "sources",
    "project_description",
    "intended_application",
    "data_set_owner",
    "data_generator",
    "data_documentor",
    "publication",
    "restrictions_description",
    ]

metadata_match = {
    }

def read_tabular_metadata(df) -> dict:
    """Reads metadata from tabular format where the column headers are process names
    and index values are keys.
    """
    # d = df.to_dict(orient='dict')

    # Normalize function
    def normalize(s):
        return (s.lower()
                .replace("_", " ")
                .strip()
                )

    # Create mapping of row index names to target metadata keys
    mapping = {}
    for index in df.index:
        norm_row = normalize(index)
        for key in metadata_keys:
            if normalize(key) == norm_row:
                mapping[index] = key
            elif normalize(key).replace(' description', '') == norm_row:
                mapping[index] = key

    # Build dictionary of dictionaries
    d = {}
    for col in df.columns:
        d[col] = {mapping[idx]: ("" if pd.isna(val) else val)
                  for idx, val in df[col].items() if idx in mapping}

    return d


if __name__ == '__main__':
    filepath = parent_path.parents[1] / 'FDC-curation-admin' / 'aluminum' / 'aluminum_metadata.xlsx'
    df1 = pd.read_excel(filepath, sheet_name='General information',
                        header=1,
                        usecols='A, D, E, F', index_col=0)
    df2 = pd.read_excel(filepath, sheet_name='Documentation',
                        usecols='A, D, E, F', index_col=0)
    df = pd.concat([df1, df2])
    metadict = read_tabular_metadata(df)
