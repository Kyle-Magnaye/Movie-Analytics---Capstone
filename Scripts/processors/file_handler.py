import pandas as pd
import json

def read_csv(filepath):
    return pd.read_csv(filepath)

def write_csv(df, filepath):
    df.to_csv(filepath, index=False)

def read_json(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def write_json(data, filepath):
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)
