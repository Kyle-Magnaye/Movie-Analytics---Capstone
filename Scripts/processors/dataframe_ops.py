import pandas as pd

def remove_duplicates(df, subset_columns):
    return df.drop_duplicates(subset=subset_columns, keep='first')

def fill_missing(df, column, fetch_func):
    for idx, value in df[column].items():
        if pd.isnull(value) or value == "":
            fetched_value = fetch_func(df.at[idx, "id"])
            if fetched_value:
                df.at[idx, column] = fetched_value
    return df
