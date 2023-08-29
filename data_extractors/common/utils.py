import pandas as pd

def save_dataframe_to_csv(df, filename):
    df.to_csv(filename, index=False)