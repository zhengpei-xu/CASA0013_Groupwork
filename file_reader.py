import pandas as pd
import janitor
import matplotlib.pyplot as plt
from typing import Optional

def read_csv_url(
    url: str,
    compression: str = "infer",
    encoding: str = "utf-8",
    dtype: Optional[dict[str, str]] = None,
    low_memory: bool = False
) -> Optional[pd.DataFrame]:
    """
    Load a CSV file from a remote URL into a pandas DataFrame.
    Column names are cleaned using janitor. If loading fails, prints a failure message.

    Parameters:
        url (str): Direct URL to the CSV file (.csv or .csv.gz).
        compression (str): Compression type ('gzip', 'infer', etc.).
        encoding (str): Character encoding used in the file.
        dtype (dict, optional): Dictionary specifying column data types (e.g., {'id': str}).
        low_memory (bool): If True, uses less memory but may misinfer column types.

    Returns:
        pd.DataFrame or None: Cleaned DataFrame with standardized column names, or None if loading fails.
    """
    try:
        df = pd.read_csv(
            url,
            compression=compression,
            encoding=encoding,
            dtype=dtype,
            low_memory=low_memory
        ).clean_names()
        print(f"Successfully loaded from URL: {url}")
        print("Preview of first 5 rows and columns:")
        print(df.iloc[:5, :5])
        return df
    except Exception as e:
        print(f"Failed to load from URL: {url}\nError: {e}")
        return None