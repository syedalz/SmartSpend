"""
    Load raw tabular data from a CSV, Excel, or JSON file into a pandas DataFrame.

    Purpose:
        Reads data from the specified file and returns it as a structured
        DataFrame. This function **does not clean, transform, or validate** the data;
        it only handles reliable ingestion of raw data.

    Parameters:
        filename (str or Path): Path to the file to load. Must exist and be readable.
            - Supports relative or absolute paths.
            - File must contain headers.

        filetype (str, optional): Type of file to read. One of "csv", "excel", "json".
            Default is "csv".

    Returns:
        pandas.DataFrame: Raw data exactly as stored in the file.
            - All columns are preserved.
            - Data is unmodified.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If the file is empty or cannot be parsed.

    Assumptions:
        - CSV/Excel/JSON files are UTF-8 encoded.
        - Files include a header row.
        - Data may contain missing or inconsistent values.
        - File sizes are manageable for in-memory loading.

    Responsibilities:
        - Only handles file reading.
        - Does not clean, validate, categorize, or transform data.
        - Designed to integrate seamlessly with downstream modules
          (e.g., clean_data.py).

    Notes / Extendability:
        - Can be extended later to support custom delimiters, other file formats,
          or batch loading multiple files.
        - Optional logging of rows/columns loaded can be added for observability.

    Example:
        df = load_data("data/raw/my_data.csv")
"""

import pandas as pd
import logging

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)

def load_data(filename, filetype="csv"):
    """Load raw tabular data into a pandas DataFrame."""

    filetype = filetype.lower()

    if filetype == "csv":
        try:
            data = pd.read_csv(filename)

        except FileNotFoundError as e:
            raise FileNotFoundError(f"CSV file {filename} not found") from e
        
        except pd.errors.EmptyDataError as e:
            raise ValueError(f"CSV file {filename} is empty") from e
        
        except pd.errors.ParserError as e:
            raise ValueError(f"CSV file {filename} could not be parsed") from e

    elif filetype == "excel":
        try:
            data = pd.read_excel(filename)

        except FileNotFoundError as e:
            raise FileNotFoundError(f"Excel file {filename} not found") from e
        
        except pd.errors.EmptyDataError as e:
            raise ValueError(f"Excel file {filename} is empty") from e
        
        except Exception as e: 
            raise ValueError(f"Excel file {filename} could not be parsed: {e}") from e

    elif filetype == "json":
        try:
            data = pd.read_json(filename)

        except FileNotFoundError as e:
            raise FileNotFoundError(f"JSON file {filename} not found") from e
        
        except pd.errors.EmptyDataError as e:
            raise ValueError(f"JSON file {filename} is empty") from e
        
        except ValueError as e: 
            raise ValueError(f"JSON file {filename} could not be parsed: {e}") from e

    else:
        raise ValueError(f"Invalid file type specified: {filetype}")

    logging.info(f"{len(data)} rows returned successfully from {filename}!")
    return data
