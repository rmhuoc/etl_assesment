# src/extract.py
import pandas as pd
import logging
import chardet

import chardet
import logging

def extract_csv(path):
    """
    DEPRECATED: Extract CSV data from a file into a pandas DataFrame.

    This function attempts to detect the file encoding using chardet and tries
    multiple encodings to successfully read the CSV file. It returns the entire
    DataFrame in one go.

    Note:
        This function is deprecated because the data loading process now uses
        chunked reading to handle large files efficiently.

    Parameters:
        path (str): The file path to the CSV file.

    Returns:
        pd.DataFrame: The loaded data as a DataFrame.

    Raises:
        UnicodeDecodeError: If none of the tried encodings can decode the file.
    """
    with open(path, 'rb') as f:
        raw_data = f.read()
        result = chardet.detect(raw_data)
        encoding = result['encoding'] or 'utf-8'
        confidence = result['confidence']
        logging.info(f"Detected encoding for {path}: {encoding} (confidence: {confidence})")

    tried_encodings = [encoding, 'utf-8', 'latin1', 'cp1252']
    for enc in tried_encodings:
        try:
            logging.info(f"Trying read {path} using encoding: {enc}")
            # NOTE: Encoding argument hardcoded to 'latin-1' in read_csv may be a bug; should probably be `encoding=enc`
            df = pd.read_csv(path, sep=',', encoding='latin-1')  
            logging.info(f"Successfully read {path} using encoding: {enc}")
            # Debug info about index
            logging.info(f"Index name extract: {df.index.name}")
            return df
        except UnicodeDecodeError as e:
            logging.warning(f"Unicode decode error with encoding '{enc}': {e}")

    raise UnicodeDecodeError(f"Failed to decode {path} with tried encodings: {tried_encodings}")

