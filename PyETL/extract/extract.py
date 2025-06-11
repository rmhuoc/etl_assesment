# src/extract.py
import pandas as pd
import logging
import chardet

import chardet
import logging

def extract_csv(path):
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
            df = pd.read_csv(path, sep=',', encoding='latin-1')  
            logging.info(f"Successfully read {path} using encoding: {enc}")
            #eliminar
            logging.info(f"Index name extract: {df.index.name}")
            return df
        except UnicodeDecodeError as e:
            logging.warning(f"Unicode decode error with encoding '{enc}': {e}")

    raise UnicodeDecodeError(f"Failed to decode {path} with tried encodings: {tried_encodings}")
