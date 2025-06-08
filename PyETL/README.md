# PyETL

This project is a Python-based ETL pipeline that extracts sales data from CSV files,
transforms it, and loads it into a SQL database using Python.

The CSV file contains information about sales transactions with the following columns: transaction_id, customer_id, product_id, quantity, and timestamp. 
The destination SQL database has a table named sales with columns: transaction_id, customer_id, product_id, quantity, and sale_date.

## Features

- Mock data generation
- Data quality checks
- Incremental & concurrent loading
- Configuration via YAML
- Logging and encryption

## Installation

```bash
pip install -r requirements.txt

## Usage

##Configuration
Edit config/config.yaml to set DB credentials and file paths.