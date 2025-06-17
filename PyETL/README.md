# ETL Project

## Overview

This project implements an ETL pipeline that loads CSV data into a PostgreSQL database, performing validations, encryption, and incremental loads using chunk processing for efficiency. The pipeline is configurable via a YAML file.

---

## Project Structure

```
project-root/
│
├── config/
│   ├── config.yaml           # Main configuration file
│   ├── secret.key            # Encryption key (should NOT be committed)
│
├── data/                     # Source CSV files (should NOT be committed)
│   ├── sales_transactions.csv
│   └── archive/              # Archived input files processed by ETL
│
├── logs/                     # Log files directory, rotated nightly (should NOT be committed)
│
├── extract/                  # Extraction logic
├── transform/                # Encryption and transformation logic
├── load/                     # Loading and incremental logic
├── utils.py                  # Utility functions (e.g., load_with_copy, schema sync)
├── main.py                   # Main entry point for ETL execution
│
├── generate_key.py           # Script to generate encryption key
├── requirements.txt          # Python dependencies
├── .gitignore                # Git ignore file
└── README.md                 # This file
```

> All required directories such as `data/`, `logs/`, and `data/archive/` are automatically created at runtime if they do not already exist, ensuring the ETL process runs smoothly without manual setup.

---

## Configuration (`config/config.yaml`)

The ETL behavior is controlled by this YAML file. Key sections include:

- **database**: Connection parameters for PostgreSQL.
- **load_process**: Schemas, log table name, and sequence for process IDs.
- **encryption**: Enable/disable encryption, key path, and columns to encrypt.
- **files_to_tables_tmp**: CSV files and their corresponding temporary tables.

  > Note: All tables must already exist in the database. This ETL does not create them.

- **files_to_tables_inc**: Mappings for incremental load from temp tables to target tables, with unique keys.
- **mock_data**: Config for generating synthetic test data.
- **csv**: Chunk size and parallelism for processing large files.
- **tables**: Data validation rules (e.g., required columns, filters).
- **logging**: Log directory, file name, encoding, and daily rotation policy.

> Logs are rotated every night. A dedicated ETL log table in the database records each execution with details such as process ID, status, duration, and errors.

---

## Setup and Running the Project

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Generate Encryption Key

If encryption is enabled in the config, generate the key and place it in the `config/` folder:

```bash
python generate_key.py
```

This will create a `secret.key` file used for encrypting the specified columns.

### 3. Prepare the Database

- Ensure the PostgreSQL database is running.
- Create required schemas and tables manually before the first run.
- Update `config.yaml` with the appropriate database credentials and schema names.

### 4. Run the ETL

Execute the ETL pipeline:

```bash
python main.py
```

The process will:

- Read encrypted CSV files in chunks.
- Validate, clean, and encrypt data.
- Load it into temporary tables.
- Perform incremental inserts into the final table.

---

## Git Branching and Version Control

### Branches

- `main`: Production-ready stable code.
- `develop`: Active development branch.
- `features/`: Feature-specific branches (e.g., `features/add-encryption`).

### Workflow

```
features/* → develop → main
```

- Develop in `features/*` branches.
- Merge to `develop` after review and testing.
- Merge `develop` to `main` for release.
- Tag the `main` branch with release version:

```bash
git checkout main
git pull origin main
git tag -a v1.0.0 -m "Release version 1.0.0"
git push origin v1.0.0
```

---

## .gitignore Recommendations

```
# Ignore secret keys
config/secret.key

# Ignore logs
logs/

# Ignore local data
data/

# Python cache
__pycache__/
*.pyc
```

---

## Improvements for Future Releases

- Move configuration of file-table mappings from `config.yaml` to a database table to enable dynamic updates.
- Add retry mechanism for partial loads or temporary database failures.
- Implement a CLI or dashboard for monitoring recent ETL runs and viewing errors.

---

## Summary

- Configure everything via `config/config.yaml`.
- Generate an encryption key if enabled.
- Ensure all necessary tables exist in the database.
- Run the ETL from `main.py`.
- Logs are rotated nightly and persisted in both file system and DB table.
- Use standard Git branching and tagging to manage releases.
