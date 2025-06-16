# ETL Project

## Overview

This project implements an ETL pipeline that loads CSV data into a PostgreSQL database, performing validations, 
encryption, and incremental loads using chunk processing for efficiency. The pipeline is configurable via a YAML file.

---

## Project Structure

project-root/
│
├── config/
│ ├── config.yaml # Main configuration file
│ ├── secret.key # Encryption key (should NOT be committed)
│
├── data/ # Source CSV files (should NOT be committed)
│ ├── sales_transactions.csv
│
├── logs/ # Log files directory (should NOT be committed)
│
├── src/
│ ├── extract/
│ ├── transform/
│ ├── load/
│ ├── utils.py # Utility functions (e.g., load_with_copy, parallel loading)
│ └── main.py # Main entry point for ETL execution
│
├── generate_key.py # Script to generate encryption key
├── requirements.txt # Python dependencies
├── .gitignore # Git ignore file
└── README.md # This file


---

## Configuration (`config/config.yaml`)

The ETL behavior is controlled by this YAML file. Key sections include:

- **database**: Connection parameters for PostgreSQL.
- **load_process**: Schemas, table names, and sequence names used in ETL logging.
- **encryption**: Enable/disable encryption, key path, and columns to encrypt.
- **files_to_tables_tmp**: CSV files and their corresponding temporary tables.
- **files_to_tables_inc**: Mapping from temporary tables to final target tables with unique keys for incremental load.
- **mock_data**: Configuration for generating mock data for testing.
- **csv**: Chunk size to read CSV files in batches.
- **tables**: Validation rules including required columns and filters.
- **logging**: Log directory, file name, rotation policy, and encoding.

Modify this file to adapt the ETL to your environment, database schema, or input files.

---

## Setup and Running the Project

### Install Dependencies

```bash
pip install -r requirements.txt

### Generate Encryption Key

If encryption is enabled in the config, generate the key and place it in the config/ folder:

python generate_key.py

This will create a secret.key file used for encrypting specified columns.

###Prepare the Database

    Ensure the PostgreSQL database exists.

    Create the necessary schemas if not already present.

    Update config.yaml with the correct database credentials and schema names.

###Run the ETL

Execute the main script to start the ETL process:

python src/main.py

The process reads CSV files chunk by chunk, validates, cleans, encrypts if enabled, and loads data efficiently into the target tables.

###Git Branching and Version Control
Branches

    main: Stable production-ready code. Always deployable.

    develop: Integration branch for the current development cycle. Feature-complete and tested but not necessarily production-ready.

    features/*: Separate branches for each new feature or bugfix, branched off from develop.

Workflow

    Work on features in features/* branches.

    Merge feature branches into develop after code review and testing.

    When the project or sprint is finished and tested, merge develop into main.

    Tag releases on the main branch with semantic version tags (e.g., v1.0.0).

Committing and Pushing

    Commit frequently with clear messages.

    Push feature branches to remote and create Pull Requests for review.

    Use .gitignore to avoid committing sensitive data like keys, logs, and raw data files.

Creating a Tag for a Release

Once the code in develop is ready and merged into main, create a tag:

git checkout main
git pull origin main
git tag -a v1.0.0 -m "Release version 1.0.0"
git push origin v1.0.0

Replace v1.0.0 with your current version.
.gitignore Recommendations

Add the following to .gitignore to avoid committing sensitive or bulky files:

# Ignore config encryption key
config/secret.key

# Ignore logs
logs/

# Ignore data files
data/

# Python cache
__pycache__/
*.pyc

Summary

    Configure all parameters in config/config.yaml.

    Use generate_key.py to create your encryption key if needed.

    Keep your database and schemas ready.

    Run the ETL via src/main.py.

    Follow Git branching conventions: features/* → develop → main.

    Tag production releases on main.

This setup ensures a smooth development process and a configurable, maintainable ETL pipeline.