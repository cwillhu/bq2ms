# BQ2MS

Pull BigQuery data into SQL Server

## Overview of setup

1. Log into Google Cloud account and download JSON file with API key 
2. Set environment variables:

    GOOGLE_APPLICATION_CREDENTIALS: Path to Google-generated JSON file
    
    BQ2MS_WORK_DIR: Temp directory where SQL and CSV files are written
       
## Usage
    
    Usage: java -jar <path to bq2ms.jar> [-hV] <datasetId> <tableId>
    
    Transfer BigQuery table to SQL Server
          <datasetId>   BigQuery dataset name (e.g., "census_bureau_acs").
          <tableId>     BigQuery tableId (e.g., "state_2018_1yr").
      -h, --help        Show this help message and exit.
      -V, --version     Print version information and exit.

