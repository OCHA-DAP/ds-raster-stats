# Raster Statistics Pipelines

This repo contains code to calculate raster (or zonal) statistics from internal stores of gridded datasets.

## Usage

This pipeline can be run from the command line by calling `python run_raster_stats.py` with appropriate input args:

```
usage: run_raster_stats.py [-h] [--mode {local,dev,prod}] [--test] {seas5,era5,imerg,floodscan}

positional arguments:
  {seas5,era5,imerg,floodscan}    Dataset for which to calculate raster stats

options:
  -h, --help            show this help message and exit
  --mode {local,dev,prod}, -m {local,dev,prod}
                        Run the pipeline in 'local', 'dev', or 'prod' mode.
  --update-stats        Calculate stats against the latest COG for a given dataset.
  --backfill            Whether to check and backfill for any missing dates.
  --update-metadata     Update the iso3 and polygon metadata tables.
  --test                Processes a smaller subset of the source data. Use to test the pipeline.
```

## Development Setup

1. Clone this repository and create a virtual Python (3.12.4) environment:

```
git clone https://github.com/OCHA-DAP/ds-raster-stats.git
python3 -m venv venv
source venv/bin/activate
```

2. Install Python dependencies:

```
pip install -r requirements.txt
pip install -r requirements-dev.txt
pip install -e .
```

3. Create a local `.env` file with the following environment variables:

```
# Azure blob storage
DSCI_AZ_BLOB_DEV_SAS=<provided-on-request>
DSCI_AZ_BLOB_PROD_SAS=<provided-on-request>
# Postgres (write creds; HOST is the private-endpoint IP, or the FQDN if you
# still have public access)
DSCI_AZ_DB_DEV_HOST=<provided-on-request>
DSCI_AZ_DB_DEV_UID_WRITE=<provided-on-request>
DSCI_AZ_DB_DEV_PW_WRITE=<provided-on-request>
DSCI_AZ_DB_PROD_HOST=<provided-on-request>
DSCI_AZ_DB_PROD_UID_WRITE=<provided-on-request>
DSCI_AZ_DB_PROD_PW_WRITE=<provided-on-request>
```

The scheduled runs are the `Raster Stats {FLOODSCAN,IMERG,ERA5,SEAS5}` Databricks
jobs (chained from the `Run *` jobs in ds-raster-pipelines); they run on the
shared Job Compute policy, which injects all of the variables above from the
`dsci` secret scope.

### Pre-Commit

All code is formatted according to black and flake8 guidelines. The repo is set-up to use pre-commit. Before you start developing in this repository, you will need to run

```
pre-commit install
```

You can run all hooks against all your files using

```
pre-commit run --all-files
```

### Table constraints
The table constraints are added automatically to new tables when they are created, however if an existing table needs to be updated then the scripts available in `tables_constraints.sql` can be run in order to have the constraints added. For dev and production this update requires permission to update table as admin.
