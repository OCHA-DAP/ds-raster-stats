# ds-raster-stats

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
```

3. Create a local `.env` file with the following environment variables:

```
# Connection to Azure blob storage
DSCI_AZ_SAS_DEV=<provided-on-request>
DSCI_AZ_SAS_PROD=<provided-on-request>
```

### Pre-Commit

All code is formatted according to black and flake8 guidelines. The repo is set-up to use pre-commit. Before you start developing in this repository, you will need to run

```
pre-commit install
```

You can run all hooks against all your files using

```
pre-commit run --all-files
