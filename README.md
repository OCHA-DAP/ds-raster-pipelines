# Pipelines for Analysis-Ready COGs

This repository contains code to create stores of cloud-optimized GeoTiFFs (COGs) for various sources of raster data. Data is ingested from various sources and stored in a private Azure Storage Container.

## Data Sources

### 1. ECMWF SEAS5 Seasonal Forecasts

**[Pipeline docs](src/seas5/README.md)**

These forecasts contain 0.4 degree resolution global data on precipitation rates across 0-6 month lead-times. Historical data from as early as 1981 has been accessed via ECMWF's [Meteorological Archival and Retrieval System](https://www.ecmwf.int/en/forecasts/access-forecasts/access-archive-datasets) (MARS). See this [User Manual](https://www.ecmwf.int/sites/default/files/medialibrary/2017-10/System5_guide.pdf) for more details.

### 2. ECMWF ERA5 Reanalysis

**[Pipeline docs](src/era5/README.md)**

The ERA5 reanalysis provides averaged monthly and hourly estimates of total precipitation across a 0.25 degree global grid. See [these docs](https://confluence.ecmwf.int/display/CKB/The+family+of+ERA5+datasets) for more information on the full family of ERA5 datasets.

## Pipeline Orchestration

TBD. At present, all pipelines are configured to run locally.


## Development Setup

1. Clone this repository and create a virtual Python (3.12.4) environment:

```
git clone https://github.com/OCHA-DAP/ds-raster-pipelines.git
python3 -m venv venv
source venv/bin/activate
```

2. Install Python dependencies:

```
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

3. If processing `.grib` files using `xarray`, the `cfgrib` engine also requires an [ecCodes system dependency](https://confluence.ecmwf.int/display/ECC/ecCodes+installation). This can be installed with

```
sudo apt-get install libeccodes-dev
```

4. Create a local `.env` file with the following environment variables:

```
# Connection to Azure blob storage
DSCI_AZ_SAS_DEV=<provided-on-request>
DSCI_AZ_SAS_PROD=<provided-on-request>

# MARS API requests
ECMWF_API_URL=<provided-on-request>
ECMWF_API_EMAIL=<provided-on-request>
ECMWF_API_KEY=<provided-on-request>

# ECMWF AWS bucket
AWS_ACCESS_KEY_ID=<provided-on-request>
AWS_SECRET_ACCESS_KEY=<provided-on-request>
AWS_BUCKET_NAME=<provided-on-request>
AWS_DEFAULT_REGION=<provided-on-request>

# CDS API credentials
CDSAPI_URL=<provided-on-request>
CDSAPI_KEY=<provided-on-request>

```

### Pre-Commit

All code is formatted according to black and flake8 guidelines. The repo is set-up to use pre-commit. Before you start developing in this repository, you will need to run

```
pre-commit install
```

You can run all hooks against all your files using

```
pre-commit run --all-files
```
