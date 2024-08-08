## Usage

This pipeline can be run from the command line as follows:

```
usage: run_era5.py [-h] [--mode {local,dev,prod}] [--start START] [--end END]
                   [--update]

options:
  -h, --help            show this help message and exit
  --mode {local,dev,prod}, -m {local,dev,prod}
                        Run the pipeline in 'local', 'dev', or 'prod' mode.
  --start START, -s START
                        Start year to retrieve and process archival ERA5 data. Must be
                        between 1981 and 2024 (default: 1981). Does not apply if
                        running `--update`.
  --end END, -e END     End year to retrieve and process archival ERA5 data. Must be
                        between 1981 and 2024 (default: 2024). Does not apply if
                        running `--update`.
  --update              If specified, will retrieve data from the current month.
                        ``--start` and `--end` years will have no impact.
```

### Example usage

1. Process the full archive from 1981 to 2023 and save to `prod` Azure container

```
python run_era5.py -m prod
```

2. Process the data from all of 2023 and save locally

```
python run_era5.py -s 2023 -e 2023
```

3. Process updated data from the current month and save to `dev` Azure container

```
python run_era5.py -m dev --update
```

## Processing details

### Raw data

From 1981 to 2023, a raw `.grib` file is downloaded for each year, containing monthly averaged total precipitation (as output from the ERA5 reanalysis). See [here](https://rmets.onlinelibrary.wiley.com/doi/10.1002/qj.3803) for an article describing ERA5 reanalysis products. Data is accessed through the [CDS (Beta) API](https://cds-beta.climate.copernicus.eu/). From 2024 onwards, raw `.grib` files are downloaded monthly.

This data is stored in Azure according to the following structure:

```
raster
├─ era5/
│       ├─ monthly/
│         ├─ raw/
│             ├─ tp_reanalysis_monthly_2022_all.grib
│             ├─ tp_reanalysis_monthly_2023_all.grib
│             ├─ tp_reanalysis_monthly_2024_01.grib
│             ├─ tp_reanalysis_monthly_2024_02.grib
│             etc.
```

### Processed data

Each `.grib` file is processed to output monthly COGs (`.tif`), projected to `EPSG:4326`.

This data is stored in Azure according to the following structure:

```
raster
├─ era5/
│       ├─ monthly/
│         ├─ processed/
│             ├─ tp_reanalysis_v2023-01-01.grib
│             ├─ tp_reanalysis_v2023-02-01.grib
│             etc.
```

Note that the date in the filename corresponds to the `valid_date`, which is the date at which the reanalysis outputs are valid (rather than the issued or initialization date).
