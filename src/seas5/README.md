## Usage

The pipeline can be run from the command line as follows:

```
usage: run_seas5.py [-h] [--mode {local,dev,prod}] [--start START] [--end END] [--update]

options:
  -h, --help            show this help message and exit
  --mode {local,dev,prod}, -m {local,dev,prod}
                        Run the pipeline in 'local', 'dev', or 'prod' mode.
  --start START, -s START
                        Start year to retrieve and process archival SEAS5 data. Must be between 1981 and 2023
                        (default: 1981). Only applies for `--source mars`
  --end END, -e END     End year to retrieve and process archival SEAS5 data. Must be between 1981 and 2024
                        (default: 2024). Only applies for `--source mars`
  --update              Will check AWS bucket for updated data from the current month.
```

This code is also configured as a Job on Databricks, called "Run SEAS5". This can be triggered manually and has been used for bulk tasks (ie. more than a couple years) due to significantly improved performance.

### Example usage

1. Process the full archive from 1981 to 2024 and save all outputs to `prod` Azure container:

```
python run_seas5.py -m prod
```

2. Process MARS data from 2000 to 2010 and save all outputs to `dev` Azure container

```
python run_seas5.py -s 2000 -e 2011 -m dev
```

3. Process the current month's outputs from AWS and save locally

```
python run_seas5.py --update
```

## Processing details

### Raw MARS files:
Global, monthly precipitation forecasts are downloaded in yearly `.grib` files. Each raw `.grib` contains all ensemble members (26 or 51, depending on the year) and lead times (0-6 months ahead). See [this JIRA ticket](https://humanitarian.atlassian.net/browse/DSCI-539?focusedCommentId=177527) for more detailed docs on how the MARS API call is parameterized. All raw `.grib` data is stored in Azure storage container under `raster/seas5/mars/raw/`.

### Raw AWS files:

TODO

### Processed files:
The `.grib` file from each year is processed to output 84 cloud-optimized-geotiffs (`.tif`):
1. Take the mean of all ensemble members
2. Separate by publication month and lead time
3. Set a CRS (`EPSG:4326`)

All processed files are saved to the `prod` Azure storage container under `raster/seas5/*/processed/`. Files are named `tprate_em_i{pub_date}_lt{leadtime}.tif`. Note that `lt0` is when valid_date=pub_date.
