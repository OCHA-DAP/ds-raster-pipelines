## Usage

The pipeline can be run locally from the command line: 

```
usage: run_seas5.py [-h] [--start START] [--end END] [--test]

options:
  -h, --help            show this help message and exit
  --start START, -s START
                        Start year to retrieve and process archival SEAS5 data. Must be
                        between 1981 and 2022 (default: 1981).
  --end END, -e END     End year to retrieve and process archival SEAS5 data. Must be
                        between 1981 and 2022 (default: 2022).
  --test, -t            Run the pipeline in test mode. Will save a subset of outputs
                        locally to 'test_outputs/' and not upload any data to Azure.
```

When not run in test mode, this will create outputs in two places: 

1) A single raw `.grib` file for each year will be saved to the `dev` Azure storage container under `global/mars/raw/`
2) For each year, 84 `.tif` files will be saved to the `prod` Azure storage container under `raster/seas5/`. See the section below for more details.

This code is also configured as a Job on Databricks, called "Update SEAS5 Archive". This can be triggered manually and has been used for bulk tasks (ie. more than a couple years) due to significantly improved performance. 

### Example usage

1. Process the full MARS archive from 1981 to 2022 and save all outputs to appropriate locations on Azure:

```
python run_seas5.py
```

2. Process data from 2000 to 2010 and save all outputs to appropriate locations on Azure: 

```
python run_seas5.py -s 2000 -e 2011
```

3. Test the pipeline by locally downloading and processing data only from 1990:

```
python run_seas5.py -s 1990 -e 1990 -t
```

## Processing details

### Raw files:
Global, monthly precipitation forecasts are downloaded in yearly `.grib` files. Each raw `.grib` contains all ensemble members (26 or 51, depending on the year) and lead times (0-6 months ahead). See [this JIRA ticket](https://humanitarian.atlassian.net/browse/DSCI-539?focusedCommentId=177527) for more detailed docs on how the MARS API call is parameterized. All raw `.grib` data is stored in the `dev` Azure storage container under `global/mars/raw/`. Files are named `seas5_mars_tprate_{year}.grib`. 

### Processed files: 
The `.grib` file from each year is processed to output 84 cloud-optimized-geotiffs (`.tif`): 
1. Take the mean of all ensemble members
2. Separate by publication month and lead time
3. Set a CRS (`EPSG:4326`)

All processed files are saved to the `prod` Azure storage container under `raster/seas5/`. Files are named `seas5_mars_tprate_em_i{pub_date}_lt{leadtime}.tif`. Note that `lt0` is when valid_date=pub_date. 