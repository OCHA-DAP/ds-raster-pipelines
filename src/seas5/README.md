## Usage

The pipeline can be run locally from the command line by calling the following from the root level directory: 

```
python run_seas5.py <scope> <start_year> <end_year>
```

- `<scope>`:  Either `global` or `test`. `global` will download data for the full planet and `test` will use a bounding box around Afghanistan. `test` should be used during development to download smaller subsets of data from MARS. 
- `<start_year>`: The year to begin downloading annual data for. Must be after 1980. 
- `<end_year>`: The year to download annual data until (not inclusive). Must be before 2023. 

This will create outputs in two places: 

1) A single raw `.grib` file for each year will be saved to the `dev` Azure storage container under `global/mars/raw/`
2) For each year, 84 `.tif` files will be saved to the `prod` Azure storage container under `raster/seas5/`. See the section below for more details.

This code is also configured as a Job on Databricks, called "Update SEAS5 Archive". This can be triggered manually and has been used for bulk tasks (ie. more than a couple years) due to significantly improved performance. 

## Processing details

### Raw files:
Global, monthly precipitation forecasts are downloaded in yearly `.grib` files. Each raw `.grib` contains all ensemble members (26 or 51, depending on the year) and lead times (0-6 months ahead). See [this JIRA ticket](https://humanitarian.atlassian.net/browse/DSCI-539?focusedCommentId=177527) for more detailed docs on how the MARS API call is parameterized. All raw `.grib` data is stored in the `dev` Azure storage container under `global/mars/raw/`. Files are named `seas5_mars_tprate_{year}.grib`. 

### Processed files: 
The `.grib` file from each year is processed to output 84 cloud-optimized-geotiffs (`.tif`): 
1. Take the mean of all ensemble members
2. Separate by publication month and lead time
3. Set a CRS (`EPSG:4326`)

All processed files are saved to the `prod` Azure storage container under `raster/seas5/`. Files are named `seas5_mars_tprate_em_i{pub_date}_lt{leadtime}.tif`. Note that `lt0` is when valid_date=pub_date. 