## Usage

The pipeline can be run locally from the command line: 

```
usage: run_imerg.py [-h] [--start-date START] [--end-date END] ...

options:
  -h, --help                    Show this help message and exit.
  -caf, --create-auth-files     Create authorization files for accessing IMERG datasets.
  --start-date, -s              Start date to retrieve and process archival IMERG data. Format: '%Y-%m-%d.
  --end-date, -e                End year to retrieve and process archival IMERG data. Format: '%Y-%m-%d.
  --mode, -m                    Run the pipeline in local/dev/prod mode.
  --run, -r                     E for early run, L for late run.
  --version, -v                 IMERG version (7 is technically 07B) or 6.
  --save-raw, -sr               Will save the unprocessed file to specified location.
```

When not run in local mode, this will create outputs in two places: 

1) Raw`.nc4` files for date will be saved to the `dev` or `prod` Azure storage container under `raster/imerg/{version}/raw/`
2) For each date, `.tif` files will be saved to the `dev` or `prod` Azure storage container under `raster/imerg/{version}/processed/`. See the section below for more details.

This code is also configured as a Job on Databricks, called "Run IMERG". This can be triggered manually and has been used for bulk tasks (ie. more than a couple years) due to significantly improved performance. 

### Example usage

1. Process the full IMERG v7 archive from 2000 to the current date and save all outputs to appropriate locations on Azure:

```
python run_imerg.py -e {today in %Y-%m-%d format} -m {dev/prod}
```

2. Process data from January 2010 to January 2020 and save all outputs to appropriate locations on Azure: 

```
python run_imerg.py -s 2000-01-01 -e 2011-01-01
```

3. Test the pipeline by locally downloading and processing data only from 1990:

```
python run_imerg.py -s 1990-01-01 -e 1990-01-31 -m local
```

## Processing details

### Raw files:
Global daily rainfall `.nc4` files. All raw `.nc4` data can be found in the `dev` or `prod` Azure storage containers under `raster/imerg/{version}/raw/`.
Files are named `imerg-daily-{late/early}-YYYY-MM-DD.nc4`. 

### Processed files: 
The `.nc4` file from each date is processed to output 84 cloud-optimized-geotiffs (`.tif`)

All processed files are saved to the `prod` Azure storage container under `raster/imerg/{version}/processed/`. Files are named `imerg-daily-{late/early}-YYYY-MM-DD.tif`. 