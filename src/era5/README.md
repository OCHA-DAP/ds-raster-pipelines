# ECMWF ERA5 Reanalysis

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

## Processing Methodology

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

### Processed outputs

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

## FAQs

### What is the resolution and extent of the raster grid?

<details>
This data is provided at 0.25x0.25 degree resolution with x values (longitude) ranging from 0 to 359.75, and y values (latitude) ranging from 90 to (-90).
</details>

### What does the `tp` variable measure specifically?

<details>
`tp` is a measure of the total amount of water accumulated over **one day**, measured in meters.
</details>

### How quickly will the previous month's data be processed and uploaded?

<details>
To be scheduled. Should be by the 5th or 6th of the following month.
</details>


### What is a reanalysis?

<details>

> Reanalysis combines model data with observations from across the world into a globally complete and consistent dataset using the laws of physics. This principle, called data assimilation, is based on the method used by numerical weather prediction centres, where every so many hours (12 hours at ECMWF) a previous forecast is combined with newly available observations in an optimal way to produce a new best estimate of the state of the atmosphere, called analysis, from which an updated, improved forecast is issued. Reanalysis works in the same way, but at reduced resolution to allow for the provision of a dataset spanning back several decades.

[src](https://cds-beta.climate.copernicus.eu/datasets/reanalysis-era5-single-levels-monthly-means?tab=overview)

</details>
