# Pipeline Usage

All pipelines can be run using the `run_pipeline.py` script from the command line.

## Basic Usage

```
python run_pipeline.py <pipeline_name> [options]
```

Replace `<pipeline_name>` with either `era5`, `seas5`, or `imerg`.

## Common Options

These options are available for both pipelines:

- `--mode {local,dev,prod}`: Specify the mode to run the pipeline in (default: local)
- `--log-level {DEBUG,INFO,WARNING,ERROR,CRITICAL}`: Set the logging level (default: INFO)
- `--use-cache`: Use cached raw data if available

## ERA5 Options

- `--start-year YEAR`: Start year for data processing. Min 1981.
- `--end-year YEAR`: End year for data processing. Max 2024.
- `--update`: Run in update mode (fetches only the data from last month)

## SEAS5 Options

- `--start-year YEAR`: Start year for data processing. Min 1981.
- `--end-year YEAR`: End year for data processing. Max 2024.
- `--update`: Run in update mode (fetches only the data from last month)

## IMERG Options

- `--start-date DATE`, `-s DATE`: Start date to retrieve and process archival IMERG data (format: YYYY-MM-DD, default: yesterday)
- `--end-date DATE`, `-e DATE`: End date to retrieve and process archival IMERG data (format: YYYY-MM-DD, default: today)
- `--run {early,late}`, `-r {early,late}`: Specify 'early' for early run or 'late' for late run (default: late)
- `--version {6,7}`, `-v {6,7}`: IMERG version to use (7 is technically 07B, default: 7)
- `--create-auth-files`, `-caf`: Create authorization files for accessing IMERG datasets

## Examples

1. Run ERA5 pipeline in local mode for years 2020-2022:
   ```
   python run_pipeline.py era5 --mode local --start-year 2020 --end-year 2022
   ```

2. Run SEAS5 pipeline in dev mode with cached data, for 2020-2022:
   ```
   python run_pipeline.py seas5 --mode dev --start-year 2020 --end-year 2022 --use-cache
   ```

3. Update ERA5 data in production mode:
   ```
   python run_pipeline.py era5 --mode prod --update
   ```

4. Run IMERG pipeline to get yesterday's data and save in production storage:
   ```
   python run_pipeline.py imerg --mode prod
   ```

Note: Ensure you have set up the necessary environment variables and dependencies before running the pipelines.
