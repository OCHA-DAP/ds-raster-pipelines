---
jupyter:
  jupytext:
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.16.3
  kernelspec:
    display_name: venv
    language: python
    name: python3
---

# Exploring ECMWF Temporal Variables

### SEAS5

Let's spend a bit of time here investigating the various temporal parameters associated with ECMWF's SEAS5 outputs. We've requested data across 7 forecast months, from baseline time `t`. Simply, I want to know where we start indexing forecast months from. With respect to `t` (eg. 1990-01-01), does forecast month 1 have a leadtime of 0 months (so is for January), or 1 month (so is for February)?

Note that with `cfgrib`, we can use the `backend_kwargs` argument in `xr.open_dataset()` to specify `time_dims` to read in (see [here](https://github.com/ecmwf/cfgrib/issues/97#issuecomment-557190695)). But let's start by just reading in the dataset as usual, where we get `time` and `step` as out temporal dimensions.


```python
import os
from dotenv import load_dotenv
import numpy as np

from azure.storage.blob import BlobServiceClient

load_dotenv()

SAS_TOKEN_PROD = os.getenv("DSCI_AZ_SAS_PROD")
STORAGE_ACCOUNT_PROD = "imb0chd0prod"
base_url = f"https://{STORAGE_ACCOUNT_PROD}.blob.core.windows.net"
sas_url = f"{base_url}/raster/seas5/mars/raw/tprate_1990.grib" f"?{SAS_TOKEN_PROD}"
blob_client = BlobClient.from_blob_url(blob_url=sas_url)

# Download file locally -- just run this once!
download_file_path = os.path.join(os.getcwd(), "tprate_1990.grib")
with open("tprate_1990.grib", "wb") as download_file:
    download_file.write(blob_client.download_blob().readall())
```

```python
import xarray as xr

ds = xr.open_dataset(
    "tprate_1990.grib",
    engine="cfgrib"
)
ds
```

Let's select a specific initial forecast time and loop through all `step` options. How many of them have data? Since we have 7 leadtime months, we would expect there to just be 7 slices with data, which looks to be the case!

```python
initial_time = "1990-01-01"

steps = ds.step.values

has_data = []
steps_data = []
for step in steps:
    arr = ds.sel({"time": initial_time, "number": 1, "step": step})["tprate"].values
    x = bool(~np.all(np.isnan(arr)))
    if x:
        has_data.append(x)
        steps_data.append(step)

assert(len(has_data) == 7)
```

What is the value of each `step`? This should tell us when each forecast applies to. Assuming not a leap year, here is how many days away the first of each month is from Jan 1:

- January 1st: 0 days
- February 1st: 31 days
- March 1st: 59 days
- April 1st: 90 days
- May 1st: 120 days
- June 1st: 151 days
- July 1st: 181 days
- August 1st: 212 days


```python
print([str(step.astype("timedelta64[D]")) for step in steps_data])
```

This seems to indicate that we **don't** have any forecast with lt=0 (months). If this were the case, I would expect to see a `step` of 0. At minimum, we have a step of 31 days, which corresponds to a leadtime of 1 month. This is surprising, as it seems to contradict [this comment](https://github.com/ecmwf/cfgrib/issues/97#issuecomment-554795840) from GitHub:

> As you can see, grib_to_netcdf is able to understand that the GRIB file has been written using local definitions (specifically table 16), and therefore it populates the time coordinate with the right values, i.e. Nov/2019 is the sensible value (label) for forecastMonth=1 in a monthly means file with nominal start date 20191101 and not Dec/2019 as it might be interpreted from the computed values validityDate/validityTime. These last sentences are not a guess, an interpretation or an expression of a personal preference, it is an explanation on how seasonal forecast monthly means GRIB files have been consistently encoded for a long time at ECMWF.

Let's take a look at some of the other temporal values to see if they help clarify things:

```python
ds = xr.open_dataset(
    "tprate_1990.grib",
    engine="cfgrib",
    backend_kwargs=dict(time_dims=("valid_time", "verifying_time", "forecastMonth", "time", "step"))
)

ds
```

Let's take the first slice across the `step` dimension that has data for `time` = `1990-01-01`. We'll also just take the first ensemble member to reduce dimensionality. This should be the monthly forecast for the time period beginning 31 days after Jan 1, 1990 -- so Feb 1, 1990.

What are the `forecastMonth` values for this data? I would assume that `forecastMonth` is a mapping to `step`. So if we're selecting data with a step of 31 days, then we should only have data for `forecastMonth = 1`.

```python
ds_sel = ds.sel({"time": initial_time, "step": steps_data[0], "number": 1})

fcmonths = ds.forecastMonth.values

has_data = []
months = []
for month in fcmonths:
    arr = ds_sel.sel({"forecastMonth": month})["tprate"].values
    x = bool(~np.all(np.isnan(arr)))
    if x:
        has_data.append(x)
        months.append(int(month))

assert(len(has_data) == 1)
assert(months[0] == 1)
```

This is correct! Now what do `verifying_time` and `valid_time` tell us? Once we have selected the only `forecastMonth` with data, we have only 1 `valid_time` option and 18 options for `verifying_time`.

The `valid_time` is 01-02-1990 (Feb 1), which confirms our understanding that the first forecast is valid as of that date. `valid_time` **should** be `time` + `step`!

```python
ds_sel = ds_sel.sel({"forecastMonth" : months[0]})

print(ds_sel.valid_time.values)
```

What about `verifying_time`? This is where things are getting a bit more confusing...

We only have 1 `verifying_time` that has any values. This corresponds to our initial `time` value, which is the initial forecast time. This is a bit surprising, since I'd expect this to be the same as `valid_time` (as they both have 18 values). Based on [this issue](https://github.com/ecmwf/cfgrib/issues/97), I'd expect `verifying_time` to also be the start date of the month that the forecast is valid for.

```python
vf_times = ds_sel.verifying_time.values

has_data = []
times = []
for time in vf_times:
    arr = ds_sel.sel({"verifying_time": time})["tprate"].values
    x = bool(~np.all(np.isnan(arr)))
    if x:
        has_data.append(x)
        times.append(time)

print(times)
```

It's odd that we have `verifying_time` values that exceed our options for `time` -- ie. they extend into 1991. Is there any data here? I would expect not?

Moreover, both `valid_time` and `verifying_time` have 18 values. Each `valid_time` value is offset from `verifying_time` by 1 month. What is the difference between these two?

```python
x = ds.sel({"verifying_time": "1991-06-01", "number": 1})

for month in x.forecastMonth.values:
    arr = x.sel({"forecastMonth": month})["tprate"].values
    y = bool(~np.all(np.isnan(arr)))
    if y:
        print(month)
    for time in x.time.values:
        arr_ = x.sel({"time": time, "forecastMonth": month})["tprate"].values
        y_ = bool(~np.all(np.isnan(arr_)))
        if y_:
            print(time)
```

What if we read in the dataset again so that `valid_time` is a dimension in itself (rather than a coord of `time` and `step`).

Let's say I want to get the 1st member of the 7 month leadtime forecast for January 1991. This means the forecast would be issued June 1990. So based on what we learned above, I'd want:
- `forecastMonth = 7`
- `valid_time = 1991-01-01`
- `time = 1990-06-01`
- `number = 1`

```python
ds = xr.open_dataset(
    "tprate_1990.grib",
    engine="cfgrib",
    backend_kwargs=dict(time_dims=("valid_time", "verifying_time", "time", "forecastMonth"))
)

x = ds.sel({
    "time": "1990-12-01",
    "forecastMonth": 7,
    "number": 1,
    "valid_time": "1991-07-01"
})["tprate"]

x
```

The resulting data still has a `verifying_time` dimension with 18 dates. Which of these have data?

```python
for t in x.verifying_time.values:
    arr = x.sel({"verifying_time": t}).values
    y = bool(~np.all(np.isnan(arr)))
    if y:
        print(t)

```

So the `verifying_time` for this is June 1, 1991. What does this correspond to?

Given the inputs below, which month does the forecasted data apply to? And what is the leadtime for that forecast?

```python
x = ds.sel({
    "time": "1990-12-01",
    "forecastMonth": 7,
    "number": 1,
    "verifying_time": "1991-06-01",
    "valid_time": "1991-07-01"
})["tprate"]

x.plot()
```
