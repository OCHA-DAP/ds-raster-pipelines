---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.16.3
  kernelspec:
    display_name: Python 3 (ipykernel)
    language: python
    name: python3
---

# Read [SEAS5 Seasonal Forecast](https://www.ecmwf.int/sites/default/files/medialibrary/2017-10/System5_guide.pdf) COGs

This notebook demos basic functionality to read from our team's store of SEAS5 seasonal forecasts, stored as COGs and downloaded originally from the [ECMWF MARS service](https://www.ecmwf.int/en/forecasts/access-forecasts/access-archive-datasets).

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import xarray as xr
import pandas as pd
import tqdm
import os
from dotenv import load_dotenv
from azure.storage.blob import ContainerClient
import rioxarray as rxr

load_dotenv()

DEV_BLOB_SAS = os.getenv("DSCI_AZ_SAS_DEV")
DEV_BLOB_NAME = "imb0chd0dev"
DEV_BLOB_URL = f"https://{DEV_BLOB_NAME}.blob.core.windows.net/"
RASTER_CONTAINER_NAME = "raster"
DEV_BLOB_GLB_URL = DEV_BLOB_URL + RASTER_CONTAINER_NAME + "?" + DEV_BLOB_SAS

dev_rst_container_client = ContainerClient.from_container_url(DEV_BLOB_GLB_URL)
```

## 1. Load in all COGs from 2000 and join into a single `DataSet` object

Start by getting all the blob names from 2000

```python
YEAR = 2002

blob_names = existing_files = [
    x.name
    for x in dev_rst_container_client.list_blobs(
        name_starts_with="seas5/mars/processed/"
    )
    if str(YEAR) in x.name
]

# For a single year's worth of data there should be 12 months * 7 leadtimes' worth of data
assert len(blob_names) == (12 * 7)
```

Now we can loop through all of them and concatenate to create a single `DataSet` in `xarray`.


```python
das = []
for blob_name in tqdm.tqdm(blob_names):
    cog_url = (
        f"https://{DEV_BLOB_NAME}.blob.core.windows.net/{RASTER_CONTAINER_NAME}/"
        f"{blob_name}?{DEV_BLOB_SAS}"
    )

    # TODO: Probably need to play with these chunk sizes
    da_in = rxr.open_rasterio(
        cog_url, masked=True, chunks={"band": 1, "x": 225, "y": 900}
    )

    date_in = pd.to_datetime(blob_name.split(".")[0][-14:-4])
    leadtime = int(blob_name.split(".")[0][-1:])

    da_in = da_in.squeeze(drop=True)
    da_in["date"] = date_in
    da_in["leadtime"] = leadtime
    da_in = da_in.expand_dims(["date", "leadtime"])
    das.append(da_in)

    # Persisting to reduce the number of downstream Dask layers
    da_in = da_in.persist()
    das.append(da_in)

ds = xr.combine_by_coords(das)
```

```python
ds
```

## 2. Plot some sample data


For example, here is the forecast published in Jan 2001, for the following month (February, leadtime of 1):

```python
ds.sel({"date": "2002-01-01", "leadtime": 1}).plot()
```
