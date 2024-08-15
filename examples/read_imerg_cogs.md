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

# Read [IMERG Global Precipitation Measurement](https://gpm.nasa.gov/data/imerg) COGs

This notebook demos basic functionality to read from our team's store of IMERG precipitation, stored as COGs and downloaded originally from the [IMERG v7 archive](https://gpm1.gesdisc.eosdis.nasa.gov/data/GPM_L3/GPM_3IMERGDL.07/).

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

PROD_BLOB_SAS = os.getenv("DSCI_AZ_SAS_PROD")
PROD_BLOB_NAME = os.getenv("STORAGE_ACCOUNT_PROD")
RASTER_CONTAINER_NAME = os.getenv("CONTAINER_RASTER")
PROD_BLOB_URL = f"https://{PROD_BLOB_NAME}.blob.core.windows.net/"
PROD_BLOB_GLB_URL = PROD_BLOB_URL + RASTER_CONTAINER_NAME + "?" + PROD_BLOB_SAS

prod_rst_container_client = ContainerClient.from_container_url(PROD_BLOB_GLB_URL)
```

## 1. Load in all COGs from 2001 and join into a single `DataSet` object

Start by getting all the blob names from 2000

```python
YEAR = 2001

blob_names = [
        x.name
        for x in prod_rst_container_client.list_blobs(
            name_starts_with=f"imerg/v7/late/processed/imerg-daily-late-{YEAR}"
        )
    ]

# For a single year's worth of data there should be 365 files
assert len(blob_names) == 365
```

Now we can loop through all of them and concatenate to create a single `DataSet` in `xarray`.


```python
das = []
for blob_name in tqdm.tqdm(blob_names):
    cog_url = (
        f"https://{PROD_BLOB_NAME}.blob.core.windows.net/{RASTER_CONTAINER_NAME}/"
        f"{blob_name}?{PROD_BLOB_SAS}"
    )

    # TODO: Probably need to play with these chunk sizes
    da_in = rxr.open_rasterio(
        cog_url, masked=True, chunks={"band": 1, "x": 225, "y": 900}
    )

    date_in = pd.to_datetime(blob_name.split(".")[0][-10:])

    da_in = da_in.squeeze(drop=True)
    da_in["date"] = date_in
    da_in = da_in.expand_dims(["date"])
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


For example, here is the data for the 1st of Jan 2001:

```python
ds.sel({"date": "2001-01-01"}).plot()
```
