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

# Exploring [ERA5 Reanalysis](https://cds-beta.climate.copernicus.eu/datasets/reanalysis-era5-single-levels-monthly-means?tab=overview) COGs

This notebook takes a closer look at our team's store of ERA5 reanalysis outputs, stored as COGs in Azure blob storage. This data is downloaded originally from the [CDS Beta API](https://cds-beta.climate.copernicus.eu). We'll demonstrate a basic analysis where we calculate the average accumulated precipitation for the month of August across admin 1 regions in Haiti.

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
import re
from dotenv import load_dotenv
from azure.storage.blob import ContainerClient
import rioxarray as rxr
import zipfile
import io
import geopandas as gpd
import matplotlib.pyplot as plt

load_dotenv()

PROD_BLOB_SAS = os.getenv("DSCI_AZ_SAS_PROD")
PROD_BLOB_NAME = os.getenv("STORAGE_ACCOUNT_PROD")
PROD_BLOB_URL = f"https://{PROD_BLOB_NAME}.blob.core.windows.net/"
RASTER_CONTAINER_NAME = "raster"
PROD_BLOB_GLB_URL = PROD_BLOB_URL + RASTER_CONTAINER_NAME + "?" + PROD_BLOB_SAS

prod_rst_container_client = ContainerClient.from_container_url(PROD_BLOB_GLB_URL)

DEV_BLOB_SAS = os.getenv("DSCI_AZ_SAS_DEV")
DEV_BLOB_NAME = os.getenv("STORAGE_ACCOUNT_DEV")
DEV_BLOB_URL = f"https://{DEV_BLOB_NAME}.blob.core.windows.net/"
DEV_BLOB_PRJ_URL = DEV_BLOB_URL + "projects" + "?" + DEV_BLOB_SAS

dev_prj_container_client = ContainerClient.from_container_url(DEV_BLOB_PRJ_URL)
```

## 1. Reading and exploring a sample file using `xarray`

COGs are all stored in the `era5/monthly/processed/` directory following this convention: `tp_reanalysis_v{yyyy-mm-dd}.tif`. All dates are the 1st of every month (ie. `2022-05-10` won't return any data, but `2022-05-01` will).


Let's first open one of the COGs to understand the data structure. Given an input URL, we can open the COG using `rioxarray`'s [`open_rasterio`](https://corteva.github.io/rioxarray/html/rioxarray.html#rioxarray-open-rasterio) method. Setting `chunks="auto"` (or manually specifying) is optional, but may improve performance by opening the COG as a Dask array.


```python
blob_name = "era5/monthly/processed/tp_reanalysis_v1981-08-01.tif"  # Sample COG for Aug 1981

cog_url = (
    f"https://{PROD_BLOB_NAME}.blob.core.windows.net/{RASTER_CONTAINER_NAME}/"
    f"{blob_name}?{PROD_BLOB_SAS}"
)

da = rxr.open_rasterio(cog_url, chunks="auto")
```

We can learn a lot about the contents of this data from the `attrs`, which are copied over from the original `.grib` file downloaded from ECMWF. Let's make sure we understand what these measurements actually represent.

By getting the ECMWF parameter ID, we can search the ECMWF parameter database to find more information: https://codes.ecmwf.int/grib/param-db/228. From this data we're getting "the total amount of water accumulated over a particular time period", measured in meters. Even though this is a monthly product, **our accumulation period is 1 day**, as stated in [these docs from CDS on "Total Precipitation"](https://cds-beta.climate.copernicus.eu/datasets/reanalysis-era5-single-levels-monthly-means?tab=overview).

```python
print(da.attrs["GRIB_paramId"])
print(da.attrs["long_name"])
print(da.attrs["units"])
```

This data is provided at 0.25x0.25 degree resolution with x values (longitude) ranging from 0-359.75, and y values (latitude) ranging from 90-(-90).

```python
print(da.attrs["GRIB_iDirectionIncrementInDegrees"])
print(da.attrs["GRIB_iDirectionIncrementInDegrees"])

print(da.x.values.min())
print(da.x.values.max())
print(da.y.values.min())
print(da.y.values.max())
```

We can also see that there is no temporal information stored in the `DataArray` object itself, so we'll have to add that back in when creating the stack.

```python
print(da.coords)
```

## 2. Create a stack of all available data from August and join into a single `Dataset` object in `xarray`.

Now let's go ahead and create our stack of data from August.

```python
PREFIX = "era5/monthly/processed"
PATTERN = r"tp_reanalysis_v\d{4}-08-01"

august_cogs = [
    x.name
    for x in prod_rst_container_client.list_blobs(name_starts_with=PREFIX)
    if re.search(PATTERN, x.name)
]
```

There should be 1 file for every year from 1981 to 2023. Let's confirm.

```python
assert len(august_cogs) == (2023 - 1981)
```

Now we can loop through all filenames and create our stack.

```python
das = []
for cog in tqdm.tqdm(august_cogs):
    cog_url = (
        f"https://{PROD_BLOB_NAME}.blob.core.windows.net/{RASTER_CONTAINER_NAME}/"
        f"{cog}?{PROD_BLOB_SAS}"
    )

    da_in = rxr.open_rasterio(cog_url, chunks="auto")
    date_in = pd.to_datetime(cog.split(".")[0][-10:])
    da_in = da_in.squeeze(drop=True)
    da_in["date"] = date_in

    # Depending on the size of the data, we may want to clip to a
    # smaller bounding box here

    # Persisting to reduce the number of downstream Dask layers
    # And improve performance of subsequent computations
    da_in = da_in.persist()
    das.append(da_in)

ds = xr.concat(das, dim="date")
```

To sanity check, let's take a look at the data from one August. Since the longitude coordinates are ranging from 0-360 and our AOI (Haiti) is west of the central meridian, we'll need to translate the coordinates to the -180-180 range.

```python
ds = ds.assign_coords(x=(((ds.x + 180) % 360) - 180))
ds = ds.sortby(ds.x)
ds.sel({"date": "1981-08-01"}).plot()
```

## 3. Clipping and averaging

Suppose we want the average monthly accumulated precipitation in August across Haiti. We'll first read in a saved Shapefile from the `dev` Azure blob.

```python
cod_name = "ds-aa-hti-hurricanes/raw/codab/hti.shp.zip"
blob_client = dev_prj_container_client.get_blob_client(cod_name)
data = blob_client.download_blob().readall()

with zipfile.ZipFile(io.BytesIO(data), "r") as zip_ref:
    zip_ref.extractall("tmp")
    gdf_adm1 = gpd.read_file(f"tmp/hti_adm1.shp")
    gdf_adm0 = gpd.read_file(f"tmp/hti_adm0.shp")
```

If our dataset were larger, we would probably do this clipping when initially reading in the COGs.

```python
ds_clip = ds.rio.clip(gdf_adm0.geometry)
```

Now we'll need to transform the data to get the average monthly accumulated precipitation in mm. Since each observation is the average **daily** precipitation, we'll need to multiply by 30 to get the monthly value. Then we'll convert from meters to millimeters and take the average across all years.

```python
ds_clip_transform = ds_clip * 30 * 1000
da = ds_clip_transform.mean(dim="date")

da.attrs["units"] = "mm"
da.attrs["long_name"] = "Average monthly accumulated precipitation"
```

Let's plot to sanity check.

```python
fig, ax = plt.subplots()
gdf_adm1.boundary.plot(ax=ax)
da.plot(ax=ax)
```

## 4. Calculating raster stats by Admin 1

There are a number of libraries we might use to compute raster stats. For now, we'll run through this manually by looping through each Admin 1.

```python
adm1s = []
means = []

for adm1 in gdf_adm1.ADM1_PCODE:
    gdf_sel = gdf_adm1[gdf_adm1.ADM1_PCODE == adm1]

    # Using all_touched=True since some areas are so small that they don't contain any pixel centroids
    da_sel = da.rio.clip(gdf_sel.geometry, all_touched=True)
    mean_precip = float(da_sel.mean().values)

    adm1s.append(adm1)
    means.append(mean_precip)

data = {
    "ADM1_PCODE": adm1s,
    "MEAN_AUG_PRECIP": means,
}
df_mean = pd.DataFrame(data)
```

We'll merge this data back with the original GeoDataFrame and create an overview map.

```python
gdf_adm1_mean = gdf_adm1.merge(df_mean, on="ADM1_PCODE", how="inner")

fig, ax = plt.subplots(1, 1, figsize=(10, 5))

gdf_adm1_mean.plot(
    column="MEAN_AUG_PRECIP",
    cmap="Blues",
    ax=ax,
    legend=True,
    edgecolor=(0, 0, 0, 0.5),
)
ax.set_axis_off()
ax.set_title(
    "Average August Accumulated Precipitation (mm)",
    fontdict={"fontsize": 18, "fontname": "Arial"},
)
fig.suptitle("Computed from ECMWF ERA5 Reanalysis, 1981-2023", fontsize=10, y=0.88)
```
