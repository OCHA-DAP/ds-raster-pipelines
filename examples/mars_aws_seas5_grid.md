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

```python
import xarray as xr
import pandas as pd
import os
from dotenv import load_dotenv
from azure.storage.blob import ContainerClient
import rioxarray as rxr
import geopandas as gpd
import numpy as np

load_dotenv()

PROD_BLOB_SAS = os.getenv("DSCI_AZ_SAS_PROD")
PROD_BLOB_NAME = "imb0chd0prod"
PROD_BLOB_URL = f"https://{PROD_BLOB_NAME}.blob.core.windows.net/"
RASTER_CONTAINER_NAME = "raster"
PROD_BLOB_GLB_URL = PROD_BLOB_URL + RASTER_CONTAINER_NAME + "?" + PROD_BLOB_SAS

prod_rst_container_client = ContainerClient.from_container_url(PROD_BLOB_GLB_URL)
```

```python
# Read in the two rasters
mars_name = "seas5/mars/processed/tprate_em_i1990-01-01_lt0.tif"
mars_cog_url = (
    f"https://{PROD_BLOB_NAME}.blob.core.windows.net/{RASTER_CONTAINER_NAME}/"
    f"{mars_name}?{PROD_BLOB_SAS}"
)
da_mars = rxr.open_rasterio(
    mars_cog_url, masked=True, chunks={"band": 1, "x": 225, "y": 900}
)
da_mars["date"] = "1990-01-01"

aws_name = "seas5/aws/processed/tprate_em_i2024-07-01_lt0.tif"
aws_cog_url = (
    f"https://{PROD_BLOB_NAME}.blob.core.windows.net/{RASTER_CONTAINER_NAME}/"
    f"{aws_name}?{PROD_BLOB_SAS}"
)
da_aws = rxr.open_rasterio(
    aws_cog_url, masked=True, chunks={"band": 1, "x": 225, "y": 900}
)
da_aws["date"] = "2024-07-01"

# Stack the two different rasters
ds = xr.concat([da_aws, da_mars], dim="date")

```

```python
# Sanity check that the spatial information is the same
da_mars.spatial_ref.attrs['spatial_ref'] == da_aws.spatial_ref.attrs['spatial_ref']
```

```python
# Let's zoom in a bit to see the pixels more closely
# There's definitely some misalignment here...

gdf_adm0 = gpd.read_file("tmp/hti_adm0.shp")
minx, miny, maxx, maxy = gdf_adm0.total_bounds
ds_clip = ds.sel(x=slice(minx, maxx), y=slice(miny, maxy))
ds_clip.mean(dim="date").plot()
```

```python
# It looks like this is coming from different "GeoTransform" specifications
print(da_mars.spatial_ref.attrs["GeoTransform"])
print(da_aws.spatial_ref.attrs["GeoTransform"])
```

```python
# What if we look at the raw .grib files from both MARS and AWS?
minx, miny, maxx, maxy = 60, 29, 75, 38

# MARS is clipped to AFG
da_mars_raw = xr.open_dataset("../test_outputs/seas5/mars/raw/tprate_1990.grib")

# Clip AWS for AFG as well
da_aws_raw = xr.open_dataset("../test_outputs/seas5/aws/raw/T8L0701000001______1.grib", filter_by_keys={'dataType': 'fcmean'})
da_aws_raw = da_aws_raw.sel(longitude=slice(minx, maxx), latitude=slice(maxy, miny))
```

```python
# Select one slice from both datasets
aws_sel = da_aws_raw.sel(number=1)['tprate']
mars_sel = da_mars_raw.sel(number=1, time="1990-01-01", step=np.timedelta64(2678400000000000))['tprate']

# Between both of the above, can see that the latitude values are misaligned by 0.2 degrees!
```

```python
# Now write the CRS on both datasets
aws_sel = aws_sel.rio.write_crs("EPSG:4326").rename({"latitude": "y", "longitude": "x"})
mars_sel = mars_sel.rio.write_crs("EPSG:4326").rename({"latitude": "y", "longitude": "x"})
```

```python
def print_raster(raster):
    print(
        f"shape: {raster.rio.shape}\n"
        f"resolution: {raster.rio.resolution()}\n"
        f"bounds: {raster.rio.bounds()}\n"
        f"sum: {raster.sum().item()}\n"
        f"CRS: {raster.rio.crs}\n"
    )
```

```python
print("AWS Source:\n-------------------\n")
print_raster(aws_sel)
print("MARS Source:\n-------------------\n")
print_raster(mars_sel)
```

```python
# Now try reprojecting to get AWS to match the MARS one
# https://corteva.github.io/rioxarray/html/examples/reproject_match.html
aws_matched = aws_sel.rio.reproject_match(mars_sel)
```

```python
# Now they're the same!
print("New AWS Source:\n-------------------\n")
print_raster(aws_matched)
print("MARS Source:\n-------------------\n")
print_raster(mars_sel)
```

```python
ds_new = xr.concat([aws_matched, mars_sel], dim="date")
```

```python
# Graph looks wonky here because there seems to be some nodata fill at the low end of the latitude,
# likely coming from where we reprojected
ds_new.mean(dim="date").plot()
```

```python
# If we cut that off things look more normal...
# BUT, are we actually losing any data here?
ds_new.mean(dim="date").sel(y=slice(38, 30)).plot()
```
