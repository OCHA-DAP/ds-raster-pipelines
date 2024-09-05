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
import matplotlib.pyplot as plt
import plotly.graph_objects as go

load_dotenv()

PROD_BLOB_SAS = os.getenv("DSCI_AZ_SAS_PROD")
PROD_BLOB_NAME = "imb0chd0prod"
PROD_BLOB_URL = f"https://{PROD_BLOB_NAME}.blob.core.windows.net/"
RASTER_CONTAINER_NAME = "raster"
PROD_BLOB_GLB_URL = PROD_BLOB_URL + RASTER_CONTAINER_NAME + "?" + PROD_BLOB_SAS

prod_rst_container_client = ContainerClient.from_container_url(PROD_BLOB_GLB_URL)
```

## Read in the processed data

```python
# Read in the two rasters
mars_name = "../test_outputs/seas5/mars/processed/daily_precip_em_i1990-12-01_lt6.tif"
mars_cog_url = (
    f"https://{PROD_BLOB_NAME}.blob.core.windows.net/{RASTER_CONTAINER_NAME}/"
    f"{mars_name}?{PROD_BLOB_SAS}"
)
da_mars = rxr.open_rasterio(mars_name)
da_mars["date"] = "1990-12-01"

aws_name = "seas5/aws/processed/tprate_em_i2024-07-01_lt0.tif"
aws_cog_url = (
    f"https://{PROD_BLOB_NAME}.blob.core.windows.net/{RASTER_CONTAINER_NAME}/"
    f"{aws_name}?{PROD_BLOB_SAS}"
)
da_aws = rxr.open_rasterio(
    aws_cog_url, chunks={"band": 1, "x": 225, "y": 900}
)
da_aws["date"] = "2024-07-01"

# Stack the two different rasters
#da_aws = da_aws.rio.reproject_match(da_mars)
#ds = xr.concat([da_aws, da_mars], dim="date")

```

```python
da_mars.x.values
```

```python
# What happens if we stack the two rasters?
ds = xr.concat([da_aws, da_mars], dim="date")
# Something has clearly gone wrong here are the x and y dimensions are now much larger than they need to be
ds
```

```python
# Try overlaying the two on top of each other
# It's a bit hard to tell, but it looks like things are aligned...
fig = go.Figure()

fig.add_trace(go.Heatmap(
        z=da_aws.values[0],
        x=da_aws.x.values,
        y=da_aws.y.values,
        opacity=0.5
))
fig.add_trace(go.Heatmap(
        z=da_mars.values[0],
        x=da_mars.x.values,
        y=da_mars.y.values,
        opacity=0.5,
        colorscale="Blues"
))
fig.update_layout(yaxis_scaleanchor="x")
fig.update_layout(template="simple_white")
fig.show()
```

```python
# Let's check that the lat/lon ranges match -- very slightly off?
print(f"MARS longitudes range from {float(da_mars.x.min())} to {float(da_mars.x.max())}")
print(f"AWS longitudes range from {float(da_aws.x.min())} to {float(da_aws.x.max())}")

print(f"MARS latitudes range from {float(da_mars.y.min())} to {float(da_mars.y.max())}")
print(f"AWS latitudes range from {float(da_aws.y.min())} to {float(da_aws.y.max())}")

# What about the size of each? Looks consistent here.
print(f"MARS lat/lon grid is {len(da_mars.y.values)}/{len(da_mars.x.values)}")
print(f"AWS lat/lon grid is {len(da_aws.y.values)}/{len(da_aws.x.values)}")
```

```python
# We can also compare the geospatial information with each
print(da_mars.spatial_ref.attrs['crs_wkt'] == da_aws.spatial_ref.attrs['crs_wkt'])
print(da_mars.spatial_ref.attrs['GeoTransform'] == da_aws.spatial_ref.attrs['GeoTransform'])

# Very slight misalignment between geotransform of both options
print(da_mars.spatial_ref.attrs['GeoTransform'])
print(da_aws.spatial_ref.attrs['GeoTransform'])
```

```python
# Now what about checking the difference between the lats and longs in both datasets?
diff_x = da_mars.x.values - da_aws.x.values
diff_y = da_mars.y.values - da_aws.y.values
```

```python
plt.hist(diff_x, bins=20)
```

```python
plt.hist(diff_y, bins=20)
```

```python
# Let's try rounding all coordinates to 4 decimal places
# (Which feels conservatively over-precise)
da_aws['y'] = np.round(da_aws['y'].values, 4)
da_aws['x'] = np.round(da_aws['x'].values, 4)

da_mars['y'] = np.round(da_mars['y'].values, 4)
da_mars['x'] = np.round(da_mars['x'].values, 4)
```

```python
# Things look pretty good now!
ds = xr.concat([da_mars, da_aws], dim="date")
ds
```

```python
ds.sel(date='1990-01-01').plot()
```

```python
# What if we zoom in closer and look at the AFG bounding box
minx, miny, maxx, maxy = 60, 30, 65, 35
m = da_mars.sel(x=slice(minx, maxx), y=slice(maxy, miny))
a = da_aws.sel(x=slice(minx, maxx), y=slice(maxy, miny))

# Grid is lining up quite nicely here
fig, ax = plt.subplots()
m.plot(alpha=0.5, ax=ax, cmap="gray", edgecolors='k')
a.plot(alpha=0.5, ax=ax, cmap=plt.cm.Reds)
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
# It looks like this is coming from different "GeoTransform" specifications?
# Although this should be the same as the rio.transform above, which are equal?
print(da_mars.spatial_ref.attrs["GeoTransform"])
print(da_aws.spatial_ref.attrs["GeoTransform"])
```

## Read in the raw data

```python
da_mars_raw = xr.open_dataset("../test_outputs/seas5/mars/raw/tprate_1990.grib")
da_aws_raw = xr.open_dataset("../test_outputs/seas5/aws/raw/T8L0701000001______1.grib", filter_by_keys={'dataType': 'fcmean'})
```

```python
# Let's check that the lat/lon ranges match -- looks good here
print(f"MARS longitudes range from {float(da_mars_raw.longitude.min())} to {float(da_mars_raw.longitude.max())}")
print(f"AWS longitudes range from {float(da_aws_raw.longitude.min())} to {float(da_aws_raw.longitude.max())}")

print(f"MARS latitudes range from {float(da_mars_raw.latitude.min())} to {float(da_mars_raw.latitude.max())}")
print(f"AWS latitudes range from {float(da_aws_raw.latitude.min())} to {float(da_aws_raw.latitude.max())}")

# What about the size of each? Still looking consistent
print(f"MARS lat/lon grid is {len(da_mars_raw.latitude.values)}/{len(da_mars_raw.longitude.values)}")
print(f"AWS lat/lon grid is {len(da_aws_raw.latitude.values)}/{len(da_aws_raw.longitude.values)}")
```

```python
# Again, what if we look at the difference between the x and y values
# In the raw data, doesn't seem to be any differences...
diff_x = da_mars_raw.longitude.values - da_aws_raw.longitude.values
diff_y = da_mars_raw.longitude.values - da_aws_raw.longitude.values
```

```python
plt.hist(diff_x)
```

```python
plt.hist(diff_y)
```

```python
# Select one slice from both datasets
aws_sel = da_aws_raw.sel(number=1)['tprate']
mars_sel = da_mars_raw.sel(number=1, time="1990-01-01", step=np.timedelta64(2678400000000000))['tprate']
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
# Things seem totally fine here, which is odd...

print("AWS Source:\n-------------------\n")
print_raster(aws_sel)
print("MARS Source:\n-------------------\n")
print_raster(mars_sel)
```

```python

```

```python
# Again, things are looking fine...?
ds_new = xr.concat([aws_sel, mars_sel], dim="date")
ds_mean = ds_new.mean(dim="date")
ds_mean.plot()
```

```python
ds
```
