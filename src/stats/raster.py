import numpy as np
import pandas as pd
from rasterio.enums import Resampling
from rasterstats import zonal_stats


# On testing values for adm regions in Haiti, this gives the same outputs as
# https://github.com/OCHA-DAP/pa-anticipatory-action/blob/d17031d61612d64e38787fa158dc4fe1660f7379/src/utils_general/raster_manipulation.py#L110
def compute_zonal_statistics(
    da,
    gdf,
    id_col,
    geom_col="geometry",
    lat_coord="y",
    lon_coord="x",
    stats=None,
    all_touched=False,
):
    if not stats:
        stats = ["mean", "std", "min", "max", "sum", "count"]
        percentiles = [f"percentile_{x}" for x in list(range(10, 100, 10))]
        stats.extend(percentiles)

    coords_transform = da.rio.set_spatial_dims(
        x_dim=lon_coord, y_dim=lat_coord
    ).rio.transform()

    stats = zonal_stats(
        vectors=gdf[[geom_col]],
        raster=da.values,
        affine=coords_transform,
        nodata=np.nan,
        all_touched=all_touched,
        stats=stats,
    )
    df_stats = pd.DataFrame(stats).round(2)
    df_stats = gdf[[id_col]].merge(df_stats, left_index=True, right_index=True)
    return df_stats


def upsample_raster(da, resampled_resolution=0.05):
    # Assuming square resolution
    input_resolution = da.rio.resolution()[0]
    upscale_factor = int(input_resolution / resampled_resolution)
    print(f"Upscaling by a factor of {upscale_factor}")

    new_width = da.rio.width * upscale_factor
    new_height = da.rio.height * upscale_factor

    return da.rio.reproject(
        da.rio.crs,
        shape=(new_height, new_width),
        resampling=Resampling.nearest,
        nodata=np.nan,
    )
