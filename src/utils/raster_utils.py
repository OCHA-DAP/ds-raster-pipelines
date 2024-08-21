def change_longitude_range(
    ds,
    lon_coord: str,
):
    """
    If longitude ranges from 0 to 360,
    change it to range from -180 to 180.
    Args:
        ds (xarray dataset): dataset that should be transformed
        lon_coord: name of the longitude coordinate

    Returns:
        ds_lon (xarray dataset): dataset with transformed longitude
        coordinates
    """
    ds_lon = ds.assign_coords(
        {lon_coord: (((ds[lon_coord] + 180) % 360) - 180)}
    ).sortby(lon_coord)
    return ds_lon
