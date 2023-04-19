from herbie import Herbie, wgrib2
from pathlib import Path
import pandas as pd

import xarray as xr
from toolbox import EasyMap, ccrs, pc

H = Herbie(
    "2022-08-13 12:00",  # model run date
    model="hrrr",        # model name
    product="sfc",       # model produce name (model dependent)
    fxx=6,               # forecast lead time
)

print(H.inventory())
