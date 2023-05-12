from herbie import Herbie, wgrib2
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from cartopy import crs as ccrs, feature as cfeature
import numpy as np
import warnings
import xarray as xr

# Testing out the concatenation of grib files

'''
List of search terms using all results:
DPT, TMP, REFC, DZDT, MSTAV, CNWAT, SPFH, WIND, PRATE, SFCR, FRICV, SHTFL, LHTFL, VEG, GFLUX, CAPE, CIN, PWAT, LCDC, 
MCDC, HCDC, DSWRF, DLWRF, HLCY, USTM, VSTM, VUCSH, VVCSH, HPBL, CANGLE, ESP

List of search terms using some results:
:HGT:surface:, :HGT:equilibrium level:, :UGRD:300 mb:, :UGRD:500 mb:, :UGRD:700 mb:, :UGRD:850 mb:, :UGRD:925 mb:
:UGRD:1000 mb:, :UGRD:10 m above ground:, :VGRD:300 mb:, :VGRD:500 mb:, :VGRD:700 mb:, :VGRD:850 mb:, :VGRD:925 mb:
:VGRD:1000 mb:, :VGRD:10 m above ground:, :RH:2 m above ground:, :LFTX:500-1000 mb:, :TCDC:entire atmosphere:,
:PRES:cloud base:, :PRES:cloud top:, :PRES:0C isotherm:, :USWRF:surface:, :ULWRF:surface:
'''

dateInput = "2022-08-13 12:00"
removeGribBool = False

# Get the model data for a date and time (9-hour forecast)
H = Herbie(
    dateInput,  # model run date
    model="hrrr",        # model name
    product="sfc",       # model produce name (model dependent)
    fxx=9,               # forecast lead time
)

# List of search terms

searchList = ("DPT", "TMP")

dataSetDPT = H.xarray(":DPT:500 mb:", remove_grib=removeGribBool)
dataSetTMP = H.xarray(":TMP:2 m", remove_grib=removeGribBool)

dptAll = (H.xarray(":DPT:", remove_grib=removeGribBool))
combined_merge = xr.merge([dptAll], compat="no_conflicts")
combined_dataset = xr.concat([combined_merge], dim='variable')

print(dptAll)
print(combined_dataset)
print("\n")
print(type(dptAll))
print(type(combined_dataset))
print(type(dataSetDPT))
print("\n")

# print(dataSetDPT)
# print(dataSetTMP)

dataList = [dataSetDPT, dataSetTMP]

# Merge the two datasets along the 'x' and 'y' dimensions
merged = xr.merge([dataSetDPT, dataSetTMP], compat="no_conflicts")

# Concatenate the merged dataset along the 'variable' dimension
concatenated = xr.concat([merged], dim='variable')

# Print the final concatenated dataset
print(concatenated)
