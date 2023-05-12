from herbie import Herbie, wgrib2
from pathlib import Path
import pandas as pd

import xarray as xr
from toolbox import EasyMap, ccrs, pc

H = Herbie(
    "2022-08-13 12:00",  # model run date
    model="hrrr",        # model name
    product="sfc",       # model produce name (model dependent)
    fxx=9,               # forecast lead time
)

searches = ["CIN", "LFTX"]

for term in searches:
    search = (H.inventory(searchString=term))
    print(f"Search for {term} returned {search}\n")

#Todo: go back through, see which variables use all searchterms
"""
List of search terms using all results:
DPT, TMP, REFC, DZDT, MSTAV, CNWAT, SPFH, WIND, PRATE, SFCR, FRICV, SHTFL, LHTFL, VEG, GFLUX, CAPE, CIN, PWAT, LCDC, 
MCDC, HCDC, DSWRF, DLWRF, HLCY, USTM, VSTM, VUCSH, VVCSH, HPBL, CANGLE, ESP

List of search terms using some results:
:HGT:surface:, :HGT:equilibrium level:, :UGRD:300 mb:, :UGRD:500 mb:, :UGRD:700 mb:, :UGRD:850 mb:, :UGRD:925 mb:
:UGRD:1000 mb:, :UGRD:10 m above ground:, :VGRD:300 mb:, :VGRD:500 mb:, :VGRD:700 mb:, :VGRD:850 mb:, :VGRD:925 mb:
:VGRD:1000 mb:, :VGRD:10 m above ground:, :RH:2 m above ground:, :LFTX:500-1000 mb:, :TCDC:entire atmosphere:,
:PRES:cloud base:, :PRES:cloud top:, :PRES:0C isotherm:, :USWRF:surface:, :ULWRF:surface:

List of all search terms:

Dew Point: (all)
:DPT:500 mb:
:DPT:700 mb: 
:DPT:850 mb: 
:DPT:925 mb: 
:DPT:1000 mb: 
:DPT:2 m above ground:

Temperature: (all)
:TMP:500 mb: 
:TMP:700 mb: 
:TMP:850 mb: 
:TMP:925 mb: 
:TMP:1000 mb: 
:TMP:surface: 
:TMP:2 m above ground: 

HGT:
:HGT:surface:
:HGT:equilibrium level:

Reflectivity: (all)
:REFC:entire atmosphere:

U Component of Wind:
:UGRD:300 mb:
:UGRD:500 mb:
:UGRD:700 mb:
:UGRD:850 mb:
:UGRD:925 mb:
:UGRD:1000 mb:
:UGRD:10 m above ground:

V Component of Wind:
:VGRD:300 mb:
:VGRD:500 mb:
:VGRD:700 mb:
:VGRD:850 mb:
:VGRD:925 mb:
:VGRD:1000 mb:
:VGRD:10 m above ground:

DZDT: (all)
:DZDT:700 mb:
:DZDT:0.5-0.8 sigma layer:

MSTAV: (all)
:MSTAV:0 m underground:

CNWAT: (all)
:CNWAT:surface:

SPFH: (all)
:SPFH:2 m above ground:

RH:
:RH:2 m above ground:

Wind: (all)
:WIND:10 m above ground:

PRATE: (all)
:PRATE:surface:

SFCR: (all)
:SFCR:surface:

FRICV: (all)
:FRICV:surface:

SHTFL: (all)
:SHTFL:surface:

LHTFL: (all)
:LHTFL:surface:

VEG: (all)
:VEG:surface:

GFLUX: (all)
:GFLUX:surface:

LFTX:
:LFTX:500-1000 mb:

CAPE: (all)
:CAPE:surface:
:CAPE:90-0 mb above ground:
:CAPE:180-0 mb above ground:
:CAPE:255-0 mb above ground:
:CAPE:0-3000 m above ground:

CIN: (all)
:CIN:surface:
:CIN:90-0 mb above ground:
:CIN:180-0 mb above ground:
:CIN:255-0 mb above ground:

PWAT: (all)
:PWAT:entire atmosphere (considered as a single layer):

Low cloud cover: (all)
:LCDC:low cloud layer:

Medium cloud cover: (all)
:MCDC:middle cloud layer:

High cloud cover: (all)
:HCDC:high cloud layer:

Total cloud cover:
:TCDC:entire atmosphere:

Pressure:
:PRES:cloud base:
:PRES:cloud top:
:PRES:0C isotherm:

DSWRF: (all)
:DSWRF:surface:

DLWRF: (all)
:DLWRF:surface:

USWRF:
:USWRF:surface:

ULWRF:
:ULWRF:surface:

HLCY: (all)
:HLCY:1000-0 m above ground:
:HLCY:3000-0 m above ground:

USTM: (all)
:USTM:0-6000 m above ground:

VSTM: (all)
:VSTM:0-6000 m above ground:

VUCSH: (all)
:VUCSH:0-1000 m above ground:
:VUCSH:0-6000 m above ground:

VVCSH: (all)
:VVCSH:0-1000 m above ground:
:VVCSH:0-6000 m above ground:

HPBL: (all)
:HPBL:surface:

CANGLE: (all)
:CANGLE:0-500 m above ground:

ESP: (all)
:ESP:0-3000 m above ground:

"""
