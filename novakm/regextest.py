from herbie import Herbie, wgrib2
import xarray

# User variables
dateInput = "2022-08-13 12:00"
removeGribBool = False

H = Herbie(         # Initialize Herbie
    dateInput,      # Model run date
    model="hrrr",   # Model name
    product="sfc",  # Model produce name (model dependent)
    fxx=0,          # Forecast lead time (0 for analysis time)
)

# Todo:
# Create 25 different files, for forecast times 0-24 hrs
# Create one file? with all forecast times 0-24 hrs? if possible

# All variable regex (work in progress, need to shorten)
regex = "(?:DPT:500 mb|DPT:700 mb|DPT:850 mb|DPT:925 mb|DPT:1000 mb|DPT:2 m above ground|TMP:500 mb|TMP:700 mb|" \
        "TMP:850 mb|TMP:925 mb|TMP:1000 mb|TMP:surface|TMP:2 m above ground|HGT:surface|HGT:equilibrium level|" \
        "REFC:entire atmosphere|UGRD:300 mb|UGRD:500 mb|UGRD:700 mb|UGRD:850 mb|UGRD:925 mb|UGRD:1000 mb|" \
        "UGRD:10 m above ground|VGRD:300 mb|VGRD:500 mb|VGRD:700 mb|VGRD:850 mb|VGRD:925 mb|VGRD:1000 mb|" \
        "VGRD:10 m above ground|DZDT:700 mb|DZDT:0\.5-0\.8 sigma layer|MSTAV:0 m underground|CNWAT:surface|" \
        "SPFH:2 m above ground|RH:2 m above ground|WIND:10 m above ground|PRATE:surface|SFCR:surface|FRICV:surface|" \
        "SHTFL:surface|LHTFL:surface|VEG:surface|GFLUX:surface|LFTX:500-1000 mb|CAPE:surface|" \
        "CAPE:90-0 mb above ground|CAPE:180-0 mb above ground|CAPE:255-0 mb above ground|CAPE:0-3000 m above ground|" \
        "CIN:surface|CIN:90-0 mb above ground|CIN:180-0 mb above ground|CIN:255-0 mb above ground|" \
        "PWAT:entire atmosphere \(considered as a single layer\)|LCDC:low cloud layer|MCDC:middle cloud layer|" \
        "HCDC:high cloud layer|TCDC:entire atmosphere|PRES:cloud base|PRES:cloud top|PRES:0C isotherm|DSWRF:surface|" \
        "DLWRF:surface|USWRF:surface|ULWRF:surface|HLCY:1000-0 m above ground|HLCY:3000-0 m above ground|" \
        "USTM:0-6000 m above ground|VSTM:0-6000 m above ground|VUCSH:0-1000 m above ground|" \
        "VUCSH:0-6000 m above ground|VVCSH:0-1000 m above ground|VVCSH:0-6000 m above ground|HPBL:surface|" \
        "CANGLE:0-500 m above ground|ESP:0-3000 m above ground)"

dataList = H.xarray(regex, remove_grib=removeGribBool)
dataListVars = []
dataSet = []

index = 1
while index != len(dataList):
    dataSet = xarray.merge([dataList[index - 1], dataList[index]], compat="override")
    tempVars = list(dataList[index - 1].data_vars.keys())
    tempVars.remove("gribfile_projection")
    for item in tempVars:
        dataListVars.append(item)
    index += 1

dataSetList = list(dataSet.data_vars.keys())
dataSetList.sort()
dataListVars.sort()

print(dataSetList)
print(dataListVars)
print(dataList)
