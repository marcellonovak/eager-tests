from herbie import Herbie, wgrib2
import xarray

dateInput = "2022-08-13 12:00"
removeGribBool = False

H = Herbie(
    dateInput,  # model run date
    model="hrrr",  # model name
    product="sfc",  # model produce name (model dependent)
    fxx=9,  # forecast lead time
)

# UGRD:300 mb|UGRD:500 mb|UGRD:700 mb|UGRD:850 mb|UGRD:925 mb|UGRD:1000 mb|

regex = "(?:DPT|TMP|DZDT|MSTAV|CNWAT|SPFH|WIND|PRATE|SFCR|FRICV|SHTFL|LHTFL|VEG|GFLUX|CAPE|CIN|PWAT|LCDC|MCDC|HCDC|" \
        "DSWRF|DLWRF|HLCY|USTM|VSTM|VUCSH|VVCSH|VVCSH|HPBL|CANGLE|ESP|REFC|" \
        "[U|V]GRD:[3|5|7|8|9|10][0|5]0 mb|[U|V]GRD:10 m above ground|U[S|L]WRF:surface|RH:2 m above ground|" \
        "LFTX:500-1000 mb|TCDC:entire atmosphere|HGT:[surface|equilibrium level]|PRES:cloud [base|top]|" \
        "PRES:0C isotherm)"

dataSet = H.xarray(regex, remove_grib=removeGribBool)

dataVars = (list(dataSet.data_vars.keys())[0])

print(dataSet)
print(dataVars)
