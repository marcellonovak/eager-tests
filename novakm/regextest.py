from herbie import Herbie, wgrib2

dateInput = "2022-08-13 12:00"
removeGribBool = False

H = Herbie(
    dateInput,  # model run date
    model="hrrr",  # model name
    product="sfc",  # model produce name (model dependent)
    fxx=9,  # forecast lead time
)

regex = "(?:DPT|TMP|DZDT|MSTAV|CNWAT|SPFH|WIND|PRATE|SFCR|FRICV|SHTFL|LHTFL|VEG|GFLUX|CAPE|CIN|PWAT|LCDC|MCDC|HCDC|" \
        "DSWRF|DLWRF|HLCY|USTM|VSTM|VUCSH|VVCSH|VVCSH|HPBL|CANGLE|ESP|REFC|" \
        "UGRD:300 mb|UGRD:500 mb|UGRD:700 mb|UGRD:850 mb|UGRD:925 mb|UGRD:1000 mb|UGRD:10 m above ground|" \
        "VGRD:300 mb|VGRD:500 mb|VGRD:700 mb|VGRD:850 mb|VGRD:925 mb|VGRD:1000 mb|VGRD:10 m above ground|" \
        "RH:2 m above ground|LFTX:500-1000 mb|TCDC:entire atmosphere|HGT:surface|HGT:equilibrium level|" \
        "PRES:cloud base|PRES:cloud top|PRES:0C isotherm|USWRF:surface|ULWRF:surface)"

search = H.xarray(regex, remove_grib=removeGribBool)

print(search)
