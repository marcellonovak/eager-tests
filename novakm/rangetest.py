from herbie import Herbie, wgrib2
from datetime import datetime, timedelta

# TODO:
# Figure out why occasionally there's a H.download(searchstring) error:
# "No index file was found for None"
# Download the full file first (with `H.download()`).
# You will need to remake the Herbie object (H = `Herbie()`)
# or delete this cached property: `del H.index_as_dataframe()`

ss = ":(?:DPT|TMP|DZDT|MSTAV|CNWAT|SPFH|WIND|PRATE|SFCR|FRICV|SHTFL|LHTFL|VEG|GFLUX|CAPE|CIN|"\
     "PWAT|LCDC|MCDC|HCDC|DSWRF|DLWRF|HLCY|USTM|VSTM|VUCSH|VVCSH|HPBL|CANGLE|ESP|REFC|"\
     "[U|V]GRD:[3|5|7|8|9|10][0|5]0 mb|[U|V]GRD:10 m above ground|U[S|L]WRF:surface|RH:2 m above ground|"\
     "LFTX:500-1000 mb|TCDC:entire atmospher|HGT:[surface|equilibrum level]|PRES:cloud[base|top]|" \
     "PRES:0C isotherm)"

# User input dates
startDateStr = "2015-06-10 00:00"
targetDateStr = "2015-06-18 00:00"  #"2016-06-15 00:00"

# User input geographic range
extent = (-116, -107, 30, 38)

# Converted dates
currentDatetime = datetime.strptime(startDateStr, "%Y-%m-%d %H:%M")  # (Initialized at start)
targetDatetime = datetime.strptime(targetDateStr, "%Y-%m-%d %H:%M") 

# Print starting message with date info
print(f"Starting data collection for range {startDateStr} to {targetDateStr}")

# While the loop hasn't reached the target...
while currentDatetime < targetDatetime:

    # Convert current datetime for usage
    currentDateStr = currentDatetime.strftime("%Y-%m-%d %H:%M")

    # Get Herbie object for current datetime
    H = Herbie(
        currentDateStr,
        model='hrrr',
        product='sfc',
        fxx=0,
        save_dir='./data',
        with_xarray=True
    )

    # Herbie download
    print(currentDateStr)
    H.download(ss)

    myFile = H.get_localFilePath(ss)
    myFile, myFile.exists()

    # Actually subset the file we requested
    subsetFile = wgrib2.region(myFile, extent, name="Arizona")

    # Increment current datetime by 1 hour, print progress
    currentDatetime += timedelta(hours=1)
