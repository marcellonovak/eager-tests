from herbie import Herbie, wgrib2

H = Herbie(
    '2021-08-22 22:00',
    model = 'hrrr',
    product = 'sfc',
    fxx = 9,
)

ss = ":(?:DPT|TMP|DZDT|MSTAV|CNWAT|SPFH|WIND|PRATE|SFCR|FRICV|SHTFL|LHTFL|VEG|GFLUX|CAPE|CIN|PWAT|LCDC|MCDC|HCDC|"\
     "DSWRF|DLWRF|HLCY|USTM|VSTM|VUCSH|VVCSH|HPBL|CANGLE|ESP|REFC|"\
     "[U|V]GRD:[3|5|7|8|9|10][0|5]0 mb|[U|V]GRD:10 m above ground|U[S|L]WRF:surface|RH:2 m above ground|"\
     "LFTX:500-1000 mb|TCDC:entire atmospher|HGT:[surface|equilibrum level]|PRES:cloud[base|top]|" \
     "PRES:0C isotherm)"
H.download(ss)
myFile = H.get_localFilePath(ss)
myFile, myFile.exists()

print(wgrib2.inventory(myFile))

