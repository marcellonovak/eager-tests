import warnings

import matplotlib.pyplot as plt
from cartopy import crs as ccrs, feature as cfeature
import test

#  Suppress warnings issued by Cartopy when downloading data files
warnings.filterwarnings('ignore')

projPC = ccrs.PlateCarree()

# Map extent, .1 degrees wider/taller than our requirements
lonW = -116.1
lonE = -106.9
latS = 29.9
latN = 38.1
cLat = (latN + latS) / 2
cLon = (lonW + lonE) / 2
res = '110m'

fig = plt.figure(figsize=(9, 8))
ax = plt.subplot(1, 1, 1, projection=projPC)
ax.set_title('AZ Test Map')

gl = ax.gridlines(
    draw_labels=True, linewidth=2, color='gray', alpha=0.5, linestyle='--'
)

ax.set_extent([lonW, lonE, latS, latN], crs=projPC)
ax.coastlines(resolution=res, color='black')
ax.add_feature(cfeature.STATES, linewidth=0.3, edgecolor='brown')
ax.add_feature(cfeature.BORDERS, linewidth=0.5, edgecolor='blue')
dataplot = ax.contourf(test.ds.longitude,
                       test.ds.latitude,
                       test.ds.t2m,
                       transform=ccrs.PlateCarree(),
                       levels=100)
plt.colorbar(dataplot, orientation='horizontal', shrink=0.5)

fig.show()
