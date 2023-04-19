import matplotlib.pyplot as plt
from cartopy import crs as ccrs, feature as cfeature
import warnings
import test

dataSet = test.ds
projPC = ccrs.PlateCarree()

# Suppress warnings issued by Cartopy when downloading data files
warnings.filterwarnings('ignore')

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
# fig, ax = plt.subplots(figsize=(9, 8))

ax.set_title('AZ Test Map')                          # Setting map title
ax.set_extent([lonW, lonE, latS, latN], crs=projPC)  # Set map extent
ax.coastlines(resolution=res, color='black')                       # Add coastlines  (black)
ax.add_feature(cfeature.STATES, linewidth=0.3, edgecolor='brown')  # Add state lines (brown)
ax.add_feature(cfeature.BORDERS, linewidth=0.5, edgecolor='blue')  # Add borderlines (blue)
ax.gridlines(
    draw_labels=True,
    linewidth=2,
    color='gray',
    alpha=0.5,
    linestyle='--'
)

mesh = ax.pcolormesh(
    dataSet.longitude,             # Latitude and Longitude
    dataSet.latitude,
    dataSet.t2m,                   # Actual data
    transform=ccrs.PlateCarree(),  # Transform to fit projection
)

# Add color bar
cbar = plt.colorbar(mesh, orientation='horizontal', pad=0.05)
cbar.set_label('Temperature (C)')

# Show plot
fig.show()
