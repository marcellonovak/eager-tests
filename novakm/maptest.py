import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from cartopy import crs as ccrs, feature as cfeature
import numpy as np
import warnings
import test

dataSet = test.ds

# Suppress warnings issued by Cartopy when downloading data files
warnings.filterwarnings('ignore')

# Map extent coordinates
lonW = -116
lonE = -107
latS = 30
latN = 38

projPC = ccrs.PlateCarree()  # Plate Carree projection
fig = plt.figure(figsize=(8.75, 8))           # Create figure
ax = plt.subplot(1, 1, 1, projection=projPC)  # Create map axes

ax.set_title('AZ Test Map')  # Setting map title
ax.set_xlabel('Longitude')   # TODO: Figure out why these don't work
ax.set_ylabel('Latitude')

ax.set_extent(
    [lonW, lonE, latS, latN],  # Set map extent (Cardinal directions)
    crs=projPC                 # Set map projection
)

# Add features to map (Coast, border, and state lines)
ax.coastlines(resolution='50m', linewidth=0.5, edgecolor='blue')
ax.add_feature(cfeature.STATES, linewidth=0.3, edgecolor='brown')
ax.add_feature(cfeature.BORDERS, linewidth=0.5, edgecolor='blue')

# Add gridlines to map
gl = ax.gridlines(
    draw_labels=True,  # Draw grid labels
    linewidth=1.0,       # Set line width
    color='black',      # Set line color
    alpha=0.5,         # Set line transparency
    linestyle=':',     # Set grid line style to dotted
)

# Set grid line spacings to 1 degree
gl.xlocator = mticker.FixedLocator(np.arange(lonW, lonE, 1))
gl.ylocator = mticker.FixedLocator(np.arange(latS, latN, 1))

# Plot data with pcolormesh
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
