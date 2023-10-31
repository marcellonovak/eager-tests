import os
import glob
import pandas as pd
import pygrib
from datetime import datetime
import numpy as np
import warnings
from itertools import product
warnings.filterwarnings("ignore", category=DeprecationWarning)

# GRIB2 Time Series Processor v3
# By Marcello Novak, 2023

# IMPROVEMENTS:
# v2:   Writing whole rows instead of individual cells
#       Caching column names
#       Using sets to check if file exists/has been processed
# v3:   Caching of files and column names
#       Buffering rows before writing to disk
# v4:   Optimized Dataframe Creation
#       Avoiding Global Variables
# v5:   Vectorized EVERYTHING
#       Optimized file handling (appending checks)



isLinux = False

# Configuration (will optimize later)
DATA_DIR = "C:\\Users\\Marce\\Documents\\Code\\Python\\full-test\\data" if not isLinux else "..\\data"
OUTPUT_DIR = "C:\\Users\\Marce\\Documents\\Code\\Python\\full-test\\old" if not isLinux else "..\\old"


# Function to process a single grid cell in the GRB file
def process_grb_file(current_sub_grid, i, j):
    current_value = current_sub_grid.values[i, j]  # Get the value from the grid cell
    if np.ma.is_masked(current_value):     # If the value is masked (nonexistent), set it to NaN
        current_value = np.nan
    current_column = f"{current_sub_grid.name}: {current_sub_grid.level} {current_sub_grid.units}"  # Create the column name
    return current_column, current_value


# Obligatory narcissistic message :)
print("GRIB2 Time Series Processor v2, by Marcello Novak, 2023")
print(f"Processing files in {DATA_DIR}...")  # Print starting message

# Create set of files that already exist
existing_point_files = set()

# Get and iterate through all grib files in the data directory
files = glob.glob(os.path.join(DATA_DIR, "*.grib2"))
for file in files:

    start_time = datetime.now()  # Start the timer for this file

    # Open the main grid stack as a pygrib file (stack of papers)
    with pygrib.open(file) as current_grib:
        all_grids = current_grib.select()
        latitudes, longitudes = all_grids[0].latlons()  # Assuming all sub-grids have same lat/lons

        # Get column names once and cache them
        columns_cache = {sub_grid: f"{sub_grid.name}: {sub_grid.level} {sub_grid.units}" for sub_grid in all_grids}

        # Create timestamp for this whole file (avoids repetitions)
        timestamp = f"{all_grids[0].year}-{str(all_grids[0].month).zfill(2)}-{str(all_grids[0].day).zfill(2)}T" \
                    f"{str(all_grids[0].hour).zfill(2)}:{str(all_grids[0].minute).zfill(2)}:{str(all_grids[0].second).zfill(2)}"

        lat_subset = latitudes.ravel()[:900]
        lon_subset = longitudes.ravel()[:900]

        for (lat, lon), (i, j) in zip(zip(lat_subset, lon_subset), product(range(30), repeat=2)):
            outfile = f"{lat}_{lon}.csv"

            if outfile in existing_point_files:
                df = pd.read_csv(outfile, index_col=0)
            else:
                df = pd.DataFrame(columns=[columns_cache[sub_grid] for sub_grid in all_grids])

            row_data = {columns_cache[sub_grid]: process_grb_file(sub_grid, i, j)[1] for sub_grid in all_grids}
            new_data = pd.DataFrame([row_data], index=[timestamp])

            if outfile in existing_point_files:
                new_data.to_csv(outfile, mode='a', header=False)
            else:
                new_data.to_csv(outfile, mode='w')
                existing_point_files.add(outfile)

    # Stop timer and print processing time
    processing_time = datetime.now() - start_time
    print(f"{timestamp} Processed in: {processing_time}")