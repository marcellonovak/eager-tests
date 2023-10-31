import os
import glob
import pandas as pd
import pygrib
from datetime import datetime
import numpy as np
import warnings
from concurrent.futures import ProcessPoolExecutor
warnings.filterwarnings("ignore", category=DeprecationWarning)

# GRIB2 Time Series Processor v3
# By Marcello Novak, 2023

# IMPROVEMENTS:
# v2:   Writing whole rows instead of individual cells
#       Caching column names
#       Using sets to check if file exists/has been processed
# v3:   Parallel Processing
#       Buffering rows before writing to disk
#       Optimized Dataframe Creation
#       Caching of files and column names
#       Vectorized Operations
#       Avoiding Global Variables

isLinux = False

# Configuration (will optimize later)
DATA_DIR = "C:\\Users\\Marce\\Documents\\Code\\Python\\full-test\\data" if not isLinux else "..\\data"
OUTPUT_DIR = "C:\\Users\\Marce\\Documents\\Code\\Python\\full-test\\old" if not isLinux else "..\\old"

BUFFER_SIZE = 10000  # Number of rows to buffer before writing to disk
data_buffer = {}  # Store the data in memory before writing
existing_files = set()  # Set for files that already exist


# Function to process a single grid cell in a sub_grid file
def process_sub_cell(current_sub_grid, i, j):
    current_value = current_sub_grid.values[i, j]  # Get the value from the grid cell
    if np.ma.is_masked(current_value):     # If the value is masked (nonexistent), set it to NaN
        current_value = np.nan
    current_column = f"{current_sub_grid.name}: {current_sub_grid.level} {current_sub_grid.units}"  # Create column name
    return current_column, current_value


def process_file(file, DATA_DIR, OUTPUT_DIR):
    data_buffer = {}

    # Open the main grid stack as a pygrib file (stack of papers)
    # Get lat/lon, get timestamp, and cache columns
    with pygrib.open(file) as current_grib:
        all_grids = current_grib.select()
        latitudes, longitudes = all_grids[0].latlons()

        timestamp = f"{all_grids[0].year}-{str(all_grids[0].month).zfill(2)}-{str(all_grids[0].day).zfill(2)}T" \
                    f"{str(all_grids[0].hour).zfill(2)}:{str(all_grids[0].minute).zfill(2)}:{str(all_grids[0].second).zfill(2)}"

        columns_cache = {}
        for sub_grid in all_grids:
            column_name = f"{sub_grid.name}: {sub_grid.level} {sub_grid.units}"
            columns_cache[sub_grid] = column_name

        # For all points:
        for i in range(20):
            # for i in range(latitudes.shape[0]):
            for j in range(20):
            # for j in range(latitudes.shape[0]):
                lat, lon = latitudes[i, j], longitudes[i, j]
                outfile = os.path.join(OUTPUT_DIR, f"{lat}_{lon}.csv")

                row_data = {}

                # Process this cell along all sub-grids
                for sub_grid in all_grids:
                    column_name = columns_cache[sub_grid]
                    value = process_sub_cell(sub_grid, i, j)[1]
                    row_data[column_name] = value

                # 8. Optimized Data Structures
                if outfile not in data_buffer:
                    data_buffer[outfile] = deque()
                data_buffer[outfile].append((timestamp, row_data))

                # 4. Optimized DataFrame Creation
                if len(data_buffer[outfile]) >= BUFFER_SIZE:
                    buffered_list = [item[1] for item in data_buffer[outfile]]
                    df = pd.DataFrame(buffered_list, index=[item[0] for item in data_buffer[outfile]])
                    df.to_csv(outfile, mode='a' if outfile in existing_files else 'w', index=True)
                    existing_files.add(outfile)
                    data_buffer[outfile] = deque()








# Obligatory narcissistic message :)
print("GRIB2 Time Series Processor v3, by Marcello Novak, 2023")
print(f"Processing files in {DATA_DIR}...")  # Print starting message

files = glob.glob(os.path.join(DATA_DIR, "*.grib2"))
for file in files:
    start_time = datetime.now()  # Start the timer for this file

    # Open the main grid stack as a pygrib file (stack of papers)
    with pygrib.open(file) as current_grib:
        all_grids = current_grib.select()
        latitudes, longitudes = all_grids[0].latlons()

        # Get column names once and cache them once
        columns_cache = {}
        for sub_grid in all_grids:
            column_name = f"{sub_grid.name}: {sub_grid.level} {sub_grid.units}"
            columns_cache[sub_grid] = column_name

        # Create timestamp for this whole file (avoids repetitions)
        timestamp = f"{all_grids[0].year}-{str(all_grids[0].month).zfill(2)}-{str(all_grids[0].day).zfill(2)}T" \
                    f"{str(all_grids[0].hour).zfill(2)}:{str(all_grids[0].minute).zfill(2)}:{str(all_grids[0].second).zfill(2)}"

        # For all points:


                # Get the lat/lon and old file name
                lat, lon = latitudes[i, j], longitudes[i, j]
                outfile = os.path.join(OUTPUT_DIR, f"{lat}_{lon}.csv")

                # Create empty row dictionary
                row_data = {}

                # For each sub-grid in the grb file (paper in the stack)
                for sub_grid in all_grids:

                    # Create timestamp and row_date
                    column_name = columns_cache[sub_grid]
                    value = process_sub_cell(sub_grid, i, j)[1]
                    row_data[column_name] = value

                # Update data buffer
                if outfile not in data_buffer:
                    data_buffer[outfile] = []
                data_buffer[outfile].append((timestamp, row_data))

                # Write buffered data to disk if the buffer reaches a certain size
                if len(data_buffer[outfile]) >= BUFFER_SIZE:

                    # Create dataframe from buffered data and write to disk
                    df = pd.DataFrame(columns=row_data.keys())
                    for timestamp, data in data_buffer[outfile]:
                        df.loc[timestamp] = data
                    df.to_csv(outfile, mode='a' if outfile in existing_files else 'w', index=True)
                    existing_files.add(outfile)
                    data_buffer[outfile] = []

    # Stop timer and print processing time
    processing_time = datetime.now() - start_time
    print(f"{timestamp} Processed in: {processing_time}")

# After processing all files, flush remaining data from buffer to disk
for outfile, buffered_data in data_buffer.items():
    if buffered_data:
        df = pd.DataFrame(columns=buffered_data[0][1].keys())
        for timestamp, data in buffered_data:
            df.loc[timestamp] = data
        df.to_csv(outfile, mode='a' if outfile in existing_files else 'w', index=True)