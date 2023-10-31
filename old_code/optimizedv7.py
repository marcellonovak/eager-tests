import os
import glob
import pandas as pd
import pygrib
import dask.dataframe as dd
from datetime import datetime
import numpy as np
import warnings
from itertools import product
from collections import defaultdict
import gc

# For diagnostics:
# import cProfile

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
# v6:   Parallel Processing
#       Moved row processing to a function
#       Moved file processing to a function
#       Moved buffer writing to a function
# v7:   Removed parallel processing, threadlocking is taking too long
#       Vectorized

isLinux = False

# Configuration (will optimize later)
DATA_DIR = "C:\\Users\\Marce\\Documents\\Code\\Python\\full-test\\data" if not isLinux else "..\\data"
OUTPUT_DIR = "C:\\Users\\Marce\\Documents\\Code\\Python\\full-test\\old" if not isLinux else "..\\old"

BUFFER_SIZE = 1000  # Write after processing 1000 files
data_buffer = {}  # Buffer for data to be written to disk
existing_point_files = set()  # Set for existing point files

# Initialize cache variables
latlons_cache = None
columns_cache = None


# Function to process a sub grid in the GRB file
def process_sub_grid(current_sub_grid):
    current_values = current_sub_grid.values.ravel()  # Get the values from all the grid cells
    current_values = np.where(np.ma.getmask(current_values), np.nan, current_values)
    current_column = f"{current_sub_grid.name}: {current_sub_grid.level} {current_sub_grid.units}"  # Create column name
    return current_column, current_values


# Function to process a whole GRB file
def process_grb_file(file, latlons_cache, columns_cache):
    start_time = datetime.now()

    local_data = defaultdict(lambda: defaultdict(list))

    print(f"{file} Processing...")

    with pygrib.open(file) as current_grib:
        all_grids = current_grib.select()

        # Cache latitudes and longitudes
        if latlons_cache is None:
            latlons_cache = all_grids[0].latlons()

        latitudes, longitudes = latlons_cache

        # Cache column names
        if columns_cache is None:
            columns_cache = {sub_grid: f"{sub_grid.name}: {sub_grid.level} {sub_grid.units}" for sub_grid in all_grids}

        timestamp = f"{all_grids[0].year}-{str(all_grids[0].month).zfill(2)}-{str(all_grids[0].day).zfill(2)}T" \
                    f"{str(all_grids[0].hour).zfill(2)}:{str(all_grids[0].minute).zfill(2)}:{str(all_grids[0].second).zfill(2)}"

        for sub_grid in all_grids:
            column_name, values = process_sub_grid(sub_grid)
            for lat, lon, val in zip(latitudes.ravel(), longitudes.ravel(), values):
                outfile = f"{lat}_{lon}.csv"
                local_data[outfile]['timestamp'].append(timestamp)
                local_data[outfile][columns_cache[sub_grid]].append(val)

    for outfile, columns_dict in local_data.items():
        local_data[outfile] = [dict(row) for row in
                               zip(*[[(col, val) for val in val_list] for col, val_list in columns_dict.items()])]

    processing_time = datetime.now() - start_time
    print(f"{timestamp} Processed in: {processing_time}")

    return local_data


# Function to clear buffer and write to disk
def write_buffer_to_disk(data_buffer, existing_point_files):
    for filename, rows in data_buffer.items():
        path = os.path.join(OUTPUT_DIR, filename)  # Create path to file
        df = pd.DataFrame(rows)  # Create a DataFrame
        if filename in existing_point_files:
            df.to_csv(path, mode='a', header=False, index=False)
        else:
            df.to_csv(path, mode='w', index=False)
            existing_point_files.add(filename)  # Add the file to the set of existing point files


# Turn off automatic garbage collection
gc.disable()

# Main
if __name__ == '__main__':

    # Obligatory narcissistic message :)
    print("GRIB2 Time Series Processor v2, by Marcello Novak, 2023")
    print(f"Processing files in {DATA_DIR}...")  # Print starting message

    ### Start profiling ###
    # profiler = cProfile.Profile()
    # profiler.enable()

    # Populate the existing_point_files set
    existing_point_files.update([f for f in os.listdir(OUTPUT_DIR) if f.endswith('.csv')])
    files = glob.glob(os.path.join(DATA_DIR, "*.grib2"))

    # Process files sequentially
    for i, file in enumerate(files, 1):
        local_data = process_grb_file(file, latlons_cache, columns_cache)

        for key, value in local_data.items():
            if key in data_buffer:
                data_buffer[key].extend(value)
            else:
                data_buffer[key] = value  # Create a new entry in the dictionary

        if i % BUFFER_SIZE == 0:
            write_buffer_to_disk(data_buffer, existing_point_files)
            data_buffer.clear()


    # Write remaining files if any left in buffer
    if data_buffer:
        write_buffer_to_disk(data_buffer, existing_point_files)

    ### Stop profiling ###
    # profiler.disable()
    # profiler.dump_stats('profiling_results.prof')