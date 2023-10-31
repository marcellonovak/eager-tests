import os
import glob
import pandas as pd
import pygrib
import dask.dataframe as dd
from datetime import datetime
import numpy as np
import warnings
from itertools import product
from concurrent.futures import ProcessPoolExecutor

# For diagnostics:
import cProfile

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

isLinux = False

# Configuration (will optimize later)
DATA_DIR = "C:\\Users\\Marce\\Documents\\Code\\Python\\full-test\\data" if not isLinux else "..\\data"
OUTPUT_DIR = "C:\\Users\\Marce\\Documents\\Code\\Python\\full-test\\old" if not isLinux else "..\\old"


BUFFER_SIZE = 1000  # Write after processing 1000 files
data_buffer = {}  # Buffer for data to be written to disk
existing_point_files = set()  # Set for existing point files


# Function to process a sub grid in the GRB file
def process_sub_grid(current_sub_grid, i, j):
    current_value = current_sub_grid.values[i, j]  # Get the value from the grid cell
    if np.ma.is_masked(current_value):     # If the value is masked (nonexistent), set it to NaN
        current_value = np.nan
    current_column = f"{current_sub_grid.name}: {current_sub_grid.level} {current_sub_grid.units}"  # Create column name
    return current_column, current_value


# Function to process a whole GRB file
def process_grb_file(file):
    start_time = datetime.now()

    local_data = {}

    with pygrib.open(file) as current_grib:
        all_grids = current_grib.select()
        latitudes, longitudes = all_grids[0].latlons()

        columns_cache = {sub_grid: f"{sub_grid.name}: {sub_grid.level} {sub_grid.units}" for sub_grid in all_grids}

        timestamp = f"{all_grids[0].year}-{str(all_grids[0].month).zfill(2)}-{str(all_grids[0].day).zfill(2)}T" \
                    f"{str(all_grids[0].hour).zfill(2)}:{str(all_grids[0].minute).zfill(2)}:{str(all_grids[0].second).zfill(2)}"

        lat_subset = latitudes.ravel()[:900]
        lon_subset = longitudes.ravel()[:900]

        for (lat, lon), (i, j) in zip(zip(lat_subset, lon_subset), product(range(30), repeat=2)):
            outfile = f"{lat}_{lon}.csv"
            row_data = {
                "timestamp": timestamp,  # Include timestamp in row data (for easier processing later)
                **{columns_cache[sub_grid]: process_sub_grid(sub_grid, i, j)[1] for sub_grid in all_grids}
            }

            if outfile in local_data:
                local_data[outfile].append(row_data)
            else:
                local_data[outfile] = [row_data]

        processing_time = datetime.now() - start_time
        return local_data, timestamp, processing_time


# Function to clear buffer and write to disk
def write_buffer_to_disk(data_buffer, existing_point_files):
    for filename, rows in data_buffer.items():
        path = os.path.join(OUTPUT_DIR, filename)  # Create path to file
        df = dd.from_pandas(pd.DataFrame(rows), npartitions=10)

        # If the file already exists, append to it, otherwise create it
        if filename in existing_point_files:
            df.to_csv(path, mode='a', header=False, single_file=True, index=False)
        else:
            df.to_csv(path, mode='w', single_file=True, index=False)
            existing_point_files.add(filename)  # Add the file to the set of existing point files


# Main
if __name__ == '__main__':

    # Obligatory narcissistic message :)
    print("GRIB2 Time Series Processor v2, by Marcello Novak, 2023")
    print(f"Processing files in {DATA_DIR}...")  # Print starting message

    ### Start profiling ###
    profiler = cProfile.Profile()
    profiler.enable()

    # Populate the existing_point_files set
    existing_point_files.update([f for f in os.listdir(OUTPUT_DIR) if f.endswith('.csv')])
    files = glob.glob(os.path.join(DATA_DIR, "*.grib2"))

    # Process files in parallel
    with ProcessPoolExecutor() as executor:
        for i, (local_data, timestamp, processing_time) in enumerate(executor.map(process_grb_file, files), 1):

            for key, value in local_data.items():
                if key in data_buffer:
                    data_buffer[key].extend(value)
                else:
                    data_buffer[key] = value

            if i % BUFFER_SIZE == 0:
                write_buffer_to_disk(data_buffer, existing_point_files)
                data_buffer.clear()

            # Print processing time
            print(f"File {i} Processed in: {processing_time}")

    # Write remaining files if any left in buffer
    if data_buffer:
        write_buffer_to_disk(data_buffer, existing_point_files)

    ### Stop profiling ###
    profiler.disable()
    profiler.dump_stats('profiling_results.prof')