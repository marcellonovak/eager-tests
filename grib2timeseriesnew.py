import os
import glob
import numpy as np
import pandas as pd
import pygrib
from datetime import datetime
from collections import defaultdict
import logging
import warnings
import gc
warnings.filterwarnings("ignore", category=DeprecationWarning)

# GRIB2 Time Series Processor v9
# Program to convert GRIB2 files to CSV files with a time series for each point
# By Marcello Novak, 2023

# Flag for testing reasons
isLinux = False

# Directory Configuration, change as needed
DATA_DIR =   os.path.join(".", "data_main", "2_day_test") \
    if not isLinux else "/data0/timeseries_hub/data_main/month_set"
OUTPUT_DIR = os.path.join(".", "output_main", "2_day_output")  \
    if not isLinux else "/data0/timeseries_hub/output_main/output_v1"
LOG_DIR =    os.path.join(".", "logs") \
    if not isLinux else "/data0/timeseries_hub/logs"

# Ensuring the directories exist, creating them if they don't
if not os.path.exists(LOG_DIR):
    print(f"Log directory not found, creating {LOG_DIR}...")
    os.makedirs(LOG_DIR)
if not os.path.exists(OUTPUT_DIR):
    print(f"Output directory not found, creating {OUTPUT_DIR}...")
    os.makedirs(OUTPUT_DIR)

# home/chennon/eager/data/hrrr/final
# home/chennon/eager/.../vars.txt

# Logging Configuration
current_time_str = datetime.now().strftime("%Y%m%dT%H%M%S")
log_filename = os.path.join(LOG_DIR, f"{current_time_str}.log")
logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Other Configuration
BUFFER_SIZE = 10       # Write after processing 10 files
data_buffer = {}       # Buffer for data to be written to disk
existing_point_files = set()  # Set for existing point files

# Initialize cache variables
latlons_cache = None
columns_cache = None

# Function to read existing rows from a file
def read_existing_timestamps(filepath):
    try:
        df = pd.read_csv(filepath, usecols=['timestamp'])
        return set(df['timestamp'].unique())
    except FileNotFoundError:
        return set()
    except pd.errors.EmptyDataError:
        return set()
    except ValueError:
        return set()


# Function to process a sub grid in the GRB file
def process_sub_grid(current_sub_grid):
    current_values = current_sub_grid.values.ravel()  # Get the values from all the grid cells
    current_values = np.where(np.ma.getmask(current_values), np.nan, current_values)
    current_column = f"{current_sub_grid.name}: {current_sub_grid.level} {current_sub_grid.units}"  # Create column name
    print(f"Processing {current_column}")
    return current_column, current_values


# Function to process a whole GRB file
def process_grb_file(file, latlons_cache, columns_cache, timestamps_cache):
    print(f"Processing {file}")
    start_time = datetime.now()  # Start the timer for this file
    local_data = defaultdict(lambda: defaultdict(list))  # Create a dictionary to store the data

    try:
        with pygrib.open(file) as current_grib:
            all_grids = current_grib.select()

            if not all_grids:
                # Log warning and return empty dictionary
                print(f"No grids found in file {file}. Skipping.")
                logging.warning(f"No grids found in file {file}. Skipping.")
                return {}

            # Cache latitudes and longitudes
            if latlons_cache is None:
                latlons_cache = all_grids[0].latlons()
            latitudes, longitudes = latlons_cache

            # Cache column names
            if columns_cache is None:
                columns_cache = {sub_grid: f"{sub_grid.name}: {sub_grid.level} {sub_grid.units}" for sub_grid in all_grids}

            # Create timestamp for this file (avoids repetitions)
            timestamp = f"{all_grids[0].month:02d}/{all_grids[0].day:02d}" \
                        f"/{all_grids[0].year} {all_grids[0].hour:02d}:00"

            # Process each sub grid in the grb file (each paper in the stack)
            for sub_grid in all_grids:

                # Check if the sub grid is unknown, skip
                if "unknown" in sub_grid.name.lower():
                    print(f"Skipping sub-grid with name containing 'unknown': {sub_grid.name}")
                    logging.warning(f"Skipping sub-grid with name containing 'unknown': {sub_grid.name}")
                    continue

                # Process the column names, values, and latitudes/longitudes
                column_name, values = process_sub_grid(sub_grid)
                for lat, lon, val in zip(latitudes.ravel(), longitudes.ravel(), values):
                    outfile = f"{lat:.3f}_{lon:.3f}.csv"  # Rounds to three decimals now

                    existing_timestamps = read_existing_timestamps(os.path.join(OUTPUT_DIR, outfile))
                    # Check if the timestamp already exists in the CSV
                    if timestamp in timestamps_cache[outfile]:
                        logging.info(f"Timestamp {timestamp} already exists in {outfile}. Skipping.")
                        continue
                    timestamps_cache[outfile].add(timestamp)

                    if outfile not in local_data:
                        local_data[outfile] = defaultdict(list)

                    local_data[outfile]['timestamp'].append(timestamp)
                    for col in columns_cache.values():
                        if col == column_name:
                            local_data[outfile][col].append(val)
                        else:
                            # Fill missing values for this timestamp with NaN
                            local_data[outfile][col].append(np.nan)

            # Convert the data to a list of dictionaries
            for outfile, columns_dict in local_data.items():
                max_length = max(len(column) for column in columns_dict.values())
                for column in columns_dict:
                    while len(columns_dict[column]) < max_length:
                        columns_dict[column].append(np.nan)

                local_data[outfile] = [dict(zip(columns_dict, row)) for row in zip(*columns_dict.values())]

            # Log processing time and return the data
            print(f"Processed in: {datetime.now() - start_time}")
            logging.info(f"{timestamp} Processed in: {datetime.now() - start_time}")
            return local_data

    # Catch any exceptions and log the errors
    except Exception as e:
        print(f"Error processing file {file}. Error: {e}")
        logging.error(f"Error processing file {file}. Error: {e}")
        return {}


# Function to clear buffer and write to disk
def write_buffer_to_disk(data_buffer, existing_point_files):
    for filename, rows in data_buffer.items():
        path = os.path.join(OUTPUT_DIR, filename)  # Create path to file
        df = pd.DataFrame(rows)  # Create a DataFrame
        # If file exist, append, otherwise create
        if filename in existing_point_files:
            df.to_csv(path, mode='a', header=False, index=False)
        else:
            df.to_csv(path, mode='w', index=False)
            existing_point_files.add(filename)  # Add the file to the set of existing point files if created

# Turn off automatic garbage collection
gc.disable()

# Main
if __name__ == '__main__':

    # Initialize the timer for main processing
    main_processing_start_time = datetime.now()

    # Initialize the timestamps cache
    timestamps_cache = defaultdict(set)

    print("GRIB2 Time Series Processor v9, by Marcello Novak, 2023")  # Obligatory narcissistic message :)
    print(f"Starting Main Processing for files in {DATA_DIR}...")  # Print starting message to terminal
    logging.info(f"Processing files in {DATA_DIR}...")  # Log starting message

    # Populate the existing_point_files set from the old directory
    existing_point_files.update([f for f in os.listdir(OUTPUT_DIR) if f.endswith('.csv')])
    files = sorted(glob.iglob(os.path.join(DATA_DIR, "*.grib2")))

    # Populate timestamps_cache with existing timestamps
    for filename in existing_point_files:
        timestamps_cache[filename] = read_existing_timestamps(os.path.join(OUTPUT_DIR, filename))

    # Process files sequentially
    for i, file in enumerate(files, 1):
        local_data = process_grb_file(file, latlons_cache, columns_cache, timestamps_cache)  # Process the file
        gc.collect()  # Collect garbage after processing each file

        # Merge the local data with the data buffer
        for key, value in local_data.items():
            if key in data_buffer:
                data_buffer[key].extend(value)
            else:
                data_buffer[key] = value  # Create a new entry in the dictionary

        # Write to disk if buffer is full
        if i % BUFFER_SIZE == 0:
            write_buffer_to_disk(data_buffer, existing_point_files)
            data_buffer.clear()
            gc.collect()  # Collect garbage

    # Write remaining files if any left in buffer after processing all files
    if data_buffer:
        write_buffer_to_disk(data_buffer, existing_point_files)
        gc.collect()  # Collect garbage

    # Log the duration of main processing
    main_processing_duration = datetime.now() - main_processing_start_time
    logging.info(f"Main processing duration: {main_processing_duration}.")
    print("Main Processing complete.")  # Print completion message to terminal for the main processing
