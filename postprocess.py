from datetime import datetime
import gc
import glob
import pandas as pd
import os
import logging
import warnings

# For some reason there are non-unique indexes because multiple data values at the same timestamp exist
# No clue why but this is probably intended
warnings.simplefilter(action='ignore', category=FutureWarning)

# Flag for testing reasons
isLinux = True
OUTPUT_DIR = os.path.join(".", "output")  if not isLinux else "./output"
LOG_DIR =    os.path.join(".", "logs")      if not isLinux else "./logs"

# Ensuring the directories exist, creating them if they don't
if not os.path.exists(LOG_DIR):
    print(f"Log directory not found, creating {LOG_DIR}...")
    os.makedirs(LOG_DIR)
if not os.path.exists(OUTPUT_DIR):
    print(f"Output directory not found, creating {OUTPUT_DIR}...")
    os.makedirs(OUTPUT_DIR)

# Logging Configuration
current_time_str = datetime.now().strftime("%Y%m%dT%H%M%S")
log_filename = os.path.join(LOG_DIR, f"{current_time_str}.log")
logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Function to post-process each file and fill any missing spaces
def post_process_file(filepath):
    start_time = datetime.now()  # Start the timer for this file

    try:
        df = pd.read_csv(filepath)  # Read the file into a DataFrame
        df['timestamp'] = pd.to_datetime(
            df['timestamp'], format='%m/%d/%Y %H:%M')  # Convert to DatetimeIndex
        df.set_index('timestamp', inplace=True)  # Set the timestamp as the index for the DataFrame

        # Aggregating duplicate indexes by taking the mean of the values
        df = df.groupby(level=0).mean()

        df = df.resample('H').asfreq()  # Resample to hourly intervals and insert missing rows
        df.fillna("NaN", inplace=True)  # Fill all missing values with "NaN"
        df.to_csv(filepath)  # Write the processed DataFrame back to the CSV file

        elapsed_time = datetime.now() - start_time  # Calculate elapsed time
        logging.info(f"Processed file {filepath} in {elapsed_time}")

    # Log all errors
    except Exception as e:
        logging.error(f"Error processing file {filepath}: {e}")


# Post-process all the files in the old directory
output_files = glob.iglob(os.path.join(OUTPUT_DIR, "*.csv"))
for i, outfile in enumerate(output_files, 1):
    post_process_file(outfile)  # Process
    gc.collect()                # Collect garbage
