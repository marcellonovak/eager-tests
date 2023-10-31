import pstats

# Load profiling data
stats = pstats.Stats("profiling_results.prof")

# Sort the statistics by the cumulative time spent in the function
stats.sort_stats("cumulative")

# Show the top 10 functions that took the most time cumulatively
stats.print_stats(10)

# Show statistics for the specific functions of interest
#stats.print_stats("process_grb_file")

# Show a summary of calls for each function sorted by the internal time
#stats.sort_stats("time").print_stats()

# Show statistics sorted by the name of the function/file
#stats.sort_stats("name").print_stats()