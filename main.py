from concurrent.futures import ThreadPoolExecutor
import os
import multiprocessing  # Import the multiprocessing module

# Global variable to signal the threads to stop
stop_threads = False

def get_available_threads():
    # Get the number of CPU cores
    num_cores = os.cpu_count()

    # Get the number of available threads
    num_threads = multiprocessing.cpu_count()

    return num_threads