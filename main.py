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

def thread_function(name):
    while not stop_threads:
        print(f"Thread {name} is running")


def callThreads():
    a = []
    with ThreadPoolExecutor(max_workers=get_available_threads) as executor:
        for x in a:
            if stop_threads:
                break  # Exit the loop if the stop flag is set

            executor.submit(thread_function, a)
            count += 1
