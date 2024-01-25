from concurrent.futures import ThreadPoolExecutor
import os
import multiprocessing  # Import the multiprocessing module

from file_splitter import split_file

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
    book = []
    with ThreadPoolExecutor(max_workers=get_available_threads) as executor:
        for chunk in book:
            if stop_threads:
                break  # Exit the loop if the stop flag is set

            executor.submit(thread_function, chunk)
            count += 1

if __name__ == "__main__":
    input_file_path = 'path/to/your/input/file.txt'  # Replace with the actual path to your input file
    output_directory = 'chunks'
    max_chunk_size = 32 * 1024 * 1024  # 32MB

    split_file(input_file_path, output_directory, max_chunk_size)