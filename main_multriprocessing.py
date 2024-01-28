import os
from multiprocessing import Pool,Array  # Import the multiprocessing module
import multiprocessing
from file_splitter import split_and_lowercase
from map_reduce import map_task, group_task, reduce_task
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
            
import os
from concurrent.futures import ThreadPoolExecutor,wait,ProcessPoolExecutor
    # Function to process files for each thread
def process_file_range(files,start, end):
    return [map_task(file) for file in files[start:end]]
def print_progress(result):
    print(f"Process {os.getpid()} completed with result: {result}")
    
def process_files_in_parallel(input_dir, num_processes):
    # List all files in the directory
    files = [os.path.join(input_dir, filename) for filename in os.listdir(input_dir) if filename.endswith(".txt")]

    with Pool(processes=num_processes) as pool:
        # Use multiprocessing.Array to create a shared list
        shared_results = Array('i', [])

        # Split the files evenly among processes
        chunk_size = len(files) // num_processes
        file_chunks = [files[i:i + chunk_size] for i in range(0, len(files), chunk_size)]

        # Map the process_file_range function to each process
        async_results = pool.starmap_async(process_file_range, [(chunk, shared_results) for chunk in file_chunks], callback=print_progress)

        # Wait for all processes to complete
        async_results.wait()

    # Convert the shared_results to a regular list
    processed_contents = list(shared_results)

    #group_task('mapStep')
    return processed_contents





if __name__ == "__main__":
    input_file_path = 'texts/test.txt'  # Replace with the actual path to your input file
    output_directory = 'chunks'
    max_chunk_size = 30 * 1024 *256  # 31.5MB 
    
    split_and_lowercase(input_file_path, output_directory, max_chunk_size)
    
    
    # Example usage
    chunks_directory = "chunks"
    num_threads = 4

    result = process_files_in_parallel(chunks_directory, num_threads)
    
    print(result)
    
