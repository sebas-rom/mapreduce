from concurrent.futures import ThreadPoolExecutor,wait
import os
import multiprocessing  # Import the multiprocessing module

from file_splitter import split_and_lowercase
from map_reduce import map_task
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
import concurrent.futures

def process_files_in_parallel(input_dir, num_threads):
    # List all files in the directory
    files = []
    for filename in os.listdir(input_dir):
        if filename.endswith(".txt"):
            files.append(os.path.join(input_dir, filename))

    # Calculate the number of files each thread should process
    files_per_thread = len(files) // num_threads

    # Function to process files for each thread
    def process_file_range(start, end):
        return [map_task(file) for file in files[start:end]]

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        # Create a list of futures for each thread
        futures = []
        for i in range(0, len(files), files_per_thread):
            future = executor.submit(process_file_range, i, i + files_per_thread)
            #
            futures.append(future)

        # Wait for all threads to complete
        wait(futures)

        # Get the results from each thread
        results = [future.result() for future in futures]

    # Flatten the list of results
    processed_contents = [item for sublist in results for item in sublist]

    return processed_contents






if __name__ == "__main__":
    input_file_path = 'texts/test.txt'  # Replace with the actual path to your input file
    output_directory = 'chunks'
    max_chunk_size = 30 * 1024 *256  # 31.5MB 
    
    #split_and_lowercase(input_file_path, output_directory, max_chunk_size)
    
    
    # Example usage
    chunks_directory = "chunks"
    num_threads = 4

    result = process_files_in_parallel(chunks_directory, num_threads)
    print(result)
