from concurrent.futures import ThreadPoolExecutor, wait
import os
import threading
import concurrent.futures
from file_splitter import split_and_lowercase
from map_reduce import (
    read_chunk,
    map_function,
    save_to_file,
    read_result_from_file,
    shuffle_and_sort,
    reduce_function,
)
import shutil
from collections import defaultdict
import re
import json


def split_and_lowercase(input_file, output_dir = 'chunks', chunk_size= 30 * 1024 * 1024):

    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    

    with open(input_file, 'rb') as infile:
        chunk_number = 1
        while True:
            # Read a chunk of data
            chunk_data = infile.read(chunk_size)

            # Break the loop if no more data is left
            if not chunk_data:
                break

            # Create a new chunk file
            chunk_filename = os.path.join(output_dir, f'chunk_{chunk_number}.txt')
            with open(chunk_filename, 'wb') as chunk_file:
                # Convert the chunk data to lowercase and write it to the new file
                chunk_file.write(chunk_data.lower())

            chunk_number += 1


def read_chunk(file_path):
    with open(file_path, 'r') as file:
        content = file.read()
        # Use regular expression to remove numbers
        content_without_numbers = re.sub(r'\d+', '', content)
        return content_without_numbers

def map_function(chunk):
    words = re.findall(r'\b\w+\b', chunk)
    word_count = defaultdict(int)
    for word in words:
        word_count[word] += 1
    return word_count.items()

def shuffle_and_sort(mapped_results):
    sorted_results = defaultdict(list)
    
    for word, count in mapped_results:
        sorted_results[word].append(count)
    
    sorted_results=dict(sorted(sorted_results.items()))
    return sorted_results.items()

def reduce_function(sorted_results):
    reduced_results = [(word, sum(counts)) for word, counts in sorted_results]
    return reduced_results

def save_to_file(result, file_path, output_dir='logs'):
    log_filename = os.path.join(output_dir, f'{file_path}')
    os.makedirs(output_dir, exist_ok=True)
    result_as_list = [list(item) for item in result]
    with open(log_filename, 'w', encoding='utf-8') as file:
        json.dump(result_as_list, file)

def read_result_from_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        loaded_result = json.load(file)
        # Convert lists back to tuples
        return [tuple(item) for item in loaded_result]

# Global variable to signal the threads to stop
stop_threads = False


class Controller:
    def __init__(self, total_chunks):
        self.chunk_state = {}  # To store the state of each chunk: available, completed, failed
        self.available_chunks = []  # List to keep track of available chunks
        self.completed_chunks = []  # List to keep track of completed chunks
        self.failed_chunks = []  # List to keep track of failed chunks
        self.lock = threading.Lock()  # Lock to ensure thread-safe operations
        self.total_chunks = total_chunks
        self.half_chunks = (total_chunks + 1) // 2  # Calculate half of total chunks

    def initialize_chunks(self, folder_path):
        for filename in os.listdir(folder_path):
            if filename.endswith(".txt"):
                chunk_id = int(filename.split("_")[1].split(".")[0])
                self.chunk_state[chunk_id] = "available"
                self.available_chunks.append(chunk_id)

    def get_next_available_chunk(self):
        with self.lock:
            if self.available_chunks:
                return self.available_chunks.pop(0)
            else:
                return None

    def mark_chunk_completed(self, chunk_id):
        with self.lock:
            self.chunk_state[chunk_id] = "completed"
            self.completed_chunks.append(chunk_id)

    def mark_chunk_failed(self, chunk_id):
        with self.lock:
            self.chunk_state[chunk_id] = "failed"
            self.failed_chunks.append(chunk_id)


class mapNode(threading.Thread):
    def __init__(self, threadID, name, controller, executor_id, max_chunks_per_executor):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.controller = controller
        self.executor_id = executor_id
        self.max_chunks_per_executor = max_chunks_per_executor
        self.processed_chunks = 0

    def run(self):
        while not stop_threads:
            chunk_id = self.controller.get_next_available_chunk()
            if chunk_id is not None:
                # Check if the maximum chunks per executor is reached
                if self.processed_chunks >= self.max_chunks_per_executor:
                    print(f"{self.name} reached maximum chunks. Stopping.")
                    break

                print(f"{self.name} processing chunk_{chunk_id}")
                try:
                    self.map_chunk(chunk_id)
                    print(f"{self.name} exiting chunk_{chunk_id}")
                    self.controller.mark_chunk_completed(chunk_id)
                    self.processed_chunks += 1

                    # If half of the chunks are processed, stop receiving new chunks
                    if self.processed_chunks == self.max_chunks_per_executor // 2:
                        print(f"{self.name} reached half of the chunks. Stopping further processing.")
                        break

                except Exception as e:
                    print(f"{self.name} failed processing chunk_{chunk_id}: {str(e)}")
                    self.controller.mark_chunk_failed(chunk_id)
            else:
                print(f"{self.name} no more chunks available. Stopping.")
                break

    def map_chunk(self, chunk_id):
        file_path = os.path.join("chunks", f"chunk_{chunk_id}.txt")
        chunk = read_chunk(file_path)
        mapped_result = map_function(chunk)
        save_to_file(mapped_result, f"chunk_{chunk_id}_map", f"mapStep{self.executor_id}")


class groupNode(threading.Thread):
    def __init__(self, name, folder_path, executor_id):
        threading.Thread.__init__(self)
        self.name = name
        self.folder_path = folder_path
        self.executor_id = executor_id

    def run(self):
        print("Starting " + self.name)
        group_f(self.folder_path, self.executor_id)
        print("Exiting " + self.name)


def group_f(folder_path, executor_id):
    groupedResults = []
    for filename in os.listdir(folder_path):
        new_path = os.path.join(folder_path, filename)
        loaded_map = read_result_from_file(new_path)
        groupedResults.extend(loaded_map)

    sorted_results = shuffle_and_sort(groupedResults)
    save_to_file(sorted_results, "group", f"groupStep{executor_id}")


class reduceNode(threading.Thread):
    def __init__(self, name, folder_path, executor_id):
        threading.Thread.__init__(self)
        self.name = name
        self.folder_path = folder_path
        self.executor_id = executor_id

    def run(self):
        print("Starting " + self.name)
        reduce_f(self.folder_path, self.executor_id)
        print("Exiting " + self.name)


def reduce_f(folder_path, executor_id):
    groupedResults = []
    for filename in os.listdir(folder_path):
        new_path = os.path.join(folder_path, filename)
        loaded_map = read_result_from_file(new_path)
        groupedResults.extend(loaded_map)

    reducedResults = reduce_function(groupedResults)
    save_to_file(reducedResults, "reduced", f"reduceStep{executor_id}")


def directory_utils(executor_id):
    # Delete directories if they exist
    for directory in [f"mapStep{executor_id}", f"groupStep{executor_id}", f"reduceStep{executor_id}"]:
        if os.path.exists(directory):
            shutil.rmtree(directory)

    # Create directories
    os.makedirs(f"mapStep{executor_id}")
    os.makedirs(f"groupStep{executor_id}")
    os.makedirs(f"reduceStep{executor_id}")

def run_map_reduce_task(executor_id, controller, max_chunks_per_executor):
    directory_utils(executor_id)
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as map_executor, \
            concurrent.futures.ThreadPoolExecutor(max_workers=1) as group_executor, \
                concurrent.futures.ThreadPoolExecutor(max_workers=1) as reduce_executor:

        map_threads = []
        for i in range(1, max_chunks_per_executor + 1):
            node = mapNode(1, f"mapNode-{i}-{executor_id}", controller, executor_id, max_chunks_per_executor)
            future = map_executor.submit(node.run)
            map_threads.append(future)

        concurrent.futures.wait(map_threads)
        print(f"Map Executor {executor_id} finished. Starting groupNode.")

        nodeG = groupNode(f"groupNode-{executor_id}",  f'mapStep{executor_id}',executor_id) 
        futureG = group_executor.submit(nodeG.run)
        concurrent.futures.wait([futureG])
        
        nodeR = reduceNode(f"reduceNode-{executor_id}",  f'groupStep{executor_id}',executor_id)
        futureR = reduce_executor.submit(nodeR.run)
        concurrent.futures.wait([futureR])

def runComputers(computer_number):
    total_chunks = len([filename for filename in os.listdir('chunks') if filename.endswith(".txt")])
    controller = Controller(total_chunks)
    controller.initialize_chunks('chunks')
    with concurrent.futures.ThreadPoolExecutor(max_workers=computer_number) as executor:
        futures = [executor.submit(run_map_reduce_task, i, controller, controller.half_chunks) for i in range(1, computer_number + 1)]
    
    wait(futures)
    print("Exiting Main Thread")

if __name__ == "__main__":

    input_file_path = "texts/bible.txt"

    split_and_lowercase(input_file_path)

    runComputers(2)

    


    
