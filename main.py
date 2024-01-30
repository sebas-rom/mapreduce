from concurrent.futures import ThreadPoolExecutor
import os
import threading
import concurrent.futures
from file_splitter import split_and_lowercase
from map_reduce import read_chunk,map_function,save_to_file,read_result_from_file,shuffle_and_sort,reduce_function
exitFlag = 0
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
                chunk_id = int(filename.split('_')[1].split('.')[0])
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
        save_to_file(mapped_result, f"chunk_{chunk_id}_map", f'mapStep{self.executor_id}')

class groupNode(threading.Thread):
    def __init__(self, threadID, name, counter, input_file): 
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.counter = counter
        self.input_file = input_file

    def run(self):
        print("Starting " + self.name)
        group_f(self.input_file)
        print("Exiting " + self.name)

def group_f(input_file):
        file_path = os.path.join('mapStep', input_file)
        loaded_map = read_result_from_file(file_path)
        sorted_results = shuffle_and_sort(loaded_map)
        save_to_file(sorted_results, input_file.replace('.txt', '') + '_group','groupStep')

def run_map_group_pair(executor_id, controller, max_chunks_per_executor):
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as map_executor, \
            concurrent.futures.ThreadPoolExecutor(max_workers=1) as group_executor:

        map_threads = []
        for i in range(1, max_chunks_per_executor + 1):
            node = mapNode(1, f"mapNode-{i}-{executor_id}", controller, executor_id, max_chunks_per_executor)
            future = map_executor.submit(node.run)
            map_threads.append(future)

        concurrent.futures.wait(map_threads)
        print(f"Map Executor {executor_id} finished. Starting groupNode.")

        # group_executor.submit(groupNode, 1, f"groupNode-{executor_id}", 1, 'mapStep')  # Placeholder for actual input

class reduceNode(threading.Thread):
    def __init__(self, threadID, name, counter, file_pair): 
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.counter = counter
        self.file_pair = file_pair

    def run(self):
        print("Starting " + self.name)
        reduce_f(self.file_pair[0], self.file_pair[1])
        print("Exiting " + self.name)

def reduce_f(input_file1, input_file2):
        file_path1 = os.path.join("groupStep", input_file1)
        file_path2 = os.path.join("groupStep", input_file2)

        loaded_group = read_result_from_file(file_path1)+read_result_from_file(file_path2)
        reduced_results = reduce_function(loaded_group)
        save_to_file(reduced_results, input_file1.replace('.txt', '') + '_to_' +input_file2.replace('.txt', '')+ '_reduce','reduceStep')


if __name__ == "__main__":
    os.makedirs("mapStep", exist_ok=True)
    os.makedirs("mapStep1", exist_ok=True)
    os.makedirs("mapStep2", exist_ok=True)
    os.makedirs("groupStep1", exist_ok=True)
    os.makedirs("groupStep2", exist_ok=True)
    os.makedirs("reduceStep", exist_ok=True)

    input_file_path = 'texts/test.txt' 
    output_directory = 'chunks'
    max_chunk_size = 10 * 1024 * 1024  # 31.5MB

    #split_and_lowercase(input_file_path, output_directory, max_chunk_size)

    total_chunks = len([filename for filename in os.listdir(output_directory) if filename.endswith(".txt")])

    controller = Controller(total_chunks)
    controller.initialize_chunks(output_directory)

    threads = []

    # Create two separate thread pools for mapNodes
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        future1 = executor.submit(run_map_group_pair, 1, controller, controller.half_chunks)
        future2 = executor.submit(run_map_group_pair, 2, controller, controller.half_chunks)

    concurrent.futures.wait([future1, future2])

    print("Exiting Main Thread")


    # with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
    #     for filename in os.listdir("chunks"):
    #         if filename.endswith(".txt"):
    #             node = mapNode(1, f"mapNode-{filename}", 4, filename)
    #             future = executor.submit(node.run)
    #             threads.append(future)

    # with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
    #     for filename in os.listdir("mapStep"):           
    #         node = groupNode(1, f"groupNode-{filename}", 4, filename)
    #         future = executor.submit(node.run)
    #         threads.append(future)

    # with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
    #     filenames = sorted(os.listdir("groupStep"))
    #     pairs = [(filenames[i], filenames[i + 1]) for i in range(0, len(filenames), 2)]
    #     for file_pair in pairs:           
    #         node = reduceNode(1, f"reduceNode-{file_pair[0]}-{file_pair[1]}", 2, file_pair)
    #         future = executor.submit(node.run)
    #         threads.append(future)
    


    # Wait for all threads to complete
    # concurrent.futures.wait(threads)

    print("Exiting Map Thread") 