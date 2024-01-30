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
    def __init__(self):
        self.chunk_state = {}  # To store the state of each chunk: available, completed, failed
        self.available_chunks = []  # List to keep track of available chunks
        self.completed_chunks = []  # List to keep track of completed chunks
        self.failed_chunks = []  # List to keep track of failed chunks
        self.lock = threading.Lock()  # Lock to ensure thread-safe operations

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
    def __init__(self, threadID, name, controller): 
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.controller = controller

    def run(self):
        while not stop_threads:
            chunk_id = self.controller.get_next_available_chunk()
            if chunk_id is not None:
                print(f"{self.name} processing chunk_{chunk_id}")
                try:
                    self.map_chunk(chunk_id)
                    print(f"{self.name} exiting chunk_{chunk_id}")
                    self.controller.mark_chunk_completed(chunk_id)
                except Exception as e:
                    print(f"{self.name} failed processing chunk_{chunk_id}: {str(e)}")
                    self.controller.mark_chunk_failed(chunk_id)

    def map_chunk(self, chunk_id):
        file_path = os.path.join("chunks", f"chunk_{chunk_id}.txt")
        chunk = read_chunk(file_path)
        mapped_result = map_function(chunk)
        save_to_file(mapped_result, f"chunk_{chunk_id}_map", 'mapStep')

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
    os.makedirs("groupStep", exist_ok=True)
    os.makedirs("reduceStep", exist_ok=True)

    input_file_path = 'texts/test.txt' 
    output_directory = 'chunks'
    max_chunk_size = 30 * 1024 * 1024  # 31.5MB

    # split_and_lowercase(input_file_path, output_directory, max_chunk_size)

    controller = Controller()
    controller.initialize_chunks(output_directory)

    threads = []

     # Create two separate thread pools for mapNodes
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor1, \
            concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor2:
        for i in range(1, 3):
            node1 = mapNode(1, f"mapNode-{i}-1", controller)
            future1 = executor1.submit(node1.run)
            threads.append(future1)

            node2 = mapNode(2, f"mapNode-{i}-2", controller)
            future2 = executor2.submit(node2.run)
            threads.append(future2)

    # Wait for all threads to complete
    concurrent.futures.wait(threads)


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
    concurrent.futures.wait(threads)

    print("Exiting Map Thread")
