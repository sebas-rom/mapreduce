from concurrent.futures import ThreadPoolExecutor
import os
import threading
import concurrent.futures
from file_splitter import split_and_lowercase
from map_reduce import read_chunk,map_function,save_to_file
exitFlag = 0
# Global variable to signal the threads to stop
stop_threads = False


class mapNode(threading.Thread):
    def __init__(self, threadID, name, counter, input_file):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.counter = counter
        self.input_file = input_file

    def run(self):
        print("Starting " + self.name)
        map_f(self.input_file)
        print("Exiting " + self.name)

def map_f(input_file):
    file_path = os.path.join("chunks", input_file)
    chunk = read_chunk(file_path)
    mapped_result = map_function(chunk)
    save_to_file(mapped_result, input_file.replace('.txt', '') + '_map', 'mapStep')
if __name__ == "__main__":
    os.makedirs("mapStep", exist_ok=True)
    os.makedirs("groupStep", exist_ok=True)
    os.makedirs("reduceStep", exist_ok=True)

    input_file_path = 'texts/test.txt'  # Replace with the actual path to your input file
    output_directory = 'chunks'
    max_chunk_size = 30 * 1024 * 1024  # 31.5MB

    split_and_lowercase(input_file_path, output_directory, max_chunk_size)
    threads = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        for filename in os.listdir("chunks"):
            if filename.endswith(".txt"):
                node = mapNode(1, f"mapNode-{filename}", 4, filename)
                future = executor.submit(node.run)
                threads.append(future)

    # Wait for all threads to complete
    concurrent.futures.wait(threads)

    print("Exiting Map Thread")

