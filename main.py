from concurrent.futures import ThreadPoolExecutor
import os
import threading

from file_splitter import split_and_lowercase

exitFlag = 0
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
    input_file_path = 'texts/test.txt'  # Replace with the actual path to your input file
    output_directory = 'chunks'
    max_chunk_size = 30 * 1024 *1024  # 31.5MB

    split_and_lowercase(input_file_path, output_directory, max_chunk_size)


class mapNode (threading.Thread):
	def __init__(self, threadID, name, input_dir):
		threading.Thread.__init__(self)
		self.threadID = threadID
		self.name = name
		self.input_dir = input_dir
	def run(self):
		print ("Starting " + self.name)
		threadLock.acquire()

		for filename in os.listdir(self.input_dir):
            if filename.endswith(".txt"):
                file_path = os.path.join(self.input_dir, filename)
                chunk = read_chunk(file_path)
                mapped_result = map_function(chunk)  
                save_to_file(mapped_result,filename.replace('.txt', '') + '_map','mapStep')
		print ("Exiting " + self.name)

threadLock = threading.Lock()
threads = []

mapNode1 = mapNode(1, "mapNode-1", "chunks")
mapNode2 = mapNode(2, "mapNode-2", "chunks")

mapNode1.start()
mapNode2.start()

threads.append(mapNode1)
threads.append(mapNode2)

for t in threads:
t.join()
print "Exiting Map Thread"

