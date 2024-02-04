from concurrent.futures import ThreadPoolExecutor, wait
import os
import threading
import concurrent.futures

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
        while True:
            chunk_id = self.controller.get_next_available_chunk()
            if chunk_id is not None:
                # Check if the maximum chunks per executor is reached
                if self.processed_chunks >= self.max_chunks_per_executor:
                    print(f"{self.name} reached maximum chunks. Stopping.")
                    break

                
                try:
                    print(f"{self.name} processing chunk_{chunk_id}")

                except Exception as e:
                    print(f"{self.name} failed processing chunk_{chunk_id}: {str(e)}")
                    self.controller.mark_chunk_failed(chunk_id)
            else:
                print(f"{self.name} no more chunks available. Stopping.")
                break

    

def run_map_reduce_task(executor_id, controller, max_chunks_per_executor):
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as map_executor:

        map_threads = []
        for i in range(1, max_chunks_per_executor + 1):
            node = mapNode(1, f"mapNode-{i}-{executor_id}", controller, executor_id, max_chunks_per_executor)
            future = map_executor.submit(node.run)
            map_threads.append(future)

        concurrent.futures.wait(map_threads)
        print(f"Map Executor {executor_id} finished. Starting groupNode.")


def runComputers(computer_number):
    total_chunks = len([filename for filename in os.listdir('chunks') if filename.endswith(".txt")])
    controller = Controller(total_chunks)
    controller.initialize_chunks('chunks')
    with concurrent.futures.ThreadPoolExecutor(max_workers=computer_number) as executor:
        futures = [executor.submit(run_map_reduce_task, i, controller, controller.half_chunks) for i in range(1, computer_number + 1)]
    
    wait(futures)
    print("Exiting Main Thread")
if __name__ == "__main__":

    runComputers(2)

    


    
