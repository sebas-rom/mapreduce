import logging
from collections import defaultdict
import os
import re



def read_chunk(file_path):
    with open(file_path, 'r') as file:
        return file.read()

def map_function(chunk):
    word_count = []
    words = re.findall(r'\b\w+\b', chunk)
    
    for word in words:
        word_count.append((word, 1))
    
    return word_count

def shuffle_and_sort(mapped_results):
    sorted_results = defaultdict(list)
    
    for word, count in mapped_results:
        sorted_results[word].append(count)
    
    return sorted_results.items()

def reduce_function(sorted_results):
    reduced_results = []
    
    for word, counts in sorted_results:
        total_count = sum(counts)
        reduced_results.append((word, total_count))
    
    return reduced_results

def save_to_file(data, file_name , output_dir = 'logs'):
    os.makedirs(output_dir, exist_ok=True)
    log_filename = os.path.join(output_dir, f'{file_name}')
    with open(log_filename, 'w') as log_file:
        for item in data:
            log_file.write(f'{item}\n')

def map_reduce(input_dir, output_file = "result.txt", verbose=False):
    if verbose:
        os.makedirs("logs", exist_ok=True)
        logging.basicConfig(filename='logs/map_reduce_log.txt', level=logging.INFO)
    mapped_results_group = []

    for filename in os.listdir(input_dir):
        if filename.endswith(".txt"):
            file_path = os.path.join(input_dir, filename)
            chunk = read_chunk(file_path)
            mapped_result = map_function(chunk)
            
        mapped_results_group.extend(mapped_result)
        save_to_file(mapped_result,filename.replace('.txt', '') + '_map.txt','mapStep')  #Save map result to chunk_x_map.txt
        if verbose:
             logging.info(f'Map result for {filename} completed')
            
        # Reduce
            # Shuffle and sort 
        sorted_results = shuffle_and_sort(mapped_result)
        save_to_file(sorted_results, filename.replace('.txt', '') + '_group.txt','groupStep')
        if verbose:
            logging.info(f'ShufleSort result for {filename} completed')
                
        reduced_results = reduce_function(sorted_results)
        if verbose:
            logging.info(f'Reduce result for {filename} completed')
        with open(output_file, 'w') as result_file:
            for word, count in reduced_results:
                result_file.write(f'("{word}",{count})\n')

    logging.info('Map-Reduce process completed successfully.')

if __name__ == "__main__":
    input_directory = 'chunks'
    
    map_reduce(input_directory, verbose=True)
