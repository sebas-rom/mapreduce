import logging
from collections import defaultdict
import os
import re

logging.basicConfig(filename='map_reduce_log.txt', level=logging.INFO)

def read_chunk(file_path):
    with open(file_path, 'r') as file:
        return file.read()

def map_function(chunk):
    word_count = []
    words = re.findall(r'\b\w+\b', chunk.lower())
    
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

def log_to_file(data, file_name):
    with open(file_name, 'w') as log_file:
        for item in data:
            log_file.write(f'{item}\n')

def map_reduce(input_dir, output_file, verbose=False):
    mapped_results = []

    for filename in os.listdir(input_dir):
        if filename.endswith(".txt"):
            file_path = os.path.join(input_dir, filename)
            chunk = read_chunk(file_path)
            mapped_result = map_function(chunk)
            mapped_results.extend(mapped_result)

            if verbose:
                log_to_file(mapped_result, 'map.txt')
                logging.info(f'Map result for {filename}: {mapped_result}')

    if verbose:
        log_to_file(mapped_results, 'shuffle_sort.txt')

    sorted_results = shuffle_and_sort(mapped_results)

    if verbose:
        log_to_file(sorted_results, 'shuffle_sort.txt')

    reduced_results = reduce_function(sorted_results)

    with open(output_file, 'w') as result_file:
        for word, count in reduced_results:
            result_file.write(f'("{word}",{count})\n')

    logging.info('Map-Reduce process completed successfully.')

if __name__ == "__main__":
    input_directory = 'chunks'
    output_file = 'result.txt'

    # Set verbose to True to enable logging intermediate results
    map_reduce(input_directory, output_file, verbose=True)
