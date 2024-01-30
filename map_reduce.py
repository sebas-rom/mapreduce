from collections import defaultdict
import os
import re
import json

def read_chunk(file_path):
    with open(file_path, 'r') as file:
        return file.read()

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