from collections import defaultdict
import os
import re

def read_chunk(file_path):
    with open(file_path, 'r') as file:
        return file.read()

def map_function(chunk):
    word_count = defaultdict(int)
    words = re.findall(r'\b\w+\b', chunk.lower())
    
    for word in words:
        word_count[word] += 1
    
    return word_count.items()

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

def map_reduce(input_dir, output_file):
    mapped_results = []
    
    # Map phase
    for filename in os.listdir(input_dir):
        if filename.endswith(".txt"):
            file_path = os.path.join(input_dir, filename)
            chunk = read_chunk(file_path)
            mapped_results.extend(map_function(chunk))

    # Shuffle and Sort phase
    sorted_results = shuffle_and_sort(mapped_results)

    # Reduce phase
    reduced_results = reduce_function(sorted_results)

    # Save the final results
    with open(output_file, 'w') as result_file:
        for word, count in reduced_results:
            result_file.write(f'("{word}",{count})\n')

if __name__ == "__main__":
    input_directory = 'chunks'
    output_file = 'result.txt'

    map_reduce(input_directory, output_file)
