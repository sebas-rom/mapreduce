from collections import defaultdict
import os
import re
import json

def read_chunk(file_path):
    with open(file_path, 'r') as file:
        return file.read()

def map_function(chunk):
    words = re.findall(r'\b\w+\b', chunk)
    word_count = [(word, 1) for word in words]
    return word_count

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
    
def map_task(file_path):
    chunk = read_chunk(file_path)
    mapped_result = map_function(chunk)
    filename = os.path.basename(file_path)  
    save_to_file(mapped_result,filename.replace('.txt', '') + '_map.txt','mapStep')  #Save map result to chunk_x_map.txt
    return filename.replace('.txt', '') + '_map.txt'
    
def group_task(dir_path):
    
    temp_group = []
    
    for filename in os.listdir(dir_path):    
        file_path = os.path.join(dir_path, filename)
        loaded_map = read_result_from_file(file_path)
        temp_group.extend(loaded_map)
        
    sorted_results = shuffle_and_sort(temp_group)
    save_to_file(sorted_results, filename.replace('.txt', '') + '_group','groupStep')   
        
def reduce_task(dir_path):
    for filename in os.listdir(dir_path):    
        file_path = os.path.join(dir_path, filename)
        loaded_group = read_result_from_file(file_path)
        reduced_results = reduce_function(loaded_group)
        save_to_file(reduced_results, filename.replace('.txt', '') + '_reduce','reduceStep')
    

def map_reduce(input_dir="chunks"):
    os.makedirs("mapStep", exist_ok=True)
    os.makedirs("groupStep", exist_ok=True)
    os.makedirs("reduceStep", exist_ok=True)
    
    # Map step
    for filename in os.listdir(input_dir):
        if filename.endswith(".txt"):
            file_path = os.path.join(input_dir, filename)
            chunk = read_chunk(file_path)
            mapped_result = map_function(chunk)  
            save_to_file(mapped_result,filename.replace('.txt', '') + '_map','mapStep')  #Save map result to chunk_x_map.txt

    # #group step
    temp_group = []
    for filename in os.listdir('mapStep'):    
        file_path = os.path.join('mapStep', filename)
        loaded_map = read_result_from_file(file_path)
        temp_group.extend(loaded_map)

    sorted_results = shuffle_and_sort(temp_group)
    save_to_file(sorted_results, filename.replace('.txt', '') + '_group','groupStep')

    #Reduce
    for filename in os.listdir('groupStep'):    
        file_path = os.path.join('groupStep', filename)
        loaded_group = read_result_from_file(file_path)
        reduced_results = reduce_function(loaded_group)
        save_to_file(reduced_results, filename.replace('.txt', '') + '_reduce','reduceStep')
        

if __name__ == "__main__":
    
    map_reduce()
