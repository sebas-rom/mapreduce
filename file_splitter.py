import os

def split_file(input_file, output_dir, chunk_size):
    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    with open(input_file, 'rb') as infile:
        chunk_number = 1
        while True:
            # Read a chunk of data
            chunk_data = infile.read(chunk_size)

            # Break the loop if no more data is left
            if not chunk_data:
                break

            # Create a new chunk file
            chunk_filename = os.path.join(output_dir, f'chunk_{chunk_number}.txt')
            with open(chunk_filename, 'wb') as chunk_file:
                # Write the chunk data to the new file
                chunk_file.write(chunk_data)

            chunk_number += 1
