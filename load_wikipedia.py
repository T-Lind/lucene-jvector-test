import sys
from datasets import load_dataset
from tqdm import tqdm


# Function to load and process chunks of the dataset
def process_dataset_in_chunks(output_file, max_entries=None, batch_size=1000, chunk_size=100_000):
    dataset = load_dataset("Cohere/wikipedia-2023-11-embed-multilingual-v3", "en")['train']
    total_size = len(dataset)

    # If max_entries is specified, use it, otherwise use the length of the dataset
    if max_entries is None:
        max_entries = total_size
    else:
        max_entries = min(max_entries, total_size)

    with open(output_file, 'w') as out_f:
        for start_idx in range(0, max_entries, chunk_size):
            end_idx = min(start_idx + chunk_size, max_entries)
            print(f"Processing records from {start_idx} to {end_idx}")

            # Load a chunk of the dataset
            ds_chunk = dataset.select(range(start_idx, end_idx))

            # Convert the dataset chunk to a list of dictionaries
            ds_chunk = ds_chunk.to_dict()

            titles = ds_chunk['title']
            embs = ds_chunk['emb']
            num_entries = len(titles)

            # Define the batch processing function
            def write_batch(start_index, end_index, file_handle):
                for i in range(start_index, end_index):
                    title = titles[i]
                    emb = embs[i]
                    emb_str = ','.join(map(str, emb))
                    line = f"{title}\t{emb_str}\n"
                    file_handle.write(line)

            # Process the dataset chunk in smaller batches and write to the output file
            for i in tqdm(range(0, num_entries, batch_size)):
                write_batch(i, min(i + batch_size, num_entries), out_f)


if __name__ == "__main__":
    # Check if the output file argument is provided
    if len(sys.argv) < 2:
        print("Usage: python script_name.py <output_file> [max_entries]")
        sys.exit(1)

    # Get the output file from the first script argument
    output_file = sys.argv[1]

    # Get the max_entries from the second script argument if provided
    max_entries = int(sys.argv[2]) if len(sys.argv) > 2 else None

    print(f"Output file: {output_file}"
          f"\nMax entries: {max_entries}")

    process_dataset_in_chunks(output_file, max_entries, batch_size=1000, chunk_size=100_000)
