# Lucene Building & Indexing Wikipedia-en

This project uses Lucene to build an index of Wikipedia-en and search it.
The search is just to verify the results, what we really care about is the build latency of the index.
It uses Kotlin Gradle (see `build.gradle.kts`).

The dataset itself is from [HuggingFace](https://huggingface.co/datasets/Cohere/wikipedia-2023-11-embed-multilingual-v3) and was downloaded / edited using `load_wikipedia.py`.
See the JVector implementation for downloading the dataset as `.fvec` and `.ivec` files: https://github.com/jbellis/jvector
There is also a less efficient implementation in `RunLoadDataset` available here.

Of particular interest are the following three programs:
* `BuildIndexLucene` -- this uses an optimized batch indexing method to build the index.
* `BuildIndexLuceneQuantized` -- this uses int8 quantization to reduce the size of the embeddings and the index. Builds upon the batch indexing method present in `BuildIndexLucene`.
* `BuildIndexLucenePlain` -- this uses the standard Lucene indexing method to build the index. It's not very memory efficient but is useful for comparison and small datasets.

Each program also keeps track of the peak memory usage (via a monitor in a separate thread), which is printed out to the terminal. Memory is analyzed every 100ms but can be changed through the `memorySleepAmount` variable.
