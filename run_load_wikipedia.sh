#!/bin/bash

# Check if the correct number of arguments are provided
if [ $# -lt 2 ]; then
    echo "Usage: $0 <output_file> <max_entries> [datasource] [split] [subset]"
    exit 1
fi

# Get the output file and max entries from the arguments
output_file=$1
max_entries=$2
datasource=${3:-"Cohere/wikipedia-2023-11-embed-multilingual-v3"}
split=${4:-"train"}
subset=$5

# Define the virtual environment directory
VENV_DIR="venv"

# Check if the virtual environment directory exists
if [ ! -d "$VENV_DIR" ]; then
    # Create a Python virtual environment in a directory named 'venv'
    python3 -m venv $VENV_DIR
fi

# Activate the virtual environment
source $VENV_DIR/bin/activate

# Check if python_requirements.txt exists in the current directory
if [ ! -f python_requirements.txt ]; then
    echo "python_requirements.txt not found in the current directory!"
    deactivate
    exit 1
fi

# Install the required packages from python_requirements.txt
pip install -r python_requirements.txt

# Check if load_wikipedia.py exists in the current directory
if [ ! -f load_wikipedia.py ]; then
    echo "load_wikipedia.py not found in the current directory!"
    deactivate
    exit 1
fi

# Run the load_wikipedia.py script with the provided arguments
if [ -z "$subset" ]; then
    python load_wikipedia.py "$output_file" "$max_entries" "$datasource" "$split"
else
    python load_wikipedia.py "$output_file" "$max_entries" "$datasource" "$split" "$subset"
fi

# Deactivate the virtual environment
deactivate
