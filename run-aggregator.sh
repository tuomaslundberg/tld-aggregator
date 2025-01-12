#!/bin/bash

#SBATCH --job-name=hplt-tld-aggregator
#SBATCH --account=project_462000353
#SBATCH --time=72:00:00
#SBATCH --partition=small
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --mem=16G
#SBATCH --cpus-per-task=1
#SBATCH --output=slurm-logs/array_%A_%a.out
#SBATCH --error=slurm-logs/array_%A_%a.err
#SBATCH --array=0-7

# If run without sbatch, invoke here
if [ -z $SLURM_JOB_ID ]; then
    sbatch "$0" "$@"
    exit
fi

# See https://gist.github.com/mohanpedala/1e2ff5661761d3abd0385e8223e16425?permalink_comment_id=3935570
set -euo pipefail

# These are not probably necessary
module use /appl/local/csc/modulefiles
module load pytorch/2.4

# The data folder
INPUT_DIR="/scratch/project_462000353/HPLT-REGISTERS/splits/deduplicated"
# The result distribution folder
OUTPUT_DIR="/scratch/project_462000353/tlundber/tld-aggregator/tld-aggregator-results"

# Build the directory structure first, so we don't get race conditions in the Python script
find "$INPUT_DIR" -type f -name "*.jsonl" | while read -r file; do
    subdir=$(dirname "$file" | sed "s|$INPUT_DIR|$OUTPUT_DIR|")
    mkdir -p "$subdir"
done

# Each job will find the .jsonl split files according to the task ID
FILE_NAME="0$SLURM_ARRAY_TASK_ID.jsonl"

# Run the processor for each file. Be careful, this loop processes an immense amount of data!
find "$INPUT_DIR" -type f -name "$FILE_NAME" | while read -r input_file; do
    output_file=$(echo "$input_file" | sed "s|$INPUT_DIR|$OUTPUT_DIR|" | sed 's/\.jsonl$/.json/')
    python3 processor.py "$input_file" "$output_file"
done

# Wait for the loop to exit although it's not concurrent, just to be sure
wait
