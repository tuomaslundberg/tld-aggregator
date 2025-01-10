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

set -euo pipefail

module use /appl/local/csc/modulefiles
module load pytorch/2.4

INPUT_DIR="/scratch/project_462000353/HPLT-REGISTERS/splits/deduplicated/fin_Latn/1"
OUTPUT_DIR="/scratch/project_462000353/tlundber/tld-aggregator-results"

find "$INPUT_DIR" -type f -name "*.jsonl" | while read -r file; do
    subdir=$(dirname "$file" | sed "s|$INPUT_DIR|$OUTPUT_DIR|")
    mkdir -p "$subdir"
done

FILE_NAME="0$SLURM_ARRAY_TASK_ID.jsonl"

find "$INPUT_DIR" -type f -name "$FILE_NAME" | while read -r input_file; do
    output_file=$(echo "$input_file" | sed "s|$INPUT_DIR|$OUTPUT_DIR|" | sed 's/\.jsonl$/.json/')
    python3 processor.py "$input_file" "$output_file"
done

wait
