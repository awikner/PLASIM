#!/bin/bash
#SBATCH -J pumaburn
#SBATCH -p skx
#SBATCH -t 4:00:00
#SBATCH -N 1
#SBATCH -n 1
#SBATCH -A TG-ATM170020

module load netcdf

DIR=/work2/09979/awikner/stampede3/PLASIM/postprocessor
export LD_LIBRARY_PATH=/opt/apps/intel24/netcdf/4.9.2/x86_64/lib:${LD_LIBRARY_PATH}

cd $DIR
INPUT_FILE=$1
OUTPUT_FILE=$2

./burn7 < ground_truth_data_namelist_else $INPUT_FILE $OUTPUT_FILE
