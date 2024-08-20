#!/bin/bash
#SBATCH -p skx
#SBATCH -t 6:00:00
#SBATCH -N 1
##SBATCH -n 48
#SBATCH -A TG-ATM170020
#SBATCH -J postproc
#SBATCH -o /work2/09979/awikner/stampede3/PLASIM/log_files/plasim-postprocess-%j.log
#SBATCH --mail-user=awikner@uchicago.edu
#SBATCH --mail-type=ALL

module load netcdf

DIR=/work2/09979/awikner/stampede3/PLASIM/postprocessor
export LD_LIBRARY_PATH=/opt/apps/intel24/netcdf/4.9.2/x86_64/lib:${LD_LIBRARY_PATH}

cd $DIR

