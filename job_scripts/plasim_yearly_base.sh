#!/bin/bash
#SBATCH -p skx
#SBATCH -t 12:00:00
#SBATCH -N 1
##SBATCH -n 48
#SBATCH -A TG-ATM170020
#SBATCH -J yearly
#SBATCH -o /work2/09979/awikner/stampede3/PLASIM/log_files/plasim-yearly-%j.out
#SBATCH -e /work2/09979/awikner/stampede3/PLASIM/log_files/plasim-yearly-%j.err
#SBATCH --mail-user=awikner@uchicago.edu
#SBATCH --mail-type=ALL

module load netcdf python
source activate globus
cd /work2/09979/awikner/stampede3/PLASIM

