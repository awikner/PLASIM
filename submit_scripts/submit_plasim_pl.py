#!/home1/09979/awikner/.conda/envs/globus/bin/python -u
#SBATCH -A TG-ATM170020
#SBATCH -p skx
#SBATCH -N 1
#SBATCH -n 1
#SBACTH --mem-per-cpu=4G
#SBATCH -t 10:00
#SBATCH -J pl-submit
#SBATCH --output=/work2/09979/awikner/stampede3/PLASIM/log_files/submit-pl-%j.out
#SBATCH --error=/work2/09979/awikner/stampede3/PLASIM/log_files/submit-pl-%j.err

import subprocess
import shlex
import re
import shutil
import os
import sys

def list_of_ints(arg):
    return list(map(int, arg.split(',')))

def main(argv):
    run_move = False
    runs = [3] + list(set(list(range(11, 49))) - set([19,39]))
    runs = runs[19:]
    simstep_start = 6
    simsteps = 1
    years_per_simstep = 50
    submit_path = '/work2/09979/awikner/stampede3/PLASIM/submit_scripts'
    data_dir = '/scratch/09979/awikner/PLASIM/data'
    base_job_script = '/work2/09979/awikner/stampede3/PLASIM/job_scripts/plasim_postprocess_base.sh'
    get_yearly_ground_truth_data = '/work2/09979/awikner/stampede3/PLASIM/data/get_yearly_ground_truth_data.py'
    ground_truth_postprocessor = '/work2/09979/awikner/stampede3/PlaSim-emulator-diagnosis/Pangu-PlaSim-postprocessor/ground_truth_postprocessor.py'
    move_postproc_data = '/work2/09979/awikner/stampede3/PLASIM/data/move_procced_data.py'
    job_dir = os.path.dirname(base_job_script)
    if not isinstance(argv, type(None)):
        runs = list_of_ints(argv[0])
        simstep_start = int(argv[1])
        simsteps = int(argv[2])
        run_move = bool(argv[3])
    start_year = simstep_start * years_per_simstep + 1
    end_year = (simstep_start + simsteps) * years_per_simstep + 1
    jobIDs_pl = []

    for run in runs:
        job_sub = ['sbatch', ground_truth_postprocessor, '-n', str(run), '-ys', str(start_year), '-ye', str(end_year)]
        out_postprocess = subprocess.check_output(job_sub)
        jobID = str(int(re.search(r'Submitted batch job (.*?)\\', str(out_postprocess)).group(1)))
        jobIDs_pl.append(jobID)
    dependency_str_pl = ':'.join(['afterok'] + jobIDs_pl)
    print(dependency_str_pl)

    if run_move:
        runs_str = ','.join([str(run) for run in runs])
        job_sub = ['sbatch', '-d', dependency_str_pl, move_postproc_data, '-r', runs_str, '-s', str(start_year), '-e', str(end_year)]
        subprocess.run(job_sub)

if __name__ == "__main__":
    print(sys.argv)
    if len(sys.argv) > 1:
        main(sys.argv[1:])
    else:
        main(None)