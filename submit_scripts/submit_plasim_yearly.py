#!/home1/09979/awikner/.conda/envs/globus/bin/python -u
#SBATCH -A TG-ATM170020
#SBATCH -p skx
#SBATCH -N 1
#SBATCH -n 1
#SBACTH --mem-per-cpu=4G
#SBATCH -t 10:00
#SBATCH -J yearly-submit
#SBATCH --output=/work2/09979/awikner/stampede3/PLASIM/log_files/submit-yearly-%j.out
#SBATCH --error=/work2/09979/awikner/stampede3/PLASIM/log_files/submit-yearly-%j.err

import subprocess
import shlex
import re
import shutil
import os
import sys

def list_of_ints(arg):
    return list(map(int, arg.split(',')))

def main(argv):
    run_pl = False
    run_move = False
    runs = [3] + list(set(list(range(11, 49))) - set([19,39]))
    simstep_start = 6
    simsteps = 1
    years_per_simstep = 50
    submit_path = '/work2/09979/awikner/stampede3/PLASIM/submit_scripts'
    data_dir = '/scratch/09979/awikner/PLASIM/data'
    base_job_script = '/work2/09979/awikner/stampede3/PLASIM/job_scripts/plasim_postprocess_base.sh'
    get_yearly_ground_truth_data = '/work2/09979/awikner/stampede3/PLASIM/data/get_yearly_ground_truth_data.py'
    job_dir = os.path.dirname(base_job_script)
    if not isinstance(argv, type(None)):
        runs = list_of_ints(argv[0])
        simstep_start = int(argv[1])
        simsteps = int(argv[2])
        run_pl = bool(argv[3])
        run_move = bool(argv[4])
    start_year = simstep_start * years_per_simstep + 1
    end_year = (simstep_start + simsteps) * years_per_simstep + 1
    jobIDs_yearly = []

    for simstep in range(simstep_start + 1, simstep_start + simsteps + 1):
        runs_str = ','.join([str(run) for run in runs])
        job_sub = ['sbatch', get_yearly_ground_truth_data, data_dir, runs_str, str(simstep)]
        out_yearly = subprocess.check_output(job_sub)
        jobID = str(int(re.search(r'Submitted batch job (.*?)\\', str(out_yearly)).group(1)))
        jobIDs_yearly.append(str(jobID))
    dependency_str_yearly = ':'.join(['afterok'] + jobIDs_yearly)
    print(dependency_str_yearly)
    if run_pl:
        run_str = ','.join([str(run) for run in runs])
        job_in = ['sbatch', '-d', dependency_str_yearly, os.path.join(submit_path, 'submit_plasim_pl.py'), run_str, 
                str(simstep_start), str(simsteps), str(run_move)]
        subprocess.run(job_in)

if __name__ == "__main__":
    print(sys.argv)
    if len(sys.argv) > 1:
        main(sys.argv[1:])
    else:
        main(None)
