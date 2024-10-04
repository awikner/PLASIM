#!/home1/09979/awikner/.conda/envs/globus/bin/python -u
#SBATCH -A TG-ATM170020
#SBATCH -p skx
#SBATCH -N 1
#SBATCH -n 1
#SBACTH --mem-per-cpu=4G
#SBATCH -t 10:00
#SBATCH -J postproc-submit
#SBATCH --output=/work2/09979/awikner/stampede3/PLASIM/log_files/submit-postproc-%j.out
#SBATCH --error=/work2/09979/awikner/stampede3/PLASIM/log_files/submit-postproc-%j.err

import subprocess
import shlex
import re
import shutil
import os
import sys

def list_of_ints(arg):
    return list(map(int, arg.split(',')))

def main(argv):
    run_yearly = True
    run_pl = False
    run_move = False
    runs = [3] + list(set(list(range(11, 49))) - set([19,39]))
    simstep_start = 6
    simsteps = 1
    years_per_simstep = 50
    submit_path = '/work2/09979/awikner/stampede3/PLASIM/submit_scripts'
    data_dir = '/scratch/09979/awikner/PLASIM/data'
    base_job_script = '/work2/09979/awikner/stampede3/PLASIM/job_scripts/plasim_postprocess_base.sh'
    job_dir = os.path.dirname(base_job_script)
    if not isinstance(argv, type(None)):
        runs = list_of_ints(argv[0])
        simstep_start = int(argv[1])
        simsteps = int(argv[2])
        run_yearly = bool(argv[3])
        run_pl = bool(argv[4])
        run_move = bool(argv[5])
    start_year = simstep_start * years_per_simstep + 1
    end_year = (simstep_start + simsteps) * years_per_simstep + 1
    jobIDs_postprocess = []

    num_cpus = 48
    runs_per_job = num_cpus // (simsteps * 3)
    for itr, run in enumerate(runs):
        if itr % runs_per_job == 0:
            if runs_per_job > 1:
                postprocess_job_script = os.path.join(job_dir, 
                                                      f'plasim_postprocess_sim{run}-to-{runs[min(itr + runs_per_job - 1, len(runs)-1)]}-{simstep_start:03}.sh')
            else:
                postprocess_job_script = os.path.join(job_dir, f'plasim_postprocess_sim{run}-{simstep_start:03}.sh')
            shutil.copy(base_job_script, postprocess_job_script)
        for simstep in range(simstep_start + 1, simstep_start + simsteps + 1):
            data_dir_run = os.path.join(data_dir, f'sim{run}-{simstep:03}')
            input_file = os.path.join(data_dir_run, f'MOST.{simstep:03}')
            output_file_else = os.path.join(data_dir_run, f'ground_truth_data{simstep:03}.nc')
            output_file_winds = os.path.join(data_dir_run, f'ground_truth_winds_data{simstep:03}.nc')
            output_file_precip = os.path.join(data_dir_run, f'ground_truth_precip_data{simstep:03}.nc')
            with open(postprocess_job_script, 'a') as file:
                line = f'./burn7 < ground_truth_data_namelist_else {input_file} {output_file_else} & \n'
                #print(line)
                file.write(line)
                line = f'./burn7 < ground_truth_data_namelist_winds {input_file} {output_file_winds} & \n'
                #print(line)
                file.write(line)
                line = f'./burn7 < ground_truth_data_namelist_precip {input_file} {output_file_precip} & \n'
                #print(line)
                file.write(line)
        if itr % runs_per_job == (runs_per_job - 1) or itr == (len(runs)-1):
            with open(postprocess_job_script, 'a') as file:
                file.write('wait')
            out_postprocess = subprocess.check_output(['sbatch', postprocess_job_script])
            jobID = str(int(re.search(r'Submitted batch job (.*?)\\', str(out_postprocess)).group(1)))
            jobIDs_postprocess.append(str(jobID))
    dependency_str = ':'.join(['afterok'] + jobIDs_postprocess)
    print(dependency_str)
    if run_yearly:
        run_str = ','.join([str(run) for run in runs])
        job_in = ['sbatch', '-d', dependency_str, os.path.join(submit_path, 'submit_plasim_yearly.py'), run_str, 
                str(simstep_start), str(simsteps), str(run_pl), str(run_move)]
        subprocess.run(job_in)

if __name__ == "__main__":
    print(sys.argv)
    if len(sys.argv) > 1:
        main(sys.argv[1:])
    else:
        main(None)
    

