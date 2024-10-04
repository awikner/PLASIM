import subprocess
import shlex
import re
import shutil
import os
import sys


run_plasim = True
run_postprocess = False
run_yearly = False
run_pl = False
run_move = False
#runs = [3] + list(range(11, 49))
#runs = [19,39]
runs = [3] + list(set(list(range(11, 49))) - set([19,39]))
runs = runs[12:20]
#runs = runs[20:]
#runs = list(set(list(range(10))) - set([3]))
#runs = 10
#runs = [4, 6, 7]
simstep_start = 7
simsteps = 6
years_per_simstep = 50
start_year = simstep_start * years_per_simstep + 1
end_year = (simstep_start + simsteps) * years_per_simstep + 1
years_per_simstep_interp = 8
"""
run_plasim = True
run_postprocess = True
run_yearly = True
run_pl = True
runs = [49]
simstep_start = 0
simsteps = 1
years_per_simstep = 5
start_year = simstep_start * years_per_simstep + 1
end_year = (simstep_start + simsteps) * years_per_simstep + 1
years_per_simstep_interp = 5
"""
jobIDs = []
data_dir = '/scratch/09979/awikner/PLASIM/data'
base_job_script = '/work2/09979/awikner/stampede3/PLASIM/job_scripts/plasim_postprocess_base.sh'
base_yearly_script = '/work2/09979/awikner/stampede3/PLASIM/job_scripts/start_get_yearly_ground_truth_data.sh'
base_interp_script = '/work2/09979/awikner/stampede3/PlaSim-emulator-diagnosis/Pangu-PlaSim-postprocessor/myjob_postproc.sh'
python_path = '/home1/09979/awikner/.conda/envs/globus/bin/python'
ground_truth_postprocessor = '/work2/09979/awikner/stampede3/PlaSim-emulator-diagnosis/Pangu-PlaSim-postprocessor/ground_truth_postprocessor.py'
get_yearly_ground_truth_data = '/work2/09979/awikner/stampede3/PLASIM/data/get_yearly_ground_truth_data.py'
move_postproc_data = '/work2/09979/awikner/stampede3/PLASIM/data/move_procced_data.py'
job_dir = os.path.dirname(base_job_script)
jobIDs_run = []
jobIDs_postprocess = []
jobIDs_yearly = []
jobIDs_pl = []
if run_plasim:
    for run in runs:
        out = subprocess.check_output(['python', 'submit_scripts/submit_plasim.py', '-n', 'sim%d' % run, 
                                       '-s', '%d' % simstep_start, '-e', str(simstep_start+simsteps)])
        #print(out)
        #sys.exit(2)
        jobID = str(int(re.search(r'Submitted batch job (.*?)\\', str(out)).group(1)))
        #print(jobID)
        jobIDs_run.append(str(jobID))
    dependency_str = ':'.join(['afterok'] + jobIDs_run)
    print(dependency_str)
    #sys.exit(2)
if run_postprocess:
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
        if itr % runs_per_job == 0:
            with open(postprocess_job_script, 'a') as file:
                file.write('wait')
            if run_plasim:
                out_postprocess = subprocess.check_output(['sbatch', '-d', dependency_str, postprocess_job_script])
            else:
                out_postprocess = subprocess.check_output(['sbatch', postprocess_job_script])
            jobID = str(int(re.search(r'Submitted batch job (.*?)\\', str(out_postprocess)).group(1)))
            jobIDs_postprocess.append(str(jobID))
    dependency_str_postprocess = ':'.join(['afterok'] + jobIDs_postprocess)
    print(dependency_str_postprocess)
if run_yearly:
    for simstep in range(simstep_start + 1, simstep_start + simsteps + 1):
        #out = subprocess.check_output(['python', 'submit_scripts/submit_plasim.py', '-n', 'sim%d' % run, '-s', '%d' % simstep_start])
        #jobID = int(re.search(r'Submitted batch job (.*?)\n', str(out)).group(1))
        #jobIDs.append(jobID)
        """
        postprocess_job_script = os.path.join(job_dir, f'start_get_yearly_ground_truth_data_step{simstep_start:03}.sh')
        shutil.copy(base_yearly_script, postprocess_job_script)
        for run in runs:
            data_dir_run = os.path.join(data_dir, f'sim{run}-{simstep:03}')
            #input_file = os.path.join(data_dir_run, f'MOST.{simstep:03}')
            output_file_else = os.path.join(data_dir_run, f'ground_truth_data{simstep:03}.nc')
            output_file_winds = os.path.join(data_dir_run, f'ground_truth_winds_data{simstep:03}.nc')
            output_file_precip = os.path.join(data_dir_run, f'ground_truth_precip_data{simstep:03}.nc')
            savedir = os.path.join(data_dir, f'sim{run}')
            if not os.path.isdir(savedir):
                os.mkdir(savedir)
            with open(postprocess_job_script, 'a') as file:
                line = f'{python_path} get_yearly_ground_truth_data.py {output_file_else} {output_file_winds} {output_file_precip} {savedir} & \n'
                #print(line)
                file.write(line)
        with open(postprocess_job_script, 'a') as file:
            file.write('wait')
        """
        runs_str = ','.join([str(run) for run in runs])
        if run_postprocess:
            job_sub = ['sbatch', '-d', dependency_str_postprocess, get_yearly_ground_truth_data, data_dir, runs_str, str(simstep)]
            out_yearly = subprocess.check_output(job_sub)
        else:
            job_sub = ['sbatch', get_yearly_ground_truth_data, data_dir, runs_str, str(simstep)]
            out_yearly = subprocess.check_output(job_sub)
        jobID = str(int(re.search(r'Submitted batch job (.*?)\\', str(out_yearly)).group(1)))
        jobIDs_yearly.append(str(jobID))
    dependency_str_yearly = ':'.join(['afterok'] + jobIDs_yearly)
    print(dependency_str_yearly)
if run_pl:
    for run in runs:
        """
        postprocess_job_script = os.path.join(job_dir, f'plasim_interp_sim{run}-{simstep_start:04}.sh')
        shutil.copy(base_interp_script, postprocess_job_script)
        for year_start, year_end in zip(range(start_year, end_year, years_per_simstep_interp),
                                         range(start_year + years_per_simstep_interp, end_year + years_per_simstep_interp, years_per_simstep_interp)):
            year_end = min(year_end, end_year)
            with open(postprocess_job_script, 'a') as file:
                line = f'{python_path} -u ground_truth_postprocessor.py -ys {year_start} -ye {year_end} -n {run} & \n'
                #print(line)
                file.write(line)
        with open(postprocess_job_script, 'a') as file:
            file.write('wait')
        """
        if run_yearly:
            job_sub = ['sbatch', '-d', dependency_str_yearly, ground_truth_postprocessor, '-n', str(run), '-ys', str(start_year), '-ye', str(end_year)]
            out_postprocess = subprocess.check_output(job_sub)
        else:
            job_sub = ['sbatch', ground_truth_postprocessor, '-n', str(run), '-ys', str(start_year), '-ye', str(end_year)]
            out_postprocess = subprocess.check_output(job_sub)
        jobID = str(int(re.search(r'Submitted batch job (.*?)\\', str(out_postprocess)).group(1)))
        jobIDs_pl.append(jobID)
    dependency_str_pl = ':'.join(['afterok'] + jobIDs_pl)
    print(dependency_str_pl)
if run_move:
    runs_str = ','.join([str(run) for run in runs])
    if run_postprocess:
        job_sub = ['sbatch', '-d', dependency_str_pl, move_postproc_data, '-r', runs_str, '-s', str(start_year), '-e', str(end_year)]
    else:
        job_sub = ['sbatch', move_postproc_data, '-r', runs_str, '-s', str(start_year), '-e', str(end_year)]
    out_move = subprocess.check_output(job_sub)

