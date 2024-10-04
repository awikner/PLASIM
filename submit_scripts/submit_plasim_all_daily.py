import subprocess
import shlex
import re
import shutil
import os
import sys


run_plasim = False
run_postprocess = False
run_yearly = False
run_pl = True
run_move = False
#runs = [3] + list(range(11, 49))
#runs = [29, 34]
#runs = [3] + list(set(list(range(11, 49))) - set([19,39]))
runs = [50]
#runs = runs[12:]
#runs = [runs[12]]
#runs = list(set(list(range(10))) - set([3]))
#runs = 10
#runs = [4, 6, 7]
#runs = [46]
#runs = [11,12,13,14,17,18]
simstep_start = 0
simsteps = 40000
years_per_simstep = 50
#start_year = simstep_start * years_per_simstep + 1
#end_year = (simstep_start + simsteps) * years_per_simstep + 1
start_year = 29
end_year = 101
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
base_yearly_job_script = '/work2/09979/awikner/stampede3/PLASIM/job_scripts/plasim_yearly_base.sh'
base_pl_job_script = '/work2/09979/awikner/stampede3/PLASIM/job_scripts/plasim_pl_base.sh'
base_yearly_script = '/work2/09979/awikner/stampede3/PLASIM/job_scripts/start_get_yearly_ground_truth_data.sh'
base_interp_script = '/work2/09979/awikner/stampede3/PlaSim-emulator-diagnosis/Pangu-PlaSim-postprocessor/myjob_postproc.sh'
python_path = '/home1/09979/awikner/.conda/envs/globus/bin/python'
ground_truth_postprocessor = '/work2/09979/awikner/stampede3/PlaSim-emulator-diagnosis/Pangu-PlaSim-postprocessor/full_data_postprocessor.py'
get_yearly_ground_truth_data = '/work2/09979/awikner/stampede3/PLASIM/data/get_yearly_ground_truth_data.py'
move_postproc_data = '/work2/09979/awikner/stampede3/PLASIM/data/move_procced_files.py'
submit_path = '/work2/09979/awikner/stampede3/PLASIM/submit_scripts'
python_path = '/home1/09979/awikner/.conda/envs/globus/bin/python'
job_dir = os.path.dirname(base_job_script)
jobIDs_run = []
jobIDs_postprocess = []
jobIDs_yearly = []
jobIDs_pl = []

def make_new_postproc_script(file_iter, make_script = True):
    postprocess_job_script = os.path.join(job_dir, f'plasim_postprocess_run_sim{runs[0]}-to-{runs[-1]}-{simstep_start+1:05}-{file_iter}.sh')
    if make_script:
        shutil.copy(base_job_script, postprocess_job_script)
        subprocess.run(['chmod', '+x', postprocess_job_script])
    return postprocess_job_script

if run_plasim:
    for run in runs:
        check_icx = str(subprocess.check_output(['squeue', '-u', 'awikner', '-p', 'icx']))
        num_icx_jobs = len(check_icx.split('\\n')) - 2
        if num_icx_jobs < 12:
            out = subprocess.check_output([python_path, os.path.join(submit_path, 'submit_plasim.py'), '-n', 'sim%d' % run, 
                                            '-s', '%d' % simstep_start, '-e', str(simstep_start+simsteps), '-i'])
        else:
            out = subprocess.check_output([python_path, os.path.join(submit_path, 'submit_plasim.py'), '-n', 'sim%d' % run, 
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
    total_cpus = len(runs) * simsteps * 3
    cpus_per_node = 48
    num_nodes = total_cpus // cpus_per_node + 1
    total_cpus_submit = cpus_per_node * num_nodes

    lines = 0
    file_iter = 0
    postprocess_job_script = make_new_postproc_script(file_iter)
    dependency_str_postprocess = 'afterok:'
    procs_per_file = 48 * 20
    for itr, run in enumerate(runs):
        for simstep in range(simstep_start + 1, simstep_start + simsteps + 1):
            data_dir_run = os.path.join(data_dir, f'sim{run}')
            input_file = os.path.join(data_dir_run, f'MOST.{simstep:05}')
            output_file = os.path.join(data_dir_run, f'data{simstep:05}.nc')
            with open(postprocess_job_script, 'a') as file:
                line = f'./burn7 < full_data_nl {input_file} {output_file} & \n'
                #print(line)
                file.write(line)
            lines += 1
            if lines == procs_per_file or (run == runs[-1] and simstep == simstep_start + simsteps):
                with open(postprocess_job_script, 'a') as file:
                    file.write('wait')
                if run_plasim:
                    job_in = ['sbatch', '-d', dependency_str, '-N', '1', '--ntasks-per-node=48', '-c', '1', postprocess_job_script]
                else:
                    job_in = ['sbatch', '-N', '1', '--ntasks-per-node=48', '-c', '1', postprocess_job_script]
                out = subprocess.check_output(job_in)
                jobID = str(int(re.search(r'Submitted batch job (.*?)\\', str(out)).group(1)))
                if not (run == runs[-1] and simstep == simstep_start + simsteps):
                    dependency_str_postprocess += f'{jobID}:'
                    file_iter += 1
                    lines = 0
                    postprocess_job_script = make_new_postproc_script(file_iter)
                else:
                    dependency_str_postprocess += f'{jobID}'
    """
    postprocess_job_script = os.path.join(job_dir, f'plasim_postprocess_run_sim{runs[0]}-to-{runs[-1]}-{simstep_start+1:03}.sh')
    shutil.copy(base_job_script, postprocess_job_script)
    subprocess.run(['chmod', '+x', postprocess_job_script])
    with open(postprocess_job_script, 'a') as file:
        for itr in range(file_iter):
            postprocess_job_script_iter = make_new_postproc_script(itr, make_script=False)
            job_sub = ' '.join(['srun', '--exclusive', '-N', '1', '-n', '48', '-c', '1', 
                                '--mem-per-cpu=4G', postprocess_job_script_iter,  '&', '\n'])
            file.write(job_sub)
        file.write('wait')
    if run_plasim:
        job_in = ['sbatch', '-d', dependency_str, '-N', str(num_nodes), '--ntasks-per-node=48', '-c', '1', postprocess_job_script]
    else:
        job_in = ['sbatch', '-N', str(num_nodes), '--ntasks-per-node=48', '-c', '1', postprocess_job_script]
    out = subprocess.check_output(job_in)
    jobID = str(int(re.search(r'Submitted batch job (.*?)\\', str(out)).group(1)))
    dependency_str_postprocess = f'afterok:{jobID}'
    """
    print(dependency_str_postprocess)
if run_yearly:
    """
    yearly_job_script = os.path.join(job_dir, f'plasim_yearly_run_sim{runs[0]}-to-{runs[-1]}-{simstep_start+1:03}.sh')
    shutil.copy(base_yearly_job_script, yearly_job_script)
    for simstep in range(simstep_start + 1, simstep_start + simsteps + 1):
        runs_str = ','.join([str(run) for run in runs])
        job_sub = ' '.join(['srun', '--exclusive', '-N', '1', '-n', '1', '-c', '48', '--mem-per-cpu=4G', python_path, '-u',
                             get_yearly_ground_truth_data, data_dir, runs_str, str(simstep), '&', '\n'])
        with open(yearly_job_script, 'a') as file:
            file.write(job_sub)
    with open(yearly_job_script, 'a') as file:
        file.write('wait')
    if run_postprocess:
        job_in = ['sbatch', '-d', dependency_str_postprocess, '-N', str(simsteps), '--ntasks-per-node=1', '-c', '48', yearly_job_script]
    else:
        job_in = ['sbatch', '-N', str(simsteps), '--ntasks-per-node=1', '-c', '48', yearly_job_script]
    out = subprocess.check_output(job_in)
    jobID = str(int(re.search(r'Submitted batch job (.*?)\\', str(out)).group(1)))
    dependency_str_yearly = f'afterok:{jobID}'
    """
    dependency_str_yearly = 'afterok'
    for run in runs:
        dependency_str_yearly += ':'
        yearly_job_script = os.path.join(job_dir, f'plasim_yearly_run_sim{run}-{simstep_start+1:03}.sh')
        shutil.copy(base_yearly_job_script, yearly_job_script)
        for simstep in range(simstep_start + 1, simstep_start + simsteps + 1):
            runs_str = ','.join([str(run) for run in runs])
            job_sub = ' '.join(['srun', '-N', '1', '-n', '1', '-c', f'{48//simsteps}', '--mem-per-cpu=4G', python_path, '-u',
                                get_yearly_ground_truth_data, data_dir, str(run), str(simstep), '&', '\n'])
            with open(yearly_job_script, 'a') as file:
                file.write(job_sub)
        with open(yearly_job_script, 'a') as file:
            file.write('wait')
        if run_postprocess:
            job_in = ['sbatch', '-J', f'{run}-yearly', '-d', dependency_str_postprocess, '-N', str(1), f'--ntasks-per-node={simsteps}', '-c', f'{48//simsteps}', yearly_job_script]
        else:
            job_in = ['sbatch', '-J', f'{run}-yearly', '-N', str(1), f'--ntasks-per-node={simsteps}', '-c', f'{48//simsteps}', yearly_job_script]
        out = subprocess.check_output(job_in)
        jobID = str(int(re.search(r'Submitted batch job (.*?)\\', str(out)).group(1)))
        dependency_str_yearly += jobID
    print(dependency_str_yearly)
if run_pl:
    dependency_str_pl = 'afterok'
    for run in runs:
        dependency_str_pl += ':'
        pl_job_script = os.path.join(job_dir, f'plasim_pl_run_sim{run}-{simstep_start+1:03}.sh')
        shutil.copy(base_pl_job_script, pl_job_script)
        job_sub = ' '.join([python_path, ground_truth_postprocessor, '-n', str(run), '-ys', str(start_year),
                               '-ye', str(end_year), '\n'])
        with open(pl_job_script, 'a') as file:
            file.write(job_sub)
        if run_yearly:
            job_in = ['sbatch', '-J', f'{run}-pl-postproc', '-d', dependency_str_yearly, '-N', str(1), '--ntasks-per-node=48', pl_job_script]
        else:
            job_in = ['sbatch', '-J', f'{run}-pl-postproc', '-N', str(1), '--ntasks-per-node=48', pl_job_script]
        out = subprocess.check_output(job_in)
        jobID = str(int(re.search(r'Submitted batch job (.*?)\\', str(out)).group(1)))
        dependency_str_pl += jobID
    """
    pl_job_script = os.path.join(job_dir, f'plasim_pl_run_sim{runs[0]}-to-{runs[-1]}-{simstep_start+1:03}.sh')
    shutil.copy(base_pl_job_script, pl_job_script)
    for run in runs:
        job_sub = ' '.join(['srun', '--exclusive', '-N', '1', '-n', '1', '-c', '48', '--mem-per-cpu=4G', python_path,
                             ground_truth_postprocessor, '-n', str(run), '-ys', str(start_year),
                               '-ye', str(end_year), '&', '\n'])
        with open(pl_job_script, 'a') as file:
            file.write(job_sub)
    with open(pl_job_script, 'a') as file:
        file.write('wait')
    if run_yearly:
        job_in = ['sbatch', '-d', dependency_str_yearly, '-N', str(len(runs)), '--ntasks-per-node=1', '-c', '48', pl_job_script]
    else:
        job_in = ['sbatch', '-N', str(len(runs)), '--ntasks-per-node=1', '-c', '48', pl_job_script]
    out = subprocess.check_output(job_in)
    jobID = str(int(re.search(r'Submitted batch job (.*?)\\', str(out)).group(1)))
    dependency_str_pl = f'afterok:{jobID}'
    """
    print(dependency_str_pl)
if run_move:
    runs_str = ','.join([str(run) for run in runs])
    if run_pl:
        job_sub = ['sbatch', '-d', dependency_str_pl, move_postproc_data, '-r', runs_str, '-s', str(start_year), '-e', str(end_year)]
    else:
        job_sub = ['sbatch', move_postproc_data, '-r', runs_str, '-s', str(start_year), '-e', str(end_year)]
    out_move = subprocess.check_output(job_sub)

