import subprocess
import shlex
import re
import shutil
import os

runs = [3] + list(range(11, 49))
simstep_start = 0
simsteps = 6
jobIDs = []
data_dir = '/scratch/09979/awikner/PLASIM/data'
base_job_script = '/work2/09979/awikner/stampede3/PLASIM/job_scripts/start_get_yearly_ground_truth_data.sh'
python_path = '/home1/09979/awikner/.conda/envs/globus/bin/python'
job_dir = os.path.dirname(base_job_script)
for simstep in range(simstep_start + 1, simstep_start + simsteps + 1):
    #out = subprocess.check_output(['python', 'submit_scripts/submit_plasim.py', '-n', 'sim%d' % run, '-s', '%d' % simstep_start])
    #jobID = int(re.search(r'Submitted batch job (.*?)\n', str(out)).group(1))
    #jobIDs.append(jobID)
    postprocess_job_script = os.path.join(job_dir, f'start_get_yearly_ground_truth_data_step{simstep_start:03}.sh')
    shutil.copy(base_job_script, postprocess_job_script)
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
            print(line)
            file.write(line)
    with open(postprocess_job_script, 'a') as file:
        file.write('wait')
    out_postprocess = subprocess.check_output(['sbatch', postprocess_job_script])
    #out_postprocess = subprocess.check_output(['sbatch', '-d', f'afterok:{jobID}', postprocess_job_script])