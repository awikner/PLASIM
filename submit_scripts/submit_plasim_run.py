import subprocess
import shlex
import re
import shutil
import os
import sys


run_postprocess = True
run_yearly = False
run_pl = False
run_move = False
runs = [3] + list(set(list(range(11, 49))) - set([19,39]))
simstep_start = 6
simsteps = 1
years_per_simstep = 50
start_year = simstep_start * years_per_simstep + 1
end_year = (simstep_start + simsteps) * years_per_simstep + 1
years_per_simstep_interp = 8
jobIDs_run = []
submit_path = '/work2/09979/awikner/stampede3/PLASIM/submit_scripts'
for run in runs:
    check_icx = str(subprocess.check_output(['squeue', '-u', 'awikner', '-p', 'icx']))
    num_icx_jobs = len(check_icx.split('\\n')) - 2
    if num_icx_jobs < 12:
        out = subprocess.check_output(['python', os.path.join(submit_path, 'submit_plasim.py'), '-n', 'sim%d' % run, 
                                        '-s', '%d' % simstep_start, '-e', str(simstep_start+simsteps), '-i'])
    else:
        out = subprocess.check_output(['python', os.path.join(submit_path, 'submit_plasim.py'), '-n', 'sim%d' % run, 
                                        '-s', '%d' % simstep_start, '-e', str(simstep_start+simsteps)])
    #print(out)
    #sys.exit(2)
    jobID = str(int(re.search(r'Submitted batch job (.*?)\\', str(out)).group(1)))
    #print(jobID)
    jobIDs_run.append(str(jobID))
dependency_str = ':'.join(['afterok'] + jobIDs_run)
print(dependency_str)
if run_postprocess:
    run_str = ','.join([str(run) for run in runs])
    job_in = ['sbatch', '-d', dependency_str, os.path.join(submit_path, 'submit_plasim_postprocess.py'), run_str, 
              str(simstep_start), str(simsteps), str(run_yearly), str(run_pl), str(run_move)]
    subprocess.run(job_in)