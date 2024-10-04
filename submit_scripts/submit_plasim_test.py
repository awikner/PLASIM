import os, sys, getopt, shutil, subprocess, time
from natsort import natsorted
import numpy as np

def main(argv):
    simname = ''
    simstep_start = 6
    simstep_end = 12
    target_dir = '/glade/derecho/scratch/awikner/PLASIM/data/2000_year_sims_new/' 
    run_dir = '/scratch/09979/awikner/PLASIM/runs'
    data_dir = '/scratch/09979/awikner/PLASIM/data'
    base_run_dir = '/work2/09979/awikner/stampede3/PLASIM/runs/base_run'
    base_flow_json = '/work2/09979/awikner/stampede3/PLASIM/submit_scripts/base_input.json'
    base_job_script = '/work2/09979/awikner/stampede3/PLASIM/job_scripts/plasim_globus_base.sh'
    try:
        opts, args = getopt.getopt(argv,"n:t:r:s:",[])
    except getopt.GetoptError:
        print ('Some input arguments not recognized.')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-n':
            simname = str(arg)
        elif opt == '-t':
            target_dir = str(arg)
        elif opt == '-r':
            base_run_dir = str(arg)
        elif opt == '-s':
            simstep_start = int(arg)
            simstep_end = simstep_start + 1
    simnames = os.listdir(run_dir)
    datanames = os.listdir(data_dir)
    #if simname in simnames:
    #    print('Simname already exists in run_dir.')
    #    raise ValueError
    #if np.any([simname + '-' in dataname for dataname in datanames]):
    #    print('Simname already exists in data_dir.')
    #    raise ValueError
    if len(simname) == 0:
        existing_sims = [name for name in simnames if name[:3] == 'sim' and name[3:].isnumeric()]
        existing_sims = natsorted(existing_sims)
        #print(existing_sims)
        if len(existing_sims) == 0:
            simname = 'sim0'
        else:
            simname = 'sim' + str(int(existing_sims[-1][3:])+1)
    #print('Simname: %s' % simname)
    if not os.path.exists(os.path.join(run_dir, simname)):
        os.mkdir(os.path.join(run_dir, simname))
    runfiles = os.listdir(base_run_dir)
    for file in runfiles:
        shutil.copy2(os.path.join(base_run_dir, file), os.path.join(run_dir, simname))

    with open(base_flow_json) as file:
        lines = file.readlines()

    new_flow_json_base = os.path.join(run_dir, simname, 'globus_input_%s_' % simname)
    new_flow_json = []
    for simstep in range(simstep_start+1, simstep_end+1):
        new_flow_json.append('%s%03d.json' % (new_flow_json_base, simstep))
        with open(new_flow_json[-1], 'w') as file:
            for line in lines:
                line = line.replace('%SOURCE%', os.path.join(data_dir, '%s-%03d' % (simname, simstep)))
                line = line.replace('%TARGET%', target_dir)
                file.write(line)
    
    with open(base_job_script) as file:
        lines = file.readlines()
    new_job_script = '/work2/09979/awikner/stampede3/PLASIM/job_scripts/plasim_globus_%s.sh' % simname
    with open(new_job_script, 'w') as file:
        for line in lines:
            line = line.replace('%STEPSSTART%', str(simstep_start))
            line = line.replace('%STEPSEND%', str(simstep_end))
            line = line.replace('%SIMNAME%', simname)
            line = line.replace('%RUNDIR%', os.path.join(run_dir, simname))
            line = line.replace('%DATADIR%', os.path.join(data_dir, simname))
            line = line.replace('%INPUTFILE%', new_flow_json_base)
            file.write(line)

    submitted = False
    while not submitted:
        try:
            out = subprocess.check_output(['sbatch',new_job_script])
            submitted = True
            print(out)
        except:
            time.sleep(1)
            #print('Submission failed, retrying...')
    #print('Done')
    

if __name__ == '__main__':
    main(sys.argv[1:])
