import os, sys, getopt, shutil, subprocess

def main(argv):
    simname = ''
    target_dir = '/glade/derecho/scratch/awikner/PLASIM/data/200_year_sims/' 
    run_dir = '/ocean/projects/atm170004p/awikner/PLASIM/runs'
    data_dir = '/ocean/projects/atm170004p/awikner/PLASIM/data'
    base_run_dir = '/ocean/projects/atm170004p/awikner/PLASIM/runs/base_run'
    base_flow_json = '/ocean/projects/atm170004p/awikner/PLASIM/submit_scripts/base_input.json'
    base_job_script = '/ocean/projects/atm170004p/awikner/PLASIM/job_scripts/plasim_globus_base_2.sh'
    try:
        opts, args = getopt.getopt(argv,"n:t:r:",[])
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
    simnames = os.listdir(run_dir)
    datanames = os.listdir(data_dir)
    if len(simname) > 0:
        simname1 = simname + '1'; simname2 = simname + '2'
        if simname1 in simnames or simname2 in simnames:
            print('Simname already exists in run_dir.')
            raise ValueError
        if simname1 in datanames or simname2 in datanames:
            print('Simname already exists in data_dir.')
            raise ValueError
    else:
        existing_sims = [name for name in simnames if name[:3] == 'sim' and name[3:].isnumeric()]
        existing_sims.sort()
        simname1 = 'sim' + str(int(existing_sims[-1][3:])+1)
        simname2 = 'sim' + str(int(existing_sims[-1][3:])+2)
    print('Simname1: %s' % simname1)
    print('Simname2: %s' % simname2)
    os.mkdir(os.path.join(run_dir, simname1))
    os.mkdir(os.path.join(run_dir, simname2))
    os.mkdir(os.path.join(data_dir, simname1))
    os.mkdir(os.path.join(data_dir, simname2))
    runfiles = os.listdir(base_run_dir)
    for file in runfiles:
        shutil.copy2(os.path.join(base_run_dir, file), os.path.join(run_dir, simname1))
        shutil.copy2(os.path.join(base_run_dir, file), os.path.join(run_dir, simname2))

    with open(base_flow_json) as file:
        lines = file.readlines()

    new_flow_json1 = os.path.join(run_dir, simname1, 'globus_input_%s.json' % simname1)
    with open(new_flow_json1, 'w') as file:
        for line in lines:
            line = line.replace('%SOURCE%', os.path.join(data_dir, simname1))
            line = line.replace('%TARGET%', target_dir)
            file.write(line)
    new_flow_json2 = os.path.join(run_dir, simname2, 'globus_input_%s.json' % simname2)
    with open(new_flow_json2, 'w') as file:
        for line in lines:
            line = line.replace('%SOURCE%', os.path.join(data_dir, simname2))
            line = line.replace('%TARGET%', target_dir)
            file.write(line)
    
    with open(base_job_script) as file:
        lines = file.readlines()
    new_job_script = '/ocean/projects/atm170004p/awikner/PLASIM/job_scripts/plasim_globus_%s_%s.sh' % (simname1, simname2)
    with open(new_job_script, 'w') as file:
        for line in lines:
            line = line.replace('%SIMNAME1%', simname1)
            line = line.replace('%RUNDIR1%', os.path.join(run_dir, simname1))
            line = line.replace('%DATADIR1%', os.path.join(data_dir, simname1))
            line = line.replace('%INPUTFILE1%', new_flow_json1)
            line = line.replace('%SIMNAME2%', simname2)
            line = line.replace('%RUNDIR2%', os.path.join(run_dir, simname2))
            line = line.replace('%DATADIR2%', os.path.join(data_dir, simname2))
            line = line.replace('%INPUTFILE2%', new_flow_json2)
            file.write(line)

    submitted = False
    while not submitted:
        try:
            out = subprocess.check_output(['sbatch',new_job_script])
            submitted = True
        except:
            print('Submission failed, retrying...')
    print('Done')
    

if __name__ == '__main__':
    main(sys.argv[1:])
