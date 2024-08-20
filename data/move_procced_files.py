#!/home1/09979/awikner/.conda/envs/globus/bin/python -u
#SBATCH -A TG-ATM170020
#SBATCH -p skx
#SBATCH -N 1
#SBATCH -n 48
#SBACTH --mem-per-cpu=4G
#SBATCH -t 4:00:00
#SBATCH -J move-data
#SBATCH --output=/work2/09979/awikner/stampede3/PLASIM/log_files/move-proc-files-%j.out
#SBATCH --error=/work2/09979/awikner/stampede3/PLASIM/log_files/move-proc-files-%j.err
import os, shutil, getopt, sys
from itertools import product
from multiprocessing import Pool
import time
import random
import subprocess

def list_of_ints(arg):
    return list(map(int, arg.split(',')))
#sims = list(range(4,10))
#sims = [4,6,7]
#sims = [31, 45, 46]
def move_procced_data(argv, remove_tmp_dirs = True, remove_unprocced_data = False, start_flow = True):
    sim = argv[0]
    start_year = argv[1]
    end_year = argv[2]
    if len(argv) == 6:
        remove_tmp_dirs = argv[3]
        remove_unprocced_data = argv[4]
        start_flow = argv[5]
    postproc_folders = ['ta', 'ua', 'va', 'hus', 'clw', 'zg']
    base_dir = '/scratch/09979/awikner/PLASIM/data'
    base_flow_input = '/work2/09979/awikner/stampede3/PLASIM/submit_scripts/base_input.json'
    flow_input = os.path.join(os.path.dirname(base_flow_input), f'move_sim{sim}_{start_year}_to_{end_year}.json')
    flow_output = os.path.join('/work2/09979/awikner/stampede3/PLASIM/log_files', f'info_move_sim{sim}_{start_year}_to_{end_year}.json')
    target_dir = '/glade/derecho/scratch/awikner/PLASIM/data/2000_year_sims_new/'
    datadir = os.path.join(base_dir, f'sim{sim}')
    if remove_tmp_dirs:
        for tmp_dir in [folder for folder in os.listdir(datadir) if os.path.isdir(os.path.join(datadir, folder)) and 'tmp' in folder]:
            shutil.rmtree(os.path.join(datadir, tmp_dir))
    for i, folder in enumerate(postproc_folders):
        years = [int(file.split('_')[0]) for file in os.listdir(os.path.join(datadir, folder)) if '_gaussian_postproc.nc' in file]
        years = list(set(years) & set(list(range(start_year, end_year))))
        if i == 0:
            procced_years = years
        else:
            procced_years = list(set(procced_years) & set(years))
    missing_years = list(set(list(range(start_year, end_year))) - set(procced_years))
    print(f'Sim {sim} is missing {len(missing_years)} proccessed years')
    print(missing_years)
    if remove_unprocced_data:
        print(f'Removing unprocessed data for Sim {sim}')
        for folder in postproc_folders:
            for year in procced_years:
                if os.path.isfile(os.path.join(datadir, folder, f'{year}_gaussian.nc')):
                    os.remove(os.path.join(datadir, folder, f'{year}_gaussian.nc'))
    """
    print(f'Moving {len(procced_years)} for Sim {sim}')
    proc_dir = os.path.join(base_dir, f'sim{sim}-proc-{start_year}-to-{end_year}')
    if not os.path.isdir(proc_dir):
        os.mkdir(proc_dir)
    for folder in [folder for folder in os.listdir(datadir) if os.path.isdir(os.path.join(datadir, folder)) and 'tmp' not in folder]:
        if not os.path.isdir(os.path.join(proc_dir, folder)):
            os.mkdir(os.path.join(proc_dir, folder))
        if folder in postproc_folders:
            for file in [f'{year}_gaussian_postproc.nc' for year in procced_years]:
                shutil.move(os.path.join(datadir, folder, file), os.path.join(proc_dir, folder, file))
        else:
            for file in [f'{year}_gaussian.nc' for year in procced_years]:
                shutil.move(os.path.join(datadir, folder, file), os.path.join(proc_dir, folder, file))
    """
    print('Moved files for Sim ' + str(sim))
    if start_flow:
        readlines = False
        while not readlines:
            try:
                with open(base_flow_input, 'r') as f:
                    lines = f.readlines()
                readlines = True
            except:
                print('Waiting to open base flow input...')
                time.sleep(random.randint(1,10))
        with open(flow_input, 'w') as f:
            for line in lines:
                line = line.replace('%SOURCE%', proc_dir)
                line = line.replace('%TARGET%', target_dir)
                f.write(line)
        flow_command = ['globus', 'flows', 'start', '-v', '--label', f'move_sim{sim}_{start_year}_to_{end_year}', 
                        '--input', flow_input, '12b4e68a-074c-42a3-a4a2-4e2cefd00c64']#, '>', flow_output]
        subprocess.run(flow_command)
    return


    

def main(argv):
    runs = [3] + list(set(list(range(11, 49))) - set([19,39]))
    simstep_start = 6
    simsteps = 1
    years_per_simstep = 50
    start_year = simstep_start * years_per_simstep + 1
    end_year = (simstep_start + simsteps) * years_per_simstep + 1
    print(argv)
    try:
        opts, args = getopt.getopt(argv,"r:s:e:",[])
    except getopt.GetoptError:
        print ('Some input arguments not recognized.')
        sys.exit(2)
    print(args)
    for opt, arg in opts:
        if opt == '-r':
            runs = list_of_ints(arg)
        elif opt == '-s':
            start_year = int(arg)
        elif opt == '-e':
            end_year = int(arg)

    pool = Pool()

    pool.map(move_procced_data, product(runs, [start_year], [end_year]))

    print("==="*5 +  " Done " + "==="*5)

    
if __name__ == "__main__":
    main(sys.argv[1:])
