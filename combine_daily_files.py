import cftime
import sys, os
import subprocess

def main(year):
    year = int(year[0])
    data_folder = '/scratch/09979/awikner/PLASIM/data/sim50'
    has_year_zero = True
    date_init = cftime.DatetimeProlepticGregorian(1, 1, 1, hour=0, has_year_zero=has_year_zero)
    date_start = cftime.DatetimeProlepticGregorian(year, 1, 1, hour=0, has_year_zero=has_year_zero)
    date_end = cftime.DatetimeProlepticGregorian(year+1, 1, 1, hour=0, has_year_zero=has_year_zero)
    start_idx = (date_start - date_init).days + 1
    end_idx = (date_end - date_init).days + 1
    data_files = [os.path.join(data_folder, f'data{idx}.nc') for idx in range(start_idx, end_idx)]
    cdo_input = ['cdo', '-P', '48', 'mergetime'] + data_files + [os.path.join(data_folder, f'data_{year}_all.nc')]
    subprocess.run(cdo_input)
    print(f'Created data_{year}_all.nc')

if __name__ == "__main__":
    main(sys.argv[1:])
