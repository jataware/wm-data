#script for automatically downloading CMIP data



variables = [
    'tas',      #'Mean-Temperature',
    'tasmin',   #'Min-Temperature',
    'tasmax',   #'Max-Temperature',
    'pr',       #'Precipitation',
]

percentile = [
    'median',   #'Median (50th)',
    'p90',      #'90th',
    'p10',      #'10th',
]

scenarios = [
    'ssp245',   #'SPP2-4.5',
    'ssp585',   #'SPP5-8.5',
]

calculation = [
    'mean',
    # 'Anomaly (from reference period, 1995-20014)', #TODO: for later
]


#examples

['Mean-Temperature', 'Median (50th)', 'SPP2-4.5', 'mean']
'https://climatedata.worldbank.org/thredds/fileServer/CRM/cmip6/all-regridded-bct-ssp245-climatology/tas/median/annual/climatology-tas-annual-mean/2020-2039/climatology-tas-annual-mean_cmip6_annual_all-regridded-bct-ssp245-climatology_median_2020-2039.nc'


['Mean-Temperature', 'Median (50th)', 'SPP5-8.5', 'mean']
'https://climatedata.worldbank.org/thredds/fileServer/CRM/cmip6/all-regridded-bct-ssp585-climatology/tas/median/annual/climatology-tas-annual-mean/2020-2039/climatology-tas-annual-mean_cmip6_annual_all-regridded-bct-ssp585-climatology_median_2020-2039.nc'


['Mean-Temperature', '90th', 'SPP2-4.5', 'mean']
'https://climatedata.worldbank.org/thredds/fileServer/CRM/cmip6/all-regridded-bct-ssp245-climatology/tas/p90/annual/climatology-tas-annual-mean/2020-2039/climatology-tas-annual-mean_cmip6_annual_all-regridded-bct-ssp245-climatology_p90_2020-2039.nc'


['Min-Temperature', '10th', 'SPP2-4.5', 'mean']
'https://climatedata.worldbank.org/thredds/fileServer/CRM/cmip6/all-regridded-bct-ssp245-climatology/tasmin/p10/annual/climatology-tasmin-annual-mean/2020-2039/climatology-tasmin-annual-mean_cmip6_annual_all-regridded-bct-ssp245-climatology_p10_2020-2039.nc'


['Max-Temperature', '90th', 'SPP2-4.5', 'mean']
'https://climatedata.worldbank.org/thredds/fileServer/CRM/cmip6/all-regridded-bct-ssp245-climatology/tasmax/p90/annual/climatology-tasmax-annual-mean/2020-2039/climatology-tasmax-annual-mean_cmip6_annual_all-regridded-bct-ssp245-climatology_p90_2020-2039.nc'

['Precipitation', 'Median (50th)', 'SPP2-4.5', 'mean']
'https://climatedata.worldbank.org/thredds/fileServer/CRM/cmip6/all-regridded-bct-ssp245-climatology/pr/median/annual/climatology-pr-annual-mean/2020-2039/climatology-pr-annual-mean_cmip6_annual_all-regridded-bct-ssp245-climatology_median_2020-2039.nc'


from subprocess import call


#process for downloading all combinations of data with wget
for variable in variables:
    for p in percentile:
        for scenario in scenarios:
            for calc in calculation:
                url = f'https://climatedata.worldbank.org/thredds/fileServer/CRM/cmip6/all-regridded-bct-{scenario}-climatology/{variable}/{p}/annual/climatology-{variable}-annual-mean/2020-2039/climatology-{variable}-annual-mean_cmip6_annual_all-regridded-bct-{scenario}-climatology_{p}_2020-2039.nc'
                print(f'getting {[variable, p, scenario, calc]} from {url}')
                call(['wget', url])

