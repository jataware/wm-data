import netCDF4
from glob import glob
import xarray as xr
from matplotlib import pyplot as plt
from datetime import date, timedelta
import json
import logging
import os

import pdb






def main():
   
    #DEBUG
    # file = 'data/timeseries-tas-annual-mean_cmip6_annual_all-regridded-bct-ssp245-timeseries_median_2015-2100.nc'
    

   
    # pdb.set_trace()
   
   
   
    data_dir = 'data'
    files = glob(f'{data_dir}/*.nc')  
    for file in files:
        #get filename from path
        filename = os.path.basename(file)
        fields = filename.split('_')
        var = fields[0]
        value = var.split('-')[1]
        model = fields[3].split('-')[3]

        #read the file


        ds = xr.open_dataset(file)
        df = ds.to_dataframe()
        df = df.reset_index()
        del(df['nv'])
        df = df.drop_duplicates()
        out = f'{model}-{value}-annual-median.csv'
        print(f'Writing {out}')
        df[['year','lat','lon',var]].to_csv(f'{model}-{value}-annual-median.csv', index=False)









if __name__ == '__main__':
    main()