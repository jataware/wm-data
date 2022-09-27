import xarray as xr
import pdb

def main():
    file = 'AR6_Projections/Regional/medium_confidence/ssp245/total_ssp245_medium_confidence_values.nc'

    ds = xr.open_dataset(file)
    df = ds.to_dataframe()
    df = df.reset_index()
    df = df.drop_duplicates()

    #keep only the median quantile
    df = df[df['quantiles'] == 0.5]

    #drop the quantiles, and locations column
    del(df['quantiles'])
    del(df['locations'])

    #save to csv
    df.to_csv('ssp245.csv', index=False)



if __name__ == '__main__':
    main()