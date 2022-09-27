import pandas as pd
import pdb


def main():
    data_path = 'idb5yr.all'

    # Read in the data with encoding latin-1
    df = pd.read_csv(data_path, encoding='latin-1', sep='|')


    #keep the columns we want
    to_keep = [
        '#YR', #year
        'NAME', #country name
        'POP', #population
        'GR', #growth rate (percent)
        'AREA_KM2', #area in km^2
        'POP_DENS', #population density (people per km^2),
        'TFR', #total fertility rate
        'E0', #life expectancy at birth
        'MR0_4', #mortality rate under 5
    ]

    df = df[to_keep]


    #save the data to a csv file
    df.to_csv('idb5yr.csv', index=False)


if __name__ == '__main__':
    main()