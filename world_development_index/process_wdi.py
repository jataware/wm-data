import pandas as pd
import json
from subprocess import call
import os
from glob import glob
from zipfile import ZipFile
from tqdm import tqdm
from datetime import datetime, timezone
from uuid import uuid4

import pdb

def main():
    download_data()

    raw_data = pd.read_csv('data/WDIData.csv')

    #list of valid country codes
    countries = pd.read_csv('country.csv')
    country_codes = set(countries['Alpha-3_Code'].tolist())
    series_info = pd.read_csv('data/WDISeries.csv')


    #DEBUG. user should define what the groups are in indicator_groups.json
    save_indicators(raw_data)

    #delete all CSVs in output folder
    for filename in glob('output/*.csv'):
        os.remove(filename)


    for name, indicators in tqdm(indicator_groups(), total=sum(1 for _ in indicator_groups()), desc='Making datasets'):

        #TODO: come up with better description from somewhere...
        description = f'World Bank Development Indicators: {", ".join(indicators)}'

        #collect all rows in df that have an indicator in indicators
        df_subset = raw_data[raw_data['Indicator Code'].isin(set(indicators))]

        #make dataset 
        df = make_dataset(df_subset, country_codes)

        #create metadata for dataset
        meta = make_metadata(df, series_info, name, description)#, feature_codes)

        #ensure output folder exists
        if not os.path.exists('output'):
            os.makedirs('output')
        
        #save data to csv and metadata to json
        df.to_csv(os.path.join('output', f'{name}.csv'), index=False)
        with open(os.path.join('output', f'{name}_meta.json'), 'w') as f:
            json.dump(meta, f)




def download_data():
    """download data and unzip if not already done"""
    
    data_link = 'http://databank.worldbank.org/data/download/WDI_csv.zip'

    # download to 'data' folder if not already downloaded
    if not os.path.exists('data'):
        os.makedirs('data')
    if not os.path.exists('data/WDI_csv.zip'):
        call(['wget', data_link, '-O', 'data/WDI_csv.zip'])
    else:
        print('Skipping download, data already exists')

    #unzip data if not already unzipped
    with ZipFile('data/WDI_csv.zip', 'r') as zip_ref:
        filenames = zip_ref.namelist()
        if not all([os.path.exists('data/' + filename) for filename in filenames]):
            print('Unzipping data...', end='', flush=True)
            zip_ref.extractall('data')
            print('Done')
        else:
            print('Skipping unzip, data already exists')
        

def save_indicators(df):
    """For debugging purposes, create mock version of indicator_groups.json"""

    indicators = df['Indicator Code'].unique().tolist()
    firsts = [indicator.split('.')[0] for indicator in indicators]
    # seconds = [f"{first}.{indicator.split('.')[1]}" for first, indicator in zip(firsts,indicators)]
    first_counts = {first: firsts.count(first) for first in set(firsts)}

    groups = {}
    for indicator, first in zip(indicators, firsts):
        count = first_counts[first]
        if count < 100:
            if first not in groups:
                groups[first] = []
            groups[first].append(indicator)
        else:
            second = f"{first}.{indicator.split('.')[1]}"
            if second not in groups:
                groups[second] = []
            groups[second].append(indicator)

    #map from name of group to its indicators
    indicators = {f"World_Development_Indicators.{name}": indicators for name, indicators in groups.items()}

    with open('indicator_groups.json', 'w') as f:
        json.dump(indicators, f)



def indicator_groups():
    """generator for returning groups of indicators to make datasets from"""

    with open ('indicator_groups.json', 'r') as f:
        groups = json.load(f)

    for name, group in groups.items():
        yield name, group




def make_dataset(df, country_codes):

    # columns = ['timestamp', 'country', 'admin1', 'admin2', 'admin3', 'lat', 'lng', 'feature', 'value']

    #year strings and timestamps (in milliseconds) from 1960 to 2021
    years = [(f'{year}', datetime(year, 1, 1, tzinfo=timezone.utc).timestamp()*1000) for year in range(1960, 2022)]

    rows = []
    # feature_codes = set(df['Indicator Code'].tolist())

    for _, row in tqdm(df.iterrows(), total=len(df), desc='Making rows', leave=False):
        #filter out rows that are not countries
        if row['Country Code'] not in country_codes:
            continue
        for year, timestamp in years:
            rows.append({
                'timestamp': timestamp,
                'country': row['Country Name'],
                'admin1': None,
                'admin2': None,
                'admin3': None,
                'lat': None,
                'lng': None,
                'feature': row['Indicator Code'], #Indicator Name will go in the description
                'value': row[year]
            })

    df = pd.DataFrame(rows)


    return df



def make_metadata(df, series_info, name, description):
    #get the min and max timestamps
    min_timestamp = df['timestamp'].min()
    max_timestamp = df['timestamp'].max()
    id = str(uuid4())

    features = df['feature'].unique().tolist()
    countries = df['country'].unique().tolist()

    #create a map from the feature name to the row in series_info['Indicator Name'] == feature name
    feature_map = {feature: dict(series_info[series_info['Series Code'] == feature].iloc[0]) for feature in features}
    def get_description(info: dict) -> str:
        ret = info.get('Long definition', None)
        if pd.isnull(ret):
            ret = info.get('Short definition', None)
        if pd.isnull(ret):
            ret = info.get('Indicator Name')
        if pd.isnull(ret):
            ret = ''
        return ret
    
    def get_unit(info) -> str:
        unit = info.get('Unit of measure', None)
        if pd.isnull(unit):
            try:
                #sometimes units are at the end of the indicator name (e.g. 'GDP (current US$)')
                unit = info['Indicator Name'].split('(')[-1].split(')')[0]
            except:
                unit = 'NA'
        return unit
        
    def get_unit_description(info) -> str:
        return get_unit(info) #no other source of info for this

    meta = {
        "id": id,
        "name": name,
        "family_name": None,
        "description": description,
        "created_at": datetime.now(timezone.utc).timestamp()*1000,
        "category": None,
        "domains": ["Economic Sciences"],
        "maintainer": {
            "name": "David Samson",
            "email": "david@jataware.com",
            "organization": "Jataware",
            "website": "http://databank.worldbank.org/data/download/WDI_csv.zip"
        },
        "data_paths": None,
        "outputs": [
            {
                "name": feature,
                "display_name": info['Indicator Name'],
                "description": get_description(info),
                "type": "float", #TODO: maybe check the datatype in df?
                "unit": get_unit(info),
                "unit_description": get_unit_description(info),
                "ontologies": None,
                "is_primary": True,
                "data_resolution": {
                    "temporal_resolution": "annual",
                    "spatial_resolution": [
                        0,
                        0
                    ]
                },
                "alias": {}
            } for feature, info in feature_map.items()
        ],
        "qualifier_outputs": [
            {
                "name": "timestamp",
                "display_name": "timestamp",
                "description": "timestamp",
                "type": "datetime",
                "unit": "ms",
                "unit_description": "milliseconds since January 1, 1970",
                "ontologies": None,
                "related_features": None
            },
            {
                "name": "country",
                "display_name": "country",
                "description": "country",
                "type": "country",
                "unit": None,
                "unit_description": None,
                "ontologies": None,
                "related_features": None
            }
        ],
        "tags": [],
        "geography": {
            "country": countries,
            "admin1": [],
            "admin2": [],
            "admin3": []
        },
        "period": {
            "gte": min_timestamp,
            "lte": max_timestamp
        },
        "deprecated": False,
        "data_sensitivity": "",
        "data_quality": ""
    }

    return meta
    



if __name__ == '__main__':
    main()