import pandas as pd
from tqdm import tqdm
import geopandas as gpd
import shapely
import numpy as np

import pdb

indicator_codes = {
    "ws": "water stress",
    "sv": "seasonal variability",
    "ut": "water demand", #[m/year] meters per year
    "bt": "water supply", #[m/year] meters per year
}
year_codes = {
    "20": 2020,
    "30": 2030,
    "40": 2040, 
}
scenario_codes = {
    "24": "optimistic",#"ssp2 rcp45 (optimistic)",
    "28": "business as usual", #"ssp2 rcp85 (business as usual)",
    "38": "pessimistic",#"ssp3 rcp85 (pessimistic)",
}
data_types = {
    "c": "change from baseline",
    "t": "future value",
    "u": "uncertainty value",# (available for seasonal variablity and water supply)
}
suffixes = {
    "l": "label string",
    "r": "raw value",
}

class Scenario:
    def __init__(self, col):
        if not isinstance(col, str) or len(col) != 8:
            raise ValueError(f'Invalid column name "{col}"')

        self.ii = col[0:2]
        self.YY = col[2:4]
        self.SS = col[4:6]
        self.T = col[6:7]
        self.X = col[7:8]

        if self.ii not in indicator_codes:
            raise ValueError(f'Invalid indicator code for column "{col}"')
        if self.YY not in year_codes:
            raise ValueError(f'Invalid year code for column "{col}"')
        if self.SS not in scenario_codes:
            raise ValueError(f'Invalid scenario code for column "{col}"')
        if self.T not in data_types:
            raise ValueError(f'Invalid data type for column "{col}"')
        if self.X not in suffixes:
            raise ValueError(f'Invalid suffix for column "{col}"')

    def __str__(self):
        return f'Scenario({indicator_codes[self.ii]}, {year_codes[self.YY]}, {scenario_codes[self.SS]}, {data_types[self.T]}, {suffixes[self.X]})'

    def __repr__(self):
        return f'Scenario({self.ii},{self.YY},{self.SS},{self.T},{self.X})'

    @property
    def raw(self):
        return f'{self.ii}{self.YY}{self.SS}{self.T}{self.X}'
    
    @property
    def label(self):
        ii = '-'.join(indicator_codes[self.ii].split(' '))
        SS = '-'.join(scenario_codes[self.SS].split(' '))
        T = '-'.join(data_types[self.T].split(' '))
        return f'{ii}_{SS}_{T}'




# Ignore warnings around version mismatch that doesn't affect results
import warnings
warnings.filterwarnings("ignore", category=UserWarning)

def convert_shape(shape_file, cell_size=0.1):
    """adapted from https://github.com/jataware/convert-shp-to-csv/blob/main/convert_shp_to_csv/main.py"""

    # Convert a shape file to a geodataframe
    shp = gpd.read_file(shape_file)

    # Pull list of data columns we want to include in the output
    # Basically, anything that is not the `geometry`
    # TODO: Allow specification of desired columns via cli arguments?
    columns = [col for col in shp.columns if col != "geometry"]

    # Determine geometric values
    cell_width = cell_size
    cell_height = cell_size
    xmin, ymin, xmax, ymax = shp.total_bounds

    # Build list of boxes representing the grid that covers the full shape image rectangle
    grid_cells = []
    for x0 in tqdm(np.arange(xmin, xmax + cell_width, cell_width), desc='Building grid'):
        for y0 in np.arange(ymin, ymax + cell_height, cell_height):
            x1 = x0 - cell_width
            y1 = y0 + cell_height
            new_cell = shapely.geometry.box(x0, y0, x1, y1)
            grid_cells.append(new_cell)

    # Create a GeoDataFrame based on the grid cells, setting a value that represents the center of the cell
    gridded = gpd.GeoDataFrame(geometry=grid_cells)
    gridded["centroid"] = gridded.geometry.apply(lambda x: x.centroid)

    gridded.set_crs(shp.crs, inplace=True)

    # Join the shape file with the grid by overlaying the shape over the grid and then removing cells whose centers
    # are outside the defined shape(s).
    print('Joining shape file with grid...', end='', flush=True)
    gdf = gpd.sjoin(
        gridded,
        shp,
    )
    print('done')

    rows = []
    for row in tqdm(gdf.iterrows(), total=len(gdf), desc='Converting grid to lat/lon'):
        rows.append({
            "latitude": row[1].centroid.y,
            "longitude": row[1].centroid.x,
            **{col: row[1][col] for col in columns},
        })

    # Convert the list of rows to a dataframe
    df = pd.DataFrame(rows)

    return df




def main(shape_path, out_path):
    
    #convert the shapefile to a gridded dataframe
    df = convert_shape(shape_path, cell_size=1.0)
    
    columns_to_keep = {
        'latitude':'latitude', 
        'longitude':'longitude',
        'BasinID':'id',
        'dwnBasinID': 'subid'
    }
    
    #collect columns that will be part of the output
    cols = []
    for col in df.columns:
        try:
            s = Scenario(col)
        except ValueError:
            continue

        #extra filters for the actual columns we want
        if s.X == 'l': #don't want labels, only values
            continue
        if s.T == 'u': #don't want uncertainty values
            continue
        
        cols.append(s)

    ####### Only if you want to split m/y values evenly by the number of grid cells per basin #######
    # #count number of times each basin id occurs
    # basin_counts = dict(df['BasinID'].value_counts())
    # to_norm = [col for col in cols if col.ii == 'ut' or col.ii == 'bt'] #only normalize baseline values
    # new_rows = []
    # for i, row in tqdm(df.iterrows(), total=len(df), desc='Normalizing volume values'):
    #     row.update({col.raw: row[col.raw] / basin_counts[row['BasinID']] for col in to_norm})
    #     new_rows.append(row)

    # #convert the rows to a dataframe
    # df = pd.DataFrame(new_rows)
    

    #generate the new output dataframe
    rows = []
    for row in tqdm(df.iterrows(), total=len(df), desc='Processing rows'):
        #keep specified values that are constant across years, e.g. id, lat, lon, etc.
        to_keep = {columns_to_keep[k]: v for k, v in row[1].items() if k in columns_to_keep}
        
        #create a row for each year
        for year in year_codes:
            scenarios = [s for s in cols if s.YY == year]
            out_row = {**to_keep, "year":year_codes[year]}
            for s in scenarios:
                out_row[s.label] = row[1][s.raw]
            rows.append(out_row)
    out = pd.DataFrame(rows)

    #save to csv
    out.to_csv(out_path,index=False)







if __name__ == '__main__':
    # data_path = 'Y2019M07D12_Aqueduct30_V01/future_projections/annual/csv/aqueduct_projections_20150309.csv'
    shape_path = 'Y2019M07D12_Aqueduct30_V01/future_projections/annual/shapefile/aqueduct_projections_20150309.shp'
    out_path = 'out.csv'
    main(shape_path, out_path)
