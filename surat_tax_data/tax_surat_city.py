import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import io
import os
import xlrd
import pandas as pd
# Importing pyplot from matplotlib for the graph visualization
# Importing geopandas library for creating GeoDataframe from Geographical coordinates


# Declaring a class for data cleaning and self initialization of member variables
class xl_op:
    def __init__(self, name, period):
        self.city = name
        self.time = period
        self.num = 23465

    def clean_data(df):
        # print(df.count())
        for col in df:
            # print(col, type(col))
            df[col] = df[col].fillna('')
        return df

    def filter_row(df, col_nm, col_val):
        new_df = 'df_{}={}'.format(col_nm, col_val)
        # print(new_df)
        new_df = df[df[col_nm] == col_val]
        new_df = new_df.groupby(['Ward', 'FinYear'])
        return new_df

    def col_distinct(df, col_name):
        print(df[col_name].unique())
        # pass


start_script = xl_op('surat', '2002-2017')
print('Working on {} city tax collection data from ogd(open source) for the period'.format(
    start_script.city, start_script.time))
# '''

try:
    try:
        excel_file_read = xlrd.open_workbook(
            r'C:\gitfolder\pyspark_self_project\surat_tax_data\surat_tax_data_2017.xls', ignore_workbook_corruption=True)
        df_source = pd.read_excel(excel_file_read)
        cleaned_df = xl_op.clean_data(df_source)
        cleaned_df = cleaned_df.drop(['CityName', 'Ward'], axis=1)
    except Exception:
        raise Exception
except FileNotFoundError:
    raise Exception
# '''

surat_map = gpd.read_file(
    r'C:\gitfolder\pyspark_self_project\surat_tax_data\Surat - Zone Boundary.shp', crs=4326)
# '''
surat_map = surat_map.drop(
    ['FID', 'Area', 'City', 'State', 'District'], axis=1)
surat_map['No'] = surat_map['No'].astype(int)

surat_map = xl_op.clean_data(surat_map)
print('Printing Surat Map on chart')
print(surat_map)
# surat_map.plot()
# print(type(surat_map))
# '''

print('Map shp file df columns')
print(surat_map.columns)
print('Map shp file df columns')
print(cleaned_df.columns)
print('Map Shp file Data')
print(surat_map.head(5))
print('Surat Tax df data')
print(cleaned_df.head(5))

final_df = cleaned_df.merge(
    surat_map, left_on='zoneserial', right_on='No', how='left')
final_df = final_df.drop(
    ['Name_1', 'No', 'zoneserial', 'FinYear', 'Demand'], axis=1)
print('final_df columns before groupby')
print(final_df.columns)

df_non_geo = final_df.drop('geometry', axis=1)
df_non_geo = df_non_geo.groupby('Zone').agg(
    {'TotalRecovery': 'sum'}).reset_index()
print('Non_Geo', df_non_geo.head(2))

df_geo = final_df.drop(['TotalRecovery'], axis=1)
print('df_Geo', df_geo.head(2))

final_df = df_non_geo.merge(df_geo, on='Zone', how='left')

final_df = final_df.drop_duplicates().reset_index()
final_df = final_df[['Zone', 'TotalRecovery', 'geometry']]
# final_df = final_df.set_index('Zone')

print('Final Dataframe to plot on map')
print(final_df.head(10))

final_df = gpd.GeoDataFrame(final_df, geometry=final_df['geometry'])

print(type(final_df))

fig, ax = plt.subplots(1, figsize=(12, 12))
ax.axis('off')
ax.set_title('Surat city tax collection ZoneWise for the period 2002-2017',
             fontdict={'fontsize': '10', 'fontweight': '3'})
# point_list = list(final_df['geometry'].exterior.coords)
# print('point_list', point_list)
for city_index, city in final_df.iterrows():
    print(city.geometry.centroid.x)
    plt.text(city.geometry.centroid.x,
             city.geometry.centroid.y, city["Zone"])

fig = final_df.plot(column='TotalRecovery', cmap='Accent', linewidth=0.5,
                    ax=ax, edgecolor='black', legend=True)
fig.plot()

plt.show()
