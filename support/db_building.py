#%%
import mario
import yaml
import os 
from ember_remapping import map_ember_to_classification

user = 'LRinaldi'

with open('paths.yml', 'r') as file:
    paths = yaml.safe_load(file)

folder = paths['onedrive_folder'][user]

#%% Parse raw Exiobase database
mode = 'flows'
db = mario.parse_from_txt(
    path = os.path.join(folder,paths['database']['exiobase']['raw'],'flows'),
    mode=mode,
    table='SUT'
    )

# %% Get excel to aggregate database (Comment if already done)
# db.get_aggregation_excel('aggregations/raw_to_aggregated.xlsx')

#%% Aggregate database  
db.aggregate(
    io = 'aggregations/raw_to_aggregated.xlsx',
    ignore_nan=True
    )

#%% Parse ember electricity generation data, map to exiobase and get electricity mix for a given year 
ee_mix = map_ember_to_classification(
    path = os.path.join(folder,paths['database']['ember']),
    classification = 'EXIO3',
    year = 2023,
    mode = 'mix',
)

#%% Implement changes in matrices
z = db.z
s = db.s

for region in db.get_index('Region'):
    print(region,end=' ')
    new_mix = ee_mix.loc[(region,slice(None),slice(None)),'Value'].to_frame().sort_index(axis=0) 
    new_mix.index = new_mix.index.get_level_values(2)
    s.loc[(region, 'Activity', new_mix.index),(region,'Commodity','Electricity')] = new_mix.values  # check if commodity electricity is called "Electricity" in aggregation excel file
    s.loc[:,(region,'Commodity','Electricity')] /= s.loc[:,(region,'Commodity','Electricity')].sum()
    
    print('done')

z.update(s)

# Fine-tune v coefficients
v = db.v
EU_countries = ['AT','BE','BG','CY','CZ','DE','DK','EE','ES','FI','FR','GR','HR','HU','IE','IT','LT','LU','LV','MT','NL','PL','PT','RO','SE','SI','SK']

# Mining on nickel ores and concentrates
v.loc[
    "Compensation of employees; wages, salaries, & employers' social contributions: High-skilled",
    (['CN','ID'],'Activity','Mining of nickel ores and concentrates')
    ] = 0.00180521909901435 # Global average calculated in Excel
v.loc[
    "Compensation of employees; wages, salaries, & employers' social contributions: Low-skilled",
    (['CN','ID'],'Activity','Mining of nickel ores and concentrates')
    ] = 0.000784460157683494 # Global average calculated in Excel
v.loc[
    "Compensation of employees; wages, salaries, & employers' social contributions: Medium-skilled",
    (['CN','ID'],'Activity','Mining of nickel ores and concentrates')
    ] = 0.00529838408190141 # Global average calculated in Excel
v.loc[
    "Operating surplus: Consumption of fixed capital",
    (['CN','ID'],'Activity','Mining of nickel ores and concentrates')
    ] = 0.00551914726087621 # Global average calculated in Excel
v.loc[
    "Operating surplus: Remaining net operating surplus",
    (['CN','ID'],'Activity','Mining of nickel ores and concentrates')
    ] = 0 # 0.00799330366538704 # Global average calculated in Excel
v.loc[
    "Other net taxes on production",
    (['CN','ID'],'Activity','Mining of nickel ores and concentrates')
    ] = -0.00581767462615802 # Global average calculated in Excel
v.loc[
    "Taxes less subsidies on products purchased: Total",
    (['CN','ID'],'Activity','Mining of nickel ores and concentrates')
    ] = 0.00101621109318206 # Global average calculated in Excel

# Mining on copper ores and concentrates
v.loc[
    'Operating surplus: Remaining net operating surplus',
    (['CN'],'Activity','Mining of copper ores and concentrates')
    ] = 0 # 0.007380648  # Global average calculated in Excel
v.loc[
    "Compensation of employees; wages, salaries, & employers' social contributions: High-skilled",
    (EU_countries,'Activity','Mining of copper ores and concentrates')
    ] = 0.001527385  # Global average calculated in Excel
v.loc[
    "Compensation of employees; wages, salaries, & employers' social contributions: Low-skilled",
    (EU_countries,'Activity','Mining of copper ores and concentrates')
    ] = 0.000749957  # Global average calculated in Excel
v.loc[
    "Compensation of employees; wages, salaries, & employers' social contributions: Medium-skilled",
    (EU_countries,'Activity','Mining of copper ores and concentrates')
    ] = 0.005101836  # Global average calculated in Excel
v.loc[
    "Operating surplus: Consumption of fixed capital",
    (EU_countries,'Activity','Mining of copper ores and concentrates')
    ] = 0.005833217  # Global average calculated in Excel
v.loc[
    "Operating surplus: Remaining net operating surplus",
    (EU_countries,'Activity','Mining of copper ores and concentrates')
    ] = 0.007380648  # Global average calculated in Excel
v.loc[
    "Other net taxes on production",
    (EU_countries,'Activity','Mining of copper ores and concentrates')
    ] = 0.000967752  # Global average calculated in Excel
v.loc[
    "Taxes less subsidies on products purchased: Total",
    (EU_countries,'Activity','Mining of copper ores and concentrates')
    ] = 0.000919065  # Global average calculated in Excel


db.update_scenarios('baseline',v=v,z=z)
db.reset_to_coefficients('baseline')

# %% Export aggregated database to txt
db.to_txt(os.path.join(folder,paths['database']['exiobase']['aggregated'])) 

# %%
