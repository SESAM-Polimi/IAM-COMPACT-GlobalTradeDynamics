#%%
import mario
import pandas as pd
import numpy as np
import os


_unit_conversions = {
    'prices': {
        # 'LFP batteries': {'unit': 'USD 2024/kWh', 'factor': 1e6/80*0.92*1.33},        # converted from MEUR/80 kWh to USD 2024/kWh
        # 'NMC batteries': {'unit': 'USD 2024/kWh', 'factor': 1e6/80*0.92*1.33},        # converted from MEUR/80 kWh to USD 2024/kWh
        # 'NCA batteries': {'unit': 'USD 2024/kWh', 'factor': 1e6/80*0.92*1.33},        # converted from MEUR/80 kWh to USD 2024/kWh
        'Batteries': {'unit': 'USD 2024/kWh', 'factor': 1e6/80*0.92*1.33},              # converted from MEUR/80 kWh to USD 2024/kWh
        'Electric vehicles': {'unit': 'USD 2024/vehicle', 'factor': 50000*0.92*1.33},   # converted from MEUR/MEUR to USD 2024/EV, assuming a price of 50'000 EUR/EV

    },
    'gwp': {
        'Carbon dioxide, fossil (air - Emiss)': 1,
        'CH4 (air - Emiss)': 29.8,
        'N2O (air - Emiss)': 273,
    },
    'ghgs': {
        'Batteries': {'unit': 'g CO2eq/kWh', 'factor': 1000/80},                        # converted from ton CO2eq/80 kWh to g CO2eq/kWh
        'Electric vehicles': {'unit': 'ton CO2eq/vehicle', 'factor': 50000/1e6},        # converted from ton CO2eq/MEUR to ton CO2eq 2024/EV, assuming a price of 50'000 EUR/EV
    }
}



#%% Calculate prices
def calc_prices(
        db:mario.Database,
        path:str,
        commodities:list,
):
    
    # calculate p using "blocks-calculations"
    # p = v s w  = v s (I - u s)^-1

    p = db.v.loc[:,(slice(None),'Activity',slice(None))].values @ db.s.values @ np.linalg.inv((np.eye(db.u.shape[0]) - db.u.values @ db.s.values))
    p = pd.DataFrame(p, index = db.v.index, columns = db.s.columns)
    p = p.sum(0)
    p = p.to_frame()
    p.columns = ['price']

    # isolate prices for commodities in p matrix
    p = p.loc[(slice(None),slice(None),commodities),:]
    p = p.unstack(-1)
    p = p.droplevel(1,axis=0)
    p = p.droplevel(0,axis=1)

    units = []
    for commodity, conversion in _unit_conversions['prices'].items():
        p.loc[:,commodity] *= conversion['factor']
        units.append(conversion['unit'])

    p.columns = pd.MultiIndex.from_arrays([
        list(p.columns),
        units,
    ])

    p.to_excel(path)


#%% Calculate GHGs
def calc_ghgs(
        db:mario.Database,
        path:str,
        commodities:list,
):
    
    # calculate f using "blocks-calculations"
    # f = v s w  = e s (I - u s)^-1

    f = db.e.loc[:,(slice(None),'Activity',slice(None))].values @ db.s.values @ np.linalg.inv((np.eye(db.u.shape[0]) - db.u.values @ db.s.values))
    f = pd.DataFrame(f, index = db.e.index, columns = db.s.columns)

    # isolate GHGs for new commodities (batteries) in f matrix (specific footprints)
    f = f.loc[_unit_conversions['gwp'].keys(),(slice(None),'Commodity',commodities)]
    
    for ghg,gwp in _unit_conversions['gwp'].items():
        f.loc[ghg,:] *= gwp # multiply each GHG by its GWP

    # rearrange the shape of the resulting dataframe
    f = f.sum(0) 
    f = f.to_frame()   
    f.columns = ['Carbon footprint']

    f = f.unstack(-1)
    f = f.droplevel(1,axis=0)
    f = f.droplevel(0,axis=1)

    units = []
    for commodity, conversion in _unit_conversions['ghgs'].items():
        f.loc[:,commodity] *= conversion['factor']
        units.append(conversion['unit'])

    f.columns = pd.MultiIndex.from_arrays([
        list(f.columns),
        units,
    ])

    f.to_excel(path)
