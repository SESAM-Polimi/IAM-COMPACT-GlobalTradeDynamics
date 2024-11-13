#%%
import mario
import yaml
import os
import numpy as np
import pandas as pd
from copy import deepcopy as dc


#%% Read import shares for commodities
def get_import_shares(
        path:str,
):

    import_shares = pd.read_excel(
        path,
        sheet_name=None,
        index_col=None
    )
    
    del import_shares['Reference']

    regions_clusters = {}
    for cluster in import_shares['Regions Clusters'].columns:
        regions_clusters[cluster] = import_shares['Regions Clusters'][cluster].dropna().values

    commodities_clusters = {}
    for cluster in import_shares['Commodities Clusters'].columns:
        commodities_clusters[cluster] = import_shares['Commodities Clusters'][cluster].dropna().values

    del import_shares['Regions Clusters']
    del import_shares['Commodities Clusters']

    for k,v in import_shares.items():
        v.columns = ['Scenario','Region from']+list(v.columns[2:])
        v = v.set_index(['Scenario','Region from'])
        v.columns.names = ['Region to']
        v = v.stack().to_frame()
        v.columns = ['Value']
        v.reset_index(inplace=True)
        import_shares[k] = v

#%% Implement new import shares  
scenarios = ['baseline']

for scenario in scenarios:
    if scenario == 'baseline':
        u = db.u
        z = db.z

    for commodity,data in import_shares.items():            # for each commodity for which import shares are defined
        if commodity not in commodities_clusters:  
            com_list = [commodity]                          # if the commodity is not part of a cluster of commodities, generate a list with the commodity itself 
        else:
            com_list = commodities_clusters[commodity]      # else, use the list of commodities in the cluster

        for com in com_list:                                # for each commodity in the list
            for region_to in data['Region to'].unique():    # for each region to which the commodity is imported

                if region_to not in regions_clusters:       
                    reg_list = [region_to]                  # same as above, but for regions
                    reg_cluster = dc(region_to)
                else:
                    reg_list = regions_clusters[region_to]
                    reg_cluster = dc(region_to)
                
                for reg_to in reg_list:                     # for each importing region in the list
                    com_use = u.loc[(slice(None),'Commodity',com),(reg_to,'Activity',slice(None))].sum(0)  # calculate the use of the commodity in the importing region
                    cons_mix_columns = com_use.index
                    com_use = com_use.to_frame().T.values
                    imp_share = data.query(f"Scenario==@scenario & `Region to`==@reg_cluster")
                    
                    regions_from = list(imp_share['Region from'])
                    imp_share = imp_share.Value.to_frame().values  # get the import share of commodity com towards region reg_to from data

                    consumption_mixes = imp_share @ com_use # calculate the consumption mix of the commodity in the importing region    
                    consumption_mixes = pd.DataFrame(       # create a dataframe with the consumption mixes of the commodity by regions_from
                        consumption_mixes, 
                        index = pd.MultiIndex.from_arrays([
                            regions_from,
                            ['Commodity']*len(regions_from),
                            [com]*len(regions_from),
                        ]), 
                        columns = cons_mix_columns) 

                    u.update(consumption_mixes) # update the use matrix with the new consumption mixes
        
    z.update(u) # update the z matrix with the new use matrix

    db.update_scenarios('baseline',z=z)
    db.reset_to_coefficients('baseline')


#%% Calculate GHG footprints
# provide a dictionary with GHGs and their GWP
ghgs = {
    'Carbon dioxide, fossil (air - Emiss)':1,
    'CH4 (air - Emiss)':29.8,
    'N2O (air - Emiss)':273
    }

# calculate f using "blocks-calculations"
# w_pp = (I - u s)^-1
# f = e s w_pp

f = db.e.loc[:,(slice(None),'Activity',slice(None))].values @ db.s.values @ np.linalg.inv((np.eye(db.u.shape[0]) - db.u.values @ db.s.values))
f = pd.DataFrame(f, index = db.e.index, columns = db.s.columns)

# isolate GHGs for new commodities (batteries) in f matrix (specific footprints)
f = f.loc[ghgs.keys(),(slice(None),'Commodity',db.new_commodities)]
for ghg,gwp in ghgs.items():
    f.loc[ghg,:] *= gwp # multiply each GHG by its GWP

# rearrange the shape of the resulting dataframe
f = f.sum(0) 
f = f.to_frame()    
f.reset_index(inplace=True)
f.columns = ['Region','Item','Commodity','Value']
f = f.drop('Item',axis=1)
f.set_index(['Region','Commodity'],inplace=True)
f = f.unstack()
f = f.droplevel(0,axis=1)
f = f*1000/80 # convert to kg CO2-eq/kWh
f.loc[:,'Electric vehicles'] = f.loc[:,'Electric vehicles']*80/1000

f.to_excel('footprints.xlsx')

#%% Calculate prices

# calculate p using "blocks-calculations"
# w_pp = (I - u s)^-1
# p = v s w_pp

p = db.v.loc[:,(slice(None),'Activity',slice(None))].values @ db.s.values @ np.linalg.inv((np.eye(db.u.shape[0]) - db.u.values @ db.s.values))
p = pd.DataFrame(p, index = db.v.index, columns = db.s.columns)
p = p.sum(0)
p = p.to_frame()
p.columns = ['price']

# isolate prices for new commodities (batteries) in f matrix
p = p.loc[(slice(None),'Commodity',db.new_commodities),:]
p = p.unstack(-1)
p = p.droplevel(1,axis=0)
p = p.droplevel(0,axis=1)

p = p*1e6/80 # convert to €/kWh
p = p/0.92 # convert from EUR to USD in 2011
p = p*1.33 # deflat from 2011 to 2024

p.loc[:,'Electric vehicles'] = p.loc[:,'Electric vehicles']*80/1e6*0.92
p.to_excel('prices.xlsx')


#%% Broken down prices
v = db.v.sum(0)
v = v.to_frame()
v = v.T

p_ex = np.diagflat(v.values) @ db.w.values
p_ex = pd.DataFrame(p_ex, index = db.w.index, columns = db.w.columns)

#%%
EU_countries = ['AT','BE','BG','CY','CZ','DE','DK','EE','ES','FI','FR','GR','HR','HU','IE','IT','LT','LU','LV','MT','NL','PL','PT','RO','SE','SI','SK']

p_ex_filtered = p_ex.loc[(slice(None),'Activity',slice(None)),(EU_countries,'Commodity',['Electric vehicles'])]
p_ex_filtered *= 1.33
p_ex_filtered.to_clipboard()

#%%
v_filtered = db.v.loc[:,(slice(None),'Activity','Mining of nickel ores and concentrates')].T
v_filtered.to_clipboard()

# %% Export aggregated database to txt
db.to_txt(
    os.path.join(folder,paths['database']['exiobase']['extended']),
    flows=False,
    coefficients=True
    )
# %%
