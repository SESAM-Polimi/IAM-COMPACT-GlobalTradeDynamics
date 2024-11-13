#%%
import mario
from mario.tools.constants import _MASTER_INDEX as MI
import pandas as pd
from copy import deepcopy as dc

EU_countries = ['AT','BE','BG','CY','CZ','DE','DK','EE','ES','FI','FR','GR','HR','HU','IE','IT','LT','LU','LV','MT','NL','PL','PT','RO','SE','SI','SK']

#%%
def electricity_mixes(
        db: mario.Database,
        ee_mix: pd.DataFrame,
        electricity_commodity:str = 'Electricity',
):

    z = db.z
    s = db.s

    for region in db.get_index(MI['r']):
        print(region,end=', ')

        new_mix = ee_mix.loc[(region,slice(None),slice(None)),'Value'].to_frame().sort_index(axis=0) 
        new_mix.index = new_mix.index.get_level_values(2)
        s.loc[(region, MI['a'], new_mix.index),(region,MI['c'],electricity_commodity)] = new_mix.values  # check if commodity electricity is called "Electricity" in aggregation excel file
        s.loc[:,(region,MI['c'],electricity_commodity)] /= s.loc[:,(region,MI['c'],electricity_commodity)].sum()
        
    print('done')

    z.update(s)
    db.update_scenarios('baseline',z=z)
    db.reset_to_coefficients('baseline')

    return db

#%%
def value_added_coefficients(
        db: mario.Database,
):

    v = db.v

    # Mining on nickel ores and concentrates
    v.loc[
        "Compensation of employees; wages, salaries, & employers' social contributions: High-skilled",
        (['CN','ID'],MI['a'],'Mining of nickel ores and concentrates')
        ] = 0.00180521909901435 # Global average calculated in Excel
    v.loc[
        "Compensation of employees; wages, salaries, & employers' social contributions: Low-skilled",
        (['CN','ID'],MI['a'],'Mining of nickel ores and concentrates')
        ] = 0.000784460157683494 # Global average calculated in Excel
    v.loc[
        "Compensation of employees; wages, salaries, & employers' social contributions: Medium-skilled",
        (['CN','ID'],MI['a'],'Mining of nickel ores and concentrates')
        ] = 0.00529838408190141 # Global average calculated in Excel
    v.loc[
        "Operating surplus: Consumption of fixed capital",
        (['CN','ID'],MI['a'],'Mining of nickel ores and concentrates')
        ] = 0.00551914726087621 # Global average calculated in Excel
    v.loc[
        "Operating surplus: Remaining net operating surplus",
        (['CN','ID'],MI['a'],'Mining of nickel ores and concentrates')
        ] = 0 # 0.00799330366538704 # Global average calculated in Excel
    v.loc[
        "Other net taxes on production",
        (['CN','ID'],MI['a'],'Mining of nickel ores and concentrates')
        ] = -0.00581767462615802 # Global average calculated in Excel
    v.loc[
        "Taxes less subsidies on products purchased: Total",
        (['CN','ID'],MI['a'],'Mining of nickel ores and concentrates')
        ] = 0.00101621109318206 # Global average calculated in Excel

    # Mining on copper ores and concentrates
    v.loc[
        'Operating surplus: Remaining net operating surplus',
        (['CN'],MI['a'],'Mining of copper ores and concentrates')
        ] = 0 # 0.007380648  # Global average calculated in Excel
    v.loc[
        "Compensation of employees; wages, salaries, & employers' social contributions: High-skilled",
        (EU_countries,MI['a'],'Mining of copper ores and concentrates')
        ] = 0.001527385  # Global average calculated in Excel
    v.loc[
        "Compensation of employees; wages, salaries, & employers' social contributions: Low-skilled",
        (EU_countries,MI['a'],'Mining of copper ores and concentrates')
        ] = 0.000749957  # Global average calculated in Excel
    v.loc[
        "Compensation of employees; wages, salaries, & employers' social contributions: Medium-skilled",
        (EU_countries,MI['a'],'Mining of copper ores and concentrates')
        ] = 0.005101836  # Global average calculated in Excel
    v.loc[
        "Operating surplus: Consumption of fixed capital",
        (EU_countries,MI['a'],'Mining of copper ores and concentrates')
        ] = 0.005833217  # Global average calculated in Excel
    v.loc[
        "Operating surplus: Remaining net operating surplus",
        (EU_countries,MI['a'],'Mining of copper ores and concentrates')
        ] = 0.007380648  # Global average calculated in Excel
    v.loc[
        "Other net taxes on production",
        (EU_countries,MI['a'],'Mining of copper ores and concentrates')
        ] = 0.000967752  # Global average calculated in Excel
    v.loc[
        "Taxes less subsidies on products purchased: Total",
        (EU_countries,MI['a'],'Mining of copper ores and concentrates')
        ] = 0.000919065  # Global average calculated in Excel

    db.update_scenarios('baseline',v=v)
    db.reset_to_coefficients('baseline')

    return db

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
    
    return import_shares, regions_clusters, commodities_clusters


#%% Implement new import shares  
def import_coefficients(
        db: mario.Database,
        import_shares: dict,
        commodities_clusters: dict,
        regions_clusters: dict,
        scenario: str = 'baseline',
):

    u = db.query('u',[scenario])
    z = db.query('z',[scenario])

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
                    com_use = u.loc[(slice(None),MI['c'],com),(reg_to,MI['a'],slice(None))].sum(0)  # calculate the use of the commodity in the importing region
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
                            [MI['c']]*len(regions_from),
                            [com]*len(regions_from),
                        ]), 
                        columns = cons_mix_columns) 

                    u.update(consumption_mixes) # update the use matrix with the new consumption mixes
            
    z.update(u) # update the z matrix with the new use matrix

    db.update_scenarios(scenario,z=z)
    db.reset_to_coefficients(scenario)

    return db
