"""
Created on Wed Jan 19 15:00:12 2022

jwb
"""



import pandas as pd
import copy
from Code.helper import get_naics_descr, get_naics_12_17
pd.options.mode.chained_assignment = None  # default='warn'

#%%
 


def build_investment_flex():
    
    try:
        flex_df_in = pd.read_csv("../investment_flex_raw.csv")
    except:
        # print("Invesmtnent flex data not available publicly")
        return
    
    naics_titles = get_naics_descr()        
    naics_titles['naics4'] =  naics_titles['naics4'].astype(int)
    naics_titles['naics3'] = naics_titles['naics4'].astype(str).str[0:3].astype(int)
    naics_titles['naics2'] = naics_titles['naics4'].astype(str).str[0:2].astype(int)
    naics_titles.loc[(naics_titles['naics2'] == 32) | (naics_titles['naics2'] == 33),'naics2'] = 31
    naics_titles.loc[(naics_titles['naics2'] == 41) | (naics_titles['naics2'] == 42),'naics2'] = 42
    naics_titles.loc[(naics_titles['naics2'] == 44) | (naics_titles['naics2'] == 45),'naics2'] = 44
    naics_titles.loc[(naics_titles['naics2'] == 48) | (naics_titles['naics2'] == 49),'naics2'] = 48
    naics_titles.loc[(naics_titles['naics2'] == 91) | (naics_titles['naics2'] == 92),'naics2'] = 91
    

    naics_titles = naics_titles.reindex(['naics4','naics3','naics2','naics4_title'],axis=1)

    flex_df_out = copy.deepcopy(naics_titles)

    for industry in ['naics4','naics3','naics2']:
        flex_df = flex_df_in[['q16b_speed_flex','q16b_startdate_flex',industry]]
            
        flex_df.loc[(flex_df['q16b_speed_flex']<=2) & (~pd.isnull(flex_df['q16b_speed_flex'])),'flex_speed'] = 1
        flex_df.loc[(flex_df['q16b_speed_flex']> 2) & (~pd.isnull(flex_df['q16b_speed_flex'])),'flex_speed'] = 0
    
        flex_df.loc[(flex_df['q16b_startdate_flex']<=2) & (~pd.isnull(flex_df['q16b_startdate_flex'])),'flex_start'] = 1
        flex_df.loc[(flex_df['q16b_startdate_flex']> 2) & (~pd.isnull(flex_df['q16b_startdate_flex'])),'flex_start'] = 0
    
        
        flex_df = flex_df.dropna(subset = ['flex_speed','flex_start'],how = 'all')
        
        inv_flex = 	flex_df.groupby([industry])[['flex_speed','flex_start']].mean().reset_index().dropna(subset = [industry])
        inv_flex = inv_flex.rename(columns = {x:x+"_"+industry for x in inv_flex if 'flex' in x})
        
        
        flex_df_out = flex_df_out.merge(inv_flex,on = industry,how = 'left')
        
    for var in ['flex_speed','flex_start']:       
        flex_df_out[var] = flex_df_out[var+'_naics4'].fillna(
                           flex_df_out[var+'_naics3']).fillna(
                           flex_df_out[var+'_naics2'])
        
    
    flex_df_out = flex_df_out.dropna(subset = ['flex_speed'],how = 'any')
    
    flex_df_out = flex_df_out.reindex(['naics4','naics4_title','flex_speed','flex_start'],axis=1)
    # flex_df_out['naics2'] = flex_df_out['naics2'].replace({31:"31-33",44:"44-45",48:"48-49"}).astype(str)

    return flex_df_out
    