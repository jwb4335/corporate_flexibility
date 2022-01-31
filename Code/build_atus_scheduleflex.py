

 
def get_naics_descr(level = 4):
    """
    Parameters
    ----------
    level : TYPE, int
        DESCRIPTION. level of industry to get description for The default is 4.

    Returns
    -------
    naics : df
        DESCRIPTION. naics codes and descriptions

    """
    import pandas as pd
    
    ind_var = 'naics{}'.format(level)
    title_var = 'naics{}_title'.format(level)
    
    naics = pd.read_excel('input_data/naics_desc/2017_naics_structure.xlsx',skiprows = [0],header=1)
    naics = naics.rename(columns = {'2017 NAICS Code':ind_var,
                                                                '2017 NAICS Title':title_var})
    
    naics[ind_var] = naics[ind_var].replace({"31-33":"31","44-45":"44","48-49":"48","41-42":"41"})
    
    naics.columns = [x.lower() for x in naics.columns]
        
    naics = naics[[ind_var,title_var]]
    
    naics[title_var] = naics[title_var].str.strip() 
    
    naics = naics.loc[naics[ind_var].astype(str).str.len() == level].\
                        loc[~pd.isnull(naics[ind_var])][[ind_var,title_var]]
    naics[title_var] = naics[title_var].str.rstrip('T')
    naics[ind_var] =  naics[ind_var].astype(int)
    naics = naics.reindex([ind_var,title_var],axis=1)
    
    return naics


 
def get_naics_12_17(level=4):
    """
    Parameters
    ----------
    level : TYPE, int
        DESCRIPTION. level of industry to pull the conversion for The default is 4.

    Returns
    -------
    naics_12_17 : df
        DESCRIPTION. naics 2012 and naics 2017 matches (unique)

    """

    import pandas as pd

    naics_12_17 = pd.read_excel(
        "input_data/wfh_atus_final/2012_to_2017_NAICS.xlsx",usecols = "A:D",skiprows = [0,1])
    
    naics_12_17.columns = ['naics2012','naics2012_title','naics2017','naics2017_title']
    
    for var in ['naics2012','naics2017']:
        naics_12_17[var] = naics_12_17[var].astype(str).str[0:level].astype(int)
    
    naics_12_17 = naics_12_17.drop_duplicates(subset = ['naics2012'])[['naics2012','naics2017']]
  
    return naics_12_17


def atus_schedule_flex(level = 4, weight = 'lvwt'):
    """
    Parameters
    ----------
    weight : TYPE, str
        DESCRIPTION. Specifies which weight to use for weighting in atus leave module 
        The default is 'lvwt'.

    Returns
    -------
    schedule_out : df
        DESCRIPTION. naics level schedule flexibility using ATUS

    """
    
    import os
    import pandas as pd
    import numpy as np
    
    if "barry" in os.getcwd():
        os.chdir("C:\\Users\\barry\\Dropbox\\graham_covid")
    
    elif "PHD-40" in os.environ['COMPUTERNAME']:
        os.chdir("D:\\Dropbox\\graham_covid")

    ## Final ind var
    ind_var = 'naics{}'.format(level)
    title_var = 'naics{}_title'.format(level)

    ## Get the necessary data
    # Leave module / atus data
    lvresp_1718 = pd.read_stata('input_data/ATUS_leave_module/atusresp_2018/lvresp_1718_final.dta',convert_categoricals=False)
    atusresp_2017 = pd.read_stata('input_data/ATUS_leave_module/atusresp_2017/atusresp_2017_final.dta',convert_categoricals=False)
    atusresp_2018 = pd.read_stata('input_data/ATUS_leave_module/atusresp_2018/atusresp_2018_final.dta',convert_categoricals=False)
    atusresp = pd.concat([atusresp_2017,atusresp_2018],axis=0)
    atusweights = pd.read_stata('input_data/ATUS_leave_module/atuslvwgts_1718/atus_00001.dta',convert_categoricals=False)
    
    # naics data
    naics_descr = get_naics_descr(level)
    naics_12_17 = get_naics_12_17(level)
    
    # ind/naics conversion data
    naics_to_ind  = pd.read_stata('input_data/wfh_atus_final/naics_to_ind.dta',convert_categoricals=False)    
    naics_to_ind = naics_to_ind.loc[naics_to_ind['naics_digit'] == level]

    ## Merge together atus datasets
    lvresp = lvresp_1718.merge(atusresp,on = ['tucaseid'], how = 'left')
    
    lvresp['caseid'] = lvresp['tucaseid'].astype(float)
    
    lvresp = lvresp.merge(atusweights,on = ['caseid'], how = 'left')
    
    lvresp = lvresp.loc[lvresp['lujf_10']!=-2]
    
    ## Define schedule flexibility variable
    lvresp.loc[lvresp['lejf_1'] == 1,'schedule_flex'] = 1
    lvresp.loc[lvresp['lejf_1'] == 2,'schedule_flex'] = 0
 
    
    ## Aggregate to industry level (using weight variable as weight)
    lvresp_agg = lvresp.groupby(['teio1icd']).apply(
                 lambda x: np.average(x['schedule_flex'],weights=x[weight])
                 ).to_frame().rename(columns = {0:'schedule_flex'}).reset_index()
    
    
    
    lvresp_agg = lvresp_agg.merge(naics_to_ind,left_on = ['teio1icd'],right_on = ['ind'],how = 'left')
    
    schedule_out = lvresp_agg.groupby(['naics']).apply(
                 lambda x: np.average(x['schedule_flex'],weights=x['afactor'])
                 ).to_frame().rename(columns = {0:'schedule_flex'}).reset_index()
    
    schedule_out = schedule_out.rename(columns = {'naics':'naics2012'})
    
    schedule_out = schedule_out.append(pd.DataFrame([[1119,0]],columns = ['naics2012','schedule_flex'])).reset_index(drop=True)
    
    schedule_out = schedule_out.merge(naics_12_17,on = 'naics2012',how = 'left')
    schedule_out = schedule_out.rename(columns = {'naics2017':ind_var}).drop('naics2012',1)
    
    ## Duplicates: take average across naics industries
    schedule_out = schedule_out.groupby([ind_var])[['schedule_flex']].mean().reset_index()

    
    schedule_out = schedule_out.drop_duplicates(subset = [ind_var])
    
    ## Merge on the descriptions
    schedule_out = schedule_out.merge(naics_descr,on = ind_var)
    
    schedule_out = schedule_out.reindex([ind_var,title_var,'schedule_flex'],axis=1)
    
    return schedule_out

