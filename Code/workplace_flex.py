    
import pandas as pd
import numpy as np
from Code.helper import get_naics_descr, get_naics_12_17


"""
 GETTING 3 OR 4 DIGIT NAICS WORK-FROM-HOME USING ATUS
 
 See guides at 
 https://www.bls.gov/tus/datafiles_2017.htm 
 https://www.bls.gov/tus/datafiles_2018.htm
 https://www.bls.gov/tus/lvdatafiles.htm
 Unique id to linke databases is tucaseid
 
 The 2017 and 2018 atus respondent files contain the relevant demographic information,
 such as respondent occupation and industry code 
 (relevant variables are "teio1icd" for industry code (NAICS-4) and "teio1ocd")
 
 The 2017/2018 atus leave module contains the relevant workplace flexibility variables:
 Define a worker as able to work from home if they answer yes to both of the following questions
 
 "As part of your (main) job, can you work at home?" (variable in lvresp_1718_final is "lujf_10")
 "Are there days when you work only at home?" (variable in lvresp_1718_final is "lejf_14")
 
 These responses can then be aggregated to the industry level using the correct weights
 
 The correct weights, according to https://www.atusdata.org/atus-action/faq is the
 variable "lvwt". See description below from page
 
 The Leave Module is a special supplement to the ATUS that collects data from ATUS respondents about
 access to paid leave and whether the respondent took paid leave during the past seven days. 
 The module was supported by the U.S. Department of Labor Women's Bureau and was fielded in 2011. 
 Because only respondents who were employed wage and salary workers were eligible for the module and 
 not everyone who was eligible participated in the module, users of data collected through the Leave 
 Module should use the weighting variable LVWT to obtain estimates rather than the standard weighting 
 variable WT06.
 
 So when aggregating ATUS data to IND level, we weight using lvwt. 
 
 IND-NAICS4 (or NAICS3) is not unique, but Evan Solta's crosswalk (naics_to_ind) provides 
 allocation factors of IND to each NAICS4 (or NAICS3), we use the following method
 
 1. Define an ATUS leave respondent as "workfromhome" if they answered yes to
 	"As part of your (main) job, can you work at home?" (variable in lvresp_1718_final is "lujf_10")
 	"Are there days when you work only at home?" (variable in lvresp_1718_final is "lejf_14")
 2. Using the appropriate weight, lvwt, aggregate workfromhome up to the IND level
 3. Join ind to naics4 (or naics3) using the Soltas crosswalk. This will give
 	some NAICS codes with multiple IND matches. Aggregate workfromhome to the NAICS4 level
 	using Solta's afactor as the weight. 
 4. This gives a unique naics workfromhome variable
 5. Update 2012 NAICS to 2017 NAICS
 """
 
def build_workplace_flex(level = 4, weight = 'lvwt'):
    """
    Parameters
    ----------
    weight : TYPE, str
        DESCRIPTION. Specifies which weight to use for weighting in atus leave module 
        The default is 'lvwt'.

    Returns
    -------
    wfh_out : df
        DESCRIPTION. naics4 level workfromhome using ATUS

    """


    ## Final ind var
    ind_var = 'naics{}'.format(level)
    title_var = 'naics{}_title'.format(level)

    ## Get the necessary data
    # Leave module / atus data
    lvresp_1718 = pd.read_csv('data/workplace_flex/lvresp_1718_final.csv')
    atusresp_2017 = pd.read_csv('data/workplace_flex/atusresp_2017_final.csv')
    atusresp_2018 = pd.read_csv('data/workplace_flex/atusresp_2018_final.csv')
    atusresp = pd.concat([atusresp_2017,atusresp_2018],axis=0)
    atusweights = pd.read_csv('data/nondownloadable/atus_00002.csv')
    atusweights.columns = [x.lower() for x in atusweights]
    # naics data
    naics_descr = get_naics_descr(level)
    naics_12_17 = get_naics_12_17(level)
    
    # ind/naics conversion data
    naics_to_ind  = pd.read_csv('data/ind_data/naics_to_ind.tab',sep = "\t")    
    naics_to_ind = naics_to_ind.loc[naics_to_ind['naics_digit'] == level]

    ## Merge together atus datasets
    lvresp = lvresp_1718.merge(atusresp,on = ['tucaseid'], how = 'left')
    
    lvresp['caseid'] = lvresp['tucaseid'].astype(float)
    
    lvresp = lvresp.merge(atusweights,on = ['caseid'], how = 'left')
    
    lvresp = lvresp.loc[lvresp['lujf_10']!=-2]
    
    ## Define workfromhome variable
    lvresp['workfromhome'] = ((lvresp['lujf_10'] == 1) & (lvresp['lejf_14'] == 1))**1
    
    ## Aggregate to industry level (using weight variable as weight)
    lvresp_agg = lvresp.groupby(['teio1icd']).apply(
                 lambda x: np.average(x['workfromhome'],weights=x[weight])
                 ).to_frame().rename(columns = {0:'workfromhome'}).reset_index()
    
    
    
    lvresp_agg = lvresp_agg.merge(naics_to_ind,left_on = ['teio1icd'],right_on = ['ind'],how = 'left')
    
    wfh_out = lvresp_agg.groupby(['naics']).apply(
                 lambda x: np.average(x['workfromhome'],weights=x['afactor'])
                 ).to_frame().rename(columns = {0:'workfromhome'}).reset_index()
    
    wfh_out = wfh_out.rename(columns = {'naics':'naics2012'})
    
    
    wfh_out = wfh_out.merge(naics_12_17,on = 'naics2012',how = 'left')
    wfh_out = wfh_out.rename(columns = {'naics2017':ind_var}).drop('naics2012',1)
    
    ## Duplicates: take average across naics industries
    wfh_out = wfh_out.groupby([ind_var])[['workfromhome']].mean().reset_index()

    
    wfh_out = wfh_out.drop_duplicates(subset = [ind_var])
    
    ## Merge on the descriptions
    wfh_out = wfh_out.merge(naics_descr,on = ind_var)
    
    wfh_out = wfh_out.reindex([ind_var,title_var,'workfromhome'],axis=1)
    
    return wfh_out

