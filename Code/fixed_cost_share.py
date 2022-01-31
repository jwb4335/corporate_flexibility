"""
Created on Thu Jan 27 12:10:28 2022

jwb
"""


import pandas as pd
import statsmodels.api as sm 
from Code.helper import get_naics_descr, get_naics_12_17

def build_fixed_cost_share(ind = 'naics4'):
    
    comp_q = pd.read_csv("data/fixed_cost_share/comp_q_fc.csv")
    
    level = int(ind[-1])
    naics_desc = get_naics_descr(level)
    
    fc_ind = _regress_by_ind(comp_q,ind)
    
    fc_ind = fc_ind.merge(naics_desc, on = ind,how = 'left')
    
    fc_ind = fc_ind.reindex([ind,'{}_title'.format(ind),'fc_ind'],axis=1)
    
    if ind == 'naics2':
        fc_ind['naics2'] = fc_ind['naics2'].astype(int).replace({31:"31-33",44:"44-45",48:"48-49"}).astype(str)
        
    return fc_ind


#%%


def _regress(data, yvar, xvars):
    """
    

    Parameters
    ----------
    data : data to run regs on
    yvar : dep. var
    xvars : indep. var(s)

    Returns
    -------
    regression coefficients 
    """
    Y = data[yvar]
    X = data[xvars]
    X['intercept'] = 1.
    result = sm.OLS(Y, X).fit()
    return result.params

def _regress_by_ind(reg_data,ind = 'naics4',yvar = 'xoprq_g',Xvar = ['saleq_g']):
    """
    

    Parameters
    ----------
    reg_data : dataframe to run regs on
    ind : industry level to run regressions at
    
    Returns
    -------
    regression coefficients by industry (dataframe)
    """
    if (ind!="naics4") & (ind!="naics3") & (ind!="naics2"):
        raise Exception("Industry code must be NAICS level")
    ## Make sure we are only focusing on the correct industry level
    reg_data = reg_data.loc[reg_data[ind].astype(int).astype(str).str.len() == int(ind[-1])]

    ## Run _regress at industry level
    reg_results = reg_data.groupby([ind]).apply(_regress, yvar, Xvar)
    reg_results = reg_results.rename(columns = {Xvar[0]:"vc_ind"})
    reg_results['fc_ind'] = 1 - reg_results['vc_ind']
    return reg_results[['fc_ind']].reset_index()

