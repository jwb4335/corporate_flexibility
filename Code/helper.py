import pandas as pd

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
    
    ind_var = 'naics{}'.format(level)
    title_var = 'naics{}_title'.format(level)
    
    naics = pd.read_excel('data/ind_data/2017_naics_structure.xlsx',skiprows = [0],header=1)
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
        "data/ind_data/2017_to_2012_NAICS.xlsx",usecols = "A:D",skiprows = [0,1])
    
    naics_12_17.columns = ['naics2012','naics2012_title','naics2017','naics2017_title']
    
    for var in ['naics2012','naics2017']:
        naics_12_17[var] = naics_12_17[var].astype(str).str[0:level].astype(int)
    
    naics_12_17 = naics_12_17.drop_duplicates(subset = ['naics2012'])[['naics2012','naics2017']]
  
    return naics_12_17
