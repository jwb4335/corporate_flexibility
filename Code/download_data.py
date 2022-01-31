
import os
import urllib
import zipfile
import pandas as pd
import copy
import wrds
import numpy as np
import glob
from scipy.stats.mstats import winsorize
import requests
from pyDataverse.api import NativeApi, DataAccessApi   

#%%
def _ask_if_overwrite(fun,folder):
    """
    
    Helper function to check if raw data is already downloaded
    Parameters
    ----------
    fun : can be one of the download functions from below
    folder : folder to check if data exists

    """
    ## Create data/workplace_flex/ directory if needed
    if not os.path.exists(folder):
        os.makedirs(folder)
    ## Check if raw data in directory
    if [x for x in glob.glob('{}/*'.format(folder)) if "atus_0000" not in x or "io_naics_match" not in x]:
        question = "Data exists in {} folder, do you want to overwrite (y/n)\n".format(folder)
        check = str(input(question)).lower().strip()
        if check[0] == 'y':
            print("downloading data from web...", end = "\n")
            fun()
        elif check[0] == 'n':
            pass
        else:
            print("please enter y or n")
            return _ask_if_overwrite(fun,folder)
    else:
        fun()


def get_workplace_flex():
    """
    
    Download data do construction workplace flexibility
    
    """
    ## Get atus data from the web
    def _dl_from_web():
        ## These are the necessary ATUS files, available directly from the BLS website
        links = {'2017 Respondents':'https://www.bls.gov/tus/special.requests/atusresp_2017.zip',
        '2018 Respondents':'https://www.bls.gov/tus/special.requests/atusresp-2018.zip',
        '2017_18 Leave Module':'https://www.bls.gov/tus/special.requests/lvresp-1718.zip'}
        ## name to save intermediate files
        files = {"2017 Respondents":["atusresp_2017.dat","atusresp_2017_final.csv"],
        "2018 Respondents":["atusresp_2018.dat","atusresp_2018_final.csv"],
        "2017_18 Leave Module":["lvresp_1718.dat","lvresp_1718_final.csv"]}
         
        ## Get the data!

        for mod in list(links.keys()):
            print("Downloading {} file...".format(mod), end = '\n')
            extract_dir = "data/workplace_flex/raw/{}".format(mod)
            if not os.path.exists(extract_dir):
                os.makedirs(extract_dir)
        
            dl_link = links[mod]
        
            zip_path, _ = urllib.request.urlretrieve(dl_link)
            with zipfile.ZipFile(zip_path, "r") as f:
                f.extractall(extract_dir)
                
                load_in = pd.read_csv(os.path.join(extract_dir,files[mod][0]))
                load_in.columns = [x.lower() for x in load_in]
                load_in.to_csv(os.path.join("data/workplace_flex/",files[mod][1]),index = False)
            f.close()
         
        ## To aggregate the data up to the industry level, we need lvwt, which to my knowledge is only
        ## directly available from atus-data.org, requires an account. Raw data is included with repo
        print("""
        We also need the lvwt's from atus-data.org, however these cannot be downloaded directly from web
        See file "data/nondownloadble/atus_00002.cbk.txt" 
        for details on how to get these weights from atus-data.org
         """)
        
        with open('data/nondownloadable/atus_00002.cbk.txt') as f:
            atuswgts_dets = f.readlines()
        f.close()
        
        print("atus-data.org lvwt download details:",end = '\n\n')
      
        print("\n".join(atuswgts_dets[:12]))
         
    _ask_if_overwrite(_dl_from_web,"data/workplace_flex")
 
 
#%%
def get_customer_interactions():
    """
    Downlaod data from web needed to build customer interactions variable
    """
    def _dl_from_web(): 
        ###################################################
        ## DOWNLOAD CROSSWALKS
        ###################################################
        print("Downloading customer interactions data from web...",end = "\n")
        
        ## need the onet-soc crosswalks
        print("Downloading ONET crosswalks...",end = "\n")
        onet_soc_2019_2018_link = 'https://www.onetcenter.org/taxonomy/2019/soc/2019_to_SOC_Crosswalk.xls?fmt=xls'
        onet_soc_2019_2018 = pd.read_excel(onet_soc_2019_2018_link,
        skiprows = [0,1],header = 1)
        onet_soc_2019_2018 = onet_soc_2019_2018.rename(columns = {'O*NET-SOC 2019 Code':'onet_soc_2019', 
        'O*NET-SOC 2019 Title':'onet_soc_2019_title', 
        '2018 SOC Code':'soc_2018',
        '2018 SOC Title':'soc_2018_title'})
        
        ## OES hybrid structure
        oes_2019_hybrid_link = 'https://www.bls.gov/oes/oes_2019_hybrid_structure.xlsx'
        oes_2019_hybrid = pd.read_excel(oes_2019_hybrid_link,skiprows = [0,1,2,3],header = 1)
        oes_2019_hybrid.columns = [x.strip() for x in oes_2019_hybrid.columns]
        rename = {'OES 2018 Estimates Code':'oes_2018',
        'OES 2018 Estimates Title':'oes_title',
        '2018 SOC Code':'soc_2018',
        '2018 SOC Title':'soc_2018_title'}
        oes_2019_hybrid = oes_2019_hybrid.rename(columns = rename)
        oes_2019_hybrid = oes_2019_hybrid[['oes_2018','soc_2018']]
        oes_2019_hybrid = oes_2019_hybrid.drop_duplicates()
        
        ###################################################
        ## DOWNLOAD ONET DATA
        ###################################################
        print("Downloading ONET data...",end = "\n")
        
        onet_link = 'https://www.onetcenter.org/dl_files/database/db_25_2_text/Work%20Activities.txt'
        
        onet = pd.read_csv(onet_link, sep='\t', lineterminator='\n')
        
        onet = onet.rename(columns = {'O*NET-SOC Code':'onet_soc_2019'})
        
        
        ## Merge on industry codes
        onet = onet.merge(onet_soc_2019_2018,on = 'onet_soc_2019')
        onet = onet.merge(oes_2019_hybrid,on = 'soc_2018')
        
        
        ## LIMIT TO "Performing for or Working Directly with the Public",
        ## which is the main feature of customer interactions
        ## Element ID = 4.A.4.a.8
        onet_out = onet.loc[onet['Element ID'] == '4.A.4.a.8']
        
        cols_to_keep = ['onet_soc_2019','soc_2018','oes_2018','Scale ID','Data Value','N']
        
        onet_out = onet_out[cols_to_keep]
        
        onet_out = onet_out.pivot(index = ['onet_soc_2019', 'soc_2018', 'oes_2018'],values = ['Data Value','N'],
        columns = ['Scale ID']).reset_index()
        
        
        ## Need to do this to save to csv and preserve multilevel column names
        onet_out.columns = pd.MultiIndex.from_tuples(
        [x if x[1]!='' else (x[0]," ") for x in onet_out.columns])
        
        ###################################################
        ## DOWNLOAD BLS DATA OCC CODE CROSSOVER
        ###################################################
        print("Downloading BLS and OCC code crosswalks...",end = "\n")
        url = 'https://www.bls.gov/oes/special.requests/oesm18in4.zip'
        extract_dir = "data/customer_interactions/raw"
        bls_dir = os.path.join(extract_dir,"oesm18in4/")
        zip_path, _ = urllib.request.urlretrieve(url)
        with zipfile.ZipFile(zip_path, "r") as f:
            f.extractall(extract_dir)
        f.close()
        
        ## Build the necessary 4,3,2-digit NAICS crossovers
        sector_dict = {'naics4':'nat4d_M2018_dl',
                        'naics3':'nat3d_M2018_dl',
                        'naics2':'natsector_M2018_dl'}
        
        bls_data_n4 = pd.read_excel(os.path.join(bls_dir,"{}.xlsx".format(sector_dict['naics4'])))
        bls_data_n4.columns = [x.strip().lower() for x in bls_data_n4.columns]
        bls_data_n4 = bls_data_n4.loc[bls_data_n4['occ_group'] == 'detailed']
        
        ## Need to fix a few industries to match to NAICS4, as BLS industries do not match perfectly:
        ind = 'naics4'
        inds = {'3250A1':['3251','3252','3253','3259'],
                            '3250A2':['3255','3256'],
                            '3320A1':['3321','3322','3325','3326','3329'],
                            '3320A2':['3323','3324'],
                            '3330A1':['3331','3332','3334','3339'],
                            '3370A1':['3371','3372'],
                            '4230A1':['4231','4232','4233','4234','4235','4236','4237','4238','4239'],
                            '4240A1':['4244','4248'],
                            '4240A2':['4242','4246'],
                            '4240A3':['4241','4247','4249'],
                            '4450A1':['4451','4452'],
                            '4530A1':['4532','4533'],
                            '5220A1':['5221','5223'],
                            '5320A1':['5322','5323','5324'],
                            '327000':['3271','3272','3273','3274','3279'],
                            '115100':['1151','1113','1119','1111','1112'],
                            '452000':['4521','4522','4523','4529'],
                            '484000':['4841','4842'],
                            '517000':['5171','5172','5173','5174','5175','5176','5177','5178','5179'],
                            '523000':['5231','5232','5239'],
                            '531000':['5311','5312','5313']}
        
        for big_naics in list(inds.keys()):
            other_naics = inds[big_naics]
            new_bls = bls_data_n4.loc[bls_data_n4['naics'] == big_naics]
        for other in other_naics:
            hold = copy.deepcopy(new_bls)
            hold.loc[hold['naics'] == big_naics,'naics'] = other
            if other == other_naics[0]:
                out = hold
            else:
                out = pd.concat([out,hold])
            bls_data_n4 = pd.concat([bls_data_n4.loc[bls_data_n4['naics']!=big_naics],out]).sort_values(
            by = ['naics','occ_code'])
        
        bls_data_n4[ind] = bls_data_n4['naics'].astype(str).str[0:4].astype(int)
        
        
        
        ## get NAICS3 croswalk
        ind = 'naics3'
        bls_data_n3 = pd.read_excel(os.path.join(bls_dir,"{}.xlsx".format(sector_dict['naics3'])))
        bls_data_n3.columns = [x.strip().lower() for x in bls_data_n3.columns]
        bls_data_n3 = bls_data_n3.loc[bls_data_n3['occ_group'] == 'detailed']
        bls_data_n3[ind] = bls_data_n3['naics'].astype(str).str[0:3].astype(int)
        
        ## get NAICS2 croswalk
        ind = 'naics2' 
        bls_data_n2 = pd.read_excel(os.path.join(bls_dir,"{}.xlsx".format(sector_dict['naics2'])))
        bls_data_n2.columns = [x.strip().lower() for x in bls_data_n2.columns]
        bls_data_n2 = bls_data_n2.loc[bls_data_n2['occ_group'] == 'detailed']
        bls_data_n2[ind] = bls_data_n2['naics']
        
        ## save all needed data to csv in data folder
         
        bls_data_n4.to_csv('data/customer_interactions/bls_data_naics4.csv',index = False)
        bls_data_n3.to_csv('data/customer_interactions/bls_data_naics3.csv',index = False)
        bls_data_n2.to_csv('data/customer_interactions/bls_data_naics2.csv',index = False)
        
        oes_2019_hybrid.to_csv('data/customer_interactions/oes_hybrid_data.csv',index=False)
        onet_out.to_csv('data/customer_interactions/onet_work_activities.csv',index=False)
        onet_soc_2019_2018.to_csv('data/customer_interactions/onet_soc_2019_2018.csv',index=False)
        
        
        ## Get the IO table from the web, which is needed to construct indirect and final measures
        print("Downloading 2012 IO table from web...",end = "\n")
        dls = "https://apps.bea.gov/industry/xls/io-annual/Use_SUT_Framework_2007_2012_DET.xlsx"
        resp = requests.get(dls)
        
        output = open('data/customer_interactions/Use_SUT_Framework_2007_2012_DET.xlsx', 'wb')
        output.write(resp.content)
        output.close()
         
    _ask_if_overwrite(_dl_from_web,"data/customer_interactions")
#%%
 
def get_fixed_cost_share():
    """
    Downlaod data from WRDS needed to build fixed cost share measure
    """
    def _dl_from_web():
        print("Downloading fixed cost share data...",end = "\n")
        print("You need to have an active WRDS subscription to download the data from Compustat",end = "\n\n")
        db = wrds.Connection()
        
        
        sql_query = """
        select distinct gvkey, fyearq, fqtr, datadate, xoprq, saleq, cogsq,
        xsgaq, atq
        from comp.fundq
        where consol = 'C' and indfmt = 'INDL' and datafmt = 'STD' and popsrc = 'D'
        and fyearq>=1995 and fyearq<=2019; 
        """
        
        ## Let's grab the Compustat data (also sort it and reset index)
        comp_q = db.raw_sql(sql_query).sort_values(by = ['gvkey','fyearq']).reset_index(drop=True)
        
        comp_q = comp_q.fillna(value=np.nan)
        
        
        sql_query = """
        select distinct gvkey, naics, conm
        from comp.company 
        """
        
        ind_codes = db.raw_sql(sql_query).sort_values(by = ['gvkey']).reset_index(drop=True)
        
        ind_codes['naics'] = pd.to_numeric(ind_codes['naics'])
        
        ind_codes.loc[~pd.isnull(ind_codes['naics']),'naics{}'.format(4)] = ind_codes['naics'].astype(str).str[0:4]
        
        ind_codes['naics4'] = pd.to_numeric(ind_codes['naics4'])
         
        
        ## need to replace some Compustat NAICS code, 
        ## see here https://tax.wv.gov/Documents/TaxForms/TrendAndPercentGoodTables.NAICS-codes.pdf
        ## Also, see sample company names of replaced naics code
        ## dictionary = {old_naics:2017_naics}
        naics_replace = {2331: 2372, 2332: 2361, 2333: 2362, 2341: 2373, 2349: 2379, 
                            2351: 2382, 2352: 2383, 2353: 2382, 2354: 2381, 2356: 2381, 
                            2359: 2389, 4211: 4231, 4212: 4232, 4213: 4233, 4214: 4234, 
                            4215: 4235, 4216: 4236, 4217: 4237, 4218: 4238, 4219: 4239,
                            4221: 4241, 4222: 4242, 4223: 4243, 4224: 4244, 4225: 4245, 
                            4226: 4246, 4227: 4247, 4228: 4248, 4229: 4249, 4521: 4522, 
                            4529: 4523, 5131: 5151, 5132: 5152, 5133: 5173, 5141: 5182, 
                            5142: 5182, 5161: 5191, 5171: 5173, 5172: 5173, 5175: 5152, 
                            5181: 5191, 7221: 7225, 7222: 7225, 234: 237, 235: 238, 421: 
                            423, 422: 424, 513: 515}
        
        ## Replace naics4 to match 2017 naics
        ind_codes['naics4'] = ind_codes['naics4'].replace(naics_replace)
         ## Drop 9999, not relevant
        ind_codes = ind_codes.loc[ind_codes['naics4']!=9999]
         ## Get naics3 and naics2
        for val in [3,2]:
            ind_codes.loc[~pd.isnull(ind_codes['naics4']),'naics{}'.format(val)] = ind_codes['naics4'].astype(str).str[0:val]
            ind_codes['naics{}'.format(val)] = pd.to_numeric(ind_codes['naics{}'.format(val)])
        
        ## NAICS2 doesn't separate 31-33, 44-45, etc., change back at end
        ind_codes.loc[(ind_codes['naics2'] == 32) | (ind_codes['naics2'] == 33),'naics2'] = 31
        ind_codes.loc[(ind_codes['naics2'] == 41) | (ind_codes['naics2'] == 42),'naics2'] = 42
        ind_codes.loc[(ind_codes['naics2'] == 44) | (ind_codes['naics2'] == 45),'naics2'] = 44
        ind_codes.loc[(ind_codes['naics2'] == 48) | (ind_codes['naics2'] == 49),'naics2'] = 48
        ind_codes.loc[(ind_codes['naics2'] == 91) | (ind_codes['naics2'] == 92),'naics2'] = 91
        
        
        
        ind_codes['gvkey'] = ind_codes['gvkey'].astype(int)
        comp_q['gvkey'] = comp_q['gvkey'].astype(int)
        
        
        ## merge one ind codes
        comp_q = comp_q.merge(ind_codes[['gvkey','naics4','naics3','naics2']])
        
        ## drop missing
        comp_q = comp_q.dropna(subset = ['gvkey','fyearq','fqtr','atq','saleq','xoprq','naics4','naics3','naics2'],how = 'any') 
        
        ## require non-negative sales, assets, operating costs
        for var in ['atq','saleq','xoprq']:
            comp_q = comp_q.loc[comp_q[var]>0]
        
        
        comp_q['yq'] = comp_q['fyearq'].astype(int)*10 + comp_q['fqtr'].astype(int)
        
        
        ## get log change in sales and op. costs 
        def _log_change(comp_q,var):
            np.seterr(divide = 'ignore') 
            
            ## Need to make sure quarters are consecutive
            timeid = comp_q[['yq']].drop_duplicates().sort_values(
            by = 'yq').reset_index(drop=True).reset_index().rename(
            columns = {"index":'time_id'})
            comp_q = comp_q[[x for x in comp_q if x not in ['time_id']]].merge(timeid,on = 'yq')
            comp_q = comp_q.sort_values(by = ['gvkey','yq'])
            ## Get the lag of the variable
            comp_q['{}_l'.format(var)] = comp_q.groupby(['gvkey'])[var].shift(1)
            ## Replace lag with missing if times don't line up
            comp_q['timecomp'] = comp_q.groupby(['gvkey'])['time_id'].shift(1)+1
            ## Get the log change
            comp_q.loc[comp_q['timecomp']!=comp_q['time_id'],'{}_l'.format(var)] = np.nan
            comp_q["{}_g".format(var)] = np.log(comp_q[var]) - np.log( comp_q['{}_l'.format(var)])
            comp_q = comp_q[[x for x in comp_q if x not in ['time_id','timecomp']]]
            return comp_q
            
        for var in ['xoprq','saleq']:
            comp_q = _log_change(comp_q,var)
        
        
        ## Drop where missing
        g_vars = ['xoprq_g','saleq_g']
        comp_q = comp_q.dropna(subset = g_vars,how='any')
        
        
        ## Winsorize vars
        for var in g_vars:
            comp_q[var] = winsorize(comp_q[var],inclusive = (False,False),limits = (0.01,0.01),nan_policy = 'omit')
         
        ## Save to .csv
        comp_q.to_csv('data/fixed_cost_share/comp_q_fc.csv',index = False)
        
    
    ## Does user want to overwrite raw data?
    _ask_if_overwrite(_dl_from_web,"data/fixed_cost_share")



def get_industry_data(**kwargs):
    print("Downloading NAICS descriptions, 2012-2017 NAICS conversions, Census-NAICS crosswalk...",
          end = "\n\n")
    def _get_harvard_dataverse_data_public(DOI):
    
        base_url = 'https://dataverse.harvard.edu/'
        
        api = NativeApi(base_url)
        
        data_api = DataAccessApi(base_url)
    
        dataset = api.get_dataset(DOI)
    
        files_list = dataset.json()['data']['latestVersion']['files']
    
        for file in files_list:
            filename = file["dataFile"]["filename"]
            file_id = file["dataFile"]["id"]
            # print("File name {}, id {}".format(filename, file_id))
            
            response = data_api.get_datafile(file_id)
            with open("data/ind_data/{}".format(filename), "wb") as f:
                f.write(response.content)
            f.close()

    def _save_file_from_link(link,loc):
        resp = requests.get(link)        
        f =  open(loc,"wb")
        f.write(resp.content)
        f.close()
    
    def _dl_from_web():
        _save_file_from_link("https://www.census.gov/naics/2017NAICS/2017_NAICS_Structure.xlsx",
                             "data/ind_data/2017_naics_structure.xlsx")
        
        _save_file_from_link("https://www.naics.com/wp-content/uploads/2017/01/2017_to_2012_NAICS-Changes.xlsx",
                         "data/ind_data/2017_to_2012_NAICS.xlsx")
            
        _get_harvard_dataverse_data_public("doi:10.7910/DVN/O7JLIC")
    
    _ask_if_overwrite(_dl_from_web,"data/ind_data/")

