import pandas as pd
import numpy as np
import copy
from Code.helper import get_naics_descr, get_naics_12_17

    
#%% Main function

def build_cust_int():
    
    ## Aggregate customer interactions to the NAICS level
    # print("Aggregating customer interactions to NAICS level...",end = "\n\n")
    naics4 = _aggregate_to_naics('naics4')
    naics3 = _aggregate_to_naics('naics3')
    naics2 = _aggregate_to_naics('naics2')
    naics2['naics2'] = naics2['naics2'].str.split('-').str[0].astype(int)
    
    ## Build the matching between Input-Output codes and NAICS
    # print("Building matches between IO codes and NAICS...",end = "\n\n")
    naics_matches,naics_matches_hold = _build_io_naics4_match()
    
    ## Get direct customer interactions at the IO level
    # print("Aggregating direct customer interactions at IO level...",end = "\n\n")
    direct = _get_direct_cust_int(naics_matches,naics4,naics3,naics2)
    
    ## Get the IO tables and consumption share to create the indirect measure
    # print("Computing indirect customer interactions using IO tables...",end = "\n\n")
    io_out,cons_share = _get_io_table()
    
    indirect = _get_indirect(io_out,direct)
    
    ## Map back to NAICS4
    # print("Mapping IO customer interactions measures back to NAICS4...",end = "\n\n")
    final_cust_int = _map_back_to_naics(naics_matches_hold,naics4,indirect,cons_share)
    
    return final_cust_int


#%% Helper functions for final

###################################################
## CLEAN ONET DATA
###################################################
def _aggregate_to_oes(var,rename):

    """
    Aggregate onet data to OES level
    """    
    onet_out = pd.read_csv('data/customer_interactions/onet_work_activities.csv',header = [0,1])

    pd.options.mode.chained_assignment = None  # default='warn'
    df = onet_out[[x for x in onet_out if x[1] == var or x[1] == ' ']]
    df.columns = [x[0] for x in df.columns]
    df['missing_N'] = (pd.isnull(df['N']))**1
    df['missing_N'] = df.groupby(['oes_2018'])['missing_N'].transform('max')
    df.loc[df['missing_N'] == 1,'N'] = 1
    out = df.groupby("oes_2018").apply(lambda dfx: (dfx["Data Value"] * dfx["N"]).sum() / dfx["N"].sum()).reset_index().\
            rename(columns = {0:rename})
    df.loc[df['missing_N'] == 1,'N'] = np.nan
    df = df[['oes_2018','N']].drop_duplicates().groupby('oes_2018')['N'].sum().reset_index().replace({0:np.nan})
    out = out.merge(df,on = ['oes_2018'])
    ## Divide importance by 5, level by 7, these are the maxes of these variables
    if var == 'IM':
        out[rename] = (out[rename] - 1).divide(5-1)
    elif var == 'LV':
        out[rename] = (out[rename] - 1).divide(7-1)
    else:
        raise Exception("Incorrect importance variable, must be IM or LV")
    return out



###################################################
## CLEAN BLS AND MATCH TO ONET
###################################################
def _aggregate_to_naics(ind = 'naics4'):

    ## oesm18in4 is split into different industry levels, this is the mapping from naics code to file name
    onet_final = _aggregate_to_oes('IM','importance')

    ## load in BLS data
    bls_data = pd.read_csv('data/customer_interactions/bls_data_{}.csv'.format(ind),dtype={'naics':str})

    if ind == 'naics4':
        
        ## Need to fix a few industries to match to NAICS4, as BLS industries do not match perfectly:
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
            new_bls = bls_data.loc[bls_data['naics'] == big_naics]
            for other in other_naics:
                hold = copy.deepcopy(new_bls)
                hold.loc[hold['naics'] == big_naics,'naics'] = other
                if other == other_naics[0]:
                    out = hold
                else:
                    out = pd.concat([out,hold])
            bls_data = pd.concat([bls_data.loc[bls_data['naics']!=big_naics],out]).sort_values(by = ['naics','occ_code'])
                
            bls_data[ind] = bls_data['naics'].astype(str).str[0:4].astype(int)

        
    elif ind == 'naics3':
        bls_data[ind] = bls_data['naics'].astype(str).str[0:3].astype(int)
    else:
        bls_data[ind] = bls_data['naics']
        
    ## clean BLS data, we are taking weighted-averages by number of employees
    bls_data = bls_data[[ind,'tot_emp','occ_code','occ_title']]
    bls_data.loc[bls_data['tot_emp'] == '**','tot_emp'] =''
    bls_data['tot_emp'] = pd.to_numeric(bls_data['tot_emp'])
    bls_data = bls_data.rename(columns = {'occ_code':'oes_2018'})
    bls_data.loc[pd.isnull(bls_data['tot_emp']),'tot_emp'] = 0
    bls_data = bls_data.groupby([ind,'oes_2018','occ_title'])['tot_emp'].sum().reset_index()
    bls_data = bls_data.merge(onet_final,on = 'oes_2018').sort_values(by = [ind,'oes_2018'])
    
    
    customer_interactions_ind = bls_data.groupby([ind]).\
                apply(lambda dfx: (dfx["importance"]*dfx["tot_emp"]).sum()/dfx["tot_emp"].sum()).\
                reset_index().rename(columns = {0:'cust_int'})
    
    return customer_interactions_ind


## Functions to build IO tables and matches to NAICS4
def _build_io_4_digit():

    io_raw = pd.read_excel('data/customer_interactions/Use_SUT_Framework_2007_2012_DET.xlsx',
                          sheet_name = '2012',skiprows = np.arange(5))
    io_raw.drop(io_raw.tail(3).index,inplace=True)

    io_raw.loc[io_raw['Code'] == "T005",'Code'] == "IIII"
    
    io_raw = io_raw.rename(columns = {'output_naicsT019':'output_naicsTTTT',
                                                    'output_naicsT001':'output_naicsIIII',
                                                    'Code':'input_naics'})
    
    
    io_raw = io_raw.drop('Commodity Description',axis=1)
    
    
    intermed = pd.melt(io_raw,id_vars=['input_naics'],var_name=['output_naics'], value_name='value').sort_values(
               by = ['input_naics','output_naics'])
    
    intermed['input_naics'] = intermed['input_naics'].astype(str).str[:-2]
    intermed['output_naics'] = intermed['output_naics'].astype(str).str[:-2]
    
    intermed = intermed.groupby(['input_naics','output_naics'])[['value']].sum().reset_index()
    
    for var in ['input_naics','output_naics']:
        for char in ['T','F','S','I','G','V']:
            intermed = intermed.loc[~(intermed[var].str[0] == char)]
    
    
    final = intermed.pivot(index = ['output_naics'],columns = ['input_naics'],
                            values = 'value' ).reset_index()
    
    
    final = final.rename(columns = {
            x:"input_naics{}".format(x) for x in final.columns if x not in ['output_naics']}).reset_index()
        
    
    return final

def _build_io_naics4_match():
    
    io_naics = pd.read_csv("data/nondownloadable/io_naics_match.csv")
    
    io_naics = io_naics.loc[io_naics['other_naics1']!='DELETE']
    io_naics.loc[pd.isnull(io_naics['other_naics1']),'other_naics1'] = io_naics['input_naics']
    io_naics = io_naics.dropna(how = 'all',axis=1)
    
    intermed = pd.melt(io_naics,id_vars=['input_naics'],
                       value_name='other_naics',var_name = 'naics4').sort_values(
                       by = ['input_naics','naics4']).reset_index(
                       drop = True)
    intermed['j'] = intermed['naics4'].str.replace("other_naics","").astype(int)
    
    intermed['naics4'] = intermed['input_naics']
    
    intermed.loc[~pd.isnull(intermed['other_naics']),'naics4'] = intermed['other_naics']
    
    intermed = intermed.drop_duplicates(subset = ['input_naics','naics4'])
    
    intermed = intermed.drop(['j','other_naics'],axis=1)
    
    intermed = intermed[pd.to_numeric(intermed['naics4'], errors='coerce').notnull()]
    
    intermed['naics'] = intermed['naics4'].astype(int)

    
    intermed.loc[intermed['naics'].astype(str).str[-1] == '0',
                 'naics'] = intermed['naics'].astype(str).str[0:3].astype(int)

    add_on = intermed.loc[intermed['input_naics'] =='517A'].drop_duplicates(subset = 'input_naics')
    add_on['naics'] = 5173

    add_on2 = intermed.loc[intermed['input_naics'] =='52A0'].drop_duplicates(subset = 'input_naics')
    add_on2['naics'] = 521    
    
    final = intermed.append(add_on).append(add_on2).sort_values(by = ['input_naics','naics'])
    
    final = final[[x for x in final if x not in ['naics4']]]
    
    ## Specific fixes
    final.loc[final['input_naics'].str[0:2]=='23','naics'] = 23
    final = final.loc[final['input_naics']!='4200']
    final.loc[final['input_naics']=='5500','naics'] = 55
    
    final = final.sort_values(by = ['input_naics','naics']).reset_index(drop=True)
    
    final_revert = copy.deepcopy(final)
    
    
    final.loc[final['input_naics'].str.startswith('11'),'naics'] = 11
    final.loc[final['input_naics'].str.startswith('8140'),'naics'] = 81


    return final,final_revert


def _get_io_table():
    io = pd.read_excel("data/customer_interactions/Use_SUT_Framework_2007_2012_DET.xlsx",
                       sheet_name = "2012")
    
    io.columns = io.iloc[4,:]
    
    io = io.iloc[5:,:]
    io = io.loc[io.index<=418]
    io['Code'] = io['Code'].astype(str)
    io['input_naics'] = io['Code'].str[0:4]
    
    io = io[['input_naics'] + [x for x in io if x not in ['input_naics']]]
    
    cons_share = io[['input_naics','T019','F01000','T001']]
    for var in ['T019','F01000','T001']:
        cons_share[var] = pd.to_numeric(cons_share[var])
        cons_share.loc[pd.isnull(cons_share[var]),var] = 0
        
    cons_share = cons_share.groupby(['input_naics'])[['F01000','T001']].sum()        
    cons_share['cons_share'] = cons_share['F01000'].divide(cons_share[['T001','F01000']].sum(axis=1))

    cons_share.loc[pd.isnull(cons_share['cons_share']),'cons_share'] = 1
    cons_share = cons_share.loc[~cons_share.index.str.startswith(('S','G','T','V','F'))]
    cons_share = cons_share.reset_index()
    cons_share = cons_share[['input_naics','cons_share']]
    
    drop_list = [x for x in io.columns if str(x).startswith(('S','G','T','V','F')) or pd.isnull(x)]
        
    io = io[[x for x in io if x not in drop_list]]
    io = io.loc[~io.Code.str.startswith(('S','G','T','V','F'))]
    io.columns = [str(x) for x in io.columns]
    io = io.loc[~pd.isnull(io['Code'])]
    io.Code = io.Code.map({str(x):x for x in io.Code.unique()})
    
    col_list = [x for x in io.columns if x not in ['input_naics', 'Code', 'Commodity Description']]
    io_2 = io.groupby(['input_naics'])[col_list].sum()
    
    io_2 = io_2.T.reset_index().rename(columns = {'index':'output_naics'})
    
    col_list = [x for x in io_2.columns if x not in ['input_naics', 'Code', 'Commodity Description']]
    
    io_2['output_naics'] = io_2['output_naics'].str[0:4]
    
    io_3 = io_2.groupby(['output_naics'])[col_list].sum()
    
    io_3_cols = [x for x in io_3.columns]
    io_3 = io_3.reset_index()
    io_4 = pd.melt(io_3,id_vars = 'output_naics',
                   value_vars = io_3_cols,var_name = 'input_naics')
    
    io_4 = io_4.sort_values(by = ['output_naics','input_naics'])
    
    # io_4.loc[io_4['input_naics'] == io_4['output_naics'],'value'] = np.nan
    io_4 = io_4.dropna(subset = ['value'])


    return io_4,cons_share


def _get_direct_cust_int(naics_matches,naics4,naics3,naics2):
    match = naics_matches.merge(naics4,left_on = 'naics',right_on = 'naics4',how = 'left').\
            rename(columns = {'cust_int':'cust_int_naics4'}).\
            merge(naics3,left_on = 'naics',right_on = 'naics3',how = 'left').\
            rename(columns = {'cust_int':'cust_int_naics3'}).\
            merge(naics2,left_on = 'naics',right_on = 'naics2',how = 'left').\
            rename(columns = {'cust_int':'cust_int_naics2'})

    for ind in ['naics4','naics3','naics2']:
        match.loc[match['naics'] == match[ind],'cust_int'] =\
                  match['cust_int_{}'.format(ind)]
           
    match = match[['input_naics','naics','cust_int']]
    
    io_match = match.groupby(['input_naics'])['cust_int'].mean().reset_index()
    
    # io_match['cust_int'] = (io_match['cust_int'] - io_match['cust_int'].min()).\
    #                        divide(io_match['cust_int'].max()-io_match['cust_int'].min())
    return io_match

def _get_indirect(io_out,direct):
    np.seterr(divide='ignore', invalid='ignore')
    
    ## Merge with direct measure to create indirect measure
    io_out = io_out.merge(direct.rename(columns = {'input_naics':'output_naics'}),on = 'output_naics')
  
    totals = io_out.groupby('input_naics')['value'].sum().reset_index()
    
    indirect = io_out.groupby(['input_naics']).\
            apply(lambda dfx: (dfx["cust_int"]*dfx["value"]).sum()/dfx["value"].sum()).\
            reset_index().rename(columns = {0:'cust_int_indirect'}).\
            merge(totals,on = 'input_naics')

            
            
    indirect.loc[indirect['value'] == 0,'cust_int_indirect'] = 0
           
    return indirect



def _map_back_to_naics(naics_matches_hold,naics4,indirect,cons_share):
    np.seterr(divide='ignore', invalid='ignore')

    indirect = indirect.merge(cons_share,on = 'input_naics')


    naics_out = naics_matches_hold.merge(indirect,on = 'input_naics')
    naics_out = naics_out.loc[~((naics_out['naics'] == 23) & (naics_out['cust_int_indirect'] == 0))]
    
    naics_out['naics'] = naics_out['naics'].astype(int)
    
    naics_out['naics4'] = naics_out['naics'].astype(str).str[0:4].astype(int)
    naics_out['naics3'] = naics_out['naics'].astype(str).str[0:3].astype(int)
    naics_out['naics2'] = naics_out['naics'].astype(str).str[0:2].astype(int)
    
    naics4['naics3'] = naics4['naics4'].astype(str).str[0:3].astype(int)
    naics4['naics2'] = naics4['naics4'].astype(str).str[0:2].astype(int)
    
    
    final = copy.deepcopy(naics4)
    for ind in ['naics4','naics3','naics2']:
        final = final.merge(naics_out[['naics','cust_int_indirect','value','cons_share']],left_on = ind,right_on = 'naics',how = 'left').\
                rename(columns = {'cust_int_indirect':'cust_int_indirect_{}'.format(ind),
                                  'value':'total_linkage_{}'.format(ind),
                                   'cons_share':'cons_share_{}'.format(ind)})
        
        final = final[[x for x in final if x not in ['naics']]]
    
    for var in ['cust_int_indirect','total_linkage','cons_share']:
        final[var] = final['{}_naics4'.format(var)]
        final.loc[pd.isnull(final[var]),var] = final["{}_naics3".format(var)]
        final.loc[pd.isnull(final[var]),var] = final["{}_naics2".format(var)]
        
    final = final[['naics4','cust_int','cust_int_indirect','total_linkage','cons_share']]
    
    ## Need to fix one duplicate ind
    final_7225 =  final.loc[final['naics4'] == 7225]
    
    final_7225_cust_int_ind = (final_7225["cust_int_indirect"]*final_7225['total_linkage']).sum()/(final_7225['total_linkage'].sum())
    final_7225_linkage = final_7225['total_linkage'].sum()
    final_7225_cons_share = final_7225['cons_share'].mean()
    final.loc[final['naics4'] == 7225,'cust_int_indirect'] = final_7225_cust_int_ind
    final.loc[final['naics4'] == 7225,'total_linkage'] = final_7225_linkage
    final.loc[final['naics4'] == 7225,'cons_share'] = final_7225_cons_share

    final = final.drop_duplicates(subset = ['naics4'])
    
    naics_desc = get_naics_descr(level = 4)

    final = final.merge(naics_desc,on = 'naics4',how = 'left')
    
    ## rename cust_int to be more clear
    final = final.rename(columns = {"cust_int":"cust_int_direct"})
    
    final['cust_int_tot'] = final['cust_int_direct'] * final['cons_share'] + final['cust_int_indirect'] * (1-final['cons_share'])
    
    final = final.reindex(['naics4','naics4_title',
                           'cust_int_tot','cust_int_direct',
                           'cust_int_indirect','cons_share'],axis=1)

    return final


