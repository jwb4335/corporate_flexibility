from Code.download_data import (get_workplace_flex, 
                                get_customer_interactions,
                                get_fixed_cost_share, 
                                get_industry_data)

from Code.customer_interactions import build_cust_int

from Code.workplace_flex import build_workplace_flex

from Code.fixed_cost_share import build_fixed_cost_share

from Code.investment_flex import build_investment_flex


#%% Download data
print("Downloading raw data...")
## Workplace Flex data - will save raw data in directory data/workplace_flex/
get_workplace_flex()

## Workplace Flex data - will save raw data in directory data/customer_interactions/
get_customer_interactions()

## Fixed Cost Share data - will save raw data in directory data/fixed_cost_share/
## Note - requires active WRDS subscription and wrds package
get_fixed_cost_share()

## Download naics descriptors, census-naics crosswalk - will save raw data in directory data/ind_data/
## Note - required pyDataverse package
get_industry_data()

#%% Create industry measures

print("Building workplace flexibility measure at 4-digit NAICS...",end = '\n\n')
workplace_flex = build_workplace_flex()

print("""Building investment flexibility measure at 4-digit NAICS...
(note: raw data not included here as it is proprietary survey data, so function will just exit)""",end = '\n\n')
investment_flex = build_investment_flex()

print("Building customer interactions measure using direct and indirect customer interactions at 4-digit NAICS...",end = '\n\n')
cust_int = build_cust_int()

print("Building fixed cost share at 4-digit NAICS...",end = '\n\n')
fixed_cost_share = build_fixed_cost_share()

## Save data
# workplace_flex.to_csv("Out_data/workplace_flex.csv",index=False)
# cust_int.to_csv("Out_data/cust_int.csv",index=False)
# fixed_cost_share.to_csv("Out_data/fixed_cost_share.csv",index=False)
# investment_flex.to_csv("Out_data/investment_flex.csv",index=False)

print("Done!")
