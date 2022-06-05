This repository contains the main industry-level measures used in [Corporate Flexibility in a Time of Crisis](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3778789) by John W. Barry, Murillo Campello, John R. Graham and Yueran Ma, along with the code to reproduce them.
## Industry measures
Please read Section 2 of the [paper](./Paper.pdf) and specifically the Data Appendix (Appendix C of the [Internet Appendix](./Internet_Appendix.pdf) for more detail on construction. The four measures included here are:
- [Workplace Flexibility](./Out_Data/workplace_flex.csv) - four-digit NAICS measure of ability to move to a remote working environment
  - The main measure of workplace flexibility is named ```workfromhome```
- [Investment Flexibility](./Out_Data/investment_flex.csv) - four-digit NAICS measure of flexibility with respect to speed of completion of investment projects
  - The main measure of investment flexibility is named ```flex_speed```
- [Customer Interactions](./Out_Data/cust_int.csv) - four-digit NAICS measure of importance of interactions with customers/consumers to a firm's business that captures both direct customer interactions and "indirect" customer interactions (i.e., the importance of direct customer interactions of downstream industries)
  - The main measure of customer interactions is ```cust_int_tot```, which is a weighted average of the direct and indirect measures of customer interactions
- [Fixed Cost Share](./Out_Data/fixed_cost_share.csv) - four-digit NAICS measure of the relative importance of fixed costs (to variable costs) for a firm's business
  - The main measure of fixed cost share is named ```fc_ind```

## Code
All code is contained in subdirectory [Code](./Code/):
- ```download_data.py``` contains functions that download the raw data needed for each measure. 
- ```workplace_flex.py``` contains functions that build the workplace flexibility measure
- ```investment_flex.py``` contains functions that build the investment flexibility measure
  - Please note: the underlying data to build this measure comes from the Duke/CFO survey, which is not available publicly, so only the code to produce the measure and the measure itself are included. 
- ```customer_interactions.py``` contains functions that build the customer interactions measure (direct, indirect, and combined)
- ```fixed_cost_share.py``` contains functions that build the fixed cost share measure
- ```runfile.py``` downloads the data and then compiles the four measures
## Replication instructions
#### Download and run code
1. Download (or clone) this repository.
2. Open a prompt and navigate to the folder location on your machine
3. Run ```pip install -r requirements.txt```, or install the packages [wrds](https://wrds-www.wharton.upenn.edu/documents/1443/wrds_connection.html) and [pyDataverse](https://pydataverse.readthedocs.io/en/latest/) yourself (if you do not have them already).
5. Either open runfile.py in your Python IDE, or enter ```python runfile.py``` in your prompt
6. The raw data will then download to directory [Data](./Data/) and the industry measures will download and overwrite the files in [Out_Data](./Out_Data/)


