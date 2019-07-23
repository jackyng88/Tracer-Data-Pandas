import pandas as pd
import numpy as np
import time

# df = DataFrame, a pandas object. Loading the two datasets.
df_campaign = pd.read_csv('source1.csv')
df_ad = pd.read_csv('source2.csv')


def campaign_spending_days(dataframe, days):
    # Function that returns a DataFrame with counts of campaigns that incurred
    # spending on at least this many days provided in the parameters.
    start_time = time.time()
    temp1 = dataframe[dataframe['spend'] > 0].groupby(['campaign_id', 'date']).size().reset_index().rename(columns={0:'count'})
    answer = temp1[temp1['count'] > days].campaign_id.unique()
    end_time = time.time()
    run_time = end_time - start_time
    return answer, run_time

def source_reporting_action(dataframe, action1, action2):
    # Function that shows which sources reported more of action1 than action2.
    # Looks through nested dictionaries in the lists and adds to the answer
    # dictionary with the "source" as the key and a tuple with values based on
    # actions. i.e. {'A': (22, 19)}. Source has 22 for action1 and 19 for action2.
    start_time = time.time()
    answer = {}


    end_time = time.time()
    run_time = end_time - start_time
    return answer, run_time


temp, camp_spend_run_time = campaign_spending_days(df_ad, 4)
print(len(temp))
print(camp_spend_run_time)
