import pandas as pd
from pandas.io.json import json_normalize
import numpy as np
import time

# df = DataFrame, a pandas object. Loading the two datasets.
df_campaign = pd.read_csv('source1.csv')
df_ad = pd.read_csv('source2.csv')


def campaign_spending_days(dataframe, spend_amt, days):
    # Function that returns a numpy array of unique campaign_id's based on the
    # passed in dataframe. Checks to see if spend_amt parameter is at least 
    # this amount, as well as occurred on at least this many days.
    start_time = time.time()
    # temp1 initially looks for rows where spend > 0, then it creates a grouping
    # between campaign_id and date and counts instances of them.
    temp1 = dataframe[dataframe['spend'] > spend_amt].groupby(['campaign_id', 'date']).size().reset_index().rename(columns={0:'count'})
    # answer is a numpy array of unique campaign_id's that have counts > 
    # the days parameter. Since temp1 is a dataframe of campaign_id and dates
    # where they spent more than 0 and counting the number of times those
    # are met.
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
    #answer = {}
    temp = pd.DataFrame(dataframe['actions'])
    #answer = json_normalize(temp)


    end_time = time.time()
    run_time = end_time - start_time
    return temp, run_time


#temp, camp_spend_run_time = campaign_spending_days(df_ad, 0, 4)
#print(len(temp))
#print(camp_spend_run_time)

#print(source_reporting_action(df_ad, 'junk', 'noise'))
answer, time = source_reporting_action(df_ad, 'junk', 'noise')
#print(type(answer))
# answer.to_csv('test.txt')
