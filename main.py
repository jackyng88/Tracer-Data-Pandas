import pandas as pd
import numpy as np
import time
import json

from helper import *

# df = DataFrame, a pandas object. Loading the two datasets.
df_campaign = pd.read_csv('source1.csv')
df_ad = pd.read_csv('source2.csv')

# Question 1
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

# Question 2
def source_action_reports(dataframe, action1, action2):
    '''
    Function that calculates which sources reported more of action1 than 
    action2. The returned result at the very end is a list of strings of said
    sources.
    '''
    start_time = time.time()

    # temp is a dataframe that only contains the values of the 'actions' column
    # from the passed in dataframe.
    temp = pd.DataFrame(dataframe['actions'])
    # sources_list will be a list that contains a list of dictionaries that 
    # fulfill the function parameter requirements.
    sources_list = []

    sources_list = dataframe_action_occurences(temp, action1, action2)
    # new_df is a new DataFrame created from the list of dictionary objects
    new_df = pd.DataFrame(sources_list)
    # This step sorts the columns in alphabetical order with the 'actions' 
    # column appearing at the very end to have the actions column eventually
    # stripped off in the preceding helper function call. new_df becomes a 
    # DataFrame with all the single letters as columns with many NaN values
    # and an integer value associated with the source. Basically this dataframe
    # looks like a sparse matrix.
    new_df = new_df.reindex(columns=sorted(new_df.columns))

    # Call the count_greater_occurrences helper function.
    result = compute_greater_occurrences(new_df, action1, action2)

    end_time = time.time()
    run_time = end_time - start_time
    return result, run_time

# Question 3
def source_action_location(dataframe1, dataframe2, source, action, location):
    '''
    Function that takes dataframe1 and finds a list of campaign_id's that 
    targetted the location parameter. With the list of campaign_id's 
    we search through dataframe2 for those rows of the source parameter 
    that are of the action parameter.
    Dataframe1 - dataset with campaign_id, audience, impression columns
    Datafram2 - dataset with ad_id, ad_type, campaign_id, date, spend, actions
    '''
    start_time = time.time()
    result = 0
    campaign_ids = campaigns_by_location(dataframe1, location)

    # temp_df is a DataFrame with only the rows where the campaign_ids were
    # found as a match in the original passed in DataFrame(dataframe2).
    temp_df = (dataframe2[dataframe2['campaign_id']
              .isin(campaign_ids)]
              .drop(labels=[
                  'ad_id', 'ad_type', 'campaign_id', 'date', 'spend'
                  ], axis=1))

    # Call the compute_source_action_count() helper function.
    result = compute_source_action_count(temp_df, source, action)

    end_time = time.time()
    run_time = end_time - start_time
    return result, run_time


print(source_action_reports(df_ad, 'junk', 'noise'))
print(source_action_location(df_campaign, df_ad, 'B', 'conversions', 'NY'))
