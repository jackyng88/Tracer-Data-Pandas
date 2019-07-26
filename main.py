import pandas as pd
import numpy as np
import json
from timeit import default_timer as timer
from itertools import chain

from helper import *


# df = DataFrame, a pandas object. Loading the two datasets.
df_campaign = pd.read_csv('source1.csv')
df_ad = pd.read_csv('source2.csv')

# Question 1
def campaign_spending_days(dataframe, spend_amt, days):
    """ Function that returns a numpy array of unique campaign_ids based on the
    passed in dataframe. Checks to see if spend_amt parameter is at least 
    this amount, as well as occurred on at least this many days.
    """
    start_time = timer()

    # temp_df initially looks for rows where spend > 0, then it creates a 
    # grouping between campaign_id and date and counts instances of them.
    temp_df = (dataframe[dataframe['spend'] > spend_amt]
              .groupby(['campaign_id', 'date']).size().reset_index()
              .rename(columns={0:'count'}))

    # answer is a numpy array of unique campaign_id's that have counts > 
    # the days parameter. Since temp_df is a dataframe of campaign_id and dates
    # where they spent more than 0 and counting the number of times those
    # are met. 
    answer = temp_df[temp_df['count'] > days].campaign_id.unique()
    end_time = timer()
    run_time = end_time - start_time

    # Optional f-string print. Un-comment for some readability. Can also change
    # print to return
    # print (f'There are {len(answer)} campaigns that spent more than \
    #         {spend_amt} on more than {days} days')

    # We return len(answer) since we want to know the number of campaigns
    return len(answer), run_time
    

# Question 2
def source_action_reports(dataframe, action1, action2):
    """ Function that calculates which sources reported more of action1 than 
    action2. The returned result at the very end is a list of strings of said
    sources.
    """
    start_time = timer()
    temp_df = pd.DataFrame(dataframe['actions'])
 
    # sources_list will be a list that contains a list of dictionaries that 
    # fulfill the function parameter requirements.
    sources_list = dataframe_action_occurences(temp_df, action1, action2)
    new_df = pd.DataFrame(sources_list)

    # This step sorts the columns in alphabetical order with the 'actions' 
    # column appearing at the very end to have the actions column eventually
    # stripped off in the preceding helper function call. new_df becomes a 
    # DataFrame with all the single letters as columns with many NaN values
    # and an integer value associated with the source. Basically this dataframe
    # looks like a sparse matrix.
    new_df = new_df.reindex(columns=sorted(new_df.columns))

    # Call the compute_greater_occurrences helper function.
    result = compute_greater_occurrences(new_df, action1, action2)

    end_time = timer()
    run_time = end_time - start_time

    # Optional f-string literal print for better readability.
    #print (f'The sources that reported more {action1} than {action2} are {result}')

    return result, run_time


# Question 3
def source_action_location(dataframe1, dataframe2, source, action, location):
    """ Function that takes dataframe1 and finds a list of campaign_id's that 
    targetted the location parameter. With the list of campaign_id's 
    we search through dataframe2 for those rows of the source parameter 
    that are of the action parameter.
    Dataframe1 - dataset with campaign_id, audience, impression columns
    Dataframe2 - dataset with ad_id, ad_type, campaign_id, date, spend, actions
    """

    start_time = timer()
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
    end_time = timer()
    run_time = end_time - start_time

    # Optional f-string print for readability.
    # print(f'Source {source} had {result} {action} for {location}')
    return result, run_time


# Question4
def total_ad_cost_per_action(dataframe, ad_type, *actions):
    """ Function that calculates total cost per view for all adds of type video
    truncated to two decimal places.
    cost per view = video spend / video views
    """
    
    start_time = timer()
    cpa = 0
    total_cpa = 0

    if 'views' in actions and ad_type != 'video':
        print('error')
    
    temp_df = (dataframe[dataframe['ad_type'] == ad_type]
            .groupby(['spend', 'actions']).size().reset_index()).iloc[:, :-1]
            
    # This for loop is similar to the one seen in dataframe_action_occurences()
    # function in helper.py
    for spend_action in temp_df.itertuples(index=False):
        # The spend_action tuple will look like (spend=number, action=[{}])
        # Since we know the structure, spend_amt is index 0 and the
        # dict_list will be index 1. Note that currently dict_list is just a
        # string.
        spend_amt = spend_action[0]
        dict_list = spend_action[1]
        # temp_dict_list becomes a list of dictionaries by using json.loads()
        # to convert since dict_list is currently a string.
        temp_dict_list = json.loads(dict_list)
        for dict_entry in temp_dict_list:
            # As we iterate through the nested dictionaries within the 
            # temp_dict_list we check to see if the action is a hit.
            if dict_entry['action'] in actions:
                # Since we know the structure of the dictionary, which is 
                # in the form of {"A": 16, "action": "conversions"} for
                # example, we know that the .values() of the dictionary will
                # get us [16, 'conversions'] if we do list(dict.values()) and
                # we call index 0 since the action count is always first.
                action_count = list(dict_entry.values())[0]
                # Since we can't divide by zero, we only calculate cpa 
                # or cost per action on values greater than zero.
                if action_count > 0:
                    cpa = spend_amt / action_count
                    total_cpa += cpa
                
    end_time = timer()
    run_time = end_time - start_time

    #print(f'The total cost per {actions} was {total_cpa:.2f} for ads of type {ad_type}')
    # Returns f-string literal of total_cpa rounded to two decimal places.
    return f'{total_cpa:.2f}', run_time


# Question 5
def state_hair_cpm(dataframe1, dataframe2, order=True):
    """Function to return the state and hair with the best CPM. 
    # CPM = spend / impressions * 1000
    """
    start_time = timer()

    # campaign_total_spend_df is just campaign_id's having their spend values
    # summed up.
    campaign_total_spend_df = (dataframe2.groupby(['campaign_id'])['spend']
                            .sum().reset_index())
    # We drop the rows with spend values that are zero.
    campaign_total_spend_df = campaign_total_spend_df[~(campaign_total_spend_df == 0).any(axis=1)]
    
    # temp_df is a DataFrame that is the merge of dataframe1 and
    # campaign_total_spend_df. The end result is the cumulative spend column
    # values are with their respective campaign_id's by merging on campaign_id.
    temp_df = pd.merge(dataframe1, campaign_total_spend_df, on="campaign_id")

    # Vectorization with numpy arrays by invoking .values, without .values 
    # it is just vectorization with a Pandas series (slightly slower).
    temp_df['cpm'] = (calculate_cpm(temp_df['spend'].values, 
                                    temp_df['impressions'].values))
    # We sort temp_df in the order specified in the function parameters. By
    # default, it sorts by ascending but upon invoking of the function if we
    # specify false, it will sort by descending order.
    temp_df.sort_values(by='cpm', ascending=order, inplace=True)

    # Since the DataFrame is sorted with either the min or max at the top-most
    # row we grab the first row and the state/hair combo from audience.
    state_hair_combo =  temp_df.iloc[0]['audience']
    end_time = timer()
    run_time = end_time - start_time

    return state_hair_combo, run_time



if __name__ == "__main__":
    # Question 1
    print(campaign_spending_days(df_ad, 0, 4))

    # Question 2
    print(source_action_reports(df_ad, 'junk', 'noise'))

    # Question 3
    print(source_action_location(df_campaign, df_ad, 'B', 'conversions', 'NY'))

    # Question 4
    print(total_ad_cost_per_action(df_ad, 'video', 'views'))

    # Question 5
    print(state_hair_cpm(df_campaign, df_ad))
