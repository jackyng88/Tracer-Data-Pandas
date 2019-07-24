import pandas as pd
from pandas.io.json import json_normalize
import numpy as np
import time
import ast
import json

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

    # Iterate through the rows in the DataFrame with .itertuples(), where 
    # each dict_row becomes a Pandas object
    for dict_row in temp.itertuples(index=False):
        # Iterate through the elements in the Pandas object. Each dict_entry
        # becomes a string.
        for dict_entry in dict_row:
            # Since each dict_entry is a string we need to convert that to a
            # dictionary object using json.loads() function.
            temp_json_data = json.loads(dict_entry)
            for dict_entry in temp_json_data:
                # As we iterate through the new dictionary objects we check to
                # see if these dictionaries fulfill the condition of whether
                # they are of the action1 or action2 function parameters.
                if dict_entry['action'] == action1 or dict_entry['action'] == action2:
                    # If either actions are a hit, append to sources_list.
                    sources_list.append(dict_entry)
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


def compute_greater_occurrences(dataframe, action1, action2):
    '''
    Helper function that gets called by source_greater_actions() that returns
    a list of strings.
    '''
    result = []

    ''' new_answer is a dictionary of single letter 'source' keys, i.e. 
    'A', 'B', 'C', etc. The values of the single letter key is another
    dictionary containing action1 and action2 as key values. With the actions
    having the counts as values. Example - 
    {'A': {'junk': 24932, 'noise': 24990}, 
    'B': {'junk': 24939, 'noise': 24980}}

    .iloc[:,:-1] takes all the single letter columns only, remember we sorted
    the columns and had the 'actions' column appear at the very end.

    .notnull() converts the sparse matrix-like DataFrame we have to have values
    of False for NaN and True when there they are notnull().

    .astype(int) converts the True and False values into 1 or 1.

    We groupby(dataframe.action) so we can sum up these pairings.

    Lastly we call the built-in to_dict() function to convert it to a dict.
    '''
    new_answer = (dataframe.iloc[:, :-1].notnull().astype(int)
                 .groupby(dataframe.action).sum().to_dict())

    # Since new_answer is a dictionary of single letter keys with a dictionary
    # as value, we iterate through the keys themselves.
    for letter in new_answer:
        # temp contains the dictionary that we retrieved that is of the form
        # {'junk': 24932, 'noise': 24990} for instance.
        temp = new_answer.get(letter)
        # Get the associated counts in the dictionary object for the two
        # actions.
        action1_count = temp.get(action1)
        action2_count = temp.get(action2)
        # Since we're trying to find out which sources report action1 more 
        # than action2 we compare the values. Only append to the result list
        # if action1_count > action2_count
        if action1_count > action2_count:
            result.append(letter)

    return result


def source_action_location(dataframe1, dataframe2, source, action, location):
    '''
    Function that takes dataframe1 and finds a list of campaign_id's that 
    targetted the location parameter. With the list of campaign_id's 
    we search through dataframe2 for those rows of the source parameter 
    that are of the action parameter.
    '''
    

#print(source_reporting_action(df_ad, 'junk', 'noise'))
answer, run_time = source_action_reports(df_ad, 'conversions', 'views')


# new_answer = (answer.set_index('action')
#   .stack().reset_index()
#   .pivot_table(index='action', columns='level_1',
#   values=0, aggfunc='count', fill_value=0).to_dict())


print(answer)
print(run_time)