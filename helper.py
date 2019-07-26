import json
from itertools import chain
'''
Python file for helper functions that get called from main.py
'''


def dataframe_action_occurences(dataframe, *args):
    """ Helper function that gets called to create a sources list by converting
    datapoint into dictionaries with json.loads
    """
    sources_list = []
    for datapoint in chain(*dataframe.loc[:, 'actions']
                           .apply(json.loads)):
        if datapoint['action'] in args:
            sources_list.append(datapoint)
    return sources_list



'''
Older version - not as fast/efficient
def dataframe_action_occurences(dataframe, *actions):
    sources_list = []
    
    for dict_row in dataframe.itertuples(index=False):
        for dict_entry in dict_row:
            temp_json_data = json.loads(dict_entry)
            for dict_entry in temp_json_data:
                if dict_entry['action'] in actions:
                    sources_list.append(dict_entry)
    return sources_list
'''

def compute_greater_occurrences(dataframe, action1, action2):
    """ Helper function that gets called by source_greater_actions() that returns
    a list of strings.
    """
    result = []

    """ new_answer is a dictionary of single letter 'source' keys, i.e. 
    'A', 'B', 'C', etc. The values of the single letter key is another
    dictionary containing action1 and action2 as key values. With the actions
    having the counts as values. Example - 
    {'A': {'junk': 24932, 'noise': 24990}, 
    'B': {'junk': 24939, 'noise': 24980}}
    """
    new_answer = (dataframe.iloc[:, :-1].notnull().astype(int)
                 .groupby(dataframe.action).sum().to_dict())

    # Since new_answer is a dictionary of single letter keys with a dictionary
    # as value, we iterate through the keys themselves.
    for letter in new_answer:
        # temp contains the dictionary that we retrieved that is of the form
        # {'junk': 24932, 'noise': 24990} for example.
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


def campaigns_by_location(dataframe, location):
    """ Function that looks through the passed in DataFrame by location. """
    campaign_ids = []
    
    # temp_df is a copy of the dataframe where we drop the 'impressions' col
    temp_df = dataframe.drop('impressions', axis=1)
    # campaign_id_df is a DataFrame which has the rows where audience matches
    # the provided location parameter
    campaign_id_df = temp_df[temp_df['audience'].str.match(location)]
    # Get the unique campaign id's from the campaign_id column
    campaign_ids = campaign_id_df.campaign_id.unique()

    return campaign_ids


def compute_source_action_count(dataframe, source, action):
    """ Calculate the number of times a source reports an action by iterating
    through the DataFrame.
    """
    result = 0
    sources_list = dataframe_action_occurences(dataframe, action)
    
    # iterate through sources_list and count the occurences of the source 'key'
    for dict_entry in sources_list:
        if source in dict_entry.keys():
            result += 1

    return result


def calculate_cpm(spend, impressions):
    """ Helper function that gets invoked to calculate CPM."""

    cpm = (spend / impressions) * 1000
    return cpm

