import json

'''
Python file for helper functions that get called from main.py
'''

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


def campaigns_by_location(dataframe, location):
    '''
    Function that looks through the passed in DataFrame by location.
    '''
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
    # Calculate the number of times a source reports an action by iterating
    # through the DataFrame.
    result = 0
    sources_list = dataframe_action_occurences(dataframe, action)

    # iterate through sources_list and count the occurences of the source 'key'
    for dict_entry in sources_list:
        if source in dict_entry.keys():
            result += 1

    return result

def dataframe_action_occurences(dataframe, *args):
    # Function that iterates dataframe with variable amount of arguments which
    # are actions to check rows.
    sources_list = []

    for dict_row in dataframe.itertuples(index=False):
        # Iterate through the elements in the Pandas object. Each dict_entry
        # becomes a string.
        for dict_entry in dict_row:
            # Since each dict_entry is a string we need to convert that to a
            # dictionary object using json.loads() function.
            temp_json_data = json.loads(dict_entry)
            for dict_entry in temp_json_data:
                # As we iterate through the new dictionary objects we check to
                # see if these dictionaries fulfill the condition of whether
                # they are of in the *args list.
                if dict_entry['action'] in args:
                    # If either actions are a hit, append to sources_list.
                    sources_list.append(dict_entry)
    return sources_list