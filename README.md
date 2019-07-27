# Tracer-Data-Engineer

## Language written in - Python (3.7.3)

## Installation

### Good practice (so I've been told) - Creating a virtual environment

1. Browse to the folder containing the code.

2. From terminal/powershell/command-prompt run command - 

    python -m venv venv
    You can replace the second venv with whatever name you want for your virtual environment.
    
3. Activate your virtual environment by running command - 

    (Windows) venv\Scripts\activate
    (Mac/Linux) source venv/bin/activate
    
4. Install dependencies with (make sure you're in the folder with requirements.txt) - 

    pip install -r requirements.txt
    

### Installing Dependencies (if you don't want to create a virtual environment) 

1. Install dependencies with (make sure you're in the folder with requirements.txt) - 

    pip install -r requirements.txt
    

## How to Use

#### Make sure you're in the folder with main.py on terminal/powershell/command-prompt


1. Make sure you have activated the proper virtual environment/installed the proper dependencies.

2. Run python main.py to start the python script.

3. Currently the outputs are tuples in the form of (answer, run_time). If you want more readable/English outputs go into the respective functions and un-comment the commented Python f-string literals.

4. You can customize a few function parameters if you go to the bottom of main.py

    A. Question 1 - You can change the values for the variables spend_threshold and day_threshold. Currently the function will look for those values that have spent more than spend_threshold AND on more than day_threshold days. By default they are at 0 and 4 days respectively.
    
    B. Question 2 - Since this is a comparison function you can change the values of larger_action and smaller_action. The function will return those sources which reported more of larger_action than those of smaller_action. By default they are 'junk' and 'noise' respectively.
    
    C. Question 3 - You can change the source, action and loc variables. Ideally your loc string will be a two letter uppercase but the following will change it to uppercase just incase.
    
    D. Question 4 - ad_type and actions are 'video' and 'views' by default. You can pass multiple actions by adding more strings to the actions list. If the actions list has 'views' in it and ad_type is 'audio' the function should return and print an error to console.
    
    E. Question 5 - Not particularly much to customize besides changing order. By default order = True so the function will return you the best (lowest) CPM state and hair combination. If you want the worst you can either set order = False or you can add a not in front of order in the function call i.e.  print(state_hair_cpm(df_campaign, df_ad, not order)).
