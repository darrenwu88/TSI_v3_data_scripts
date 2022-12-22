'''
Author: Darren Wu
Date: 12/5/2022 

Takes token as input and outputs flat telemetry from the TSI server per requestUrl query

Note that this script file is extremely boilerplate, and not "automated". This is just for proof of concept. 
For the automated version, check out level0.py (the combined script which intergrates all the base functions together)

###IMPORTANT
Can only request telemetry of ONE device at a time, otherwise server throws error

PARAMS:
token_json_file: name of json file with token
start_date = data start point
end_date = data end point
'''