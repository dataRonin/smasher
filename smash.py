from smasher3 import *
import sys


if len(sys.argv) >= 1:
	desired_database = sys.argv[1]
	desired_daily_entity = sys.argv[2]
	desired_start_day = sys.argv[3]
	desired_end_day = sys.argv[4]

else:


try:
    # This is imported from `form_connection.py`
    # conn is the connection for doing updates - you can only ever have 1 conn
    # cur is the cursor for doing selects - you can have as many curs as you'd like.
    conn, cur = form_connection()
except Exception:
    print(" Please create the form_connection script following instructions by Fox ")

# a dictionary telling the column names in the tables in LTERLogger_pro.
# ex. database_map = {'HT004':{'51': ['DATE_TIME', 'DB_TABLE, 'EC_INST'] }}
database_map = get_unique_tables_and_columns(cur)
# tells which daily go with which high-resolution, i.e. daily_index = {'HT004':{'1':'11', '41':'51'}}
daily_index = is_daily(database_map)
# all the information from method_history (hr_methods) and method_history_daily (daily_methods) for a given probe code
# for example, hr_methods['RELh1502'] ={0:{'critical_value' : 95, 'height' : 150, 'resolution' : '15 minutes'}}. See README.md.
hr_methods, daily_methods = get_methods_for_all_probes(cur)



## Required inputs: database and daily table desired, start and end dates of aggregation (or determine from what is there)
desired_database = 'MS034'
desired_daily_entity = '04'
desired_start_day = '2014-10-01 00:00:00'
desired_end_day = '2015-04-10 00:00:00'

# returns are the names of the columns in the
raw_data, column_names, xt, smashed_template = select_raw_data(cur, database_map, daily_index, hr_methods, daily_methods, desired_database, desired_daily_entity, desired_start_day, desired_end_day)

# perform daily calculations
temporary_smash = comprehend_daily(smashed_template, raw_data, column_names, xt)

# calculate daily flags
temporary_flags = calculate_daily_flags(raw_data, column_names, temporary_smash)

# create some output structure containing both the data and the flags
smashed_data = unite_data(temporary_smash, temporary_flags)

output_dictionary = create_outs(raw_data, smashed_template, smashed_data, desired_database, desired_daily_entity)

insert_data(cur, output_dictionary, daily_index, desired_database, desired_daily_entity)