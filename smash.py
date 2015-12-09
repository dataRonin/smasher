from smasher3 import *
import sys

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

print("\"<grunt> SMASHER SMASH DATA! <grunt>\"")

if len(sys.argv) == 5:
	print("You have provided all the inputs: database, entity, start, end")
	desired_database = sys.argv[1]
	desired_daily_entity = sys.argv[2]
	desired_start_day = sys.argv[3]
	desired_end_day = sys.argv[4]

	# raw_data is the data as {`PROBE CODE`:{`date`:{`attribute`:[value1, value2, value3, etc.]}}}
	# column names are the columns of high resolution data
	# xt is an indicator for if the maxtime or mintime needs to be computed
	# smashed template is the basic daily structure to populate
	raw_data, column_names, xt, smashed_template = select_raw_data(cur, database_map, daily_index, hr_methods, daily_methods, desired_database, desired_daily_entity, desired_start_day, desired_end_day)

	# perform daily calculations -- temporary smash is created within the function and then populated with each column that we can populate it with -- it is grouped by attribute, like {`AIRTEMP_MEAN_DAY`:{`date`:{`PROBE_CODE`: value}}}
	temporary_smash = comprehend_daily(smashed_template, raw_data, column_names, xt)

	# calculate daily flags by creating 'flag_counters' which indicate the number of unique flags. It is organized by attribute, like {`AIRTEMP_MEAN_FLAG`:{`date`:{`PROBE_CODE`: flag}}}
	temporary_flags = calculate_daily_flags(raw_data, column_names, temporary_smash, smashed_template)

	# creates output structure containing both the data and the flags
	smashed_data = unite_data(temporary_smash, temporary_flags)

	# reorganizes the output structure from being organized by attribute, to being organized by probe. cleans up little things like VPD MINTIME, WINDROSE, converting "M" to "A" for when the max column could be found from the mean, etc.
	output_dictionary = create_outs(raw_data, smashed_template, smashed_data, desired_database, desired_daily_entity, xt)

	# Insertion into SQL server.
	insert_data(cur, output_dictionary, daily_index, desired_database, desired_daily_entity, smashed_template, conn)

	print("finished smashing daily for " + desired_database + desired_daily_entity)

elif len(sys.argv) == 4:
	print("Today will be used as the final date. The beginning date has been specified")
	desired_database = sys.argv[1]
	desired_daily_entity = sys.argv[2]
	desired_start_day = sys.argv[3]
	desired_end_day = datetime.datetime.strftime(datetime.datetime.now().year, datetime.datetime.now().month, datetime.datetime.now().day, '%Y-%m-%d %H:%M:%S')

	# raw_data is the data as {`PROBE CODE`:{`date`:{`attribute`:[value1, value2, value3, etc.]}}}
	# column names are the columns of high resolution data
	# xt is an indicator for if the maxtime or mintime needs to be computed
	# smashed template is the basic daily structure to populate
	raw_data, column_names, xt, smashed_template = select_raw_data(cur, database_map, daily_index, hr_methods, daily_methods, desired_database, desired_daily_entity, desired_start_day, desired_end_day)

	# perform daily calculations -- temporary smash is created within the function and then populated with each column that we can populate it with -- it is grouped by attribute, like {`AIRTEMP_MEAN_DAY`:{`date`:{`PROBE_CODE`: value}}}
	temporary_smash = comprehend_daily(smashed_template, raw_data, column_names, xt)

	# calculate daily flags by creating 'flag_counters' which indicate the number of unique flags. It is organized by attribute, like {`AIRTEMP_MEAN_FLAG`:{`date`:{`PROBE_CODE`: flag}}}
	temporary_flags = calculate_daily_flags(raw_data, column_names, temporary_smash, smashed_template)

	# creates output structure containing both the data and the flags
	smashed_data = unite_data(temporary_smash, temporary_flags)

	# reorganizes the output structure from being organized by attribute, to being organized by probe. cleans up little things like VPD MINTIME, WINDROSE, converting "M" to "A" for when the max column could be found from the mean, etc.
	output_dictionary = create_outs(raw_data, smashed_template, smashed_data, desired_database, desired_daily_entity, xt)

	# Insertion into SQL server.
	insert_data(cur, output_dictionary, daily_index, desired_database, desired_daily_entity, smashed_template, conn)

	print("finished smashing daily for " + desired_database + desired_daily_entity)

elif len(sys.argv) == 3:
	desired_database = sys.argv[1]
	desired_daily_entity = sys.argv[2]
	desired_start_day = ""
	desired_end_day = ""
	print("Processing all data available for " + desired_database + desired_daily_entity)

	desired_start_day, desired_end_day = detect_recent_data(cur, desired_database, desired_daily_entity, daily_index)
	if desired_start_day == "" and desired_end_day == "":
		print("Not able to find start and ending dates or date_times in either the high_resolution or daily data")
		sys.exit("Check the SQL server that high-resolution data exists for " + desired_database + daily_index[desired_database][desired_daily_entity])
	else:
		raw_data, column_names, xt, smashed_template = select_raw_data(cur, database_map, daily_index, hr_methods, daily_methods, desired_database, desired_daily_entity, desired_start_day, desired_end_day)

		# perform daily calculations -- temporary smash is created within the function and then populated with each column that we can populate it with -- it is grouped by attribute, like {`AIRTEMP_MEAN_DAY`:{`date`:{`PROBE_CODE`: value}}}
		temporary_smash = comprehend_daily(smashed_template, raw_data, column_names, xt)

	    # calculate daily flags by creating 'flag_counters' which indicate the number of unique flags. It is organized by attribute, like {`AIRTEMP_MEAN_FLAG`:{`date`:{`PROBE_CODE`: flag}}}
		temporary_flags = calculate_daily_flags(raw_data, column_names, temporary_smash, smashed_template)

	    # creates output structure containing both the data and the flags
		smashed_data = unite_data(temporary_smash, temporary_flags)

	    # reorganizes the output structure from being organized by attribute, to being organized by probe. cleans up little things like VPD MINTIME, WINDROSE, converting "M" to "A" for when the max column could be found from the mean, etc.
		output_dictionary = create_outs(raw_data, smashed_template, smashed_data, desired_database, desired_daily_entity, xt)

	    # Insertion into SQL server.
		insert_data(cur, output_dictionary, daily_index, desired_database, desired_daily_entity, smashed_template, conn)

		print("finished smashing daily for " + desired_database + desired_daily_entity)

elif len(sys.argv) == 2:
	desired_database = sys.argv[1]
	desired_daily_entity = ""
	desired_start_day = ""
	desired_end_day = ""
	print("All tables in " + desired_database + " will be processed from the last complete day")

	for desired_daily_entity in sorted(list(daily_index[desired_database].keys())):
		desired_start_day, desired_end_day = detect_recent_data(cur, desired_database, desired_daily_entity, daily_index)

		if desired_start_day == "" and desired_end_day == "":
			print("Not able to find start and ending dates or date_times in either the high_resolution or daily data")
			print("Check the SQL server that high-resolution data exists for " + desired_database + daily_index[desired_database][desired_daily_entity])
			continue
		else:
			raw_data, column_names, xt, smashed_template = select_raw_data(cur, database_map, daily_index, hr_methods, daily_methods, desired_database, desired_daily_entity, desired_start_day, desired_end_day)

			# perform daily calculations -- temporary smash is created within the function and then populated with each column that we can populate it with -- it is grouped by attribute, like {`AIRTEMP_MEAN_DAY`:{`date`:{`PROBE_CODE`: value}}}
			temporary_smash = comprehend_daily(smashed_template, raw_data, column_names, xt)

		    # calculate daily flags by creating 'flag_counters' which indicate the number of unique flags. It is organized by attribute, like {`AIRTEMP_MEAN_FLAG`:{`date`:{`PROBE_CODE`: flag}}}
			temporary_flags = calculate_daily_flags(raw_data, column_names, temporary_smash, smashed_template)

		    # creates output structure containing both the data and the flags
			smashed_data = unite_data(temporary_smash, temporary_flags)

		    # reorganizes the output structure from being organized by attribute, to being organized by probe. cleans up little things like VPD MINTIME, WINDROSE, converting "M" to "A" for when the max column could be found from the mean, etc.
			output_dictionary = create_outs(raw_data, smashed_template, smashed_data, desired_database, desired_daily_entity, xt)

		    # Insertion into SQL server.
			insert_data(cur, output_dictionary, daily_index, desired_database, desired_daily_entity, smashed_template, conn)

			print("...cleaning up variables...")

			del output_dictionary
			del smashed_data
			del temporary_smash
			del temporary_flags
			del column_names
			del smashed_template
			del xt
			del desired_start_day
			del desired_end_day

			print("finished smashing daily for " + desired_database + desired_daily_entity)

elif len(sys.argv) == 1:
	desired_start_day = ""
	desired_end_day = ""
	desired_daily_entity = ""
	desired_database = ""

	print("All tables in all databases will be processed from the last complete day.")

	for desired_database in sorted(list(daily_index.keys())):
		for desired_daily_entity in sorted(list(daily_index[desired_database].keys())):
			desired_start_day, desired_end_day = detect_recent_data(cur, desired_database, desired_daily_entity, daily_index)

			if desired_start_day == "" and desired_end_day == "":
				print("Not able to find start and ending dates or date_times in either the high_resolution or daily data")
				print("Check the SQL server that high-resolution data exists for " + desired_database + daily_index[desired_database][desired_daily_entity])
				continue

			else:
				raw_data, column_names, xt, smashed_template = select_raw_data(cur, database_map, daily_index, hr_methods, daily_methods, desired_database, desired_daily_entity, desired_start_day, desired_end_day)

				# perform daily calculations -- temporary smash is created within the function and then populated with each column that we can populate it with -- it is grouped by attribute, like {`AIRTEMP_MEAN_DAY`:{`date`:{`PROBE_CODE`: value}}}
				temporary_smash = comprehend_daily(smashed_template, raw_data, column_names, xt)

			    # calculate daily flags by creating 'flag_counters' which indicate the number of unique flags. It is organized by attribute, like {`AIRTEMP_MEAN_FLAG`:{`date`:{`PROBE_CODE`: flag}}}
				temporary_flags = calculate_daily_flags(raw_data, column_names, temporary_smash, smashed_template)

			    # creates output structure containing both the data and the flags
				smashed_data = unite_data(temporary_smash, temporary_flags)

			    # reorganizes the output structure from being organized by attribute, to being organized by probe. cleans up little things like VPD MINTIME, WINDROSE, converting "M" to "A" for when the max column could be found from the mean, etc.
				output_dictionary = create_outs(raw_data, smashed_template, smashed_data, desired_database, desired_daily_entity, xt)

			    # Insertion into SQL server.
				insert_data(cur, output_dictionary, daily_index, desired_database, desired_daily_entity, smashed_template, conn)

				print("...cleaning up variables...")

				del output_dictionary
				del smashed_data
				del temporary_smash
				del temporary_flags
				del column_names
				del smashed_template
				del xt
				del desired_start_day
				del desired_end_day

				print("finished smashing daily for " + desired_database + desired_daily_entity)