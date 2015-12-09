#/anaconda/bin/python
import pymssql
import datetime
from collections import defaultdict
import math
import itertools
from if_none import *
from form_connection import form_connection
from daily_functions import *


def isfloat(string):
    """ From an single input, returns either a float or a None. """
    try:
        return float(string)
    except Exception:
        return None

def get_unique_tables_and_columns(cur):
    """ Gets the tables and columns from the information schema of LTERLogger_pro. Returns a dictionary.

    Tables are `dbcodes` like `MS043`. Columns are the names of attributes, like `AIRTEMP_MEAN`.
    There are some tables we do not want, if you encounter others, add to `bad_strings` internally.
    """

    database_map= {}

    sql = "select table_name, column_name from lterlogger_pro.information_schema.columns group by table_name, column_name"

    cur.execute(sql)

    bad_strings = ['View_', 'FlagI', 'LastU', 'Test', 'metho', 'View_', 'dtpro',  'tblCo', 'PROBE', 'vwFin', 'CSV2D', 'CSVTa', 'Table', 'vwOne', 'sysdi', 'vwUni']

    for row in cur:

        # bad strings are a list of table prefixes that don't contain data
        if str(row[0])[0:5] in bad_strings:
            continue
        if str(row[0])[0:5] not in database_map:
            database_map[str(row[0])[0:5]] = {str(row[0])[5:7].rstrip() : [str(row[1]).rstrip()]}
        elif str(row[0])[0:5] in database_map:
            if str(row[0])[5:7].rstrip() not in database_map[str(row[0])[0:5]]:
                database_map[str(row[0])[0:5]][str(row[0])[5:7].rstrip()] = [str(row[1]).rstrip()]
            elif str(row[0])[5:7] in database_map[str(row[0])[0:5]]:
                database_map[str(row[0])[0:5]][str(row[0])[5:7]].append(str(row[1]))

    return database_map

def is_daily(database_map):
    """ Finds the high-resolution dbcodes that match to each daily dbcode and outputs a dictionary.

    The daily value is the inner key and the high resolution value is the inner value. The outer key is the dbcode. Ex. MS043:{'02':'12'}
    """
    daily_index = {}

    for each_dbcode in database_map.keys():


        # sort the entities within so that it's faster to find the matches, since they are in similar order.
        for each_entity in sorted(database_map[each_dbcode].keys()):

            # number that we switch to daily
            critical_value = 10

            # if the table is less than 10 or between 21 and 30, it might be daily. Works up to the 81-90 range.
            if int(each_entity) <= critical_value or (int(each_entity) >= critical_value*2+1 and int(each_entity) <=critical_value*3) or (int(each_entity) >= critical_value*4+1 and int(each_entity) <=critical_value*5) or (int(each_entity) >= critical_value*6+1 and int(each_entity) <=critical_value*7) or (int(each_entity) >= critical_value*8+1 and int(each_entity) <=critical_value*9):

                # if at least one value has day in it, test that there is a match between at least one of those keywords containing "day" and some word in the high-resolution column names
                if len([x for x in database_map[each_dbcode][each_entity] if "_DAY" in x]) > 0:

                    keywords = [x.rstrip("_DAY") for x in database_map[each_dbcode][each_entity] if "_DAY" in x]
                    hr_test_entity = str(int(each_entity) + 10)

                    # keywords are the list of possible daily attributes containing the word '_DAY'
                    for each_keyword in keywords:

                        if len([x for x in database_map[each_dbcode][hr_test_entity] if each_keyword in x]) > 0:

                            # if that dbcode is not in the output daily_index, add it to the daily index and map the daily entity to the hr entity; otherwise, update that dbcodes entry with the new pair
                            if each_dbcode not in daily_index:
                                daily_index[each_dbcode] = {each_entity:hr_test_entity}

                            elif each_dbcode in daily_index:
                                if each_entity not in daily_index[each_dbcode]:
                                    daily_index[each_dbcode][each_entity] = hr_test_entity

                                # if an entry has already been included once, don't re-include it because there was another keyword mapping. One is enough to know it worked.
                                elif each_entity in daily_index[each_dbcode]:
                                    continue

            # if the integer is really big,this isn't going to work for you.
            elif int(each_entity) > 91:
                print("Consider reconstructing the `is_daily` function to include entities above 91.")
            else:
                continue

    return daily_index

def flag_count(flag_list):
    """ Quickly count flags in a list, outputing a dictionary and an integer of the count.

    collections.defaultdict initializes a blank dictionary but unlike most dictionaries that throw key errors, if a key isn't found in a default dictionary it is simply added to the dictionary. ex:

            test = defaultdict(int)
            >>> test
            defaultdict(<class 'int'>, {})
            >>> test['p']
            0
            >>> test
            defaultdict(<class 'int'>, {'p': 0})


    The output is like: (<int>, {'x': count(x), 'y': count(y)})
    """
    flag_counter = defaultdict(int)
    for flag in flag_list:
        flag_counter[flag] +=1

    return flag_counter

def daily_flag(flag_counter, critical_value, critical_flag):
    """ Figure out what the daily flag is based on the outputs of the flag counter.

    If the number of E's is more than 0.05 it's an E, otherwise if the number of Q's is greater than 0.05 it's a Q, otherwise if the number of M is more than 0.2, than the value is 'M', otherwise if E and M and Q sum to more than 0.05, it's Q, and in all other cases it's A.

    If the number of flags in the day is much more than the accepted number of flags for the day, just return the acceptable value. This really only happens on CENMET when the 15 goes to 5 as far as I can tell.
    """

    if flag_counter[critical_flag] >= critical_value:
        return critical_flag

    # here's the case where there are too many values, when the 15 and 5 minute datas are merged...
    elif sum(flag_counter[x] for x in flag_counter.keys()) >= critical_value + 3:
        return critical_flag
    elif flag_counter['E']/critical_value > 0.05:
        return 'E'
    elif flag_counter['Q']/critical_value > 0.05:
        return 'Q'
    elif flag_counter['M']/critical_value > 0.2:
        return 'M'
    elif (flag_counter['E'] + flag_counter['Q'] + flag_counter['M'])/critical_value > 0.05:
        return 'Q'
    else:
        return critical_flag

def select_raw_data(cur, database_map, daily_index, hr_methods, daily_methods, dbcode, daily_entity, *args):
    """ Collects the raw data from the database and couples it with information from the method_history and method_history_daily entities in LTERLogger_new.

    `cur` is the database connection
    `database_map` are the columns by dbtable and entity
    `daily_index` is the partnership dictionary containing the high resolution and daily entities that are mapped together.
    `hr_methods` and `daily_methods` contain the information about the method needed to do the subsequent processing
    `dbcode` is the database table, i.e. `HT004`, `MS043`, etc.
    `daily_entity` is the entity you want to map into, i.e. `01`

    `args` are the optional arguments for a start-date (`sd`) and end-date (`ed`). If none are given, the function will run for the previous water year, figuring out when that started based on today (which may not be what you want if it's like October 2nd). If run in automation, these should be supplied by querying the daily table.

    This function will call the process_data function on the SQL server. It will return `raw_data` : a mapping, by day, of all the high resolution data and flags. It will also return a list of column names that can have algorithms for daily aggregation applied to them.
    """

    # if no start and end date are specified - i.e., re-running it outside of daily runs or script
    if not args:

        # For doing the full-year replacements from Adam for the provisional data, we'll need a water year assignment.
        #Otherwise, `args` will contain start dates and end dates for daily updates.
        this_month = datetime.datetime.now().month

        if this_month >= 10:
            wy_previous = datetime.datetime.now().year

        elif this_month < 10:
            wy_previous = datetime.datetime.now().year-1


        sd = datetime.datetime.strftime(datetime.datetime(wy_previous, 10, 1, 0, 0, 0),'%Y-%m-%d %H:%M:%S')
        ed = datetime.datetime.strftime(datetime.datetime(datetime.datetime.now().year, datetime.datetime.now().month, datetime.datetime.now().day, 0, 0, 0),'%Y-%m-%d %H:%M:%S')

    elif args:
        sd = args[0]
        ed = args[1]

    else:
        print("No start date or end date could be determined!")

    # get the entity number of the high resolution entity and the column' names that go with it
    hr_entity = daily_index[dbcode][daily_entity]
    hr_columns = database_map[dbcode][hr_entity]

    # shallow copy of column names from the high resolution - needed to not delete them from the list later
    initial_column_names = database_map[dbcode][hr_entity][:]
    daily_columns = database_map[dbcode][daily_entity]

    # empty dictionary for storing daily outputs - this can fit into the smashed data
    # smashed_template = {k: None for k in daily_columns if 'DB_TABLE' not in k}
    # this is how you create a uniform dictionary from a list.
    smashed_template = {k: None for k in daily_columns}

    # check for max columns (not including flag) in the daily columns
    xval = [x for x in daily_columns if 'MAX' in x and 'FLAG' not in x]

    # Check for maxtime or mintime; remove it from the list of daily columns if present. We don't want to look for it as a separate data attribute.
    if xval != []:
        xtime = [x for x in xval if 'TIME' in x]
        for x in xtime:
            xval.remove(x)

    else:
        xtime = []

    # check for min columns (not including flag) in the daily columns
    nval = [x for x in daily_columns if 'MIN' in x and 'FLAG' not in x]

    # check for mintime; remove it if present. We don't want to look for it as a separate data attribute.
    if nval != []:
        ntime = [x for x in nval if 'TIME' in x]
        for x in ntime:
            nval.remove(x)
    else:
        ntime = []

    # `xt` is a variable to tell SQL if it needs to bring in the high-resolution date_time data or not. You need this to get the maxtime and mintime because the logger doesn't measure these (it should!)
    # Storing and converting high resolution date_time is slow, so don't do it if you don't have to. True means there is at least 1 date_time data needed; false means there is not.
    if xtime != [] or ntime != []:
        xt = True
    else:
        xt = False

    # Gather the raw data by calling the process_data function
    raw_data, column_names = process_data(cur, dbcode, hr_entity, initial_column_names, hr_methods, daily_methods, sd, ed, xt)

    return raw_data, column_names, xt, smashed_template


def process_data(cur, dbcode, hr_entity, initial_column_names, hr_methods, daily_methods, sd, ed, xt):
    """ Bring in the high-resolution data from SQL server.

    The column_names array is initialized to build an ordered query from the server. Flag columns precede the date column which is followed by numerical columns. The first two columns are method and probe code. As columns are selected from a copy of the `initial_column_names` list and moved into `column_names`, they are deleted from `initial_column_names`.

    For VPD, there is a little more specificity. At the very end of the process, we go back and clear out the `column_names` list and then put the vpd-specific prefixes into it.
    """

    column_names = []

    # assign `method` to the first column for the SQL query
    is_method = [x for x in initial_column_names if '_METHOD' in x][0]
    column_names.append(is_method)

    # delete this column containing `method` from the original list
    initial_column_names.remove(is_method)

    # assign `probe` to the second column for the SQL query
    is_probe = [x for x in initial_column_names if 'PROBE' in x][0]
    column_names.append(is_probe)

    # delete this column containing `probe` from the original list
    initial_column_names.remove(is_probe)

    # assign `db_table` to the third column for the SQL query
    is_db_table =[x for x in initial_column_names if 'DB_TABLE' in x][0]
    column_names.append(is_db_table)

    # delete the column containing `db_table` from the original list
    initial_column_names.remove(is_db_table)

    # if the data is VPD, this is going to be much more difficult. Get the names of the air temperature columns and flags.

    is_vpd = [x for x in initial_column_names if "VPD" in x]

    if is_vpd != []:

        local_database_map = get_unique_tables_and_columns(cur)
        airtemp_columns = ["LTERLogger_pro.dbo." + dbcode + "11"+ "." + x for x in local_database_map[dbcode]['11'] if 'AIRTEMP_MEAN' in x]
        relhum_columns = ["LTERLogger_pro.dbo." + dbcode + "12"+ "." + x for x in local_database_map[dbcode]['12'] if 'RELHUM_MEAN' in x]

        initial_column_names += airtemp_columns
        initial_column_names += relhum_columns

    # assign a variable number of flag columns, which need the word 'FLAG' to work, to the SQL query
    contains_flags = [x for x in initial_column_names if 'FLAG' in x]
    column_names += contains_flags

    # remove flags from the original list
    for each_column in contains_flags:
        try:
            initial_column_names.remove(each_column)
        except Exception:
            pass

    # remove informational columns from the original list. Note that we hardcoded in these "worthless" columns. If more are needed, append here. These columns are added into the final output after the method table is linked in so that the height, method, and sitecode are referenced from there.
    columns_worthless = ['DBCODE', 'ENTITY','EVENT_CODE','SITECODE','QC_LEVEL','ID','HEIGHT','DEPTH']

    # remove `worthless` columns from the original list
    for each_column in columns_worthless:
        try:
            initial_column_names.remove(each_column)
        except Exception:
            pass

    # The name of the column in the original data that contains `date` is the date column to use.
    is_the_date = [x for x in initial_column_names if 'DATE' in x][0]

    # Add all the remaining initial columns to the SQL query, following the date column. These SHOULD all be numerical data.
    date_position = len(column_names)

    # basically our sql looks like INFORMATION then FLAGS then DATE then DATA
    # each of these types of inputs has different general functions that can be applied to it.
    # the data index begins one column after the date. Anything with that index or higher is numerical value that could be used in a daily computation.
    data_follows = date_position + 1
    column_names.append(is_the_date)
    initial_column_names.remove(is_the_date)

    if is_vpd == []:
        # column_names are the numerical columns we'll use to decide which functions to employ in aggregation
        column_names += initial_column_names

        # join the column names with a comma for the SQL, needs a string without punctuatin
        column_names_joined = ", ".join(column_names)

        # create and execute the SQL for these columns
        sql = "select " + column_names_joined + " from lterlogger_pro.dbo." + dbcode + hr_entity + " where " + is_the_date + " > \'" + sd + "\' and " + is_the_date + " <= \'" + ed + "\' order by " + is_probe + ", " + is_the_date + " asc"

    elif is_vpd !=[]:

        prefix_names = ["LTERLogger_pro.dbo." + dbcode + hr_entity + "." + x for x in column_names if "AIRTEMP" not in x and "RELHUM" not in x and "DATE_TIME" not in x] + [x for x in column_names if "AIRTEMP" in x or "RELHUM" in x]

        prefix_names.append("LTERLogger_pro.dbo." + dbcode + hr_entity + ".DATE_TIME")

        prefix_other_names = ["LTERLogger_pro.dbo." + dbcode + hr_entity + "." + x for x in initial_column_names if "AIRTEMP" not in x and "RELHUM" not in x] + [x for x in initial_column_names if "AIRTEMP" in x or "RELHUM" in x]

        united_names = prefix_names + prefix_other_names


        joiner = " from LTERLogger_pro.dbo." + dbcode + hr_entity + " left join LTERLogger_pro.dbo." + dbcode + "11 on LTERLogger_pro.dbo." + dbcode + hr_entity + ".DATE_TIME = LTERLogger_pro.dbo." + dbcode + "11.DATE_TIME AND LTERLogger_pro.dbo." + dbcode + hr_entity + ".SITECODE = LTERLogger_pro.dbo." + dbcode + "11.SITECODE AND LTERLogger_pro.dbo." + dbcode + hr_entity + ".HEIGHT = LTERLogger_pro.dbo." + dbcode + "11.HEIGHT inner join LTERLogger_pro.dbo." + dbcode + "12 on LTERLogger_pro.dbo." + dbcode + "11.DATE_TIME = LTERLogger_pro.dbo." + dbcode + "12.DATE_TIME AND LTERLogger_pro.dbo." + dbcode + "11.SITECODE = LTERLogger_pro.dbo." + dbcode + "12.SITECODE AND LTERLogger_pro.dbo." + dbcode + "11.HEIGHT = LTERLogger_pro.dbo." + dbcode + "12.HEIGHT where LTERLogger_pro.dbo." + dbcode + hr_entity + "." +is_the_date + " > \'" + sd + "\' and LTERLogger_pro.dbo." + dbcode + hr_entity + "." + is_the_date + " <= \'" + ed + "\' order by LTERLogger_pro.dbo." + dbcode + hr_entity + "." +is_probe + ", LTERLogger_pro.dbo." + dbcode + hr_entity + "." + is_the_date + " asc"

        sql = "select " + ", ".join(united_names) + joiner

    # EXECUTE THE SQL (works for both the VPD and for the regular stuff)
    cur.execute(sql)

    # raw gathered high-resoluton data populates raw_data; each probe, date, and attribute has within it a list of the daily values and flags.
    raw_data = {}

    for row in cur:

        try:
            # if the date is 10-01-2014 00:05 to 10-02-2014 00:00:05, then these values will lose five minutes to 10-01-2014 00:00:00 and 10-02-2014 00:00:00 and be mapped to the day 10-01-2014.

            # for hourly and 15 minute this trick should still work, because the key value is that value on the very hour, i.e. 24:00:00 or 0:00:00. We just must remember that if `date_time` is needed, adjusted date time needs to be added back 5 minutes.

            adjusted_date_time = datetime.datetime.strptime(str(row[date_position]),'%Y-%m-%d %H:%M:%S') - datetime.timedelta(minutes=5)

            adjusted_date = datetime.datetime(adjusted_date_time.year, adjusted_date_time.month, adjusted_date_time.day)

        except Exception:
            # if there's no data, go on to the next probe
            print("looks like there's no data here")
            continue

        # the probe code is in the second column
        probe_code = str(row[1]).rstrip()

        # the db_table is in the 3rd column
        db_table = str(row[2]).rstrip()

        # adding each row to the raw_data array
        if probe_code not in raw_data:

            # if the probe has not yet been processed, figure out the high-resolution method and therefore also the resolution. If there's only one option, go with that option. If there are multiple options, go with one of those.
            if len(hr_methods[probe_code]) == 1:
                this_method = hr_methods[probe_code][0]

            elif len(hr_methods[probe_code]) > 1:
                #  find the first method that fits where we are within the range of the dates
                this_method = [hr_methods[probe_code][x] for x in sorted(hr_methods[probe_code].keys()) if datetime.datetime.strptime(str(row[data_follows]), '%Y-%m-%d %H:%M:%S') < hr_methods[probe_code][x]['dte'] and datetime.datetime.strptime(str(row[data_follows]), '%Y-%m-%d %H:%M:%S') >= hr_methods[probe_code][x]['dtb']]

            # for the daily, same operation.
            if len(daily_methods[probe_code]) == 1:
                this_daily_method = daily_methods[probe_code][0]

            elif len(daily_methods[probe_code]) > 1:
                #  find the first method that fits where we are within the range of the dates
                this_daily_method = [daily_methods[probe_code][x] for x in sorted(daily_methods[probe_code].keys()) if datetime.datetime.strptime(str(row[data_follows]), '%Y-%m-%d %H:%M:%S') < daily_methods[probe_code][x]['dte'] and datetime.datetime.strptime(str(row[data_follows]), '%Y-%m-%d %H:%M:%S') >= daily_methods[probe_code][x]['dtb']]

            # put the daily method for the method code, but use the high-resolution information for the other data. also, add the db_table.
            raw_data[probe_code]={adjusted_date:{is_method: this_daily_method['method_code'], 'critical_flag': this_method['critical_flag'], 'critical_value':this_method['critical_value'],'height': this_method['height'], 'depth': this_method['depth'], 'sitecode': this_method['sitecode'], 'db_table': db_table}}

            # for debugging only - you can delete this without ill effect. Let's you know which probes may be a problem. Also helps you monitor the input.
            print("KEYS ADDED SO FAR TO DAILY DATA:")
            print(raw_data.keys())

            # put the values in - first the flags, prior to the date, then the values, following it
            if is_vpd != []:
                raw_data[probe_code][adjusted_date].update({cleanse(x):[str(row[3+i])] for i,x in enumerate(united_names[3:date_position])})
                raw_data[probe_code][adjusted_date].update({cleanse(x):[isfloat(row[date_position+1+i])] for i,x in enumerate(united_names[date_position+1:])})
            else:
                raw_data[probe_code][adjusted_date].update({x:[str(row[3+i])] for i,x in enumerate(column_names[3:date_position])})
                raw_data[probe_code][adjusted_date].update({x:[isfloat(row[date_position+1+i])] for i,x in enumerate(column_names[date_position+1:])})

            # if the mintime and maxtime are needed, add in a column called 'date_time' to contain the high-resolution time stamp. Note that this time stamp will be shifted back by five minutes as all the time stamps are, to make it appear on the the right day.
            if xt == True:
                # add in the data for the date stamp for mintime and maxtime
                raw_data[probe_code][adjusted_date].update({'date_time': [adjusted_date_time + datetime.timedelta(minutes = 5)]})
            else:
                pass

        # if the probe_code is already in the daily data, check the date again versus a change in the method tables.
        elif probe_code in raw_data.keys():

            # if the adjusted date has not yet been added, add it.
            if adjusted_date not in raw_data[probe_code].keys():

                # find the method and make sure its the same
                if len(hr_methods[probe_code]) == 1:
                    this_method = hr_methods[probe_code][0]
                elif len(hr_methods[probe_code]) > 1:
                    this_method = [hr_methods[probe_code][x] for x in sorted(hr_methods[probe_code].keys()) if adjusted_date_time < hr_methods[probe_code][x]['dte'] and adjusted_date_time >= hr_methods[probe_code][x]['dtb']]

                if len(daily_methods[probe_code]) == 1:
                    this_daily_method = daily_methods[probe_code][0]
                elif len(daily_methods[probe_code]) > 1:
                    this_daily_method = [daily_methods[probe_code][x] for x in sorted(daily_methods[probe_code].keys()) if datetime.datetime.strptime(str(row[data_follows]), '%Y-%m-%d %H:%M:%S') < daily_methods[probe_code][x]['dte'] and datetime.datetime.strptime(str(row[data_follows]), '%Y-%m-%d %H:%M:%S') >= daily_methods[probe_code][x]['dtb']]

                # update the raw data with information about methods and sources
                raw_data[probe_code].update({adjusted_date:{is_method: this_daily_method['method_code'], 'critical_flag':this_method['critical_flag'], 'critical_value':this_method['critical_value'], 'height': this_method['height'], 'depth': this_method['depth'], 'sitecode':this_method['sitecode'], 'db_table': db_table}})

                # put the values in, flags first, then numerical values
                if is_vpd != []:
                    raw_data[probe_code][adjusted_date].update({cleanse(x):[str(row[3+i])] for i,x in enumerate(united_names[3:date_position])})
                    raw_data[probe_code][adjusted_date].update({cleanse(x):[isfloat(row[date_position+1+i])] for i,x in enumerate(united_names[date_position+1:])})
                else:
                    raw_data[probe_code][adjusted_date].update({x:[str(row[3+i])] for i,x in enumerate(column_names[3:date_position])})
                    raw_data[probe_code][adjusted_date].update({x:[isfloat(row[date_position+1+i])] for i,x in enumerate(column_names[date_position+1:])})

                # add in date-time, if needed for maxtime and mintime
                if xt == True:
                    raw_data[probe_code][adjusted_date].update({'date_time': [adjusted_date_time + datetime.timedelta(minutes=5)]})
                else:
                    pass

            # if just appending to the same row of data a new high-resolution value, the previous information regarding the daily method is the same, so there is no need to get it again.
            elif adjusted_date in raw_data[probe_code].keys():

                if is_vpd != []:

                    for i, x in enumerate(united_names[3:date_position]):
                        raw_data[probe_code][adjusted_date][cleanse(x)].append(str(row[3+i]))

                    for i, x in enumerate(united_names[date_position+1:]):
                        raw_data[probe_code][adjusted_date][cleanse(x)].append(str(row[date_position+1+i]))
                else:
                    for i, x in enumerate(column_names[3:date_position]):
                        raw_data[probe_code][adjusted_date][x].append(str(row[3+i]))

                    for i, x in enumerate(column_names[date_position+1:]):
                        raw_data[probe_code][adjusted_date][x].append(str(row[date_position+1+i]))

                if xt == True:
                    raw_data[probe_code][adjusted_date]['date_time'].append(adjusted_date_time + datetime.timedelta(minutes=5))
                else:
                    pass

    # give back the 'column names' variable from VPD. Strip the database prefixes from column names
    if is_vpd != []:

        vpd_column_names = [x.lstrip('LTERLogger_pro.dbo.MS0431').lstrip('2.').lstrip('8.').lstrip('1.') for x in united_names]
        column_names = []
        column_names = vpd_column_names
        return raw_data, column_names
    else:
        return raw_data, column_names

def cleanse(x):
    """ Remove LTERLogger_pro prefixes from column names
    """
    return x.lstrip('LTERLogger_pro.dbo.MS0431').lstrip('2.').lstrip('8.').lstrip('1.')

def calculate_daily_flags(raw_data, column_names, temporary_smash, smashed_template):
    """ Daily flags computed with flag_count function.

    `flag_count` function creates a defaultdict. we find the columns in the high-resolution (raw) data that have flags and compute flags for them by counting the number of each unique key during that day.
    """
    temporary_flags ={}

    # what are the flags in the day we will need?
    flags_in_day = [x for x in smashed_template.keys() if "FLAG" in x]

    # find the columns that contain flags and remove the FLAG_PRO column
    flag_columns = column_names[3:column_names.index('DATE_TIME')]

    # if there's some daily flag that doesn't have a high resolution partner
    if flags_in_day != flag_columns:
        # add it to that day
        for each_flag in flags_in_day:
            if each_flag in flag_columns:
                continue
            elif each_flag not in flag_columns:
                flag_columns.append(each_flag)

    # remove provisional columns and saturated vapor pressure (not used in daily)
    for x in flag_columns:
        if 'FLAG_PRO' in x or 'SATVP' in x:
            flag_columns.remove(x)
        else:
            pass

    valid_columns = flag_columns[:]

    for each_flag in valid_columns:
        # note that at this point, the 'min flags' and 'max flags' may appear to be 'missing' even if a value is there.
        try:
            data_flags = {each_probe:{dt: daily_flag(flag_count(raw_data[each_probe][dt][each_flag]), raw_data[each_probe][dt]['critical_value'], raw_data[each_probe][dt]['critical_flag']) for dt in raw_data[each_probe].keys()} for each_probe in raw_data.keys()}

            temporary_flags.update({each_flag:data_flags})

        except KeyError:

            # because VAP_MAX_FLAG not indicated in MS04318.
            try:
                # there's a daily max or min, but no five minute max or min, and no indication we'd measure it.
                if 'MAX' in each_flag or 'MIN' in each_flag:
                    # remove '_flag' and put in '_day', check data columns for this. try to find a '_mean_flag' as a replacement.
                    converted_name = each_flag.rstrip('_FLAG') + '_DAY'

                    if converted_name in temporary_smash.keys() and each_flag.split('_')[0]+"_MEAN_FLAG" in flag_columns:

                        data_flags = {each_probe:{dt: daily_flag(flag_count(raw_data[each_probe][dt][each_flag.split('_')[0]+"_MEAN_FLAG"]), raw_data[each_probe][dt]['critical_value'], raw_data[each_probe][dt]['critical_flag']) for dt in raw_data[each_probe].keys()} for each_probe in raw_data.keys()}

                        temporary_flags.update({each_flag: data_flags})

            except Exception:
                import pdb; pdb.set_trace()

    return temporary_flags

def comprehend_daily(smashed_template, raw_data, column_names, xt):
    """ Aggregates the raw data based on column names. Performs basic sums, means, etc. by calling functions in the file `if_none.py`

    """

    # Creates MINTIME or MAXTIME attribute for your specific data based on the function that gets called from the is_none.py file.
    time_attribute = lambda func: func.__name__.split('_')[1].upper() +"_"+ func.__name__.split('_')[0].upper() + "TIME"

    # Turns any regular attribute to a "_DAY" name
    day_attribute = lambda name: name.upper() + "_DAY"

    # Store temporary output, scopes to the namespace of comprehend daily.
    temporary_smash = {}

    # Data is in the columns following the datetime; data_columns is the names for these columns
    data_columns = column_names[column_names.index('DATE_TIME')+1:]

    # Copy of the column names. We will work over these names, removing things that don't do the mean, doing their functions and updating temporary_smash with them, and finally, do all the means.
    valid_columns = data_columns[:]

    # Find if there is a maximum column or more in the high resolution. This doesn't mean that you don't compute a max if no present, but that there is one explicitly present.
    is_max = sorted([x for x in data_columns if 'MAX' in x])

    for each_column in is_max:
        valid_columns.remove(each_column)

    # Find out if there is a minimum column in the high resolution. You still may have to min even if there isn't one, but this tells if one is explicitly present.
    is_min = sorted([x for x in data_columns if 'MIN' in x])

    for each_column in is_min:
        valid_columns.remove(each_column)

    # if it contains propellor anemometer it should say 'PRO' - speed is regular and that's okay because we can use the maxtime functions more easily there
    is_windpro = [x for x in data_columns if 'PRO' in x and 'SPD' not in x]

    for each_column in is_windpro:
        valid_columns.remove(each_column)

    if is_windpro != []:

        dir_name = [x for x in is_windpro if 'DIR' in x and 'STDDEV' not in x][0]
        mag_name = [x for x in is_windpro if 'MAG' in x][0]
        dir_dev_name = [x for x in is_windpro if 'STDDEV' in x][0]

        mean_direction = daily_functions_speed_dir(raw_data, is_windpro, valid_columns, wind_dir_if_none, output_name="DIR")
        mean_magnitude = daily_functions_speed_dir(raw_data, is_windpro, valid_columns, wind_mag_if_none, output_name="MAG")
        mean_stddev, _ = daily_functions_normal(raw_data, dir_name, wind_std_if_none, xt)
        temporary_smash.update({day_attribute(dir_name) : mean_direction})
        temporary_smash.update({day_attribute(mag_name) : mean_magnitude})
        temporary_smash.update({day_attribute(dir_dev_name) : mean_stddev})

    # if it contains sonic, it should say 'SNC' -- remove all but max
    is_windsnc = [x for x in data_columns if 'SNC' in x and 'SPD' not in x]

    for each_column in is_windsnc:
        valid_columns.remove(each_column)

    if is_windsnc != []:
        # regular mean
        columns_for_a_regular_mean = [x for x in is_windsnc if 'DIR' not in x and 'STDDEV' not in x and 'MAX' not in x]
        # mean for all the mean stuff
        for each_column in columns_for_a_regular_mean:
            mean_data_from_mean_snc, _ = daily_functions_normal(raw_data, each_column, mean_if_none, xt)
            temporary_smash.update({day_attribute(each_column): mean_data_from_mean_snc})

        columns_for_deviation = [x for x in is_windsnc if 'STDDEV' in x and 'DIR' not in x]
        columns_for_dir_deviation = [x for x in is_windsnc if 'DIR' in x and 'STDDEV' not in x]

        for each_column in columns_for_deviation:
            wind_std_others, _ = daily_functions_normal(raw_data, each_column, regular_std_if_none, xt)
            temporary_smash.update({'WDIR_SNC_STDDEV_DAY': wind_std_others})

        for each_column in columns_for_dir_deviation:

            wind_std_snc, _ = daily_functions_normal(raw_data, each_column, wind_std_if_none, xt)
            temporary_smash.update({day_attribute(each_column): wind_std_snc})
            mean_direction = daily_functions_speed_dir_snc(raw_data, 'WSPD_SNC_MEAN', each_column, wind_dir_if_none)

            temporary_smash.update({day_attribute(each_column): mean_direction})
            #mean_stddev, _ = daily_functions_normal(raw_data, each_column, wind_std_if_none, xt)
            #temporary_smash.update({day_attribute(each_column): mean_stddev})

    # if it says 'TOT' its a total
    is_tot = [x for x in data_columns if 'TOT' in x and 'MEAN' not in x]

    for each_column in is_tot:
        valid_columns.remove(each_column)

    # if it says "INST" its instantaneous
    is_inst = [x for x in data_columns if 'INST' in x]

    for each_column in is_inst:
        valid_columns.remove(each_column)


    # for the VPD --> (the daily aggregation functions are in `daily_functions.py`)
    do_vpd = [x for x in data_columns if 'VPD' in x]
    do_vap = [x for x in data_columns if 'VAP' in x]

    if do_vpd != []:

        # mean vapor pressure - from the mean
        mean_vap_data_from_mean, _ = daily_functions_vpd(raw_data, do_vap, valid_columns, vap_if_none, xt)
        temporary_smash.update({'VAP_MEAN_DAY':mean_vap_data_from_mean})

        # max vapor pressure - from the mean
        max_vap_data_from_mean, maxtime_vap_from_mean = daily_functions_vpd(raw_data, do_vap, valid_columns, max_vap_if_none, xt)
        temporary_smash.update({'VAP_MAX_DAY': max_vap_data_from_mean})
        time_name = time_attribute(max_vap_if_none)
        temporary_smash.update({time_name : maxtime_vap_from_mean})

        # min vapor pressure - from the mean
        min_vap_data_from_mean, mintime_vap_from_mean = daily_functions_vpd(raw_data, do_vap, valid_columns, min_vap_if_none, xt)
        temporary_smash.update({'VAP_MIN_DAY': min_vap_data_from_mean})
        time_name = time_attribute(min_vap_if_none)
        temporary_smash.update({time_name : mintime_vap_from_mean})

        # mean vpd - from the mean
        mean_vpd_data_from_mean, _ = daily_functions_vpd(raw_data, do_vpd, valid_columns, vpd_if_none, xt)
        temporary_smash.update({'VPD_MEAN_DAY':mean_vpd_data_from_mean})

        # max vpd - from the mean
        max_vpd_data_from_mean, maxtime_vpd_from_mean = daily_functions_vpd(raw_data, do_vpd, valid_columns, max_vpd_if_none, xt)
        temporary_smash.update({'VPD_MAX_DAY': max_vpd_data_from_mean})
        time_name = time_attribute(max_vpd_if_none)
        temporary_smash.update({time_name : maxtime_vpd_from_mean})

        # min vpd - from the mean
        min_vpd_data_from_mean, mintime_vpd_from_mean = daily_functions_vpd(raw_data, do_vpd, valid_columns, min_vpd_if_none, xt)
        temporary_smash.update({'VPD_MIN_DAY': min_vpd_data_from_mean})
        time_name = time_attribute(min_vpd_if_none)
        temporary_smash.update({time_name : mintime_vpd_from_mean})


    # Here the re-aggregation begins for means, mins, and maxes...

    # if the mean isn't empty, mean from mean - will still show all the decimals
    if do_vpd == [] and valid_columns != []:

        # normal data
        for each_column in valid_columns:
            mean_data_from_mean, _ = daily_functions_normal(raw_data, each_column, mean_if_none, xt)
            temporary_smash.update({day_attribute(each_column) : mean_data_from_mean})

            max_name = each_column.split("_")[0] + "_MAX_DAY"
            min_name = each_column.split("_")[0] + "_MIN_DAY"

            if is_windpro != []:
                max_name = each_column.split("_")[0] + "_PRO_MAX_DAY"
                min_name = each_column.split("_")[0] + "_PRO_MIN_DAY"

            if is_windsnc != []:
                max_name = each_column.split("_")[0] + "_SNC_MAX_DAY"
                min_name = each_column.split("_")[0] + "_SNC_MIN_DAY"

            # maximums from mean
            if max_name in smashed_template.keys() and xt == True:
                max_time_name = max_name.rstrip("_DAY") + "TIME"

                max_data_from_mean, maxtime_from_mean = daily_functions_normal(raw_data, each_column, max_if_none, xt)
                temporary_smash.update({max_name: max_data_from_mean})
                temporary_smash.update({max_time_name: maxtime_from_mean})

            elif max_name in smashed_template.keys() and xt != True:

                max_data_from_mean,_ = daily_functions_normal(raw_data, each_column, max_if_none, xt)
                temporary_smash.update({max_name: max_data_from_mean})

            else:
                pass

            # minimums from mean
            if min_name in smashed_template.keys() and xt == True:
                min_time_name = min_name.rstrip("_DAY") + "TIME"

                min_data_from_mean, mintime_from_mean = daily_functions_normal(raw_data, each_column, min_if_none, xt)
                temporary_smash.update({min_name: min_data_from_mean})
                temporary_smash.update({min_time_name: mintime_from_mean})

            elif min_name in smashed_template.keys() and xt != True:

                min_data_from_mean, _ = daily_functions_normal(raw_data, each_column, min_if_none, xt)
                temporary_smash.update({min_name: min_data_from_mean})

            else:
                pass

    # if max from the high-resolution data isn't empty, compute max and possibly maxtime
    if is_max != []:

        for each_max_column in is_max:
            max_data_from_max, maxtime_from_max = daily_functions_normal(raw_data, each_max_column, max_if_none, xt)

            old_column = each_max_column + "_DAY"

            if old_column in temporary_smash.keys():
                #new_column = each_max_column + "DAY_2"
                #temporary_smash.update({new_column: max_data_from_max})

                my_probes = sorted(list(max_data_from_max.keys()))
                for each_probe in my_probes:
                    my_dates = []
                    my_dates = sorted(list(max_data_from_max[each_probe].keys()))
                    for each_date in my_dates:
                        if str(max_data_from_max[each_probe][each_date]) != 'None':
                            temporary_smash[old_column][each_probe][each_date] = max_data_from_max[each_probe][each_date]
                        else:
                            pass

            elif each_max_column not in temporary_smash.keys():
                temporary_smash.update({each_max_column + "_DAY": max_data_from_max})

            # name the time attribute
            old_column = each_max_column + "TIME"

            if old_column in temporary_smash.keys() and xt ==True:
                #new_column = each_max_column + "TIME_2"
                #temporary_smash.update({new_column: maxtime_from_max})

                my_probes = []
                my_probes = sorted(list(maxtime_from_max.keys()))
                for each_probe in my_probes:
                    my_dates = []
                    my_dates = sorted(list(maxtime_from_max[each_probe].keys()))
                    for each_date in my_dates:
                        if str(max_data_from_max[each_probe][each_date]) != 'None':
                            temporary_smash[old_column][each_probe][each_date] = maxtime_from_max[each_probe][each_date]
                        else:
                            pass

            elif each_max_column not in temporary_smash.keys() and xt == True:
                temporary_smash.update({each_max_column + "TIME": max_time_from_max})

            else:
                pass

    # if min from the high-resolution data isn't empty, compute min and possibly min time
    if is_min != []:

        for each_min_column in is_min:

            min_data_from_min, mintime_from_min = daily_functions_normal(raw_data, each_min_column, min_if_none, xt)

            old_column = each_min_column + "_DAY"

            if old_column in temporary_smash.keys():
                #new_column = each_min_column + "DAY_2"
                #temporary_smash.update({new_column: min_data_from_min})

                my_probes = sorted(list(min_data_from_min.keys()))
                for each_probe in my_probes:
                    my_dates = []
                    my_dates = sorted(list(min_data_from_min[each_probe].keys()))
                    for each_date in my_dates:
                        if str(min_data_from_min[each_probe][each_date]) != 'None':
                            temporary_smash[old_column][each_probe][each_date] = min_data_from_min[each_probe][each_date]
                        else:
                            pass

            elif each_min_column not in temporary_smash.keys():
                temporary_smash.update({each_min_column + "_DAY": min_data_from_min})

            # name the time attribute
            old_column = each_min_column + "TIME"

            if old_column in temporary_smash.keys() and xt ==True:
                #new_column = each_min_column + "TIME_2"
                #temporary_smash.update({new_column: mintime_from_min})

                my_probes = []
                my_probes = sorted(list(mintime_from_min.keys()))
                for each_probe in my_probes:
                    my_dates = []
                    my_dates = sorted(list(mintime_from_min[each_probe].keys()))
                    for each_date in my_dates:
                        if str(min_data_from_min[each_probe][each_date]) != 'None':
                            temporary_smash[old_column][each_probe][each_date] = mintime_from_min[each_probe][each_date]
                        else:
                            pass

            elif each_min_column not in temporary_smash.keys() and xt == True:
                temporary_smash.update({each_min_column + "TIME": min_time_from_min})

            else:
                pass

    # totals
    if is_tot != []:
        for each_total_col in is_tot:
            tot, _ = daily_functions_normal(raw_data, each_total_col, sum_if_none, xt)
            temporary_smash.update({day_attribute(each_total_col) : tot})

    return temporary_smash

def unite_data(temporary_smash, temporary_flags):
    """
    Simple : takes temporary smash, updates it with temporary flags, makes an output array
    """

    smashed_data = {}
    smashed_data.update(temporary_smash)
    smashed_data.update(temporary_flags)
    return smashed_data

def fix_max_min(smashed_data, raw_data, prefix="MAX"):
    """ If there is a value for the maximum data or minimum data but the flag says missing then change the flag to be whatever the accepted flag for the day is. """

    max_attrs = [x for x in sorted(list(smashed_data.keys())) if "_" + prefix + "_DAY" in x and "FLAG" not in x]

    if max_attrs == []:
        return smashed_data

    else:
        for mx in max_attrs:
            my_flag = mx.rstrip("_DAY") + "_FLAG"
            test_flag_mean = mx.rstrip("_" + prefix + "_DAY") + "_MEAN_FLAG"
            test_flag_tot = mx.rstrip("_" + prefix + "_DAY") + "_TOT_FLAG"
            test_flag_inst = mx.rstrip("_" + prefix + "_DAY") + "_INST_FLAG"
            if test_flag_mean in sorted(list(smashed_data.keys())):
                supplement_flag = test_flag_mean
            elif test_flag_tot in sorted(list(smashed_data.keys())):
                supplement_flag = test_flag_tot
            elif test_flag_inst in sorted(list(smashed_data.keys())):
                supplement_flag = test_flag_inst
            else:
                return smashed_data

            my_probes = []
            my_probes = sorted(list(smashed_data[mx].keys()))

            for each_probe in my_probes:
                my_dates = []
                my_dates = sorted(list(smashed_data[mx][each_probe].keys()))

                for each_date in my_dates:
                    # if the day is not None but the data indicates an 'M', then say the day is whatever the actual day is
                    if str(smashed_data[mx][each_probe][each_date]) != 'None' and str(smashed_data[my_flag][each_probe][each_date]) == 'M':
                        try:
                            smashed_data[my_flag][each_probe][each_date] = smashed_data[supplement_flag][each_probe][each_date]
                        except Exception:
                            pass

                    else:
                        pass

    return smashed_data

def clean_up_data(output_dictionary, xt):
    """ If the data is `present` because it has been calculated but in fact it should be none because it is missing, set that data to None
    """
    for each_probe in sorted(list(output_dictionary.keys())):
        for each_date in sorted(list(output_dictionary[each_probe].keys())):

            this_list = sorted(list(output_dictionary[each_probe][each_date].keys()))

            # stupid vpd bug - won't output a null
            if 'VPD' in each_probe and 'VPD_MINTIME' not in this_list:
                output_dictionary[each_probe][each_date]['VPD_MINTIME'] = 'None'
            else:
                pass

            if 'VPD' in each_probe and 'VPD_MAXTIME' not in this_list:
                output_dictionary[each_probe][each_date]['VPD_MAXTIME'] = 'None'
            else:
                pass


            for each_attribute in this_list:
                if 'MAXTIME' in each_attribute or 'MINTIME' in each_attribute:
                    try:
                        this_value = output_dictionary[each_probe][each_date][each_attribute]
                        new_value = (str(this_value.hour) + str(this_value.minute)).zfill(4)
                        output_dictionary[each_probe][each_date][each_attribute] = new_value

                    except Exception:
                        # if you can't make a character of the datetime, you will need to just set it to none
                        output_dictionary[each_probe][each_date][each_attribute] = 'None'

    return output_dictionary

def windrose_fix(output_dictionary):
    """ If there are windrose flags in the data, set them to 'M' """

    # check for wind rose columns and return if there aren't any
    rose_flag_columns = [x for x in sorted(list(smashed_template.keys())) if "_FLAG" in x and "ROSE" in x]

    for each_probe in sorted(list(output_dictionary.keys())):
        for dt in sorted(list(output_dictionary[each_probe].keys())):

            for each_column in rose_flag_columns:
                data_column = each_column.rstrip("_FLAG") + "_DAY"
                if each_column not in output_dictionary[each_probe][dt]:
                    output_dictionary[each_probe][dt][each_column] = 'M'
                    output_dictionary[each_probe][dt][data_column] = None
                elif each_column in output_dictionary[each_probe][dt]:
                    pass

    return output_dictionary

def create_outs(raw_data, smashed_template, smashed_data, dbcode, daily_entity,xt):
    """ Generate appropriate output structures - first get the data together and then get the information with it.
    """
    output_dictionary = {}

    smashed_data = fix_max_min(smashed_data, raw_data, prefix="MAX")
    smashed_data = fix_max_min(smashed_data, raw_data, prefix="MIN")

    missing_attributes = [x for x in list(smashed_template.keys()) if x not in list(smashed_data.keys())]

    missing_attributes.remove('ID')

    additional_attributes = [x for x in smashed_data.keys() if x not in smashed_template.keys()]

    # remove from the list of attributes the ones which shouldn't be in the output.
    attribute_list = sorted(list(smashed_data.keys()))
    if additional_attributes != []:
        for x in additional_attributes:
            attribute_list.remove(x)

    # First get all the attributes from the data
    for each_attribute in attribute_list:

        list_of_probes = []
        list_of_probes = sorted(list(smashed_data[each_attribute].keys()))

        for each_probe in list_of_probes:
            list_of_dates = sorted(list(smashed_data[each_attribute][each_probe].keys()))

            for each_date in list_of_dates:
                this_value = smashed_data[each_attribute][each_probe][each_date]

                if each_probe not in output_dictionary:
                    output_dictionary[each_probe] = {each_date: {each_attribute: this_value}}

                elif each_probe in output_dictionary.keys():

                    if each_date not in output_dictionary[each_probe]:
                        output_dictionary[each_probe][each_date] = {each_attribute: this_value}
                    elif each_date in output_dictionary[each_probe]:

                        if each_attribute not in output_dictionary[each_probe][each_date]:
                            output_dictionary[each_probe][each_date][each_attribute] = this_value

                        elif each_attribute in output_dictionary[each_probe][each_date]:
                            pass

    # a function that turns the date times from the min and max to characters
    output_dictionary = clean_up_data(output_dictionary, xt)


    if daily_entity == '04':
        output_dictionary = windrose_fix(output_dictionary)

    is_probe = [x for x in smashed_template.keys() if 'PROBE' in x][0]
    is_method = [x for x in smashed_template.keys() if 'METHOD' in x][0]

    for each_probe in sorted(list(output_dictionary.keys())):
        for each_date in sorted(list(output_dictionary[each_probe].keys())):

            if 'DBCODE' not in output_dictionary[each_probe][each_date]:
                output_dictionary[each_probe][each_date]['DBCODE'] = dbcode
            else:
                pass

            if is_probe not in output_dictionary[each_probe][each_date]:
                output_dictionary[each_probe][each_date][is_probe] = each_probe
            else:
                pass

            if 'ENTITY' not in output_dictionary[each_probe][each_date]:
                output_dictionary[each_probe][each_date]['ENTITY'] = daily_entity
            else:
                pass

            if 'SITECODE' not in output_dictionary[each_probe][each_date]:
                output_dictionary[each_probe][each_date]['SITECODE'] = raw_data[each_probe][each_date]['sitecode']
            else:
                pass

            if 'DATE' not in output_dictionary[each_probe][each_date]:
                output_dictionary[each_probe][each_date]['DATE'] = datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S')
            else:
                pass

            if 'EVENT_CODE' not in output_dictionary[each_probe][each_date]:
                output_dictionary[each_probe][each_date]['EVENT_CODE'] = 'NA'
            else:
                pass

            if 'DB_TABLE' not in output_dictionary[each_probe][each_date]:
                output_dictionary[each_probe][each_date]['DB_TABLE'] = raw_data[each_probe][each_date]['db_table']
            else:
                pass

            if 'QC_LEVEL' not in output_dictionary[each_probe][each_date]:
                output_dictionary[each_probe][each_date]['QC_LEVEL'] = '1P'
            else:
                pass

            if is_method not in output_dictionary[each_probe][each_date]:
                output_dictionary[each_probe][each_date][is_method] = raw_data[each_probe][each_date][is_method]
            else:
                pass

            if 'HEIGHT' in missing_attributes:
                output_dictionary[each_probe][each_date]['HEIGHT'] = raw_data[each_probe][each_date]['height']
            elif 'DEPTH' in missing_attributes:
                output_dictionary[each_probe][each_date]['DEPTH'] = raw_data[each_probe][each_date]['depth']


    return output_dictionary


def get_methods_for_all_probes(cur, *sd):
    """ Creates hr_methods and daily_methods dictionaries to reference when importing raw data.

    The method tables tell us which flags to use for various resolutions. The `critical_value` refers to the number of flags needed for a complete day. The `critical_flag` refers to the flag that a complete day should assign. `dtb` is datetime begin. `dte` is datetime end. If a method isn't present in the database, the default `xxxxxx` syntax is used for that method.
    """

    # defaults for if a method is not present in the method history tables
    hr_methods = defaultdict(lambda: {0:{'sitecode':'XXXXXX', 'method_code':'XXXXXX','dtb': datetime.datetime(9,9,9,9,9,9), 'dte': datetime.datetime(9999,9,9,9,9,9), 'height':9, 'depth':0, 'resolution': 'None', 'critical_flag':'A', 'critical_value': 287}})
    daily_methods = defaultdict(lambda: {0:{'sitecode':'XXXXXX', 'method_code':'XXXXXX','dtb': datetime.datetime(9,9,9,9,9,9), 'dte': datetime.datetime(9999,9,9,9,9,9), 'height':9, 'depth':0}})

    # limit the amount of history to get - only to a water year or so back.
    this_month = datetime.datetime.now().month

    if this_month >= 10:
        wy_previous = datetime.datetime.now().year

    elif this_month < 10:
        wy_previous = datetime.datetime.now().year-1

    # if no start date is given for gathering methods, start at the last wateryear.
    if not sd:
        sd = datetime.datetime.strftime(datetime.datetime(wy_previous, 10, 1, 0, 0, 0),'%Y-%m-%d %H:%M:%S')

    sql_hr = "select sitecode, probe_code, date_time_bgn, date_time_end, method_code, finest_res, height, depth from lterlogger_new.dbo.method_history where date_time_end >= \'" + sd + "\' order by probe_code, date_time_bgn"

    sql_daily = "select sitecode, probe_code, date_bgn, date_end, method_code, height, depth from lterlogger_new.dbo.method_history_daily where date_end >= \'" + sd + "\' order by probe_code, date_bgn"

    cur.execute(sql_hr)

    for row in cur:

        # which flags and values to use for QC are based on the `finest res` attribute in the method_history table. For specific cases like wind, this will be changed outside of this function. The critical value is 1 less than the total number of measurements in the day, because I've noticed on file merges often there is a missing midnight
        if 'daily' in str(row[5]).rstrip():
            print("daily data in the hr table! -" + str(row[1]).rstrip() + " " + str(row[4]).rstrip())
            continue
        elif '15' in str(row[5]).rstrip():
            critical_flag = 'F'
            critical_value = 95
        elif '60' in str(row[5]).rstrip():
            critical_flag = 'H'
            critical_value = 23
        else:
            critical_flag = 'A'
            critical_value = 287

        if str(row[1]).rstrip() not in hr_methods.keys():
            # sometimes there is more than one method, so the counter is used to index them in order of their starting dates
            counter = 0

            hr_methods[str(row[1]).rstrip()] = {counter: {'sitecode': str(row[0]).rstrip(), 'dtb': datetime.datetime.strptime(str(row[2]), '%Y-%m-%d %H:%M:%S'), 'dte': datetime.datetime.strptime(str(row[3]),'%Y-%m-%d %H:%M:%S'), 'method_code' : str(row[4]).rstrip(), 'resolution': str(row[5]).rstrip(), 'height': str(row[6]).rstrip(), 'depth': str(row[7]).rstrip(), 'critical_flag': critical_flag, 'critical_value': critical_value}}

            counter += 1

        elif str(row[1]).rstrip() in hr_methods.keys():
            if counter not in hr_methods[str(row[1]).rstrip()]:
                hr_methods[str(row[1])][datetime.datetime.strptime(str(row[2]),'%Y-%m-%d %H:%M:%S')] = {'sitecode': str(row[0]).rstrip(), 'dtb': datetime.datetime.strptime(str(row[2]), '%Y-%m-%d %H:%M:%S'), 'dte': datetime.datetime.strptime(str(row[3]),'%Y-%m-%d %H:%M:%S'), 'method_code' : str(row[4]).rstrip(), 'resolution': str(row[5]).rstrip(), 'height': str(row[6]).rstrip(), 'depth': str(row[7]).rstrip(), 'critical_flag': critical_flag, 'critical_value': critical_value}
                counter += 1
            else:
                print("error in bringing in high_resolution methods")

    cur.execute(sql_daily)

    for row in cur:
        if str(row[1]).rstrip() not in daily_methods:
            counter = 0
            daily_methods[str(row[1]).rstrip()] = {counter: {'sitecode': str(row[0]).rstrip(), 'dtb': datetime.datetime.strptime(str(row[2]), '%Y-%m-%d %H:%M:%S'), 'dte': datetime.datetime.strptime(str(row[3]),'%Y-%m-%d %H:%M:%S'), 'method_code' : str(row[4]).rstrip(), 'height': str(row[5]).rstrip(), 'depth': str(row[6]).rstrip()}}
            counter += 1
        elif str(row[1]).rstrip() in daily_methods:
            if counter not in daily_methods[str(row[1]).rstrip()]:
                daily_methods[str(row[1])][datetime.datetime.strptime(str(row[2]),'%Y-%m-%d %H:%M:%S')] = {'sitecode': str(row[0]).rstrip(), 'dtb': datetime.datetime.strptime(str(row[2]), '%Y-%m-%d %H:%M:%S'), 'dte': datetime.datetime.strptime(str(row[3]),'%Y-%m-%d %H:%M:%S'), 'method_code' : str(row[4]).rstrip(), 'height': str(row[5]).rstrip(), 'depth': str(row[6]).rstrip()}
                counter += 1
            else:
                print("error in bringing in daily methods")

    return hr_methods, daily_methods

def insert_data(cur, output_dictionary, daily_index, dbcode, daily_entity, smashed_template):
    """ Create a SQL statement for inserting your data back into the database.

    The `var_types` variable uses the meta-information about the variable to figure out what type to assign it in the database.
    """

    tuple_list = []

    for each_probe in sorted(list(output_dictionary.keys())):
        for each_date in sorted(list(output_dictionary[each_probe].keys())):

            sorted_keys = sorted(list(output_dictionary[each_probe][each_date].keys()))

            tuple_data = [output_dictionary[each_probe][each_date][x] for x in sorted_keys]
            #tuple_data = [output_dictionary[each_probe][each_date].get(x,'None') for x in sorted_keys]

            # this works for ms00501 ....
            converter = lambda x: '%s' if x =='str' else '%d'
            #var_types = [converter(x.__class__.__name__) for x in tuple_data]

            tuple_list.append(tuple(tuple_data))
            #print("insert into LTERLogger_Pro.dbo." + dbcode + daily_entity + " (" + ", ".join(sorted_keys) +") VALUES (" + ", ".join(var_types) + ")")

            #print("insert into LTERLogger_Pro.dbo." + dbcode + daily_entity + " (" + ", ".join(sorted_keys) +") VALUES (" + ", ".join(var_types) + ")")

    var_types = [converter(x.__class__.__name__) for x in tuple_list[0]]

    cur.executemany("insert into LTERLogger_Pro.dbo." + dbcode + daily_entity + " (" + ", ".join(sorted_keys) +") VALUES (" + ", ".join(var_types) + ")", tuple_list)


    print("check the sql!")

    conn.commit()

def detect_recent_data(cur, dbcode, daily_entity, daily_index):
    """ Detect the most recent dt in the daily data before updating. Does not return a start date if there's not one in there.
    """

    try:
        sql= "select top 1 date from lterlogger_pro.dbo." + dbcode + daily_entity + " order by date desc"
        cur.execute(sql)
        last_date = cur.fetchone()

        if last_date == None:
            print("no data has been entered yet, get full range of high-resolution data")
            hr_entity = daily_index[dbcode][daily_entity]
            sql= "select top 1 date_time from lterlogger_pro.dbo." + dbcode + hr_entity + " order by date_time asc"
            cur.execute(sql)
            last_date = cur.fetchone()

        desired_start_date = datetime.datetime.strftime(last_date[0] + datetime.timedelta(days=1),'%Y-%m-%d %H:%M:%S')

        today = datetime.datetime(datetime.datetime.now().year, datetime.datetime.now().month, datetime.datetime.now().day, 0, 0, 0)
        desired_end_day = datetime.datetime.strftime(today, '%Y-%m-%d %H:%M:%S')

        return desired_start_date, desired_end_day

    except Exception:
        print("no data found, beginning with water year")
        return "",""


if __name__ == "__main__":


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
    desired_database = 'MS005'
    desired_daily_entity = '01'
    desired_start_day = '2015-01-01 00:00:00'
    desired_end_day = '2015-07-10 00:00:00'

    if desired_daily_entity == '06' and desired_database == 'MS043':
        print("soil moisture potential - 06 and 16 - is not valid currently")

    # raw_data is the data as {`PROBE CODE`:{`date`:{`attribute`:[value1, value2, value3, etc.]}}}
    # column names are the columns of high resolution data
    # xt is an indicator for if the maxtime or mintime needs to be computed
    # smashed template is the basic daily structure to populate
    raw_data, column_names, xt, smashed_template = select_raw_data(cur, database_map, daily_index, hr_methods, daily_methods, desired_database, desired_daily_entity, desired_start_day, desired_end_day)

    import pdb; pdb.set_trace()
    # perform daily calculations -- temporary smash is created within the function and then populated with each column that we can populate it with -- it is grouped by attribute, like {`AIRTEMP_MEAN_DAY`:{`date`:{`PROBE_CODE`: value}}}
    temporary_smash = comprehend_daily(smashed_template, raw_data, column_names, xt)

    # calculate daily flags by creating 'flag_counters' which indicate the number of unique flags. It is organized by attribute, like {`AIRTEMP_MEAN_FLAG`:{`date`:{`PROBE_CODE`: flag}}}
    temporary_flags = calculate_daily_flags(raw_data, column_names, temporary_smash, smashed_template)

    # creates output structure containing both the data and the flags
    smashed_data = unite_data(temporary_smash, temporary_flags)

    # reorganizes the output structure from being organized by attribute, to being organized by probe. cleans up little things like VPD MINTIME, WINDROSE, converting "M" to "A" for when the max column could be found from the mean, etc.
    output_dictionary = create_outs(raw_data, smashed_template, smashed_data, desired_database, desired_daily_entity, xt)

    # Insertion into SQL server.
    insert_data(cur, output_dictionary, daily_index, desired_database, desired_daily_entity, smashed_template)

    print("the end")