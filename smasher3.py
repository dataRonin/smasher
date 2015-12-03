#/anaconda/bin/python
import pymssql
import datetime
from collections import defaultdict
import math
import itertools
from form_connection import form_connection


def isfloat(string):
    """ From an single input, returns either a float or a None """
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

            if each_entity == '41' or each_entity == '51':
                continue

            # number that we switch to daily
            critical_value = 10

            # if the table is less than 10 or between 21 and 30, it might be daily. Works up to the 81-90 range.
            if int(each_entity) <= critical_value or (int(each_entity) >= critical_value*2+1 and int(each_entity) <=critical_value*3) or (int(each_entity) >= critical_value*4+1 and int(each_entity) <=critical_value*5) or (int(each_entity) >= critical_value*6+1 and int(each_entity) <=critical_value*7) or (int(each_entity) >= critical_value*8+1 and int(each_entity) <=critical_value*9):

                # if at least one value has day in it, test that there is a match between at least one of those keywords containing "day" and some word in the high-resolution column names
                if len([x for x in database_map[each_dbcode][each_entity] if "_DAY" in x]) > 0:

                    keywords = [x.rstrip("_DAY") for x in database_map[each_dbcode][each_entity] if "_DAY" in x]
                    hr_test_entity = str(int(each_entity) + 10)



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

            elif int(each_entity) > 91:
                print("Consider reconstructing the `is_daily` function to include entities above 91.")
                        # if that keyword
            else:
                continue

    return daily_index

def flag_count(flag_list):
    """ Quickly count flags in a list, outputing a dictionary and an integer of the count.

    The output is like: (<int>, {'x': count(x), 'y': count(y)})
    """
    flag_counter = defaultdict(int)
    for flag in flag_list:
        flag_counter[flag] +=1

    return flag_counter

def daily_flag(flag_counter, critical_value, critical_flag):
    """ Figure out what the daily flag is based on the outputs of the flag counter.

    If the number of E's is more than 0.05 it's an E, otherwise if the number of Q's is greater than 0.05 it's a Q, otherwise if the number of M is more than 0.2, than the value is 'M', otherwise if E and M and Q sum to more than 0.05, it's Q, and in all other cases it's A.
    """
    if flag_counter[critical_flag] >= critical_value:
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

    `args` are the optional arguments for a start-date (`sd`) and end-date (`ed`). If none are given, the selector will run for the previous water year, figuring out when that started based on today. If run for a daily process, these will be supplied as functions of the last known good data in the daily table.

    This function will call the process_data function on the SQL server. It will return `raw_data` : a mapping, by day, of all the high resolution data apart from the informational columns such as site code, the `column_names` are the column names, in order, explicitly pulled from the server as rows, daily_columns, xt, smashed_template
    """

    # if no start and end date are specified - i.e., re-running it outside of daily runs or script
    if not args:

        # For doing the full-year replacements from Adam, we'll need a water year assignment. Otherwise, args will contain start dates and end dates for daily updates.
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
        print("no args!")

    # get the entity number of the high resolution entity and the column' names that go with it
    hr_entity = daily_index[dbcode][daily_entity]
    hr_columns = database_map[dbcode][hr_entity]

    # shallow copy of column names from the high resolution - needed to not delete them from the list later
    initial_column_names = database_map[dbcode][hr_entity][:]
    daily_columns = database_map[dbcode][daily_entity]

    # empty dictionary for storing daily outputs - this can fit into the smashed data
    # smashed_template = {k: None for k in daily_columns if 'DB_TABLE' not in k}
    smashed_template = {k: None for k in daily_columns}

    # check for max columns (not including flag) in the daily columns
    xval = [x for x in daily_columns if 'MAX' in x and 'FLAG' not in x]

    # check for maxtime or mintime; remove it from the list of daily columns if present
    if xval != []:
        xtime = [x for x in xval if 'TIME' in x]
        for x in xtime:
            xval.remove(x)

    else:
        xtime = []

    # check for min columns (not including flag) in the daily columns
    nval = [x for x in daily_columns if 'MIN' in x and 'FLAG' not in x]

    # check for mintime; remove it if present
    if nval != []:
        ntime = [x for x in nval if 'TIME' in x]
        for x in ntime:
            nval.remove(x)
    else:
        ntime = []

    # use `xt` as a variable to tell SQL if it needs to bring in the high-resolution time data or not. This is expensive to do if you don't have to. True means there is at least 1 time data, false means there is not
    if xtime != [] or ntime != []:
        xt = True
    else:
        xt = False

    # gather the raw data and return it
    raw_data, column_names = process_data(cur, dbcode, hr_entity, initial_column_names, hr_methods, daily_methods, sd, ed, xt)

    return raw_data, column_names, daily_columns, xt, smashed_template


def generate_smashed_data(smashed_template, raw_data):
    """ Creates an output structure for `smashed data` that contains the daily columns with each probe, each day, and the appropriate headers for the output dictionary.
    """
    smashed_data = {each_probe:{dt:smashed_template for dt, raw_data[each_probe][dt] in raw_data[each_probe].items()} for each_probe, raw_data[each_probe] in raw_data.items()}

    return smashed_data


def process_data(cur, dbcode, hr_entity, initial_column_names, hr_methods, daily_methods, sd, ed, xt):
    """ Bring in the high-resolution data from SQL server.

    The column_names array is initialized to build an ordered query from the server. Flag columns precede the date column which is followed by numerical columns. The first two columns are method and probe code. As columns are selected from a copy of the `initial_column_names` list and moved into `column_names`, they are deleted from `initial_column_names`.
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

        # also we need to give the matching probe_code columns

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

    # remove worthless columns from the original list
    for each_column in columns_worthless:
        try:
            initial_column_names.remove(each_column)
        except Exception:
            pass

    # The column in the original data that contains `date` is the date column to use.
    is_the_date = [x for x in initial_column_names if 'DATE' in x][0]

    # Add all the remaining initial columns to the SQL query, following the date column. These SHOULD all be numerical data.
    date_position = len(column_names)

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

    # raw gathered HR data populates raw_data; each probe, date, and attribute has within it a list of the daily values and flags.
    raw_data = {}

    for row in cur:

        try:
            # # if the date is 10-01-2014 00:05 to 10-02-2014 00:00:05, then these values will lose five minutes to 10-01-2014 00:00:00 and 10-02-2014 00:00:00 and be mapped to the day 10-01-2014. Remember this! Because the hourly and 15 minute values will ultimately be part of the "day" rather than the "hour", they will not be affected differently. However, the maxtime and mintime will need to be re-adjusted if calculated.
            adjusted_date_time = datetime.datetime.strptime(str(row[date_position]),'%Y-%m-%d %H:%M:%S') - datetime.timedelta(minutes=5)

            adjusted_date = datetime.datetime(adjusted_date_time.year, adjusted_date_time.month, adjusted_date_time.day)
        except Exception:
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

            # put the daily method for the method code, but use the high-resolution information for the other data. also, add the db_table
            raw_data[probe_code]={adjusted_date:{is_method: this_daily_method['method_code'], 'critical_flag': this_method['critical_flag'], 'critical_value':this_method['critical_value'],'height': this_method['height'], 'depth': this_method['depth'], 'sitecode': this_method['sitecode'], 'db_table': db_table}}

            # for debugging only - you can delete this without ill effect.
            print("KEYS ADDED SO FAR TO DAILY DATA:")
            print(raw_data.keys())

            # put the values in - first the flags, prior to the date, then the values, following it
            raw_data[probe_code][adjusted_date].update({x:[str(row[3+i])] for i,x in enumerate(column_names[3:date_position])})
            raw_data[probe_code][adjusted_date].update({x:[isfloat(row[date_position+1+i])] for i,x in enumerate(column_names[date_position+1:])})

            # if the mintime and maxtime are needed, add in a column called 'date_time' to contain the high-resolution time stamp. Note that this time stamp will be shifted back by five minutes as all the time stamps are, to make it appear on the the right day.
            if xt == True:
                # add in the data for the date stamp for mintime and maxtime
                raw_data[probe_code][adjusted_date].update({'date_time': [adjusted_date_time]})
            else:
                pass

        # if the probe_code is already in the daily data, check the date again versus a change in the method tables.
        elif probe_code in raw_data.keys():

            if adjusted_date not in raw_data[probe_code].keys():

                if len(hr_methods[probe_code]) == 1:
                    this_method = hr_methods[probe_code][0]
                elif len(hr_methods[probe_code]) > 1:
                    this_method = [hr_methods[probe_code][x] for x in sorted(hr_methods[probe_code].keys()) if adjusted_date_time < hr_methods[probe_code][x]['dte'] and adjusted_date_time >= hr_methods[probe_code][x]['dtb']]

                if len(daily_methods[probe_code]) == 1:
                    this_daily_method = daily_methods[probe_code][0]
                elif len(daily_methods[probe_code]) > 1:
                    this_daily_method = [daily_methods[probe_code][x] for x in sorted(daily_methods[probe_code].keys()) if datetime.datetime.strptime(str(row[data_follows]), '%Y-%m-%d %H:%M:%S') < daily_methods[probe_code][x]['dte'] and datetime.datetime.strptime(str(row[data_follows]), '%Y-%m-%d %H:%M:%S') >= daily_methods[probe_code][x]['dtb']]

                # update the raw data with information
                raw_data[probe_code].update({adjusted_date:{is_method: this_daily_method['method_code'], 'critical_flag':this_method['critical_flag'], 'critical_value':this_method['critical_value'], 'height': this_method['height'], 'depth': this_method['depth'], 'sitecode':this_method['sitecode'], 'db_table': db_table}})

                # put the values in, flags first, then values
                raw_data[probe_code][adjusted_date].update({x:[str(row[3+i])] for i,x in enumerate(column_names[3:date_position])})
                raw_data[probe_code][adjusted_date].update({x:[isfloat(row[date_position+1+i])] for i,x in enumerate(column_names[date_position+1:])})

                # add in date-time, if needed for maxtime and mintime
                if xt == True:
                    raw_data[probe_code][adjusted_date].update({'date_time': [adjusted_date_time]})
                else:
                    pass

            # if just appending to the same row of data a new high-resolution value, the previous information regarding the daily method is the same, so there is no need to get it again.
            elif adjusted_date in raw_data[probe_code].keys():

                for i, x in enumerate(column_names[3:date_position]):
                    raw_data[probe_code][adjusted_date][x].append(str(row[3+i]))

                for i, x in enumerate(column_names[date_position+1:]):
                    raw_data[probe_code][adjusted_date][x].append(str(row[date_position+1+i]))

                if xt == True:
                    raw_data[probe_code][adjusted_date]['date_time'].append(adjusted_date_time)
                else:
                    pass

    if is_vpd != []:
        vpd_column_names = [x.lstrip('LTERLogger_pro.dbo.MS0431').lstrip('2.').lstrip('8.').lstrip('1.') for x in united_names]
        return raw_data, vpd_column_names
    else:
        return raw_data, column_names

def sum_if_none(data_list):
    """ Computes a sum from a list of data even if there are None values. Rounds it to 2 decimal points. Returns a 0 if there are no values.
    """

    rounder = lambda x: round(x,2)
    try:
        return rounder(sum([isfloat(x) for x in data_list if x != None and x !='None']))
    except Exception:
        return 0

def len_if_none(data_list):
    """ Computes the length of a list for values that are not None. Returns a zero if the list has no length.
    """
    try:
        return len([x for x in data_list if x != None and x != 'None'])
    except Exception:
        return 0

def mean_if_none(data_list):
    """ Computes the mean for a list, even if there are None values.

    Uses `sum_if_none` and `len_if_none` to assure both numerator and denominator have same values. Returns None if the list cannot be computed.
    """

    rounder = lambda x: round(x,2)

    if all(x is None for x in data_list) != True:

        try:
            return rounder(sum([isfloat(x) for x in data_list if x != 'None' and x != None])/len([x for x in data_list if x != 'None' and x != None]))
        except Exception:
            return None

    else:
        return None

def vpd_if_none(airtemp_list, relhum_list):
    """ Compute the vapor pressure defecit from air temperature and relative humidity.
    """
    try:
        satvp = lambda x: 6.1094*math.exp(17.625*(isfloat(x))/(243.04+isfloat(x))) if x !=None else None

        vpd = mean_if_none([((100-isfloat(y))*0.01)*satvp(x) for x, y in zip(airtemp_list, relhum_list) if x != 'None' and x != None and y != 'None' and y != None])
        return vpd

    except Exception:
        return None


def satvp_if_none(data_list):
    """ Computes saturated vapor pressure as a function of air temperature.

    `data_list` in this context refers to air temperature.
    """
    try:
        # the days satvp - a function of air temp - and a mean of the day
        return mean([6.1094*math.exp(17.625*(isfloat(x))/(243.04+isfloat(x))) for x in data_list if x !='None' and x != None])
    except Exception:
        return None

def vap_if_none(airtemp_list, relhum_list):
    """ Computes the vapor pressure as a function of air temperature and relative humidity.
    """
    try:
        satvp = lambda x: 6.1094*math.exp(17.625*(smasher3.isfloat(x))/(243.04+smasher3.isfloat(x))) if x !=None else None

        dewpoint = lambda x,y: 237.3*math.log(satvp(x)*isfloat(y)/611.)/(7.5*math.log(10)-math.log(satvp(x)*isfloat(y)/611.)) if x != None and y != None else None

        vap = lambda x,y: 6.1094*math.exp((17.625*dewpoint(x,y))/(243.04+dewpoint(x,y))) if x != None and y!= None else None

        return mean_if_none([vap(x,y) for x,y in zip(airtemp_list, relhum_list)])

    except Exception:
        return None


def max_if_none(data_list):
    """ Computes a maximum even if there are none values in the data, or returns None.
    """
    try:
        return max([float(x) for x in data_list if x != None])
    except Exception:
        return None

def min_if_none(data_list):
    """ Computes a minimum even if there are none values, or returns None.
    """
    try:
        return min([float(x) for x in data_list if x != None])
    except Exception:
        return None

def wind_mag_if_none(speed_list, dir_list):
    """ Computes the wind magnitude, and needs both the windspeed and the wind direction.
    """

    rounder = lambda x: round(x,3)

    num_valid = len([x for x in zip(speed_list,dir_list) if x[0] != None and x[0] != 'None' and x[1]!= 'None' and x[1] != None])

    if num_valid == 0:
        return None

    daily_mag_x_part = (sum([float(speed) * math.cos(math.radians(float(direction))) for (speed, direction) in zip(speed_list,dir_list) if speed != 'None' and speed != None and direction != 'None' and direction != None])/num_valid)**2

    daily_mag_y_part = (sum([float(speed) * math.sin(math.radians(float(direction))) for (speed, direction) in zip(speed_list,dir_list) if speed !='None' and speed != None and direction != 'None' and direction != None])/num_valid)**2

    return rounder(math.sqrt(daily_mag_y_part + daily_mag_x_part))


def wind_dir_if_none(speed_list, dir_list):
    """ Computes the weighted wind speed, and needs both speed and direction.
    """

    num_valid = len([x for x in zip(speed_list,dir_list) if x[0] != None and x[1] != None])

    theta_u = math.atan2(sum([float(speed) * math.sin(math.radians(float(direction))) for (speed, direction) in zip(speed_list, dir_list) if speed != 'None' and speed != None and direction != 'None' and direction != None])/num_valid, sum_if_none([float(speed) * math.cos(math.radians(float(direction))) for (speed, direction) in zip(speed_list,dir_list) if speed != 'None' and speed != None and direction != 'None' and direction !=None])/num_valid)

    daily_dir = round(math.degrees(theta_u),3)

    # roll over the zero
    if daily_dir < 0.:
        daily_dir +=360
    else:
        pass

    return daily_dir

def wind_std_if_none(dir_list):
    """ Computes the standard deviation of the wind direction and needs both speed and direction.
    """

    num_valid = len_if_none(dir_list)

    daily_epsilon = math.sqrt(1-((sum([math.sin(math.radians(float(direction))) for direction in dir_list if direction != 'None' and direction != None])/num_valid)**2 + (sum([math.cos(math.radians(float(direction))) for direction in dir_list if direction != 'None' and direction != None])/num_valid)**2))

    daily_sigma_theta = math.degrees(math.asin(daily_epsilon)*(1+(2./math.sqrt(3))-1)*daily_epsilon)

    # if it gives you back a less than 0 value due to the conversion, abs it.
    if daily_sigma_theta < 0.:
        daily_sigma_theta = round(abs(daily_sigma_theta),3)
    else:
        daily_sigma_theta = round(daily_sigma_theta,3)

    return daily_sigma_theta

def calculate_daily_flags(raw_data, column_names, smashed_data):
    """ Daily flags computed with flag_count function.
    """

    # find the columns that contain flags and remove the FLAG_PRO column
    flag_columns = column_names[3:column_names.index('DATE_TIME')]

    # remove provisional columns
    for x in flag_columns:
        if 'FLAG_PRO' in x:
            flag_columns.remove(x)
        else:
            pass

    valid_columns = flag_columns[:]

    # column names are the columns with flags.
    data_flags = {each_probe:{dt:{column_name: flag_count(raw_data[each_probe][dt][column_name]) for column_name, raw_data[each_probe][dt][column_name] in raw_data[each_probe][dt].items() if column_name in valid_columns} for dt, raw_data[each_probe][dt] in raw_data[each_probe].items()} for each_probe, raw_data[each_probe] in raw_data.items()}


    for each_probe in smashed_data.keys():
        for dt in smashed_data[each_probe].keys():
            for each_column in data_flags[each_probe][dt].keys():
                smashed_data[each_probe][dt][each_column] = daily_flag(data_flags[each_probe][dt][each_column], raw_data[each_probe][dt]['critical_value'], raw_data[each_probe][dt]['critical_flag'])
            #smashed_data[each_probe][dt].update({column_name: daily_flag(data_flags[each_probe][dt][column_name], raw_data[each_probe][dt]['critical_value'], raw_data[each_probe][dt]['critical_flag']) for column_name, data_flags[each_probe][dt][column_name] in data_flags[each_probe][dt].items()})

    return smashed_data

def daily_functions(raw_data, column_list, function_choice, xt):
    """ Computes the daily aggregaations from the raw_data inputs for each probe, date_time, and attribute that needs a minimum computed. The function choice is passed in an an attribute given to it.

    when using the data from a 5 minute minimum, the second attribute will be called `column_list`

    function_choice is either min_if_none, max_if_none, sum_if_none
    """
    # data is in the columns following the datetime
    data_columns = [x for x in column_list if 'TIME' not in x]

    data = {each_probe:{dt:{each_attribute: function_choice(raw_data[each_probe][dt][each_attribute]) for each_attribute, raw_data[each_probe][dt][each_attribute] in raw_data[each_probe][dt].items() if each_attribute in data_columns} for dt,raw_data[each_probe][dt] in raw_data[each_probe].items()} for each_probe, raw_data[each_probe] in raw_data.items()}

    if function_choice != min_if_none and function_choice != max_if_none:
        data2 = {}

        return data, data2

    else:

        if xt != True:
            data2 = {}

            return data, data2

        elif xt == True:
            data2 = {}

            # for each probe, date, and attribute, if that column is not in the column listing, pass over it
            for each_probe in list(raw_data.keys()):
                for dt in list(raw_data[each_probe].keys()):
                    for each_attribute in list(raw_data[each_probe][dt].keys()):
                        new_attribute_name = each_attribute + "TIME"

                        if each_attribute not in data_columns:
                            continue

                        try:
                            # output may be as a string
                            function_time = raw_data[each_probe][dt]['date_time'][raw_data[each_probe][dt][each_attribute].index(str(function_choice(raw_data[each_probe][dt][each_attribute])))]
                        except Exception:
                            try:
                                # or a number that is a float
                                function_time = raw_data[each_probe][dt]['date_time'][raw_data[each_probe][dt][each_attribute].index(function_choice(raw_data[each_probe][dt][each_attribute]))]
                            except Exception:
                                try:
                                    # or like solar, an integer...
                                    function_time = raw_data[each_probe][dt]['date_time'][raw_data[each_probe][dt][each_attribute].index(str(int(function_choice(raw_data[each_probe][dt][each_attribute]))))]
                                except Exception:
                                    import pdb; pdb.set_trace()

                        if each_probe not in data2.keys():

                            data2[each_probe] = {dt:{new_attribute_name: function_time}}

                        elif each_probe in data2.keys():
                            if dt not in data2[each_probe].keys():
                                data2[each_probe][dt] = {new_attribute_name: function_time}
                            elif dt in data2[each_probe].keys():
                                if new_attribute_name not in data2[each_probe][dt].keys():
                                    data2[each_probe][dt][new_attribute_name] = function_time
                                elif new_attribute_name in data2[each_probe][dt][new_attribute_name].keys():
                                    print("error in adding the new data")
                                    import pdb; pdb.set_trace()

            return data, data2

def daily_functions_speed_dir(raw_data, is_windpro, valid_columns, function_choice, output_name="DIR"):
    """ For functions like wind that need both a speed and a direction
    """
    dir_cols = [x for x in is_windpro if 'DIR' in x and 'STDDEV' not in x][0]
    mag_cols = [x for x in is_windpro if 'MAG' in x][0]
    speed_cols = [x for x in valid_columns if 'SPD' in x][0]

    rounder = lambda x: round(x,3) if x != 'None' and x != None else None

    if 'DIR' in output_name:
        this_attribute = str(dir_cols)
    elif 'MAG' in output_name:
        this_attribute = str(mag_cols)


    data = {each_probe:{dt:{each_attribute: rounder(function_choice(raw_data[each_probe][dt][speed_cols], raw_data[each_probe][dt][dir_cols])) for each_attribute, raw_data[each_probe][dt][each_attribute] in raw_data[each_probe][dt].items() if each_attribute == this_attribute} for dt,raw_data[each_probe][dt] in raw_data[each_probe].items()} for each_probe, raw_data[each_probe] in raw_data.items()}

    return data

def matching_min_or_max(extrema_data_from_extrema, extrema_data_from_mean, extrematime_from_extrema, extrematime_from_mean, xt, column_names, valid_columns, extrema_key="_MIN"):
    """ Performs the min or max iteration over the min/max data and possibly also the time, replacing None with mean when possible.
    """

    extrema_attribute_keys = [x.rstrip(extrema_key) for x in column_names]

    #airtemp_
    for keyword in extrema_attribute_keys:
        #[airtemp_mean]
        matching_attribute = [x for x in valid_columns if keyword in x]

    if xt == True:
        extrematime_attribute = [x+"TIME" for x in column_names]
        matching_time_attribute = [x+"TIME" for x in matching_attribute]

        for i,each_attribute in enumerate(extrematime_attribute):

            # Data from mean-based max/min time to max/min time label
            new_extrematime_data={each_probe:{dt:{each_attribute: str(extrematime_from_mean[each_probe][dt][matching_time_attribute[i]].hour).zfill(2) + str(extrematime_from_mean[each_probe][dt][matching_time_attribute[i]].minute).zfill(2) for matching_time_attribute[i], extrematime_from_mean[each_probe][dt][matching_time_attribute[i]] in extrematime_from_mean[each_probe][dt].items()} for dt, extrematime_from_mean[each_probe][dt] in extrematime_from_mean[each_probe].items()} for each_probe, extrematime_from_mean[each_probe] in extrematime_from_mean.items()}

            # if the max time or min time is None then replace with the mean
            extrematime_from_extrema.update({each_probe:{dt:{each_attribute: new_extrematime_data[each_probe][dt][each_attribute] for each_attribute, new_extrematime_data[each_probe][dt][each_attribute] in new_extrematime_data[each_probe][dt].items() if extrema_data_from_extrema[each_probe][dt][column_names[i]] == None} for dt, new_extrematime_data[each_probe][dt] in new_extrematime_data[each_probe].items()} for each_probe, new_extrematime_data[each_probe] in new_extrematime_data.items()})
    else:
        pass

    for i, each_attribute in enumerate(column_names):

        # Data from mean-based max/min data to max/min data label
        new_extrema_data={each_probe:{dt:{each_attribute: extrema_data_from_mean[each_probe][dt][matching_attribute[i]] for matching_attribute[i], extrema_data_from_mean[each_probe][dt][matching_attribute[i]] in extrema_data_from_mean[each_probe][dt].items()} for dt, extrema_data_from_mean[each_probe][dt] in extrema_data_from_mean[each_probe].items()} for each_probe, extrema_data_from_mean[each_probe] in extrema_data_from_mean.items()}

        # import pdb; pdb.set_trace()

        # # Raw data flags
        # new_extrema_flags={each_probe:{dt:{each_attribute + "_FLAG": flag_count(raw_data[each_probe][dt][matching_attribute[i] + "_FLAG"]) for matching_attribute[i], raw_data[each_probe][dt][matching_attribute[i]] in raw_data[each_probe][dt].items()} for dt, raw_data[each_probe][dt] in raw_daa[each_probe].items()} for each_probe, raw_data[each_probe] in raw_data.items()}

        # print(new_extrema_flags)

        # Replace the data if the max or min data is None
        extrema_data_from_extrema.update({each_probe:{dt:{each_attribute: extrema_data_from_mean[each_probe][dt][matching_attribute[i]] for matching_attribute[i], extrema_data_from_mean[each_probe][dt][matching_attribute[i]] in extrema_data_from_mean[each_probe][dt].items() if extrema_data_from_extrema[each_probe][dt][column_names[i]] == None} for dt, extrema_data_from_mean[each_probe][dt] in extrema_data_from_mean[each_probe].items()} for each_probe, extrema_data_from_mean[each_probe] in extrema_data_from_mean.items()})

    return extrema_data_from_extrema, extrematime_from_extrema

def comprehend_daily(smashed_data, raw_data, column_names, daily_columns, xt):
    """ Aggregates the raw data based on column names.

    """

    temporary_smash = {}

    # data is in the columns following the datetime
    data_columns = column_names[column_names.index('DATE_TIME')+1:]

    # copy of the column names - these are ultimately all the columns we can "mean"
    valid_columns = data_columns[:]

    # find if there is a maximum column or more in x
    is_max = sorted([x for x in data_columns if 'MAX' in x])

    for each_column in is_max:
        valid_columns.remove(each_column)

    # find out if there is a minimum column
    is_min = sorted([x for x in data_columns if 'MIN' in x])

    for each_column in is_min:
        valid_columns.remove(each_column)

    # if it contains propellor anemometer it should say 'PRO' - but wind speed pro is just a mean
    is_windpro = [x for x in data_columns if 'PRO' in x and 'SPD' not in x]

    for each_column in is_windpro:
        valid_columns.remove(each_column)

    # if it contains sonic, it should say 'SNC' -- remove all but max
    is_windsnc = [x for x in data_columns if 'SNC' in x]

    for each_column in is_windsnc:
        valid_columns.remove(each_column)

        # regular mean
        columns_for_a_regular_mean = [x for x in is_windsnc if 'DIR' not in x and 'STDDEV' not in x and 'MAX' not in x]
        # mean for all the mean stuff
        mean_data_from_mean_snc, _ = daily_functions(raw_data, columns_for_a_regular_mean, mean_if_none, xt)

        # max only on speed
        columns_for_a_max = [x for x in is_windsnc if 'MAX' in x]
        # max for the mean if we need it
        max_data_from_max_snc, maxtime_from_max_snc = daily_functions(raw_data, columns_for_a_max, max_if_none, xt)

        # max from mean only on speed
        columns_for_max_speed_from_mean =[x.rstrip('_MAX') + '_MEAN' for x in columns_for_a_max]

        # max for the mean if we need it
        max_data_from_mean_snc, maxtime_from_mean_snc = daily_functions(raw_data, columns_for_max_speed_from_mean, max_if_none, xt)

        # fix the max if it's missing to be computed from the mean
        max_data_from_max_snc, maxtime_from_max_snc = matching_min_or_max(max_data_from_max_snc, max_data_from_mean_snc, maxtime_from_max_snc, maxtime_from_mean_snc, xt, is_max, valid_columns, extrema_key="_MAX")

        columns_for_deviation = [x for x in is_windsnc if 'STDDEV' in x and 'DIR' not in x]
        columns_for_dir_deviation = [x for x in is_windsnc if 'DIR' in x and 'STDDEV' not in x]

        wind_std_snc, _ = daily_functions(raw_data, columns_for_dir_deviation, wind_std_if_none, xt)

    # if it says 'TOT' its a total
    is_tot = [x for x in data_columns if 'TOT' in x]

    for each_column in is_tot:
        valid_columns.remove(each_column)

    # if it says "INST" its instantaneous
    is_inst = [x for x in data_columns if 'INST' in x]

    for each_column in is_inst:
        valid_columns.remove(each_column)

    # Here the re-aggregation begins -->
    # if the mean isn't empty, mean from mean - will still show all the decimals
    if valid_columns != []:
        mean_data_from_mean, _ = daily_functions(raw_data, valid_columns, mean_if_none, xt)


    # if max isn't empty, compute max and possibly max time
    if is_max != []:
        max_data_from_mean, maxtime_from_mean = daily_functions(raw_data, valid_columns, max_if_none, xt)
        max_data_from_max, maxtime_from_max = daily_functions(raw_data, is_max, max_if_none, xt)

        # use the extrema function to replace the maxes from the max with the maxes from the means when there are none. Be smart about replacing the time.
        max_data_from_max, maxtime_from_max = matching_min_or_max(max_data_from_max, max_data_from_mean, maxtime_from_max, maxtime_from_mean, xt, is_max, valid_columns, extrema_key="_MAX")

    # if min isn't empty, compute min and possibly min time
    if is_min != []:

        min_data_from_mean, mintime_from_mean = daily_functions(raw_data, valid_columns, min_if_none, xt)
        min_data_from_min, mintime_from_min = daily_functions(raw_data, is_min, min_if_none, xt)

        # use the extrema function to replace the min with means when they are none
        min_data_from_min, mintime_from_min = matching_min_or_max(min_data_from_min, min_data_from_mean, mintime_from_min, mintime_from_mean, xt, is_min, valid_columns, extrema_key="_MIN")

    # if wind pro isn't empty, compute wind stuff
    if is_windpro != []:

        std_cols = [x for x in is_windpro if 'STDDEV' in x]
        wind_std, _ = daily_functions(raw_data, std_cols, wind_std_if_none, xt)

        # we will only have one speed and direction from the props, so we can know this will always have a length of 1 and the value is in index 0.
        mean_dir = daily_functions_speed_dir(raw_data, is_windpro, valid_columns, wind_dir_if_none, output_name="DIR")
        mean_mag = daily_functions_speed_dir(raw_data, is_windpro, valid_columns, wind_mag_if_none, output_name="MAG")

    # totals
    if is_tot != []:
        tot, _ = daily_functions(raw_data, is_tot, sum_if_none, xt)

        for each_probe_t in list(tot.keys()):
            for dt_t in list(tot[each_probe_t].keys()):
                for each_attribute_t in tot[each_probe_t][dt_t].keys():
                    smashed_data[each_probe_t][dt_t][each_attribute_t + "_DAY"] = tot[each_probe_t][dt_t][each_attribute_t]


    import pdb; pdb.set_trace()
    return smashed_data

def wind_snc(raw_data, column_names):
    pass

def vpd(raw_data, column_names):
    pass


def fix_max_min(smashed_data, prefix="MAX"):
    """ If there is a value for the maximum data or minimum data but the flag says missing then change the flag to be whatever the accepted flag for the day is. """

    for each_probe in smashed_data.keys():
        for dt in smashed_data[each_probe].keys():

            max_attrs = [x for x in smashed_data[each_probe][dt].keys() if "_" + prefix + "_DAY" in x and "FLAG" not in x]

            if max_attrs:
                for mx in max_attrs:

                    # if the day is not None but the data indicates an 'M'
                    if smashed_data[each_probe][dt][mx] != None and smashed_data[each_probe][dt][mx.rstrip(prefix + "_DAY") + "_" + prefix + "_FLAG"] == 'M':

                        try:
                            smashed_data[each_probe][dt][mx.rstrip("DAY") + "FLAG"] = smashed_data[each_probe][dt][mx.rstrip("_" + prefix + "_DAY") + "_MEAN_FLAG"]
                        except Exception:
                            try:
                                smashed_data[each_probe][dt][mx.rstrip("DAY") + "FLAG"] = smashed_data[each_probe][dt][mx.rstrip("_" + prefix + "_DAY") + "_TOT_FLAG"]
                            except Exception:
                                import pdb; pdb.set_trace()
            else:
                pass

    return smashed_data

def set_missing_to_none(smashed_data):
    """ If the data is `present` because it has been calculated but in fact it should be none because it is missing, set that data to None"""


    for each_probe in smashed_data.keys():
        for dt in smashed_data[each_probe].keys():

            flag_columns = [x for x in smashed_data[each_probe][dt].keys() if "_FLAG" in x]
            data_columns = [x.rstrip("_FLAG") for x in flag_columns]

            set_to_none = {x+"_DAY": None for x in data_columns if smashed_data[each_probe][dt][x+"_FLAG"]=="M" and x+"_DAY" in smashed_data[each_probe][dt].keys()}

            # if there are values to update, update them
            if set_to_none != {}:
                for each_update in set_to_none.keys():
                    smashed_data[each_probe][dt][each_update] = set_to_none[each_update]

            else:
                pass

    return smashed_data

def windrose_fix(smashed_data):
    """ If there are windrose flags in the data, set them to 'M' """

    for each_probe in smashed_data.keys():
        for dt in smashed_data[each_probe].keys():

            # check for wind rose columns and return if there aren't any
            rose_flag_columns = [x for x in smashed_data[each_probe][dt].keys() if "_FLAG" in x and "ROSE" in x]

            if rose_flag_columns == []:
                return smashed_data
            else:
                data_columns = [x.rstrip("_FLAG")+"_DAY" for x in rose_flag_columns]

                # set wind rose flags to M if wind rose is missing
                set_to_m = {x: "M" for x in rose_flag_columns if smashed_data[each_probe][dt][x.rstrip("_FLAG") + "_DAY"]== None or smashed_data[each_probe][dt][x.rstrip("_FLAG") + "_DAY"] =='None'}

                # if there are values to update, update them
                if set_to_m != {}:
                    for each_update in set_to_m.keys():
                        smashed_data[each_probe][dt][each_update] = set_to_m[each_update]

                else:
                    pass

    return smashed_data


def daily_information(smashed_data, dbcode, daily_entity, daily_columns, raw_data):
    """ Gets the daily information about the smashed_data from the condensed flags and updates it.
    """

    is_probe = [x for x in daily_columns if 'PROBE' in x][0]
    is_method = [x for x in daily_columns if 'METHOD' in x][0]

    for each_probe in smashed_data.keys():

        for dt in smashed_data[each_probe].keys():

            smashed_data[each_probe][dt]['DBCODE'] = dbcode
            smashed_data[each_probe][dt][is_probe] = each_probe
            smashed_data[each_probe][dt]['ENTITY'] = daily_entity
            smashed_data[each_probe][dt]['SITECODE'] = raw_data[each_probe][dt]['sitecode']
            smashed_data[each_probe][dt]['DATE'] = datetime.datetime.strftime(dt, '%Y-%m-%d %H:%M:%S')
            smashed_data[each_probe][dt]['EVENT_CODE'] ='NA'
            smashed_data[each_probe][dt]['DB_TABLE'] = raw_data[each_probe][dt]['db_table']
            smashed_data[each_probe][dt]['QC_LEVEL'] = '1P'
            smashed_data[each_probe][dt][is_method] = raw_data[each_probe][dt][is_method]

            if 'HEIGHT' in smashed_data[each_probe][dt].keys():
                smashed_data[each_probe][dt]['HEIGHT'] = raw_data[each_probe][dt]['height']
            elif 'DEPTH' in smashed_data[each_probe][dt].keys():
                smashed_data[each_probe][dt]['DEPTH'] = raw_data[each_probe][dt]['depth']
            else:
                pass

    return smashed_data


def get_methods_for_all_probes(cur, *sd):
    """ Creates hr_methods and daily_methods dictionaries to reference when importing raw data.

    The method tables tell us which flags to use for various resolutions. The `critical_value` refers to the number of flags needed for a complete day. The `critical_flag` refers to the flag that a complete day should assign. `dtb` is datetime begin. `dte` is datetime end. If a method isn't present in the database, the default `xxxxxx` syntax is used for that method.
    """

    # defaults for if a method is not present in the method history tables
    hr_methods = defaultdict(lambda: {0:{'sitecode':'XXXXXX', 'method_code':'XXXXXX','dtb': datetime.datetime(9,9,9,9,9,9), 'dte': datetime.datetime(9999,9,9,9,9,9), 'height':9, 'depth':0, 'resolution': 'None', 'critical_flag':'A', 'critical_value': 287}})
    daily_methods = defaultdict(lambda: {0:{'sitecode':'XXXXXX', 'method_code':'XXXXXX','dtb': datetime.datetime(9,9,9,9,9,9), 'dte': datetime.datetime(9999,9,9,9,9,9), 'height':9, 'depth':0}})

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

        # what flags and values to use for QC are based on the `finest res` attribute in the method_history table. For specific cases like wind, this will be changed outside of this function.
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

def insert_data(cur, smashed_data, daily_index):
    """ Create a SQL statement for inserting your data back into the database.

    The `var_types` variable uses the meta-information about the variable to figure out what type to assign it in the database.
    """

    for each_probe in smashed_data.keys():
        for dt in sorted(smashed_data[each_probe].keys()):

            sorted_keys = sorted(smashed_data[each_probe][dt].keys())
            sorted_keys.remove('ID')
            tuple_data = [smashed_data[each_probe][dt][x] for x in sorted_keys]

            # this works for ms00501 ....
            converter = lambda x: '%s' if x =='str' else '%d'
            var_types = [converter(x.__class__.__name__) for x in tuple_data]

            import pdb; pdb.set_trace()

            print("insert into LTERLogger_Pro.dbo." + smashed_data[each_probe][dt]['DBCODE'] + smashed_data[each_probe][dt]['ENTITY'] + " (" + ", ".join(sorted_keys) +") VALUES (" + ", ".join(var_types) + ")")
            # The following SQL is constructed (raw) like this, FYI
            #cursor.execute("insert into LTERLogger_Pro.dbo." + smashed_data[each_probe][dt]['DBCODE'] + smashed_data[each_probe][dt]['ENTITY'] + " (" + ", ".join(sorted_keys) +") VALUES (%s, %d, %s, %d, %s, %s, %s, %d, %s, %s, %s, %s, %s, %s, %d, %s, %s, %s)", tuple(tuple_data))

            cur.execute("insert into LTERLogger_Pro.dbo." + smashed_data[each_probe][dt]['DBCODE'] + smashed_data[each_probe][dt]['ENTITY'] + " (" + ", ".join(sorted_keys) +") VALUES (" + ", ".join(var_types) + ")", tuple(tuple_data))


            print("check the sql!")

            #conn.commit()

def detect_recent_data(cur, smashed_data, each_probe, dt):
    """ Detect the most recent dt in the daily data before updating. Does not return a start date if there's not one in there.
    """

    try:
        sql= "select top 1 date from lterlogger_pro.dbo." + smashed_data[each_probe][dt]['DBCODE'] + smashed_data[each_probe][dt]['ENTITY'] + " order by date desc"
        cur.execute(sql)
        last_date = cur.fetchone()
        desired_start_date = last_date[0] + datetime.timedelta(days=1)
        return desired_start_date
    except Exception:
        print("no data found, beginning with water year")
        return ""



if __name__ == "__main__":


    try:
        # now this is imported from `form_connection.py`
        conn, cur = form_connection()
    except Exception:
        print(" Please create the form_connection script following instructions by Fox ")

    database_map = get_unique_tables_and_columns(cur)
    daily_index = is_daily(database_map)
    hr_methods, daily_methods = get_methods_for_all_probes(cur)

    ## this simulates some possible inputs we might see
    desired_database = 'MS043'
    desired_daily_entity = '08'
    desired_start_day = '2014-10-01 00:00:00'
    desired_end_day = '2015-04-10 00:00:00'

    # returns are the names of the columns in the
    raw_data, column_names, daily_columns, xt, smashed_template = select_raw_data(cur, database_map, daily_index, hr_methods, daily_methods, desired_database, desired_daily_entity, desired_start_day, desired_end_day)

    # creates a data structure for raw data to be smashed into
    smashed_data_out = generate_smashed_data(smashed_template, raw_data)

    import pdb; pdb.set_trace()

    # perform daily calculations
    smashed_data_out = comprehend_daily(smashed_data_out, raw_data, column_names, daily_columns, xt)


    # create the daily flags
    smashed_data_out = calculate_daily_flags(raw_data, column_names, smashed_data_out)

    # fixes the windrose if needed
    smashed_data_out = windrose_fix(smashed_data_out)

    # fixes min/max flags associated with a min/max flag that is 'missing'
    smashed_data_out = fix_max_min(smashed_data_out, prefix="MAX")
    smashed_data_out = fix_max_min(smashed_data_out, prefix="MIN")

    # any values which are missing but not none should be none
    #smashed_data_out = set_missing_to_none(smashed_data_out)
    # at the very end, add the fluff
    smashed_data_out = daily_information(smashed_data_out, desired_database, desired_daily_entity, daily_columns, raw_data)


    insert_data(cur, smashed_data_out, daily_index)

    print("the end")