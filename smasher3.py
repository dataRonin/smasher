#/anaconda/bin/python
import pymssql
import datetime
from collections import defaultdict
import math
from decimal import *
import itertools

def isdecimal(number, *prec):
    """ From an single numerical input, returns a Decimal. See:

    If the `prec` argument is included, set the precision to that number. If the number is already a float it will not be affected by `prec`.
    Recall that Decimals cannot do operations with floats, or there will be type error.
    """
    if prec:
        getcontext().prec = prec
    else:
        pass

    try:
        return Decimal(number)
    except Exception:
        return None

def isfloat(string):
    """ From an single input, returns either a float or a None """
    try:
        return float(string)
    except Exception:
        return None

def form_connection():
    """ Connects to the SQL server database - Warning: currently hardcoded """

    server = "sheldon.forestry.oregonstate.edu:1433"
    user = 'petersonf'
    password = 'D0ntd1sATLGA!!'
    conn = pymssql.connect(server, user, password)
    cur = conn.cursor()

    return conn, cur


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
    """
    if flag_counter['M']/critical_value > 0.2:
        return 'M'
    elif flag_counter['E']/critical_value > 0.05:
        return 'E'
    elif flag_counter['Q'] + flag_counter['E'] + flag_counter['M'] > 0.05:
        return 'Q'
    else:
        return critical_value

def drange(start, stop, step):
    """ Generates an iterator over any sort of range, such as dates or decimals.

    Useful for making a filled `map` of dates. You could start at 2012-10-01 and stop at 2013-10-01 with a step of five minutes. All inputs should be of the same type.
    """
    r = start
    while r <= stop:
        yield r
        r+=step


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
    smashed_template = {k:None for k in daily_columns if 'DB_TABLE' not in k}

    # check for max columns (not including flag) in the daily columns
    xval = [x for x in daily_columns if 'MAX' in x and 'FLAG' not in x]

    # check for maxtime; remove it if present
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
    raw_data, column_names = process_data(cur, dbcode, hr_entity, hr_columns, initial_column_names, hr_methods, daily_methods, sd, ed, xt)

    return raw_data, column_names, daily_columns, xt, smashed_template


def process_data(cur, dbcode, hr_entity, this_data, initial_column_names, hr_methods, daily_methods, sd, ed, xt):
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

    # assign a variable number of flag columns, which need the word 'FLAG' to work, to the SQL query
    contains_flags = [x for x in initial_column_names if 'FLAG' in x]
    column_names += contains_flags

    # remove flags from the original list
    for each_column in contains_flags:
        try:
            initial_column_names.remove(each_column)
        except Exception:
            pass

    # remove informational columns from the original list. Note that we hardcoded in these "worthless" columns. If more are needed, append here.
    columns_worthless = ['DBCODE', 'DB_TABLE','ENTITY','EVENT_CODE','SITECODE','QC_LEVEL','ID','HEIGHT','DEPTH']

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

    # the data index begins one column after the date
    data_follows = date_position + 1
    column_names.append(is_the_date)
    initial_column_names.remove(is_the_date)

    column_names += initial_column_names

    # join the column names with a comma for the SQL
    column_names_joined = ", ".join(column_names)

    # create and execute the SQL for these columns
    sql = "select " + column_names_joined + " from lterlogger_pro.dbo." + dbcode + hr_entity + " where " + is_the_date + " > \'" + sd + "\' and " + is_the_date + " <= \'" + ed + "\' order by " + is_probe + ", " + is_the_date + " asc"

    cur.execute(sql)

    # output of the daily data for each probe will go here:
    raw_data = {}

    for row in cur:

        try:
            # # if the date is 10-01-2014 00:05 to 10-02-2014 00:00:05, then these values will lose five minutes to 10-01-2014 00:00:00 and 10-02-2014 00:00:00 and be mapped to the day 10-01-2014
            adjusted_date_time = datetime.datetime.strptime(str(row[date_position]),'%Y-%m-%d %H:%M:%S') - datetime.timedelta(minutes=5)

            adjusted_date = datetime.datetime(adjusted_date_time.year, adjusted_date_time.month, adjusted_date_time.day)

        except Exception:
            print("looks like there's no data here")
            continue

        # the probe code is in the first row
        probe_code = str(row[1]).rstrip()

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

            # put the daily method for the method code, but use the high-resolution information for the other data
            raw_data[probe_code]={adjusted_date:{is_method: this_daily_method['method_code'], 'critical_flag': this_method['critical_flag'], 'critical_value':this_method['critical_value'],'height': this_method['height'], 'depth': this_method['depth'], 'sitecode': this_method['sitecode']}}

            # for debugging only
            print("KEYS ADDED SO FAR TO DAILY DATA:")
            print(raw_data.keys())

            # put the values in - first the flags, prior to the date, then the values, following it
            raw_data[probe_code][adjusted_date].update({x:[str(row[2+i])] for i,x in enumerate(column_names[2:date_position])})
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
                raw_data[probe_code].update({adjusted_date:{is_method: this_daily_method['method_code'], 'critical_flag':this_method['critical_flag'], 'critical_value':this_method['critical_value'], 'height': this_method['height'], 'depth': this_method['depth'], 'sitecode':this_method['sitecode']}})

                # put the values in, flags first, then values
                raw_data[probe_code][adjusted_date].update({x:[str(row[2+i])] for i,x in enumerate(column_names[2:date_position])})
                raw_data[probe_code][adjusted_date].update({x:[isfloat(row[date_position+1+i])] for i,x in enumerate(column_names[date_position+1:])})

                # add in date-time, if needed for maxtime and mintime
                if xt == True:
                    raw_data[probe_code][adjusted_date].update({'date_time': [adjusted_date_time]})
                else:
                    pass

            # if just appending to the same row of data a new high-resolution value, the previous information regarding the daily method is the same, so there is no need to get it again.
            elif adjusted_date in raw_data[probe_code].keys():

                for i, x in enumerate(column_names[2:date_position]):
                    raw_data[probe_code][adjusted_date][x].append(str(row[2+i]))

                for i, x in enumerate(column_names[date_position+1:]):
                    raw_data[probe_code][adjusted_date][x].append(str(row[date_position+1+i]))

                if xt == True:
                    raw_data[probe_code][adjusted_date]['date_time'].append(adjusted_date_time)
                else:
                    pass

    return raw_data, column_names

def sum_if_none(data_list):
    """ Computes a sum from a list of data even if there are None values.
    """
    try:
        return sum([x for x in data_list if x != None])
    except Exception:
        return 0

def len_if_none(data_list):
    """ Computes the length of a list for values that are not None. Returns a zero if the list has no length.
    """
    try:
        return len([x for x in data_list if x != None])
    except Exception:
        return 0

def mean_if_none(data_list):
    """ Computes the mean for a list, even if there are None values. Uses sum_if_none and len_if_none to assure both numerator and denominator have same values. Returns None if the list cannot be computed.
    """
    if all(x is None for x in data_list) != True:

        try:
            return sum([float(x) for x in data_list if x != 'None' and x != None])/len([x for x in data_list if x != 'None' and x != None])
        except Exception:
            return None

    else:
        return None

def vpd_if_none(airtemp_list, relhum_list):
    """ Compute the vapor pressure defecit from air temperature and relative humidity.
    """
    try:
        satvp = lambda x: 6.1094*math.exp(17.625*(smasher3.isfloat(x))/(243.04+smasher3.isfloat(x))) if x !=None else None

        vpd = mean_if_none([((100-isfloat(y))*0.01)*satvp(x) for x, y in zip(airtemp_list, relhum_list) if x != None and y != None])
        return vpd

    except Exception:
        return None


def satvp_if_none(data_list):
    """ Computes saturated vapor pressure as a function of air temperature.

    `data_list` in this context refers to air temperature.
    """
    try:
        # the days satvp - a function of air temp - and a mean of the day
        return mean([6.1094*math.exp(17.625*(float(x))/(243.04+float(x))) for x in data_list if x != None])
    except Exception:
        return None

def vap_if_none(airtemp_list, relhum_list):
    """ Computes the vapor pressure as a function of air temperature and relative humidity.
    """
    try:
        satvp = lambda x: 6.1094*math.exp(17.625*(smasher3.isfloat(x))/(243.04+smasher3.isfloat(x))) if x !=None else None

        dewpoint = lambda x,y: 237.3*math.log(satvp(x)*isfloat(y)/611.)/(7.5*math.log(10)-math.log(satvp(x)*isfloat(y)/611.)) if x != None and y != None else None

        vap = lambda x,y: 6.1094*math.exp((17.625*dewpoint(x,y))/(243.04+dewpoint(x,y))) if x != None and y!= None else None

        return mean_if_none([vap(x,y) for x,y in itertools.izip(airtemp_list, relhum_list)])

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

    num_valid = len([x for x in itertools.izip(speed_list,dir_list) if x[0] != None and x[1] != None])

    daily_mag_x_part = (sum([speed * math.cos(math.radians(direction)) for (speed, direction) in itertools.izip(speed_list,dir_list) if speed != None and direction != None])/num_valid)**2
    daily_mag_y_part = (sum([speed * math.sin(math.radians(direction)) for (speed, direction) in itertools.izip(speed_list,dir_list) if speed != None and direction != None])/num_valid)**2

    return math.sqrt(daily_mag_y_part + daily_mag_x_part)


def wind_dir_if_none(speed_list, dir_list):
    """ Computes the weighted wind speed, and needs both speed and direction.
    """

    num_valid = len([x for x in itertools.izip(speed_list,dir_list) if x[0] != None and x[1] != None])

    theta_u = math.atan2(sum([speed * math.sin(math.radians(direction)) for (speed, direction) in itertools.izip(speed_list, dir_list) if speed != None and direction != None])/num_valid, sum([speed * math.cos(math.radians(direction)) for (speed, direction) in itertools.izip(speed_list,dir_list) if speed != None and direction != None])/num_valid)

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

    daily_epsilon = math.sqrt(1-((sum([math.sin(math.radians(direction)) for direction in dir_list if direction != None])/num_valid)**2 + (sum([math.cos(math.radians(direction)) for direction in dir_list if direction != None])/num_valid)**2))

    daily_sigma_theta = math.degrees(math.asin(daily_epsilon)*(1+(2./math.sqrt(3))-1)*daily_epsilon)

    # if it gives you back a less than 0 value due to the conversion, abs it.
    if daily_sigma_theta < 0.:
        daily_sigma_theta = round(abs(daily_sigma_theta),3)
    else:
        daily_sigma_theta = round(daily_sigma_theta,3)

    return daily_sigma_theta

def daily_flags_and_information(raw_data, column_names, smashed_template):
    """ Daily flags computed with flag_count function.
    """

    smashed_data = {each_probe:{dt:smashed_template for dt, raw_data[each_probe][dt] in raw_data[each_probe].items()} for each_probe, raw_data[each_probe] in raw_data.items()}

    flag_columns = column_names[2:column_names.index('DATE_TIME')]

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

            smashed_data[each_probe][dt].update({column_name: daily_flag(data_flags[each_probe][dt][column_name], raw_data[each_probe][dt]['critical_value'],raw_data[each_probe][dt]['critical_flag']) for column_name, data_flags[each_probe][dt][column_name] in data_flags[each_probe][dt].items()})

    return smashed_data

# def daily_maxes(raw_data, max_column_list, xt):
#     """ Computes the maximums from the raw_data inputs for each probe, date_time, and attribute that needs a maximum computed.

#     when using the data from a five minute max, the second argument will be called `max_column_list`

#     each_probe iterates over a list of probes; each_attribute iterates over a list of attributes (such as `AIRTEMP_MEAN`)
#     """

#     # data is in the columns following the datetime
#     data_columns = [x for x in max_column_list if 'TIME' not in x]

#     data = {each_probe: {dt: {each_attribute: {'max_data': max_if_none(raw_data[each_probe][dt][each_attribute])} for each_attribute, raw_data[each_probe][dt][each_attribute] in raw_data[each_probe][dt].items() if each_attribute in data_columns} for dt,raw_data[each_probe][dt] in raw_data[each_probe].items()} for each_probe, raw_data[each_probe] in raw_data.items()}


#     if xt != True:
#         print("no `MAXTIME` present")
#         data2 = {}

#         return data, data2

#     elif xt == True:

#         data2 = {}
#         print("`MAXTIME` must be computed")

#         for each_probe in raw_data.keys():
#             for dt in raw_data[each_probe].keys():
#                 for each_attribute in raw_data[each_probe][dt].keys():

#                     if each_attribute not in data_columns:
#                         continue

#                     try:
#                         maxtime = raw_data[each_probe][dt]['date_time'][raw_data[each_probe][dt][each_attribute].index(str(max_if_none(raw_data[each_probe][dt][each_attribute])))]
#                     except Exception:
#                         maxtime = raw_data[each_probe][dt]['date_time'][raw_data[each_probe][dt][each_attribute].index(max_if_none(raw_data[each_probe][dt][each_attribute]))]

#                     if each_probe not in data2.keys():
#                         data2[each_probe] = {dt:{each_attribute:{'maxtime':maxtime}}}

#                     elif each_probe in data2.keys():

#                         if dt not in data2[each_probe].keys():
#                             data2[each_probe][dt] = {each_attribute:{'maxtime': maxtime}}
#                         elif dt in data2[each_probe].keys():
#                             if each_attribute not in data2[each_probe][dt].keys():
#                                 data2[each_probe][dt][each_attribute] = {'maxtime': maxtime}
#                             elif each_attribute in data2[each_probe][dt][each_attribute].keys():
#                                 print("error in adding the new data")
#                                 import pdb; pdb.set_trace()

#        return data, data2

def daily_functions(raw_data, column_list, function_choice, xt):
    """ Computes the daily aggregaations from the raw_data inputs for each probe, date_time, and attribute that needs a minimum computed. The function choice is passed in an an attribute given to it.

    when using the data from a 5 minute minimum, the second attribute will be called `column_list`

    function_choice is either min_if_none, max_if_none, sum_if_none
    """

    function_map = {min_if_none:'min_data', max_if_none:'max_data', sum_if_none:'total_data', mean_if_none:'mean_data', vpd_if_none: 'vpd_data', satvp_if_none: 'satvp_data', vap_if_non: 'vap_data'}

    # data is in the columns following the datetime
    data_columns = [x for x in column_list if 'TIME' not in x]

    data = {each_probe:{dt:{each_attribute: {'min_data': min_if_none(raw_data[each_probe][dt][each_attribute])} for each_attribute, raw_data[each_probe][dt][each_attribute] in raw_data[each_probe][dt].items() if each_attribute in data_columns} for dt,raw_data[each_probe][dt] in raw_data[each_probe].items()} for each_probe, raw_data[each_probe] in raw_data.items()}


    if xt != True:
        print("no `MINTIME` needed")
        data2 = {}

        return data, data2

    elif xt == True:

        data2 = {}
        print("`MINTIME` must be computed")

        for each_probe in raw_data.keys():
            for dt in raw_data[each_probe].keys():
                for each_attribute in raw_data[each_probe][dt].keys():

                    if each_attribute not in data_columns:
                        continue

                    try:
                        mintime = raw_data[each_probe][dt]['date_time'][raw_data[each_probe][dt][each_attribute].index(str(min_if_none(raw_data[each_probe][dt][each_attribute])))]
                    except Exception:
                        mintime = raw_data[each_probe][dt]['date_time'][raw_data[each_probe][dt][each_attribute].index(min_if_none(raw_data[each_probe][dt][each_attribute]))]

                    if each_probe not in data2.keys():
                        data2[each_probe] = {dt:{each_attribute:{'mintime':mintime}}}

                    elif each_probe in data2.keys():

                        if dt not in data2[each_probe].keys():
                            data2[each_probe][dt] = {each_attribute:{'mintime': mintime}}
                        elif dt in data2[each_probe].keys():
                            if each_attribute not in data2[each_probe][dt].keys():
                                data2[each_probe][dt][each_attribute] = {'mintime':mintime}
                            elif each_attribute in data2[each_probe][dt][each_attribute].keys():
                                print("error in adding the new data")
                                import pdb; pdb.set_trace()

        return data, data2


        def daily_mins(raw_data, min_column_list, xt):
            """ Computes the minimums from the raw_data inputs for each probe, date_time, and attribute that needs a minimum computed.

            when using the data from a 5 minute minimum, the second attribute will be called `column_list`

            p is the probe
            """

            # data is in the columns following the datetime
            data_columns = [x for x in min_column_list if 'TIME' not in x]

            data = {each_probe:{dt:{each_attribute: {'min_data': min_if_none(raw_data[each_probe][dt][each_attribute])} for each_attribute, raw_data[each_probe][dt][each_attribute] in raw_data[each_probe][dt].items() if each_attribute in data_columns} for dt,raw_data[each_probe][dt] in raw_data[each_probe].items()} for each_probe, raw_data[each_probe] in raw_data.items()}


            if xt != True:
                print("no `MINTIME` needed")
                data2 = {}

                return data, data2

            elif xt == True:

                data2 = {}
                print("`MINTIME` must be computed")

                for each_probe in raw_data.keys():
                    for dt in raw_data[each_probe].keys():
                        for each_attribute in raw_data[each_probe][dt].keys():

                            if each_attribute not in data_columns:
                                continue

                            try:
                                mintime = raw_data[each_probe][dt]['date_time'][raw_data[each_probe][dt][each_attribute].index(str(min_if_none(raw_data[each_probe][dt][each_attribute])))]
                            except Exception:
                                mintime = raw_data[each_probe][dt]['date_time'][raw_data[each_probe][dt][each_attribute].index(min_if_none(raw_data[each_probe][dt][each_attribute]))]

                            if each_probe not in data2.keys():
                                data2[each_probe] = {dt:{each_attribute:{'mintime':mintime}}}

                            elif each_probe in data2.keys():

                                if dt not in data2[each_probe].keys():
                                    data2[each_probe][dt] = {each_attribute:{'mintime': mintime}}
                                elif dt in data2[each_probe].keys():
                                    if each_attribute not in data2[each_probe][dt].keys():
                                        data2[each_probe][dt][each_attribute] = {'mintime':mintime}
                                    elif each_attribute in data2[each_probe][dt][each_attribute].keys():
                                        print("error in adding the new data")
                                        import pdb; pdb.set_trace()

                return data, data2

def comprehend_daily(raw_data, column_names, daily_columns, xt):
    """ Aggregates the raw data based on column names.

    """

    import pdb; pdb.set_trace()

    ### HERE TRY TO PRE-POPULATE WITH DAILY SUFFIXES!
    smashed_data = {}

    # data is in the columns following the datetime
    data_columns = column_names[column_names.index('DATE_TIME')+1:]

    # copy of the column names
    valid_columns = data_columns[:]

    # find if there is a maximum column or more in x
    is_max = sorted([x for x in data_columns if 'MAX' in x])

    # if max isn't empty, compute max first from the max column
    if is_max != []:
        max_data_from_max, maxtime_from_max = daily_maxes(raw_data, is_max, xt)

        if len(is_max) == 1:
            max_name = is_max[0]
        else:
            print("more than one maximum exists")
            import pdb; pdb.set_trace()

        for each_column in is_max:
            valid_columns.remove(each_column)

    # find out if there is a minimum column
    is_min = sorted([x for x in data_columns if 'MIN' in x])

    # if min isn't empty, compute min first from the min column
    if is_min != []:
        min_data_from_min, mintime_from_min = daily_mins(raw_data, is_min, xt)

        if len(is_min) == 1:
            min_name = is_min[0]

        for each_column in is_min:
            valid_columns.remove(each_column)

    is_windpro = [x for x in data_columns if 'PRO' in x and 'SPD' not in x]
    for each_column in is_windpro:
        valid_columns.remove(each_column)

    is_windsnc = [x for x in data_columns if 'SNC' in x and 'SPD' not in x]
    for each_column in is_windsnc:
        valid_columns.remove(each_column)

    is_tot = [x for x in data_columns if 'TOT' in x]
    for each_column in is_tot:
        valid_columns.remove(each_column)

    # if everything else is removed, now we want to get the mean.
    data_mean = {each_probe:{dt:{each_attribute: mean_if_none(raw_data[each_probe][dt][each_attribute]) for each_attribute, raw_data[each_probe][dt][each_attribute] in raw_data[each_probe][dt].items() if each_attribute in data_columns} for dt,raw_data[each_probe][dt] in raw_data[each_probe].items()} for each_probe, raw_data[each_probe] in raw_data.items()}

    # if there's only one mean-type column, that is what should be used for the max. if there's more than one, we need to find a way to fix it (Not sure how yet.)
    if len(valid_columns) == 1:
        mean_name = valid_columns[0]
    else:
        print("there is still more than one valid_column!")
        print("computing from the mean columns")

    # put the mean data into the smashed -- it's just a basic copy.
    smashed_data = data_mean

    # now add back in the other data:

    # compute a max from the mean
    max_data_from_mean, maxtime_from_mean = daily_maxes(raw_data, valid_columns, xt)

    import pdb; pdb.set_trace()
    if is_max != []:
        for each_probe in max_data_from_max.keys():
            for dt in max_data_from_max[each_probe].keys():
                for each_attribute in max_data_from_max[each_probe][dt].keys():

                    if max_data_from_max[each_probe][dt][each_attribute]['max_data'] == None:
                        # if the max data is none, replace with max from the mean
                        final_maxvalue = max_data_from_mean[each_probe][dt][mean_name]['max_data']

                        # if we need a max because it's also missing, get from the mean data
                        if xt != False:

                            final_maxtime = maxtime_from_mean[each_probe][dt][mean_name]['maxtime']

                            # put the time back if it needs to go back
                            if each_attribute not in smashed_data[each_probe][dt].keys():
                                smashed_data[each_probe][dt].update({each_attribute + "TIME": final_maxtime})
                        else:
                            pass
                    else:
                        pass

                # put the max back, if it needs to go back
                if each_attribute not in smashed_data[each_probe][dt].keys():
                    smashed_data[each_probe][dt].update({each_attribute:final_maxvalue})


    import pdb; pdb.set_trace()

    if is_min != []:

        min_data_from_mean, mintime_from_mean = daily_mins(raw_data, valid_columns, xt)




    import pdb; pdb.set_trace()

    # if the data max's value is not none then update it - not working yet
    #max_data_from_max.update({each_probe:{dt:{is_max: max_data_from_max[each_probe][dt][y] for y, max_data_from_max[each_probe][dt][y] in max_data_from_max[each_probe][dt][y].items() if max_data_from_max[each_probe][dt][y] !=None} for dt, max_data_from_max[each_probe][dt] in max_data_from_max[each_probe].items()} for each_probe, max_data_from_max[each_probe] in max_data_from_max.items()})


    # if the data max's value is not none then update it - not working yet
    #data_min.update(p:{dt:{y:v for y, v in min_data_from_min[p][dt][y].items() if v !=None} for y, min_data_from_min[p][dt][y] for dt, min_data_from_min[p][dt] in raw_data[p].items()} for p, raw_data[p] in raw_data.items())

    data_sums = {each_probe:{dt:sum_if_none(raw_data[each_probe][dt][y]) for each_probe in raw_data.keys() for dt in raw_data[each_probe].keys() for y in is_tot}}


    pass

def wind_pro(raw_data, column_names):
    pass

def wind_snc(raw_data, column_names):
    pass

def vpd(raw_data, column_names):
    pass


def daily_information(smashed_data, dbcode, daily_entity, daily_columns, raw_data):
    """ Gets the daily information about the smashed_data from the condensed flags and updates it
    """

    is_probe = [x for x in daily_columns if 'PROBE' in x][0]
    is_method = [x for x in daily_columns if 'METHOD' in x][0]

    for each_probe in raw_data.keys():
        for dt in raw_data[each_probe][dt].keys()
            smashed_data[each_probe][dt].update({'DBCODE': dbcode})
            smashed_data[each_probe][dt].update({'ENTITY': daily_entity})
            smashed_data[each_probe][dt].update({'SITECODE': raw_data[each_probe][dt]['site_code']})
            smashed_data[each_probe][dt].update({'DATE': datetime.datetime.strftime(dt, '%Y-%m-%d %H:%M:%S'})
            smashed_data[each_probe][dt].update({'EVENT_CODE': 'NA'})
            smashed_data[each_probe][dt].update({'DB_TABLE': 'NOT_APPLICABLE'})
            smashed_data[each_probe][dt].update({'QC_LEVEL': '1P'})
            smashed_data[each_probe][dt].update({is_method: raw_data[each_probe][dt]['method_code']})

            if 'HEIGHT' in smashed_data[each_probe][dt].keys():
                smashed_data[each_probe][dt].update({is_method: raw_data[each_probe][dt]['height'])
            elif 'DEPTH' in smashed_data[each_probe][dt].keys():
                smashed_data[each_probe][dt].update({is_method: raw_data[each_probe][dt]['depth'])
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


if __name__ == "__main__":

    conn, cur = form_connection()
    database_map = get_unique_tables_and_columns(cur)
    daily_index = is_daily(database_map)
    hr_methods, daily_methods = get_methods_for_all_probes(cur)

    ## this simulates some possible inputs we might see
    desired_database = 'MS005'
    desired_daily_entity = '01'
    desired_start_day = '2014-10-01 00:00:00'
    desired_end_day = '2015-04-10 00:00:00'

    # returns are the names of the columns in the
    raw_data, column_names, daily_columns, xt, smashed_template = select_raw_data(cur, database_map, daily_index, hr_methods, daily_methods, desired_database, desired_daily_entity, desired_start_day, desired_end_day)

    smashed_data_out = daily_flags_and_information(raw_data, column_names, smashed_template)

    smashed_data_out = daily_information(smashed_data_out, desired_database, desired_daily_entity, daily_columns, raw_data)
    import pdb; pdb.set_trace()

    print("hi")