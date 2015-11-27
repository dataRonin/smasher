#/anaconda/bin/python
import pymssql
import datetime
from collections import defaultdict
import math

def isfloat(string):
    """ From an input, returns either a float or a None """
    try:
        return float(string)
    except Exception:
        return None

def form_connection():
    """ Connect to the SQL server database - update using safer parameters """

    server = "sheldon.forestry.oregonstate.edu:1433"
    user = 'petersonf'
    password = 'D0ntd1sATLGA!!'
    conn = pymssql.connect(server, user, password)
    cur = conn.cursor()

    return conn, cur


def get_unique_tables_and_columns(cur):
    """ Get the tables and columns from the information schema of LTERLogger_pro.

    The table names are matched to column names and then both are put into a nested structure so that the headers for each database and entity are in one structure.
    """

    sql = "select table_name, column_name from lterlogger_pro.information_schema.columns group by table_name, column_name"
    cur.execute(sql)

    database_map= {}

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
    """ Finds the high-resolution matches to each daily table and outputs them as a pari.

    The daily key has an high-resolution value mapped to it. Ex. MS043:{'02':'12'}
    """
    daily_index = {}

    for each_dbcode in database_map.keys():
        for each_entity in sorted(database_map[each_dbcode].keys()):

            # number that we switch to daily
            critical_value = 10

            # if the table is less than 10 or between 21 and 30, it might be daily
            if int(each_entity) <= critical_value or (int(each_entity) >= critical_value*2+1 and int(each_entity) <=critical_value*3) or (int(each_entity) >= critical_value*4+1 and int(each_entity) <=critical_value*5) or (int(each_entity) >= critical_value*6+1 and int(each_entity) <=critical_value*7):

                # if at least one value has day in it, test that there is a match between at least one of those keywords and a key in the high resolution data
                if len([x for x in database_map[each_dbcode][each_entity] if "_DAY" in x]) > 0:
                    keywords = [x.rstrip("_DAY") for x in database_map[each_dbcode][each_entity] if "_DAY" in x]
                    hr_test_entity = str(int(each_entity) + 10)

                    for each_keyword in keywords:

                        if len([x for x in database_map[each_dbcode][hr_test_entity] if each_keyword in x]) > 0:
                            if each_dbcode not in daily_index:
                                daily_index[each_dbcode] = {each_entity:hr_test_entity}

                            elif each_dbcode in daily_index:
                                if each_entity not in daily_index[each_dbcode]:
                                    daily_index[each_dbcode][each_entity] = hr_test_entity

                                elif each_entity in daily_index[each_dbcode]:
                                    continue

            elif int(each_entity) > 71:
                print("Consider reconstructing the `is_daily` function")
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

def daily_flag(flag_counter):


    pass

def drange(start, stop, step):
    """ Iteration generator for just about anything.

    It will be very useful for date ranges. The step in this case should be of datetime.timedelta variety.
    """
    r = start
    while r <= stop:
        yield r
        r+=step


def select_raw_data(cur, database_map,  daily_index, hr_methods, daily_methods, dbcode, daily_entity, *args):
    """ Collects the raw data from the database and couples it with methods and information."""

    # For doing the full-year replacements from Adam, we'll need a water year assignment. Otherwise, args will contain start dates and end dates for daily updates.
    this_month = datetime.datetime.now().month

    if this_month >= 10:
        wy_previous = datetime.datetime.now().year

    elif this_month < 10:
        wy_previous = datetime.datetime.now().year-1

    if not args:
        sd = datetime.datetime.strftime(datetime.datetime(wy_previous, 10, 1, 0, 0, 0),'%Y-%m-%d %H:%M:%S')
        ed = datetime.datetime.strftime(datetime.datetime(datetime.datetime.now().year, datetime.datetime.now().month, datetime.datetime.now().day, 0, 0, 0),'%Y-%m-%d %H:%M:%S')

    elif args:
        sd = args[0]
        ed = args[1]

    else:
        print("no args!")

    # get the entity number of the high resolution entity and the columns names that go with it
    hr_entity = daily_index[dbcode][daily_entity]
    hr_columns = database_map[dbcode][hr_entity]

    # shallow copy of column names from the high resolution - needed to not delete them from the list later
    initial_column_names = database_map[dbcode][hr_entity][:]
    daily_columns = database_map[dbcode][daily_entity]

    # check for max columns (not including flag) in the daily columns
    xval = [x for x in daily_columns if 'MAX' in x and 'FLAG' not in x]

    # check for maxtime; remove it if present
    if xval != []:
        xtime = [x for x in xval if 'TIME' in x]
        xval.remove(xtime[0])
    else:
        xtime = []

    # check for min columns (not including flag) in the daily columns
    nval = [x for x in daily_columns if 'MIN' in x and 'FLAG' not in x]

    # check for mintime; remove it if present
    if nval != []:
        ntime = [x for x in nval if 'TIME' in x]
        nval.remove(ntime[0])
    else:
        ntime = []

    # use `xt` as a variable to tell SQL if it needs to bring in the high-resolution time data or not. This is expensive to do if you don't have to. True means there is at least 1 time data, false means there is not
    if xtime != [] or ntime != []:
        xt = True
    else:
        xt = False

    raw_data, column_names = process_data(cur, dbcode, hr_entity, hr_columns, initial_column_names, hr_methods, daily_methods, sd, ed, xt)

    comprehend_daily(raw_data, column_names, xt)

    return raw_data, column_names

def sum_if_none(data_list):
    """ Computes a sum even if there are none values
    """
    try:
        return sum([x for x in data_list if x != None])
    except Exception:
        return 0

def len_if_none(data_list):
    """ Computes a length even if there are none values
    """
    try:
        return len([x for x in data_list if x != None])
    except Exception:
        print("An empty list.")
        return 0

def mean_if_none(data_list):
    """ Computes a length even if there are none values
    """
    try:
        return sum([float(x) for x in data_list if x != 'None' and x != None])/len([x for x in data_list if x != 'None' and x != None])
    except Exception:
        print("An empty list.")
        return None

def vpd_if_none(data_list1, datalist2):
    pass


def satvp_if_none(data_list1, datalist2):
    pass

def vap_if_none(datalist1, datalist2):
    pass


def max_if_none(data_list):
    """ Computes a maximum even if there are none values in the data.
    """
    try:
        return max([float(x) for x in data_list if x != None])
    except Exception:
        return None

def min_if_none(data_list):
    """ Computes a length even if there are none values
    """
    try:
        return min([float(x) for x in data_list if x != None])
    except Exception:
        return None

def wind_mag_if_none(speed_list, dir_list):
    """ Needs both the windspeed and the wind direction
    """

    num_valid = len([x for x in itertools.izip(speed_list,dir_list) if x[0] != None and x[1] != None])

    daily_mag_x_part = (sum([speed * math.cos(math.radians(direction)) for (speed, direction) in itertools.izip(speed_list,dir_list) if speed != None and direction != None])/num_valid)**2
    daily_mag_y_part = (sum([speed * math.sin(math.radians(direction)) for (speed, direction) in itertools.izip(speed_list,dir_list) if speed != None and direction != None])/num_valid)**2

    return math.sqrt(daily_mag_y_part + daily_mag_x_part)


def wind_dir_if_none(speed_list, dir_list):
    """ Needs both the windspeed and the wind direction
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
    """ standard deviation of the wind direction
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

def daily_flags_and_information(raw_data, column_names):

    flag_columns = column_names[:column_names.index('DATE_TIME')]

    valid_columns = flag_columns[:]

    data_flags = {p:{dt:{column_name: flag_count(raw_data[p][dt][column_name]) for column_name, raw_data[p][dt][column_name] in raw_data[p][dt].items() if column_name in valid_columns} for dt, raw_data[p][dt] in raw_data[p].items()} for p, raw_data[p] in raw_data.items()}

    # flag_count(raw_data['AIRR1203'][datetime.datetime(2015, 1, 9, 0, 0)]['AIRTEMP_MEAN_FLAG'])

def comprehend_daily(raw_data, column_names, xt):
    """ Aggregates the raw data based on column names.

    """
    smashed_data = {}

    # data is in the columns following the datetime
    data_columns = column_names[column_names.index('DATE_TIME')+1:]


    # copy of the column names
    valid_columns = data_columns[:]

    is_max = sorted([x for x in data_columns if 'MAX' in x])
    if is_max != []:

        try:
            max_data_from_max, maxtime_from_max = daily_maxes(raw_data, is_max, xt)
        except Exception:
            max_data_from_max = daily_maxes(raw_data, is_max, xt)

        if len(is_max) == 1:
            max_name = is_max[0]
    for each_column in is_max:
        valid_columns.remove(each_column)

    is_min = sorted([x for x in data_columns if 'MIN' in x])
    if is_min != []:
        min_data_from_min = daily_mins(raw_data, is_min, xt)
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

    data_mean = {p:{dt:{y:mean_if_none(raw_data[p][dt][y]) for y, raw_data[p][dt][y] in raw_data[p][dt].items() if y in valid_columns} for dt,raw_data[p][dt] in raw_data[p].items()} for p, raw_data[p] in raw_data.items()}

    data_max = {p:{dt:{max_name:max_if_none(raw_data[p][dt][y]) for y, raw_data[p][dt][y] in raw_data[p][dt].items() if y in valid_columns} for dt,raw_data[p][dt] in raw_data[p].items()} for p, raw_data[p] in raw_data.items()}

    data_min = {p:{dt:{min_name:min_if_none(raw_data[p][dt][y]) for y, raw_data[p][dt][y] in raw_data[p][dt].items() if y in valid_columns} for dt,raw_data[p][dt] in raw_data[p].items()} for p, raw_data[p] in raw_data.items()}

    # if the data max's value is not none then update it - not working yet
    #data_max.update(p:{dt:{y:max_data_from_max[p][dt][y] for y, max_data_from_max[p][dt][y] in max_data_from_max[p][dt][y].items() if max_data_from_max[p][dt][y] !=None} for dt, max_data_from_max[p][dt] in max_data_from_max[p].items()} for p,max_data_from_max[p] in max_data_from_max.items())

    import pdb; pdb.set_trace()

    # if the data max's value is not none then update it - not working yet
    #data_min.update(p:{dt:{y:v for y, v in min_data_from_min[p][dt][y].items() if v !=None} for y, min_data_from_min[p][dt][y] for dt, min_data_from_min[p][dt] in raw_data[p].items()} for p, raw_data[p] in raw_data.items())

    data_sums = {each_probe:{dt:sum_if_none(raw_data[each_probe][dt][y]) for each_probe in raw_data.keys() for dt in raw_data[each_probe].keys() for y in is_tot}}


    pass

def daily_maxes(raw_data, is_max, xt):
    """ Computes the maximums from the raw_data inputs for each probe, date_time, and attribute that needs a maximum computed.

    p is the probe
    """

    # data is in the columns following the datetime
    data_columns = [x for x in is_max if 'TIME' not in x]

    data = {each_probe:{dt:{each_attribute: max_if_none(raw_data[each_probe][dt][each_attribute]) for each_attribute, raw_data[each_probe][dt][y] in raw_data[each_probe][dt].items() if each_attribute in data_columns} for dt,raw_data[each_probe][dt] in raw_data[each_probe].items()} for each_probe, raw_data[each_probe] in raw_data.items()}


    if xt != True:
        print("no max time present")
        return data

    elif xt == True:

        print("max time must be computed")

        try:
            data2 = {each_probe:{dt:{each_attribute: max_if_none(zip(raw_data[each_probe][dt][each_attribute], raw_data[each_probe][dt]['date_time']))[1] for each_attribute, raw_data[each_probe][dt][each_attribute] in raw_data[each_probe][dt].items() if each_attribute in data_columns} for dt,raw_data[each_probe][dt] in raw_data[each_probe].items()} for each_probe, raw_data[each_probe] in raw_data.items()}

        except Exception:
            data2 = {each_probe:{dt:{each_attribute: None for each_attribute, raw_data[each_probe][dt][each_attribute] in raw_data[each_probe][dt].items() if each_attribute in data_columns} for dt,raw_data[each_probe][dt] in raw_data[each_probe].items()} for each_probe, raw_data[each_probe] in raw_data.items()}

        return data, data2

def daily_mins(raw_data, is_min, xt):

    # data is in the columns following the datetime
    data_columns = [x for x in is_min if 'TIME' not in x]

    data = {p:{dt:{y:min_if_none(raw_data[p][dt][y]) for y, raw_data[p][dt][y] in raw_data[p][dt].items() if y in data_columns} for dt,raw_data[p][dt] in raw_data[p].items()} for p, raw_data[p] in raw_data.items()}


    if xt != True:
        return data

    elif xt == True:
        print("xt is true")

    return data


def wind_pro(raw_data, column_names):
    pass

def wind_snc(raw_data, column_names):
    pass

def vpd(raw_data, column_names):
    pass


def output_string_construction(database, desired_database, desired_entity, daily_index, desired_daily_columns):

    output_fluff =[]
    output_fluff.append(desired_database)
    output_fluff.append(desired_entity)

    pass

def process_data(cur, db_code, hr_entity, this_data, initial_column_names, hr_methods, daily_methods, sd, ed, xt):

    # the key here is to sort the imported columns by what type they will be. Flags first, data second. Data will take mostly ints and floats, but flags will take strings
    column_names = []

    # method is in the first column
    is_method = [x for x in initial_column_names if '_METHOD' in x][0]
    column_names.append(is_method)
    # we deplete the original list so we don't re-use columns that fit extra criteria
    initial_column_names.remove(is_method)

    # probe is in the second column
    is_probe = [x for x in initial_column_names if 'PROBE' in x][0]
    column_names.append(is_probe)
    initial_column_names.remove(is_probe)

    # now the flag columns, which need the word 'FLAG' to work
    contains_flags = [x for x in initial_column_names if 'FLAG' in x]
    column_names += contains_flags

    # remove flags and worthless columns
    for each_column in contains_flags:
        try:
            initial_column_names.remove(each_column)
        except Exception:
            pass

    # hardcoded in these "worthless" columns we do not need right now
    columns_worthless = ['DBCODE', 'DB_TABLE','ENTITY','EVENT_CODE','SITECODE','QC_LEVEL','ID','HEIGHT','DEPTH']

    # remove flags and worthless columns
    for each_column in columns_worthless:
        try:
            initial_column_names.remove(each_column)
        except Exception:
            pass

    # first instance of the date is the main date
    is_the_date = [x for x in initial_column_names if 'DATE' in x][0]

    # the position of the date in the final data is going to be the length of column_names now
    # knowing this is important because it tells us how to import the data in that row - strings precede, floats after
    date_position = len(column_names)
    column_names.append(is_the_date)
    initial_column_names.remove(is_the_date)

    column_names += initial_column_names

    # join the column_names for the sql
    column_names_joined = ", ".join(column_names)

    sql = "select " + column_names_joined + " from lterlogger_pro.dbo." + db_code + hr_entity + " where " + is_the_date + " > \'" + sd + "\' and " + is_the_date + " <= \'" + ed + "\' order by " + is_probe + ", " + is_the_date + " asc"

    cur.execute(sql)

    # output of the daily data for each probe will go here:
    daily_data = {}

    for row in cur:

        # is the data not being returned, so maybe it is daily? or missing ...
        try:
            # # if the date is 10-01-2014 00:05 to 10-02-2014 00:00:05, then these values will lose five minutes to 10-01-2014 00:00:00 and 10-02-2014 00:00:00 and be mapped to the day 10-01-2014
            adjusted_date_time = datetime.datetime.strptime(str(row[date_position]),'%Y-%m-%d %H:%M:%S') - datetime.timedelta(minutes=5)
            adjusted_date = datetime.datetime(adjusted_date_time.year, adjusted_date_time.month, adjusted_date_time.day)

        except Exception:
            print("looks like theres no data here")
            continue

        # the first time the word probe is seen is the probecode
        probe_code = str(row[1]).rstrip()

        if probe_code not in daily_data:

            # if the probe has not yet been processed, figure out the method and resolution

            if len(hr_methods[probe_code]) == 1:
                this_method = hr_methods[probe_code][0]

            elif len(hr_methods[probe_code]) > 1:
                #  find the first method that fits where we are within the range of the dates
                this_method = [hr_methods[probe_code][x] for x in sorted(hr_methods[probe_code].keys()) if datetime.datetime.strptime(str(row[data_follows]), '%Y-%m-%d %H:%M:%S') < hr_methods[probe_code][x]['dte'] and datetime.datetime.strptime(str(row[data_follows]), '%Y-%m-%d %H:%M:%S') >= hr_methods[probe_code][x]['dtb']]


            # this is the daily method :)
            daily_data[probe_code]={adjusted_date:{is_method:this_method['method_code'], 'critical_flag': this_method['critical_flag'], 'critical_value':this_method['critical_value'],'height': this_method['height'], 'depth': this_method['depth'], 'sitecode':this_method['sitecode']}}


            print("KEYS ADDED SO FAR TO DAILY DATA:")
            print(daily_data.keys())

            # put the values in - first the flags
            daily_data[probe_code][adjusted_date].update({x:[str(row[2+i])] for i,x in enumerate(column_names[2:date_position])})
            daily_data[probe_code][adjusted_date].update({x:[isfloat(row[date_position+1+i])] for i,x in enumerate(column_names[date_position+1:])})

            if xt == True:
                # add in the data for the date stamp for min time and max time
                daily_data[probe_code][adjusted_date].update({'date_time': [adjusted_date_time]})
            else:
                pass

        # if the probe_code is already in the daily data, assign method and test versus the daily value
        elif probe_code in daily_data.keys():

            if adjusted_date not in daily_data[probe_code].keys():

                if len(hr_methods[probe_code]) == 1:
                    this_method = hr_methods[probe_code][0]

                elif len(hr_methods[probe_code]) > 1:

                    #  find the first method that fits where we are within the range of the dates
                    this_method = [hr_methods[probe_code][x] for x in sorted(hr_methods[probe_code].keys()) if adjusted_date_time < hr_methods[probe_code][x]['dte'] and adjusted_date_time >= hr_methods[probe_code][x]['dtb']]

                daily_data[probe_code].update({adjusted_date:{is_method:this_method['method_code'], 'critical_flag':this_method['critical_flag'], 'critical_value':this_method['critical_value'], 'height': this_method['height'], 'depth': this_method['depth'], 'sitecode':this_method['sitecode']}})

                # put the values in
                daily_data[probe_code][adjusted_date].update({x:[str(row[2+i])] for i,x in enumerate(column_names[2:date_position])})
                daily_data[probe_code][adjusted_date].update({x:[isfloat(row[date_position+1+i])] for i,x in enumerate(column_names[date_position+1:])})

                if xt == True:
                    daily_data[probe_code][adjusted_date].update({'date_time': [adjusted_date_time]})
                else:
                    pass

            elif adjusted_date in daily_data[probe_code].keys():

                for i, x in enumerate(column_names[2:date_position]):
                    daily_data[probe_code][adjusted_date][x].append(str(row[2+i]))

                for i, x in enumerate(column_names[date_position+1:]):
                    # put the values in
                    daily_data[probe_code][adjusted_date][x].append(str(row[date_position+1+i]))

                if xt == True:
                    daily_data[probe_code][adjusted_date]['date_time'].append(adjusted_date_time)
                else:
                    pass

    return daily_data, column_names

def update_daily(daily_columns):

    table_name = db_code + daily_entity

    daily_columns_joined = ", ".join(daily_columns)
    prefix = "Insert into LTERLogger_pro.dbo." + table_name + " (" + daily_columns_joined + ") VALUES (" + format_string + ")"

    pass

def get_methods_for_all_probes(cur, *sd):
    """ Creates hr_methods and daily_methods dictionaries to reference when importing raw data.

    The method tables tell us which flags to use for various resolutions.
    """

    hr_methods = defaultdict(lambda: {0:{'sitecode':'XXXXXX', 'method_code':'XXXXXX','dtb': datetime.datetime(9,9,9,9,9,9), 'dte': datetime.datetime(9999,9,9,9,9,9), 'height':9, 'depth':0, 'resolution': 'None', 'critical_flag':'A', 'critical_value': 287}})
    daily_methods = defaultdict(lambda: {0:{'sitecode':'XXXXXX', 'method_code':'XXXXXX','dtb': datetime.datetime(9,9,9,9,9,9), 'dte': datetime.datetime(9999,9,9,9,9,9), 'height':9, 'depth':0}})

    this_month = datetime.datetime.now().month

    if this_month >= 10:
        wy_previous = datetime.datetime.now().year

    elif this_month < 10:
        wy_previous = datetime.datetime.now().year-1

    if not sd:
        sd = datetime.datetime.strftime(datetime.datetime(wy_previous, 10, 1, 0, 0, 0),'%Y-%m-%d %H:%M:%S')

    sql_hr = "select sitecode, probe_code, date_time_bgn, date_time_end, method_code, finest_res, height, depth from lterlogger_new.dbo.method_history where date_time_end >= \'" + sd + "\' order by probe_code, date_time_bgn"

    sql_daily = "select sitecode, probe_code, date_bgn, date_end, method_code, height, depth from lterlogger_new.dbo.method_history_daily where date_end >= \'" + sd + "\' order by probe_code, date_bgn"

    cur.execute(sql_hr)

    for row in cur:

        # what flags and values to use for QC based on method
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
    raw_data, column_names = select_raw_data(cur, database_map, daily_index, hr_methods, daily_methods, desired_database, desired_daily_entity, desired_start_day, desired_end_day)

    import pdb; pdb.set_trace()

    print("hi")