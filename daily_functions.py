import datetime
from collections import defaultdict
import math
from if_none import *

def daily_functions_vpd(raw_data, vpd_list, valid_columns, function_choice, xt):
    """ For daily computation on functions in VPD that need a min and max and use 2 inputs.
    """
    # gets the names of the columns containing 'AIRTEMP' and 'RELHUM'. By default there should only be 1 column of each.

    airtemp_data = [x for x in valid_columns if 'AIRTEMP' in x][0]
    relhum_data = [x for x in valid_columns if 'RELHUM' in x][0]

    rounder = lambda x: round(x,3) if x != 'None' and x != None else None
    rounder2 = lambda x: round(x,2) if x != 'None' and x != None else None
    rounder1 = lambda x: round(x,2) if x != 'None' and x != None else None

    # there's only two outputs we need - VAP and VPD - so we can use the functions from those to get the correct outputs.

    try:
        # keep it as a list!
        this_attribute = str(vpd_list[0])

    except Exception:
        data = {}
        data2 = {}
        return data, data2

    # if you jus do the mean
    if 'max' not in function_choice.__name__ and 'min' not in function_choice.__name__:

        data = {each_probe:{dt: rounder(function_choice(raw_data[each_probe][dt][airtemp_data], raw_data[each_probe][dt][relhum_data])) for dt in raw_data[each_probe].keys()} for each_probe in raw_data.keys()}

        data2 = {}

        return data, data2

    elif 'max' in function_choice.__name__ or 'min' in function_choice.__name__:

        # if you want the max, but no time
        if xt != True:
            data = {each_probe:{dt: rounder(function_choice(raw_data[each_probe][dt][airtemp_data], raw_data[each_probe][dt][relhum_data], ind=False)) for dt in raw_data[each_probe].keys()} for each_probe in raw_data.keys()}

            data_2 = {}
            return data, data2

        elif xt == True:

            # get the data outside of the tuple, so you don't need to index later
            data = {each_probe:{dt: rounder2(function_choice(raw_data[each_probe][dt][airtemp_data], raw_data[each_probe][dt][relhum_data], ind=False)) for dt in raw_data[each_probe].keys()} for each_probe in raw_data.keys()}

            # get the time stamps in the tuple
            try:
                data2 = {each_probe:{dt: raw_data[each_probe][dt]['date_time'][function_choice(raw_data[each_probe][dt][airtemp_data], raw_data[each_probe][dt][relhum_data], ind=True)[1]] for dt in raw_data[each_probe].keys() if function_choice(raw_data[each_probe][dt][airtemp_data], raw_data[each_probe][dt][relhum_data], ind=True) != None} for each_probe in raw_data.keys()}
            except Exception:
                try:
                    data2 = {each_probe:{dt: raw_data[each_probe][dt]['date_time'][function_choice(raw_data[each_probe][dt]['AIRTEMP_MEAN'], raw_data[each_probe][dt]['RELHUM_MEAN'], ind=True)[1]] for dt in raw_data[each_probe].keys() if function_choice(raw_data[each_probe][dt]['AIRTEMP_MEAN'], raw_data[each_probe][dt]['RELHUM_MEAN'], ind=True)[1] != None} for each_probe in raw_data.keys()}
                except Exception:
                    try:
                        data2 = {each_probe:{dt: raw_data[each_probe][dt]['date_time'][function_choice(raw_data[each_probe][dt]['AIRTEMP_MEAN'], raw_data[each_probe][dt]['RELHUM_MEAN'], ind=True)[1]] for dt in raw_data[each_probe].keys() if str(function_choice(raw_data[each_probe][dt]['AIRTEMP_MEAN'], raw_data[each_probe][dt]['RELHUM_MEAN'], ind=True)[1])!= 'None'} for each_probe in raw_data.keys()}
                    except Exception:
                        try:
                            data2 = {each_probe:{dt: raw_data[each_probe][dt]['date_time'][function_choice(raw_data[each_probe][dt]['AIRTEMP_MEAN'], raw_data[each_probe][dt]['RELHUM_MEAN'], ind=True)[1]] for dt in raw_data[each_probe].keys() if str(function_choice(raw_data[each_probe][dt]['AIRTEMP_MEAN'], raw_data[each_probe][dt]['RELHUM_MEAN'], ind=True))!= 'None'} for each_probe in raw_data.keys()}
                        except Exception:
                            try:
                                data2 = {each_probe:{dt: raw_data[each_probe][dt]['date_time'][function_choice(raw_data[each_probe][dt]['AIRTEMP_MEAN'], raw_data[each_probe][dt]['RELHUM_MEAN'], ind=True)[1]] for dt in raw_data[each_probe].keys() if function_choice(raw_data[each_probe][dt]['AIRTEMP_MEAN'], raw_data[each_probe][dt]['RELHUM_MEAN'], ind=True) != None} for each_probe in raw_data.keys()}
                            except Exception:
                                try:
                                    data2 = {each_probe:{dt: raw_data[each_probe][dt]['date_time'][function_choice(raw_data[each_probe][dt]['AIRTEMP_MEAN'], raw_data[each_probe][dt]['RELHUM_MEAN'], ind=True)[1]] for dt in raw_data[each_probe].keys() if function_choice(raw_data[each_probe][dt]['AIRTEMP_MEAN'], raw_data[each_probe][dt]['RELHUM_MEAN'], ind=True)[0] != None} for each_probe in raw_data.keys()}
                                except Exception:
                                    data2 = {each_probe:{dt: raw_data[each_probe][dt]['date_time'][function_choice(raw_data[each_probe][dt]['AIRTEMP_MEAN'], raw_data[each_probe][dt]['RELHUM_MEAN'], ind=True)[1]] for dt in raw_data[each_probe].keys() if str(function_choice(raw_data[each_probe][dt]['AIRTEMP_MEAN'], raw_data[each_probe][dt]['RELHUM_MEAN'], ind=True)[0]) != 'None'} for each_probe in raw_data.keys()}

            return data, data2

        else:
            print("error in function for computing vpd means/max/mins")

def daily_functions_normal(raw_data, column_name, function_choice, xt):
    """ Computes the daily aggregaations from the raw_data inputs for each probe, date_time, and attribute that needs a minimum computed. The function choice is passed in an an attribute given to it.

    The column name should be  a string.

    function_choice is min_if_none, max_if_none, mean_if_none, or sum_if_none
    """

    # rounds the output to 3, 2, or 1 decimal place (or an int) depending on what output is desired.
    rounder3 = lambda x: round(x,3) if x != 'None' and x != None else None
    rounder2 = lambda x: round(x,2) if x != 'None' and x != None else None
    rounder1 = lambda x: round(x,1) if x != 'None' and x != None else None
    rounderint = lambda x: int(x) if x != 'None' and x != None else None

    # If you are not trying to compute the minimum or maximum values
    if 'max' not in function_choice.__name__ and 'min' not in function_choice.__name__:

        data = {each_probe: {dt: function_choice(raw_data[each_probe][dt][column_name]) for dt in raw_data[each_probe].keys()} for each_probe in raw_data.keys()}

        data2 = {}
        return data, data2

    elif 'max' in function_choice.__name__ or 'min' in function_choice.__name__:

        if xt != True:
            data = {each_probe:{dt: function_choice(raw_data[each_probe][dt][column_name]) for dt in raw_data[each_probe].keys()} for each_probe in raw_data.keys()}

            data2 = {}
            return data, data2

        elif xt == True:

            data = {each_probe:{dt: function_choice(raw_data[each_probe][dt][column_name]) for dt in raw_data[each_probe].keys()} for each_probe in raw_data.keys()}
            try:
                data2 = {each_probe:{dt: raw_data[each_probe][dt]['date_time'][[str(x) for x in raw_data[each_probe][dt][column_name]].index(str(function_choice(raw_data[each_probe][dt][column_name])))] for dt in raw_data[each_probe].keys()} for each_probe in raw_data.keys()}
            except Exception:
                try:
                    data2 = {each_probe:{dt: raw_data[each_probe][dt]['date_time'][[str(x) for x in raw_data[each_probe][dt][column_name]].index(str(int(function_choice(raw_data[each_probe][dt][column_name]))))] for dt in raw_data[each_probe].keys()} for each_probe in raw_data.keys()}
                except Exception:
                    try:
                        data2 = {each_probe:{dt: raw_data[each_probe][dt]['date_time'][[str(x) for x in raw_data[each_probe][dt][column_name]].index(str(round(function_choice(raw_data[each_probe][dt][column_name]))),2)] for dt in raw_data[each_probe].keys()} for each_probe in raw_data.keys()}
                    except Exception:
                        try:
                            data2 = {each_probe:{dt: raw_data[each_probe][dt]['date_time'][[str(x) for x in raw_data[each_probe][dt][column_name]].index(str(round(function_choice(raw_data[each_probe][dt][column_name]))),3)] for dt in raw_data[each_probe].keys()} for each_probe in raw_data.keys()}
                        except Exception:
                            try:
                                data2 = {each_probe: {dt: raw_data[each_probe][dt]['date_time'][[str(x) for x in raw_data[each_probe][dt][column_name]].index(str(function_choice(raw_data[each_probe][dt][column_name])))] for dt in raw_data[each_probe].keys()} for each_probe in raw_data.keys()}
                            except Exception:
                                data2 = {}
                                for each_probe in sorted(list(raw_data.keys())):
                                    for dt in sorted(list(raw_data[each_probe].keys())):
                                        try:
                                            new_value = raw_data[each_probe][dt]['date_time'][[str(x) for x in raw_data[each_probe][dt][column_name]].index(str(function_choice(raw_data[each_probe][dt][column_name])))]
                                        except Exception:
                                            try:
                                                new_value = raw_data[each_probe][dt]['date_time'][[str(x).rstrip('0') for x in raw_data[each_probe][dt][column_name]].index(str(function_choice(raw_data[each_probe][dt][column_name])))]
                                            except Exception:
                                                try:
                                                    new_value = raw_data[each_probe][dt]['date_time'][[str(int(x)) for x in raw_data[each_probe][dt][column_name]].index(str(int(function_choice(raw_data[each_probe][dt][column_name]))))]
                                                except Exception:
                                                    try:
                                                        new_value = raw_data[each_probe][dt]['date_time'][[str(x).rstrip('0').rstrip('.') for x in raw_data[each_probe][dt][column_name]].index(str(int(function_choice(raw_data[each_probe][dt][column_name]))))]
                                                    except Exception:
                                                        if str(function_choice(raw_data[each_probe][dt][column_name])) == 'None':
                                                            new_value = None
                                                        else:
                                                            print("Error finding matching max or min time for " + datetime.datetime.strftime(dt, '%Y-%m-%d') + " on probe " + each_probe + " for data in " + column_name)

                                        if each_probe not in data2:
                                            data2[each_probe] = {dt: new_value}
                                        elif each_probe in data2:
                                            if dt not in data2[each_probe]:
                                                data2[each_probe][dt] = new_value
                                            elif dt in data2[each_probe]:
                                                pass

            return data, data2

        else:
            print("error in function for computing means/max/mins")

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

    data = {each_probe:{dt: function_choice(raw_data[each_probe][dt][speed_cols], raw_data[each_probe][dt][dir_cols]) for dt in raw_data[each_probe].keys()} for each_probe in raw_data.keys()}

    return data

def daily_functions_speed_dir_snc(raw_data, speed_name, dir_name, function_choice):

    """ For functions like wind that need both a speed and a direction
    """

    rounder = lambda x: round(x,3) if x != 'None' and x != None else None


    data = {each_probe:{dt: function_choice(raw_data[each_probe][dt][speed_name], raw_data[each_probe][dt][dir_name]) for dt in raw_data[each_probe].keys()} for each_probe in raw_data.keys()}

    return data

