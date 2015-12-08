import datetime
import math

def isfloat(string):
    """ From an single input, returns either a float or a None """
    try:
        return float(string)
    except Exception:
        return None

def max_if_none(data_list):
    """ Computes a maximum even if there are none values in the data, or returns None.
    """
    try:
        return max([float(x) for x in data_list if x != 'None' and x !=None])
    except Exception:
        return None

def min_if_none(data_list):
    """ Computes a minimum even if there are none values, or returns None.
    """
    try:
        return min([float(x) for x in data_list if x != 'None' and x !=None])
    except Exception:
        return None

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
    """ Computes the vapor pressure defecit from air temperature and relative humidity.
    """
    try:
        satvp = lambda x: 6.1094*math.exp(17.625*(isfloat(x))/(243.04+isfloat(x))) if x !=None else None

        vpd = mean_if_none([((100-isfloat(y))*0.01)*satvp(x) for x, y in zip(airtemp_list, relhum_list) if x != 'None' and x != None and y != 'None' and y != None])
        return vpd

    except Exception:
        return None


def max_vpd_if_none(airtemp_list, relhum_list, ind=False):
    """ Computes the maximum the vapor pressure defecit from air temperature and relative humidity.

    `Index` tells us where the max vpd is so we can map the date-time
    """
    try:
        satvp = lambda x: 6.1094*math.exp(17.625*(isfloat(x))/(243.04+isfloat(x))) if x !=None else None

        vpd_1 = [((100-isfloat(y))*0.01)*satvp(x) for x, y in zip(airtemp_list, relhum_list) if x != 'None' and x != None and y != 'None' and y != None]

        max_vpd = max_if_none(vpd_1)

        if ind != False:
            try:
                max_index = vpd_1.index(max_if_none(vpd_1))
                return (max_vpd, max_index)

            except Exception:
                return (None, None)
        else:
            return max_vpd

    except Exception:

        if ind == False:
            return None
        else:
            return (None, None)


def min_vpd_if_none(airtemp_list, relhum_list, ind=False):
    """ Computes the minimum the vapor pressure defecit from air temperature and relative humidity.
    """
    try:
        satvp = lambda x: 6.1094*math.exp(17.625*(isfloat(x))/(243.04+isfloat(x))) if x !=None else None

        vpd_1 = [((100-isfloat(y))*0.01)*satvp(x) for x, y in zip(airtemp_list, relhum_list) if x != 'None' and x != None and y != 'None' and y != None]

        min_vpd = min_if_none(vpd_1)

        if ind != False:
            try:
                min_index = vpd_1.index(min_if_none(vpd_1))
                return (min_vpd, min_index)
            except Exception:
                return (None, None)
        else:
            return min_vpd

    except Exception:
        if ind == False:
            return None
        else:
            return (None, None)


def satvp_if_none(data_list):
    """ Computes saturated vapor pressure as a function of air temperature.

    `data_list` in this context refers to air temperature.
    """
    try:
        # the days satvp - a function of air temp - and a mean of the day
        return mean_if_none([6.1094*math.exp(17.625*(isfloat(x))/(243.04+isfloat(x))) for x in data_list if x !='None' and x != None])
    except Exception:
        return None

def max_satvp_if_none(data_list):
    """ Computes the maximum saturated vapor pressure as a function of air temperature.

    `data_list` in this context refers to air temperature.
    """
    try:
        # the days satvp - a function of air temp - and a mean of the day
        return max_if_none([6.1094*math.exp(17.625*(isfloat(x))/(243.04+isfloat(x))) for x in data_list if x !='None' and x != None])
    except Exception:
        return None


def min_satvp_if_none(data_list):
    """ Computes the minimum saturated vapor pressure as a function of air temperature.

    `data_list` in this context refers to air temperature.
    """
    try:
        # the days satvp - a function of air temp - and a mean of the day
        return min_if_none([6.1094*math.exp(17.625*(isfloat(x))/(243.04+isfloat(x))) for x in data_list if x !='None' and x != None])
    except Exception:
        return None


def vap_if_none(airtemp_list, relhum_list):
    """ Computes the vapor pressure as a function of air temperature and relative humidity. Uses saturated vapor pressue along the way.
    """
    try:
        satvp = lambda x: 6.1094*math.exp(17.625*(isfloat(x))/(243.04+isfloat(x))) if x !=None and x!='None' else None

        dewpoint = lambda x,y: 237.3*math.log(satvp(x)*isfloat(y)/611.)/(7.5*math.log(10)-math.log(satvp(x)*isfloat(y)/611.)) if x != None and y != None else None

        vap = lambda x,y: 6.1094*math.exp((17.625*dewpoint(x,y))/(243.04+dewpoint(x,y))) if x != None and y!= None else None

        return mean_if_none([vap(x,y) for x,y in zip(airtemp_list, relhum_list)])

    except Exception:
        return None

def max_vap_if_none(airtemp_list, relhum_list, ind=False):
    """ Computes the maximum vapor pressure as a function of air temperature and relative humidity. Uses saturated vapor pressue along the way.

    `Index` tells where the index of the max is, so we can align it with the date time
    """
    try:
        satvp = lambda x: 6.1094*math.exp(17.625*(isfloat(x))/(243.04+isfloat(x))) if x !=None and x!='None' else None

        dewpoint = lambda x,y: 237.3*math.log(satvp(x)*isfloat(y)/611.)/(7.5*math.log(10)-math.log(satvp(x)*isfloat(y)/611.)) if x != None and y != None else None

        vap = lambda x,y: 6.1094*math.exp((17.625*dewpoint(x,y))/(243.04+dewpoint(x,y))) if x != None and y!= None else None

        vap_1 = [vap(x,y) for x,y in zip(airtemp_list, relhum_list)]

        max_vap = max_if_none(vap_1)

        if ind != False:
            try:
                max_index = vap_1.index(max_if_none(vap_1))
                return (max_vap, max_index)
            except Exception:
                return (None, None)
        else:
            return max_vap

    except Exception:
        return None


def min_vap_if_none(airtemp_list, relhum_list, ind=False):
    """ Computes the minimum vapor pressure as a function of air temperature and relative humidity. Uses saturated vapor pressue along the way.

    `Index` tells us where the minimum is, so we can map it to the date-time.
    """
    try:
        satvp = lambda x: 6.1094*math.exp(17.625*(isfloat(x))/(243.04+isfloat(x))) if x !=None and x!='None' else None

        dewpoint = lambda x,y: 237.3*math.log(satvp(x)*isfloat(y)/611.)/(7.5*math.log(10)-math.log(satvp(x)*isfloat(y)/611.)) if x != None and y != None else None

        vap = lambda x,y: 6.1094*math.exp((17.625*dewpoint(x,y))/(243.04+dewpoint(x,y))) if x != None and y!= None else None

        vap_1 = [vap(x,y) for x,y in zip(airtemp_list, relhum_list)]

        min_vap = min_if_none(vap_1)

        if ind != False:
            try:
                min_index = vap_1.index(min_if_none(vap_1))
                return (min_vap, min_index)
            except Exception:
                return (None, None)
        else:
            return min_vap

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