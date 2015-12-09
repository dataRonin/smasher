Smasher3
========

Smasher3 is a better smasher.


NOTE!
---

To run this program you'll need to create a file called `form_connection.py`. In this file you will want to put the following import and def statements.


		import pymssql

		def form_connection():
		    """ Connects to the SQL server database"""

		    server = "SERVERNAME.FORESTRY.OREGONSTATE.EDU:1433"
		    user = YOUR SQL SERVER USERNAME, IN QUOTES
		    password = YOUR SQL SERVER PASSWORD, IN QUOTES
		    conn = pymssql.connect(server, user, password)
		    cur = conn.cursor()

		    return conn, cur

I have purposely left this file in .gitignore for safety.


READ THE CODE!!
---------

There are extensive comments for nearly every line of code, in lieu of a bunch of docs here. The comments should help you to find bugs.


Reading a Python ``traceback``
----

If the code breaks, and it will, you will get an error. This is likely because something in the data doesn't match the expectation of the program. This is very likely in this script as we traded stability for assumptions and variability. I won't be here to fix it, so here's what a traceback looks like:

		>>> wind_dir_if_none([1,2,3,1,2,3],[])
		Traceback (most recent call last):
		  File "<stdin>", line 1, in <module>
		  File "/Users/dataronin/Documents/november2015/smasher/if_none.py", line 263, in wind_dir_if_none
		    theta_u = math.atan2(sum([float(speed) * math.sin(math.radians(float(direction))) for (speed, direction) in zip(speed_list, dir_list) if speed != 'None' and speed != None and direction != 'None' and direction != None])/num_valid, sum_if_none([float(speed) * math.cos(math.radians(float(direction))) for (speed, direction) in zip(speed_list,dir_list) if speed != 'None' and speed != None and direction != 'None' and direction !=None])/num_valid)
		ZeroDivisionError: division by zero
		>>> quit()

Here, I called the function `wind_dir_if_none` on two lists, one of which was empty. The Traceback tells me where the program snagged. You want the location in the bottom most File. In this case it is "/Users/dataronin/Documents/november2015/smasher/if_none.py". The error occurs in line 263. The function that called the error was `wind_dir_if_none`. The variable scope in which the error was called was `theta_u`. The error itself was a `zero division error`. We see that the `theta_u` variable divides by another variable `num_valid`. The `num_valid` is the length of the `dir_list`. Since it is empty, it's length is zero.

To debug this, you'd simply go into that file and wrap the function in an exception block, as I did, to read:

		def wind_dir_if_none(speed_list, dir_list):
		    """ Computes the weighted direction (by speed); needs both speed and direction.
		    """

		    num_valid = len([x for x in zip(speed_list,dir_list) if x[0] != None and x[1] != None])

		    # if there are no valid directions given, there is no daily wind direction
		    try:
		        theta_u = math.atan2(sum([float(speed) * math.sin(math.radians(float(direction))) for (speed, direction) in zip(speed_list, dir_list) if speed != 'None' and speed != None and direction != 'None' and direction != None])/num_valid, sum_if_none([float(speed) * math.cos(math.radians(float(direction))) for (speed, direction) in zip(speed_list,dir_list) if speed != 'None' and speed != None and direction != 'None' and direction !=None])/num_valid)
		    except Exception:
		        return None

		    daily_dir = round(math.degrees(theta_u),3)

		    # roll over the zero
		    if daily_dir < 0.:
		        daily_dir +=360
		    else:
		        pass

		    return daily_dir

Now you can see that if `theta_u` fails, it will return the numerical None in all cases. This is correct here, because if there is not any direction in the day, then how can the day have any standard deviation?


`hr_methods` and `daily_methods`
---

These are variables created to store the information from `method_history` and `method_history_daily`. Because there are many instances where these tables may not contain the probe we are looking for or during the time we are looking for it, a default set of information, which is mostly the letter X, is used to fill in the values. For height and depth on these defaults, 9 is used.

If the data is there, however, it will come back as such:

		>>> hr_methods['RELH1502']
		{0: {'critical_value': 95, 'height': '150', 'resolution': '15 minutes', 'sitecode': 'H15MET', 'dtb': datetime.datetime(2013, 1, 31, 11, 0), 'critical_flag': 'F', 'dte': datetime.datetime(2050, 12, 31, 0, 0), 'depth': '0', 'method_code': 'REL014'}}

		>>> daily_methods['RELH1502']
		{0: {'height': '150', 'sitecode': 'H15MET', 'dtb': datetime.datetime(2013, 2, 1, 0, 0), 'dte': datetime.datetime(2050, 12, 31, 0, 0), 'depth': '0', 'method_code': 'REL314'}}

The `critical_value` key indicates the number of values a day must exceed to be entirely complete. The `critical_flag` attribute is the flag given to an acceptable day. The `resolution` is from `finest_res` and is used to determine what the `critical_flag` and `critical_value` should be.


pymssql errors
------

These are the worst to debug, and generally (maybe 90 percent of the time) they have nothing to do with the Python and rather to do with a change in the database. For example, if a column was `not nullable` and now it is `nullable` or if a high-resolution data has been added but its daily counterpart has not.

The best way I find to debug these is to open python and make a connection to the database like this:

		from form_connection import *

		conn, cur = form_connection()

Then, make a variable called `sql` and type some SQL in it to target the suspected error. For example,

		sql = "select * from lterlogger_pro.dbo.ms04318 where probe_code like 'AIRCEN01' and date_time > '2014-11-01 00:00:00' and date_time <= '2014-11-02 00:00:00' order by date_time asc"

Use the python cursor to execute the sql.

		cur.execute(sql)

If you don't get an error, the SQL server was able to execute the code, that's a start.

Now check the output... for many lines you can print a list comprehension of all the returned rows, or if you just want one line, use `fetchone()`

		# prints many lines
		print([row for row in cur])

		# prints one line
		cur.fetchone()

See if the output is as you expect.


raw_data checks
---

Checking the raw data versus the code has only been done marginally, but I have found it very effective to use the daily max and min as checks, since those are easy to find in SQL with raw SQL code and also easy to compute. I admit not all of this code has been checked vs. the database, and more checks are great.

		>>> raw_data['AIRPRI07'][datetime.datetime(2015,1,1,0,0)].keys()
		dict_keys(['date_time', 'height', 'AIRTEMP_METHOD', 'AIRTEMP_MEAN', 'critical_flag', 'critical_value', 'AIRTEMP_MEAN_FLAG', 'db_table', 'sitecode', 'AIRTEMP_MIN_FLAG', 'AIRTEMP_MIN', 'AIRTEMP_MAX', 'depth', 'AIRTEMP_MAX_FLAG'])

The behavior is as desired: We want that the final value on 12-31-2014 23:55:00 is on that day, and the first value on Jan 1, 2015 is on that day.

Notice that in the SQL server we are missing some numbers..

	>>> raw_data['AIRPRI07'][datetime.datetime(2014,12,31,0,0)]['AIRTEMP_MEAN'][-1:]
	['-3.6']

	>>> raw_data['AIRPRI07'][datetime.datetime(2015,1,1,0,0)]['AIRTEMP_MEAN'][0]
	-3.7

Corresponding to:

date_time	airtemp_mean
Dec 31 2014 11:55:00:000PM	-3.6
Jan  1 2015 12:05:00:000AM	-3.7


computing the airtemp_mean
----

		>>> data['AIRPRI07'][datetime.datetime(2014,12,31,0,0)]
		-4.52

		1> select avg(AIRTEMP_MEAN) from lterlogger_pro.dbo.ms04311 where probe_code like 'airpri07' and date_time >='2014-12-31 00:00:00' and date_time<'2014-12-31 23:55:00'
		2> go

		-4.530069


checking the airtemp_max and airtemp_maxtime
----

		>>> data['AIRPRI07'][datetime.datetime(2014,12,31,0,0)]
		0.0

		1> select max(AIRTEMP_MEAN) from lterlogger_pro.dbo.ms04311 where probe_code like 'airpri07' and date_time >='2014-12-31 00:00:00' and date_time<'2014-12-31 23:55:00'
		2> go

		0.0

maxtime:
--

	>>> data2['AIRPRI07'][datetime.datetime(2014,12,31,0,0)]
	datetime.datetime(2014, 12, 31, 14, 5)

	1> select date_time, AIRTEMP_MEAN from lterlogger_pro.dbo.ms04311 where probe_code like 'airpri07' and date_time >='2014-12-31 14:00:00' and date_time<'2014-12-31 15:00:00'
	2> go
	date_time	AIRTEMP_MEAN
	Dec 31 2014 02:00:00:000PM	-0.2
	Dec 31 2014 02:05:00:000PM	-0.2
	Dec 31 2014 02:10:00:000PM	0.0
	Dec 31 2014 02:15:00:000PM	0.0
	Dec 31 2014 02:20:00:000PM	0.0
	Dec 31 2014 02:25:00:000PM	-0.3
	Dec 31 2014 02:30:00:000PM	-0.3
	Dec 31 2014 02:35:00:000PM	-0.2
	Dec 31 2014 02:40:00:000PM	-0.3
	Dec 31 2014 02:45:00:000PM	-0.4
	Dec 31 2014 02:50:00:000PM	-0.4
	Dec 31 2014 02:55:00:000PM	-0.4
	(12 rows affected)

checking the update
---

After running the update:

		>>> temporary_smash['AIRTEMP_MAX_DAY']['AIRPRI07'][datetime.datetime(2015,4,9,0,0)]
		17.3
		>>> max_data_from_mean['AIRPRI07'][datetime.datetime(2015,4,9,0,0)]
		17.3


		Apr  9 2015 01:20:00:000PM	16.5	16.7
		Apr  9 2015 01:25:00:000PM	16.8	17.1
		Apr  9 2015 01:30:00:000PM	17.0	17.2
		Apr  9 2015 01:35:00:000PM	16.9	17.2
		Apr  9 2015 01:40:00:000PM	16.8	17.0
		Apr  9 2015 01:45:00:000PM	16.6	17.2
		Apr  9 2015 01:50:00:000PM	16.8	17.3
		Apr  9 2015 01:55:00:000PM	17.0	17.3

It works also for the min:

	(Pdb) temporary_smash['AIRTEMP_MIN_DAY']['AIRPRI07'][datetime.datetime(2014,12,31,0,0)]
	-8.0

	(Pdb) temporary_smash['AIRTEMP_MINTIME']['AIRPRI07'][datetime.datetime(2014,12,31,0,0)]
	datetime.datetime(2014, 12, 31, 3, 55)

	Dec 31 2014 03:45:00:000AM	-7.9	NULL
	Dec 31 2014 03:50:00:000AM	-7.9	NULL
	Dec 31 2014 03:55:00:000AM	-8.0	NULL
	Dec 31 2014 04:00:00:000AM	-7.8	NULL
	Dec 31 2014 04:05:00:000AM	-7.9	NULL

	(Pdb) output_dictionary['AIRCEN01'][datetime.datetime(2014,12,31,0,0)]
	{'AIRTEMP_MINTIME': datetime.datetime(2014, 12, 31, 0, 5), 'AIRTEMP_MAXTIME': datetime.datetime(2014, 12, 31, 14, 5), 'AIRTEMP_MIN_FLAG': 'M', 'AIRTEMP_MAX_DAY': 3.4, 'AIRTEMP_MAX_FLAG': 'M', 'AIRTEMP_MEAN_DAY': -3.61, 'AIRTEMP_MIN_DAY': -9.1, 'AIRTEMP_MEAN_FLAG': 'M'}
	(Pdb) output_dictionary['AIRPRI07'][datetime.datetime(2014,12,31,0,0)]
	{'AIRTEMP_MINTIME': datetime.datetime(2014, 12, 31, 3, 55), 'AIRTEMP_MAXTIME': datetime.datetime(2014, 12, 31, 14, 10), 'AIRTEMP_MIN_FLAG': 'A', 'AIRTEMP_MAX_DAY': 0.0, 'AIRTEMP_MAX_FLAG': 'A', 'AIRTEMP_MEAN_DAY': -4.52, 'AIRTEMP_MIN_DAY': -8.0, 'AIRTEMP_MEAN_FLAG': 'A'}
	(Pdb) output_dictionary['AIRCEN01'][datetime.datetime(2015,3,3,0,0)]
	{'AIRTEMP_MINTIME': datetime.datetime(2015, 3, 4, 0, 0), 'AIRTEMP_MAXTIME': datetime.datetime(2015, 3, 3, 15, 30), 'AIRTEMP_MIN_FLAG': 'A', 'AIRTEMP_MAX_DAY': 10.2, 'AIRTEMP_MAX_FLAG': 'A', 'AIRTEMP_MEAN_DAY': 1.84, 'AIRTEMP_MIN_DAY': -1.7, 'AIRTEMP_MEAN_FLAG': 'A'}
	(Pdb)
