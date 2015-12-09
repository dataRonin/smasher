Smasher3
========

Smasher3 is a better smasher.
NOTE: This was super rushed code. It WILL break. Yes. It will. The `smash.py` interface HAS NOT been well-tested. In general I have been using the `smasher3.py` main loop, which is found at the end of the script under `if __name__ == "__main__":`.

I have tried and successfully run `smasher3.py` from `smash.py` for MS04301 from 2015-04-10 00:00:00 to current data, on all of MS00521, and then to repopulate MS005 in its entirety. I also attempted to run it on HT004, but there was no high resolution data present.


`smash.py` : the interface part
-------

Here's what you need to do if you use `smash.py` -- pass it up to 4 arguments, with single quotes on all of them.

- 1st is the DBCode (ex. 'MS043')
- 2nd is the DAILY Entity, left padded with a '0' if < 10. (ex. '02')
- 3rd is the start day minus one day (ex. '2015-10-20 00:00:00' - this would start the analysis on 2015-10-21 00:00:00)
- 4th is the end day for the analysis (or if you pass it nothing, it will default to "today", and stop when it stops.)

(The reason for option 3 is because in the automated script, the last detected day has one day added to it, so this takes the same parameter, and it needs to be behind one day of where you want to start)


Ex. of the four-argument style:

		Foxs-MacBook-Pro:smasher dataronin$ python smash.py 'MS005' '21' '2015-01-01 00:00:00' '2015-07-10 00:00:00'


Ex. of the two-argument style:

		Foxs-MacBook-Pro:smasher dataronin$ python smash.py 'MS005'


`smasher3.py` : the main script that you can also use
-------


In the main `smasher3.py` file, scroll to the end, and you can manipulate around line 1200 this section:

		## Required inputs: database and daily table desired, start and end dates of aggregation (or determine from what is there)
		desired_database = 'MS005'
		desired_daily_entity = '01'
		desired_start_day = '2015-01-01 00:00:00'
		desired_end_day = '2015-07-10 00:00:00'

Change those four values to be like what is in the `smash.py` arguments. They are exact matches.

Basically `smash.py` is just a wrapper around these lines of code, with a variable number of arguments.

If the end isn't given, it should use "today".

If the start isn't given, it should use the last given date in the database for the daily, and if that doesn't exist, the whole of the high-resolution, and if that doesn't exist, of course, it will fail.

If the entity isn't given, the whole of that `desired_database` will be processed. Currently, `MS04310` has not been well-tested.

If nothing is given, everything is processed, which will probably take quite a while.

NOTE: The darn reference stands are still in MS04311. MS00512 doesn't seem to have anything in it. HT00411 and HT00451 also appear empty.

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


Reading a Python Traceback (Error thrown that breaks program)
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


An error you may see is `KeyError`. For example, in executing the code, I thought to run on `MS00531`, which of course is NOT a daily entity. Trying to reference this in the daily index, Python could not find it, and rightly called me out:

		Foxs-MacBook-Pro:smasher dataronin$ python smash.py 'MS005' '31' '2015-01-01 00:00:00' '2015-07-10 00:00:00'

Here's Python... Can you understand this Traceback on KeyError?

		Traceback (most recent call last):
		  File "smash.py", line 34, in <module>
		    raw_data, column_names, xt, smashed_template = select_raw_data(cur, database_map, daily_index, hr_methods, daily_methods, desired_database, desired_daily_entity, desired_start_day, desired_end_day)
		  File "/Users/dataronin/Documents/november2015/smasher/smasher3.py", line 186, in select_raw_data
		    hr_entity = daily_index[dbcode][daily_entity]
		KeyError: '31'

Remember to always start from the bottom with Tracebacks. Seriously, that's the easiest way. In this case, the fix was simply to give it the right value it could use!

		Foxs-MacBook-Pro:smasher dataronin$ python smash.py 'MS005' '21' '2015-01-01 00:00:00' '2015-07-10 00:00:00'

And now we are in business...

		"<grunt> SMASHER SMASH DATA! <grunt>"
		You have provided all the inputs: database, entity, start, end
		KEYS ADDED SO FAR TO DAILY DATA:
		dict_keys(['SOIR0201'])


You see this printed output re. the added KEYS. This is because as I am working through the data, it was easier to debug problems specific to certain probes if the error happened after a certain probe got added. I am leaving this print in the code. If you hate it, just `CTRL + F` to get to that line and remove it.

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


Checking the raw data.
---

First of all, it appears the missing midnight issue is still happening in the high-resolution data. You can see it on the SQL server as well as in the code.

Notice that in the SQL server we are missing mid-night here in Python:

	>>> raw_data['AIRPRI07'][datetime.datetime(2014,12,31,0,0)]['AIRTEMP_MEAN'][-1:]
	['-3.6']
	>>> raw_data['AIRPRI07'][datetime.datetime(2015,1,1,0,0)]['AIRTEMP_MEAN'][0]
	-3.7

Corresponding to the same missing midnight in SQL server.

		date_time	airtemp_mean
		Dec 31 2014 11:55:00:000PM	-3.6
		Jan  1 2015 12:05:00:000AM	-3.7


AIRTEMP mean, min, max, maxtime, mintime, etc.
----

Checking the raw data versus the code has only been done marginally, but I have found it very effective to use the daily max and min as checks, since those are easy to find in SQL with raw SQL code and also easy to compute. I admit not all of this code has been checked vs. the database, and more checks are great. First, here's the data we will have to work with.

		>>> raw_data['AIRPRI07'][datetime.datetime(2015,1,1,0,0)].keys()
		dict_keys(['date_time', 'height', 'AIRTEMP_METHOD', 'AIRTEMP_MEAN', 'critical_flag', 'critical_value', 'AIRTEMP_MEAN_FLAG', 'db_table', 'sitecode', 'AIRTEMP_MIN_FLAG', 'AIRTEMP_MIN', 'AIRTEMP_MAX', 'depth', 'AIRTEMP_MAX_FLAG'])


For example, here's the average from a pretty chilly day on AIRPRI07. The behavior is as desired: We want that the final value on 12-31-2014 23:55:00 is attributed to that day, 2014-12-31, so while Python shows this date time of `datetime.datetime(2014,12,31,0,0)` that really means the `2400` hour.


In Python: (from the `mean_data_from_mean` function's output of `data`)

		>>> data['AIRPRI07'][datetime.datetime(2014,12,31,0,0)]
		-4.52

In SQL:

		1> select avg(AIRTEMP_MEAN) from lterlogger_pro.dbo.ms04311 where probe_code like 'airpri07' and date_time >='2014-12-31 00:00:00' and date_time<'2014-12-31 23:55:00'
		2> go

		-4.520069

Checking the max, similarly, in Python, from the `max_data_from_mean` function's output of `data`:

		>>> data['AIRPRI07'][datetime.datetime(2014,12,31,0,0)]
		0.0

And in SQL.

		1> select max(AIRTEMP_MEAN) from lterlogger_pro.dbo.ms04311 where probe_code like 'airpri07' and date_time >='2014-12-31 00:00:00' and date_time<'2014-12-31 23:55:00'
		2> go

		0.0


The maxtime must be found by indexing the string of air temperature values to the max of those values, and then matching that time to the corresponding `date_time` attribute from the SQL. This is by far the buggiest, most annoying function to work through, because the indices must match both in memory and in the data. For example, if the data is brought in optimally, as a number, Python must also store it as that same number with that same precision. If it comes in as a string, we must test that string, which must be the same character match with the same precision. There are about 7 levels of exceptions programmed into the `daily_functions.py` file under `daily_functions_normal` to deal with this. However, it still may not work in the future. Really, it's a very annoying thing to detect!

But, when it works; it works.

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


Updating the `MAX` to reflect the max from the `AIRTEMP_MAX` versus the `AIRTEMP_MEAN`
---

In most cases, this is sort of hard to do. We don't know when the probes changed, which ones changed, and if there is values in the MAX column that are real, or not! Maybe the NULLS are okay. Maybe they aren't. However, we can start with the assumption that daily values in the MAX that are not NULL are real. Here I show the sql with the AIRTEMP MEAN on the left and the max on the right, and below it the Python after the update.


		Apr  9 2015 01:20:00:000PM	16.5	16.7
		Apr  9 2015 01:25:00:000PM	16.8	17.1
		Apr  9 2015 01:30:00:000PM	17.0	17.2
		Apr  9 2015 01:35:00:000PM	16.9	17.2
		Apr  9 2015 01:40:00:000PM	16.8	17.0
		Apr  9 2015 01:45:00:000PM	16.6	17.2
		Apr  9 2015 01:50:00:000PM	16.8	17.3
		Apr  9 2015 01:55:00:000PM	17.0	17.3

After running the update:

		>>> temporary_smash['AIRTEMP_MAX_DAY']['AIRPRI07'][datetime.datetime(2015,4,9,0,0)]
		17.3
		>>> max_data_from_mean['AIRPRI07'][datetime.datetime(2015,4,9,0,0)]
		17.3


It works also for the min:

		>>>temporary_smash['AIRTEMP_MIN_DAY']['AIRPRI07'][datetime.datetime(2014,12,31,0,0)]
		-8.0

		>>>temporary_smash['AIRTEMP_MINTIME']['AIRPRI07'][datetime.datetime(2014,12,31,0,0)]
		datetime.datetime(2014, 12, 31, 3, 55)

		Dec 31 2014 03:45:00:000AM	-7.9	NULL
		Dec 31 2014 03:50:00:000AM	-7.9	NULL
		Dec 31 2014 03:55:00:000AM	-8.0	NULL
		Dec 31 2014 04:00:00:000AM	-7.8	NULL
		Dec 31 2014 04:05:00:000AM	-7.9	NULL


The helper files
----

There are two files that "help" `smasher3.py`. One is `if_none.py`, which contains all the mathematical functions to compute the various daily attributes, with lots of exceptions for handling nones and nulls and NaN's and all of that. The other is `daily_functions.py`, which essentially contains 3 tools for mapping the `if_none` functions onto a stream of data or multiple streams of data, and matching up dates and date times if necessary. There are really 4 forms of mapping -- "normal", which is one function to one stream of data, "VPD" which needs to have specific airtemp and relhum prefixes, "wind" which may have two attributes of speed and direction, or just one of direction, and "sonic", which needs the same functions as "wind", but has more outputs explicitly processed and therefore doesn't have the same sources.


More information later!