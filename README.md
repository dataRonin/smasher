Smasher3
---------

some good things


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


hr_methods and daily_methods
---

		>>> hr_methods['RELH1502']
		{0: {'critical_value': 95, 'height': '150', 'resolution': '15 minutes', 'sitecode': 'H15MET', 'dtb': datetime.datetime(2013, 1, 31, 11, 0), 'critical_flag': 'F', 'dte': datetime.datetime(2050, 12, 31, 0, 0), 'depth': '0', 'method_code': 'REL014'}}

		>>> daily_methods['RELH1502']
		{0: {'height': '150', 'sitecode': 'H15MET', 'dtb': datetime.datetime(2013, 2, 1, 0, 0), 'dte': datetime.datetime(2050, 12, 31, 0, 0), 'depth': '0', 'method_code': 'REL314'}}


database_map
----


tables are ok
---


raw_data checks
---

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