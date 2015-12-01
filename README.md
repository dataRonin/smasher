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