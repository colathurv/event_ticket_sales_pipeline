# What does this Repo contain ?

Basic prototype that leverages the python mysql connector to do the following:
1. Execute necessary DDLs to create a new user, database and a Sales event table in MySQL
2. Bulk Load the Sales Event CSV to the Sales event table.
3. Report the Top 3 events based on the tickets sold. 

# How does one run it ?

Enter the root username and password in db.conf. Then run the following.

`python3 event_system_pipeline.py`

Note: 
1. The host, port, dbname and the query user credentials can be changed if necessary in db.conf.
2. Refer to the log file called pipeline.log
