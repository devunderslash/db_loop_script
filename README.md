# db_loop_script
Script to loop around hosts and db names and get a count from Interactions

script to loop through a list of mysql rds databases, run a query on each one and store the result in multiple variables

usage: python3 db_query.py

requires: mysql.connector

- to run this script, you need to first run a venv 
python -m venv venv
source venv/bin/activate
pip install mysql-connector-python

- Add host_dbs.txt with following format
{
    'legacydbhostname.c5tte6k5wthk': 'credit-suisse',
    'host2': 'db2'
}
