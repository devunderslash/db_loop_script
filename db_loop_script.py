#  script to loop through a list of mysql rds databases, run a query on each one and store the result in multiple variables
#
#  usage: python3 db_query.py
#
#  requires: mysql.connector
#

#  to run this script, you need to first run a venv 
# python -m venv venv
# source venv/bin/activate
# pip install mysql-connector-python

# Add host_dbs.txt with following format
# {
#     'legacydbhostname.c5tte6k5wthk': 'credit-suisse',
#     'host2': 'db2'
# }



import mysql.connector
import sys

# prompt user for input of password
password = input("Enter password: ")

# TODO: update the above to grab password from AWS Parameter Store

# host/database keypairs
# host_dbs = {
#     'legacydbhostname.c5tte6k5wthk': 'credit-suisse',
#     'host2': 'db2'
# }


# get host_dbs key/pairs from host_dbs.txt file and store as dictionary
with open('host_dbs.txt', 'r') as file:
    host_dbs = eval(file.read())
    print(host_dbs)



port = 2750
protocol = "TCP"


# query to run on each database to get the count for the last week in similar format to select count(*) from Interactions where startTime >= unix_timestamp('2022-11-21 00:00:00.000')*1000 and endTime <= unix_timestamp('2022-11-25 23:59:59.999')*1000;
query = "SELECT COUNT(*) FROM Interactions WHERE unix_timestamp BETWEEN DATE_SUB(NOW(), INTERVAL 7 DAY) AND NOW()"

query_result = {}

# loop through each database
for endpoint, db_name in host_dbs.items():
    # connect to the database

    connection_string = endpoint + ".us-east-1.rds.amazonaws.com"
    database = db_name

    # construct mysql connection string, test
    string = "mysql+mysqlconnector://" + connection_string + ":" + str(port) + "/" + database + "?protocol=" + protocol

    print (string)


    try:
        cnx = mysql.connector.connect(user='sa', password='password', host=connection_string, database=database, port=port)
        print(cnx)
    except mysql.connector.Error as err:
        print("Something went wrong: {}".format(err))
        sys.exit(1)

    # run the query
    cursor = cnx.cursor()
    cursor.execute(query)

    # Create a new key value pair dict query_result and store the database name and the count
    # append the response from the query to query_result
 
    query_result[db_name] = cursor.fetchone()[0]

    # print the result
    print(query_result)

    # close the connection
    cursor.close()
    cnx.close()

#  collate the results into a single string
message = ""
for key, value in query_result.items():
    message += key + ": " + str(value) + ","

print (message)

#  With query result post to slack

import json
import requests


def messageBuilderSlack(self, messageTitle, messageSubtitle,messageBodyHeader, messageBodyValue):
    """
    This function builds message format for Slack
    """
    msg = {
        "text": '*' + messageTitle + '*',
        "attachments": [

            {
                "pretext": '*' + messageSubtitle + '*',
                "fields" : [
                    {
                        "title": messageBodyHeader,
                        "value": "```" + messageBodyValue + "```"
                    }
                ]

            }

        ]
    }

    self.__logger.debug(f"Slack Message:\n{json.dumps(msg, indent=3)}")

    return msg

slackMessageBodyValue = (f"DB Name: {db_name} \n Count: {query_result[db_name]}")

messageTitle = "DB Query Result"
messageSubtitle = "DB Query Result"
messageBodyHeader = "DB Query Result"


slack_api_endpoint = 'https://hooks.slack.com/services/XXXXXXXXX/XXXXXXXXX/XXXXXXXXXXXXXXXXXXXXXXXX'



msg = messageBuilderSlack(messageTitle, messageSubtitle, messageBodyHeader, slackMessageBodyValue)

def post_to_slack(message):
    payload = {'text': message}
    requests.post(slack_api_endpoint, data=json.dumps(payload))


#  Post to slack
post_to_slack(query_result)


#  create csv file

import csv

with open('db_query.csv', 'w') as csv_file:
    writer = csv.writer(csv_file)
    for key, value in query_result.items():
       writer.writerow([key, value])

