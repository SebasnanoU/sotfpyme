import mysql.connector
from mysql.connector import errorcode

# Obtain connection string information from the portal

config = {
    "host": "<host.name>",
    "user": "<username>",
    "password": "<password>",
    "database": "mysql",
}

# Construct connection string

try:
    conn = mysql.connector.connect(**config)
    print("Connection established")
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with the user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
else:
    cursor = conn.cursor()

cursor.execute("select user from mysql.user;")

my_users = cursor.fetchall()
list_users = []
for (user) in my_users:
    list_users.append(user[0])
cursor.execute('SHOW TABLES;')
table_names = []
for record in cursor.fetchall():
    table_names.append(record[0])



