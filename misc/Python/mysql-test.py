import mysql.connector

# Connect to server
cnx = mysql.connector.connect(
    host="127.0.0.1",
    port=3306,
    user="root",
    password="123456789")

# Get a cursor
cur = cnx.cursor()

# Execute a query
cur.execute("USE Akshay")
cur.execute("select * from Employee")


# Fetch one result
row = cur.fetchall()
for i in row:
    print(i)

# Close connection
cnx.close()