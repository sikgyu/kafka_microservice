import mysql.connector
db_conn = mysql.connector.connect(host="sean-kafka.eastus2.cloudapp.azure.com", user="user",
password="password", database="events")
db_cursor = db_conn.cursor()

db_cursor.execute('''
 DROP TABLE delivery_order, pickup_order
''')
db_conn.commit()
db_conn.close()
