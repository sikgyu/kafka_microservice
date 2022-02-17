import mysql.connector
db_conn = mysql.connector.connect(host="sean-kafka.eastus2.cloudapp.azure.com", user="user",
                                  password="password", database="events")
db_cursor = db_conn.cursor()


db_cursor.execute('''
CREATE TABLE delivery_order
          (id INTEGER PRIMARY KEY AUTO_INCREMENT, 
           total INTEGER NOT NULL,
           driverName VARCHAR(250) NOT NULL,
           remainingTime INTEGER NOT NULL,
           address VARCHAR(100) NOT NULL,
           items VARCHAR(200) NOT NULL,
           orderTime VARCHAR(100) NOT NULL)
 ''')
db_cursor.execute('''
          CREATE TABLE pickup_order
          (id INTEGER PRIMARY KEY AUTO_INCREMENT, 
           total VARCHAR(250) NOT NULL,
           pickupPlace VARCHAR(250) NOT NULL,
           cookReady INTEGER NOT NULL,
           items VARCHAR(200) NOT NULL,
           orderTime VARCHAR(100) NOT NULL)
 ''')
db_conn.commit()
db_conn.close()
