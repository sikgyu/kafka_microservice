import sqlite3

conn = sqlite3.connect('readings.sqlite')
c = conn.cursor()

c.execute('''
          CREATE TABLE delivery_order
          (id INTEGER PRIMARY KEY ASC, 
           total INTEGER NOT NULL,
           driverName VARCHAR(250) NOT NULL,
           remainingTime INTEGER NOT NULL,
           address VARCHAR(100) NOT NULL,
           items VARCHAR(200) NOT NULL,
           orderTime VARCHAR(100) NOT NULL)
          ''')

c.execute('''
          CREATE TABLE pickup_order
          (id INTEGER PRIMARY KEY ASC, 
           total VARCHAR(250) NOT NULL,
           pickupPlace VARCHAR(250) NOT NULL,
           cookReady INTEGER NOT NULL,
           items VARCHAR(200) NOT NULL,
           orderTime VARCHAR(100) NOT NULL)
          ''')

conn.commit()
conn.close()
