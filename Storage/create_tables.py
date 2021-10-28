import mysql.connector

db_conn = mysql.connector.connect(host="sean-kafka.eastus2.cloudapp.azure.com", user="user",
password="password", database="events")

db_cursor = db_conn.cursor()

db_cursor.execute('''
          CREATE TABLE student_account
          (id INT NOT NULL AUTO_INCREMENT, 
           fullname VARCHAR(50) NOT NULL,
           username VARCHAR(100) NOT NULL,
           password VARCHAR(100) NOT NULL,
           final_grade INTEGER NOT NULL,
           timestamp VARCHAR(100) NOT NULL,
           date_created VARCHAR(100) NOT NULL,
           CONSTRAINT student_account_pk PRIMARY KEY (id))
          ''')

db_cursor.execute('''
          CREATE TABLE instructor_account
          (id INT NOT NULL AUTO_INCREMENT, 
           fullname VARCHAR(50) NOT NULL,
           username VARCHAR(100) NOT NULL,
           password VARCHAR(100) NOT NULL,
           math VARCHAR(50) NOT NULL,
           python VARCHAR(50) NOT NULL,
           timestamp VARCHAR(100) NOT NULL,
           date_created VARCHAR(100) NOT NULL,
           CONSTRAINT instructor_account_pk PRIMARY KEY (id))
          ''')

db_conn.commit()
db_conn.close()
