import mysql.connector
import random
import string

import datetime
import os
import sys


class WorkerDB ():
    def __init__(self):
        self.mydb, self.mycursor = self.connect()

    def connect(self):
        mydb = mysql.connector.connect(
            host=os.environ['DB_HOST'],
            user=os.environ['DB_USER'], 
            passwd=os.environ['DB_PASSWD'], 
            database=os.environ['DB']
        )
        mycursor = mydb.cursor(buffered=True, dictionary=True)
        return mydb, mycursor

    def execute_sql(self, sql, val):
        try:
            print(sql, val)
            self.mycursor.execute(sql, val)
            self.mydb.commit()
        except mysql.connector.errors.DatabaseError as e:
            try:
                print(e)
                self.mycursor.close()
                self.mydb.close()
            except Exception as ee:
                print(ee)
            self.mydb, self.mycursor = self.connect()
            self.mycursor.execute(sql, val)
            self.mydb.commit()
        except Exception as e:
            print(e)
            raise e
        return self.mycursor

    def get_instance_status(self, ip):
        sql = "SELECT status, request_id FROM instances WHERE ip = %s"
        result =  self.execute_sql(sql,(ip,)).fetchone()
        return result['status'], result['request_id']

    def get_request_data(self, request_id):
        sql = "SELECT sha FROM requests WHERE id = %s"
        result =  self.execute_sql(sql,(request_id,)).fetchone()
        return result

    def update_instance_status(self, ip, status):
        sql = "UPDATE instances SET status = %s WHERE ip = %s"
        self.execute_sql(sql, (status, ip))
        