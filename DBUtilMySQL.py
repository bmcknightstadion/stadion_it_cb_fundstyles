'''
Created on Jul 20, 2018

@author: bmcknight
'''
import pymysql
from collections import namedtuple
import os


class Cursor:
    conn = None
    def execute(self,sql):
        with self.conn.cursor() as cursor:
            cursor.execute(sql)
            cols = cursor.description
            colNames = []
            for c in cols:
                colNames.append(c[0])
            result2 = []
            result = cursor.fetchall()
            for r in result:
                Row = namedtuple("Row",colNames)
                newRow = Row(*r)
                result2.append(newRow)
            return result2
    def update(self,sql):
        with self.conn.cursor() as cursor:
            cursor.execute(sql)
            

class MyConn:
    newc = None
    def cursor(self):
        self.newc = Cursor()
        self.newc.conn = self.conn
        return self.newc
    def execute(self,sql):
        self.newc = Cursor()
        self.newc.conn = self.conn
        self.newc.update(sql)
    def commit(self):
        self.newc.conn.commit()
    def close(self):
        self.newc.conn.close()
        
    
def get_db(connStr):
    rds_host = os.environ['rds_host']
    name = os.environ['name']
    password = os.environ['password']
    db_name = "ned2"
    port = 3306
        
    conn = MyConn()
    conn.conn = pymysql.connect(host=rds_host, user=name, passwd=password, db=db_name, connect_timeout=50,charset='utf8mb4',ssl={"fake_flag_to_enable_tls":True})
    return conn

