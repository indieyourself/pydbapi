"""
Test for pydbapi
table: create table user(id int auto_increment, name varchar(64), age int, primary key(id));
"""
import pymysql
import pydbapi

dbConfig = {
      "host": "127.0.0.1",
      "port": 3306,
      "user": "root",
      "password": "abc61409",
      "db": "test",
      "charset": "utf8",
}
dbConnection = None
tableName = "user"

def testInsert( txn ):
	dataDict = {
		"name": "hanwenfang",
		"age": 28
	}
	return pydbapi.insert(txn, tableName, dataDict)

def testSelect(txn):
	return pydbapi.select(txn, tableName, ["name"], {"age":28})

def testUpdate(txn):
	return pydbapi.update(txn, tableName, {"name": "Mr.X"}, {"id": 1)

def testDelete(txn):
	return pydbapi.delete(txn, tableName, {"id": 1})

def main():
	global dbConnection
	dbConnection = pymysql.connect( host = dbConfig['host'], port=dbConfig["port"], user=dbConfig['user'], 
                                                                         password=dbConfig['password'], db=dbConfig['db'], charset=dbConfig['charset'],
                                                                         cursorclass=pymysql.cursors.DictCursor);
	print pydbapi.runInteraction( dbConnection, testInsert)
	print pydbapi.runInteraction( dbConnection, testSelect)
	print pydbapi.runInteraction( dbConnection, testUpdate)
	print pydbapi.runInteraction( dbConnection, testDelete)

if __name__ == "__main__":
	main()
