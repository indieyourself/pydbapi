# -*- coding: utf-8 -*-
'''
    Some wappers for python [synchronous] database operation
    
    Tested with PyMysql
'''

import sys

def runInteraction(conn, interaction, *args):
    """
    A logic execution unit wapper with transaction support naturally. 
    Condition: 1. autocommit: off .
    
    Example:
        def interaction( txn, name, age):
            sql = "select * from user where name=%s and age=%s"
            txn.execute( sql, (name, age))
            return txn.fetchall()
    """
    trans = conn.cursor()
    try:
        result = interaction(trans, *args)
        trans.close()
        conn.commit()
        return result
    except:
        excType, excValue, excTraceback = sys.exc_info()
        try:
            conn.rollback()
        except:
            pass
        raise excType, excValue, excTraceback
        
def runInsertWithId(conn, interaction, *args):
    """
    Run insert sql, and return the id
    """
    runInteraction(conn, interaction, *args)
    return conn.insert_id()

def runUpdateWithUpdateRows(self, conn, interaction, *args, **kw):
    """
     You may need the affected rows count when implement Optimistic Lock with select - update.
    """
    rowCount = runInteraction(conn, interaction, args, kw)
    return rowCount
    
def getModifiedSql(conn, sql, *args):
    """
    Get escaped sql
    """
    return conn.cursor().mogrify(sql, *args)

"""
Simple ORM interaction api wrapper
"""
def insert(txn, tableName, dataDict):
    keys = dataDict.keys()
    values = dataDict.values()
    bindParam = ",".join( [ "%s"  for i in len(keys) ] )
    fields = ",".join( [ i for i in keys ] )
    sql = "insert into {TABLENAME}(FIELDS) values({BIND_PARAM})".format(TABLENAME=tableName, FIELDS=fields, BIND_PARAM = bindParam )
    return txn.execute(sql, tuple(values))




