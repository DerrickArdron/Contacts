def createTable(dbFile, tableName, primaryKey, *cols):
    con = sqlite3.connect(dbFile)
    cur = con.cursor()

    stmt = 'Drop TABLE IF EXISTS "'+ tableName +'"'
    cur.execute(stmt)
    con.commit()

    stmt = "create table if not exists \"" + tableName + "\" (%s)" % ",".join(cols)
    stmt = stmt[:-1]
    stmt = stmt + ', CONSTRAINT glRefPK PRIMARY KEY (' +primaryKey +'))'
    with open("createTable.txt","w") as txtFile:
        txtFile.write(stmt)
        txtFile.close()
    cur.execute(stmt)
    con.commit()

    '''
    stmt = "DELETE from " +tableName
    cur.execute(stmt)
    con.commit()
    con.close()
    '''


def dataAdder(caller,dbFile, table, pKeyName,pKey, **other):
    # print('~112',caller, dbFile, table, pKeyName, pKey, other)
    con = sqlite3.connect(dbFile)
    cur = con.cursor()
    keyStr = ''
    valueStr = ''
    valueList = []
    for key in other:
        keyStr = keyStr + ', '+ key
        valueList.append(str(other[key]))
    for item in valueList:
        valueStr = valueStr + '\''+item +'\','
    valueStr = valueStr[:-1]
    stmt = 'SELECT "GlRef" from Output where "GlRef" = \'' + pKey +"'"
    cur.execute(stmt)
    con.commit()
    itemString = str(cur.fetchone())
    if itemString.upper() == 'NONE':
        stmt = 'INSERT INTO ' +table+'(' + pKeyName + keyStr +')\nVALUES (\''+ pKey + '\', ' +valueStr + ')'

    else:
        subStmt = ""
        for key in other:
            value = "{value}".format(value = other[key])
            if "'" in value:
                value = value.replace("'","''")
            subStmt = subStmt + key + ' = \'' + value +'\','
        subStmt = subStmt[:-1]
        stmt = 'UPDATE Output\nSET '+subStmt +'\nWHERE GlRef = \'' + pKey +'\''
    #print('stmt ~138', stmt)
    cur.execute(stmt)
    con.commit()
