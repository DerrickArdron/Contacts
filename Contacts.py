'''
Program to create a spreadsheet for each Lodge which has bad contact records

**********************
TO DO


'''

import csv, sqlite3


def _get_col_datatypes(fin):
    dr = csv.DictReader(fin) # comma is default delimiter
    fieldTypes = {} # a
    for entry in dr:
        fieldsLeft = [f for f in dr.fieldnames if f not in fieldTypes.keys()]
        if not fieldsLeft: break # We're done
        for field in fieldsLeft:
            data = entry[field]
        # Need data to decide
        if len(data) == 0:
            #continue
            fieldTypes[field] = "TEXT"

        if data.isdigit():
            fieldTypes[field] = "INTEGER"
        else:
            fieldTypes[field] = "TEXT"
    # TODO: Currently there's no support for DATE in sqllite

    if len(fieldsLeft) > 0:
        raise Exception("Failed to find all the columns data types - Maybe some are empty?")

    return fieldTypes


def escapingGenerator(f):
    for line in f:
        yield line.encode("ascii", "xmlcharrefreplace").decode("ascii")


def csvToDb(csvFile,dbFile,tablename, outputToFile = False):

    with open(csvFile,mode='r', encoding="ISO-8859-1") as fin:
        dt = _get_col_datatypes(fin)

        fin.seek(0)

        reader = csv.DictReader(fin)

        # Keep the order of the columns name just as in the CSV
        fields = reader.fieldnames
        cols = []

        # Set field and type
        for f in fields:
            cols.append("\"%s\" %s" % (f, 'TEXT'))
            #cols.append("\"%s\" %s" % (f, dt[f]))

        # Generate create table statement:
        stmt = "create table if not exists \"" + tablename + "\" (%s)" % ",".join(cols)
        with open("csvToDb.txt","w") as txtFile:
            txtFile.write(stmt)
            txtFile.close()
        con = sqlite3.connect(dbFile)
        cur = con.cursor()
        cur.execute(stmt)
        con.commit()

        stmt = "DELETE from " +tablename
        cur.execute(stmt)

        fin.seek(0)


        reader = csv.reader(escapingGenerator(fin))

        # Generate insert statement:
        stmt = "INSERT INTO \"" + tablename + "\" VALUES(%s);" % ','.join('?' * len(cols))

        cur.executemany(stmt, reader)
        con.commit()
        con.close()

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
    print('~112',caller, dbFile, table, pKeyName, pKey, other)
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


def scanMembers(csvFile):
    with open(csvFile,mode='r', newline = '') as membersCSV:
        membersCSV.seek(0)
        reader = csv.DictReader(membersCSV)
        for row in reader:
            glRef = str(row['Gl ref'])
            address = ''
            email = ''
            telephone = ''
            stmtBase = "'scanMembers','contacts.db','Output','GlRef', glRef,"
            address1 = str(row['Address1'])
            if address1.upper() == 'UNKNOWN' :
                address = 'UNKNOWN'
            email = str(row['Primary email']).strip()
            if email == '' :
                email = 'No Email'
            phone = str(row['Primary phone']).strip()
            mobile = str(row['Primary mobile']).strip()
            if phone == '' and mobile =='':
                 telephone = 'No Phone'
            if address =='UNKNOWN' or email =='No Email' or telephone == 'No Phone':
                if address == 'UNKNOWN':
                    dataAdder('scanMembers','contacts.db','Output','GlRef', glRef,Address = address)
                if email == 'No Email':
                    dataAdder('scanMembers','contacts.db','Output','GlRef', glRef,Email = email)
                if telephone == 'No Phone':
                    dataAdder('scanMembers','contacts.db','Output','GlRef', glRef,Telephone = telephone)


def addBounces(dbFile, table, csvFile):
    with open(csvFile,mode='r', newline = '') as bouncesCSV:
        bouncesCSV.seek(0)
        reader = csv.DictReader(bouncesCSV)
        for row in reader:
            glRef  = str(row['Custom Field 1'])
            email = str(row['Email address - other'])
            bounceReason  = str(row['Bounce Reason'])
            dataAdder('addBounces','contacts.db','Output','GlRef',glRef.replace("'",'') , Email = email, EmailBounceReason =  bounceReason)
            '''
            ToDo his is having issues with Gl Ref 1123459
            '''


def addUnsubscribes(dbFile, csvFile):
    '''
    Scan the file and the possibilities are:
        IF. there is a Gl ref which can be found in Adelphi, and
            IF it has the same email as Adelphi
                THEN record in contacts.db output table email and unsubscribed
            ELSE
                Record it in UnsubsNotAdelphi
        ELSE it has no currently valid Gl ref
            IF the ccEmail can be found in Members
                THEN ADD to output (glRef, email, unsubscribed)
            ELSE
                Record it in UnsubsNotAdelphi

    '''
    con = sqlite3.connect(dbFile)
    cur = con.cursor()
    with open(csvFile,mode='r', newline = '') as source:
        source.seek(0)
        reader = csv.DictReader(source)
        for row in reader:
            ccGlRef = str(row['Custom Field 1']).strip()
            ccEmail = str(row['Email address - other']).strip()
            # we know that the Custom Field 1 holds things other than Gl ref, including P&Tyler's Green so need to handle apostrophe
            if "'" in ccGlRef:
                ccGlRef = ccGlRef.replace("'","''")
            # Is the ccGlRef obtained from Unsubscribes in Adelphi
            stmt = 'SELECT "Primary email" from Members where "Gl ref" = \'' + ccGlRef +"'"
            cur.execute(stmt)
            adelphiEmail = str(cur.fetchone()).strip()
            if ccGlRef != '' and adelphiEmail.upper() != 'NONE':

                '''
                TO DO TO DO TO DO
                What about
                The ccGlRef we have from Unsubscribes is
                    1. Not Blank, and
                    2. Has been found in Adelphi
                '''
                adelphiEmail = adelphiEmail.replace("'",'')
                adelphiEmail = adelphiEmail.replace(",",'')
                adelphiEmail = adelphiEmail.replace("(",'')
                adelphiEmail = adelphiEmail.replace(")",'')
                if adelphiEmail == ccEmail:
                    dataAdder('addUnsubscribes','contacts.db','Output','GlRef',ccGlRef, UnsubscribedCC = 'Unsubscribed', UnsubEmail = adelphiEmail)
                else:
                    givenName = str(row['Last name'])
                    if "'" in givenName:
                        givenName = givenName.replace("'","''")
                    stmt = 'SELECT "ccEmail" from UnsubsNotAdelphi where "Email" = \'' +ccEmail +"'"
                    cur.execute(stmt)
                    con.commit()
                    itemString = str(cur.fetchone())
                    if itemString.upper() == 'NONE':
                        dataAdder('addUnsubscribes','contacts.db','UnsubsNotAdelphi','Email', ccEmail, FamilyName = str(row['First name']), GivenName = givenName, Occurences = 1, Comment ='Not found in current Bucks Adelphi')
                    else:
                        stmt = 'UPDATE UnsubsNotAdelphi\nSET Occurences = Occurences +1 WHERE Email = \'' + ccEmail +'\''


def output(dbFile, table, fileName):
    con = sqlite3.connect(dbFile)
    cur = con.cursor()
    cur.execute('select * from '+ table)
    colnames = cur.description
    headers =[]
    for row in colnames:
        headers.append(row[0])
        print(headers)
    with open(fileName, 'w', newline = '') as outCSV:
        outWriter = csv.writer(outCSV)
        outWriter.writerow(headers)
        stmt = 'SELECT * FROM ' + table
        cur.execute(stmt)
        rows = cur.fetchall()
        for row in rows:
            outWriter.writerow(row)
        outCSV.close()


def addAdelphi():
    '''
    This serves to add data to contacts.db output table
    '''
    con = sqlite3.connect('contacts.db')
    cur = con.cursor()
    stmt = 'SELECT "GlRef" FROM Output'
    cur.execute(stmt)
    rows = cur.fetchall()
    for row in rows:
        rowString = str(row)
        rowString = rowString.replace('(', '')
        rowString = rowString.replace(')', '')
        rowString = rowString.replace(',', '')
        stmt = 'SELECT "Subscr mshps my prov", "Family name", "Given name" FROM Members WHERE "Gl Ref" = ' + rowString
        cur.execute(stmt)
        data = cur.fetchone()
        lodgeID = data[0]
        position = lodgeID.find(',')
        if position > 0:
            lodgeID = lodgeID[:position]
        dataAdder('addAdelphi','contacts.db','Output','GlRef',rowString.replace("'",'') , LodgeID = lodgeID, FamilyName =  data[1], GivenName = data[2])



def main():
    csvToDb("advanced_current_members_by_mshp_status.csv","contacts.db","Members")
    #csvToDb("lc_details.csv","contacts.db","Lodges")
    createTable('contacts.db','Output', 'GlRef','GlRef', 'LodgeID','FamilyName', 'GivenName', 'Address', 'Telephone',  'Email', 'EmailBounceReason', 'UnsubscribedCC', 'UnsubEmail')
    createTable('contacts.db','UnsubsNotAdelphi', 'Email','Email', 'FamilyName', 'GivenName', 'Address', 'Lodge','Occurences','Comment')
    scanMembers('advanced_current_members_by_mshp_status.csv')
    addBounces('contacts.db','Output','bounces.csv')
    addUnsubscribes('contacts.db','Unsubscribes.csv')
    addAdelphi()
    output('contacts.db', 'output','output.csv')
    output('contacts.db', 'UnsubsNotAdelphi','UnsubsNotAdelphi.csv')


if __name__=='__main__':
    main()
