'''
Program to create a spreadsheet for each Lodge which has bad contact records

**********************
TO DO


'''

#import csv, sqlite3, os, da_utils, xlrd, re
import csv, sqlite3, os, re
#from sendmail import mailer
# from openpyxl import load_workbook

os.system("cls")

SENDER = "dardron@buckspgl.org"
TEST_RECIPIENT = "derrick.ardron@outlook.com"

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
        reader.__next__()


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
    # print('~112',caller, dbFile, table, pKeyName, pKey, other)
    con = sqlite3.connect(dbFile)
    con.isolation_level = None
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
    if caller == 'addAudit' and pKey == '10155590':
        print('~148 stmt =', stmt)
    cur.execute(stmt)
    con.commit()
    con.close()


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
            email = str(row['Email address'])
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
            ccEmail = str(row['Email address']).strip()
            # we know that the Custom Field 1 holds things other than Gl ref, including P&Tyler's Green so need to handle apostrophe
            if "'" in ccGlRef:
                ccGlRef = ccGlRef.replace("'","''")
            # Does the ccGlRef obtained from Unsubscribes occur in Adelphi
            stmt = 'SELECT "Primary email" from Members where "Gl ref" = \'' + ccGlRef +"'"
            cur.execute(stmt)
            adelphiEmail = str(cur.fetchone()).strip()
            if ccGlRef != '' and adelphiEmail.upper() != 'NONE':
                # there is a none blank ccGlRef and it can be found in Adelphi
                adelphiEmail = adelphiEmail.replace("'",'')
                adelphiEmail = adelphiEmail.replace(",",'')
                adelphiEmail = adelphiEmail.replace("(",'')
                adelphiEmail = adelphiEmail.replace(")",'')
                if adelphiEmail.upper() == ccEmail.upper():
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
                        dataAdder('addUnsubscribes','contacts.db','UnsubsNotAdelphi','Email', ccEmail, GlRef = ccGlRef, FamilyName = str(row['First name']), GivenName = givenName, Occurences = 1, Comment ='Not found in current Bucks Adelphi')
                    else:
                        stmt = 'UPDATE UnsubsNotAdelphi\nSET Occurences = Occurences +1 WHERE Email = \'' + ccEmail +'\''
            elif ccGlRef == '':
                stmt = 'SELECT "GlRef" from Members where "email" = \'' + ccEmail +"'"
                cur.execute(stmt)
                con.commit()
                itemString = str(cur.fetchone())
                if itemString.upper() != 'NONE':
                    # we have a record in unsubsribes without a ccGlRef but a valid email
                    dataAdder('addUnsubscribes','contacts.db','Output','GlRef',itemString, UnsubscribedCC = 'Unsubscribed', UnsubEmail = adelphiEmail)
                else:
                    # we have a record in unsubsribes without a ccGlRef and without a valid email in Adelphi
                    givenName = str(row['Last name'])
                    if "'" in givenName:
                        givenName = givenName.replace("'","''")
                    stmt = 'SELECT "ccEmail" from UnsubsNotAdelphi where "Email" = \'' +ccEmail +"'"
                    cur.execute(stmt)
                    con.commit()
                    itemString = str(cur.fetchone())
                    if itemString.upper() == 'NONE':
                        dataAdder('addUnsubscribes','contacts.db','UnsubsNotAdelphi','Email', ccEmail, GlRef = ccGlRef, FamilyName = str(row['First name']), GivenName = givenName, Occurences = 1, Comment ='Not found in current Bucks Adelphi')
                    else:
                        stmt = 'UPDATE UnsubsNotAdelphi\nSET Occurences = Occurences +1 WHERE Email = \'' + ccEmail +'\''
            else:
                givenName = str(row['Last name'])
                if "'" in givenName:
                    givenName = givenName.replace("'","''")
                stmt = 'SELECT "ccEmail" from UnsubsNotAdelphi where "Email" = \'' +ccEmail +"'"
                cur.execute(stmt)
                con.commit()
                itemString = str(cur.fetchone())
                if itemString.upper() == 'NONE':
                    dataAdder('addUnsubscribes','contacts.db','UnsubsNotAdelphi','Email', ccEmail, GlRef = ccGlRef, FamilyName = str(row['First name']), GivenName = givenName, Occurences = 1, Comment ='Not found in current Bucks Adelphi')
                else:
                    stmt = 'UPDATE UnsubsNotAdelphi\nSET Occurences += 1 WHERE Email = \'' + ccEmail +'\''


def addAudit(dbFile, table, csvFile):
    '''
Adds the Adelphi Audit to the Output Table
    '''
    con = sqlite3.connect(dbFile)
    cur = con.cursor()
    with open(csvFile,mode='r', newline = '') as auditCSV:
        auditCSV.seek(0)
        reader = csv.DictReader(auditCSV)
        newGlRef = 0
        for row in reader:
            glRef = str(row['Gl Ref'])
            data1 = str(row['Data1'])
            data2 = str(row['Data2'])
            if glRef =='10155590':
                print("~297", data1, data2)
            if data1 == 'Memberships My Province':
                m = re.search('L\d{3,4}',data2)
                lodgeID = m.group()
                #print('~296',lodgeID )
                '''
                if m:
                    stmt ='SELECT "GL Ref" FROM Output WHERE "Gl Ref" = \'' + glRef + "'"
                    print('~297',stmt)
                    cur.execute(stmt)
                    con.commit()
                    itemString = str(cur.fetchone())
                    if itemString.upper() == 'NONE':
                        newGlRef = 1

                    #This member is not in Output an so the next lines in which the same GL Ref appears need to be recorded in
                        dataAdder('addAudit','contacts.db','Output','GlRef', glRef, Adelphi_1 = data2)
                '''
            elif data1 == 'Memberships Other Provinces':
                continue
            elif data1 =='':
                continue
            else:
                stmt ='SELECT "Adelphi_1" FROM Output WHERE "GlRef" = \'' + glRef + "'"
                if glRef =='10155590':
                    print("~323 stmt =", stmt)
                cur.execute(stmt)
                con.commit()
                itemString = str(cur.fetchone())
                if glRef =='10155590':
                    print("~326 Adelphi_1 =", itemString)
                if glRef =='10155590':
                    print("~329", data1, data2)
                if itemString.upper() == 'NONE':
                    dataAdder('addAudit','contacts.db','Output','GlRef', glRef,Adelphi_1 = data1+' - ' + data2)
                else:
                    dataAdder('addAudit','contacts.db','Output','GlRef', glRef,Adelphi_2 = data1+' - ' + data2)


'''
            email = str(row['Email address'])
            bounceReason  = str(row['Bounce Reason'])
            dataAdder('addBounces','contacts.db','Output','GlRef',glRef.replace("'",'') , Email = email, EmailBounceReason =  bounceReason)
'''
def output(dbFile, table, fileName):
    con = sqlite3.connect(dbFile)
    cur = con.cursor()
    cur.execute('select * from '+ table)
    colnames = cur.description
    headers =[]
    for row in colnames:
        headers.append(row[0])
        # print(headers)
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
        #print("addAdelphi, "+ stmt)
        cur.execute(stmt)
        data = cur.fetchone()
        if data is not None:
            lodgeID = data[0]
            position = lodgeID.find(',')
            if position > 0:
                lodgeID = lodgeID[:position]
            dataAdder('addAdelphi','contacts.db','Output','GlRef',rowString.replace("'",'') , LodgeID = lodgeID, FamilyName =  data[1], GivenName = data[2])

def get_secretaries():
    '''
    wb = load_workbook(filename = 'secretaries.xlsx')
    print(wb.sheetnames)
    sheet = wb.get_sheet_by_name("USE THIS ONE")
    for row in sheet.iter_rows(min_row=1, min_col=1, max_row=6, max_col=8):
        for cell in row:
            print(cell.value)
    book = xlrd.open_workbook('secretaries.xlsx')
    sheet = sheet_by_name("USE THIS ONE")
    print(book.sheet_names())
'''
def emails():
    '''
    Assembles all the data required by sendmail.py mailer function

    def mailer(sender, recipient, subject, body_text, body_html, attachment):
    '''
    con = sqlite3.connect('contacts.db')
    cur = con.cursor()
    cur2 = con.cursor()
    stmt = 'SELECT "AdelphiCode","LC name","Primary email" FROM secretaries'
    cur.execute(stmt)
    con.commit()
    rows = cur.fetchall()
    for row in rows:
        sec_email = row[2]
        stmt2 = 'SELECT * FROM output where "LodgeID" = \'' + row[0] + "'"
        cur2.execute(stmt2)
        members = cur2.fetchall()
        if members:
            subject = row[0] + ' Provincial Email Failures'
            with open('Contacts Email Text.TXT', 'r') as reader:
                body_text = reader.read()
            with open('Contacts Email Text-2.HTML', 'r') as reader:
                body_html = reader.read()
            with open('temp.csv', 'w', newline ='') as csvfile:
                datawriter = csv.writer(csvfile,delimiter = ",", quotechar = "'")
                datawriter.writerow(['GlRef', 'GivenName', 'FamilyName','Email','Bounced', 'Unsubscribed'])
                for m in members:
                    # 6 =email, either 'No Email' bounced email address
                    # 7 = bounced reason
                    # 8 "Unsubscribed"
                    # 9 UnsubEmail
                    if m[6] or m[7] or m[8] or m[9]:
                        if m[6]:
                            email_to_insert = m[6]
                        elif m[9]:
                            email_to_insert = m[9]
                        else:
                            email_to_insert = ''
                    if m[7]:
                            bounced = m[7]
                    else:
                        bounced = ''
                        unsub = ""
                    if m[9]:
                        unsub = m[8]
                    else:
                        unsub = ""
                    datawriter.writerow([m[0], m[3], m[2],email_to_insert, bounced, unsub])
        mailer(SENDER, sec_email, subject, body_text, body_html, "Bucks PGL Survey v04.pdf", "temp.csv")

    #mailer(SENDER,"..\\password-dardron.txt", TEST_RECIPIENT, subject, body_text, body_html, ["c:\\pyproj\\contacts\\Bucks PGL Survey v04.pdf", "c:\\pyproj\\contacts\\temp.csv"])


def mailer(sender, recipient, subject, body_text, body_html, *attachments):
    print("attachments", attachments)
    import email,smtplib, ssl, os
    from email import encoders
    from email.mime.base import MIMEBase
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    os.system("cls")
    print("Mailer running")
    sender_email = sender
    receiver_email = recipient
    with open('..\\password-dardron.txt', 'r') as reader:
        password = reader.read()
    message = MIMEMultipart("alternative")
    message["Subject"] = subject
    message["From"] = sender
    message["To"] = recipient

    # Turn these into plain/html MIMEText objects
    part1 = MIMEText(body_text, "plain")
    part2 = MIMEText(body_html, "html")
    message.attach(part1)
    message.attach(part2)
    if attachments:
        print("attachments", attachments)
        n=3
        for item in attachments:
            print("item", item)
            # Open PDF file in binary mode
            with open(item, "rb") as attachment:
                print(attachment)

        # Add file as application/octet-stream
        # Email client can usually download this automatically as attachment
                part_number = "part" + str(n)
                part_number = MIMEBase("application", "octet-stream")
                part_number.set_payload(attachment.read())
                # Encode file in ASCII characters to send by email
                encoders.encode_base64(part_number)
                # Add header as key/value pair to attachment part
                part_number.add_header( "Content-Disposition", f"attachment; filename= {item}")
                message.attach(part_number)
                n += 1
    # Add HTML/plain-text parts to MIMEMultipart message
    # The email client will try to render the last part first


    #print(message.as_string())
    # Create secure connection with server and send email
    context = ssl.create_default_context()
    with smtplib.SMTP('smtp-mail.outlook.com', 587) as server:
        server.ehlo()
        server.starttls()
        server.login(sender, password)
        server.sendmail('dardron@buckspgl.org',recipient, message.as_string())






def main():

    csvToDb("advanced_current_members_by_mshp_status.csv","contacts.db","Members")
    #csvToDb("lc_details.csv","contacts.db","Lodges")
    csvToDb('secretaries.csv','contacts.db','secretaries')
    createTable('contacts.db','Output', 'GlRef','GlRef', 'LodgeID','FamilyName', 'GivenName', 'Address', 'Telephone',  'Email', 'EmailBounceReason', 'UnsubscribedCC', 'UnsubEmail', 'Adelphi_1', 'Adelphi_2')
    createTable('contacts.db','UnsubsNotAdelphi', 'Email','GlRef','Email', 'FamilyName', 'GivenName', 'Address', 'Lodge','Occurences','Comment')
    scanMembers('advanced_current_members_by_mshp_status.csv')
    addBounces('contacts.db','Output','bounces.csv')
    addUnsubscribes('contacts.db', 'Unsubscribes.csv')
    addAudit('contacts.db', 'Output', 'Adelphi Audit.csv')
    # Now add basicdata to contacts.db output table

    addAdelphi()
    output('contacts.db', 'output','output.csv')
    output('contacts.db', 'UnsubsNotAdelphi', 'UnsubsNotAdelphi.csv')
    get_secretaries()
    #emails()



if __name__ == '__main__':
    main()
