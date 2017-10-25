import csv, sqlite3, json, os, sys

#Constants for easy running
IN_DB = r'./a084e65cd9403d813e8ec667ca329a63.sqlite'
OUT_DIR = r'./out'

def readS123db(inDB=IN_DB):
    conn = sqlite3.connect(inDB)
    cur = conn.cursor()
    surveys = {}
    #conn.text_factory = lambda x: x.decode("utf-16")
    #Status indicates which 'box' teh Survey is in
    #0 - Drafts
    #1 - Outbox
    #2 - Sent
    #3 - Submission Error
    #4 - Inbox
    for row in cur.execute('SELECT name, feature, status from Surveys where status = 1 or status = 3'):
        print(row)
        print ('-----------------')
        surveyName = row[0]
        if surveyName not in surveys.keys():
            surveys[surveyName] = {"adds":[], "updates":[]}
        jTransaction = json.loads(json.loads(row[1]))[0]
        print(jTransaction)
        #Case for Adds
        if "adds" in jTransaction.keys():
            jRow = jTransaction["adds"][0]
            outRow = jRow["attributes"]
            #Add Geometry
            outRow[u"x_geometry"] = jRow["geometry"]["x"]
            outRow[u"y_geometry"] = jRow["geometry"]["y"]
            outRow[u"z_geometry"] = jRow["geometry"]["z"]
            #Add Attachments
            if "attachments" in jTransaction:
                jAttach = jTransaction["attachments"]
                for jAttRow in jAttach:
                    for jAtt in jAttRow:
                        outRow[jAtt["fieldName"]] = jAtt["fileName"]
            surveys[surveyName]["adds"].append(outRow)
        if "updates" in jTransaction.keys():
            jRow = jTransaction["updates"][0]
            outRow = jRow["attributes"]
            #Add Geometry
            outRow[u"x_geometry"] = jRow["geometry"]["x"]
            outRow[u"y_geometry"] = jRow["geometry"]["y"]
            outRow[u"z_geometry"] = jRow["geometry"]["z"]
            if "attachments" in jTransaction:
                jAttach = jTransaction["attachments"]
                for jAttRow in jAttach:
                    for jAtt in jAttRow:
                        outRow[jAtt["fieldName"]] = jAtt["fileName"]
            surveys[surveyName]["adds"].append(outRow)
        #print(outRow)
    return surveys

def writeCSV(surveys, outDir = OUT_DIR):
    import csv
    for surveyName, surveyTransactions in surveys.items():
        surveyAdds = surveyTransactions["adds"]
        addfile = os.path.join(outDir, 'survey_{0}_adds.csv'.format(surveyName.encode('utf-8').decode('utf-8')))
        surveyUpdates = surveyTransactions["updates"]
        updatefile = os.path.join(outDir, 'survey_{0}_updates.csv'.format(surveyName.encode('utf-8').decode('utf-8')))

#        outfile=os.path.join(outDir, 'survey_{0}.csv'.format(surveyName.encode('utf-8').decode('utf-8')))
#        outfile = ''.join(outfile.split()).encode('utf-8')
        with open(addfile, 'w') as outFile:
            #get all possible fieldnames
            fieldnames = []
            for row in surveyAdds:
                fieldnames.extend(row.keys())
            fieldnamesSet = set(fieldnames)
            fieldnames = list(fieldnamesSet)
            writer = csv.DictWriter(outFile, fieldnames=fieldnames)
#            writer = DictUnicodeWriter(outFile, fieldnames=fieldnames)
            writer.writeheader()
            for row in surveyAdds:
                #print(u'{0}\t{1}'.format(surveyName, row))
                writer.writerow(row)
        with open(updatefile, 'w') as outFile:
            #get all possible fieldnames
            fieldnames = []
            for row in surveyUpdates:
                fieldnames.extend(row.keys())
            fieldnamesSet = set(fieldnames)
            fieldnames = list(fieldnamesSet)
            writer = csv.DictWriter(outFile, fieldnames=fieldnames)
#            writer = DictUnicodeWriter(outFile, fieldnames=fieldnames)
            writer.writeheader()
            for row in surveyUpdates:
                #print(u'{0}\t{1}'.format(surveyName, row))
                writer.writerow(row)

if __name__ == '__main__':
    inDB = IN_DB
    outDir = OUT_DIR
    if len(sys.argv) > 1:
        inDB = sys.argv[1]
        outDirectory = sys.argv[2]
    surveys = readS123db(inDB)
    writeCSV(surveys, outDir)
