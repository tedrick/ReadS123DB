import csv, sqlite3, json, os, sys

#Constants for easy running
IN_DB = r'/Users/jame6423/Documents/Survey123/Utilities/ExtractFromDevice/ArcGIS/My Surveys/Databases/a084e65cd9403d813e8ec667ca329a63.sqlite'
OUT_DIR = r'/Users/jame6423/Documents/Survey123/Utilities/ExtractFromDevice/outSurveys'

def readS123db(inDB=IN_DB):
    conn = sqlite3.connect(inDB)
    cur = conn.cursor()
    surveys = {}
    #conn.text_factory = lambda x: x.decode("utf-16")
    for row in cur.execute('SELECT name, feature from Surveys'):
        print(row)
        print '-----------------'
        surveyName = row[0]
        if surveyName not in surveys.keys():
            surveys[surveyName] = []
        jRow = json.loads(json.loads(row[1]))
        outRow = jRow["attributes"]
        outRow[u"x_geometry"] = jRow["geometry"]["x"]
        outRow[u"y_geometry"] = jRow["geometry"]["y"]
        outRow[u"z_geometry"] = jRow["geometry"]["z"]
        surveys[surveyName].append(outRow)
        #print(outRow)
    return surveys

def writeCSV(surveys, outDir = OUT_DIR):
    import csv
    for surveyName, surveyRows in surveys.items():
        outfile=os.path.join(outDir, 'survey_{0}.csv'.format(surveyName.encode('utf-8').decode('utf-8')))
        outfile = ''.join(outfile.split()).encode('utf-8')
        with open(outfile, 'w') as outFile:
            #get all possible fieldnames
            fieldnames = []
            for row in surveyRows:
                fieldnames.extend(row.keys())
            fieldnamesSet = set(fieldnames)
            fieldnames = list(fieldnamesSet)
            writer = csv.DictWriter(outFile, fieldnames=fieldnames)
#            writer = DictUnicodeWriter(outFile, fieldnames=fieldnames)
            writer.writeheader()
            for row in surveyRows:
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
