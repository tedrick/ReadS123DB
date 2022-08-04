'''readDb.py - A script to read the Survey123 database and export the records as CSV files
Usage: python readDb.py [input sqlite file] [directory to write the CSV files to]

Built-in globals will set the parameters to: 
- the Survey123 DB copied into the same directory as where this script is run 
- a directory called 'out' where this script is run
'''
import csv, sqlite3, json, os, sys, uuid

# Constants for easy running
IN_DB = r'./a084e65cd9403d813e8ec667ca329a63.sqlite'
OUT_DIR = r'./out'

def shape2WKT(in_shape_data):
    # Read coordinates and return WKT
    # print(in_shape_data)
    out_text = ""
    if "rings" in in_shape_data:
        # Polygons are lists of a list of coordinates
        ring_texts = []
        for ring in in_shape_data["rings"]:
            out_text = out_text + "("
            coordinate_text = ",".join([" ".join([str(value) for value in coordinate]) for coordinate in ring])
            ring_text = "({0})".format(coordinate_text)
            ring_texts.append(ring_text)
        out_text = "POLYGON ({0})".format("".join(ring_texts))
        pass
    elif "paths" in in_shape_data:
        # Lines are a list of coordinates; Survey123 stores as if multiple segments are supported but only uses 1
        path0 = in_shape_data["paths"][0]
        coordinate_text = ",".join([" ".join([str(value) for value in coordinate]) for coordinate in path0])
        out_text = "LINESTRING ({0})".format(coordinate_text)
        pass
    else:
        # Assume point
        out_text = "POINT ({0} {1}".format(str(in_shape_data["x"]), str(in_shape_data["y"]))
        if "z" in in_shape_data and in_shape_data["z"]:
            out_text = out_text + " " + str(in_shape_data["z"])
        out_text = out_text + ")"
    # print(out_text)
    return out_text

def read_data(indata, parentglobalid=""):
    # Dedicated function to read data.
    outData = []
    for parentTable, parentValuesDict in indata.items():
        print(parentTable)
        print(parentValuesDict)
        outFeature = {"editMode": 0, "table": parentTable, "data":{}}
        globalIDfield = "globalid"
        if "__meta__" in parentValuesDict:
            globalIDfield = parentValuesDict["__meta__"]["globalIdField"]
            outFeature["editMode"] = parentValuesDict["__meta__"]["editMode"]
        identifier = ""
        if globalIDfield in parentValuesDict.keys():
            identifier = parentValuesDict[globalIDfield]
        else:
            identifier = str(uuid.uuid4())
            outFeature["data"][globalIDfield] = identifier
        if parentglobalid != "":
            outFeature["data"]["parentglobalid"] = parentglobalid
        for fieldName, fieldValue in parentValuesDict.items():
            # Test the key to see if it's a normal attribute, select_multiple (list), geometry (dict), repeat (list), or metadata (dict)
            if isinstance(fieldValue, dict):
                # process geometry
                if "spatialReference" in fieldValue.keys():
                    wkt = shape2WKT(fieldValue)
                    outFeature["data"][fieldName] = wkt
                    # Additional helper for points - seperate x/y/z fields
                    if "x" in fieldValue.keys():
                        outFeature["data"][u"x_geometry"] = fieldValue["x"]
                        outFeature["data"][u"y_geometry"] = fieldValue["y"]
                        if "z" in fieldValue.keys():
                            outFeature["data"][u"z_geometry"] = fieldValue["z"]
            elif isinstance(fieldValue, list) and len(fieldValue) > 0:
                # Repeat - iterate through the repeats to generate their own records
                if isinstance(fieldValue[0], dict):
                    for record in fieldValue:
                        repeat_record = read_data({"{0}_{1}".format(parentTable, fieldName): record}, identifier)
                        outData.extend(repeat_record)
                else:
                    if fieldValue[0] is not None:
                    # Select_multiple - other item types; change to comma seperated list
                        outFeature["data"][fieldName] = ",".join(fieldValue)
            else:
                outFeature["data"][fieldName] = fieldValue
        outData.append(outFeature)
    return outData


def readS123db(inDB=IN_DB):
    conn = sqlite3.connect(inDB)
    cur = conn.cursor()
    surveys = []
    outTables = {}
    # conn.text_factory = lambda x: x.decode("utf-16")
    # Status indicates which 'box' the Survey is in
    # 0 - Drafts
    # 1 - Outbox
    # 2 - Sent
    # 3 - Submission Error
    # 4 - Inbox
    for row in cur.execute('SELECT name, data, status from Surveys where status = 1 or status = 3'):
        # print(row)
        print ('-----------------')
        surveyName = row[0]
        jFeature = json.loads(row[1])
        # print(jFeature)
        features = read_data(jFeature)
        surveys.extend(features)
    for feature in surveys:
        if not feature["table"] in outTables:
            outTables[feature["table"]] = {"adds":[], "updates":[]}
        if feature["editMode"] == 0:
            outTables[feature["table"]]["adds"].append(feature["data"])
        elif feature["editMode"] == 1:
            outTables[feature["table"]]["updates"].append(feature["data"])
    return outTables

def writeCSV(surveys, outDir = OUT_DIR):
    import csv
    for surveyName, surveyTransactions in surveys.items():
        surveyAdds = surveyTransactions["adds"]
        addfile = os.path.join(outDir, 'survey_{0}_adds.csv'.format(surveyName.encode('utf-8').decode('utf-8')))
        surveyUpdates = surveyTransactions["updates"]
        updatefile = os.path.join(outDir, 'survey_{0}_updates.csv'.format(surveyName.encode('utf-8').decode('utf-8')))

        # outfile=os.path.join(outDir, 'survey_{0}.csv'.format(surveyName.encode('utf-8').decode('utf-8')))
        # outfile = ''.join(outfile.split()).encode('utf-8')
        with open(addfile, 'w') as outFile:
            #get all possible fieldnames
            fieldnames = []
            for row in surveyAdds:
                fieldnames.extend(row.keys())
            fieldnamesSet = set(fieldnames)
            fieldnames = list(fieldnamesSet)
            writer = csv.DictWriter(outFile, fieldnames=fieldnames)
            # writer = DictUnicodeWriter(outFile, fieldnames=fieldnames)
            writer.writeheader()
            for row in surveyAdds:
                # print(u'{0}\t{1}'.format(surveyName, row))
                writer.writerow(row)
        with open(updatefile, 'w') as outFile:
            # get all possible fieldnames
            fieldnames = []
            for row in surveyUpdates:
                fieldnames.extend(row.keys())
            fieldnamesSet = set(fieldnames)
            fieldnames = list(fieldnamesSet)
            writer = csv.DictWriter(outFile, fieldnames=fieldnames)
            # writer = DictUnicodeWriter(outFile, fieldnames=fieldnames)
            writer.writeheader()
            for row in surveyUpdates:
                # print(u'{0}\t{1}'.format(surveyName, row))
                writer.writerow(row)

if __name__ == '__main__':
    inDB = IN_DB
    outDir = OUT_DIR
    if len(sys.argv) > 1:
        inDB = sys.argv[1]
        outDir = sys.argv[2]
    surveys = readS123db(inDB)
    writeCSV(surveys, outDir)
