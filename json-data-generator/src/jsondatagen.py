#!/usr/bin/python3

import sys

## convert the file size to readable format
def format_bytes(size):
    # 2**10 = 1024
    power = 2**10
    n = 0
    power_labels = {0 : ' Bytes', 1: 'KB', 2: 'MB', 3: 'GB', 4: 'TB'}
    while size > power:
        size /= power
        n += 1
    return str(round(size, 2)) + power_labels[n]

## User input parameters
print("General Information ------------------------------------------------------")
totalNumberOfRecords = int(sys.argv[1])
numberOfRecordsPerFile = int(sys.argv[2])
fileName = sys.argv[3]
recordSize = int(sys.argv[4])
recordFieldsCount = int(sys.argv[5])
totalDataSize = totalNumberOfRecords * recordSize

print("Total number of records: ", totalNumberOfRecords)
print("Number of records per file: ", numberOfRecordsPerFile)
print("Expected total data size: ", format_bytes(totalDataSize))
print("File name: ", fileName)
print("Record size: ", format_bytes(recordSize))
print("Number of fields per record: ", recordFieldsCount)
print("----------------------------------------------------------------------------\n")


## Calculate the size of each field
print("Per Record Information ------------------------------------------------------")
eachFieldSize = int(recordSize / recordFieldsCount)
lastFieldLeftOver = recordSize % recordFieldsCount
print("Each record will have ", recordFieldsCount, " fields")
print("Each field will be ", format_bytes(eachFieldSize))
print("Last field will be ", format_bytes(eachFieldSize + lastFieldLeftOver))


## Create the JSON document as per the provided input
print("Generating record started...")
eachFieldValue = "a" * eachFieldSize
lastFieldValue = ("a" * eachFieldSize) + ("a" * lastFieldLeftOver)
jsonObjectStr = "{"
for fieldNumber in range(recordFieldsCount - 1):
    jsonObjectStr += "\"f" + str(fieldNumber) + "\":\"" + eachFieldValue + "\","
jsonObjectStr += "\"f" + str(recordFieldsCount - 1) + "\":\"" + lastFieldValue + "\"}\n"
print("Record creation completed. Each record size is: ", format_bytes(len(jsonObjectStr)))
print("----------------------------------------------------------------------------\n")


## We will generate a single file, however, display the stats for the total numbers
print("File Information ------------------------------------------------------")
leftOverRecords = totalNumberOfRecords % numberOfRecordsPerFile
numberOfFiles = int(totalNumberOfRecords / numberOfRecordsPerFile)
print("Number of files to be generated: ", numberOfFiles)
print("Number of records per file: ", numberOfRecordsPerFile)
print("Number of records in last file: ", numberOfRecordsPerFile + leftOverRecords)
print("Total size per file: ", format_bytes(numberOfRecordsPerFile * len(jsonObjectStr)))
print("Actual total data size: ", format_bytes(totalNumberOfRecords * len(jsonObjectStr)))

finalResult = jsonObjectStr * numberOfRecordsPerFile
file = open(fileName, "w")
file.write(finalResult)
file.close()
