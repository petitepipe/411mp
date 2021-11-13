"""
This is a helper file to list the main steps that need to be finished for the operator implementation.

"""
import csv
from os import truncate
import sys
import json
import math

class OnePassOperator:
    def __init__(self, config):
        self.memory = 0
        self._loadConfig(config)

def _loadConfig(configFile):
    f = open(configFile)
    data = json.load(f)
    memorySize, blockSize = data["memory_size"], data["block_size"]
    return memorySize, blockSize


def _loadColNames(inputFile):
    with open(inputFile) as csvFile:
        firstRow = csvFile.readline().strip()
        colNames = firstRow.split(",")
        return colNames
    return []

# TODO: Return the number of data rows in the table. You are only allowed to use one tuple in memory for this method. (do not load entire table in memory to calculate the size)
def _loadTableSize(tableFile):
    numRows = 0
    f = open(tableFile, 'r')
    for _ in f:
        numRows += 1
    f.close()
    return numRows-1
    
def onePassOperator(configFile, table1File, table2File, outputFile):
    outputFileCursor = open(outputFile, 'w')
    csvWriter = csv.writer(outputFileCursor)
    # TODO: step 1 - load config.txt as json, extract memory size (Number of blocks in the main memory) 
    # and block size(Number of Tuples in a block), compute maximal number of rows(tuples) that 
    # can be loaded into memory.
    memorySize, blockSize = _loadConfig(configFile)
    maxNumOfTuples = memorySize * blockSize # step 1 SOLN
    
    size1, size2 = _loadTableSize(table1File), _loadTableSize(table2File)
    # TODO: step 2.1 - check if there is enough memory available to carry out the intersection operation
    if (min(size1, size2)+2) % blockSize != 0 :
        expectedTupleSize = (min(size1, size2)+2) / blockSize +1
    else : 
        expectedTupleSize = (min(size1, size2)+2) / blockSize 
    if maxNumOfTuples <  expectedTupleSize:
        failingConditions = True
    else  :
        failingConditions = False

    if failingConditions:
        outputFileCursor.write("INVALID MEMORY\n")
        outputFileCursor.close()
        return

    colNames1 = _loadColNames(table1File)
    colNames2 = _loadColNames(table2File)

    #TODO: step 2.2 - check if the column names of input table 1 and input table 2 are valid
    if colNames1 == colNames2 :
        failingConditions = False
    else  :
        failingConditions = True
        
    if failingConditions:
        outputFileCursor.write("INVALID SCHEMA/INPUT\n")
        outputFileCursor.close()
        return
    # TODO step 2.3 - flush one colNames dict as they are the same
    # TODO: step 3 - load smaller data table into memory


    if size1 > size2:
        colNames1.clear()
        colToWrite = colNames2
        tableS = table2File
        tableR = table1File
    else : 
        colNames2.clear()
        colToWrite = colNames1
        tableS = table1File
        tableR = table2File
            
    memory = []
    with open(tableS) as csvFile:
        csvReader = csv.reader(csvFile, delimiter=",")
        for row in csvReader:
            memory.append(row)
            
    print("memory", memory)
    print("colToWrite", colToWrite)

    # TODO: step 4 - write column names to the outputFile
    # TODO: step 4.1 - flush the other colNames as well as its no longer needed
    csvWriter.writerow(colToWrite)
    colToWrite.clear()

    # TODO: step 5 - load larger data table into the memory row by row and perform intersection
    # using the  smaller table in memory
    with open(tableR) as csvFile:
        csvReader = csv.reader(csvFile, delimiter=",")
        bufferBlock = []
        for i, row in enumerate(csvReader):
            if i == 0:
                continue
            bufferBlock.append(row)
            if (i-1)%blockSize == 0:
                for bufferIdx in range(len(bufferBlock)): 
                    for memoryIdx in range(len(memory)):
                        if bufferBlock[bufferIdx] == memory[memoryIdx]:
                            csvWriter.writerow(bufferBlock[bufferIdx])
                bufferBlock.clear()
    outputFileCursor.close()

if __name__ == "__main__":
    configFile, table1File, table2File, outputFile = sys.argv[1:]
    onePassOperator(configFile, table1File, table2File, outputFile)