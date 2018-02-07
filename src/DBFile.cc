#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include "stdlib.h"
#include "string.h"


// stub file .. replace it with your own DBFile.cc

DBFile::DBFile () {
}

int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    char *pathStr = strdup(f_path);
    dataFile.Open(0,pathStr);
    free(pathStr);
    return 1;
}

void DBFile::Load (Schema &f_schema, const char *loadpath) {
    
    // open up the text file and start processing it
    FILE *tableFile = fopen(loadpath, "r");

    Record temp;
    off_t pageCount = 0;

    // read in all of the records from the text file
    while (temp.SuckNextRecord(&f_schema, tableFile) == 1) {
        int appendResult = buffer.Append(&temp);
        if(appendResult == 0) { //indicates that the page is full
            dataFile.AddPage(&buffer,pageCount++); //write loaded buffer to file
            buffer.EmptyItOut();
            buffer.Append(&temp);
        }
    }
    dataFile.AddPage(&buffer,pageCount); //write remaining records in buffer to file
    buffer.EmptyItOut();
}

int DBFile::Open (const char *f_path) {
    char *pathStr = strdup(f_path);
    dataFile.Open(1,pathStr);
    free(pathStr);
    return 1;
}

void DBFile::MoveFirst () {
}

int DBFile::Close () {
    dataFile.Close();
    return 1;
}

void DBFile::Add (Record &rec) {
}

int DBFile::GetNext (Record &fetchme) {
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
}