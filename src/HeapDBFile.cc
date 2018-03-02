#include <iostream>
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include "File.h"
#include "Record.h"
#include "Schema.h"
#include "TwoWayList.h"
#include "stdlib.h"
#include "string.h"

HeapDBFile::HeapDBFile() { pageIndex = 0; }
HeapDBFile::~HeapDBFile() {
    if (mode == WRITE) flushBuffer();
}

int HeapDBFile::Create(const char *f_path, fType f_type, void *startup) {
    if (mode == WRITE) flushBuffer();
    char *pathStr = strdup(f_path);
    dataFile.Open(0, pathStr);
    free(pathStr);
    return 1;
}

void HeapDBFile::bufferAppend(Record *rec) {
    int appendResult = buffer.Append(rec);
    if (appendResult == 0) {  // indicates that the page is full
        dataFile.AddPage(&buffer,
                         pageIndex++);  // write loaded buffer to file
        buffer.EmptyItOut();
        buffer.Append(rec);
    }
}

void HeapDBFile::Load(Schema &f_schema, const char *loadpath) {
    if (mode == READ) buffer.EmptyItOut();

    if (dataFile.GetLength() == -1) {
        cout
            << "BAD : create/open the file to which you want to add a record\n";
        exit(1);
    }

    if (dataFile.GetLength() == 0)
        pageIndex = 0;
    else {
        pageIndex = dataFile.GetLength() - 2;
        dataFile.GetPage(&buffer, pageIndex);
    }

    mode = WRITE;
    Record temp;
    // open up the text file and start processing it
    FILE *tableFile = fopen(loadpath, "r");
    // read in all of the records from the text file
    while (temp.SuckNextRecord(&f_schema, tableFile) == 1) {
        bufferAppend(&temp);
    }
}

void HeapDBFile::flushBuffer() {
    dataFile.AddPage(&buffer,
                     pageIndex);  // write remaining records in buffer to file
    buffer.EmptyItOut();
    mode = READ;
    pageIndex = 0;
}

int HeapDBFile::Open(const char *f_path) {
    if (mode == WRITE) flushBuffer();
    char *pathStr = strdup(f_path);
    dataFile.Open(1, pathStr);
    free(pathStr);
    return 1;
}

void HeapDBFile::MoveFirst() {
    if (mode == WRITE) flushBuffer();
    pageIndex = 0;
    dataFile.GetPage(&buffer, pageIndex);
}

int HeapDBFile::Close() {
    if (mode == WRITE) flushBuffer();
    dataFile.Close();
    return 1;
}

void HeapDBFile::Add(Record &rec) {
    if (mode == WRITE) {
        bufferAppend(&rec);
        return;
    }
    // mode = READ
    if (dataFile.GetLength() == -1) {
        cout
            << "BAD : create/open the file to which you want to add a record\n";
        exit(1);
    }

    if (dataFile.GetLength() == 0)
        pageIndex = 0;
    else {
        pageIndex = dataFile.GetLength() - 2;
        dataFile.GetPage(&buffer, pageIndex);
    }
    mode = WRITE;
    bufferAppend(&rec);
}

int HeapDBFile::GetNext(Record &fetchme) {
    if (mode == WRITE) return 0;

    int status = buffer.GetFirst(&fetchme);
    if (!status) {
        if (dataFile.GetLength() <= pageIndex + 2) return 0;
        dataFile.GetPage(&buffer, ++pageIndex);
        status = buffer.GetFirst(&fetchme);
    }
    return status;
}

int HeapDBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    int status = 0;
    ComparisonEngine comp;

    while (!status) {
        // No more records to fetch.
        status = GetNext(fetchme);
        if (!status) break;

        if (comp.Compare(&fetchme, &literal, &cnf)) {
            status = 1;
            break;
        }
        status = 0;
    }
    return status;
}
