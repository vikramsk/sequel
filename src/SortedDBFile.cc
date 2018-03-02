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

SortedDBFile::SortedDBFile() { 
    pageIndex = 0; 
    queryOrder = NULL;
}
SortedDBFile::~SortedDBFile() { 
    if (mode == WRITE) flushBuffer();
}

int SortedDBFile::Create(const char *f_path, fType f_type, void *startup) {
    if (mode == WRITE) flushBuffer();
    char *pathStr = strdup(f_path);
    dataFile.Open(0, pathStr);
    free(pathStr);

    typedef struct SortInfo {
        OrderMaker *o;
        int l;
    };
    SortInfo *sortInfo = (SortInfo *)startup;
    originalOrder = sortInfo->o;
    runLength = sortInfo->l;
    return 1;
}

void SortedDBFile::Load(Schema &f_schema, const char *loadpath) {}

void SortedDBFile::flushBuffer() {
    // dataFile.AddPage(&buffer,
    //                  pageIndex);  // write remaining records in buffer to file
    // buffer.EmptyItOut();
    mode = READ;
    pageIndex = 0;
}

int SortedDBFile::Open(const char *f_path) {
    if (mode == WRITE) flushBuffer();
    char *pathStr = strdup(f_path);
    dataFile.Open(1, pathStr);
    free(pathStr);
    return 1;
}

void SortedDBFile::MoveFirst() {
    if (mode == WRITE) flushBuffer();
    pageIndex = 0;
    dataFile.GetPage(&buffer, pageIndex);
}

int SortedDBFile::Close() {
    if (mode == WRITE) flushBuffer();
    dataFile.Close();
    return 1;
}

void SortedDBFile::Add(Record &rec) {}

int SortedDBFile::GetNext(Record &fetchme) { 
    if (mode == WRITE) {
        flushBuffer();
        MoveFirst();
    }

    int status = buffer.GetFirst(&fetchme);
    if (!status) {
        if (dataFile.GetLength() <= pageIndex + 2) return 0;
        dataFile.GetPage(&buffer, ++pageIndex);
        status = buffer.GetFirst(&fetchme);
    }
    return status;
}

int SortedDBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    if (mode == WRITE) {
        flushBuffer();
        MoveFirst();
    }

    int status = 0;

    if (!queryOrder) {
        queryOrder = new OrderMaker();
        status = cnf.GetQueryOrder(*originalOrder,*queryOrder);
        if (!status) return GetEqualToLiteral(fetchme, cnf, literal);
    }

    // Binary search

    // TODO: Check if you need to free queryOrder

    return status;

}

int SortedDBFile::GetEqualToLiteral(Record &fetchme, CNF &cnf, Record &literal) {
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
