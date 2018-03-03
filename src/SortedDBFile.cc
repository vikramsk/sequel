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
    queryLiteralOrder = NULL;
    inPipe = new Pipe(100);
    outPipe = new Pipe(100);
    //*originalOrder = startup;
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
    //TODO: Determine when to free / reinitialize BigQ
    bigQ = new BigQ(*inPipe, *outPipe, *originalOrder, 2);
    return 1;
}

void SortedDBFile::Load(Schema &f_schema, const char *loadpath) {}

void SortedDBFile::flushBuffer() {
    inPipe->ShutDown();
    mergeRecords();
    dataFile.AddPage(&buffer,pageIndex);  // write remaining records to file
    buffer.EmptyItOut();
    mode = READ;
    pageIndex = 0;
    if (queryOrder) {
        free(queryOrder);
        free(queryLiteralOrder);
        queryOrder = NULL;
        queryLiteralOrder = NULL;
    }
}

void SortedDBFile::bufferAppend(Record *rec) {
    int appendResult = buffer.Append(rec);
    if (appendResult == 0) {  // indicates that the page is full
        dataFile.AddPage(&buffer,
                         pageIndex++);  // write loaded buffer to file
        buffer.EmptyItOut();
        buffer.Append(rec);
    }
}

void SortedDBFile::mergeRecords() {
    Record rec;
    while (outPipe->Remove(&rec)) {
        bufferAppend(&rec);
    }
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
    
    // mode = READ
    if (queryOrder) {
        free(queryOrder);
        free(queryLiteralOrder);
        queryOrder = NULL;
        queryLiteralOrder = NULL;
    }
    pageIndex = 0;
    dataFile.GetPage(&buffer, pageIndex);
}

int SortedDBFile::Close() {
    if (mode == WRITE) flushBuffer();
    dataFile.Close();
    return 1;
}

void SortedDBFile::Add(Record &rec) {
    //TODO: Handle other things before setting mode to write
    if (mode == READ) {
        mode = WRITE;
    }

    if (mode == WRITE) {
        inPipe->Insert(&rec);
    }
}

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
        MoveFirst(); //load first page into the buffer
    }

    int status = 0;

    if (!queryOrder) {
        queryOrder = new OrderMaker();
        queryLiteralOrder = new OrderMaker();
        if (cnf.GetQueryOrder(*originalOrder, *queryOrder, *queryLiteralOrder) == 0) {
            // Linear search
            return getEqualToLiteral(fetchme, cnf, literal);
        } else {
            // Binary search to get the right page into the buffer
            off_t candidatePageIndex = binarySearch(pageIndex,dataFile.GetLength()-2,literal);
            if (candidatePageIndex == -1) { 
                return 0;
            } else if (candidatePageIndex != pageIndex) {
                pageIndex = candidatePageIndex;
                buffer.EmptyItOut();
                dataFile.GetPage(&buffer, pageIndex);
                status = buffer.GetFirst(&fetchme);
            } else if (candidatePageIndex == 0) { // handle case when move first in not explicitly called in read mode
                status = buffer.GetFirst(&fetchme);  
                if (!status) {
                    dataFile.GetPage(&buffer, pageIndex);
                    status = buffer.GetFirst(&fetchme);
                }
            }
        }
    } else { //Else there are candidate records in the buffer
        status = buffer.GetFirst(&fetchme);
        if (!status) {
            if (dataFile.GetLength() <= pageIndex + 2) return 0;
            dataFile.GetPage(&buffer, ++pageIndex);
            status = buffer.GetFirst(&fetchme);
        }
    }

    ComparisonEngine comp;
    
    //Find first record in the buffer that satisfies queryOrder
    while (status && comp.Compare(&fetchme, queryOrder, &literal, queryLiteralOrder) == -1) {
        status = buffer.GetFirst(&fetchme);
    }

    if(!status) { // Boundary condition: very first match found as the first record of the next page
        if (dataFile.GetLength() <= pageIndex + 2) return 0;
        dataFile.GetPage(&buffer, ++pageIndex);
        status = buffer.GetFirst(&fetchme);
    }
    
    while (comp.Compare(&fetchme, queryOrder, &literal, queryLiteralOrder) == 0) {
        if (comp.Compare(&fetchme, &literal, &cnf)) {
            return 1;
        }
        status = buffer.GetFirst(&fetchme);
        if (!status) {
            if (dataFile.GetLength() <= pageIndex + 2) return 0;
            dataFile.GetPage(&buffer, ++pageIndex);
            status = buffer.GetFirst(&fetchme);
        }
    }

    return 0;
}

off_t SortedDBFile::binarySearch(off_t start, off_t end, Record &literal) {
    if (start > end) return -1;
    ComparisonEngine comp;
    while (start <= end) {
        off_t mid = (start + end) / 2;
        Page tempBuffer;
        Record tempRec;
        dataFile.GetPage(&tempBuffer, mid);
        tempBuffer.GetFirst(&tempRec);
        if (comp.Compare(&tempRec, queryOrder, &literal, queryLiteralOrder) >= 0) {
            end = mid - 1;
        } else if (start == mid) start = mid + 1;
        else start = mid;
    }
    return end < 0 ? 0 : end;
}

int SortedDBFile::getEqualToLiteral(Record &fetchme, CNF &cnf,
                                    Record &literal) {
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
