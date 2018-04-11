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

SortedDBFile::SortedDBFile(const char *f_path) : filePath(f_path) {
    pageIndex = 0;
    queryOrder = NULL;
    queryLiteralOrder = NULL;
    inPipe = new Pipe(100);
    outPipe = new Pipe(100);
    bigQ = NULL;
    mode = READ;
}

typedef struct SortInfo {
    OrderMaker *o;
    int l;
};

SortedDBFile::~SortedDBFile() {
    if (mode == WRITE) flushBuffer();
}

char *metaFilePath(const char *f_path) {
    char *p = strdup(f_path);
    string path(p);

    size_t start_pos = path.find(".bin");
    path.replace(start_pos, 4, ".meta");
    return strdup(path.c_str());
}

void writeToMetaFile(const char *f_path, void *startup) {
    ofstream metaFile(metaFilePath(f_path), std::ios_base::app);

    SortInfo *si = (SortInfo *)startup;
    metaFile << si->l << endl;
    OrderMaker *om = (OrderMaker *)si->o;
    metaFile << *om;
    metaFile.close();
}

void SortedDBFile::readMetaFile(const char *f_path) {
    ifstream metaFile(metaFilePath(f_path));

    OrderMaker *om = new OrderMaker();
    string fileType;
    getline(metaFile, fileType);

    int runLen;
    metaFile >> runLen;
    metaFile >> *om;

    originalOrder = om;
    runLength = runLen;
}

int SortedDBFile::Create(const char *f_path, fType f_type, void *startup) {
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
    writeToMetaFile(f_path, startup);
    return 1;
}

void bufferAppend(Record *rec, File &file, Page &buf, off_t &index) {
    int appendResult = buf.Append(rec);
    if (appendResult == 0) {  // indicates that the page is full
        file.AddPage(&buf,
                     index++);  // write loaded buffer to file
        buf.EmptyItOut();
        buf.Append(rec);
    }
}

void SortedDBFile::Load(Schema &f_schema, const char *loadpath) {
    if (!bigQ) bigQ = new BigQ(*inPipe, *outPipe, *originalOrder, runLength);

    mode = WRITE;
    Record temp;
    // open up the text file and start processing it
    FILE *tableFile = fopen(loadpath, "r");
    // read in all of the records from the text file
    while (temp.SuckNextRecord(&f_schema, tableFile) == 1) {
        Add(temp);
    }
}

void SortedDBFile::flushBuffer() {
    buffer.EmptyItOut();
    mode = READ;
    pageIndex = 0;
    if (bigQ) {
        inPipe->ShutDown();
        mergeRecords();
    }
    if (queryOrder) {
        free(queryOrder);
        free(queryLiteralOrder);
        queryOrder = NULL;
        queryLiteralOrder = NULL;
    }
}

void SortedDBFile::mergeRecords() {
    Record pipeRecord;
    Record fileRecord;
    File newDataFile;
    Page newFilePage;

    char *newFilePath = strdup(filePath);
    strcat(newFilePath, "1");

    newDataFile.Open(0, newFilePath);
    
    MoveFirst();
    int fileStatus = GetNext(fileRecord);
    int pipeStatus = outPipe->Remove(&pipeRecord);
    off_t newPageIndex = 0;
    ComparisonEngine comp;

    while (fileStatus && pipeStatus) {
        int status = comp.Compare(&fileRecord, &pipeRecord, originalOrder);
        if (status < 0) {
            bufferAppend(&fileRecord, newDataFile, newFilePage, newPageIndex);
            fileStatus = GetNext(fileRecord);
        } else {
            bufferAppend(&pipeRecord, newDataFile, newFilePage, newPageIndex);
            pipeStatus = outPipe->Remove(&pipeRecord);
        }
    }

    while (fileStatus) {
        bufferAppend(&fileRecord, newDataFile, newFilePage, newPageIndex);
        fileStatus = GetNext(fileRecord);
    }

    while (pipeStatus) {
        bufferAppend(&pipeRecord, newDataFile, newFilePage, newPageIndex);
        pipeStatus = outPipe->Remove(&pipeRecord);
    }

    newDataFile.AddPage(&newFilePage,
                        newPageIndex);  // write remaining records to file
    
    free(bigQ);
    newDataFile.Close();
    Close();
    remove(filePath);
    rename(newFilePath, filePath);
    Open(filePath);
}

int SortedDBFile::Open(const char *f_path) {
    readMetaFile(f_path);
    char *pathStr = strdup(f_path);
    dataFile.Open(1, pathStr);
    free(pathStr);
    pageIndex = 0;
    queryOrder = NULL;
    queryLiteralOrder = NULL;
    inPipe = new Pipe(100);
    outPipe = new Pipe(100);
    bigQ = NULL;
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
    if (dataFile.GetLength()>0) dataFile.GetPage(&buffer, pageIndex);
}

int SortedDBFile::Close() {
    if (mode == WRITE)  flushBuffer();
    dataFile.Close();
    return 1;
}

void SortedDBFile::Add(Record &rec) {
    if (mode == READ) {
        mode = WRITE;
    }
    if (!bigQ) bigQ = new BigQ(*inPipe, *outPipe, *originalOrder, runLength);
    inPipe->Insert(&rec);
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
        MoveFirst();  // load first page into the buffer
    }
    int status = 0;
    if (!queryOrder) {
        queryOrder = new OrderMaker();
        queryLiteralOrder = new OrderMaker();
        if (cnf.GetQueryOrder(*originalOrder, *queryOrder,
                              *queryLiteralOrder) == 0) {
            // Linear search
            return getEqualToLiteral(fetchme, cnf, literal);
        } else {
            // Binary search to get the next candidate page into the buffer
            off_t candidatePageIndex =
                binarySearch(pageIndex, dataFile.GetLength() - 2, literal);
            if (candidatePageIndex == -1) {
                return 0;
            } else if (candidatePageIndex > pageIndex) {
                pageIndex = candidatePageIndex;
                buffer.EmptyItOut();
                dataFile.GetPage(&buffer, pageIndex);
                status = buffer.GetFirst(&fetchme);
            } else {// handle the following cases:
                    // 1. (candidatePageIndex = pageIndex)
                    // 2. (candidatePageIndex = pageIndex = 0) with empty buffer
                    // 3. (candidatePageIndex = pageIndex - 1) 
                    // In Case 3 we simply resume from where we left
                status = buffer.GetFirst(&fetchme);
                if (!status && candidatePageIndex == pageIndex && pageIndex == 0) { 
                    dataFile.GetPage(&buffer, pageIndex);
                    status = buffer.GetFirst(&fetchme);
                } 
            }
        }
    } else {  // Else there are candidate records in the buffer
        status = buffer.GetFirst(&fetchme);
    }
    return getFirstValidRecord(status,fetchme,cnf,literal,queryOrder,queryLiteralOrder);
}

int SortedDBFile::getFirstValidRecord(int status, Record &fetchme, CNF &cnf, Record &literal, OrderMaker *qOrder, OrderMaker *qlOrder) {
    ComparisonEngine comp;
    // Find first record in the buffer that satisfies queryOrder
    while (status && comp.Compare(&fetchme, qOrder, &literal,
                                  qlOrder) == -1) {
        status = buffer.GetFirst(&fetchme);
    }
    if (!status) {  // Boundary condition: very first match found as the first
                    // record of the next page
        if (dataFile.GetLength() <= pageIndex + 2) return 0;
        dataFile.GetPage(&buffer, ++pageIndex);
        status = buffer.GetFirst(&fetchme);
    }
    while (comp.Compare(&fetchme, qOrder, &literal, qlOrder) ==
           0) {
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
        Page tempBuffer;
        Record tempRec;
        off_t mid = (start + end) / 2;
        dataFile.GetPage(&tempBuffer, mid);
        tempBuffer.GetFirst(&tempRec);
        if (comp.Compare(&tempRec, queryOrder, &literal, queryLiteralOrder) >=
            0) {
            end = mid - 1;
        } else if (start == mid)
            start = mid + 1;
        else
            start = mid;
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
