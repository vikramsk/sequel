#include "DBFile.h"
#include <iostream>
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Defs.h"
#include "File.h"
#include "Record.h"
#include "Schema.h"
#include "TwoWayList.h"
#include "stdlib.h"
#include "string.h"

DBFile::DBFile() {
    pageIndex = 0;
    dbInstance = NULL;
}

DBFile::~DBFile() {
    if (mode == WRITE) flushBuffer();
}

int DBFile::Create(const char *f_path, fType f_type, void *startup) {
    if (dbInstance) {
        return dbInstance->Create(f_path, f_type, startup);
    }

    if (f_type == heap) {
        dbInstance = new HeapDBFile();
    } else if (f_type == sorted) {
        // dbInstance = new SortedDBFile();
    }

    return dbInstance->Create(f_path, f_type, startup);
}

void DBFile::bufferAppend(Record *rec) {
    int appendResult = buffer.Append(rec);
    if (appendResult == 0) {  // indicates that the page is full
        dataFile.AddPage(&buffer,
                         pageIndex++);  // write loaded buffer to file
        buffer.EmptyItOut();
        buffer.Append(rec);
    }
}

GenericDBFile *DBFile::getInstance(const char *f_path) {
    return new HeapDBFile();
}

void DBFile::Load(Schema &f_schema, const char *loadpath) {
    dbInstance->Load(f_schema, loadpath);
}

void DBFile::flushBuffer() {
    dataFile.AddPage(&buffer,
                     pageIndex);  // write remaining records in buffer to file
    buffer.EmptyItOut();
    mode = READ;
    pageIndex = 0;
}

int DBFile::Open(const char *f_path) {
    if (dbInstance == NULL) {
        dbInstance = getInstance(f_path);
    }
    return dbInstance->Open(f_path);
}

void DBFile::MoveFirst() { return dbInstance->MoveFirst(); }

int DBFile::Close() { return dbInstance->Close(); }

void DBFile::Add(Record &rec) { dbInstance->Add(rec); }

int DBFile::GetNext(Record &fetchme) {
    if (!dbInstance) return 0;
    return dbInstance->GetNext(fetchme);
}

int DBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    if (!dbInstance) return 0;
    return dbInstance->GetNext(fetchme, cnf, literal);
}
