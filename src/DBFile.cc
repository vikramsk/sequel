#include "DBFile.h"
#include <iostream>
#include <string>
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Defs.h"
#include "File.h"
#include "Record.h"
#include "Schema.h"
#include "TwoWayList.h"
#include "stdlib.h"

using namespace std;

DBFile::DBFile() {
    pageIndex = 0;
    dbInstance = NULL;
}
DBFile::~DBFile() {}

int DBFile::Create(const char *f_path, fType f_type, void *startup) {
    if (dbInstance) {
        return dbInstance->Create(f_path, f_type, startup);
    }

    if (f_type == heap) {
        dbInstance = new HeapDBFile();
    } else if (f_type == sorted) {
        dbInstance = new SortedDBFile();
    }

    return dbInstance->Create(f_path, f_type, startup);
}

GenericDBFile *DBFile::getInstance(const char *f_path) {
    char *p = strdup(f_path);
    string path(p);
    // path(f_path);

    size_t start_pos = path.find(".bin");
    path.replace(start_pos, 4, ".meta");

    cout << path << endl;
    return new HeapDBFile();
}

void DBFile::Load(Schema &f_schema, const char *loadpath) {
    dbInstance->Load(f_schema, loadpath);
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
