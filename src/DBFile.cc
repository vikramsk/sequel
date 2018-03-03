#include "DBFile.h"
#include <fstream>
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

using namespace std;

DBFile::DBFile() { dbInstance = NULL; }
DBFile::~DBFile() {}

char *getMetaFilePath(const char *f_path) {
    char *p = strdup(f_path);
    string path(p);

    size_t start_pos = path.find(".bin");
    path.replace(start_pos, 4, ".meta");
    return strdup(path.c_str());
}

void createMetaFile(char *filePath, fType type) {
    ofstream metaFile(filePath);

    if (metaFile.is_open()) {
        if (type == heap)
            metaFile << "heap\n";
        else
            metaFile << "sorted\n";
        metaFile.close();
    }
}

fType readFileType(char *filePath) {
    string type;
    fType fileType;
    ifstream metaFile(filePath);

    if (metaFile.is_open()) {
        getline(metaFile, type);
        if (type.compare("heap") == 0)
            fileType = heap;
        else
            fileType = sorted;
        metaFile.close();
    }
    return fileType;
}

int DBFile::Create(const char *f_path, fType f_type, void *startup) {
    if (f_type == heap) {
        dbInstance = new HeapDBFile();
    } else if (f_type == sorted) {
        dbInstance = new SortedDBFile(f_path);
    }

    createMetaFile(getMetaFilePath(f_path), f_type);
    return dbInstance->Create(f_path, f_type, startup);
}

GenericDBFile *DBFile::getInstance(const char *f_path) {
    fType type;
    type = readFileType(getMetaFilePath(f_path));
    if (type == heap)
        return new HeapDBFile();
    else {
        OrderMaker sortOrder;
        int runLength;
        //TODO: Fetch sortOrder and runLength from meta file
        return new SortedDBFile(&sortOrder,runLength);
    }
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
