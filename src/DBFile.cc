#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <fstream>

// stub file .. replace it with your own DBFile.cc

DBFile::DBFile () {
}

int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    //TODO: Add switch case on fType
    file.open(f_path, fstream::binary | fstream::trunc |fstream::out);
    if(!file)
        return 0;
    else
        return DBFile::Close();
}

void DBFile::Load (Schema &f_schema, const char *loadpath) {
}

int DBFile::Open (const char *f_path) {
    file.open(f_path, fstream::binary | fstream::in | fstream::out);
    return file.is_open() ? 1 : 0;
}

void DBFile::MoveFirst () {
    //TODO: Handle case where file wasn't opened before calling MoveFirst
    file.seekg(ios::beg);
}

int DBFile::Close () {
    file.close();
    return 1;
}

void DBFile::Add (Record &rec) {
}

int DBFile::GetNext (Record &fetchme) {
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
}
