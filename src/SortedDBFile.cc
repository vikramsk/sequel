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

SortedDBFile::SortedDBFile() {}
SortedDBFile::~SortedDBFile() {}

int SortedDBFile::Create(const char *f_path, fType f_type, void *startup) {}

void SortedDBFile::Load(Schema &f_schema, const char *loadpath) {}

int SortedDBFile::Open(const char *f_path) {}

void SortedDBFile::MoveFirst() {}

int SortedDBFile::Close() {}

void SortedDBFile::Add(Record &rec) {}

int SortedDBFile::GetNext(Record &fetchme) { return 0; }

int SortedDBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    return 0;
}
