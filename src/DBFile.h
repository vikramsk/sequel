#ifndef DBFILE_H
#define DBFILE_H

#include "BigQ.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "File.h"
#include "Record.h"
#include "Schema.h"
#include "TwoWayList.h"

typedef enum { heap, sorted, tree } fType;

typedef enum { READ, WRITE } modeType;

class GenericDBFile {
   private:
    fType fileType;

   public:
    modeType mode;
    File dataFile;

    virtual int Create(const char *fpath, fType file_type, void *startup) = 0;
    virtual int Open(const char *fpath) = 0;
    virtual int Close() = 0;

    virtual void Load(Schema &myschema, const char *loadpath) = 0;

    virtual void MoveFirst() = 0;
    virtual void Add(Record &addme) = 0;
    virtual int GetNext(Record &fetchme) = 0;
    virtual int GetNext(Record &fetchme, CNF &cnf, Record &literal) = 0;
};

class HeapDBFile : public virtual GenericDBFile {
   private:
    Page buffer;
    off_t pageIndex;

    void flushBuffer();
    void bufferAppend(Record *rec);

   public:
    HeapDBFile();
    ~HeapDBFile();

    int Create(const char *fpath, fType file_type, void *startup);
    int Open(const char *fpath);
    int Close();

    void Load(Schema &myschema, const char *loadpath);

    void MoveFirst();
    void Add(Record &addme);
    int GetNext(Record &fetchme);
    int GetNext(Record &fetchme, CNF &cnf, Record &literal);
};

class SortedDBFile : public virtual GenericDBFile {
   private:
    BigQ *bigQ;
    Pipe *inPipe;
    Pipe *outPipe;

    int runLength;
    const char *filePath;
    OrderMaker *originalOrder;
    OrderMaker *queryOrder;
    OrderMaker *queryLiteralOrder;
    Page buffer;
    off_t pageIndex;

    void flushBuffer();
    void mergeRecords();
    int getFirstValidRecord(int &status, Record &fetchme, CNF &cnf, Record &literal);
    int getEqualToLiteral(Record &fetchme, CNF &cnf, Record &literal);
    off_t binarySearch(off_t start, off_t end, Record &literal);
    void readMetaFile(const char *f_path);

   public:
    SortedDBFile(const char *filePath);
    // SortedDBFile(OrderMaker *o, int r);
    ~SortedDBFile();

    int Create(const char *fpath, fType file_type, void *startup);
    int Open(const char *fpath);
    int Close();

    void Load(Schema &myschema, const char *loadpath);

    void MoveFirst();
    void Add(Record &addme);
    int GetNext(Record &fetchme);
    int GetNext(Record &fetchme, CNF &cnf, Record &literal);
};

class DBFile {
   private:
    GenericDBFile *dbInstance;
    GenericDBFile *getInstance(const char *f_path);
    char *filePath;

   public:
    DBFile();
    ~DBFile();

    int Create(const char *fpath, fType file_type, void *startup);
    int Open(const char *fpath);
    int Close();
    int Delete();
    void Load(Schema &myschema, const char *loadpath);

    void MoveFirst();
    void Add(Record &addme);
    int GetNext(Record &fetchme);
    int GetNext(Record &fetchme, CNF &cnf, Record &literal);
};

#endif
