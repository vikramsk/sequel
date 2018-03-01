#ifndef DBFILE_H
#define DBFILE_H

#include "Comparison.h"
#include "ComparisonEngine.h"
#include "File.h"
#include "Record.h"
#include "Schema.h"
#include "TwoWayList.h"

typedef enum { heap, sorted, tree } fType;

typedef enum { READ, WRITE } modeType;
// stub DBFile header..replace it with your own DBFile.h

class GenericDBFile {
   private:
    fType fileType;

    //    off_t pageIndex;
    //    modeType mode;

   public:
    modeType mode;
    File dataFile;

    // GenericDBFile();
    //~GenericDBFile();

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
   public:
    SortedDBFile();
    ~SortedDBFile();
};

class DBFile {
   private:
    GenericDBFile *dbInstance;
    File dataFile;
    Page buffer;
    off_t pageIndex;
    modeType mode;

    void flushBuffer();
    void bufferAppend(Record *rec);
    GenericDBFile *getInstance(const char *f_path);

   public:
    DBFile();
    ~DBFile();

    int Create(const char *fpath, fType file_type, void *startup);
    int Open(const char *fpath);
    int Close();

    void Load(Schema &myschema, const char *loadpath);

    void MoveFirst();
    void Add(Record &addme);
    int GetNext(Record &fetchme);
    int GetNext(Record &fetchme, CNF &cnf, Record &literal);
};
#endif
