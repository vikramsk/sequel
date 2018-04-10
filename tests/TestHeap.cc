#include <File.h>
#include <Record.h>
#include "BigQ.h"
#include "Comparison.h"
#include "DBFile.h"
#include "Pipe.h"
#include "testsuite.h"

#include <pthread.h>
#include <climits>
#include <fstream>
#include <iostream>
#include "gtest/gtest.h"
#include "string.h"

const char *dummyFile = "build/dbFiles/testFile.bin";

TEST(HeapFileTest, Create) {
    DBFile dbfile;
    remove("build/tests/test.bin");
    ASSERT_FALSE(ifstream("build/tests/test.bin").good());
    int ans;
    ans = dbfile.Create("build/tests/test.bin", heap, NULL);
    ASSERT_EQ(ans, 1);
    ASSERT_TRUE(ifstream("build/tests/test.bin").good());
}

TEST(HeapFileTest, OpenClose) {
    DBFile dbfile;
    ASSERT_TRUE(ifstream("build/tests/test.bin").good());
    int ans;
    ans = dbfile.Open("build/tests/test.bin");
    ASSERT_EQ(ans, 1);
    ans = dbfile.Close();
    ASSERT_EQ(ans, 1);
}

TEST(HeapFileTest, Load) {
    relation *rel_ptr = new relation(strdup(lineitem), relation::dbfile_dir);

    DBFile dbfile;
    dbfile.Create(rel_ptr->path(), heap, NULL);

    char tbl_path[100];  // construct path of the tpch flat text file
    sprintf(tbl_path, "%s%s.tbl", relation::tpch_dir, rel_ptr->name());
    dbfile.Load(*(rel_ptr->schema()), tbl_path);
    dbfile.Close();
    File file;
    char *pathStr = strdup(rel_ptr->path());
    file.Open(1, pathStr);
    free(pathStr);
    Page buffer;
    off_t noOfPages = file.GetLength() - 1;
    ASSERT_GT(noOfPages, 0);

    Record temp;
    for (off_t page = 0; page < noOfPages; page++) {
        file.GetPage(&buffer, page);
        ASSERT_EQ(buffer.GetFirst(&temp), 1);
        buffer.EmptyItOut();
    }
    file.Close();
    delete rel_ptr;
}

TEST(HeapFileTest, AddRecords) {
    DBFile dbfile;
    const char *fpath = "build/tests/region.bin";
    remove(fpath);

    dbfile.Create(fpath, heap, NULL);
    dbfile.Open(fpath);

    Record temp;
    Schema mySchema("data/catalog", "region");
    FILE *tableFile = fopen("data/10M/region.tbl", "r");
    int counter = 0;
    while (temp.SuckNextRecord(&mySchema, tableFile) == 1) {
        counter++;
        dbfile.Add(temp);
    }
    ASSERT_GT(counter, 0);  // check if at least some records were read
    dbfile.Close();

    File file;
    char *pathStr = strdup(fpath);
    file.Open(1, pathStr);
    ASSERT_GT(file.GetLength(),
              1);  // check if at least one page was written successfully
    file.Close();
}

TEST(HeapFileTest, AddWithGetNext) {
    DBFile dbfile;
    const char *fpath = "build/tests/region.bin";
    remove(fpath);

    dbfile.Create(fpath, heap, NULL);
    dbfile.Open(fpath);

    Record temp;
    Schema mySchema("data/catalog", "region");
    FILE *tableFile = fopen("data/10M/region.tbl", "r");
    int counter = 0;
    while (temp.SuckNextRecord(&mySchema, tableFile) == 1) {
        counter++;
        dbfile.Add(temp);
    }
    dbfile.MoveFirst();
    while (dbfile.GetNext(temp) == 1) counter--;
    ASSERT_EQ(counter, 0);  // check if all records were read into the file
    dbfile.Close();
}

TEST(HeapFileTest, AddRecordsToExisting) {
    DBFile dbfile;
    const char *fpath = "build/tests/region.bin";
    remove(fpath);
    Schema mySchema("data/catalog", "region");

    dbfile.Create(fpath, heap, NULL);
    dbfile.Load(mySchema, "data/10M/region.tbl");

    Record temp;
    FILE *tableFile = fopen("data/10M/region.tbl", "r");
    int counter = 0;
    while (temp.SuckNextRecord(&mySchema, tableFile) == 1) {
        counter++;
        dbfile.Add(temp);
    }
    dbfile.MoveFirst();
    while (dbfile.GetNext(temp) == 1) counter--;
    ASSERT_LT(counter, 0);  // check that counter is less than 0 as records read
                            // > records added
    dbfile.Close();
}

TEST(HeapFileTest, LoadRecordsToExisting) {
    DBFile dbfile;
    const char *fpath = "build/tests/region.bin";
    remove(fpath);

    dbfile.Create(fpath, heap, NULL);
    Record temp;
    Schema mySchema("data/catalog", "region");
    FILE *tableFile = fopen("data/10M/region.tbl", "r");
    int counter = 0;
    while (temp.SuckNextRecord(&mySchema, tableFile) == 1) {
        counter++;
        dbfile.Add(temp);
    }
    dbfile.Close();

    dbfile.Open(fpath);
    dbfile.Load(mySchema, "data/10M/region.tbl");
    dbfile.MoveFirst();
    while (dbfile.GetNext(temp) == 1) counter--;
    ASSERT_LT(counter, 0);  // check that counter is less than 0 as records read
                            // > records loaded
    dbfile.Close();
}

TEST(HeapFileTest, GetNextFromFirstRecord) { DBFile dbfile; }

bool createDummyBinaryFile() {
    DBFile dbfile;
    // Schema dbfile.Load();
}

TEST(HeapFileTest, GetNextFromLastRecordInPage) {}

TEST(HeapFileTest, GetNextFromLastRecord) {}

TEST(HeapFileTest, GetNextFromEmptyFile) {
    DBFile dbf;
    Record rec;
    int status = dbf.GetNext(rec);
    ASSERT_EQ(status, 0);
}
