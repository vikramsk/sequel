#include <File.h>
#include <Record.h>
#include <fstream>
#include <iostream>
#include "DBFile.h"
#include "gtest/gtest.h"
#include "string.h"
#include "test.h"

const char *dummyFile = "build/dbFiles/testFile.bin";

TEST(HeapFileTest, Create) {
    DBFile dbfile;
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
    // setup(catalog_path, dbfile_dir, tpch_dir);
    setup("data/catalog", "build/tests/", "data/10M/");
    relation *rel_ptr[] = {n, r, c, p, ps, o, li};

    for (int i = 0; i < 7; i++) {
        DBFile dbfile;
        dbfile.Create(rel_ptr[i]->path(), heap, NULL);

        char tbl_path[100];  // construct path of the tpch flat text file
        sprintf(tbl_path, "%s%s.tbl", "data/10M/", rel_ptr[i]->name());
        dbfile.Load(*(rel_ptr[i]->schema()), tbl_path);
        dbfile.Close();

        File file;
        char *pathStr = strdup(rel_ptr[i]->path());
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
    }
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
