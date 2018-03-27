#include <File.h>
#include <Record.h>
#include "BigQ.h"
#include "Comparison.h"
#include "DBFile.h"
#include "Pipe.h"
#include "test.h"

#include <pthread.h>
#include <fstream>
#include <iostream>
#include <climits>
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
    // setup(catalog_path, dbfile_dir, tpch_dir);
    setup("data/catalog", "build/tests/", "data/10M/");
    relation *rel_ptr = li;

    DBFile dbfile;
    dbfile.Create(rel_ptr->path(), heap, NULL);

    char tbl_path[100];  // construct path of the tpch flat text file
    sprintf(tbl_path, "%s%s.tbl", "data/10M/", rel_ptr->name());
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
    cleanup();
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

// TEST(SortedFileTest, CreateWorker) {
//     setup("data/catalog", "build/tests/", "data/10M/");
//     relation *rel_ptr = li;
//     OrderMaker om;
//     rel_ptr->get_sort_order(om);
//     int bufferSize = 10;
//     int runLength = 2;
//     Pipe inputPipe(bufferSize);
//     Pipe outputPipe(bufferSize);
//     BigQ bigQInstance(inputPipe, outputPipe, om, runLength);
//     char tbl_path[100];  // construct path of the tpch flat text file
//     sprintf(tbl_path, "%s%s.tbl", "data/10M/", rel_ptr->name());
//     FILE *tblfile = fopen(tbl_path, "r");
//     int proc = -1, res = 1, tot = 0;

//     Record temp;
//     int numrecs = 10000;
//     while ((res = temp.SuckNextRecord(rel_ptr->schema(), tblfile)) &&
//         ++proc < numrecs) {
//         inputPipe.Insert(&temp);
//     }
//     tot += proc;
//     inputPipe.ShutDown();
//     cout << "\n create finished.. " << tot << " recs inserted\n";
//     ASSERT_EQ(0, fclose(tblfile));

//     while (outputPipe.Remove(&temp)) {
//         tot--;
//     }
//     ASSERT_EQ(0, tot);

//     void *status;
//     int rc = pthread_join(bigQInstance.worker, &status);
//     ASSERT_FALSE(rc);
//     /*
//     if (rc) {
//         cout<<"ERROR; return code from pthread_join() is " << rc << endl;
//         exit(-1);
//     }
//     cout << "Main: completed join with worker thread having a status of
//         "<<(long)status << endl;
//      */
//     cleanup();
// }

// TEST(SortedFileTest, BigQPipe) {
//    relation *rel = new relation(lineitem, new Schema("data/catalog",
//    lineitem),
//                                 "build/tests/");
//
//    OrderMaker om;
//    rel->get_sort_order(om);
//
//    int bufferSize = 10;
//    int runLength = 1;
//
//    Pipe inputPipe(bufferSize);
//    Pipe outputPipe(bufferSize);
//    BigQ bigQInstance(inputPipe, outputPipe, om, runLength);
//    char tbl_path[100];
//    sprintf(tbl_path, "%s%s.tbl", "data/10M/", rel->name());
//    FILE *tblfile = fopen(tbl_path, "r");
//    for (size_t i = 0; i < 10000; i++) {
//        Record temp;
//        auto res = temp.SuckNextRecord(rel->schema(), tblfile);
//        inputPipe.Insert(&temp);
//    }
//    inputPipe.ShutDown();
//
//    for (size_t i = 0; i < 10000; i++) {
//        Record temp;
//        auto res = outputPipe.Remove(&temp);
//        temp.Print(rel->schema());
//
//        if (res == 0) {
//            break;
//        }
//    }
//    outputPipe.ShutDown();
//    void *status;
//    int rc = pthread_join(bigQInstance.worker, &status);
//}

// TEST(SortedFileTest, Load) {
//     // setup(catalog_path, dbfile_dir, tpch_dir);
//     setup("data/catalog", "build/tests/", "data/10M/");
//     relation *rel_ptr = li;

//     OrderMaker om;
//     rel_ptr->get_sort_order(om);

//     int runlen = 2;
//     struct {
//         OrderMaker *o;
//         int l;
//     } startup = {&om, runlen};

//     DBFile dbfile;
//     dbfile.Create(rel_ptr->path(), sorted, &startup);

//     char tbl_path[100];  // construct path of the tpch flat text file
//     sprintf(tbl_path, "%s%s.tbl", "data/10M/", rel_ptr->name());
//     dbfile.Load(*(rel_ptr->schema()), tbl_path);
//     dbfile.Close();

//     File file;
//     char *pathStr = strdup(rel_ptr->path());
//     file.Open(1, pathStr);
//     free(pathStr);
//     Page buffer;
//     off_t noOfPages = file.GetLength() - 1;
//     ASSERT_GT(noOfPages, 0);

//     Record temp;
//     for (off_t page = 0; page < noOfPages; page++) {
//         file.GetPage(&buffer, page);
//         ASSERT_EQ(buffer.GetFirst(&temp), 1);
//         buffer.EmptyItOut();
//     }
//     file.Close();
//     cleanup();
// }

// TEST(SortedFileTest, GetNextWithSelectionPredicate) {
//     setup("data/catalog", "build/tests/", "data/10M/");
//     relation *rel_ptr = li;
//     OrderMaker om;
//     rel_ptr->get_sort_order(om);

//     int runlen = 2;
//     struct {
//         OrderMaker *o;
//         int l;
//     } startup = {&om, runlen};

//     DBFile dbfile;
//     cout << "\n output to dbfile : " << rel_ptr->path() << endl;
//     dbfile.Create(rel_ptr->path(), sorted, &startup);

//     char tbl_path[100];  // construct path of the tpch flat text file
//     sprintf(tbl_path, "%s%s.tbl", "data/10M/", rel_ptr->name());
//     FILE *tblfile = fopen(tbl_path, "r");
//     int proc = -1, res = 1, tot = 0;

//     Record temp;
//     int numrecs = 10000;
//     while ((res = temp.SuckNextRecord(rel_ptr->schema(), tblfile)) &&
//            ++proc < numrecs) {
//         dbfile.Add(temp);
//     }
//     tot += proc;
//     cout << "\n create finished.. " << tot << " recs inserted\n";
//     ASSERT_EQ(0, fclose(tblfile));

//     CNF cnf;
//     Record literal;
//     cin.clear();
//     std::cin.ignore(INT_MAX);
//     rel_ptr->get_cnf(cnf, literal);
//     dbfile.MoveFirst();

//     int cnt = 0;
//     cerr << "\t";
//     while (dbfile.GetNext(temp, cnf, literal) && ++cnt) {
//         temp.Print(rel_ptr->schema());
//     }
//     cout << "\n query over " << rel_ptr->path() << " returned " << cnt
//          << " recs\n";
//     dbfile.Close();
//     cleanup();
// }

// TEST(SortedFileTest, CompareSortedLists) {
//     setup("data/catalog", "build/tests/", "data/10M/");
//     relation *rel_ptr = ps;
//     OrderMaker om;
//     rel_ptr->get_sort_order(om);
    
//     DBFile dbfile1;
//     DBFile dbfile2;
//     dbfile1.Open("build/dbfiles/partsupp1.bin");
//     dbfile2.Open("build/dbfiles/partsupp2.bin");
//     dbfile1.MoveFirst();
//     dbfile2.MoveFirst();

//     ComparisonEngine comp;
//     Record rec1,rec2;
//     int cnt = 0;
//     while (dbfile1.GetNext(rec1) && dbfile2.GetNext(rec2)) {
//         if(comp.Compare(&rec1,&rec2,&om) == 0) {
//             cnt++;
//         }
//     }
//     cout << "\n query returned " << cnt
//          << " recs\n";
//     dbfile1.Close();
//     dbfile2.Close();
//     cleanup();
// }