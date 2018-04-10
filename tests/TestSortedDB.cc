#include "DBFile.h"
#include "ParseTree.h"
#include "Pipe.h"
#include "gtest/gtest.h"
#include "testsuite.h"
extern "C" struct YY_BUFFER_STATE *yy_scan_string(const char *);
extern "C" int yyparse(void);

TEST(SortedFileTest, CreateWorker) {
    relation *rel_ptr = new relation(strdup(lineitem), relation::dbfile_dir);
    char *cnf = "(l_orderkey)";
    yy_scan_string(cnf);
    OrderMaker om;
    rel_ptr->get_sort_order(om);
    int bufferSize = 10;
    int runLength = 2;
    Pipe inputPipe(bufferSize);
    Pipe outputPipe(bufferSize);
    BigQ bigQInstance(inputPipe, outputPipe, om, runLength);
    char tbl_path[100];  // construct path of the tpch flat text file
    sprintf(tbl_path, "%s%s.tbl", "data/10M/", rel_ptr->name());
    FILE *tblfile = fopen(tbl_path, "r");
    int proc = -1, res = 1, tot = 0;

    Record temp;
    int numrecs = 10000;
    while ((res = temp.SuckNextRecord(rel_ptr->schema(), tblfile)) &&
           ++proc < numrecs) {
        inputPipe.Insert(&temp);
    }
    tot += proc;
    inputPipe.ShutDown();
    // cout << "\n create finished.. " << tot << " recs inserted\n";
    ASSERT_EQ(0, fclose(tblfile));

    while (outputPipe.Remove(&temp)) {
        tot--;
    }
    ASSERT_EQ(0, tot);

    void *status;
    int rc = pthread_join(bigQInstance.worker, &status);
    ASSERT_FALSE(rc);
    delete rel_ptr;
}

TEST(SortedFileTest, BigQPipe) {
    relation *rel = new relation(strdup(lineitem), relation::dbfile_dir);
    char *cnf = "(l_orderkey)";
    yy_scan_string(cnf);

    OrderMaker om;
    rel->get_sort_order(om);

    int bufferSize = 10;
    int runLength = 1;

    Pipe inputPipe(bufferSize);
    Pipe outputPipe(bufferSize);
    BigQ bigQInstance(inputPipe, outputPipe, om, runLength);
    char tbl_path[100];
    sprintf(tbl_path, "%s%s.tbl", "data/10M/", rel->name());

    FILE *tblfile = fopen(tbl_path, "r");
    for (size_t i = 0; i < 10000; i++) {
        Record temp;
        auto res = temp.SuckNextRecord(rel->schema(), tblfile);
        inputPipe.Insert(&temp);
    }
    inputPipe.ShutDown();

    int outputCount = 0;
    Record temp;
    while (outputPipe.Remove(&temp)) {
        outputCount++;
    }
    outputPipe.ShutDown();
    void *status;
    int rc = pthread_join(bigQInstance.worker, &status);
    ASSERT_EQ(outputCount, 10000);
    delete rel;
}

TEST(SortedFileTest, Load) {
    relation *rel_ptr = new relation(strdup(lineitem), relation::dbfile_dir);
    char *cnf = "(l_orderkey)";
    yy_scan_string(cnf);

    OrderMaker om;
    rel_ptr->get_sort_order(om);

    int runlen = 2;
    struct {
        OrderMaker *o;
        int l;
    } startup = {&om, runlen};

    DBFile dbfile;
    dbfile.Create(rel_ptr->path(), sorted, &startup);

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
    delete rel_ptr;
}

// TEST(SortedFileTest, CompareSortedLists) {
//    relation *rel_ptr = new relation(strdup(partsupp), relation::dbfile_dir);
//    char *cnf = "(l_orderkey)";
//    yy_scan_string(cnf);
//    OrderMaker om;
//    rel_ptr->get_sort_order(om);
//
//    DBFile dbfile1;
//    DBFile dbfile2;
//    dbfile1.Open("build/dbfiles/partsupp1.bin");
//    dbfile2.Open("build/dbfiles/partsupp2.bin");
//    dbfile1.MoveFirst();
//    dbfile2.MoveFirst();
//
//    ComparisonEngine comp;
//    Record rec1, rec2;
//    int cnt = 0;
//    while (dbfile1.GetNext(rec1) && dbfile2.GetNext(rec2)) {
//        if (comp.Compare(&rec1, &rec2, &om) == 0) {
//            cnt++;
//        }
//    }
//    cout << "\n query returned " << cnt << " recs\n";
//    dbfile1.Close();
//    dbfile2.Close();
//    delete rel_ptr;
//}
