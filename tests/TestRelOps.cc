#include "DBFile.h"
#include "ParseTree.h"
#include "Pipe.h"
#include "gtest/gtest.h"
#include "testsuite.h"
extern "C" struct YY_BUFFER_STATE *yy_scan_string(const char *);
extern "C" int yyparse(void);

TEST(RelOpsTest, GetNextWithSelectionPredicate) {
    relation *rel_ptr = new relation(strdup(lineitem), relation::dbfile_dir);
    char *cnfString = "(l_orderkey)";
    yy_scan_string(cnfString);
    OrderMaker om;
    rel_ptr->get_sort_order(om);

    int runlen = 2;
    struct {
        OrderMaker *o;
        int l;
    } startup = {&om, runlen};

    DBFile dbfile;
    // cout << "\n output to dbfile : " << rel_ptr->path() << endl;
    dbfile.Create(rel_ptr->path(), sorted, &startup);

    char tbl_path[100];  // construct path of the tpch flat text file
    sprintf(tbl_path, "%s%s.tbl", "data/10M/", rel_ptr->name());
    FILE *tblfile = fopen(tbl_path, "r");
    int proc = -1, res = 1, tot = 0;

    Record temp;
    int numrecs = 10000;
    while ((res = temp.SuckNextRecord(rel_ptr->schema(), tblfile)) &&
           ++proc < numrecs) {
        dbfile.Add(temp);
    }
    tot += proc;
    // cout << "\n create finished.. " << tot << " recs inserted\n";
    ASSERT_EQ(0, fclose(tblfile));

    Record literal;
    cnfString = "(l_returnflag = 'R')";
    yy_scan_string(cnfString);
    CNF cnf;
    rel_ptr->get_cnf(cnf, literal);
    dbfile.MoveFirst();

    int cnt = 0;
    cerr << "\t";
    while (dbfile.GetNext(temp, cnf, literal) && ++cnt) {
        //    temp.Print(rel_ptr->schema());
    }
    // cout << "\n query over " << rel_ptr->path() << " returned " << cnt
    //     << " recs\n";
    dbfile.Close();
    delete rel_ptr;
}
