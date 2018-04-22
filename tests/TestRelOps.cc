#include "DBFile.h"
#include "ParseTree.h"
#include "Pipe.h"
#include "gtest/gtest.h"
#include "testsuite.h"
#include "RelOp.h"

relation *s,*ps;

SelectFile SF_ps, SF_s;
DBFile dbf_ps, dbf_s;
Pipe _ps(100),_s(100);
CNF cnf_ps, cnf_s;
Record lit_ps, lit_s;

Attribute IA = {"int", Int};
Attribute SA = {"string", String};
Attribute DA = {"double", Double};

int psAtts = 5;
int sAtts = 7;

int clear_pipe(Pipe &in_pipe, Schema *schema, bool print) {
    Record rec;
    int cnt = 0;
    while (in_pipe.Remove(&rec)) {
        if (print) {
            rec.Print(schema);
        }
        cnt++;
    }
    return cnt;
}

void init_SF_ps(char *pred_str, int numpgs) {
    dbf_ps.Create(ps->path(), heap, NULL);
    char tbl_path[100];  // construct path of the tpch flat text file
    sprintf(tbl_path, "%s%s.tbl", relation::tpch_dir, ps->name());
    dbf_ps.Load(*(ps->schema()), tbl_path);
    s->get_cnf(pred_str, ps->schema(), cnf_ps, lit_ps);
    SF_ps.Use_n_Pages(numpgs);
}

void init_SF_s(char *pred_str, int numpgs) {
    dbf_s.Create(ps->path(), heap, NULL);
    char tbl_path[100];  // construct path of the tpch flat text file
    sprintf(tbl_path, "%s%s.tbl", relation::tpch_dir, s->name());
    dbf_s.Load(*(s->schema()), tbl_path);
    ps->get_cnf(pred_str, s->schema(), cnf_s, lit_s);
    SF_s.Use_n_Pages(numpgs);
}

TEST(RelOpsTest, Join) {
    s = new relation(strdup(supplier), relation::dbfile_dir);
    ps = new relation(strdup(partsupp), relation::dbfile_dir);
    char *pred_s = "(s_suppkey = s_suppkey)";
    init_SF_s(pred_s, 100);
    SF_s.Run(dbf_s, _s, cnf_s, lit_s);  // 10k recs qualified
    char *pred_ps = "(ps_suppkey = ps_suppkey)";
    init_SF_ps(pred_ps, 100);

    Join J;
    Pipe _s_ps(100);
    CNF cnf_p_ps;
    Record lit_p_ps;
    s->get_cnf("(s_suppkey < 11) AND (ps_suppkey < 101)", s->schema(),
            ps->schema(), cnf_p_ps, lit_p_ps);
    int outAtts = sAtts + psAtts;
    Attribute ps_supplycost = {"ps_supplycost", Double};
    Attribute joinatt[] = {
        IA, SA, SA, IA, SA, DA, SA, IA, IA, IA, ps_supplycost, SA};
    Schema join_sch("join_sch", outAtts, joinatt);
    
    SF_ps.Run(dbf_ps, _ps, cnf_ps, lit_ps);
    J.Run(_s, _ps, _s_ps, cnf_p_ps, lit_p_ps);

    SF_ps.WaitUntilDone();
    ASSERT_EQ(clear_pipe(_s_ps, &join_sch, false),80000);
    J.WaitUntilDone();
    dbf_s.Close();
    dbf_ps.Close();
}