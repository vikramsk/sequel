#include <string>
#include <unordered_map>
#include <unordered_set>
#include "ParseTree.h"
#include "Pipe.h"
#include "Statistics.h"

using namespace std;

typedef enum { JOIN, SELFILE, SELPIPE, PROJECT, SUM, GROUPBY, DISTINCT } opType;

class RelOrPair {
    unordered_set<string> relations;
    vector<OrList *> clauses;
};

class Node {
   private:
    unordered_set<string> relations;
    opType operation;
    CNF *cnf;
    Pipe *outPipe;
    Pipe *inPipeL;
    Pipe *inPipeR;

    Node();
    ~Node();

    void Print();

    friend class QueryPlanner;
};

class QueryTokens {
   private:
    FuncOperator *aggFunction;
    TableList *tables;
    AndList *andList;
    NameList *groupingAtts;
    NameList *attsToSelect;
    map<string, vector<RelOrPair *>> relClauses;

   public:
    QueryTokens(FuncOperator *fo, TableList *t, AndList *al, NameList *ga,
                NameList *ats) {
        aggFunction = fo;
        tables = t;
        andList = al;
        groupingAtts = ga;
        attsToSelect = ats;
    }

    friend class QueryPlanner;
};

class QueryPlanner {
   private:
    Statistics stats;
    Node *root;
    unordered_map<string, Node *> relationNode;
    QueryTokens &tokens;

    void initializeStats();
    void processAndList();
    void processAggFuncs();

   public:
    QueryPlanner(QueryTokens &qt) : tokens(qt) {}
    ~QueryPlanner();
    void Create();
    void Print();
};
