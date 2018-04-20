#include <string>
#include <unordered_map>
#include <unordered_set>
#include "ParseTree.h"
#include "Pipe.h"
#include "Statistics.h"

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
    bool distinctNoAgg;
    bool distinctWithAgg;
    unordered_map<string, vector<RelOrPair *>> relClauses;

   public:
    QueryTokens(FuncOperator *fo, TableList *t, AndList *al, NameList *ga,
                NameList *ats, int &distinctAtts, int &distinctFunc) {
        aggFunction = fo;
        tables = t;
        andList = al;
        groupingAtts = ga;
        attsToSelect = ats;
        // 1 if there is a DISTINCT in a non-aggregate query 
        if (distinctAtts == 1) {
            distinctNoAgg = true;
            distinctWithAgg = false;
        } else { 
            distinctNoAgg = false;
            // 1 if there is a DISTINCT in an aggregate query
            if (distinctFunc == 0) distinctWithAgg = false;
            else distinctWithAgg = true;
        }
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
    void createProjectNode();
    void createGroupByNode();
    void createSumNode();
    void createDupRemovalNode();

   public:
    QueryPlanner(QueryTokens &qt) : tokens(qt) {}
    ~QueryPlanner();
    void Create();
    void Print();
};
