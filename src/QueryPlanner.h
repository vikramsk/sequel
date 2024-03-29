#ifndef QueryPlanner_H
#define QueryPlanner_H
#include <list>
#include <map>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include "Defs.h"
#include "Function.h"
#include "ParseTree.h"
#include "Pipe.h"
#include "RelOp.h"
#include "Schema.h"
#include "Statistics.h"

class RelOrPair {
   public:
    unordered_set<string> relations;
    OrList *orList;
};

class Node {
   private:
    unordered_set<string> relations;
    opType operation;

    int numAttsIn;
    int numAttsOut;
    int *attsToKeep;
    string fileName;

    CNF cnf;
    Function func;
    Record literal;
    Schema *outSchema;
    RelationalOp *relOp;
    OrderMaker *groupOrder;

    Pipe *outPipe;
    Pipe *inPipeL;
    Pipe *inPipeR;

    Node *leftLink;
    Node *rightLink;

    Node(opType opt)
        : operation(opt),
          relOp(NULL),
          inPipeL(NULL),
          inPipeR(NULL),
          outPipe(NULL),
          leftLink(NULL),
          rightLink(NULL) {}

    ~Node() {}

    string getFilePath(string dirPath);
    int clear_pipe(bool print);
    void Print(int &inPipeL_ID, int &inPipeR_ID, int &outPipe_ID);
    void Execute();

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
    list<RelOrPair *> relOrPairs;
    int pipeSize;  // buffer sz allowed for each pipe
    char *outFileName;

    void createRelOrPairs();

   public:
    QueryTokens(FuncOperator *fo, TableList *t, AndList *al, NameList *ga,
                NameList *ats, int &distinctAtts, int &distinctFunc,
                char *outFile) {
        aggFunction = fo;
        tables = t;
        andList = al;
        groupingAtts = ga;
        attsToSelect = ats;
        pipeSize = 5000;  // TODO: Accept as a parameter in the constructor
        outFileName = outFile;

        // 1 if there is a DISTINCT in a non-aggregate query
        if (distinctAtts == 1) {
            distinctNoAgg = true;
            distinctWithAgg = false;
        } else {
            distinctNoAgg = false;
            // 1 if there is a DISTINCT in an aggregate query
            if (distinctFunc == 0)
                distinctWithAgg = false;
            else
                distinctWithAgg = true;
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

    unordered_map<string, string> initializeStats();
    void processAndList(unordered_map<string, string> relAliasMap);
    void processAggFuncs();
    void createSelectionNodes(unordered_map<string, string> relAliasMap);

    void createJoinOrder();
    Node *createJoinNode(vector<RelOrPair *> &relOrPairs);
    void performCrossJoins();
    void mergeCheapestRelations();
    double estimate(vector<RelOrPair *> relOrPairs, bool applyEstimate);
    bool areNodesMergeable(unordered_set<string> rels);

    void createProjectNode();
    void createGroupByNode();
    void createSumNode();
    void createDupRemovalNode();
    void createWriteOutNode();
    int *setAttributesList(int &numAttsOut, Schema *&newSchema);
    void addAttsToList(map<int, Attribute> &finalAtts, int &numAttsOut,
                       NameList *selAttribute);
    NameList *extractAttsFromFunc(FuncOperator *root, NameList *rest);
    Schema *setupGroupOrder(Schema *&newSchema);
    int recurseAndPrint(Node *ptr, int &outPipe_ID);
    void recurseAndExecute(Node *ptr);

   public:
    QueryPlanner(QueryTokens &qt) : tokens(qt) {}
    ~QueryPlanner();
    void Create();
    void Print();
    void Execute(int outType);
};

#endif
