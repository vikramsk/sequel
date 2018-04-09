#ifndef STATISTICS_
#define STATISTICS_
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "ParseTree.h"

using namespace std;

class RelationStats {
   private:
    double numTuples;
    std::unordered_set<string> relations;
    unordered_map<string, int> attrDistinctsMap;

    RelationStats();
    ~RelationStats();
    RelationStats(RelationStats &copyMe);  // Performs deep copy

    friend class Statistics;
};

class Statistics {
   private:
    unordered_map<string, RelationStats *> relationStats;
    void verifyMerge(vector<string> relations);
    void validateAndList(struct AndList *parseTree, vector<string> relations);
    void validateOrList(struct OrList *parseTree, vector<string> relations);
    void validateComparisonOp(struct ComparisonOp *parseTree,
                              vector<string> relations);
    void validateOperand(struct Operand *op, vector<string> relations);
    void evaluateAndList(struct AndList *parseTree, vector<string> relations);
    void evaluateOrList(struct OrList *parseTree, vector<string> relations);
    char * getRelationName(string attrName, vector<string> relations);
    vector<ComparisonOp *> flattenOrExpressionsTree(struct OrList *parseTree);
    int processORWithLitValues(vector<ComparisonOp *> expressions,
                               vector<string> relations, double &numTuplesOR);
    bool isPredicateWithLitValue(ComparisonOp *exp);
    double getStatsForState(Statistics &stateToCopy, ComparisonOp *exp,
                            vector<string> relations);
    void evaluateANDofOR(vector<ComparisonOp *> expressions,
                         vector<string> relations, int index,
                         double &numTuplesAND);
    double applyPredicate(struct ComparisonOp *parseTree,
                          vector<string> relations);
    void loadAttributeInfo(string attr, vector<string> relations, string &rel,
                           double &numTuples, int &numDistincts);
    // merges two relation stats using a relation from each partition, the
    // tuple count for the merged set.
    void mergeRelationStats(string rel1, string rel2, double tupleCount);

   public:
    Statistics();
    Statistics(Statistics &copyMe);  // Performs deep copy
    ~Statistics();

    void AddRel(char *relName, int numTuples);
    void AddAtt(char *relName, char *attName, int numDistincts);
    void CopyRel(char *oldName, char *newName);

    void Read(char *fromWhere);
    void Write(char *fromWhere);

    void Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
    double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);
};

#endif
