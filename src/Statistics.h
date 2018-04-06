#ifndef STATISTICS_
#define STATISTICS_
#include <string>
#include <unordered_map>
#include <unordered_set>
#include "ParseTree.h"

using namespace std;

class RelationStats {
   private:
    std::unordered_set<string> relations;
    int numTuples;
    unordered_map<string, int> attrDistinctsMap;

    friend class Statistics;
};

class Statistics {
   private:
    unordered_map<string, RelationStats *> relationStats;

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
