#include "Statistics.h"
#include <iostream>
#include <string>
#include <unordered_map>

string getString(char *val) {
    std::string str(val);
    return str;
}

Statistics::Statistics() {}
Statistics::Statistics(Statistics &copyMe) {}
Statistics::~Statistics() {}

void Statistics::AddRel(char *relName, int numTuples) {
    string rel = getString(relName);
    if (relationStats.find(rel) == relationStats.end()) {
        relationStats[rel] = new RelationStats();
        relationStats[rel]->relations.insert(rel);
        relationStats[rel]->numTuples = numTuples;
    } else {
        relationStats[rel]->numTuples = numTuples;
    }
}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts) {
    string rel = getString(relName);
    string att = getString(attName);
    if (relationStats.find(rel) == relationStats.end()) {
        cerr << "relation doesn't exist for given attribute: " << rel << endl;
        exit(1);
    }
    if (numDistincts == -1) {
        numDistincts = relationStats[rel]->numTuples;
    }
    relationStats[rel]->attrDistinctsMap[att] = numDistincts;
}

void Statistics::CopyRel(char *oldName, char *newName) {}

void Statistics::Read(char *fromWhere) {}
void Statistics::Write(char *fromWhere) {}

void Statistics::Apply(struct AndList *parseTree, char *relNames[],
                       int numToJoin) {}
double Statistics::Estimate(struct AndList *parseTree, char **relNames,
                            int numToJoin) {}
