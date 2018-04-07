#include "Statistics.h"
#include <iostream>
#include <string>
#include <unordered_map>

string getString(char *val) {
    std::string str(val);
    return str;
}

Statistics::Statistics() {}
Statistics::Statistics(Statistics &copyMe) {
    for (auto rel : copyMe.relationStats)
    {
        RelationStats rs = *rel.second;
        relationStats[rel.first] = new RelationStats();
        relationStats[rel.first]->relations = rs.relations;
        relationStats[rel.first]->numTuples = rs.numTuples;
        relationStats[rel.first]->attrDistinctsMap = rs.attrDistinctsMap;
    }
}
Statistics::~Statistics() {}

void Statistics::AddRel(char *relName, int numTuples) {
    string rel = getString(relName);
    if (relationStats.find(rel) == relationStats.end()) {
        relationStats[rel] = new RelationStats();
        relationStats[rel]->relations.insert(rel);
        relationStats[rel]->numTuples = numTuples;
    } else {
        relationStats[rel]->numTuples = numTuples;
        // TODO: Handle case when rel is a set of more than one relations
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

void Statistics::CopyRel(char *oldName, char *newName) {
    string oldRel = getString(oldName);
    string newRel = getString(newName);
    if (relationStats.find(oldRel) == relationStats.end()) {
        cerr << "relation to be copied doesn't exist: " << oldRel << endl;
        exit(1);
    }
    relationStats[newRel] = new RelationStats();
    relationStats[newRel]->relations = relationStats[oldRel]->relations;
    relationStats[newRel]->numTuples = relationStats[oldRel]->numTuples;
    relationStats[newRel]->attrDistinctsMap = relationStats[oldRel]->attrDistinctsMap;
}

void Statistics::Read(char *fromWhere) {}
void Statistics::Write(char *fromWhere) {}

void Statistics::Apply(struct AndList *parseTree, char *relNames[],
                       int numToJoin) {}
double Statistics::Estimate(struct AndList *parseTree, char **relNames,
                            int numToJoin) {}
