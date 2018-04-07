#include "Statistics.h"
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

using namespace std;

string getString(char *val) {
    std::string str(val);
    return str;
}

Statistics::Statistics() {}

Statistics::Statistics(Statistics &copyMe) {
    for (auto rel : copyMe.relationStats) {
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
    relationStats[newRel]->attrDistinctsMap =
        relationStats[oldRel]->attrDistinctsMap;
}

void Statistics::Read(char *fromWhere) {
    ifstream statFile(fromWhere);
    if (!statFile.is_open()) {
        cerr << "could not open stat file" << endl;
        exit(1);
    }

    int numRelStats, numRels, numAttrs;
    statFile >> numRelStats;
    while (numRelStats--) {
        RelationStats *relStats = new RelationStats();
        statFile >> numRels >> numAttrs;
        statFile >> relStats->numTuples;
        while (numRels--) {
            string rel;
            statFile >> rel;
            relStats->relations.insert(rel);
            relationStats[rel] = relStats;
        }
        while (numAttrs--) {
            string attr;
            int distincts;
            statFile >> attr >> distincts;
            relStats->attrDistinctsMap[attr] = distincts;
        }
    }
    statFile.close();
}

void Statistics::Write(char *fromWhere) {
    ofstream statFile(fromWhere);
    if (!statFile.is_open()) {
        cerr << "could not open stat file" << endl;
        exit(1);
    }

    unordered_set<RelationStats *> uniqueRels;
    for (auto rs : relationStats) {
        uniqueRels.insert(rs.second);
    }

    statFile << uniqueRels.size() << endl;
    for (auto rs : uniqueRels) {
        statFile << rs->relations.size() << "\t" << rs->attrDistinctsMap.size()
                 << endl;
        statFile << rs->numTuples << endl;

        for (auto rel : rs->relations) {
            statFile << rel << endl;
        }
        for (auto attr : rs->attrDistinctsMap) {
            statFile << attr.first << "\t" << attr.second << endl;
        }
    }
    statFile.close();
}

void Statistics::Apply(struct AndList *parseTree, char *relNames[],
                       int numToJoin) {}

double Statistics::Estimate(struct AndList *parseTree, char **relNames,
                            int numToJoin) {}
