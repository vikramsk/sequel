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

vector<string> getStrings(char *vals[], int finalIndex) {
    vector<string> strs;
    for (int i = 0; i < finalIndex; i++) {
        strs.push_back(getString(vals[i]));
    }
    return strs;
}

RelationStats::RelationStats() {}

RelationStats::RelationStats(RelationStats &copyMe) {
    relations = copyMe.relations;
    numTuples = copyMe.numTuples;
    attrDistinctsMap = copyMe.attrDistinctsMap;
}

Statistics::Statistics() {}

Statistics::Statistics(Statistics &copyMe) {
    for (auto rel : copyMe.relationStats) {
        relationStats[rel.first] = new RelationStats(*rel.second);
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
        if (relationStats[rel]->relations.size() > 1) {
            cerr << "invalid update query" << endl;
            exit(1);
        }
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

void Statistics::CopyRel(char *oldName, char *newName) {
    string oldRel = getString(oldName);
    string newRel = getString(newName);
    if (relationStats.find(oldRel) == relationStats.end()) {
        cerr << "relation to be copied doesn't exist: " << oldRel << endl;
        exit(1);
    }
    relationStats[newRel] = new RelationStats(*relationStats[oldRel]);
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
                       int numToJoin) {
    vector<string> relations = getStrings(relNames, numToJoin);
    verifyMerge(relations);

    if (!parseTree) {
        bool sameSet = true;
        for (auto r : relations) {
            if (relationStats[relations[0]]->relations.find(r) ==
                relationStats[relations[0]]->relations.end()) {
                sameSet = false;
                break;
            }
        }
        if (sameSet) return;
    }

    validateAndList(parseTree, relations);

    evaluateAndList(parseTree, relations);
}

void Statistics::validateAndList(struct AndList *parseTree,
                                 vector<string> relations) {
    if (parseTree->left) validateOrList(parseTree->left, relations);
    if (parseTree->rightAnd) validateAndList(parseTree->rightAnd, relations);
}

void Statistics::validateOrList(struct OrList *parseTree,
                                vector<string> relations) {
    if (parseTree->left) validateComparisonOp(parseTree->left, relations);
    if (parseTree->rightOr) validateOrList(parseTree->rightOr, relations);
}

void Statistics::validateComparisonOp(struct ComparisonOp *parseTree,
                                      vector<string> relations) {
    if (parseTree->left) validateOperand(parseTree->left, relations);
    if (parseTree->right) validateOperand(parseTree->right, relations);
}

void Statistics::validateOperand(struct Operand *op, vector<string> relations) {
    for (auto r : relations) {
        if (relationStats[r]->attrDistinctsMap.find(getString(op->value)) ==
            relationStats[r]->attrDistinctsMap.end()) {
            return;
        }
    }
    // attribute not found in any relation.
    cerr << "invalid attribute" << endl;
    exit(1);
}

void Statistics::verifyMerge(vector<string> relations) {
    int statRelCount = 0;

    for (auto r : relations) {
        if (relationStats.find(r) == relationStats.end()) {
            cerr << "relation data doesn't exist for relation: " << r << endl;
            exit(1);
        }
        statRelCount += relationStats[r]->relations.size();
    }
    if (statRelCount != relations.size()) {
        cerr << "invalid match for relations" << endl;
        exit(1);
    }
}

double Statistics::Estimate(struct AndList *parseTree, char **relNames,
                            int numToJoin) {
    Statistics relJoin(*this);
    relJoin.Apply(parseTree,relNames,numToJoin);
    // TODO: Look up any relName for numTuples
    string sortedRelsKey;
    return relJoin.relationStats[sortedRelsKey]->numTuples;
}

void Statistics::evaluateAndList(struct AndList *parseTree, vector<string> relNames) {
    evaluateOrList(parseTree->left,relNames);
    if (parseTree->rightAnd != NULL) {
        evaluateAndList(parseTree->rightAnd,relNames);
    }
}

void Statistics::evaluateOrList(struct OrList *parseTree, vector<string> relNames) {

}

void Statistics::applyPredicate(struct ComparisonOp *parseTree, vector<string> relNames) {

}
