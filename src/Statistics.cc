#include "Statistics.h"
#include <fstream>
#include <iostream>
#include <cstring>
#include <unordered_map>
#include <algorithm>
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
    for (auto attr : relationStats[oldRel]->attrDistinctsMap) {
        string newAttrName = newRel + "." + attr.first;
        relationStats[newRel]->attrDistinctsMap[newAttrName] = attr.second;
    }
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
    // Look up any relName for numTuples
    string key;
    if (parseTree->left->left->left->code == NAME) {
        key = getString(parseTree->left->left->left->value);
    } else {
        key = getString(parseTree->left->left->right->value);
    }
    return relJoin.relationStats[key]->numTuples;
}

void Statistics::evaluateAndList(struct AndList *parseTree, vector<string> relations) {
    evaluateOrList(parseTree->left,relations);
    if (parseTree->rightAnd != NULL) {
        evaluateAndList(parseTree->rightAnd,relations);
    }
}

void Statistics::evaluateOrList(struct OrList *parseTree, vector<string> relations) {
    vector<ComparisonOp *> orExpressions = flattenOrExpressionsTree(parseTree);
    double numTuplesOR = 0;
    double numTuplesAND = 0;

    // Save state for future ORing
    Statistics stats(*this);

    // Updates numTuplesOR as result of ORing all predicates with literal values
    // Update state for direct use in ANDing
    int stateIndex = processORWithLitValues(orExpressions,relations,numTuplesOR);
    
    if(stateIndex == orExpressions.size())  return;
        
    // OR remaining predicates 
    for (int index = stateIndex; index < orExpressions.size(); index++) {
        numTuplesOR += getStatsForState(stats,orExpressions[index],relations);
    }

    // AND remaining predicates
    evaluateANDofOR(orExpressions,relations,stateIndex,numTuplesAND);
    char *mergingOperand = orExpressions[stateIndex]->left->value;
    AddRel(mergingOperand,numTuplesOR-numTuplesAND); //update num tuples value
}

vector<ComparisonOp *> Statistics::flattenOrExpressionsTree(struct OrList *parseTree) {
    vector<ComparisonOp *> expressions;
    expressions.push_back(parseTree->left);
    while (parseTree->rightOr != NULL) {
        parseTree = parseTree->rightOr;
        expressions.push_back(parseTree->left);
    }
    // Sort expressions such that all attribute-literal predicates are first
    sort(expressions.begin(), expressions.end(), [] (ComparisonOp *exp1, ComparisonOp *exp2)
        {
            int op1Left,op1Right,op2Left,op2Right;
            char *op1Val, *op2Val;
            if (exp1->left->code == NAME) {
                op1Left = NAME;
                op1Right = exp1->right->code;
                if (op1Right == NAME && strcmp(exp1->left->value,exp1->right->value) > 0) {
                    op1Val = exp1->right->value;
                } else {
                    op1Val = exp1->left->value;
                }
            } else {
                op1Left = NAME;
                op1Right = exp1->left->code;
                op1Val = exp1->right->value;
            }

            if (exp2->left->code == NAME) {
                op2Left = NAME;
                op2Right = exp2->right->code;
                if (op2Right == NAME && strcmp(exp2->left->value,exp2->right->value) > 0) {
                    op2Val = exp2->right->value;
                } else {
                    op2Val = exp2->left->value;
                }
            } else {
                op2Left = NAME;
                op2Right = exp2->left->code;
                op2Val = exp2->right->value;
            }
            
            int retVal = 0;
            if (op1Left == NAME && op1Right == NAME) {
                retVal++;
                if (op2Left == NAME && op2Right == NAME) {
                    retVal = strcmp(op1Val,op2Val);
                }
            } else {
                retVal--;
                if (op2Left == NAME && op2Right != NAME) {
                    retVal = strcmp(op1Val,op2Val);
                }
            }
            
            return retVal;
        });
    return expressions;
}

int Statistics::processORWithLitValues(vector<ComparisonOp *> orExpressions, vector<string> relations, double &numTuplesOR) {
    // Save state for future ORing
    Statistics stats(*this);
    
    int end = 0;
    while (end < orExpressions.size() && isPredicateWithLitValue(orExpressions[end])) {
        end++;
    }

    int i = 0;
    while (i < end) {
        double sameOperandTuples = 0;
        char *prevOperand = orExpressions[i]->left->value;
        while (i < end && strcmp(prevOperand,orExpressions[i]->left->value) == 0) {
            sameOperandTuples += getStatsForState(stats,orExpressions[i++],relations);
        }
        AddRel(prevOperand,sameOperandTuples); //update num tuples value
        numTuplesOR += sameOperandTuples;
    }

    return end;
}


bool Statistics::isPredicateWithLitValue(ComparisonOp * exp) {
    return exp->right->code != NAME || exp->left->code != NAME;
}

double Statistics::getStatsForState(Statistics &stateToCopy, ComparisonOp * exp, vector<string> relations) {
    Statistics orStats(stateToCopy);
    return orStats.applyPredicate(exp,relations);
}


void Statistics::evaluateANDofOR(vector<ComparisonOp *> expressions, vector<string> relations, int index, double &numTuplesAND) {
    do
    {
        numTuplesAND += applyPredicate(expressions[index++],relations);
    } while (index < expressions.size());
}


double Statistics::applyPredicate(struct ComparisonOp *parseTree, vector<string> relations) {

}
