#include "QueryPlanner.h"
#include <iostream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include "ComparisonEngine.h"
#include "ParseTree.h"
#include "Schema.h"
#include "cpptoml.h"

using namespace std;

vector<char *> convertToCStrings(vector<string> strs) {
    vector<char *> cstrings{};

    for (auto &str : strs) {
        cstrings.push_back(&str.front());
    }
    return cstrings;
}

char *getCString(string str) {
    char *val = new char[str.length() + 1];
    strcpy(val, str.c_str());
    return val;
}

void Node::Print(int &inPipeL_ID, int &inPipeR_ID, int &outPipe_ID) {
    cout << endl << endl;
    switch(operation) {
        case JOIN:
            cout << "JOIN : " << endl;
            cout << ">>> Input pipe ID 1 : " << inPipeL_ID << endl;
            cout << ">>> Input pipe ID 2 : " << inPipeR_ID << endl;
            cout << ">>> Output pipe ID : " << outPipe_ID << endl;
            cout << ">>> CNF : ";
            cnf.Print();
            cout << ">>> Literal : ";
            literal.Print(outSchema);
            break;
        case SELFILE:
            cout << "SELECT FILE : " << endl;
            cout << ">>> Output pipe ID : " << outPipe_ID << endl;
            cout << ">>> Input file : " << endl; //TODO: Print filename
            cout << ">>> CNF : ";
            cnf.Print();
            cout << ">>> Literal : ";
            literal.Print(outSchema);
            break;
        case PROJECT:
            cout << "PROJECT : " << endl;
            cout << ">>> Input pipe ID : " << inPipeL_ID << endl;
            cout << ">>> Output pipe ID : " << outPipe_ID << endl;
            cout << ">>> No. of input attributes : " << numAttsIn;
            cout << ">>> No. of attributes to keep : " << numAttsOut;
            cout << ">>> Index of attributes to keep : ";
            for ( int i = 0; i < numAttsOut; i++ ) {
    	        cout << attsToKeep[i] << ' ';
            }
            cout << endl;
            break; 
        case SUM: 
            cout << "SUM : " << endl;
            cout << ">>> Input pipe ID : " << inPipeL_ID << endl;
            cout << ">>> Output pipe ID : " << outPipe_ID << endl;
            cout << ">>> Function : " << endl; //TODO: Print function
            break;
        case GROUPBY:
            cout << "GROUP BY : " << endl;
            cout << ">>> Input pipe ID : " << inPipeL_ID << endl;
            cout << ">>> Output pipe ID : " << outPipe_ID << endl;
            cout << ">>> Attributes to group by : ";
            groupOrder->Print();
            cout << ">>> Function : " << endl; //TODO: Print function
            break;
        case DISTINCT:
            cout << "DUPLICATE REMOVAL : " << endl;
            cout << ">>> Input pipe ID : " << inPipeL_ID << endl;
            cout << ">>> Output pipe ID : " << outPipe_ID << endl;
            cout << ">>> Schema : ";
            OrderMaker om(outSchema);
            om.Print();
            break;
        // Note that SELPIPE is not used here
    }
}

QueryPlanner::~QueryPlanner() {}

void QueryPlanner::Create() {
    auto relAliasMap = initializeStats();

    if (tokens.andList) processAndList(relAliasMap);

    for (auto r : relationNode) {
        r.second->cnf.Print();
    }
    if (tokens.aggFunction) processAggFuncs();
}

void QueryPlanner::Print() {
    Node *ptr = root;
    int outCounter = 0;
    recurseAndPrint(ptr, outCounter);
}

int QueryPlanner::recurseAndPrint(Node *ptr,int &outPipe_ID) {
    if (!ptr) return 0;
    int inPipeL_ID = recurseAndPrint(ptr->leftLink, outPipe_ID);
    int inPipeR_ID = recurseAndPrint(ptr->rightLink, outPipe_ID);
    outPipe_ID++;
    ptr->Print(inPipeL_ID, inPipeR_ID, outPipe_ID);
    return outPipe_ID;
}

// reads the table list and loads the stats
// for the relations given in the table list.
unordered_map<string, string> QueryPlanner::initializeStats() {
    auto config = cpptoml::parse_file("config.toml");
    auto entry = config->get_as<string>("stats");
    char *statsPath = new char[entry->length() + 1];
    strcpy(statsPath, entry->c_str());

    stats.Read(statsPath);

    TableList *tblPtr = tokens.tables;
    unordered_set<string> relList;
    unordered_map<string, string> relAliasMap;
    while (tblPtr != NULL) {
        if (tblPtr == NULL) cout << "Woot" << endl;

        stats.CopyRel(tblPtr->tableName, tblPtr->aliasAs);
        string tblAlias(tblPtr->aliasAs);
        relAliasMap[tblAlias] = string(tblPtr->tableName);

        // this loads the key in the map.
        tokens.relClauses[tblAlias];
        stats.DeleteRel(tblPtr->tableName);

        tblPtr = tblPtr->next;
    }

    // TODO: Delete all extra relations using the catalog.
}

void QueryPlanner::processAndList(unordered_map<string, string> relAliasMap) {
    tokens.createRelOrPairs();
    cout << "reached" << endl;
    createSelectionNodes(relAliasMap);
}

struct orListComparator {
    inline bool operator()(const OrList *or1, const OrList *or2) {
        if (or1->rightOr) {
            return false;
        }
        return true;
    }
};

AndList *createAndList(vector<OrList *> orList) {
    if (!orList.size()) return NULL;
    sort(orList.begin(), orList.end(), orListComparator());
    AndList *aList = new AndList();
    AndList *aListPtr = aList;
    for (auto &orl : orList) {
        aListPtr->left = orl;
        aListPtr = aListPtr->rightAnd;
    }
    return aList;
}

void QueryPlanner::createSelectionNodes(
    unordered_map<string, string> relAliasMap) {
    auto config = cpptoml::parse_file("config.toml");
    auto entry = config->get_as<string>("catalog");
    char *catalogPath = new char[entry->length() + 1];
    strcpy(catalogPath, entry->c_str());

    for (auto &rc : tokens.relClauses) {
        vector<OrList *> orList;
        for (auto &rp : rc.second) {
            if (rp->relations.size() == 1) {
                orList.push_back(rp->orList);
            }
        }

        AndList *andList = createAndList(orList);
        vector<string> relName{rc.first};
        vector<char *> relNames = convertToCStrings(relName);

        relationNode[rc.first] = new Node(SELFILE);
        relationNode[rc.first]->relations.insert(rc.first);
        relationNode[rc.first]->outSchema =
            new Schema(catalogPath, getCString(relAliasMap[rc.first]));

        if (andList) {
            stats.Apply(andList, relNames.data(), 1);
            relationNode[rc.first]->cnf.GrowFromParseTree(
                andList, relationNode[rc.first]->outSchema,
                relationNode[rc.first]->literal);
        }
    }
}

void QueryPlanner::processAggFuncs() {
    if (tokens.distinctNoAgg) {
        if (tokens.groupingAtts) {
            cerr << "group by without aggregate function is not supported by "
                    "this database (yet)"
                 << endl;
            exit(1);
        } else {
            createProjectNode();
            createDupRemovalNode();
        }
    } else {
        if (tokens.distinctWithAgg) {
            if (tokens.groupingAtts) {
                cerr << "group by with distinct sum function is not supported "
                        "by this database (yet)"
                     << endl;
                exit(1);
            } else {
                createProjectNode();  // TODO: Figure how to use attributes
                                      // inside the function
                createDupRemovalNode();
                createSumNode();
            }
        } else {
            if (tokens.groupingAtts) {
                if (tokens.aggFunction) {
                    createGroupByNode();
                    NameList *sumAtt;
                    *sumAtt = {"GroupSum",tokens.attsToSelect};
                    tokens.attsToSelect = sumAtt;
                    createProjectNode();
                }
                else {
                    cerr << "group by without aggregate function is not "
                            "supported by this database (yet)"
                         << endl;
                    exit(1);
                }
            } else if (tokens.aggFunction)
                createSumNode();
            else
                createProjectNode();
        }
    }
}

unordered_set<string> getRelations(struct ComparisonOp *cmpOp) {
    unordered_set<string> relations;
    if (cmpOp->left && cmpOp->left->code == NAME) {
        string attName(cmpOp->left->value);
        string relName = attName.substr(0, attName.find("."));
        relations.insert(relName);
    }
    if (cmpOp->right && cmpOp->right->code == NAME) {
        string attName(cmpOp->right->value);
        string relName = attName.substr(0, attName.find("."));
        relations.insert(relName);
    }
    return relations;
}

RelOrPair *parseOrPair(struct OrList *orList) {
    RelOrPair *rlp = new RelOrPair();
    rlp->orList = orList;
    OrList *orPtr = orList;
    while (orPtr) {
        unordered_set<string> rels = getRelations(orPtr->left);
        if (rels.size() > 0) rlp->relations.insert(rels.begin(), rels.end());
        orPtr = orPtr->rightOr;
    }
    return rlp;
}

void QueryTokens::createRelOrPairs() {
    AndList *andPtr = andList;
    while (andPtr) {
        RelOrPair *relOrPair = parseOrPair(andPtr->left);
        relOrPairs.push_back(relOrPair);
        for (auto rel : relOrPair->relations) {
            relClauses[rel].push_back(relOrPair);
        }
        andPtr = andPtr->rightAnd;
    }
}
