#include <iostream>
#include "QueryPlanner.h"
#include "cpptoml.h"

using namespace std;

extern "C" {
int yysqlparse(void);
}

// the aggregate function (NULL if no agg)
extern struct FuncOperator *finalFunction;

// the list of tables and aliases in the query
extern struct TableList *tables;

// the predicate in the WHERE clause
extern struct AndList *boolean;

// grouping atts (NULL if no grouping)
extern struct NameList *groupingAtts;

// the set of attributes in the SELECT (NULL if no such atts)
extern struct NameList *attsToSelect;

// data associated with creating a Table
extern struct CreateTable *createData;

// 1 if there is a DISTINCT in a non-aggregate query
extern int distinctAtts;

// 1 if there is a DISTINCT in an aggregate query
extern int distinctFunc;

// Says whether it is a create table, insert, drop table, set output, or select
extern int command;

// The type of the output
extern int outType;

// a referenced file
extern char *refFile;

// a referenced table
extern char *refTable;

string dbfilesDir;
string catalogFile;

void doSelect() {
    QueryTokens qt(finalFunction, tables, boolean, groupingAtts, attsToSelect,
                   distinctAtts, distinctFunc, refFile);
    QueryPlanner qp(qt);
    qp.Create();
    qp.Execute(outType);
}

void doDropTable() {
    string binFile = dbfilesDir + string(refTable) + ".bin";
    ifstream stream(binFile);
    if (!stream.good()) {
        cout << refTable << " table does not exist.";
    } else {
        remove(binFile.c_str());
        remove(string(dbfilesDir + string(refTable) + ".meta").c_str());
        cout << "\nDropped " << refTable << " table.\n";
    }
    //TODO: delete table from catalog;
    // 1. Delete from map, 2. perform write out
}

void doInsertIntoTable() {
    DBFile dbfile;
    string binFileName = dbfilesDir + string(refTable) + ".bin";
    Schema sch(catalogFile.c_str(),refTable);
    dbfile.Open(binFileName.c_str());
    dbfile.Load(sch, refFile);
}

void initMain() {
    refFile = NULL;
    outType = SET_NONE;
    auto config = cpptoml::parse_file("config.toml");

    auto dbfilePath = config->get_as<string>("dbfiles");
    dbfilesDir = *dbfilePath;

    auto catalogFilePath = config->get_as<string>("catalog");
    catalogFile = *catalogFilePath;
}

int main() {
    initMain();
    bool quit = false;
    while (!quit) {
        command = 0;
        cout << "\nSQL> ";
        yysqlparse();
        switch (command) {
            case CREATE: {
            } break;
            case INSERT_INTO: {
                doInsertIntoTable();
            } break;
            case DROP: {
                doDropTable();
            } break;
            case OUTPUT_SET: {
                cout << "\nOutput mode has been set!\n";
            } break;
            case SELECT_TABLE: {
                doSelect();
            } break;
            case QUIT_SQL: {
                quit = true;
            } break;
        }
    }
}
