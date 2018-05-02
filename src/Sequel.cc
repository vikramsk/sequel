#include "Sequel.h"
#include "Catalog.h"
#include "QueryPlanner.h"
#include "cpptoml.h"

using namespace std;

Sequel::Sequel() {
    auto config = cpptoml::parse_file("config.toml");

    auto dbfilePath = config->get_as<string>("dbfiles");
    dbFilesDir = *dbfilePath;
}

void Sequel::doSelect(QueryTokens &qt, int &outType) {
    QueryPlanner qp(qt);
    qp.Create();
    qp.Execute(outType);
}

void Sequel::doDrop(char *refTable) {
    string binFile = dbFilesDir + string(refTable) + ".bin";
    ifstream stream(binFile);
    if (!stream.good()) {
        cout << endl << refTable << " table does not exist.";
    } else {
        remove(binFile.c_str());
        remove(string(dbFilesDir + string(refTable) + ".meta").c_str());
        cout << "\nDropped " << refTable << " table.\n";
    }
    string relName(refTable);
    catalog.relAttributes.erase(relName);
    catalog.Write();
}

void Sequel::doInsert(char *refTable, char *refFile) {
    DBFile dbfile;
    string binFileName = dbFilesDir + string(refTable) + ".bin";
    Schema sch(catalog.fileName, refTable);
    dbfile.Open(binFileName.c_str());
    dbfile.Load(sch, refFile);
}

void Sequel::doCreate(CreateTable *createData, char *refTable) {
    string relName(refTable);
    if (catalog.relAttributes.count(relName) > 0) {
        cout << "Table: " << relName << " already exists in the database."
             << endl;
        return;
    }
}

void Sequel::doUpdate(char *refTable) {}
