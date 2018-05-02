#include "Sequel.h"
#include "Catalog.h"
#include "ParseTree.h"
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
        cout << endl << "Table: " << refTable << " does not exist." << endl;
    } else {
        remove(binFile.c_str());
        remove(string(dbFilesDir + string(refTable) + ".meta").c_str());
        cout << endl << "Dropped " << refTable << " table." << endl;
    }
    string relName(refTable);
    catalog.relAttributes.erase(relName);
    catalog.Write();
}

void Sequel::performSelection(string relName, Pipe &outPipe) {
    DBFile dbfile;
    string filePath = dbFilesDir + relName + ".bin";
    dbfile.Open(filePath.c_str());

    SelectFile *selFile = new SelectFile();
    selFile->Use_n_Pages(100);
    Record literal;
    CNF cnf;
    selFile->Run(dbfile, outPipe, cnf, literal);
}

void Sequel::performProjection(Pipe &inPipe, Pipe &outPipe, int attIndex,
                               int numAttsInput, Schema *schema,
                               char *relName) {
    int *attsToKeep = new int[1]{attIndex};
    Project *proj = new Project();
    proj->Use_n_Pages(100);

    Schema inSchema(catalog.fileName, relName);

    Attribute *attsList = new Attribute[1]{inSchema.GetAtts()[attIndex]};
    schema = new Schema("proj_schema", 1, attsList);
    proj->Run(inPipe, outPipe, attsToKeep, numAttsInput, 1);
}

void Sequel::performDistinct(Pipe &inPipe, Pipe &outPipe, Schema &schema) {
    DuplicateRemoval *dup = new DuplicateRemoval();
    dup->Run(inPipe, outPipe, schema);
}

void Sequel::doUpdate(char *refTable) {
    Statistics stats;
    auto config = cpptoml::parse_file("config.toml");
    auto entry = config->get_as<string>("stats");
    char *statsPath = new char[entry->length() + 1];
    strcpy(statsPath, entry->c_str());

    stats.Read(statsPath);
    string relName(refTable);
    if (!catalog.relAttributes.count(relName)) {
        cout << "Table: " << relName << " doesn't exist in the database."
             << endl;
        return;
    }

    Pipe recPipe(5000);
    performSelection(relName, recPipe);
    Record dummy;
    int numTuples = 0;
    while (recPipe.Remove(&dummy)) {
        numTuples++;
    }
    int numAtts = catalog.relAttributes[relName].size();
    for (int i = 0; i < numAtts; i++) {
        Pipe selPipe(5000);
        performSelection(relName, selPipe);

        Pipe projPipe(5000);
        Schema schema;
        performProjection(selPipe, projPipe, i, numAtts, &schema, refTable);

        Pipe outPipe(5000);
        performDistinct(projPipe, outPipe, schema);

        Record rec;
        int distinctCount = 0;
        while (outPipe.Remove(&rec)) {
            distinctCount++;
        }
        char *attName = strdup(catalog.relAttributes[relName][i]->name.c_str());
        stats.AddAtt(refTable, attName, distinctCount);
    }
    stats.Write(statsPath);
}

void Sequel::doInsert(char *refTable, char *refFile) {
    DBFile dbfile;
    string binFileName = dbFilesDir + string(refTable) + ".bin";
    Schema sch(catalog.fileName, refTable);
    dbfile.Open(binFileName.c_str());
    dbfile.Load(sch, refFile);
    dbfile.Close();
    cout << endl << "Loaded records into " << refTable << " table." << endl;
}

void Sequel::doCreate(CreateTable *createData, char *refTable) {
    string relName(refTable);
    if (catalog.relAttributes.count(relName) > 0) {
        cout << endl
             << "Table: " << relName << " already exists in the database."
             << endl;
        return;
    }

    AttDesc *attPtr = createData->atts;
    while (attPtr) {
        string type;
        switch (attPtr->type) {
            case DOUBLE: {
                type = "Double";
            } break;
            case INT: {
                type = "Int";
            } break;
            case STRING: {
                type = "String";
            } break;
        }
        CatAttribute *att = new CatAttribute();
        string attName(attPtr->name);
        string attType(type);
        att->name = attName;
        att->type = attType;
        catalog.relAttributes[relName].push_back(att);
        attPtr = attPtr->next;
    }
    catalog.Write();
    string filePath = dbFilesDir + string(refTable) + ".bin";
    if (createData->type == HEAP_DB) {
        DBFile dbfile;
        dbfile.Create(filePath.c_str(), heap, NULL);
        dbfile.Close();
        cout << endl << "Heap DB file created." << endl;
        return;
    }
}
