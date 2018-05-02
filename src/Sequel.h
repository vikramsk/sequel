#include "Catalog.h"
#include "QueryPlanner.h"

class Sequel {
   private:
    string dbFilesDir;

    Catalog catalog;

    void performProjection(Pipe &inPipe, Pipe &outPipe, int attIndex,
                           int numAttsInput, Schema *&schema, char *relName);

    void performSelection(string relName, Pipe &outPipe);

    void performDistinct(Pipe &inPipe, Pipe &outPipe, Schema &schema);

   public:
    Sequel();

    void doSelect(QueryTokens &qt, int &outType);
    void doDrop(char *refTable);
    void doInsert(char *refTable, char *refFile);
    void doCreate(struct CreateTable *createData, char *refTable);
    void doUpdate(char *refTable);
};
