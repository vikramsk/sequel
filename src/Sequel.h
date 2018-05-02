#include "Catalog.h"
#include "QueryPlanner.h"

class Sequel {
   private:
    string dbFilesDir;

    Catalog catalog;

   public:
    Sequel();

    void doSelect(QueryTokens &qt, int &outType);
    void doDrop(char *refTable);
    void doInsert(char *refTable, char *refFile);
    void doCreate(struct CreateTable *createData, char *refTable);
    void doUpdate(char *refTable);
};
