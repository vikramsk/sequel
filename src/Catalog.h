#ifndef Catalog_H
#define Catalog_H
#include <string>
#include <unordered_map>
#include <vector>

using namespace std;

class CatAttribute {
   private:
    string name;
    string type;

    friend class Catalog;
};

class Catalog {
   private:
    void parseCatalog();

   public:
    char* fileName;

    unordered_map<string, vector<CatAttribute*>> relAttributes;

    Catalog();
    void Write();
};
#endif
