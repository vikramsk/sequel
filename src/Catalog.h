#include <string>
#include <unordered_map>

using namespace std;

class CatAttribute {
   private:
    string name;
    string type;

    friend class Catalog;
};

class Catalog {
   private:
    char* fileName;

    void parseCatalog();

   public:
    unordered_map<string, vector<CatAttribute*>> relAttributes;

    Catalog();
    void Write();
};
