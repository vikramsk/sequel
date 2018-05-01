#include <unordered_map>

class Attribute {
   private:
    string name;
    string type;

    friend class Catalog;
};

class Catalog {
    string fileName;

    unordered_map < string, vector<Attribute> attributes;

   public:
    Catalog(string fileName);
    Write();
};
